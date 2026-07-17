import type {
  Client,
  InArgs,
  InStatement,
  InValue,
  ResultSet,
  Row,
} from "@libsql/client";

import { callDaemonQuery, type DaemonIpcCaller } from "./ipc";

interface DaemonSqlReadRequest {
  sql: string;
  args: unknown[];
}

interface DaemonSqlReadResponse {
  columns: string[];
  rows: unknown[][];
  rowsAffected: number;
}

export interface DaemonReadClientOptions {
  nexusHome?: string;
  query?: (params: DaemonSqlReadRequest) => Promise<DaemonSqlReadResponse>;
}

const gatewayCaller: DaemonIpcCaller = {
  name: "Nexus Gateway",
  project: "default",
  sessionId: "local-operator",
  runtimeId: "local-operator",
  kind: "human",
  tier: "admin",
};

/**
 * LibSQL-compatible read client whose actual SQL connection remains inside the daemon.
 *
 * Drizzle and the gateway's existing read projections keep their typed SELECT builders, while
 * the gateway process owns no canonical database handle. Mutating/transaction methods fail
 * closed; the daemon independently accepts only one SELECT statement per request.
 */
export function createDaemonReadClient(
  options: DaemonReadClientOptions = {},
): Client {
  const query = options.query ?? ((params: DaemonSqlReadRequest) =>
    callDaemonQuery<DaemonSqlReadResponse>(
      "local.store.read",
      params,
      gatewayCaller,
      {
        ...(options.nexusHome ? { nexusHome: options.nexusHome } : {}),
      },
    ));
  let closed = false;

  const execute = async (
    statement: InStatement,
    args?: InArgs,
  ): Promise<ResultSet> => {
    if (closed) throw new Error("daemon read client is closed");
    const normalized = normalizeStatement(statement, args);
    const response = await query({
      sql: normalized.sql,
      args: normalized.args.map(encodeValue),
    });
    return resultSet(response);
  };

  return {
    execute,
    batch: async (statements: Array<InStatement | [string, InArgs?]>) => {
      const results: ResultSet[] = [];
      for (const statement of statements) {
        const normalized = Array.isArray(statement)
          ? normalizeStatement(statement[0], statement[1])
          : normalizeStatement(statement);
        results.push(await execute(normalized));
      }
      return results;
    },
    migrate: async () => {
      throw new Error("daemon read client does not support migrations");
    },
    transaction: async () => {
      throw new Error("daemon read client does not support transactions");
    },
    executeMultiple: async () => {
      throw new Error("daemon read client accepts exactly one SELECT statement");
    },
    sync: async () => undefined,
    close: () => {
      closed = true;
    },
    reconnect: () => {
      closed = false;
    },
    get closed() {
      return closed;
    },
    protocol: "file",
  } as unknown as Client;
}

function normalizeStatement(statement: InStatement, args?: InArgs): {
  sql: string;
  args: InValue[];
} {
  return typeof statement === "string"
    ? { sql: statement, args: positionalArgs(args) }
    : { sql: statement.sql, args: positionalArgs(statement.args) };
}

function positionalArgs(args?: InArgs): InValue[] {
  if (!args) return [];
  if (Array.isArray(args)) return [...args];
  throw new Error("daemon read client accepts positional SQL arguments only");
}

function encodeValue(value: InValue): unknown {
  if (value instanceof Uint8Array) return { $blob: [...value] };
  if (value instanceof ArrayBuffer) return { $blob: [...new Uint8Array(value)] };
  if (typeof value === "bigint") return { $integer: value.toString() };
  if (value instanceof Date) return value.valueOf();
  return value;
}

function decodeValue(value: unknown): unknown {
  if (!value || typeof value !== "object" || Array.isArray(value)) return value;
  const marker = value as { $blob?: unknown; $integer?: unknown };
  if (Array.isArray(marker.$blob)) {
    return Uint8Array.from(marker.$blob.map(Number)).buffer;
  }
  if (typeof marker.$integer === "string") return BigInt(marker.$integer);
  return value;
}

function resultSet(response: DaemonSqlReadResponse): ResultSet {
  const rows = response.rows.map((values) => row(response.columns, values));
  return {
    columns: [...response.columns],
    columnTypes: response.columns.map(() => ""),
    rows,
    rowsAffected: response.rowsAffected,
    lastInsertRowid: undefined,
    toJSON() {
      return {
        columns: this.columns,
        rows: this.rows,
        rowsAffected: this.rowsAffected,
        lastInsertRowid: this.lastInsertRowid,
      };
    },
  } as ResultSet;
}

function row(columns: readonly string[], raw: readonly unknown[]): Row {
  const values = raw.map(decodeValue);
  const out: Record<PropertyKey, unknown> = {};
  for (let index = 0; index < values.length; index += 1) {
    Object.defineProperty(out, index, {
      value: values[index],
      enumerable: false,
      configurable: false,
    });
  }
  Object.defineProperty(out, "length", {
    value: values.length,
    enumerable: false,
  });
  Object.defineProperty(out, Symbol.iterator, {
    value: function* iterator() {
      yield* values;
    },
    enumerable: false,
  });
  columns.forEach((column, index) => {
    Object.defineProperty(out, column, {
      value: values[index],
      enumerable: true,
      configurable: false,
    });
  });
  return out as unknown as Row;
}
