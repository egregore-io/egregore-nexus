#!/usr/bin/env node

import { createHash, randomBytes } from "node:crypto";
import {
  chmod,
  lstat,
  mkdir,
  open,
  readFile,
  realpath,
  rename,
  rm,
} from "node:fs/promises";
import { createRequire } from "node:module";
import { homedir } from "node:os";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { makeWindowsFilePrivate } from "./lib/windows-private-file.mjs";

const requireFromGateway = createRequire(new URL("../gateway/package.json", import.meta.url));
const { createClient } = requireFromGateway("@libsql/client");

const SNAPSHOT_SCHEMA = "nexus-v016-identity-continuity/1";
const RECEIPT_SCHEMA = "nexus-v016-identity-continuity-verified/1";
const FINGERPRINT_PREFIX = "nexus-v016-continuity|";

const STORE_SPECS = Object.freeze({
  gateway: Object.freeze({
    human_user: tableSpec("name_key", ["client_key", "password_hash"]),
    human_session: tableSpec("cookie_token", ["client_key", "cookie_token"]),
  }),
  daemon: Object.freeze({
    agents: tableSpec("agent_id", []),
    identity_sessions: tableSpec("runtime_id", ["client_key", "native_resume_key"]),
    native_thread_bindings: tableSpec("native_thread_binding", []),
  }),
});

const REQUIRED_LEGACY_COLUMNS = Object.freeze({
  human_user: [
    "client_key",
    "created_at",
    "daemon_session_id",
    "name",
    "name_key",
    "password_hash",
    "project",
    "updated_at",
  ],
  human_session: [
    "client_key",
    "cookie_token",
    "created_at",
    "daemon_session_id",
    "name",
    "project",
  ],
  agents: [
    "agent_id",
    "created_at",
    "dead_reason",
    "default_harness",
    "disabled_at",
    "lifecycle_state",
    "metadata_json",
    "name",
    "owner_agent_id",
    "owner_name",
    "owner_project",
    "owner_session_id",
    "project",
    "role",
    "tier",
  ],
  identity_sessions: [
    "agent_id",
    "backend",
    "client_key",
    "cwd",
    "harness",
    "mode",
    "native_resume_key",
    "project",
    "runtime_id",
    "updated_at",
  ],
  native_thread_bindings: [
    "agent_id",
    "created_at",
    "first_runtime_id",
    "harness",
    "last_runtime_id",
    "native_thread_id",
    "project",
    "released_at",
    "updated_at",
  ],
});

await main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});

async function main() {
  const options = parseArguments(process.argv.slice(2));
  if (options.help) {
    process.stdout.write(usage());
    return;
  }

  const paths = await resolveStorePaths(options);
  if (options.command === "snapshot") {
    const snapshot = await collectSnapshot(paths);
    await writeCanonicalAtomic(options.out, snapshot);
    process.stdout.write("identity continuity snapshot written\n");
    return;
  }

  const preBytes = await readFile(options.pre);
  const pre = parseSnapshot(preBytes);
  await verifySnapshot(pre, paths);
  await verifyOrPublishMintedIds(options.pre, preBytes, paths.gateway);
  process.stdout.write("identity continuity verified\n");
}

function parseArguments(argv) {
  if (argv.length === 0 || argv.includes("--help") || argv.includes("-h")) {
    return { help: true };
  }
  const command = argv[0];
  if (command !== "snapshot" && command !== "verify") {
    throw new Error(`usage error: expected snapshot or verify`);
  }

  const values = new Map();
  for (let index = 1; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith("--") || value === undefined || value.startsWith("--")) {
      throw new Error(`usage error: ${key ?? "missing option"} requires one value`);
    }
    if (!["--out", "--pre", "--gateway-db", "--daemon-db"].includes(key)) {
      throw new Error(`usage error: unknown option ${key}`);
    }
    if (values.has(key)) throw new Error(`usage error: duplicate option ${key}`);
    values.set(key, value);
  }

  const required = command === "snapshot" ? "--out" : "--pre";
  const forbidden = command === "snapshot" ? "--pre" : "--out";
  if (!values.has(required) || values.has(forbidden)) {
    throw new Error(`usage error: ${command} requires ${required}`);
  }
  return {
    help: false,
    command,
    out: values.get("--out"),
    pre: values.get("--pre"),
    gatewayDb: values.get("--gateway-db"),
    daemonDb: values.get("--daemon-db"),
  };
}

function usage() {
  return [
    "Usage:",
    "  v016-identity-continuity.mjs snapshot --out FILE [--gateway-db FILE] [--daemon-db FILE]",
    "  v016-identity-continuity.mjs verify --pre FILE [--gateway-db FILE] [--daemon-db FILE]",
    "",
  ].join("\n");
}

async function resolveStorePaths(options) {
  const gatewayRaw = options.gatewayDb
    ?? process.env.NEXUS_GATEWAY_DB
    ?? process.env.NEXUS_WEBCONSOLE_DB
    ?? join(homedir(), ".nexus", "gateway.db");
  const daemonRaw = options.daemonDb
    ?? process.env.NEXUS_DAEMON_DB
    ?? process.env.NEXUS_DB_PATH
    ?? join(process.env.NEXUS_HOME || join(homedir(), ".nexus"), "nexus.db");
  return {
    gateway: await resolveDatabasePath("gateway", gatewayRaw),
    daemon: await resolveDatabasePath("daemon", daemonRaw),
  };
}

async function resolveDatabasePath(label, raw) {
  let path = String(raw).trim();
  if (path.startsWith("file:")) {
    if (path.startsWith("file:~/")) path = join(homedir(), path.slice("file:~/".length));
    else path = fileURLToPath(path);
  } else if (path.startsWith("~/")) {
    path = join(homedir(), path.slice(2));
  }
  path = resolve(path);
  if (!isAbsolute(path)) throw new Error(`${label} store path must be absolute`);
  const info = await lstat(path);
  if (!info.isFile()) throw new Error(`${label} store is not a regular file`);
  return realpath(path);
}

async function collectSnapshot(paths) {
  const [gateway, daemon] = await Promise.all([
    collectStore(paths.gateway, STORE_SPECS.gateway),
    collectStore(paths.daemon, STORE_SPECS.daemon),
  ]);
  return {
    schemaVersion: SNAPSHOT_SCHEMA,
    stores: { daemon, gateway },
  };
}

async function collectStore(path, specs) {
  return withReadonlyDatabase(path, async (db) => {
    const tables = {};
    for (const [table, spec] of Object.entries(specs)) {
      tables[table] = await inventoryTable(db, table, spec);
    }
    return { tables };
  });
}

async function inventoryTable(db, table, spec) {
  const columns = await tableColumns(db, table);
  requireSnapshotColumns(table, columns);
  const result = await db.execute(`SELECT * FROM source.${quoteIdentifier(table)}`);
  const rows = result.rows.map((row) => inventoryRow(table, spec, columns, row));
  rows.sort((left, right) => left.row.localeCompare(right.row));
  return { columns: [...columns].sort(), rows };
}

function inventoryRow(table, spec, columns, row) {
  const values = {};
  for (const column of [...columns].sort()) {
    values[column] = serializeValue(table, column, row[column], spec.secrets.has(column));
  }
  return {
    row: rowIdentity(table, spec, row),
    values,
  };
}

function parseSnapshot(bytes) {
  let value;
  try {
    value = JSON.parse(bytes.toString("utf8"));
  } catch {
    throw new Error("continuity snapshot is not valid JSON");
  }
  if (!isPlainObject(value) || value.schemaVersion !== SNAPSHOT_SCHEMA) {
    throw new Error("continuity snapshot schema is unsupported");
  }
  for (const [store, specs] of Object.entries(STORE_SPECS)) {
    const tables = value.stores?.[store]?.tables;
    if (!isPlainObject(tables)) throw new Error(`continuity snapshot is missing ${store} tables`);
    for (const table of Object.keys(specs)) {
      const inventory = tables[table];
      if (!isPlainObject(inventory) || !Array.isArray(inventory.columns) || !Array.isArray(inventory.rows)) {
        throw new Error(`continuity snapshot is missing ${table}`);
      }
      if (!inventory.columns.every((column) => typeof column === "string")) {
        throw new Error(`continuity snapshot has invalid ${table} columns`);
      }
      requireSnapshotColumns(table, inventory.columns);
      for (const row of inventory.rows) {
        if (!isPlainObject(row) || typeof row.row !== "string" || !isPlainObject(row.values)) {
          throw new Error(`continuity snapshot has invalid ${table} rows`);
        }
      }
    }
  }
  return value;
}

async function verifySnapshot(pre, paths) {
  await withReadonlyDatabase(paths.gateway, async (db) => {
    await verifyStoreRows(db, pre.stores.gateway.tables, STORE_SPECS.gateway);
    await verifyHumanIdentityShape(db);
  });
  await withReadonlyDatabase(paths.daemon, async (db) => {
    await verifyStoreRows(db, pre.stores.daemon.tables, STORE_SPECS.daemon);
  });
}

async function verifyStoreRows(db, inventories, specs) {
  for (const [table, spec] of Object.entries(specs)) {
    const inventory = inventories[table];
    const currentColumns = await tableColumns(db, table);
    const expectedColumns = inventory.columns;
    const selected = expectedColumns.map((column) => currentColumn(table, column));
    for (const column of selected) {
      if (!currentColumns.includes(column)) {
        drift(table, "schema", column);
      }
    }
    if (table === "native_thread_bindings" && expectedColumns.includes("harness")) {
      if (!currentColumns.includes("kind")) drift(table, "schema", "kind");
    }

    const result = await db.execute(`SELECT * FROM source.${quoteIdentifier(table)}`);
    const currentByRow = new Map();
    for (const row of result.rows) {
      const projected = {};
      for (const expectedColumn of expectedColumns) {
        const actualColumn = currentColumn(table, expectedColumn);
        projected[expectedColumn] = serializeValue(
          table,
          expectedColumn,
          row[actualColumn],
          spec.secrets.has(expectedColumn),
        );
      }
      currentByRow.set(rowIdentity(table, spec, row), projected);
      if (
        table === "native_thread_bindings"
        && expectedColumns.includes("harness")
        && row.kind !== "harness"
      ) {
        drift(table, rowIdentity(table, spec, row), "kind");
      }
    }

    for (const expected of inventory.rows) {
      const current = currentByRow.get(expected.row);
      if (!current) drift(table, expected.row, "row");
      for (const column of expectedColumns) {
        if (canonicalJson(current[column]) !== canonicalJson(expected.values[column])) {
          drift(table, expected.row, column);
        }
      }
    }
  }
}

async function verifyHumanIdentityShape(db) {
  const users = (await db.execute(
    "SELECT name_key, client_key, human_user_id FROM source.human_user ORDER BY name_key",
  )).rows;
  const seenHumanIds = new Set();
  const stableRows = [];
  for (const user of users) {
    const row = `name_key:${safeRowPart(user.name_key)}`;
    const humanUserId = asNonEmptyString(user.human_user_id, "human_user", row, "human_user_id");
    if (seenHumanIds.has(humanUserId)) drift("human_user", row, "human_user_id");
    seenHumanIds.add(humanUserId);
    const principalId = `h_${humanUserId.slice(3)}`;
    const principals = (await db.execute({
      sql: `SELECT p.principal_id, p.kind, p.access
              FROM source.principal_aliases AS a
              JOIN source.principals AS p ON p.principal_id = a.principal_id
             WHERE a.alias = ?`,
      args: [`human:${humanUserId}`],
    })).rows;
    if (
      principals.length !== 1
      || principals[0].principal_id !== principalId
      || principals[0].kind !== "local.human"
      || principals[0].access !== "admin"
    ) {
      drift("principals", row, "principal_id");
    }
    stableRows.push({ humanUserId, principalId, row });
  }

  const sessions = (await db.execute(
    `SELECT s.cookie_token, s.human_user_id, s.principal_id,
            u.human_user_id AS expected_human_user_id
       FROM source.human_session s
       LEFT JOIN source.human_user u ON u.client_key = s.client_key`,
  )).rows;
  for (const session of sessions) {
    const row = `cookie_token:${fingerprint("human_session", "cookie_token", session.cookie_token)}`;
    if (
      session.expected_human_user_id === null
      || session.human_user_id !== session.expected_human_user_id
    ) {
      drift("human_session", row, "human_user_id");
    }
    const expectedPrincipalId = `h_${String(session.human_user_id).slice(3)}`;
    if (session.principal_id !== expectedPrincipalId) {
      drift("human_session", row, "principal_id");
    }
  }
  return stableRows;
}

async function verifyOrPublishMintedIds(prePath, preBytes, gatewayPath) {
  const minted = await withReadonlyDatabase(gatewayPath, verifyHumanIdentityShape);
  const receipt = {
    minted: minted.sort((left, right) => left.row.localeCompare(right.row)),
    preSha256: createHash("sha256").update(preBytes).digest("hex"),
    schemaVersion: RECEIPT_SCHEMA,
  };
  const receiptPath = `${prePath}.verified.json`;
  let existing;
  try {
    existing = JSON.parse(await readFile(receiptPath, "utf8"));
  } catch (error) {
    if (error?.code !== "ENOENT") throw new Error("continuity verification receipt is invalid");
  }
  if (existing === undefined) {
    await writeCanonicalAtomic(receiptPath, receipt);
    return;
  }
  if (canonicalJson(existing) !== canonicalJson(receipt)) {
    const oldByRow = new Map((existing.minted ?? []).map((row) => [row.row, row]));
    for (const row of receipt.minted) {
      const old = oldByRow.get(row.row);
      if (!old || old.humanUserId !== row.humanUserId) {
        drift("human_user", row.row, "human_user_id");
      }
      if (old.principalId !== row.principalId) drift("principals", row.row, "principal_id");
    }
    throw new Error("continuity verification receipt does not match the snapshot");
  }
}

async function withReadonlyDatabase(path, operation) {
  const db = createClient({ url: ":memory:" });
  const uri = `${pathToFileURL(path).href}?mode=ro&immutable=1`;
  try {
    await db.execute(`ATTACH DATABASE '${uri.replaceAll("'", "''")}' AS source`);
    return await operation(db);
  } finally {
    db.close();
  }
}

async function tableColumns(db, table) {
  const result = await db.execute(`PRAGMA source.table_info(${quoteString(table)})`);
  if (result.rows.length === 0) throw new Error(`continuity store is missing table ${table}`);
  return result.rows.map((row) => String(row.name));
}

function requireSnapshotColumns(table, columns) {
  const required = REQUIRED_LEGACY_COLUMNS[table];
  for (const column of required) {
    if (!columns.includes(column)) {
      throw new Error(`continuity snapshot is missing ${table}.${column}`);
    }
  }
}

function currentColumn(table, expectedColumn) {
  return table === "native_thread_bindings" && expectedColumn === "harness"
    ? "provider"
    : expectedColumn;
}

function rowIdentity(table, spec, row) {
  if (spec.key === "native_thread_binding") {
    const provider = row.provider ?? row.harness;
    return `provider:${safeRowPart(provider)}/native_thread_id:${safeRowPart(row.native_thread_id)}`;
  }
  const value = row[spec.key];
  if (spec.secrets.has(spec.key)) {
    return `${spec.key}:${fingerprint(table, spec.key, value)}`;
  }
  return `${spec.key}:${safeRowPart(value)}`;
}

function serializeValue(table, column, value, secret) {
  if (secret && value !== null && value !== undefined) {
    return { sha256: fingerprint(table, column, value) };
  }
  if (typeof value === "bigint") return { integer: value.toString() };
  if (value instanceof Uint8Array) return { base64: Buffer.from(value).toString("base64") };
  if (["string", "number", "boolean"].includes(typeof value) || value === null) return value;
  if (value === undefined) return null;
  throw new Error(`continuity store contains unsupported ${table}.${column} value`);
}

function fingerprint(table, column, value) {
  const normalized = value instanceof Uint8Array
    ? Buffer.from(value).toString("base64")
    : String(value);
  return createHash("sha256")
    .update(`${FINGERPRINT_PREFIX}${table}.${column}|${normalized}`)
    .digest("hex");
}

function tableSpec(key, secrets) {
  return Object.freeze({ key, secrets: new Set(secrets) });
}

function safeRowPart(value) {
  if (value === null || value === undefined || String(value) === "") return "<missing>";
  return encodeURIComponent(String(value));
}

function asNonEmptyString(value, table, row, column) {
  if (typeof value !== "string" || value.length === 0) drift(table, row, column);
  return value;
}

function drift(table, row, column) {
  throw new Error(`continuity drift: table=${table} row=${row} column=${column}`);
}

async function writeCanonicalAtomic(path, value) {
  const target = resolve(path);
  const parent = dirname(target);
  await mkdir(parent, { recursive: true });
  const temporary = join(
    parent,
    `.${basename(target)}.tmp-${process.pid}-${randomBytes(8).toString("hex")}`,
  );
  let handle;
  try {
    handle = await open(temporary, "wx", 0o600);
    if (process.platform === "win32") await makeWindowsFilePrivate(temporary);
    await handle.writeFile(`${canonicalJson(value)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = undefined;
    if (process.platform !== "win32") await chmod(temporary, 0o600);
    await rename(temporary, target);
    // Windows does not support fsync on this directory handle. The file itself was flushed
    // before rename; directory-entry crash durability is only asserted on POSIX.
    if (process.platform !== "win32") {
      const directory = await open(parent, "r");
      try {
        await directory.sync();
      } finally {
        await directory.close();
      }
    }
  } finally {
    if (handle) await handle.close().catch(() => {});
    await rm(temporary, { force: true }).catch(() => {});
  }
}

function canonicalJson(value) {
  return JSON.stringify(sortValue(value), null, 2);
}

function sortValue(value) {
  if (Array.isArray(value)) return value.map(sortValue);
  if (!isPlainObject(value)) return value;
  return Object.fromEntries(
    Object.keys(value)
      .sort()
      .map((key) => [key, sortValue(value[key])]),
  );
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function quoteIdentifier(value) {
  if (!/^[a-z_][a-z0-9_]*$/.test(value)) throw new Error("invalid continuity table name");
  return `"${value}"`;
}

function quoteString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}
