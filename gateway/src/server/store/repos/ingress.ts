import type { Client, Row } from "@libsql/client";

export interface GatewayIngressRow {
  idempotencyKey: string;
  commandId?: string;
  status: string;
  request: unknown;
  result?: unknown;
  error?: unknown;
  createdAt: number;
  updatedAt: number;
  callerPrincipalId?: string;
}

export interface BeginIngressInput {
  idempotencyKey: string;
  commandId?: string;
  request: unknown;
  now: number;
  callerPrincipalId?: string;
}

export async function beginIngress(
  db: Client,
  input: BeginIngressInput,
): Promise<{ created: boolean; row: GatewayIngressRow }> {
  const result = await db.execute({
    sql: `INSERT OR IGNORE INTO gateway_ingress
          (idempotency_key, command_id, status, request_json, result_json,
           error_json, created_at, updated_at, caller_principal_id)
          VALUES (?, ?, 'pending', ?, NULL, NULL, ?, ?, ?)`,
    args: [
      input.idempotencyKey,
      input.commandId ?? null,
      JSON.stringify(input.request),
      input.now,
      input.now,
      input.callerPrincipalId ?? null,
    ],
  });
  const row = await getIngress(db, input.idempotencyKey);
  if (!row) throw new Error(`failed to create Gateway ingress ${input.idempotencyKey}`);
  return { created: result.rowsAffected > 0, row };
}

export async function settleIngress(
  db: Client,
  idempotencyKey: string,
  settlement: {
    commandId: string;
    status: string;
    result?: unknown;
    error?: unknown;
    now: number;
  },
): Promise<void> {
  const result = await db.execute({
    sql: `UPDATE gateway_ingress SET
            command_id = ?, status = ?, result_json = ?, error_json = ?, updated_at = ?
          WHERE idempotency_key = ?`,
    args: [
      settlement.commandId,
      settlement.status,
      settlement.result === undefined ? null : JSON.stringify(settlement.result),
      settlement.error === undefined ? null : JSON.stringify(settlement.error),
      settlement.now,
      idempotencyKey,
    ],
  });
  if (result.rowsAffected === 0) {
    throw new Error(`unknown Gateway ingress ${idempotencyKey}`);
  }
}

export async function getIngress(
  db: Client,
  idempotencyKey: string,
): Promise<GatewayIngressRow | null> {
  const result = await db.execute({
    sql: "SELECT * FROM gateway_ingress WHERE idempotency_key = ? LIMIT 1",
    args: [idempotencyKey],
  });
  return result.rows[0] ? mapIngress(result.rows[0]) : null;
}

function mapIngress(row: Row): GatewayIngressRow {
  return {
    idempotencyKey: String(row.idempotency_key),
    commandId: optionalString(row.command_id),
    status: String(row.status),
    request: JSON.parse(String(row.request_json)),
    result: optionalJson(row.result_json),
    error: optionalJson(row.error_json),
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
    callerPrincipalId: optionalString(row.caller_principal_id),
  };
}

function optionalString(value: unknown): string | undefined {
  return value === null || value === undefined ? undefined : String(value);
}

function optionalJson(value: unknown): unknown {
  return value === null || value === undefined ? undefined : JSON.parse(String(value));
}
