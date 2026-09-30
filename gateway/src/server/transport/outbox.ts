import { createHash } from "node:crypto";

import type { Client, Row, Transaction } from "@libsql/client";

export interface TransportMessageRow {
  messageId: string;
  kind: string;
  toName?: string;
  threadId?: string;
  body: string;
  createdAt: number;
}

export interface TransportObligation {
  obligationId: string;
  messageId: string;
  provider: string;
  externalChatId: string;
  laneKind: "thread" | "dm";
  laneName: string;
  text: string;
  state: "pending" | "delivered";
  externalMessageId?: string;
  createdAt: number;
  settledAt?: number;
}

type SqlExecutor = Pick<Client, "execute"> | Pick<Transaction, "execute">;
type OutboxWakeListener = () => void;
const wakeListeners = new Set<OutboxWakeListener>();

export function onTransportOutboxWake(listener: OutboxWakeListener): () => void {
  wakeListeners.add(listener);
  return () => wakeListeners.delete(listener);
}

export function notifyTransportOutbox(): void {
  for (const listener of [...wakeListeners]) listener();
}

export function obligationIdFor(
  messageId: string,
  provider: string,
  externalChatId: string,
): string {
  const canonical = JSON.stringify({ externalChatId, messageId, provider });
  return `ob_${createHash("sha256").update(canonical, "utf8").digest("hex").slice(0, 24)}`;
}

/** The sole durable producer for external transport delivery obligations. */
export async function enqueueObligationsForMessage(
  db: SqlExecutor,
  message: TransportMessageRow,
): Promise<number> {
  const lane = await outboundLane(db, message);
  if (!lane) return 0;
  const chats = await db.execute({
    sql: `SELECT provider, external_chat_id
          FROM transport_lane_bindings
          WHERE lane_kind = ? AND lane_name = ?
          ORDER BY provider, external_chat_id`,
    args: [lane.kind, lane.name],
  });
  if (chats.rows.length === 0) {
    await db.execute({
      sql: `INSERT INTO logs (ts, level, scope, conversation_id, message, data)
            VALUES (?, 'warn', 'transport', ?, 'transport.no_route', ?)`,
      args: [message.createdAt, `${lane.kind}:${lane.name}`, JSON.stringify({
        code: "transport.no_route",
        messageId: message.messageId,
        laneKind: lane.kind,
        laneName: lane.name,
      })],
    });
    return 0;
  }

  let inserted = 0;
  for (const row of chats.rows) {
    const provider = String(row.provider);
    const externalChatId = String(row.external_chat_id);
    const result = await db.execute({
      sql: `INSERT OR IGNORE INTO transport_outbox
            (obligation_id, message_id, provider, external_chat_id, lane_kind,
             lane_name, text, state, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
      args: [
        obligationIdFor(message.messageId, provider, externalChatId),
        message.messageId,
        provider,
        externalChatId,
        lane.kind,
        lane.name,
        message.body,
        message.createdAt,
      ],
    });
    inserted += result.rowsAffected;
  }
  return inserted;
}

export async function listPendingObligations(
  db: SqlExecutor,
  provider: string,
  limit: number,
): Promise<TransportObligation[]> {
  const bounded = Math.max(1, Math.min(256, Math.trunc(limit)));
  const rows = await db.execute({
    sql: `SELECT * FROM transport_outbox
          WHERE provider = ? AND state = 'pending'
          ORDER BY created_at, obligation_id LIMIT ?`,
    args: [provider, bounded],
  });
  return rows.rows.map(mapObligation);
}

export async function settleObligation(
  db: SqlExecutor,
  obligationId: string,
  externalMessageId: string,
  settledAt = Date.now(),
): Promise<boolean> {
  const result = await db.execute({
    sql: `UPDATE transport_outbox
          SET state = 'delivered', external_message_id = ?, settled_at = ?
          WHERE obligation_id = ? AND state = 'pending'`,
    args: [externalMessageId, settledAt, obligationId],
  });
  return result.rowsAffected > 0;
}

async function outboundLane(
  db: SqlExecutor,
  message: TransportMessageRow,
): Promise<{ kind: "thread" | "dm"; name: string } | undefined> {
  if (message.kind === "thread") {
    let name = message.toName?.trim();
    if (!name && message.threadId) {
      const thread = await db.execute({
        sql: "SELECT name FROM threads WHERE thread_id = ? LIMIT 1",
        args: [message.threadId],
      });
      if (thread.rows[0]?.name) name = String(thread.rows[0].name);
    }
    return name ? { kind: "thread", name } : undefined;
  }
  if (message.kind === "dm") {
    const name = message.toName?.trim();
    return name ? { kind: "dm", name } : undefined;
  }
  return undefined;
}

function mapObligation(row: Row): TransportObligation {
  return {
    obligationId: String(row.obligation_id),
    messageId: String(row.message_id),
    provider: String(row.provider),
    externalChatId: String(row.external_chat_id),
    laneKind: String(row.lane_kind) as "thread" | "dm",
    laneName: String(row.lane_name),
    text: String(row.text),
    state: String(row.state) as "pending" | "delivered",
    externalMessageId: optionalString(row.external_message_id),
    createdAt: Number(row.created_at),
    settledAt: optionalNumber(row.settled_at),
  };
}

function optionalString(value: unknown): string | undefined {
  return value === null || value === undefined ? undefined : String(value);
}

function optionalNumber(value: unknown): number | undefined {
  return value === null || value === undefined ? undefined : Number(value);
}
