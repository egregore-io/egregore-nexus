import type { Client, Row } from "@libsql/client";

import type { GatewayChangeBus } from "../changeBus";

export interface CanonicalGatewayMessage {
  messageId: string;
  kind: string;
  fromName?: string;
  fromAgentId?: string;
  toName?: string;
  toAgentId?: string;
  threadId?: string;
  topic?: string;
  summary?: string;
  body: string;
  provenance: Record<string, unknown>;
  createdAt: number;
}

export type CanonicalMessageTarget =
  | { threadId: string }
  | { threadName: string }
  | { toAgentId: string }
  | { dmAgentId: string }
  | { dmName: string }
  | { topic: string };

export interface MessagePageOptions {
  limit: number;
  before?: string;
  after?: string;
}

export interface CanonicalMessagePage {
  messages: CanonicalGatewayMessage[];
  before?: string;
  after?: string;
  rebased: boolean;
}

interface MessageCursor {
  v: 1;
  at: number;
  id: string;
}

export function encodeMessageCursor(createdAt: number, messageId: string): string {
  return Buffer.from(JSON.stringify({ v: 1, at: createdAt, id: messageId })).toString(
    "base64url",
  );
}

function decodeMessageCursor(value: string): MessageCursor | null {
  try {
    const decoded = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as Partial<MessageCursor>;
    if (
      decoded.v !== 1 ||
      typeof decoded.at !== "number" ||
      !Number.isFinite(decoded.at) ||
      typeof decoded.id !== "string" ||
      decoded.id.length === 0
    ) {
      return null;
    }
    return decoded as MessageCursor;
  } catch {
    return null;
  }
}

interface TargetClause {
  sql: string;
  args: string[];
  key: string;
}

function targetClause(target: CanonicalMessageTarget): TargetClause {
  if ("threadId" in target) {
    return { sql: "thread_id = ?", args: [target.threadId], key: `thread:${target.threadId}` };
  }
  if ("threadName" in target) {
    return {
      sql: "kind = 'thread' AND to_name = ?",
      args: [target.threadName],
      key: `thread-name:${target.threadName}`,
    };
  }
  if ("toAgentId" in target) {
    return { sql: "to_agent_id = ?", args: [target.toAgentId], key: `dm:${target.toAgentId}` };
  }
  if ("dmAgentId" in target) {
    return {
      sql: "kind = 'dm' AND (from_agent_id = ? OR to_agent_id = ?)",
      args: [target.dmAgentId, target.dmAgentId],
      key: `dm:${target.dmAgentId}`,
    };
  }
  if ("dmName" in target) {
    return {
      sql: "kind = 'dm' AND (from_name = ? OR to_name = ?)",
      args: [target.dmName, target.dmName],
      key: `dm-name:${target.dmName}`,
    };
  }
  return { sql: "topic = ?", args: [target.topic], key: `topic:${target.topic}` };
}

function messageKeys(message: CanonicalGatewayMessage): string[] {
  const keys: string[] = [];
  if (message.threadId) keys.push(`thread:${message.threadId}`);
  if (message.toName && message.kind === "thread") keys.push(`thread-name:${message.toName}`);
  if (message.fromAgentId && message.kind === "dm") keys.push(`dm:${message.fromAgentId}`);
  if (message.toAgentId) keys.push(`dm:${message.toAgentId}`);
  if (message.fromName && message.kind === "dm") keys.push(`dm-name:${message.fromName}`);
  if (message.toName && message.kind === "dm") keys.push(`dm-name:${message.toName}`);
  if (message.topic) keys.push(`topic:${message.topic}`);
  return [...new Set(keys)];
}

export async function insertCanonicalMessage(
  db: Client,
  message: CanonicalGatewayMessage,
  changeBus?: GatewayChangeBus,
): Promise<"inserted" | "duplicate"> {
  const result = await db.execute({
    sql: `INSERT OR IGNORE INTO bus_messages
          (message_id, kind, from_name, from_agent_id, to_name, to_agent_id,
           thread_id, topic, summary, body, provenance_json, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    args: [
      message.messageId,
      message.kind,
      message.fromName ?? null,
      message.fromAgentId ?? null,
      message.toName ?? null,
      message.toAgentId ?? null,
      message.threadId ?? null,
      message.topic ?? null,
      message.summary ?? null,
      message.body,
      JSON.stringify(message.provenance),
      message.createdAt,
    ],
  });
  if (result.rowsAffected === 0) return "duplicate";
  for (const key of messageKeys(message)) changeBus?.publish(key);
  return "inserted";
}

export async function pageCanonicalMessages(
  db: Client,
  target: CanonicalMessageTarget,
  options: MessagePageOptions,
): Promise<CanonicalMessagePage> {
  const limit = Math.max(1, Math.min(200, Math.trunc(options.limit)));
  const clause = targetClause(target);
  const suppliedCursor = options.before ?? options.after;
  const fromOrigin = options.after === "origin";
  let cursor = suppliedCursor && !fromOrigin ? decodeMessageCursor(suppliedCursor) : null;

  if (suppliedCursor && !fromOrigin && cursor) {
    const anchor = await db.execute({
      sql: `SELECT 1 FROM bus_messages
            WHERE ${clause.sql} AND created_at = ? AND message_id = ? LIMIT 1`,
      args: [...clause.args, cursor.at, cursor.id],
    });
    if (anchor.rows.length === 0) cursor = null;
  }

  if (suppliedCursor && !fromOrigin && !cursor) {
    const latest = await latestPage(db, clause, limit);
    return toPage(latest, true);
  }

  if (options.before && cursor) {
    const result = await db.execute({
      sql: `SELECT * FROM bus_messages
            WHERE ${clause.sql}
              AND (created_at < ? OR (created_at = ? AND message_id < ?))
            ORDER BY created_at DESC, message_id DESC LIMIT ?`,
      args: [...clause.args, cursor.at, cursor.at, cursor.id, limit],
    });
    return toPage(result.rows.map(mapMessage).reverse(), false);
  }

  if (options.after && cursor) {
    const result = await db.execute({
      sql: `SELECT * FROM bus_messages
            WHERE ${clause.sql}
              AND (created_at > ? OR (created_at = ? AND message_id > ?))
            ORDER BY created_at ASC, message_id ASC LIMIT ?`,
      args: [...clause.args, cursor.at, cursor.at, cursor.id, limit],
    });
    return toPage(result.rows.map(mapMessage), false);
  }

  if (fromOrigin) {
    const result = await db.execute({
      sql: `SELECT * FROM bus_messages WHERE ${clause.sql}
            ORDER BY created_at ASC, message_id ASC LIMIT ?`,
      args: [...clause.args, limit],
    });
    return toPage(result.rows.map(mapMessage), false);
  }

  return toPage(await latestPage(db, clause, limit), false);
}

async function latestPage(
  db: Client,
  clause: TargetClause,
  limit: number,
): Promise<CanonicalGatewayMessage[]> {
  const result = await db.execute({
    sql: `SELECT * FROM bus_messages WHERE ${clause.sql}
          ORDER BY created_at DESC, message_id DESC LIMIT ?`,
    args: [...clause.args, limit],
  });
  return result.rows.map(mapMessage).reverse();
}

function toPage(messages: CanonicalGatewayMessage[], rebased: boolean): CanonicalMessagePage {
  const first = messages[0];
  const last = messages.at(-1);
  return {
    messages,
    before: first ? encodeMessageCursor(first.createdAt, first.messageId) : undefined,
    after: last ? encodeMessageCursor(last.createdAt, last.messageId) : undefined,
    rebased,
  };
}

function mapMessage(row: Row): CanonicalGatewayMessage {
  return {
    messageId: String(row.message_id),
    kind: String(row.kind),
    fromName: optionalString(row.from_name),
    fromAgentId: optionalString(row.from_agent_id),
    toName: optionalString(row.to_name),
    toAgentId: optionalString(row.to_agent_id),
    threadId: optionalString(row.thread_id),
    topic: optionalString(row.topic),
    summary: optionalString(row.summary),
    body: String(row.body),
    provenance: JSON.parse(String(row.provenance_json)) as Record<string, unknown>,
    createdAt: Number(row.created_at),
  };
}

function optionalString(value: unknown): string | undefined {
  return value === null || value === undefined ? undefined : String(value);
}
