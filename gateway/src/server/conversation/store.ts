// The rendered-conversation cache is a bounded projection inside the Gateway's
// canonical persistent database. The compatibility exports in this module keep
// existing callers stable while storage ownership moves behind the Gateway client.
//
// AionUi pattern: `conversations` + `messages`, where a message stores the full
// rendered turn as JSON `content` (blocks incl. tool-calls), a `role`
// (you/agent/system), and a `status` (streaming → final). This is a stopgap; the
// JSON ledger here maps directly to PACT later via `file_create_from_ledger_json`
// (a conversation becomes a context tree that also carries the system message).
import type { Client } from "@libsql/client";

import { closeGatewayStore, getGatewayStore } from "../store/client";
import { gatewayStoreConfig, type GatewayStoreConfig } from "../store/config";
import { migrateGatewayStore } from "../store/migrations";

/** One persisted message row (its `content` is the JSON-encoded rendered turn). */
export interface StoredMessage {
  id: string;
  conversationId: string;
  /** Where it sits in the chat: the operator, the agent, or a system line. */
  role: "you" | "agent" | "system";
  /** Display author (operator name / agent name). */
  author: string;
  /** Opaque JSON the web renderer round-trips (a `PaneMessage`). */
  content: string;
  /** `streaming` while a turn is still arriving; `final` once settled. */
  status: "streaming" | "final";
  /** Epoch ms. */
  createdAt: number;
}

/** Compatibility name for the canonical, local-only Gateway store configuration. */
export function conversationDbConfig(env: NodeJS.ProcessEnv = process.env): GatewayStoreConfig {
  return gatewayStoreConfig(env);
}

/** Return the process-wide Gateway client and prune bounded operational residue. */
export async function createConversationStore(
  env: NodeJS.ProcessEnv = process.env,
): Promise<Client> {
  const client = await getGatewayStore(env);
  await pruneWebconsoleOperationalRows(client, webconsoleRetentionSettings(env));
  return client;
}

/** One durable log line — "if it moves it has a log". Written to the SAME store. */
export interface StoredLog {
  /** Monotonic-ish row id (autoincrement). Present on reads, omit on writes. */
  seq?: number;
  /** Epoch ms. */
  ts: number;
  /** debug | info | warn | error. */
  level: string;
  /** A short scope tag (e.g. "composer", "observe", "persist", "run"). */
  scope: string;
  /** The conversation this line belongs to (when applicable). */
  conversationId?: string;
  /** Human-readable message. */
  message: string;
  /** Optional structured detail (JSON-encoded). */
  data?: string;
}

/** Compatibility migration entrypoint for tests and callers with an injected client. */
export async function initSchema(db: Client): Promise<void> {
  await migrateGatewayStore(db);
}

/** Append one log line (durable, in the separate webconsole store). */
export async function appendLog(db: Client, l: StoredLog): Promise<void> {
  await appendLogs(db, [l]);
}

/** Append one request's log lines in one write round trip. */
export async function appendLogs(db: Client, logs: readonly StoredLog[]): Promise<void> {
  if (logs.length === 0) return;
  await db.batch(
    logs.map((l) => ({
      sql: `INSERT INTO logs (ts, level, scope, conversation_id, message, data)
            VALUES (?, ?, ?, ?, ?, ?)`,
      args: [l.ts, l.level, l.scope, l.conversationId ?? null, l.message, l.data ?? null],
    })),
    "write",
  );
}

export interface WebconsoleRetentionSettings {
  /** Operational log retention window. Negative disables pruning. */
  retentionMs: number;
  /** Shared size threshold setting; continuous threshold-triggered sweeps are daemon-owned. */
  sizeThresholdMb: number;
  /** Test seam. Defaults to Date.now(). */
  now?: number;
}

export interface WebconsoleRetentionSweep {
  logs: number;
  restBearerTokens: number;
}

/** Parse the shared retention env used by the daemon for operational tables. */
export function webconsoleRetentionSettings(
  env: NodeJS.ProcessEnv = process.env,
): WebconsoleRetentionSettings {
  return {
    retentionMs: parseIntegerEnv(env.NEXUS_COMMAND_INTENT_RETENTION_MS, 24 * 60 * 60 * 1000),
    sizeThresholdMb: parseIntegerEnv(env.NEXUS_REAP_SIZE_THRESHOLD_MB, 1024),
  };
}

/**
 * Prune webconsole-only operational residue. This intentionally does not delete the
 * `rendered_conversations`/`rendered_messages` refresh cache: those rows are user-visible state and need a
 * separate LRU/cache policy. The high-volume operational `logs` stream and expired/revoked bearer
 * token rows can be safely bounded by the shared retention window.
 */
export async function pruneWebconsoleOperationalRows(
  db: Client,
  settings: WebconsoleRetentionSettings = webconsoleRetentionSettings(),
): Promise<WebconsoleRetentionSweep> {
  if (settings.retentionMs < 0) return { logs: 0, restBearerTokens: 0 };
  const nowMs = settings.now ?? Date.now();
  const cutoff = nowMs - settings.retentionMs;
  const logs = await db.execute({
    sql: `DELETE FROM logs WHERE ts <= ?`,
    args: [cutoff],
  });
  const restBearerTokens = await db.execute({
    sql: `DELETE FROM rest_bearer_token
          WHERE refresh_expires_at <= ?
             OR (revoked_at IS NOT NULL AND revoked_at <= ?)`,
    args: [cutoff, cutoff],
  });
  return {
    logs: Number(logs.rowsAffected ?? 0),
    restBearerTokens: Number(restBearerTokens.rowsAffected ?? 0),
  };
}

/** Read recent logs (ascending by seq); `afterSeq` tails only newer lines. */
export async function getLogs(
  db: Client,
  opts: { limit?: number; afterSeq?: number; conversationId?: string } = {},
): Promise<StoredLog[]> {
  const { limit = 300, afterSeq = 0, conversationId } = opts;
  const where = conversationId
    ? "WHERE seq > ? AND conversation_id = ?"
    : "WHERE seq > ?";
  const args = conversationId ? [afterSeq, conversationId, limit] : [afterSeq, limit];
  const res = await db.execute({
    sql: `SELECT seq, ts, level, scope, conversation_id, message, data
          FROM logs ${where} ORDER BY seq ASC LIMIT ?`,
    args,
  });
  return res.rows.map((r) => ({
    seq: Number(r.seq),
    ts: Number(r.ts),
    level: String(r.level),
    scope: String(r.scope),
    conversationId: r.conversation_id == null ? undefined : String(r.conversation_id),
    message: String(r.message),
    data: r.data == null ? undefined : String(r.data),
  }));
}

function parseIntegerEnv(raw: string | undefined, fallback: number): number {
  if (raw == null || raw.trim() === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/** Upsert a conversation header (bumps `updated_at`). */
export async function upsertConversation(
  db: Client,
  args: { id: string; kind: string; title?: string; at: number },
): Promise<void> {
  await db.execute({
    sql: `INSERT INTO rendered_conversations (id, kind, title, updated_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at,
            title = COALESCE(excluded.title, rendered_conversations.title)`,
    args: [args.id, args.kind, args.title ?? null, args.at],
  });
}

/**
 * Upsert a message by `id` — INSERT or REPLACE so a streaming turn can be
 * rewritten in place (same id, content/status updated) until it settles.
 */
export async function saveMessage(db: Client, m: StoredMessage): Promise<void> {
  await upsertConversation(db, {
    id: m.conversationId,
    kind: m.conversationId.startsWith("dm:") ? "dm" : "channel",
    at: m.createdAt,
  });
  await db.execute({
    sql: `INSERT INTO rendered_messages (id, conversation_id, role, author, content, status, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(id) DO UPDATE SET content = excluded.content, status = excluded.status`,
    args: [m.id, m.conversationId, m.role, m.author, m.content, m.status, m.createdAt],
  });
}

/** The shared read-write conversation store (constructed + migrated on first use). */
export function getConversationStore(): Promise<Client> {
  return createConversationStore();
}

/** Close the shared client during hot reload or controlled shutdown. */
export async function __resetConversationStore(): Promise<void> {
  await closeGatewayStore();
}

/** PURGE a conversation from the web console's store (the `delete` op): its messages, its header
 * row, and its log lines. Complements the daemon-side purge so a deleted agent leaves no trace. */
export async function deleteConversation(db: Client, conversationId: string): Promise<void> {
  await db.batch(
    [
      { sql: `DELETE FROM rendered_messages WHERE conversation_id = ?`, args: [conversationId] },
      { sql: `DELETE FROM rendered_conversations WHERE id = ?`, args: [conversationId] },
      { sql: `DELETE FROM logs WHERE conversation_id = ?`, args: [conversationId] },
    ],
    "write",
  );
}

/** Read the most recent conversation messages, returned in chronological display order. */
export async function getMessages(
  db: Client,
  conversationId: string,
  limit = 100,
): Promise<StoredMessage[]> {
  const res = await db.execute({
    sql: `SELECT id, conversation_id, role, author, content, status, created_at
          FROM (
            SELECT id, conversation_id, role, author, content, status, created_at
            FROM rendered_messages WHERE conversation_id = ? ORDER BY created_at DESC LIMIT ?
          ) ORDER BY created_at ASC`,
    args: [conversationId, limit],
  });
  return res.rows.map((r) => ({
    id: String(r.id),
    conversationId: String(r.conversation_id),
    role: r.role as StoredMessage["role"],
    author: String(r.author),
    content: String(r.content),
    status: r.status as StoredMessage["status"],
    createdAt: Number(r.created_at),
  }));
}
