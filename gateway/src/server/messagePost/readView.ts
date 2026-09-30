// Read-only Message Post projection.
//
// This is the gateway-owned source for Lane A AGUI observe. It tails committed
// `messages` rows from the daemon-owned libSQL read view. It never writes; the
// daemon/store remains the source of truth.
import { and, asc, eq, gt, or, sql } from "drizzle-orm";
import type { Client, Row } from "@libsql/client";

import type { ReadDb } from "@drizzle/client";
import { messages } from "@drizzle/schema.gen";
import { getReadDb } from "@drizzle/client";
import { getGatewayStore } from "@server/store/client";
import { gatewayChangeBus, type GatewayChangeBus } from "@server/store/changeBus";
import { Kind, Scope, type Message, type SendTarget } from "@shared/types";

export interface MessagePostTailOptions {
  after: number;
  afterRowid?: number;
  project?: string;
  selfName?: string;
  limit?: number;
}

export interface MessagePostCursor {
  createdAt: number;
  rowid: number;
}

export type MessagePostMessage = Message & {
  cursor?: MessagePostCursor;
};

export interface MessagePostSource {
  ready: Promise<void>;
  /** Stop advancing this subscriber's cursor while its downstream is full. */
  pause?(): void;
  /** Resume this subscriber from its last accepted exact cursor. */
  resume?(): void;
  close(): void;
}

export interface MessagePostSourceOptions {
  target: SendTarget;
  project?: string;
  selfName?: string;
  /** Start after this committed-message cursor. Defaults to "now". */
  after?: number;
  afterRowid?: number;
  pollIntervalMs?: number;
  heartbeatIntervalMs?: number;
  /** Return false to retain the cursor and pause this subscriber. */
  onMessage: (message: MessagePostMessage) => boolean | void;
  onHeartbeat?: () => boolean | void;
  onError?: (error: unknown) => void;
}

export type MessagePostSourceFactory = (
  opts: MessagePostSourceOptions,
) => MessagePostSource;

function scopeFromKind(kind: string | null): Scope {
  switch (kind) {
    case "thread":
      return Scope.Thread;
    case "topic":
      return Scope.Topic;
    default:
      return Scope.Dm;
  }
}

function fallbackProvenance(row: {
  fromName: string | null;
  kind: string | null;
  toName: string | null;
  topic: string | null;
}) {
  const scope = scopeFromKind(row.kind);
  return {
    from: row.fromName ?? "",
    kind: Kind.Agent,
    thread: scope === Scope.Thread ? row.toName ?? undefined : undefined,
    topic: scope === Scope.Topic ? row.topic ?? row.toName ?? undefined : undefined,
  };
}

function parseProvenance(
  raw: string | null,
  row: {
    fromName: string | null;
    kind: string | null;
    toName: string | null;
    topic: string | null;
  },
): Message["provenance"] {
  if (raw) {
    try {
      return JSON.parse(raw) as Message["provenance"];
    } catch {
      // Fall through to a display-safe provenance shape below.
    }
  }
  return fallbackProvenance(row);
}

function rowToMessage(row: {
  rowid?: number | null;
  messageId: string | null;
  fromName: string | null;
  kind: string | null;
  toName: string | null;
  threadId: string | null;
  topic: string | null;
  summary: string | null;
  body: string | null;
  provenance: string | null;
  project: string | null;
  createdAt: number | null;
}): MessagePostMessage {
  const scope = scopeFromKind(row.kind);
  const createdAt = row.createdAt ?? 0;
  const rowid = row.rowid ?? 0;
  return {
    id: row.messageId ?? "",
    project: row.project ?? "",
    from: row.fromName ?? "",
    scope,
    thread: row.threadId ?? undefined,
    topic: row.topic ?? undefined,
    body: row.body ?? "",
    summary: row.summary ?? undefined,
    provenance: parseProvenance(row.provenance, row),
    createdAt,
    ...(rowid > 0 ? { cursor: { createdAt, rowid } } : {}),
  };
}

function projectClause(project: string | undefined) {
  return project ? eq(messages.project, project) : undefined;
}

function targetClause(target: SendTarget, selfName?: string) {
  switch (target.verb) {
    case "post":
      return and(eq(messages.kind, "thread"), eq(messages.toName, target.thread));
    case "publish":
      return and(eq(messages.kind, "topic"), eq(messages.toName, target.topic));
    case "dm": {
      const participant = target.name;
      const participantId = target.agentId;
      if (participantId) {
        const scopedPair = selfName
          ? or(
              and(eq(messages.fromName, selfName), eq(messages.toAgentId, participantId)),
              and(eq(messages.fromAgentId, participantId), eq(messages.toName, selfName)),
            )
          : or(
              eq(messages.fromAgentId, participantId),
              eq(messages.toAgentId, participantId),
            );
        return and(eq(messages.kind, "dm"), scopedPair);
      }
      if (!participant) {
        throw new Error("dm target requires name or agentId");
      }
      const scopedPair = selfName
        ? or(
            and(eq(messages.fromName, selfName), eq(messages.toName, participant)),
            and(eq(messages.fromName, participant), eq(messages.toName, selfName)),
          )
        : or(eq(messages.fromName, participant), eq(messages.toName, participant));
      return and(eq(messages.kind, "dm"), scopedPair);
    }
    case "reply":
      throw new Error("cannot tail context-aware reply without a concrete target");
  }
}

const messageRowid = sql<number>`rowid`;

function afterCursorClause(opts: MessagePostTailOptions) {
  const afterCreatedAt = gt(messages.createdAt, opts.after);
  if (!opts.afterRowid) return afterCreatedAt;
  return or(
    afterCreatedAt,
    and(
      eq(messages.createdAt, opts.after),
      gt(messageRowid, opts.afterRowid),
    ),
  );
}

/**
 * Fetch a committed message by id from the read view. `project`, when supplied,
 * enforces the caller's project scope at the gateway read boundary.
 */
export async function messageById(
  db: ReadDb,
  id: string,
  project?: string,
): Promise<Message | null> {
  const rows = await db
    .select({
      messageId: messages.messageId,
      fromName: messages.fromName,
      kind: messages.kind,
      toName: messages.toName,
      threadId: messages.threadId,
      topic: messages.topic,
      summary: messages.summary,
      body: messages.body,
      provenance: messages.provenance,
      project: messages.project,
      createdAt: messages.createdAt,
    })
    .from(messages)
    .where(
      and(
        eq(messages.messageId, id),
        projectClause(project),
      ),
    )
    .limit(1);
  return rows[0] ? rowToMessage(rows[0]) : null;
}

/** Fetch committed messages for a bus target after a `(created_at, rowid)` cursor. */
export async function messagesForTargetAfter(
  db: ReadDb,
  target: SendTarget,
  opts: MessagePostTailOptions,
): Promise<MessagePostMessage[]> {
  const rows = await db
    .select({
      rowid: messageRowid,
      messageId: messages.messageId,
      fromName: messages.fromName,
      kind: messages.kind,
      toName: messages.toName,
      threadId: messages.threadId,
      topic: messages.topic,
      summary: messages.summary,
      body: messages.body,
      provenance: messages.provenance,
      project: messages.project,
      createdAt: messages.createdAt,
    })
    .from(messages)
    .where(
      and(
        targetClause(target, opts.selfName),
        afterCursorClause(opts),
      ),
    )
    .orderBy(asc(messages.createdAt), asc(messageRowid))
    .limit(opts.limit ?? 100);
  return rows.map(rowToMessage);
}

/** Gateway-owned equivalent used by production stream readers after canonical cutover. */
export async function canonicalMessagesForTargetAfter(
  db: Client,
  target: SendTarget,
  opts: MessagePostTailOptions,
): Promise<MessagePostMessage[]> {
  const clause = canonicalSqlTarget(target, opts.selfName);
  const limit = Math.max(1, Math.min(200, Math.trunc(opts.limit ?? 100)));
  const result = await db.execute({
    sql: `SELECT rowid, * FROM bus_messages
          WHERE ${clause.sql}
            AND (created_at > ? OR (created_at = ? AND rowid > ?))
          ORDER BY created_at ASC, rowid ASC LIMIT ?`,
    args: [...clause.args, opts.after, opts.after, opts.afterRowid ?? 0, limit],
  });
  return result.rows.map(canonicalRowToMessage);
}

function canonicalSqlTarget(target: SendTarget, selfName?: string): { sql: string; args: string[] } {
  switch (target.verb) {
    case "post":
      return { sql: "kind = 'thread' AND (thread_id = ? OR to_name = ?)", args: [target.thread, target.thread] };
    case "publish":
      return { sql: "kind = 'topic' AND topic = ?", args: [target.topic] };
    case "dm": {
      const participant = target.name;
      const participantId = target.agentId;
      if (participantId) {
        return selfName
          ? {
              sql: `kind = 'dm' AND ((from_name = ? AND to_agent_id = ?)
                    OR (from_agent_id = ? AND to_name = ?))`,
              args: [selfName, participantId, participantId, selfName],
            }
          : {
              sql: "kind = 'dm' AND (from_agent_id = ? OR to_agent_id = ?)",
              args: [participantId, participantId],
            };
      }
      if (!participant) throw new Error("dm target requires name or agentId");
      return selfName
        ? {
            sql: `kind = 'dm' AND ((from_name = ? AND to_name = ?)
                  OR (from_name = ? AND to_name = ?))`,
            args: [selfName, participant, participant, selfName],
          }
        : {
            sql: "kind = 'dm' AND (from_name = ? OR to_name = ?)",
            args: [participant, participant],
          };
    }
    case "reply":
      throw new Error("cannot tail context-aware reply without a concrete target");
  }
}

function canonicalRowToMessage(row: Row): MessagePostMessage {
  const kind = String(row.kind);
  const createdAt = Number(row.created_at);
  const rawProvenance = parseCanonicalProvenance(row.provenance_json);
  const from = row.from_name === null ? "" : String(row.from_name);
  const provenance: Message["provenance"] = {
    ...rawProvenance,
    from: typeof rawProvenance.from === "string" ? rawProvenance.from : from,
    kind: typeof rawProvenance.kind === "string"
      ? rawProvenance.kind as Message["provenance"]["kind"]
      : Kind.Agent,
  };
  return {
    id: String(row.message_id),
    project: typeof rawProvenance.project === "string" ? rawProvenance.project : "",
    from,
    scope: scopeFromKind(kind),
    thread: row.thread_id === null ? undefined : String(row.thread_id),
    topic: row.topic === null ? undefined : String(row.topic),
    body: String(row.body),
    summary: row.summary === null ? undefined : String(row.summary),
    provenance,
    createdAt,
    cursor: { createdAt, rowid: Number(row.rowid) },
  };
}

function parseCanonicalProvenance(value: unknown): Record<string, unknown> {
  try {
    return JSON.parse(String(value)) as Record<string, unknown>;
  } catch {
    return {};
  }
}

const DEFAULT_HEARTBEAT_MS = 20_000;

type FetchMessages = (
  target: SendTarget,
  opts: MessagePostTailOptions,
) => Promise<MessagePostMessage[]>;

export interface ReadViewMessagePostRegistryDeps {
  fetchMessages?: FetchMessages;
  changeBus?: GatewayChangeBus;
  now?: () => number;
}

export interface ReadViewMessagePostRegistry {
  createSource(opts: MessagePostSourceOptions): MessagePostSource;
  activePollerCount(): number;
  close(): void;
}

interface Subscriber {
  id: number;
  cursor: MessagePostCursor;
  heartbeatIntervalMs: number;
  lastHeartbeatAt: number;
  paused: boolean;
  closed: boolean;
  readyResolved: boolean;
  resolveReady: () => void;
  onMessage: MessagePostSourceOptions["onMessage"];
  onHeartbeat?: MessagePostSourceOptions["onHeartbeat"];
  onError?: MessagePostSourceOptions["onError"];
}

function canonicalTargetKey(target: SendTarget, selfName?: string): string {
  switch (target.verb) {
    case "post":
      return `thread-name:${target.thread}`;
    case "publish":
      return `topic:${target.topic}`;
    case "dm":
      return target.agentId ? `dm:${target.agentId}` : `dm-name:${target.name ?? selfName ?? ""}`;
    case "reply":
      throw new Error("cannot tail context-aware reply without a concrete target");
  }
}

function cursorBefore(left: MessagePostCursor, right: MessagePostCursor): boolean {
  return left.createdAt < right.createdAt ||
    (left.createdAt === right.createdAt && left.rowid < right.rowid);
}

function messageCursor(message: MessagePostMessage, previous: MessagePostCursor): MessagePostCursor {
  return message.cursor ?? {
    createdAt: Math.max(previous.createdAt, message.createdAt),
    rowid: 0,
  };
}

/**
 * One process-level poll loop per canonical Message Post target. Project is
 * intentionally absent from both the registry key and the SQL selection:
 * it is display metadata, never a transport boundary.
 */
export function createReadViewMessagePostRegistry(
  deps: ReadViewMessagePostRegistryDeps = {},
): ReadViewMessagePostRegistry {
  const now = deps.now ?? Date.now;
  const changeBus = deps.changeBus ?? gatewayChangeBus;
  const fetchMessages: FetchMessages = deps.fetchMessages ?? (async (target, opts) =>
    canonicalMessagesForTargetAfter(await getGatewayStore(), target, opts));
  const pollers = new Map<string, ReturnType<typeof makePoller>>();
  let subscriberSeq = 0;

  function makePoller(
    key: string,
    target: SendTarget,
    selfName: string | undefined,
  ) {
    const subscribers = new Map<number, Subscriber>();
    let heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
    let ticking = false;
    let scheduled = false;
    let rerun = false;
    let stopped = false;

    const scheduleHeartbeat = () => {
      if (stopped || subscribers.size === 0 || heartbeatTimer) return;
      const at = now();
      const delay = Math.max(1, Math.min(...[...subscribers.values()].map(
        (subscriber) => subscriber.lastHeartbeatAt + subscriber.heartbeatIntervalMs - at,
      )));
      heartbeatTimer = setTimeout(() => {
        heartbeatTimer = undefined;
        const current = now();
        for (const subscriber of subscribers.values()) {
          if (
            subscriber.closed || subscriber.paused || !subscriber.onHeartbeat ||
            current - subscriber.lastHeartbeatAt < subscriber.heartbeatIntervalMs
          ) continue;
          try {
            const accepted = subscriber.onHeartbeat();
            if (accepted === false) subscriber.paused = true;
            else subscriber.lastHeartbeatAt = current;
          } catch (error) {
            subscriber.paused = true;
            subscriber.onError?.(error);
          }
        }
        scheduleHeartbeat();
      }, delay);
    };

    const requestTick = () => {
      if (stopped || subscribers.size === 0) return;
      if (ticking) {
        rerun = true;
        return;
      }
      if (scheduled) return;
      scheduled = true;
      queueMicrotask(() => {
        scheduled = false;
        void tick();
      });
    };

    const tick = async (): Promise<void> => {
      if (stopped || ticking || subscribers.size === 0) return;
      ticking = true;
      const included = [...subscribers.values()].filter((s) => !s.closed);
      const readable = included.filter((s) => !s.paused);
      try {
        if (readable.length > 0) {
          const cursor = readable.reduce(
            (oldest, subscriber) => cursorBefore(subscriber.cursor, oldest)
              ? subscriber.cursor
              : oldest,
            readable[0]!.cursor,
          );
          const rows = await fetchMessages(target, {
            after: cursor.createdAt,
            afterRowid: cursor.rowid,
            selfName,
            project: undefined,
          });
          for (const message of rows) {
            const next = messageCursor(message, cursor);
            for (const subscriber of included) {
              if (subscriber.closed || subscriber.paused || !cursorBefore(subscriber.cursor, next)) {
                continue;
              }
              try {
                const accepted = subscriber.onMessage(message);
                if (accepted === false) {
                  subscriber.paused = true;
                  continue;
                }
                subscriber.cursor = next;
              } catch (error) {
                subscriber.paused = true;
                subscriber.onError?.(error);
              }
            }
          }
        }
      } catch (error) {
        for (const subscriber of included) subscriber.onError?.(error);
      } finally {
        for (const subscriber of included) {
          if (!subscriber.readyResolved) {
            subscriber.readyResolved = true;
            subscriber.resolveReady();
          }
        }
        ticking = false;
        if (stopped || subscribers.size === 0) return;
        if (rerun) {
          rerun = false;
          requestTick();
        }
      }
    };

    const unsubscribe = changeBus.subscribe(key, requestTick);

    return {
      add(opts: MessagePostSourceOptions): MessagePostSource {
        const id = ++subscriberSeq;
        let resolveReady = () => {};
        const ready = new Promise<void>((resolve) => {
          resolveReady = resolve;
        });
        const subscriber: Subscriber = {
          id,
          cursor: {
            createdAt: opts.after ?? now() - 1,
            rowid: opts.afterRowid ?? 0,
          },
          heartbeatIntervalMs: Math.max(1, opts.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS),
          lastHeartbeatAt: now(),
          paused: false,
          closed: false,
          readyResolved: false,
          resolveReady,
          onMessage: opts.onMessage,
          onHeartbeat: opts.onHeartbeat,
          onError: opts.onError,
        };
        subscribers.set(id, subscriber);
        requestTick();
        scheduleHeartbeat();
        return {
          ready,
          pause() {
            subscriber.paused = true;
          },
          resume() {
            if (subscriber.closed || !subscriber.paused) return;
            subscriber.paused = false;
            requestTick();
          },
          close() {
            if (subscriber.closed) return;
            subscriber.closed = true;
            subscribers.delete(id);
            if (!subscriber.readyResolved) subscriber.resolveReady();
            if (subscribers.size === 0) {
              stopped = true;
              if (heartbeatTimer) clearTimeout(heartbeatTimer);
              unsubscribe();
              pollers.delete(key);
            }
          },
        };
      },
      stop() {
        stopped = true;
        if (heartbeatTimer) clearTimeout(heartbeatTimer);
        unsubscribe();
        for (const subscriber of subscribers.values()) {
          subscriber.closed = true;
          if (!subscriber.readyResolved) subscriber.resolveReady();
        }
        subscribers.clear();
      },
    };
  }

  return {
    createSource(opts) {
      const key = canonicalTargetKey(opts.target, opts.selfName);
      let poller = pollers.get(key);
      if (!poller) {
        poller = makePoller(key, opts.target, opts.selfName);
        pollers.set(key, poller);
      }
      return poller.add(opts);
    },
    activePollerCount() {
      return pollers.size;
    },
    close() {
      for (const poller of pollers.values()) poller.stop();
      pollers.clear();
    },
  };
}

const sharedReadViewRegistry = createReadViewMessagePostRegistry();

/** Subscribe to the process-wide committed Message Post poller registry. */
export function createReadViewMessagePostSource(
  opts: MessagePostSourceOptions,
): MessagePostSource {
  return sharedReadViewRegistry.createSource(opts);
}
