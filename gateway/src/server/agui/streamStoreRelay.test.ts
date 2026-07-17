import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";

import { createClient, type Client } from "@libsql/client";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createStreamStoreDoorbell,
  createStreamStoreRelay,
  createStoreBackedAgentSessionRelay,
  observeRawStream,
  resolveStreamStorePath,
  type StreamStoreDoorbell,
} from "@server/agui/streamStoreRelay";
import {
  createMaterializedTurnRelay,
  materializedCursorForStreamAfter,
} from "@server/agui/agentSessionProjection";
import type { DaemonPushConnector } from "@server/agui/daemonPushRelay.mjs";
import type { WsEvent } from "@shared/types";
import { removeTempPath } from "../../test/removeTempPath";

const tempDirs: string[] = [];
const fixtureClients = new Set<Client>();

async function createFixtureStore(): Promise<{ client: Client; path: string }> {
  const dir = await mkdtemp(join(tmpdir(), "nexus-stream-store-"));
  tempDirs.push(dir);
  const path = join(dir, "nexus-stream.db");
  const client = await initStreamStore(path);
  return { client, path };
}

async function initStreamStore(path: string): Promise<Client> {
  const client = createClient({ url: `file:${path}` });
  fixtureClients.add(client);
  await client.batch([
    `CREATE TABLE stream_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      data TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )`,
    "CREATE INDEX idx_stream_events_session_id ON stream_events(session_id, id)",
    `CREATE TABLE stream_raw (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      chunk BLOB NOT NULL,
      created_at INTEGER NOT NULL
    )`,
    "CREATE INDEX idx_stream_raw_session_id ON stream_raw(session_id, id)",
  ]);
  return client;
}

async function createMaterializedStore(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  fixtureClients.add(db);
  await db.batch([
    `CREATE TABLE agent_session_turns (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      status TEXT NOT NULL,
      first_stream_event_id INTEGER NOT NULL,
      last_stream_event_id INTEGER NOT NULL,
      started_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      finalized_at INTEGER
    )`,
    `CREATE TABLE agent_session_messages (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      turn_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL,
      role TEXT NOT NULL,
      author TEXT,
      content_json TEXT NOT NULL,
      status TEXT NOT NULL,
      first_stream_event_id INTEGER NOT NULL,
      last_stream_event_id INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      finalized_at INTEGER,
      UNIQUE(turn_id, ordinal)
    )`,
  ]);
  return db;
}

async function insertMaterializedTextTurn(
  client: Client,
  args: {
    turnId: string;
    sessionId: string;
    text: string;
    firstStreamEventId: number;
    lastStreamEventId: number;
    finalizedAt: number;
  },
): Promise<void> {
  await client.batch([
    {
      sql:
        "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
        "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        args.turnId,
        args.sessionId,
        "final",
        args.firstStreamEventId,
        args.lastStreamEventId,
        args.finalizedAt - 10,
        args.finalizedAt,
        args.finalizedAt,
      ],
    },
    {
      sql:
        "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
        "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        `${args.turnId}:m`,
        args.sessionId,
        args.turnId,
        0,
        "assistant",
        null,
        JSON.stringify({ schema: 1, blocks: [{ type: "text", text: args.text }] }),
        "final",
        args.firstStreamEventId,
        args.lastStreamEventId,
        args.finalizedAt - 10,
        args.finalizedAt,
        args.finalizedAt,
      ],
    },
  ]);
}

async function insertStreamEvent(
  client: Client,
  sessionId: string,
  kind: string,
  data: unknown,
): Promise<void> {
  await client.execute({
    sql:
      "INSERT INTO stream_events (session_id, kind, data, created_at) " +
      "VALUES (?, ?, ?, ?)",
    args: [sessionId, kind, JSON.stringify(data), Date.now()],
  });
}

async function insertRawChunk(
  client: Client,
  sessionId: string,
  chunk: string,
): Promise<void> {
  await client.execute({
    sql: "INSERT INTO stream_raw (session_id, chunk, created_at) VALUES (?, ?, ?)",
    args: [sessionId, Buffer.from(chunk), Date.now()],
  });
}

async function readUntilRawEvents(
  stream: ReadableStream<Uint8Array>,
  count: number,
): Promise<Array<Record<string, unknown>>> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";
  try {
    for (let i = 0; i < 30; i++) {
      const chunk = await Promise.race([
        reader.read(),
        new Promise<{ value: undefined; done: true }>((resolve) =>
          setTimeout(() => resolve({ value: undefined, done: true }), 50),
        ),
      ]);
      if (chunk.done) break;
      if (chunk.value) text += decoder.decode(chunk.value, { stream: true });
      const events = decodeRawEvents(text);
      if (events.length >= count) return events;
    }
    return decodeRawEvents(text);
  } finally {
    await reader.cancel();
  }
}

function decodeRawEvents(text: string): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = [];
  for (const block of text.split("\n\n")) {
    const dataLine = block.split("\n").find((line) => line.startsWith("data:"));
    if (!dataLine) continue;
    out.push(JSON.parse(dataLine.slice(5).trim()) as Record<string, unknown>);
  }
  return out;
}

async function waitFor(predicate: () => boolean, timeoutMs = 300): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("timed out waiting for predicate");
}

afterEach(async () => {
  for (const client of fixtureClients) client.close();
  fixtureClients.clear();
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) await removeTempPath(dir, { recursive: true });
  }
});

describe("streamStoreRelay lane 1", () => {
  it("resolves the same named stream-store path contract the daemon uses", () => {
    expect(
      resolveStreamStorePath({
        NEXUS_STREAM_DB_PATH: "file:/tmp/custom-stream.db",
      } as NodeJS.ProcessEnv),
    ).toBe("/tmp/custom-stream.db");
    expect(
      resolveStreamStorePath({
        XDG_RUNTIME_DIR: "/run/user/1000",
      } as NodeJS.ProcessEnv),
    ).toBe(join("/run/user/1000", "nexus-stream.db"));
  });

  it("tails agent.update rows for one session from a fixture file store", async () => {
    const { client, path } = await createFixtureStore();
    await insertStreamEvent(client, "s_ada", "text", { text: "old" });

    const seen: WsEvent[] = [];
    const turnEnds: number[] = [];
    const relay = createStreamStoreRelay("s_ada", {
      storePath: path,
      afterId: 1,
      pollMs: 5,
      onTurnEnd: (id) => turnEnds.push(id),
    })({
      onEvent: (ev) => seen.push(ev),
    });

    await insertStreamEvent(client, "s_ada", "thinking", { text: "working" });
    await insertStreamEvent(client, "s_other", "text", { text: "ignore me" });
    await insertStreamEvent(client, "s_ada", "text", { text: "done" });
    await insertStreamEvent(client, "s_ada", "turn_end", {});

    await waitFor(() => seen.length === 3);
    relay.close();

    expect(seen).toEqual([
      {
        type: "agent.update",
        sessionId: "s_ada",
        kind: "thinking",
        data: { text: "working", streamEventId: 2 },
      },
      {
        type: "agent.update",
        sessionId: "s_ada",
        kind: "text",
        data: { text: "done", streamEventId: 4 },
      },
      {
        type: "agent.update",
        sessionId: "s_ada",
        kind: "turn_end",
        data: { streamEventId: 5 },
      },
    ]);
    expect(turnEnds).toEqual([5]);
    await relay.ready;
  });

  it("wakes relays from one directory fs.watch doorbell filtered to the store WAL", async () => {
    const { client, path } = await createFixtureStore();
    let watchedPath = "";
    let watchListener: ((eventType: string, filename: string | Buffer | null) => void) | undefined;
    let watchClosed = false;
    const doorbell = createStreamStoreDoorbell(path, {
      debounceMs: 1,
      watch: (target, listener) => {
        watchedPath = target;
        watchListener = listener;
        return {
          close() {
            watchClosed = true;
          },
        };
      },
    });

    const seen: WsEvent[] = [];
    const relay = createStreamStoreRelay("s_ada", {
      storePath: path,
      doorbell,
      heartbeatMs: 60_000,
    })({
      onEvent: (ev) => seen.push(ev),
    });
    await relay.ready;

    await insertStreamEvent(client, "s_ada", "text", { text: "doorbell" });
    watchListener?.("change", `${basename(path)}-wal`);
    await waitFor(() => seen.length === 1);

    await insertStreamEvent(client, "s_ada", "text", { text: "ignored until heartbeat" });
    watchListener?.("change", "unrelated.db-wal");
    await new Promise((resolve) => setTimeout(resolve, 20));

    relay.close();
    doorbell.close();

    expect(watchedPath).toBe(dirname(path));
    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatchObject({
      type: "agent.update",
      sessionId: "s_ada",
      kind: "text",
      data: { text: "doorbell" },
    });
    expect(watchClosed).toBe(true);
  });

  it("uses the heartbeat fallback when a store change notification is missed", async () => {
    const { client, path } = await createFixtureStore();
    const quietDoorbell: StreamStoreDoorbell = {
      subscribe: () => () => {},
      close: () => {},
    };
    const seen: WsEvent[] = [];
    const relay = createStreamStoreRelay("s_ada", {
      storePath: path,
      doorbell: quietDoorbell,
      heartbeatMs: 5,
    })({
      onEvent: (ev) => seen.push(ev),
    });
    await relay.ready;

    await insertStreamEvent(client, "s_ada", "text", { text: "heartbeat" });
    await waitFor(() => seen.length === 1);
    relay.close();

    expect(seen[0]).toMatchObject({
      type: "agent.update",
      sessionId: "s_ada",
      kind: "text",
      data: { text: "heartbeat" },
    });
  });

  it("releases a relay-owned stream-store client between drains", async () => {
    const { path } = await createFixtureStore();
    const close = vi.fn();
    const client = {
      execute: vi.fn(async () => ({ rows: [] })),
      close,
    } as unknown as Client;
    const relay = createStreamStoreRelay("s_ada", {
      storePath: path,
      heartbeatMs: 60_000,
      clientFactory: () => client,
    })({ onEvent: () => {} });

    await relay.ready;
    expect(close).toHaveBeenCalledOnce();
    relay.close();

    expect(close).toHaveBeenCalledOnce();
  });

  it("drains a doorbell wake that arrives while lane 1 is already draining", async () => {
    let wake: (() => void) | undefined;
    const doorbell: StreamStoreDoorbell = {
      subscribe: (listener) => {
        wake = listener;
        return () => {};
      },
      close: () => {},
    };
    const rows = [
      {
        id: 1,
        session_id: "s_ada",
        kind: "text",
        data: JSON.stringify({ text: "first" }),
      },
    ];
    const client = {
      execute: async ({ args }: { args?: unknown[] }) => {
        const cursor = Number(args?.[1] ?? 0);
        return {
          rows: rows.filter((row) => row.id > cursor),
        };
      },
    } as unknown as Client;

    const seen: WsEvent[] = [];
    const relay = createStreamStoreRelay("s_ada", {
      client,
      doorbell,
      heartbeatMs: 60_000,
    })({
      onEvent: (ev) => {
        seen.push(ev);
        if (seen.length === 1) {
          rows.push({
            id: 2,
            session_id: "s_ada",
            kind: "text",
            data: JSON.stringify({ text: "second" }),
          });
          wake?.();
        }
      },
    });
    await relay.ready;
    await waitFor(() => seen.length === 2);
    relay.close();

    expect(seen.map((ev) => (ev as { data?: { text?: string } }).data?.text)).toEqual([
      "first",
      "second",
    ]);
  });

  it("holds the exact stream cursor while a slow consumer is paused", async () => {
    const { client, path } = await createFixtureStore();
    await insertStreamEvent(client, "s_ada", "text", { text: "first" });
    await insertStreamEvent(client, "s_ada", "text", { text: "second" });
    const attempts: string[] = [];
    const accepted: string[] = [];
    let block = true;
    const relay = createStreamStoreRelay("s_ada", {
      storePath: path,
      heartbeatMs: 60_000,
    })({
      onEvent: (event) => {
        const text = (event as { data?: { text?: string } }).data?.text ?? "";
        attempts.push(text);
        if (block) return false;
        accepted.push(text);
        return true;
      },
    });
    await relay.ready;
    expect(attempts).toEqual(["first"]);
    expect(accepted).toEqual([]);

    block = false;
    relay.resume?.();
    await waitFor(() => accepted.length === 2);
    relay.close();

    expect(attempts).toEqual(["first", "first", "second"]);
    expect(accepted).toEqual(["first", "second"]);
  });

  it("resumes a materialized turn at the rejected event without advancing the turn cursor", async () => {
    const client = await createMaterializedStore();
    await insertMaterializedTextTurn(client, {
      turnId: "turn_s_ada_1",
      sessionId: "s_ada",
      text: "materialized",
      firstStreamEventId: 1,
      lastStreamEventId: 2,
      finalizedAt: 100,
    });
    const accepted: string[] = [];
    let attempts = 0;
    let block = true;
    const relay = createMaterializedTurnRelay("s_ada", {
      client,
      pollMs: 60_000,
    })({
      onEvent: (event) => {
        attempts += 1;
        if (block) return false;
        accepted.push((event as { kind: string }).kind);
        return true;
      },
    });
    await relay.ready;
    await waitFor(() => attempts === 1);
    expect(accepted).toEqual([]);
    expect(relay.resume).toBeTypeOf("function");

    block = false;
    relay.resume?.();
    await waitFor(() => accepted.includes("turn_end"));
    relay.close();
    expect(accepted).toContain("text");
    expect(accepted.at(-1)).toBe("turn_end");
  });

  it("resets the stream cursor when the named store epoch changes", async () => {
    const path = join(tmpdir(), "nexus-stream.db");
    let watchListener: ((eventType: string, filename: string | Buffer | null) => void) | undefined;
    const doorbell = createStreamStoreDoorbell(path, {
      debounceMs: 1,
      watch: (_target, listener) => {
        watchListener = listener;
        return { close() {} };
      },
    });
    let epoch = "first";
    const first = {
      execute: vi.fn(async () => ({
        rows: [{
          id: 1,
          session_id: "s_ada",
          kind: "text",
          data: '{"text":"before restart"}',
        }],
      })),
      close: vi.fn(),
    } as unknown as Client;
    const second = {
      execute: vi.fn(async () => ({
        rows: [{
          id: 1,
          session_id: "s_ada",
          kind: "text",
          data: '{"text":"after restart"}',
        }],
      })),
      close: vi.fn(),
    } as unknown as Client;
    const clients = [first, second];
    const seen: WsEvent[] = [];
    const relay = createStreamStoreRelay("s_ada", {
      storePath: path,
      doorbell,
      heartbeatMs: 60_000,
      epochFactory: async () => epoch,
      clientFactory: () => clients.shift()!,
    })({
      onEvent: (ev) => seen.push(ev),
    });
    await relay.ready;
    expect(seen).toHaveLength(1);

    epoch = "second";
    watchListener?.("rename", basename(path));
    await waitFor(() => seen.length === 2);

    relay.close();
    doorbell.close();

    expect(seen.map((ev) => (ev as { data?: { text?: string } }).data?.text)).toEqual([
      "before restart",
      "after restart",
    ]);
    expect(first.execute).toHaveBeenCalledWith(
      expect.objectContaining({ args: ["s_ada", 0] }),
    );
    expect(second.execute).toHaveBeenCalledWith(
      expect.objectContaining({ args: ["s_ada", 0] }),
    );
  });

  it("keeps materialized fallback from double-emitting turns already closed by the stream lane", async () => {
    const { client: streamClient, path } = await createFixtureStore();
    const materialized = await createMaterializedStore();
    let watchListener: ((eventType: string, filename: string | Buffer | null) => void) | undefined;
    const doorbell = createStreamStoreDoorbell(path, {
      debounceMs: 1,
      watch: (_target, listener) => {
        watchListener = listener;
        return { close() {} };
      },
    });
    const seen: WsEvent[] = [];
    const relay = createStoreBackedAgentSessionRelay("s_ada", {
      storePath: path,
      doorbell,
      streamHeartbeatMs: 60_000,
      fallbackClient: materialized,
      fallbackPollMs: 5,
    })({
      onEvent: (ev) => seen.push(ev),
    });
    await relay.ready;

    await insertStreamEvent(streamClient, "s_ada", "text", { text: "live" });
    await insertStreamEvent(streamClient, "s_ada", "turn_end", {});
    watchListener?.("change", `${basename(path)}-wal`);
    await waitFor(() => seen.length === 2);

    await insertMaterializedTextTurn(materialized, {
      turnId: "turn_live",
      sessionId: "s_ada",
      text: "live",
      firstStreamEventId: 1,
      lastStreamEventId: 2,
      finalizedAt: 100,
    });
    await new Promise((resolve) => setTimeout(resolve, 30));

    relay.close();
    doorbell.close();

    expect(seen).toEqual([
      {
        type: "agent.update",
        sessionId: "s_ada",
        kind: "text",
        data: { text: "live", streamEventId: 1 },
      },
      {
        type: "agent.update",
        sessionId: "s_ada",
        kind: "turn_end",
        data: { streamEventId: 2 },
      },
    ]);
  });

  it("does not replay a finalized turn after daemon push already streamed its turn_end", async () => {
    const { path } = await createFixtureStore();
    const materialized = await createMaterializedStore();
    await insertMaterializedTextTurn(materialized, {
      turnId: "turn_pushed",
      sessionId: "s_ada",
      text: "pushed once",
      firstStreamEventId: 41,
      lastStreamEventId: 42,
      finalizedAt: 100,
    });

    let fallbackReads = 0;
    const fallbackClient = new Proxy(materialized, {
      get(target, prop, receiver) {
        if (prop !== "execute") return Reflect.get(target, prop, receiver);
        return (...args: Parameters<Client["execute"]>) => {
          fallbackReads += 1;
          return target.execute(...args);
        };
      },
    }) as Client;
    let onFrame: ((frame: unknown) => void) | undefined;
    const connector: DaemonPushConnector = () => ({
      ready: Promise.resolve(),
      subscribe(_subscription, handlers) {
        onFrame = handlers.onFrame;
        return () => {};
      },
      close() {},
    });

    const seen: WsEvent[] = [];
    const relay = createStoreBackedAgentSessionRelay("s_ada", {
      storePath: path,
      streamHeartbeatMs: 60_000,
      fallbackClient,
      fallbackPollMs: 10,
      daemonPushConnector: connector,
    })({
      onEvent: (event) => seen.push(event),
    });
    await relay.ready;

    onFrame?.({
      t: "agent.update",
      sessionId: "s_ada",
      streamEventId: 41,
      kind: "text",
      data: { text: "pushed once" },
    });
    onFrame?.({
      t: "agent.update",
      sessionId: "s_ada",
      streamEventId: 42,
      kind: "turn_end",
      data: {},
    });
    await waitFor(() => fallbackReads > 0);
    relay.close();

    expect(seen).toEqual([
      {
        type: "agent.update",
        sessionId: "s_ada",
        kind: "text",
        data: { text: "pushed once", streamEventId: 41 },
      },
      {
        type: "agent.update",
        sessionId: "s_ada",
        kind: "turn_end",
        data: { streamEventId: 42 },
      },
    ]);
  });

  it("keeps the materialized fallback dormant while the live stream lane is active", async () => {
    const { client: streamClient, path } = await createFixtureStore();
    const materialized = await createMaterializedStore();
    let fallbackReads = 0;
    const fallbackClient = new Proxy(materialized, {
      get(target, prop, receiver) {
        if (prop !== "execute") return Reflect.get(target, prop, receiver);
        return (...args: Parameters<Client["execute"]>) => {
          fallbackReads += 1;
          return target.execute(...args);
        };
      },
    }) as Client;

    await insertStreamEvent(streamClient, "s_ada", "text", { text: "live" });

    const seen: WsEvent[] = [];
    const relay = createStoreBackedAgentSessionRelay("s_ada", {
      storePath: path,
      streamHeartbeatMs: 60_000,
      fallbackClient,
      // This assertion is about lane selection after a live frame, not whether a SQLite read
      // completes inside 100 ms while the full suite shares two CPU-capped validator cores.
      // Keep the silence deadline comfortably outside scheduler noise; other tests exercise the
      // actual fallback timer with deliberately short intervals.
      fallbackPollMs: 2_000,
    })({
      onEvent: (ev) => seen.push(ev),
    });
    await relay.ready;
    await waitFor(() => seen.length === 1);
    await new Promise((resolve) => setTimeout(resolve, 30));
    relay.close();

    expect(seen[0]).toMatchObject({
      type: "agent.update",
      sessionId: "s_ada",
      kind: "text",
      data: { text: "live" },
    });
    expect(fallbackReads).toBe(0);
  });

  it("uses materialized fallback to repair a reconnect when the session has no live stream rows", async () => {
    const { path } = await createFixtureStore();
    const materialized = await createMaterializedStore();
    await insertMaterializedTextTurn(materialized, {
      turnId: "turn_old",
      sessionId: "s_iris",
      text: "already rendered",
      firstStreamEventId: 10,
      lastStreamEventId: 20,
      finalizedAt: 120,
    });
    await insertMaterializedTextTurn(materialized, {
      turnId: "turn_partial",
      sessionId: "s_iris",
      text: "repair me",
      firstStreamEventId: 21,
      lastStreamEventId: 30,
      finalizedAt: 140,
    });
    await insertMaterializedTextTurn(materialized, {
      turnId: "turn_later",
      sessionId: "s_iris",
      text: "later durable row",
      firstStreamEventId: 31,
      lastStreamEventId: 40,
      finalizedAt: 160,
    });
    const fallbackAfterCursor = await materializedCursorForStreamAfter("s_iris", 25, {
      client: materialized,
    });

    const seen: WsEvent[] = [];
    const relay = createStoreBackedAgentSessionRelay("s_iris", {
      storePath: path,
      streamAfterId: 25,
      streamHeartbeatMs: 60_000,
      fallbackClient: materialized,
      fallbackAfterCursor,
      fallbackPollMs: 5,
    })({
      onEvent: (ev) => seen.push(ev),
    });

    await waitFor(() => seen.length === 4);
    relay.close();

    expect(seen.map((ev) => (ev as { data?: { text?: string } }).data?.text).filter(Boolean))
      .toEqual(["repair me", "later durable row"]);
    expect(seen.filter((ev) => ev.type === "agent.update" && ev.kind === "turn_end"))
      .toHaveLength(2);
  });

  it("does not replay repaired materialized turns when fallback restarts after stream resumes", async () => {
    const { client: streamClient, path } = await createFixtureStore();
    const materialized = await createMaterializedStore();
    let watchListener: ((eventType: string, filename: string | Buffer | null) => void) | undefined;
    const doorbell = createStreamStoreDoorbell(path, {
      debounceMs: 1,
      watch: (_target, listener) => {
        watchListener = listener;
        return { close() {} };
      },
    });
    await insertMaterializedTextTurn(materialized, {
      turnId: "turn_repaired",
      sessionId: "s_iris",
      text: "repair once",
      firstStreamEventId: 10,
      lastStreamEventId: 20,
      finalizedAt: 120,
    });

    const seen: WsEvent[] = [];
    const relay = createStoreBackedAgentSessionRelay("s_iris", {
      storePath: path,
      doorbell,
      streamHeartbeatMs: 60_000,
      fallbackClient: materialized,
      fallbackPollMs: 5,
    })({
      onEvent: (ev) => seen.push(ev),
    });
    await waitFor(() => seen.length === 2);

    await insertStreamEvent(streamClient, "s_iris", "text", { text: "live resumes" });
    watchListener?.("change", `${basename(path)}-wal`);
    await waitFor(() => seen.length === 3);
    await new Promise((resolve) => setTimeout(resolve, 20));
    relay.close();
    doorbell.close();

    expect(seen.map((ev) => (ev as { data?: { text?: string } }).data?.text).filter(Boolean))
      .toEqual(["repair once", "live resumes"]);
  });

  it("streams raw-lane chunks in order as SSE records", async () => {
    const { client, path } = await createFixtureStore();
    let watchListener: ((eventType: string, filename: string | Buffer | null) => void) | undefined;
    const doorbell = createStreamStoreDoorbell(path, {
      debounceMs: 1,
      watch: (_target, listener) => {
        watchListener = listener;
        return { close() {} };
      },
    });

    const stream = observeRawStream("s_ada", {
      storePath: path,
      doorbell,
      heartbeatMs: 60_000,
    });

    await insertRawChunk(client, "s_ada", "first");
    await insertRawChunk(client, "s_other", "ignore");
    await insertRawChunk(client, "s_ada", "second");
    watchListener?.("change", `${basename(path)}-wal`);

    const events = await readUntilRawEvents(stream, 2);
    doorbell.close();

    expect(events).toEqual([
      {
        id: 1,
        sessionId: "s_ada",
        chunkBase64: Buffer.from("first").toString("base64"),
        encoding: "base64",
      },
      {
        id: 3,
        sessionId: "s_ada",
        chunkBase64: Buffer.from("second").toString("base64"),
        encoding: "base64",
      },
    ]);
  });

  it("closes a raw-lane store client when its reader disconnects", async () => {
    const { path } = await createFixtureStore();
    const close = vi.fn();
    const execute = vi.fn(async () => ({ rows: [] }));
    const stream = observeRawStream("s_ada", {
      storePath: path,
      heartbeatMs: 60_000,
      clientFactory: () => ({ execute, close }) as unknown as Client,
    });
    const reader = stream.getReader();

    await waitFor(() => execute.mock.calls.length > 0);
    await reader.cancel();

    expect(close).toHaveBeenCalledOnce();
  });
});
