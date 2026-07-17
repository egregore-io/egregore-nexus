import { describe, expect, it, vi } from "vitest";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  createDaemonPushAgentSessionRelay,
  createDaemonPushDeveloperEventSource,
  resetSharedDaemonPushConnectorForTests,
  sharedDaemonPushConnector,
  type DaemonPushConnector,
} from "@server/agui/daemonPushRelay.mjs";
import {
  createStoreBackedAgentSessionRelay,
  type StreamStoreDoorbell,
} from "@server/agui/streamStoreRelay";
import { createClient } from "@libsql/client";
import { removeTempPath } from "../../test/removeTempPath";

async function waitFor(predicate: () => boolean, timeoutMs = 300): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("timed out waiting for predicate");
}

function manualDoorbell(): StreamStoreDoorbell & { ring(): void } {
  const listeners = new Set<() => void>();
  return {
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    close() {
      listeners.clear();
    },
    ring() {
      for (const listener of [...listeners]) listener();
    },
  };
}

function writeFramedJson(socket: Socket, value: unknown): void {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length, 0);
  socket.write(Buffer.concat([header, payload]));
}

async function startPushFixture(
  path: string,
  onFrame: (frame: Record<string, unknown>, socket: Socket) => void,
): Promise<{ server: Server; sockets: Set<Socket>; close(): Promise<void> }> {
  const sockets = new Set<Socket>();
  const server = createServer((socket) => {
    sockets.add(socket);
    let buffer = Buffer.alloc(0);
    socket.on("close", () => sockets.delete(socket));
    socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      while (buffer.length >= 4) {
        const length = buffer.readUInt32BE(0);
        if (buffer.length < length + 4) return;
        const payload = buffer.subarray(4, length + 4);
        buffer = buffer.subarray(length + 4);
        onFrame(JSON.parse(payload.toString("utf8")), socket);
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(path, resolve);
  });
  return {
    server,
    sockets,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      if (process.platform !== "win32") await rm(path, { force: true });
    },
  };
}

function fixtureSocketPath(dir: string, label: string): string {
  if (process.platform === "win32") {
    return `\\\\.\\pipe\\nexus-gateway-${process.pid}-${label}`;
  }
  return join(dir, `${label}.sock`);
}

async function initStreamStore(path: string) {
  const client = createClient({ url: `file:${path}` });
  await client.batch([
    `CREATE TABLE stream_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      data TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )`,
    "CREATE INDEX idx_stream_events_session_id ON stream_events(session_id, id)",
  ]);
  return client;
}

describe("daemon push relay", () => {
  it("emits daemon-pushed agent updates as normal WsEvents", async () => {
    let onFrame: ((frame: unknown) => void) | undefined;
    const connector: DaemonPushConnector = () => {
      return {
        ready: Promise.resolve(),
        subscribe(_subscription, handlers) {
          onFrame = handlers.onFrame;
          return () => {};
        },
        close() {},
      };
    };
    const events: unknown[] = [];

    const relay = createDaemonPushAgentSessionRelay("s_ada", {
      connector,
      afterId: 41,
    })({
      onEvent(event) {
        events.push(event);
      },
    });
    await relay.ready;
    onFrame?.({
      t: "agent.update",
      sessionId: "s_ada",
      streamEventId: 42,
      kind: "text",
      data: { text: "hello", streamEventId: 42 },
    });

    expect(events).toEqual([
      {
        type: "agent.update",
        sessionId: "s_ada",
        kind: "text",
        data: { text: "hello", streamEventId: 42 },
      },
    ]);
    relay.close();
  });

  it("emits daemon-pushed developer events for the matching topic", async () => {
    let onFrame: ((frame: unknown) => void) | undefined;
    let subscribed: unknown;
    const connector: DaemonPushConnector = () => {
      return {
        ready: Promise.resolve(),
        subscribe(subscription, handlers) {
          subscribed = subscription;
          onFrame = handlers.onFrame;
          return () => {};
        },
        close() {},
      };
    };
    const events: unknown[] = [];

    const source = createDaemonPushDeveloperEventSource("s_ada", { connector });
    source?.subscribe("sys.agent.ada.tool_call", 7, {
      onEvent(event) {
        events.push(event);
      },
    });
    onFrame?.({
      t: "developer.event",
      sessionId: "s_ada",
      event: {
        kind: "tool_call",
        topic: "sys.agent.ada.tool_call",
        seq: 8,
        ts: 1783300000000,
        agent: "ada",
        sessionId: "s_ada",
        tool: "Read",
        phase: "pre",
        ok: true,
      },
    });
    onFrame?.({
      t: "developer.event",
      sessionId: "s_ada",
      event: {
        kind: "tool_call",
        topic: "sys.agent.other.tool_call",
        seq: 9,
        ts: 1783300000001,
        agent: "other",
        sessionId: "s_ada",
        tool: "Read",
        phase: "pre",
        ok: true,
      },
    });

    expect(subscribed).toEqual({
      lane: "developer_event",
      sessionId: "s_ada",
      afterId: 7,
    });
    expect(events).toEqual([
      {
        kind: "tool_call",
        topic: "sys.agent.ada.tool_call",
        seq: 8,
        ts: 1783300000000,
        agent: "ada",
        sessionId: "s_ada",
        tool: "Read",
        phase: "pre",
        ok: true,
      },
    ]);
  });

  it("rereads the boot manifest and resubscribes after the daemon socket closes", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nexus-daemon-reconnect-"));
    const socketA = fixtureSocketPath(dir, "boot-a");
    const socketB = fixtureSocketPath(dir, "boot-b");
    const manifest = join(dir, "gateway-stream-endpoint.json");
    const previousHome = process.env.NEXUS_HOME;
    const hellos: Array<Record<string, unknown>> = [];
    const events: Array<Record<string, unknown>> = [];
    const secondSubscriberEvents: Array<Record<string, unknown>> = [];
    let upstreamSubscribeCount = 0;
    let first: Awaited<ReturnType<typeof startPushFixture>> | undefined;
    let second: Awaited<ReturnType<typeof startPushFixture>> | undefined;

    try {
      process.env.NEXUS_HOME = dir;
      first = await startPushFixture(socketA, (frame, socket) => {
        if (frame.t === "subscribe") upstreamSubscribeCount += 1;
        if (frame.t !== "hello") return;
        hellos.push(frame);
        writeFramedJson(socket, { t: "ready", version: 1, daemonBootId: "boot-a", resume: "store" });
        writeFramedJson(socket, {
          t: "developer.event",
          sessionId: "fleet",
          event: {
            kind: "agent_lifecycle",
            topic: "sys.fleet.status",
            seq: 1,
            ts: 1,
            sessionId: "fleet",
            lifecycle: "resync",
            data: { reason: "subscribe", source: "members", boot: "a" },
          },
        });
        // Leave an incomplete old-socket header behind. Reconnect must discard it before parsing
        // the replacement endpoint's first frame.
        socket.write(Buffer.from([0, 0]));
      });
      await writeFile(
        manifest,
        JSON.stringify({ path: socketA, token: "token-a", daemonBootId: "boot-a" }),
      );
      resetSharedDaemonPushConnectorForTests();
      const source = createDaemonPushDeveloperEventSource("fleet");
      expect(source).toBeDefined();
      source?.subscribe("sys.fleet.status", 0, {
        onEvent(event) {
          events.push(event as unknown as Record<string, unknown>);
        },
      });
      await waitFor(() => events.some((event) => event.lifecycle === "resync"), 1_000);
      const firstSubscriberCount = events.length;
      source?.subscribe("sys.fleet.status", 999, {
        onEvent(event) {
          secondSubscriberEvents.push(event as unknown as Record<string, unknown>);
        },
      });
      await waitFor(() => secondSubscriberEvents.length === 1, 1_000);
      expect(secondSubscriberEvents[0]).toMatchObject({ lifecycle: "resync", seq: 1 });
      expect(events).toHaveLength(firstSubscriberCount);
      expect(upstreamSubscribeCount).toBe(0);

      second = await startPushFixture(socketB, (frame, socket) => {
        if (frame.t !== "hello") return;
        hellos.push(frame);
        writeFramedJson(socket, { t: "ready", version: 1, daemonBootId: "boot-b", resume: "store" });
        writeFramedJson(socket, {
          t: "developer.event",
          sessionId: "fleet",
          event: {
            kind: "agent_lifecycle",
            topic: "sys.fleet.status",
            seq: 1,
            ts: 2,
            sessionId: "fleet",
            lifecycle: "resync",
            data: { reason: "subscribe", source: "members", boot: "b" },
          },
        });
        writeFramedJson(socket, {
          t: "developer.event",
          sessionId: "fleet",
          event: {
            kind: "agent_lifecycle",
            topic: "sys.fleet.status",
            seq: 2,
            ts: 3,
            sessionId: "s_percy",
            lifecycle: "status",
            data: { presence: "online", paused: false },
          },
        });
      });
      await writeFile(
        manifest,
        JSON.stringify({ path: socketB, token: "token-b", daemonBootId: "boot-b" }),
      );
      await first.close();
      first = undefined;

      await waitFor(
        () => events.some((event) => (event.data as { boot?: string } | undefined)?.boot === "b"),
        2_000,
      );
      expect(hellos).toHaveLength(2);
      expect(hellos[1]).toMatchObject({
        t: "hello",
        token: "token-b",
        subscriptions: [{ lane: "developer_event", sessionId: "fleet", afterId: 1 }],
      });
      expect(events.at(-1)).toMatchObject({
        lifecycle: "status",
        sessionId: "s_percy",
        data: { presence: "online", paused: false },
      });
    } finally {
      resetSharedDaemonPushConnectorForTests();
      await first?.close();
      await second?.close();
      if (previousHome === undefined) delete process.env.NEXUS_HOME;
      else process.env.NEXUS_HOME = previousHome;
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("resumes agent delivery from the latest acknowledged cursor after an endpoint reconnect", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nexus-agent-reconnect-"));
    const socketA = fixtureSocketPath(dir, "endpoint-a");
    const socketB = fixtureSocketPath(dir, "endpoint-b");
    const manifest = join(dir, "gateway-stream-endpoint.json");
    const previousHome = process.env.NEXUS_HOME;
    const hellos: Array<Record<string, unknown>> = [];
    const events: Array<Record<string, unknown>> = [];
    let first: Awaited<ReturnType<typeof startPushFixture>> | undefined;
    let second: Awaited<ReturnType<typeof startPushFixture>> | undefined;
    let relay: ReturnType<ReturnType<typeof createDaemonPushAgentSessionRelay>> | undefined;

    try {
      process.env.NEXUS_HOME = dir;
      first = await startPushFixture(socketA, (frame, socket) => {
        if (frame.t !== "hello") return;
        hellos.push(frame);
        writeFramedJson(socket, { t: "ready", version: 1, daemonBootId: "boot-same", resume: "store" });
        writeFramedJson(socket, {
          t: "agent.update",
          sessionId: "s_ada",
          streamEventId: 42,
          kind: "text",
          data: { text: "first" },
        });
        socket.write(Buffer.from([0, 0]));
      });
      await writeFile(
        manifest,
        JSON.stringify({ path: socketA, token: "token-a", daemonBootId: "boot-same" }),
      );
      resetSharedDaemonPushConnectorForTests();
      relay = createDaemonPushAgentSessionRelay("s_ada", {
        connector: sharedDaemonPushConnector,
      })({
        onEvent(event) {
          events.push(event as unknown as Record<string, unknown>);
        },
      });
      await relay.ready;
      await waitFor(
        () => events.some((event) => (event.data as { streamEventId?: number }).streamEventId === 42),
        1_000,
      );

      second = await startPushFixture(socketB, (frame, socket) => {
        if (frame.t !== "hello") return;
        hellos.push(frame);
        writeFramedJson(socket, { t: "ready", version: 1, daemonBootId: "boot-same", resume: "store" });
        writeFramedJson(socket, {
          t: "agent.update",
          sessionId: "s_ada",
          streamEventId: 42,
          kind: "text",
          data: { text: "replayed overlap" },
        });
        writeFramedJson(socket, {
          t: "agent.update",
          sessionId: "s_ada",
          streamEventId: 43,
          kind: "text",
          data: { text: "second" },
        });
      });
      await writeFile(
        manifest,
        JSON.stringify({ path: socketB, token: "token-b", daemonBootId: "boot-same" }),
      );
      await first.close();
      first = undefined;

      await waitFor(
        () => events.some((event) => (event.data as { streamEventId?: number }).streamEventId === 43),
        2_000,
      );
      expect(hellos).toHaveLength(2);
      expect(hellos[1]).toMatchObject({
        t: "hello",
        token: "token-b",
        subscriptions: [{ lane: "agent", sessionId: "s_ada", afterId: 42 }],
      });
      expect(events.at(-1)).toMatchObject({
        type: "agent.update",
        sessionId: "s_ada",
        kind: "text",
        data: { text: "second", streamEventId: 43 },
      });
      expect(
        events.map((event) => (event.data as { streamEventId?: number }).streamEventId),
      ).toEqual([42, 43]);
    } finally {
      relay?.close();
      resetSharedDaemonPushConnectorForTests();
      await first?.close();
      await second?.close();
      if (previousHome === undefined) delete process.env.NEXUS_HOME;
      else process.env.NEXUS_HOME = previousHome;
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("keeps the store fallback path live after a daemon push gap", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nexus-daemon-push-"));
    const path = join(dir, "nexus-stream.db");
    const db = await initStreamStore(path);
    const doorbell = manualDoorbell();
    let onFrame: ((frame: unknown) => void) | undefined;
    const connector: DaemonPushConnector = () => ({
      ready: Promise.resolve(),
      subscribe(_subscription, handlers) {
        onFrame = handlers.onFrame;
        return () => {};
      },
      close() {},
    });
    const events: Array<Record<string, unknown>> = [];
    const errors: unknown[] = [];

    const relay = createStoreBackedAgentSessionRelay("s_ada", {
      storePath: path,
      doorbell,
      streamHeartbeatMs: 10_000,
      fallbackPollMs: 10_000,
      daemonPushConnector: connector,
    })({
      onEvent(event) {
        events.push(event as unknown as Record<string, unknown>);
      },
      onError(error) {
        errors.push(error);
      },
    });
    await relay.ready;

    await db.execute({
      sql:
        "INSERT INTO stream_events (session_id, kind, data, created_at) " +
        "VALUES (?, ?, ?, ?)",
      args: ["s_ada", "text", JSON.stringify({ text: "from store" }), Date.now()],
    });

    onFrame?.({
      t: "gap",
      lane: "agent",
      sessionId: "s_ada",
      afterId: 0,
      retry: "store",
    });
    doorbell.ring();

    await waitFor(() => events.some((event) => event.kind === "text"));
    expect(errors).toEqual([]);
    expect(events).toContainEqual({
      type: "agent.update",
      sessionId: "s_ada",
      kind: "text",
      data: { text: "from store", streamEventId: 1 },
    });
    relay.close();
    db.close();
    await removeTempPath(dir, { recursive: true });
  });

  it("falls back to the store path when a stale daemon push endpoint fails to open", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nexus-daemon-push-"));
    const path = join(dir, "nexus-stream.db");
    const db = await initStreamStore(path);
    const doorbell = manualDoorbell();
    const connector: DaemonPushConnector = () => ({
      ready: Promise.reject(new Error("stale gateway stream endpoint")),
      subscribe() {
        return () => {};
      },
      close() {},
    });
    const events: Array<Record<string, unknown>> = [];
    const errors: unknown[] = [];

    await db.execute({
      sql:
        "INSERT INTO stream_events (session_id, kind, data, created_at) " +
        "VALUES (?, ?, ?, ?)",
      args: ["s_ada", "text", JSON.stringify({ text: "from stale fallback" }), Date.now()],
    });
    const relay = createStoreBackedAgentSessionRelay("s_ada", {
      storePath: path,
      doorbell,
      streamHeartbeatMs: 10_000,
      fallbackPollMs: 10_000,
      daemonPushConnector: connector,
    })({
      onEvent(event) {
        events.push(event as unknown as Record<string, unknown>);
      },
      onError(error) {
        errors.push(error);
      },
    });

    await relay.ready;
    doorbell.ring();
    await waitFor(() => events.some((event) => event.kind === "text"));
    expect(errors).toEqual([]);
    expect(events).toContainEqual({
      type: "agent.update",
      sessionId: "s_ada",
      kind: "text",
      data: { text: "from stale fallback", streamEventId: 1 },
    });
    relay.close();
    db.close();
    await removeTempPath(dir, { recursive: true });
  });
});
