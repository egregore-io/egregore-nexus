import { describe, expect, it, vi } from "vitest";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createManifestDaemonPushConnection, type DaemonPushConnection, type DaemonPushSubscription } from "../agui/daemonPushRelay.mjs";
import { handleSessionEvents } from "./sessionEvents";
import { encodeCursor, SessionFanoutHub } from "./sessionFanout";
import { subscribeRetainedSession } from "./retainedSessionReplay";
import { toObserveRequest } from "../agui/ws.mjs";


function source(initial: unknown[] = [], boot = "boot-a") {
  let handlers: Parameters<DaemonPushConnection["subscribe"]>[1] | undefined;
  let resolve!: () => void;
  const ready = new Promise<void>((done) => { resolve = done; });
  const subscriptions: DaemonPushSubscription[] = [];
  const close = vi.fn();
  const unsubscribe = vi.fn();
  const connection: DaemonPushConnection = {
    ready,
    daemonBootId: boot,
    subscribe(subscription, next) {
      subscriptions.push(subscription);
      handlers = next;
      for (const frame of initial) next.onFrame(frame);
      return unsubscribe;
    },
    close,
  };
  return { connection, subscriptions, close, unsubscribe, resolve,
    emit: (frame: unknown) => handlers?.onFrame(frame),
    error: () => handlers?.onError(new Error("source unavailable")),
  };
}

function agent(id: number, sessionId = "s_ada", kind = "text") {
  return { t: "agent.update", sessionId, streamEventId: id, kind, data: { text: `row-${id}` } };
}

function open(fixture: ReturnType<typeof source>, after?: string) {
  const shared = vi.fn(() => ({ ready: Promise.resolve(), close() {} }));
  const params = new URLSearchParams({
    replay: "retained", view: "agui", agentId: "a_ada", expectedSessionId: "s_ada",
    ...(after === undefined ? {} : { after }),
  });
  const response = handleSessionEvents(
    new Request(`http://localhost/api/v1/agent-sessions/s_ada/events?${params}`), "s_ada",
    { retainedConnector: () => fixture.connection, fanout: new SessionFanoutHub(shared) },
  );
  return { response, shared };
}

async function next(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<string> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      reader.read(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("retained frame timeout")), 300);
      }),
    ]);
    return new TextDecoder().decode(result.value);
  } finally { clearTimeout(timer); }
}

describe("retained agent-session replay", () => {
  it("preserves exact retained replay options through the existing WebSocket observe translation", () => {
    const after = encodeCursor("boot-a", 601);
    const params = new URLSearchParams({ replay: "retained", session: "ada", agentId: "a_ada",
      expectedSessionId: "s_ada", after });
    const request = toObserveRequest(new Request(`http://localhost/api/agui/ws?${params}`, {
      headers: { authorization: "Bearer synthetic-token" },
    }));
    const url = new URL(request.url);
    expect(url.pathname).toBe("/api/agui/observe");
    expect(url.searchParams.get("replay")).toBe("retained");
    expect(url.searchParams.get("agentId")).toBe("a_ada");
    expect(url.searchParams.get("expectedSessionId")).toBe("s_ada");
    expect(url.searchParams.get("after")).toBe(after);
    expect(request.headers.get("authorization")).toBe("Bearer synthetic-token");
  });

  it("sends authenticated source proof even when ready precedes empty catch-up", async () => {
    const fixture = source();
    const { response, shared } = open(fixture);
    const reader = response.body!.getReader();
    fixture.resolve();
    try {
      const proof = await next(reader);
      expect(proof).toContain('"name":"nexus.recording.source"');
      expect(proof).toContain('"mode":"retained-agent"');
      expect(proof).toContain('"sessionId":"s_ada"');
      expect(proof).toContain('"daemonBootId":"boot-a"');
      expect(proof).not.toContain("cursor");
      expect(shared).not.toHaveBeenCalled();
      expect(fixture.subscriptions).toEqual([{ lane: "agent", sessionId: "s_ada", afterId: 0 }]);
      fixture.emit(agent(1));
      expect(await next(reader)).toContain("row-1");
    } finally { await reader.cancel(); }
    expect(fixture.close).toHaveBeenCalledTimes(1);
    expect(fixture.unsubscribe).toHaveBeenCalledTimes(1);
  });

  it("streams more than 512 retained frames in order without Gateway ring eviction", async () => {
    const fixture = source([
      ...Array.from({ length: 600 }, (_, i) => agent(i + 1)),
      agent(601, "s_ada", "turn_end"),
    ]);
    const { response } = open(fixture);
    const reader = response.body!.getReader();
    fixture.resolve();
    try {
      const chunks = [await next(reader)];
      expect(chunks[0]).toContain("nexus.recording.source");
      for (let id = 1; id <= 601; id++) {
        const chunk = await next(reader);
        chunks.push(chunk);
        expect(chunk).toContain(id === 601 ? "RUN_FINISHED" : `row-${id}`);
        expect(chunk).toContain(encodeCursor("boot-a", id));
        expect(chunk).not.toContain("resync");
      }
      // Explicit opt-in only: a create-new disposable artifact for the real Lens/native gate.
      if (process.env.NEXUS_RETAINED_REPLAY_ARTIFACT) {
        const resumedSource = source([agent(601, "s_ada", "turn_end"), agent(602), agent(603, "s_ada", "turn_end")]);
        const resumedReader = open(resumedSource, encodeCursor("boot-a", 601)).response.body!.getReader();
        resumedSource.resolve();
        try {
          const resumed = [await next(resumedReader), await next(resumedReader), await next(resumedReader)];
          expect(resumed[0]).toContain("nexus.recording.source");
          expect(resumed[1]).toContain("row-602");
          expect(resumed[2]).toContain("RUN_FINISHED");
          await writeFile(process.env.NEXUS_RETAINED_REPLAY_ARTIFACT, JSON.stringify({
            sessionId: "s_ada", agentId: "a_ada", fresh: chunks, resumed,
            freshCursor: encodeCursor("boot-a", 601), resumedCursor: encodeCursor("boot-a", 603),
          }), { flag: "wx" });
        } finally { await resumedReader.cancel(); }
      }
    } finally { await reader.cancel(); }
  });

  it("requests the exact anchor and only emits its ordered successors", async () => {
    const fixture = source([agent(20, "s_ada", "turn_end"), agent(21), agent(22)]);
    const { response } = open(fixture, encodeCursor("boot-a", 20));
    const reader = response.body!.getReader();
    fixture.resolve();
    try {
      expect(await next(reader)).toContain("nexus.recording.source");
      expect(fixture.subscriptions[0]?.afterId).toBe(19);
      expect(await next(reader)).toContain("row-21");
      expect(await next(reader)).toContain("row-22");
    } finally { await reader.cancel(); }
  });

  it.each([
    { frames: [agent(21)], cursor: encodeCursor("boot-a", 20), reason: "source_cursor_missing" },
    { frames: [agent(21)], cursor: encodeCursor("old-boot", 20), reason: "boot_mismatch" },
    { frames: [agent(1, "s_foreign")], reason: "source_session_mismatch" },
    { frames: [{ t: "gap", lane: "agent", sessionId: "s_ada", afterId: 1 }], reason: "daemon_gap" },
    { frames: [agent(0)], reason: "source_frame_invalid" },
    { frames: [agent(1, "s_ada", "invented")], reason: "source_frame_invalid" },
    { frames: [agent(1)], cursor: "bad", reason: "malformed_cursor" },
  ])("closes without content on $reason", async ({ frames, cursor, reason }) => {
    const fixture = source(frames);
    const { response } = open(fixture, cursor);
    const reader = response.body!.getReader();
    fixture.resolve();
    try {
      let text = await next(reader);
      if (!text.includes("nexus.session.resync")) text += await next(reader);
      expect(text).toContain(reason);
      expect(text).not.toContain("row-");
      expect(await next(reader)).toBe("");
      expect(fixture.close).toHaveBeenCalledTimes(1);
    } finally { await reader.cancel(); }
  });

  it("closes on changed authenticated boot after ready", async () => {
    const fixture = source();
    const { response } = open(fixture);
    const reader = response.body!.getReader();
    fixture.resolve();
    try {
      await next(reader);
      Object.assign(fixture.connection, { daemonBootId: "boot-b" });
      fixture.emit(agent(1));
      expect(await next(reader)).toContain("boot_mismatch");
      expect(await next(reader)).toBe("");
    } finally { await reader.cancel(); }
  });

  it("cancels before readiness without subscribing or leaking the source", async () => {
    const fixture = source();
    const { response } = open(fixture);
    await response.body!.cancel();
    fixture.resolve();
    await Promise.resolve();
    expect(fixture.subscriptions).toEqual([]);
    expect(fixture.close).toHaveBeenCalledTimes(1);
  });

  it.each([false, "throws"])("closes when a downstream subscriber returns %s", async (behavior) => {
    const fixture = source();
    const onFrame = vi.fn(() => {
      if (behavior === "throws") throw new Error("consumer failed");
      return false;
    });
    const stream = subscribeRetainedSession("s_ada", {
      onSource() {}, onFrame, onEnd() {},
    }, () => fixture.connection);
    fixture.resolve();
    await stream.ready;
    expect(() => fixture.emit(agent(1))).not.toThrow();
    fixture.emit(agent(2));
    expect(onFrame).toHaveBeenCalledTimes(1);
    expect(fixture.close).toHaveBeenCalledTimes(1);
    expect(fixture.unsubscribe).toHaveBeenCalledTimes(1);
  });

  it("reconnects transport failure from the accepted anchor without reporting a source gap", async () => {
    const fixture = source();
    const { response } = open(fixture);
    const reader = response.body!.getReader();
    fixture.resolve();
    try {
      await next(reader);
      fixture.emit(agent(1));
      expect(await next(reader)).toContain("row-1");
      fixture.error();
      expect(await next(reader)).toBe("");
      expect(fixture.close).toHaveBeenCalledTimes(1);
    } finally { await reader.cancel(); }
    // Reader acceptance/native durability is external: retry exactly the supplied checkpoint,
    // not the failed connection's later speculative content.
    const retried = source([agent(1, "s_ada", "turn_end"), agent(2)]);
    const retryReader = open(retried, encodeCursor("boot-a", 1)).response.body!.getReader();
    retried.resolve();
    try {
      expect(await next(retryReader)).toContain("nexus.recording.source");
      expect(await next(retryReader)).toContain("row-2");
      expect(retried.subscriptions[0]?.afterId).toBe(0);
    } finally { await retryReader.cancel(); }
  });

  it("accepts an explicit current-boot zero cursor as the retained beginning", async () => {
    const fixture = source([agent(8)]);
    const reader = open(fixture, encodeCursor("boot-a", 0)).response.body!.getReader();
    fixture.resolve();
    try {
      expect(await next(reader)).toContain("nexus.recording.source");
      expect(await next(reader)).toContain("row-8");
      expect(fixture.subscriptions[0]?.afterId).toBe(0);
    } finally { await reader.cancel(); }
  });

  it("ends unavailable transport setup without a false loss-of-history claim", async () => {
    const response = handleSessionEvents(new Request(
      "http://localhost/api/v1/agent-sessions/s_ada/events?replay=retained&view=agui&agentId=a_ada&expectedSessionId=s_ada",
    ), "s_ada", { retainedConnector: () => undefined });
    expect(await response.text()).toBe("");
  });

  it("does not create a source for an already-aborted authenticated request", async () => {
    const abort = new AbortController();
    abort.abort();
    const connector = vi.fn(() => source().connection);
    const response = handleSessionEvents(new Request(
      "http://localhost/api/v1/agent-sessions/s_ada/events?replay=retained&view=agui&agentId=a_ada&expectedSessionId=s_ada",
      { signal: abort.signal },
    ), "s_ada", { retainedConnector: connector });
    try {
      expect(connector).not.toHaveBeenCalled();
      expect(await response.text()).toBe("");
    } finally { if (!response.bodyUsed) await response.body?.cancel(); }
  });

  it("cannot open a socket after cancellation during the actual manifest read", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nexus-retained-cancel-"));
    const previousHome = process.env.NEXUS_HOME;
    let release!: () => void;
    let startRead!: () => void;
    const entered = new Promise<void>((resolve) => { startRead = resolve; });
    const wait = new Promise<void>((resolve) => { release = resolve; });
    let connection: DaemonPushConnection | undefined;
    try {
      await writeFile(join(dir, "gateway-stream-endpoint.json"), JSON.stringify({
        // A closed-before-open source must never even attempt this nonexistent socket.
        path: join(dir, "must-not-open.sock"), token: "synthetic-secret", daemonBootId: "boot-a",
      }), { flag: "wx" });
      process.env.NEXUS_HOME = dir;
      connection = createManifestDaemonPushConnection({ readManifest: async (path) => {
        startRead();
        await wait;
        return readFile(path, "utf8");
      } })!;
      const settled = connection.ready.then(() => "closed", () => "attempted-socket");
      await entered;
      connection.close();
      release();
      expect(await settled).toBe("closed");
    } finally {
      release();
      connection?.close();
      if (previousHome === undefined) delete process.env.NEXUS_HOME;
      else process.env.NEXUS_HOME = previousHome;
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("bounds slow-reader catch-up on private authenticated sockets while another reader progresses", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nexus-retained-"));
    const path = process.platform === "win32"
      ? `\\\\.\\pipe\\nexus-retained-${process.pid}-${Date.now()}` : join(dir, "push.sock");
    const sockets = new Set<Socket>();
    const wire: Record<string, unknown>[] = [];
    const frameBytes = (value: unknown) => {
      const body = Buffer.from(JSON.stringify(value));
      const header = Buffer.alloc(4);
      header.writeUInt32BE(body.length);
      return Buffer.concat([header, body]);
    };
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
      let buffer = Buffer.alloc(0);
      socket.on("data", (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);
        while (buffer.length >= 4) {
          const size = buffer.readUInt32BE(0);
          if (buffer.length < size + 4) break;
          const frame = JSON.parse(buffer.subarray(4, size + 4).toString()) as Record<string, unknown>;
          buffer = buffer.subarray(size + 4);
          wire.push(frame);
          if (frame.t === "hello") {
            socket.write(frameBytes({ t: "ready", daemonBootId: "boot-a", version: 1 }));
          } else if (frame.t === "subscribe") {
            const anchor = Number(frame.afterId) + 1;
            socket.write(Buffer.concat([
              frameBytes(agent(anchor, "s_ada", "turn_end")),
              ...(anchor === 100 ? Array.from({ length: 600 }, (_, i) => frameBytes({
                ...agent(101 + i), data: { text: `row-${101 + i}:` + "x".repeat(2048) },
              })) : [frameBytes(agent(anchor + 1))]),
            ]));
          }
        }
      });
    });
    const previousHome = process.env.NEXUS_HOME;
    const readers: ReadableStreamDefaultReader<Uint8Array>[] = [];
    try {
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(path, resolve);
      });
      await writeFile(join(dir, "gateway-stream-endpoint.json"), JSON.stringify({
        version: 1, path, token: "synthetic-secret", daemonBootId: "boot-a",
      }), { flag: "wx" });
      process.env.NEXUS_HOME = dir;
      let sourceFrames = 0;
      let pauses = 0;
      let resumes = 0;
      for (const anchor of [100, 20]) {
        const query = new URLSearchParams({ replay: "retained", view: "agui", agentId: "a_ada",
          expectedSessionId: "s_ada", after: encodeCursor("boot-a", anchor) });
        const reader = handleSessionEvents(new Request(`http://localhost/api/v1/agent-sessions/s_ada/events?${query}`), "s_ada", anchor === 100 ? {
          retainedConnector: () => {
            const connection = createManifestDaemonPushConnection()!;
            return {
              ready: connection.ready,
              get daemonBootId() { return connection.daemonBootId; },
              subscribe(subscription, handlers) {
                return connection.subscribe(subscription, { ...handlers, onFrame(frame) {
                  sourceFrames++;
                  handlers.onFrame(frame);
                } });
              },
              pause() { pauses++; connection.pause?.(); },
              resume() { resumes++; connection.resume?.(); },
              close() { connection.close(); },
            };
          },
        } : {}).body!.getReader();
        readers.push(reader);
        expect(await next(reader)).toContain("nexus.recording.source");
        if (anchor === 100) {
          await vi.waitFor(() => expect(pauses).toBeGreaterThan(0), { timeout: 1000 });
          expect(sourceFrames).toBeLessThan(601);
        } else {
          // The other exact observer remains responsive while the first socket is paused.
          expect(await next(reader)).toContain("row-21");
          expect(sourceFrames).toBeLessThan(601);
        }
      }
      for (let id = 101; id <= 700; id++) {
        expect(await next(readers[0]!)).toContain(`row-${id}:`);
      }
      expect(sourceFrames).toBe(601);
      expect(resumes).toBeGreaterThan(0);
      const hellos = wire.filter((frame) => frame.t === "hello");
      expect(hellos).toHaveLength(2);
      expect(hellos.every((hello) => hello.token === "synthetic-secret")).toBe(true);
      expect(hellos.every((hello) => JSON.stringify(hello.subscriptions) === "[]")).toBe(true);
      expect(wire.filter((frame) => frame.t === "subscribe").map((frame) => ({
        lane: frame.lane, sessionId: frame.sessionId, afterId: frame.afterId,
      }))).toEqual([
        { lane: "agent", sessionId: "s_ada", afterId: 99 },
        { lane: "agent", sessionId: "s_ada", afterId: 19 },
      ]);
    } finally {
      await Promise.all(readers.map((reader) => reader.cancel()));
      if (previousHome === undefined) delete process.env.NEXUS_HOME;
      else process.env.NEXUS_HOME = previousHome;
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(dir, { recursive: true, force: true });
    }
  });
});
