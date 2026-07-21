import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";

import { handleSessionEvents } from "../stream/sessionEvents";
import {
  encodeCursor,
  SessionFanoutHub,
  type SessionFanoutFrame,
} from "../stream/sessionFanout";

type WsModule = typeof import("./ws.mjs");

const BOOT_ID = "boot-backpressure";
const MAX_OUTBOUND_BYTES = 1024 * 1024;
const BACKPRESSURE_PREFIX = "session.bp:";

class TestSocket extends EventEmitter {
  sent: string[] = [];
  closes: Array<{ code?: number; reason?: string }> = [];
  bufferedAmount = 0;
  onSend?: (payload: string) => void;

  send(data: string | Buffer) {
    const payload = String(data);
    this.sent.push(payload);
    this.onSend?.(payload);
  }

  close(code?: number, reason?: string) {
    this.closes.push({ code, reason });
    this.emit("close");
  }
}

function textStream(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}

function controlledTextStream() {
  const encoder = new TextEncoder();
  let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  return {
    stream: new ReadableStream<Uint8Array>({
      start(streamController) {
        controller = streamController;
      },
    }),
    enqueue(chunk: string) {
      controller?.enqueue(encoder.encode(chunk));
    },
  };
}

function agentFrame(sessionId: string, id: number, text: string): SessionFanoutFrame {
  return {
    lane: "agent",
    epoch: BOOT_ID,
    id,
    cursor: encodeCursor(BOOT_ID, id),
    event: {
      type: "agent.update",
      sessionId,
      kind: "text",
      data: { text },
    },
  };
}

function fanoutHarness() {
  const emitters = new Map<string, (frame: SessionFanoutFrame) => void>();
  const hub = new SessionFanoutHub((sessionId, handlers) => {
    emitters.set(sessionId, handlers.onFrame);
    return {
      ready: Promise.resolve(),
      daemonBootId: () => BOOT_ID,
      close() {},
    };
  }, 32);
  return {
    hub,
    emit(sessionId: string, frame: SessionFanoutFrame) {
      const emit = emitters.get(sessionId);
      if (!emit) throw new Error(`session ${sessionId} has no upstream`);
      emit(frame);
    },
  };
}

function boundSessionResponse(
  request: Request,
  sessionId: string,
  hub: SessionFanoutHub,
): Response {
  const source = new URL(request.url);
  const events = new URL(
    `http://localhost/api/v1/agent-sessions/${encodeURIComponent(sessionId)}/events`,
  );
  events.searchParams.set("view", "agui");
  const after = source.searchParams.get("after");
  if (after !== null) events.searchParams.set("after", after);
  const response = handleSessionEvents(new Request(events, { signal: request.signal }), sessionId, {
    fanout: hub,
  });
  const headers = new Headers(response.headers);
  headers.set("x-nexus-session-id", sessionId);
  headers.set("x-nexus-agent-id", `a_${sessionId}`);
  headers.set("x-nexus-agent-name", sessionId);
  return new Response(response.body, { status: response.status, headers });
}

function boundTextResponse(chunks: string[], sessionId: string): Response {
  return new Response(textStream(chunks), {
    headers: {
      "content-type": "text/event-stream",
      "x-nexus-session-id": sessionId,
      "x-nexus-agent-id": `a_${sessionId}`,
      "x-nexus-agent-name": sessionId,
    },
  });
}

function opaqueCursorFromClose(socket: TestSocket): string {
  expect(socket.closes).toHaveLength(1);
  expect(socket.closes[0]?.code).toBe(1013);
  const reason = socket.closes[0]?.reason ?? "";
  expect(reason.startsWith(BACKPRESSURE_PREFIX)).toBe(true);
  return reason.slice(BACKPRESSURE_PREFIX.length);
}

function eventTexts(socket: TestSocket): string[] {
  return socket.sent.flatMap((payload) => {
    const parsed = JSON.parse(payload) as {
      type?: string;
      delta?: string;
      event?: { data?: { text?: string } };
    };
    if (parsed.type === "TEXT_MESSAGE_CONTENT" && parsed.delta) return [parsed.delta];
    return parsed.event?.data?.text ? [parsed.event.data.text] : [];
  });
}

async function loadWs(): Promise<WsModule> {
  return import("./ws.mjs") as Promise<WsModule>;
}

describe("agent-session WebSocket backpressure", () => {
  it("keeps a production-sized resume cursor inside the 123-byte close-reason limit", async () => {
    const { pumpSseResponseToSocket } = await loadWs();
    const socket = new TestSocket();
    const productionBootId = `boot_${"f".repeat(32)}`;
    const acceptedCursor = encodeCursor(productionBootId, Number.MAX_SAFE_INTEGER);
    socket.onSend = () => {
      socket.bufferedAmount = MAX_OUTBOUND_BYTES + 1;
    };

    await pumpSseResponseToSocket(
      new Response(textStream([
        `data: ${JSON.stringify({
          cursor: acceptedCursor,
          epoch: productionBootId,
          event: { type: "agent.update", data: { text: "accepted" } },
        })}\n\n`,
        `data: ${JSON.stringify({
          cursor: encodeCursor(productionBootId, Number.MAX_SAFE_INTEGER - 1),
          epoch: productionBootId,
          event: { type: "agent.update", data: { text: "not accepted" } },
        })}\n\n`,
      ])),
      socket,
      undefined,
      undefined,
      { session: true },
    );

    const reason = socket.closes[0]?.reason ?? "";
    expect(Buffer.byteLength(reason, "utf8")).toBeLessThanOrEqual(123);
    expect(opaqueCursorFromClose(socket)).toBe(acceptedCursor);
  });

  it("does not commit a cursor until same-cursor siblings split across chunks are accepted", async () => {
    const { handleWs } = await loadWs();
    const sessionId = "split-siblings";
    const priorCursor = encodeCursor(BOOT_ID, 40);
    const siblingCursor = encodeCursor(BOOT_ID, 41);
    const prior = JSON.stringify({
      type: "TEXT_MESSAGE_CONTENT",
      messageId: "prior",
      delta: "prior accepted",
      cursor: priorCursor,
      epoch: BOOT_ID,
    });
    const siblingA = JSON.stringify({
      type: "TEXT_MESSAGE_START",
      messageId: "same-cursor",
      cursor: siblingCursor,
      epoch: BOOT_ID,
    });
    const siblingB = JSON.stringify({
      type: "TEXT_MESSAGE_CONTENT",
      messageId: "same-cursor",
      delta: "sibling B must replay",
      cursor: siblingCursor,
      epoch: BOOT_ID,
    });
    const first = new TestSocket();
    first.onSend = (payload) => {
      if (payload === siblingA) first.bufferedAmount = MAX_OUTBOUND_BYTES + 1;
    };
    const firstControl = handleWs(
      first,
      new Request(`http://localhost/api/v1/agent-sessions/${sessionId}/events?view=agui`),
      {
        observe: async () => boundTextResponse(
          [`data: ${prior}\n\n`, `data: ${siblingA}\n\n`, `data: ${siblingB}\n\n`],
          sessionId,
        ),
      },
    );

    await firstControl.closed;
    const reconnectCursor = opaqueCursorFromClose(first);
    expect(reconnectCursor).toBe(priorCursor);
    expect(first.sent).toEqual([prior, siblingA]);

    const resumed = new TestSocket();
    const resumedControl = handleWs(
      resumed,
      new Request(
        `http://localhost/api/v1/agent-sessions/${sessionId}/events` +
          `?view=agui&after=${encodeURIComponent(reconnectCursor)}`,
      ),
      {
        observe: async (request: Request) => {
          expect(new URL(request.url).searchParams.get("after")).toBe(priorCursor);
          return boundTextResponse([`data: ${siblingA}\n\ndata: ${siblingB}\n\n`], sessionId);
        },
      },
    );

    await resumedControl.closed;
    expect(resumed.sent).toEqual([siblingA, siblingB]);
  });

  it("rejects a frame that would exceed the byte bound before accepting it", async () => {
    const { pumpSseResponseToSocket } = await loadWs();
    const socket = new TestSocket();
    socket.bufferedAmount = MAX_OUTBOUND_BYTES - 1;

    await pumpSseResponseToSocket(
      new Response(textStream([
        `data: ${JSON.stringify({
          cursor: encodeCursor(BOOT_ID, 1),
          epoch: BOOT_ID,
          event: { type: "agent.update", data: { text: "not accepted" } },
        })}\n\n`,
      ])),
      socket,
      undefined,
      undefined,
      { session: true },
    );

    expect(socket.sent).toEqual([]);
    expect(opaqueCursorFromClose(socket)).toBe("none");
  });

  it("closes before an incomplete SSE record can grow the subscriber buffer past its bound", async () => {
    const { pumpSseResponseToSocket } = await loadWs();
    const socket = new TestSocket();
    const source = controlledTextStream();
    const pumping = pumpSseResponseToSocket(
      new Response(source.stream),
      socket,
      undefined,
      undefined,
      { session: true },
    );

    source.enqueue(`data: ${"x".repeat(MAX_OUTBOUND_BYTES + 1)}`);

    await vi.waitFor(() => expect(socket.closes).toHaveLength(1));
    await pumping;
    expect(socket.sent).toEqual([]);
    expect(opaqueCursorFromClose(socket)).toBe("none");
  });

  it("reconnects from the last accepted opaque cursor without loss or duplicates", async () => {
    const { handleWs } = await loadWs();
    const source = fanoutHarness();
    const sessionId = "slow-agent";
    const keeper = source.hub.subscribe(sessionId, { view: "nexus", onFrame() {} });
    await keeper.ready;

    const slow = new TestSocket();
    slow.onSend = () => {
      slow.bufferedAmount = MAX_OUTBOUND_BYTES + 1;
    };
    const first = handleWs(
      slow,
      new Request(`http://localhost/api/agui/ws?session=${sessionId}`),
      { observe: async (request: Request) => boundSessionResponse(request, sessionId, source.hub) },
    );

    source.emit(sessionId, agentFrame(sessionId, 1, "accepted once"));
    await vi.waitFor(() => expect(eventTexts(slow)).toEqual(["accepted once"]));
    source.emit(sessionId, agentFrame(sessionId, 2, "replay exactly once"));
    source.emit(sessionId, agentFrame(sessionId, 2, "duplicate source row"));
    await first.closed;

    const acceptedCursor = opaqueCursorFromClose(slow);
    expect(acceptedCursor).toBe(encodeCursor(BOOT_ID, 1));

    const resumed = new TestSocket();
    const second = handleWs(
      resumed,
      new Request(
        `http://localhost/api/agui/ws?session=${sessionId}&after=${encodeURIComponent(acceptedCursor)}`,
      ),
      { observe: async (request: Request) => boundSessionResponse(request, sessionId, source.hub) },
    );

    await vi.waitFor(() => expect(eventTexts(resumed)).toEqual(["replay exactly once"]));
    source.emit(sessionId, agentFrame(sessionId, 3, "then live once"));
    source.emit(sessionId, agentFrame(sessionId, 3, "duplicate live row"));
    await vi.waitFor(() => {
      expect(eventTexts(resumed)).toEqual(["replay exactly once", "then live once"]);
    });

    second.close();
    await second.closed;
    keeper.close();
  });

  it("applies resumable session backpressure to queue snapshots", async () => {
    const { handleWs } = await loadWs();
    const sessionId = "slow-queue";
    const source = controlledTextStream();
    let onSnapshot: ((snapshot: Record<string, unknown>) => void) | undefined;
    const socket = new TestSocket();
    const control = handleWs(
      socket,
      new Request(`http://localhost/api/agui/ws?session=${sessionId}`),
      {
        observe: async () => new Response(source.stream, {
          headers: {
            "content-type": "text/event-stream",
            "x-nexus-session-id": sessionId,
            "x-nexus-agent-id": `a_${sessionId}`,
            "x-nexus-agent-name": sessionId,
          },
        }),
        commandQueueHub: {
          subscribe(_request: Request, _target: unknown, handlers: {
            onSnapshot(snapshot: Record<string, unknown>): void;
          }) {
            onSnapshot = handlers.onSnapshot;
            return () => {};
          },
        },
      },
    );
    await vi.waitFor(() => expect(onSnapshot).toBeDefined());

    const firstCursor = encodeCursor(BOOT_ID, 1);
    const secondCursor = encodeCursor(BOOT_ID, 2);
    source.enqueue(`data: ${JSON.stringify({
      type: "TEXT_MESSAGE_CONTENT",
      messageId: "accepted-1",
      delta: "accepted one",
      cursor: firstCursor,
      epoch: BOOT_ID,
    })}\n\ndata: ${JSON.stringify({
      type: "TEXT_MESSAGE_CONTENT",
      messageId: "accepted-2",
      delta: "accepted two",
      cursor: secondCursor,
      epoch: BOOT_ID,
    })}\n\n`);
    await vi.waitFor(() => expect(socket.sent).toHaveLength(2));

    socket.bufferedAmount = MAX_OUTBOUND_BYTES + 1;
    onSnapshot?.({ sessionId, seq: 2, commands: [] });

    await control.closed;
    expect(opaqueCursorFromClose(socket)).toBe(firstCursor);
    expect(socket.sent).toHaveLength(2);
  });

  it("allows one bounded canonical queue snapshot on an empty socket", async () => {
    const { handleWs } = await loadWs();
    const sessionId = "full-queue";
    const source = controlledTextStream();
    let onSnapshot: ((snapshot: Record<string, unknown>) => void) | undefined;
    const socket = new TestSocket();
    const control = handleWs(
      socket,
      new Request(`http://localhost/api/agui/ws?session=${sessionId}`),
      {
        observe: async () => new Response(source.stream, {
          headers: {
            "content-type": "text/event-stream",
            "x-nexus-session-id": sessionId,
            "x-nexus-agent-id": `a_${sessionId}`,
            "x-nexus-agent-name": sessionId,
          },
        }),
        commandQueueHub: {
          subscribe(_request: Request, _target: unknown, handlers: {
            onSnapshot(snapshot: Record<string, unknown>): void;
          }) {
            onSnapshot = handlers.onSnapshot;
            return () => {};
          },
        },
      },
    );
    await vi.waitFor(() => expect(onSnapshot).toBeDefined());

    onSnapshot?.({
      sessionId,
      seq: 100,
      commands: [{ commandId: "cmd_full", text: "Q".repeat(2 * 1024 * 1024) }],
    });

    expect(socket.closes).toEqual([]);
    expect(JSON.parse(socket.sent[0] ?? "{}")).toMatchObject({
      t: "queue.snapshot",
      sessionId,
      seq: 100,
    });
    control.close();
    await control.closed;
  });

  it("isolates a saturated subscriber from a healthy agent-session lane", async () => {
    const { handleWs } = await loadWs();
    const source = fanoutHarness();
    const slowSession = "slow-lane";
    const fastSession = "fast-lane";

    const slow = new TestSocket();
    slow.bufferedAmount = MAX_OUTBOUND_BYTES + 1;
    const fast = new TestSocket();
    const slowControl = handleWs(
      slow,
      new Request(`http://localhost/api/agui/ws?session=${slowSession}`),
      { observe: async (request: Request) => boundSessionResponse(request, slowSession, source.hub) },
    );
    const fastControl = handleWs(
      fast,
      new Request(`http://localhost/api/agui/ws?session=${fastSession}`),
      { observe: async (request: Request) => boundSessionResponse(request, fastSession, source.hub) },
    );
    await vi.waitFor(() => expect(source.hub.upstreamCount()).toBe(2));

    source.emit(slowSession, agentFrame(slowSession, 1, "slow-only"));
    source.emit(fastSession, agentFrame(fastSession, 1, "fast-only"));

    await vi.waitFor(() => expect(eventTexts(fast)).toEqual(["fast-only"]));
    await slowControl.closed;
    expect(eventTexts(slow)).toEqual([]);
    expect(opaqueCursorFromClose(slow)).toBe("none");
    expect(eventTexts(fast)).not.toContain("slow-only");

    fastControl.close();
    await fastControl.closed;
  });
});
