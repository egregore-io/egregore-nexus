import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";

type WsModule = typeof import("./ws.mjs");

class FakeSocket extends EventEmitter {
  sent: string[] = [];
  closed: { code?: number; reason?: string } | null = null;
  bufferedAmount = 0;

  send(data: string | Buffer) {
    this.sent.push(String(data));
  }

  close(code?: number, reason?: string) {
    this.closed = { code, reason };
    this.emit("close");
  }
}

type ToolCallEventHandlers = {
  onEvent: (event: unknown) => void;
  onGap?: (frame: unknown) => void;
  onError?: (error: unknown) => void;
};

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
      start(c) {
        controller = c;
      },
    }),
    enqueue(chunk: string) {
      controller?.enqueue(encoder.encode(chunk));
    },
    close() {
      controller?.close();
    },
  };
}

async function loadWs(): Promise<WsModule> {
  return import("./ws.mjs") as Promise<WsModule>;
}

describe("AG-UI WebSocket server transport", () => {
  it("pumps SSE data records as raw AG-UI JSON websocket messages", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const observe = vi.fn(async () => new Response(
      textStream([
        ": nexus-open\n\n",
        "data: {\"type\":\"RUN_STARTED\",\"runId\":\"r1\"}\n\n",
        "data: {\"type\":\"RUN_FINISHED\",\"runId\":\"r1\"}\n\n",
      ]),
      { status: 200, headers: { "content-type": "text/event-stream" } },
    ));

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      { observe },
    );
    await control.closed;

    expect(observe).toHaveBeenCalledWith(
      expect.objectContaining({ url: "http://localhost/api/agui/observe?session=otto" }),
    );
    expect(socket.sent).toEqual([
      "{\"type\":\"RUN_STARTED\",\"runId\":\"r1\"}",
      "{\"type\":\"RUN_FINISHED\",\"runId\":\"r1\"}",
    ]);
  });

  it("handles ping envelopes without touching the observe stream", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    handleWs(socket, new Request("http://localhost/api/agui/ws?thread=ops"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
    });

    socket.emit("message", JSON.stringify({ t: "ping" }));

    expect(socket.sent).toContain(JSON.stringify({ t: "pong" }));
  });

  it("isolates malformed frames and keeps the socket usable", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    handleWs(socket, new Request("http://localhost/api/agui/ws"), {});

    socket.emit("message", "{not-json");
    socket.emit("message", JSON.stringify({ t: "ping" }));
    await vi.waitFor(() => expect(socket.sent).toContain(JSON.stringify({ t: "pong" })));

    expect(socket.sent).toContain(JSON.stringify({ t: "input.err", error: "frame must be JSON" }));
    expect(socket.closed).toBeNull();
  });

  it("closes a presentation socket with retry-later under bounded backpressure", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    socket.bufferedAmount = 1024 * 1024 + 1;
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () => new Response(
          textStream(['data: {"type":"RUN_STARTED","runId":"r1"}\n\n']),
          { status: 200, headers: { "content-type": "text/event-stream" } },
        ),
      },
    );
    await control.closed;

    expect(socket.closed).toEqual({ code: 1013, reason: "ag-ui websocket backpressure" });
    expect(socket.sent).toEqual([]);
  });

  it("routes session input through prompt ingress and acknowledges durable queueing", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const sessionInput = vi.fn(async () => new Response(
      JSON.stringify({
        ok: true,
        receipt: {
          commandId: "cmd_1",
          clientMessageId: "cm_1",
          sessionId: "s_otto",
          state: "queued",
          revision: 1,
          seq: 7,
        },
      }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      sessionInput,
    });

    socket.emit(
      "message",
      JSON.stringify({
        t: "input",
        mode: "session",
        target: "otto",
        text: "continue",
        clientMessageId: "cm_1",
      }),
    );
    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "input.ack",
        commandId: "cmd_1",
        clientMessageId: "cm_1",
        sessionId: "s_otto",
        state: "queued",
        revision: 1,
        seq: 7,
      }));
    });

    expect(sessionInput).toHaveBeenCalledWith(
      expect.objectContaining({
        target: "otto",
        text: "continue",
        clientMessageId: "cm_1",
      }),
      expect.any(Request),
    );
  });

  it("routes explicit steer frames through the separate steer dependency", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const sessionInput = vi.fn();
    const steerInput = vi.fn(async () => new Response(
      JSON.stringify({
        ok: true,
        result: { accepted: true, delivery: "steered", turnId: "turn_7" },
      }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      sessionInput,
      steerInput,
    });

    socket.emit("message", JSON.stringify({
      t: "steer",
      text: "inspect the failing assertion before continuing",
      clientMessageId: "cm_steer_1",
    }));
    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "steer.ack",
        clientMessageId: "cm_steer_1",
        accepted: true,
        delivery: "steered",
        turnId: "turn_7",
      }));
    });

    expect(steerInput).toHaveBeenCalledWith(
      {
        target: "otto",
        text: "inspect the failing assertion before continuing",
        clientMessageId: "cm_steer_1",
      },
      expect.any(Request),
    );
    expect(sessionInput).not.toHaveBeenCalled();
  });

  it("defaults steer frames to the authenticated conversation steer route", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const requests: Request[] = [];

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto", {
      headers: { cookie: "nexus_human=human-token" },
    }), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      fetchHandler: vi.fn(async (request: Request) => {
        requests.push(request);
        return new Response(
          JSON.stringify({ ok: true, result: { accepted: true, delivery: "fallback_started" } }),
          { status: 201, headers: { "content-type": "application/json" } },
        );
      }),
    });

    socket.emit("message", JSON.stringify({
      t: "steer",
      target: { name: "otto", agentId: "a_otto" },
      text: "use the smaller patch",
      clientMessageId: "cm_steer_2",
    }));
    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "steer.ack",
        clientMessageId: "cm_steer_2",
        accepted: true,
        delivery: "fallback_started",
      }));
    });

    expect(requests).toHaveLength(1);
    expect(new URL(requests[0]!.url).pathname).toBe("/api/conversation/steer");
    expect(requests[0]!.headers.get("cookie")).toBe("nexus_human=human-token");
    await expect(requests[0]!.json()).resolves.toEqual({
      name: "otto",
      agentId: "a_otto",
      text: "use the smaller patch",
      clientMessageId: "cm_steer_2",
    });
  });

  it("returns one steer.err for a stale turn without falling through to normal input", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const sessionInput = vi.fn();
    const steerInput = vi.fn(async () => new Response(
      JSON.stringify({ error: "active harness does not support steering" }),
      { status: 409, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      sessionInput,
      steerInput,
    });

    socket.emit("message", JSON.stringify({
      t: "steer",
      text: "redirect this turn",
      clientMessageId: "cm_steer_err",
    }));
    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "steer.err",
        clientMessageId: "cm_steer_err",
        status: 409,
        error: "active harness does not support steering",
      }));
    });

    expect(
      socket.sent.filter((frame) => frame.includes('"t":"steer.err"')),
    ).toHaveLength(1);
    expect(
      socket.sent.some((frame) => frame.includes('"t":"steer.ack"')),
    ).toBe(false);
    expect(steerInput).toHaveBeenCalledTimes(1);
    expect(sessionInput).not.toHaveBeenCalled();
  });

  it("defaults a bare legacy input frame on a session socket to session mode (Lens plugin shape)", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const sessionInput = vi.fn(async () => new Response(
      JSON.stringify({
        ok: true,
        receipt: {
          commandId: "cmd_lens",
          clientMessageId: "cm_lens",
          sessionId: "s_otto",
          state: "queued",
          revision: 1,
          seq: 8,
        },
      }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      sessionInput,
    });

    // The Lens nexus plugin sends no mode/target: the socket's ?session= carries both.
    socket.emit(
      "message",
      JSON.stringify({ t: "input", text: "continue", clientMessageId: "cm_lens" }),
    );
    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "input.ack",
        commandId: "cmd_lens",
        clientMessageId: "cm_lens",
        sessionId: "s_otto",
        state: "queued",
        revision: 1,
        seq: 8,
      }));
    });

    expect(sessionInput).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "session",
        target: "otto",
        text: "continue",
        clientMessageId: "cm_lens",
      }),
      expect.any(Request),
    );
  });

  it("defaults a bare legacy input frame on a dm socket to a bus dm send", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const busInput = vi.fn(async () => new Response(
      JSON.stringify({ ok: true }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?dm=morgan"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      busInput,
    });

    socket.emit(
      "message",
      JSON.stringify({ t: "input", text: "hi", clientMessageId: "cm_dm" }),
    );
    await Promise.resolve();

    expect(busInput).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "bus",
        target: { verb: "dm", name: "morgan" },
        text: "hi",
      }),
      expect.any(Request),
    );
    expect(socket.sent).toContain(JSON.stringify({
      t: "input.ack",
      clientMessageId: "cm_dm",
      delivered: true,
    }));
  });

  it("still rejects a modeless input frame when the socket has no scoping query", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    handleWs(socket, new Request("http://localhost/api/agui/ws"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
    });

    socket.emit(
      "message",
      JSON.stringify({ t: "input", text: "orphan", clientMessageId: "cm_x" }),
    );
    await Promise.resolve();

    const err = socket.sent
      .map((raw: string) => JSON.parse(raw))
      .find((f: { t: string }) => f.t === "input.err");
    expect(err?.error).toMatch(/input\.mode/);
  });

  it("keeps an identity-addressed DM target on modeless socket input", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const busInput = vi.fn(async () => new Response(
      JSON.stringify({ ok: true }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?dm=display&agentId=a_real"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      busInput,
    });
    socket.emit("message", JSON.stringify({ t: "input", text: "hi" }));
    await Promise.resolve();

    expect(busInput).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "bus",
        target: { verb: "dm", name: "display", agentId: "a_real" },
      }),
      expect.any(Request),
    );
  });

  it("routes bus input through the existing /api/v1/messages ingress", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const busInput = vi.fn(async () => new Response(
      JSON.stringify({ ok: true }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?thread=nexus-project"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      busInput,
    });

    socket.emit(
      "message",
      JSON.stringify({
        t: "input",
        mode: "bus",
        target: { verb: "post", thread: "nexus-project" },
        text: "status",
        clientMessageId: "cm_bus",
      }),
    );
    await Promise.resolve();

    expect(busInput).toHaveBeenCalledWith(
      expect.objectContaining({
        target: { verb: "post", thread: "nexus-project" },
        text: "status",
        clientMessageId: "cm_bus",
      }),
      expect.any(Request),
    );
    expect(socket.sent).toContain(JSON.stringify({
      t: "input.ack",
      clientMessageId: "cm_bus",
      delivered: true,
    }));
  });

  it("replays developer events over a sys topic subscription", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const since = vi.fn(async (topic: string, afterSeq: number) => {
      if (topic !== "sys.agent.lifecycle" || afterSeq !== 7) return [];
      return [{
        kind: "agent_lifecycle",
        topic,
        seq: 8,
        ts: 1783300000000,
        agent: "otto",
        sessionId: "s_otto",
        lifecycle: "current_work",
        currentWork: "R1.5",
      }];
    });

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () => new Response(textStream([]), { status: 200 }),
        developerEvents: { since },
        developerEventPollMs: 10_000,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.lifecycle",
      afterSeq: 7,
    }));

    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        type: "developer.event",
        event: {
          kind: "agent_lifecycle",
          topic: "sys.agent.lifecycle",
          seq: 8,
          ts: 1783300000000,
          agent: "otto",
          sessionId: "s_otto",
          lifecycle: "current_work",
          currentWork: "R1.5",
        },
      }));
    });
    expect(socket.sent).toContain(JSON.stringify({
      t: "subscribe.ack",
      topic: "sys.agent.lifecycle",
      afterSeq: 7,
    }));

    control.close();
    await control.closed;
  });

  it("tails ephemeral tool-call developer events for the observed session", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () => new Response(source.stream, { status: 200 }),
        daemonToolCallEvents: null,
        developerEventPollMs: 10_000,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 0,
    }));

    source.enqueue("data: {\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"tc1\",\"toolCallName\":\"Read\"}\n\n");
    source.enqueue("data: {\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"tc1\",\"content\":\"done\",\"status\":\"completed\"}\n\n");

    await vi.waitFor(() => {
      const events = developerEvents(socket);
      expect(events).toMatchObject([
        {
          kind: "tool_call",
          topic: "sys.agent.otto.tool_call",
          seq: 1,
          agent: "otto",
          tool: "Read",
          phase: "pre",
          ok: true,
        },
        {
          kind: "tool_call",
          topic: "sys.agent.otto.tool_call",
          seq: 2,
          agent: "otto",
          tool: "Read",
          phase: "post",
          ok: true,
        },
      ]);
    });

    source.close();
    control.close();
    await control.closed;
  });

  it("tails daemon-origin tool-call developer events and suppresses socket-local duplicates", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    let handlers: ToolCallEventHandlers | undefined;
    const unsubscribe = vi.fn();
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () => new Response(source.stream, { status: 200 }),
        daemonToolCallEvents: {
          subscribe(topic: string, afterSeq: number, h: ToolCallEventHandlers) {
            expect(topic).toBe("sys.agent.otto.tool_call");
            expect(afterSeq).toBe(0);
            handlers = h;
            return unsubscribe;
          },
        },
        developerEventPollMs: 10_000,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 0,
    }));
    handlers?.onEvent({
      kind: "tool_call",
      topic: "sys.agent.otto.tool_call",
      seq: 1,
      ts: 1783300000000,
      agent: "otto",
      sessionId: "s_otto",
      tool: "Read",
      phase: "pre",
      ok: true,
    });
    source.enqueue("data: {\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"tc1\",\"toolCallName\":\"Read\"}\n\n");

    await vi.waitFor(() => {
      expect(socket.sent).toContain("{\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"tc1\",\"toolCallName\":\"Read\"}");
      const events = developerEvents(socket);
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        kind: "tool_call",
        topic: "sys.agent.otto.tool_call",
        seq: 1,
        agent: "otto",
        tool: "Read",
        phase: "pre",
        ok: true,
      });
    });

    source.close();
    control.close();
    await control.closed;
    expect(unsubscribe).toHaveBeenCalled();
  });

  it("tails ephemeral sys.fleet.status events from the daemon push lane without a session param", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const durableSince = vi.fn(async () => {
      throw new Error("fleet status must not poll durable developer_events");
    });
    let handlers: ToolCallEventHandlers | undefined;
    const unsubscribe = vi.fn();
    const control = handleWs(
      socket,
      // No ?session= — the fleet topic is not session-scoped.
      new Request("http://localhost/api/agui/ws"),
      {
        observe: async () => new Response(source.stream, { status: 200 }),
        developerEvents: { since: durableSince },
        daemonFleetStatusEvents: {
          subscribe(topic: string, afterSeq: number, h: ToolCallEventHandlers) {
            expect(topic).toBe("sys.fleet.status");
            expect(afterSeq).toBe(999);
            handlers = h;
            return unsubscribe;
          },
        },
        developerEventPollMs: 10_000,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.fleet.status",
      // Cursor retained from an older daemon boot; resync must reset it.
      afterSeq: 999,
    }));
    handlers?.onEvent({
      kind: "agent_lifecycle",
      topic: "sys.fleet.status",
      seq: 1,
      ts: 1783300000000,
      sessionId: "fleet",
      lifecycle: "resync",
      data: { reason: "subscribe", source: "members" },
    });
    handlers?.onEvent({
      kind: "agent_lifecycle",
      topic: "sys.fleet.status",
      seq: 2,
      ts: 1783300000001,
      sessionId: "s_percy",
      lifecycle: "status",
      data: { presence: "online", paused: false },
    });
    // Stale seq must be suppressed by the cursor.
    handlers?.onEvent({
      kind: "agent_lifecycle",
      topic: "sys.fleet.status",
      seq: 2,
      ts: 1783300000002,
      sessionId: "s_percy",
      lifecycle: "status",
      data: { presence: "online", paused: false },
    });

    await vi.waitFor(() => {
      expect(socket.sent).toContain(
        JSON.stringify({ t: "subscribe.ack", topic: "sys.fleet.status", afterSeq: 999 }),
      );
      const events = developerEvents(socket);
      expect(events).toHaveLength(2);
      expect(events[0]).toMatchObject({
        kind: "agent_lifecycle",
        topic: "sys.fleet.status",
        seq: 1,
        lifecycle: "resync",
        data: { reason: "subscribe", source: "members" },
      });
      expect(events[1]).toMatchObject({
        kind: "agent_lifecycle",
        topic: "sys.fleet.status",
        seq: 2,
        lifecycle: "status",
        data: { presence: "online", paused: false },
      });
    });
    expect(durableSince).not.toHaveBeenCalled();

    source.close();
    control.close();
    await control.closed;
    expect(unsubscribe).toHaveBeenCalled();
  });

  it("fails a sys.fleet.status subscribe loudly when the daemon push source is unavailable", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws"),
      {
        observe: async () => new Response(source.stream, { status: 200 }),
        daemonFleetStatusEvents: null,
        developerEventPollMs: 10_000,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.fleet.status",
      afterSeq: 0,
    }));

    await vi.waitFor(() => {
      const errors = socket.sent
        .map((raw) => JSON.parse(raw))
        .filter((message) => message.t === "subscribe.err");
      expect(errors).toHaveLength(1);
      expect(errors[0]).toMatchObject({
        topic: "sys.fleet.status",
        error: "fleet status push unavailable on this gateway",
      });
    });

    source.close();
    control.close();
    await control.closed;
  });

  it("smokes disposable-session daemon tool-call events with socket input and no durable event source", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const durableSince = vi.fn(async () => {
      throw new Error("tool-call topics must not poll durable developer_events");
    });
    let handlers: ToolCallEventHandlers | undefined;
    const sessionInput = vi.fn(async () => new Response(
      JSON.stringify({
        ok: true,
        receipt: {
          commandId: "cmd_smoke",
          clientMessageId: "cm_smoke",
          sessionId: "s_smoke",
          state: "queued",
          revision: 1,
          seq: 9,
        },
      }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=smoke-agent"),
      {
        observe: async () => new Response(source.stream, { status: 200 }),
        sessionInput,
        developerEvents: { since: durableSince },
        daemonToolCallEvents: {
          subscribe(topic: string, afterSeq: number, h: ToolCallEventHandlers) {
            expect(topic).toBe("sys.agent.smoke-agent.tool_call");
            expect(afterSeq).toBe(0);
            handlers = h;
            return () => {};
          },
        },
        developerEventPollMs: 10_000,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.smoke-agent.tool_call",
      afterSeq: 0,
    }));
    socket.emit("message", JSON.stringify({
      t: "input",
      mode: "session",
      target: { name: "smoke-agent", agentId: "a_smoke" },
      text: "R1.6 disposable WS smoke",
      clientMessageId: "cm_smoke",
    }));
    handlers?.onEvent({
      kind: "tool_call",
      topic: "sys.agent.smoke-agent.tool_call",
      seq: 1,
      ts: 1783300000000,
      agent: "smoke-agent",
      sessionId: "s_smoke",
      tool: "Bash",
      phase: "pre",
      ok: true,
    });
    handlers?.onEvent({
      kind: "tool_call",
      topic: "sys.agent.smoke-agent.tool_call",
      seq: 2,
      ts: 1783300000001,
      agent: "smoke-agent",
      sessionId: "s_smoke",
      tool: "Bash",
      phase: "post",
      ok: false,
    });
    source.enqueue("data: {\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"tc-smoke\",\"toolCallName\":\"Bash\"}\n\n");
    source.enqueue("data: {\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"tc-smoke\",\"content\":\"boom\",\"status\":\"failed\"}\n\n");
    source.enqueue("data: {\"type\":\"RUN_FINISHED\",\"runId\":\"r-smoke\"}\n\n");

    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "input.ack",
        commandId: "cmd_smoke",
        clientMessageId: "cm_smoke",
        sessionId: "s_smoke",
        state: "queued",
        revision: 1,
        seq: 9,
      }));
      expect(socket.sent).toContain("{\"type\":\"RUN_FINISHED\",\"runId\":\"r-smoke\"}");
      expect(developerEvents(socket)).toEqual([
        {
          kind: "tool_call",
          topic: "sys.agent.smoke-agent.tool_call",
          seq: 1,
          ts: 1783300000000,
          agent: "smoke-agent",
          sessionId: "s_smoke",
          tool: "Bash",
          phase: "pre",
          ok: true,
        },
        {
          kind: "tool_call",
          topic: "sys.agent.smoke-agent.tool_call",
          seq: 2,
          ts: 1783300000001,
          agent: "smoke-agent",
          sessionId: "s_smoke",
          tool: "Bash",
          phase: "post",
          ok: false,
        },
      ]);
    });
    expect(sessionInput).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "session",
        target: { name: "smoke-agent", agentId: "a_smoke" },
        text: "R1.6 disposable WS smoke",
        clientMessageId: "cm_smoke",
      }),
      expect.any(Request),
    );
    expect(durableSince).not.toHaveBeenCalled();

    source.close();
    control.close();
    await control.closed;
  });

  it("surfaces daemon-origin ephemeral gaps while keeping the socket open", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    let handlers: ToolCallEventHandlers | undefined;

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () => new Response(textStream([]), { status: 200 }),
        daemonToolCallEvents: {
          subscribe(_topic: string, _afterSeq: number, h: ToolCallEventHandlers) {
            handlers = h;
            return () => {};
          },
        },
        developerEventPollMs: 10_000,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 17,
    }));
    handlers?.onGap?.({
      t: "gap",
      lane: "developer_event",
      sessionId: "s_otto",
      afterId: 17,
      retry: "ephemeral",
    });

    expect(socket.sent).toContain(JSON.stringify({
      t: "subscribe.gap",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 17,
      retry: "ephemeral",
    }));

    control.close();
    await control.closed;
  });

  it("replays ephemeral tool-call events from the socket-local ring on late subscribe", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () => new Response(source.stream, { status: 200 }),
        daemonToolCallEvents: null,
        developerEventPollMs: 10_000,
      },
    );

    source.enqueue("data: {\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"tc2\",\"toolCallName\":\"Bash\"}\n\n");
    source.enqueue("data: {\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"tc2\",\"content\":\"boom\",\"status\":\"failed\"}\n\n");
    await vi.waitFor(() => {
      expect(socket.sent).toContain("{\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"tc2\",\"content\":\"boom\",\"status\":\"failed\"}");
    });

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 0,
    }));

    await vi.waitFor(() => {
      const events = developerEvents(socket);
      expect(events).toHaveLength(2);
      expect(events[1]).toMatchObject({
        kind: "tool_call",
        topic: "sys.agent.otto.tool_call",
        seq: 2,
        agent: "otto",
        tool: "Bash",
        phase: "post",
        ok: false,
      });
    });

    source.close();
    control.close();
    await control.closed;
  });

  it("evicts cached tool names after the terminal post event is built", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () => new Response(source.stream, { status: 200 }),
        daemonToolCallEvents: null,
        developerEventPollMs: 10_000,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 0,
    }));

    source.enqueue("data: {\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"tc3\",\"toolCallName\":\"Read\"}\n\n");
    source.enqueue("data: {\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"tc3\",\"content\":\"done\",\"status\":\"completed\"}\n\n");
    source.enqueue("data: {\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"tc3\",\"content\":\"late\",\"status\":\"completed\"}\n\n");

    await vi.waitFor(() => {
      const events = developerEvents(socket);
      expect(events).toHaveLength(3);
      expect(events[1]).toMatchObject({ phase: "post", tool: "Read" });
      expect(events[2]).toMatchObject({ phase: "post", tool: "tool" });
    });

    source.close();
    control.close();
    await control.closed;
  });

  it("caps cached names for starts that never complete", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () => new Response(source.stream, { status: 200 }),
        daemonToolCallEvents: null,
        developerEventPollMs: 10_000,
      },
    );

    for (let i = 0; i < 1025; i += 1) {
      source.enqueue(
        `data: {"type":"TOOL_CALL_START","toolCallId":"tc${i}","toolCallName":"Tool${i}"}\n\n`,
      );
    }
    await vi.waitFor(() => {
      expect(socket.sent).toContain("{\"type\":\"TOOL_CALL_START\",\"toolCallId\":\"tc1024\",\"toolCallName\":\"Tool1024\"}");
    });

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 0,
    }));
    source.enqueue("data: {\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"tc0\",\"content\":\"old\",\"status\":\"completed\"}\n\n");
    source.enqueue("data: {\"type\":\"TOOL_CALL_RESULT\",\"toolCallId\":\"tc1024\",\"content\":\"new\",\"status\":\"completed\"}\n\n");

    await vi.waitFor(() => {
      const events = developerEvents(socket);
      expect(events.at(-2)).toMatchObject({ phase: "post", tool: "tool" });
      expect(events.at(-1)).toMatchObject({ phase: "post", tool: "Tool1024" });
    });

    source.close();
    control.close();
    await control.closed;
  });

  it("rejects tool-call subscriptions for a different observed session", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const since = vi.fn(async () => []);

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      daemonToolCallEvents: null,
      developerEvents: { since },
    });

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.iris.tool_call",
      afterSeq: 0,
    }));

    expect(socket.sent).toContain(JSON.stringify({
      t: "subscribe.err",
      topic: "sys.agent.iris.tool_call",
      error: "tool_call subscriptions require matching ?session=iris",
    }));
    expect(since).not.toHaveBeenCalled();
  });

  it("rejects non-system developer event subscriptions", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const since = vi.fn(async () => []);

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      developerEvents: { since },
    });

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "thread.ops",
      afterSeq: 0,
    }));

    expect(socket.sent).toContain(JSON.stringify({
      t: "subscribe.err",
      error: "subscribe.topic must be a sys.* topic",
    }));
    expect(since).not.toHaveBeenCalled();
  });

  it("shares one durable queue watcher across mounted lanes and filters by session", async () => {
    const { CommandQueueHub } = await loadWs();
    let eventPolls = 0;
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      const target = url.searchParams.get("name");
      if (target) {
        return Response.json({
          target,
          sessionId: target === "otto" ? "s_otto" : "s_morgan",
          turnActive: true,
          steerCapability: "native_steer",
          seq: 5,
          revision: 5,
          commands: [],
        });
      }
      eventPolls += 1;
      return Response.json({
        events: [
          {
            seq: 6,
            sessionId: "s_otto",
            commandId: "cmd_otto",
            state: "claimed",
            mode: "queue",
            revision: 2,
          },
          {
            seq: 7,
            sessionId: "s_morgan",
            commandId: "cmd_morgan",
            state: "started",
            mode: "queue",
            revision: 3,
          },
        ],
        latestSeq: 7,
        gap: false,
      });
    });
    const hub = new CommandQueueHub({
      fetchHandler,
      commandQueueEventPollMs: 10_000,
    });
    const otto: unknown[] = [];
    const morgan: unknown[] = [];
    const offOtto = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      { onSnapshot() {}, onTransition: (event) => otto.push(event), onError: vi.fn() },
    );
    const offMorgan = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=morgan"),
      "morgan",
      { onSnapshot() {}, onTransition: (event) => morgan.push(event), onError: vi.fn() },
    );

    await vi.waitFor(() => {
      expect(otto).toHaveLength(1);
      expect(morgan).toHaveLength(1);
    });
    expect(eventPolls).toBe(1);
    expect(otto[0]).toMatchObject({ seq: 6, sessionId: "s_otto", state: "claimed" });
    expect(morgan[0]).toMatchObject({ seq: 7, sessionId: "s_morgan", state: "started" });
    offOtto();
    offMorgan();
  });

  it("coalesces queued-event redraws into one authoritative snapshot for every mounted client", async () => {
    const { CommandQueueHub } = await loadWs();
    let eventPolls = 0;
    let snapshotFetches = 0;
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        snapshotFetches += 1;
        const refreshed = eventPolls > 0;
        return Response.json({
          target: "otto",
          sessionId: "s_otto",
          turnActive: true,
          steerCapability: "native_steer",
          seq: refreshed ? 7 : 5,
          revision: refreshed ? 7 : 5,
          commands: refreshed
            ? [
                { commandId: "cmd_b", text: "edited B", state: "queued", revision: 2, seq: 7 },
                { commandId: "cmd_a", text: "A", state: "queued", revision: 2, seq: 6 },
              ]
            : [
                { commandId: "cmd_a", text: "A", state: "queued", revision: 1, seq: 4 },
                { commandId: "cmd_b", text: "B", state: "queued", revision: 1, seq: 5 },
              ],
        });
      }
      eventPolls += 1;
      return Response.json({
        events: [
          {
            seq: 6,
            sessionId: "s_otto",
            commandId: "cmd_a",
            state: "queued",
            mode: "queue",
            revision: 2,
          },
          {
            seq: 7,
            sessionId: "s_otto",
            commandId: "cmd_b",
            state: "queued",
            mode: "queue",
            revision: 2,
          },
        ],
        latestSeq: 7,
        gap: false,
      });
    });
    const hub = new CommandQueueHub({
      fetchHandler,
      commandQueueEventPollMs: 10_000,
    });
    const firstSnapshots: Array<Record<string, unknown>> = [];
    const secondSnapshots: Array<Record<string, unknown>> = [];
    const firstTransitions: unknown[] = [];
    const secondTransitions: unknown[] = [];
    const request = () => new Request("http://localhost/api/agui/ws?session=otto");
    const firstOff = hub.subscribe(request(), "otto", {
      onSnapshot: (snapshot) => firstSnapshots.push(snapshot as Record<string, unknown>),
      onTransition: (event) => firstTransitions.push(event),
      onError: vi.fn(),
    });
    const secondOff = hub.subscribe(request(), "otto", {
      onSnapshot: (snapshot) => secondSnapshots.push(snapshot as Record<string, unknown>),
      onTransition: (event) => secondTransitions.push(event),
      onError: vi.fn(),
    });

    await vi.waitFor(() => {
      expect(firstSnapshots).toHaveLength(2);
      expect(secondSnapshots).toHaveLength(2);
    });
    expect(eventPolls).toBe(1);
    expect(snapshotFetches).toBe(3); // two mount snapshots + one shared event-driven redraw
    expect(firstTransitions).toHaveLength(2);
    expect(secondTransitions).toHaveLength(2);
    expect(firstSnapshots.at(-1)?.commands).toEqual([
      { commandId: "cmd_b", text: "edited B", state: "queued", revision: 2, seq: 7 },
      { commandId: "cmd_a", text: "A", state: "queued", revision: 2, seq: 6 },
    ]);
    expect(secondSnapshots.at(-1)?.commands).toEqual(firstSnapshots.at(-1)?.commands);
    firstOff();
    secondOff();
  });

  it("rehydrates a queue snapshot when retained transition history has a gap", async () => {
    const { CommandQueueHub } = await loadWs();
    let snapshotsServed = 0;
    let eventPolls = 0;
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        snapshotsServed += 1;
        const seq = snapshotsServed === 1 ? 5 : 9;
        return Response.json({
          target: "otto",
          sessionId: "s_otto",
          turnActive: false,
          steerCapability: "native_steer",
          seq,
          revision: seq,
          commands: [{ commandId: "cmd_1", state: "queued", revision: seq, seq }],
        });
      }
      eventPolls += 1;
      return Response.json({ events: [], nextSeq: 9, latestSeq: 9, gap: true });
    });
    const hub = new CommandQueueHub({
      fetchHandler,
      commandQueueEventPollMs: 10_000,
    });
    const snapshots: Array<Record<string, unknown>> = [];
    const transitions: unknown[] = [];
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      {
        onSnapshot: (snapshot) => snapshots.push(snapshot as Record<string, unknown>),
        onTransition: (event) => transitions.push(event),
        onError: vi.fn(),
      },
    );

    await vi.waitFor(() => expect(snapshots).toHaveLength(2));
    expect(eventPolls).toBe(1);
    expect(snapshots.map((snapshot) => snapshot.seq)).toEqual([5, 9]);
    expect(transitions).toEqual([]);
    unsubscribe();
  });

  it("maps observe auth failures to websocket close codes", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();

    const control = handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () => new Response(JSON.stringify({ error: "not logged in" }), {
        status: 401,
        headers: { "content-type": "application/json" },
      }),
    });
    await control.closed;

    expect(socket.closed).toEqual({ code: 4401, reason: "not logged in" });
  });
});

describe("AG-UI WebSocket command surface (SPEC-ws-full-command-access v1)", () => {
  function frames(socket: FakeSocket) {
    return socket.sent.map((raw) => JSON.parse(raw) as Record<string, unknown>);
  }

  async function openSocket(deps: Record<string, unknown>) {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () => new Response(textStream([]), { status: 200 }),
      ...deps,
    });
    return socket;
  }

  const claudeHarness = async () => "claude";

  it("answers commands.list with the full claude catalog", async () => {
    const socket = await openSocket({ resolveHarness: claudeHarness });

    socket.emit("message", JSON.stringify({ t: "commands.list", target: "otto" }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "commands.catalog")).toBe(true);
    });

    const catalog = frames(socket).find((f) => f.t === "commands.catalog")!;
    expect(catalog).toMatchObject({
      target: "otto",
      harness: "claude",
      catalogVersion: "v1",
    });
    const names = (catalog.commands as Array<{ name: string; class: string }>).map(
      (c) => [c.name, c.class],
    );
    expect(names).toEqual([
      ["compact", "gateway"],
      ["model", "harness"],
      ["clear", "harness"],
    ]);
  });

  it("answers commands.list for a non-claude harness with gateway verbs only", async () => {
    const socket = await openSocket({ resolveHarness: async () => "codex" });

    socket.emit("message", JSON.stringify({ t: "commands.list", target: "otto" }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "commands.catalog")).toBe(true);
    });

    const catalog = frames(socket).find((f) => f.t === "commands.catalog")!;
    expect(catalog).toMatchObject({ harness: "codex", catalogVersion: "v1" });
    expect((catalog.commands as Array<{ name: string }>).map((c) => c.name)).toEqual([
      "compact",
    ]);
  });

  it("defaults commands.list to the observed session", async () => {
    const resolveHarness = vi.fn(claudeHarness);
    const socket = await openSocket({ resolveHarness });

    socket.emit("message", JSON.stringify({ t: "commands.list" }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "commands.catalog")).toBe(true);
    });

    expect(resolveHarness).toHaveBeenCalledWith("otto", expect.any(Request));
    expect(frames(socket).find((f) => f.t === "commands.catalog")).toMatchObject({
      target: "otto",
    });
  });

  it("dispatches a gateway verb to its REST handler and replies ack then done", async () => {
    const fetchHandler = vi.fn(async (request: Request) => {
      expect(new URL(request.url).pathname).toBe("/api/conversation/compact");
      expect(await request.json()).toEqual({ name: "otto" });
      return new Response(JSON.stringify({ ok: true }), { status: 201 });
    });
    const socket = await openSocket({ resolveHarness: claudeHarness, fetchHandler });

    socket.emit("message", JSON.stringify({
      t: "command",
      name: "compact",
      target: "otto",
      clientCommandId: "cc_1",
    }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "command.done")).toBe(true);
    });

    expect(fetchHandler).toHaveBeenCalledTimes(1);
    const replies = frames(socket).filter((f) => f.t?.toString().startsWith("command."));
    expect(replies).toEqual([
      { t: "command.ack", clientCommandId: "cc_1" },
      { t: "command.done", clientCommandId: "cc_1", ok: true },
    ]);
  });

  it("renders a harness command to slash text and injects it exactly once via the prompt ingress", async () => {
    const sessionInput = vi.fn(async () => new Response(
      JSON.stringify({
        ok: true,
        receipt: {
          commandId: "cmd_slash",
          clientMessageId: "cm_slash",
          sessionId: "s_otto",
          state: "queued",
          revision: 1,
          seq: 10,
        },
      }),
      { status: 201 },
    ));
    const socket = await openSocket({ resolveHarness: claudeHarness, sessionInput });

    socket.emit("message", JSON.stringify({
      t: "command",
      name: "model",
      target: "otto",
      clientCommandId: "cc_model",
      args: { model: "opus-4-8" },
    }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "command.done")).toBe(true);
    });

    // The injection-once pin: ONE rendered send, no passthrough duplicate.
    expect(sessionInput).toHaveBeenCalledTimes(1);
    expect(sessionInput).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "session",
        target: "otto",
        text: "/model opus-4-8",
        clientMessageId: "cc_model",
      }),
      expect.any(Request),
    );
    expect(frames(socket)).toContainEqual({ t: "command.ack", clientCommandId: "cc_model" });
    expect(frames(socket)).toContainEqual({
      t: "command.done",
      clientCommandId: "cc_model",
      ok: true,
    });
  });

  it("replays the terminal frames for a duplicate clientCommandId without re-dispatching", async () => {
    const sessionInput = vi.fn(async () => new Response(
      JSON.stringify({ ok: true }),
      { status: 201 },
    ));
    const socket = await openSocket({ resolveHarness: claudeHarness, sessionInput });
    const send = JSON.stringify({
      t: "command",
      name: "clear",
      target: "otto",
      clientCommandId: "cc_dup",
    });

    socket.emit("message", send);
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "command.done")).toBe(true);
    });
    socket.emit("message", send);
    await vi.waitFor(() => {
      expect(frames(socket).filter((f) => f.t === "command.done")).toHaveLength(2);
    });

    expect(sessionInput).toHaveBeenCalledTimes(1);
    expect(frames(socket).filter((f) => f.t === "command.ack")).toHaveLength(2);
  });

  it("rejects an unknown command name loudly", async () => {
    const sessionInput = vi.fn();
    const socket = await openSocket({ resolveHarness: claudeHarness, sessionInput });

    socket.emit("message", JSON.stringify({
      t: "command",
      name: "teleport",
      target: "otto",
      clientCommandId: "cc_unknown",
    }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "command.err")).toBe(true);
    });

    expect(frames(socket)).toContainEqual({
      t: "command.err",
      clientCommandId: "cc_unknown",
      error: "unknown command: teleport",
      code: "unknown_command",
    });
    expect(sessionInput).not.toHaveBeenCalled();
  });

  it("rejects a harness command for a harness that does not advertise it", async () => {
    const sessionInput = vi.fn();
    const socket = await openSocket({ resolveHarness: async () => "codex", sessionInput });

    socket.emit("message", JSON.stringify({
      t: "command",
      name: "model",
      target: "otto",
      clientCommandId: "cc_codex_model",
      args: { model: "gpt-5.5" },
    }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "command.err")).toBe(true);
    });

    expect(frames(socket)).toContainEqual({
      t: "command.err",
      clientCommandId: "cc_codex_model",
      error: "unknown command: model",
      code: "unknown_command",
    });
    expect(sessionInput).not.toHaveBeenCalled();
  });

  it("validates args against the descriptor before any dispatch", async () => {
    const sessionInput = vi.fn();
    const socket = await openSocket({ resolveHarness: claudeHarness, sessionInput });

    socket.emit("message", JSON.stringify({
      t: "command",
      name: "model",
      target: "otto",
      clientCommandId: "cc_bad_args",
      args: { modle: "opus-4-8" },
    }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "command.err")).toBe(true);
    });

    expect(frames(socket)).toContainEqual({
      t: "command.err",
      clientCommandId: "cc_bad_args",
      error: "unknown arg: modle",
      code: "invalid_args",
    });
    expect(sessionInput).not.toHaveBeenCalled();
  });

  it("requires clientCommandId on command frames", async () => {
    const sessionInput = vi.fn();
    const socket = await openSocket({ resolveHarness: claudeHarness, sessionInput });

    socket.emit("message", JSON.stringify({ t: "command", name: "clear", target: "otto" }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "command.err")).toBe(true);
    });

    expect(frames(socket)).toContainEqual({
      t: "command.err",
      error: "command.clientCommandId is required",
      code: "invalid_args",
    });
    expect(sessionInput).not.toHaveBeenCalled();
  });

  it("maps auth failures from the dispatch handler to done ok=false code unauthorized", async () => {
    const socket = await openSocket({
      resolveHarness: claudeHarness,
      fetchHandler: async () => new Response(
        JSON.stringify({ error: "not logged in" }),
        { status: 401, headers: { "content-type": "application/json" } },
      ),
    });

    socket.emit("message", JSON.stringify({
      t: "command",
      name: "compact",
      target: "otto",
      clientCommandId: "cc_noauth",
    }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "command.done")).toBe(true);
    });

    // Accepted (validation passed) but the dispatch was rejected: ack then
    // done ok=false — err is reserved for pre-acceptance failures.
    expect(frames(socket)).toContainEqual({
      t: "command.ack",
      clientCommandId: "cc_noauth",
    });
    expect(frames(socket)).toContainEqual({
      t: "command.done",
      clientCommandId: "cc_noauth",
      ok: false,
      error: "not logged in",
      code: "unauthorized",
    });
  });

  it("sends ack at acceptance, before the dispatch resolves", async () => {
    let releaseDispatch: (() => void) | undefined;
    const dispatchGate = new Promise<void>((resolve) => {
      releaseDispatch = resolve;
    });
    const fetchHandler = vi.fn(async () => {
      await dispatchGate;
      return new Response(JSON.stringify({ ok: true }), { status: 201 });
    });
    const socket = await openSocket({ resolveHarness: claudeHarness, fetchHandler });

    socket.emit("message", JSON.stringify({
      t: "command",
      name: "compact",
      target: "otto",
      clientCommandId: "cc_early_ack",
    }));
    await vi.waitFor(() => {
      expect(frames(socket)).toContainEqual({
        t: "command.ack",
        clientCommandId: "cc_early_ack",
      });
    });
    expect(frames(socket).some((f) => f.t === "command.done")).toBe(false);

    releaseDispatch?.();
    await vi.waitFor(() => {
      expect(frames(socket)).toContainEqual({
        t: "command.done",
        clientCommandId: "cc_early_ack",
        ok: true,
      });
    });
  });

  it("destroys wrong-path upgrade sockets instead of leaving clients hanging", async () => {
    const { attachAguiWsUpgrade } = await loadWs();
    const server = new EventEmitter();
    await attachAguiWsUpgrade(server, {});

    const destroy = vi.fn();
    server.emit(
      "upgrade",
      { url: "/definitely/not/agui", headers: { host: "localhost" } },
      { destroy },
      Buffer.alloc(0),
    );

    expect(destroy).toHaveBeenCalledTimes(1);
  });

  it("keeps plain input as pure passthrough — slash text is never parsed server-side", async () => {
    const sessionInput = vi.fn(async () => new Response(
      JSON.stringify({
        ok: true,
        receipt: {
          commandId: "cmd_slash",
          clientMessageId: "cm_slash",
          sessionId: "s_otto",
          state: "queued",
          revision: 1,
          seq: 10,
        },
      }),
      { status: 201 },
    ));
    const socket = await openSocket({ resolveHarness: claudeHarness, sessionInput });

    socket.emit("message", JSON.stringify({
      t: "input",
      mode: "session",
      target: "otto",
      text: "/model opus-4-8",
      clientMessageId: "cm_slash",
    }));
    await vi.waitFor(() => {
      expect(frames(socket).some((f) => f.t === "input.ack")).toBe(true);
    });

    expect(sessionInput).toHaveBeenCalledTimes(1);
    expect(sessionInput).toHaveBeenCalledWith(
      expect.objectContaining({ text: "/model opus-4-8" }),
      expect.any(Request),
    );
    expect(frames(socket).some((f) => f.t === "command.ack")).toBe(false);
  });
});

function developerEvents(socket: FakeSocket) {
  return socket.sent
    .map((raw) => {
      try {
        return JSON.parse(raw);
      } catch {
        return undefined;
      }
    })
    .filter((message) => message?.type === "developer.event")
    .map((message) => message.event);
}

describe("dedicated events lane (no observe target)", () => {
  it("keeps a bare no-target socket open and serves subscribe frames", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const observe = vi.fn(async () => new Response(textStream([]), { status: 200 }));
    handleWs(socket, new Request("http://localhost/api/agui/ws"), {
      observe,
      daemonFleetStatusEvents: { subscribe: () => () => {} },
    });

    socket.emit(
      "message",
      JSON.stringify({ t: "subscribe", topic: "sys.fleet.status", afterSeq: 0 }),
    );
    await Promise.resolve();

    // The events lane never opens the observe stream and never closes the socket.
    expect(observe).not.toHaveBeenCalled();
    expect(socket.closed).toBeFalsy();
    const ack = socket.sent
      .map((raw: string) => JSON.parse(raw))
      .find((f: { t: string }) => f.t === "subscribe.ack");
    expect(ack?.topic).toBe("sys.fleet.status");
  });
});
