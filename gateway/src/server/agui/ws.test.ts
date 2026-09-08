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

function boundSessionResponse(
  stream: ReadableStream<Uint8Array>,
  binding: { sessionId: string; agentId: string; name: string } = {
    sessionId: "s_real",
    agentId: "a_real",
    name: "current-name",
  },
): Response {
  return new Response(stream, {
    status: 200,
    headers: {
      "content-type": "text/event-stream",
      "x-nexus-session-id": binding.sessionId,
      "x-nexus-agent-id": binding.agentId,
      "x-nexus-agent-name": binding.name,
    },
  });
}

async function loadWs(): Promise<WsModule> {
  return import("./ws.mjs") as Promise<WsModule>;
}

describe("AG-UI WebSocket server transport", () => {
  it("refreshes same-cursor native activity once per shared exact lane at elapsed cadence", async () => {
    const { CommandQueueHub } = await loadWs();
    vi.useFakeTimers();
    let reads = 0;
    let open = true;
    const snapshots: any[][] = [[], []];
    const hub = new CommandQueueHub({
      commandQueueEventPollMs: 10,
      commandQueueObservationPollMs: 100,
      fetchHandler: async (request: Request) => {
        const url = new URL(request.url);
        if (url.searchParams.has("eventsAfter")) return Response.json({events:[], latestSeq:0, nextSeq:0});
        reads++;
        return Response.json({sessionId:"s_otto", seq:0, revision:0, commands:[], observation:{sessionId:"s_otto", state:open ? "nativeOpen" : "verifiedIdle", owner:"A", revision:open ? 1 : 2, steerCapability:"none"}});
      },
    } as any);
    const off = snapshots.map((events) => hub.subscribe(new Request("http://localhost/api/agui/ws"),
      {agentId:"a_otto", expectedSessionId:"s_otto"},
      {onSnapshot:(snapshot: unknown) => events.push(snapshot), onTransition:vi.fn(), onError:vi.fn()}));
    try {
      await vi.advanceTimersByTimeAsync(1);
      const initial = reads;
      open = false;
      await vi.advanceTimersByTimeAsync(99);
      expect(reads).toBe(initial + 1);
      expect(snapshots.map((events) => events.at(-1)?.observation.state)).toEqual(["verifiedIdle", "verifiedIdle"]);
      await vi.advanceTimersByTimeAsync(90);
      expect(reads).toBe(initial + 1);
    } finally { off.forEach((stop) => stop()); vi.useRealTimers(); }
  });

  it("discards a superseded hydrate observation but retains independently valid queue facts", async () => {
    const {CommandQueueHub} = await loadWs();
    let finishOld!: (response:Response) => void;
    let reads = 0;
    const first: any[] = [];
    const second: any[] = [];
    const hub = new CommandQueueHub({fetchHandler:async (request:Request) => {
      if (new URL(request.url).searchParams.has("eventsAfter")) return Response.json({events:[], latestSeq:4});
      if (++reads === 1) return new Promise<Response>((done) => {finishOld = done;});
      return Response.json({sessionId:"s_otto", seq:4, commands:[], observation:{sessionId:"s_otto", state:"nativeOpen", owner:"NEW", revision:1, steerCapability:"none"}});
    }});
    const request = new Request("http://localhost/api/agui/ws");
    const target = {agentId:"a_otto", expectedSessionId:"s_otto"};
    const handlers = (out:any[]) => ({onSnapshot:(value:unknown) => out.push(value), onTransition:vi.fn(), onError:vi.fn()});
    const offA = hub.subscribe(request, target, handlers(first));
    const offB = hub.subscribe(request, target, handlers(second));
    try {
      await vi.waitFor(() => expect(second).toHaveLength(1));
      finishOld(Response.json({sessionId:"s_otto", seq:3, commands:[{commandId:"old-queue-fact"}], observation:{sessionId:"s_otto", state:"verifiedIdle", owner:"OLD", revision:99, steerCapability:"none"}}));
      await vi.waitFor(() => expect(first).toHaveLength(1));
      expect(first[0].commands[0].commandId).toBe("old-queue-fact");
      expect(first[0].observation).toMatchObject({owner:"NEW", state:"nativeOpen"});
    } finally {offA(); offB();}
  });

  it("bounds observation-only retries while empty filtered pages catch up immediately", async () => {
    const {CommandQueueHub} = await loadWs();
    vi.useFakeTimers();
    let reads = 0;
    let polls = 0;
    const errors = vi.fn();
    const hub = new CommandQueueHub({commandQueueEventPollMs:10, commandQueueObservationPollMs:100,
      fetchHandler:async (request:Request) => {
        const url = new URL(request.url);
        if (url.searchParams.has("eventsAfter")) {
          polls++;
          return Response.json({events:[], nextSeq:polls, latestSeq:1000});
        }
        if (++reads > 1) return new Response("unavailable", {status:503});
        return Response.json({sessionId:"S1", seq:0, commands:[], observation:{sessionId:"S1", state:"nativeOpen", owner:"A", revision:1, steerCapability:"none"}});
      }});
    const snapshots:any[] = [];
    const off = hub.subscribe(new Request("http://localhost/api/agui/ws"), {agentId:"A",expectedSessionId:"S1"},
      {onSnapshot:(value:unknown) => snapshots.push(value), onTransition:vi.fn(), onError:errors});
    try {
      await vi.advanceTimersByTimeAsync(250);
      expect(polls).toBeGreaterThan(100);
      expect(reads).toBe(3);
      expect(errors).toHaveBeenCalled();
      expect(snapshots).toHaveLength(1);
      expect(snapshots[0].observation.state).toBe("nativeOpen");
    } finally {off(); vi.useRealTimers();}
  });

  it.each(["new-owner", "ownerless-unknown", "equal-conflict"])("shares accepted native evidence across same-generation gap hydrations without losing queue facts: %s", async (next) => {
    const {CommandQueueHub} = await loadWs();
    let reads = 0;
    let polls = 0;
    let releaseOld!: (response:Response) => void;
    let sawNew!: () => void;
    let sawOldQueue!: () => void;
    const newDelivered = new Promise<void>((resolve) => {sawNew = resolve;});
    const oldQueueDelivered = new Promise<void>((resolve) => {sawOldQueue = resolve;});
    const outputs:any[][] = [[], []];
    const snapshot = (owner:string, state:string, seq:number, capability = "none") => Response.json({
      sessionId:"S1", seq, commands:[{commandId:`queue-${seq}`}],
      turnActive:state === "nativeOpen", steerCapability:capability,
      observation:{sessionId:"S1", owner, state, revision:1, steerCapability:capability},
    });
    const hub = new CommandQueueHub({commandQueueEventPollMs:10000, commandQueueObservationPollMs:10000,
      fetchHandler:async (request:Request) => {
        if (new URL(request.url).searchParams.has("eventsAfter")) return Response.json(++polls === 1
          ? {gap:true, latestSeq:4} : {events:[], nextSeq:4, latestSeq:4});
        if (++reads <= 2) return snapshot("BASE", "nativeOpen", 1);
        if (reads === 3) return new Promise<Response>((resolve) => {releaseOld = resolve;});
        if (next === "ownerless-unknown") return Response.json({
          sessionId:"S1", seq:4, commands:[], turnActive:false, steerCapability:"native_steer",
          observation:{sessionId:"S1", state:"unknown", steerCapability:"native_steer"},
        });
        if (next === "equal-conflict") return snapshot("BASE", "verifiedIdle", 4, "native_steer");
        return snapshot("NEW", "nativeOpen", 4, "native_steer");
      }});
    const stops = outputs.map((out) => hub.subscribe(new Request("http://localhost/api/agui/ws"),
      {agentId:"A", expectedSessionId:"S1"}, {
        onSnapshot:(value:any) => {
          out.push(value);
          if (value.seq === 4) sawNew();
          if (value.commands?.some((command:any) => command.commandId === "queue-3")) sawOldQueue();
        }, onTransition:vi.fn(), onError:vi.fn(),
      }));
    try {
      await newDelivered;
      releaseOld(snapshot("OLD", "verifiedIdle", 3));
      await oldQueueDelivered;
      expect(reads).toBe(4);
      for (const out of outputs) {
        expect(out.some((value) => value.observation?.owner === "OLD")).toBe(false);
        expect(out.at(-1)).toMatchObject({turnActive:true,
          steerCapability:next === "new-owner" ? "native_steer" : "none",
          observation:{owner:next === "new-owner" ? "NEW" : "BASE", state:"nativeOpen"}});
      }
      expect(outputs[0]!.at(-1)).toMatchObject({sessionId:"S1", seq:3, commands:[{commandId:"queue-3"}]});
    } finally {stops.forEach((stop) => stop());}
  });

  it("rejects a current S2 opener response before binding or pumping an exact S1 socket", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const fetchHandler = vi.fn();
    const control = handleWs(
      socket,
      new Request(
        "http://localhost/api/agui/ws?agentId=a_real&expectedSessionId=s_old",
      ),
      {
        observe: async () =>
          boundSessionResponse(
            textStream(['data: {"type":"RUN_FINISHED"}\n\n']),
          ),
        fetchHandler,
      },
    );
    await control.closed;
    expect(socket.sent).toEqual([]);
    expect(fetchHandler).not.toHaveBeenCalled();
    expect(socket.closed?.code).toBe(1011);
  });
  it.each(["queue.redirect", "queue.cancel", "queue.edit", "queue.reorder"].flatMap((t) =>
    ["", "dm=otto&agentId=a_real", "thread=work&agentId=a_real", "topic=updates&agentId=a_real"]
      .map((query) => ({ t, query })),
  ))("rejects $t on non-session socket $query before any queue fallback", async ({ t, query }) => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const effect = vi.fn(async () => Response.json({ sessionId: "s_real" }));
    const control = handleWs(socket, new Request(`http://localhost/api/agui/ws?${query}`), {
      observe: async () => new Response(source.stream),
      fetchHandler: effect,
    });
    socket.emit("message", JSON.stringify({
      t,
      agentId: "a_real",
      expectedSessionId: "s_real",
      clientMutationId: "mut",
      commandId: "cmd",
      expectedRevision: 1,
      text: "changed",
      commandIds: ["cmd"],
      expectedRevisions: [1],
    }));
    try {
      await vi.waitFor(() => expect(socket.sent.length).toBeGreaterThan(0));
      expect(effect).not.toHaveBeenCalled();
      expect(socket.sent.map((raw) => JSON.parse(raw))).toEqual([
        expect.objectContaining({ t: "queue.mutation.err", clientMutationId: "mut" }),
      ]);
    } finally {
      control.close();
      source.close();
    }
  });

  it.each(["input", "steer", "interrupt", "queue.redirect", "queue.cancel", "queue.edit", "queue.reorder", "command"])(
    "rejects partial nested identity for %s even with complete root identity",
    async (t) => {
      const { handleWs } = await loadWs();
      for (const target of [{ agentId: "a_real" }, { expectedSessionId: "s_real" }]) {
        const source = controlledTextStream();
        const socket = new FakeSocket();
        const effect = vi.fn(async () => Response.json({ result: { sessionId: "s_real" } }));
        const control = handleWs(socket, new Request("http://localhost/api/v1/agent-sessions/s_real/events"), {
          observe: async () => boundSessionResponse(source.stream),
          sessionInput: effect,
          steerInput: effect,
          interruptInput: effect,
          fetchHandler: effect,
          resolveHarness: async () => "claude",
        });
        socket.emit("message", JSON.stringify({
          t, target,
          agentId: "a_real", expectedSessionId: "s_real",
          text: "input", name: "clear",
          clientMessageId: "cm", clientCommandId: "cc", clientMutationId: "mut",
          commandId: "cmd", expectedRevision: 1,
        }));
        try {
          await vi.waitFor(() => expect(socket.sent.some((raw) => /\.(ack|err|done)$/.test(JSON.parse(raw).t))).toBe(true));
          expect(effect).not.toHaveBeenCalled();
          expect(socket.sent.some((raw) => JSON.parse(raw).t.endsWith(".err"))).toBe(true);
        } finally {
          control.close();
          source.close();
        }
      }
    },
  );

  it.each(["input", "steer", "interrupt", "command"])(
    "does not offer a name-only %s escape on an unbound socket",
    async (t) => {
      const { handleWs } = await loadWs();
      const socket = new FakeSocket();
      const effect = vi.fn(async () =>
        Response.json({ result: { sessionId: "s_real" } }),
      );
      const control = handleWs(
        socket,
        new Request("http://localhost/api/agui/ws"),
        {
          sessionInput: effect,
          steerInput: effect,
          interruptInput: effect,
          fetchHandler: effect,
          resolveHarness: async () => "claude",
        },
      );
      socket.emit(
        "message",
        JSON.stringify({
          t,
          mode: "session",
          target: "otto",
          text: "unsafe",
          name: "clear",
          clientMessageId: "cm",
          clientCommandId: "cc",
        }),
      );
      await vi.waitFor(() =>
        expect(
          socket.sent.some((raw) => JSON.parse(raw).t.endsWith(".err")),
        ).toBe(true),
      );
      expect(effect).not.toHaveBeenCalled();
      control.close();
    },
  );

  it.each(["compact", "clear"])(
    "checks actual structured %s response identity and validates identity before replay",
    async (name) => {
      const { handleWs } = await loadWs();
      for (const sessionId of ["s_real", "s_foreign", undefined]) {
        const source = controlledTextStream();
        const socket = new FakeSocket();
        const effect = vi.fn(async () =>
          Response.json(
            name === "compact"
              ? { result: { started: true, sessionId } }
              : {
                  receipt: {
                    commandId: "cmd",
                    state: "queued",
                    revision: 1,
                    seq: 1,
                    sessionId,
                  },
                },
          ),
        );
        const control = handleWs(
          socket,
          new Request("http://localhost/api/v1/agent-sessions/s_real/events"),
          {
            observe: async () => boundSessionResponse(source.stream),
            sessionInput: effect,
            fetchHandler: effect,
            resolveHarness: async () => "claude",
          },
        );
        const frame = {
          t: "command",
          name,
          agentId: "a_real",
          expectedSessionId: "s_real",
          clientCommandId: "cc",
        };
        socket.emit("message", JSON.stringify(frame));
        await vi.waitFor(() =>
          expect(
            socket.sent.some((raw) => JSON.parse(raw).t === "command.done"),
          ).toBe(true),
        );
        expect(
          socket.sent
            .map((raw) => JSON.parse(raw))
            .find((frame) => frame.t === "command.done")?.ok,
        ).toBe(sessionId === "s_real");
        socket.sent = [];
        socket.emit(
          "message",
          JSON.stringify({ ...frame, expectedSessionId: "s_other" }),
        );
        await vi.waitFor(() =>
          expect(
            socket.sent.some((raw) => JSON.parse(raw).t === "command.err"),
          ).toBe(true),
        );
        expect(effect).toHaveBeenCalledTimes(1);
        control.close();
        source.close();
      }
    },
  );

  it.each(["input", "steer"])(
    "does not drop unsupported intent from exact %s",
    async (t) => {
      const { handleWs } = await loadWs();
      for (const option of [
        { delivery: "auto" },
        { modelSelection: { modelId: "other" } },
      ]) {
        const source = controlledTextStream();
        const socket = new FakeSocket();
        const effect = vi.fn();
        const control = handleWs(
          socket,
          new Request("http://localhost/api/v1/agent-sessions/s_real/events"),
          {
            observe: async () => boundSessionResponse(source.stream),
            sessionInput: effect,
            steerInput: effect,
          },
        );
        socket.emit(
          "message",
          JSON.stringify({
            t,
            text: "do not downgrade",
            agentId: "a_real",
            expectedSessionId: "s_real",
            ...option,
          }),
        );
        await vi.waitFor(() =>
          expect(
            socket.sent.some((raw) =>
              JSON.parse(raw).error?.includes("not supported"),
            ),
          ).toBe(true),
        );
        expect(effect).not.toHaveBeenCalled();
        control.close();
        source.close();
      }
    },
  );
  it.each(["input", "steer", "interrupt", "queue.cancel", "command"])(
    "requires a complete exact identity for %s on session lanes",
    async (t) => {
      const { handleWs } = await loadWs();
      for (const identity of [
        {},
        { agentId: "a_real" },
        { expectedSessionId: "s_real" },
        { agentId: "a_real", expectedSessionId: null },
        { agentId: "a_real", expectedSessionId: "s_other" },
        { agentId: "a_real", expectedSessionId: "s_real", target: { agentId: "a_real", expectedSessionId: "s_other" } },
        { agentId: "a_real", expectedSessionId: "s_real", target: { agentId: "", name: "current-name" } },
      ]) {
        const source = controlledTextStream();
        const socket = new FakeSocket();
        const effect = vi.fn(
          async () =>
            new Response(JSON.stringify({ result: { sessionId: "s_real" } })),
        );
        const control = handleWs(
          socket,
          new Request("http://localhost/api/v1/agent-sessions/s_real/events"),
          {
            observe: async () => boundSessionResponse(source.stream),
            sessionInput: effect,
            steerInput: effect,
            interruptInput: effect,
            fetchHandler: effect,
            resolveHarness: async () => "claude",
          },
        );
        socket.emit(
          "message",
          JSON.stringify({
            t,
            text: "input",
            name: "clear",
            clientMessageId: "cm",
            clientCommandId: "cc",
            clientMutationId: "mut",
            commandId: "cmd",
            expectedRevision: 1,
            ...identity,
          }),
        );
        await vi.waitFor(() =>
          expect(
            socket.sent
              .map((raw) => JSON.parse(raw))
              .some((frame) => frame.t.endsWith(".err")),
          ).toBe(true),
        );
        expect(effect).not.toHaveBeenCalled();
        control.close();
        source.close();
      }
    },
  );

  it.each(["input", "steer", "interrupt", "queue.redirect", "queue.cancel", "queue.edit", "queue.reorder"])(
    "carries %s binding through the default HTTP handler and rejects foreign success",
    async (t) => {
      const { handleWs } = await loadWs();
      for (const sessionId of ["s_real", "s_foreign", undefined]) {
        const source = controlledTextStream();
        const socket = new FakeSocket();
        const bodies: unknown[] = [];
        const control = handleWs(
          socket,
          new Request("http://localhost/api/v1/agent-sessions/s_real/events"),
          {
            observe: async () => boundSessionResponse(source.stream),
            fetchHandler: async (request) => {
              bodies.push(await request.json());
              const result = {
                sessionId,
                commandId: "cmd",
                state: "queued",
                revision: 1,
                seq: 1,
              };
              return new Response(
                JSON.stringify(
                  t === "input"
                    ? { receipt: result }
                    : t.startsWith("queue.")
                      ? result
                      : { result },
                ),
              );
            },
          },
        );
        socket.emit(
          "message",
          JSON.stringify({
            t,
            agentId: "a_real",
            expectedSessionId: "s_real",
            text: "input",
            clientMessageId: "cm",
            clientMutationId: "mut",
            commandId: "cmd",
            expectedRevision: 1,
          }),
        );
        await vi.waitFor(() =>
          expect(
            socket.sent
              .map((raw) => JSON.parse(raw))
              .some((frame) => /\.(ack|err)$/.test(frame.t)),
          ).toBe(true),
        );
        expect(bodies).toEqual([
          expect.objectContaining({
            agentId: "a_real",
            expectedSessionId: "s_real",
          }),
        ]);
        const frames = socket.sent.map((raw) => JSON.parse(raw));
        expect(frames[0]).toMatchObject({
          t: "session.bound",
          agentId: "a_real",
          sessionId: "s_real",
        });
        expect(frames.some((frame) => frame.t.endsWith(".ack"))).toBe(
          sessionId === "s_real",
        );
        control.close();
        source.close();
      }
    },
  );

  it("does not bind a legacy session lane from an unproven name-only response", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () =>
          new Response(textStream(['data: {"type":"RUN_STARTED"}\n\n'])),
      },
    );
    await control.closed;
    expect(socket.sent).toEqual([]);
    expect(socket.closed?.code).toBe(1011);
  });
  it("pumps SSE data records as raw AG-UI JSON websocket messages", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const observe = vi.fn(
      async () =>
        new Response(
          textStream([
            ": nexus-open\n\n",'data: {"type":"RUN_STARTED","runId":"r1"}\n\n',
            'data: {"type":"RUN_FINISHED","runId":"r1"}\n\n',
          ]),
      { status: 200, headers: { "content-type": "text/event-stream",
              "x-nexus-agent-name": "otto",
              "x-nexus-agent-id": "a_otto",
              "x-nexus-session-id": "s_otto",
            } },
        ),
    );

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
      JSON.stringify({
        t: "session.bound",
        agentId: "a_otto",
        sessionId: "s_otto",
      }),
      '{"type":"RUN_STARTED","runId":"r1"}',
      '{"type":"RUN_FINISHED","runId":"r1"}',
    ]);
  });

  it("drops session keep-alive heartbeats instead of relaying them downstream", async () => {
    // `sessionEvents` emits `: ping` every 20s to defeat proxy idle timeouts.
    // Those comment frames must die at the relay: forwarding them would push
    // junk into every consumer of this socket.
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const observe = vi.fn(
      async () =>
        new Response(
          textStream([
            ": ping\n\n",'data: {"type":"RUN_STARTED","runId":"r1"}\n\n',
            ": ping\n\n",
            ": ping\n\n",
            'data: {"type":"RUN_FINISHED","runId":"r1"}\n\n',
            ": ping\n\n",
          ]),
      { status: 200, headers: { "content-type": "text/event-stream",
              "x-nexus-agent-name": "otto",
              "x-nexus-agent-id": "a_otto",
              "x-nexus-session-id": "s_otto",
            } },
        ),
    );

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      { observe },
    );
    await control.closed;
    expect(socket.sent).toEqual([
      JSON.stringify({
        t: "session.bound",
        agentId: "a_otto",
        sessionId: "s_otto",
      }),
      '{"type":"RUN_STARTED","runId":"r1"}',
      '{"type":"RUN_FINISHED","runId":"r1"}',
    ]);
    expect(socket.sent.join("")).not.toContain("ping");
  });

  it("handles ping envelopes without touching the observe stream", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    handleWs(socket, new Request("http://localhost/api/agui/ws?thread=ops"), {
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
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
          { status: 200, headers: { "content-type": "text/event-stream",
                "x-nexus-agent-name": "otto",
                "x-nexus-agent-id": "a_otto",
                "x-nexus-session-id": "s_otto",
              } },
        ),
      },
    );
    await control.closed;

    expect(socket.closed).toEqual({ code: 1013, reason: "session.bp:none" });
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
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
      sessionInput,
    });

    socket.emit(
      "message",
      JSON.stringify({
        t: "input",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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
        target: {
          name: "otto",
          agentId: "a_otto",
          expectedSessionId: "s_otto",
        },
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
        result: {
              sessionId: "s_otto",
              accepted: true, delivery: "steered", turnId: "turn_7" },
      }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
      sessionInput,
      steerInput,
    });

    socket.emit("message", JSON.stringify({
      t: "steer",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
        text: "inspect the failing assertion before continuing",
      clientMessageId: "cm_steer_1",
    }));
    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "steer.ack",
        clientMessageId: "cm_steer_1",
          sessionId: "s_otto",
          accepted: true,
        delivery: "steered",
        turnId: "turn_7",
      }));
    });

    expect(steerInput).toHaveBeenCalledWith(
      {
        target: {
          name: "otto",
          agentId: "a_otto",
          expectedSessionId: "s_otto",
        },
        text: "inspect the failing assertion before continuing",
        clientMessageId: "cm_steer_1",
      },
      expect.any(Request),
    );
    expect(sessionInput).not.toHaveBeenCalled();
  });

  it("routes explicit interrupt frames through the authenticated conversation interrupt route", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const requests: Request[] = [];

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto", {
      headers: { cookie: "nexus_human=human-token" },
    }), {
      observe: async () =>
          boundSessionResponse(textStream([]), {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
      fetchHandler: vi.fn(async (request: Request) => {
        requests.push(request);
        return Response.json({ ok: true, result: { sessionId: "s_otto", interrupted: true } }, { status: 201 });
      }),
    });

    socket.emit("message", JSON.stringify({
      t: "interrupt",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
        clientMessageId: "cm_interrupt_1",
    }));

    await vi.waitFor(() => expect(socket.sent).toContain(JSON.stringify({
      t: "interrupt.ack",
      clientMessageId: "cm_interrupt_1",
          sessionId: "s_otto",
          interrupted: true,
    })));
    expect(requests).toHaveLength(1);
    expect(new URL(requests[0]!.url).pathname).toBe("/api/conversation/interrupt");
    expect(requests[0]!.headers.get("cookie")).toBe("nexus_human=human-token");
    await expect(requests[0]!.json()).resolves.toEqual({
      name: "otto",
      agentId: "a_otto",
      expectedSessionId: "s_otto",
      clientMessageId: "cm_interrupt_1",
    });
  });

  it("forwards completed prompt, steer, interrupt, and compact events as caller-bound receipts", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    let onTransition: ((transition: Record<string, unknown>) => void) | undefined;
    const control = handleWs(
      socket,
      new Request("http://localhost/api/v1/agent-sessions/s_real/events?view=agui"),
      {
        observe: async () => boundSessionResponse(source.stream),
        commandQueueHub: {
          subscribe: (_request: Request, _target: unknown, handlers: {
            onTransition: (transition: Record<string, unknown>) => void;
          }) => {
            onTransition = handlers.onTransition;
            return () => {};
          },
        },
      },
    );
    await vi.waitFor(() => expect(onTransition).toBeTypeOf("function"));

    onTransition!({
      seq: 8,
      sessionId: "s_real",
      commandId: "cmd_started",
      clientMessageId: "cm_started",
      commandKind: "harness.prompt",
      callerName: "Operator",
      callerSessionId: "s_human",
      callerKind: "human",
      state: "started",
      mode: "queue",
      revision: 3,
    });
    onTransition!({
      seq: 9,
      sessionId: "s_real",
      commandId: "cmd_missing_caller",
      clientMessageId: "cm_missing_caller",
      commandKind: "harness.prompt",
      state: "completed",
      mode: "queue",
      revision: 4,
    });

    for (const [index, commandKind] of [
      "harness.prompt",
      "harness.steer",
      "harness.interrupt",
      "harness.compact",
    ].entries()) {
      onTransition!({
        seq: index + 10,
        sessionId: "s_real",
        commandId: `cmd_${index}`,
        clientMessageId: `cm_${index}`,
        commandKind,
        callerName: "Operator",
        callerSessionId: "s_human",
        callerPrincipalId: "h_operator",
        callerKind: "human",
        state: "completed",
        mode: commandKind.slice("harness.".length),
        revision: 4,
      });
    }

    expect(socket.sent.map((raw) => JSON.parse(raw)).filter((frame) => frame.t === "command.receipt"
    )).toEqual([
      "harness.prompt",
      "harness.steer",
      "harness.interrupt",
      "harness.compact",
    ].map((commandKind, index) => ({
      t: "command.receipt",
      commandKind,
      commandId: `cmd_${index}`,
      clientId: `cm_${index}`,
      sessionId: "s_real",
      state: "completed",
      revision: 4,
      seq: index + 10,
      callerId: "s_human",
      callerPrincipalId: "h_operator",
      callerKind: "human",
      callerName: "Operator",
      callerSessionId: "s_human",
    })));

    source.close();
    control.close();
    await control.closed;
  });

  it("defaults steer frames to the authenticated conversation steer route", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const requests: Request[] = [];

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto", {
      headers: { cookie: "nexus_human=human-token" },
    }), {
      observe: async () =>
          boundSessionResponse(textStream([]), {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
      fetchHandler: vi.fn(async (request: Request) => {
        requests.push(request);
        return new Response(
          JSON.stringify({ ok: true, result: {
                sessionId: "s_otto",
                accepted: true, delivery: "fallback_started" } }),
          { status: 201, headers: { "content-type": "application/json" } },
        );
      }),
    });

    socket.emit("message", JSON.stringify({
      t: "steer",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
        target: { name: "otto", agentId: "a_otto", expectedSessionId: "s_otto" },
      text: "use the smaller patch",
      clientMessageId: "cm_steer_2",
    }));
    await vi.waitFor(() => {
      expect(socket.sent).toContain(JSON.stringify({
        t: "steer.ack",
        clientMessageId: "cm_steer_2",
          sessionId: "s_otto",
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
      expectedSessionId: "s_otto",
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
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
      sessionInput,
      steerInput,
    });

    socket.emit("message", JSON.stringify({
      t: "steer",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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

  it("accepts an exact identity frame without redundant session mode or target", async () => {
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
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
      sessionInput,
    });

    // Exact identity is required even when mode and display target are omitted.
    socket.emit(
      "message",
      JSON.stringify({ t: "input",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
        text: "continue", clientMessageId: "cm_lens" }),
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
        target: {
          name: "otto",
          agentId: "a_otto",
          expectedSessionId: "s_otto",
        },
        text: "continue",
        clientMessageId: "cm_lens",
      }),
      expect.any(Request),
    );
  });

  it("keeps the stable agent id when a session socket carries a stale display name", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const sessionInput = vi.fn(async () => new Response(JSON.stringify({
      receipt: {
        commandId: "cmd_stable",
        sessionId: "s_real",
        state: "queued",
        revision: 1,
        seq: 1,
      },
    }), { status: 201 }));

    handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=stale-name&agentId=a_real"),
      {
        observe: async () =>
          boundSessionResponse(textStream([]), {
            name: "stale-name",
            agentId: "a_real",
            sessionId: "s_real",
          }),
        sessionInput,
      },
    );
    socket.emit("message", JSON.stringify({ t: "input",
        agentId: "a_real",
        expectedSessionId: "s_real",
        text: "continue" }));

    await vi.waitFor(() => expect(sessionInput).toHaveBeenCalledWith(
      expect.objectContaining({
        mode: "session",
        target: { name: "stale-name", agentId: "a_real",
            expectedSessionId: "s_real",
          },
      }),
      expect.any(Request),
    ));
  });

  it("supports an agent-id-only session socket for observe and input", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const observe = vi.fn(async () =>
      boundSessionResponse(textStream([]), {
        name: "current-name",
        agentId: "a_real",
        sessionId: "s_real",
      }));
    const sessionInput = vi.fn(async () => new Response(JSON.stringify({
      receipt: {
        commandId: "cmd_id_only",
        sessionId: "s_real",
        state: "queued",
        revision: 1,
        seq: 1,
      },
    }), { status: 201 }));

    handleWs(socket, new Request("http://localhost/api/agui/ws?agentId=a_real"), {
      observe,
      sessionInput,
    });
    socket.emit("message", JSON.stringify({ t: "input",
        agentId: "a_real",
        expectedSessionId: "s_real",
        text: "continue" }));

    await vi.waitFor(() => expect(sessionInput).toHaveBeenCalledWith(
      expect.objectContaining({ mode: "session", target: {
            name: "current-name",
            agentId: "a_real",
            expectedSessionId: "s_real",
          } }),
      expect.any(Request),
    ));
    expect(observe).toHaveBeenCalledWith(expect.objectContaining({
      url: "http://localhost/api/agui/observe?agentId=a_real",
    }));
  });

  it("binds every mutation on a stable session path to its one canonical agent target", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const sessionInput = vi.fn(async (input: { clientMessageId?: string }) => new Response(
      JSON.stringify({
        receipt: {
          commandId: `cmd_${input.clientMessageId ?? "input"}`,
          clientMessageId: input.clientMessageId,
          sessionId: "s_real",
          state: "queued",
          revision: 1,
          seq: 1,
        },
      }),
      { status: 201 },
    ));
    const steerInput = vi.fn(async () => Response.json({
      result: { accepted: true, delivery: "steered" },
    }, { status: 201 }));
    const queueSubscribe = vi.fn(() => () => {});
    const routed: Array<{ path: string; body: unknown }> = [];
    const fetchHandler = vi.fn(async (request: Request) => {
      const path = new URL(request.url).pathname;
      routed.push({ path, body: await request.json() });
      if (path === "/api/conversation/prompt") {
        return Response.json({
          clientMutationId: "mut_bound",
          commandId: "queued_bound",
          state: "cancelled",
          revision: 2,
          seq: 2,
        });
      }
      return Response.json({ ok: true }, { status: 201 });
    });
    const control = handleWs(
      socket,
      new Request("http://localhost/api/v1/agent-sessions/s_real/events?view=agui"),
      {
        observe: async () => boundSessionResponse(source.stream),
        sessionInput,
        steerInput,
        resolveHarness: async () => "codex",
        commandQueueHub: { subscribe: queueSubscribe },
        fetchHandler,
      },
    );

    await vi.waitFor(() => expect(queueSubscribe).toHaveBeenCalledWith(
      expect.any(Request),
      { name: "current-name", agentId: "a_real",
          expectedSessionId: "s_real",
        },
      expect.objectContaining({ onSnapshot: expect.any(Function) }),
    ));
    socket.emit("message", JSON.stringify({
      t: "input",
        agentId: "a_real",
        expectedSessionId: "s_real",
        text: "bare stable input",
      clientMessageId: "cm_bare",
    }));
    socket.emit("message", JSON.stringify({
      t: "input",
        agentId: "a_real",
        expectedSessionId: "s_real",
        mode: "session",
      target: { name: "stale-name", agentId: "a_real",
          expectedSessionId: "s_real",
        },
      text: "stable id wins",
      clientMessageId: "cm_stable",
    }));
    socket.emit("message", JSON.stringify({
      t: "steer",
        agentId: "a_real",
        expectedSessionId: "s_real",
        text: "steer canonical lane",
      clientMessageId: "cm_steer_bound",
    }));
    socket.emit("message", JSON.stringify({
      t: "command",
        agentId: "a_real",
        expectedSessionId: "s_real",
        name: "compact",
      clientCommandId: "cc_bound",
    }));
    socket.emit("message", JSON.stringify({
      t: "queue.cancel",
        agentId: "a_real",
        expectedSessionId: "s_real",
        clientMutationId: "mut_bound",
      commandId: "queued_bound",
      expectedRevision: 1,
    }));

    await vi.waitFor(() => expect(sessionInput).toHaveBeenCalledTimes(2));
    expect(sessionInput).toHaveBeenNthCalledWith(1, expect.objectContaining({
      mode: "session",
      target: { name: "current-name", agentId: "a_real",
          expectedSessionId: "s_real",
        },
      text: "bare stable input",
    }), expect.any(Request));
    expect(sessionInput).toHaveBeenNthCalledWith(2, expect.objectContaining({
      mode: "session",
      target: { name: "current-name", agentId: "a_real",
          expectedSessionId: "s_real",
        },
      text: "stable id wins",
    }), expect.any(Request));
    await vi.waitFor(() => expect(steerInput).toHaveBeenCalledWith({
      target: { name: "current-name", agentId: "a_real",
            expectedSessionId: "s_real",
          },
      text: "steer canonical lane",
      clientMessageId: "cm_steer_bound",
    }, expect.any(Request)));
    await vi.waitFor(() => expect(routed).toEqual(expect.arrayContaining([
      {
        path: "/api/conversation/compact",
        body: { name: "current-name", agentId: "a_real",
              expectedSessionId: "s_real",
              clientMessageId: "cc_bound" },
      },
      {
        path: "/api/conversation/prompt",
        body: {
          name: "current-name",
          agentId: "a_real",
              expectedSessionId: "s_real",
              action: "cancel",
          clientMutationId: "mut_bound",
          commandId: "queued_bound",
          expectedRevision: 1,
        },
      },
    ])));

    source.close();
    control.close();
    await control.closed;
  });

  it.each([
    {
      label: "session input target",
      frame: {
        t: "input",
        agentId: "a_real",
        expectedSessionId: "s_real",
        mode: "session",
        target: "other-agent",
        text: "cross lane",
        clientMessageId: "cm_cross_input",
      },
      errorType: "input.err",
    },
    {
      label: "bus mode on a session lane",
      frame: {
        t: "input",
        agentId: "a_real",
        expectedSessionId: "s_real",
        mode: "bus",
        target: { verb: "post", thread: "other-lane" },
        text: "cross lane",
        clientMessageId: "cm_cross_bus",
      },
      errorType: "input.err",
    },
    {
      label: "steer target",
      frame: {
        t: "steer",
        agentId: "a_real",
        expectedSessionId: "s_real",
        target: { name: "current-name", agentId: "a_other" },
        text: "cross lane",
        clientMessageId: "cm_cross_steer",
      },
      errorType: "steer.err",
    },
    {
      label: "command target",
      frame: {
        t: "command",
        agentId: "a_real",
        expectedSessionId: "s_real",
        target: "other-agent",
        name: "compact",
        clientCommandId: "cc_cross",
      },
      errorType: "command.err",
    },
    {
      label: "queue target",
      frame: {
        t: "queue.cancel",
        agentId: "a_real",
        expectedSessionId: "s_real",
        target: "other-agent",
        clientMutationId: "mut_cross",
        commandId: "cmd_cross",
      },
      errorType: "queue.mutation.err",
    },
  ])("rejects a cross-lane $label before mutation ingress", async ({ frame, errorType }) => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const sessionInput = vi.fn();
    const busInput = vi.fn();
    const steerInput = vi.fn();
    const fetchHandler = vi.fn();
    const queueSubscribe = vi.fn(() => () => {});
    const control = handleWs(
      socket,
      new Request("http://localhost/api/v1/agent-sessions/s_real/events?view=agui"),
      {
        observe: async () => boundSessionResponse(source.stream),
        sessionInput,
        busInput,
        steerInput,
        resolveHarness: async () => "codex",
        commandQueueHub: { subscribe: queueSubscribe },
        fetchHandler,
      },
    );
    await vi.waitFor(() => expect(queueSubscribe).toHaveBeenCalled());

    socket.emit("message", JSON.stringify(frame));

    await vi.waitFor(() => expect(
      socket.sent.map((raw) => JSON.parse(raw)).some((sent) => sent.t === errorType),
    ).toBe(true));
    expect(sessionInput).not.toHaveBeenCalled();
    expect(busInput).not.toHaveBeenCalled();
    expect(steerInput).not.toHaveBeenCalled();
    expect(fetchHandler).not.toHaveBeenCalled();

    source.close();
    control.close();
    await control.closed;
  });

  it("defaults a bare legacy input frame on a dm socket to a bus dm send", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const busInput = vi.fn(async () => new Response(
      JSON.stringify({ ok: true }),
      { status: 201, headers: { "content-type": "application/json" } },
    ));

    handleWs(socket, new Request("http://localhost/api/agui/ws?dm=morgan"), {
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
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
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
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
      observe: async () =>
          boundSessionResponse(textStream([]), {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
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
      observe: async () =>
          boundSessionResponse(textStream([]), {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
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
    const subscribe = vi.fn((topic: string, afterSeq: number, handlers: ToolCallEventHandlers) => {
      expect(topic).toBe("sys.agent.lifecycle");
      expect(afterSeq).toBe(7);
      queueMicrotask(() => handlers.onEvent({
        kind: "agent_lifecycle",
        topic,
        seq: 8,
        ts: 1783300000000,
        agent: "otto",
        sessionId: "s_otto",
        lifecycle: "current_work",
        currentWork: "R1.5",
      }));
      return { ready: Promise.resolve(), close: vi.fn() };
    });

    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () =>
          boundSessionResponse(textStream([]), {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        developerEvents: { subscribe },
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

  it("uses an event-driven Gateway source for durable thread subscriptions", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const closeSubscription = vi.fn();
    let handlers: ToolCallEventHandlers | undefined;
    const subscribe = vi.fn((topic: string, afterSeq: number, next: ToolCallEventHandlers) => {
      expect(topic).toBe("sys.message.thread.design");
      expect(afterSeq).toBe(7);
      handlers = next;
      return { ready: Promise.resolve(), close: closeSubscription };
    });
    const control = handleWs(socket, new Request("http://localhost/api/agui/ws"), {
      developerEvents: { subscribe },
    });

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.message.thread.design",
      afterSeq: 7,
    }));
    handlers?.onEvent({
      kind: "message",
      topic: "sys.message.thread.design",
      seq: 8,
      ts: 1_780_000_000_008,
      thread: "design",
      from: "Ada",
      messageId: "m_8",
    });

    await vi.waitFor(() => expect(developerEvents(socket)).toContainEqual(expect.objectContaining({
      topic: "sys.message.thread.design",
      seq: 8,
      messageId: "m_8",
    })));
    expect(subscribe).toHaveBeenCalledTimes(1);

    control.close();
    await control.closed;
    expect(closeSubscription).toHaveBeenCalledTimes(1);
  });

  it("closes a durable event socket with its last cursor unadvanced under backpressure", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    let handlers: ToolCallEventHandlers | undefined;
    const control = handleWs(socket, new Request("http://localhost/api/agui/ws"), {
      developerEvents: {
        subscribe(_topic: string, _afterSeq: number, next: ToolCallEventHandlers) {
          handlers = next;
          return { ready: Promise.resolve(), close: vi.fn() };
        },
      },
    });
    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.message.thread.design",
      afterSeq: 7,
    }));
    socket.bufferedAmount = 1024 * 1024 + 1;

    expect(handlers?.onEvent({
      kind: "message",
      topic: "sys.message.thread.design",
      seq: 8,
      ts: 1_780_000_000_008,
      messageId: "m_8",
    })).toBe(false);
    await control.closed;
    expect(socket.closed).toEqual({ code: 1013, reason: "developer.backpressure:7" });
    expect(developerEvents(socket)).toEqual([]);
  });

  it("tails ephemeral tool-call developer events for the observed session", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        daemonToolCallEvents: null,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 0,
    }));

    source.enqueue(
      'data: {"type":"TOOL_CALL_START","toolCallId":"tc1","toolCallName":"Read"}\n\n',
    );
    source.enqueue(
      'data: {"type":"TOOL_CALL_RESULT","toolCallId":"tc1","content":"done","status":"completed"}\n\n',
    );

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
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        daemonToolCallEvents: {
          subscribe(topic: string, afterSeq: number, h: ToolCallEventHandlers) {
            expect(topic).toBe("sys.agent.otto.tool_call");
            expect(afterSeq).toBe(0);
            handlers = h;
            return unsubscribe;
          },
        },
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
    source.enqueue(
      'data: {"type":"TOOL_CALL_START","toolCallId":"tc1","toolCallName":"Read"}\n\n',
    );

    await vi.waitFor(() => {

    expect(socket.sent).toContain(
        '{"type":"TOOL_CALL_START","toolCallId":"tc1","toolCallName":"Read"}',
      );
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
    const durableSubscribe = vi.fn(() => {
      throw new Error("fleet status must not subscribe to durable message events");
    });
    let handlers: ToolCallEventHandlers | undefined;
    const unsubscribe = vi.fn();
    const control = handleWs(
      socket,
      // No ?session= — the fleet topic is not session-scoped.
      new Request("http://localhost/api/agui/ws"),
      {
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        developerEvents: { subscribe: durableSubscribe },
        daemonFleetStatusEvents: {
          subscribe(topic: string, afterSeq: number, h: ToolCallEventHandlers) {
            expect(topic).toBe("sys.fleet.status");
            expect(afterSeq).toBe(999);
            handlers = h;
            return unsubscribe;
          },
        },
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
    expect(durableSubscribe).not.toHaveBeenCalled();

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
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        daemonFleetStatusEvents: null,
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
    const durableSubscribe = vi.fn(() => {
      throw new Error("tool-call topics must not subscribe to durable message events");
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
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "smoke-agent",
            agentId: "a_smoke",
            sessionId: "s_smoke",
          }),
        sessionInput,
        developerEvents: { subscribe: durableSubscribe },
        daemonToolCallEvents: {
          subscribe(topic: string, afterSeq: number, h: ToolCallEventHandlers) {
            expect(topic).toBe("sys.agent.smoke-agent.tool_call");
            expect(afterSeq).toBe(0);
            handlers = h;
            return () => {};
          },
        },
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.smoke-agent.tool_call",
      afterSeq: 0,
    }));
    socket.emit("message", JSON.stringify({
      t: "input",
        agentId: "a_smoke",
        expectedSessionId: "s_smoke",
        mode: "session",
      target: { name: "smoke-agent", agentId: "a_smoke",
          expectedSessionId: "s_smoke",
        },
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
    source.enqueue(
      'data: {"type":"TOOL_CALL_START","toolCallId":"tc-smoke","toolCallName":"Bash"}\n\n',
    );
    source.enqueue(
      'data: {"type":"TOOL_CALL_RESULT","toolCallId":"tc-smoke","content":"boom","status":"failed"}\n\n',
    );
    source.enqueue('data: {"type":"RUN_FINISHED","runId":"r-smoke"}\n\n');

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

    expect(socket.sent).toContain(
        '{"type":"RUN_FINISHED","runId":"r-smoke"}',
      );
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
        target: { name: "smoke-agent", agentId: "a_smoke",
          expectedSessionId: "s_smoke",
        },
        text: "R1.6 disposable WS smoke",
        clientMessageId: "cm_smoke",
      }),
      expect.any(Request),
    );
    expect(durableSubscribe).not.toHaveBeenCalled();

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
        observe: async () =>
          boundSessionResponse(textStream([]), {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        daemonToolCallEvents: {
          subscribe(_topic: string, _afterSeq: number, h: ToolCallEventHandlers) {
            handlers = h;
            return () => {};
          },
        },
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
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        daemonToolCallEvents: null,
      },
    );

    source.enqueue(
      'data: {"type":"TOOL_CALL_START","toolCallId":"tc2","toolCallName":"Bash"}\n\n',
    );
    source.enqueue(
      'data: {"type":"TOOL_CALL_RESULT","toolCallId":"tc2","content":"boom","status":"failed"}\n\n',
    );
    await vi.waitFor(() => {

    expect(socket.sent).toContain(
        '{"type":"TOOL_CALL_RESULT","toolCallId":"tc2","content":"boom","status":"failed"}',
      );
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
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        daemonToolCallEvents: null,
      },
    );

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 0,
    }));

    source.enqueue(
      'data: {"type":"TOOL_CALL_START","toolCallId":"tc3","toolCallName":"Read"}\n\n',
    );
    source.enqueue(
      'data: {"type":"TOOL_CALL_RESULT","toolCallId":"tc3","content":"done","status":"completed"}\n\n',
    );
    source.enqueue(
      'data: {"type":"TOOL_CALL_RESULT","toolCallId":"tc3","content":"late","status":"completed"}\n\n',
    );

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
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        daemonToolCallEvents: null,
      },
    );

    for (let i = 0; i < 1025; i += 1) {
      source.enqueue(
        `data: {"type":"TOOL_CALL_START","toolCallId":"tc${i}","toolCallName":"Tool${i}"}\n\n`,
      );
    }
    await vi.waitFor(() => {

    expect(socket.sent).toContain(
        '{"type":"TOOL_CALL_START","toolCallId":"tc1024","toolCallName":"Tool1024"}',
      );
    });

    socket.emit("message", JSON.stringify({
      t: "subscribe",
      topic: "sys.agent.otto.tool_call",
      afterSeq: 0,
    }));
    source.enqueue(
      'data: {"type":"TOOL_CALL_RESULT","toolCallId":"tc0","content":"old","status":"completed"}\n\n',
    );
    source.enqueue(
      'data: {"type":"TOOL_CALL_RESULT","toolCallId":"tc1024","content":"new","status":"completed"}\n\n',
    );

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
    const subscribe = vi.fn();

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
      daemonToolCallEvents: null,
      developerEvents: { subscribe },
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
    expect(subscribe).not.toHaveBeenCalled();
  });

  it("rejects non-system developer event subscriptions", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const subscribe = vi.fn();

    handleWs(socket, new Request("http://localhost/api/agui/ws?session=otto"), {
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
      developerEvents: { subscribe },
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
    expect(subscribe).not.toHaveBeenCalled();
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

  it("hydrates a queue by stable agentId when its display name is stale", async () => {
    const { CommandQueueHub } = await loadWs();
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      expect(url.searchParams.get("name")).toBe("stale-name");
      expect(url.searchParams.get("agentId")).toBe("a_real");
      return Response.json({
        target: "current-name",
        sessionId: "s_real",
        turnActive: false,
        steerCapability: "native_steer",
        seq: 0,
        revision: 0,
        commands: [],
      });
    });
    const hub = new CommandQueueHub({
      fetchHandler,
      commandQueueEventPollMs: 10_000,
    });
    const snapshots: unknown[] = [];
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=stale-name&agentId=a_real"),
      { name: "stale-name", agentId: "a_real" },
      {
        onSnapshot: (snapshot) => snapshots.push(snapshot),
        onTransition: vi.fn(),
        onError: vi.fn(),
      },
    );

    await vi.waitFor(() => expect(snapshots).toHaveLength(1));
    expect(snapshots[0]).toMatchObject({ target: "current-name", sessionId: "s_real" });
    unsubscribe();
  });

  it("uses the socket stable agentId for queue hydration and mutations", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const subscribe = vi.fn(() => () => {});
    const fetchHandler = vi.fn(async (request: Request) => {
      expect(new URL(request.url).pathname).toBe("/api/conversation/prompt");
      expect(await request.json()).toEqual({
        name: "current-name",
        agentId: "a_real",
        expectedSessionId: "s_real",
        action: "cancel",
        clientMutationId: "mut_real",
        commandId: "cmd_real",
        expectedRevision: 1,
      });
      return Response.json({
        sessionId: "s_real",
        clientMutationId: "mut_real",
        commandId: "cmd_real",
        state: "cancelled",
        steerCapability: "native_steer",
        revision: 2,
        seq: 9,
      });
    });
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?agentId=a_real"),
      {
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "current-name",
            agentId: "a_real",
            sessionId: "s_real",
          }),
        commandQueueHub: { subscribe },
        fetchHandler,
      },
    );

    await vi.waitFor(() => expect(subscribe).toHaveBeenCalledWith(
      expect.any(Request),
      {
          name: "current-name",
          agentId: "a_real",
          expectedSessionId: "s_real",
        },
      expect.objectContaining({ onSnapshot: expect.any(Function) }),
    ));
    socket.emit("message", JSON.stringify({
      t: "queue.cancel",
        agentId: "a_real",
        expectedSessionId: "s_real",
        clientMutationId: "mut_real",
      commandId: "cmd_real",
      expectedRevision: 1,
    }));
    await vi.waitFor(() => {
      expect(socket.sent.map((raw) => JSON.parse(raw))).toContainEqual({
        t: "queue.mutation.ack",
        action: "cancel",
        sessionId: "s_real",
        clientMutationId: "mut_real",
        commandId: "cmd_real",
        state: "cancelled",
        steerCapability: "native_steer",
        revision: 2,
        seq: 9,
      });
    });

    source.close();
    control.close();
    await control.closed;
  });

  it.each(["queued", "failed"])("coalesces %s-event redraws into one authoritative snapshot for every mounted client", async (transitionState) => {
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
            state: transitionState,
            mode: "queue",
            revision: 2,
          },
          {
            seq: 7,
            sessionId: "s_otto",
            commandId: "cmd_b",
            state: transitionState,
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

  it("marks initial and retained-gap hydration failures as fatal typed queue errors", async () => {
    const { CommandQueueHub } = await loadWs();
    let snapshotsServed = 0;
    const errors: Array<[string, Record<string, unknown>]> = [];
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        snapshotsServed += 1;
        if (snapshotsServed === 1) {
          return Response.json({
            target: "otto",
            sessionId: "s_otto",
            turnActive: false,
            steerCapability: "native_steer",
            seq: 5,
            revision: 5,
            commands: [],
          });
        }
        return Response.json({ error: "snapshot authority unavailable" }, { status: 502 });
      }
      return Response.json({ events: [], latestSeq: 9, gap: true });
    });
    const hub = new CommandQueueHub({
      fetchHandler,
      commandQueueEventPollMs: 10_000,
    });
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: (error, details) => errors.push([error, details]),
      },
    );

    await vi.waitFor(() => expect(errors).toHaveLength(1));
    expect(errors).toEqual([[
      "snapshot authority unavailable",
      { phase: "hydrate", fatal: true },
    ]]);
    unsubscribe();

    const initialErrors: Array<[string, Record<string, unknown>]> = [];
    const initialHub = new CommandQueueHub({
      fetchHandler: async () => Response.json(
        { error: "initial subscription unavailable" },
        { status: 502 },
      ),
      commandQueueEventPollMs: 10_000,
    });
    const offInitial = initialHub.subscribe(
      new Request("http://localhost/api/agui/ws?session=iris"),
      "iris",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: (error, details) => initialErrors.push([error, details]),
      },
    );
    await vi.waitFor(() => expect(initialErrors).toHaveLength(1));
    expect(initialErrors).toEqual([[
      "initial subscription unavailable",
      { phase: "subscribe", fatal: true },
    ]]);
    offInitial();
  });

  it("keeps recoverable queue authority failures live and emits one restoration", async () => {
    const { CommandQueueHub } = await loadWs();
    let eventPolls = 0;
    let snapshots = 0;
    const errors: Array<[string, Record<string, unknown>]> = [];
    const restored: number[] = [];
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        snapshots += 1;
        if (snapshots === 2) {
          return Response.json({ error: "queue redraw unavailable" }, { status: 503 });
        }
        return Response.json({
          target: "otto",
          sessionId: "s_otto",
          turnActive: true,
          steerCapability: "native_steer",
          seq: snapshots === 1 ? 5 : 6,
          revision: snapshots === 1 ? 5 : 6,
          commands: [],
        });
      }
      eventPolls += 1;
      if (eventPolls === 1) {
        return Response.json({
          events: [{
            seq: 6,
            sessionId: "s_otto",
            commandId: "cmd_1",
            state: "queued",
            mode: "queue",
            revision: 1,
          }],
          latestSeq: 6,
          gap: false,
        });
      }
      return Response.json({ events: [], latestSeq: 6, gap: false });
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 1 });
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: (error, details) => errors.push([error, details]),
        onRestored: (seq) => restored.push(seq),
      },
    );

    await vi.waitFor(() => expect(restored).toEqual([6]));
    expect(errors).toEqual([[
      "queue redraw unavailable",
      { phase: "refresh", fatal: false },
    ]]);
    expect(eventPolls).toBeGreaterThanOrEqual(2);
    unsubscribe();
  });

  it("restores queue authority after a recoverable event-page failure", async () => {
    const { CommandQueueHub } = await loadWs();
    let eventPolls = 0;
    const errors: Array<[string, Record<string, unknown>]> = [];
    const restored: number[] = [];
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        return Response.json({
          target: "otto",
          sessionId: "s_otto",
          turnActive: false,
          steerCapability: "native_steer",
          seq: 5,
          revision: 5,
          commands: [],
        });
      }
      eventPolls += 1;
      if (eventPolls === 1) {
        return Response.json({ error: "transition page unavailable" }, { status: 503 });
      }
      return Response.json({ events: [], latestSeq: 7, gap: false });
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 1 });
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: (error, details) => errors.push([error, details]),
        onRestored: (seq) => restored.push(seq),
      },
    );

    await vi.waitFor(() => expect(restored).toEqual([7]));
    expect(errors).toEqual([[
      "transition page unavailable",
      { phase: "events", fatal: false },
    ]]);
    unsubscribe();
  });

  it("isolates event-page failures to their authenticated queue group", async () => {
    const { CommandQueueHub } = await loadWs();
    const errorsA: unknown[] = [];
    const errorsB: unknown[] = [];
    const transitionsB: unknown[] = [];
    let pollsA = 0;
    let pollsB = 0;
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      const cookie = request.headers.get("cookie");
      if (url.searchParams.has("name")) {
        const name = url.searchParams.get("name");
        return Response.json({
          target: name,
          sessionId: `s_${name}`,
          turnActive: false,
          steerCapability: "native_steer",
          seq: 1,
          revision: 1,
          commands: [],
        });
      }
      if (cookie === "auth=A") {
        pollsA += 1;
        return Response.json({ error: "A unavailable" }, { status: 503 });
      }
      pollsB += 1;
      return Response.json({
        events: [{
          seq: 2,
          sessionId: "s_b",
          commandId: "cmd_b",
          state: "started",
          mode: "queue",
          revision: 1,
        }],
        latestSeq: 2,
        gap: false,
      });
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 1 });
    const offA = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=a", { headers: { cookie: "auth=A" } }),
      "a",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: (error, details) => errorsA.push([error, details]),
        onRestored: vi.fn(),
      },
    );
    const offB = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=b", { headers: { cookie: "auth=B" } }),
      "b",
      {
        onSnapshot: vi.fn(),
        onTransition: (event) => transitionsB.push(event),
        onError: (error, details) => errorsB.push([error, details]),
        onRestored: vi.fn(),
      },
    );

    await vi.waitFor(() => expect(transitionsB).toHaveLength(1));
    expect(pollsA).toBeGreaterThan(0);
    expect(pollsB).toBeGreaterThan(0);
    expect(errorsA.length).toBeGreaterThan(0);
    expect(errorsB).toEqual([]);
    offA();
    offB();
  });

  it("isolates cookie-less bearer principals into distinct queue groups", async () => {
    const { CommandQueueHub } = await loadWs();
    const errorsA: unknown[] = [];
    const errorsB: unknown[] = [];
    const transitionsB: unknown[] = [];
    const pollAuth: string[] = [];
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      const authorization = request.headers.get("authorization") ?? "";
      if (url.searchParams.has("name")) {
        const name = url.searchParams.get("name");
        return Response.json({
          target: name,
          sessionId: `s_${name}`,
          seq: 1,
          revision: 1,
          commands: [],
        });
      }
      pollAuth.push(authorization);
      if (authorization === "Bearer A") {
        return Response.json({ error: "A unavailable" }, { status: 503 });
      }
      return Response.json({
        events: [{
          seq: 2,
          sessionId: "s_b",
          commandId: "cmd_bearer_b",
          state: "started",
          mode: "queue",
          revision: 1,
        }],
        latestSeq: 2,
        gap: false,
      });
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 5 });
    const offA = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=a", {
        headers: { authorization: "Bearer A" },
      }),
      "a",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: (error, details) => errorsA.push([error, details]),
      },
    );
    const offB = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=b", {
        headers: { authorization: "Bearer B" },
      }),
      "b",
      {
        onSnapshot: vi.fn(),
        onTransition: (event) => transitionsB.push(event),
        onError: (error, details) => errorsB.push([error, details]),
      },
    );

    await vi.waitFor(() => expect(transitionsB).toHaveLength(1));
    expect(pollAuth).toContain("Bearer A");
    expect(pollAuth).toContain("Bearer B");
    expect(errorsA.length).toBeGreaterThan(0);
    expect(errorsB).toEqual([]);
    offA();
    offB();
  });

  it("waits for reversed initial hydrations and polls from the minimum cursor", async () => {
    const { CommandQueueHub } = await loadWs();
    let resolveA: ((response: Response) => void) | undefined;
    let resolveB: ((response: Response) => void) | undefined;
    const eventAfter: string[] = [];
    const transitionsB: unknown[] = [];
    const fetchHandler = vi.fn((request: Request) => {
      const url = new URL(request.url);
      const name = url.searchParams.get("name");
      if (name === "a") {
        return new Promise<Response>((resolve) => { resolveA = resolve; });
      }
      if (name === "b") {
        return new Promise<Response>((resolve) => { resolveB = resolve; });
      }
      const after = url.searchParams.get("eventsAfter") ?? "";
      eventAfter.push(after);
      return Promise.resolve(Response.json({
        events: after === "5"
          ? [{
              seq: 6,
              sessionId: "s_b",
              commandId: "cmd_b",
              state: "started",
              mode: "queue",
              revision: 1,
            }]
          : [],
        latestSeq: after === "5" ? 6 : 10,
        gap: false,
      }));
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 1 });
    const offA = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=a"),
      "a",
      { onSnapshot: vi.fn(), onTransition: vi.fn(), onError: vi.fn() },
    );
    const offB = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=b"),
      "b",
      { onSnapshot: vi.fn(), onTransition: (event) => transitionsB.push(event), onError: vi.fn() },
    );
    await vi.waitFor(() => {
      expect(resolveA).toBeDefined();
      expect(resolveB).toBeDefined();
    });
    resolveA?.(Response.json({
      target: "a", sessionId: "s_a", seq: 10, revision: 10, commands: [],
    }));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(eventAfter).toEqual([]);
    resolveB?.(Response.json({
      target: "b", sessionId: "s_b", seq: 5, revision: 5, commands: [],
    }));

    await vi.waitFor(() => expect(transitionsB).toHaveLength(1));
    expect(eventAfter[0]).toBe("5");
    offA();
    offB();
  });

  it("discards an in-flight page when a newly hydrated subscriber lowers the group cursor", async () => {
    const { CommandQueueHub } = await loadWs();
    let resolveStalePage: ((response: Response) => void) | undefined;
    const eventAfter: string[] = [];
    const transitionsB: Array<Record<string, unknown>> = [];
    const fetchHandler = vi.fn((request: Request) => {
      const url = new URL(request.url);
      const name = url.searchParams.get("name");
      if (name) {
        const seq = name === "a" ? 5 : 3;
        return Promise.resolve(Response.json({
          target: name,
          sessionId: `s_${name}`,
          seq,
          revision: seq,
          commands: [],
        }));
      }
      const after = url.searchParams.get("eventsAfter") ?? "";
      eventAfter.push(after);
      if (eventAfter.length === 1) {
        return new Promise<Response>((resolve) => { resolveStalePage = resolve; });
      }
      if (after === "3") {
        return Promise.resolve(Response.json({
          events: [4, 5].map((seq) => ({
            seq,
            sessionId: "s_b",
            commandId: `cmd_b_${seq}`,
            state: "started",
            mode: "queue",
            revision: 1,
          })),
          latestSeq: 5,
          gap: false,
        }));
      }
      return Promise.resolve(Response.json({ events: [], latestSeq: 6, gap: false }));
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 5 });
    const offA = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=a"),
      "a",
      { onSnapshot: vi.fn(), onTransition: vi.fn(), onError: vi.fn() },
    );
    await vi.waitFor(() => expect(eventAfter).toEqual(["5"]));
    const offB = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=b"),
      "b",
      {
        onSnapshot: vi.fn(),
        onTransition: (event) => transitionsB.push(event as Record<string, unknown>),
        onError: vi.fn(),
      },
    );
    await vi.waitFor(() => expect(resolveStalePage).toBeDefined());
    resolveStalePage?.(Response.json({
      events: [{
        seq: 6,
        sessionId: "s_a",
        commandId: "cmd_stale_page",
        state: "started",
        mode: "queue",
        revision: 1,
      }],
      latestSeq: 6,
      gap: false,
    }));

    await vi.waitFor(() => expect(transitionsB.map((event) => event.seq)).toEqual([4, 5]));
    expect(eventAfter.slice(0, 2)).toEqual(["5", "3"]);
    offA();
    offB();
  });

  it("waits for reversed gap hydrations and resumes from the minimum cursor", async () => {
    const { CommandQueueHub } = await loadWs();
    const snapshotCounts = new Map<string, number>();
    let resolveGapA: ((response: Response) => void) | undefined;
    let resolveGapB: ((response: Response) => void) | undefined;
    const eventAfter: string[] = [];
    const transitionsB: unknown[] = [];
    const fetchHandler = vi.fn((request: Request) => {
      const url = new URL(request.url);
      const name = url.searchParams.get("name");
      if (name) {
        const count = (snapshotCounts.get(name) ?? 0) + 1;
        snapshotCounts.set(name, count);
        if (count === 1) {
          return Promise.resolve(Response.json({
            target: name,
            sessionId: `s_${name}`,
            seq: 1,
            revision: 1,
            commands: [],
          }));
        }
        return new Promise<Response>((resolve) => {
          if (name === "a") resolveGapA = resolve;
          else resolveGapB = resolve;
        });
      }
      const after = url.searchParams.get("eventsAfter") ?? "";
      eventAfter.push(after);
      if (eventAfter.length === 1) {
        return Promise.resolve(Response.json({ events: [], latestSeq: 10, gap: true }));
      }
      return Promise.resolve(Response.json({
        events: after === "5"
          ? [{
              seq: 6,
              sessionId: "s_b",
              commandId: "cmd_gap_b",
              state: "started",
              mode: "queue",
              revision: 1,
            }]
          : [],
        latestSeq: after === "5" ? 6 : 10,
        gap: false,
      }));
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 1 });
    const offA = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=a"),
      "a",
      { onSnapshot: vi.fn(), onTransition: vi.fn(), onError: vi.fn() },
    );
    const offB = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=b"),
      "b",
      { onSnapshot: vi.fn(), onTransition: (event) => transitionsB.push(event), onError: vi.fn() },
    );
    await vi.waitFor(() => {
      expect(resolveGapA).toBeDefined();
      expect(resolveGapB).toBeDefined();
    });
    resolveGapA?.(Response.json({
      target: "a", sessionId: "s_a", seq: 10, revision: 10, commands: [],
    }));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(eventAfter).toEqual(["1"]);
    resolveGapB?.(Response.json({
      target: "b", sessionId: "s_b", seq: 5, revision: 5, commands: [],
    }));

    await vi.waitFor(() => expect(eventAfter.length).toBeGreaterThanOrEqual(2));
    expect(eventAfter[1]).toBe("5");
    await vi.waitFor(() => expect(transitionsB).toHaveLength(1));
    offA();
    offB();
  });

  it("restores once only after overlapping event and refresh failures both recover", async () => {
    const { CommandQueueHub } = await loadWs();
    let snapshots = 0;
    let eventPolls = 0;
    const trace: string[] = [];
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        snapshots += 1;
        if (snapshots === 2 || snapshots === 3) {
          trace.push("refresh-fail");
          return Response.json({ error: "refresh unavailable" }, { status: 503 });
        }
        if (snapshots === 4) trace.push("refresh-success");
        return Response.json({
          target: "otto",
          sessionId: "s_otto",
          seq: 6,
          revision: 6,
          commands: [],
        });
      }
      eventPolls += 1;
      if (eventPolls === 1) {
        return Response.json({
          events: [{
            seq: 6,
            sessionId: "s_otto",
            commandId: "cmd_overlap",
            state: "queued",
            mode: "queue",
            revision: 1,
          }],
          latestSeq: 6,
          gap: false,
        });
      }
      if (eventPolls === 2) {
        trace.push("events-fail");
        return Response.json({ error: "events unavailable" }, { status: 503 });
      }
      if (eventPolls === 3) trace.push("events-success");
      return Response.json({ events: [], latestSeq: 6, gap: false });
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 1 });
    const restored: number[] = [];
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: vi.fn(),
        onRestored: (seq) => {
          trace.push("restored");
          restored.push(seq);
        },
      },
    );

    await vi.waitFor(() => expect(restored).toEqual([6]));
    expect(trace.indexOf("restored")).toBeGreaterThan(trace.indexOf("events-success"));
    expect(trace.indexOf("restored")).toBeGreaterThan(trace.indexOf("refresh-success"));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(restored).toEqual([6]);
    unsubscribe();
  });

  it("does not restore events authority before a queued-event refresh succeeds", async () => {
    const { CommandQueueHub } = await loadWs();
    let snapshots = 0;
    let eventPolls = 0;
    const trace: string[] = [];
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        snapshots += 1;
        if (snapshots === 2) {
          return Response.json({ error: "refresh unavailable" }, { status: 503 });
        }
        return Response.json({
          target: "otto",
          sessionId: "s_otto",
          seq: snapshots === 1 ? 1 : 2,
          revision: snapshots === 1 ? 1 : 2,
          commands: [],
        });
      }
      eventPolls += 1;
      if (eventPolls === 1) {
        return Response.json({ error: "events unavailable" }, { status: 503 });
      }
      return Response.json({
        events: eventPolls === 2
          ? [{
              seq: 2,
              sessionId: "s_otto",
              commandId: "cmd_inverse_overlap",
              state: "queued",
              mode: "queue",
              revision: 1,
            }]
          : [],
        latestSeq: 2,
        gap: false,
      });
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 5 });
    const restored: number[] = [];
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: (_error, details) => trace.push(`err:${details.phase}`),
        onRestored: (seq) => {
          trace.push("restored");
          restored.push(seq);
        },
      },
    );

    await vi.waitFor(() => expect(restored).toEqual([2]));
    expect(trace).toContain("err:events");
    expect(trace).toContain("err:refresh");
    expect(trace.indexOf("restored")).toBeGreaterThan(trace.indexOf("err:refresh"));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(restored).toEqual([2]);
    unsubscribe();
  });

  it("keeps an always-gapping group on bounded cadence while another group catches up", async () => {
    const { CommandQueueHub } = await loadWs();
    let gapPolls = 0;
    const pollMs = 30;
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      const cookie = request.headers.get("cookie");
      const name = url.searchParams.get("name");
      if (name) {
        return Response.json({
          target: name,
          sessionId: `s_${name}`,
          seq: 0,
          revision: 0,
          commands: [],
        });
      }
      const after = Number(url.searchParams.get("eventsAfter") ?? "0");
      if (cookie === "auth=gap") {
        gapPolls += 1;
        return Response.json({ events: [], latestSeq: after + 1, gap: true });
      }
      return Response.json({
        events: [{
          seq: after + 1,
          sessionId: "s_busy",
          commandId: `cmd_busy_${after + 1}`,
          state: "started",
          mode: "queue",
          revision: 1,
        }],
        latestSeq: after + 2,
        gap: false,
      });
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: pollMs });
    const offGap = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=gap", { headers: { cookie: "auth=gap" } }),
      "gap",
      { onSnapshot: vi.fn(), onTransition: vi.fn(), onError: vi.fn() },
    );
    const offBusy = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=busy", { headers: { cookie: "auth=busy" } }),
      "busy",
      { onSnapshot: vi.fn(), onTransition: vi.fn(), onError: vi.fn() },
    );

    await new Promise((resolve) => setTimeout(resolve, 125));
    expect(gapPolls).toBeGreaterThanOrEqual(2);
    expect(gapPolls).toBeLessThanOrEqual(6);
    offGap();
    offBusy();
  });

  it("supersedes a retained refresh when a gap starts fatal rehydration", async () => {
    const { CommandQueueHub } = await loadWs();
    let snapshots = 0;
    let eventPolls = 0;
    let rejectGapHydrate: ((response: Response) => void) | undefined;
    const visibleSnapshots: unknown[] = [];
    const errors: Array<Record<string, unknown>> = [];
    const restored: number[] = [];
    const fetchHandler = vi.fn((request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        snapshots += 1;
        if (snapshots === 1) {
          return Promise.resolve(Response.json({
            target: "otto", sessionId: "s_otto", seq: 1, revision: 1, commands: [],
          }));
        }
        if (snapshots === 2) {
          return Promise.resolve(Response.json({ error: "refresh unavailable" }, { status: 503 }));
        }
        if (snapshots === 3) {
          return new Promise<Response>((resolve) => { rejectGapHydrate = resolve; });
        }
        return Promise.resolve(Response.json({
          target: "otto", sessionId: "s_otto", seq: 2, revision: 2, commands: [],
        }));
      }
      eventPolls += 1;
      if (eventPolls === 1) {
        return Promise.resolve(Response.json({
          events: [{
            seq: 2,
            sessionId: "s_otto",
            commandId: "cmd_before_gap",
            state: "queued",
            mode: "queue",
            revision: 1,
          }],
          latestSeq: 2,
          gap: false,
        }));
      }
      return Promise.resolve(Response.json({ events: [], latestSeq: 3, gap: true }));
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 5 });
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      {
        onSnapshot: (snapshot) => visibleSnapshots.push(snapshot),
        onTransition: vi.fn(),
        onError: (_error, details) => errors.push(details),
        onRestored: (seq) => restored.push(seq),
      },
    );

    await vi.waitFor(() => expect(rejectGapHydrate).toBeDefined());
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(visibleSnapshots).toHaveLength(1);
    expect(restored).toEqual([]);
    rejectGapHydrate?.(Response.json({ error: "gap hydrate unavailable" }, { status: 503 }));
    await vi.waitFor(() => expect(errors).toContainEqual({ phase: "hydrate", fatal: true }));
    expect(visibleSnapshots).toHaveLength(1);
    expect(restored).toEqual([]);
    unsubscribe();
  });

  it("restores queue authority when successful gap hydration supersedes an older refresh outage", async () => {
    const { CommandQueueHub } = await loadWs();
    let snapshots = 0;
    let eventPolls = 0;
    const errors: Array<Record<string, unknown>> = [];
    const restored: number[] = [];
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.searchParams.has("name")) {
        snapshots += 1;
        if (snapshots === 2) {
          return Response.json({ error: "refresh unavailable" }, { status: 503 });
        }
        return Response.json({
          target: "otto",
          sessionId: "s_otto",
          seq: snapshots === 1 ? 1 : 5,
          revision: snapshots === 1 ? 1 : 5,
          commands: [],
        });
      }
      eventPolls += 1;
      if (eventPolls === 1) {
        return Response.json({
          events: [{
            seq: 2,
            sessionId: "s_otto",
            commandId: "cmd_before_recovery_gap",
            state: "queued",
            mode: "queue",
            revision: 1,
          }],
          latestSeq: 2,
          gap: false,
        });
      }
      return Response.json({ events: [], latestSeq: 5, gap: true });
    });
    const hub = new CommandQueueHub({ fetchHandler, commandQueueEventPollMs: 5 });
    const unsubscribe = hub.subscribe(
      new Request("http://localhost/api/agui/ws?session=otto"),
      "otto",
      {
        onSnapshot: vi.fn(),
        onTransition: vi.fn(),
        onError: (_error, details) => errors.push(details),
        onRestored: (seq) => restored.push(seq),
      },
    );

    await vi.waitFor(() => expect(restored).toEqual([5]));
    expect(errors).toContainEqual({ phase: "refresh", fatal: false });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(restored).toEqual([5]);
    unsubscribe();
  });

  it("emits typed queue error and restoration frames on a session socket", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    let handlers: Record<string, (...args: any[]) => void> | undefined;
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=otto"),
      {
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "otto",
            agentId: "a_otto",
            sessionId: "s_otto",
          }),
        commandQueueHub: {
          subscribe: vi.fn((_request, _target, nextHandlers) => {
            handlers = nextHandlers;
            return () => {};
          }),
        },
      },
    );
    await vi.waitFor(() => expect(handlers).toBeDefined());

    handlers?.onError?.("queue events unavailable", { phase: "events", fatal: false });
    handlers?.onRestored?.(17);

    expect(socket.sent.map((raw) => JSON.parse(raw))).toEqual(expect.arrayContaining([
      {
        t: "queue.err",
        error: "queue events unavailable",
        phase: "events",
        fatal: false,
      },
      { t: "queue.restored", seq: 17 },
    ]));
    source.close();
    control.close();
    await control.closed;
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
      observe: async () =>
        boundSessionResponse(textStream([]), {
          name: "otto",
          agentId: "a_otto",
          sessionId: "s_otto",
        }),
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

    expect(resolveHarness).toHaveBeenCalledWith(
      { name: "otto", agentId: "a_otto", expectedSessionId: "s_otto" },
      expect.any(Request));
    expect(frames(socket).find((f) => f.t === "commands.catalog")).toMatchObject({
      target: "otto",
    });
  });

  it("resolves command catalogs and dispatch by stable agentId before a stale name", async () => {
    const { handleWs } = await loadWs();
    const socket = new FakeSocket();
    const source = controlledTextStream();
    const fetchHandler = vi.fn(async (request: Request) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/v1/members") {
        return Response.json([
          { name: "current-name", agentId: "a_real", agent: "claude", sessionId: "s_real" },
        ]);
      }
      expect(url.pathname).toBe("/api/conversation/compact");
      expect(await request.json()).toEqual({
        name: "stale-name",
        agentId: "a_real",
        expectedSessionId: "s_real",
        clientMessageId: "cc_stable",
      });
      return Response.json({ ok: true, result: { started: true, sessionId: "s_real" } }, { status: 201 });
    });
    const control = handleWs(
      socket,
      new Request("http://localhost/api/agui/ws?session=stale-name&agentId=a_real"),
      {
        observe: async () =>
          boundSessionResponse(source.stream, {
            name: "stale-name",
            agentId: "a_real",
            sessionId: "s_real",
          }),
        fetchHandler,
      },
    );

    socket.emit("message", JSON.stringify({ t: "commands.list" }));
    await vi.waitFor(() => {
      expect(frames(socket)).toContainEqual(expect.objectContaining({
        t: "commands.catalog",
        target: "stale-name",
        agentId: "a_real",
        harness: "claude",
      }));
    });

    socket.emit("message", JSON.stringify({
      t: "command",
        agentId: "a_real",
        expectedSessionId: "s_real",
        name: "compact",
      clientCommandId: "cc_stable",
    }));
    await vi.waitFor(() => {
      expect(frames(socket)).toContainEqual({
        t: "command.done",
        clientCommandId: "cc_stable",
        ok: true,
      });
    });

    source.close();
    control.close();
    await control.closed;
  });

  it("dispatches a gateway verb to its REST handler and replies ack then done", async () => {
    const fetchHandler = vi.fn(async (request: Request) => {
      expect(new URL(request.url).pathname).toBe("/api/conversation/compact");
      expect(await request.json()).toEqual({ name: "otto",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
        clientMessageId: "cc_1" });
      return new Response(JSON.stringify({ ok: true,
          result: { started: true, sessionId: "s_otto" },
        }), { status: 201 });
    });
    const socket = await openSocket({ resolveHarness: claudeHarness, fetchHandler });

    socket.emit("message", JSON.stringify({
      t: "command",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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
        target: {
          name: "otto",
          agentId: "a_otto",
          expectedSessionId: "s_otto",
        },
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
      agentId: "a_otto",
      expectedSessionId: "s_otto",
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
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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

    socket.emit("message", JSON.stringify({ t: "command",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
        name: "clear", target: "otto" }));
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
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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
      return new Response(JSON.stringify({ ok: true,
          result: { started: true, sessionId: "s_otto" },
        }), { status: 201 });
    });
    const socket = await openSocket({ resolveHarness: claudeHarness, fetchHandler });

    socket.emit("message", JSON.stringify({
      t: "command",
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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
        agentId: "a_otto",
        expectedSessionId: "s_otto",
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
    const observe = vi.fn(async () =>
      boundSessionResponse(textStream([]), {
        name: "otto",
        agentId: "a_otto",
        sessionId: "s_otto",
      }));
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
