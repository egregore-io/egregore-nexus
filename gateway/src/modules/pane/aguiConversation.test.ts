// Task 4 (plan 60) — the AG-UI → pane-model reducer (the web console payoff).
//
// `useAguiConversation` opens `GET /api/agui/observe?thread=…`, decodes the SSE
// with `@ag-ui/client`, and reduces the decoded `BaseEvent`s into the EXISTING
// `PaneMessage[]`/`Block[]` model the prototype thread already renders. This
// file drives the PURE reducer (`reduceAguiEvents`) with a hand-built AG-UI event
// sequence and asserts the resulting rows: a reasoning → thinking card, a merged
// tool-call block, and streamed text accumulated into a paragraph. The reducer is
// what the hook threads per frame; testing it pure keeps the AG-UI → view-model
// translation socket-free and deterministic.
import { afterEach, describe, it, expect, vi } from "vitest";
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";

import {
  AguiWebSocketSource,
  reduceAguiEvents,
  newConversationState,
  appendUserMessage,
  parseAguiData,
  observeSessionUrl,
  observeWebSocketUrl,
  resolveAguiTransport,
  observeCursorsFromEvent,
  historyRowsToPaneMessages,
  mergeBacklogMessages,
} from "./aguiConversation";
import type { AguiConversationState } from "./aguiConversation";
import type { Block, PaneMessage } from "./types";

afterEach(() => {
  document.cookie = "nexus_csrf=; Max-Age=0; Path=/";
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

/** Tiny typed builders for the decoded AG-UI events the daemon's run emits. */
const ev = {
  runStarted: (runId: string): BaseEvent =>
    ({ type: EventType.RUN_STARTED, threadId: "t", runId }) as BaseEvent,
  runFinished: (runId: string): BaseEvent =>
    ({ type: EventType.RUN_FINISHED, threadId: "t", runId }) as BaseEvent,
  reasoningStart: (messageId: string): BaseEvent =>
    ({ type: EventType.REASONING_MESSAGE_START, messageId }) as BaseEvent,
  reasoningContent: (messageId: string, delta: string): BaseEvent =>
    ({ type: EventType.REASONING_MESSAGE_CONTENT, messageId, delta }) as BaseEvent,
  reasoningEnd: (messageId: string): BaseEvent =>
    ({ type: EventType.REASONING_MESSAGE_END, messageId }) as BaseEvent,
  toolStart: (toolCallId: string, toolCallName: string): BaseEvent =>
    ({ type: EventType.TOOL_CALL_START, toolCallId, toolCallName }) as BaseEvent,
  toolArgs: (toolCallId: string, delta: string): BaseEvent =>
    ({ type: EventType.TOOL_CALL_ARGS, toolCallId, delta }) as BaseEvent,
  toolResult: (
    toolCallId: string,
    content: string,
    opts: { append?: boolean; status?: string } = {},
  ): BaseEvent =>
    ({ type: EventType.TOOL_CALL_RESULT, toolCallId, content, ...opts }) as BaseEvent,
  textStart: (messageId: string): BaseEvent =>
    ({ type: EventType.TEXT_MESSAGE_START, messageId }) as BaseEvent,
  // A `role:"user"` message — the operator's own input echoed by the session stream (what map.ts's
  // `user_input` case emits). This is the half that mirrors TUI ↔ web.
  userStart: (
    messageId: string,
    provenance: { name?: string; kind?: string } = {},
  ): BaseEvent =>
    ({ type: EventType.TEXT_MESSAGE_START, messageId, role: "user", ...provenance }) as BaseEvent,
  textContent: (messageId: string, delta: string): BaseEvent =>
    ({ type: EventType.TEXT_MESSAGE_CONTENT, messageId, delta }) as BaseEvent,
  textEnd: (messageId: string): BaseEvent =>
    ({ type: EventType.TEXT_MESSAGE_END, messageId }) as BaseEvent,
};

/** Fold a whole event sequence through the reducer; return the rows. */
function reduceAll(events: BaseEvent[]): PaneMessage[] {
  let state = newConversationState();
  for (const e of events) state = reduceAguiEvents(state, e);
  return state.messages;
}

describe("reduceAguiEvents — AG-UI BaseEvent[] → PaneMessage[]/Block[]", () => {
  it("reduces a full turn into one row: thinking card, merged tool-call, streamed text", () => {
    const rows = reduceAll([
      ev.runStarted("r1"),
      ev.reasoningStart("m1"),
      ev.reasoningContent("m1", "let me "),
      ev.reasoningContent("m1", "look"),
      ev.reasoningEnd("m1"),
      ev.toolStart("tc1", "grep"),
      ev.toolArgs("tc1", JSON.stringify({ pattern: "bug" })),
      ev.toolResult("tc1", "found it"),
      ev.textStart("m2"),
      ev.textContent("m2", "here "),
      ev.textContent("m2", "you go"),
      ev.textEnd("m2"),
      ev.runFinished("r1"),
    ]);

    expect(rows).toHaveLength(1);
    const row = rows[0]!;
    // The turn is an agent row keyed by the run id.
    expect(row.chip).toBe("agent");
    expect(row.id).toContain("r1");
    // A completed run is no longer streaming.
    expect(row.streaming).toBe(false);

    const expectedBlocks: Block[] = [
      { b: "thinking", text: "let me look" },
      {
        b: "toolcall",
        name: "grep",
        status: "ok",
        output: "found it",
      },
      { b: "p", runs: [{ t: "text", v: "here you go" }] },
    ];
    expect(row.blocks).toEqual(expectedBlocks);
  });

  it("attributes the row to the START event's `name` (Message Post provenance)", () => {
    const named = (messageId: string, name: string): BaseEvent =>
      ({ type: EventType.TEXT_MESSAGE_START, messageId, role: "assistant", name }) as BaseEvent;
    const rows = reduceAll([
      ev.runStarted("r1"),
      named("m1", "hermes"),
      ev.textContent("m1", "on it"),
      ev.textEnd("m1"),
      ev.runFinished("r1"),
    ]);
    expect(rows).toHaveLength(1);
    // The row shows the real sender, not the generic default agent identity.
    expect(rows[0]!.who).toBe("hermes");
    expect(rows[0]!.glyph).toBe("H");
  });

  it("keeps a named live human author distinct from an agent", () => {
    const rows = reduceAll([
      ev.runStarted("r1"),
      ({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "m1",
        role: "assistant",
        name: "other-browser-user",
        kind: "human",
      }) as BaseEvent,
      ev.textContent("m1", "from another human"),
      ev.textEnd("m1"),
      ev.runFinished("r1"),
    ]);

    expect(rows[0]).toMatchObject({ who: "other-browser-user", chip: "human" });
  });

  it("merges repeated tool-call updates by id (START once, then ARGS/RESULT)", () => {
    const rows = reduceAll([
      ev.runStarted("r1"),
      ev.toolStart("tc1", "git rebase"),
      ev.toolResult("tc1", "ok, rebased"),
      ev.runFinished("r1"),
    ]);
    // Exactly one tool-call block, carrying the later result (not a duplicate).
    const toolBlocks = rows[0]!.blocks.filter((b) => b.b === "toolcall");
    expect(toolBlocks).toHaveLength(1);
    expect(toolBlocks[0]).toMatchObject({ name: "git rebase", output: "ok, rebased" });
  });

  it("appends live shell output deltas, then replaces them with the final aggregate", () => {
    const rows = reduceAll([
      ev.runStarted("r1"),
      ev.toolStart("cmd1", "cargo test"),
      ev.toolResult("cmd1", "running\\n", { append: true, status: "in_progress" }),
      ev.toolResult("cmd1", "test a ... ok\\n", { append: true, status: "in_progress" }),
      ev.toolResult("cmd1", "running\\ntest a ... ok\\n", { status: "completed" }),
      ev.runFinished("r1"),
    ]);

    const toolBlocks = rows[0]!.blocks.filter((b) => b.b === "toolcall");
    expect(toolBlocks).toHaveLength(1);
    expect(toolBlocks[0]).toMatchObject({
      name: "cargo test",
      status: "ok",
      output: "running\\ntest a ... ok\\n",
    });
  });

  it("marks the row streaming until RUN_FINISHED", () => {
    let state = newConversationState();
    for (const e of [ev.runStarted("r1"), ev.textStart("m1"), ev.textContent("m1", "typing")]) {
      state = reduceAguiEvents(state, e);
    }
    // Mid-run: the open turn shows as streaming (drives the caret + indicator).
    expect(state.messages[0]!.streaming).toBe(true);

    state = reduceAguiEvents(state, ev.runFinished("r1"));
    expect(state.messages[0]!.streaming).toBe(false);
  });

  it("repairs a partially rendered run when reconnect replays the same run id", () => {
    let state = newConversationState();
    for (const e of [
      ev.runStarted("r1"),
      ev.textStart("m1"),
      ev.textContent("m1", "par"),
    ]) {
      state = reduceAguiEvents(state, e);
    }
    expect(state.messages).toHaveLength(1);
    expect(JSON.stringify(state.messages[0]!.blocks)).toContain("par");
    expect(state.messages[0]!.streaming).toBe(true);

    for (const e of [
      ev.runStarted("r1"),
      ev.textStart("m1"),
      ev.textContent("m1", "complete"),
      ev.textEnd("m1"),
      ev.runFinished("r1"),
    ]) {
      state = reduceAguiEvents(state, e);
    }

    expect(state.messages).toHaveLength(1);
    expect(state.messages[0]!.id).toBe("agui:r1");
    expect(state.messages[0]!.streaming).toBe(false);
    expect(state.messages[0]!.blocks).toEqual([{ b: "p", runs: [{ t: "text", v: "complete" }] }]);
  });

  it("opens a fresh row per run (a second observed turn is its own message)", () => {
    const rows = reduceAll([
      ev.runStarted("r1"),
      ev.textStart("m1"),
      ev.textContent("m1", "first"),
      ev.textEnd("m1"),
      ev.runFinished("r1"),
      ev.runStarted("r2"),
      ev.textStart("m2"),
      ev.textContent("m2", "second"),
      ev.textEnd("m2"),
      ev.runFinished("r2"),
    ]);
    expect(rows).toHaveLength(2);
    expect(rows[0]!.blocks).toEqual([{ b: "p", runs: [{ t: "text", v: "first" }] }]);
    expect(rows[1]!.blocks).toEqual([{ b: "p", runs: [{ t: "text", v: "second" }] }]);
  });

  it("accumulates consecutive text deltas into a single paragraph block", () => {
    const rows = reduceAll([
      ev.runStarted("r1"),
      ev.textStart("m1"),
      ev.textContent("m1", "a"),
      ev.textContent("m1", "b"),
      ev.textContent("m1", "c"),
      ev.runFinished("r1"),
    ]);
    expect(rows[0]!.blocks).toEqual([{ b: "p", runs: [{ t: "text", v: "abc" }] }]);
  });
});

describe("parseAguiData — AG-UI SSE `data:` payload → BaseEvent", () => {
  it("JSON-parses a frame payload into an event", () => {
    const frame = JSON.stringify({ type: EventType.TEXT_MESSAGE_CONTENT, messageId: "m1", delta: "hi" });
    expect(parseAguiData(frame)).toEqual({
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "m1",
      delta: "hi",
    });
  });

  it("returns null for blank / malformed payloads (never throws mid-stream)", () => {
    expect(parseAguiData("")).toBeNull();
    expect(parseAguiData("   ")).toBeNull();
    expect(parseAguiData("{not json")).toBeNull();
  });
});

describe("observeSessionUrl — canonical Gateway session stream", () => {
  it("targets the Gateway AG-UI view and carries its opaque cursor", () => {
    expect(observeSessionUrl("s_design")).toBe(
      "/api/v1/agent-sessions/s_design/events?view=agui",
    );
    expect(observeSessionUrl("s design", "epoch:42")).toBe(
      "/api/v1/agent-sessions/s%20design/events?view=agui&after=epoch%3A42",
    );
  });
});

describe("AG-UI WebSocket transport helpers", () => {
  it("rewrites observe URLs to the gateway WebSocket endpoint", () => {
    expect(
      observeWebSocketUrl(
        "/api/agui/observe?session=otto&afterId=42",
        new URL("http://localhost:4100/agent/otto"),
      ),
    ).toBe("ws://localhost:4100/api/agui/ws?session=otto&afterId=42");
  });

  it("uses wss when the page is served over https", () => {
    expect(
      observeWebSocketUrl(
        "/api/agui/observe?thread=nexus-project&after=10&afterRowid=2",
        new URL("https://nexus.example.test/c/nexus-project"),
      ),
    ).toBe("wss://nexus.example.test/api/agui/ws?thread=nexus-project&after=10&afterRowid=2");
  });

  it("keeps SSE as the default transport and enables WS only by flag", () => {
    expect(resolveAguiTransport()).toBe("sse");
    expect(resolveAguiTransport("sse")).toBe("sse");
    expect(resolveAguiTransport("ws")).toBe("ws");
    expect(resolveAguiTransport("bogus")).toBe("sse");
  });
});

describe("AguiWebSocketSource", () => {
  class FakeWebSocket extends EventTarget {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSING = 2;
    static readonly CLOSED = 3;
    static instances: FakeWebSocket[] = [];

    readyState = FakeWebSocket.CONNECTING;
    sent: string[] = [];
    readonly url: string;
    readonly protocols: string | string[] | undefined;

    constructor(url: string, protocols?: string | string[]) {
      super();
      this.url = url;
      this.protocols = protocols;
      FakeWebSocket.instances.push(this);
    }

    send(data: string): void {
      this.sent.push(data);
    }

    close(): void {
      this.readyState = FakeWebSocket.CLOSED;
      this.dispatchEvent(new Event("close"));
    }

    open(): void {
      this.readyState = FakeWebSocket.OPEN;
      this.dispatchEvent(new Event("open"));
    }

    message(data: string): void {
      this.dispatchEvent(new MessageEvent("message", { data }));
    }
  }

  function installFakeWebSocket(): typeof FakeWebSocket {
    FakeWebSocket.instances = [];
    vi.stubGlobal("WebSocket", FakeWebSocket);
    return FakeWebSocket;
  }

  it("forwards raw AG-UI JSON frames while handling control frames internally", () => {
    document.cookie = "nexus_csrf=csrf-source; Path=/";
    const Socket = installFakeWebSocket();
    const source = new AguiWebSocketSource("/api/agui/observe?session=otto");
    const socket = Socket.instances[0]!;
    const messages: string[] = [];
    source.onmessage = (ev) => messages.push(ev.data);

    socket.open();
    socket.message(JSON.stringify({ t: "pong" }));
    socket.message(JSON.stringify({ type: EventType.RUN_STARTED, threadId: "otto", runId: "r1" }));

    expect(socket.url).toBe("ws://localhost:3000/api/agui/ws?session=otto");
    expect(socket.protocols).toEqual(["nexus-v1", "nexus-csrf.csrf-source"]);
    expect(messages).toEqual([
      JSON.stringify({ type: EventType.RUN_STARTED, threadId: "otto", runId: "r1" }),
    ]);
    source.close();
  });

  it("sends input envelopes and resolves them from input.ack", async () => {
    const Socket = installFakeWebSocket();
    const source = new AguiWebSocketSource("/api/agui/observe?session=otto");
    const socket = Socket.instances[0]!;
    socket.open();

    const delivered = source.sendInput({
      t: "input",
      mode: "session",
      target: "otto",
      text: "hello",
      clientMessageId: "cm1",
    });
    expect(socket.sent.at(-1)).toBe(
      JSON.stringify({
        t: "input",
        mode: "session",
        target: "otto",
        text: "hello",
        clientMessageId: "cm1",
      }),
    );

    socket.message(JSON.stringify({ t: "input.ack", clientMessageId: "cm1", delivered: true }));
    await expect(delivered).resolves.toBe(true);
    source.close();
  });

  it("sends heartbeat pings, accepts pong, and errors after missed pongs", () => {
    vi.useFakeTimers();
    const Socket = installFakeWebSocket();
    const source = new AguiWebSocketSource("/api/agui/observe?session=otto");
    const socket = Socket.instances[0]!;
    const errors: unknown[] = [];
    source.onerror = (err) => errors.push(err);
    socket.open();

    vi.advanceTimersByTime(15_000);
    expect(socket.sent.at(-1)).toBe(JSON.stringify({ t: "ping" }));
    socket.message(JSON.stringify({ t: "pong" }));
    vi.advanceTimersByTime(15_000);
    expect(socket.sent.at(-1)).toBe(JSON.stringify({ t: "ping" }));
    vi.advanceTimersByTime(30_000);

    expect(socket.readyState).toBe(FakeWebSocket.CLOSED);
    expect(errors).toHaveLength(1);
    source.close();
  });
});

describe("observeCursorsFromEvent", () => {
  it("extracts the compound Message Post cursor from AG-UI metadata", () => {
    expect(
      observeCursorsFromEvent({
        type: EventType.TEXT_MESSAGE_START,
        messageId: "m1",
        createdAt: 1_782_976_600_000,
        cursor: { createdAt: 1_782_976_600_000, rowid: 42 },
      } as BaseEvent),
    ).toEqual({
      messageAfter: 1_782_976_600_000,
      messageAfterRowid: 42,
    });
  });
});

describe("thread backlog hydration", () => {
  it("maps daemon history rows into pane messages and marks the operator as you", () => {
    const rows = historyRowsToPaneMessages(
      [
        { messageId: "m1", from: "bianca", body: "from me" },
        { messageId: "m2", from: "blake", body: "from blake" },
      ],
      { who: "bianca", glyph: "B" },
    );

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ id: "m1", who: "bianca", chip: "you", isYou: true });
    expect(rows[1]).toMatchObject({ id: "m2", who: "blake", chip: "agent", glyph: "B" });
    expect(rows[0]!.blocks).toEqual([{ b: "p", runs: [{ t: "text", v: "from me" }] }]);
  });

  it("keeps a non-self human backlog author distinct from an agent", () => {
    expect(historyRowsToPaneMessages([
      {
        messageId: "m-human",
        from: "other-browser-user",
        fromKind: "human",
        body: "from another human",
      },
    ], { who: "bianca", glyph: "B" })).toMatchObject([
      { id: "m-human", who: "other-browser-user", chip: "human" },
    ]);
  });

  it("keeps live rows that arrived while backlog was loading without duplicating ids", () => {
    const backlog = historyRowsToPaneMessages([{ messageId: "m1", from: "bianca", body: "old" }]);
    const live = historyRowsToPaneMessages([
      { messageId: "m1", from: "bianca", body: "old duplicate" },
      { messageId: "m2", from: "blake", body: "new live" },
    ]);

    expect(mergeBacklogMessages(backlog, live).map((m) => m.id)).toEqual(["m1", "m2"]);
  });
});

describe("operator input mirroring — no double echo, TUI-origin renders as you", () => {
  function fold(state: AguiConversationState, events: BaseEvent[]): AguiConversationState {
    let s = state;
    for (const e of events) s = reduceAguiEvents(s, e);
    return s;
  }

  it("dedupes the stream's user_input against our optimistic echo by message id", () => {
    // The web send already appended an optimistic "you" bubble + recorded a pending echo.
    let state = newConversationState({ who: "enzo", glyph: "E", chip: "agent" });
    state = appendUserMessage(state, {
      id: "you:1",
      who: "you",
      glyph: "Y",
      text: "ping",
      reconcileBy: "id",
    });

    // The same text comes back over observe (user_input), wrapped in the turn's run, then the reply.
    state = fold(state, [
      ev.runStarted("r1"),
      ev.userStart("you:1"),
      ev.textContent("you:1", "ping"),
      ev.textEnd("you:1"),
      ev.textStart("a1"),
      ev.textContent("a1", "pong"),
      ev.textEnd("a1"),
      ev.runFinished("r1"),
    ]);

    const youRows = state.messages.filter((m) => m.isYou);
    expect(youRows).toHaveLength(1); // the optimistic one only — the stream copy was dropped
    expect(state.pendingEchoes).toHaveLength(0); // pending echo consumed
    const agentRows = state.messages.filter((m) => m.chip === "agent");
    expect(agentRows).toHaveLength(1);
    expect(JSON.stringify(agentRows[0]!.blocks)).toContain("pong");
    // Order: you("ping") then agent("pong").
    expect(state.messages.map((m) => m.isYou ?? false)).toEqual([true, false]);
  });

  it("does not dedupe a same-text streamed user_input with a different message id", () => {
    let state = newConversationState({ who: "enzo", glyph: "E", chip: "agent" });
    state = appendUserMessage(state, {
      id: "you:1",
      who: "you",
      glyph: "Y",
      text: "ping",
      reconcileBy: "id",
    });

    state = fold(state, [
      ev.runStarted("r1"),
      ev.userStart("u-from-tui"),
      ev.textContent("u-from-tui", "ping"),
      ev.textEnd("u-from-tui"),
      ev.runFinished("r1"),
    ]);

    const youRows = state.messages.filter((m) => m.isYou);
    expect(youRows.map((m) => m.id)).toEqual(["you:1", "agui-user:u-from-tui"]);
    expect(state.pendingEchoes).toHaveLength(1);
  });

  it("renders TUI-origin user_input (no optimistic echo) as a you row before the reply", () => {
    let state = newConversationState({ who: "enzo", glyph: "E", chip: "agent" });
    state = fold(state, [
      ev.runStarted("r1"),
      ev.userStart("u1"),
      ev.textContent("u1", "typed in the TUI"),
      ev.textEnd("u1"),
      ev.textStart("a1"),
      ev.textContent("a1", "got it"),
      ev.textEnd("a1"),
      ev.runFinished("r1"),
    ]);

    expect(state.messages).toHaveLength(2);
    const [first, second] = state.messages;
    expect(first!.isYou).toBe(true);
    expect(JSON.stringify(first!.blocks)).toContain("typed in the TUI");
    expect(second!.chip).toBe("agent");
    expect(JSON.stringify(second!.blocks)).toContain("got it");
  });

  it("renders authenticated self as you while the target reply remains the target agent", () => {
    let state = newConversationState(
      { who: "ben", glyph: "B", chip: "agent" },
      { who: "alice", glyph: "A" },
    );
    state = fold(state, [
      ev.runStarted("r1"),
      ev.userStart("u-self", { name: "alice", kind: "human" }),
      ev.textContent("u-self", "from alice"),
      ev.textEnd("u-self"),
      ev.textStart("a1"),
      ev.textContent("a1", "from ben"),
      ev.textEnd("a1"),
      ev.runFinished("r1"),
    ]);

    expect(state.messages).toMatchObject([
      { who: "alice", chip: "you", isYou: true },
      { who: "ben", chip: "agent" },
    ]);
  });

  it("renders another authenticated human as human instead of you or the target", () => {
    let state = newConversationState(
      { who: "ben", glyph: "B", chip: "agent" },
      { who: "alice", glyph: "A" },
    );
    state = fold(state, [
      ev.runStarted("r1"),
      ev.userStart("u-other", { name: "peer-user", kind: "human" }),
      ev.textContent("u-other", "from peer-user"),
      ev.textEnd("u-other"),
      ev.runFinished("r1"),
    ]);

    expect(state.messages).toMatchObject([
      { who: "peer-user", chip: "human" },
    ]);
    expect(state.messages[0]).not.toHaveProperty("isYou");
    expect(state.messages[0]?.who).not.toBe("ben");
  });

  it("drops an agent run that carried only the operator's input (no empty bubble)", () => {
    let state = newConversationState({ who: "enzo", glyph: "E", chip: "agent" });
    state = fold(state, [
      ev.runStarted("r1"),
      ev.userStart("u1"),
      ev.textContent("u1", "just me"),
      ev.textEnd("u1"),
      ev.runFinished("r1"),
    ]);
    // Only the you row remains; the empty agent run was dropped.
    expect(state.messages).toHaveLength(1);
    expect(state.messages[0]!.isYou).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Task 9a — channel path: committed message.created dedup (Lane A / /c)
// ---------------------------------------------------------------------------
//
// `observeMessagePost` wraps each committed message.created in its own run:
//   RUN_STARTED → TEXT_MESSAGE_START(role:user) → CONTENT → END → RUN_FINISHED
// The optimistic echo was already added by `appendUserMessage` + pendingEchoes.
// The stream's self-authored user row MUST be dropped — exactly one "hello" shown.

describe("useMessagePosts — channel path: one human post, exactly one bubble", () => {
  function fold(state: AguiConversationState, events: BaseEvent[]): AguiConversationState {
    let s = state;
    for (const e of events) s = reduceAguiEvents(s, e);
    return s;
  }

  it("reconciles optimistic send('hello') against a committed message.created (selfName match) — ONE row", () => {
    // Step 1: optimistic send — the "you" bubble appears immediately.
    let state = newConversationState();
    state = appendUserMessage(state, { id: "you:1", who: "you", glyph: "Y", text: "hello" });

    // Step 2: the committed message.created arrives as a standalone AG-UI run
    // (observeMessagePost wraps it in RUN_STARTED/RUN_FINISHED).
    // selfName === from → role:"user" TEXT_MESSAGE.
    state = fold(state, [
      ev.runStarted("msg-run-1"),
      {
        type: EventType.TEXT_MESSAGE_START,
        messageId: "msg-1",
        role: "user",
        createdAt: 1_782_976_800_000,
      } as BaseEvent,
      ev.textContent("msg-1", "hello"),
      ev.textEnd("msg-1"),
      ev.runFinished("msg-run-1"),
    ]);

    // Assert: exactly ONE "hello" message — the optimistic bubble, NOT a duplicate.
    const youRows = state.messages.filter((m) => m.isYou);
    expect(youRows).toHaveLength(1);
    expect(state.messages).toHaveLength(1);
    expect(state.pendingEchoes).toHaveLength(0); // pending echo consumed
    expect(youRows[0]!.id).toBe("msg-1"); // durable daemon id, so history refresh dedupes it
    const text = JSON.stringify(youRows[0]!.blocks);
    expect(text).toContain("hello");
  });

  it("regression guard: a committed message.created from another participant renders as attributed assistant row (not deduped)", () => {
    // No optimistic echo — this post came from "alice", not from us.
    let state = newConversationState();

    // alice's committed message arrives: selfName !== from → role:"assistant" TEXT_MESSAGE.
    state = fold(state, [
      ev.runStarted("msg-run-2"),
      {
        type: EventType.TEXT_MESSAGE_START,
        messageId: "msg-2",
        role: "assistant",
        name: "alice",
        createdAt: 1_782_976_800_001,
      } as BaseEvent,
      ev.textContent("msg-2", "hey everyone"),
      ev.textEnd("msg-2"),
      ev.runFinished("msg-run-2"),
    ]);

    // Assert: alice's message rendered as an agent row, not deduped.
    expect(state.messages).toHaveLength(1);
    const row = state.messages[0]!;
    expect(row.isYou).toBeFalsy();
    expect(row.chip).toBe("agent");
    expect(row.id).toBe("msg-2"); // durable daemon id, matching historyRowsToPaneMessages
    const text = JSON.stringify(row.blocks);
    expect(text).toContain("hey everyone");
  });

  it("ignores a replayed committed message when history already contains that message id", () => {
    let state = newConversationState();
    state = {
      ...state,
      messages: historyRowsToPaneMessages([
        { messageId: "msg-replay", from: "alice", body: "already in history" },
      ]),
    };

    state = fold(state, [
      ev.runStarted("transient-run-replay"),
      {
        type: EventType.TEXT_MESSAGE_START,
        messageId: "msg-replay",
        role: "assistant",
        name: "alice",
        createdAt: 1_782_976_800_002,
      } as BaseEvent,
      ev.textContent("msg-replay", "already in history"),
      ev.textEnd("msg-replay"),
      ev.runFinished("transient-run-replay"),
    ]);

    expect(state.messages).toHaveLength(1);
    expect(state.messages[0]!.id).toBe("msg-replay");
    expect(JSON.stringify(state.messages[0]!.blocks)).toContain("already in history");
  });
});
