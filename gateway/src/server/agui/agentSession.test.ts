// Lane B guardrail test for `observeAgentSession`.
//
// Asserts:
//   1. `agent.update`s (thinking/text/tool_call) render as streaming AG-UI events.
//   2. A `user_input` update also renders (operator's typed line).
//   3. An `agent.update` `turn_end` closes the run (RUN_FINISHED) and re-opens on
//      the next renderable update (continuous multi-turn watching).
//   4. A `message.created` event produces NO output and does NOT close the run —
//      Lane B is structurally incapable of acting on Lane A signals.
import { afterEach, describe, it, expect, vi } from "vitest";
import { EventType } from "@ag-ui/client";
import { observeAgentSession } from "@server/agui/agentSession";
import type { RunDeps } from "@server/agui/_sseCore";
import type { WsEvent } from "@shared/types";
import { makeRelay, decodeSse, eventTypes } from "@server/agui/_test-relay";

// AG-UI event types that are NEVER legal from Lane B (committed message signals).
// Lane B only emits agent-activity-derived frames + run lifecycle.
const LANE_A_ONLY_TYPES = new Set(["message.created"]);

afterEach(() => {
  vi.useRealTimers();
});

/** Drive the stream: emit relay events, wait a tick, read up to stopOn. */
async function driveAndRead(
  stream: ReadableStream<Uint8Array>,
  drive: () => void,
  stopOn: string = "RUN_FINISHED",
): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";

  drive();
  await Promise.resolve();
  await Promise.resolve();

  for (let i = 0; i < 20; i++) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) text += decoder.decode(value, { stream: true });
    if (text.includes(stopOn)) break;
  }
  await reader.cancel();
  return text;
}

describe("observeAgentSession (Lane B guardrail)", () => {
  it("keeps an idle Agent Session SSE alive with a comment heartbeat", async () => {
    vi.useFakeTimers();
    const relay = makeRelay();
    const stream = observeAgentSession("ben", {
      createRelay: relay.factory,
      heartbeatIntervalMs: 1_000,
    });
    const reader = stream.getReader();
    const next = reader.read();
    await vi.advanceTimersByTimeAsync(1_000);

    const heartbeat = await next;
    expect(new TextDecoder().decode(heartbeat.value)).toBe(": ping\n\n");
    await reader.cancel();
  });

  it("pauses the relay when one projected event fills the SSE queue and resumes on pull", async () => {
    let onEvent: ((event: WsEvent) => unknown) | undefined;
    const pause = vi.fn();
    const resume = vi.fn();
    const stream = observeAgentSession("ben", {
      createRelay: (opts) => {
        onEvent = opts.onEvent;
        return { ready: Promise.resolve(), close: vi.fn(), pause, resume };
      },
    });

    onEvent?.({
      type: "agent.update",
      sessionId: "s_ben",
      kind: "text",
      data: { text: "fills queue" },
    });
    expect(pause).toHaveBeenCalledTimes(1);

    const reader = stream.getReader();
    for (let i = 0; i < 10 && resume.mock.calls.length === 0; i++) {
      await reader.read();
    }
    expect(resume).toHaveBeenCalled();
    await reader.cancel();
  });

  it("renders thinking/text/tool_call agent.updates as AG-UI events", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };
    const stream = observeAgentSession("ben", deps);

    const text = await driveAndRead(stream, () => {
      relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "thinking", data: { text: "let me look" } });
      relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "tool_call", data: { id: "tc1", title: "grep", status: "pending" } });
      relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "tool_call", data: { id: "tc1", status: "completed", content: "found it" } });
      relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "text", data: { text: "here you go" } });
      // turn_end closes the run.
      relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "turn_end", data: {} });
    });

    const events = decodeSse(text);
    const types = eventTypes(events);

    expect(types).toContain("RUN_STARTED");
    expect(types).toContain(EventType.REASONING_MESSAGE_START);
    expect(types).toContain(EventType.REASONING_MESSAGE_CONTENT);
    expect(types).toContain(EventType.REASONING_MESSAGE_END);
    expect(types).toContain(EventType.TOOL_CALL_START);
    expect(types).toContain(EventType.TOOL_CALL_RESULT);
    expect(types).toContain(EventType.TEXT_MESSAGE_START);
    expect(types).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(types).toContain(EventType.TEXT_MESSAGE_END);
    expect(types).toContain("RUN_FINISHED");

    // Text content is the agent's message.
    const content = events.find((e) => e["type"] === EventType.TEXT_MESSAGE_CONTENT);
    expect(content?.["delta"]).toBe("here you go");
  });

  it("renders a user_input update (operator's typed line)", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };
    const stream = observeAgentSession("ben", deps);

    const text = await driveAndRead(stream, () => {
      // user_input is a real wire kind not yet in the generated union — cast required (matches _harness-e2e.ts pattern).
      relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "user_input", data: { text: "go ahead" } } as unknown as WsEvent);
      relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "text", data: { text: "on it" } });
      relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "turn_end", data: {} });
    });

    const events = decodeSse(text);
    const types = eventTypes(events);

    // user_input renders as a user-role text message block.
    expect(types).toContain(EventType.TEXT_MESSAGE_START);
    expect(types).toContain(EventType.TEXT_MESSAGE_CONTENT);

    // The user message comes first (user_input), then assistant text.
    const starts = events.filter((e) => e["type"] === EventType.TEXT_MESSAGE_START);
    expect(starts[0]?.["role"]).toBe("user");
    expect(starts[1]?.["role"]).toBe("assistant");
  });

  it("turn_end closes the current run and a new run opens on the next turn", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };
    const stream = observeAgentSession("ben", deps);

    // First turn.
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "text", data: { text: "turn 1" } });
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "turn_end", data: {} });
    await Promise.resolve();
    await Promise.resolve();

    // Second turn.
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "text", data: { text: "turn 2" } });
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "turn_end", data: {} });
    await Promise.resolve();
    await Promise.resolve();

    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let text = "";
    let runFinishedCount = 0;
    for (let i = 0; i < 30; i++) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) {
        const chunk = decoder.decode(value, { stream: true });
        text += chunk;
        runFinishedCount = (text.match(/RUN_FINISHED/g) ?? []).length;
      }
      if (runFinishedCount >= 2) break;
    }
    await reader.cancel();

    const events = decodeSse(text);
    const types = eventTypes(events);

    // Two complete runs: RUN_STARTED … RUN_FINISHED, RUN_STARTED … RUN_FINISHED.
    const runStartedCount = types.filter((t) => t === "RUN_STARTED").length;
    expect(runStartedCount).toBe(2);
    expect(runFinishedCount).toBe(2);

    // Both text contents present.
    const contents = events
      .filter((e) => e["type"] === EventType.TEXT_MESSAGE_CONTENT)
      .map((e) => e["delta"]);
    expect(contents).toContain("turn 1");
    expect(contents).toContain("turn 2");
  });

  it("derives the materializer's turn id as runId when the relay stamps streamEventId", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };
    const stream = observeAgentSession("ben", deps);

    const text = await driveAndRead(stream, () => {
      // Store-relay events carry the stream row id; the run must adopt the SAME
      // id the daemon materializer will assign (`turn_<sessionId>_<firstId>`) so
      // a later history replay of this turn is idempotent for id-keyed clients.
      relay.emit({
        type: "agent.update",
        sessionId: "s_ben",
        kind: "text",
        data: { text: "hello", streamEventId: 41 },
      });
      relay.emit({
        type: "agent.update",
        sessionId: "s_ben",
        kind: "turn_end",
        data: { streamEventId: 42 },
      });
    });

    const events = decodeSse(text);
    const started = events.find((e) => e["type"] === "RUN_STARTED");
    const finished = events.find((e) => e["type"] === "RUN_FINISHED");
    expect(started?.["runId"]).toBe("turn_s_ben_41");
    expect(finished?.["runId"]).toBe("turn_s_ben_41");
  });

  it("message.created produces NO output and does NOT close the run (Lane B purity)", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };
    const stream = observeAgentSession("ben", deps);

    // Start a turn so a run is open.
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "text", data: { text: "working" } });
    await Promise.resolve();
    await Promise.resolve();

    // Inject a message.created — Lane A event. Lane B must ignore it entirely.
    relay.emit({ type: "message.created", messageId: "m_commit" });
    await Promise.resolve();
    await Promise.resolve();

    // The run is still open (no RUN_FINISHED yet). Now close it via the correct Lane B signal.
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "turn_end", data: {} });
    await Promise.resolve();
    await Promise.resolve();

    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let text = "";
    for (let i = 0; i < 20; i++) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) text += decoder.decode(value, { stream: true });
      if (text.includes("RUN_FINISHED")) break;
    }
    await reader.cancel();

    const events = decodeSse(text);
    const types = eventTypes(events);

    // Exactly ONE RUN_STARTED and ONE RUN_FINISHED (message.created did not emit a second).
    expect(types.filter((t) => t === "RUN_STARTED")).toHaveLength(1);
    expect(types.filter((t) => t === "RUN_FINISHED")).toHaveLength(1);

    // No Lane A event type leaked into the output.
    for (const ev of events) {
      expect(LANE_A_ONLY_TYPES.has(ev["type"] as string)).toBe(false);
    }

    // The agent text DID render (the run was not prematurely closed before it).
    const content = events.find((e) => e["type"] === EventType.TEXT_MESSAGE_CONTENT);
    expect(content?.["delta"]).toBe("working");
  });

  it("turn_end with no prior renderable update does NOT emit an empty RUN_STARTED", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };
    const stream = observeAgentSession("ben", deps);

    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "turn_end", data: {} });
    await Promise.resolve();
    await Promise.resolve();

    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let text = "";
    // Read a bounded number of frames — there should be none at all.
    for (let i = 0; i < 5; i++) {
      const chunk = await Promise.race([
        reader.read(),
        new Promise<{ value: undefined; done: true }>((r) => setTimeout(() => r({ value: undefined, done: true }), 5)),
      ]);
      if (chunk.done) break;
      if (chunk.value) text += decoder.decode(chunk.value, { stream: true });
    }
    await reader.cancel();

    const events = decodeSse(text);
    // No lifecycle frames emitted — a lone turn_end with no open run is a no-op.
    expect(events.filter((e) => e["type"] === "RUN_STARTED")).toHaveLength(0);
    expect(events.filter((e) => e["type"] === "RUN_FINISHED")).toHaveLength(0);
  });

  it("Agent Session observe keeps ignoring Message Post noise", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };

    const stream = observeAgentSession("ben", deps);

    // Confirm message.created via observe() delegation is also ignored.
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "text", data: { text: "agent says" } });
    relay.emit({ type: "message.created", messageId: "m_ignored" });
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "turn_end", data: {} });

    await Promise.resolve();
    await Promise.resolve();

    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let text = "";
    for (let i = 0; i < 20; i++) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) text += decoder.decode(value, { stream: true });
      if (text.includes("RUN_FINISHED")) break;
    }
    await reader.cancel();

    const events = decodeSse(text);
    // Run opened and closed cleanly, agent content present.
    expect(eventTypes(events)).toContain("RUN_STARTED");
    expect(eventTypes(events)).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(eventTypes(events)).toContain("RUN_FINISHED");
    // Only ONE run (message.created did not trigger a second).
    expect(events.filter((e) => e["type"] === "RUN_STARTED")).toHaveLength(1);
  });
});
