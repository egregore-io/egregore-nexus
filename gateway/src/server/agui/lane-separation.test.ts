// Task 11 — Lane separation guard tests.
//
// These tests encode the separation invariant so that a future edit reconnecting
// the two pipelines fails here rather than silently leaking events across lanes.
//
// Lane A (Message Post): `observeMessagePost` renders ONLY committed `message.created`
//   events. No `agent.update`-derived frame ever leaves this pipeline.
// Lane B (Agent Session): `observeAgentSession` renders ONLY `agent.update`-derived
//   frames. A `message.created` event never causes it to emit anything.
//
// Guard tests (runtime):
//   1. Mixed stream → `observeMessagePost` emits only committed-message frames.
//   2. Mixed stream → `observeAgentSession` emits only agent-update-derived frames.
//
// Type tests (compile-time, @ts-expect-error):
//   3. `messageToAguiEvents` rejects an `AgentUpdateEvent`.
//   4. `acpToAguiEvents` rejects a `Message`.
//
// The runtime guards PASS against the current (correct) pipelines. They would fail
// if someone routes one lane's event through the other pipeline.
import { describe, it, expect } from "vitest";
import { EventType } from "@ag-ui/client";
import type { Message } from "@shared/types";
import { observeMessagePost } from "@server/agui/messagePost";
import { observeAgentSession } from "@server/agui/agentSession";
import { messageToAguiEvents } from "@server/agui/mapMessage";
import {
  acpToAguiEvents,
  newBracket,
  type AgentUpdateEvent,
} from "@server/agui/mapAgentUpdate";
import type { RunDeps } from "@server/agui/_sseCore";
import { makeRelay, decodeSse, eventTypes } from "@server/agui/_test-relay";
import type { MessagePostSourceFactory } from "@server/messagePost/readView";

// ── AG-UI event types that may ONLY come from Lane B (agent activity). ──────────
// If any of these appear in Lane A output the separation has been violated.
const LANE_B_ONLY_EVENT_TYPES = new Set([
  EventType.REASONING_MESSAGE_START,
  EventType.REASONING_MESSAGE_CONTENT,
  EventType.REASONING_MESSAGE_END,
  EventType.TOOL_CALL_START,
  EventType.TOOL_CALL_ARGS,
  EventType.TOOL_CALL_RESULT,
  EventType.STATE_DELTA,
  EventType.CUSTOM,
]);

/** Minimal `Message` factory for tests. */
function msg(id: string, from: string, body: string): Message {
  return {
    id,
    from,
    body,
    scope: "thread",
    project: "p",
    provenance: "agent",
    createdAt: 1,
  } as unknown as Message;
}

function makeMessageSource() {
  let onMessage: ((message: Message) => void) | null = null;
  const factory: MessagePostSourceFactory = (opts) => {
    onMessage = opts.onMessage;
    return {
      ready: Promise.resolve(),
      close() {},
    };
  };
  return {
    factory,
    emit(message: Message) {
      onMessage?.(message);
    },
  };
}

/** Drive the stream: emit relay events, tick twice (async chain), read until stopOn. */
async function driveAndRead(
  stream: ReadableStream<Uint8Array>,
  drive: () => void,
  stopOn = "RUN_FINISHED",
): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";

  drive();
  // Allow the async promise-chain in observeMessagePost to settle.
  await new Promise((r) => setTimeout(r, 0));
  await new Promise((r) => setTimeout(r, 0));

  for (let i = 0; i < 20; i++) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) text += decoder.decode(value, { stream: true });
    if (text.includes(stopOn)) break;
  }
  await reader.cancel();
  return text;
}

// ── Guard test 1: Lane A rejects all agent.update noise ─────────────────────────
describe("lane-separation guard: observeMessagePost (Lane A)", () => {
  it("emits ONLY committed-message frames and never opens the agent-update relay", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
      createRelay: () => {
        throw new Error("Lane A must not open the agent-update relay");
      },
    };

    const stream = observeMessagePost({ verb: "post", thread: "t1" }, deps);
    const text = await driveAndRead(stream, () => {
      // Only committed message should render.
      source.emit(msg("m_commit", "ben", "committed"));
    });

    const events = decodeSse(text);

    // No Lane B event type must appear.
    for (const ev of events) {
      const t = ev["type"] as EventType;
      expect(LANE_B_ONLY_EVENT_TYPES.has(t)).toBe(false);
    }

    // The committed message DID render correctly.
    expect(eventTypes(events)).toContain("RUN_STARTED");
    expect(eventTypes(events)).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(eventTypes(events)).toContain("RUN_FINISHED");
    const content = events.find((e) => e["type"] === EventType.TEXT_MESSAGE_CONTENT);
    expect(content?.["delta"]).toBe("committed");
  });
});

// ── Guard test 2: Lane B rejects all message.created noise ──────────────────────
describe("lane-separation guard: observeAgentSession (Lane B)", () => {
  it("emits ONLY agent.update-derived frames even when message.created events are present", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };

    const stream = observeAgentSession("ben", deps);
    const text = await driveAndRead(stream, () => {
      // Kick off an agent turn.
      relay.emit({
        type: "agent.update",
        sessionId: "s_ben",
        kind: "text",
        data: { text: "agent output" },
      });
      // Inject Lane A noise mid-turn — Lane B must ignore entirely.
      relay.emit({ type: "message.created", messageId: "m_ignored" });
      relay.emit({ type: "message.created", messageId: "m_also_ignored" });
      // Close the turn.
      relay.emit({
        type: "agent.update",
        sessionId: "s_ben",
        kind: "turn_end",
        data: {},
      });
    });

    const events = decodeSse(text);

    // No raw "message.created" string must appear in the SSE payload.
    expect(text).not.toContain('"message.created"');

    // Exactly ONE complete run (message.created did not open/close an extra run).
    expect(events.filter((e) => e["type"] === "RUN_STARTED")).toHaveLength(1);
    expect(events.filter((e) => e["type"] === "RUN_FINISHED")).toHaveLength(1);

    // Agent content rendered.
    const content = events.find((e) => e["type"] === EventType.TEXT_MESSAGE_CONTENT);
    expect(content?.["delta"]).toBe("agent output");
  });

  it("mixed stream with both kinds: only agent.update-derived frames emitted", async () => {
    const relay = makeRelay();
    const deps: RunDeps = { createRelay: relay.factory };

    const stream = observeAgentSession("ada", deps);
    const text = await driveAndRead(stream, () => {
      // Interleave Lane A and Lane B events.
      relay.emit({ type: "message.created", messageId: "m_pre" });
      relay.emit({
        type: "agent.update",
        sessionId: "s_ben",
        kind: "thinking",
        data: { text: "hmm" },
      });
      relay.emit({ type: "message.created", messageId: "m_mid" });
      relay.emit({
        type: "agent.update",
        sessionId: "s_ben",
        kind: "text",
        data: { text: "result" },
      });
      relay.emit({ type: "message.created", messageId: "m_post" });
      relay.emit({
        type: "agent.update",
        sessionId: "s_ben",
        kind: "turn_end",
        data: {},
      });
    });

    const events = decodeSse(text);

    // No Lane A type leaked.
    expect(text).not.toContain('"message.created"');

    // Run lifecycle is clean.
    expect(events.filter((e) => e["type"] === "RUN_STARTED")).toHaveLength(1);
    expect(events.filter((e) => e["type"] === "RUN_FINISHED")).toHaveLength(1);

    // Agent types present.
    expect(eventTypes(events)).toContain(EventType.REASONING_MESSAGE_START);
    expect(eventTypes(events)).toContain(EventType.TEXT_MESSAGE_CONTENT);
  });
});

// ── Type tests (compile-time only) ──────────────────────────────────────────────
// These catch cross-lane type misuse at compile time. The functions below are NEVER
// called — they exist purely so `tsc --noEmit` checks the argument types.
//
// If a mapper signature is ever widened to accept the other lane's input type, the
// `@ts-expect-error` comment becomes a "Unused '@ts-expect-error' directive" error,
// causing `tsc --noEmit` (and thus CI) to fail.

/** @ts-expect-error — messageToAguiEvents only accepts Message, not AgentUpdateEvent.
 *  If this line stops being a TS error, the types have widened to allow cross-lane
 *  input — a violation of the Lane A / Lane B separation invariant. */
function _typeGuard_laneA_rejects_agentUpdate(ev: AgentUpdateEvent): void {
  // @ts-expect-error
  messageToAguiEvents(ev);
}

/** @ts-expect-error — acpToAguiEvents only accepts AgentUpdateEvent, not Message.
 *  If this line stops being a TS error, the types have widened to allow cross-lane
 *  input — a violation of the Lane B / Lane A separation invariant. */
function _typeGuard_laneB_rejects_message(m: Message): void {
  const bracket = newBracket();
  // @ts-expect-error
  acpToAguiEvents(m, bracket);
}

// Prevent "declared but never read" warnings (dead code linters).
void _typeGuard_laneA_rejects_agentUpdate;
void _typeGuard_laneB_rejects_message;

describe("lane-separation type tests (compile-time)", () => {
  it("type guards are defined (compile-time assertions verified by tsc --noEmit)", () => {
    // The real assertions are the @ts-expect-error lines above. If those lines ever
    // stop being TS errors, tsc --noEmit fails (unused-expect-error directive).
    // This test just confirms the module loaded without issues.
    expect(typeof _typeGuard_laneA_rejects_agentUpdate).toBe("function");
    expect(typeof _typeGuard_laneB_rejects_message).toBe("function");
  });
});
