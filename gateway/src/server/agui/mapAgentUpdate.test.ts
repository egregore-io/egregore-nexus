// Tests for `acpToAguiEvents` (agent.update → AG-UI BaseEvent[]) and its
// bracket machinery. Relocated from `map.test.ts`.
import { describe, it, expect } from "vitest";
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";
import {
  acpToAguiEvents,
  closeRun,
  newBracket,
} from "@server/agui/mapAgentUpdate";
import type { AgentUpdateEvent } from "@server/agui/mapAgentUpdate";

// Build a contract `agent.update` event (the shape `acpToAguiEvents` consumes).
function upd(
  kind: AgentUpdateEvent["kind"],
  data: unknown,
  sessionId = "s_ben",
): AgentUpdateEvent {
  return { type: "agent.update", sessionId, kind, data };
}

// Drive a sequence of updates through the mapper, threading the bracket, and
// return the flat list of emitted events (+ the final bracket for closeRun).
function run(updates: AgentUpdateEvent[]) {
  let bracket = newBracket();
  const events: BaseEvent[] = [];
  for (const u of updates) {
    const out = acpToAguiEvents(u, bracket);
    events.push(...out.events);
    bracket = out.bracket;
  }
  return { events, bracket };
}

const types = (events: BaseEvent[]) => events.map((e) => e.type);

describe("acpToAguiEvents (agent.update → AG-UI BaseEvent[])", () => {
  it("brackets the canonical turn: thinking a/b → tool(id1) start+result → text → closeRun", () => {
    const { events, bracket } = run([
      upd("thinking", { text: "a" }),
      upd("thinking", { text: "b" }),
      upd("tool_call", { id: "id1", title: "grep", status: "pending" }),
      upd("tool_call", { id: "id1", status: "completed", content: "done" }),
      upd("text", { text: "hi" }),
    ]);
    const final = [...events, ...closeRun(bracket)];

    expect(types(final)).toEqual([
      EventType.REASONING_MESSAGE_START,
      EventType.REASONING_MESSAGE_CONTENT,
      EventType.REASONING_MESSAGE_CONTENT,
      EventType.REASONING_MESSAGE_END,
      EventType.TOOL_CALL_START,
      EventType.TOOL_CALL_RESULT,
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
    ]);

    // Reasoning deltas carry the chunk text.
    const contents = final.filter(
      (e) => e.type === EventType.REASONING_MESSAGE_CONTENT,
    ) as Array<BaseEvent & { delta: string }>;
    expect(contents.map((e) => e.delta)).toEqual(["a", "b"]);

    // Tool-call start/result keyed by the data id.
    const start = final.find((e) => e.type === EventType.TOOL_CALL_START) as
      | (BaseEvent & { toolCallId: string; toolCallName: string })
      | undefined;
    expect(start?.toolCallId).toBe("id1");
    expect(start?.toolCallName).toBe("grep");
    const result = final.find((e) => e.type === EventType.TOOL_CALL_RESULT) as
      | (BaseEvent & { toolCallId: string; content: string })
      | undefined;
    expect(result?.toolCallId).toBe("id1");
    expect(result?.content).toBe("done");

    // The text delta carries "hi".
    const textContent = final.find(
      (e) => e.type === EventType.TEXT_MESSAGE_CONTENT,
    ) as (BaseEvent & { delta: string }) | undefined;
    expect(textContent?.delta).toBe("hi");
  });

  it("does NOT re-emit TOOL_CALL_START for an already-open tool-call id", () => {
    const { events } = run([
      upd("tool_call", { id: "id1", title: "grep" }),
      upd("tool_call", { id: "id1", input: { q: "foo" } }),
      upd("tool_call", { id: "id1", status: "completed", content: "ok" }),
    ]);
    const starts = events.filter((e) => e.type === EventType.TOOL_CALL_START);
    expect(starts).toHaveLength(1);
    // The middle update with new args emits TOOL_CALL_ARGS, the last a RESULT.
    expect(types(events)).toEqual([
      EventType.TOOL_CALL_START,
      EventType.TOOL_CALL_ARGS,
      EventType.TOOL_CALL_RESULT,
    ]);
  });

  it("keeps one native text item bracketed across an interleaved tool call", () => {
    const { events, bracket } = run([
      upd("text", { itemId: "am1", text: "checking the" }),
      upd("tool_call", {
        id: "tc1",
        tool: "shell",
        status: "completed",
        output: "focused.test.ts",
      }),
      upd("text", { itemId: "am1", text: " focused test" }),
    ]);
    const final = [...events, ...closeRun(bracket)];

    expect(types(final)).toEqual([
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TOOL_CALL_START,
      EventType.TOOL_CALL_RESULT,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
    ]);
    expect(final.filter((event) => event.type === EventType.TEXT_MESSAGE_START)).toEqual([
      expect.objectContaining({ messageId: "am1", role: "assistant" }),
    ]);
    expect(final.filter((event) => event.type === EventType.TEXT_MESSAGE_CONTENT)).toEqual([
      expect.objectContaining({ messageId: "am1", delta: "checking the" }),
      expect.objectContaining({ messageId: "am1", delta: " focused test" }),
    ]);
    expect(final.filter((event) => event.type === EventType.TEXT_MESSAGE_END)).toEqual([
      expect.objectContaining({ messageId: "am1" }),
    ]);
  });

  it("maps contract output to TOOL_CALL_RESULT so shell output is visible", () => {
    const { events } = run([
      upd("tool_call", { id: "cmd1", title: "cargo test", status: "in_progress", output: "running\\n" }),
      upd("tool_call", { id: "cmd1", title: "cargo test", status: "completed", output: "running\\nok\\n" }),
    ]);

    expect(types(events)).toEqual([
      EventType.TOOL_CALL_START,
      EventType.TOOL_CALL_RESULT,
      EventType.TOOL_CALL_RESULT,
    ]);
    const results = events.filter((e) => e.type === EventType.TOOL_CALL_RESULT) as Array<
      BaseEvent & { content: string; append?: boolean; status?: string }
    >;
    expect(results[0]).toMatchObject({ content: "running\\n", append: true, status: "in_progress" });
    expect(results[1]).toMatchObject({ content: "running\\nok\\n", append: false, status: "completed" });
  });

  it("opens a NEW message block when the message channel changes (text→thinking)", () => {
    const { events, bracket } = run([
      upd("text", { text: "x" }),
      upd("thinking", { text: "y" }),
    ]);
    const final = [...events, ...closeRun(bracket)];
    expect(types(final)).toEqual([
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END, // text block closed on channel change
      EventType.REASONING_MESSAGE_START,
      EventType.REASONING_MESSAGE_CONTENT,
      EventType.REASONING_MESSAGE_END, // closed by closeRun
    ]);
  });

  it("maps plan → STATE_DELTA (a JSON-Patch the lens reader tolerates)", () => {
    const { events } = run([
      upd("plan", { entries: [{ content: "step 1", status: "pending" }] }),
    ]);
    expect(types(events)).toEqual([EventType.STATE_DELTA]);
    const ev = events[0] as BaseEvent & { delta: unknown[] };
    expect(Array.isArray(ev.delta)).toBe(true);
    // JSON-Patch op replacing the plan in shared state.
    expect(ev.delta[0]).toMatchObject({ op: "replace", path: "/plan" });
  });

  it("maps commands → CUSTOM nexus.commands", () => {
    const { events } = run([
      upd("commands", { commands: [{ name: "compact" }, { name: "clear" }] }),
    ]);
    expect(types(events)).toEqual([EventType.CUSTOM]);
    const ev = events[0] as BaseEvent & { name: string; value: unknown };
    expect(ev.name).toBe("nexus.commands");
    expect(ev.value).toMatchObject({
      sessionId: "s_ben",
      commands: [{ name: "compact" }, { name: "clear" }],
    });
  });

  it("maps user_input clientMessageId to the AG-UI user message id", () => {
    const { events } = run([
      upd("user_input", { text: "hello agent", clientMessageId: "you:123:1" }),
    ]);

    expect(types(events)).toEqual([
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
    ]);
    expect(events[0]).toMatchObject({
      type: EventType.TEXT_MESSAGE_START,
      messageId: "you:123:1",
      role: "user",
    });
    expect(events[1]).toMatchObject({
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "you:123:1",
      delta: "hello agent",
    });
    expect(events[2]).toMatchObject({
      type: EventType.TEXT_MESSAGE_END,
      messageId: "you:123:1",
    });
  });

  it("preserves initial_prompt metadata on user_input AG-UI events", () => {
    const { events } = run([
      upd("user_input", {
        text: "You are Ada.",
        clientMessageId: "initial-prompt:s_ada",
        source: "initial_prompt",
        name: "ada",
        harness: "codex",
        runtimeId: "s_ada",
      }),
    ]);

    expect(types(events)).toEqual([
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
    ]);
    for (const ev of events) {
      expect(ev).toMatchObject({
        source: "initial_prompt",
        name: "ada",
        harness: "codex",
        runtimeId: "s_ada",
      });
    }
  });

  it("dedupes a native user_input echo when the synthetic row already had a clientMessageId", () => {
    const { events } = run([
      upd("user_input", { text: "hello agent", clientMessageId: "you:123:1" }),
      upd("user_input", { text: "hello agent" }),
    ]);

    expect(types(events)).toEqual([
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
    ]);
    expect(events[0]).toMatchObject({
      type: EventType.TEXT_MESSAGE_START,
      messageId: "you:123:1",
      role: "user",
    });
  });

  it("closeRun on an empty bracket emits nothing", () => {
    expect(closeRun(newBracket())).toEqual([]);
  });

  // --- C-TOOL v1 (docs/tool-call-contract.md) --------------------------------

  it("prefers the canonical tool name over the display title", () => {
    const { events } = run([
      upd("tool_call", {
        id: "t1",
        tool: "shell",
        title: "/usr/bin/zsh -lc 'ls'",
        input: { command: "ls" },
      }),
    ]);
    const start = events.find((e) => e.type === EventType.TOOL_CALL_START) as
      | (BaseEvent & { toolCallName: string })
      | undefined;
    expect(start?.toolCallName).toBe("shell");
  });

  it("single-encodes structured input: TOOL_CALL_ARGS.delta parses back to the object", () => {
    const input = { file_path: "/tmp/a.md", content: "hi", nested: { n: 1 } };
    const { events } = run([
      upd("tool_call", { id: "t2", tool: "write", input }),
    ]);
    const args = events.find((e) => e.type === EventType.TOOL_CALL_ARGS) as
      | (BaseEvent & { delta: string })
      | undefined;
    expect(args).toBeDefined();
    const parsed: unknown = JSON.parse(args?.delta ?? "");
    expect(parsed).toEqual(input);
    expect(typeof parsed).toBe("object");
  });

  it("emits RESULT from the contract output field", () => {
    const { events } = run([
      upd("tool_call", { id: "t3", tool: "shell", input: { command: "pwd" } }),
      upd("tool_call", { id: "t3", status: "completed", output: "/tmp" }),
    ]);
    const result = events.find((e) => e.type === EventType.TOOL_CALL_RESULT) as
      | (BaseEvent & { content: string })
      | undefined;
    expect(result?.content).toBe("/tmp");
  });
});
