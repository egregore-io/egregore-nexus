// Tests for `messageToAguiEvents` (committed message → Lane A text message).
// Relocated from `map.test.ts`.
import { describe, it, expect } from "vitest";
import { messageToAguiEvents } from "@server/agui/mapMessage";
import type { Message } from "@shared/types";

function msg(over: Partial<Message> = {}): Message {
  return {
    id: "m_1",
    project: "p",
    from: "ben",
    scope: "thread",
    body: "shipped it",
    provenance: "agent",
    createdAt: 1,
    ...over,
  } as Message;
}

describe("messageToAguiEvents (committed message → Lane A text message)", () => {
  it("emits START(assistant)/CONTENT/END keyed by the message id", () => {
    const ev = messageToAguiEvents(msg(), "ada");
    expect(ev.map((e) => e.type)).toEqual([
      "TEXT_MESSAGE_START",
      "TEXT_MESSAGE_CONTENT",
      "TEXT_MESSAGE_END",
    ]);
    expect(ev[0]).toMatchObject({ messageId: "m_1", role: "assistant" });
    expect(ev[1]).toMatchObject({ messageId: "m_1", delta: "shipped it" });
    expect(ev[2]).toMatchObject({ messageId: "m_1" });
  });

  it("carries the committed sender as `name` for provenance (non-self)", () => {
    const ev = messageToAguiEvents(msg({ from: "hermes" }), "ada");
    expect(ev[0]).toMatchObject({ role: "assistant", name: "hermes" });
  });

  it("uses role 'user' (and no foreign name) when the author is the watcher", () => {
    const ev = messageToAguiEvents(msg({ from: "ada" }), "ada");
    expect(ev[0]).toMatchObject({ role: "user" });
    expect((ev[0] as Record<string, unknown>).name).toBeUndefined();
  });

  it("renders nothing for an empty body", () => {
    expect(messageToAguiEvents(msg({ body: "  " }), "ada")).toEqual([]);
  });
});
