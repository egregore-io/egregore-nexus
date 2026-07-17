import { describe, expect, it } from "vitest";

import { handleSessionEvents } from "./sessionEvents";
import { SessionFanoutHub, encodeCursor, type SessionFanoutFrame } from "./sessionFanout";

describe("canonical agent-session events endpoint", () => {
  it("serves semantic Nexus SSE from the shared fanout", async () => {
    let emit!: (frame: SessionFanoutFrame) => void;
    const fanout = new SessionFanoutHub((_session, handlers) => {
      emit = handlers.onFrame;
      return { ready: Promise.resolve(), close() {} };
    });
    const response = handleSessionEvents(
      new Request("http://localhost/api/v1/agent-sessions/s_ada/events?view=nexus"),
      "s_ada",
      { fanout },
    );
    const reader = response.body!.getReader();
    emit({
      lane: "agent", epoch: "epoch", id: 1, cursor: encodeCursor("epoch", 1),
      event: { type: "agent.update", sessionId: "s_ada", kind: "text", data: { text: "hello" } },
    });
    const chunk = await reader.read();
    expect(new TextDecoder().decode(chunk.value)).toContain('"text":"hello"');
    await reader.cancel();
  });
});
