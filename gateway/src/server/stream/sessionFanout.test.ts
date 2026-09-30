import { describe, expect, it, vi } from "vitest";

import { SessionFanoutHub, encodeCursor, type SessionFanoutFrame } from "./sessionFanout";

describe("agent-session fanout", () => {
  it("uses one upstream for Nexus and AG-UI subscribers and replays its bounded cursor ring", async () => {
    let emit!: (frame: SessionFanoutFrame) => void;
    const close = vi.fn();
    const factory = vi.fn((_session, handlers) => {
      emit = handlers.onFrame;
      return { ready: Promise.resolve(), close };
    });
    const hub = new SessionFanoutHub(factory, 4);
    const nexus = vi.fn();
    const agui = vi.fn();
    const first = hub.subscribe("s_ada", { view: "nexus", onFrame: nexus });
    const second = hub.subscribe("s_ada", { view: "agui", onFrame: agui });
    expect(factory).toHaveBeenCalledTimes(1);

    emit(agent(1, "one"));
    emit(agent(2, "two"));
    expect(nexus).toHaveBeenCalledTimes(2);
    expect(agui).toHaveBeenCalledTimes(2);

    const replay = vi.fn();
    const third = hub.subscribe("s_ada", {
      view: "nexus",
      after: encodeCursor("epoch", 1),
      onFrame: replay,
    });
    expect(replay).toHaveBeenCalledWith(agent(2, "two"));
    first.close(); second.close(); third.close();
    expect(close).toHaveBeenCalledTimes(1);
  });
});

function agent(id: number, text: string): SessionFanoutFrame {
  return {
    lane: "agent",
    epoch: "epoch",
    id,
    cursor: encodeCursor("epoch", id),
    event: { type: "agent.update", sessionId: "s_ada", kind: "text", data: { text } },
  };
}
