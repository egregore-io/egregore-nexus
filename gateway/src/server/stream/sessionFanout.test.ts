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
    await Promise.all([first.ready, second.ready]);

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
    await third.ready;
    expect(replay).toHaveBeenCalledWith(agent(2, "two"));
    first.close(); second.close(); third.close();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("filters upstream catch-up per subscriber and delivers only subsequent events once", async () => {
    let emit!: (frame: SessionFanoutFrame) => void;
    const hub = new SessionFanoutHub((_session, handlers) => {
      emit = handlers.onFrame;
      return {
        ready: Promise.resolve(),
        daemonBootId: () => "boot-a",
        close() {},
      };
    });
    const keeper = hub.subscribe("s_ada", { view: "nexus", onFrame() {} });
    await keeper.ready;
    emit(agent(41, "already accepted", "boot-a"));

    const received = vi.fn();
    const reconnect = hub.subscribe("s_ada", {
      view: "nexus",
      after: encodeCursor("boot-a", 41),
      onFrame: received,
    });
    await reconnect.ready;

    // The daemon may replay the overlap after the browser subscriber has attached.
    emit(agent(41, "overlap", "boot-a"));
    emit(agent(42, "next", "boot-a"));
    emit(agent(42, "duplicate next", "boot-a"));

    expect(received.mock.calls.map(([frame]) => frame.id)).toEqual([42]);
    keeper.close();
    reconnect.close();
  });

  it("keeps the supplied cursor while an empty-ring resync waits for upstream catch-up", async () => {
    let emit!: (frame: SessionFanoutFrame) => void;
    const received = vi.fn();
    const hub = new SessionFanoutHub((_session, handlers) => {
      emit = handlers.onFrame;
      return {
        ready: Promise.resolve(),
        daemonBootId: () => "boot-a",
        close() {},
      };
    });

    const subscription = hub.subscribe("s_ada", {
      view: "nexus",
      after: encodeCursor("boot-a", 41),
      onFrame: received,
    });
    await subscription.ready;
    emit(agent(41, "catch-up overlap", "boot-a"));
    emit(agent(42, "subsequent", "boot-a"));

    expect(received.mock.calls.map(([frame]) => [frame.lane, frame.id])).toEqual([
      ["gap", 41],
      ["agent", 42],
    ]);
    subscription.close();
  });

  it.each([
    {
      label: "malformed",
      after: "not-a-cursor",
      frames: [agent(10, "ten", "boot-a")],
      expected: "malformed_cursor",
    },
    {
      label: "different daemon boot",
      after: encodeCursor("boot-old", 10),
      frames: [agent(10, "ten", "boot-a")],
      expected: "boot_mismatch",
    },
    {
      label: "ahead of the retained tail",
      after: encodeCursor("boot-a", 99),
      frames: [agent(10, "ten", "boot-a")],
      expected: "ahead_cursor",
    },
    {
      label: "behind the evicted floor",
      after: encodeCursor("boot-a", 9),
      frames: [
        agent(10, "ten", "boot-a"),
        agent(20, "twenty", "boot-a"),
        agent(30, "thirty", "boot-a"),
      ],
      expected: "stale_cursor",
      ringLimit: 2,
    },
    {
      label: "valid but the replay ring is empty",
      after: encodeCursor("boot-a", 10),
      frames: [],
      expected: "empty_ring",
    },
  ])("emits one typed resync for a $label cursor", async ({ after, frames, expected, ringLimit }) => {
    const received = vi.fn();
    const hub = preloadedHub(frames, "boot-a", ringLimit);

    const subscription = hub.subscribe("s_ada", {
      view: "nexus",
      after,
      onFrame: received,
    });
    await subscription.ready;

    expect(received).toHaveBeenCalledTimes(1);
    const gap = received.mock.calls[0]![0] as SessionFanoutFrame;
    expect(gap).toMatchObject({ lane: "gap", epoch: "boot-a", reason: expected });
    expect(cursorPayload(gap.cursor)).toEqual({ v: 1, daemonBootId: "boot-a", id: gap.id });
    subscription.close();
  });

  it("translates a legacy numeric afterId only within the current daemon boot", async () => {
    const received = vi.fn();
    const hub = preloadedHub([
      agent(10, "ten", "boot-a"),
      agent(11, "eleven", "boot-a"),
    ], "boot-a");

    const subscription = hub.subscribe("s_ada", {
      view: "nexus",
      legacyAfterId: "10",
      onFrame: received,
    });
    await subscription.ready;

    expect(received.mock.calls.map(([frame]) => frame.id)).toEqual([11]);
    subscription.close();
  });

  it("turns a malformed legacy afterId into a typed resync", async () => {
    const received = vi.fn();
    const hub = preloadedHub([agent(10, "ten", "boot-a")], "boot-a");

    const subscription = hub.subscribe("s_ada", {
      view: "nexus",
      legacyAfterId: "ten",
      onFrame: received,
    });
    await subscription.ready;

    expect(received).toHaveBeenCalledWith(expect.objectContaining({
      lane: "gap",
      epoch: "boot-a",
      reason: "malformed_cursor",
    }));
    subscription.close();
  });
});

function preloadedHub(
  frames: SessionFanoutFrame[],
  daemonBootId: string,
  ringLimit = 512,
): SessionFanoutHub {
  return new SessionFanoutHub((_session, handlers) => {
    for (const frame of frames) handlers.onFrame(frame);
    return {
      ready: Promise.resolve(),
      daemonBootId: () => daemonBootId,
      close() {},
    };
  }, ringLimit);
}

function cursorPayload(cursor: string): unknown {
  return JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
}

function agent(id: number, text: string, epoch = "epoch"): SessionFanoutFrame {
  return {
    lane: "agent",
    epoch,
    id,
    cursor: encodeCursor(epoch, id),
    event: { type: "agent.update", sessionId: "s_ada", kind: "text", data: { text } },
  };
}
