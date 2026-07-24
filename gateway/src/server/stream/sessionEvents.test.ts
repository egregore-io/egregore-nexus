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

  it("keeps an idle session stream alive with an inert SSE comment heartbeat", async () => {
    // An idle session emits no frames, so without a keep-alive the connection is
    // silent until some proxy's idle timeout kills it (undici's is 300s).
    const fanout = new SessionFanoutHub(() => ({ ready: Promise.resolve(), close() {} }));
    const response = handleSessionEvents(
      new Request("http://localhost/api/v1/agent-sessions/s_ada/events?view=nexus"),
      "s_ada",
      { fanout, heartbeatIntervalMs: 5 },
    );

    const reader = response.body!.getReader();
    const chunk = await reader.read();
    // Must be a `:` comment, NOT a `data:` frame — comments are discarded by
    // EventSource and by the WS relay, so they never reach a consumer.
    expect(new TextDecoder().decode(chunk.value)).toBe(": ping\n\n");
    await reader.cancel();
  });

  it("stops the heartbeat and the fanout subscription when the client disconnects", async () => {
    let closed = false;
    const fanout = new SessionFanoutHub(() => ({
      ready: Promise.resolve(),
      close() {
        closed = true;
      },
    }));
    const response = handleSessionEvents(
      new Request("http://localhost/api/v1/agent-sessions/s_ada/events?view=nexus"),
      "s_ada",
      { fanout, heartbeatIntervalMs: 5 },
    );

    const reader = response.body!.getReader();
    await reader.read();
    await reader.cancel();

    expect(closed).toBe(true);
  });

  it("surfaces malformed opaque cursors as a typed Nexus resync", async () => {
    const fanout = preloadedFanout([], "boot-a");
    const response = handleSessionEvents(
      new Request("http://localhost/api/v1/agent-sessions/s_ada/events?view=nexus&after=bad"),
      "s_ada",
      { fanout },
    );

    const text = await readOne(response);
    expect(text).toContain('"type":"resync"');
    expect(text).toContain('"reason":"malformed_cursor"');
    expect(text).toContain('"epoch":"boot-a"');
  });

  it("surfaces cursor gaps on the AG-UI view instead of emitting an empty stream", async () => {
    const fanout = preloadedFanout([], "boot-a");
    const response = handleSessionEvents(
      new Request("http://localhost/api/v1/agent-sessions/s_ada/events?view=agui&after=bad"),
      "s_ada",
      { fanout },
    );

    const text = await readOne(response);
    expect(text).toContain('"type":"CUSTOM"');
    expect(text).toContain('"name":"nexus.session.resync"');
    expect(text).toContain('"reason":"malformed_cursor"');
  });

  it("translates legacy afterId against the explicit current daemon boot", async () => {
    const fanout = preloadedFanout([
      agent(10, "already rendered", "boot-a"),
      agent(11, "next", "boot-a"),
    ], "boot-a");
    const response = handleSessionEvents(
      new Request("http://localhost/api/v1/agent-sessions/s_ada/events?view=nexus&afterId=10"),
      "s_ada",
      { fanout },
    );

    const text = await readOne(response);
    expect(text).not.toContain("already rendered");
    expect(text).toContain('"text":"next"');
  });
});

function preloadedFanout(
  frames: SessionFanoutFrame[],
  daemonBootId: string,
): SessionFanoutHub {
  return new SessionFanoutHub((_session, handlers) => {
    for (const frame of frames) handlers.onFrame(frame);
    return {
      ready: Promise.resolve(),
      daemonBootId: () => daemonBootId,
      close() {},
    };
  });
}

async function readOne(response: Response): Promise<string> {
  const reader = response.body!.getReader();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const result = await Promise.race([
      reader.read(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("timed out waiting for session event")), 200);
      }),
    ]);
    return new TextDecoder().decode(result.value);
  } finally {
    if (timer) clearTimeout(timer);
    await reader.cancel();
  }
}

function agent(id: number, text: string, epoch: string): SessionFanoutFrame {
  return {
    lane: "agent",
    epoch,
    id,
    cursor: encodeCursor(epoch, id),
    event: {
      type: "agent.update",
      sessionId: "s_ada",
      kind: "text",
      data: { text },
    },
  };
}
