// Task 6 — Lane A guardrail test for `observeMessagePost`.
//
// Asserts:
//   1. Committed message rows render as complete AG-UI runs
//      (RUN_STARTED → TEXT_MESSAGE_START → TEXT_MESSAGE_CONTENT → TEXT_MESSAGE_END
//      → RUN_FINISHED), attributed by `from`.
//   2. Lane A is sourced from committed messages, not agent-session `agent.update`.
//   3. The function is target-agnostic: post / dm / publish all work.
//   4. Empty/unrenderable messages are skipped; the stream stays alive.
import { describe, it, expect, vi } from "vitest";
import { EventType } from "@ag-ui/client";
import type { Message } from "@shared/types";
import { observeMessagePost } from "@server/agui/messagePost";
import type { RunDeps } from "@server/agui/_sseCore";
import type { MessagePostSourceFactory } from "@server/messagePost/readView";
import { decodeSse, eventTypes } from "@server/agui/_test-relay";

// AG-UI event types that are NEVER legal from Lane A (agent activity, not messages).
const AGENT_UPDATE_DERIVED = new Set([
  EventType.REASONING_MESSAGE_START,
  EventType.REASONING_MESSAGE_CONTENT,
  EventType.REASONING_MESSAGE_END,
  EventType.TOOL_CALL_START,
  EventType.TOOL_CALL_END,
  EventType.TOOL_CALL_RESULT,
]);

/** Minimal Message factory for tests. */
function msg(id: string, from: string, body: string): Message {
  return { id, from, body, scope: "thread", project: "p", provenance: "agent", createdAt: 1 } as unknown as Message;
}

function makeMessageSource() {
  let onMessage: ((message: Message) => void) | null = null;
  let closed = false;
  const factory: MessagePostSourceFactory = (opts) => {
    onMessage = opts.onMessage;
    return {
      ready: Promise.resolve(),
      close() {
        closed = true;
      },
    };
  };
  return {
    factory,
    emit(message: Message) {
      onMessage?.(message);
    },
    get closed() {
      return closed;
    },
  };
}

/** Drive the stream: emit committed messages, wait one tick, read buffered frames. */
async function driveAndRead(
  stream: ReadableStream<Uint8Array>,
  drive: () => void,
  stopOn: string = "RUN_FINISHED",
): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";

  drive();
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

describe("observeMessagePost (Lane A guardrail)", () => {
  it("flushes an SSE open comment before any committed message arrives", async () => {
    const source = makeMessageSource();
    const stream = observeMessagePost(
      { verb: "post", thread: "design" },
      { createMessageSource: source.factory },
    );

    const reader = stream.getReader();
    const first = await reader.read();
    await reader.cancel();

    expect(first.done).toBe(false);
    expect(new TextDecoder().decode(first.value)).toBe(
      ": nexus-message-post-observe-open\n\n",
    );
  });

  it("emits a Lane A heartbeat and pauses the source under stream backpressure", async () => {
    let options: Parameters<MessagePostSourceFactory>[0] | undefined;
    const pause = vi.fn();
    const resume = vi.fn();
    const source: MessagePostSourceFactory = (opts) => {
      options = opts;
      return { ready: Promise.resolve(), close: vi.fn(), pause, resume };
    };
    const stream = observeMessagePost(
      { verb: "post", thread: "design" },
      { createMessageSource: source },
    );
    const reader = stream.getReader();
    await reader.read();

    options?.onHeartbeat?.();
    const ping = await reader.read();
    expect(new TextDecoder().decode(ping.value)).toBe(": ping\n\n");
    expect(resume).toHaveBeenCalled();
    expect(pause).toHaveBeenCalled();

    await reader.cancel();
  });

  it("renders a committed thread message as a complete AG-UI run", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
      selfName: "ada",
    };

    const stream = observeMessagePost({ verb: "post", thread: "design" }, deps);
    const text = await driveAndRead(stream, () => {
      source.emit(msg("m_1", "ben", "on it"));
    });

    const events = decodeSse(text);
    expect(eventTypes(events)).toEqual([
      "RUN_STARTED",
      "TEXT_MESSAGE_START",
      "TEXT_MESSAGE_CONTENT",
      "TEXT_MESSAGE_END",
      "RUN_FINISHED",
    ]);

    // Content matches the message body.
    const content = events.find((e) => e["type"] === EventType.TEXT_MESSAGE_CONTENT);
    expect(content?.["delta"]).toBe("on it");

    // Attributed as assistant (not self).
    const start = events.find((e) => e["type"] === EventType.TEXT_MESSAGE_START);
    expect(start?.["role"]).toBe("assistant");
  });

  it("renders own posts with role user (selfName attribution)", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
      selfName: "ada",
    };

    const stream = observeMessagePost({ verb: "post", thread: "design" }, deps);
    const text = await driveAndRead(stream, () => {
      source.emit(msg("m_ada", "ada", "here is my take"));
    });

    const events = decodeSse(text);
    const start = events.find((e) => e["type"] === EventType.TEXT_MESSAGE_START);
    expect(start?.["role"]).toBe("user");
  });

  it("NEVER emits an agent.update-derived frame (the guardrail)", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
    };

    const stream = observeMessagePost({ verb: "post", thread: "design" }, deps);
    const text = await driveAndRead(stream, () => {
      source.emit(msg("m_commit", "ben", "done"));
    });

    const events = decodeSse(text);
    // No agent.update-derived frame in output.
    for (const ev of events) {
      expect(AGENT_UPDATE_DERIVED.has(ev["type"] as EventType)).toBe(false);
    }
    // But the committed message DID render.
    expect(eventTypes(events)).toContain("RUN_STARTED");
    expect(eventTypes(events)).toContain("TEXT_MESSAGE_CONTENT");
    expect(eventTypes(events)).toContain("RUN_FINISHED");
  });

  it("skips empty messages without breaking the stream", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
    };

    const stream = observeMessagePost({ verb: "post", thread: "design" }, deps);
    const text = await driveAndRead(stream, () => {
      source.emit(msg("m_empty", "ben", "   "));
      source.emit(msg("m_good", "ben", "got it"));
    });

    const events = decodeSse(text);
    // m_empty was skipped; m_good rendered.
    const contents = events.filter((e) => e["type"] === EventType.TEXT_MESSAGE_CONTENT);
    expect(contents).toHaveLength(1);
    expect(contents[0]?.["delta"]).toBe("got it");
  });

  it("works for dm target (target-agnostic)", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
    };

    const stream = observeMessagePost({ verb: "dm", name: "ben" }, deps);
    const text = await driveAndRead(stream, () => {
      source.emit(msg("m_dm", "ben", "a dm"));
    });

    const events = decodeSse(text);
    expect(eventTypes(events)).toContain("RUN_STARTED");
    const started = events.find((e) => e["type"] === "RUN_STARTED");
    // threadId for dm target.
    expect(started?.["threadId"]).toBe("dm:ben");
  });

  it("works for publish target (target-agnostic)", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
    };

    const stream = observeMessagePost({ verb: "publish", topic: "news" }, deps);
    const text = await driveAndRead(stream, () => {
      source.emit(msg("m_pub", "ben", "published"));
    });

    const events = decodeSse(text);
    const started = events.find((e) => e["type"] === "RUN_STARTED");
    expect(started?.["threadId"]).toBe("publish:news");
  });

  it("observe() delegation — thread post routes through observeMessagePost", async () => {
    // This test exercises the delegation path in run.ts observe() to prove
    // the existing observe() tests keep working via delegation.
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
      selfName: "ada",
    };

    // Import observe to exercise the delegation path.
    const { observe } = await import("@server/agui/run");
    const stream = observe({ verb: "post", thread: "design" }, deps);
    const text = await driveAndRead(stream, () => {
      source.emit(msg("m_1", "ben", "on it"));
    });

    const events = decodeSse(text);
    expect(eventTypes(events)).toEqual([
      "RUN_STARTED",
      "TEXT_MESSAGE_START",
      "TEXT_MESSAGE_CONTENT",
      "TEXT_MESSAGE_END",
      "RUN_FINISHED",
    ]);
  });
});
