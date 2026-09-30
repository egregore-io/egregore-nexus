// the AG-UI run orchestrator.
//
// `run(input, target, deps)` and `observe(target, deps)` produce the AG-UI SSE
// byte stream that an AG-UI client (egregore-lens / CopilotKit) consumes. Both
// are driven here with an injected send function + a mock read-view source so
// the streaming + mapping + run-lifecycle wiring is exercised with no daemon
// transport.
//
// `run`: emit RUN_STARTED → send through the injected Message Post seam →
// render the committed Message Post row from the read-view source →
// RUN_FINISHED, then close. An error → RUN_ERROR.
// `observe`: same committed-message rendering, but NO send and it follows the
// thread continuously.
import { describe, it, expect } from "vitest";
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";
import { run, observe } from "@server/agui/run";
import type { RunDeps } from "@server/agui/run";
import type { RunAgentInput } from "@ag-ui/client";
import type { Message, SendRequest } from "@shared/types";
import type { MessagePostSourceFactory } from "@server/messagePost/readView";

// --- mock building blocks ---------------------------------------------------

/** A mock Message Post sender that records every send call. */
function makeSendRecorder() {
  const sends: SendRequest[] = [];
  return {
    sends,
    sendMessage: async (req: SendRequest) => {
      sends.push(req);
      return { ok: true };
    },
  };
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

function committedMessage(id: string, from: string, body: string, scope = "thread"): Message {
  return { id, from, body, scope, project: "p", provenance: "agent", createdAt: 1 } as unknown as Message;
}

// --- SSE collection + decode ------------------------------------------------

/** Read a ReadableStream<Uint8Array> fully into a single decoded string. */
async function readAll(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let out = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) out += decoder.decode(value, { stream: true });
  }
  out += decoder.decode();
  return out;
}

/** Decode AG-UI SSE framing (`data: <json>\n\n`) into events — proves the bytes. */
function decodeSse(text: string): BaseEvent[] {
  const out: BaseEvent[] = [];
  for (const line of text.split("\n")) {
    if (!line.startsWith("data:")) continue;
    const json = line.slice(5).trim();
    if (!json) continue;
    out.push(JSON.parse(json) as BaseEvent);
  }
  return out;
}

const types = (events: BaseEvent[]) => events.map((e) => e.type);

const INPUT: RunAgentInput = {
  threadId: "t_design",
  runId: "r_1",
  state: {},
  messages: [{ id: "u1", role: "user", content: "find the bug" }],
  tools: [],
  context: [],
  forwardedProps: {},
};

describe("run (AG-UI agent endpoint — drive a run over SSE)", () => {
  it("emits a valid AG-UI run and calls send with the mapped SendRequest", async () => {
    const source = makeMessageSource();
    const { sendMessage, sends } = makeSendRecorder();

    const deps: RunDeps = {
      sendMessage,
      createMessageSource: source.factory,
    };

    const stream = run(INPUT, { verb: "post", thread: "design" }, deps);

    // Drive the committed row once the read-view source is wired.
    await Promise.resolve();
    await Promise.resolve();
    source.emit(committedMessage("m_done", "ben", "here you go"));

    const text = await readAll(stream);
    const events = decodeSse(text);

    // send was called with the in-mapped request.
    expect(sends).toEqual([{ to: { verb: "post", thread: "design" }, body: "find the bug" }]);

    // A well-formed AG-UI run: lifecycle wraps the mapped body, in order.
    expect(types(events)).toEqual([
      EventType.RUN_STARTED,
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
      EventType.RUN_FINISHED,
    ]);

    // RUN_STARTED carries the thread + run ids (lens reads runId).
    const started = events[0] as BaseEvent & { threadId: string; runId: string };
    expect(started.threadId).toBe("t_design");
    expect(started.runId).toBe("r_1");

    const content = events.find((e) => e.type === EventType.TEXT_MESSAGE_CONTENT) as
      | (BaseEvent & { delta: string })
      | undefined;
    expect(content?.delta).toBe("here you go");

    // The source is torn down after the run completes.
    expect(source.closed).toBe(true);
  });

  it("renders only committed messages from the read-view source", async () => {
    const source = makeMessageSource();
    const { sendMessage } = makeSendRecorder();

    const deps: RunDeps = {
      sendMessage,
      createMessageSource: source.factory,
    };

    const stream = run(INPUT, { verb: "post", thread: "design" }, deps);
    await Promise.resolve();
    await Promise.resolve();

    source.emit(committedMessage("m_done", "ben", "store row"));

    const events = decodeSse(await readAll(stream));
    const textContents = events.filter((e) => e.type === EventType.TEXT_MESSAGE_CONTENT) as Array<
      BaseEvent & { delta: string }
    >;
    expect(textContents.map((e) => e.delta)).toEqual(["store row"]);
  });

  it("emits RUN_ERROR when send fails", async () => {
    const source = makeMessageSource();
    const failingSend = async () => {
        throw new Error("daemon unreachable");
    };

    const deps: RunDeps = { sendMessage: failingSend, createMessageSource: source.factory };
    const stream = run(INPUT, { verb: "post", thread: "design" }, deps);

    const events = decodeSse(await readAll(stream));
    expect(types(events)).toEqual([EventType.RUN_STARTED, EventType.RUN_ERROR]);
    const err = events[1] as BaseEvent & { message: string };
    expect(err.message).toContain("daemon unreachable");
    // A failed run still tears the source down (no leak).
    expect(source.closed).toBe(true);
  });
});

describe("observe (AG-UI watch — stream a thread's runs, no send)", () => {
  it("streams mapped frames for a publish target without any send call (Lane A, message.created)", async () => {
    const source = makeMessageSource();
    const { sends } = makeSendRecorder();

    const deps: RunDeps = {
      createMessageSource: source.factory,
    };
    // T9b: observe() now routes ALL targets (post/dm/publish) through observeMessagePost (Lane A).
    const stream = observe({ verb: "publish", topic: "design" }, deps);

    // One committed message on the bus that this watcher renders.
    source.emit(committedMessage("m_pub", "ben", "watching"));

    // Allow the async resolve chain to flush.
    await new Promise((r) => setTimeout(r, 0));
    await new Promise((r) => setTimeout(r, 0));

    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let text = "";
    // Read the frames produced for this message (RUN_STARTED + text bracket + RUN_FINISHED).
    for (let i = 0; i < 6; i++) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) text += decoder.decode(value, { stream: true });
      if (text.includes(EventType.RUN_FINISHED)) break;
    }
    await reader.cancel();

    const events = decodeSse(text);
    // No run was initiated — observe is read-only.
    expect(sends).toEqual([]);
    // The committed message materialized as a bracketed AG-UI run.
    expect(types(events)).toContain(EventType.RUN_STARTED);
    expect(types(events)).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(types(events)).toContain(EventType.RUN_FINISHED);
    const content = events.find((e) => e.type === EventType.TEXT_MESSAGE_CONTENT) as
      | (BaseEvent & { delta: string })
      | undefined;
    expect(content?.delta).toBe("watching");
  });

  it("thread mode renders committed messages and ignores agent.update", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
      selfName: "ada",
    };
    const stream = observe({ verb: "post", thread: "design" }, deps);
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let text = "";

    // A committed thread message must render as one text message.
    source.emit(committedMessage("m_1", "ben", "on it"));

    // observe never self-ends. The macrotask ticks flush the async resolve chain so
    // its frames are buffered before we read (deterministic — no timer race; mirrors
    // the non-thread observe test above). The bounded plain reads return the buffered
    // frames and break on RUN_FINISHED; reader.cancel() closes the watch.
    await new Promise((r) => setTimeout(r, 0));
    await new Promise((r) => setTimeout(r, 0));
    for (let i = 0; i < 6; i++) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) text += decoder.decode(value, { stream: true });
      if (text.includes("RUN_FINISHED")) break;
    }
    await reader.cancel();

    const events = types(decodeSse(text));

    expect(events).toEqual([
      "RUN_STARTED",
      "TEXT_MESSAGE_START",
      "TEXT_MESSAGE_CONTENT",
      "TEXT_MESSAGE_END",
      "RUN_FINISHED",
    ]);
  });

  // --- T9b: ?dm= observe → Message Post (regression guard) ---
  it("dm mode renders committed message.created (not agent.update) — ?dm= is now Lane A", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
      selfName: "ada",
    };
    // observe() with a dm target now routes through observeMessagePost (Lane A).
    const stream = observe({ verb: "dm", name: "ben" }, deps);
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let text = "";

    // A committed DM message MUST render (message.created).
    source.emit(committedMessage("m_dm", "ben", "a direct message", "dm"));

    await new Promise((r) => setTimeout(r, 0));
    await new Promise((r) => setTimeout(r, 0));
    for (let i = 0; i < 6; i++) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) text += decoder.decode(value, { stream: true });
      if (text.includes("RUN_FINISHED")) break;
    }
    await reader.cancel();

    const events = types(decodeSse(text));
    // Lane A: only message.created-derived frames, never agent.update-derived.
    expect(events).toEqual([
      "RUN_STARTED",
      "TEXT_MESSAGE_START",
      "TEXT_MESSAGE_CONTENT",
      "TEXT_MESSAGE_END",
      "RUN_FINISHED",
    ]);
    // The content is the DM body, not the leaked agent.update text.
    const content = decodeSse(text).find((e) => e.type === "TEXT_MESSAGE_CONTENT") as
      | (BaseEvent & { delta: string })
      | undefined;
    expect(content?.delta).toBe("a direct message");
  });
});
