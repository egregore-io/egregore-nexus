// the HTTP adapters (`handleRun` / `handleObserve`). These sit
// between the framework route files and the orchestrator; here we drive them with
// a plain `Request` + injected deps (mock send + hand-driven relay) and assert the
// request→Response translation: SSE content-type, target parsing, the 400s, and
// that `observe` never sends.
// cross-lane guard tests: `handleAgentSession` (?session=) vs `handleObserve`
// (?thread=) are structurally isolated; each pipeline NEVER emits the other lane's frames.
import { describe, it, expect } from "vitest";
import { EventType } from "@ag-ui/client";
import {
  handleRun,
  handleObserve,
  handleAgentSession,
  targetFromQuery,
} from "@server/agui/http";
import type { RunDeps } from "@server/agui/run";
import type { RealtimeRelay, RealtimeRelayOptions } from "@server/agui/relayTypes";
import type { Message, SendRequest, SendTarget, WsEvent } from "@shared/types";
import type { MessagePostSourceFactory } from "@server/messagePost/readView";

function makeRelay() {
  let onEvent: ((ev: WsEvent) => void) | null = null;
  return {
    factory: (o: RealtimeRelayOptions): RealtimeRelay => {
      onEvent = o.onEvent;
      return { ready: Promise.resolve(), close() {} };
    },
    emit: (ev: WsEvent) => onEvent?.(ev),
  };
}

function makeSendRecorder() {
  const sends: SendRequest[] = [];
  return {
    sends,
    sendMessage: async (req: SendRequest) => {
      sends.push(req);
      return {};
    },
  };
}

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
  let target: SendTarget | null = null;
  let after: number | undefined = undefined;
  let afterRowid: number | undefined = undefined;
  let closed = false;
  const factory: MessagePostSourceFactory = (opts) => {
    target = opts.target;
    after = opts.after;
    afterRowid = opts.afterRowid;
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
    get target() {
      return target;
    },
    get after() {
      return after;
    },
    get afterRowid() {
      return afterRowid;
    },
    get closed() {
      return closed;
    },
  };
}

async function readAll(res: Response): Promise<string> {
  const body = res.body;
  if (!body) return "";
  const reader = body.getReader();
  const dec = new TextDecoder();
  let out = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) out += dec.decode(value, { stream: true });
  }
  return out + dec.decode();
}

async function readUntilFinished(res: Response): Promise<string> {
  const reader = res.body!.getReader();
  const dec = new TextDecoder();
  let text = "";
  for (let i = 0; i < 20; i++) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) text += dec.decode(value, { stream: true });
    if (text.includes(EventType.RUN_FINISHED)) break;
  }
  await reader.cancel();
  return text;
}

const RUN_INPUT_BODY = JSON.stringify({
  threadId: "t1",
  runId: "r1",
  state: {},
  messages: [{ id: "u1", role: "user", content: "hi" }],
  tools: [],
  context: [],
  forwardedProps: {},
});

describe("targetFromQuery", () => {
  it("maps ?thread / ?dm / ?topic to the SendTarget", () => {
    expect(targetFromQuery(new URL("http://x/api/agui?thread=design"))).toEqual({
      verb: "post",
      thread: "design",
    });
    expect(targetFromQuery(new URL("http://x/api/agui?dm=ben"))).toEqual({
      verb: "dm",
      name: "ben",
    });
    expect(targetFromQuery(new URL("http://x/api/agui?agentId=a_ben"))).toEqual({
      verb: "dm",
      agentId: "a_ben",
    });
    expect(targetFromQuery(new URL("http://x/api/agui?dm=ben&agentId=a_ben"))).toEqual({
      verb: "dm",
      name: "ben",
      agentId: "a_ben",
    });
    expect(targetFromQuery(new URL("http://x/api/agui?topic=builds"))).toEqual({
      verb: "publish",
      topic: "builds",
    });
    expect(targetFromQuery(new URL("http://x/api/agui"))).toBeNull();
  });
});

describe("handleRun (POST /api/agui)", () => {
  it("returns a text/event-stream SSE Response and sends the mapped request", async () => {
    const source = makeMessageSource();
    const { sendMessage, sends } = makeSendRecorder();
    const deps: RunDeps = { sendMessage, createMessageSource: source.factory };

    const req = new Request("http://x/api/agui?thread=design", {
      method: "POST",
      body: RUN_INPUT_BODY,
    });
    const res = await handleRun(req, deps);

    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");

    // Drive a minimal committed row so the stream completes.
    await Promise.resolve();
    await Promise.resolve();
    source.emit(msg("m1", "ben", "yo"));

    const text = await readAll(res);
    expect(text).toContain(EventType.RUN_STARTED);
    expect(text).toContain(EventType.RUN_FINISHED);
    expect(sends).toEqual([{ to: { verb: "post", thread: "design" }, body: "hi" }]);
  });

  it("400s when the target is missing", async () => {
    const res = await handleRun(
      new Request("http://x/api/agui", { method: "POST", body: RUN_INPUT_BODY }),
    );
    expect(res.status).toBe(400);
  });

  it("400s when the body is not a RunAgentInput", async () => {
    const res = await handleRun(
      new Request("http://x/api/agui?thread=design", { method: "POST", body: "not json" }),
    );
    expect(res.status).toBe(400);
  });
});

/** Decode AG-UI SSE framing (`data: <json>\n\n`) → event objects. */
function decodeSse(text: string): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = [];
  for (const line of text.split("\n")) {
    if (!line.startsWith("data:")) continue;
    const json = line.slice(5).trim();
    if (!json) continue;
    out.push(JSON.parse(json) as Record<string, unknown>);
  }
  return out;
}

const eventTypes = (events: Array<Record<string, unknown>>): string[] =>
  events.map((e) => e["type"] as string);

describe("handleObserve (GET /api/agui/observe)", () => {
  it("returns an SSE Response and never sends (T9b: topic/dm/thread all use Lane A)", async () => {
    const source = makeMessageSource();
    const { sends } = makeSendRecorder();
    const deps: RunDeps = {
      createMessageSource: source.factory,
    };

    // Use ?topic= to verify non-thread targets also use Lane A.
    const res = handleObserve(
      new Request("http://x/api/agui/observe?topic=design"),
      deps,
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");
    expect(source.target).toEqual({ verb: "publish", topic: "design" });

    source.emit(msg("m_watch", "ben", "watch"));
    const text = await readUntilFinished(res);

    expect(text).toContain(": nexus-message-post-observe-open");
    expect(text).toContain(EventType.RUN_STARTED);
    expect(text).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(sends).toEqual([]);
  });

  it("400s when the target is missing", () => {
    const res = handleObserve(new Request("http://x/api/agui/observe"));
    expect(res.status).toBe(400);
  });

  it("passes the compound after cursor to the Message Post observe source", () => {
    const source = makeMessageSource();
    const res = handleObserve(
      new Request("http://x/api/agui/observe?thread=design&after=1782976700000&afterRowid=42"),
      { createMessageSource: source.factory },
    );

    expect(res.status).toBe(200);
    expect(source.target).toEqual({ verb: "post", thread: "design" });
    expect(source.after).toBe(1_782_976_700_000);
    expect(source.afterRowid).toBe(42);
    void res.body?.cancel();
  });

  it("a ?thread= observe drives the Lane A thread path (read, not agent.update)", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
      createRelay: () => {
        throw new Error("Lane A observe must not open a daemon relay");
      },
    };

    const res = handleObserve(
      new Request("http://x/api/agui/observe?thread=design"),
      deps,
    );
    expect(res.status).toBe(200);
    expect(source.target).toEqual({ verb: "post", thread: "design" });

    source.emit(msg("msg-lane-a", "ben", "from read view"));
    const events = decodeSse(await readUntilFinished(res));
    expect(eventTypes(events)).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(source.closed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Cross-lane guard tests.
// These are the structural isolation tests: each pipeline is driven with BOTH
// Lane A (message.created) and Lane B (agent.update) events; we assert that
// each pipeline only renders its own lane's events and silently ignores the other.
// ---------------------------------------------------------------------------

describe("handleAgentSession (?session= — Lane B)", () => {
  it("renders agent.update events and NEVER a message.created-derived frame", async () => {
    const relay = makeRelay();
    const deps = { sessionName: "ben", createRelay: relay.factory };

    const res = handleAgentSession(
      new Request("http://x/api/agui/observe?session=ben"),
      deps,
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("text/event-stream");

    await Promise.resolve();
    await Promise.resolve();

    // Drive BOTH a Lane B event (agent.update) and a Lane A event (message.created).
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "text", data: { text: "from agent" } });
    relay.emit({ type: "message.created", messageId: "m_lane_a" });
    relay.emit({ type: "agent.update", sessionId: "s_ben", kind: "turn_end", data: {} });

    // Read frames until RUN_FINISHED.
    const reader = res.body!.getReader();
    const dec = new TextDecoder();
    let text = "";
    for (let i = 0; i < 20; i++) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) text += dec.decode(value, { stream: true });
      if (text.includes(EventType.RUN_FINISHED)) break;
    }
    await reader.cancel();

    const events = decodeSse(text);
    const types = eventTypes(events);

    // Lane B renders the agent.update content.
    expect(types).toContain(EventType.RUN_STARTED);
    expect(types).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(types).toContain(EventType.RUN_FINISHED);

    // Lane B NEVER emits a frame derived from message.created — it ignores Lane A entirely.
    const contentEvents = events.filter((e) => e["type"] === EventType.TEXT_MESSAGE_CONTENT);
    for (const ev of contentEvents) {
      // All content must come from the agent.update (the text "from agent"), not from a
      // message.created resolution path (which Lane B structurally cannot reach).
      expect(ev["delta"]).toBe("from agent");
    }

    // Exactly ONE run opened and closed (message.created did NOT open a second run).
    expect(types.filter((t) => t === EventType.RUN_STARTED)).toHaveLength(1);
    expect(types.filter((t) => t === EventType.RUN_FINISHED)).toHaveLength(1);
    expect(events.find((event) => event["type"] === EventType.RUN_STARTED)?.["threadId"]).toBe(
      "session:ben",
    );
  });
});

describe("handleObserve (?thread= — Lane A) cross-lane guard", () => {
  it("renders Message Post frames and never opens the agent-update relay", async () => {
    const source = makeMessageSource();
    const deps: RunDeps = {
      createMessageSource: source.factory,
      createRelay: () => {
        throw new Error("Lane A observe must not open a daemon relay");
      },
    };

    const res = handleObserve(
      new Request("http://x/api/agui/observe?thread=general"),
      deps,
    );
    expect(res.status).toBe(200);

    source.emit(msg("msg-lane-a-guard", "ben", "lane-a-content"));
    const events = decodeSse(await readUntilFinished(res));
    const types = eventTypes(events);
    expect(types).toContain(EventType.TEXT_MESSAGE_CONTENT);
    expect(
      events.some(
        (event) =>
          event["type"] === EventType.TEXT_MESSAGE_CONTENT &&
          event["delta"] === "lane-a-content",
      ),
    ).toBe(true);
  });
});
