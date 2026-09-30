// the AG-UI bridge END-TO-END proof, against the REAL
// `@ag-ui/client` decoder.
//
// run.test.ts already asserts the bytes WE emit decode (with a hand-rolled SSE
// splitter) into the right event order. That proves our framing is self-
// consistent — but NOT that the canonical AG-UI client accepts it. This test
// closes that gap: it drives the actual endpoint (`handleRun` -> `run()` over a
// mock send + a hand-driven read-view source), captures the SSE `Response`, and
// feeds it to the SAME decoder egregore-lens uses - `HttpAgent`, whose
// `.run(input)` pipes `runHttpRequest` -> `transformHttpEventStream` (the real
// SSE parser + the chunk->message bracketer). We then push the decoded
// `BaseEvent`s through a lens-shaped `applyAguiEvent` sink (mirroring
// egregore-lens's `aguiReader.ts`) and assert the materialized message row.
//
// `HttpAgent` is constructed with an injected `fetch` (its documented seam — the
// egregore-lens path) so no socket is opened: `fetch(url)` returns the Response
// produced by our own endpoint. This proves byte-compatibility with the real
// client, not just with our own framing assumptions.
import { describe, it, expect } from "vitest";
import { HttpAgent } from "@ag-ui/client";
import { EventType } from "@ag-ui/client";
import type { BaseEvent, RunAgentInput } from "@ag-ui/client";

import { handleRun } from "@server/agui/http";
import type { RunDeps } from "@server/agui/run";
import type { Message, SendRequest } from "@shared/types";
import type { MessagePostSourceFactory } from "@server/messagePost/readView";

// --- mock building blocks (same read-view seam as run.test.ts) --------------

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

/** The canonical agent turn the daemon streams: thinking → tool(start+result) → text → turn-end. */
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

function committedMessage(id: string, from: string, body: string): Message {
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

// --- a lens-shaped row sink (mirrors egregore-lens src/runtime/agui/aguiReader.ts) -

type LiveChannel = "content" | "reasoning";

/** A materialized row, the way a consuming UI would accumulate one. */
type Row =
  | { kind: "message"; channel: LiveChannel; id: string; text: string }
  | { kind: "tool"; id: string; name: string; args: string; result?: string };

/**
 * Apply ONE decoded AG-UI event into `rows`, exactly as egregore-lens's
 * `applyAguiEvent` dispatches: text/reasoning brackets accumulate by messageId,
 * tool calls merge by toolCallId. This is the consumer half — if our SSE is
 * byte-compatible, replaying it here reconstructs the turn.
 */
function applyAguiEvent(event: BaseEvent, rows: Row[], runs: string[]): void {
  const e = event as BaseEvent & Record<string, unknown>;
  const str = (v: unknown): string => (typeof v === "string" ? v : "");
  const findMsg = (id: string, channel: LiveChannel): Row => {
    let row = rows.find(
      (r): r is Extract<Row, { kind: "message" }> =>
        r.kind === "message" && r.id === id,
    );
    if (!row) {
      row = { kind: "message", channel, id, text: "" };
      rows.push(row);
    }
    return row;
  };
  const findTool = (id: string): Extract<Row, { kind: "tool" }> => {
    let row = rows.find(
      (r): r is Extract<Row, { kind: "tool" }> => r.kind === "tool" && r.id === id,
    );
    if (!row) {
      row = { kind: "tool", id, name: "tool", args: "" };
      rows.push(row);
    }
    return row;
  };

  switch (e.type) {
    case EventType.RUN_STARTED:
      runs.push(`start:${str(e.runId)}`);
      return;
    case EventType.RUN_FINISHED:
      runs.push("finish");
      return;
    case EventType.RUN_ERROR:
      runs.push("error");
      return;

    case EventType.TEXT_MESSAGE_START:
      findMsg(str(e.messageId), "content");
      return;
    case EventType.TEXT_MESSAGE_CONTENT: {
      const row = findMsg(str(e.messageId), "content");
      if (row.kind === "message") row.text += str(e.delta);
      return;
    }
    case EventType.TEXT_MESSAGE_END:
      return;

    case EventType.REASONING_MESSAGE_START:
      findMsg(str(e.messageId), "reasoning");
      return;
    case EventType.REASONING_MESSAGE_CONTENT: {
      const row = findMsg(str(e.messageId), "reasoning");
      if (row.kind === "message") row.text += str(e.delta);
      return;
    }
    case EventType.REASONING_MESSAGE_END:
      return;

    case EventType.TOOL_CALL_START: {
      const row = findTool(str(e.toolCallId));
      row.name = str(e.toolCallName) || "tool";
      return;
    }
    case EventType.TOOL_CALL_ARGS: {
      const row = findTool(str(e.toolCallId));
      row.args += str(e.delta);
      return;
    }
    case EventType.TOOL_CALL_RESULT: {
      const row = findTool(str(e.toolCallId));
      row.result = str(e.content);
      return;
    }
    default:
      // STATE_DELTA / CUSTOM etc. — not part of the core render path here.
      return;
  }
}

// --- the real decoder: an HttpAgent whose fetch returns our endpoint's Response -

const INPUT: RunAgentInput = {
  threadId: "t_design",
  runId: "r_1",
  state: {},
  messages: [{ id: "u1", role: "user", content: "find the bug" }],
  tools: [],
  context: [],
  forwardedProps: {},
};

/**
 * Build an `HttpAgent` whose injected `fetch` IGNORES the network and instead
 * runs our own endpoint (`handleRun`) with the test deps, returning its SSE
 * `Response`. `agent.run(input)` then decodes that Response with the real
 * `@ag-ui/client` parser. The relay is driven on the next tick (the body is a
 * pull stream, so the fetch resolves before the turn is emitted).
 */
/**
 * Drain an `HttpAgent.run(input)` Observable to an array of decoded events.
 * Uses the Observable's own `subscribe` (rxjs is a transitive dep of
 * `@ag-ui/client`, not a direct one — so we don't import its operators).
 */
function collect(obs: {
  subscribe(o: {
    next: (e: BaseEvent) => void;
    error: (err: unknown) => void;
    complete: () => void;
  }): unknown;
}): Promise<BaseEvent[]> {
  return new Promise((resolve, reject) => {
    const out: BaseEvent[] = [];
    obs.subscribe({
      next: (e) => out.push(e),
      error: reject,
      complete: () => resolve(out),
    });
  });
}

function agentOverEndpoint(deps: RunDeps, source: ReturnType<typeof makeMessageSource>): HttpAgent {
  return new HttpAgent({
    url: "http://test.local/api/agui?thread=design",
    fetch: async (url, init) => {
      const request = new Request(url, {
        method: init?.method ?? "POST",
        headers: init?.headers as HeadersInit,
        body: init?.body as BodyInit,
      });
      const response = await handleRun(request, deps);
      // Drive the committed row on the next tick - after the decoder subscribes.
      queueMicrotask(() => source.emit(committedMessage("m_done", "ben", "here you go")));
      return response;
    },
  });
}

describe("AG-UI bridge e2e — our SSE decoded by the real @ag-ui/client (HttpAgent)", () => {
  it("round-trips the committed message into ordered AG-UI events", async () => {
    const source = makeMessageSource();
    const { sendMessage, sends } = makeSendRecorder();
    const deps: RunDeps = { sendMessage, createMessageSource: source.factory };

    const agent = agentOverEndpoint(deps, source);

    // `agent.run(input)` is the raw decoded BaseEvent Observable — the exact seam
    // egregore-lens reads. Collect every event the real decoder yields.
    const events = await collect(agent.run(INPUT));
    const types = events.map((e) => e.type);

    // The endpoint forwarded our in-mapped send to the daemon.
    expect(sends).toEqual([{ to: { verb: "post", thread: "design" }, body: "find the bug" }]);

    // The REAL decoder materialized the full, correctly-ordered AG-UI run.
    expect(types).toEqual([
      EventType.RUN_STARTED,
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
      EventType.RUN_FINISHED,
    ]);

    // RUN_STARTED carries our thread + run ids verbatim through the real parser.
    const started = events[0] as BaseEvent & { threadId: string; runId: string };
    expect(started.threadId).toBe("t_design");
    expect(started.runId).toBe("r_1");
  });

  it("the decoded events reconstruct the turn's rows via a lens-shaped sink", async () => {
    const source = makeMessageSource();
    const { sendMessage } = makeSendRecorder();
    const deps: RunDeps = { sendMessage, createMessageSource: source.factory };

    const agent = agentOverEndpoint(deps, source);
    const events = await collect(agent.run(INPUT));

    // Replay the decoded stream into a consumer (the egregore-lens reader shape).
    const rows: Row[] = [];
    const runs: string[] = [];
    for (const ev of events) applyAguiEvent(ev, rows, runs);

    // One committed message row from the read-view source.
    expect(rows).toEqual([
      { kind: "message", channel: "content", id: "m_done", text: "here you go" },
    ]);

    // The run was wrapped start…finish (the lifecycle the UI keys off).
    expect(runs).toEqual(["start:r_1", "finish"]);
  });
});
