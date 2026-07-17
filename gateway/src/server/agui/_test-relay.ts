// Shared test helpers for agui stream tests — relay + send mocks.
// Not a test file itself; imported by run.test.ts and messagePost.test.ts.
import type { RealtimeRelay, RealtimeRelayOptions } from "@server/agui/relayTypes";
import type { WsEvent, SendRequest } from "@shared/types";

/** A relay whose frames the test drives by hand (no socket). */
export interface ControllableRelay {
  /** The factory to hand `run`/`observe`/`observeMessagePost` as `deps.createRelay`. */
  factory: (opts: RealtimeRelayOptions) => RealtimeRelay;
  /** Push one WsEvent to whatever `onEvent` the orchestrator registered. */
  emit(ev: WsEvent): void;
  /** True once the orchestrator closed the relay. */
  readonly closed: boolean;
  /** The options the orchestrator used to build the relay (filter inspection). */
  readonly opts: RealtimeRelayOptions | null;
}

export function makeRelay(): ControllableRelay {
  let onEvent: ((ev: WsEvent) => void) | null = null;
  let closed = false;
  let opts: RealtimeRelayOptions | null = null;
  return {
    factory(o) {
      opts = o;
      onEvent = o.onEvent;
      return {
        ready: Promise.resolve(),
        close() {
          closed = true;
        },
      };
    },
    emit(ev) {
      onEvent?.(ev);
    },
    get closed() {
      return closed;
    },
    get opts() {
      return opts;
    },
  };
}

/** A mock Message Post sender that records every send call. */
export function makeSendRecorder() {
  const sends: SendRequest[] = [];
  return {
    sends,
    sendMessage: async (req: SendRequest) => {
      sends.push(req);
      return { ok: true };
    },
  };
}

/** Read a ReadableStream<Uint8Array> fully into a single decoded string. */
export async function readAll(stream: ReadableStream<Uint8Array>): Promise<string> {
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
export function decodeSse(text: string): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = [];
  for (const line of text.split("\n")) {
    if (!line.startsWith("data:")) continue;
    const json = line.slice(5).trim();
    if (!json) continue;
    out.push(JSON.parse(json) as Record<string, unknown>);
  }
  return out;
}

export const eventTypes = (events: Array<Record<string, unknown>>): string[] =>
  events.map((e) => e["type"] as string);
