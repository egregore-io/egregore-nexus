import { EventEncoder } from "@ag-ui/encoder";

import { AguiSessionView } from "./aguiView";
import { nexusView } from "./nexusView";
import { sessionFanout, type SessionFanoutHub, type SessionFanoutFrame, type SessionStreamView } from "./sessionFanout";
import { terminalView } from "./terminalView";
import { subscribeRetainedSession } from "./retainedSessionReplay";
import type { DaemonPushConnector } from "../agui/daemonPushRelay.mjs";

/** Idle keep-alive cadence, matching the legacy AG-UI SSE paths (`_sseCore`). */
const DEFAULT_HEARTBEAT_MS = 20_000;
/** Retained intake pauses at this byte budget, with at most one semantic frame-group overshoot. */
const RETAINED_QUEUE_BYTES = 256 * 1024;

export interface SessionEventsDeps {
  fanout?: SessionFanoutHub;
  /** Heartbeat cadence in ms. Test seam; production uses the 20s default. */
  heartbeatIntervalMs?: number;
  /** Isolated retained-source connection; production never uses the shared fanout connector. */
  retainedConnector?: DaemonPushConnector;
}

export function handleSessionEvents(
  request: Request,
  sessionId: string,
  deps: SessionEventsDeps = {},
): Response {
  const url = new URL(request.url);
  const view = sessionView(url.searchParams.get("view"));
  if (!view) return json({ error: { code: "bad_request", message: "view must be nexus, agui, or terminal" } }, 400);
  const replay = url.searchParams.get("replay");
  if (replay !== null && (replay !== "retained" || url.searchParams.getAll("replay").length !== 1)) {
    return json({ error: { code: "bad_request", message: "replay must be retained when supplied" } }, 400);
  }
  if (replay === "retained" && (view === "terminal" || url.searchParams.has("afterId")
    || url.searchParams.get("expectedSessionId") !== sessionId
    || !url.searchParams.get("agentId")?.trim()
    || url.searchParams.getAll("agentId").length !== 1
    || url.searchParams.getAll("expectedSessionId").length !== 1
    || url.searchParams.getAll("after").length > 1)) {
    return json({ error: { code: "bad_request", message: "retained replay requires an exact agent/session and opaque after cursor" } }, 400);
  }
  const after = url.searchParams.get("after") ?? undefined;
  const legacyAfterId = url.searchParams.has("afterId")
    ? (url.searchParams.get("afterId") ?? "")
    : undefined;
  const fanout = deps.fanout ?? sessionFanout;
  const text = new TextEncoder();
  const agui = view === "agui" ? new AguiSessionView(sessionId) : undefined;
  const encoder = view === "agui" ? new EventEncoder() : undefined;
  const heartbeatIntervalMs = Math.max(1, deps.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS);
  let subscription: { close(): void; pause?(): void; resume?(): void } | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;
  let closed = false;
  // Tear down both the fanout subscription and the keep-alive timer exactly once,
  // so a disconnected client can never leave an interval running.
  const stop = () => {
    closed = true;
    if (heartbeat !== undefined) clearInterval(heartbeat);
    heartbeat = undefined;
    subscription?.close();
  };
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      if (request.signal.aborted) {
        closed = true;
        controller.close();
        return;
      }
      const enqueue = (chunk: Uint8Array) => {
        controller.enqueue(chunk);
        if (replay === "retained" && (controller.desiredSize ?? 0) <= 0) subscription?.pause?.();
      };
      const send = (payload: unknown) => {
        if (closed) return;
        enqueue(text.encode(`data: ${JSON.stringify(payload)}\n\n`));
      };
      const finish = () => {
        if (closed) return;
        stop();
        controller.close();
      };
      const subscriber = {
        view,
        after,
        legacyAfterId,
        onFrame(frame: SessionFanoutFrame) {
          if (closed) return false;
          if (view === "nexus") {
            const payload = nexusView(frame);
            if (payload) send(payload);
          } else if (view === "terminal") {
            const payload = terminalView(frame);
            if (payload) send(payload);
          } else {
            const projected = agui!.project(frame);
            const encoded = (projected?.events ?? []).map((event) => (
              encoder!.encodeSSE({
                ...event,
                cursor: projected!.cursor,
                epoch: projected!.epoch,
              })
            ));
            // Preserve one stream chunk per semantic fanout cursor. The WebSocket relay can then
            // accept or reject the complete projection group without advancing partway through it.
            if (encoded.length > 0) enqueue(text.encode(encoded.join("")));
          }
        },
        onError(error: unknown) {
          send({ type: "stream.error", message: error instanceof Error ? error.message : String(error) });
        },
      };
      subscription = replay === "retained"
        ? subscribeRetainedSession(sessionId, {
          after,
          onSource(value) { send({ type: "CUSTOM", name: "nexus.recording.source", value }); },
          onFrame: subscriber.onFrame,
          onEnd: finish,
        }, deps.retainedConnector)
        : fanout.subscribe(sessionId, subscriber);
      // An idle session emits no frames at all, so without this the connection
      // goes silent and dies on the first idle timeout in the path (undici's
      // 300s body timeout in the WebUI proxy, and any intermediary's own limit).
      // A `:` comment frame is inert: EventSource discards it, and the WebSocket
      // relay drops it in `ssePayload` because it does not start with `data:`.
      heartbeat = setInterval(() => {
        if (closed) return;
        if (replay === "retained" && (controller.desiredSize ?? 0) <= 0) return;
        try {
          enqueue(text.encode(": ping\n\n"));
        } catch {
          stop();
        }
      }, heartbeatIntervalMs);
      request.signal.addEventListener("abort", () => {
        finish();
      }, { once: true });
    },
    cancel() {
      stop();
    },
    pull(controller) {
      if (!closed && replay === "retained" && (controller.desiredSize ?? 0) > 0) subscription?.resume?.();
    },
  }, replay === "retained" ? { highWaterMark: RETAINED_QUEUE_BYTES, size: (chunk) => chunk.byteLength } : undefined);
  return new Response(body, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
    },
  });
}

function sessionView(value: string | null): SessionStreamView | null {
  if (value === "nexus" || value === "agui" || value === "terminal") return value;
  return value === null ? "nexus" : null;
}

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
