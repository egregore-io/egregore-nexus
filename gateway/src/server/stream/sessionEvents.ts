import { EventEncoder } from "@ag-ui/encoder";

import { AguiSessionView } from "./aguiView";
import { nexusView } from "./nexusView";
import { sessionFanout, type SessionFanoutHub, type SessionStreamView } from "./sessionFanout";
import { terminalView } from "./terminalView";

/** Idle keep-alive cadence, matching the legacy AG-UI SSE paths (`_sseCore`). */
const DEFAULT_HEARTBEAT_MS = 20_000;

export interface SessionEventsDeps {
  fanout?: SessionFanoutHub;
  /** Heartbeat cadence in ms. Test seam; production uses the 20s default. */
  heartbeatIntervalMs?: number;
}

export function handleSessionEvents(
  request: Request,
  sessionId: string,
  deps: SessionEventsDeps = {},
): Response {
  const url = new URL(request.url);
  const view = sessionView(url.searchParams.get("view"));
  if (!view) return json({ error: { code: "bad_request", message: "view must be nexus, agui, or terminal" } }, 400);
  const after = url.searchParams.get("after") ?? undefined;
  const legacyAfterId = url.searchParams.has("afterId")
    ? (url.searchParams.get("afterId") ?? "")
    : undefined;
  const fanout = deps.fanout ?? sessionFanout;
  const text = new TextEncoder();
  const agui = view === "agui" ? new AguiSessionView(sessionId) : undefined;
  const encoder = view === "agui" ? new EventEncoder() : undefined;
  const heartbeatIntervalMs = Math.max(1, deps.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS);
  let subscription: { close(): void } | undefined;
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
      const send = (payload: unknown) => {
        controller.enqueue(text.encode(`data: ${JSON.stringify(payload)}\n\n`));
      };
      subscription = fanout.subscribe(sessionId, {
        view,
        after,
        legacyAfterId,
        onFrame(frame) {
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
            if (encoded.length > 0) controller.enqueue(text.encode(encoded.join("")));
          }
        },
        onError(error) {
          send({ type: "stream.error", message: error instanceof Error ? error.message : String(error) });
        },
      });
      // An idle session emits no frames at all, so without this the connection
      // goes silent and dies on the first idle timeout in the path (undici's
      // 300s body timeout in the WebUI proxy, and any intermediary's own limit).
      // A `:` comment frame is inert: EventSource discards it, and the WebSocket
      // relay drops it in `ssePayload` because it does not start with `data:`.
      heartbeat = setInterval(() => {
        if (closed) return;
        try {
          controller.enqueue(text.encode(": ping\n\n"));
        } catch {
          stop();
        }
      }, heartbeatIntervalMs);
      request.signal.addEventListener("abort", () => {
        stop();
        controller.close();
      }, { once: true });
    },
    cancel() {
      stop();
    },
  });
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
