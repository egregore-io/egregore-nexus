import { EventEncoder } from "@ag-ui/encoder";

import { AguiSessionView } from "./aguiView";
import { nexusView } from "./nexusView";
import { sessionFanout, type SessionFanoutHub, type SessionStreamView } from "./sessionFanout";
import { terminalView } from "./terminalView";

export interface SessionEventsDeps {
  fanout?: SessionFanoutHub;
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
  const fanout = deps.fanout ?? sessionFanout;
  const text = new TextEncoder();
  const agui = view === "agui" ? new AguiSessionView(sessionId) : undefined;
  const encoder = view === "agui" ? new EventEncoder() : undefined;
  let subscription: { close(): void } | undefined;
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      const send = (payload: unknown) => {
        controller.enqueue(text.encode(`data: ${JSON.stringify(payload)}\n\n`));
      };
      subscription = fanout.subscribe(sessionId, {
        view,
        after,
        onFrame(frame) {
          if (view === "nexus") {
            const payload = nexusView(frame);
            if (payload) send(payload);
          } else if (view === "terminal") {
            const payload = terminalView(frame);
            if (payload) send(payload);
          } else {
            const projected = agui!.project(frame);
            for (const event of projected?.events ?? []) {
              controller.enqueue(text.encode(encoder!.encodeSSE({
                ...event,
                cursor: projected!.cursor,
                epoch: projected!.epoch,
              })));
            }
          }
        },
        onError(error) {
          send({ type: "stream.error", message: error instanceof Error ? error.message : String(error) });
        },
      });
      request.signal.addEventListener("abort", () => {
        subscription?.close();
        controller.close();
      }, { once: true });
    },
    cancel() {
      subscription?.close();
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
