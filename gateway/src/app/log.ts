// Client-side logger → the durable webconsole log stream (persisted in the
// separate Turso via `POST /api/conversation/logs`). "If it moves it has a log":
// pane opens, every click/interaction, and the domain events (send, run,
// observe, persist, errors) all flow here. One fire-and-forget POST per event —
// Turso handles the async write volume fine; logging must never break the UI.
import { gatewayFetch } from "./gatewayClient";

export type LogLevel = "debug" | "info" | "warn" | "error";

export function logEvent(
  scope: string,
  level: LogLevel,
  message: string,
  opts: { conversationId?: string; data?: unknown; persist?: boolean } = {},
): void {
  // eslint-disable-next-line no-console
  (console[level === "debug" ? "log" : level] ?? console.log)(`[${scope}] ${message}`, opts.data ?? "");
  if (opts.persist === false) return;
  try {
    void gatewayFetch("/api/conversation/logs", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ts: Date.now(),
        level,
        scope,
        conversationId: opts.conversationId,
        message,
        data: opts.data === undefined ? undefined : safeJson(opts.data),
      }),
    }).catch(() => {});
  } catch {
    /* never throw from logging */
  }
}

function safeJson(v: unknown): string | undefined {
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}
