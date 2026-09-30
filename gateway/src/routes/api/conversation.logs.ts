// `/api/conversation/logs` — the web console's durable log stream, in the SAME
// separate webconsole store. "If it moves it has a log": the client + server
// append every move (send, run POST, observe connect/events, persist, errors)
// here, and the in-UI log drawer tails it.
//   GET  ?afterSeq=<n>&conversation=<id>  → log lines newer than `afterSeq`
//   POST { ts?, level, scope, conversationId?, message, data? } → append one line
import { createFileRoute } from "@tanstack/react-router";

import {
  getConversationStore,
  appendLogs,
  getLogs,
  type StoredLog,
} from "@server/conversation/store";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function handleGet(request: Request): Promise<Response> {
  const u = new URL(request.url).searchParams;
  const db = await getConversationStore();
  const logs = await getLogs(db, {
    afterSeq: Number(u.get("afterSeq") ?? 0) || 0,
    limit: Number(u.get("limit") ?? 300) || 300,
    conversationId: u.get("conversation") ?? undefined,
  });
  return json({ logs });
}

async function handlePost(request: Request): Promise<Response> {
  let body: Partial<StoredLog> | { logs?: Partial<StoredLog>[] };
  try {
    body = (await request.json()) as Partial<StoredLog> | { logs?: Partial<StoredLog>[] };
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }
  const lines = Array.isArray((body as { logs?: Partial<StoredLog>[] }).logs)
    ? (body as { logs: Partial<StoredLog>[] }).logs
    : [body as Partial<StoredLog>];
  const db = await getConversationStore();
  const valid: StoredLog[] = [];
  for (const l of lines) {
    if (!l.scope || !l.message) continue;
    valid.push({
      ts: typeof l.ts === "number" ? l.ts : Date.now(),
      level: l.level ?? "info",
      scope: l.scope,
      conversationId: l.conversationId,
      message: l.message,
      data: l.data,
    });
  }
  await appendLogs(db, valid);
  return json({ ok: true, written: valid.length }, 201);
}

/** Dispatches durable WebUI operational logs at the Gateway edge. */
export async function handleConversationLogsRequest(request: Request): Promise<Response> {
  switch (request.method) {
    case "GET": return handleGet(request);
    case "POST": return handlePost(request);
    default: return json({ error: "method not allowed" }, 405);
  }
}

export const Route = createFileRoute("/api/conversation/logs")({
  server: {
    handlers: {
      GET: ({ request }) => handleConversationLogsRequest(request),
      POST: ({ request }) => handleConversationLogsRequest(request),
    },
  },
});
