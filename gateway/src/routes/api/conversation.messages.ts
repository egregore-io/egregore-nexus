// `/api/conversation/messages` — the web console's OWN conversation persistence
// edge (its separate read-write Turso/libSQL store, NOT the daemon view).
//   GET  ?conversation=<id>  → the stored backlog (replayed on pane mount / refresh)
//   POST { ...StoredMessage } → persist/upsert one message (operator send or a
//                               decoded agent turn captured off `observe`)
import { createFileRoute } from "@tanstack/react-router";

import {
  getConversationStore,
  getMessages,
  saveMessage,
  type StoredMessage,
} from "@server/conversation/store";

const DEFAULT_BACKLOG_LIMIT = 100;
const MAX_BACKLOG_LIMIT = 500;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function backlogLimit(raw: string | null): number {
  if (!raw) return DEFAULT_BACKLOG_LIMIT;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_BACKLOG_LIMIT;
  return Math.min(n, MAX_BACKLOG_LIMIT);
}

async function handleGet(request: Request): Promise<Response> {
  const params = new URL(request.url).searchParams;
  const conversation = params.get("conversation");
  if (!conversation) return json({ error: "missing ?conversation=" }, 400);
  const db = await getConversationStore();
  return json({ messages: await getMessages(db, conversation, backlogLimit(params.get("limit"))) });
}

async function handlePost(request: Request): Promise<Response> {
  let body: Partial<StoredMessage>;
  try {
    body = (await request.json()) as Partial<StoredMessage>;
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }
  const { id, conversationId, role, author, content, status, createdAt } = body;
  if (!id || !conversationId || !role || content === undefined) {
    return json({ error: "id, conversationId, role, content are required" }, 400);
  }
  const db = await getConversationStore();
  await saveMessage(db, {
    id,
    conversationId,
    role,
    author: author ?? role,
    content,
    status: status === "streaming" ? "streaming" : "final",
    createdAt: typeof createdAt === "number" ? createdAt : Date.now(),
  });
  return json({ ok: true }, 201);
}

export const Route = createFileRoute("/api/conversation/messages")({
  server: {
    handlers: {
      GET: ({ request }) => handleGet(request),
      POST: ({ request }) => handlePost(request),
    },
  },
});
