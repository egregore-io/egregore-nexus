// `/api/projects/assign` — compatibility no-op for the removed project-group feature.
import { createFileRoute } from "@tanstack/react-router";

import { assignThread, projectsStore } from "@server/projects/store";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export async function handleProjectsAssignRequest(request: Request): Promise<Response> {
  if (request.method !== "POST") return json({ error: "method not allowed" }, 405);
  let body: { threadName?: unknown; projectId?: unknown };
  try {
    body = (await request.json()) as { threadName?: unknown; projectId?: unknown };
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }
  const threadName = typeof body.threadName === "string" ? body.threadName.trim() : "";
  if (!threadName) return json({ error: "threadName is required" }, 400);
  // `projectId` may be a string (assign/move) or explicit null (unassign).
  const projectId =
    body.projectId === null
      ? null
      : typeof body.projectId === "string" && body.projectId
        ? body.projectId
        : undefined;
  if (projectId === undefined) {
    return json({ error: "projectId must be a string or null" }, 400);
  }
  const db = await projectsStore();
  await assignThread(db, { threadName, projectId, createdAt: Date.now() });
  return json({ ok: true, ignored: true });
}

export const Route = createFileRoute("/api/projects/assign")({
  server: {
    handlers: {
      POST: ({ request }) => handleProjectsAssignRequest(request),
    },
  },
});
