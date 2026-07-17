// `/api/projects` — compatibility edge for the removed project-group feature.
// Project is now only the daemon scope label; no gateway grouping is persisted.
import { createFileRoute } from "@tanstack/react-router";

import { listProjects, projectsStore } from "@server/projects/store";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function handleGet(): Promise<Response> {
  const db = await projectsStore();
  return json({ projects: await listProjects(db) });
}

async function handlePost(request: Request): Promise<Response> {
  let body: { name?: unknown } | undefined;
  try {
    body = (await request.json()) as { name?: unknown };
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }
  const name = typeof body.name === "string" ? body.name.trim() : "";
  if (!name) return json({ error: "name is required" }, 400);
  return json({ error: "project groups are no longer supported" }, 410);
}

async function handleDelete(request: Request): Promise<Response> {
  const id = new URL(request.url).searchParams.get("id");
  if (!id) return json({ error: "missing ?id=" }, 400);
  return json({ error: "project groups are no longer supported" }, 410);
}

/** Dispatches the project-metadata compatibility edge for the API-only Gateway. */
export async function handleProjectsRequest(request: Request): Promise<Response> {
  switch (request.method) {
    case "GET": return handleGet();
    case "POST": return handlePost(request);
    case "DELETE": return handleDelete(request);
    default: return json({ error: "method not allowed" }, 405);
  }
}

export const Route = createFileRoute("/api/projects")({
  server: {
    handlers: {
      GET: ({ request }) => handleProjectsRequest(request),
      POST: ({ request }) => handleProjectsRequest(request),
      DELETE: ({ request }) => handleProjectsRequest(request),
    },
  },
});
