// the HTTP adapters that turn `run`/`observe` into framework
// `Response`s. Kept OUT of the route files so the framework coupling there is a
// one-liner and the request→stream translation is plain, testable code here.
//
// Both endpoints answer with `text/event-stream`: the `ReadableStream<Uint8Array>`
// the orchestrator produces is handed straight to the `Response` body (AG-UI SSE
// framing is `data: <json>\n\n`, which `@ag-ui/client`'s decoder reads).
import { run, observe } from "@server/agui/run";
import type { RunDeps, RunAgentInput } from "@server/agui/run";
import { observeAgentSession } from "@server/agui/agentSession";
import type { SendTarget } from "@shared/types";

/** SSE response headers — no buffering, keep the connection open. */
const SSE_HEADERS: Record<string, string> = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
  // Defeat proxy buffering (nginx) so frames flush as they're produced.
  "x-accel-buffering": "no",
};

/** Wrap an AG-UI SSE byte stream in a streaming `Response`. */
function sseResponse(stream: ReadableStream<Uint8Array>): Response {
  return new Response(stream, { status: 200, headers: SSE_HEADERS });
}

/** A 400 with a plain JSON body (bad target / unparseable run input). */
function badRequest(message: string): Response {
  return new Response(JSON.stringify({ error: message }), {
    status: 400,
    headers: { "content-type": "application/json" },
  });
}

/**
 * Resolve the conversation target from query params. `?thread=<name>` → a
 * thread post; `?dm=<name>` → a named DM; `?agentId=<id>` → an identity-addressed
 * DM; `?topic=<name>` → a publish. Exactly one target must be present.
 */
export function targetFromQuery(url: URL): SendTarget | null {
  const thread = url.searchParams.get("thread");
  const dm = url.searchParams.get("dm");
  const agentId = url.searchParams.get("agentId");
  const topic = url.searchParams.get("topic");
  if (thread) return { verb: "post", thread };
  if (agentId) return { verb: "dm", agentId, ...(dm ? { name: dm } : {}) };
  if (dm) return { verb: "dm", name: dm };
  if (topic) return { verb: "publish", topic };
  return null;
}

/**
 * `POST /api/agui` — DRIVE a run. The JSON body is an AG-UI `RunAgentInput`; the
 * target comes from the query (`?thread=` / `?dm=` / `?topic=`). Returns the
 * run's SSE stream (RUN_STARTED → … → RUN_FINISHED), or a 400 on a bad request.
 */
export async function handleRun(
  request: Request,
  deps: RunDeps = {},
): Promise<Response> {
  const url = new URL(request.url);
  const target = targetFromQuery(url);
  if (!target) {
    return badRequest("missing target: pass ?thread=, ?dm=, ?agentId=, or ?topic=");
  }

  let input: RunAgentInput;
  try {
    input = (await request.json()) as RunAgentInput;
  } catch {
    return badRequest("body must be a JSON RunAgentInput");
  }
  if (!input || !Array.isArray(input.messages)) {
    return badRequest("RunAgentInput.messages must be an array");
  }

  return sseResponse(run(input, target, deps));
}

/**
 * `GET /api/agui/observe` — WATCH a thread's runs (no `send`). The target comes
 * from the query (`?thread=` / `?dm=` / `?topic=`). Returns the continuous SSE
 * stream (a RUN_STARTED…RUN_FINISHED per observed turn), or a 400.
 */
export function handleObserve(request: Request, deps: RunDeps = {}): Response {
  const url = new URL(request.url);
  const target = targetFromQuery(url);
  if (!target) {
    return badRequest("missing target: pass ?thread=, ?dm=, ?agentId=, or ?topic=");
  }
  const after = Number(url.searchParams.get("after") ?? "");
  const messageAfter = Number.isFinite(after) && after > 0 ? Math.floor(after) : undefined;
  const afterRowid = Number(url.searchParams.get("afterRowid") ?? "");
  const messageAfterRowid =
    Number.isFinite(afterRowid) && afterRowid > 0 ? Math.floor(afterRowid) : undefined;
  return sseResponse(observe(target, { ...deps, messageAfter, messageAfterRowid }));
}

/**
 * `GET /api/agui/observe?session=<name>` — WATCH a dedicated agent session stream
 * (Lane B only). The `session` param names the agent; `deps.initialEvents` may carry compact
 * materialized history, and `deps.createRelay` must be a store-projection relay scoped to that
 * session for the live tail. This path does not construct a Message Post target;
 * `/agent` is session-addressed and labeled separately from DM/thread/publish observe.
 */
export function handleAgentSession(
  request: Request,
  deps: RunDeps & { sessionName: string },
): Response {
  const { sessionName, ...runDeps } = deps;
  if (!runDeps.createRelay) {
    return badRequest("missing session stream projection");
  }
  return sseResponse(observeAgentSession(sessionName, runDeps));
}
