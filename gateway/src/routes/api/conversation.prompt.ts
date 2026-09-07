// `POST /api/conversation/prompt` — the DIRECT operator→agent session send.
// Durably enqueues a daemon-owned `harness.prompt` command intent, which injects
// the text straight into the agent's ACP/headed session (NO bus, no router, no
// "operator must be a member"). The HTTP receipt confirms durable acceptance;
// turn execution and the reply continue over the agent-session observe lane
// (`/api/agui/observe?session=...`).
import { createFileRoute } from "@tanstack/react-router";
import type { Client } from "@libsql/client";

import {
  COMMAND_KINDS,
  enqueueCommandIntent,
  type CommandIngressOptions,
} from "@server/command/ingress";
import { GatewayError } from "@server/api/http";
import { getConversationStore } from "@server/conversation/store";
import { currentHuman } from "@server/identity/human";
import { parseCookies } from "@server/http/cookies";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
} from "@server/auth/webAuthMode";
import {
  handleConversationQueueGet,
  handleConversationQueuePost,
  exactSessionError,
  exactSessionResultError,
} from "@server/command/sessionQueue";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function agentCommandTarget(id: string): { name: string; agentId?: string } {
  return { name: id };
}

function getWriteDbLazy(): Promise<Client> {
  return getConversationStore();
}

interface ConversationPromptDeps {
  env?: NodeJS.ProcessEnv;
  getWriteDb?: () => Promise<Client>;
  commandIngress?: CommandIngressOptions;
}

function statusForError(err: unknown): number {
  if (err instanceof GatewayError && err.code >= 400 && err.code <= 599) {
    return err.code;
  }
  return 502;
}

export async function handleConversationPromptPost(
  request: Request,
  deps: ConversationPromptDeps = {},
): Promise<Response> {
  let body: {
    name?: string;
    agentId?: string;
    expectedSessionId?: string;
    text?: string;
    clientMessageId?: string;
    delivery?: unknown;
    modelSelection?: unknown;
  };
  try {
    body = (await request.json()) as typeof body;
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }
  const selectorError = exactSessionError(body);
  if (selectorError) return json({ error: selectorError }, 400);
  // Never discard an explicit model selection or delivery policy and enqueue a different
  // operation on the user's behalf.
  for (const option of ["delivery", "modelSelection"] as const) {
    if (body[option] != null) {
      return json({ error: `${option} is not supported by this prompt endpoint` }, 400);
    }
  }
  const targetName = body.name ?? body.agentId;
  if (!targetName || !body.text) return json({ error: "name and text are required" }, 400);
  const target = {
    ...agentCommandTarget(targetName),
    ...(body.agentId ? { agentId: body.agentId } : {}),
    ...(body.expectedSessionId
      ? { expectedSessionId: body.expectedSessionId }
      : {}),
  };

  // Same identity resolution as every other gateway surface (`/api/v1/$`,
  // `agui.observe`): cookie first, then the local-operator caller in local web
  // auth mode. Without the fallback a zero-login local console gets a 401
  // ("command intent requires an explicit Principal") on every /agent send.
  const mode = webAuthModeFromEnv(deps.env ?? process.env);
  const token = parseCookies(request.headers.get("cookie")).get("nexus_human");
  const getWriteDb = deps.getWriteDb ?? getWriteDbLazy;
  const cookieIdentity = token
    ? await currentHuman(token, { db: await getWriteDb() }).catch(() => null)
    : null;
  const identity =
    cookieIdentity ?? (isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null);

  try {
    const queued = await enqueueCommandIntent(
      COMMAND_KINDS.harnessPrompt,
      {
        ...target,
        text: body.text,
        ...(body.clientMessageId ? { clientMessageId: body.clientMessageId } : {}),
      },
      identity ?? undefined,
      deps.commandIngress,
      body.clientMessageId,
    );
    const resultError = exactSessionResultError(body.expectedSessionId, queued);
    if (resultError) return json({ error: resultError }, 502);
    return json({
      ok: true,
      receipt: {
        commandId: queued.commandId,
        ...(body.clientMessageId ? { clientMessageId: body.clientMessageId } : {}),
        ...(queued.sessionId ? { sessionId: queued.sessionId } : {}),
        state: queueState(queued.status),
        revision: queued.revision,
        seq: queued.seq,
      },
    }, 201);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return json({ error: message }, statusForError(err));
  }
}

/** Dispatches the session queue/prompt edge for the API-only Gateway. */
export async function handleConversationPromptRequest(request: Request): Promise<Response> {
  switch (request.method) {
    case "GET": return handleConversationQueueGet(request);
    case "POST": return handleConversationPromptPost(request);
    case "PATCH": return handleConversationQueuePost(request);
    default: return json({ error: "method not allowed" }, 405);
  }
}

function queueState(status: string): string {
  switch (status) {
    case "pending": return "queued";
    case "claimed": return "claimed";
    case "done": return "completed";
    case "error": return "failed";
    case "cancelled": return "cancelled";
    default: return "failed";
  }
}

export const Route = createFileRoute("/api/conversation/prompt")({
  server: {
    handlers: {
      GET: ({ request }) => handleConversationPromptRequest(request),
      POST: ({ request }) => handleConversationPromptRequest(request),
      PATCH: ({ request }) => handleConversationPromptRequest(request),
    },
  },
});
