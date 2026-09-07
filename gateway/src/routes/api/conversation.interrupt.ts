// `POST /api/conversation/interrupt` — stop the active agent-session turn without
// injecting replacement input. The daemon command is durable and caller-bound;
// completion is projected back to the session WebSocket as `command.receipt`.
import { createFileRoute } from "@tanstack/react-router";
import type { Client } from "@libsql/client";

import {
  COMMAND_KINDS,
  submitCommandIntent,
  type CommandIngressOptions,
} from "@server/command/ingress";
import { GatewayError } from "@server/api/http";
import { getConversationStore } from "@server/conversation/store";
import { currentHuman } from "@server/identity/human";
import { parseCookies } from "@server/http/cookies";
import type { InterruptResponse } from "@shared/types";
import {
  exactSessionError,
  exactSessionResultError,
} from "@server/command/sessionQueue";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
} from "@server/auth/webAuthMode";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

export interface ConversationInterruptDeps {
  env?: NodeJS.ProcessEnv;
  getWriteDb?: () => Promise<Client>;
  commandIngress?: CommandIngressOptions;
}

function statusForError(err: unknown): number {
  if (err instanceof GatewayError && err.code >= 400 && err.code <= 599) return err.code;
  return 502;
}

export async function handleConversationInterruptPost(
  request: Request,
  deps: ConversationInterruptDeps = {},
): Promise<Response> {
  let body: {
    name?: string;
    agentId?: string;
    expectedSessionId?: string;
    clientMessageId?: string;
  };
  try {
    body = (await request.json()) as typeof body;
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }
  const selectorError = exactSessionError(body);
  if (selectorError) return json({ error: selectorError }, 400);
  const targetName = body.name ?? body.agentId;
  if (!targetName) return json({ error: "name is required" }, 400);

  const mode = webAuthModeFromEnv(deps.env ?? process.env);
  const token = parseCookies(request.headers.get("cookie")).get("nexus_human");
  const getWriteDb = deps.getWriteDb ?? getConversationStore;
  const cookieIdentity = token
    ? await currentHuman(token, { db: await getWriteDb() }).catch(() => null)
    : null;
  const identity = cookieIdentity
    ?? (isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null);
  if (!identity) return json({ error: "not logged in" }, 401);

  try {
    const result = await submitCommandIntent<InterruptResponse>(
      COMMAND_KINDS.harnessInterrupt,
      {
        name: targetName,
        ...(body.agentId ? { agentId: body.agentId } : {}),
        ...(body.expectedSessionId
          ? { expectedSessionId: body.expectedSessionId }
          : {}),
        ...(body.clientMessageId ? { clientMessageId: body.clientMessageId } : {}),
      },
      identity,
      deps.commandIngress,
      body.clientMessageId,
    );
    const resultError = exactSessionResultError(body.expectedSessionId, result);
    if (resultError) return json({ error: resultError }, 502);
    return json({ ok: true, result }, 201);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return json({ error: message }, statusForError(err));
  }
}

export const Route = createFileRoute("/api/conversation/interrupt")({
  server: {
    handlers: {
      POST: ({ request }) => handleConversationInterruptPost(request),
    },
  },
});
