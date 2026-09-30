// `POST /api/conversation/steer` — explicit same-turn Codex steering.
//
// Unlike `harness.prompt`, this command does not join the normal boundary queue. The daemon may
// inject it into the active native Codex turn or issue a separate fresh turn when that turn has
// just ended. The route waits for the daemon's final result; a merely claimed command is never
// reported as accepted.
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
import type { SteerResponse } from "@shared/types";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
} from "@server/auth/webAuthMode";

// Stable Nexus application error for an active-turn operation that raced with completion.
const ACTIVE_TURN_REQUIRED = -32006;

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

export interface ConversationSteerDeps {
  env?: NodeJS.ProcessEnv;
  getWriteDb?: () => Promise<Client>;
  commandIngress?: CommandIngressOptions;
}

function statusForError(err: unknown): number {
  if (err instanceof GatewayError && err.code === ACTIVE_TURN_REQUIRED) {
    return 409;
  }
  if (err instanceof GatewayError && err.code >= 400 && err.code <= 599) {
    return err.code;
  }
  return 502;
}

export async function handleConversationSteerPost(
  request: Request,
  deps: ConversationSteerDeps = {},
): Promise<Response> {
  let body: { name?: string; agentId?: string; text?: string; clientMessageId?: string };
  try {
    body = (await request.json()) as typeof body;
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }

  const targetName = body.name ?? body.agentId;
  if (!targetName || !body.text) return json({ error: "name and text are required" }, 400);
  const target = {
    ...agentCommandTarget(targetName),
    ...(body.agentId ? { agentId: body.agentId } : {}),
  };

  const mode = webAuthModeFromEnv(deps.env ?? process.env);
  const token = parseCookies(request.headers.get("cookie")).get("nexus_human");
  const getWriteDb = deps.getWriteDb ?? getWriteDbLazy;
  const cookieIdentity = token
    ? await currentHuman(token, { db: await getWriteDb() }).catch(() => null)
    : null;
  const identity =
    cookieIdentity ?? (isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null);
  if (!identity) return json({ error: "not logged in" }, 401);

  try {
    const result = await submitCommandIntent<SteerResponse>(
      COMMAND_KINDS.harnessSteer,
      {
        ...target,
        text: body.text,
        ...(body.clientMessageId ? { clientMessageId: body.clientMessageId } : {}),
      },
      identity,
      deps.commandIngress,
      body.clientMessageId,
    );
    return json({ ok: true, result }, 201);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return json({ error: message }, statusForError(err));
  }
}

export const Route = createFileRoute("/api/conversation/steer")({
  server: {
    handlers: {
      POST: ({ request }) => handleConversationSteerPost(request),
    },
  },
});
