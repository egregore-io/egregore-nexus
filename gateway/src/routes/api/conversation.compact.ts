// `POST /api/conversation/compact` — trigger NATIVE context compaction on an
// agent's session. Enqueues a daemon-owned `harness.compact` command intent:
// codex app-server sessions run `thread/compact/start`; headed PTY sessions get
// the typed `/compact`; transports without a compaction verb return an explicit
// error (never a fake "delivered"). Progress/result render on the observe lane.
import { createFileRoute } from "@tanstack/react-router";
import type { Client } from "@libsql/client";

import { COMMAND_KINDS, submitCommandIntent } from "@server/command/ingress";
import { GatewayError } from "@server/api/http";
import { createConversationStore } from "@server/conversation/store";
import { currentHuman } from "@server/identity/human";
import { parseCookies } from "@server/http/cookies";
import type { CompactResponse } from "@shared/types";
import {
  exactSessionError,
  exactSessionResultError,
} from "@server/command/sessionQueue";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
} from "@server/auth/webAuthMode";

// Native compaction may wait for an active turn to settle and then perform model-backed
// summarization. Keep the generic IPC deadline short, but give this explicitly long-running
// operation the same bounded window its WebSocket command surface advertises.
const COMPACT_COMMAND_TIMEOUT_MS = 120_000;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function agentCommandTarget(id: string): { name: string; agentId?: string } {
  return { name: id };
}

let writeDbPromise: Promise<Client> | undefined;
function getWriteDbLazy(): Promise<Client> {
  if (!writeDbPromise) {
    writeDbPromise = createConversationStore();
    writeDbPromise.catch(() => {
      writeDbPromise = undefined;
    });
  }
  return writeDbPromise;
}

function statusForError(err: unknown): number {
  if (err instanceof GatewayError && err.code >= 400 && err.code <= 599) {
    return err.code;
  }
  return 502;
}

export async function handleConversationCompactPost(request: Request): Promise<Response> {
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
  const mode = webAuthModeFromEnv(process.env);
  const token = parseCookies(request.headers.get("cookie")).get("nexus_human");
  const cookieIdentity = token
    ? await currentHuman(token, { db: await getWriteDbLazy() }).catch(() => null)
    : null;
  const identity =
    cookieIdentity ?? (isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null);

  try {
    const result = await submitCommandIntent<CompactResponse>(
      COMMAND_KINDS.harnessCompact,
      {
        ...target,
        ...(body.clientMessageId ? { clientMessageId: body.clientMessageId } : {}),
      },
      identity ?? undefined,
      { timeoutMs: COMPACT_COMMAND_TIMEOUT_MS },
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

export const Route = createFileRoute("/api/conversation/compact")({
  server: {
    handlers: {
      POST: ({ request }) => handleConversationCompactPost(request),
    },
  },
});
