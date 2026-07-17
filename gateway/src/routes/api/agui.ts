// TanStack Start server route: `POST /api/agui` — the AG-UI agent endpoint that
// DRIVES a bus turn and streams it back as AG-UI SSE. This file is the only
// framework-coupled part; the request→SSE-stream translation lives in the
// framework-agnostic `@server/agui/http` (`handleRun`), which wires `run()` to
// command ingress + AG-UI translation.
//
// TanStack Start server-route API (same shape the REST catch-all `api/v1/$.ts`
// uses, verified against @tanstack/react-start@1.168): `createFileRoute(path)`
// takes a `server.handlers` map keyed by HTTP method; each handler receives
// `{ request, params, pathname }` and returns a `Response`. We return a streaming
// `Response` whose body is the AG-UI SSE `ReadableStream`.
import { createFileRoute } from "@tanstack/react-router";
import type { Client } from "@libsql/client";

import { handleRun } from "@server/agui/http";
import type { RunDeps } from "@server/agui/run";
import { currentHuman } from "@server/identity/human";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
  type WebAuthMode,
} from "@server/auth/webAuthMode";
import { getConversationStore } from "@server/conversation/store";
import type { MessagePostSender } from "@server/api/http";
import {
  createCommandIngressSender,
  type CommandIngressSenderOptions,
} from "@server/messagePost/commandIngress";

function getWriteDbLazy(): Promise<Client> {
  return getConversationStore();
}

function humanCookie(request: Request): string | undefined {
  const header = request.headers.get("cookie");
  if (!header) return undefined;
  for (const part of header.split(";")) {
    const eq = part.indexOf("=");
    if (eq === -1) continue;
    if (part.slice(0, eq).trim() === "nexus_human") return part.slice(eq + 1).trim();
  }
  return undefined;
}

type HandleRun = (request: Request, deps?: RunDeps) => Promise<Response>;

export interface AguiRunWithHumanDeps {
  /** Getter for the write-capable webconsole DB (for currentHuman). */
  db: () => Promise<Client>;
  /** Server-configured web auth facet; production resolves from process env. */
  authMode?: WebAuthMode;
  /** Message Post sender; production defaults to command ingress. */
  messagePost?: MessagePostSender;
  /** Raw daemon DB client for command-intent writes; tests inject an in-memory DB. */
  commandIngressDb?: () => Promise<Client> | Client;
  /** Command ingress tuning/test seams. */
  commandIngress?: Omit<CommandIngressSenderOptions, "db">;
  /** Test seam for avoiding a real SSE run. */
  handleRun?: HandleRun;
}

export function makeRunWithHuman(deps: AguiRunWithHumanDeps) {
  const messagePost =
    deps.messagePost ??
    createCommandIngressSender({
      ...deps.commandIngress,
      ...(deps.commandIngressDb ? { db: deps.commandIngressDb } : {}),
    });
  const runHandler = deps.handleRun ?? handleRun;

  return async function runWithHuman(request: Request): Promise<Response> {
    const mode = deps.authMode ?? webAuthModeFromEnv(process.env);
    const token = humanCookie(request);
    const identity = token
      ? await currentHuman(token, { db: await deps.db() }).catch(() => null)
      : null;
    const caller =
      identity ?? (isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null);
    if (!caller) {
      return new Response(JSON.stringify({ error: "not logged in" }), {
        status: 401,
        headers: { "content-type": "application/json" },
      });
    }
    return runHandler(request, {
      selfName: caller.name,
      project: caller.project,
      sendMessage: (body) => messagePost.send(body, caller),
    });
  };
}

const runWithHuman = makeRunWithHuman({
  db: () => getWriteDbLazy(),
});

export const Route = createFileRoute("/api/agui")({
  server: {
    handlers: {
      // POST drives a run: body = RunAgentInput, target from ?thread=/?dm=/?topic=.
      POST: ({ request }) => runWithHuman(request),
    },
  },
});
