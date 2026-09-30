// POST /api/login — registers a web-console human and sets the session cookie.
// GET  /api/login — session check: 200 { name, project } when valid, 401 otherwise.
// DI seam: `makeLoginHandler` / `makeGetLoginHandler` are exported for testing;
// the TanStack Route wires in real deps. Keep it additive: no cookie on error.
import { createFileRoute } from "@tanstack/react-router";

import {
  registerHuman,
  currentHuman,
  HumanAuthError,
  type HumanDeps,
} from "@server/identity/human";
import type { CommandIntentSender } from "@server/api/http";
import {
  createCommandIngressSubmitter,
  type CommandIngressOptions,
} from "@server/command/ingress";
import { createConversationStore } from "@server/conversation/store";
import type { Client } from "@libsql/client";
import { parseCookies } from "@server/http/cookies";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
  type WebAuthMode,
} from "@server/auth/webAuthMode";

// ── DI types ─────────────────────────────────────────────────────────────────

export interface LoginHandlerDeps {
  /** Getter for the write-capable webconsole DB (lazy for tests). */
  db: () => Promise<Client>;
  /** Store-backed command sender for daemon-owned identity registration. */
  commands: CommandIntentSender;
  /** Random id generator. `registerHuman` uses two ids; the handler uses one more for CSRF. */
  genId: () => string;
  /** Epoch-ms clock. */
  now: () => number;
}

export interface GetLoginHandlerDeps {
  db: () => Promise<Client>;
  authMode?: WebAuthMode;
}

// ── Response helper ───────────────────────────────────────────────────────────

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

// ── POST handler factory ──────────────────────────────────────────────────────

/**
 * Build the POST handler for `/api/login`.  Exported for unit testing.
 * The real route wires in command ingress + `createConversationStore`.
 */
export function makeLoginHandler(deps: LoginHandlerDeps) {
  return async function handlePost(request: Request): Promise<Response> {
    let body: { name?: unknown; password?: unknown };
    try {
      body = (await request.json()) as { name?: unknown; password?: unknown };
    } catch {
      return json({ error: "body must be JSON" }, 400);
    }

    const name =
      typeof body.name === "string" ? body.name.trim() : "";
    if (!name) return json({ error: "name is required" }, 400);
    const password = typeof body.password === "string" ? body.password : "";
    if (!password) return json({ error: "password is required" }, 400);

    const humanDeps: HumanDeps = {
      db: await deps.db(),
      commands: deps.commands,
      genId: deps.genId,
      now: deps.now,
    };

    let cookieToken: string;
    try {
      const result = await registerHuman({ name, password }, humanDeps);
      cookieToken = result.cookieToken;
    } catch (err) {
      if (err instanceof HumanAuthError) {
        return json({ error: err.message }, 401);
      }
      const msg = err instanceof Error ? err.message : String(err);
      return json({ error: `registration failed: ${msg}` }, 500);
    }

    const csrfToken = deps.genId();
    const sessionCookie = [
      `nexus_human=${cookieToken}`,
      "Path=/",
      "HttpOnly",
      "SameSite=Lax",
    ].join("; ");
    const csrfCookie = [
      `nexus_csrf=${csrfToken}`,
      "Path=/",
      "SameSite=Lax",
    ].join("; ");

    const headers = new Headers({ "content-type": "application/json" });
    headers.append("set-cookie", sessionCookie);
    headers.append("set-cookie", csrfCookie);

    return new Response(JSON.stringify({ ok: true, csrfToken }), {
      status: 200,
      headers,
    });
  };
}

// ── GET handler factory ───────────────────────────────────────────────────────

/**
 * Build the GET handler for `/api/login`.  Returns 200 { name, project } when
 * the cookie is valid, 401 otherwise.  Exported for unit testing.
 */
export function makeGetLoginHandler(deps: GetLoginHandlerDeps) {
  return async function handleGet(request: Request): Promise<Response> {
    const mode = deps.authMode ?? webAuthModeFromEnv(process.env);
    const token = parseCookies(request.headers.get("cookie")).get("nexus_human");
    const identity = token
      ? await currentHuman(token, { db: await deps.db() }).catch(() => null)
      : null;

    if (!identity && isLocalOperatorWebAuthMode(mode)) {
      const local = localOperatorCaller();
      return json({ name: local.name, project: local.project, mode });
    }
    if (!identity) return json({ error: "not logged in" }, 401);

    return json({ name: identity.name, project: identity.project });
  };
}

// ── Lazy real deps ────────────────────────────────────────────────────────────

// The conversation store is lazy (first request) to avoid constructing the DB
// handle at module load time (mirrors the read-DB pattern in api/v1/$.ts).
let dbPromise: Promise<Client> | undefined;
function getDbLazy(): Promise<Client> {
  if (!dbPromise) {
    dbPromise = createConversationStore();
    dbPromise.catch(() => { dbPromise = undefined; });
  }
  return dbPromise;
}

function newId(): string {
  const c = globalThis.crypto;
  if (c?.randomUUID) return c.randomUUID().replace(/-/g, "").slice(0, 24);
  return `${Date.now().toString(36)}${Math.floor(Math.random() * 1e9).toString(36)}`;
}

const realHandler = makeLoginHandler({
  db: () => getDbLazy(),
  commands: createCommandIngressSubmitter({} satisfies CommandIngressOptions),
  genId: newId,
  now: () => Date.now(),
});

const realGetHandler = makeGetLoginHandler({ db: () => getDbLazy() });

/** Dispatches the login edge for both TanStack Start and the API-only Gateway. */
export async function handleLoginRequest(request: Request): Promise<Response> {
  switch (request.method) {
    case "GET": return realGetHandler(request);
    case "POST": return realHandler(request);
    default: return json({ error: "method not allowed" }, 405);
  }
}

// ── TanStack Start route ──────────────────────────────────────────────────────

export const Route = createFileRoute("/api/login")({
  server: {
    handlers: {
      GET: ({ request }) => handleLoginRequest(request),
      POST: ({ request }) => handleLoginRequest(request),
    },
  },
});
