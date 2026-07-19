// TanStack Start catch-all SERVER ROUTE mounting the public HTTP API under
// `/api/v1/*`. This file is the ONLY framework-coupled part of the public API: it
// adapts the framework `Request` into the plain `ApiRequest` shape and writes the
// router's `{ status, body }` back as a framework `Response`. All the logic —
// routing, auth, validation, dispatch, error mapping — lives in the
// framework-agnostic, fully-tested `@server/api/router` (`handle`).
//
// Cookie→caller wiring: each request reads the `nexus_human` cookie, resolves it
// via `currentHuman`, and carries that identity into both the Message Post
// sender and command-intent writes. No cookie / unknown cookie → existing
// behavior (no caller) for routes that require one.
import { createFileRoute } from "@tanstack/react-router";

import { handle } from "@server/api/router";
import type { ApiRequest, ReadDbGetter } from "@server/api/router";
import type {
  CommandIntentSender,
  HookDiagnosticsReader,
  MessagePostSender,
  PrincipalScope,
  SourceRegistryReader,
} from "@server/api/http";
import { createReadDb, type ReadDb } from "@drizzle/client";
import { currentHuman } from "@server/identity/human";
import {
  BearerTokenError,
  currentBearer,
  issueBearerToken,
  issueOperatorBearerToken,
  refreshBearerToken,
  revokeBearerToken,
} from "@server/identity/bearer";
import type { GatewayCallerIdentity } from "@server/api/http";
import {
  AGENT_ATTACH_SCOPE,
  principalHasScope,
  principalMeetsTier,
} from "@server/auth/principal";
import { Tier } from "@shared/types";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  requiresHumanLogin,
  webAuthModeFromEnv,
  type WebAuthMode,
} from "@server/auth/webAuthMode";
import { createConversationStore } from "@server/conversation/store";
import { getGatewayStore } from "@server/store/client";
import {
  createCommandIngressSender,
  type CommandIngressSenderOptions,
} from "@server/messagePost/commandIngress";
import {
  createCommandIngressSubmitter,
  type CommandIngressOptions,
} from "@server/command/ingress";
import {
  createHumanReadDeliveryMarker,
  type HumanReadDeliveryMarker,
} from "@server/delivery/humanRead";
import type { Client } from "@libsql/client";
import { parseCookies } from "@server/http/cookies";
import { handleSessionEvents } from "@server/stream/sessionEvents";
import { createDaemonSourceRegistry } from "@server/source/daemonRegistry";
import { gatewayHookDiagnostics } from "@server/hooks/diagnostics";

/** Methods that may carry a JSON body. */
const BODY_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);
const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);
const CSRF_COOKIE = "nexus_csrf";
const CSRF_HEADER = "x-nexus-csrf";

/** Adapt the framework `Request` → the router's plain `ApiRequest`. */
async function toApiRequest(request: Request): Promise<ApiRequest> {
  const url = new URL(request.url);

  const query: Record<string, string | undefined> = {};
  for (const [k, v] of url.searchParams.entries()) query[k] = v;

  const headers: Record<string, string | undefined> = {};
  request.headers.forEach((value, key) => {
    headers[key.toLowerCase()] = value;
  });

  let body: unknown = undefined;
  let rawBody: string | undefined = undefined;
  if (BODY_METHODS.has(request.method.toUpperCase())) {
    const text = await request.text();
    if (text) {
      rawBody = text;
      try {
        body = JSON.parse(text);
      } catch {
        // Leave `body` undefined; per-route Zod validation reports the 400 with a
        // clear field-level message rather than a generic parse error.
        body = undefined;
      }
    }
  }

  return {
    method: request.method,
    path: url.pathname,
    query,
    headers,
    body,
    rawBody,
  };
}

// ── lazy, memoized read-DB getter ───────────────────────────────────────────────
// The read handle is built ONCE, on first use by a read handler — never eagerly
// per request. So `/health`, every write/op, and any request that fails
// auth/validation never construct it, and a read-DB misconfig can only ever
// affect actual read routes.
//
// Always the real daemon-IPC read client (`createReadDb()`); the gateway never opens the
// canonical store. There is no `USE_MOCK` seed branch: tests inject a seeded in-memory `db`
// getter via DI (`handle(req, { db, commands })`), so this runtime path is daemon-only.
let readDbPromise: Promise<ReadDb> | undefined;

function getReadDbLazy(): Promise<ReadDb> {
  if (!readDbPromise) {
    readDbPromise = Promise.resolve(createReadDb());
    // If construction fails, don't cache the rejection — let the next read retry
    // (for example, after the daemon restarts and publishes a new boot manifest) instead of
    // wedging every read.
    readDbPromise.catch(() => {
      readDbPromise = undefined;
    });
  }
  return readDbPromise;
}

const dbGetter: ReadDbGetter = () => getReadDbLazy();

// ── DI types for testability ──────────────────────────────────────────────────

export interface DispatchDeps {
  /** Getter for the write-capable webconsole DB (for currentHuman). */
  db: () => Promise<Client>;
  /** Optional read-view getter for tests; production uses the daemon read DB. */
  readDb?: ReadDbGetter;
  /** Canonical Gateway backend used by migrated REST reads. */
  canonicalDb?: () => Promise<Client> | Client;
  /** Server-configured web auth facet; production resolves from process env. */
  authMode?: WebAuthMode;
  /** Message Post sender; production defaults to command ingress, tests may inject a recorder. */
  messagePost?: MessagePostSender;
  /** Generic command sender; production defaults to command ingress, tests may inject a recorder. */
  commands?: CommandIntentSender;
  /** Typed daemon-owned notification-source identity view. */
  sourceRegistry?: SourceRegistryReader;
  /** Process-local read-only hook diagnostics; resolved per request after service startup. */
  hookDiagnostics?: () => HookDiagnosticsReader | undefined;
  /** Public notification signing secret; production falls back to `NEXUS_HMAC_SECRET`. */
  notifyHmacSecret?: string;
  /** Optional human read-receipt marker; production defaults to the daemon DB. */
  humanReads?: HumanReadDeliveryMarker;
  /** Raw daemon DB client for command-intent writes; tests inject an in-memory DB. */
  commandIngressDb?: () => Promise<Client> | Client;
  /** Command ingress tuning/test seams. */
  commandIngress?: Omit<CommandIngressOptions, "db">;
  /** Epoch-ms clock used for scoped REST bearer expiry and audit fields. */
  now?: () => number;
  /** Test seam for deterministic REST bearer token ids. */
  genId?: () => string;
  /** Test seam for deterministic REST bearer secrets. */
  randomSecret?: (prefix: string) => string;
}

// ── lazy real webconsole DB (mirrors the lazy read-DB pattern) ────────────────
let writeDbPromise: Promise<Client> | undefined;
function getWriteDbLazy(): Promise<Client> {
  if (!writeDbPromise) {
    writeDbPromise = createConversationStore();
    writeDbPromise.catch(() => { writeDbPromise = undefined; });
  }
  return writeDbPromise;
}

const realDispatchDeps: DispatchDeps = {
  db: () => getWriteDbLazy(),
  canonicalDb: () => getGatewayStore(),
  sourceRegistry: createDaemonSourceRegistry(),
  hookDiagnostics: gatewayHookDiagnostics,
};

// ── dispatch factory ──────────────────────────────────────────────────────────

/**
 * Build the per-request dispatch function.  Exported for unit testing via DI.
 * Production code uses `realDispatchDeps`; tests inject in-memory DBs.
 */
export function makeDispatch(deps: DispatchDeps) {
  const commandIngressOptions: CommandIngressSenderOptions = {
    ...deps.commandIngress,
    ...(deps.canonicalDb ? { ingressDb: deps.canonicalDb } : {}),
    ...(deps.commandIngressDb ? { db: deps.commandIngressDb } : {}),
  };
  const commands =
    deps.commands ??
    createCommandIngressSubmitter(commandIngressOptions);
  const messagePost =
    deps.messagePost ??
    createCommandIngressSender(commandIngressOptions);
  const humanReads =
    deps.humanReads ??
    createHumanReadDeliveryMarker(commandIngressOptions.db, {
      now: deps.now,
      nexusHome: deps.commandIngress?.nexusHome,
    });

  return async function dispatch(request: Request): Promise<Response> {
    const apiReq = await toApiRequest(request);
    const mode = deps.authMode ?? webAuthModeFromEnv(process.env);

    // ── cookie → _caller resolution ───────────────────────────────────────────
    // DI: command ingress for writes plus a LAZY read-only handle getter that
    // read handlers resolve only when needed.
    // These are REAL here; tests inject mocks through the same seams.
    const cookies = parseCookies(request.headers.get("cookie"));
    const token = cookies.get("nexus_human");
    const bearerToken = bearerFromAuthorization(request.headers.get("authorization"));

    const identity = await identityFromToken(token, deps);
    if (identity) {
      apiReq.caller = identity;
    } else if (bearerToken) {
      const bearer = await identityFromBearer(bearerToken, deps);
      if (!bearer) {
        return json(
          { error: { code: "unauthorized", message: "invalid bearer token" } },
          401,
        );
      }
      apiReq.caller = bearer;
    } else if (isLocalOperatorWebAuthMode(mode)) {
      apiReq.caller = localOperatorCaller();
    } else if (
      !isBearerRefreshRoute(apiReq) &&
      requiresHumanLogin(apiReq.method, apiReq.path)
    ) {
      return json({ error: { code: "unauthorized", message: "not logged in" } }, 401);
    }

    if (requiresRemoteCookieCsrf(apiReq, mode)) {
      const csrfCookie = cookies.get(CSRF_COOKIE);
      const csrfHeader = request.headers.get(CSRF_HEADER);
      if (!csrfCookie || !csrfHeader || csrfCookie !== csrfHeader) {
        return json(
          { error: { code: "forbidden", message: "missing or invalid CSRF token" } },
          403,
        );
      }
    }

    const tokenRoute = await handleBearerTokenRoute(apiReq, deps);
    if (tokenRoute) return tokenRoute;

    const policy = routePolicyFor(apiReq);
    if (policy.tier && !principalMeetsTier(apiReq.caller, policy.tier)) {
      return json(
        { error: { code: "forbidden", message: `requires ${policy.tier} tier` } },
        403,
      );
    }
    if (policy.scope && !principalHasScope(apiReq.caller, policy.scope)) {
      return json(
        { error: { code: "forbidden", message: `missing required scope: ${policy.scope}` } },
        403,
      );
    }

    const sessionEvents = /^\/api\/v1\/agent-sessions\/([^/]+)\/events$/.exec(apiReq.path);
    if (apiReq.method === "GET" && sessionEvents) {
      return handleSessionEvents(request, decodeURIComponent(sessionEvents[1]!));
    }

    const res = await handle(apiReq, {
      db: deps.readDb ?? dbGetter,
      canonicalDb: deps.canonicalDb,
      messagePost,
      commands,
      sourceRegistry: deps.sourceRegistry,
      hooks: deps.hookDiagnostics?.(),
      humanReads,
      notifyHmacSecret: deps.notifyHmacSecret ?? process.env.NEXUS_HMAC_SECRET,
      now: deps.now ?? Date.now,
    });
    return new Response(JSON.stringify(res.body), {
      status: res.status,
      headers: { "content-type": "application/json" },
    });
  };
}

async function identityFromToken(
  token: string | undefined,
  deps: DispatchDeps,
): Promise<GatewayCallerIdentity | null> {
  if (!token) return null;
  try {
    const writeDb = await deps.db();
    return await currentHuman(token, { db: writeDb });
  } catch {
    return null;
  }
}

export async function identityFromBearer(
  token: string,
  deps: DispatchDeps,
): Promise<GatewayCallerIdentity | null> {
  try {
    const writeDb = await deps.db();
    return await currentBearer(token, {
      db: writeDb,
      now: deps.now ?? Date.now,
    });
  } catch {
    return null;
  }
}

async function handleBearerTokenRoute(
  apiReq: ApiRequest,
  deps: DispatchDeps,
): Promise<Response | null> {
  const path = apiReq.path.replace(/\/+$/, "") || "/";
  const method = apiReq.method.toUpperCase();
  if (path === "/api/v1/auth/operator-token" && method === "POST") {
    if (apiReq.caller?.credentialFacet !== "local") {
      return json(
        {
          error: {
            code: "forbidden",
            message: "operator token bootstrap is available only in local mode",
          },
        },
        403,
      );
    }
    const body = recordBody(apiReq);
    const name = stringField(body?.name);
    if (!name) {
      return json(
        { error: { code: "bad_request", message: "name is required" } },
        400,
      );
    }
    try {
      const issued = await issueOperatorBearerToken(
        {
          name,
          project: stringField(body?.project) ?? apiReq.caller.project,
          scopes: stringArray(body?.scopes),
          ttlMs: numberField(body?.ttlMs),
          refreshTtlMs: numberField(body?.refreshTtlMs),
        },
        await bearerDeps(deps),
      );
      return json(issued, 201);
    } catch (err) {
      return bearerError(err);
    }
  }

  if (path === "/api/v1/auth/tokens" && method === "POST") {
    if (!apiReq.caller) {
      return json({ error: { code: "unauthorized", message: "not logged in" } }, 401);
    }
    if (!principalMeetsTier(apiReq.caller, Tier.Admin)) {
      return json({ error: { code: "forbidden", message: "requires admin tier" } }, 403);
    }
    const body = recordBody(apiReq);
    const scopes = stringArray(body?.scopes);
    try {
      const issued = await issueBearerToken(
        {
          actor: apiReq.caller,
          scopes,
          ttlMs: numberField(body?.ttlMs),
          refreshTtlMs: numberField(body?.refreshTtlMs),
        },
        await bearerDeps(deps),
      );
      return json(issued, 201);
    } catch (err) {
      return bearerError(err);
    }
  }

  if (path === "/api/v1/auth/tokens/refresh" && method === "POST") {
    const body = recordBody(apiReq);
    const refreshToken =
      typeof body?.refreshToken === "string" ? body.refreshToken : "";
    if (!refreshToken) {
      return json(
        { error: { code: "bad_request", message: "refreshToken is required" } },
        400,
      );
    }
    try {
      return json(await refreshBearerToken(refreshToken, await bearerDeps(deps)), 201);
    } catch (err) {
      return bearerError(err);
    }
  }

  const revokeMatch = /^\/api\/v1\/auth\/tokens\/([^/]+)$/.exec(path);
  if (revokeMatch && method === "DELETE") {
    if (!apiReq.caller) {
      return json({ error: { code: "unauthorized", message: "not logged in" } }, 401);
    }
    if (!principalMeetsTier(apiReq.caller, Tier.Admin)) {
      return json({ error: { code: "forbidden", message: "requires admin tier" } }, 403);
    }
    const tokenId = decodeURIComponent(revokeMatch[1]!);
    const revoked = await revokeBearerToken(tokenId, await bearerDeps(deps));
    if (!revoked) {
      return json({ error: { code: "not_found", message: "token not found" } }, 404);
    }
    return json({ ok: true, tokenId });
  }

  return null;
}

function requiresRemoteCookieCsrf(apiReq: ApiRequest, mode: WebAuthMode): boolean {
  return (
    !isLocalOperatorWebAuthMode(mode) &&
    apiReq.caller?.credentialFacet === "human" &&
    MUTATING_METHODS.has(apiReq.method.toUpperCase()) &&
    requiresHumanLogin(apiReq.method, apiReq.path)
  );
}

function isBearerRefreshRoute(apiReq: ApiRequest): boolean {
  return (
    apiReq.method.toUpperCase() === "POST" &&
    (apiReq.path.replace(/\/+$/, "") || "/") === "/api/v1/auth/tokens/refresh"
  );
}

async function bearerDeps(deps: DispatchDeps) {
  return {
    db: await deps.db(),
    now: deps.now ?? Date.now,
    genId: deps.genId,
    randomSecret: deps.randomSecret,
  };
}

export function bearerFromAuthorization(header: string | null): string | undefined {
  if (!header) return undefined;
  const match = /^Bearer\s+(.+)$/i.exec(header.trim());
  return match?.[1]?.trim() || undefined;
}

interface RoutePolicy {
  scope?: PrincipalScope;
  tier?: Tier;
}

function routePolicyFor(apiReq: ApiRequest): RoutePolicy {
  const method = apiReq.method.toUpperCase();
  const path = apiReq.path.replace(/\/+$/, "") || "/";

  if (method === "GET" && /^\/api\/v1\/agents\/[^/]+\/terminal$/.test(path)) {
    return { scope: AGENT_ATTACH_SCOPE, tier: Tier.Agent };
  }
  if (
    /^(GET|PATCH)$/.test(method) &&
    /^\/api\/v1\/(messages|sessions|threads|agents)\/[^/]+\/metadata$/.test(path)
  ) {
    return {};
  }
  if (method === "GET" && (
    /^\/api\/v1\/agents\/[^/]+$/.test(path) ||
    /^\/api\/v1\/agents\/[^/]+\/runtimes$/.test(path) ||
    path === "/api/v1/runtimes"
  )) {
    return { scope: "agent:read", tier: Tier.Agent };
  }
  if (
    (method === "POST" && /^\/api\/v1\/agents\/[^/]+\/credentials$/.test(path)) ||
    (method === "DELETE" && /^\/api\/v1\/agents\/[^/]+\/credentials\/[^/]+$/.test(path))
  ) {
    return { scope: "agent:admin", tier: Tier.Admin };
  }
  if (method === "POST" && path === "/api/v1/register") {
    return { scope: "runtime:register", tier: Tier.Agent };
  }
  if (method === "POST" && path === "/api/v1/rename") {
    return { scope: "message:send", tier: Tier.Agent };
  }
  if (
    (method === "POST" && /^\/api\/v1\/threads\/[^/]+\/archive$/.test(path)) ||
    (method === "PATCH" && /^\/api\/v1\/threads\/[^/]+$/.test(path)) ||
    (method === "DELETE" && /^\/api\/v1\/threads\/[^/]+$/.test(path))
  ) {
    return { scope: "thread:write", tier: Tier.Admin };
  }
  if (method === "POST" && path === "/api/v1/messages") {
    return { scope: "message:send", tier: Tier.Agent };
  }
  if (method === "POST" && path === "/api/v1/notifications") {
    return { scope: "message:send", tier: Tier.Agent };
  }
  if (method === "POST" && path === "/api/v1/notify") {
    return {};
  }
  if (method === "GET" && path === "/api/v1/search") {
    return { scope: "search:read", tier: Tier.Agent };
  }
  if (method === "GET") {
    return { scope: "message:read", tier: Tier.Agent };
  }
  if (path.startsWith("/api/v1/admin/") || path.startsWith("/api/v1/agents")) {
    return { scope: "admin:*", tier: Tier.Admin };
  }
  if (path.startsWith("/api/v1/sources") && !path.endsWith("/push")) {
    return { scope: "source:manage", tier: Tier.Admin };
  }
  return {};
}

function recordBody(apiReq: ApiRequest): Record<string, unknown> | undefined {
  return apiReq.body && typeof apiReq.body === "object"
    ? (apiReq.body as Record<string, unknown>)
    : undefined;
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((v): v is string => typeof v === "string") : [];
}

function stringField(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function numberField(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

async function bearerError(err: unknown): Promise<Response> {
  if (err instanceof BearerTokenError) {
    const code =
      err.status === 400
        ? "bad_request"
        : err.status === 403
          ? "forbidden"
          : "unauthorized";
    return json(
      { error: { code, message: err.message } },
      err.status,
    );
  }
  const message = err instanceof Error ? err.message : "bearer token error";
  return json({ error: { code: "bad_request", message } }, 400);
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

// ── production dispatch ───────────────────────────────────────────────────────
//
// Exported so the standalone headless gateway can serve the same REST spine
// without booting the webconsole route tree.
export const dispatchApiV1 = makeDispatch(realDispatchDeps);

export const Route = createFileRoute("/api/v1/$")({
  server: {
    handlers: {
      GET: ({ request }) => dispatchApiV1(request),
      POST: ({ request }) => dispatchApiV1(request),
      PUT: ({ request }) => dispatchApiV1(request),
      PATCH: ({ request }) => dispatchApiV1(request),
      DELETE: ({ request }) => dispatchApiV1(request),
    },
  },
});
