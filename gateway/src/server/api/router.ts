// The public HTTP API router — framework-agnostic + dependency-injected.
//
// `handle(req, deps)` is the router-backed public API in one pure function: it matches
// `req.method` + `req.path` against a DECLARATIVE route table, requires a resolved
// gateway Principal for protected routes, runs the matched handler (which validates + dispatches to
// command ingress or a `@server/read` query), and maps any failure to an HTTP
// status with a leak-free body. `req` is the plain `ApiRequest` shape (NOT the
// framework `Request`) and `deps = { db, commands }` where `db` is a LAZY getter
// (built only when a read handler runs), so tests inject a seeded read-view
// getter and command spies and drive this directly.
//
// The router holds NO business logic and never writes the DB — those invariants
// live in the handlers (each a thin translator). This file only routes, guards,
// and maps errors.
import type { ApiRequest, ApiResponse, ApiDeps, Handler, HandlerCtx } from "./http";
import { fail, GatewayError, ok, ValidationError } from "./http";
import * as h from "./handlers";
import { COMMAND_KINDS } from "@server/command/ingress";
import { HOOK_EVENTS } from "@server/hooks/events";

export type { ApiRequest, ApiResponse, ApiDeps, ReadDbGetter } from "./http";

// ── route table ───────────────────────────────────────────────────────────────
interface Route {
  method: string;
  /** Path pattern under `/api/v1`, with `:param` segments (e.g. `/threads/:name/history`). */
  pattern: string;
  handler: Handler;
  /** Edge auth required? Defaults to true; `false` for health and signed HMAC ingress. */
  auth?: boolean;
  /** Registry projection metadata for capabilities/openapi. */
  projection: RouteProjection;
}

const PREFIX = "/api/v1";

interface RouteProjection {
  read?: string;
  command?: string;
  commands?: string[];
  credential?: string;
  status?: number;
}

export interface RestCapabilityRoute extends RouteProjection {
  method: string;
  path: string;
  auth: boolean;
}

type ProjectedRoute = Omit<Route, "handler">;

// Declarative: one row per endpoint. READS -> read-view (GET); WRITES/OPS ->
// command ingress (POST/DELETE).
// Breadth is cheap here: adding a daemon capability is one row + one thin
// handler. Order matters only for overlap; patterns are
// disjoint by design (static segments distinguish e.g. `/agents/:id` from
// `/agents/:id/metadata`, longest-static wins via specificity sort below).
const ROUTES: Route[] = [
  // health (unauthenticated probe)
  // Health exercises the canonical Gateway store. Legacy fixtures may still inject the old read
  // view, but production must not make daemon presentation reads part of Gateway readiness.
  { method: "GET", pattern: "/health", auth: false, projection: { read: "health" }, handler: async ({ deps }) => {
      try {
        if (deps.canonicalDb) {
          await (await deps.canonicalDb()).execute("SELECT 1");
        } else {
          const db = await deps.db();
          await (db as { $client: { execute(sql: string): Promise<unknown> } }).$client.execute("SELECT 1");
        }
        return { status: 200, body: { status: "ok", store: deps.canonicalDb ? "gateway" : "legacy" } };
      } catch (e) {
        return { status: 503, body: { status: "degraded", store: "unreachable", detail: String((e as Error)?.message ?? e).slice(0, 200) } };
      }
    } },
  { method: "GET", pattern: "/capabilities", projection: { read: "rest.capabilities" }, handler: getCapabilities },
  { method: "GET", pattern: "/openapi", projection: { read: "rest.openapi" }, handler: getOpenApi },

  // ── reads (read-view) ──
  { method: "GET", pattern: "/threads", projection: { read: "thread.list" }, handler: h.getThreads },
  { method: "GET", pattern: "/threads/:name/header", projection: { read: "thread.header" }, handler: h.getThreadHeader },
  { method: "GET", pattern: "/threads/:name/history", projection: { read: "thread.history" }, handler: h.getThreadHistory },
  { method: "GET", pattern: "/threads/:name/members", projection: { read: "thread.members" }, handler: h.getThreadMembers },
  { method: "GET", pattern: "/dms/:name/history", projection: { read: "dm.history" }, handler: h.getDmHistory },
  { method: "GET", pattern: "/history", projection: { read: "history" }, handler: h.getHistory },
  { method: "GET", pattern: "/members", projection: { read: "member.list" }, handler: h.getMembers },
  { method: "GET", pattern: "/search", projection: { read: "search.messages" }, handler: h.getSearch },
  { method: "GET", pattern: "/topics", projection: { read: "topic.list" }, handler: h.getTopics },
  { method: "GET", pattern: "/notifications", projection: { read: "notification.list" }, handler: h.getNotifications },
  { method: "GET", pattern: "/routing-rules", projection: { read: "routing_rules.list" }, handler: h.getRoutingRules },
  { method: "GET", pattern: "/projects", projection: { read: "project.list" }, handler: h.getProjects },
  { method: "GET", pattern: "/whoami", projection: { read: "identity.whoami" }, handler: h.getWhoami },
  { method: "GET", pattern: "/messages/:id/metadata", projection: { read: "metadata.get" }, handler: h.getMessageMetadata },
  { method: "GET", pattern: "/messages/:id", projection: { read: "message.get" }, handler: h.getMessage },
  { method: "GET", pattern: "/sessions/:id/metadata", projection: { read: "metadata.get" }, handler: h.getSessionMetadata },
  { method: "GET", pattern: "/threads/:name/metadata", projection: { read: "metadata.get" }, handler: h.getThreadMetadata },
  { method: "GET", pattern: "/agents/:id/metadata", projection: { read: "metadata.get" }, handler: h.getAgentMetadata },
  { method: "GET", pattern: "/agents/:id", projection: { read: "agent.show" }, handler: h.getAgent },
  { method: "GET", pattern: "/agents/:id/runtimes", projection: { read: "agent.runtimes" }, handler: h.getAgentRuntimes },
  { method: "GET", pattern: "/runtimes", projection: { read: "agent.runtimes" }, handler: h.getRuntimes },
  { method: "GET", pattern: "/hooks", projection: { read: "hooks.list" }, handler: h.getHooks },
  { method: "GET", pattern: "/hooks/public-key", projection: { read: "hooks.publicKey" }, handler: h.getHookPublicKey },
  { method: "GET", pattern: "/hooks/audit", projection: { read: "hooks.audit" }, handler: h.getHookAudit },

  // ── writes / ops (command ingress) ──
  { method: "POST", pattern: "/notify", auth: false, projection: { command: COMMAND_KINDS.notificationNotify }, handler: h.postNotify },
  { method: "POST", pattern: "/notifications", projection: { command: COMMAND_KINDS.notificationSend, status: 201 }, handler: h.postNotification },
  { method: "POST", pattern: "/messages", projection: { command: COMMAND_KINDS.messagePostSend }, handler: h.postMessage },
  { method: "PATCH", pattern: "/messages/:id/metadata", projection: { command: COMMAND_KINDS.metadataSet }, handler: h.patchMessageMetadata },
  { method: "PATCH", pattern: "/sessions/:id/metadata", projection: { command: COMMAND_KINDS.metadataSet }, handler: h.patchSessionMetadata },
  { method: "PATCH", pattern: "/threads/:name/metadata", projection: { command: COMMAND_KINDS.metadataSet }, handler: h.patchThreadMetadata },
  { method: "PATCH", pattern: "/agents/:id/metadata", projection: { command: COMMAND_KINDS.metadataSet }, handler: h.patchAgentMetadata },

  { method: "POST", pattern: "/agents", projection: { command: COMMAND_KINDS.adminSpawn, status: 201 }, handler: h.postAgent },
  { method: "POST", pattern: "/agents/:id/credentials", projection: { command: COMMAND_KINDS.agentCredentialCreate, status: 201 }, handler: h.postAgentCredential },
  { method: "DELETE", pattern: "/agents/:id/credentials/:credentialId", projection: { command: COMMAND_KINDS.agentCredentialRevoke }, handler: h.deleteAgentCredential },
  { method: "DELETE", pattern: "/agents/:id", projection: { commands: [COMMAND_KINDS.adminRemove, COMMAND_KINDS.adminEvict, COMMAND_KINDS.adminDelete] }, handler: h.deleteAgent },
  { method: "POST", pattern: "/agents/:id/tier", projection: { command: COMMAND_KINDS.adminGrantTier }, handler: h.postAgentTier },
  { method: "POST", pattern: "/agents/:id/access", projection: { command: COMMAND_KINDS.agentGrantAccess }, handler: h.postAgentAccess },
  { method: "DELETE", pattern: "/agents/:id/access/:principal", projection: { command: COMMAND_KINDS.agentRevokeAccess }, handler: h.deleteAgentAccess },
  { method: "POST", pattern: "/agents/:id/owner", projection: { command: COMMAND_KINDS.agentTransferOwner }, handler: h.postAgentOwner },

  { method: "POST", pattern: "/threads", projection: { command: COMMAND_KINDS.threadCreate, status: 201 }, handler: h.postThread },
  { method: "POST", pattern: "/threads/:name/join", projection: { command: COMMAND_KINDS.threadJoin }, handler: h.postThreadJoin },
  { method: "POST", pattern: "/threads/:name/leave", projection: { command: COMMAND_KINDS.threadLeave }, handler: h.postThreadLeave },
  { method: "POST", pattern: "/threads/:name/archive", projection: { command: COMMAND_KINDS.threadArchive }, handler: h.postThreadArchive },
  { method: "PATCH", pattern: "/threads/:name", projection: { command: COMMAND_KINDS.threadRename }, handler: h.patchThread },
  { method: "DELETE", pattern: "/threads/:name", projection: { command: COMMAND_KINDS.threadDelete }, handler: h.deleteThread },
  { method: "POST", pattern: "/threads/:name/members", projection: { command: COMMAND_KINDS.threadAddMember, status: 201 }, handler: h.postThreadMember },
  { method: "DELETE", pattern: "/threads/:name/members/:member", projection: { command: COMMAND_KINDS.threadRemoveMember }, handler: h.deleteThreadMember },

  { method: "POST", pattern: "/topics/:name/subscribe", projection: { command: COMMAND_KINDS.topicSubscribe }, handler: h.postTopicSubscribe },
  { method: "POST", pattern: "/topics/:name/unsubscribe", projection: { command: COMMAND_KINDS.topicUnsubscribe }, handler: h.postTopicUnsubscribe },

  { method: "POST", pattern: "/status", projection: { command: COMMAND_KINDS.presenceStatus }, handler: h.postStatus },
  { method: "POST", pattern: "/heartbeat", projection: { command: COMMAND_KINDS.presenceHeartbeat }, handler: h.postHeartbeat },

  { method: "POST", pattern: "/inbox/consume", projection: { command: COMMAND_KINDS.inboxConsume }, handler: h.postConsume },
  { method: "POST", pattern: "/inbox/ack", projection: { command: COMMAND_KINDS.inboxAck }, handler: h.postAck },
  { method: "POST", pattern: "/inbox/ack-threads", projection: { command: COMMAND_KINDS.inboxAckThreads }, handler: h.postAckThreads },

  { method: "POST", pattern: "/register", projection: { command: COMMAND_KINDS.identityRegister, status: 201 }, handler: h.postRegister },
  { method: "POST", pattern: "/rename", projection: { command: COMMAND_KINDS.identityRename }, handler: h.postRename },
  { method: "POST", pattern: "/routing-rules", projection: { status: 501 }, handler: h.postRoutingRule },

  { method: "POST", pattern: "/admin/channel", projection: { command: COMMAND_KINDS.adminChannel }, handler: h.postAdminChannel },
  { method: "POST", pattern: "/admin/route", projection: { command: COMMAND_KINDS.adminRoute }, handler: h.postAdminRoute },
  { method: "POST", pattern: "/admin/monitor", projection: { command: COMMAND_KINDS.adminMonitor }, handler: h.postAdminMonitor },
  { method: "POST", pattern: "/admin/transport/secrets", projection: { command: "gateway.transport.secret.set" }, handler: h.postTransportSecret },
  { method: "DELETE", pattern: "/admin/transport/secrets/:key", projection: { command: "gateway.transport.secret.rm" }, handler: h.deleteTransportSecret },

  // ── notification sources ──
  { method: "GET", pattern: "/sources", projection: { read: "source.list" }, handler: h.getSources },
  { method: "POST", pattern: "/sources", projection: { command: COMMAND_KINDS.sourceRegister, status: 201 }, handler: h.postSource },
  { method: "GET", pattern: "/sources/:name", projection: { read: "source.show" }, handler: h.getSource },
  { method: "POST", pattern: "/sources/:name/enable", projection: { command: COMMAND_KINDS.sourceEnable }, handler: h.postSourceEnable },
  { method: "POST", pattern: "/sources/:name/disable", projection: { command: COMMAND_KINDS.sourceDisable }, handler: h.postSourceDisable },
  { method: "POST", pattern: "/sources/:name/rotate", projection: { command: COMMAND_KINDS.sourceRotate }, handler: h.postSourceRotate },
  { method: "DELETE", pattern: "/sources/:name", projection: { command: COMMAND_KINDS.sourceRemove }, handler: h.deleteSource },
  { method: "POST", pattern: "/sources/:name/push", auth: false, projection: { command: COMMAND_KINDS.sourcePush }, handler: h.postSourcePush },
];

// Edge-owned credential routes are handled in the TanStack catch-all before the
// pure router. They still belong to `/api/v1` discovery, so keep their projection
// beside the router table instead of in a stale handlers-local list.
const EDGE_ROUTES: ProjectedRoute[] = [
  { method: "POST", pattern: "/auth/operator-token", projection: { credential: "auth.operatorToken", status: 201 } },
  { method: "POST", pattern: "/auth/tokens", projection: { credential: "auth.token.issue", status: 201 } },
  { method: "POST", pattern: "/auth/tokens/refresh", auth: false, projection: { credential: "auth.token.refresh", status: 201 } },
  { method: "DELETE", pattern: "/auth/tokens/:tokenId", projection: { credential: "auth.token.revoke" } },
];

/** Project the registered REST routes into the public capabilities/openapi contract. */
export function restCapabilityRoutes(): RestCapabilityRoute[] {
  return [...ROUTES, ...EDGE_ROUTES].map((route) => ({
    method: route.method,
    path: restPath(route.pattern),
    auth: route.auth !== false,
    ...route.projection,
  }));
}

function restPath(pattern: string): string {
  return `${PREFIX}${pattern}`.replace(/:([A-Za-z0-9_]+)/g, "{$1}");
}

async function getCapabilities({ deps }: HandlerCtx) {
  return ok({
    version: 1,
    principle: "REST is a projection of the daemon command/read spine",
    protocol: {
      gateway: "0.1.6",
      surfaces: {
        eventsLane: { version: 1, replay: "afterSeq-ring" },
        sessionLane: { version: 1, replay: "cursor" },
        threadDmRead: { version: 1, replay: "durable-cursor" },
        notify: { version: 1, idempotency: true },
        hooks: { version: 1, events: [...HOOK_EVENTS] },
        transports: { version: 1, providers: [...(deps.transportStates?.() ?? [])] },
      },
    },
    routes: restCapabilityRoutes(),
  });
}

async function getOpenApi() {
  return ok({
    openapi: "3.1.0",
    info: { title: "Nexus REST API", version: "1" },
    paths: openApiPaths(restCapabilityRoutes()),
  });
}

function openApiPaths(routes: RestCapabilityRoute[]) {
  const paths: Record<string, Record<string, unknown>> = {};
  for (const route of routes) {
    paths[route.path] ??= {};
    paths[route.path]![route.method.toLowerCase()] = {
      operationId: operationIdFor(route),
      responses: {
        [route.status ?? 200]: { description: route.status === 501 ? "Not implemented" : "OK" },
      },
    };
  }
  return paths;
}

function operationIdFor(route: RestCapabilityRoute): string {
  const source =
    route.command ??
    route.commands?.join(".") ??
    route.read ??
    route.credential ??
    route.path;
  return source
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

// Pre-split patterns once; sort so more-specific (more static segments) routes win
// when two patterns could match the same path.
interface CompiledRoute extends Route {
  segments: string[];
  staticCount: number;
}
const COMPILED: CompiledRoute[] = ROUTES.map((r) => {
  const segments = r.pattern.split("/").filter(Boolean);
  const staticCount = segments.filter((s) => !s.startsWith(":")).length;
  return { ...r, segments, staticCount };
}).sort((a, b) => b.staticCount - a.staticCount);

// ── matching ───────────────────────────────────────────────────────────────────
function stripPrefix(path: string): string | null {
  // Normalize trailing slash and ensure the path is under /api/v1.
  const clean = path.replace(/\/+$/, "") || "/";
  if (clean === PREFIX) return "/";
  if (!clean.startsWith(`${PREFIX}/`)) return null;
  return clean.slice(PREFIX.length);
}

function matchPath(
  segments: string[],
  pathSegments: string[],
): Record<string, string> | null {
  if (segments.length !== pathSegments.length) return null;
  const params: Record<string, string> = {};
  for (let i = 0; i < segments.length; i++) {
    const pat = segments[i]!;
    const seg = pathSegments[i]!;
    if (pat.startsWith(":")) {
      params[pat.slice(1)] = decodeURIComponent(seg);
    } else if (pat !== seg) {
      return null;
    }
  }
  return params;
}

// ── auth (verified gateway Principal at the edge) ───────────────────────────────
function authorized(req: ApiRequest): boolean {
  return Boolean(req.caller);
}

// ── error → status mapping ──────────────────────────────────────────────────────
/**
 * Map a command/gateway error `code` to an HTTP status. Command workers can
 * surface JSON-RPC-style negative codes from daemon dispatch, while gateway
 * validation/transport failures use ordinary HTTP status codes.
 */
function statusForGatewayCode(code: number): number {
  // Gateway surfaced an HTTP status directly.
  if (code >= 400 && code <= 599) return code;
  switch (code) {
    case -32700: // parse error
    case -32600: // invalid request
    case -32602: // invalid params
      return 400;
    case -32601: // method not found
      return 404;
    case -32603: // internal error
      return 500;
    default:
      // App-defined JSON-RPC error space (e.g. unauthorized/forbidden/notfound):
      // map a few conventional ranges, else treat as a server-side failure.
      if (code === -32001) return 401;
      if (code === -32002) return 403;
      if (code === -32004) return 404;
      return 502; // a daemon-reported failure we don't recognize → bad gateway
  }
}

// ── the entry point ──────────────────────────────────────────────────────────────
/**
 * Resolve one request to a response. Pure + injectable: all I/O goes through
 * `deps.commands` (writes) or `deps.db()`
 * (reads, resolved lazily so non-read routes never touch the read DB).
 */
export async function handle(req: ApiRequest, deps: ApiDeps): Promise<ApiResponse> {
  const rel = stripPrefix(req.path);
  if (rel === null) return fail(404, `no route for ${req.path}`);
  const pathSegments = rel.split("/").filter(Boolean);

  const method = req.method.toUpperCase();
  let methodMismatch = false;

  for (const route of COMPILED) {
    const params = matchPath(route.segments, pathSegments);
    if (params === null) continue;
    if (route.method !== method) {
      methodMismatch = true;
      continue;
    }

    // Edge auth (default on; health opts out).
    if (route.auth !== false && !authorized(req)) {
      return fail(401, "missing or invalid Principal");
    }

    try {
      return await route.handler({ req, deps, params });
    } catch (e) {
      return mapError(e);
    }
  }

  // A path matched but not the method → 405; otherwise no route → 404.
  if (methodMismatch) return fail(405, `method ${method} not allowed on ${req.path}`);
  return fail(404, `no route for ${method} ${req.path}`);
}

/** Translate a thrown error into a leak-free HTTP response. */
function mapError(e: unknown): ApiResponse {
  if (e instanceof ValidationError) {
    return fail(400, e.message);
  }
  if (e instanceof GatewayError) {
    const status = statusForGatewayCode(e.code);
    // Pass the command/gateway user-facing message through; never a stack.
    return fail(status, e.message);
  }
  // Unknown/unexpected → 500 with a generic message (no internals, no stack).
  return fail(500, "internal error");
}
