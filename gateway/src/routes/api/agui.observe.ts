// TanStack Start server route: `GET /api/agui/observe?thread=…` — the AG-UI
// WATCH endpoint. Streams a thread's runs as AG-UI SSE WITHOUT initiating one
// (read-only observe), for UIs that render runs others started (egregore-lens
// observing the bus, our own web console). The translation lives in
// `@server/agui/http` (`handleObserve` → `observe()`); this file is the thin
// framework adapter (the dot in `agui.observe.ts` maps to `/api/agui/observe`).
//
// Scoping:
//   ?session=<name>&agentId=<id> — Lane B (agent.update), dedicated to /agent route (T8/T10).
//                      Hydrates materialized history, then tails raw stream rows.
//   ?dm=<name>       — Lane A (message.created), used by /dm route (T9b).
//                      Falls through to handleObserve like ?thread= and ?topic=.
//   ?thread=<name>   — Lane A (message.created), used by /c (channel) route.
//   ?topic=<name>    — Lane A (message.created), used by publish targets.
import { createFileRoute } from "@tanstack/react-router";
import type { Client } from "@libsql/client";

import { handleObserve, handleAgentSession } from "@server/agui/http";
import { getReadDb } from "@drizzle/client";
import {
  agentAccessGrantsByAgentId,
  agentOwnerByName,
  whoamiRow,
  type AgentAccessGrantRow,
  type AgentOwnerRow,
} from "@server/read/queries";
import {
  createMaterializedTurnRelay,
  loadAgentSessionSnapshot,
  materializedCursorForStreamAfter,
} from "@server/agui/agentSessionProjection";
import {
  createStoreBackedAgentSessionRelay,
  observeRawStream,
  streamStoreFileExists,
} from "@server/agui/streamStoreRelay";
import { currentHuman } from "@server/identity/human";
import type { GatewayCallerIdentity } from "@server/api/http";
import { getConversationStore } from "@server/conversation/store";
import { COMMAND_KINDS, submitCommandIntent } from "@server/command/ingress";
import { Kind, Tier } from "@shared/types";
import {
  canonicalAgentAccessGrantsByAgentId,
  canonicalAgentSessionTarget,
  type CanonicalAgentSessionTarget,
} from "@server/read/canonical";
import { getGatewayStore } from "@server/store/client";
import { handleSessionEvents } from "@server/stream/sessionEvents";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
} from "@server/auth/webAuthMode";

type WarmCommand = (
  kind: string,
  request: unknown,
  caller?: GatewayCallerIdentity,
) => Promise<unknown>;

export interface AguiObserveDeps {
  /** Explicit unit-test seam; production submits warm commands over daemon IPC. */
  submitCommand?: WarmCommand;
  /** Gateway-owned canonical identity/runtime projection used in split-authority mode. */
  canonicalDb?: () => Promise<Client> | Client;
}

function getWriteDbLazy(): Promise<Client> {
  return getConversationStore();
}

const SSE_HEADERS: Record<string, string> = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
  "x-accel-buffering": "no",
};

function sseResponse(stream: ReadableStream<Uint8Array>): Response {
  return new Response(stream, { status: 200, headers: SSE_HEADERS });
}

function agentDirectoryErrorResponse(error: unknown): Response {
  const message = error instanceof Error ? error.message : String(error);
  const ambiguous = ["ambiguous agent name", "ambiguous session name"]
    .some((marker) => message.includes(marker));
  return new Response(JSON.stringify({
    error: {
      code: ambiguous ? "ambiguous_agent" : "agent_directory_unavailable",
      message,
    },
  }), {
    status: ambiguous ? 409 : 503,
    headers: { "content-type": "application/json" },
  });
}

/** Read the `nexus_human` cookie value off a request, or undefined. */
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

/**
 * Resolve the watching operator's own name from the `nexus_human` cookie. This
 * is the `selfName` the Message Post lane uses to render the watcher's own
 * committed posts as `role:"user"` (so they reconcile with the optimistic echo
 * instead of duplicating as an attributed "assistant" bubble). Best-effort:
 * any failure (no cookie, unknown token, DB error) yields undefined.
 */
async function identityFromRequest(request: Request): Promise<GatewayCallerIdentity | null> {
  const mode = webAuthModeFromEnv(process.env);
  const token = humanCookie(request);
  if (!token) return isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null;
  try {
    const identity = await currentHuman(token, { db: await getWriteDbLazy() });
    return identity ?? (isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null);
  } catch {
    return isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null;
  }
}

function agentCommandTarget(name: string, agentId?: string): { name: string; agentId?: string } {
  return { name, ...(agentId ? { agentId } : {}) };
}

/**
 * Pre-warm the agent's ACP/headed session the moment its pane opens (this `observe`
 * subscribe IS the "conversation opened" signal). Fire-and-forget: we DON'T await it,
 * so the SSE stream starts immediately while the harness spawns + opens/resumes its
 * session in the background. By the operator's first send the session is live,
 * so the send no longer pays the cold-start (`npx` + ACP initialize +
 * `session/new`) — this is how AionUi has "no send latency" (it spawns the
 * harness at conversation-open, not on first message). Errors are swallowed:
 * warming is best-effort, while prompts still go through the command worker.
 */
function warmSession(
  name: string,
  agentId: string | undefined,
  identity: GatewayCallerIdentity | null,
  submitCommand: WarmCommand = submitCommandIntent,
): void {
  void submitCommand(
    COMMAND_KINDS.harnessWarm,
    agentCommandTarget(name, agentId),
    identity ?? undefined,
  ).catch(() => {});
}

function isProtectedAgentObserveTarget(agent: AgentOwnerRow): boolean {
  return (
    agent.tier === Tier.Admin ||
    agent.sessionKind === Kind.Human ||
    agent.sessionTier === Tier.Admin
  );
}

function hasObserveAdminOverride(
  agent: AgentOwnerRow,
  identity: GatewayCallerIdentity,
): boolean {
  if (identity.tier !== Tier.Admin) return false;
  if (identity.kind === Kind.Human) return true;
  return !isProtectedAgentObserveTarget(agent);
}

/** Managed `/agent` panes allow the owner, explicit delegates, or #48-gated admin override. */
export function canObserveAgentSession(
  agent: AgentOwnerRow | undefined,
  identity: GatewayCallerIdentity,
  grants: AgentAccessGrantRow[] = [],
): boolean {
  if (!agent || (!agent.ownerAgentId && !agent.ownerName)) return true;
  if (agent.ownerAgentId) {
    if (agent.ownerAgentId === identity.agentId) return true;
  } else if (agent.ownerSessionId) {
    if (agent.ownerSessionId === identity.sessionId) return true;
  } else if (agent.ownerName === identity.name) {
    return true;
  }
  if (hasObserveAdminOverride(agent, identity)) return true;
  return grants.some(
    (grant) => {
      const principalMatches = grant.principalAgentId
        ? grant.principalAgentId === identity.agentId
        : grant.principalSessionId
          ? grant.principalSessionId === identity.sessionId
          : grant.principalName === identity.name;
      return principalMatches && (grant.role === "viewer" || grant.role === "co_owner");
    },
  );
}

/** Attach the immutable lane identity that WebSocket mutation handling binds once per socket. */
export function withAgentSessionBinding(
  response: Response,
  target: CanonicalAgentSessionTarget,
): Response {
  const headers = new Headers(response.headers);
  if (target.sessionId) headers.set("x-nexus-session-id", target.sessionId);
  headers.set("x-nexus-agent-id", target.owner.agentId);
  headers.set("x-nexus-agent-name", target.owner.name);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export async function observeScoped(
  request: Request,
  deps: AguiObserveDeps = {},
): Promise<Response> {
  const url = new URL(request.url);
  const identity = await identityFromRequest(request);
  if (!identity) {
    return new Response(JSON.stringify({ error: "not logged in" }), {
      status: 401,
      headers: { "content-type": "application/json" },
    });
  }

  // --- ?session=<name>: dedicated agent-session endpoint (Lane B only, added T8). ---
  // Read BEFORE ?dm= so the new dedicated path takes priority over the shared one.
  const session = url.searchParams.get("session");
  const requestedAgentId = url.searchParams.get("agentId") ?? undefined;
  if (session || requestedAgentId) {
    let owner: AgentOwnerRow | undefined;
    let sessionId: string | undefined;
    let canonicalTarget: CanonicalAgentSessionTarget | undefined;
    let canonicalName = session ?? requestedAgentId!;
    let canonicalAgentId = requestedAgentId;
    let canonicalReadDb: Client | undefined;
    let legacyReadDb: ReturnType<typeof getReadDb> | undefined;
    if (deps.canonicalDb) {
      let target: CanonicalAgentSessionTarget | undefined;
      try {
        canonicalReadDb = await deps.canonicalDb();
        target = await canonicalAgentSessionTarget(canonicalReadDb, {
          ...(requestedAgentId ? { agentId: requestedAgentId } : {}),
          ...(session ? { name: session } : {}),
        });
      } catch (error) {
        return agentDirectoryErrorResponse(error);
      }
      owner = target?.owner;
      sessionId = target?.sessionId;
      canonicalTarget = target;
      canonicalName = target?.owner.name ?? canonicalName;
      canonicalAgentId = target?.owner.agentId ?? canonicalAgentId;
      if (!target) {
        return new Response(JSON.stringify({ error: "agent session stream is not materialized" }), {
          status: 404,
          headers: { "content-type": "application/json" },
        });
      }
    } else {
      if (!session) {
        return new Response(JSON.stringify({ error: "agent session stream is not materialized" }), {
          status: 404,
          headers: { "content-type": "application/json" },
        });
      }
      legacyReadDb = getReadDb();
      try {
        const [legacyOwner, sessionRow] = await Promise.all([
          agentOwnerByName(legacyReadDb, session),
          whoamiRow(legacyReadDb, session),
        ]);
        owner = legacyOwner;
        sessionId = sessionRow?.sessionId;
      } catch (error) {
        return agentDirectoryErrorResponse(error);
      }
      if (!sessionId) {
        return new Response(JSON.stringify({ error: "agent session stream is not materialized" }), {
          status: 404,
          headers: { "content-type": "application/json" },
        });
      }
    }
    let grants: AgentAccessGrantRow[] = [];
    if (canonicalReadDb && owner && !canObserveAgentSession(owner, identity)) {
      try {
        grants = await canonicalAgentAccessGrantsByAgentId(canonicalReadDb, owner.agentId);
      } catch {
        // Missing or malformed canonical ACL state fails closed to owner-only access.
        grants = [];
      }
    } else if (legacyReadDb && owner && !canObserveAgentSession(owner, identity)) {
      try {
        grants = await agentAccessGrantsByAgentId(legacyReadDb, owner.agentId);
      } catch {
        // A stale read projection should degrade to owner-only access, not legacy-open access.
        grants = [];
      }
    }
    if (!canObserveAgentSession(owner, identity, grants)) {
      return new Response(JSON.stringify({ error: "agent session owner required" }), {
        status: 403,
        headers: { "content-type": "application/json" },
      });
    }

    // Eager spawn: bring the agent's ACP session live as the pane opens. Fire-and-forget.
    warmSession(canonicalName, canonicalAgentId, identity, deps.submitCommand);

    // Resolve the agent's session id so we can project its durable Turso stream.
    if (!sessionId) {
      return new Response(
        JSON.stringify({ error: "agent session stream is not materialized" }),
        {
          status: 404,
          headers: { "content-type": "application/json" },
        },
      );
    }
    if (deps.canonicalDb) {
      const eventsUrl = new URL(request.url);
      eventsUrl.pathname = `/api/v1/agent-sessions/${encodeURIComponent(sessionId)}/events`;
      eventsUrl.searchParams.set(
        "view",
        url.searchParams.get("lane") === "raw" ? "terminal" : "agui",
      );
      eventsUrl.searchParams.delete("session");
      eventsUrl.searchParams.delete("lane");
      eventsUrl.searchParams.delete("historyTurns");
      const response = handleSessionEvents(new Request(eventsUrl, {
        method: request.method,
        headers: request.headers,
        signal: request.signal,
      }), sessionId);
      return canonicalTarget ? withAgentSessionBinding(response, canonicalTarget) : response;
    }
    if (url.searchParams.get("lane") === "raw") {
      const rawAfterId = Number(url.searchParams.get("afterId") ?? "");
      const afterId = Number.isFinite(rawAfterId) && rawAfterId > 0 ? Math.floor(rawAfterId) : 0;
      return sseResponse(observeRawStream(sessionId, { afterId }));
    }

    // `afterId` is a reconnect hint: any positive value means the client already
    // rendered history, so skip snapshot replay and resume the live stream after
    // that row. Without this, a plain SSE drop on the same stream-store file can
    // replay the current turn from the last finalized snapshot cursor.
    const afterId = Number(url.searchParams.get("afterId") ?? "");
    const requestedAfterId = Number.isFinite(afterId) && afterId > 0 ? Math.floor(afterId) : 0;
    // `historyTurns` lets a cursor-less client bound (or skip, with 0) the
    // snapshot replay. Tail-only consumers can avoid recommitting historical rows.
    // Ignored when afterId is set — a reconnect already skips the snapshot.
    const historyTurnsParam = url.searchParams.get("historyTurns");
    const historyTurnsRaw = historyTurnsParam === null ? Number.NaN : Number(historyTurnsParam);
    const requestedHistoryTurns =
      Number.isFinite(historyTurnsRaw) && historyTurnsRaw >= 0
        ? Math.floor(historyTurnsRaw)
        : undefined;
    const streamStoreReadyPromise = streamStoreFileExists();
    const snapshot = await loadAgentSessionSnapshot(sessionId, canonicalName, {
      historyTurns: requestedAfterId > 0 ? 0 : requestedHistoryTurns,
    });
    const fallbackAfterCursor = requestedAfterId > 0
      ? await materializedCursorForStreamAfter(sessionId, requestedAfterId)
      : snapshot.tailCursor;
    const streamStoreReady = await streamStoreReadyPromise;
    const createRelay = streamStoreReady
      ? createStoreBackedAgentSessionRelay(sessionId, {
          streamAfterId: requestedAfterId || snapshot.tailAfterId,
          fallbackAfterCursor,
        })
      : createMaterializedTurnRelay(sessionId, {
          afterCursor: fallbackAfterCursor,
        });

    return handleAgentSession(request, {
      sessionName: canonicalName,
      initialEvents: snapshot.events,
      createRelay,
    });
  }

  // ?dm=<name>: Message Post observe — rendered as committed message.created events
  // (Lane A), same as ?thread= and ?topic=. Falls through to handleObserve so
  // run.ts observe() routes it through observeMessagePost. No Turso-projection
  // special-case: the DM pane now subscribes to the bus, not a direct ACP session.
  // Pass the watcher's own name so their committed posts render as `role:"user"`
  // (reconciled with the optimistic echo) rather than a duplicate "agent" bubble.
  return handleObserve(
    request,
    { selfName: identity.name, project: identity.project },
  ); // dm/thread/topic
}

export const Route = createFileRoute("/api/agui/observe")({
  server: {
    handlers: {
      GET: ({ request }) => observeScoped(request, { canonicalDb: getGatewayStore }),
    },
  },
});
