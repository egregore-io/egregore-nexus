// TanStack Start server route: `GET /api/agui/observe?thread=…` — the AG-UI
// WATCH endpoint. Streams a thread's runs as AG-UI SSE WITHOUT initiating one
// (read-only observe), for UIs that render runs others started (egregore-lens
// observing the bus, our own web console). The translation lives in
// `@server/agui/http` (`handleObserve` → `observe()`); this file is the thin
// framework adapter (the dot in `agui.observe.ts` maps to `/api/agui/observe`).
//
// Scoping:
//   ?session=<name>  — Lane B (agent.update), dedicated to /agent route (T8/T10).
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
import { canonicalAgentSessionTarget } from "@server/read/canonical";
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

function agentCommandTarget(id: string): { name: string; agentId?: string } {
  return { name: id };
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
  identity: GatewayCallerIdentity | null,
  submitCommand: WarmCommand = submitCommandIntent,
): void {
  void submitCommand(
    COMMAND_KINDS.harnessWarm,
    agentCommandTarget(name),
    identity ?? undefined,
  ).catch(() => {});
}

function isLeadRole(role: string | undefined): boolean {
  return role?.toLowerCase() === "lead";
}

function isProtectedAgentObserveTarget(agent: AgentOwnerRow): boolean {
  return (
    agent.tier === Tier.Admin ||
    isLeadRole(agent.role) ||
    agent.sessionKind === Kind.Human ||
    agent.sessionTier === Tier.Admin ||
    isLeadRole(agent.sessionRole)
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
  if (!agent?.ownerName) return true;
  if (
    agent.ownerName === identity.name &&
    (!agent.ownerProject || agent.ownerProject === identity.project)
  ) {
    return true;
  }
  if (hasObserveAdminOverride(agent, identity)) return true;
  return grants.some(
    (grant) =>
      grant.principalName === identity.name &&
      grant.principalProject === identity.project &&
      (grant.role === "viewer" || grant.role === "co_owner"),
  );
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
  if (session) {
    let owner: AgentOwnerRow | undefined;
    let sessionId: string | undefined;
    let legacyReadDb: ReturnType<typeof getReadDb> | undefined;
    if (deps.canonicalDb) {
      const target = await canonicalAgentSessionTarget(await deps.canonicalDb(), session)
        .catch(() => undefined);
      owner = target?.owner;
      sessionId = target?.sessionId;
    } else {
      legacyReadDb = getReadDb();
      const ownerPromise = agentOwnerByName(legacyReadDb, session).catch(() => undefined);
      const sessionPromise = whoamiRow(legacyReadDb, session).catch(() => undefined);
      const [legacyOwner, sessionRow] = await Promise.all([ownerPromise, sessionPromise]);
      owner = legacyOwner;
      sessionId = sessionRow?.sessionId;
    }
    let grants: AgentAccessGrantRow[] = [];
    if (legacyReadDb && owner && !canObserveAgentSession(owner, identity)) {
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
    warmSession(session, identity, deps.submitCommand);

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
      eventsUrl.searchParams.delete("afterId");
      eventsUrl.searchParams.delete("historyTurns");
      return handleSessionEvents(new Request(eventsUrl, {
        method: request.method,
        headers: request.headers,
        signal: request.signal,
      }), sessionId);
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
    // snapshot replay. Motivation: Lens's PACT recorder attaches to EVERY
    // agent; the default 100-turn snapshot recommitted per attach pinned its
    // sidecar for minutes (pactree seal_staged is O(n) per row, 2026-07-10).
    // Ignored when afterId is set — a reconnect already skips the snapshot.
    const historyTurnsParam = url.searchParams.get("historyTurns");
    const historyTurnsRaw = historyTurnsParam === null ? Number.NaN : Number(historyTurnsParam);
    const requestedHistoryTurns =
      Number.isFinite(historyTurnsRaw) && historyTurnsRaw >= 0
        ? Math.floor(historyTurnsRaw)
        : undefined;
    const streamStoreReadyPromise = streamStoreFileExists();
    const snapshot = await loadAgentSessionSnapshot(sessionId, session, {
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
      sessionName: session,
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
