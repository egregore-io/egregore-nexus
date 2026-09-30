// Live shell navigation — the rail/top-bar IA fetched from the REAL read-view.
//
// Replaces the seeded `nav.ts` constants at runtime. Channels = real named
// threads (`GET /api/v1/threads`), DMs/members = real members
// (`GET /api/v1/members`), projects = real projects (`GET /api/v1/projects`), and
// the "me" footer = the resolved caller (`GET /api/v1/whoami`). All via TanStack
// Query for fetching/caching/invalidation, keyed by `@server/read/keys` (`qk`).
//
// This is a typed GATEWAY CALL to the existing read-view: the browser hits the
// already-wired public HTTP API (`routes/api/v1/$.ts` → `@server/api/router` →
// `@server/read/queries` over the real `createReadDb()`), so NO server-only module
// (`@drizzle`/libSQL) is pulled into the client bundle — only `fetch` + mappers
// live here. The read-view query SHAPES are the contract (`@server/read/queries`).
import { useQuery } from "@tanstack/react-query";

import type { PresenceValue } from "@shared/ui";
import { gatewayFetch } from "@app/gatewayClient";
import { useCurrentIdentity } from "@app/identity";
import { qk } from "@shared/queryKeys";
import { isAgentMember } from "@shared/entityKind";
import type {
  MemberRow,
  ProjectRow,
  ThreadRow,
  WhoamiRow,
} from "@shared/readView";

import type {
  AgentKind,
  ChannelNavItem,
  DmNavItem,
  MeIdentity,
  ProjectNavItem,
  TeamNavItem,
} from "./nav";

// ── REST fetch helpers (the public read-view surface) ─────────────────────────

/** GET a read-view list endpoint and parse JSON, or throw a compact error. */
async function getJson<T>(path: string): Promise<T> {
  const res = await gatewayFetch(path, { headers: { accept: "application/json" } });
  if (!res.ok) {
    throw new Error(`read-view ${path} → HTTP ${res.status}`);
  }
  return (await res.json()) as T;
}

// ── normalizers (read view-model → shell nav shape) ───────────────────────────

/** Coerce any presence string to a render-safe `PresenceValue` (default offline). */
function presence(v: string | undefined): PresenceValue {
  return v === "online" || v === "busy" ? v : "offline";
}

function memberKind(m: MemberRow): AgentKind {
  return isAgentMember(m) ? "agent" : "app";
}

function toChannel(t: ThreadRow): ChannelNavItem {
  return { id: t.name, name: t.name };
}

type AddressableMember = MemberRow & { agentId: string };

function isAddressableMember(m: MemberRow): m is AddressableMember {
  return typeof m.agentId === "string" && m.agentId.trim().length > 0;
}

function toDm(m: AddressableMember): DmNavItem {
  return {
    id: m.agentId,
    agentId: m.agentId,
    name: m.name,
    kind: memberKind(m),
    presence: presence(m.presence),
    kindLabel: m.agent,
  };
}

function toProject(p: ProjectRow): ProjectNavItem {
  // The read-view doesn't carry per-project presence; show online (it exists).
  return { projectId: p.projectId, name: p.name, presence: "online" };
}

function toMe(w: WhoamiRow): MeIdentity {
  return {
    name: w.name,
    role: w.tier,
    presence: presence(w.presence),
  };
}

// ── query hooks (each a thin useQuery over the read-view) ─────────────────────

const STALE = 15_000;
// TanStack pauses intervals in hidden tabs and refetches stale data on return.
// Both roster observers share the same cache/in-flight HTTP request.
const ROSTER_REFRESH = {
  refetchInterval: 30_000,
  refetchIntervalInBackground: false,
  refetchOnWindowFocus: true,
};

// Threads/members are read GLOBALLY now: a "project" is a console-only grouping (see
// `@app/projects`), so the rail fetches every thread/member and `AppShell` filters channels to the
// active group's members client-side. (The daemon's own project scope is a single workspace.)

/** Real named threads → channel rows (all of them; the rail filters by project group). */
export function useChannels() {
  return useQuery({
    queryKey: qk.threads(),
    queryFn: () => getJson<ThreadRow[]>("/api/v1/threads"),
    select: (rows) => rows.map(toChannel),
    staleTime: STALE,
  });
}

/**
 * Real members → DM rows (online + busy; offline omitted). Humans (the operator
 * and any other web-console participant, registered with `kind: "human"`) are
 * NOT DM targets — operators talk to agents, not to themselves — so they are
 * filtered out of the DM list.
 */
export function useDms() {
  return useQuery({
    ...ROSTER_REFRESH,
    queryKey: qk.members("all"),
    queryFn: () => getJson<MemberRow[]>("/api/v1/members?includeOffline=true"),
    select: (rows) => rows
      .filter((m): m is AddressableMember =>
        m.kind !== "human"
          && presence(m.presence) !== "offline"
          && isAddressableMember(m))
      .map(toDm),
    staleTime: STALE,
  });
}

/** Real members → the live-agents count. */
export function useAgentsLive() {
  return useQuery({
    ...ROSTER_REFRESH,
    queryKey: qk.members("all"),
    queryFn: () => getJson<MemberRow[]>("/api/v1/members?includeOffline=true"),
    select: (rows) => rows.filter(
      (m) => isAgentMember(m) && presence(m.presence) !== "offline",
    ).length,
    staleTime: STALE,
  });
}

/** Real (daemon) projects -> switcher rows. The live console feeds
 * the switcher from the gateway project groups (see `@app/projects`). */
export function useProjects() {
  return useQuery({
    queryKey: qk.projects(),
    queryFn: () => getJson<ProjectRow[]>("/api/v1/projects"),
    select: (rows) => rows.map(toProject),
    staleTime: STALE,
  });
}

/** The resolved caller → the rail's "me" footer. */
export function useMe() {
  const query = useCurrentIdentity();
  return {
    ...query,
    data: query.data ? toMe(query.data) : undefined,
  };
}

// ── derived (presentational) values ───────────────────────────────────────────

/**
 * Teams aren't a first-class read-view list yet — the daemon models them as named
 * threads. Until a dedicated `/teams` endpoint exists, the rail renders no team
 * rows from live data (empty group) rather than seeded ones.
 */
export function useTeams(): TeamNavItem[] {
  return [];
}
