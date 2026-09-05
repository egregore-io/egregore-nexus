// Live read-view data for the pane surfaces (Pub feed, Admin agents, the
// context panels, and the conversation headers). Replaces the deleted seeded
// `seed.ts` tables: every row here comes from the REAL public read-view over
// TanStack Query — the same typed gateway calls the shell nav uses
// (`routes/api/v1/$.ts` → `@server/api/router` → `@server/read/queries`). No
// server-only module is pulled into the client bundle; only `fetch` + mappers
// live here. When a source is empty the hooks return `[]` — callers render an
// honest empty state, never a placeholder.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import type {
  Source,
  SourceListResponse,
  SourceRegisterResponse,
  SourceTokenResponse,
} from "@shared/types/contracts.gen";

import type { Face, PresenceValue } from "@shared/ui";
import { gatewayFetch } from "@app/gatewayClient";
import { qk } from "@shared/queryKeys";
import type {
  MemberRow,
  NotificationRow,
  RouteRuleRow,
  ThreadRow,
} from "@shared/readView";

import type { ConversationView } from "./conversationView";

// ── display row types (what the presentational views render) ──────────────────

export interface FeedRow {
  src: string;
  title: string;
  meta: string;
  routedTo: string[];
}

export interface AdminRow {
  name: string;
  presence: PresenceValue;
  harness: string;
  tier: string;
  status: string;
}

export interface MemberItem {
  glyph: string;
  presence: PresenceValue;
  name: string;
  state: string;
}

export type Fact = { dt: string; dd: string };

// ── helpers ───────────────────────────────────────────────────────────────────

async function getJson<T>(path: string): Promise<T> {
  const res = await gatewayFetch(path, { headers: { accept: "application/json" } });
  if (!res.ok) throw new Error(`read-view ${path} → HTTP ${res.status}`);
  return (await res.json()) as T;
}

/** Coerce any presence string to a render-safe value (default offline). */
function presenceOf(v: string | undefined): PresenceValue {
  return v === "online" || v === "busy" ? v : "offline";
}

function isAgentMember(m: MemberRow): boolean {
  return m.kind === "agent" || (m.kind === undefined && Boolean(m.agent));
}

const glyphOf = (name: string): string =>
  (name.trim()[0] ?? "?").toUpperCase();

/** A compact relative-time label from an epoch-ms stamp. */
function timeAgo(when: number | undefined): string {
  if (!when) return "";
  const secs = Math.max(0, Math.round((Date.now() - when) / 1000));
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.round(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.round(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.round(hrs / 24)}d ago`;
}

const presenceState = (m: MemberRow): string =>
  m.currentWork ? `${presenceOf(m.presence)} · ${m.currentWork}` : presenceOf(m.presence);

// ── raw read-view queries ─────────────────────────────────────────────────────
//
// These read GLOBALLY (no `?project=`): a "project" is a console-only grouping of threads now
// (see `@app/projects`), not a daemon scope. Sending the active group's id as `?project=` to the
// daemon read-view would match no backend project and return nothing — which is what made the
// Admin table / rosters go empty whenever a project was selected.

// Read views refresh over HTTP, without a shared shell WebSocket.
// Periodic reads pause in hidden tabs; stale data refreshes on return.
const FALLBACK_REFETCH_MS = 30_000;
const visibleFallbackInterval = (): number | false =>
  typeof document !== "undefined" && document.hidden ? false : FALLBACK_REFETCH_MS;

/** The member directory (agents + humans/apps, incl. offline). Polled so the roster stays live. */
export function useMembers() {
  return useQuery({
    queryKey: qk.members("all"),
    queryFn: () =>
      getJson<MemberRow[]>("/api/v1/members?includeOffline=true"),
    refetchInterval: visibleFallbackInterval,
    refetchOnWindowFocus: true,
  });
}

/** Named threads (channels) with their member names. Polled so channels stay live. */
export function useThreads() {
  return useQuery({
    queryKey: qk.threads(),
    queryFn: () => getJson<ThreadRow[]>("/api/v1/threads"),
    refetchInterval: visibleFallbackInterval,
    refetchOnWindowFocus: true,
  });
}

/**
 * The member rows of one thread, derived from the read-view: the thread's member
 * names (`/threads`) cross-referenced against the directory (`/members`) for
 * presence/role/work. Pure read-view, so it degrades to `[]`.
 */
function useThreadMemberRows(thread: string | undefined): MemberRow[] {
  const threads = useThreads();
  const members = useMembers();
  if (!thread) return [];
  const names = new Set(
    (threads.data ?? []).find((t) => t.name === thread)?.members ?? [],
  );
  return (members.data ?? []).filter((m) => names.has(m.name));
}

/** The Pub feed: incoming external notifications (who-got-what audit). */
export function useNotifications() {
  return useQuery({
    queryKey: qk.notifications(),
    queryFn: () => getJson<NotificationRow[]>("/api/v1/notifications"),
    refetchInterval: visibleFallbackInterval,
    refetchOnWindowFocus: true,
  });
}

/** Standing routing rules. */
export function useRoutingRules() {
  return useQuery({
    queryKey: qk.routingRules(),
    queryFn: () => getJson<RouteRuleRow[]>("/api/v1/routing-rules"),
    refetchInterval: visibleFallbackInterval,
    refetchOnWindowFocus: true,
  });
}

// ── mapped, ready-to-render selectors ─────────────────────────────────────────

/** Pub feed rows mapped from live notifications. */
export function usePubFeed(): { rows: FeedRow[]; isLoading: boolean } {
  const q = useNotifications();
  const rows = (q.data ?? []).map<FeedRow>((n) => ({
    src: n.source ?? "—",
    title: n.topic ? `topic: ${n.topic}` : n.notifId,
    meta: [n.routedTo.length ? `→ ${n.routedTo.join(", ")}` : "unrouted", timeAgo(n.when)]
      .filter(Boolean)
      .join(" · "),
    routedTo: n.routedTo,
  }));
  return { rows, isLoading: q.isLoading };
}

/** The Admin agents table: members that are agents. */
export function useAdminAgents(): { rows: AdminRow[]; isLoading: boolean } {
  const q = useMembers();
  const rows = (q.data ?? [])
    .filter(isAgentMember)
    .map<AdminRow>((m) => ({
      name: m.name,
      presence: presenceOf(m.presence),
      harness: m.agent ?? "—",
      tier: m.tier ?? "agent",
      status: presenceState(m),
    }));
  return { rows, isLoading: q.isLoading };
}

/** Roster for the context panel: a thread's members, or the whole directory. */
export function useRoster(thread?: string): { members: MemberItem[]; isLoading: boolean } {
  const all = useMembers();
  const threadRows = useThreadMemberRows(thread);
  const rows = thread ? threadRows : all.data ?? [];
  const members = rows.map<MemberItem>((m) => ({
    glyph: glyphOf(m.name),
    presence: presenceOf(m.presence),
    name: m.name,
    state: presenceState(m),
  }));
  return { members, isLoading: all.isLoading };
}

/** Resolve canonical agent ids first while retaining legacy display-name deep links. */
function memberForAgentReference(
  rows: MemberRow[] | undefined,
  agentReference: string,
): MemberRow | undefined {
  return rows?.find((row) => row.agentId === agentReference)
    ?? rows?.find((row) => row.name === agentReference);
}

/** Agent-detail facts for the DM context panel. `undefined` until resolved. */
export function useAgentFacts(agentReference: string): { facts: Fact[]; isLoading: boolean; found: boolean } {
  const q = useMembers();
  const m = memberForAgentReference(q.data, agentReference);
  const facts: Fact[] = m
    ? [
        { dt: "Name", dd: m.name },
        { dt: "Harness", dd: m.agent ?? "—" },
        { dt: "Status", dd: presenceState(m) },
      ]
    : [];
  return { facts, isLoading: q.isLoading, found: !!m };
}

/** Routing-rule facts for the Pub context panel. */
export function usePubRuleFacts(): { facts: Fact[]; isLoading: boolean } {
  const q = useRoutingRules();
  const facts = (q.data ?? []).map<Fact>((r) => ({
    dt: `${r.source ?? r.topic ?? "any"} →`,
    dd: r.to,
  }));
  return { facts, isLoading: q.isLoading };
}

/** Tier summary for the Admin context panel — counts derived from the roster. */
export function useTierFacts(): { facts: Fact[]; isLoading: boolean } {
  const q = useMembers();
  const all = q.data ?? [];
  const agents = all.filter(isAgentMember);
  const online = all.filter((m) => presenceOf(m.presence) === "online");
  const facts: Fact[] = [
    { dt: "Members", dd: String(all.length) },
    { dt: "Agents", dd: String(agents.length) },
    { dt: "Online", dd: String(online.length) },
  ];
  return { facts, isLoading: q.isLoading };
}

// ── mutations ─────────────────────────────────────────────────────────────────

/**
 * Spawn a new headless agent. POSTs to `/api/v1/agents` with `{kind, name?}`
 * Fleet push refreshes the canonical roster; the visible-tab fallback covers a
 * missed event without a mutation-triggered refetch storm.
 */
export function useSpawnAgent() {
  return useMutation({
    mutationFn: async ({ kind, name }: { kind: string; name?: string }) => {
      const res = await gatewayFetch("/api/v1/agents", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ kind, ...(name ? { name } : {}) }),
      });
      if (!res.ok) throw new Error(`spawn-agent → HTTP ${res.status}`);
      return res.json();
    },
  });
}

/** The three distinct agent lifecycle ops. */
export type AgentOp = "kill" | "evict" | "delete";

/**
 * Run a lifecycle op on an agent via `DELETE /api/v1/agents/:id?<op>=1`, where `:id` may be a
 * stable agent id or current name:
 *   - `kill`   → terminate the harness process (record kept; resumable by DMing again)
 *   - `evict`  → remove the agent from every thread (session + process kept)
 *   - `delete` → purge the agent entirely (daemon store + the web console's Turso conversation)
 * Deterministic delete state is patched directly; other lifecycle state arrives
 * over the daemon fleet event lane.
 */
export function useAgentOp() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ name, op }: { name: string; op: AgentOp }) => {
      const url = `/api/v1/agents/${encodeURIComponent(name)}?${op}=1`;
      const res = await gatewayFetch(url, { method: "DELETE" });
      if (!res.ok) throw new Error(`${op}-agent → HTTP ${res.status}`);
      return res.status === 204 ? null : res.json();
    },
    onSuccess: (_data, { name, op }) => {
      if (op === "delete") {
        queryClient.setQueryData<MemberRow[]>(qk.members("all"), (rows) =>
          rows?.filter((row) => row.name !== name),
        );
      }
    },
  });
}

/**
 * Grant or revoke an agent's durable admin tier. The daemon enforces the
 * human-admin-only gate; the UI exposes the same command as the CLI escape hatch.
 */
export function useGrantTier() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ name, tier }: { name: string; tier: "agent" | "admin" }) => {
      const res = await gatewayFetch(`/api/v1/agents/${encodeURIComponent(name)}/tier`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ tier }),
      });
      if (!res.ok) throw new Error(`grant-tier → HTTP ${res.status}`);
      return res.json();
    },
    onSuccess: (_data, { name, tier }) => {
      queryClient.setQueryData<MemberRow[]>(qk.members("all"), (rows) =>
        rows?.map((row) => row.name === name ? { ...row, tier } : row),
      );
    },
  });
}

/**
 * Add an agent/member to an existing thread. POSTs to `/api/v1/threads/:name/members` with
 * `{member}` and patches the canonical thread row directly.
 */
export function useAddThreadMember() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ thread, member }: { thread: string; member: string }) => {
      const res = await gatewayFetch(
        `/api/v1/threads/${encodeURIComponent(thread)}/members`,
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ member }),
        },
      );
      if (!res.ok) throw new Error(`add-thread-member → HTTP ${res.status}`);
      return res.status === 204 ? null : res.json();
    },
    onSuccess: (_data, { thread, member }) => {
      queryClient.setQueryData<ThreadRow[]>(qk.threads(), (rows) =>
        rows?.map((row) => row.name === thread && !row.members.includes(member)
          ? { ...row, members: [...row.members, member] }
          : row),
      );
    },
  });
}

/** Remove a member from a thread. `DELETE /api/v1/threads/:name/members/:member`. */
export function useRemoveThreadMember() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ thread, member }: { thread: string; member: string }) => {
      const res = await gatewayFetch(
        `/api/v1/threads/${encodeURIComponent(thread)}/members/${encodeURIComponent(member)}`,
        { method: "DELETE" },
      );
      if (!res.ok) throw new Error(`remove-thread-member → HTTP ${res.status}`);
      return res.status === 204 ? null : res.json();
    },
    onSuccess: (_data, { thread, member }) => {
      queryClient.setQueryData<ThreadRow[]>(qk.threads(), (rows) =>
        rows?.map((row) => row.name === thread
          ? { ...row, members: row.members.filter((name) => name !== member) }
          : row),
      );
    },
  });
}

/**
 * Rename a thread while preserving membership and message history. `PATCH /api/v1/threads/:name`
 * accepts the new visible name; the daemon enforces admin-tier permission.
 */
export function useRenameThread() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ thread, name }: { thread: string; name: string }) => {
      const res = await gatewayFetch(
        `/api/v1/threads/${encodeURIComponent(thread)}`,
        {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ name }),
        },
      );
      if (!res.ok) {
        let detail = `rename-thread HTTP ${res.status}`;
        try {
          const body = (await res.json()) as { error?: string; message?: string };
          detail = body.error ?? body.message ?? detail;
        } catch {
          // Non-JSON errors still surface the status above.
        }
        throw new Error(detail);
      }
      return res.json() as Promise<{ name: string; previous: string }>;
    },
    onMutate: async ({ thread, name }) => {
      await queryClient.cancelQueries({ queryKey: qk.threads() });
      const previous = queryClient.getQueryData<ThreadRow[]>(qk.threads());
      queryClient.setQueryData<ThreadRow[]>(qk.threads(), (rows) =>
        rows?.map((row) => (row.name === thread ? { ...row, name } : row)),
      );
      return { previous };
    },
    onError: (_error, _vars, ctx) => {
      if (ctx?.previous) queryClient.setQueryData(qk.threads(), ctx.previous);
    },
  });
}

/** Archive a thread while preserving its durable history. `POST /api/v1/threads/:name/archive`. */
export function useArchiveThread() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ thread }: { thread: string }) => {
      const res = await gatewayFetch(
        `/api/v1/threads/${encodeURIComponent(thread)}/archive`,
        { method: "POST" },
      );
      if (!res.ok) throw new Error(`archive-thread → HTTP ${res.status}`);
      return res.status === 204 ? null : res.json();
    },
    onSuccess: (_data, { thread }) => {
      queryClient.setQueryData<ThreadRow[]>(qk.threads(), (rows) =>
        rows?.filter((row) => row.name !== thread),
      );
    },
  });
}

/** Delete a thread from active routing while retaining durable message rows. `DELETE /api/v1/threads/:name`. */
export function useDeleteThread() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ thread }: { thread: string }) => {
      const res = await gatewayFetch(
        `/api/v1/threads/${encodeURIComponent(thread)}`,
        { method: "DELETE" },
      );
      if (!res.ok) throw new Error(`delete-thread → HTTP ${res.status}`);
      return res.status === 204 ? null : res.json();
    },
    onSuccess: (_data, { thread }) => {
      queryClient.setQueryData<ThreadRow[]>(qk.threads(), (rows) =>
        rows?.filter((row) => row.name !== thread),
      );
    },
  });
}

// ── notification sources ──────────────────────────────────────────────────────

/** The registered notification sources. Polled so changes made via CLI are reflected quickly. */
export function useSources(): { sources: Source[]; isLoading: boolean } {
  const q = useQuery({
    queryKey: qk.sources(),
    queryFn: () =>
      getJson<SourceListResponse>("/api/v1/sources").then((r) => r.sources),
    refetchInterval: visibleFallbackInterval,
    refetchOnWindowFocus: true,
  });
  return { sources: q.data ?? [], isLoading: q.isLoading };
}

/** Register a new notification source. Returns the new `Source` + plaintext token (shown once). */
export function useRegisterSource() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({
      name,
      topic,
    }: {
      name: string;
      topic?: string;
    }): Promise<SourceRegisterResponse> => {
      const res = await gatewayFetch("/api/v1/sources", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name, ...(topic ? { topic } : {}) }),
      });
      if (!res.ok) throw new Error(`register-source → HTTP ${res.status}`);
      return res.json() as Promise<SourceRegisterResponse>;
    },
    onSuccess: ({ source }) => {
      queryClient.setQueryData<Source[]>(qk.sources(), (rows) => [
        ...(rows ?? []).filter((row) => row.name !== source.name),
        source,
      ]);
    },
  });
}

/** Enable a source (POST /api/v1/sources/:name/enable). */
export function useEnableSource() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ name }: { name: string }) => {
      const res = await gatewayFetch(
        `/api/v1/sources/${encodeURIComponent(name)}/enable`,
        { method: "POST" },
      );
      if (!res.ok) throw new Error(`enable-source → HTTP ${res.status}`);
      return res.status === 204 ? null : res.json();
    },
    onSuccess: (_data, { name }) => {
      queryClient.setQueryData<Source[]>(qk.sources(), (rows) =>
        rows?.map((row) => row.name === name ? { ...row, enabled: true } : row),
      );
    },
  });
}

/** Disable a source (POST /api/v1/sources/:name/disable). */
export function useDisableSource() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ name }: { name: string }) => {
      const res = await gatewayFetch(
        `/api/v1/sources/${encodeURIComponent(name)}/disable`,
        { method: "POST" },
      );
      if (!res.ok) throw new Error(`disable-source → HTTP ${res.status}`);
      return res.status === 204 ? null : res.json();
    },
    onSuccess: (_data, { name }) => {
      queryClient.setQueryData<Source[]>(qk.sources(), (rows) =>
        rows?.map((row) => row.name === name ? { ...row, enabled: false } : row),
      );
    },
  });
}

/** Rotate a source's token (POST /api/v1/sources/:name/rotate). Returns the new plaintext token. */
export function useRotateSource() {
  return useMutation({
    mutationFn: async ({
      name,
    }: {
      name: string;
    }): Promise<SourceTokenResponse> => {
      const res = await gatewayFetch(
        `/api/v1/sources/${encodeURIComponent(name)}/rotate`,
        { method: "POST" },
      );
      if (!res.ok) throw new Error(`rotate-source → HTTP ${res.status}`);
      return res.json() as Promise<SourceTokenResponse>;
    },
  });
}

/** Remove a source (DELETE /api/v1/sources/:name). */
export function useRemoveSource() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ name }: { name: string }) => {
      const res = await gatewayFetch(
        `/api/v1/sources/${encodeURIComponent(name)}`,
        { method: "DELETE" },
      );
      if (!res.ok) throw new Error(`remove-source → HTTP ${res.status}`);
      return res.status === 204 ? null : res.json();
    },
    onSuccess: (_data, { name }) => {
      queryClient.setQueryData<Source[]>(qk.sources(), (rows) =>
        rows?.filter((row) => row.name !== name),
      );
    },
  });
}

// ── conversation header builders (live pane chrome) ───────────────────────────

/** Build a channel/team pane header from the live thread roster. */
export function useChannelView(name: string): ConversationView {
  const rows = useThreadMemberRows(name);
  const faces: Face[] | undefined = rows.length
    ? rows.slice(0, 6).map<Face>((m) => ({
        glyph: glyphOf(m.name),
        presence: presenceOf(m.presence),
      }))
    : undefined;
  const topic = rows.length ? `${rows.length} member${rows.length === 1 ? "" : "s"}` : undefined;
  return {
    key: name,
    title: name,
    target: { verb: "post", thread: name },
    glyph: "#",
    topic,
    faces,
    composerPlaceholder: `Message #${name}`,
    composerLabel: `Message #${name}`,
  };
}

/** Build a DM pane header from an exact agent id or legacy display name. */
export function useDmView(agentReference: string): ConversationView {
  const { data } = useMembers();
  const m = memberForAgentReference(data, agentReference);
  const displayName = m?.name ?? agentReference;
  return {
    key: m?.agentId ?? agentReference,
    title: displayName,
    target: {
      verb: "dm",
      name: displayName,
      ...(m?.agentId ? { agentId: m.agentId } : {}),
    },
    presence: presenceOf(m?.presence),
    composerPlaceholder: `Message ${displayName}`,
    composerLabel: `Message ${displayName}`,
  };
}

/**
 * Build an agent-session pane header from the live member row (presence). Used
 * by the `/agent/$handle` route — mirrors `useDmView` but does NOT set a `dm`
 * target (the route drives the session via `useAgentSession` directly, not via
 * `LiveConversationPane`'s target-based wiring).
 */
export function useAgentSessionView(name: string): {
  title: string;
  presence: ReturnType<typeof presenceOf>;
  composerPlaceholder: string;
  composerLabel: string;
} {
  const { data } = useMembers();
  const m = data?.find((x) => x.name === name);
  return {
    title: name,
    presence: presenceOf(m?.presence),
    composerPlaceholder: `Message ${name}`,
    composerLabel: `Message ${name}`,
  };
}
