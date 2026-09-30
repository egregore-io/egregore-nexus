// Read-view query functions — the display/query seam for the web console, the MCP
// resources, and the public HTTP API. Every function here is a SELECT
// against the read-only Drizzle handle (`ReadDb`); the daemon owns ALL writes.
//
// These return DISPLAY VIEW-MODELS — plain, flat row shapes the UI/API render —
// NOT the generated contract wire types (`@shared/types`). The plan is explicit:
// the Drizzle read models are a separate layer and must not stand in for the
// contract types. (They happen to resemble some contract shapes because both
// describe the same data, but they are distinct, owned here.)
//
// Query keys for caching/invalidation come from `@server/read/keys` (`qk`).
import {
  and,
  asc,
  desc,
  eq,
  gt,
  gte,
  inArray,
  isNotNull,
  isNull,
  lt,
  ne,
  or,
  sql,
} from "drizzle-orm";

import type { ReadDb } from "@drizzle/client";
import {
  sessions,
  agents,
  agentAclGrants,
  agentRuntimes,
  messages,
  threads,
  threadMembers,
  topics,
  subscriptions,
  notifications,
  developerEventTopics,
  sources,
  routingRules,
} from "@drizzle/schema.gen";

const DEFAULT_HEARTBEAT_TTL_MS = 30_000;

// ── Display view-models (read layer; NOT contract wire types) ─────────────────

/** A roster entry as the web console renders it. */
export interface MemberRow {
  name: string;
  /** The session this member is bound to — lets the console link to `/agent/<name>:<session_id>`. */
  sessionId: string;
  /** Durable a_* agent identity (sessions.agent_id). Authoritative — never
   * derivable from sessionId (later sessions keep the FIRST session's id). */
  agentId?: string;
  agent?: string;
  /** Registration kind: `agent` | `human` | `app` | `notification`. Humans are not DM targets. */
  kind?: string;
  tier?: string;
  presence: string;
  currentWork?: string;
}

/** A named thread with its members + last-activity stamp. */
export interface ThreadRow {
  name: string;
  topic?: string;
  description?: string;
  members: string[];
  lastAt?: number;
  /** Latest metadata-only developer event sequence for `sys.message.thread.<name>`. */
  latestSeq?: number;
}

export interface ThreadHeaderMemberRow {
  name: string;
  kind?: string;
  agent?: string;
  presence: string;
  sessionId: string;
  currentWork?: string;
}

/** Channel-open header read: metadata, roster, presence, and live-runtime count. */
export interface ThreadHeaderRow {
  name: string;
  topic?: string;
  description?: string;
  lastAt?: number;
  members: ThreadHeaderMemberRow[];
  memberCount: number;
  activeSessions: number;
}

/** One chronological history line (thread or DM). */
export interface HistoryRow {
  messageId: string;
  from: string;
  when: number;
  summary?: string;
  body: string;
  /** Exact forward-resume boundary for fallback readers. */
  cursor?: { createdAt: number; rowid: number };
}

/** One search hit (FTS snippet over messages). */
export interface SearchRow {
  messageId: string;
  from: string;
  when: number;
  snippet: string;
  score: number;
}

/** A topic with its live subscriber count. */
export interface TopicRow {
  topic: string;
  subscribers: number;
}

/** A standing route rule projection. */
export interface RouteRuleRow {
  source?: string;
  topic?: string;
  to: string;
}

/** A notification audit row (who-got-what). */
export interface NotificationRow {
  notifId: string;
  source?: string;
  topic?: string;
  hmacOk: boolean;
  routedTo: string[];
  when: number;
}

/** Registered notification source display row. */
export interface SourceRow {
  name: string;
  topic: string;
  enabled: boolean;
  createdAt: number;
  lastFiredAt?: number;
}

/** Source row plus secret token, used only by the signed-push edge. */
export interface SourceSecretRow extends SourceRow {
  token: string;
}

/** A daemon project scope label derived from registered sessions. */
export interface ProjectRow {
  projectId: string;
  name: string;
  createdBy?: string;
  rootPath?: string;
  createdAt?: number;
}

/** The caller's resolved identity (web console header / public `whoami`). */
export interface WhoamiRow {
  agentId?: string;
  name: string;
  sessionId: string;
  tier: string;
  presence: string;
}

export interface AgentRuntimeRow {
  runtimeId: string;
  agentId: string;
  harness: string;
  cwd?: string;
  transport?: string;
  presence: string;
  active: boolean;
  startedAt: number;
  stoppedAt?: number;
  lastHeartbeat?: number;
}

export interface AgentSummaryRow {
  agentId: string;
  name: string;
  defaultHarness?: string;
  tier?: string;
  disabled: boolean;
  activeRuntime?: AgentRuntimeRow;
}

export interface AgentOwnerRow {
  agentId: string;
  name: string;
  tier?: string;
  ownerName?: string;
  ownerSessionId?: string;
  ownerAgentId?: string;
  sessionKind?: string;
  sessionTier?: string;
}

export interface AgentAccessGrantRow {
  agentId: string;
  principalProject: string;
  principalName: string;
  principalSessionId?: string;
  principalAgentId?: string;
  role: string;
}

export interface AgentShowRow {
  agent: AgentSummaryRow;
  runtimes: AgentRuntimeRow[];
}

interface PresenceOptions {
  /** Epoch milliseconds used by tests; production defaults to Date.now(). */
  now?: number;
  /** Must match nexus-common Config::default().heartbeat_ttl_ms unless explicitly overridden. */
  heartbeatTtlMs?: number;
}

// Internal helper: nullable text/int columns come back as `string | null` /
// `number | null`; normalize to `undefined` for the display shape.
function opt<T>(v: T | null): T | undefined {
  return v === null ? undefined : v;
}

function effectivePresence(
  presence: string | null,
  lastHeartbeat: number | null,
  opts: PresenceOptions = {},
): string {
  const ttl = opts.heartbeatTtlMs ?? DEFAULT_HEARTBEAT_TTL_MS;
  const ts = opts.now ?? Date.now();
  if (lastHeartbeat === null || ts - lastHeartbeat > ttl) return "offline";
  return presence ?? "offline";
}

function runtimePresence(presence: string | null, active: number | null): string {
  return presence ?? (active === 1 ? "online" : "offline");
}

/** Resolve a thread NAME → its internal id (`dm:` names are their own id). */
async function threadIdByName(
  db: ReadDb,
  name: string,
): Promise<string | undefined> {
  const rows = await db
    .select({ threadId: threads.threadId })
    .from(threads)
    .where(and(eq(threads.name, name), isNull(threads.archivedAt)))
    .limit(1);
  return rows[0]?.threadId ?? undefined;
}

// ── listMembers ───────────────────────────────────────────────────────────────
/**
 * GET /members — the roster. `includeOffline` (default false) controls whether
 * offline sessions are returned (matches the daemon's `MemberListRequest`).
 */
export async function listMembers(
  db: ReadDb,
  opts: { includeOffline?: boolean; project?: string } & PresenceOptions = {},
): Promise<MemberRow[]> {
  const now = opts.now ?? Date.now();
  const heartbeatTtlMs = opts.heartbeatTtlMs ?? DEFAULT_HEARTBEAT_TTL_MS;
  const liveOnly = opts.includeOffline === true
    ? undefined
    : and(
        isNotNull(sessions.lastHeartbeat),
        gte(sessions.lastHeartbeat, now - heartbeatTtlMs),
        isNotNull(sessions.presence),
        ne(sessions.presence, "offline"),
      );
  const rows = await db
    .select({
      agentId: sessions.agentId,
      name: sessions.name,
      sessionId: sessions.sessionId,
      agent: sessions.agent,
      kind: sessions.kind,
      tier: sessions.tier,
      presence: sessions.presence,
      lastHeartbeat: sessions.lastHeartbeat,
      currentWork: sessions.currentWork,
    })
    .from(sessions)
    .where(and(opts.project ? eq(sessions.project, opts.project) : undefined, liveOnly))
    .orderBy(asc(sessions.name));

  return rows
    .map((r) => ({
      ...r,
      effectivePresence: effectivePresence(r.presence, r.lastHeartbeat, opts),
    }))
    .map((r) => {
      const kind = r.kind ?? (r.agent ? "agent" : "app");
      return {
        name: r.name ?? "",
        sessionId: r.sessionId ?? "",
        agentId: opt(r.agentId),
        agent: kind === "agent" ? opt(r.agent) : undefined,
        kind,
        tier: opt(r.tier),
        presence: r.effectivePresence,
        currentWork: opt(r.currentWork),
      };
    });
}

// ── durable agents ───────────────────────────────────────────────────────────
/** Resolve one durable agent by stable id or display name. */
export async function agentShow(
  db: ReadDb,
  id: string,
  opts: { project?: string; includeStopped?: boolean } = {},
): Promise<AgentShowRow | undefined> {
  const agent = await agentByIdOrName(db, id, opts.project);
  if (!agent) return undefined;
  const runtimes = await agentRuntimeRows(db, {
    agentId: agent.agentId,
    includeStopped: opts.includeStopped ?? true,
  });
  return {
    agent: { ...agent, activeRuntime: runtimes.find((r) => r.active) },
    runtimes: opts.includeStopped === false ? runtimes.filter((r) => r.active) : runtimes,
  };
}

/** List runtimes for one durable agent, addressed by stable id or name. */
export async function agentRuntimesFor(
  db: ReadDb,
  args: { id: string; project?: string; includeStopped?: boolean },
): Promise<{ agentId: string; runtimes: AgentRuntimeRow[] } | undefined> {
  const agent = await agentByIdOrName(db, args.id, args.project);
  if (!agent) return undefined;
  return {
    agentId: agent.agentId,
    runtimes: await agentRuntimeRows(db, {
      agentId: agent.agentId,
      includeStopped: args.includeStopped ?? false,
    }),
  };
}

/**
 * List runtimes across ALL durable agents — the fleet view (`GET /api/v1/runtimes`
 * with no target query). Rows carry the owning agent's display name so a fleet
 * UI can render them without a per-agent follow-up query.
 */
export async function listRuntimes(
  db: ReadDb,
  opts: { project?: string; includeStopped?: boolean } = {},
): Promise<Array<AgentRuntimeRow & { name?: string }>> {
  const activeOnly = opts.includeStopped !== true ? eq(agentRuntimes.active, 1) : undefined;
  const projectOnly = opts.project ? eq(agents.project, opts.project) : undefined;
  const rows = await db
    .select({
      runtimeId: agentRuntimes.runtimeId,
      agentId: agentRuntimes.agentId,
      name: agents.name,
      harness: agentRuntimes.harness,
      cwd: agentRuntimes.cwd,
      transport: agentRuntimes.transport,
      presence: agentRuntimes.presence,
      active: agentRuntimes.active,
      startedAt: agentRuntimes.startedAt,
      stoppedAt: agentRuntimes.stoppedAt,
      lastHeartbeat: agentRuntimes.lastHeartbeat,
    })
    .from(agentRuntimes)
    .leftJoin(agents, eq(agentRuntimes.agentId, agents.agentId))
    .where(and(activeOnly, projectOnly))
    .orderBy(desc(agentRuntimes.active), asc(agentRuntimes.startedAt));

  return rows.map((r) => ({
    runtimeId: r.runtimeId,
    agentId: r.agentId ?? "",
    name: opt(r.name),
    harness: r.harness ?? "other",
    cwd: opt(r.cwd),
    transport: opt(r.transport),
    presence: runtimePresence(r.presence, r.active),
    active: r.active === 1,
    startedAt: r.startedAt ?? 0,
    stoppedAt: opt(r.stoppedAt),
    lastHeartbeat: opt(r.lastHeartbeat),
  }));
}

async function agentByIdOrName(
  db: ReadDb,
  id: string,
  _project?: string,
): Promise<AgentSummaryRow | undefined> {
  const columns = {
    agentId: agents.agentId,
    name: agents.name,
    defaultHarness: agents.defaultHarness,
    tier: agents.tier,
    disabledAt: agents.disabledAt,
  };
  const exact = await db
    .select(columns)
    .from(agents)
    .where(eq(agents.agentId, id))
    .limit(1);
  const rows = exact.length > 0 ? exact : await db
    .select(columns)
    .from(agents)
    .where(eq(agents.name, id))
    .limit(2);
  if (exact.length === 0 && rows.length > 1) {
    throw new Error(`ambiguous agent name ${JSON.stringify(id)}; address it by stable agent id`);
  }

  const row = rows[0];
  if (!row) return undefined;
  return {
    agentId: row.agentId,
    name: row.name ?? "",
    defaultHarness: opt(row.defaultHarness),
    tier: opt(row.tier),
    disabled: row.disabledAt !== null,
  };
}

/** Resolve managed-owner fields by exact stable id, then one globally unique display alias. */
export async function agentOwnerByName(
  db: ReadDb,
  nameOrAgentId: string,
): Promise<AgentOwnerRow | undefined> {
  const columns = {
    agentId: agents.agentId,
    name: agents.name,
    tier: agents.tier,
    ownerName: agents.ownerName,
    ownerSessionId: agents.ownerSessionId,
    ownerAgentId: agents.ownerAgentId,
  };
  const exact = await db
    .select(columns)
    .from(agents)
    .where(eq(agents.agentId, nameOrAgentId))
    .limit(1);
  const rows = exact.length > 0 ? exact : await db
    .select(columns)
    .from(agents)
    .where(eq(agents.name, nameOrAgentId))
    .limit(2);

  if (exact.length === 0 && rows.length > 1) {
    throw new Error(
      `ambiguous agent name ${JSON.stringify(nameOrAgentId)}; address it by stable agent id`,
    );
  }

  const row = rows[0];
  if (!row) return undefined;
  const sessionColumns = {
    sessionKind: sessions.kind,
    sessionTier: sessions.tier,
  };
  let sessionRows = await db
    .select(sessionColumns)
    .from(sessions)
    .where(eq(sessions.agentId, row.agentId))
    .orderBy(desc(sessions.createdAt))
    .limit(1);
  if (sessionRows.length === 0 && row.name) {
    sessionRows = await db
      .select(sessionColumns)
      .from(sessions)
      .where(eq(sessions.name, row.name))
      .orderBy(desc(sessions.createdAt))
      .limit(2);
    if (sessionRows.length > 1) {
      throw new Error(
        `ambiguous session name ${JSON.stringify(row.name)}; address it by stable agent id`,
      );
    }
  }
  const session = sessionRows[0];
  return {
    agentId: row.agentId,
    name: row.name ?? "",
    tier: opt(row.tier),
    ownerName: opt(row.ownerName),
    ownerSessionId: opt(row.ownerSessionId),
    ownerAgentId: opt(row.ownerAgentId),
    sessionKind: opt(session?.sessionKind),
    sessionTier: opt(session?.sessionTier),
  };
}

/** List delegated `/agent` observe grants for one durable agent id. */
export async function agentAccessGrantsByAgentId(
  db: ReadDb,
  agentId: string,
): Promise<AgentAccessGrantRow[]> {
  const rows = await db
    .select({
      agentId: agentAclGrants.agentId,
      principalProject: agentAclGrants.principalProject,
      principalName: agentAclGrants.principalName,
      principalSessionId: agentAclGrants.principalSessionId,
      principalAgentId: agentAclGrants.principalAgentId,
      role: agentAclGrants.role,
    })
    .from(agentAclGrants)
    .where(eq(agentAclGrants.agentId, agentId));
  return rows.map((row) => ({
    agentId: row.agentId ?? agentId,
    principalProject: row.principalProject ?? "",
    principalName: row.principalName ?? "",
    principalSessionId: opt(row.principalSessionId),
    principalAgentId: opt(row.principalAgentId),
    role: row.role ?? "",
  }));
}

async function agentRuntimeRows(
  db: ReadDb,
  args: { agentId: string; includeStopped: boolean },
): Promise<AgentRuntimeRow[]> {
  const where = args.includeStopped
    ? eq(agentRuntimes.agentId, args.agentId)
    : and(eq(agentRuntimes.agentId, args.agentId), eq(agentRuntimes.active, 1));
  const rows = await db
    .select({
      runtimeId: agentRuntimes.runtimeId,
      agentId: agentRuntimes.agentId,
      harness: agentRuntimes.harness,
      cwd: agentRuntimes.cwd,
      transport: agentRuntimes.transport,
      presence: agentRuntimes.presence,
      active: agentRuntimes.active,
      startedAt: agentRuntimes.startedAt,
      stoppedAt: agentRuntimes.stoppedAt,
      lastHeartbeat: agentRuntimes.lastHeartbeat,
    })
    .from(agentRuntimes)
    .where(where)
    .orderBy(desc(agentRuntimes.active), asc(agentRuntimes.startedAt));

  return rows.map((r) => ({
    runtimeId: r.runtimeId,
    agentId: r.agentId ?? args.agentId,
    harness: r.harness ?? "other",
    cwd: opt(r.cwd),
    transport: opt(r.transport),
    presence: runtimePresence(r.presence, r.active),
    active: r.active === 1,
    startedAt: r.startedAt ?? 0,
    stoppedAt: opt(r.stoppedAt),
    lastHeartbeat: opt(r.lastHeartbeat),
  }));
}

// ── listThreads ───────────────────────────────────────────────────────────────
/**
 * GET /threads — named threads only (DM threads, by the `dm:` naming convention,
 * are excluded from the channel list). Each carries its member names + the last
 * message time across the thread's messages.
 */
export async function listThreads(
  db: ReadDb,
  _opts: { project?: string } = {},
): Promise<ThreadRow[]> {
  const threadRows = await db
    .select({
      threadId: threads.threadId,
      name: threads.name,
      topic: threads.topic,
      description: threads.description,
    })
    .from(threads)
    .where(isNull(threads.archivedAt))
    .orderBy(asc(threads.name));

  const threadIds = threadRows.map((t) => t.threadId).filter((id): id is string => Boolean(id));
  const memberRows = threadIds.length
    ? await db
      .select({
        threadId: threadMembers.threadId,
        sessionName: threadMembers.sessionName,
        joinedAt: threadMembers.joinedAt,
      })
      .from(threadMembers)
      .where(inArray(threadMembers.threadId, threadIds))
      .orderBy(asc(threadMembers.threadId), asc(threadMembers.joinedAt), asc(threadMembers.sessionName))
    : [];
  const latestMessageRows = threadIds.length
    ? await db
      .select({
        threadId: messages.threadId,
        createdAt: sql<number | null>`MAX(${messages.createdAt})`,
      })
      .from(messages)
      .where(inArray(messages.threadId, threadIds))
      .groupBy(messages.threadId)
    : [];
  const latestMessageByThreadId = new Map<string, number>();
  for (const row of latestMessageRows) {
    if (!row.threadId || row.createdAt === null) continue;
    latestMessageByThreadId.set(row.threadId, row.createdAt);
  }
  const threadEventTopics = threadRows
    .map((t) => t.name ?? "")
    .filter((name) => name && !name.startsWith("dm:"))
    .map((name) => `sys.message.thread.${name}`);
  // latestSeq enrichment reads a table the daemon only creates at the session-stream schema.
  // The gateway is a read-only consumer and may run ahead of the live schema; missing
  // enrichment must degrade to latestSeq=0, never fail the whole channel list.
  let eventTopicRows: { topic: string | null; latestSeq: number | null }[] = [];
  if (threadEventTopics.length) {
    try {
      eventTopicRows = await db
        .select({
          topic: developerEventTopics.topic,
          latestSeq: developerEventTopics.latestSeq,
        })
        .from(developerEventTopics)
        .where(inArray(developerEventTopics.topic, threadEventTopics));
    } catch {
      eventTopicRows = [];
    }
  }
  const latestSeqByTopic = new Map<string, number>();
  for (const row of eventTopicRows) {
    if (!row.topic || row.latestSeq === null) continue;
    latestSeqByTopic.set(row.topic, row.latestSeq);
  }

  const out: ThreadRow[] = [];
  for (const t of threadRows) {
    const name = t.name ?? "";
    if (name.startsWith("dm:")) continue; // DM threads aren't channels.
    const mine = memberRows.filter((m) => m.threadId === t.threadId);
    const members = mine.map((m) => m.sessionName ?? "").filter(Boolean);
    const lastAt = t.threadId ? latestMessageByThreadId.get(t.threadId) : undefined;
    out.push({
      name,
      topic: opt(t.topic),
      description: opt(t.description),
      members,
      lastAt,
      latestSeq: latestSeqByTopic.get(`sys.message.thread.${name}`) ?? 0,
    });
  }
  return out;
}

// ── threadMembersFor ─────────────────────────────────────────────────────────
/** GET /threads/:name/members — members for one named thread from the read store. */
export async function threadMembersFor(
  db: ReadDb,
  args: { thread: string; project?: string },
): Promise<ThreadRow | undefined> {
  const threadRows = await db
    .select({
      threadId: threads.threadId,
      name: threads.name,
    })
    .from(threads)
    .where(and(eq(threads.name, args.thread), isNull(threads.archivedAt)))
    .limit(1);

  const thread = threadRows[0];
  if (!thread) return undefined;

  const memberRows = await db
    .select({
      sessionName: threadMembers.sessionName,
    })
    .from(threadMembers)
    .where(eq(threadMembers.threadId, thread.threadId))
    .orderBy(asc(threadMembers.joinedAt), asc(threadMembers.sessionName));

  return {
    name: thread.name ?? args.thread,
    members: memberRows.map((m) => m.sessionName ?? "").filter(Boolean),
  };
}

// ── threadHeader ─────────────────────────────────────────────────────────────
/** GET /threads/:name/header — one compact channel-open read from active thread state. */
export async function threadHeader(
  db: ReadDb,
  args: { thread: string; project?: string } & PresenceOptions,
): Promise<ThreadHeaderRow | undefined> {
  const threadRows = await db
    .select({
      threadId: threads.threadId,
      name: threads.name,
      topic: threads.topic,
      description: threads.description,
    })
    .from(threads)
    .where(and(eq(threads.name, args.thread), isNull(threads.archivedAt)))
    .limit(1);

  const thread = threadRows[0];
  if (!thread) return undefined;

  const memberRows = await db
    .select({
      sessionName: threadMembers.sessionName,
      sessionId: sessions.sessionId,
      kind: sessions.kind,
      agent: sessions.agent,
      presence: sessions.presence,
      currentWork: sessions.currentWork,
      lastHeartbeat: sessions.lastHeartbeat,
    })
    .from(threadMembers)
    .leftJoin(sessions, eq(sessions.name, threadMembers.sessionName))
    .where(eq(threadMembers.threadId, thread.threadId))
    .orderBy(asc(threadMembers.joinedAt), asc(threadMembers.sessionName));

  const now = args.now ?? Date.now();
  const heartbeatTtlMs = args.heartbeatTtlMs ?? DEFAULT_HEARTBEAT_TTL_MS;
  const activeRuntimeRows = await db
    .select({
      activeSessions: sql<number>`COUNT(DISTINCT ${sessions.sessionId})`,
    })
    .from(threadMembers)
    .innerJoin(sessions, eq(sessions.name, threadMembers.sessionName))
    .innerJoin(agentRuntimes, eq(agentRuntimes.agentId, sessions.agentId))
    .where(and(
      eq(threadMembers.threadId, thread.threadId),
      eq(agentRuntimes.active, 1),
      isNull(agentRuntimes.stoppedAt),
      isNotNull(sessions.lastHeartbeat),
      gte(sessions.lastHeartbeat, now - heartbeatTtlMs),
      isNotNull(sessions.presence),
      ne(sessions.presence, "offline"),
    ));

  const latestMessageRows = await db
    .select({ createdAt: sql<number | null>`MAX(${messages.createdAt})` })
    .from(messages)
    .where(eq(messages.threadId, thread.threadId));
  const lastAt = latestMessageRows[0]?.createdAt ?? undefined;

  const members = memberRows.map((m) => {
    const sessionId = m.sessionId ?? m.sessionName ?? "";
    return {
      name: m.sessionName ?? "",
      kind: opt(m.kind),
      agent: opt(m.agent),
      presence: effectivePresence(m.presence, m.lastHeartbeat, args),
      sessionId,
      currentWork: opt(m.currentWork),
    };
  }).filter((m) => m.name);

  return {
    name: thread.name ?? args.thread,
    topic: opt(thread.topic),
    description: opt(thread.description),
    lastAt,
    members,
    memberCount: members.length,
    activeSessions: activeRuntimeRows[0]?.activeSessions ?? 0,
  };
}

// ── threadHistory ─────────────────────────────────────────────────────────────
/**
 * GET /threads/:name/history — chronological (ascending) recall of one named
 * thread. `before` (epoch-millis) pages backwards; `limit` caps the window.
 */
export async function threadHistory(
  db: ReadDb,
  args: {
    thread: string;
    limit?: number;
    before?: number;
    after?: number;
    afterRowid?: number;
  },
): Promise<HistoryRow[]> {
  const threadId = await threadIdByName(db, args.thread);
  if (!threadId) return [];
  return historyByThreadId(
    db,
    threadId,
    args.limit,
    args.before,
    args.after,
    args.afterRowid,
  );
}

// ── dmHistory ─────────────────────────────────────────────────────────────────
/**
 * GET history for a DM with `with` (a partner name). Real Message Post DMs are
 * stored as direct `messages.kind = "dm"` rows with `thread_id = NULL`, so this
 * matches the caller↔partner pair directly instead of looking for a synthetic
 * `dm:<a>:<b>` thread.
 */
export async function dmHistory(
  db: ReadDb,
  args: {
    with: string;
    me?: string;
    project?: string;
    limit?: number;
    before?: number;
    after?: number;
    afterRowid?: number;
  },
): Promise<HistoryRow[]> {
  const pair = args.me
    ? or(
        and(eq(messages.fromName, args.me), eq(messages.toName, args.with)),
        and(eq(messages.fromName, args.with), eq(messages.toName, args.me)),
      )
    : or(eq(messages.fromName, args.with), eq(messages.toName, args.with));
  const where = and(
    eq(messages.kind, "dm"),
    isNull(messages.threadId),
    pair,
    args.project ? eq(messages.project, args.project) : undefined,
    args.before === undefined ? undefined : lt(messages.createdAt, args.before),
    historyAfterCursor(args.after, args.afterRowid),
  );

  const rows = await db
    .select({
      rowid: messageRowid,
      messageId: messages.messageId,
      from: messages.fromName,
      when: messages.createdAt,
      summary: messages.summary,
      body: messages.body,
    })
    .from(messages)
    .where(where)
    .orderBy(
      ...(args.after === undefined
        ? [desc(messages.createdAt), desc(messageRowid)]
        : [asc(messages.createdAt), asc(messageRowid)]),
    )
    .limit(args.limit ?? 50);

  const mapped = rows.map((r) => ({
      messageId: r.messageId,
      from: r.from ?? "",
      when: r.when ?? 0,
      summary: opt(r.summary),
      body: r.body ?? "",
      ...(r.rowid && r.rowid > 0
        ? { cursor: { createdAt: r.when ?? 0, rowid: r.rowid } }
        : {}),
    }));
  return args.after === undefined ? mapped.reverse() : mapped;
}

const messageRowid = sql<number>`rowid`;

function historyAfterCursor(after?: number, afterRowid?: number) {
  if (after === undefined) return undefined;
  const afterCreatedAt = gt(messages.createdAt, after);
  if (!afterRowid) return afterCreatedAt;
  return or(
    afterCreatedAt,
    and(eq(messages.createdAt, after), gt(messageRowid, afterRowid)),
  );
}

// Shared chronological-recall body for thread + DM history.
async function historyByThreadId(
  db: ReadDb,
  threadId: string,
  limit = 50,
  before?: number,
  after?: number,
  afterRowid?: number,
): Promise<HistoryRow[]> {
  const where = and(
    eq(messages.threadId, threadId),
    before === undefined ? undefined : lt(messages.createdAt, before),
    historyAfterCursor(after, afterRowid),
  );

  const rows = await db
    .select({
      rowid: messageRowid,
      messageId: messages.messageId,
      from: messages.fromName,
      when: messages.createdAt,
      summary: messages.summary,
      body: messages.body,
    })
    .from(messages)
    .where(where)
    .orderBy(
      ...(after === undefined
        ? [desc(messages.createdAt), desc(messageRowid)]
        : [asc(messages.createdAt), asc(messageRowid)]),
    )
    .limit(limit);

  const mapped = rows.map((r) => ({
      messageId: r.messageId,
      from: r.from ?? "",
      when: r.when ?? 0,
      summary: opt(r.summary),
      body: r.body ?? "",
      ...(r.rowid && r.rowid > 0
        ? { cursor: { createdAt: r.when ?? 0, rowid: r.rowid } }
        : {}),
    }));
  return after === undefined ? mapped.reverse() : mapped;
}

// ── searchMessages ────────────────────────────────────────────────────────────
/**
 * GET /search — FTS5 search over messages(summary, body). Optionally scope to a
 * thread (by name). Returns ranked hits (best first). Issued as a read-only raw
 * SELECT against the FTS5 virtual table joined to `messages` (Drizzle has no
 * FTS5 query builder); it is a SELECT only — never a write.
 */
export async function searchMessages(
  db: ReadDb,
  args: {
    query: string;
    mode?: string;
    thread?: string;
    topic?: string;
    with?: string;
    since?: number;
    project?: string;
    caller?: string;
    limit?: number;
  },
): Promise<SearchRow[]> {
  const limit = args.limit ?? 50;
  if (args.mode === "semantic") return [];

  let threadId: string | undefined;
  if (args.thread) {
    threadId = await threadIdByName(db, args.thread);
    if (!threadId) return [];
  }

  // bm25() is a negative-better rank; map to a positive descending score for
  // display. snippet() wraps matches; we strip the markers for a plain preview.
  const filters: string[] = ["messages_fts MATCH ?"];
  const sqlArgs: (string | number)[] = [args.query];
  if (threadId) {
    filters.push("m.thread_id = ?");
    sqlArgs.push(threadId);
  }
  if (args.topic) {
    filters.push("m.topic = ?");
    sqlArgs.push(args.topic);
  }
  if (args.with) {
    if (!args.caller) return [];
    filters.push("m.kind = 'dm'");
    filters.push("((m.from_name = ? AND m.to_name = ?) OR (m.from_name = ? AND m.to_name = ?))");
    sqlArgs.push(args.caller, args.with, args.with, args.caller);
  } else if (args.caller) {
    // Unscoped search can see public thread/topic rows plus the caller's own DMs.
    filters.push("(m.kind != 'dm' OR m.from_name = ? OR m.to_name = ?)");
    sqlArgs.push(args.caller, args.caller);
  }
  if (args.project) {
    filters.push("(m.project = ? OR m.project = ?)");
    sqlArgs.push(args.project, `p_${args.project}`);
  }
  if (args.since !== undefined) {
    filters.push("m.created_at >= ?");
    sqlArgs.push(args.since);
  }
  sqlArgs.push(limit);

  const sql =
    "SELECT m.message_id AS messageId, m.from_name AS fromName, " +
    "m.created_at AS createdAt, m.body AS body, bm25(messages_fts) AS rank " +
    "FROM messages_fts " +
    "JOIN messages m ON m.rowid = messages_fts.rowid " +
    `WHERE ${filters.join(" AND ")} ` +
    " ORDER BY rank ASC LIMIT ?";

  const res = await db.$client.execute({ sql, args: sqlArgs });

  return res.rows.map((row) => {
    const r = row as unknown as {
      messageId: string;
      fromName: string | null;
      createdAt: number | null;
      body: string | null;
      rank: number | null;
    };
    const body = r.body ?? "";
    return {
      messageId: r.messageId,
      from: r.fromName ?? "",
      when: r.createdAt ?? 0,
      snippet: body.length > 160 ? `${body.slice(0, 157)}...` : body,
      // Turn bm25's "lower is better" into a 0..1-ish descending score.
      score: r.rank === null ? 0 : 1 / (1 + Math.abs(r.rank)),
    };
  });
}

// ── listTopics ────────────────────────────────────────────────────────────────
/** GET /topics — topics with their live subscriber counts. */
export async function listTopics(db: ReadDb): Promise<TopicRow[]> {
  const topicRows = await db
    .select({
      topic: topics.topic,
      subscribers: sql<number>`COUNT(${subscriptions.subscriberSession})`,
    })
    .from(topics)
    .leftJoin(subscriptions, eq(subscriptions.topic, topics.topic))
    .groupBy(topics.topic)
    .orderBy(asc(topics.topic));

  return topicRows.map((t) => ({
    topic: t.topic ?? "",
    subscribers: t.subscribers,
  }));
}

// ── listRoutingRules ──────────────────────────────────────────────────────────
/** Standing route rules (web console admin surface). */
export async function listRoutingRules(db: ReadDb): Promise<RouteRuleRow[]> {
  // Standing routing rules are not persisted by the daemon yet (there is no
  // `routing_rules` table in the live store — see the unwired `POST /routing-rules`).
  // Treat a missing table as "no rules" so the read-view returns an empty list
  // instead of a 500; real rows appear once the daemon creates the table.
  let rows: { source: string | null; topic: string | null; to: string | null }[];
  try {
    rows = await db
      .select({
        source: routingRules.source,
        topic: routingRules.topic,
        to: routingRules.toName,
      })
      .from(routingRules)
      .orderBy(asc(routingRules.ruleId));
  } catch (err) {
    // Drizzle wraps the libSQL error, so the "no such table" text can be on the
    // cause rather than the top-level message — check the whole chain.
    const text = `${(err as Error)?.message ?? ""} ${
      String((err as { cause?: { message?: string } })?.cause?.message ?? "")
    }`;
    if (text.includes("no such table")) return [];
    throw err;
  }

  return rows.map((r) => ({
    source: opt(r.source),
    topic: opt(r.topic),
    to: r.to ?? "",
  }));
}

// ── listNotifications ─────────────────────────────────────────────────────────
/**
 * Notification routing audit and delivered source pushes (who-got-what), newest first.
 * Each backing source is cursor-limited before the bounded merge so row transfer is O(page).
 */
export async function listNotifications(
  db: ReadDb,
  opts: { limit?: number; before?: number } = {},
): Promise<NotificationRow[]> {
  const limit = Math.min(500, Math.max(1, Math.trunc(opts.limit ?? 100)));
  const auditRows = await db
    .select({
      notifId: notifications.notifId,
      source: notifications.source,
      topic: notifications.topic,
      hmacOk: notifications.hmacOk,
      routedTo: notifications.routedTo,
      when: notifications.createdAt,
    })
    .from(notifications)
    .where(opts.before === undefined ? undefined : lt(notifications.createdAt, opts.before))
    .orderBy(desc(notifications.createdAt), desc(notifications.notifId))
    .limit(limit);

  const audits = auditRows.map((r) => ({
    notifId: r.notifId,
    source: opt(r.source),
    topic: opt(r.topic),
    hmacOk: r.hmacOk === 1,
    routedTo: csvList(r.routedTo),
    when: r.when ?? 0,
  }));

  // `source.push` writes a canonical Message Post topic row, not a legacy `notifications` row.
  // Project those delivered rows into the same Pub feed shape so `/pub` reflects real pushes.
  const sourcePushRows = await db.$client.execute({
    sql:
      "SELECT m.message_id AS notifId, m.from_name AS source, m.topic AS topic, " +
      "COALESCE(GROUP_CONCAT(DISTINCT COALESCE(s.name, f.recipient_session)), '') AS routedTo, " +
      "m.created_at AS createdAt " +
      "FROM messages m " +
      "JOIN sources src ON src.name = m.from_name " +
      "LEFT JOIN in_flight f ON f.message_id = m.message_id " +
      "LEFT JOIN sessions s ON s.session_id = f.recipient_session " +
      "WHERE m.kind = 'topic' " +
      (opts.before === undefined ? "" : "AND m.created_at < ? ") +
      "GROUP BY m.message_id, m.from_name, m.topic, m.created_at " +
      "ORDER BY m.created_at DESC, m.message_id DESC " +
      "LIMIT ?",
    args: opts.before === undefined ? [limit] : [opts.before, limit],
  });

  const pushed = sourcePushRows.rows.map((row) => {
    const r = row as unknown as {
      notifId: string;
      source: string | null;
      topic: string | null;
      routedTo: string | null;
      createdAt: number | null;
    };
    return {
      notifId: r.notifId,
      source: opt(r.source),
      topic: opt(r.topic),
      hmacOk: true,
      routedTo: csvList(r.routedTo),
      when: r.createdAt ?? 0,
    };
  });

  return [...audits, ...pushed]
    .sort((a, b) => b.when - a.when || b.notifId.localeCompare(a.notifId))
    .slice(0, limit);
}

function csvList(value: string | null): string[] {
  return (value ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

// ── listSources ───────────────────────────────────────────────────────────────
/** GET /sources — registered notification sources from the read store. */
export async function listSources(
  db: ReadDb,
): Promise<{ sources: SourceRow[] }> {
  const rows = await db
    .select({
      name: sources.name,
      topic: sources.topic,
      enabled: sources.enabled,
      createdAt: sources.createdAt,
      lastFiredAt: sources.lastFiredAt,
    })
    .from(sources)
    .orderBy(asc(sources.name));

  return { sources: rows.map(sourceRow) };
}

// ── sourceByName ─────────────────────────────────────────────────────────────
/** GET /sources/:name — one registered notification source from the read store. */
export async function sourceByName(
  db: ReadDb,
  name: string,
): Promise<SourceRow | undefined> {
  const rows = await db
    .select({
      name: sources.name,
      topic: sources.topic,
      enabled: sources.enabled,
      createdAt: sources.createdAt,
      lastFiredAt: sources.lastFiredAt,
    })
    .from(sources)
    .where(eq(sources.name, name))
    .limit(1);

  return rows[0] ? sourceRow(rows[0]) : undefined;
}

// ── sourceSecretByName ───────────────────────────────────────────────────────
/** Resolve a source token for gateway-side HMAC verification before source.push. */
export async function sourceSecretByName(
  db: ReadDb,
  name: string,
): Promise<SourceSecretRow | undefined> {
  const rows = await db
    .select({
      name: sources.name,
      token: sources.token,
      topic: sources.topic,
      enabled: sources.enabled,
      createdAt: sources.createdAt,
      lastFiredAt: sources.lastFiredAt,
    })
    .from(sources)
    .where(eq(sources.name, name))
    .limit(1);

  const row = rows[0];
  if (!row) return undefined;
  return { ...sourceRow(row), token: row.token ?? "" };
}

function sourceRow(row: {
  name: string | null;
  topic: string | null;
  enabled: number | null;
  createdAt: number | null;
  lastFiredAt: number | null;
}): SourceRow {
  return {
    name: row.name ?? "",
    topic: row.topic ?? "",
    enabled: row.enabled !== 0,
    createdAt: row.createdAt ?? 0,
    lastFiredAt: opt(row.lastFiredAt),
  };
}

// ── listProjects ──────────────────────────────────────────────────────────────
/**
 * The project labels the web console can scope to, keyed by NAME.
 *
 * Project is intentionally not a durable primitive: it is the plain scope
 * string carried on sessions/threads (`sessions.project`). The list therefore
 * derives from live daemon rows only. `projectId` is the project NAME — the
 * value the rest of the read-view scopes on (`?project=<name>`).
 */
export async function listProjects(db: ReadDb): Promise<ProjectRow[]> {
  const scopes = await db
    .selectDistinct({ project: sessions.project })
    .from(sessions);

  const byName = new Map<string, ProjectRow>();
  for (const s of scopes) {
    const name = s.project ?? "";
    if (name && !byName.has(name)) byName.set(name, { projectId: name, name });
  }

  return [...byName.values()].sort((a, b) => a.name.localeCompare(b.name));
}

// ── whoamiRow ─────────────────────────────────────────────────────────────────
/**
 * GET /whoami — resolve a session by name into the caller's identity row. The
 * gateway knows the caller's name from the auth seam; this maps it to the
 * displayable identity. Returns `undefined` if the name isn't registered.
 */
export async function whoamiRow(
  db: ReadDb,
  name: string,
  opts: PresenceOptions = {},
): Promise<WhoamiRow | undefined> {
  const rows = await db
    .select({
      agentId: sessions.agentId,
      name: sessions.name,
      sessionId: sessions.sessionId,
      tier: sessions.tier,
      presence: sessions.presence,
      lastHeartbeat: sessions.lastHeartbeat,
    })
    .from(sessions)
    .where(eq(sessions.name, name))
    .limit(2);

  if (rows.length > 1) {
    throw new Error(
      `ambiguous session name ${JSON.stringify(name)}; address it by stable agent id`,
    );
  }

  const r = rows[0];
  if (!r) return undefined;
  return {
    agentId: opt(r.agentId),
    name: r.name ?? "",
    sessionId: r.sessionId,
    tier: r.tier ?? "agent",
    presence: effectivePresence(r.presence, r.lastHeartbeat, opts),
  };
}
