import type { Client, Row } from "@libsql/client";

import {
  pageCanonicalMessages,
  type CanonicalGatewayMessage,
  type MessagePageOptions,
} from "../store/repos/messages";
import { Kind, Scope, type Message, type Provenance } from "@shared/types";
import { parseEntityKind } from "@server/identity/entityKind";
import type {
  AgentAccessGrantRow,
  AgentOwnerRow,
  AgentRuntimeRow,
  AgentShowRow,
  MemberRow,
  ProjectRow,
  RouteRuleRow,
  ThreadHeaderRow,
  ThreadRow,
  WhoamiRow,
} from "./queries";
import type { MetadataEntityKind, MetadataReadResult } from "./metadata";

export interface CanonicalSearchRow {
  messageId: string;
  from: string;
  when: number;
  snippet: string;
  score: number;
}

export interface CanonicalHistoryRow {
  messageId: string;
  from: string;
  fromKind?: string;
  when: number;
  summary?: string;
  body: string;
  cursor: { createdAt: number; rowid: 0; opaque: string };
}

export interface CanonicalHistoryPage {
  rows: CanonicalHistoryRow[];
  before?: string;
  after?: string;
  rebased: boolean;
}

export interface CanonicalAgentSessionTarget {
  sessionId?: string;
  owner: AgentOwnerRow;
}

export interface CanonicalAgentSessionLookup {
  agentId?: string;
  sessionId?: string;
  name?: string;
  /** Strict retained pair: no runtime-id alias or current-runtime fallback. */
  exact?: boolean;
}

interface CanonicalRuntimeRow extends AgentRuntimeRow {
  name?: string;
}

/** Project the roster from Gateway-owned identity and latest-runtime state. */
export async function canonicalMembers(
  db: Client,
  options: { includeOffline?: boolean; project?: string } = {},
): Promise<MemberRow[]> {
  const result = await db.execute(`
    SELECT i.agent_id, i.name, i.tier, i.metadata_json,
           r.runtime_id, r.session_id, r.harness, r.status, r.updated_at
    FROM identities i
    LEFT JOIN runtime_descriptors r ON r.agent_id = i.agent_id
    ORDER BY i.name ASC,
      CASE WHEN lower(COALESCE(r.status, 'stopped')) = 'stopped' THEN 1 ELSE 0 END ASC,
      r.updated_at DESC
  `);
  const members = new Map<string, MemberRow>();
  for (const row of result.rows) {
    const agentId = String(row.agent_id);
    if (members.has(agentId)) continue;
    const metadata = parseObject(row.metadata_json);
    if (options.project && identityProject(metadata) !== options.project) continue;
    const status = canonicalPresence(row.status);
    if (options.includeOffline !== true && status === "offline") continue;
    const kind = optionalString(metadata.kind) ?? "agent";
    const harness = optionalString(row.harness) ?? optionalString(metadata.defaultHarness);
    members.set(agentId, {
      name: optionalString(row.name) ?? agentId,
      sessionId: optionalString(row.session_id) ?? optionalString(row.runtime_id) ?? "",
      agentId,
      ...(kind === "agent" && harness ? { agent: harness } : {}),
      kind,
      tier: optionalString(row.tier),
      presence: status,
      currentWork: optionalString(metadata.currentWork),
    });
  }
  return [...members.values()];
}

/** Project runtime fleet rows from Gateway-owned resurrection descriptors. */
export async function canonicalRuntimes(
  db: Client,
  options: { includeStopped?: boolean; project?: string } = {},
): Promise<CanonicalRuntimeRow[]> {
  const result = await db.execute(`
    SELECT r.*, i.name, i.metadata_json
    FROM runtime_descriptors r
    LEFT JOIN identities i ON i.agent_id = r.agent_id
    ORDER BY CASE WHEN lower(r.status) = 'stopped' THEN 1 ELSE 0 END ASC,
      r.updated_at DESC
  `);
  return result.rows.flatMap((row) => {
    const metadata = parseObject(row.metadata_json);
    if (options.project && identityProject(metadata) !== options.project) return [];
    const active = String(row.status).toLowerCase() !== "stopped";
    if (options.includeStopped !== true && !active) return [];
    const updatedAt = Number(row.updated_at);
    return [{
      runtimeId: String(row.runtime_id),
      agentId: String(row.agent_id),
      name: optionalString(row.name),
      harness: String(row.harness),
      cwd: optionalString(row.cwd),
      transport: optionalString(row.backend),
      presence: canonicalPresence(row.status),
      active,
      startedAt: updatedAt,
      stoppedAt: active ? undefined : updatedAt,
      lastHeartbeat: active ? updatedAt : undefined,
    }];
  });
}

export async function canonicalAgentRuntimesFor(
  db: Client,
  id: string,
  options: { includeStopped?: boolean; project?: string } = {},
): Promise<{ agentId: string; runtimes: CanonicalRuntimeRow[] } | undefined> {
  const identity = await canonicalIdentity(db, id);
  if (!identity) return undefined;
  const runtimes = await canonicalRuntimes(db, {
    includeStopped: options.includeStopped,
  });
  return { agentId: identity.agentId, runtimes: runtimes.filter((row) => row.agentId === identity.agentId) };
}

export async function canonicalAgentShow(
  db: Client,
  id: string,
  options: { includeStopped?: boolean; project?: string } = {},
): Promise<AgentShowRow | undefined> {
  const identity = await canonicalIdentity(db, id);
  if (!identity) return undefined;
  const runtimeRows = await canonicalAgentRuntimesFor(db, id, options);
  const runtimes = runtimeRows?.runtimes ?? [];
  return {
    agent: {
      agentId: identity.agentId,
      name: identity.name,
      defaultHarness: optionalString(identity.metadata.defaultHarness) ?? runtimes[0]?.harness,
      tier: identity.tier,
      disabled: identity.metadata.disabledAt !== undefined && identity.metadata.disabledAt !== null,
      activeRuntime: runtimes.find((runtime) => runtime.active),
    },
    runtimes,
  };
}

export async function canonicalProjects(db: Client): Promise<ProjectRow[]> {
  const result = await db.execute("SELECT agent_id, owner, metadata_json, updated_at FROM identities ORDER BY updated_at ASC");
  const projects = new Map<string, ProjectRow>();
  for (const row of result.rows) {
    const metadata = parseObject(row.metadata_json);
    const name = identityProject(metadata);
    if (!projects.has(name)) {
      projects.set(name, {
        projectId: name,
        name,
        createdBy: optionalString(row.owner) ?? String(row.agent_id),
        rootPath: optionalString(metadata.rootPath),
        createdAt: Number(row.updated_at),
      });
    }
  }
  return [...projects.values()].sort((a, b) => a.name.localeCompare(b.name));
}

export async function canonicalRoutingRules(db: Client): Promise<RouteRuleRow[]> {
  const result = await db.execute(`
    SELECT s.topic, s.group_name, i.name, s.agent_id
    FROM topic_subscriptions s
    LEFT JOIN identities i ON i.agent_id = s.agent_id
    ORDER BY s.topic ASC, COALESCE(i.name, s.group_name, s.agent_id) ASC
  `);
  return result.rows.map((row) => ({
    topic: String(row.topic),
    to: optionalString(row.name) ?? optionalString(row.group_name) ?? optionalString(row.agent_id) ?? "",
  })).filter((row) => row.to.length > 0);
}

export async function canonicalWhoami(db: Client, name: string): Promise<WhoamiRow | undefined> {
  const identity = await canonicalIdentity(db, name);
  if (!identity) return undefined;
  const member = (await canonicalMembers(db, { includeOffline: true }))
    .find((row) => row.agentId === identity.agentId);
  return {
    agentId: identity.agentId,
    name: identity.name,
    sessionId: member?.sessionId ?? "",
    tier: identity.tier ?? "Agent",
    presence: member?.presence ?? "offline",
  };
}

export async function canonicalThreadMembers(
  db: Client,
  name: string,
): Promise<ThreadRow | undefined> {
  const result = await db.execute({
    sql: `SELECT t.name,
                 COALESCE(
                   i.name,
                   CASE
                     WHEN tm.agent_id LIKE 'legacy-name:%'
                     THEN substr(tm.agent_id, 13)
                   END
                 ) AS member_name
          FROM threads t
          LEFT JOIN thread_members tm ON tm.thread_id = t.thread_id AND tm.left_at IS NULL
          LEFT JOIN identities i ON i.agent_id = tm.agent_id
          WHERE t.name = ? AND t.archived_at IS NULL
          ORDER BY tm.joined_at ASC, i.name ASC`,
    args: [name],
  });
  if (result.rows.length === 0) return undefined;
  return {
    name: String(result.rows[0]!.name),
    members: result.rows.flatMap((row) => optionalString(row.member_name) ?? []),
  };
}

export async function canonicalThreadHeader(
  db: Client,
  name: string,
): Promise<ThreadHeaderRow | undefined> {
  const thread = await canonicalThreadMembers(db, name);
  if (!thread) return undefined;
  const roster = await canonicalMembers(db, { includeOffline: true });
  const members = thread.members.map((memberName) => {
    const member = roster.find((row) => row.name === memberName);
    return {
      name: memberName,
      kind: member?.kind,
      agent: member?.agent,
      presence: member?.presence ?? "offline",
      sessionId: member?.sessionId ?? "",
      currentWork: member?.currentWork,
    };
  });
  const latest = await db.execute({
    sql: `SELECT MAX(m.created_at) AS last_at
          FROM bus_messages m JOIN threads t ON t.thread_id = m.thread_id
          WHERE t.name = ?`,
    args: [name],
  });
  return {
    name,
    lastAt: optionalNumber(latest.rows[0]?.last_at),
    members,
    memberCount: members.length,
    activeSessions: members.filter((member) => member.presence !== "offline").length,
  };
}

/** Read the metadata represented in the canonical Gateway projection. */
export async function canonicalMetadata(
  db: Client,
  entity: MetadataEntityKind,
  id: string,
): Promise<MetadataReadResult | undefined> {
  if (entity === "agent") {
    const identity = await canonicalIdentity(db, id);
    return identity ? { entity, id, metadata: identity.metadata } : undefined;
  }
  let result;
  switch (entity) {
    case "message":
      result = await db.execute({
        sql: "SELECT provenance_json AS metadata_json FROM bus_messages WHERE message_id = ? LIMIT 1",
        args: [id],
      });
      break;
    case "session":
      result = await db.execute({
        sql: `SELECT '{}' AS metadata_json FROM runtime_descriptors
              WHERE session_id = ? OR runtime_id = ? LIMIT 1`,
        args: [id, id],
      });
      break;
    case "thread":
      result = await db.execute({
        sql: "SELECT '{}' AS metadata_json FROM threads WHERE name = ? OR thread_id = ? LIMIT 1",
        args: [id, id],
      });
      break;
  }
  const row = result.rows[0];
  if (!row) return undefined;
  const stored = parseObject(row.metadata_json);
  return {
    entity,
    id,
    metadata: entity === "message" && stored.metadata !== undefined ? stored.metadata : stored,
  };
}

/** Resolve a display name to the newest live runtime using Gateway-owned projection state. */
export async function canonicalAgentSessionTarget(
  db: Client,
  lookup: string | CanonicalAgentSessionLookup,
): Promise<CanonicalAgentSessionTarget | undefined> {
  const exactAgentId = typeof lookup === "string" ? undefined : optionalString(lookup.agentId);
  const exactSessionId = typeof lookup === "string" ? undefined : optionalString(lookup.sessionId);
  const strict = typeof lookup !== "string" && lookup.exact === true;
  if (strict && (!exactAgentId || !exactSessionId)) return undefined;
  const displayLookup = typeof lookup === "string"
    ? lookup
    : optionalString(lookup.name);
  const target = exactAgentId ?? exactSessionId ?? displayLookup;
  if (!target) return undefined;
  let identity: CanonicalIdentity | undefined;
  let sessionId: string | undefined;
  if (exactSessionId) {
    const runtimeColumns = `SELECT i.agent_id, i.name, i.owner, i.tier, i.metadata_json,
                   r.session_id, r.runtime_id
            FROM runtime_descriptors r
            JOIN identities i ON i.agent_id = r.agent_id`;
    const runtimeOrder = `ORDER BY CASE
              WHEN lower(COALESCE(r.status, '')) IN ('stopped', 'offline') THEN 1
              ELSE 0
            END ASC,
            r.updated_at DESC
            LIMIT 1`;
    let result = await db.execute({
      sql: `SELECT i.agent_id, i.name, i.owner, i.tier, i.metadata_json,
                   r.session_id, r.runtime_id
            FROM runtime_descriptors r
            JOIN identities i ON i.agent_id = r.agent_id
            WHERE r.session_id = ?
            ${runtimeOrder}`,
      args: [exactSessionId],
    });
    if (!result.rows[0] && !strict) {
      result = await db.execute({
        sql: `${runtimeColumns} WHERE r.runtime_id = ? ${runtimeOrder}`,
        args: [exactSessionId],
      });
    }
    const row = result.rows[0];
    if (!row) return undefined;
    identity = canonicalIdentityFromRow(row);
    if (exactAgentId && identity.agentId !== exactAgentId) return undefined;
    sessionId = optionalString(row.session_id) ?? optionalString(row.runtime_id);
  } else {
    identity = exactAgentId
      ? await canonicalIdentityByAgentId(db, exactAgentId)
      : await canonicalIdentity(db, displayLookup!);
    if (!identity) return undefined;
    const runtime = await db.execute({
      sql: `SELECT session_id, runtime_id
            FROM runtime_descriptors
            WHERE agent_id = ?
            ORDER BY CASE
              WHEN lower(COALESCE(status, '')) IN ('stopped', 'offline') THEN 1
              ELSE 0
            END ASC,
            updated_at DESC
            LIMIT 1`,
      args: [identity.agentId],
    });
    sessionId = optionalString(runtime.rows[0]?.session_id)
      ?? optionalString(runtime.rows[0]?.runtime_id);
  }
  return {
    ...(sessionId ? { sessionId } : {}),
    owner: {
      agentId: identity.agentId,
      name: identity.name,
      tier: identity.tier,
      ownerName: identity.owner,
      ownerSessionId: optionalString(identity.metadata.ownerSessionId),
      ownerAgentId: optionalString(identity.metadata.ownerAgentId),
      sessionKind: optionalString(identity.metadata.kind),
      sessionTier: optionalString(identity.metadata.sessionTier),
    },
  };
}

/** Read the delegated ACL snapshot carried by the canonical identity projection. */
export async function canonicalAgentAccessGrantsByAgentId(
  db: Client,
  agentId: string,
): Promise<AgentAccessGrantRow[]> {
  const identity = await canonicalIdentityByAgentId(db, agentId);
  const raw = identity?.metadata.accessGrants;
  if (!Array.isArray(raw)) return [];
  const grants: AgentAccessGrantRow[] = [];
  for (const value of raw) {
    const grant = parseObject(value);
    const role = optionalString(grant.role);
    const principalSessionId = optionalString(grant.principalSessionId);
    const principalAgentId = optionalString(grant.principalAgentId);
    const principalName = optionalString(grant.principalName) ?? principalAgentId;
    if (!role || !principalName) continue;
    grants.push({
      agentId,
      principalProject: optionalString(grant.principalProject) ?? "default",
      principalName,
      ...(principalSessionId ? { principalSessionId } : {}),
      ...(principalAgentId ? { principalAgentId } : {}),
      role,
    });
  }
  return grants;
}

export async function canonicalThreads(db: Client): Promise<Array<{
  name: string;
  members: string[];
  lastAt?: number;
  latestSeq: number;
}>> {
  const result = await db.execute(`
    SELECT t.thread_id, t.name,
           COALESCE(
             i.name,
             CASE
               WHEN tm.agent_id LIKE 'legacy-name:%'
               THEN substr(tm.agent_id, 13)
             END
           ) AS member_name,
           (SELECT MAX(created_at) FROM bus_messages m WHERE m.thread_id = t.thread_id) AS last_at
    FROM threads t
    LEFT JOIN thread_members tm ON tm.thread_id = t.thread_id AND tm.left_at IS NULL
    LEFT JOIN identities i ON i.agent_id = tm.agent_id
    WHERE t.archived_at IS NULL
    ORDER BY COALESCE(last_at, t.updated_at) DESC, t.name ASC
  `);
  const byId = new Map<string, { name: string; members: string[]; lastAt?: number; latestSeq: number }>();
  for (const row of result.rows) {
    const id = String(row.thread_id);
    const item = byId.get(id) ?? {
      name: String(row.name),
      members: [],
      lastAt: optionalNumber(row.last_at),
      latestSeq: 0,
    };
    const member = optionalString(row.member_name);
    if (member && !item.members.includes(member)) item.members.push(member);
    byId.set(id, item);
  }
  return [...byId.values()];
}

export async function canonicalThreadHistory(
  db: Client,
  name: string,
  options: MessagePageOptions,
): Promise<CanonicalHistoryPage> {
  const thread = await db.execute({
    sql: "SELECT thread_id FROM threads WHERE name = ? LIMIT 1",
    args: [name],
  });
  const threadId = thread.rows[0]?.thread_id;
  const target = threadId === undefined || threadId === null
    ? { threadName: name }
    : { threadId: String(threadId) };
  return mapHistory(await pageCanonicalMessages(db, target, options));
}

export async function canonicalDmHistory(
  db: Client,
  nameOrAgentId: string,
  callerName: string,
  options: MessagePageOptions,
): Promise<CanonicalHistoryPage> {
  const identity = await canonicalIdentity(db, nameOrAgentId);
  const target = !identity
    ? { dmName: nameOrAgentId, callerName }
    : { dmAgentId: identity.agentId, callerName };
  return mapHistory(await pageCanonicalMessages(db, target, options));
}

export async function canonicalHistory(
  db: Client,
  options: {
    caller?: string;
    thread?: string;
    with?: string;
    topic?: string;
    limit: number;
    before?: number;
  },
): Promise<CanonicalHistoryRow[]> {
  const limit = Math.max(1, Math.min(200, Math.trunc(options.limit)));
  const filters: string[] = [];
  const args: Array<string | number> = [];
  if (options.thread) {
    filters.push("kind = 'thread' AND (to_name = ? OR thread_id = ?)");
    args.push(options.thread, options.thread);
  } else if (options.with) {
    if (!options.caller) return [];
    filters.push("kind = 'dm' AND ((from_name = ? AND to_name = ?) OR (from_name = ? AND to_name = ?))");
    args.push(options.caller, options.with, options.with, options.caller);
  } else if (options.topic) {
    filters.push("kind = 'topic' AND topic = ?");
    args.push(options.topic);
  } else if (options.caller) {
    filters.push("(kind != 'dm' OR from_name = ? OR to_name = ?)");
    args.push(options.caller, options.caller);
  } else {
    filters.push("kind != 'dm'");
  }
  if (options.before !== undefined) {
    filters.push("created_at < ?");
    args.push(options.before);
  }
  const result = await db.execute({
    sql: `SELECT message_id, from_name, from_agent_id, created_at, summary, body
          FROM bus_messages WHERE ${filters.join(" AND ")}
          ORDER BY created_at DESC, message_id DESC LIMIT ?`,
    args: [...args, limit],
  });
  return result.rows.reverse().map((row) => ({
    messageId: String(row.message_id),
    from: optionalString(row.from_name) ?? optionalString(row.from_agent_id) ?? "unknown",
    when: Number(row.created_at),
    summary: optionalString(row.summary),
    body: String(row.body),
    cursor: { createdAt: Number(row.created_at), rowid: 0, opaque: "" },
  }));
}

export async function canonicalSearch(
  db: Client,
  options: {
    query: string;
    mode?: string;
    caller?: string;
    thread?: string;
    topic?: string;
    with?: string;
    since?: number;
    limit: number;
  },
): Promise<CanonicalSearchRow[]> {
  if (options.mode === "semantic") return [];
  const filters = ["instr(lower(COALESCE(summary, '') || ' ' || body), lower(?)) > 0"];
  const args: Array<string | number> = [options.query];
  if (options.thread) {
    filters.push("kind = 'thread' AND (to_name = ? OR thread_id = ?)");
    args.push(options.thread, options.thread);
  }
  if (options.topic) {
    filters.push("kind = 'topic' AND topic = ?");
    args.push(options.topic);
  }
  if (options.with) {
    if (!options.caller) return [];
    filters.push("kind = 'dm' AND ((from_name = ? AND to_name = ?) OR (from_name = ? AND to_name = ?))");
    args.push(options.caller, options.with, options.with, options.caller);
  } else if (options.caller) {
    filters.push("(kind != 'dm' OR from_name = ? OR to_name = ?)");
    args.push(options.caller, options.caller);
  } else {
    filters.push("kind != 'dm'");
  }
  if (options.since !== undefined) {
    filters.push("created_at >= ?");
    args.push(options.since);
  }
  const limit = Math.max(1, Math.min(200, Math.trunc(options.limit)));
  const result = await db.execute({
    sql: `SELECT message_id, from_name, from_agent_id, created_at, body
          FROM bus_messages WHERE ${filters.join(" AND ")}
          ORDER BY created_at DESC, message_id DESC LIMIT ?`,
    args: [...args, limit],
  });
  return result.rows.map((row) => {
    const body = String(row.body);
    return {
      messageId: String(row.message_id),
      from: optionalString(row.from_name) ?? optionalString(row.from_agent_id) ?? "unknown",
      when: Number(row.created_at),
      snippet: body.length > 160 ? `${body.slice(0, 157)}...` : body,
      score: 1,
    };
  });
}

export async function canonicalMessageById(
  db: Client,
  id: string,
  caller?: string,
): Promise<Message | null> {
  const result = await db.execute({
    sql: "SELECT * FROM bus_messages WHERE message_id = ? LIMIT 1",
    args: [id],
  });
  const row = result.rows[0];
  if (!row) return null;
  const kind = String(row.kind);
  if (
    kind === "dm" &&
    (!caller || (optionalString(row.from_name) !== caller && optionalString(row.to_name) !== caller))
  ) {
    return null;
  }
  const raw = parseObject(row.provenance_json);
  const from = optionalString(row.from_name) ?? optionalString(row.from_agent_id) ?? "unknown";
  const entityKind = parseEntityKind(raw.kind ?? Kind.Agent, raw.locality);
  const provenance: Provenance = {
    from: typeof raw.from === "string" ? raw.from : from,
    kind: entityKind.kind,
    locality: entityKind.locality,
    access: optionalString(raw.access),
    thread: typeof raw.thread === "string" ? raw.thread : undefined,
    topic: typeof raw.topic === "string" ? raw.topic : undefined,
    stamp: isStamp(raw.stamp) ? raw.stamp : undefined,
  };
  return {
    id: String(row.message_id),
    project: typeof raw.project === "string" ? raw.project : "default",
    from,
    scope: kind === "thread" ? Scope.Thread : kind === "topic" ? Scope.Topic : Scope.Dm,
    thread: optionalString(row.thread_id),
    topic: optionalString(row.topic),
    body: String(row.body),
    summary: optionalString(row.summary),
    provenance,
    createdAt: Number(row.created_at),
  };
}

export async function canonicalTopics(db: Client): Promise<Array<{
  topic: string;
  subscribers: number;
}>> {
  const result = await db.execute(`
    SELECT t.topic, COUNT(s.rowid) AS subscribers
    FROM topics t LEFT JOIN topic_subscriptions s ON s.topic = t.topic
    GROUP BY t.topic ORDER BY t.topic ASC
  `);
  return result.rows.map((row) => ({
    topic: String(row.topic),
    subscribers: Number(row.subscribers),
  }));
}

export async function canonicalNotifications(
  db: Client,
  options: { limit: number; before?: number },
): Promise<Array<{
  notifId: string;
  messageId: string;
  source?: string;
  topic?: string;
  hmacOk: boolean;
  routedTo: string[];
  summary?: string;
  body: string;
  when: number;
}>> {
  const limit = Math.max(1, Math.min(200, Math.trunc(options.limit)));
  const result = await db.execute({
    sql: `SELECT * FROM notifications
          ${options.before === undefined ? "" : "WHERE created_at < ?"}
          ORDER BY created_at DESC, notification_id DESC LIMIT ?`,
    args: options.before === undefined ? [limit] : [options.before, limit],
  });
  return result.rows.map(mapNotification).reverse();
}

function mapHistory(page: {
  messages: CanonicalGatewayMessage[];
  before?: string;
  after?: string;
  rebased: boolean;
}): CanonicalHistoryPage {
  return {
    rows: page.messages.map((message) => ({
      messageId: message.messageId,
      from: message.fromName ?? message.fromAgentId ?? "unknown",
      fromKind: optionalString(message.provenance.kind),
      when: message.createdAt,
      summary: message.summary,
      body: message.body,
      cursor: {
        createdAt: message.createdAt,
        rowid: 0,
        opaque: page.messages.length === 1
          ? (page.after ?? page.before ?? "")
          : encodeRowCursor(message),
      },
    })),
    before: page.before,
    after: page.after,
    rebased: page.rebased,
  };
}

function encodeRowCursor(message: CanonicalGatewayMessage): string {
  return Buffer.from(JSON.stringify({ v: 1, at: message.createdAt, id: message.messageId }))
    .toString("base64url");
}

function mapNotification(row: Row) {
  const target = JSON.parse(String(row.target_json)) as unknown;
  return {
    notifId: String(row.notification_id),
    messageId: String(row.message_id),
    source: optionalString(row.source),
    topic: notificationTopic(target),
    hmacOk: true,
    routedTo: notificationTargets(target),
    summary: optionalString(row.summary),
    body: String(row.body),
    when: Number(row.created_at),
  };
}

function notificationTopic(target: unknown): string | undefined {
  if (!target || typeof target !== "object" || Array.isArray(target)) return undefined;
  const topic = (target as Record<string, unknown>).topic;
  return typeof topic === "string" && topic.length > 0 ? topic : undefined;
}

function notificationTargets(target: unknown): string[] {
  if (typeof target === "string") return [target];
  if (Array.isArray(target)) return target.flatMap(notificationTargets);
  if (!target || typeof target !== "object") return [];
  const record = target as Record<string, unknown>;
  return [record.agentId, record.name, record.thread, record.topic, record.group]
    .filter((value): value is string => typeof value === "string" && value.length > 0);
}

function optionalString(value: unknown): string | undefined {
  return value === null || value === undefined ? undefined : String(value);
}

function optionalNumber(value: unknown): number | undefined {
  return value === null || value === undefined ? undefined : Number(value);
}

function parseObject(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  try {
    const parsed = JSON.parse(String(value)) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? ( parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}

function identityProject(metadata: Record<string, unknown>): string {
  return optionalString(metadata.project) ?? "default";
}

function canonicalPresence(status: unknown): string {
  const value = optionalString(status)?.toLowerCase();
  return !value || value === "stopped" || value === "offline" ? "offline" : value;
}

interface CanonicalIdentity {
  agentId: string;
  name: string;
  tier?: string;
  owner?: string;
  metadata: Record<string, unknown>;
}

function canonicalIdentityFromRow(row: Row): CanonicalIdentity {
  const metadata = parseObject(row.metadata_json);
  return {
    agentId: String(row.agent_id),
    name: optionalString(row.name) ?? String(row.agent_id),
    tier: optionalString(row.tier),
    owner: optionalString(row.owner),
    metadata,
  };
}

async function canonicalIdentityByAgentId(
  db: Client,
  agentId: string,
): Promise<CanonicalIdentity | undefined> {
  const result = await db.execute({
    sql: `SELECT agent_id, name, owner, tier, metadata_json
          FROM identities WHERE agent_id = ? LIMIT 1`,
    args: [agentId],
  });
  const row = result.rows[0];
  return row ? canonicalIdentityFromRow(row) : undefined;
}

async function canonicalIdentity(db: Client, id: string): Promise<CanonicalIdentity | undefined> {
  const exact = await canonicalIdentityByAgentId(db, id);
  if (exact) return exact;
  const result = await db.execute({
    sql: `SELECT agent_id, name, owner, tier, metadata_json
          FROM identities WHERE name = ? ORDER BY agent_id LIMIT 2`,
    args: [id],
  });
  if (result.rows.length > 1) {
    throw new Error(`ambiguous agent name ${JSON.stringify(id)}; address it by stable agent id`);
  }
  const row = result.rows[0];
  return row ? canonicalIdentityFromRow(row) : undefined;
}

function isStamp(value: unknown): value is Provenance["stamp"] & object {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const stamp = value as Record<string, unknown>;
  return ( typeof stamp.algo === "string" && typeof stamp.signature === "string" &&
    typeof stamp.signedAt === "number"
  );
}
