// Task 4 — read-only Drizzle view tests.
//
// Drives the read-view query functions against the seeded in-memory libSQL DB
// (`seedDb()`), which is created with the SAME introspected DDL as production
// Turso and seeded from the shared read-view fixtures (`makeSeed()`), so route,
// query, and UI tests tell one consistent story. Also asserts the read-only
// invariant: no write/migration call site exists under `drizzle/` or
// `server/read/`.
import { describe, it, expect, beforeAll } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";

import { seedDb } from "@drizzle/__mocks__/seedDb";
import type { ReadDb } from "@drizzle/client";
import {
  listMembers,
  listThreads,
  threadHeader,
  threadHistory,
  dmHistory,
  searchMessages,
  listTopics,
  listRoutingRules,
  listNotifications,
  listProjects,
  agentShow,
  agentOwnerByName,
  whoamiRow,
} from "@server/read/queries";
import { entityMetadata } from "@server/read/metadata";

const __dirname = dirname(fileURLToPath(import.meta.url));

async function captureRowTransfer<T>(
  db: ReadDb,
  read: () => Promise<T>,
): Promise<{ value: T; maxRows: number; totalRows: number }> {
  const client = db.$client as unknown as {
    execute: (...args: unknown[]) => Promise<{ rows?: unknown[] }>;
  };
  const originalExecute = client.execute;
  let maxRows = 0;
  let totalRows = 0;
  client.execute = async (...args: unknown[]) => {
    const result = await originalExecute.apply(client, args);
    const rows = result.rows?.length ?? 0;
    maxRows = Math.max(maxRows, rows);
    totalRows += rows;
    return result;
  };
  try {
    return { value: await read(), maxRows, totalRows };
  } finally {
    client.execute = originalExecute;
  }
}

describe("read-view queries (seeded in-memory libSQL)", () => {
  let db: ReadDb;

  beforeAll(async () => {
    db = await seedDb();
  });

  it("listMembers returns the seeded roster (display shape)", async () => {
    const members = await listMembers(db, { includeOffline: true });
    const names = members.map((m) => m.name).sort();
    expect(names).toEqual(["ben", "blake", "dylan", "etan"]);

    const ben = members.find((m) => m.name === "ben")!;
    expect(ben.agent).toBe("claude");
    // Durable identity rides the roster row — and it is NOT a_s_<session>:
    // an agent's later sessions keep the FIRST session's a_* id, so clients
    // must read it here, never derive it (daemon rejects fabricated ids).
    expect(ben.agentId).toBe("a_ben");
    expect(ben.agentId).not.toBe(`a_${ben.sessionId}`);
    expect(ben).not.toHaveProperty("role");
    expect(ben.presence).toBe("online");
    expect(ben.currentWork).toBe("post-merge gate");

    const etan = members.find((m) => m.name === "etan")!;
    expect(etan.kind).toBe("human");
    // Daemon human rows may carry the `other` harness token, but the read view
    // must not expose them as agent rows.
    expect(etan.agent).toBeUndefined();
    expect(etan.agentId).toBeUndefined();

    // A display view-model row, NOT a contract wire type: no nested provenance. It DOES carry
    // `sessionId` now — the operator console addresses an exact daemon-owned session as
    // `/agent/<name>:<session_id>` (operator-only; agents still never see internal ids, §6.6).
    expect(typeof ben.sessionId).toBe("string");
    expect(ben.sessionId.length).toBeGreaterThan(0);
    expect(ben).not.toHaveProperty("provenance");
  });

  it("listMembers hides offline members unless includeOffline", async () => {
    const online = await listMembers(db, { includeOffline: false });
    expect(online.map((m) => m.name)).not.toContain("dylan");
    const all = await listMembers(db, { includeOffline: true });
    expect(all.map((m) => m.name)).toContain("dylan");
  });

  it("agentOwnerByName projects managed owner fields", async () => {
    const owner = await agentOwnerByName(db, "ben");
    expect(owner?.ownerName).toBe("etan");
    expect(owner?.ownerSessionId).toBe("s_etan");
  });

  it("legacy agent reads prefer an exact id and reject duplicate global names", async () => {
    const localDb = await seedDb();
    await localDb.$client.batch([
      "DROP INDEX idx_agents_name_unique",
      `INSERT INTO agents
        (agent_id, project, name, tier, created_at) VALUES
        ('a_collision','ops','id-owner','agent',10),
        ('a_alias_owner','default','a_collision','agent',11),
        ('a_duplicate_one','default','duplicate','agent',12),
        ('a_duplicate_two','ops','duplicate','agent',13)`,
    ], "write");

    await expect(agentShow(localDb, "a_collision", { project: "default" }))
      .resolves.toMatchObject({ agent: { agentId: "a_collision", name: "id-owner" } });
    await expect(agentOwnerByName(localDb, "a_collision"))
      .resolves.toMatchObject({ agentId: "a_collision", name: "id-owner" });
    await expect(agentShow(localDb, "duplicate"))
      .rejects.toThrow("ambiguous agent name");
    await expect(agentOwnerByName(localDb, "duplicate"))
      .rejects.toThrow("ambiguous agent name");
  });

  it("legacy agent owner resolution does not confuse multiple runtimes with duplicate identities", async () => {
    const localDb = await seedDb();
    await localDb.$client.batch([
      "DROP INDEX idx_sessions_name",
      `INSERT INTO sessions
        (session_id, name, agent, kind, tier, project, presence, created_at, agent_id) VALUES
        ('s_ben_replacement','ben','claude','agent','agent','nexus','online',20,'a_ben')`,
    ], "write");

    await expect(agentOwnerByName(localDb, "ben"))
      .resolves.toMatchObject({ agentId: "a_ben", name: "ben" });
  });

  it("legacy agent owner session fallback is global and rejects ambiguous display aliases", async () => {
    const localDb = await seedDb();
    await localDb.$client.batch([
      "DROP INDEX idx_sessions_name",
      `INSERT INTO agents
        (agent_id, project, name, tier, created_at) VALUES
        ('a_global_session','default','global-session','agent',30),
        ('a_ambiguous_session','default','ambiguous-session','agent',31)`,
      `INSERT INTO sessions
        (session_id, name, kind, role, tier, project, presence, created_at, agent_id) VALUES
        ('s_global_session','global-session','agent','reviewer','admin','ops','offline',30,NULL),
        ('s_ambiguous_session_default','ambiguous-session','agent','reviewer','agent','default','offline',31,NULL),
        ('s_ambiguous_session_ops','ambiguous-session','agent','operator','admin','ops','offline',32,NULL)`,
    ], "write");

    await expect(agentOwnerByName(localDb, "a_global_session"))
      .resolves.toMatchObject({
        agentId: "a_global_session",
        sessionKind: "agent",
        sessionTier: "admin",
      });
    await expect(agentOwnerByName(localDb, "a_ambiguous_session"))
      .rejects.toThrow("ambiguous session name");
  });

  it("legacy agent metadata prefers exact ids and rejects duplicate names across projects", async () => {
    const localDb = await seedDb();
    await localDb.$client.batch([
      "DROP INDEX idx_agents_name_unique",
      `INSERT INTO agents
        (agent_id, project, name, tier, metadata_json, created_at) VALUES
        ('a_metadata_alias','default','a_metadata_exact','agent','{"marker":"alias"}',10),
        ('a_metadata_exact','ops','id-owner','agent','{"marker":"id"}',11),
        ('a_metadata_dup_one','default','metadata-duplicate','agent','{}',12),
        ('a_metadata_dup_two','ops','metadata-duplicate','agent','{}',13)`,
    ], "write");

    await expect(entityMetadata(localDb, "agent", "a_metadata_exact", "default"))
      .resolves.toMatchObject({ metadata: { marker: "id" } });
    await expect(entityMetadata(localDb, "agent", "metadata-duplicate"))
      .rejects.toThrow("ambiguous agent name");
  });

  it("listMembers treats stale heartbeats as offline", async () => {
    const staleNow = 1_700_000_060_000;
    await db.$client.execute({
      sql: "UPDATE sessions SET presence = 'online', last_heartbeat = ? WHERE name = 'ben'",
      args: [staleNow - 60_000],
    });

    const online = await listMembers(db, {
      includeOffline: false,
      now: staleNow,
      heartbeatTtlMs: 30_000,
    });
    expect(online.map((m) => m.name)).not.toContain("ben");

    const all = await listMembers(db, {
      includeOffline: true,
      now: staleNow,
      heartbeatTtlMs: 30_000,
    });
    const ben = all.find((m) => m.name === "ben")!;
    expect(ben.presence).toBe("offline");
  });

  it("listMembers pushes the default live-heartbeat filter into SQL", async () => {
    const localDb = await seedDb();
    const now = 1_700_001_000_000;
    await localDb.$client.execute({
      sql:
        "WITH digits(d) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)), " +
        "nums(n) AS (SELECT a.d + 10*b.d + 100*c.d + 1000*d.d FROM digits a, digits b, digits c, digits d) " +
        "INSERT INTO sessions " +
        "(session_id, name, agent, kind, role, tier, project, current_work, presence, last_heartbeat, created_at) " +
        "SELECT 's_stale_' || n, 'stale-' || n, 'other', 'agent', NULL, 'agent', 'nexus', NULL, " +
        "'online', ?, ? FROM nums",
      args: [now - 60_000, now - 120_000],
    });

    const transfer = await captureRowTransfer(localDb, () =>
      listMembers(localDb, { includeOffline: false, now, heartbeatTtlMs: 30_000 }),
    );

    expect(transfer.value.some((member) => member.name.startsWith("stale-"))).toBe(false);
    expect(transfer.maxRows).toBeLessThan(100);
  });

  it("listThreads returns the seeded threads with members + lastAt", async () => {
    const threads = await listThreads(db);
    const names = threads.map((t) => t.name);
    // DM-kind threads are excluded from the named-thread list.
    expect(names).toContain("design");
    expect(names).toContain("ops");
    expect(names).not.toContain("dm:etan:ben");

    const design = threads.find((t) => t.name === "design")!;
    expect(design.members.sort()).toEqual(["ben", "blake", "etan"]);
    expect(typeof design.lastAt).toBe("number");
  });

  it("listThreads uses the latest thread message time for lastAt", async () => {
    const localDb = await seedDb();
    const latestMessageAt = 1_700_000_500_000;
    await localDb.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, (SELECT thread_id FROM threads WHERE name = ?), ?, ?, ?)",
      args: [
        "m_design_newest_activity",
        "ben",
        "thread",
        "design",
        "design",
        "newest design thread activity",
        "nexus",
        latestMessageAt,
      ],
    });

    const threads = await listThreads(localDb);
    const design = threads.find((t) => t.name === "design")!;
    expect(design.lastAt).toBe(latestMessageAt);
  });

  it("listThreads transfers one latest-message aggregate per listed thread", async () => {
    const localDb = await seedDb();
    await localDb.$client.execute({
      sql:
        "WITH digits(d) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)), " +
        "nums(n) AS (SELECT a.d + 10*b.d + 100*c.d + 1000*d.d FROM digits a, digits b, digits c, digits d) " +
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "SELECT 'm_volume_' || n, 'ben', 'thread', 'design', 't_design', 'volume', 'nexus', " +
        "1700001000000 + n FROM nums",
      args: [],
    });

    const transfer = await captureRowTransfer(localDb, () => listThreads(localDb));
    const design = transfer.value.find((thread) => thread.name === "design");

    expect(design?.lastAt).toBe(1_700_001_000_000 + 9_999);
    expect(transfer.maxRows).toBeLessThan(100);
    expect(transfer.totalRows).toBeLessThan(200);
  });

  it("threadHeader resolves active members through agent identity, not runtime-id coincidence", async () => {
    const localDb = await seedDb();
    await localDb.$client.execute({
      sql: "UPDATE agent_runtimes SET runtime_id = ? WHERE runtime_id = ?",
      args: ["r_ben_live", "s_ben"],
    });

    const header = await threadHeader(localDb, { thread: "design" });

    expect(header?.activeSessions).toBe(2);
  });

  it("threadHeader transfers only the latest message aggregate", async () => {
    const localDb = await seedDb();
    await localDb.$client.execute({
      sql:
        "WITH digits(d) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)), " +
        "nums(n) AS (SELECT a.d + 10*b.d + 100*c.d + 1000*d.d FROM digits a, digits b, digits c, digits d) " +
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "SELECT 'm_header_volume_' || n, 'ben', 'thread', 'design', 't_design', 'volume', 'nexus', " +
        "1700002000000 + n FROM nums",
      args: [],
    });

    const transfer = await captureRowTransfer(localDb, () =>
      threadHeader(localDb, { thread: "design" }),
    );

    expect(transfer.value?.lastAt).toBe(1_700_002_000_000 + 9_999);
    expect(transfer.maxRows).toBeLessThan(100);
  });

  it("listThreads includes the latest developer event sequence per thread", async () => {
    const localDb = await seedDb();
    await localDb.$client.execute({
      sql:
        "INSERT INTO developer_event_topics (topic, latest_seq, updated_at) " +
        "VALUES (?, ?, ?)",
      args: ["sys.message.thread.design", 17, 1_700_000_600_000],
    });

    const threads = await listThreads(localDb);
    const design = threads.find((t) => t.name === "design")!;
    const ops = threads.find((t) => t.name === "ops")!;
    expect(design.latestSeq).toBe(17);
    expect(ops.latestSeq).toBe(0);
  });

  it("listThreads degrades latestSeq to 0 when the developer_event_topics table is missing", async () => {
    // The gateway is a read-only consumer and can run ahead of the live daemon's schema
    // (2026-07-09 incident: channels 500'd for the whole UI because this enrichment table
    // did not exist yet). Missing enrichment must never fail the channel list.
    const localDb = await seedDb();
    await localDb.$client.execute({ sql: "DROP TABLE developer_event_topics", args: [] });

    const threads = await listThreads(localDb);
    expect(threads.length).toBeGreaterThan(0);
    for (const t of threads) expect(t.latestSeq).toBe(0);
  });

  it("threadHistory returns the seeded messages in chronological order", async () => {
    const history = await threadHistory(db, { thread: "design", limit: 50 });
    expect(history.length).toBe(2);
    // ascending by time
    expect(history[0]!.from).toBe("ben");
    expect(history[0]!.body).toBe("hi team, kicking off the gateway");
    expect(history[1]!.from).toBe("blake");
    expect(history[0]!.when).toBeLessThan(history[1]!.when);
  });

  it("threadHistory honors the `before` cursor", async () => {
    const all = await threadHistory(db, { thread: "design", limit: 50 });
    const cutoff = all[1]!.when; // exclude the last message
    const before = await threadHistory(db, {
      thread: "design",
      limit: 50,
      before: cutoff,
    });
    expect(before.length).toBe(1);
    expect(before[0]!.from).toBe("ben");
  });

  it("threadHistory resumes after an exact created-at and rowid cursor", async () => {
    const localDb = await seedDb();
    const createdAt = 1_700_003_000_000;
    await localDb.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "VALUES (?, ?, 'thread', 'design', 't_design', ?, 'nexus', ?), " +
        "(?, ?, 'thread', 'design', 't_design', ?, 'nexus', ?)",
      args: [
        "m_cursor_first",
        "ben",
        "first at same millisecond",
        createdAt,
        "m_cursor_second",
        "blake",
        "second at same millisecond",
        createdAt,
      ],
    });
    const first = await localDb.$client.execute({
      sql: "SELECT rowid FROM messages WHERE message_id = ?",
      args: ["m_cursor_first"],
    });
    const afterRowid = Number(first.rows[0]?.rowid ?? 0);

    const page = await threadHistory(localDb, {
      thread: "design",
      limit: 50,
      after: createdAt,
      afterRowid,
    });

    expect(page.map((row) => row.messageId)).toEqual(["m_cursor_second"]);
    expect(page[0]?.cursor).toEqual({ createdAt, rowid: expect.any(Number) });
  });

  it("archived threads are hidden from thread list and history", async () => {
    await db.$client.execute({
      sql: "UPDATE threads SET archived_at = ? WHERE name = ?",
      args: [1_700_000_100_000, "ops"],
    });

    const threads = await listThreads(db);
    expect(threads.map((t) => t.name)).not.toContain("ops");
    await expect(threadHistory(db, { thread: "ops", limit: 50 })).resolves.toEqual([]);
  });

  it("dmHistory returns the conversation with a partner", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_dm_read",
        "ben",
        "dm",
        "etan",
        null,
        "dm read-view history",
        "nexus",
        1_700_000_003_000,
      ],
    });

    const dm = await dmHistory(db, { with: "ben", me: "etan", project: "nexus", limit: 50 });
    expect(dm).toContainEqual(
      expect.objectContaining({
        messageId: "m_dm_read",
        from: "ben",
        body: "dm read-view history",
      }),
    );
  });

  it("searchMessages returns a seeded FTS-style hit", async () => {
    const hits = await searchMessages(db, { query: "hi" });
    expect(hits.length).toBeGreaterThan(0);
    const hit = hits[0]!;
    expect(hit.from).toBe("ben");
    expect(hit.snippet).toContain("hi");
    expect(typeof hit.when).toBe("number");
    expect(typeof hit.messageId).toBe("string");
  });

  it("searchMessages can scope to a thread", async () => {
    const hits = await searchMessages(db, { query: "ack", thread: "design" });
    expect(hits.some((h) => h.from === "blake")).toBe(true);
  });

  it("listTopics returns the seeded topics with subscriber counts", async () => {
    const topics = await listTopics(db);
    const byName = Object.fromEntries(topics.map((t) => [t.topic, t.subscribers]));
    expect(byName["builds"]).toBe(2);
    expect(byName["alerts"]).toBe(1);
  });

  it("listTopics aggregates subscriber counts in SQL", async () => {
    const localDb = await seedDb();
    await localDb.$client.execute({
      sql:
        "WITH digits(d) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)), " +
        "nums(n) AS (SELECT a.d + 10*b.d + 100*c.d + 1000*d.d FROM digits a, digits b, digits c, digits d) " +
        "INSERT INTO subscriptions (topic, subscriber_session, sub_group, cursor, subscribed_at) " +
        "SELECT 'builds', 's_volume_' || n, NULL, 0, 1700000000000 FROM nums",
      args: [],
    });

    const transfer = await captureRowTransfer(localDb, () => listTopics(localDb));
    const builds = transfer.value.find((topic) => topic.topic === "builds");

    expect(builds?.subscribers).toBe(10_002);
    expect(transfer.maxRows).toBeLessThan(100);
  });

  it("listRoutingRules returns the seeded standing rule", async () => {
    const rules = await listRoutingRules(db);
    expect(rules).toContainEqual({ source: "ci", topic: "builds", to: "ben" });
  });

  it("listNotifications returns the seeded notification audit row", async () => {
    const notifs = await listNotifications(db);
    expect(notifs.length).toBeGreaterThan(0);
    const n = notifs[0]!;
    expect(n.source).toBe("ci");
    expect(Array.isArray(n.routedTo)).toBe(true);
    expect(n.routedTo).toContain("ben");
  });

  it("listNotifications projects delivered source-push topic messages", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, topic, summary, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_source_push",
        "github-ci",
        "topic",
        "builds",
        null,
        "builds",
        "build passed",
        "source push body",
        "p_nexus",
        1_700_000_010_000,
      ],
    });
    await db.$client.execute({
      sql:
        "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state) " +
        "VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
      args: [
        "if_source_push_ben",
        "m_source_push",
        "s_ben",
        "pending",
        "if_source_push_blake",
        "m_source_push",
        "s_blake",
        "pending",
      ],
    });

    const notifs = await listNotifications(db);
    expect(notifs).toContainEqual(
      expect.objectContaining({
        notifId: "m_source_push",
        source: "github-ci",
        topic: "builds",
        hmacOk: true,
        routedTo: expect.arrayContaining(["ben", "blake"]),
        when: 1_700_000_010_000,
      }),
    );
  });

  it("listNotifications bounds both sources with one additive cursor page", async () => {
    const localDb = await seedDb();
    await localDb.$client.execute({
      sql:
        "WITH RECURSIVE nums(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM nums WHERE n < 299) " +
        "INSERT INTO notifications (notif_id, source, topic, hmac_ok, payload, routed_to, created_at) " +
        "SELECT 'n_page_' || n, 'ci', 'builds', 1, '{}', 'ben', 1700010000000 + n FROM nums",
      args: [],
    });
    await localDb.$client.execute({
      sql:
        "WITH RECURSIVE nums(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM nums WHERE n < 299) " +
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, topic, body, project, created_at) " +
        "SELECT 'm_page_' || n, 'ci', 'topic', 'builds', NULL, 'builds', 'push', 'nexus', " +
        "1700010000000 + n FROM nums",
      args: [],
    });

    const before = 1_700_010_000_250;
    const transfer = await captureRowTransfer(localDb, () =>
      listNotifications(localDb, { limit: 25, before }),
    );

    expect(transfer.value).toHaveLength(25);
    expect(transfer.value.every((row) => row.when < before)).toBe(true);
    expect(transfer.value.map((row) => row.when)).toEqual(
      [...transfer.value].map((row) => row.when).sort((a, b) => b - a),
    );
    expect(transfer.maxRows).toBeLessThanOrEqual(25);
    expect(transfer.totalRows).toBeLessThanOrEqual(50);
  });

  it("listProjects returns the project scope label from sessions", async () => {
    const projects = await listProjects(db);
    expect(projects.some((p) => p.name === "nexus")).toBe(true);
    const nexus = projects.find((p) => p.name === "nexus")!;
    expect(nexus).toEqual({ projectId: "nexus", name: "nexus" });
  });

  it("whoamiRow resolves a member by name into the caller identity", async () => {
    const me = await whoamiRow(db, "etan");
    expect(me).toBeDefined();
    expect(me!.name).toBe("etan");
    expect(me!.tier).toBe("admin");
    expect(me).not.toHaveProperty("project");
    expect(me!.presence).toBe("online");
  });

  it("whoamiRow reports a stale caller as offline", async () => {
    const staleNow = 1_700_000_060_000;
    await db.$client.execute({
      sql: "UPDATE sessions SET presence = 'online', last_heartbeat = ? WHERE name = 'etan'",
      args: [staleNow - 60_000],
    });

    const me = await whoamiRow(db, "etan", {
      now: staleNow,
      heartbeatTtlMs: 30_000,
    });
    expect(me).toBeDefined();
    expect(me!.presence).toBe("offline");
  });

  it("whoamiRow rejects an ambiguous global display alias", async () => {
    const localDb = await seedDb();
    await localDb.$client.batch([
      "DROP INDEX idx_sessions_name",
      `INSERT INTO sessions
        (session_id, name, kind, tier, project, presence, created_at) VALUES
        ('s_whoami_default','ambiguous-human','human','admin','default','online',40),
        ('s_whoami_ops','ambiguous-human','human','admin','ops','online',41)`,
    ], "write");

    await expect(whoamiRow(localDb, "ambiguous-human"))
      .rejects.toThrow("ambiguous session name");
  });
});

describe("read-only invariant (no write/migration call sites)", () => {
  // Walk drizzle/ and server/read/ source; assert NONE call a write or migrate.
  // The gateway never writes the DB — the daemon is the sole writer.
  const roots = [
    resolve(__dirname, "..", "..", "drizzle"),
    resolve(__dirname, "..", "read"),
  ];

  function collectSourceFiles(dir: string): string[] {
    const out: string[] = [];
    for (const entry of readdirSync(dir)) {
      const full = join(dir, entry);
      if (statSync(full).isDirectory()) {
        out.push(...collectSourceFiles(full));
        continue;
      }
      if (!/\.[cm]?tsx?$/.test(entry)) continue;
      // The seed lives in test/mock code (__mocks__) and legitimately INSERTs into
      // the throwaway in-memory DB; the read PATH (client/queries/schema) must not.
      if (full.includes("__mocks__")) continue;
      // Don't scan the test files themselves (they contain the regexes below).
      if (/\.(test|spec)\.[cm]?tsx?$/.test(entry)) continue;
      out.push(full);
    }
    return out;
  }

  const writeCall = /\.(insert|update|delete)\s*\(/;
  const migrateCall = /\bmigrate\s*\(/;

  it("no .insert(/.update(/.delete( in drizzle/ or server/read/ (excluding mocks)", () => {
    const files = roots.flatMap(collectSourceFiles);
    expect(files.length).toBeGreaterThan(0);
    const offenders = files.filter((f) => writeCall.test(readFileSync(f, "utf8")));
    expect(offenders).toEqual([]);
  });

  it("no migrate( call site in drizzle/ or server/read/ (excluding mocks)", () => {
    const files = roots.flatMap(collectSourceFiles);
    const offenders = files.filter((f) =>
      migrateCall.test(readFileSync(f, "utf8")),
    );
    expect(offenders).toEqual([]);
  });
});

describe("read-view project scoping", () => {
  let db: ReadDb;

  beforeAll(async () => {
    db = await seedDb();
  });

  it("listProjects derives projects from active sessions, keyed by name", async () => {
    const projects = await listProjects(db);
    const nexus = projects.find((p) => p.name === "nexus");
    expect(nexus).toBeDefined();
    // projectId IS the name — the scope key the read-view filters on.
    expect(nexus!.projectId).toBe("nexus");
  });

  it("listMembers scopes to the given project (empty for an unknown one)", async () => {
    const inProject = await listMembers(db, { includeOffline: true, project: "nexus" });
    expect(inProject.map((m) => m.name).sort()).toEqual(["ben", "blake", "dylan", "etan"]);

    const elsewhere = await listMembers(db, {
      includeOffline: true,
      project: "does-not-exist",
    });
    expect(elsewhere).toEqual([]);
  });

  it("listThreads ignores deprecated project scope", async () => {
    const inProject = await listThreads(db, { project: "nexus" });
    expect(inProject.map((t) => t.name).sort()).toEqual(["design", "ops"]);

    const elsewhere = await listThreads(db, { project: "does-not-exist" });
    expect(elsewhere.map((t) => t.name).sort()).toEqual(["design", "ops"]);
  });

  it("omitting the project returns the unscoped (global) rows", async () => {
    const all = await listMembers(db, { includeOffline: true });
    expect(all.length).toBeGreaterThan(0);
  });
});
