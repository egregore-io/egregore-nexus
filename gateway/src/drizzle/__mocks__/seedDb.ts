// In-memory seeded read DB for tests/dev — stands in for the shared Turso until
// it exists. It is created with the SAME introspected DDL as production (the
// `drizzle-kit pull` mirror in `schema.gen.ts`, expressed here as executable
// SQLite so the in-memory DB is "introspected identically to Turso") and seeded
// with rows from the shared read-view fixtures. That alignment is the whole
// point: route, query, and UI tests tell ONE consistent story (same members,
// threads, topics, messages, routing rule, notification).
//
// IMPORTANT: the INSERTs below live only in this `__mocks__` seed (test/mock
// code). The app's read path (`client.ts`, `queries.ts`) never writes — the
// daemon is the sole writer. The read.test.ts read-only guard explicitly skips
// `__mocks__` for exactly this reason.
import { createClient } from "@libsql/client";

import { readDb, type ReadDb } from "../client";
import { makeSeed, PROJECT_ID } from "@drizzle/__mocks__/fixtures";

// The DDL is the executable form of `schema.gen.ts` (== core/migrations/0001_init.sql,
// plus the display-only `routing_rules` table the read view projects). Applied to
// a fresh `:memory:` libSQL so the seeded DB is shaped exactly like introspected
// Turso.
const DDL = `
CREATE TABLE IF NOT EXISTS sessions (
  session_id         TEXT PRIMARY KEY,
  name               TEXT,
  agent              TEXT,
  kind               TEXT,
  role               TEXT,
  tier               TEXT DEFAULT 'agent',
  harness_session_id TEXT,
  client_key         TEXT,
  cwd                TEXT,
  project            TEXT,
  current_work       TEXT,
  presence           TEXT,
  paused             INTEGER DEFAULT 0,
  paused_by          TEXT,
  callback_url       TEXT,
  last_heartbeat     INTEGER,
  created_at         INTEGER,
  metadata_json      TEXT,
  agent_id           TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_name       ON sessions(name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_client_key ON sessions(client_key);
CREATE        INDEX IF NOT EXISTS idx_sessions_harness    ON sessions(harness_session_id);
CREATE        INDEX IF NOT EXISTS idx_sessions_project    ON sessions(project);

CREATE TABLE IF NOT EXISTS agents (
  agent_id        TEXT PRIMARY KEY,
  project         TEXT NOT NULL,
  name            TEXT,
  default_harness TEXT,
  role            TEXT,
  tier            TEXT DEFAULT 'agent',
  disabled_at     INTEGER,
  created_at      INTEGER NOT NULL,
  metadata_json   TEXT,
  owner_name      TEXT,
  owner_project   TEXT,
  owner_session_id TEXT,
  owner_agent_id  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_name_unique  ON agents(name) WHERE name IS NOT NULL;
CREATE        INDEX IF NOT EXISTS idx_agents_project_name ON agents(project, name) WHERE name IS NOT NULL;

CREATE TABLE IF NOT EXISTS agent_credentials (
  credential_id TEXT PRIMARY KEY,
  agent_id      TEXT NOT NULL,
  secret_hash   TEXT NOT NULL,
  purpose       TEXT,
  label         TEXT,
  scopes_json   TEXT NOT NULL,
  metadata_json TEXT,
  revoked_at    INTEGER,
  last_used_at  INTEGER,
  created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_credentials_agent ON agent_credentials(agent_id);

CREATE TABLE IF NOT EXISTS agent_runtimes (
  runtime_id     TEXT PRIMARY KEY,
  agent_id       TEXT NOT NULL,
  harness        TEXT NOT NULL,
  cwd            TEXT,
  transport      TEXT,
  presence       TEXT,
  active         INTEGER NOT NULL DEFAULT 1,
  started_at     INTEGER NOT NULL,
  stopped_at     INTEGER,
  last_heartbeat INTEGER
);
CREATE INDEX IF NOT EXISTS idx_agent_runtimes_agent ON agent_runtimes(agent_id, active);

CREATE TABLE IF NOT EXISTS messages (
  message_id TEXT PRIMARY KEY,
  from_name  TEXT,
  kind       TEXT,
  to_name    TEXT,
  thread_id  TEXT,
  topic      TEXT,
  summary    TEXT,
  body       TEXT,
  provenance TEXT,
  project    TEXT,
  created_at INTEGER,
  from_agent_id TEXT,
  to_agent_id TEXT,
  metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_to_created ON messages(to_name, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_thread     ON messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_from       ON messages(from_name);
CREATE INDEX IF NOT EXISTS idx_messages_from_agent_created ON messages(from_agent_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_to_agent_created ON messages(to_agent_id, created_at);

CREATE TABLE IF NOT EXISTS in_flight (
  in_flight_id      TEXT PRIMARY KEY,
  message_id        TEXT,
  recipient_session TEXT,
  state             TEXT DEFAULT 'pending',
  delivered_at      INTEGER,
  acked_at          INTEGER
);
CREATE        INDEX IF NOT EXISTS idx_in_flight_recipient_state ON in_flight(recipient_session, state);
CREATE UNIQUE INDEX IF NOT EXISTS idx_in_flight_msg_recipient   ON in_flight(message_id, recipient_session);

CREATE TABLE IF NOT EXISTS threads (
  thread_id  TEXT PRIMARY KEY,
  name       TEXT UNIQUE,
  project    TEXT,
  topic      TEXT,
  description TEXT,
  created_by TEXT,
  created_at INTEGER,
  archived_at INTEGER,
  metadata_json TEXT
);
CREATE TABLE IF NOT EXISTS thread_members (
  thread_id    TEXT,
  session_name TEXT,
  joined_at    INTEGER,
  PRIMARY KEY (thread_id, session_name)
);

CREATE TABLE IF NOT EXISTS topics (
  topic      TEXT PRIMARY KEY,
  project    TEXT,
  created_at INTEGER
);
CREATE TABLE IF NOT EXISTS subscriptions (
  topic              TEXT,
  subscriber_session TEXT,
  sub_group          TEXT,
  cursor             INTEGER DEFAULT 0,
  subscribed_at      INTEGER,
  PRIMARY KEY (topic, subscriber_session)
);

CREATE TABLE IF NOT EXISTS notifications (
  notif_id   TEXT PRIMARY KEY,
  source     TEXT,
  topic      TEXT,
  hmac_ok    INTEGER,
  payload    TEXT,
  routed_to  TEXT,
  created_at INTEGER
);

CREATE TABLE IF NOT EXISTS developer_event_topics (
  topic      TEXT PRIMARY KEY,
  latest_seq INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS developer_events (
  topic        TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  kind         TEXT NOT NULL,
  message_id   TEXT,
  thread_name  TEXT,
  dm_name      TEXT,
  from_name    TEXT,
  agent_name   TEXT,
  session_id   TEXT,
  lifecycle    TEXT,
  current_work TEXT,
  data_json    TEXT,
  created_at   INTEGER NOT NULL,
  PRIMARY KEY (topic, seq)
);
CREATE INDEX IF NOT EXISTS idx_developer_events_message
  ON developer_events(message_id);

CREATE TABLE IF NOT EXISTS command_intents (
  command_id        TEXT PRIMARY KEY,
  kind              TEXT NOT NULL,
  status            TEXT NOT NULL,
  project           TEXT NOT NULL,
  caller_name       TEXT NOT NULL,
  caller_session_id TEXT,
  caller_agent_id   TEXT,
  caller_runtime_id TEXT,
  caller_client_key TEXT,
  caller_kind       TEXT,
  caller_tier       TEXT,
  idempotency_key   TEXT,
  request_json      TEXT NOT NULL,
  result_json       TEXT,
  error_json        TEXT,
  attempts          INTEGER NOT NULL DEFAULT 0,
  created_at        INTEGER NOT NULL,
  claimed_at        INTEGER,
  lease_until       INTEGER,
  completed_at      INTEGER
);
CREATE INDEX IF NOT EXISTS idx_command_intents_pending
  ON command_intents(status, created_at);
CREATE INDEX IF NOT EXISTS idx_command_intents_lease
  ON command_intents(status, lease_until);
CREATE UNIQUE INDEX IF NOT EXISTS idx_command_intents_idempotency_scope
  ON command_intents(project, kind, COALESCE(caller_client_key, caller_session_id, caller_name), idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS sources (
  name          TEXT PRIMARY KEY,
  token         TEXT NOT NULL,
  topic         TEXT NOT NULL,
  enabled       INTEGER NOT NULL DEFAULT 1,
  created_at    INTEGER NOT NULL,
  last_fired_at INTEGER
);

-- Display-only projection (not in 0001_init.sql); the read view's listRoutingRules.
CREATE TABLE IF NOT EXISTS routing_rules (
  rule_id    TEXT PRIMARY KEY,
  source     TEXT,
  topic      TEXT,
  to_name    TEXT,
  project    TEXT,
  created_at INTEGER
);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  summary,
  body,
  content='messages',
  content_rowid='rowid'
);
`;

/** Map a thread NAME to the resolved internal id the daemon would assign. */
function threadIdFor(name: string): string {
  // DM threads keep their literal "dm:a:b" name as id; named threads get "t_<name>".
  return name.startsWith("dm:") ? name : `t_${name}`;
}

/** A thread is a DM (not a named channel) iff its name uses the `dm:` convention. */
function isDmThread(name: string): boolean {
  return name.startsWith("dm:");
}

function requireSeedName(name: string | undefined, context: string): string {
  if (!name) throw new Error(`${context} name is required for named gateway fixtures`);
  return name;
}

/**
 * Build a fresh in-memory libSQL DB, apply the introspected DDL, seed it to match
 * `makeSeed()`, and return the read-only Drizzle handle. Each call is isolated
 * (a new `:memory:` connection), so tests never share state.
 */
export async function seedDb(): Promise<ReadDb> {
  const client = createClient({ url: ":memory:" });
  await client.executeMultiple(DDL);

  const seed = makeSeed();
  const whoamiName = seed.whoami.name;
  if (!whoamiName) {
    throw new Error("seed whoami.name is required for named gateway fixtures");
  }
  const now = seed.project.createdAt;
  const heartbeatNow = Date.now();

  // ── sessions (members) ──────────────────────────────────────────────────────
  // The whoami fixture carries the admin tier + session id for the owner; the
  // other members are agents. Tier defaults to 'agent' unless this row is whoami.
  for (const m of seed.members) {
    const memberName = requireSeedName(m.name, "member");
    const isMe = memberName === whoamiName;
    await client.execute({
      sql:
        "INSERT INTO sessions " +
        "(session_id, name, agent, kind, role, tier, project, current_work, presence, last_heartbeat, created_at, agent_id) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        isMe ? seed.whoami.sessionId : `s_${memberName}`,
        memberName,
        isMe ? "other" : (m.agent ?? null),
        isMe ? "human" : (m.agent ? "agent" : "app"),
        m.role ?? null,
        isMe ? seed.whoami.tier : "agent",
        seed.project.name,
        m.currentWork ?? null,
        m.presence,
        m.presence === "offline" ? null : heartbeatNow,
        now,
        // Durable identity (mirrors the agents rows below). Deliberately NOT
        // a_s_<session>: an agentId is never derivable from a sessionId.
        !isMe && m.agent ? `a_${memberName}` : null,
      ],
    });
  }

  // ── durable agents + runtimes ───────────────────────────────────────────────
  for (const m of seed.members.filter((member) => member.agent)) {
    const memberName = requireSeedName(m.name, "agent member");
    await client.execute({
      sql:
        "INSERT INTO agents " +
        "(agent_id, project, name, default_harness, role, tier, disabled_at, created_at, owner_name, owner_project, owner_session_id, owner_agent_id) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        `a_${memberName}`,
        seed.project.name,
        memberName,
        m.agent ?? null,
        m.role ?? null,
        "agent",
        null,
        now,
        whoamiName,
        seed.project.name,
        seed.whoami.sessionId,
        null,
      ],
    });
    await client.execute({
      sql:
        "INSERT INTO agent_runtimes " +
        "(runtime_id, agent_id, harness, cwd, transport, presence, active, started_at, stopped_at, last_heartbeat) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        `s_${memberName}`,
        `a_${memberName}`,
        m.agent ?? "other",
        `/tmp/${memberName}`,
        "mcp",
        m.presence,
        m.presence === "offline" ? 0 : 1,
        now,
        m.presence === "offline" ? now + 5_000 : null,
        m.presence === "offline" ? null : heartbeatNow,
      ],
    });
  }
  await client.execute({
    sql:
      "INSERT INTO agent_runtimes " +
      "(runtime_id, agent_id, harness, cwd, transport, presence, active, started_at, stopped_at, last_heartbeat) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    args: [
      "s_ben_old",
      "a_ben",
      "claude",
      "/tmp/ben-old",
      "mcp",
      "offline",
      0,
      now - 10_000,
      now - 5_000,
      null,
    ],
  });
  await client.execute({
    sql:
      "INSERT INTO agent_credentials " +
      "(credential_id, agent_id, secret_hash, purpose, label, scopes_json, metadata_json, revoked_at, last_used_at, created_at) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    args: [
      "cred_ben_laptop",
      "a_ben",
      "hash_ben_laptop",
      "runtime",
      "ben laptop",
      JSON.stringify(["runtime:register"]),
      null,
      null,
      null,
      now,
    ],
  });

  // ── threads + thread_members ────────────────────────────────────────────────
  // Named threads come from `seed.threads`; the DM thread is only in
  // `threadMembers` (keyed "dm:erin:ben"). Seed every entry in threadMembers so
  // the DM thread exists as a row too (kind inferred from the `dm:` name).
  for (const [name, names] of Object.entries(seed.threadMembers)) {
    const threadId = threadIdFor(name);
    const summary = seed.threads.find((t) => t.name === name);
    await client.execute({
      sql:
        "INSERT INTO threads " +
        "(thread_id, name, project, topic, description, created_by, created_at, archived_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
      args: [
        threadId,
        name,
        seed.project.name,
        summary?.topic ?? null,
        summary?.description ?? null,
        seed.project.createdBy,
        now,
      ],
    });
    for (const sessionName of names) {
      await client.execute({
        sql: "INSERT INTO thread_members (thread_id, session_name, joined_at) VALUES (?, ?, ?)",
        args: [threadId, sessionName, summary?.lastAt ?? now],
      });
    }
  }

  // ── messages (+ FTS mirror) ─────────────────────────────────────────────────
  for (const msg of seed.messages) {
    const res = await client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, topic, summary, body, provenance, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        msg.id,
        msg.from,
        // contract Message.scope ('dm'|'thread'|'topic') maps to messages.kind here;
        // a notification-kind message is seeded via notifications below.
        msg.scope,
        // to_name = the thread NAME for thread scope (resolved from thread_id).
        msg.thread ? nameForThreadId(seed, msg.thread) : (msg.topic ?? null),
        msg.thread ?? null,
        msg.topic ?? null,
        msg.summary ?? null,
        msg.body,
        JSON.stringify(msg.provenance),
        msg.project,
        msg.createdAt,
      ],
    });
    // Keep the FTS5 external-content index in sync (content_rowid = messages.rowid).
    await client.execute({
      sql: "INSERT INTO messages_fts (rowid, summary, body) VALUES (?, ?, ?)",
      args: [Number(res.lastInsertRowid), msg.summary ?? "", msg.body],
    });
  }

  // ── topics + subscriptions ──────────────────────────────────────────────────
  for (const t of seed.topics) {
    await client.execute({
      sql: "INSERT INTO topics (topic, project, created_at) VALUES (?, ?, ?)",
      args: [t.topic, seed.project.name, now],
    });
    // Seed `subscribers` distinct subscription rows so the count query agrees.
    for (let i = 0; i < t.subscribers; i++) {
      await client.execute({
        sql: "INSERT INTO subscriptions (topic, subscriber_session, sub_group, cursor, subscribed_at) VALUES (?, ?, ?, ?, ?)",
        args: [t.topic, `s_sub_${t.topic}_${i}`, null, 0, now],
      });
    }
  }

  // ── sources ────────────────────────────────────────────────────────────────
  for (const s of seed.sources) {
    await client.execute({
      sql: "INSERT INTO sources (name, token, topic, enabled, created_at, last_fired_at) VALUES (?, ?, ?, ?, ?, ?)",
      args: [
        s.name,
        `src_${s.name}token`,
        s.topic,
        s.enabled ? 1 : 0,
        s.createdAt,
        s.lastFiredAt ?? null,
      ],
    });
  }

  // ── routing_rules ───────────────────────────────────────────────────────────
  let ruleN = 0;
  for (const r of seed.routingRules) {
    await client.execute({
      sql: "INSERT INTO routing_rules (rule_id, source, topic, to_name, project, created_at) VALUES (?, ?, ?, ?, ?, ?)",
      args: [
        `rr_${ruleN++}`,
        r.source ?? null,
        r.topic ?? null,
        r.to,
        seed.project.name,
        now,
      ],
    });
  }

  // ── notifications (audit) ───────────────────────────────────────────────────
  // One seeded notification consistent with the routing rule (source ci → ben).
  await client.execute({
    sql: "INSERT INTO notifications (notif_id, source, topic, hmac_ok, payload, routed_to, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
    args: [
      "n_seed_1",
      "ci",
      "builds",
      1,
      JSON.stringify({ status: "green" }),
      "ben",
      now + 3_000,
    ],
  });

  return readDb(client);
}

/** Reverse `threadIdFor` using the seed's thread set (id → display name). */
function nameForThreadId(
  seed: ReturnType<typeof makeSeed>,
  threadId: string,
): string {
  for (const name of Object.keys(seed.threadMembers)) {
    if (threadIdFor(name) === threadId) return name;
  }
  // Fall back to stripping the conventional "t_" prefix.
  return threadId.startsWith("t_") ? threadId.slice(2) : threadId;
}

export { PROJECT_ID, isDmThread, threadIdFor };
