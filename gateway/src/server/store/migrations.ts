import type { Client, InStatement } from "@libsql/client";

import { GATEWAY_CANONICAL_TABLES } from "./schema";

export const CURRENT_GATEWAY_SCHEMA_VERSION = 1;
export const CURRENT_GATEWAY_SCHEMA_NAME = "v0.1.0_baseline";

const INITIAL_SCHEMA: InStatement[] = [
  `CREATE TABLE IF NOT EXISTS projection_events (
    event_id TEXT PRIMARY KEY,
    daemon_epoch TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_projection_epoch_seq
    ON projection_events(daemon_epoch, seq)`,
  `CREATE TABLE IF NOT EXISTS projection_cursors (
    source TEXT PRIMARY KEY,
    daemon_epoch TEXT NOT NULL,
    through_seq INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS projection_gaps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    daemon_epoch TEXT NOT NULL,
    after_seq INTEGER,
    through_seq INTEGER,
    reason TEXT NOT NULL,
    recorded_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS gateway_ingress (
    idempotency_key TEXT PRIMARY KEY,
    command_id TEXT,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT,
    error_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS identities (
    agent_id TEXT PRIMARY KEY,
    name TEXT,
    owner TEXT,
    role TEXT,
    tier TEXT,
    metadata_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS runtime_descriptors (
    runtime_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    session_id TEXT,
    harness TEXT NOT NULL,
    mode TEXT NOT NULL,
    backend TEXT,
    cwd TEXT,
    native_resume_key TEXT,
    status TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_runtime_agent ON runtime_descriptors(agent_id)`,
  `CREATE TABLE IF NOT EXISTS threads (
    thread_id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    archived_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS thread_members (
    thread_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    joined_at INTEGER NOT NULL,
    left_at INTEGER,
    PRIMARY KEY(thread_id, agent_id)
  )`,
  `CREATE TABLE IF NOT EXISTS topics (
    topic TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS topic_subscriptions (
    topic TEXT NOT NULL,
    agent_id TEXT,
    group_name TEXT,
    cursor TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(topic, agent_id, group_name)
  )`,
  `CREATE TABLE IF NOT EXISTS bus_messages (
    message_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    from_name TEXT,
    from_agent_id TEXT,
    to_name TEXT,
    to_agent_id TEXT,
    thread_id TEXT,
    topic TEXT,
    summary TEXT,
    body TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_bus_messages_thread
    ON bus_messages(thread_id, created_at, message_id)`,
  `CREATE INDEX IF NOT EXISTS idx_bus_messages_recipient
    ON bus_messages(to_agent_id, created_at, message_id)`,
  `CREATE INDEX IF NOT EXISTS idx_bus_messages_topic
    ON bus_messages(topic, created_at, message_id)`,
  `CREATE TABLE IF NOT EXISTS delivery_outcomes (
    message_id TEXT NOT NULL,
    recipient_agent_id TEXT NOT NULL,
    recipient_session_id TEXT,
    state TEXT NOT NULL,
    error_json TEXT,
    attempted_at INTEGER,
    settled_at INTEGER,
    PRIMARY KEY(message_id, recipient_agent_id)
  )`,
  `CREATE TABLE IF NOT EXISTS notifications (
    notification_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    source TEXT,
    target_json TEXT NOT NULL,
    summary TEXT,
    body TEXT NOT NULL,
    created_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS human_user (
    name_key TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    client_key TEXT NOT NULL UNIQUE,
    project TEXT NOT NULL,
    daemon_session_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS human_session (
    cookie_token TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    client_key TEXT NOT NULL,
    project TEXT NOT NULL,
    daemon_session_id TEXT NOT NULL,
    created_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS rest_bearer_token (
    token_id TEXT PRIMARY KEY,
    family_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_project TEXT NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_tier TEXT NOT NULL,
    scopes_json TEXT NOT NULL,
    access_hash TEXT NOT NULL UNIQUE,
    refresh_hash TEXT NOT NULL UNIQUE,
    expires_at INTEGER NOT NULL,
    refresh_expires_at INTEGER NOT NULL,
    revoked_at INTEGER,
    last_used_at INTEGER,
    created_at INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_rest_bearer_access ON rest_bearer_token(access_hash)`,
  `CREATE INDEX IF NOT EXISTS idx_rest_bearer_refresh ON rest_bearer_token(refresh_hash)`,
  `CREATE INDEX IF NOT EXISTS idx_rest_bearer_family ON rest_bearer_token(family_id)`,
  `CREATE TABLE IF NOT EXISTS logs (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    level TEXT NOT NULL,
    scope TEXT NOT NULL,
    conversation_id TEXT,
    message TEXT NOT NULL,
    data TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_logs_seq ON logs(seq)`,
  `CREATE TABLE IF NOT EXISTS rendered_conversations (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    title TEXT,
    updated_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS rendered_messages (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    role TEXT NOT NULL,
    author TEXT NOT NULL,
    content TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS idx_rendered_messages_conv
    ON rendered_messages(conversation_id, created_at)`,
];

const PROJECTION_QUARANTINE_SCHEMA: InStatement[] = [
  `CREATE TABLE IF NOT EXISTS projection_quarantine (
    event_id TEXT PRIMARY KEY,
    daemon_epoch TEXT,
    seq INTEGER,
    frame_json TEXT NOT NULL,
    error TEXT NOT NULL,
    quarantined_at INTEGER NOT NULL
  )`,
];

const REQUIRED_INDEXES = [
  "idx_projection_epoch_seq",
  "idx_runtime_agent",
  "idx_bus_messages_thread",
  "idx_bus_messages_recipient",
  "idx_bus_messages_topic",
  "idx_rest_bearer_access",
  "idx_rest_bearer_refresh",
  "idx_rest_bearer_family",
  "idx_logs_seq",
  "idx_rendered_messages_conv",
] as const;

/** Bootstrap or validate the single local-only Gateway v0.1.0 schema baseline. */
export async function migrateGatewayStore(db: Client): Promise<void> {
  if (!(await tableExists(db, "gateway_schema_migrations"))) {
    if (!(await userSchemaIsEmpty(db))) {
      throw new Error(
        "unrecognized non-empty Gateway database; refusing to modify it. Back it up and start with a fresh v0.1.0 Gateway store",
      );
    }
    await db.batch(
      [
        `CREATE TABLE gateway_schema_migrations (
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at INTEGER NOT NULL
        )`,
        ...INITIAL_SCHEMA,
        ...PROJECTION_QUARANTINE_SCHEMA,
        {
          sql: `INSERT INTO gateway_schema_migrations (version, name, applied_at)
                VALUES (?, ?, ?)`,
          args: [
            CURRENT_GATEWAY_SCHEMA_VERSION,
            CURRENT_GATEWAY_SCHEMA_NAME,
            Date.now(),
          ],
        },
      ],
      "write",
    );
  } else {
    await validateBaselineMarker(db);
  }

  await validateRequiredObjects(db);
}

async function validateBaselineMarker(db: Client): Promise<void> {
  const rows = await db.execute(
    "SELECT version, name FROM gateway_schema_migrations ORDER BY version",
  );
  if (rows.rows.length !== 1) {
    throw unsupportedSchema("pre-release migration ladder");
  }
  const marker = rows.rows[0]!;
  const version = Number(marker.version);
  const name = String(marker.name);
  if (
    version !== CURRENT_GATEWAY_SCHEMA_VERSION ||
    name !== CURRENT_GATEWAY_SCHEMA_NAME
  ) {
    throw unsupportedSchema(`${name}@${version}`);
  }
}

async function validateRequiredObjects(db: Client): Promise<void> {
  for (const table of GATEWAY_CANONICAL_TABLES) {
    if (!(await objectExists(db, "table", table))) {
      throw new Error(`incomplete v0.1.0 Gateway schema: missing table ${table}`);
    }
  }
  for (const index of REQUIRED_INDEXES) {
    if (!(await objectExists(db, "index", index))) {
      throw new Error(`incomplete v0.1.0 Gateway schema: missing index ${index}`);
    }
  }
}

async function userSchemaIsEmpty(db: Client): Promise<boolean> {
  const result = await db.execute(
    `SELECT COUNT(*) AS count FROM sqlite_master
     WHERE type IN ('table', 'view', 'trigger', 'index')
       AND name NOT LIKE 'sqlite_%'`,
  );
  return Number(result.rows[0]?.count ?? 0) === 0;
}

function unsupportedSchema(marker: string): Error {
  return new Error(
    `unsupported pre-release Gateway database schema (${marker}); v0.1.0 does not perform silent upgrades. Back it up and start with a fresh v0.1.0 Gateway store`,
  );
}

async function tableExists(db: Client, table: string): Promise<boolean> {
  return objectExists(db, "table", table);
}

async function objectExists(db: Client, kind: string, name: string): Promise<boolean> {
  const result = await db.execute({
    sql: "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ? LIMIT 1",
    args: [kind, name],
  });
  return result.rows.length > 0;
}
