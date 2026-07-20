import type { Client, InStatement } from "@libsql/client";

import { GATEWAY_CANONICAL_TABLES } from "./schema";

const GATEWAY_V1_SCHEMA_VERSION = 1;
const GATEWAY_V1_SCHEMA_NAME = "v0.1.0_baseline";
const GATEWAY_V2_SCHEMA_VERSION = 2;
const GATEWAY_V2_SCHEMA_NAME = "v0.1.5_message_hooks";
const GATEWAY_V3_SCHEMA_VERSION = 3;
const GATEWAY_V3_SCHEMA_NAME = "v0.1.5_message_hook_receipts";
const GATEWAY_V4_SCHEMA_VERSION = 4;
const GATEWAY_V4_SCHEMA_NAME = "v0.1.5_resumable_message_hooks";
const GATEWAY_V5_SCHEMA_VERSION = 5;
const GATEWAY_V5_SCHEMA_NAME = "v0.1.5_bearer_authority";
const GATEWAY_V6_SCHEMA_VERSION = 6;
const GATEWAY_V6_SCHEMA_NAME = "v0.1.6_principals_and_transport_bindings";
export const CURRENT_GATEWAY_SCHEMA_VERSION = 7;
export const CURRENT_GATEWAY_SCHEMA_NAME = "v0.1.6_transport_host";

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

const HOOK_AUDIT_SCHEMA: InStatement[] = [
  `CREATE TABLE IF NOT EXISTS hook_pipeline_evaluations (
    evaluation_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    event TEXT NOT NULL,
    registry_generation TEXT NOT NULL,
    original_message_json TEXT NOT NULL,
    state TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER,
    result_json TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_hook_pipeline_message_event
    ON hook_pipeline_evaluations(message_id, event)`,
  `CREATE TABLE IF NOT EXISTS hook_handler_executions (
    invocation_id TEXT PRIMARY KEY,
    evaluation_id TEXT NOT NULL,
    hook_id TEXT NOT NULL,
    event TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    executed_by_json TEXT NOT NULL,
    result_json TEXT
  )`,
  `CREATE INDEX IF NOT EXISTS idx_hook_execution_evaluation
    ON hook_handler_executions(evaluation_id, started_at, invocation_id)`,
  `CREATE TABLE IF NOT EXISTS hook_receipt_completion (
    message_id TEXT PRIMARY KEY,
    invocation_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'claimed',
    claimed_at INTEGER NOT NULL,
    completed_at INTEGER,
    metadata_patch_json TEXT,
    executed_by_json TEXT,
    merge_acked_at INTEGER,
    last_error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0
  )`,
];

const MESSAGE_HOOK_PROJECTION_SCHEMA: InStatement[] = [
  "ALTER TABLE bus_messages ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'",
  "ALTER TABLE bus_messages ADD COLUMN mention_json TEXT NOT NULL DEFAULT '[]'",
];

const V2_RECEIPT_UPGRADE_SCHEMA: InStatement[] = [
  "ALTER TABLE hook_receipt_completion ADD COLUMN state TEXT NOT NULL DEFAULT 'claimed'",
  "ALTER TABLE hook_receipt_completion ADD COLUMN completed_at INTEGER",
  "ALTER TABLE hook_receipt_completion ADD COLUMN metadata_patch_json TEXT",
  "ALTER TABLE hook_receipt_completion ADD COLUMN executed_by_json TEXT",
  "ALTER TABLE hook_receipt_completion ADD COLUMN merge_acked_at INTEGER",
  "ALTER TABLE hook_receipt_completion ADD COLUMN last_error TEXT",
  "ALTER TABLE hook_receipt_completion ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
];

const RESUMABLE_HOOK_UPGRADE_SCHEMA: InStatement[] = [
  "ALTER TABLE hook_pipeline_evaluations ADD COLUMN result_json TEXT",
  "ALTER TABLE hook_handler_executions ADD COLUMN result_json TEXT",
];

const BEARER_AUTHORITY_UPGRADE_SCHEMA: InStatement[] = [
  "ALTER TABLE rest_bearer_token ADD COLUMN actor_session_id TEXT",
  "ALTER TABLE rest_bearer_token ADD COLUMN actor_agent_id TEXT",
  "ALTER TABLE rest_bearer_token ADD COLUMN actor_runtime_id TEXT",
  "ALTER TABLE rest_bearer_token ADD COLUMN actor_client_key TEXT",
];

const V016_IDENTITY_COLUMNS: InStatement[] = [
  "ALTER TABLE human_user ADD COLUMN human_user_id TEXT",
  "ALTER TABLE human_session ADD COLUMN human_user_id TEXT",
  "ALTER TABLE human_session ADD COLUMN principal_id TEXT",
  "ALTER TABLE gateway_ingress ADD COLUMN caller_principal_id TEXT",
];

const V016_PRINCIPAL_TRANSPORT_SCHEMA: InStatement[] = [
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_human_user_id ON human_user(human_user_id)",
  `CREATE TABLE IF NOT EXISTS principals (
    principal_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    access TEXT NOT NULL,
    created_at INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS principal_aliases (
    principal_id TEXT NOT NULL REFERENCES principals(principal_id),
    alias TEXT NOT NULL,
    UNIQUE(alias)
  )`,
  `CREATE TABLE IF NOT EXISTS subject_bindings (
    provider TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
    principal_id TEXT NOT NULL REFERENCES principals(principal_id),
    display_name TEXT,
    created_at INTEGER NOT NULL,
    UNIQUE(provider, external_user_id)
  )`,
  `CREATE TABLE IF NOT EXISTS transport_lane_bindings (
    provider TEXT NOT NULL,
    external_chat_id TEXT NOT NULL,
    lane_kind TEXT NOT NULL,
    lane_name TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(provider, external_chat_id)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_transport_lane
    ON transport_lane_bindings(lane_kind, lane_name, provider, external_chat_id)`,
  `CREATE TABLE IF NOT EXISTS transport_secrets (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
  )`,
];

const V016_TRANSPORT_HOST_SCHEMA: InStatement[] = [
  `CREATE TABLE IF NOT EXISTS transport_ingress (
    provider TEXT NOT NULL,
    ingress_id TEXT NOT NULL,
    received_at INTEGER NOT NULL,
    PRIMARY KEY(provider, ingress_id)
  )`,
  `CREATE TABLE IF NOT EXISTS transport_outbox (
    obligation_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    external_chat_id TEXT NOT NULL,
    lane_kind TEXT NOT NULL,
    lane_name TEXT NOT NULL,
    text TEXT NOT NULL,
    state TEXT NOT NULL,
    external_message_id TEXT,
    created_at INTEGER NOT NULL,
    settled_at INTEGER
  )`,
  `CREATE INDEX IF NOT EXISTS idx_transport_outbox_pending
    ON transport_outbox(provider, state, created_at, obligation_id)`,
];

const V1_REQUIRED_INDEXES = [
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
const HOOK_REQUIRED_INDEXES = [
  "idx_hook_pipeline_message_event",
  "idx_hook_execution_evaluation",
] as const;
const V016_REQUIRED_INDEXES = ["idx_human_user_id", "idx_transport_lane"] as const;
const V016_HOST_REQUIRED_INDEXES = ["idx_transport_outbox_pending"] as const;
const REQUIRED_INDEXES = [
  ...V1_REQUIRED_INDEXES,
  ...HOOK_REQUIRED_INDEXES,
  ...V016_REQUIRED_INDEXES,
  ...V016_HOST_REQUIRED_INDEXES,
] as const;
const V016_TABLES = new Set([
  "principals",
  "principal_aliases",
  "subject_bindings",
  "transport_lane_bindings",
  "transport_secrets",
  "transport_ingress",
  "transport_outbox",
]);

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
        ...HOOK_AUDIT_SCHEMA,
        ...MESSAGE_HOOK_PROJECTION_SCHEMA,
        ...BEARER_AUTHORITY_UPGRADE_SCHEMA,
        ...V016_IDENTITY_COLUMNS,
        ...V016_PRINCIPAL_TRANSPORT_SCHEMA,
        ...V016_TRANSPORT_HOST_SCHEMA,
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
    await upgradeOrValidateBaseline(db);
  }

  await validateRequiredObjects(db);
}

async function upgradeOrValidateBaseline(db: Client): Promise<void> {
  const rows = await db.execute(
    "SELECT version, name FROM gateway_schema_migrations ORDER BY version",
  );
  if (rows.rows.length !== 1) {
    throw unsupportedSchema("pre-release migration ladder");
  }
  const marker = rows.rows[0]!;
  const version = Number(marker.version);
  const name = String(marker.name);
  if (version === CURRENT_GATEWAY_SCHEMA_VERSION && name === CURRENT_GATEWAY_SCHEMA_NAME) {
    return;
  }
  if (version === GATEWAY_V6_SCHEMA_VERSION && name === GATEWAY_V6_SCHEMA_NAME) {
    await validateV6Objects(db);
    return upgradeTransportHost(db);
  }
  if (version === GATEWAY_V1_SCHEMA_VERSION && name === GATEWAY_V1_SCHEMA_NAME) {
    await validateV1Objects(db);
    await db.batch(
      [
        ...HOOK_AUDIT_SCHEMA,
        ...MESSAGE_HOOK_PROJECTION_SCHEMA,
        ...BEARER_AUTHORITY_UPGRADE_SCHEMA,
        migrationMarkerUpdate(
          version,
          name,
          GATEWAY_V5_SCHEMA_VERSION,
          GATEWAY_V5_SCHEMA_NAME,
        ),
      ],
      "write",
    );
    return upgradePrincipalIdentity(db);
  }
  if (version === GATEWAY_V2_SCHEMA_VERSION && name === GATEWAY_V2_SCHEMA_NAME) {
    await validateV2Objects(db);
    await db.batch(
      [
        ...MESSAGE_HOOK_PROJECTION_SCHEMA,
        ...V2_RECEIPT_UPGRADE_SCHEMA,
        ...RESUMABLE_HOOK_UPGRADE_SCHEMA,
        ...BEARER_AUTHORITY_UPGRADE_SCHEMA,
        migrationMarkerUpdate(
          version,
          name,
          GATEWAY_V5_SCHEMA_VERSION,
          GATEWAY_V5_SCHEMA_NAME,
        ),
      ],
      "write",
    );
    return upgradePrincipalIdentity(db);
  }
  if (version === GATEWAY_V3_SCHEMA_VERSION && name === GATEWAY_V3_SCHEMA_NAME) {
    await validateV2Objects(db);
    await db.batch(
      [
        ...RESUMABLE_HOOK_UPGRADE_SCHEMA,
        ...BEARER_AUTHORITY_UPGRADE_SCHEMA,
        migrationMarkerUpdate(
          version,
          name,
          GATEWAY_V5_SCHEMA_VERSION,
          GATEWAY_V5_SCHEMA_NAME,
        ),
      ],
      "write",
    );
    return upgradePrincipalIdentity(db);
  }
  if (version === GATEWAY_V4_SCHEMA_VERSION && name === GATEWAY_V4_SCHEMA_NAME) {
    await validateV2Objects(db);
    await db.batch(
      [
        ...BEARER_AUTHORITY_UPGRADE_SCHEMA,
        migrationMarkerUpdate(
          version,
          name,
          GATEWAY_V5_SCHEMA_VERSION,
          GATEWAY_V5_SCHEMA_NAME,
        ),
      ],
      "write",
    );
    return upgradePrincipalIdentity(db);
  }
  if (version === GATEWAY_V5_SCHEMA_VERSION && name === GATEWAY_V5_SCHEMA_NAME) {
    await validateV2Objects(db);
    return upgradePrincipalIdentity(db);
  }
  throw unsupportedSchema(`${name}@${version}`);
}

function migrationMarkerUpdate(
  version: number,
  name: string,
  targetVersion = CURRENT_GATEWAY_SCHEMA_VERSION,
  targetName = CURRENT_GATEWAY_SCHEMA_NAME,
): InStatement {
  return {
    sql: `UPDATE gateway_schema_migrations
          SET version = ?, name = ?, applied_at = ?
          WHERE version = ? AND name = ?`,
    args: [targetVersion, targetName, Date.now(), version, name],
  };
}

async function upgradePrincipalIdentity(db: Client): Promise<void> {
  const statements: InStatement[] = [];
  if (!(await columnExists(db, "human_user", "human_user_id"))) {
    statements.push(V016_IDENTITY_COLUMNS[0]!);
  }
  if (!(await columnExists(db, "human_session", "human_user_id"))) {
    statements.push(V016_IDENTITY_COLUMNS[1]!);
  }
  if (!(await columnExists(db, "human_session", "principal_id"))) {
    statements.push(V016_IDENTITY_COLUMNS[2]!);
  }
  if (!(await columnExists(db, "gateway_ingress", "caller_principal_id"))) {
    statements.push(V016_IDENTITY_COLUMNS[3]!);
  }
  statements.push(
    ...V016_PRINCIPAL_TRANSPORT_SCHEMA,
    `UPDATE human_user
       SET human_user_id = 'hu_' || lower(hex(randomblob(12)))
       WHERE human_user_id IS NULL`,
    `INSERT OR IGNORE INTO principals (principal_id, kind, access, created_at)
       SELECT 'h_' || substr(human_user_id, 4), 'local.human', 'admin', ${Date.now()}
       FROM human_user`,
    `INSERT OR IGNORE INTO principal_aliases (principal_id, alias)
       SELECT 'h_' || substr(human_user_id, 4), 'human:' || human_user_id
       FROM human_user`,
    `UPDATE human_session
       SET human_user_id = (
             SELECT u.human_user_id FROM human_user AS u
             WHERE u.client_key = human_session.client_key
           ),
           principal_id = (
             SELECT 'h_' || substr(u.human_user_id, 4) FROM human_user AS u
             WHERE u.client_key = human_session.client_key
           )`,
    migrationMarkerUpdate(
      GATEWAY_V5_SCHEMA_VERSION,
      GATEWAY_V5_SCHEMA_NAME,
      GATEWAY_V6_SCHEMA_VERSION,
      GATEWAY_V6_SCHEMA_NAME,
    ),
  );
  await db.batch(statements, "write");
  await upgradeTransportHost(db);
}

async function upgradeTransportHost(db: Client): Promise<void> {
  await db.batch([
    ...V016_TRANSPORT_HOST_SCHEMA,
    migrationMarkerUpdate(
      GATEWAY_V6_SCHEMA_VERSION,
      GATEWAY_V6_SCHEMA_NAME,
    ),
  ], "write");
}

async function columnExists(
  db: Client,
  table: string,
  column: string,
): Promise<boolean> {
  const result = await db.execute(`PRAGMA table_info(${table})`);
  return result.rows.some((row) => String(row.name) === column);
}

async function validateV1Objects(db: Client): Promise<void> {
  for (const table of GATEWAY_CANONICAL_TABLES) {
    if (V016_TABLES.has(table)) continue;
    if (table.startsWith("hook_")) continue;
    if (!(await objectExists(db, "table", table))) {
      throw new Error(`incomplete v0.1.0 Gateway schema: missing table ${table}`);
    }
  }
  for (const index of V1_REQUIRED_INDEXES) {
    if (!(await objectExists(db, "index", index))) {
      throw new Error(`incomplete v0.1.0 Gateway schema: missing index ${index}`);
    }
  }
}

async function validateV2Objects(db: Client): Promise<void> {
  for (const table of GATEWAY_CANONICAL_TABLES) {
    if (V016_TABLES.has(table)) continue;
    if (!(await objectExists(db, "table", table))) {
      throw new Error(`incomplete v0.1.5 Gateway hook schema: missing table ${table}`);
    }
  }
  for (const index of [...V1_REQUIRED_INDEXES, ...HOOK_REQUIRED_INDEXES]) {
    if (!(await objectExists(db, "index", index))) {
      throw new Error(`incomplete v0.1.5 Gateway hook schema: missing index ${index}`);
    }
  }
}

async function validateV6Objects(db: Client): Promise<void> {
  for (const table of GATEWAY_CANONICAL_TABLES) {
    if (table === "transport_ingress" || table === "transport_outbox") continue;
    if (!(await objectExists(db, "table", table))) {
      throw new Error(`incomplete v0.1.6 Gateway principal schema: missing table ${table}`);
    }
  }
  for (const index of [...V1_REQUIRED_INDEXES, ...HOOK_REQUIRED_INDEXES, ...V016_REQUIRED_INDEXES]) {
    if (!(await objectExists(db, "index", index))) {
      throw new Error(`incomplete v0.1.6 Gateway principal schema: missing index ${index}`);
    }
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

/** @internal Test support for proving the one supported additive upgrade. */
export async function createGatewayV1StoreForTest(db: Client): Promise<void> {
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
        args: [GATEWAY_V1_SCHEMA_VERSION, GATEWAY_V1_SCHEMA_NAME, Date.now()],
      },
    ],
    "write",
  );
}
