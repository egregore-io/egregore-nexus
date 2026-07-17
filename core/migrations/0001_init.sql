-- Nexus v0.1.0 baseline schema.
-- Embedded libSQL (SQLite-compatible). Project is descriptive metadata, never a routing key.

-- Runtime/session launch index. Durable agent identity lives in agents and agent_runtimes; this
-- table supplies the current launch/resume lookup surface keyed by stable agent ids.
CREATE TABLE IF NOT EXISTS sessions (
  session_id         TEXT PRIMARY KEY,
  agent_id           TEXT,
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
  transport          TEXT,
  metadata_json      TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_name       ON sessions(name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_client_key ON sessions(client_key);
CREATE        INDEX IF NOT EXISTS idx_sessions_harness    ON sessions(harness_session_id);
CREATE        INDEX IF NOT EXISTS idx_sessions_project    ON sessions(project);
CREATE        INDEX IF NOT EXISTS idx_sessions_agent_id   ON sessions(agent_id);

CREATE TABLE IF NOT EXISTS agents (
  agent_id        TEXT PRIMARY KEY,
  project         TEXT NOT NULL,
  name            TEXT,
  default_harness TEXT,
  role            TEXT,
  tier            TEXT NOT NULL DEFAULT 'agent',
  disabled_at     INTEGER,
  created_at      INTEGER NOT NULL,
  metadata_json   TEXT,
  owner_name      TEXT,
  owner_project   TEXT,
  owner_session_id TEXT,
  owner_agent_id  TEXT,
  lifecycle_state TEXT,
  dead_reason     TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_name_unique ON agents(name)
  WHERE name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_agents_project_name ON agents(project, name)
  WHERE name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_agents_owner ON agents(project, owner_name);
CREATE INDEX IF NOT EXISTS idx_agents_owner_agent ON agents(owner_agent_id);

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

CREATE INDEX IF NOT EXISTS idx_agent_credentials_agent_active
  ON agent_credentials(agent_id, created_at)
  WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS agent_acl_grants (
  agent_id             TEXT NOT NULL,
  principal_project    TEXT NOT NULL,
  principal_name       TEXT NOT NULL,
  principal_session_id TEXT,
  principal_agent_id   TEXT,
  role                 TEXT NOT NULL,
  granted_by_name      TEXT NOT NULL,
  granted_by_project   TEXT NOT NULL,
  created_at           INTEGER NOT NULL,
  updated_at           INTEGER NOT NULL,
  granted_by_agent_id  TEXT,
  PRIMARY KEY (agent_id, principal_project, principal_name)
);

CREATE INDEX IF NOT EXISTS idx_agent_acl_grants_principal
  ON agent_acl_grants(principal_project, principal_name);
CREATE INDEX IF NOT EXISTS idx_agent_acl_grants_principal_agent
  ON agent_acl_grants(principal_agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_acl_grants_grantor_agent
  ON agent_acl_grants(granted_by_agent_id);

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
  last_heartbeat INTEGER,
  os_pid         INTEGER,
  os_pgid        INTEGER
);

CREATE INDEX IF NOT EXISTS idx_agent_runtimes_agent_active
  ON agent_runtimes(agent_id, active, started_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_runtimes_one_active
  ON agent_runtimes(agent_id)
  WHERE active = 1 AND stopped_at IS NULL;

CREATE TABLE IF NOT EXISTS agent_groups (
  project    TEXT NOT NULL,
  group_name TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (project, group_name)
);

CREATE TABLE IF NOT EXISTS agent_group_members (
  project     TEXT NOT NULL,
  group_name  TEXT NOT NULL,
  agent_id    TEXT NOT NULL,
  agent_name  TEXT,
  assigned_at INTEGER NOT NULL,
  PRIMARY KEY (project, group_name, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_group_members_agent
  ON agent_group_members(project, agent_id);

CREATE TABLE IF NOT EXISTS messages (
  message_id    TEXT PRIMARY KEY,
  from_name     TEXT,
  kind          TEXT,
  to_name       TEXT,
  thread_id     TEXT,
  topic         TEXT,
  summary       TEXT,
  body          TEXT,
  provenance    TEXT,
  project       TEXT,
  created_at    INTEGER,
  from_agent_id    TEXT,
  to_agent_id      TEXT,
  sender_session_id TEXT,
  idempotency_key   TEXT,
  metadata_json     TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_to_created ON messages(to_name, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_name);
CREATE INDEX IF NOT EXISTS idx_messages_from_agent_created
  ON messages(from_agent_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_to_agent_created
  ON messages(to_agent_id, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_sender_idempotency
  ON messages(sender_session_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL AND sender_session_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS in_flight (
  in_flight_id       TEXT PRIMARY KEY,
  message_id         TEXT,
  recipient_session  TEXT,
  state              TEXT DEFAULT 'pending',
  delivered_at       INTEGER,
  acked_at           INTEGER,
  recipient_agent_id TEXT,
  attempt_count      INTEGER NOT NULL DEFAULT 0,
  attempt_started_at INTEGER,
  failed_at          INTEGER,
  error_code         TEXT,
  error_reason       TEXT,
  error_details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_in_flight_recipient_state
  ON in_flight(recipient_session, state);
CREATE UNIQUE INDEX IF NOT EXISTS idx_in_flight_msg_recipient
  ON in_flight(message_id, recipient_session);
CREATE INDEX IF NOT EXISTS idx_in_flight_agent_state
  ON in_flight(recipient_agent_id, state);
CREATE UNIQUE INDEX IF NOT EXISTS idx_in_flight_msg_agent
  ON in_flight(message_id, recipient_agent_id)
  WHERE recipient_agent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_in_flight_undrained
  ON in_flight(state, message_id, in_flight_id)
  WHERE state IN ('pending','notified');
CREATE INDEX IF NOT EXISTS idx_in_flight_injecting
  ON in_flight(state, in_flight_id)
  WHERE state = 'injecting';
CREATE INDEX IF NOT EXISTS idx_in_flight_error
  ON in_flight(state, message_id, in_flight_id)
  WHERE state = 'error';

CREATE TABLE IF NOT EXISTS threads (
  thread_id  TEXT PRIMARY KEY,
  name       TEXT UNIQUE,
  project    TEXT,
  created_by TEXT,
  created_at INTEGER,
  archived_at INTEGER,
  metadata_json TEXT,
  topic TEXT,
  description TEXT
);

CREATE TABLE IF NOT EXISTS thread_members (
  thread_id    TEXT,
  session_name TEXT,
  joined_at    INTEGER,
  agent_id     TEXT,
  PRIMARY KEY (thread_id, session_name)
);

CREATE INDEX IF NOT EXISTS idx_thread_members_agent
  ON thread_members(agent_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_thread_members_thread_agent
  ON thread_members(thread_id, agent_id)
  WHERE agent_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS topics (
  topic      TEXT PRIMARY KEY,
  project    TEXT,
  created_at INTEGER
);

CREATE TABLE IF NOT EXISTS subscriptions (
  topic               TEXT,
  subscriber_session  TEXT,
  sub_group           TEXT,
  cursor              INTEGER DEFAULT 0,
  subscribed_at       INTEGER,
  subscriber_agent_id TEXT,
  PRIMARY KEY (topic, subscriber_session)
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_agent
  ON subscriptions(subscriber_agent_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_topic_agent
  ON subscriptions(topic, subscriber_agent_id)
  WHERE subscriber_agent_id IS NOT NULL;

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
  topic       TEXT PRIMARY KEY,
  latest_seq  INTEGER NOT NULL DEFAULT 0,
  updated_at  INTEGER NOT NULL
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

-- Store-backed ingress for producer commands. Producers submit intent rows; the daemon claims and
-- executes them, preserving the daemon as the canonical writer of messages/in_flight/routing state.
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
  revision          INTEGER NOT NULL DEFAULT 1,
  created_at        INTEGER NOT NULL,
  claimed_at        INTEGER,
  started_at        INTEGER,
  lease_until       INTEGER,
  completed_at      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_command_intents_pending
  ON command_intents(status, created_at);
CREATE INDEX IF NOT EXISTS idx_command_intents_lease
  ON command_intents(status, lease_until);
CREATE INDEX IF NOT EXISTS idx_command_intents_terminal_completed
  ON command_intents(status, completed_at)
  WHERE status IN ('done', 'error', 'cancelled') AND completed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_command_intents_harness_claim_agent
  ON command_intents(kind, status, json_extract(request_json, '$.agentId'), created_at)
  WHERE kind IN ('harness.prompt', 'harness.warm', 'harness.compact')
    AND status IN ('pending', 'claimed')
    AND json_extract(request_json, '$.agentId') IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_command_intents_harness_claim_name
  ON command_intents(kind, status, json_extract(request_json, '$.name'), created_at)
  WHERE kind IN ('harness.prompt', 'harness.warm', 'harness.compact')
    AND status IN ('pending', 'claimed')
    AND json_extract(request_json, '$.agentId') IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_command_intents_idempotency_scope
  ON command_intents(
    project,
    kind,
    COALESCE(caller_client_key, caller_session_id, caller_name),
    idempotency_key
  )
  WHERE idempotency_key IS NOT NULL;

-- Append-only projection of command_intents transitions. The command row remains queue truth;
-- this log supplies monotonic reconnect/push cursors and never participates in claiming.
CREATE TABLE IF NOT EXISTS command_intent_events (
  seq               INTEGER PRIMARY KEY AUTOINCREMENT,
  project           TEXT NOT NULL,
  session_id        TEXT,
  command_id        TEXT NOT NULL,
  client_message_id TEXT,
  state             TEXT NOT NULL,
  mode              TEXT NOT NULL,
  revision          INTEGER NOT NULL,
  created_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_command_intent_events_session_seq
  ON command_intent_events(session_id, seq);

-- Idempotency ledger for queue edits/cancels/reorders/redirects. The gateway writes this in the
-- same transaction as the authoritative command_intents mutation so ack loss is replay-safe.
CREATE TABLE IF NOT EXISTS command_queue_mutations (
  project            TEXT NOT NULL,
  client_mutation_id TEXT NOT NULL,
  request_json       TEXT NOT NULL,
  response_status    INTEGER,
  response_json      TEXT,
  created_at         INTEGER NOT NULL,
  PRIMARY KEY (project, client_mutation_id)
);
CREATE INDEX IF NOT EXISTS idx_command_queue_mutations_completed
  ON command_queue_mutations(created_at)
  WHERE response_json IS NOT NULL;

-- Queue writes return their append-only event cursor in the same statement. BEFORE triggers make
-- that event visible to SQLite's RETURNING projection while preserving one atomic transaction.
CREATE TRIGGER trg_command_intent_queue_insert
BEFORE INSERT ON command_intents
WHEN NEW.kind IN ('harness.prompt', 'harness.steer')
BEGIN
  INSERT INTO command_intent_events (
    project, session_id, command_id, client_message_id, state, mode, revision, created_at
  ) VALUES (
    NEW.project,
    COALESCE(
      (SELECT session_id FROM command_intent_events
       WHERE command_id = NEW.command_id AND session_id IS NOT NULL
       ORDER BY seq DESC LIMIT 1),
      (SELECT session_id FROM sessions s WHERE s.project = NEW.project AND (
        (json_extract(NEW.request_json, '$.agentId') IS NOT NULL
         AND s.agent_id = json_extract(NEW.request_json, '$.agentId'))
        OR (json_extract(NEW.request_json, '$.agentId') IS NULL
            AND s.name = json_extract(NEW.request_json, '$.name'))
      ) ORDER BY s.created_at DESC LIMIT 1)
    ),
    NEW.command_id,
    json_extract(NEW.request_json, '$.clientMessageId'),
    'queued',
    CASE WHEN NEW.kind = 'harness.steer' THEN 'redirect' ELSE 'queue' END,
    NEW.revision,
    NEW.created_at
  );
END;

CREATE TRIGGER trg_command_intent_queue_update
BEFORE UPDATE ON command_intents
WHEN NEW.revision != OLD.revision
 AND (NEW.kind IN ('harness.prompt', 'harness.steer')
      OR OLD.kind IN ('harness.prompt', 'harness.steer'))
BEGIN
  INSERT INTO command_intent_events (
    project, session_id, command_id, client_message_id, state, mode, revision, created_at
  ) VALUES (
    NEW.project,
    COALESCE(
      (SELECT session_id FROM command_intent_events
       WHERE command_id = NEW.command_id AND session_id IS NOT NULL
       ORDER BY seq DESC LIMIT 1),
      (SELECT session_id FROM sessions s WHERE s.project = NEW.project AND (
        (json_extract(NEW.request_json, '$.agentId') IS NOT NULL
         AND s.agent_id = json_extract(NEW.request_json, '$.agentId'))
        OR (json_extract(NEW.request_json, '$.agentId') IS NULL
            AND s.name = json_extract(NEW.request_json, '$.name'))
      ) ORDER BY s.created_at DESC LIMIT 1)
    ),
    NEW.command_id,
    json_extract(NEW.request_json, '$.clientMessageId'),
    CASE
      WHEN NEW.status = 'pending' THEN 'queued'
      WHEN NEW.status = 'claimed' AND NEW.started_at IS NULL THEN 'claimed'
      WHEN NEW.status = 'claimed' THEN 'started'
      WHEN NEW.status = 'done' THEN 'completed'
      WHEN NEW.status = 'error' THEN 'failed'
      WHEN NEW.status = 'cancelled' THEN 'cancelled'
      ELSE 'failed'
    END,
    CASE WHEN NEW.kind = 'harness.steer' THEN 'redirect' ELSE 'queue' END,
    NEW.revision,
    COALESCE(NEW.completed_at, NEW.started_at, NEW.claimed_at, NEW.created_at)
  );
END;

CREATE TABLE IF NOT EXISTS daemon_state (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
  name          TEXT PRIMARY KEY,
  token         TEXT NOT NULL,
  topic         TEXT NOT NULL,
  enabled       INTEGER NOT NULL DEFAULT 1,
  created_at    INTEGER NOT NULL,
  last_fired_at INTEGER
);

CREATE TABLE IF NOT EXISTS initial_prompt_deliveries (
  runtime_id        TEXT PRIMARY KEY,
  agent_id          TEXT NOT NULL,
  session_id        TEXT NOT NULL,
  harness           TEXT NOT NULL,
  template          TEXT NOT NULL,
  rendered_prompt   TEXT NOT NULL,
  client_message_id TEXT NOT NULL,
  status            TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'failed')),
  error             TEXT,
  created_at_ms     INTEGER NOT NULL,
  accepted_at_ms    INTEGER,
  failed_at_ms      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_initial_prompt_deliveries_session_status
  ON initial_prompt_deliveries(session_id, status);

CREATE TABLE IF NOT EXISTS inbox_subscriptions (
  subscription_id   TEXT PRIMARY KEY,
  project           TEXT NOT NULL,
  caller_name       TEXT NOT NULL,
  caller_session_id TEXT NOT NULL,
  caller_agent_id   TEXT,
  caller_client_key TEXT,
  status            TEXT NOT NULL,
  timeout_ms        INTEGER,
  max               INTEGER,
  created_at        INTEGER NOT NULL,
  updated_at        INTEGER NOT NULL,
  last_drained_at   INTEGER,
  last_error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_inbox_subscriptions_active
  ON inbox_subscriptions(status, project, caller_session_id);

CREATE TABLE IF NOT EXISTS inbox_subscription_batches (
  batch_id          TEXT PRIMARY KEY,
  subscription_id   TEXT NOT NULL,
  batch_json        TEXT NOT NULL,
  message_signature TEXT NOT NULL,
  status            TEXT NOT NULL,
  created_at        INTEGER NOT NULL,
  consumed_at       INTEGER,
  FOREIGN KEY(subscription_id) REFERENCES inbox_subscriptions(subscription_id)
);

CREATE INDEX IF NOT EXISTS idx_inbox_subscription_batches_pending
  ON inbox_subscription_batches(subscription_id, status, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inbox_subscription_batches_pending_signature
  ON inbox_subscription_batches(subscription_id, message_signature)
  WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS producer_identities (
  runtime_id  TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (runtime_id, producer_id)
);

CREATE INDEX IF NOT EXISTS idx_producer_identities_runtime_created
  ON producer_identities(runtime_id, created_at);

CREATE TABLE IF NOT EXISTS transcript_archive (
  runtime_id      TEXT PRIMARY KEY,
  agent_id        TEXT,
  agent_name      TEXT NOT NULL,
  project         TEXT NOT NULL,
  harness         TEXT NOT NULL,
  source_kind     TEXT NOT NULL,
  source_path     TEXT NOT NULL,
  archive_path    TEXT NOT NULL,
  archive_offset  INTEGER NOT NULL DEFAULT 0,
  bytes_archived  INTEGER NOT NULL DEFAULT 0,
  prefix_sha256   TEXT,
  last_event      TEXT,
  sealed_at       INTEGER,
  seal_reason     TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transcript_archive_agent
  ON transcript_archive(agent_name, project, updated_at);

CREATE TABLE IF NOT EXISTS native_thread_bindings (
  harness          TEXT NOT NULL,
  native_thread_id TEXT NOT NULL,
  agent_id         TEXT NOT NULL,
  project          TEXT NOT NULL,
  first_runtime_id TEXT,
  last_runtime_id  TEXT,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL,
  released_at      INTEGER,
  PRIMARY KEY (harness, native_thread_id)
);

CREATE INDEX IF NOT EXISTS idx_native_thread_bindings_agent
  ON native_thread_bindings(agent_id);
CREATE INDEX IF NOT EXISTS idx_native_thread_bindings_runtime
  ON native_thread_bindings(last_runtime_id);

CREATE TABLE IF NOT EXISTS agent_session_turns (
  id                    TEXT PRIMARY KEY,
  session_id            TEXT NOT NULL,
  status                TEXT NOT NULL,
  first_stream_event_id INTEGER NOT NULL,
  last_stream_event_id  INTEGER NOT NULL,
  started_at            INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  finalized_at          INTEGER
);

CREATE INDEX IF NOT EXISTS idx_agent_session_turns_session_event
  ON agent_session_turns(session_id, last_stream_event_id);
CREATE INDEX IF NOT EXISTS idx_agent_session_turns_streaming
  ON agent_session_turns(status, session_id)
  WHERE status = 'streaming';

CREATE TABLE IF NOT EXISTS agent_session_messages (
  id                    TEXT PRIMARY KEY,
  session_id            TEXT NOT NULL,
  turn_id               TEXT NOT NULL,
  ordinal               INTEGER NOT NULL,
  role                  TEXT NOT NULL,
  author                TEXT,
  content_json          TEXT NOT NULL,
  status                TEXT NOT NULL,
  first_stream_event_id INTEGER NOT NULL,
  last_stream_event_id  INTEGER NOT NULL,
  created_at            INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  finalized_at          INTEGER,
  UNIQUE(turn_id, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_agent_session_messages_session_created
  ON agent_session_messages(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_agent_session_messages_turn
  ON agent_session_messages(turn_id, ordinal);

CREATE TABLE IF NOT EXISTS agent_session_stream_cursors (
  session_id                           TEXT PRIMARY KEY,
  last_materialized_stream_event_id    INTEGER NOT NULL,
  updated_at                           INTEGER NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  summary,
  body,
  content='messages',
  content_rowid='rowid'
);

-- One payload-free route cursor per recipient. Rich transport rows may be discarded after
-- settlement, while a reply still needs the conversation that entered the harness.
CREATE TABLE IF NOT EXISTS reply_contexts (
  recipient_key      TEXT PRIMARY KEY,
  recipient_session  TEXT NOT NULL,
  recipient_agent_id TEXT,
  scope              TEXT NOT NULL,
  from_name          TEXT NOT NULL,
  from_agent_id      TEXT,
  thread_id          TEXT,
  message_id         TEXT NOT NULL,
  updated_at         INTEGER NOT NULL
);

CREATE TRIGGER IF NOT EXISTS in_flight_reply_context_on_injecting
AFTER UPDATE OF state ON in_flight
WHEN NEW.state = 'injecting' AND OLD.state != 'injecting'
BEGIN
  INSERT INTO reply_contexts (
    recipient_key, recipient_session, recipient_agent_id, scope, from_name,
    from_agent_id, thread_id, message_id, updated_at
  )
  SELECT
    COALESCE(NEW.recipient_agent_id, NEW.recipient_session),
    NEW.recipient_session,
    NEW.recipient_agent_id,
    message.kind,
    COALESCE(message.from_name, ''),
    message.from_agent_id,
    message.thread_id,
    message.message_id,
    COALESCE(message.created_at, 0)
  FROM messages AS message
  WHERE message.message_id = NEW.message_id
  ON CONFLICT(recipient_key) DO UPDATE SET
    recipient_session = excluded.recipient_session,
    recipient_agent_id = excluded.recipient_agent_id,
    scope = excluded.scope,
    from_name = excluded.from_name,
    from_agent_id = excluded.from_agent_id,
    thread_id = excluded.thread_id,
    message_id = excluded.message_id,
    updated_at = excluded.updated_at;
END;

-- One-round-trip parameterized Message Post broadcast ingress. This view is write-only; its
-- trigger expands internal recipient/event JSON while the complete send remains one atomic SQLite
-- statement. Project is stored as descriptive message metadata and never participates in routing.
CREATE VIEW IF NOT EXISTS nexus_broadcast_ingress AS
SELECT
  NULL AS message_id,
  NULL AS from_name,
  NULL AS kind,
  NULL AS to_name,
  NULL AS thread_id,
  NULL AS topic,
  NULL AS summary,
  NULL AS body,
  NULL AS provenance,
  NULL AS project,
  NULL AS created_at,
  NULL AS from_agent_id,
  NULL AS to_agent_id,
  NULL AS sender_session_id,
  NULL AS idempotency_key,
  NULL AS recipients_json,
  NULL AS events_json
WHERE 0;

CREATE TRIGGER IF NOT EXISTS nexus_broadcast_ingress_insert
INSTEAD OF INSERT ON nexus_broadcast_ingress
BEGIN
  INSERT INTO messages (
    message_id, from_name, kind, to_name, thread_id, topic, summary, body, provenance,
    project, created_at, from_agent_id, to_agent_id, sender_session_id, idempotency_key
  ) VALUES (
    NEW.message_id, NEW.from_name, NEW.kind, NEW.to_name, NEW.thread_id, NEW.topic,
    NEW.summary, NEW.body, NEW.provenance, NEW.project, NEW.created_at, NEW.from_agent_id,
    NEW.to_agent_id, NEW.sender_session_id, NEW.idempotency_key
  );

  INSERT INTO messages_fts (rowid, summary, body)
  VALUES ((SELECT rowid FROM messages WHERE message_id = NEW.message_id), NEW.summary, NEW.body);

  INSERT OR IGNORE INTO in_flight (
    in_flight_id, message_id, recipient_session, recipient_agent_id, state
  )
  SELECT
    json_extract(recipient.value, '$.inFlightId'),
    NEW.message_id,
    json_extract(recipient.value, '$.session'),
    json_extract(recipient.value, '$.agentId'),
    'pending'
  FROM json_each(NEW.recipients_json) AS recipient;

  INSERT INTO developer_event_topics (topic, latest_seq, updated_at)
  SELECT DISTINCT json_extract(event.value, '$.topic'), 1, NEW.created_at
  FROM json_each(NEW.events_json) AS event
  WHERE json_type(event.value, '$.topic') = 'text'
  ON CONFLICT(topic) DO UPDATE SET
    latest_seq = developer_event_topics.latest_seq + 1,
    updated_at = excluded.updated_at;

  INSERT INTO developer_events (
    topic, seq, kind, message_id, thread_name, dm_name, from_name, created_at
  )
  SELECT
    json_extract(event.value, '$.topic'),
    topic.latest_seq,
    'message',
    NEW.message_id,
    json_extract(event.value, '$.threadName'),
    json_extract(event.value, '$.dmName'),
    NEW.from_name,
    NEW.created_at
  FROM json_each(NEW.events_json) AS event
  JOIN developer_event_topics AS topic
    ON topic.topic = json_extract(event.value, '$.topic');
END;
