CREATE TABLE IF NOT EXISTS identity_sessions (
  runtime_id        TEXT PRIMARY KEY,
  agent_id          TEXT NOT NULL,
  project           TEXT NOT NULL,
  harness           TEXT NOT NULL,
  mode              TEXT NOT NULL,
  backend           TEXT,
  cwd               TEXT,
  native_resume_key TEXT,
  client_key        TEXT,
  updated_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_identity_sessions_agent
  ON identity_sessions(agent_id, updated_at DESC);

-- Minimal routing continuity only. Gateway owns thread history, presentation and search; these
-- rows retain just enough stable identity edges for daemon fan-out and auto-wake after restart.
CREATE TABLE IF NOT EXISTS routing_threads (
  thread_id   TEXT PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,
  project     TEXT NOT NULL,
  created_by  TEXT,
  created_at  INTEGER NOT NULL,
  archived_at INTEGER
);

CREATE TABLE IF NOT EXISTS routing_thread_members (
  thread_id    TEXT NOT NULL,
  session_name TEXT NOT NULL,
  agent_id     TEXT,
  joined_at    INTEGER NOT NULL,
  PRIMARY KEY (thread_id, session_name)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_routing_thread_members_agent
  ON routing_thread_members(thread_id, agent_id)
  WHERE agent_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS delivery_obligations (
  message_id           TEXT NOT NULL,
  recipient_agent_id   TEXT NOT NULL,
  recipient_runtime_id TEXT,
  payload_json          TEXT NOT NULL,
  dedupe_key            TEXT NOT NULL UNIQUE,
  attempt               INTEGER NOT NULL DEFAULT 0,
  state                 TEXT NOT NULL,
  created_at            INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  PRIMARY KEY (message_id, recipient_agent_id)
);

CREATE INDEX IF NOT EXISTS idx_delivery_obligations_pending
  ON delivery_obligations(state, created_at)
  WHERE state NOT IN ('delivered', 'rejected');

-- The pre-release single-store queue projection resolved targets through the mixed `sessions`
-- table.
-- Split daemon storage has no live-session table on disk, so resolve only through durable
-- resurrection identity. A command can legitimately have no runtime yet; its first event then
-- carries NULL until launch binds a runtime, while subsequent revisions retain any known id.
DROP TRIGGER IF EXISTS trg_command_intent_queue_insert;
DROP TRIGGER IF EXISTS trg_command_intent_queue_update;

CREATE TRIGGER trg_command_intent_queue_insert
BEFORE INSERT ON command_intents
WHEN NEW.kind IN ('harness.prompt', 'harness.steer', 'harness.interrupt', 'harness.compact')
BEGIN
  INSERT INTO command_intent_events (
    project, session_id, command_id, client_message_id, state, mode, revision, created_at
  ) VALUES (
    NEW.project,
    COALESCE(
      json_extract(NEW.request_json, '$.sessionId'),
      (SELECT i.runtime_id FROM identity_sessions i
       LEFT JOIN agents a ON a.agent_id = i.agent_id
       WHERE (json_extract(NEW.request_json, '$.agentId') IS NOT NULL
              AND i.agent_id = json_extract(NEW.request_json, '$.agentId'))
          OR (json_extract(NEW.request_json, '$.agentId') IS NULL
              AND a.name = json_extract(NEW.request_json, '$.name'))
       ORDER BY i.updated_at DESC LIMIT 1)
    ),
    NEW.command_id,
    json_extract(NEW.request_json, '$.clientMessageId'),
    'queued',
    CASE NEW.kind
      WHEN 'harness.steer' THEN 'redirect'
      WHEN 'harness.interrupt' THEN 'interrupt'
      WHEN 'harness.compact' THEN 'compact'
      ELSE 'queue'
    END,
    NEW.revision,
    NEW.created_at
  );
END;

CREATE TRIGGER trg_command_intent_queue_update
BEFORE UPDATE ON command_intents
WHEN NEW.revision != OLD.revision
 AND (NEW.kind IN ('harness.prompt', 'harness.steer', 'harness.interrupt', 'harness.compact')
      OR OLD.kind IN ('harness.prompt', 'harness.steer', 'harness.interrupt', 'harness.compact'))
BEGIN
  INSERT INTO command_intent_events (
    project, session_id, command_id, client_message_id, state, mode, revision, created_at
  ) VALUES (
    NEW.project,
    COALESCE(
      (SELECT session_id FROM command_intent_events
       WHERE command_id = NEW.command_id AND session_id IS NOT NULL
       ORDER BY seq DESC LIMIT 1),
      json_extract(NEW.request_json, '$.sessionId'),
      (SELECT i.runtime_id FROM identity_sessions i
       LEFT JOIN agents a ON a.agent_id = i.agent_id
       WHERE (json_extract(NEW.request_json, '$.agentId') IS NOT NULL
              AND i.agent_id = json_extract(NEW.request_json, '$.agentId'))
          OR (json_extract(NEW.request_json, '$.agentId') IS NULL
              AND a.name = json_extract(NEW.request_json, '$.name'))
       ORDER BY i.updated_at DESC LIMIT 1)
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
    CASE NEW.kind
      WHEN 'harness.steer' THEN 'redirect'
      WHEN 'harness.interrupt' THEN 'interrupt'
      WHEN 'harness.compact' THEN 'compact'
      ELSE 'queue'
    END,
    NEW.revision,
    COALESCE(NEW.completed_at, NEW.started_at, NEW.claimed_at, NEW.created_at)
  );
END;
