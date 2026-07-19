CREATE TABLE IF NOT EXISTS live_sessions (
  runtime_id    TEXT PRIMARY KEY,
  presence      TEXT NOT NULL,
  connection_id TEXT,
  boot_epoch    TEXT NOT NULL,
  updated_at    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_live_sessions_presence
  ON live_sessions(presence, updated_at DESC);

-- One payload-free, boot-scoped route cursor per recipient identity. Rich transport rows are
-- discarded after settlement; `reply` still needs the conversation that entered the harness.
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

-- Atomic boot-scoped Message Post ingress. Search indexing is Gateway-owned, so this transport
-- trigger deliberately omits the old messages_fts side effect.
CREATE VIEW IF NOT EXISTS nexus_broadcast_ingress AS
SELECT
  NULL AS message_id, NULL AS from_name, NULL AS kind, NULL AS to_name,
  NULL AS thread_id, NULL AS topic, NULL AS summary, NULL AS body,
  NULL AS provenance, NULL AS project, NULL AS created_at,
  NULL AS from_agent_id, NULL AS to_agent_id, NULL AS sender_session_id,
  NULL AS idempotency_key, NULL AS metadata_json, NULL AS mention_json,
  NULL AS delivery_timing, NULL AS recipients_json, NULL AS events_json
WHERE 0;

CREATE TRIGGER IF NOT EXISTS nexus_broadcast_ingress_insert
INSTEAD OF INSERT ON nexus_broadcast_ingress
BEGIN
  INSERT INTO messages (
    message_id, from_name, kind, to_name, thread_id, topic, summary, body, provenance,
    project, created_at, from_agent_id, to_agent_id, sender_session_id, idempotency_key,
    metadata_json, mention_json
  ) VALUES (
    NEW.message_id, NEW.from_name, NEW.kind, NEW.to_name, NEW.thread_id, NEW.topic,
    NEW.summary, NEW.body, NEW.provenance, NEW.project, NEW.created_at, NEW.from_agent_id,
    NEW.to_agent_id, NEW.sender_session_id, NEW.idempotency_key, NEW.metadata_json,
    NEW.mention_json
  );

  INSERT OR IGNORE INTO in_flight (
    in_flight_id, message_id, recipient_session, recipient_agent_id, state, delivery_timing
  )
  SELECT
    json_extract(recipient.value, '$.inFlightId'),
    NEW.message_id,
    json_extract(recipient.value, '$.session'),
    json_extract(recipient.value, '$.agentId'),
    'pending',
    COALESCE(NEW.delivery_timing, 'interrupt')
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
    json_extract(event.value, '$.topic'), topic.latest_seq, 'message', NEW.message_id,
    json_extract(event.value, '$.threadName'), json_extract(event.value, '$.dmName'),
    NEW.from_name, NEW.created_at
  FROM json_each(NEW.events_json) AS event
  JOIN developer_event_topics AS topic
    ON topic.topic = json_extract(event.value, '$.topic');
END;
