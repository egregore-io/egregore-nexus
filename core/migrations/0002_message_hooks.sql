-- Nexus v0.1.5 message-hook storage additions.
--
-- Keep the v0.1.0 baseline immutable. This forward migration preserves every message while adding
-- the hook-mutable mention projection and replacing the write-only atomic broadcast ingress with
-- its metadata-aware shape.

ALTER TABLE messages ADD COLUMN mention_json TEXT;

DROP TRIGGER IF EXISTS nexus_broadcast_ingress_insert;
DROP VIEW IF EXISTS nexus_broadcast_ingress;

CREATE VIEW nexus_broadcast_ingress AS
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
  NULL AS metadata_json,
  NULL AS mention_json,
  NULL AS recipients_json,
  NULL AS events_json
WHERE 0;

CREATE TRIGGER nexus_broadcast_ingress_insert
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
