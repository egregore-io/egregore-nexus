/** Canonical durable tables owned exclusively by Nexus Gateway. */
export const GATEWAY_CANONICAL_TABLES = [
  "gateway_schema_migrations",
  "projection_events",
  "projection_cursors",
  "projection_gaps",
  "projection_quarantine",
  "gateway_ingress",
  "identities",
  "runtime_descriptors",
  "threads",
  "thread_members",
  "topics",
  "topic_subscriptions",
  "bus_messages",
  "delivery_outcomes",
  "notifications",
  "human_user",
  "human_session",
  "rest_bearer_token",
  "logs",
  "rendered_conversations",
  "rendered_messages",
] as const;

export type GatewayCanonicalTable = (typeof GATEWAY_CANONICAL_TABLES)[number];
