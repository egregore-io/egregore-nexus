import type { Client, Transaction } from "@libsql/client";

import {
  applyProjectionEvent,
  type GatewayProjectionRecord,
} from "../store/repos/events";
import {
  enqueueObligationsForMessage,
  notifyTransportOutbox,
} from "../transport/outbox";

export interface CanonicalProjectionEvent extends GatewayProjectionRecord {
  version: number;
  kind:
    | "identity.upserted"
    | "identity.removed"
    | "runtime.upserted"
    | "runtime.stopped"
    | "thread.declared"
    | "thread.membership.changed"
    | "topic.declared"
    | "topic.subscription.changed"
    | "message.accepted"
    | "delivery.settled"
    | "notification.emitted"
    | "presence.changed";
}

/** Apply one canonical daemon fact and advance its cursor in the same Gateway transaction. */
export async function applyCanonicalProjection(
  db: Client,
  event: CanonicalProjectionEvent,
): Promise<"applied" | "duplicate"> {
  if (event.version !== 1) {
    throw new Error(`unsupported projection version ${event.version}`);
  }
  const result = await applyProjectionEvent(db, "daemon", event, async (tx, context) => {
    if (context.epochChanged) {
      await tx.execute({
        sql: `UPDATE runtime_descriptors
              SET status = 'stopped', updated_at = ?
              WHERE lower(status) NOT IN ('stopped', 'offline')`,
        args: [event.occurredAt],
      });
    }
    await materialize(tx, event);
  });
  if (result === "applied" && event.kind === "message.accepted") notifyTransportOutbox();
  return result;
}

async function materialize(tx: Transaction, event: CanonicalProjectionEvent): Promise<void> {
  const payload = objectPayload(event.payload);
  switch (event.kind) {
    case "identity.upserted": {
      const agentId = requiredString(payload, "agentId");
      await tx.execute({
        sql: `INSERT INTO identities
              (agent_id, name, owner, role, tier, metadata_json, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(agent_id) DO UPDATE SET
                name = excluded.name, owner = excluded.owner, role = excluded.role,
                tier = excluded.tier, metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at`,
        args: [
          agentId,
          optionalString(payload.name),
          optionalString(payload.owner),
          optionalString(payload.role),
          optionalString(payload.tier),
          JSON.stringify(payload),
          event.occurredAt,
        ],
      });
      return;
    }
    case "identity.removed": {
      await tx.execute({
        sql: "DELETE FROM identities WHERE agent_id = ?",
        args: [requiredString(payload, "agentId")],
      });
      return;
    }
    case "runtime.upserted": {
      const runtimeId = requiredString(payload, "runtimeId");
      const agentId = requiredString(payload, "agentId");
      const transport = optionalString(payload.transport);
      await tx.execute({
        sql: `INSERT INTO runtime_descriptors
              (runtime_id, agent_id, session_id, harness, mode, backend, cwd,
               native_resume_key, status, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(runtime_id) DO UPDATE SET
                agent_id = excluded.agent_id, session_id = excluded.session_id,
                harness = excluded.harness, mode = excluded.mode, backend = excluded.backend,
                cwd = excluded.cwd, native_resume_key = excluded.native_resume_key,
                status = excluded.status, updated_at = excluded.updated_at`,
        args: [
          runtimeId,
          agentId,
          optionalString(payload.sessionId) ?? runtimeId,
          optionalString(payload.harness) ?? "unknown",
          optionalString(payload.mode) ?? (transport === "pty" ? "headed" : "headless"),
          optionalString(payload.backend) ?? transport,
          optionalString(payload.cwd),
          optionalString(payload.nativeResumeKey),
          payload.active === false ? "stopped" : optionalString(payload.presence) ?? "online",
          event.occurredAt,
        ],
      });
      return;
    }
    case "runtime.stopped": {
      await tx.execute({
        sql: "UPDATE runtime_descriptors SET status = 'stopped', updated_at = ? WHERE runtime_id = ?",
        args: [event.occurredAt, requiredString(payload, "runtimeId")],
      });
      return;
    }
    case "presence.changed": {
      await tx.execute({
        sql: "UPDATE runtime_descriptors SET status = ?, updated_at = ? WHERE runtime_id = ?",
        args: [
          optionalString(payload.presence) ?? "unknown",
          event.occurredAt,
          requiredString(payload, "runtimeId"),
        ],
      });
      return;
    }
    case "thread.declared": {
      const threadId = requiredString(payload, "threadId");
      if (payload.deleted === true) {
        await tx.execute({ sql: "DELETE FROM thread_members WHERE thread_id = ?", args: [threadId] });
        await tx.execute({ sql: "DELETE FROM threads WHERE thread_id = ?", args: [threadId] });
        return;
      }
      await tx.execute({
        sql: `INSERT INTO threads (thread_id, name, archived_at, created_at, updated_at)
              VALUES (?, ?, ?, ?, ?)
              ON CONFLICT(thread_id) DO UPDATE SET name = excluded.name,
                archived_at = excluded.archived_at, updated_at = excluded.updated_at`,
        args: [
          threadId,
          requiredString(payload, "name"),
          optionalNumber(payload.archivedAt),
          optionalNumber(payload.createdAt) ?? event.occurredAt,
          event.occurredAt,
        ],
      });
      return;
    }
    case "thread.membership.changed": {
      const threadId = requiredString(payload, "threadId");
      const members = Array.isArray(payload.members) ? payload.members : [];
      await tx.execute({ sql: "DELETE FROM thread_members WHERE thread_id = ?", args: [threadId] });
      for (const raw of members) {
        const member = typeof raw === "string" ? { name: raw } : objectPayload(raw);
        const agentId = optionalString(member.agentId) ?? `legacy-name:${requiredString(member, "name")}`;
        await tx.execute({
          sql: `INSERT INTO thread_members (thread_id, agent_id, joined_at, left_at)
                VALUES (?, ?, ?, NULL)`,
          args: [threadId, agentId, event.occurredAt],
        });
      }
      return;
    }
    case "topic.declared": {
      await tx.execute({
        sql: `INSERT INTO topics (topic, created_at, updated_at) VALUES (?, ?, ?)
              ON CONFLICT(topic) DO UPDATE SET updated_at = excluded.updated_at`,
        args: [
          requiredString(payload, "topic"),
          optionalNumber(payload.createdAt) ?? event.occurredAt,
          event.occurredAt,
        ],
      });
      return;
    }
    case "topic.subscription.changed": {
      const topic = requiredString(payload, "topic");
      await tx.execute({ sql: "DELETE FROM topic_subscriptions WHERE topic = ?", args: [topic] });
      for (const raw of Array.isArray(payload.subscribers) ? payload.subscribers : []) {
        const subscriber = objectPayload(raw);
        await tx.execute({
          sql: `INSERT INTO topic_subscriptions
                (topic, agent_id, group_name, cursor, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)`,
          args: [
            topic,
            optionalString(subscriber.agentId),
            optionalString(subscriber.group),
            subscriber.cursor === undefined ? null : String(subscriber.cursor),
            optionalNumber(subscriber.subscribedAt) ?? event.occurredAt,
            event.occurredAt,
          ],
        });
      }
      return;
    }
    case "message.accepted": {
      const provenance = objectPayload(payload.provenance ?? {});
      if (payload.project !== undefined) provenance.project = payload.project;
      const inserted = await tx.execute({
        sql: `INSERT OR IGNORE INTO bus_messages
              (message_id, kind, from_name, from_agent_id, to_name, to_agent_id,
               thread_id, topic, summary, body, provenance_json, metadata_json,
               mention_json, created_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          requiredString(payload, "messageId"),
          requiredString(payload, "scope"),
          optionalString(payload.fromName),
          optionalString(payload.fromAgentId),
          optionalString(payload.toName),
          optionalString(payload.toAgentId),
          optionalString(payload.threadId),
          optionalString(payload.topic),
          optionalString(payload.summary),
          requiredString(payload, "body"),
          JSON.stringify(provenance),
          JSON.stringify(objectOrEmpty(payload.metadata)),
          JSON.stringify(stringArray(payload.mention)),
          optionalNumber(payload.createdAt) ?? event.occurredAt,
        ],
      });
      if (inserted.rowsAffected > 0) {
        await enqueueObligationsForMessage(tx, {
          messageId: requiredString(payload, "messageId"),
          kind: requiredString(payload, "scope"),
          toName: optionalString(payload.toName) ?? undefined,
          threadId: optionalString(payload.threadId) ?? undefined,
          body: requiredString(payload, "body"),
          createdAt: optionalNumber(payload.createdAt) ?? event.occurredAt,
        });
      }
      return;
    }
    case "delivery.settled": {
      const messageId = requiredString(payload, "messageId");
      const recipientSessionId = optionalString(payload.recipientSessionId);
      const recipientAgentId = optionalString(payload.recipientAgentId)
        ?? (recipientSessionId ? `session:${recipientSessionId}` : undefined);
      if (!recipientAgentId) throw new Error("delivery settlement requires recipient identity");
      await tx.execute({
        sql: `INSERT INTO delivery_outcomes
              (message_id, recipient_agent_id, recipient_session_id, state, error_json,
               attempted_at, settled_at) VALUES (?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(message_id, recipient_agent_id) DO UPDATE SET
                recipient_session_id = excluded.recipient_session_id,
                state = excluded.state, error_json = excluded.error_json,
                attempted_at = excluded.attempted_at, settled_at = excluded.settled_at`,
        args: [
          messageId,
          recipientAgentId,
          recipientSessionId,
          requiredString(payload, "state"),
          JSON.stringify({
            code: optionalString(payload.errorCode),
            reason: optionalString(payload.errorReason),
            details: payload.errorDetails,
          }),
          event.occurredAt,
          optionalNumber(payload.deliveredAt) ?? optionalNumber(payload.failedAt) ?? event.occurredAt,
        ],
      });
      return;
    }
    case "notification.emitted": {
      const messageId = requiredString(payload, "messageId");
      const provenance = objectPayload(payload.provenance ?? {});
      const target = payload.threadId
        ? { kind: "thread", threadId: payload.threadId }
        : payload.topic
          ? { kind: "topic", topic: payload.topic }
          : payload.toAgentId
            ? { kind: "agent", agentId: payload.toAgentId }
            : { kind: "name", name: payload.toName };
      await tx.execute({
        sql: `INSERT OR IGNORE INTO notifications
              (notification_id, message_id, source, target_json, summary, body, created_at)
              VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: [
          messageId,
          messageId,
          optionalString(provenance.from) ?? optionalString(payload.fromName),
          JSON.stringify(target),
          optionalString(payload.summary),
          requiredString(payload, "body"),
          optionalNumber(payload.createdAt) ?? event.occurredAt,
        ],
      });
      return;
    }
    default:
      throw new Error(`unsupported projection kind ${String(event.kind)}`);
  }
}

function objectOrEmpty(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : [];
}

function objectPayload(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("projection payload must be an object");
  }
  return value as Record<string, unknown>;
}

function requiredString(payload: Record<string, unknown>, key: string): string {
  const value = payload[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`projection payload requires ${key}`);
  }
  return value;
}

function optionalString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function optionalNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
