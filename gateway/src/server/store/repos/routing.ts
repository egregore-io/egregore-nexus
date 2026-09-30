import type { Client } from "@libsql/client";

export interface GatewayIdentity {
  agentId: string;
  name?: string;
  owner?: string;
  role?: string;
  tier?: string;
  metadata: Record<string, unknown>;
  updatedAt: number;
}

export interface GatewayThread {
  threadId: string;
  name: string;
  archivedAt?: number;
  createdAt: number;
  updatedAt: number;
}

export async function upsertIdentity(db: Client, identity: GatewayIdentity): Promise<void> {
  await db.execute({
    sql: `INSERT INTO identities
          (agent_id, name, owner, role, tier, metadata_json, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(agent_id) DO UPDATE SET
            name = excluded.name,
            owner = excluded.owner,
            role = excluded.role,
            tier = excluded.tier,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at`,
    args: [
      identity.agentId,
      identity.name ?? null,
      identity.owner ?? null,
      identity.role ?? null,
      identity.tier ?? null,
      JSON.stringify(identity.metadata),
      identity.updatedAt,
    ],
  });
}

export async function declareThread(db: Client, thread: GatewayThread): Promise<void> {
  await db.execute({
    sql: `INSERT INTO threads
          (thread_id, name, archived_at, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(thread_id) DO UPDATE SET
            name = excluded.name,
            archived_at = excluded.archived_at,
            updated_at = excluded.updated_at`,
    args: [
      thread.threadId,
      thread.name,
      thread.archivedAt ?? null,
      thread.createdAt,
      thread.updatedAt,
    ],
  });
}

export async function setThreadMember(
  db: Client,
  threadId: string,
  agentId: string,
  joinedAt: number,
  leftAt?: number,
): Promise<void> {
  await db.execute({
    sql: `INSERT INTO thread_members (thread_id, agent_id, joined_at, left_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(thread_id, agent_id) DO UPDATE SET
            joined_at = excluded.joined_at,
            left_at = excluded.left_at`,
    args: [threadId, agentId, joinedAt, leftAt ?? null],
  });
}

export async function listThreadMemberIds(db: Client, threadId: string): Promise<string[]> {
  const result = await db.execute({
    sql: `SELECT agent_id FROM thread_members
          WHERE thread_id = ? AND left_at IS NULL ORDER BY agent_id ASC`,
    args: [threadId],
  });
  return result.rows.map((row) => String(row.agent_id));
}
