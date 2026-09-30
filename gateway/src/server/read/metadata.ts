import type { ReadDb } from "@drizzle/client";

export type MetadataEntityKind = "message" | "session" | "thread" | "agent";

export interface MetadataReadResult {
  entity: MetadataEntityKind;
  id: string;
  metadata: unknown;
}

/** Read one opaque metadata bag from the daemon-owned store. Missing/NULL metadata is `{}`. */
export async function entityMetadata(
  db: ReadDb,
  entity: MetadataEntityKind,
  id: string,
  project?: string,
): Promise<MetadataReadResult | undefined> {
  const row = await selectMetadata(db, entity, id, project);
  if (!row) return undefined;
  return {
    entity,
    id,
    metadata: parseMetadata(row.metadataJson),
  };
}

async function selectMetadata(
  db: ReadDb,
  entity: MetadataEntityKind,
  id: string,
  project?: string,
): Promise<{ metadataJson: string | null } | undefined> {
  if (entity === "agent") return selectAgentMetadata(db, id);
  const result = await db.$client.execute({
    sql: sqlFor(entity, project),
    args: argsFor(entity, id, project),
  });
  const row = result.rows[0] as { metadata_json?: unknown } | undefined;
  if (!row) return undefined;
  return {
    metadataJson: typeof row.metadata_json === "string" ? row.metadata_json : null,
  };
}

async function selectAgentMetadata(
  db: ReadDb,
  id: string,
): Promise<{ metadataJson: string | null } | undefined> {
  let result = await db.$client.execute({
    sql: "SELECT metadata_json FROM agents WHERE agent_id = ? LIMIT 1",
    args: [id],
  });
  if (result.rows.length === 0) {
    result = await db.$client.execute({
      sql: "SELECT metadata_json FROM agents WHERE name = ? ORDER BY agent_id LIMIT 2",
      args: [id],
    });
    if (result.rows.length > 1) {
      throw new Error(`ambiguous agent name ${JSON.stringify(id)}; address it by stable agent id`);
    }
  }
  const row = result.rows[0] as { metadata_json?: unknown } | undefined;
  if (!row) return undefined;
  return {
    metadataJson: typeof row.metadata_json === "string" ? row.metadata_json : null,
  };
}

function sqlFor(entity: MetadataEntityKind, project?: string): string {
  const projectClause = project ? " AND project = ?" : "";
  switch (entity) {
    case "message":
      return `SELECT metadata_json FROM messages WHERE message_id = ?${projectClause} LIMIT 1`;
    case "session":
      return `SELECT metadata_json FROM sessions WHERE session_id = ?${projectClause} LIMIT 1`;
    case "thread":
      return "SELECT metadata_json FROM threads WHERE name = ? LIMIT 1";
    case "agent":
      throw new Error("agent metadata uses stable identity resolution");
  }
}

function argsFor(entity: MetadataEntityKind, id: string, project?: string): string[] {
  if (entity === "thread") return [id];
  return project ? [id, project] : [id];
}

function parseMetadata(raw: string | null): unknown {
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    return {};
  }
}
