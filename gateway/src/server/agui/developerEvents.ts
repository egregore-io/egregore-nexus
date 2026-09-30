/**
 * Read-only developer event projection for the AG-UI WebSocket subscribe frame.
 *
 * Developer events are metadata-only rows written by the daemon/store layer. The gateway reads
 * them for UI/tooling cursors, but never writes them and never feeds them back into agent turns.
 */
import { and, asc, eq, gt } from "drizzle-orm";

import { createReadDb, type ReadDb } from "@drizzle/client";
import { developerEvents } from "@drizzle/schema.gen";
import { DeveloperEventKind, type DeveloperEventEnvelope } from "@shared/types";

export interface DeveloperEventSource {
  since(topic: string, afterSeq: number): Promise<DeveloperEventEnvelope[]>;
}

export function createDeveloperEventSource(db: ReadDb = createReadDb()): DeveloperEventSource {
  return {
    async since(topic, afterSeq) {
      const rows = await db
        .select()
        .from(developerEvents)
        .where(and(
          eq(developerEvents.topic, topic),
          gt(developerEvents.seq, afterSeq),
        ))
        .orderBy(asc(developerEvents.seq));
      return rows.map(rowToEnvelope).filter((row): row is DeveloperEventEnvelope => row !== undefined);
    },
  };
}

function rowToEnvelope(row: typeof developerEvents.$inferSelect): DeveloperEventEnvelope | undefined {
  const topic = row.topic ?? "";
  const seq = Number(row.seq ?? 0);
  const ts = Number(row.createdAt ?? 0);
  if (!topic || seq <= 0 || ts <= 0) return undefined;
  let data: unknown;
  if (row.dataJson) {
    try {
      data = JSON.parse(row.dataJson);
    } catch {
      data = undefined;
    }
  }
  const kind = row.kind === DeveloperEventKind.AgentLifecycle
    ? DeveloperEventKind.AgentLifecycle
    : row.kind === DeveloperEventKind.Action
      ? DeveloperEventKind.Action
      : DeveloperEventKind.Message;
  return {
    kind,
    topic,
    seq,
    ts,
    ...(row.threadName ? { thread: row.threadName } : {}),
    ...(row.dmName ? { dm: row.dmName } : {}),
    ...(row.fromName ? { from: row.fromName } : {}),
    ...(row.messageId ? { messageId: row.messageId } : {}),
    ...(row.agentName ? { agent: row.agentName } : {}),
    ...(row.sessionId ? { sessionId: row.sessionId } : {}),
    ...(row.lifecycle ? { lifecycle: row.lifecycle } : {}),
    ...(row.currentWork ? { currentWork: row.currentWork } : {}),
    ...(data !== undefined ? { data } : {}),
  };
}
