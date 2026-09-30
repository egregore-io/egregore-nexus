import type { Client, Transaction } from "@libsql/client";

export interface GatewayProjectionRecord {
  eventId: string;
  daemonEpoch: string;
  seq: number;
  kind: string;
  occurredAt: number;
  payload: unknown;
}

export interface GatewayProjectionCursor {
  daemonEpoch: string;
  throughSeq: number;
}

export interface GatewayProjectionGap {
  daemonEpoch: string;
  afterSeq?: number;
  throughSeq?: number;
  reason: string;
  recordedAt: number;
}

/** Apply an immutable source event, its projection, and cursor in one transaction. */
export async function applyProjectionEvent(
  db: Client,
  source: string,
  event: GatewayProjectionRecord,
  project?: (tx: Transaction) => Promise<void>,
): Promise<"applied" | "duplicate"> {
  const tx = await db.transaction("write");
  try {
    const duplicate = await tx.execute({
      sql: `SELECT event_id, daemon_epoch, seq FROM projection_events
            WHERE event_id = ? OR (daemon_epoch = ? AND seq = ?)
            ORDER BY CASE WHEN event_id = ? THEN 0 ELSE 1 END LIMIT 1`,
      args: [event.eventId, event.daemonEpoch, event.seq, event.eventId],
    });
    if (duplicate.rows.length > 0) {
      const row = duplicate.rows[0]!;
      if (String(row.event_id) !== event.eventId) {
        throw new Error(
          `projection sequence collision at ${event.daemonEpoch}:${event.seq}`,
        );
      }
      await advanceProjectionCursor(tx, source, event);
      await tx.commit();
      return "duplicate";
    }

    await tx.execute({
      sql: `INSERT INTO projection_events
            (event_id, daemon_epoch, seq, kind, occurred_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)`,
      args: [
        event.eventId,
        event.daemonEpoch,
        event.seq,
        event.kind,
        event.occurredAt,
        JSON.stringify(event.payload),
      ],
    });
    await project?.(tx);
    await advanceProjectionCursor(tx, source, event);
    await tx.commit();
    return "applied";
  } catch (error) {
    try {
      await tx.rollback();
    } catch {
      // Preserve the projection error; rollback may report an already-closed transaction.
    }
    throw error;
  }
}

async function advanceProjectionCursor(
  tx: Transaction,
  source: string,
  event: GatewayProjectionRecord,
): Promise<void> {
  await tx.execute({
    sql: `INSERT INTO projection_cursors
          (source, daemon_epoch, through_seq, updated_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(source) DO UPDATE SET
            daemon_epoch = excluded.daemon_epoch,
            through_seq = CASE
              WHEN projection_cursors.daemon_epoch = excluded.daemon_epoch
                THEN MAX(projection_cursors.through_seq, excluded.through_seq)
              ELSE excluded.through_seq
            END,
            updated_at = excluded.updated_at`,
    args: [source, event.daemonEpoch, event.seq, event.occurredAt],
  });
}

export async function getProjectionCursor(
  db: Client,
  source: string,
): Promise<GatewayProjectionCursor | null> {
  const result = await db.execute({
    sql: `SELECT daemon_epoch, through_seq
          FROM projection_cursors WHERE source = ? LIMIT 1`,
    args: [source],
  });
  const row = result.rows[0];
  if (!row) return null;
  return {
    daemonEpoch: String(row.daemon_epoch),
    throughSeq: Number(row.through_seq),
  };
}

/** Record an explicit observability gap without pretending it was projected. */
export async function recordProjectionGap(
  db: Client,
  gap: GatewayProjectionGap,
): Promise<void> {
  await db.execute({
    sql: `INSERT INTO projection_gaps
          (daemon_epoch, after_seq, through_seq, reason, recorded_at)
          VALUES (?, ?, ?, ?, ?)`,
    args: [
      gap.daemonEpoch,
      gap.afterSeq ?? null,
      gap.throughSeq ?? null,
      gap.reason,
      gap.recordedAt,
    ],
  });
}
