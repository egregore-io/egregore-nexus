import type { Client } from "@libsql/client";

import { getProjectionCursor, recordProjectionGap } from "../store/repos/events";
import {
  applyCanonicalProjection,
  type CanonicalProjectionEvent,
} from "./apply";

export type ProjectionFrame =
  | { t: "projection"; event: CanonicalProjectionEvent }
  | {
      t: "projection.gap";
      gap: {
        daemonEpoch: string;
        afterSeq?: number;
        throughSeq?: number;
        reason: string;
        recordedAt?: number;
      };
    };

export interface ProjectionTransport {
  ack(ack: { daemonEpoch: string; throughSeq: number }): Promise<unknown>;
}

export interface GatewayProjectionConsumerOptions {
  afterCommit?: (event: CanonicalProjectionEvent, result: "applied" | "duplicate") => Promise<void>;
  now?: () => number;
}

/** Ordered, single-consumer daemon projection application and ACK boundary. */
export class GatewayProjectionConsumer {
  private readonly now: () => number;

  constructor(
    private readonly db: Client,
    private readonly transport: ProjectionTransport,
    private readonly options: GatewayProjectionConsumerOptions = {},
  ) {
    this.now = options.now ?? Date.now;
  }

  async handleFrame(frame: ProjectionFrame): Promise<void> {
    if (frame.t === "projection.gap") {
      await recordProjectionGap(this.db, {
        daemonEpoch: frame.gap.daemonEpoch,
        afterSeq: frame.gap.afterSeq,
        throughSeq: frame.gap.throughSeq,
        reason: frame.gap.reason,
        recordedAt: frame.gap.recordedAt ?? this.now(),
      });
      return;
    }

    const event = frame.event;
    if (!isProjectionEvent(event)) {
      await this.quarantine(event, "malformed projection envelope");
      return;
    }
    const cursor = await getProjectionCursor(this.db, "daemon");
    const expected = cursor?.daemonEpoch === event.daemonEpoch
      ? cursor.throughSeq + 1
      : 1;
    if (event.seq > expected) {
      await recordProjectionGap(this.db, {
        daemonEpoch: event.daemonEpoch,
        afterSeq: expected - 1,
        throughSeq: event.seq - 1,
        reason: "sequence_gap",
        recordedAt: this.now(),
      });
      return;
    }

    try {
      const result = await applyCanonicalProjection(this.db, event);
      await this.options.afterCommit?.(event, result);
      await this.transport.ack({
        daemonEpoch: event.daemonEpoch,
        throughSeq: event.seq,
      });
    } catch (error) {
      await this.quarantineAndSettle(
        event,
        error instanceof Error ? error.message : String(error),
      );
    }
  }

  private async quarantineAndSettle(
    event: CanonicalProjectionEvent,
    error: string,
  ): Promise<void> {
    const tx = await this.db.transaction("write");
    try {
      await tx.execute({
        sql: `INSERT INTO projection_quarantine
              (event_id, daemon_epoch, seq, frame_json, error, quarantined_at)
              VALUES (?, ?, ?, ?, ?, ?)
              ON CONFLICT(event_id) DO UPDATE SET frame_json = excluded.frame_json,
                error = excluded.error, quarantined_at = excluded.quarantined_at`,
        args: [
          event.eventId,
          event.daemonEpoch,
          event.seq,
          JSON.stringify(event),
          error,
          this.now(),
        ],
      });
      await tx.execute({
        sql: `INSERT INTO projection_cursors
              (source, daemon_epoch, through_seq, updated_at)
              VALUES ('daemon', ?, ?, ?)
              ON CONFLICT(source) DO UPDATE SET
                daemon_epoch = excluded.daemon_epoch,
                through_seq = CASE
                  WHEN projection_cursors.daemon_epoch = excluded.daemon_epoch
                    THEN MAX(projection_cursors.through_seq, excluded.through_seq)
                  ELSE excluded.through_seq
                END,
                updated_at = excluded.updated_at`,
        args: [event.daemonEpoch, event.seq, this.now()],
      });
      await tx.commit();
    } catch (transactionError) {
      try {
        await tx.rollback();
      } catch {
        // Preserve the quarantine/cursor failure; rollback may already have closed the txn.
      }
      throw transactionError;
    }
    await this.transport.ack({
      daemonEpoch: event.daemonEpoch,
      throughSeq: event.seq,
    });
  }

  private async quarantine(event: unknown, error: string): Promise<void> {
    const candidate = event && typeof event === "object"
      ? event as Record<string, unknown>
      : {};
    const eventId = typeof candidate.eventId === "string" && candidate.eventId
      ? candidate.eventId
      : `malformed:${this.now()}`;
    await this.db.execute({
      sql: `INSERT INTO projection_quarantine
            (event_id, daemon_epoch, seq, frame_json, error, quarantined_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET frame_json = excluded.frame_json,
              error = excluded.error, quarantined_at = excluded.quarantined_at`,
      args: [
        eventId,
        typeof candidate.daemonEpoch === "string" ? candidate.daemonEpoch : null,
        typeof candidate.seq === "number" ? candidate.seq : null,
        JSON.stringify(event),
        error,
        this.now(),
      ],
    });
  }
}

function isProjectionEvent(value: unknown): value is CanonicalProjectionEvent {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const event = value as Record<string, unknown>;
  return typeof event.eventId === "string"
    && event.eventId.length > 0
    && typeof event.daemonEpoch === "string"
    && event.daemonEpoch.length > 0
    && Number.isInteger(event.seq)
    && Number(event.seq) >= 0
    && typeof event.occurredAt === "number"
    && typeof event.kind === "string"
    && typeof event.version === "number";
}
