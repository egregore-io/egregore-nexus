import { createClient, type Client } from "@libsql/client";
import { beforeEach, describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../migrations";
import {
  applyProjectionEvent,
  getProjectionCursor,
  recordProjectionGap,
  type GatewayProjectionRecord,
} from "./events";

function event(overrides: Partial<GatewayProjectionRecord> = {}): GatewayProjectionRecord {
  return {
    eventId: "evt-1",
    daemonEpoch: "epoch-1",
    seq: 1,
    kind: "message.accepted",
    occurredAt: 100,
    payload: { messageId: "m1" },
    ...overrides,
  };
}

describe("Gateway projection repository", () => {
  let activeDb: Client | undefined;
  let db: Client;
  beforeEach(async () => {
    // Bare `:memory:` is connection-local in libSQL. The production write
    // transaction checks out another connection, so use its shared-memory URL.
    if (!activeDb) {
      activeDb = createClient({ url: "file::memory:?cache=shared" });
      await migrateGatewayStore(activeDb);
    } else {
      await activeDb.batch([
        "DELETE FROM projection_gaps",
        "DELETE FROM projection_cursors",
        "DELETE FROM projection_events",
        "DELETE FROM identities",
      ]);
    }
    db = activeDb;
  });

  it("commits the event, projected rows and cursor atomically", async () => {
    await applyProjectionEvent(db, "daemon", event(), async (tx) => {
      await tx.execute({
        sql: `INSERT INTO identities
              (agent_id, name, owner, role, tier, metadata_json, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: ["a_ada", "ada", null, "agent", "Member", "{}", 100],
      });
    });

    expect(await getProjectionCursor(db, "daemon")).toEqual({
      daemonEpoch: "epoch-1",
      throughSeq: 1,
    });
    expect((await db.execute("SELECT agent_id FROM identities")).rows).toMatchObject([
      { agent_id: "a_ada" },
    ]);
  });

  it("rolls back event, projection and cursor together", async () => {
    await expect(
      applyProjectionEvent(db, "daemon", event(), async (tx) => {
        await tx.execute({
          sql: `INSERT INTO identities
                (agent_id, name, owner, role, tier, metadata_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)`,
          args: ["a_ada", "ada", null, "agent", "Member", "{}", 100],
        });
        throw new Error("projection failed");
      }),
    ).rejects.toThrow("projection failed");

    expect((await db.execute("SELECT event_id FROM projection_events")).rows).toHaveLength(0);
    expect((await db.execute("SELECT agent_id FROM identities")).rows).toHaveLength(0);
    expect(await getProjectionCursor(db, "daemon")).toBeNull();
  });

  it("treats duplicate event id as a no-op and rejects a different event at the same position", async () => {
    expect(await applyProjectionEvent(db, "daemon", event())).toBe("applied");
    expect(await applyProjectionEvent(db, "daemon", event())).toBe("duplicate");
    await expect(
      applyProjectionEvent(
        db,
        "daemon",
        event({ eventId: "evt-other", payload: { ignored: true } }),
      ),
    ).rejects.toThrow(/sequence collision/);
    expect((await db.execute("SELECT event_id FROM projection_events")).rows).toHaveLength(1);
  });

  it("records a gap without advancing the projection cursor", async () => {
    await recordProjectionGap(db, {
      daemonEpoch: "epoch-1",
      afterSeq: 4,
      throughSeq: 9,
      reason: "backlog_overflow",
      recordedAt: 200,
    });

    expect(await getProjectionCursor(db, "daemon")).toBeNull();
    expect((await db.execute("SELECT reason FROM projection_gaps")).rows).toMatchObject([
      { reason: "backlog_overflow" },
    ]);
  });
});
