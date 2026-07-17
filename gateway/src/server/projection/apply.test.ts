import { createClient, type Client } from "@libsql/client";
import { randomUUID } from "node:crypto";
import { rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../store/migrations";
import { applyCanonicalProjection } from "./apply";

describe("canonical Gateway projection application", () => {
  let db: Client;
  let dbPath: string;
  beforeEach(async () => {
    dbPath = join(tmpdir(), `nexus-gateway-apply-${randomUUID()}.db`);
    db = createClient({ url: `file:${dbPath}` });
    await migrateGatewayStore(db);
    await db.batch([
      "DELETE FROM projection_events",
      "DELETE FROM projection_cursors",
      "DELETE FROM bus_messages",
      "DELETE FROM delivery_outcomes",
    ]);
  });
  afterEach(async () => {
    db.close();
    await rm(dbPath, { force: true });
  });

  it("commits a materialized message and cursor in one transaction", async () => {
    await expect(
      applyCanonicalProjection(db, {
        eventId: "message:m1",
        daemonEpoch: "boot-1",
        seq: 1,
        occurredAt: 10,
        kind: "message.accepted",
        version: 1,
        payload: {
          messageId: "m1",
          scope: "dm",
          fromName: "ada",
          toAgentId: "a_blake",
          body: "hello",
          provenance: { from: "ada", kind: "agent" },
          project: "metadata-only",
          createdAt: 10,
        },
      }),
    ).resolves.toBe("applied");
    expect((await db.execute("SELECT message_id, body FROM bus_messages")).rows).toMatchObject([
      { message_id: "m1", body: "hello" },
    ]);
    expect((await db.execute("SELECT daemon_epoch, through_seq FROM projection_cursors")).rows)
      .toMatchObject([{ daemon_epoch: "boot-1", through_seq: 1 }]);
  });

  it("rolls back the source event and cursor when materialization rejects", async () => {
    await expect(
      applyCanonicalProjection(db, {
        eventId: "message:bad",
        daemonEpoch: "boot-1",
        seq: 1,
        occurredAt: 10,
        kind: "message.accepted",
        version: 1,
        payload: { messageId: "bad" },
      }),
    ).rejects.toThrow(/requires/i);
    expect((await db.execute("SELECT * FROM projection_events")).rows).toHaveLength(0);
    expect((await db.execute("SELECT * FROM projection_cursors")).rows).toHaveLength(0);
  });

  it("advances the cursor when a stable event id is replayed under a new daemon epoch", async () => {
    const base = {
      eventId: "message:m1",
      occurredAt: 10,
      kind: "message.accepted" as const,
      version: 1,
      payload: {
        messageId: "m1",
        scope: "dm",
        body: "hello",
        provenance: {},
        createdAt: 10,
      },
    };
    await applyCanonicalProjection(db, { ...base, daemonEpoch: "boot-1", seq: 1 });
    await expect(
      applyCanonicalProjection(db, { ...base, daemonEpoch: "boot-2", seq: 1 }),
    ).resolves.toBe("duplicate");
    expect((await db.execute("SELECT daemon_epoch, through_seq FROM projection_cursors")).rows)
      .toMatchObject([{ daemon_epoch: "boot-2", through_seq: 1 }]);
    expect((await db.execute("SELECT * FROM bus_messages")).rows).toHaveLength(1);
  });
});
