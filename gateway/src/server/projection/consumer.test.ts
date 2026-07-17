import { createClient } from "@libsql/client";
import { randomUUID } from "node:crypto";
import { rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import { migrateGatewayStore } from "../store/migrations";
import { GatewayProjectionConsumer, type ProjectionTransport } from "./consumer";

function event(seq: number, overrides: Record<string, unknown> = {}) {
  return {
    t: "projection" as const,
    event: {
      eventId: `message:m${seq}`,
      daemonEpoch: "boot-1",
      seq,
      occurredAt: seq,
      kind: "message.accepted" as const,
      version: 1,
      payload: {
        messageId: `m${seq}`,
        scope: "dm",
        body: `body ${seq}`,
        provenance: {},
        createdAt: seq,
      },
      ...overrides,
    },
  };
}

describe("Gateway projection consumer", () => {
  const clients: ReturnType<typeof createClient>[] = [];
  const paths: string[] = [];
  async function testDb() {
    const dbPath = join(tmpdir(), `nexus-gateway-consumer-${randomUUID()}.db`);
    const db = createClient({ url: `file:${dbPath}` });
    clients.push(db);
    paths.push(dbPath);
    await migrateGatewayStore(db);
    return db;
  }
  afterEach(async () => {
    for (const client of clients.splice(0)) client.close();
    await Promise.all(paths.splice(0).map((dbPath) => rm(dbPath, { force: true })));
  });

  it("ACKs only after transaction completion and ACKs duplicate replay", async () => {
    const db = await testDb();
    const order: string[] = [];
    const transport: ProjectionTransport = {
      ack: vi.fn(async () => order.push("ack")),
    };
    const consumer = new GatewayProjectionConsumer(db, transport, {
      afterCommit: async () => {
        order.push("commit");
      },
    });

    await consumer.handleFrame(event(1));
    await consumer.handleFrame(event(1));
    expect(order).toEqual(["commit", "ack", "commit", "ack"]);
    expect(transport.ack).toHaveBeenCalledTimes(2);
    expect((await db.execute("SELECT * FROM bus_messages")).rows).toHaveLength(1);
  });

  it("does not move the durable cursor backward on stale duplicate replay", async () => {
    const db = await testDb();
    const ack = vi.fn(async () => undefined);
    const consumer = new GatewayProjectionConsumer(db, { ack });

    await consumer.handleFrame(event(1));
    await consumer.handleFrame(event(2));
    await consumer.handleFrame(event(1));

    expect((await db.execute("SELECT daemon_epoch, through_seq FROM projection_cursors")).rows)
      .toMatchObject([{ daemon_epoch: "boot-1", through_seq: 2 }]);
  });

  it("records a sequence gap and pauses ACK until bounded replay arrives", async () => {
    const db = await testDb();
    const ack = vi.fn(async () => undefined);
    const consumer = new GatewayProjectionConsumer(db, { ack });
    await consumer.handleFrame(event(2));
    expect(ack).not.toHaveBeenCalled();
    expect((await db.execute("SELECT reason FROM projection_gaps")).rows).toMatchObject([
      { reason: "sequence_gap" },
    ]);
  });

  it("quarantines a malformed event without poisoning the next committed event", async () => {
    const db = await testDb();
    const ack = vi.fn(async () => undefined);
    const consumer = new GatewayProjectionConsumer(db, { ack });
    await consumer.handleFrame(event(1, { payload: { messageId: "broken" } }));
    await consumer.handleFrame(event(2));
    expect((await db.execute("SELECT event_id FROM projection_quarantine")).rows).toMatchObject([
      { event_id: "message:m1" },
    ]);
    expect((await db.execute("SELECT daemon_epoch, through_seq FROM projection_cursors")).rows)
      .toMatchObject([{ daemon_epoch: "boot-1", through_seq: 2 }]);
    expect((await db.execute("SELECT message_id FROM bus_messages")).rows).toMatchObject([
      { message_id: "m2" },
    ]);
    expect(ack).toHaveBeenNthCalledWith(1, { daemonEpoch: "boot-1", throughSeq: 1 });
    expect(ack).toHaveBeenNthCalledWith(2, { daemonEpoch: "boot-1", throughSeq: 2 });
  });
});
