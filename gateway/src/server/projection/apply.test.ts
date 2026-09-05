import { createClient, type Client } from "@libsql/client";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { removeTempPath } from "../../test/removeTempPath";
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
    await removeTempPath(dbPath);
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
          provenance: {
            from: "ada",
            kind: "human",
            locality: "external",
            access: "guest",
          },
          project: "metadata-only",
          createdAt: 10,
        },
      }),
    ).resolves.toBe("applied");
    const projected = (await db.execute(
      "SELECT message_id, body, provenance_json, metadata_json, mention_json FROM bus_messages",
    )).rows[0];
    expect(projected).toMatchObject({
      message_id: "m1",
      body: "hello",
      metadata_json: "{}",
      mention_json: "[]",
    });
    expect(JSON.parse(String(projected?.provenance_json))).toMatchObject({
      kind: "human",
      locality: "external",
      access: "guest",
    });
    expect((await db.execute("SELECT daemon_epoch, through_seq FROM projection_cursors")).rows)
      .toMatchObject([{ daemon_epoch: "boot-1", through_seq: 1 }]);
  });

  it("materializes canonical message metadata and mentions for receipt hooks", async () => {
    await applyCanonicalProjection(db, {
      eventId: "message:m_hook",
      daemonEpoch: "boot-1",
      seq: 1,
      occurredAt: 10,
      kind: "message.accepted",
      version: 1,
      payload: {
        messageId: "m_hook",
        scope: "thread",
        threadId: "t_release",
        body: "review",
        metadata: { nested: { source: "before_send" } },
        mention: ["fable"],
      },
    });

    const row = (await db.execute(
      "SELECT metadata_json, mention_json FROM bus_messages WHERE message_id = 'm_hook'",
    )).rows[0];
    expect(JSON.parse(String(row?.metadata_json))).toEqual({
      nested: { source: "before_send" },
    });
    expect(JSON.parse(String(row?.mention_json))).toEqual(["fable"]);
  });

  it("uses the projection ingest transaction as the sole idempotent outbox producer", async () => {
    await db.execute(`INSERT INTO transport_lane_bindings
      (provider, external_chat_id, lane_kind, lane_name, created_at)
      VALUES ('telegram', 'group-design', 'thread', 'design', 1)`);
    const event = {
      eventId: "message:m_transport",
      daemonEpoch: "boot-1",
      seq: 1,
      occurredAt: 10,
      kind: "message.accepted" as const,
      version: 1,
      payload: {
        messageId: "m_transport",
        scope: "thread",
        threadId: "t_design",
        toName: "design",
        fromAgentId: "a_author",
        body: "ship it",
        createdAt: 10,
      },
    };

    await expect(applyCanonicalProjection(db, event)).resolves.toBe("applied");
    await expect(applyCanonicalProjection(db, event)).resolves.toBe("duplicate");
    await expect(applyCanonicalProjection(db, {
      ...event,
      daemonEpoch: "boot-2",
      seq: 1,
    })).resolves.toBe("duplicate");

    expect((await db.execute("SELECT * FROM transport_outbox")).rows).toMatchObject([{
      message_id: "m_transport",
      provider: "telegram",
      external_chat_id: "group-design",
      lane_kind: "thread",
      lane_name: "design",
      state: "pending",
    }]);
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

  it("retires stale runtimes and rematerializes current runtimes on a daemon epoch change", async () => {
    const identity = (agentId: string, name: string, seq: number) => ({
      eventId: `identity:${agentId}`,
      daemonEpoch: "boot-1",
      seq,
      occurredAt: seq,
      kind: "identity.upserted" as const,
      version: 1,
      payload: { agentId, name, project: "default", defaultHarness: "codex" },
    });
    const runtime = (runtimeId: string, agentId: string, seq: number) => ({
      eventId: `runtime:${runtimeId}`,
      daemonEpoch: "boot-1",
      seq,
      occurredAt: seq,
      kind: "runtime.upserted" as const,
      version: 1,
      payload: {
        runtimeId,
        agentId,
        harness: "codex",
        transport: "acp",
        presence: "online",
        active: true,
      },
    });

    const staleIdentity = identity("a_stale", "stale-agent", 1);
    const staleRuntime = runtime("s_stale", "a_stale", 2);
    const currentIdentity = identity("a_current", "current-agent", 3);
    const currentRuntime = runtime("s_current", "a_current", 4);
    for (const event of [staleIdentity, staleRuntime, currentIdentity, currentRuntime]) {
      await applyCanonicalProjection(db, event);
    }

    await applyCanonicalProjection(db, {
      ...currentIdentity,
      daemonEpoch: "boot-2",
      seq: 1,
      occurredAt: 10,
    });
    await applyCanonicalProjection(db, {
      ...currentRuntime,
      daemonEpoch: "boot-2",
      seq: 2,
      occurredAt: 11,
    });

    expect((await db.execute(
      "SELECT runtime_id, status FROM runtime_descriptors ORDER BY runtime_id",
    )).rows).toMatchObject([
      { runtime_id: "s_current", status: "online" },
      { runtime_id: "s_stale", status: "stopped" },
    ]);
    expect((await db.execute(
      "SELECT agent_id FROM identities ORDER BY agent_id",
    )).rows).toMatchObject([
      { agent_id: "a_current" },
      { agent_id: "a_stale" },
    ]);
  });

  it("replays post-resume snapshots into missing and stopped descriptors without a spawn event", async () => {
    // Consumer-side composition guard. Producer registration + actual late connection replay
    // are exercised in core/crates/nexus/tests/registration_projection.rs.
    let seq = 0;
    for (const prior of ["missing", "stopped"] as const) {
      const agentId = `a_${prior}`;
      const runtimeId = `s_${prior}`;
      const apply = async (kind: "identity.upserted" | "runtime.upserted", payload: Record<string, unknown>) => {
        seq += 1;
        return applyCanonicalProjection(db, {
          eventId: `resume:${seq}`, daemonEpoch: "same-boot", seq, occurredAt: seq,
          kind, version: 1, payload,
        });
      };
      if (prior === "stopped") {
        await apply("identity.upserted", { agentId, name: prior, project: "default" });
        await apply("runtime.upserted", { agentId, runtimeId, sessionId: runtimeId, presence: "offline", active: false });
      }
      await apply("identity.upserted", { agentId, name: prior, project: "default", defaultHarness: "codex" });
      await apply("runtime.upserted", {
        agentId, runtimeId, sessionId: runtimeId, harness: "codex",
        presence: "online", active: true, stoppedAt: null,
      });
      expect((await db.execute({
        sql: "SELECT agent_id, session_id, status FROM runtime_descriptors WHERE runtime_id = ?",
        args: [runtimeId],
      })).rows).toMatchObject([{ agent_id: agentId, session_id: runtimeId, status: "online" }]);
    }
    expect((await db.execute("SELECT through_seq FROM projection_cursors WHERE daemon_epoch = 'same-boot'")).rows)
      .toMatchObject([{ through_seq: seq }]);
  });
});
