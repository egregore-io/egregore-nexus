import { createClient } from "@libsql/client";
import { describe, expect, it } from "vitest";

import { bindLane } from "@server/store/repos/lane-bindings";
import { migrateGatewayStore } from "@server/store/migrations";
import {
  enqueueObligationsForMessage,
  listPendingObligations,
  obligationIdFor,
  settleObligation,
} from "./outbox";

describe("chat-addressed transport outbox", () => {
  it("fans one thread message out once per bound chat, not per external subject", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await bindLane(db, { provider: "telegram", externalChatId: "group-1", laneKind: "thread", laneName: "design" });
    await db.batch([
      "INSERT INTO principals VALUES ('x_1','external.human','guest',1)",
      "INSERT INTO principals VALUES ('x_2','external.human','guest',1)",
      "INSERT INTO principals VALUES ('x_3','external.human','guest',1)",
      "INSERT INTO subject_bindings VALUES ('telegram','u1','x_1','One',1)",
      "INSERT INTO subject_bindings VALUES ('telegram','u2','x_2','Two',1)",
      "INSERT INTO subject_bindings VALUES ('telegram','u3','x_3','Three',1)",
    ], "write");

    await expect(enqueueObligationsForMessage(db, message("m_group", "thread", "design")))
      .resolves.toBe(1);
    const rows = await listPendingObligations(db, "telegram", 256);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      externalChatId: "group-1",
      laneKind: "thread",
      laneName: "design",
      text: "hello",
    });
    db.close();
  });

  it("creates distinct deterministic obligations for two chats and replay adds none", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await bindLane(db, { provider: "telegram", externalChatId: "group-a", laneKind: "thread", laneName: "design" });
    await bindLane(db, { provider: "telegram", externalChatId: "group-b", laneKind: "thread", laneName: "design" });
    const row = message("m_two", "thread", "design");
    await expect(enqueueObligationsForMessage(db, row)).resolves.toBe(2);
    await expect(enqueueObligationsForMessage(db, row)).resolves.toBe(0);
    const pending = await listPendingObligations(db, "telegram", 256);
    expect(pending.map((item) => item.externalChatId).sort()).toEqual(["group-a", "group-b"]);
    expect(new Set(pending.map((item) => item.obligationId)).size).toBe(2);
    expect(pending.find((item) => item.externalChatId === "group-a")?.obligationId)
      .toBe(obligationIdFor("m_two", "telegram", "group-a"));
    db.close();
  });

  it("routes DMs by external principal, logs no-route, and settles idempotently", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await expect(enqueueObligationsForMessage(db, message("m_none", "dm", "x_missing")))
      .resolves.toBe(0);
    const log = await db.execute("SELECT message, data FROM logs ORDER BY seq DESC LIMIT 1");
    expect(log.rows[0]?.message).toBe("transport.no_route");
    expect(String(log.rows[0]?.data)).toContain("x_missing");

    await bindLane(db, { provider: "telegram", externalChatId: "private-7", laneKind: "dm", laneName: "x_person" });
    await expect(enqueueObligationsForMessage(db, message("m_dm", "dm", "x_person")))
      .resolves.toBe(1);
    const [pending] = await listPendingObligations(db, "telegram", 256);
    expect(pending?.externalChatId).toBe("private-7");
    await expect(settleObligation(db, pending!.obligationId, "external-1", 44)).resolves.toBe(true);
    await expect(settleObligation(db, pending!.obligationId, "external-1", 45)).resolves.toBe(false);
    await expect(listPendingObligations(db, "telegram", 256)).resolves.toEqual([]);
    db.close();
  });
});

function message(messageId: string, kind: "thread" | "dm", laneName: string) {
  return {
    messageId,
    kind,
    toName: laneName,
    body: "hello",
    createdAt: 10,
  };
}
