import { createClient, type Client } from "@libsql/client";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { GatewayChangeBus } from "../changeBus";
import { migrateGatewayStore } from "../migrations";
import {
  encodeMessageCursor,
  insertCanonicalMessage,
  pageCanonicalMessages,
  type CanonicalGatewayMessage,
} from "./messages";

function message(
  messageId: string,
  createdAt: number,
  overrides: Partial<CanonicalGatewayMessage> = {},
): CanonicalGatewayMessage {
  return {
    messageId,
    kind: "thread",
    fromName: "ada",
    fromAgentId: "a_ada",
    threadId: "t_design",
    body: messageId,
    provenance: { project: "metadata-only" },
    createdAt,
    ...overrides,
  };
}

describe("Gateway canonical message repository", () => {
  let db: Client;
  beforeEach(async () => {
    db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
  });

  it("pages latest, before and after in stable display order without overlap", async () => {
    for (const [id, at] of [["m1", 1], ["m2", 2], ["m3", 3]] as const) {
      await insertCanonicalMessage(db, message(id, at));
    }

    const latest = await pageCanonicalMessages(db, { threadId: "t_design" }, { limit: 2 });
    expect(latest.messages.map((row) => row.messageId)).toEqual(["m2", "m3"]);
    const older = await pageCanonicalMessages(
      db,
      { threadId: "t_design" },
      { limit: 2, before: latest.before },
    );
    expect(older.messages.map((row) => row.messageId)).toEqual(["m1"]);
    const newer = await pageCanonicalMessages(
      db,
      { threadId: "t_design" },
      { limit: 5, after: older.after },
    );
    expect(newer.messages.map((row) => row.messageId)).toEqual(["m2", "m3"]);
  });

  it("rebases malformed and stale cursors to a bounded latest page", async () => {
    await insertCanonicalMessage(db, message("m1", 1));
    await insertCanonicalMessage(db, message("m2", 2));

    const malformed = await pageCanonicalMessages(
      db,
      { threadId: "t_design" },
      { limit: 1, after: "not-a-cursor" },
    );
    expect(malformed.rebased).toBe(true);
    expect(malformed.messages.map((row) => row.messageId)).toEqual(["m2"]);

    const stale = await pageCanonicalMessages(
      db,
      { threadId: "t_design" },
      { limit: 1, after: encodeMessageCursor(99, "missing") },
    );
    expect(stale.rebased).toBe(true);
    expect(stale.messages.map((row) => row.messageId)).toEqual(["m2"]);
  });

  it("treats the virtual origin cursor as a valid empty-tail anchor", async () => {
    const empty = await pageCanonicalMessages(
      db,
      { threadId: "t_design" },
      { limit: 10, after: "origin" },
    );
    expect(empty).toEqual({ messages: [], rebased: false });

    await insertCanonicalMessage(db, message("m1", 1));
    const first = await pageCanonicalMessages(
      db,
      { threadId: "t_design" },
      { limit: 10, after: "origin" },
    );
    expect(first.messages.map((row) => row.messageId)).toEqual(["m1"]);
    expect(first.rebased).toBe(false);
  });

  it("does not partition uniqueness or reads by project metadata", async () => {
    await insertCanonicalMessage(db, message("m1", 1, { provenance: { project: "one" } }));
    await insertCanonicalMessage(db, message("m2", 2, { provenance: { project: "two" } }));
    expect(
      (await pageCanonicalMessages(db, { threadId: "t_design" }, { limit: 10 })).messages.map(
        (row) => row.messageId,
      ),
    ).toEqual(["m1", "m2"]);
  });

  it("signals the target change bus once only after a committed insert", async () => {
    const bus = new GatewayChangeBus();
    const listener = vi.fn();
    bus.subscribe("thread:t_design", listener);

    expect(await insertCanonicalMessage(db, message("m1", 1), bus)).toBe("inserted");
    expect(listener).toHaveBeenCalledTimes(1);
    expect(await insertCanonicalMessage(db, message("m1", 1), bus)).toBe("duplicate");
    expect(listener).toHaveBeenCalledTimes(1);
  });
});
