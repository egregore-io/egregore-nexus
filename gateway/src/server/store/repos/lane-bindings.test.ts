import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createClient, type Client } from "@libsql/client";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../migrations";
import {
  bindLane,
  chatsForLane,
  LaneBindingConflictError,
  laneForChat,
} from "./lane-bindings";

describe("Gateway transport lane bindings", () => {
  let db: Client;

  beforeEach(async () => {
    db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
  });

  afterEach(() => db.close());

  it("is idempotent for one lane and fails typed on a conflicting lane", async () => {
    const row = {
      provider: "telegram",
      externalChatId: "chat-1",
      laneKind: "thread" as const,
      laneName: "design",
    };
    await expect(bindLane(db, row)).resolves.toEqual(row);
    await expect(bindLane(db, row)).resolves.toEqual(row);
    await expect(bindLane(db, { ...row, laneName: "other" })).rejects.toMatchObject({
      name: "LaneBindingConflictError",
      code: "transport_lane_conflict",
    } satisfies Partial<LaneBindingConflictError>);
    await expect(laneForChat(db, "telegram", "chat-1")).resolves.toEqual(row);
  });

  it("returns every chat bound to one lane in canonical order", async () => {
    await bindLane(db, {
      provider: "telegram",
      externalChatId: "chat-2",
      laneKind: "thread",
      laneName: "design",
    });
    await bindLane(db, {
      provider: "matrix",
      externalChatId: "chat-1",
      laneKind: "thread",
      laneName: "design",
    });
    await expect(chatsForLane(db, "thread", "design")).resolves.toEqual([
      { provider: "matrix", externalChatId: "chat-1", laneKind: "thread", laneName: "design" },
      { provider: "telegram", externalChatId: "chat-2", laneKind: "thread", laneName: "design" },
    ]);
  });

  it("resolves identically after the database is reopened", async () => {
    db.close();
    const root = await mkdtemp(join(tmpdir(), "nexus-v016-lane-"));
    const url = `file:${join(root, "gateway.db")}`;
    try {
      db = createClient({ url });
      await migrateGatewayStore(db);
      await bindLane(db, {
        provider: "telegram",
        externalChatId: "chat-reopen",
        laneKind: "dm",
        laneName: "x_peer",
      });
      db.close();
      db = createClient({ url });
      await migrateGatewayStore(db);
      await expect(laneForChat(db, "telegram", "chat-reopen")).resolves.toEqual({
        provider: "telegram",
        externalChatId: "chat-reopen",
        laneKind: "dm",
        laneName: "x_peer",
      });
    } finally {
      db.close();
      db = createClient({ url: ":memory:" });
      await rm(root, { recursive: true, force: true });
    }
  });
});
