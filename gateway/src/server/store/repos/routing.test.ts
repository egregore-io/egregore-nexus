import { createClient, type Client } from "@libsql/client";
import { beforeEach, describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../migrations";
import {
  declareThread,
  listThreadMemberIds,
  setThreadMember,
  upsertIdentity,
} from "./routing";

describe("Gateway routing projection repository", () => {
  let db: Client;
  beforeEach(async () => {
    db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
  });

  it("stores project only as identity metadata and routes by stable ids", async () => {
    await upsertIdentity(db, {
      agentId: "a_ada",
      name: "ada",
      role: "agent",
      tier: "Member",
      metadata: { project: "one" },
      updatedAt: 1,
    });
    await upsertIdentity(db, {
      agentId: "a_ben",
      name: "ben",
      role: "agent",
      tier: "Member",
      metadata: { project: "two" },
      updatedAt: 1,
    });
    await declareThread(db, {
      threadId: "t_design",
      name: "design",
      createdAt: 1,
      updatedAt: 1,
    });
    await setThreadMember(db, "t_design", "a_ada", 1);
    await setThreadMember(db, "t_design", "a_ben", 1);

    expect(await listThreadMemberIds(db, "t_design")).toEqual(["a_ada", "a_ben"]);
  });
});
