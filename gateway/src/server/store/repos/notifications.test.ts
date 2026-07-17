import { createClient, type Client } from "@libsql/client";
import { beforeEach, describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../migrations";
import { getNotification, insertNotification } from "./notifications";

describe("Gateway notification repository", () => {
  let db: Client;
  beforeEach(async () => {
    db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
  });

  it("round-trips explicit targets and optional source without registration", async () => {
    await insertNotification(db, {
      notificationId: "n1",
      messageId: "m1",
      source: "crit",
      target: { kind: "agent", agentId: "a_ada" },
      summary: "Review complete",
      body: "The review is ready.",
      createdAt: 10,
    });

    expect(await getNotification(db, "n1")).toEqual({
      notificationId: "n1",
      messageId: "m1",
      source: "crit",
      target: { kind: "agent", agentId: "a_ada" },
      summary: "Review complete",
      body: "The review is ready.",
      createdAt: 10,
    });
  });
});
