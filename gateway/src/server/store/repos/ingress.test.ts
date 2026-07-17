import { createClient, type Client } from "@libsql/client";
import { beforeEach, describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../migrations";
import { beginIngress, getIngress, settleIngress } from "./ingress";

describe("Gateway ingress idempotency repository", () => {
  let db: Client;
  beforeEach(async () => {
    db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
  });

  it("returns the original request and terminal result for duplicate idempotency", async () => {
    const first = await beginIngress(db, {
      idempotencyKey: "key-1",
      request: { target: "ada", body: "hello" },
      now: 1,
    });
    const duplicate = await beginIngress(db, {
      idempotencyKey: "key-1",
      request: { target: "ben", body: "different" },
      now: 2,
    });
    expect(first.created).toBe(true);
    expect(duplicate.created).toBe(false);
    expect(duplicate.row.request).toEqual({ target: "ada", body: "hello" });

    await settleIngress(db, "key-1", {
      commandId: "c1",
      status: "accepted",
      result: { messageId: "m1" },
      now: 3,
    });
    expect(await getIngress(db, "key-1")).toMatchObject({
      commandId: "c1",
      status: "accepted",
      result: { messageId: "m1" },
    });
  });
});
