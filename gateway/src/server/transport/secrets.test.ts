import { createClient } from "@libsql/client";
import { describe, expect, it, vi } from "vitest";

import { Tier } from "@shared/types";
import { handle } from "@server/api/router";
import { migrateGatewayStore } from "@server/store/migrations";
import {
  removeTransportSecret,
  resolveTransportSecret,
  resolveTransportSecretRefs,
  setTransportSecret,
} from "./secrets";

describe("transport secrets", () => {
  it("sets, resolves, rotates, maps, and removes without returning values", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await expect(setTransportSecret(db, "telegram.token", "first", 10))
      .resolves.toEqual({ key: "telegram.token", updatedAt: 10 });
    await expect(resolveTransportSecret(db, "telegram.token")).resolves.toBe("first");
    await expect(setTransportSecret(db, "telegram.token", "second", 11))
      .resolves.toEqual({ key: "telegram.token", updatedAt: 11 });
    await expect(resolveTransportSecretRefs(db, { TELEGRAM_TOKEN: "telegram.token" }))
      .resolves.toEqual({ TELEGRAM_TOKEN: "second" });
    await expect(removeTransportSecret(db, "telegram.token")).resolves.toEqual({
      key: "telegram.token",
      removed: true,
    });
    await expect(resolveTransportSecret(db, "telegram.token")).resolves.toBeUndefined();
    db.close();
  });

  it("exposes authenticated admin POST/DELETE routes without echoing the value", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    const deps = { db: vi.fn(), canonicalDb: () => db };
    const unauthenticated = await handle({
      method: "POST",
      path: "/api/v1/admin/transport/secrets",
      query: {}, headers: {}, body: { key: "fake.token", value: "never-echo-me" },
    }, deps);
    expect(unauthenticated.status).toBe(401);

    const caller = { name: "operator", project: "default", tier: Tier.Admin };
    const set = await handle({
      method: "POST",
      path: "/api/v1/admin/transport/secrets",
      query: {}, headers: {}, caller, body: { key: "fake.token", value: "never-echo-me" },
    }, deps);
    expect(set.status).toBe(200);
    expect(JSON.stringify(set.body)).not.toContain("never-echo-me");
    await expect(resolveTransportSecret(db, "fake.token")).resolves.toBe("never-echo-me");

    const removed = await handle({
      method: "DELETE",
      path: "/api/v1/admin/transport/secrets/fake.token",
      query: {}, headers: {}, caller,
    }, deps);
    expect(removed.status).toBe(200);
    expect(JSON.stringify(removed.body)).not.toContain("never-echo-me");
    db.close();
  });
});
