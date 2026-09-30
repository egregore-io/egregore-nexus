import { createClient } from "@libsql/client";
import { describe, expect, it, vi } from "vitest";

import { migrateGatewayStore } from "../store/migrations";
import { GatewayProjectionService } from "./service";

describe("Gateway projection service", () => {
  it("owns one daemon projection subscription for the whole Gateway process", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    const unsubscribe = vi.fn();
    const subscribeProjections = vi.fn(() => unsubscribe);
    const service = new GatewayProjectionService(db, {
      ready: Promise.resolve(),
      subscribeProjections,
      ackProjection: vi.fn(async () => undefined),
      close: vi.fn(),
    });
    await Promise.all([service.start(), service.start()]);
    expect(subscribeProjections).toHaveBeenCalledTimes(1);
    await service.close();
    expect(unsubscribe).toHaveBeenCalledTimes(1);
    db.close();
  });
});
