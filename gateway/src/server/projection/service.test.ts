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

  it("hands only newly applied accepted messages to the receipt processor before ACK", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    let onFrame: ((frame: any) => void) | undefined;
    const receipt = vi.fn(async () => "completed" as const);
    const ackProjection = vi.fn(async () => undefined);
    const service = new GatewayProjectionService(db, {
      ready: Promise.resolve(),
      subscribeProjections: (handlers) => {
        onFrame = handlers.onFrame;
        return () => undefined;
      },
      ackProjection,
      close: vi.fn(),
    }, undefined, { afterReceipt: receipt });
    await service.start();

    const event = {
      eventId: "message:m_service",
      daemonEpoch: "boot-1",
      seq: 1,
      occurredAt: 10,
      kind: "message.accepted" as const,
      version: 1,
      payload: { messageId: "m_service", scope: "dm", body: "hello" },
    };
    onFrame?.({ t: "projection", event });
    await service.close();

    expect(receipt).toHaveBeenCalledTimes(1);
    expect(ackProjection).toHaveBeenCalledAfter(receipt);
    db.close();
  });
});
