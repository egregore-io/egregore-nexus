import { describe, expect, it, vi } from "vitest";

import { GatewayChangeBus } from "./changeBus";

describe("Gateway in-process change bus", () => {
  it("wakes target waiters once and advances a monotonic revision", async () => {
    const bus = new GatewayChangeBus();
    const wake = vi.fn();
    const unsubscribe = bus.subscribe("thread:design", wake);

    expect(bus.publish("thread:design")).toBe(1);
    expect(bus.publish("thread:other")).toBe(1);
    expect(wake).toHaveBeenCalledTimes(1);
    expect(wake).toHaveBeenCalledWith(1);
    unsubscribe();
    bus.publish("thread:design");
    expect(wake).toHaveBeenCalledTimes(1);
  });

  it("resolves a bounded waiter and removes an aborted waiter", async () => {
    const bus = new GatewayChangeBus();
    const waiting = bus.waitForChange("dm:a_ada", 0, { timeoutMs: 1000 });
    bus.publish("dm:a_ada");
    await expect(waiting).resolves.toBe(1);

    const abort = new AbortController();
    const aborted = bus.waitForChange("dm:a_ada", 1, {
      timeoutMs: 1000,
      signal: abort.signal,
    });
    abort.abort();
    await expect(aborted).resolves.toBe(1);
    expect(bus.waiterCount("dm:a_ada")).toBe(0);
  });
});
