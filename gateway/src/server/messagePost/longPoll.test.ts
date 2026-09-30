import { describe, expect, it, vi } from "vitest";

import { GatewayChangeBus } from "../store/changeBus";
import { longPollCanonicalPage } from "./longPoll";

describe("canonical message long poll", () => {
  it("waits on the in-process change bus, re-queries once, and releases aborts", async () => {
    const bus = new GatewayChangeBus();
    const read = vi.fn()
      .mockResolvedValueOnce({ rows: [], after: "cursor", before: "cursor", rebased: false })
      .mockResolvedValueOnce({ rows: [], after: "cursor", before: "cursor", rebased: false })
      .mockResolvedValueOnce({
        rows: [{ messageId: "m_1" }], after: "next", before: "next", rebased: false,
      });
    const waiting = longPollCanonicalPage({
      key: "thread:design",
      after: "cursor",
      waitMs: 30_000,
      bus,
      read,
    });
    await vi.waitFor(() => expect(bus.waiterCount("thread:design")).toBe(1));
    bus.publish("thread:design");
    await expect(waiting).resolves.toMatchObject({ rows: [{ messageId: "m_1" }] });
    expect(read).toHaveBeenCalledTimes(3);
    expect(bus.waiterCount("thread:design")).toBe(0);
  });
});
