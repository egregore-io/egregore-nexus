import { expect, it, vi } from "vitest";

import { createHumanReadDeliveryMarker } from "./humanRead";

it("settles production human reads through the daemon IPC owner", async () => {
  const daemonMark = vi.fn(async () => ({ changed: 2 }));
  const marker = createHumanReadDeliveryMarker(undefined, {
    now: () => 9_000,
    daemonMark,
  });

  await expect(marker.markDelivered("human-session", ["m_1", "m_2", "m_1", " "]))
    .resolves.toBe(2);
  expect(daemonMark).toHaveBeenCalledWith({
    sessionId: "human-session",
    messageIds: ["m_1", "m_2"],
    now: 9_000,
  });
});
