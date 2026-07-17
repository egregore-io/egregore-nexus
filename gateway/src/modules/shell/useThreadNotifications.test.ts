// The notification hook's change detection: which threads count as "new
// activity" between two polls of the threads read-view. The DOM/Notification
// side is deliberately thin glue (verified live on the demo rig); the logic
// that decides WHEN to toast is what must not regress — especially "the first
// snapshot baselines, the backlog never toasts".
import { describe, expect, it } from "vitest";

import type { ThreadRow } from "@server/read/queries";

import { advancedThreads } from "./useThreadNotifications";

const row = (
  name: string,
  latestSeq?: number,
  lastAt?: number,
): ThreadRow => ({ name, members: [], latestSeq, lastAt });

describe("advancedThreads", () => {
  it("returns nothing when stamps are unchanged", () => {
    const seen = new Map([
      ["builds", 5],
      ["backend", 2],
    ]);
    expect(advancedThreads(seen, [row("builds", 5), row("backend", 2)])).toEqual([]);
  });

  it("returns threads whose seq advanced and updates the seen map", () => {
    const seen = new Map([["builds", 5]]);
    expect(advancedThreads(seen, [row("builds", 7)])).toEqual(["builds"]);
    expect(seen.get("builds")).toBe(7);
    // A second identical poll is quiet — no re-toast for the same message.
    expect(advancedThreads(seen, [row("builds", 7)])).toEqual([]);
  });

  it("treats first-seen threads as baseline, not activity", () => {
    // A thread created (or subscribed) mid-session shows up with history the
    // human never saw land — baseline it silently, toast only its NEXT event.
    const seen = new Map([["builds", 5]]);
    expect(advancedThreads(seen, [row("builds", 5), row("ops", 9)])).toEqual([]);
    expect(advancedThreads(seen, [row("builds", 5), row("ops", 10)])).toEqual(["ops"]);
  });

  it("falls back to lastAt when latestSeq is absent", () => {
    const seen = new Map([["builds", 1000]]);
    expect(advancedThreads(seen, [row("builds", undefined, 2000)])).toEqual(["builds"]);
  });

  it("never fires on regression (stamp went backwards)", () => {
    // e.g. read-view rebuilt / db swapped under us — stay quiet, re-baseline.
    const seen = new Map([["builds", 9]]);
    expect(advancedThreads(seen, [row("builds", 3)])).toEqual([]);
    expect(seen.get("builds")).toBe(3);
  });

  it("handles a burst across multiple threads in one poll", () => {
    const seen = new Map([
      ["builds", 1],
      ["backend", 1],
      ["quiet", 1],
    ]);
    expect(
      advancedThreads(seen, [row("builds", 2), row("backend", 3), row("quiet", 1)]),
    ).toEqual(["builds", "backend"]);
  });
});
