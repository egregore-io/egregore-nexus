import { focusManager } from "@tanstack/react-query";
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AppProviders } from "./providers";
import { useAgentsLive, useDms } from "@modules/shell/useShellNav";

afterEach(() => {
  cleanup();
  focusManager.setFocused(undefined);
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function setup() {
  vi.useFakeTimers();
  focusManager.setFocused(true);
  const sockets = vi.fn();
  vi.stubGlobal("WebSocket", class {
    constructor() { sockets(); }
    addEventListener() {}
    close() {}
  });
  let online = true;
  const fetchSpy = vi.fn(async () => new Response(JSON.stringify([
    { name: "Agent", agentId: "a_test", kind: "agent", presence: online ? "online" : "offline" },
  ]), { status: 200 }));
  vi.stubGlobal("fetch", fetchSpy);
  const hook = renderHook(() => ({ dms: useDms(), live: useAgentsLive() }), {
    wrapper: AppProviders,
  });
  return { ...hook, sockets, fetchSpy, goOffline: () => { online = false; } };
}

async function advance(ms: number) {
  await act(async () => { await vi.advanceTimersByTimeAsync(ms); });
}

describe("HTTP-only shell", () => {
  it("never opens a WebSocket and refreshes the roster over HTTP", async () => {
    const probe = setup();
    await advance(1);
    expect(probe.result.current.dms.data?.[0]?.agentId).toBe("a_test");
    expect(probe.result.current.live.data).toBe(1);
    expect(probe.sockets).not.toHaveBeenCalled();

    probe.goOffline();
    await advance(30_001);
    expect(probe.fetchSpy.mock.calls.length).toBeGreaterThan(1);
    expect(probe.result.current.dms.data).toEqual([]);
    expect(probe.result.current.live.data).toBe(0);
    expect(probe.sockets).not.toHaveBeenCalled();
  });

  it("pauses periodic reads when hidden, refreshes on return, and stops on unmount", async () => {
    const probe = setup();
    await advance(1);
    act(() => { focusManager.setFocused(false); });
    const initialReads = probe.fetchSpy.mock.calls.length;
    probe.goOffline();
    await advance(60_000);
    expect(probe.fetchSpy).toHaveBeenCalledTimes(initialReads);

    act(() => { focusManager.setFocused(true); });
    await advance(1);
    expect(probe.result.current.live.data).toBe(0);
    expect(probe.fetchSpy.mock.calls.length).toBeGreaterThan(initialReads);
    probe.unmount();
    const finalReads = probe.fetchSpy.mock.calls.length;
    await advance(60_000);
    expect(probe.fetchSpy).toHaveBeenCalledTimes(finalReads);
  });
});
