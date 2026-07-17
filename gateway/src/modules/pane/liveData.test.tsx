import type { PropsWithChildren } from "react";
import { QueryClient, QueryClientProvider, useQueryClient } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { qk } from "@server/read/keys";

import {
  useAddThreadMember,
  useDmView,
  usePubFeed,
  usePubRuleFacts,
  useRenameThread,
} from "./liveData";
import type { ThreadRow } from "@server/read/queries";

function wrapper({ children }: PropsWithChildren) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
    },
  });
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

function wrapperWithClient(queryClient: QueryClient) {
  return function ClientWrapper({ children }: PropsWithChildren) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("liveData polling", () => {
  it("keeps the DM name and adds stable identity when the roster knows it", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(JSON.stringify([
          {
            name: "ben",
            agentId: "a_ben",
            sessionId: "s_ben",
            presence: "online",
          },
        ]), { status: 200 }),
      ),
    );

    const hook = renderHook(() => useDmView("ben"), { wrapper });
    await waitFor(() => {
      expect(hook.result.current.target).toEqual({
        verb: "dm",
        name: "ben",
        agentId: "a_ben",
      });
    });
  });

  it("refreshes Pub feed and routing facts without a manual reload", async () => {
    vi.useFakeTimers();
    let notificationCalls = 0;
    let routingCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;
      if (url === "/api/v1/notifications") {
        notificationCalls += 1;
        return new Response(JSON.stringify(
          notificationCalls === 1
            ? []
            : [{
                notifId: "n1",
                source: "github",
                topic: "builds",
                routedTo: ["roman"],
                when: Date.now(),
              }],
        ), { status: 200 });
      }
      if (url === "/api/v1/routing-rules") {
        routingCalls += 1;
        return new Response(JSON.stringify(
          routingCalls === 1
            ? []
            : [{ source: "github", to: "roman" }],
        ), { status: 200 });
      }
      return new Response(JSON.stringify([]), { status: 200 });
    }));

    const pub = renderHook(() => usePubFeed(), { wrapper });
    const rules = renderHook(() => usePubRuleFacts(), { wrapper });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(pub.result.current.rows).toEqual([]);
    expect(rules.result.current.facts).toEqual([]);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000);
      await vi.runOnlyPendingTimersAsync();
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(notificationCalls).toBeGreaterThan(1);
    expect(routingCalls).toBeGreaterThan(1);
    expect(pub.result.current.rows[0]?.src).toBe("github");
    expect(rules.result.current.facts[0]).toEqual({ dt: "github →", dd: "roman" });
  });

  it("does not run read-view fallback polling while the tab is hidden", async () => {
    vi.useFakeTimers();
    vi.spyOn(document, "hidden", "get").mockReturnValue(true);
    const fetchSpy = vi.fn(async () =>
      new Response(JSON.stringify([]), { status: 200 }),
    );
    vi.stubGlobal("fetch", fetchSpy);

    renderHook(() => usePubFeed(), { wrapper });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(fetchSpy).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000);
    });

    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });
});

describe("thread mutation cache updates", () => {
  it("adds a thread member in cache without invalidation fan-out", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
    queryClient.setQueryData<ThreadRow[]>(qk.threads(), [
      { name: "ops", members: ["roman"], lastAt: 1 },
    ]);
    const invalidate = vi.spyOn(queryClient, "invalidateQueries");
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify({ ok: true }), { status: 200 })),
    );

    const hook = renderHook(() => useAddThreadMember(), {
      wrapper: wrapperWithClient(queryClient),
    });
    await act(async () => {
      await hook.result.current.mutateAsync({ thread: "ops", member: "ada" });
    });

    expect(queryClient.getQueryData<ThreadRow[]>(qk.threads())?.[0]?.members).toEqual([
      "roman",
      "ada",
    ]);
    expect(invalidate).not.toHaveBeenCalled();
  });
});

describe("useRenameThread", () => {
  it("patches thread.rename and updates the thread list optimistically", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
    queryClient.setQueryData<ThreadRow[]>(qk.threads(), [
      { name: "ops", members: ["roman"], lastAt: 1 },
    ]);
    const fetchSpy = vi.fn(async () =>
      new Response(JSON.stringify({ previous: "ops", name: "launch" }), { status: 200 }),
    );
    const invalidate = vi.spyOn(queryClient, "invalidateQueries");
    vi.stubGlobal("fetch", fetchSpy);

    const hook = renderHook(
      () => {
        const rename = useRenameThread();
        const client = useQueryClient();
        return { rename, client };
      },
      { wrapper: wrapperWithClient(queryClient) },
    );

    await act(async () => {
      await hook.result.current.rename.mutateAsync({ thread: "ops", name: "launch" });
    });

    expect(fetchSpy).toHaveBeenCalledWith("/api/v1/threads/ops", {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "launch" }),
    });
    expect(hook.result.current.client.getQueryData<ThreadRow[]>(qk.threads())?.[0]?.name)
      .toBe("launch");
    expect(invalidate).not.toHaveBeenCalled();
  });

  it("restores the thread list and surfaces the daemon error when rename is denied", async () => {
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
    queryClient.setQueryData<ThreadRow[]>(qk.threads(), [
      { name: "ops", members: ["roman"], lastAt: 1 },
    ]);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(JSON.stringify({ error: "unauthorized" }), { status: 403 }),
      ),
    );

    const hook = renderHook(() => useRenameThread(), {
      wrapper: wrapperWithClient(queryClient),
    });

    await expect(
      act(async () => {
        await hook.result.current.mutateAsync({ thread: "ops", name: "launch" });
      }),
    ).rejects.toThrow("unauthorized");
    expect(queryClient.getQueryData<ThreadRow[]>(qk.threads())?.[0]?.name).toBe("ops");
  });
});
