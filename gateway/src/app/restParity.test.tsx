import type { PropsWithChildren } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useRegisterOperator } from "./identity";
import { useCreateThread } from "./projects";
import {
  useAddThreadMember,
  useAgentOp,
  useGrantTier,
  useDisableSource,
  useEnableSource,
  useRegisterSource,
  useRemoveSource,
  useRemoveThreadMember,
  useRotateSource,
  useSpawnAgent,
} from "@modules/pane/liveData";

interface FetchCall {
  url: string;
  method: string;
  body?: unknown;
}

function wrapper({ children }: PropsWithChildren) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

function installFetch(): FetchCall[] {
  const calls: FetchCall[] = [];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string"
      ? input
      : input instanceof URL
        ? input.toString()
        : input.url;
    const method = (init?.method ?? "GET").toUpperCase();
    const body = typeof init?.body === "string" ? JSON.parse(init.body) : undefined;
    calls.push({ url, method, body });
    return new Response(
      JSON.stringify({
        ok: true,
        name: "qa",
        token: "tok_qa",
        source: { name: "qa-source", topic: "qa", enabled: true, createdAt: 1 },
      }),
      { status: method === "POST" ? 201 : 200 },
    );
  }));
  return calls;
}

describe("UI REST parity", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("routes daemon-backed UI mutations through /api/v1 REST endpoints", async () => {
    const calls = installFetch();

    const operator = renderHook(() => useRegisterOperator(), { wrapper });
    await act(async () => {
      await operator.result.current.mutateAsync("operator");
    });

    const createThread = renderHook(() => useCreateThread(), { wrapper });
    await act(async () => {
      await createThread.result.current.mutateAsync({ name: "design", members: ["roman"] });
    });

    const spawnAgent = renderHook(() => useSpawnAgent(), { wrapper });
    await act(async () => {
      await spawnAgent.result.current.mutateAsync({ kind: "claude", name: "worker" });
    });

    const agentOp = renderHook(() => useAgentOp(), { wrapper });
    await act(async () => {
      await agentOp.result.current.mutateAsync({ name: "worker", op: "evict" });
    });

    const grantTier = renderHook(() => useGrantTier(), { wrapper });
    await act(async () => {
      await grantTier.result.current.mutateAsync({ name: "worker", tier: "admin" });
    });

    const addMember = renderHook(() => useAddThreadMember(), { wrapper });
    await act(async () => {
      await addMember.result.current.mutateAsync({ thread: "design", member: "worker" });
    });

    const removeMember = renderHook(() => useRemoveThreadMember(), { wrapper });
    await act(async () => {
      await removeMember.result.current.mutateAsync({ thread: "design", member: "worker" });
    });

    const registerSource = renderHook(() => useRegisterSource(), { wrapper });
    await act(async () => {
      await registerSource.result.current.mutateAsync({ name: "qa-source", topic: "qa" });
    });

    const enableSource = renderHook(() => useEnableSource(), { wrapper });
    await act(async () => {
      await enableSource.result.current.mutateAsync({ name: "qa-source" });
    });

    const disableSource = renderHook(() => useDisableSource(), { wrapper });
    await act(async () => {
      await disableSource.result.current.mutateAsync({ name: "qa-source" });
    });

    const rotateSource = renderHook(() => useRotateSource(), { wrapper });
    await act(async () => {
      await rotateSource.result.current.mutateAsync({ name: "qa-source" });
    });

    const removeSource = renderHook(() => useRemoveSource(), { wrapper });
    await act(async () => {
      await removeSource.result.current.mutateAsync({ name: "qa-source" });
    });

    expect(calls).toEqual([
      expect.objectContaining({ url: "/api/v1/register", method: "POST" }),
      expect.objectContaining({ url: "/api/v1/threads", method: "POST" }),
      expect.objectContaining({ url: "/api/v1/agents", method: "POST" }),
      expect.objectContaining({ url: "/api/v1/agents/worker?evict=1", method: "DELETE" }),
      expect.objectContaining({
        url: "/api/v1/agents/worker/tier",
        method: "POST",
        body: { tier: "admin" },
      }),
      expect.objectContaining({ url: "/api/v1/threads/design/members", method: "POST" }),
      expect.objectContaining({ url: "/api/v1/threads/design/members/worker", method: "DELETE" }),
      expect.objectContaining({ url: "/api/v1/sources", method: "POST" }),
      expect.objectContaining({ url: "/api/v1/sources/qa-source/enable", method: "POST" }),
      expect.objectContaining({ url: "/api/v1/sources/qa-source/disable", method: "POST" }),
      expect.objectContaining({ url: "/api/v1/sources/qa-source/rotate", method: "POST" }),
      expect.objectContaining({ url: "/api/v1/sources/qa-source", method: "DELETE" }),
    ]);
    expect(calls.every((call) => call.url.startsWith("/api/v1/"))).toBe(true);
  });
});
