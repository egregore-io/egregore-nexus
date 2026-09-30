import type { PropsWithChildren } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import type { MemberRow } from "@shared/readView";
import { useAgentsLive, useDms } from "@modules/shell/useShellNav";
import { useAdminAgents, useTierFacts } from "./liveData";

afterEach(() => vi.unstubAllGlobals());

it("classifies canonical and legacy agents consistently across shell and pane", async () => {
  const agentKinds = ["agent", "local.agent", "external.agent", "trusted.agent"];
  const rejectedKinds = ["human", "local.human", "external.human", "trusted.human",
    "app", "local.app", "notification", "trusted.notification", "unknown.agent",
    "local.agent.extra", ".agent", "agent.", "LOCAL.agent", " local.agent", ""];
  const kinds = [...agentKinds, ...rejectedKinds, undefined];
  const rows: MemberRow[] = kinds.map((kind, i) => ({
    name: `member${i}`, sessionId: `s_${i}`, agentId: `a_${i}`, kind,
    agent: "claude", presence: i === 1 ? "busy" : "online",
  }));
  rows.push({ name: "offline", sessionId: "s_offline", agentId: "a_offline",
    kind: "local.agent", presence: "offline" });
  rows.push({ name: "noKindOrHarness", sessionId: "s_none", agentId: "a_none", presence: "online" });
  const expectedLive = rows.filter((_, i) => i < 4 || i === kinds.length - 1).map((m) => m.name);
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(rows), { status: 200 })));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  const wrapper = ({ children }: PropsWithChildren) =>
    <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  const hook = renderHook(() => ({
    count: useAgentsLive(), dms: useDms(), admin: useAdminAgents(), facts: useTierFacts(),
  }), { wrapper });
  await waitFor(() => expect(hook.result.current.count.isSuccess).toBe(true));
  expect(hook.result.current.count.data).toBe(5);
  expect(hook.result.current.dms.data?.filter((m) => m.kind === "agent").map((m) => m.name))
    .toEqual(expectedLive);
  expect(hook.result.current.admin.rows.map((m) => m.name)).toEqual([...expectedLive, "offline"]);
  expect(hook.result.current.facts.facts.find((f) => f.dt === "Agents")?.dd).toBe("6");
});
