// Part B proof — the shell rail/top-bar rendered from LIVE read-view data.
//
// Mounts the real AppShell (TopBar + Sidebar + ProjectSwitcher) and stubs `fetch`
// with REAL-SHAPED read-view JSON (the exact shapes `@server/read/queries`
// returns: ThreadRow[]/MemberRow[]/ProjectRow[]/WhoamiRow). Asserts the rail shows
// real channels (named threads), real DMs (members), the real agents-live count,
// and the real "me" footer — i.e. the seed is gone and the components draw the
// live read-view via `useShellNav` (TanStack Query). The wire under it (REST →
// router → queries over libSQL) is proven against a real daemon in the Part D run.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";

import { AppProviders } from "@app/providers";
import { useIdentityName, useIdentityStore } from "@app/identity";

import { AppShell } from "./AppShell";

// Real read-view response shapes (one consistent story across the endpoints).
const THREADS = [
  { name: "backend", members: ["etan", "ben"], lastAt: 1000 },
  { name: "design", members: ["ben"], lastAt: 2000 },
];
const MEMBERS = [
  { name: "ben", agent: "claude", role: "admin", presence: "online", currentWork: "gate" },
  { name: "dylan", agent: "claude", role: "agent", presence: "busy" },
  { name: "etan", agent: "other", kind: "human", role: "operator", presence: "online" },
];
const PROJECTS = [
  { projectId: "p_nexus", name: "egregore-nexus" },
  { projectId: "p_lens", name: "egregore-lens" },
];
const WHOAMI = {
  name: "etan",
  sessionId: "s_etan",
  kind: "human",
  role: "operator",
  tier: "admin",
  project: "egregore-nexus",
  presence: "online",
};

/** Route each read-view path to its real-shaped JSON. */
function fakeFetch(input: RequestInfo | URL): Promise<Response> {
  const url = typeof input === "string" ? input : input.toString();
  const body = url.includes("/api/v1/threads")
    ? THREADS
    : url.includes("/api/v1/members")
      ? MEMBERS
      : url.includes("/api/v1/projects")
        ? PROJECTS
        : url.includes("/api/v1/whoami")
          ? WHOAMI
          : null;
  return Promise.resolve(
    new Response(JSON.stringify(body), {
      status: body === null ? 404 : 200,
      headers: { "content-type": "application/json" },
    }),
  );
}

beforeEach(() => {
  window.localStorage.clear();
  useIdentityStore.setState({ name: null });
  vi.spyOn(window, "scrollTo").mockImplementation(() => {});
  vi.stubGlobal("fetch", vi.fn(fakeFetch));
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  window.localStorage.clear();
  useIdentityStore.setState({ name: null });
});

function IdentityProbe() {
  return <output data-testid="identity-name">{useIdentityName() ?? "none"}</output>;
}

function renderShell(initialPath = "/") {
  const rootRoute = createRootRoute({
    component: () => (
      <AppProviders>
        <IdentityProbe />
        <AppShell />
      </AppProviders>
    ),
  });
  const stub = (path: string) =>
    createRoute({
      getParentRoute: () => rootRoute,
      path,
      component: () => <div data-testid={`view-${path}`}>{path}</div>,
    });
  const routeTree = rootRoute.addChildren([
    stub("/"),
    stub("/pub"),
    stub("/admin"),
    stub("/c/$channel"),
    stub("/dm/$agent"),
  ]);
  const router = createRouter({
    routeTree,
    history: createMemoryHistory({ initialEntries: [initialPath] }),
  });
  return render(<RouterProvider router={router as never} />);
}

describe("shell — rendered from LIVE read-view data (no seed)", () => {
  it("renders real channels (named threads) in the rail", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    expect(await within(rail).findByText("backend")).toBeInTheDocument();
    expect(within(rail).getByText("design")).toBeInTheDocument();
  });

  it("renders real DMs (members) with presence + kind label", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    expect(await within(rail).findByText("ben")).toBeInTheDocument();
    expect(within(rail).getByText("dylan")).toBeInTheDocument();
    // The agent's kind label is its brain ("claude"), shown on the DM row.
    expect(within(rail).getAllByText("claude").length).toBeGreaterThan(0);
  });

  it("renders the real 'me' footer from whoami", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    // The footer identity from whoami: "etan" (also a DM row) + "operator" role.
    expect((await within(rail).findAllByText("etan")).length).toBeGreaterThan(0);
    expect(within(rail).getAllByText("operator").length).toBeGreaterThan(0);
  });

  it("shares whoami as the shell and pane identity authority while ignoring stale browser identity", async () => {
    window.localStorage.setItem("nexus.identity", JSON.stringify({
      state: { name: "ben" },
      version: 0,
    }));
    useIdentityStore.setState({ name: "ben" });
    const fetchSpy = vi.fn(fakeFetch);
    vi.stubGlobal("fetch", fetchSpy);

    renderShell();

    await waitFor(() => {
      expect(screen.getByTestId("identity-name")).toHaveTextContent("etan");
    });
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    expect((await within(rail).findAllByText("etan")).length).toBeGreaterThan(0);
    const whoamiCalls = fetchSpy.mock.calls.filter(([input]) =>
      String(input).includes("/api/v1/whoami"),
    );
    expect(whoamiCalls).toHaveLength(1);
  });

  it("treats logged-out whoami as an absent identity, not a shell query error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const url = typeof input === "string" ? input : input.toString();
        if (url.includes("/api/v1/whoami")) {
          return Promise.resolve(
            new Response(JSON.stringify({ error: "not logged in" }), {
              status: 401,
              headers: { "content-type": "application/json" },
            }),
          );
        }
        return fakeFetch(input);
      }),
    );

    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    expect(await within(rail).findByText("backend")).toBeInTheDocument();
    expect(await within(rail).findByText("—")).toBeInTheDocument();
  });

  it("shows the real agents-live count and the project switcher in the top bar", async () => {
    renderShell();
    // 2 members carry an `agent` brain (ben, dylan) → "2 agents live".
    expect(await screen.findByText(/2 agents live/)).toBeInTheDocument();
    // Projects are now a console-only grouping fed by `/api/projects` (gateway). With no groups
    // created, the switcher rests on its default "All threads" label rather than a daemon project.
    expect(screen.getByText("All threads")).toBeInTheDocument();
  });

  it("shares one canonical member request across DMs and the live-agent count", async () => {
    const fetchSpy = vi.fn(fakeFetch);
    vi.stubGlobal("fetch", fetchSpy);

    renderShell();
    expect(await screen.findByText(/2 agents live/)).toBeInTheDocument();

    const memberCalls = fetchSpy.mock.calls.filter(([input]) =>
      String(input).includes("/api/v1/members"),
    );
    expect(memberCalls).toHaveLength(1);
  });
});
