// Presentational shell CONTRACT — the pure `Sidebar`/`TopBar` render the rail
// IA when fed nav fixtures as props.
//
// The runtime path is LIVE (no seed defaults): `useShellNav` populates these from
// the read-view. This file feeds local fixtures to the now-pure components and
// asserts the rows (channels, DMs, teams, me footer, agents-live pill, project
// switcher) render through the real components. The router is needed because rail
// rows are `<Link>`s.
import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";

import { TooltipProvider } from "@shared/ui";

import { Sidebar } from "./Sidebar/Sidebar";
import { TopBar } from "./TopBar";
import type {
  ChannelNavItem,
  DmNavItem,
  MeIdentity,
  ProjectNavItem,
  TeamNavItem,
} from "./nav";

// ── local test fixtures (not runtime data) ────────────────────────────────────
const FX_CHANNELS: ChannelNavItem[] = [
  { id: "backend", name: "backend", count: 3 },
  { id: "design", name: "design" },
  { id: "ops", name: "ops", count: 7, alert: true },
];
const FX_DMS: DmNavItem[] = [
  { id: "a_ben", agentId: "a_ben", name: "ben", kind: "agent", presence: "online", kindLabel: "agent" },
  { id: "a_dylan", agentId: "a_dylan", name: "dylan", kind: "agent", presence: "busy", kindLabel: "agent" },
];
const FX_TEAMS: TeamNavItem[] = [{ id: "core", name: "core" }];
const FX_ME: MeIdentity = { name: "erin", role: "operator", presence: "online" };
const FX_PROJECTS: ProjectNavItem[] = [
  { projectId: "p_nexus", name: "egregore-nexus", presence: "online" },
  { projectId: "p_lens", name: "egregore-lens", presence: "online" },
];
const FX_AGENTS_LIVE = 4;

/** Render `node` inside a memory router so `<Link>`s resolve. */
function renderRouted(node: React.ReactNode) {
  const rootRoute = createRootRoute({
    component: () => <TooltipProvider>{node}</TooltipProvider>,
  });
  const stub = (path: string) =>
    createRoute({
      getParentRoute: () => rootRoute,
      path,
      component: () => <div>{path}</div>,
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
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  return render(<RouterProvider router={router as never} />);
}

describe("Sidebar — rows from seed fixtures (props)", () => {
  it("renders the seeded channels, DMs, teams, and me footer", async () => {
    renderRouted(
      <Sidebar
        channels={FX_CHANNELS}
        dms={FX_DMS}
        teams={FX_TEAMS}
        me={FX_ME}
      />,
    );
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    expect(within(rail).getByText("backend")).toBeInTheDocument();
    expect(within(rail).getByText("ben")).toBeInTheDocument();
    expect(within(rail).getByText("core")).toBeInTheDocument();
    expect(within(rail).getByText("erin")).toBeInTheDocument();
    expect(within(rail).getByText("operator")).toBeInTheDocument();
  });

  it("renders empty-state rows when given no data", async () => {
    renderRouted(<Sidebar channels={[]} dms={[]} teams={[]} />);
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    expect(within(rail).getByText("No channels yet")).toBeInTheDocument();
    expect(within(rail).getByText("No members online")).toBeInTheDocument();
    expect(within(rail).getByText("No teams")).toBeInTheDocument();
  });

  it("opens a durable DM by stable agent id", async () => {
    renderRouted(
      <Sidebar channels={[]} dms={FX_DMS} teams={[]} />,
    );

    const link = await screen.findByRole("link", { name: /ben/i });
    expect(link).toHaveAttribute("href", "/dm/a_ben");
    expect(link).not.toHaveAttribute("href", "/agent/ben:s_ben");
  });
});

describe("TopBar — header from seed fixtures (props)", () => {
  it("renders the agents-live pill and the active project in the switcher", async () => {
    // The switcher shows the ACTIVE group on the trigger ("All threads" when none); the other
    // groups live in the dropdown. Pass an active id so the trigger names that group.
    renderRouted(
      <TopBar
        agentsLive={FX_AGENTS_LIVE}
        projects={FX_PROJECTS}
        activeProjectId="p_nexus"
      />,
    );
    expect(
      await screen.findByText(`${FX_AGENTS_LIVE} agents live`),
    ).toBeInTheDocument();
    expect(screen.getByText("egregore-nexus")).toBeInTheDocument();
  });
});
