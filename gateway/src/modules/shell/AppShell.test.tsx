import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";

import { AppProviders } from "@app/providers";

import { AppShell } from "./AppShell";

// The shell is now fed by the LIVE read-view (`useShellNav` → fetch `/api/v1/*`).
// These structural tests don't stand up a server, so we stub `fetch` to reject —
// the queries deterministically land in their EMPTY state and we assert the
// shell frame (landmarks, groups, brand, search) renders regardless. Live row
// rendering from real data is proven in `liveNav.test.tsx`; the row
// contract (seed fixtures through the presentational components) in `shellNav.test.tsx`.
beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(() => Promise.reject(new Error("no server in unit test"))),
  );
});
afterEach(() => {
  vi.unstubAllGlobals();
});

// A self-contained router whose root renders the shell, plus trivial child routes
// for every path the rail links to (so the <Link>s resolve). This mounts the real
// shell (TopBar + Sidebar + ContextPanelHost) without the document-level __root,
// which would nest <html> inside the test container.
function renderShell(initialPath = "/") {
  const rootRoute = createRootRoute({
    component: () => (
      <AppProviders>
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
    stub("/members"),
    stub("/pub"),
    stub("/admin"),
    stub("/search"),
    stub("/c/$channel"),
    stub("/dm/$agent"),
  ]);

  const router = createRouter({
    routeTree,
    history: createMemoryHistory({ initialEntries: [initialPath] }),
  });

  return render(<RouterProvider router={router as never} />);
}

describe("AppShell — structural frame", () => {
  it("renders the NEXUS brand wordmark", async () => {
    renderShell();
    expect(await screen.findByText("NEXUS")).toBeInTheDocument();
  });

  it("renders the project switcher and the global search input", async () => {
    renderShell();
    expect(
      await screen.findByRole("button", { name: "Switch project" }),
    ).toBeInTheDocument();
    // No project groups (fetch rejected) → the switcher shows its default "All threads" label.
    expect(screen.getByText("All threads")).toBeInTheDocument();
    // Search starts collapsed (uiPrefs default) → the icon-trigger is present.
    // Clicking it expands to the full searchbox.
    const searchTrigger = screen.getByRole("button", { name: "Search" });
    expect(searchTrigger).toBeInTheDocument();
    expect(searchTrigger).toHaveAttribute("aria-expanded", "false");
  });

  it("renders the live-agents pill and a notifications affordance", async () => {
    renderShell();
    expect(await screen.findByText(/agents live/)).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Notifications" }),
    ).toBeInTheDocument();
  });

  it("renders the grouped rail (Channels / Direct messages / Teams / Pub feed / Admin)", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    for (const label of [
      "Channels",
      "Direct messages",
      "Teams",
      "Pub feed",
      "Admin",
    ]) {
      expect(within(rail).getByText(label)).toBeInTheDocument();
    }
    // With no live data the rail shows no SEEDED rows — the seed is gone from the
    // runtime path (the groups render skeleton/empty states instead).
    expect(within(rail).queryByText("backend")).not.toBeInTheDocument();
    expect(within(rail).queryByText("ben")).not.toBeInTheDocument();
    expect(within(rail).queryByText("core")).not.toBeInTheDocument();
  });

  it("renders the 'me' footer (settings affordance; identity from live whoami)", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    // No live whoami (fetch rejected) → no seeded "erin"/"operator".
    expect(within(rail).queryByText("erin")).not.toBeInTheDocument();
    expect(within(rail).queryByText("operator")).not.toBeInTheDocument();
    expect(
      within(rail).getByRole("button", { name: "Settings" }),
    ).toBeInTheDocument();
  });

  it("exposes the main pane and the context-panel landmarks", async () => {
    renderShell();
    expect(
      await screen.findByRole("main", { name: "Conversation" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("complementary", { name: "Details" }),
    ).toBeInTheDocument();
  });
});
