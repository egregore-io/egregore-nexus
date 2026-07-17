// src/modules/shell/collapse.test.tsx
import { describe, expect, it, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRouter,
} from "@tanstack/react-router";
import { useUiPrefsStore } from "@app/uiPrefs";
import { PanelToggle, TopBar } from "./TopBar";

function renderTopBar() {
  const rootRoute = createRootRoute({ component: () => <TopBar /> });
  const router = createRouter({
    routeTree: rootRoute,
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  return render(<RouterProvider router={router as never} />);
}

describe("PanelToggle", () => {
  beforeEach(() =>
    useUiPrefsStore.setState({ collapsed: { rail: false, ctx: false, search: true } }),
  );

  it("toggles the rail collapse flag and reflects aria-pressed", () => {
    render(<PanelToggleHarness />);
    const btn = screen.getByRole("button", { name: /toggle sidebar/i });
    expect(btn).toHaveAttribute("aria-expanded", "true");
    fireEvent.click(btn);
    expect(useUiPrefsStore.getState().collapsed.rail).toBe(true);
    expect(btn).toHaveAttribute("aria-expanded", "false");
    expect(btn).toHaveAttribute("aria-pressed", "true");
  });
});

describe("TopBar collapsible search", () => {
  beforeEach(() =>
    useUiPrefsStore.setState({ collapsed: { rail: false, ctx: false, search: true } }),
  );

  it("'/' expands + focuses search; Escape collapses it", async () => {
    renderTopBar();   // TopBar reads the store directly
    // Wait for the component to mount (router is async); the icon-trigger is the sentinel.
    await screen.findByRole("button", { name: /search/i });
    // collapsed.search starts true → only the icon-trigger shows; input is hidden.
    // Pressing "/" must set collapsed.search=false and reveal+focus the input.
    fireEvent.keyDown(document, { key: "/" });
    const input = await screen.findByRole("searchbox", { name: /search/i });
    expect(useUiPrefsStore.getState().collapsed.search).toBe(false);
    expect(document.activeElement).toBe(input);
    fireEvent.keyDown(input, { key: "Escape" });
    expect(useUiPrefsStore.getState().collapsed.search).toBe(true);
  });

  it("blur with empty value collapses search", async () => {
    renderTopBar();
    // Click the search icon button to open search
    const searchBtn = await screen.findByRole("button", { name: /search/i });
    fireEvent.click(searchBtn);
    // Wait for the expanded input to appear
    const input = await screen.findByRole("searchbox", { name: /search/i });
    expect(useUiPrefsStore.getState().collapsed.search).toBe(false);
    // Blur with empty value should collapse
    fireEvent.blur(input);
    expect(useUiPrefsStore.getState().collapsed.search).toBe(true);
  });
});

function PanelToggleHarness() {
  const collapsed = useUiPrefsStore((s) => s.collapsed.rail);
  const toggle = useUiPrefsStore((s) => s.togglePanel);
  return (
    <PanelToggle
      panel="rail"
      label="Toggle sidebar"
      collapsed={collapsed}
      onToggle={() => toggle("rail")}
    />
  );
}
