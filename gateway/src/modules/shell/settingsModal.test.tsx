// src/modules/shell/settingsModal.test.tsx
//
// SettingsModal contract tests.
// Verifies: (1) dialog opens from Sidebar Settings button, (2) Escape closes it,
// (3) accent swatch selects correctly and updates the store + dataset,
// (4) backdrop-dim segmented control selects correctly and updates the store +
// CSS property, (5) appearance values are re-applied on mount.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, fireEvent, within, waitFor } from "@testing-library/react";
import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";

import { AppProviders } from "@app/providers";
import { useUiPrefsStore } from "@app/uiPrefs";

import { AppShell } from "./AppShell";

// Suppress fetch (no server in unit tests).
beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(() => Promise.reject(new Error("no server in unit test"))),
  );
  // Reset store to defaults before each test.
  useUiPrefsStore.setState({ accent: "mono", backdropDim: 0.66 });
  // Reset dataset
  delete document.documentElement.dataset.accent;
  document.documentElement.style.removeProperty("--lens-backdrop-dim");
});
afterEach(() => {
  vi.unstubAllGlobals();
});

function renderShell() {
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

describe("SettingsModal", () => {
  it("opens when the Settings button in the Sidebar footer is clicked", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    const settingsBtn = within(rail).getByRole("button", { name: "Settings" });
    fireEvent.click(settingsBtn);
    expect(
      await screen.findByRole("dialog", { name: "Settings" }),
    ).toBeInTheDocument();
  });

  it("closes on Escape key press", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    fireEvent.click(within(rail).getByRole("button", { name: "Settings" }));
    const dialog = await screen.findByRole("dialog", { name: "Settings" });
    expect(dialog).toBeInTheDocument();
    fireEvent.keyDown(dialog, { key: "Escape" });
    await waitFor(() =>
      expect(screen.queryByRole("dialog", { name: "Settings" })).not.toBeInTheDocument(),
    );
  });

  it("selecting an accent swatch updates the store and document.documentElement.dataset.accent", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    fireEvent.click(within(rail).getByRole("button", { name: "Settings" }));
    const dialog = await screen.findByRole("dialog", { name: "Settings" });

    const accentGroup = within(dialog).getByRole("radiogroup", { name: /accent/i });
    const greenSwatch = within(accentGroup).getByRole("radio", { name: /green/i });
    fireEvent.click(greenSwatch);

    expect(useUiPrefsStore.getState().accent).toBe("green");
    expect(document.documentElement.dataset.accent).toBe("green");
  });

  it("selecting backdrop-dim updates the store and --lens-backdrop-dim CSS var", async () => {
    renderShell();
    const rail = await screen.findByRole("navigation", { name: "Primary" });
    fireEvent.click(within(rail).getByRole("button", { name: "Settings" }));
    const dialog = await screen.findByRole("dialog", { name: "Settings" });

    const dimGroup = within(dialog).getByRole("radiogroup", { name: /backdrop dim/i });
    const heavyOpt = within(dimGroup).getByRole("radio", { name: /heavy/i });
    fireEvent.click(heavyOpt);

    expect(useUiPrefsStore.getState().backdropDim).toBe(0.85);
    expect(
      document.documentElement.style.getPropertyValue("--lens-backdrop-dim"),
    ).toBe("0.85");
  });

  it("re-applies stored appearance to the document on mount (persistence after navigation)", async () => {
    // Pre-set store to non-default values before rendering.
    useUiPrefsStore.setState({ accent: "amber", backdropDim: 0.45 });
    renderShell();
    // The AppShell (or SettingsModal) must apply stored values on mount.
    await screen.findByRole("navigation", { name: "Primary" });
    await waitFor(() => {
      expect(document.documentElement.dataset.accent).toBe("amber");
      expect(
        document.documentElement.style.getPropertyValue("--lens-backdrop-dim"),
      ).toBe("0.45");
    });
  });
});
