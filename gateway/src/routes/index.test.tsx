import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { IndexView } from "./-IndexView";

// IndexView renders the #backend conversation pane wired to the LIVE AG-UI
// `observe` stream and the live read-view (`useChannelView`/`useRoster` over
// TanStack Query). With no EventSource (jsdom) the thread renders EMPTY — there
// is no seed fallback at runtime — but the head + composer chrome still mount
// from the route param. The context-panel content portals through the shell's
// slot provider (absent in isolation), so it simply renders nothing here.

/** Render IndexView inside a QueryClient; the read-view returns empty here. */
function renderIndex() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <IndexView />
    </QueryClientProvider>,
  );
}

describe("index route — #backend pane", () => {
  beforeEach(() => {
    // Read-view calls resolve to empty arrays so the chrome renders with no rows.
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("[]", { status: 200 })),
    );
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders the pane head for #backend", () => {
    renderIndex();
    expect(screen.getByRole("heading", { name: "backend" })).toBeInTheDocument();
  });

  it("renders an EMPTY thread (no seed fallback) when there is no live stream", () => {
    renderIndex();
    const log = screen.getByRole("log");
    // No seeded rows/chips are injected at runtime.
    expect(within(log).queryByText("etan")).not.toBeInTheDocument();
    expect(log).toBeEmptyDOMElement();
  });

  it("renders the composer with the channel placeholder", () => {
    renderIndex();
    expect(screen.getByRole("form", { name: "Message #backend" })).toBeInTheDocument();
    expect(screen.getByRole("textbox")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Send" })).toBeInTheDocument();
  });
});
