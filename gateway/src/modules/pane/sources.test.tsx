// SourcesView + liveData sources hooks — component + mutation tests.
//
// Pattern: render SourcesView with a mocked global fetch that returns a fixture
// SourceListResponse; assert rows, action buttons, token reveal, and that
// mutations fire the right HTTP requests.

import { describe, expect, it, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, within, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { SourcesView } from "./SourcesView";
import type { Source } from "@shared/types/contracts.gen";

// ── fixtures ──────────────────────────────────────────────────────────────────

const now = Date.now();

const sources: Source[] = [
  {
    name: "github-ci",
    topic: "ci",
    enabled: true,
    createdAt: now - 3600_000,
    lastFiredAt: now - 60_000,
  },
  {
    name: "sentry-errors",
    topic: "errors",
    enabled: false,
    createdAt: now - 7200_000,
    lastFiredAt: undefined,
  },
];

const listResponse = { sources };

// ── test helpers ──────────────────────────────────────────────────────────────

function makeFetch(
  responses: Record<string, { status: number; body: unknown }>,
) {
  return vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const entry = responses[url];
    if (!entry) {
      return new Response(JSON.stringify({ error: "not found" }), { status: 404 });
    }
    return new Response(JSON.stringify(entry.body), { status: entry.status });
  });
}

function renderSources(fetchMock: typeof globalThis.fetch) {
  const qc = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, staleTime: 0 },
      mutations: { retry: false },
    },
  });
  vi.stubGlobal("fetch", fetchMock);
  return render(
    <QueryClientProvider client={qc}>
      <SourcesView />
    </QueryClientProvider>,
  );
}

// ── tests ─────────────────────────────────────────────────────────────────────

describe("SourcesView — rendering", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("renders a Sources heading and register form", async () => {
    const fetch = makeFetch({ "/api/v1/sources": { status: 200, body: listResponse } });
    renderSources(fetch);
    // PaneHead renders an h1; the section also has an h2 — both say "Sources"
    const headings = screen.getAllByRole("heading", { name: "Sources" });
    expect(headings.length).toBeGreaterThanOrEqual(1);
    expect(screen.getByRole("form", { name: "Register source" })).toBeInTheDocument();
  });

  it("renders one row per source with name, topic, and enabled state", async () => {
    const fetch = makeFetch({ "/api/v1/sources": { status: 200, body: listResponse } });
    renderSources(fetch);
    // Wait for the query to resolve
    const table = await screen.findByRole("table");
    expect(within(table).getByText("github-ci")).toBeInTheDocument();
    expect(within(table).getByText("ci")).toBeInTheDocument();
    expect(within(table).getByText("sentry-errors")).toBeInTheDocument();
    expect(within(table).getByText("errors")).toBeInTheDocument();
  });

  it("shows enabled/disabled badge per source", async () => {
    const fetch = makeFetch({ "/api/v1/sources": { status: 200, body: listResponse } });
    renderSources(fetch);
    await screen.findByRole("table");
    // github-ci is enabled, sentry-errors is disabled
    const rows = screen.getAllByRole("row");
    // row[0] is the header; row[1] = github-ci; row[2] = sentry-errors
    expect(within(rows[1]!).getByText("enabled")).toBeInTheDocument();
    expect(within(rows[2]!).getByText("disabled")).toBeInTheDocument();
  });

  it("shows last-fired relative time for github-ci and '—' for sentry-errors", async () => {
    const fetch = makeFetch({ "/api/v1/sources": { status: 200, body: listResponse } });
    renderSources(fetch);
    await screen.findByRole("table");
    // github-ci fired ~60s ago → '1m ago'
    expect(screen.getByText("1m ago")).toBeInTheDocument();
    // sentry-errors never fired
    expect(screen.getAllByText("—").length).toBeGreaterThan(0);
  });

  it("shows an empty state when there are no sources", async () => {
    const fetch = makeFetch({ "/api/v1/sources": { status: 200, body: { sources: [] } } });
    renderSources(fetch);
    expect(await screen.findByText("No sources yet")).toBeInTheDocument();
  });
});

describe("SourcesView — mutations", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("clicking Disable issues POST /api/v1/sources/:name/disable", async () => {
    const fetchMock = makeFetch({
      "/api/v1/sources": { status: 200, body: listResponse },
      "/api/v1/sources/github-ci/disable": { status: 200, body: { name: "github-ci" } },
    });
    renderSources(fetchMock);
    await screen.findByRole("table");
    fireEvent.click(screen.getByRole("button", { name: "Disable github-ci" }));
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/sources/github-ci/disable",
        expect.objectContaining({ method: "POST" }),
      ),
    );
  });

  it("clicking Enable issues POST /api/v1/sources/:name/enable", async () => {
    const fetchMock = makeFetch({
      "/api/v1/sources": { status: 200, body: listResponse },
      "/api/v1/sources/sentry-errors/enable": { status: 200, body: { name: "sentry-errors" } },
    });
    renderSources(fetchMock);
    await screen.findByRole("table");
    fireEvent.click(screen.getByRole("button", { name: "Enable sentry-errors" }));
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/sources/sentry-errors/enable",
        expect.objectContaining({ method: "POST" }),
      ),
    );
  });

  it("clicking Remove issues DELETE /api/v1/sources/:name", async () => {
    const fetchMock = makeFetch({
      "/api/v1/sources": { status: 200, body: listResponse },
      "/api/v1/sources/sentry-errors": { status: 204, body: null },
    });
    renderSources(fetchMock);
    await screen.findByRole("table");
    fireEvent.click(screen.getByRole("button", { name: "Remove sentry-errors" }));
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/sources/sentry-errors",
        expect.objectContaining({ method: "DELETE" }),
      ),
    );
  });

  it("clicking Rotate issues POST /api/v1/sources/:name/rotate", async () => {
    const fetchMock = makeFetch({
      "/api/v1/sources": { status: 200, body: listResponse },
      "/api/v1/sources/github-ci/rotate": {
        status: 200,
        body: { name: "github-ci", token: "new-token-xyz" },
      },
    });
    renderSources(fetchMock);
    await screen.findByRole("table");
    fireEvent.click(screen.getByRole("button", { name: "Rotate github-ci" }));
    await waitFor(() =>
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/sources/github-ci/rotate",
        expect.objectContaining({ method: "POST" }),
      ),
    );
  });

  it("rotate reveals the new token once", async () => {
    const fetchMock = makeFetch({
      "/api/v1/sources": { status: 200, body: listResponse },
      "/api/v1/sources/github-ci/rotate": {
        status: 200,
        body: { name: "github-ci", token: "new-token-xyz" },
      },
    });
    renderSources(fetchMock);
    await screen.findByRole("table");
    fireEvent.click(screen.getByRole("button", { name: "Rotate github-ci" }));
    expect(await screen.findByText("new-token-xyz")).toBeInTheDocument();
  });
});

describe("SourcesView — register form", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("submitting the register form issues POST /api/v1/sources", async () => {
    const newSource: Source = {
      name: "my-source",
      topic: "deploys",
      enabled: true,
      createdAt: Date.now(),
    };
    const fetchMock = makeFetch({
      "/api/v1/sources": { status: 200, body: listResponse },
      // POST to /api/v1/sources registers the new source
    });
    // Override to handle the POST specially
    const realFetch = fetchMock;
    const patchedFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : (input as Request).url;
      if (url === "/api/v1/sources" && init?.method === "POST") {
        return new Response(
          JSON.stringify({ source: newSource, token: "tok-abc123" }),
          { status: 201 },
        );
      }
      return realFetch(input, init);
    });

    const qc = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    vi.stubGlobal("fetch", patchedFetch);
    render(
      <QueryClientProvider client={qc}>
        <SourcesView />
      </QueryClientProvider>,
    );

    const nameInput = screen.getByRole("textbox", { name: /source name/i });
    const topicInput = screen.getByRole("textbox", { name: /topic/i });
    fireEvent.change(nameInput, { target: { value: "my-source" } });
    fireEvent.change(topicInput, { target: { value: "deploys" } });
    fireEvent.submit(screen.getByRole("form", { name: "Register source" }));

    await waitFor(() =>
      expect(patchedFetch).toHaveBeenCalledWith(
        "/api/v1/sources",
        expect.objectContaining({ method: "POST" }),
      ),
    );
  });

  it("register reveals the token returned by the server", async () => {
    const newSource: Source = {
      name: "my-source",
      topic: "deploys",
      enabled: true,
      createdAt: Date.now(),
    };
    const listFetch = makeFetch({
      "/api/v1/sources": { status: 200, body: listResponse },
    });
    const patchedFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : (input as Request).url;
      if (url === "/api/v1/sources" && init?.method === "POST") {
        return new Response(
          JSON.stringify({ source: newSource, token: "tok-super-secret" }),
          { status: 201 },
        );
      }
      return listFetch(input, init);
    });

    const qc = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    vi.stubGlobal("fetch", patchedFetch);
    render(
      <QueryClientProvider client={qc}>
        <SourcesView />
      </QueryClientProvider>,
    );

    const nameInput = screen.getByRole("textbox", { name: /source name/i });
    fireEvent.change(nameInput, { target: { value: "my-source" } });
    fireEvent.submit(screen.getByRole("form", { name: "Register source" }));

    expect(await screen.findByText("tok-super-secret")).toBeInTheDocument();
  });
});

describe("SourcesView — copy recipe", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("renders a copy recipe button per source row", async () => {
    const fetchMock = makeFetch({ "/api/v1/sources": { status: 200, body: listResponse } });
    renderSources(fetchMock);
    await screen.findByRole("table");
    expect(screen.getByRole("button", { name: /copy recipe.*github-ci/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /copy recipe.*sentry-errors/i })).toBeInTheDocument();
  });
});
