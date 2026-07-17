import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@drizzle/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@drizzle/client")>();
  return {
    ...actual,
    getReadDb: vi.fn(() => ({})),
  };
});

vi.mock("@server/read/queries", () => ({
  agentAccessGrantsByAgentId: vi.fn(async () => []),
  agentOwnerByName: vi.fn(async () => undefined),
  whoamiRow: vi.fn(async () => ({ sessionId: "s_ben" })),
}));

vi.mock("@server/agui/agentSessionProjection", () => ({
  createMaterializedTurnRelay: vi.fn(() => () => ({
    ready: Promise.resolve(),
    close() {},
  })),
  loadAgentSessionSnapshot: vi.fn(async () => ({
    events: [],
    tailAfterId: 0,
    tailCursor: { finalizedAt: 0, rowid: 0 },
  })),
}));

vi.mock("@server/agui/streamStoreRelay", () => ({
  createStoreBackedAgentSessionRelay: vi.fn(),
  observeRawStream: vi.fn(() => new ReadableStream<Uint8Array>()),
  streamStoreFileExists: vi.fn(async () => false),
}));

vi.mock("@server/agui/http", () => ({
  handleAgentSession: vi.fn(() => new Response("ok", { status: 200 })),
  handleObserve: vi.fn(() => new Response("lane-a", { status: 200 })),
}));

describe("/api/agui/observe?session warm ingress", () => {
  afterEach(async () => {
    delete process.env.NEXUS_WEB_AUTH_MODE;
    vi.resetModules();
  });

  it("opens the webconsole agent pane by submitting harness.warm, not a spawn/duplicate-session path", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const { observeScoped } = await import("./agui.observe");
    const { localOperatorCaller } = await import("@server/auth/webAuthMode");
    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben"),
      { submitCommand },
    );

    expect(response.status).toBe(200);
    await vi.waitFor(() => expect(submitCommand).toHaveBeenCalledOnce());
    expect(submitCommand).toHaveBeenCalledWith(
      "harness.warm",
      { name: "ben" },
      localOperatorCaller(),
    );
  });

  it("resolves the session from Gateway-owned projection state in split-authority mode", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const canonicalDb = {
      execute: vi.fn(async () => ({
        rows: [{
          agent_id: "a_ben",
          name: "ben",
          owner: null,
          role: "agent",
          tier: "agent",
          metadata_json: '{"project":"default"}',
          session_id: "s_gateway_ben",
        }],
      })),
    };
    const { observeScoped } = await import("./agui.observe");
    const { getReadDb } = await import("@drizzle/client");
    vi.mocked(getReadDb).mockClear();

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben"),
      { submitCommand, canonicalDb: () => canonicalDb as never },
    );

    expect(response.status).toBe(200);
    expect(canonicalDb.execute).toHaveBeenCalledOnce();
    expect(getReadDb).not.toHaveBeenCalled();
  });
});
