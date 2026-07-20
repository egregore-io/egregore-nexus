import { afterEach, describe, expect, it, vi } from "vitest";

const authMocks = vi.hoisted(() => ({
  currentHuman: vi.fn(),
}));

vi.mock("@server/identity/human", () => ({
  currentHuman: authMocks.currentHuman,
}));

vi.mock("@server/conversation/store", () => ({
  getConversationStore: vi.fn(async () => ({})),
}));

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
    authMocks.currentHuman.mockReset();
    vi.clearAllMocks();
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
      execute: vi.fn(async (query: string | { sql: string; args?: unknown[] }) => {
        const sql = typeof query === "string" ? query : query.sql;
        if (sql.includes("FROM identities WHERE agent_id")) return { rows: [] };
        if (sql.includes("FROM identities WHERE name")) {
          return {
            rows: [{
              agent_id: "a_ben",
              name: "ben",
              owner: null,
              role: "agent",
              tier: "agent",
              metadata_json: '{"project":"default"}',
            }],
          };
        }
        if (sql.includes("FROM runtime_descriptors")) {
          return { rows: [{ session_id: "s_gateway_ben", runtime_id: "r_gateway_ben" }] };
        }
        return { rows: [] };
      }),
    };
    const { observeScoped } = await import("./agui.observe");
    const { getReadDb } = await import("@drizzle/client");
    vi.mocked(getReadDb).mockClear();

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben"),
      { submitCommand, canonicalDb: () => canonicalDb as never },
    );

    expect(response.status).toBe(200);
    expect(canonicalDb.execute).toHaveBeenNthCalledWith(1, expect.objectContaining({
      sql: expect.stringContaining("WHERE agent_id = ?"),
      args: ["ben"],
    }));
    expect(canonicalDb.execute).toHaveBeenNthCalledWith(2, expect.objectContaining({
      sql: expect.stringContaining("WHERE name = ?"),
      args: ["ben"],
    }));
    expect(canonicalDb.execute).toHaveBeenNthCalledWith(3, expect.objectContaining({
      sql: expect.stringContaining("FROM runtime_descriptors"),
      args: ["a_ben"],
    }));
    expect(getReadDb).not.toHaveBeenCalled();
  });

  it("observes and warms by stable agent id when the supplied display name is stale", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const canonicalDb = {
      execute: vi.fn(async () => ({
        rows: [{
          agent_id: "a_ben",
          name: "current-ben",
          owner: null,
          role: "agent",
          tier: "agent",
          metadata_json: '{"project":"default"}',
          session_id: "s_gateway_ben",
        }],
      })),
    };
    const { observeScoped } = await import("./agui.observe");

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=stale-ben&agentId=a_ben"),
      { submitCommand, canonicalDb: () => canonicalDb as never },
    );

    expect(response.status).toBe(200);
    expect(canonicalDb.execute).toHaveBeenCalledWith(expect.objectContaining({ args: ["a_ben"] }));
    await vi.waitFor(() => expect(submitCommand).toHaveBeenCalledWith(
      "harness.warm",
      { name: "current-ben", agentId: "a_ben" },
      expect.any(Object),
    ));
  });

  it("returns 404 before warm or stream work when an explicit agent id is missing", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const canonicalDb = {
      execute: vi.fn(async (query: string | { sql: string; args?: unknown[] }) => {
        const sql = typeof query === "string" ? query : query.sql;
        if (sql.includes("FROM identities WHERE agent_id")) return { rows: [] };
        if (sql.includes("FROM identities WHERE name")) {
          return { rows: [{ agent_id: "a_alias", name: "valid-alias", metadata_json: "{}" }] };
        }
        return { rows: [] };
      }),
    };
    const projection = await import("@server/agui/agentSessionProjection");
    const stream = await import("@server/agui/streamStoreRelay");
    const agui = await import("@server/agui/http");
    const { observeScoped } = await import("./agui.observe");

    for (const query of [
      "session=valid-alias&agentId=a_missing",
      "agentId=a_missing",
    ]) {
      const response = await observeScoped(
        new Request(`http://localhost/api/agui/observe?${query}`),
        { submitCommand, canonicalDb: () => canonicalDb as never },
      );
      expect(response.status).toBe(404);
    }
    expect(submitCommand).not.toHaveBeenCalled();
    expect(projection.loadAgentSessionSnapshot).not.toHaveBeenCalled();
    expect(projection.createMaterializedTurnRelay).not.toHaveBeenCalled();
    expect(stream.streamStoreFileExists).not.toHaveBeenCalled();
    expect(stream.observeRawStream).not.toHaveBeenCalled();
    expect(stream.createStoreBackedAgentSessionRelay).not.toHaveBeenCalled();
    expect(agui.handleAgentSession).not.toHaveBeenCalled();
  });

  it("denies a same-name wrong-id observer before warm, snapshot, fanout, or stream", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "remote";
    authMocks.currentHuman.mockResolvedValue({
      name: "old-owner-name",
      agentId: "a_intruder",
      sessionId: "s_intruder",
      project: "default",
      kind: "agent",
      tier: "agent",
      clientKey: "ck_intruder",
    });
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const identityRow = {
      agent_id: "a_managed",
      name: "managed",
      owner: "old-owner-name",
      role: "agent",
      tier: "agent",
      metadata_json: JSON.stringify({ ownerAgentId: "a_owner" }),
    };
    const canonicalDb = {
      execute: vi.fn(async (query: string | { sql: string; args?: unknown[] }) => {
        const sql = typeof query === "string" ? query : query.sql;
        if (sql.includes("FROM identities WHERE agent_id")) return { rows: [identityRow] };
        if (sql.includes("FROM runtime_descriptors")) {
          return { rows: [{ session_id: "s_managed", runtime_id: "r_managed" }] };
        }
        return { rows: [] };
      }),
    };
    const projection = await import("@server/agui/agentSessionProjection");
    const stream = await import("@server/agui/streamStoreRelay");
    const agui = await import("@server/agui/http");
    const { observeScoped } = await import("./agui.observe");

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?agentId=a_managed", {
        headers: { cookie: "nexus_human=human-cookie" },
      }),
      { submitCommand, canonicalDb: () => canonicalDb as never },
    );

    expect(response.status).toBe(403);
    expect(submitCommand).not.toHaveBeenCalled();
    expect(projection.loadAgentSessionSnapshot).not.toHaveBeenCalled();
    expect(projection.createMaterializedTurnRelay).not.toHaveBeenCalled();
    expect(stream.streamStoreFileExists).not.toHaveBeenCalled();
    expect(stream.observeRawStream).not.toHaveBeenCalled();
    expect(stream.createStoreBackedAgentSessionRelay).not.toHaveBeenCalled();
    expect(agui.handleAgentSession).not.toHaveBeenCalled();
  });

  it("loads delegated access from the canonical Gateway identity projection", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "remote";
    authMocks.currentHuman.mockResolvedValue({
      name: "renamed-delegate",
      agentId: "a_delegate",
      project: "other-project",
      kind: "agent",
      tier: "agent",
      sessionId: "s_delegate",
      clientKey: "ck_delegate",
    });
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const identityRow = {
      agent_id: "a_managed",
      name: "managed",
      owner: "owner",
      role: "agent",
      tier: "agent",
      metadata_json: JSON.stringify({
        ownerAgentId: "a_owner",
        accessGrants: [{
          principalName: "old-delegate-name",
          principalAgentId: "a_delegate",
          principalProject: "stale-project",
          role: "viewer",
        }],
      }),
    };
    const canonicalDb = {
      execute: vi.fn(async (query: string | { sql: string; args?: unknown[] }) => {
        const sql = typeof query === "string" ? query : query.sql;
        const args = typeof query === "string" ? [] : query.args ?? [];
        if (sql.includes("FROM identities WHERE agent_id")) {
          return { rows: args[0] === "a_managed" ? [identityRow] : [] };
        }
        if (sql.includes("FROM identities WHERE name")) return { rows: [identityRow] };
        if (sql.includes("FROM runtime_descriptors")) {
          return { rows: [{ session_id: "s_managed", runtime_id: "r_managed" }] };
        }
        return { rows: [] };
      }),
    };
    const { observeScoped } = await import("./agui.observe");

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=managed", {
        headers: { cookie: "nexus_human=human-cookie" },
      }),
      { submitCommand, canonicalDb: () => canonicalDb as never },
    );

    expect(response.status).toBe(200);
    await vi.waitFor(() => expect(submitCommand).toHaveBeenCalledOnce());
  });

  it("rejects an ambiguous canonical name before warming any runtime", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const canonicalDb = {
      execute: vi.fn(async (query: string | { sql: string; args?: unknown[] }) => {
        const sql = typeof query === "string" ? query : query.sql;
        if (sql.includes("WHERE agent_id = ?")) return { rows: [] };
        if (sql.includes("WHERE name = ?")) {
          return {
            rows: [
              { agent_id: "a_one", name: "duplicate", metadata_json: "{}" },
              { agent_id: "a_two", name: "duplicate", metadata_json: "{}" },
            ],
          };
        }
        return { rows: [] };
      }),
    };
    const { observeScoped } = await import("./agui.observe");

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=duplicate"),
      { submitCommand, canonicalDb: () => canonicalDb as never },
    );

    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toMatchObject({
      error: { code: "ambiguous_agent" },
    });
    expect(submitCommand).not.toHaveBeenCalled();
  });

  it("rejects an ambiguous legacy owner-session alias before touching any session lane", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const queries = await import("@server/read/queries");
    const projection = await import("@server/agui/agentSessionProjection");
    const raw = await import("@server/agui/streamStoreRelay");
    const agui = await import("@server/agui/http");
    vi.mocked(queries.agentOwnerByName).mockRejectedValueOnce(
      new Error("ambiguous session name 'duplicate-owner'"),
    );
    const { observeScoped } = await import("./agui.observe");

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=duplicate-owner"),
      { submitCommand },
    );

    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toMatchObject({
      error: { code: "ambiguous_agent" },
    });
    expect(submitCommand).not.toHaveBeenCalled();
    expect(projection.loadAgentSessionSnapshot).not.toHaveBeenCalled();
    expect(projection.createMaterializedTurnRelay).not.toHaveBeenCalled();
    expect(raw.streamStoreFileExists).not.toHaveBeenCalled();
    expect(raw.observeRawStream).not.toHaveBeenCalled();
    expect(raw.createStoreBackedAgentSessionRelay).not.toHaveBeenCalled();
    expect(agui.handleAgentSession).not.toHaveBeenCalled();
  });

  it("rejects an ambiguous legacy whoami alias before touching any session lane", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const queries = await import("@server/read/queries");
    const projection = await import("@server/agui/agentSessionProjection");
    const raw = await import("@server/agui/streamStoreRelay");
    const agui = await import("@server/agui/http");
    vi.mocked(queries.whoamiRow).mockRejectedValueOnce(
      new Error("ambiguous session name 'duplicate-whoami'"),
    );
    const { observeScoped } = await import("./agui.observe");

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=duplicate-whoami"),
      { submitCommand },
    );

    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toMatchObject({
      error: { code: "ambiguous_agent" },
    });
    expect(submitCommand).not.toHaveBeenCalled();
    expect(projection.loadAgentSessionSnapshot).not.toHaveBeenCalled();
    expect(projection.createMaterializedTurnRelay).not.toHaveBeenCalled();
    expect(raw.streamStoreFileExists).not.toHaveBeenCalled();
    expect(raw.observeRawStream).not.toHaveBeenCalled();
    expect(raw.createStoreBackedAgentSessionRelay).not.toHaveBeenCalled();
    expect(agui.handleAgentSession).not.toHaveBeenCalled();
  });

  it("returns a typed legacy directory outage before warming or opening a stream", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const submitCommand = vi.fn(async () => ({ warmed: true }));
    const queries = await import("@server/read/queries");
    const projection = await import("@server/agui/agentSessionProjection");
    const agui = await import("@server/agui/http");
    vi.mocked(queries.whoamiRow).mockRejectedValueOnce(new Error("projection unavailable"));
    const { observeScoped } = await import("./agui.observe");

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben"),
      { submitCommand },
    );

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toMatchObject({
      error: { code: "agent_directory_unavailable" },
    });
    expect(submitCommand).not.toHaveBeenCalled();
    expect(projection.loadAgentSessionSnapshot).not.toHaveBeenCalled();
    expect(agui.handleAgentSession).not.toHaveBeenCalled();
  });
});
