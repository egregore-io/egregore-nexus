import { afterEach, describe, expect, it, vi } from "vitest";
import type { BaseEvent } from "@ag-ui/client";

const mocks = vi.hoisted(() => ({
  agentAccessGrantsByAgentId: vi.fn(async () => []),
  agentOwnerByName: vi.fn(async () => undefined),
  createMaterializedTurnRelay: vi.fn(() => () => ({
    ready: Promise.resolve(),
    close() {},
  })),
  createStoreBackedAgentSessionRelay: vi.fn(() => () => ({
    ready: Promise.resolve(),
    close() {},
  })),
  handleAgentSession: vi.fn(() => new Response("session-ok", { status: 200 })),
  handleObserve: vi.fn(() => new Response("lane-a", { status: 200 })),
  loadAgentSessionSnapshot: vi.fn(),
  materializedCursorForStreamAfter: vi.fn(),
  observeRawStream: vi.fn(() => new ReadableStream<Uint8Array>()),
  streamStoreFileExists: vi.fn(async () => true),
  submitCommandIntent: vi.fn(async () => ({ ok: true })),
  whoamiRow: vi.fn(async () => ({ sessionId: "s_ben" })),
}));

vi.mock("@drizzle/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@drizzle/client")>();
  return {
    ...actual,
    getReadDb: vi.fn(() => ({})),
  };
});

vi.mock("@server/read/queries", () => ({
  agentAccessGrantsByAgentId: mocks.agentAccessGrantsByAgentId,
  agentOwnerByName: mocks.agentOwnerByName,
  whoamiRow: mocks.whoamiRow,
}));

vi.mock("@server/agui/agentSessionProjection", () => ({
  createMaterializedTurnRelay: mocks.createMaterializedTurnRelay,
  loadAgentSessionSnapshot: mocks.loadAgentSessionSnapshot,
  materializedCursorForStreamAfter: mocks.materializedCursorForStreamAfter,
}));

vi.mock("@server/agui/streamStoreRelay", () => ({
  createStoreBackedAgentSessionRelay: mocks.createStoreBackedAgentSessionRelay,
  observeRawStream: mocks.observeRawStream,
  streamStoreFileExists: mocks.streamStoreFileExists,
}));

vi.mock("@server/agui/http", () => ({
  handleAgentSession: mocks.handleAgentSession,
  handleObserve: mocks.handleObserve,
}));

vi.mock("@server/command/ingress", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@server/command/ingress")>();
  return {
    ...actual,
    submitCommandIntent: mocks.submitCommandIntent,
  };
});

function snapshot(events: BaseEvent[] = []) {
  return {
    events,
    tailAfterId: 7,
    tailCursor: { finalizedAt: 100, rowid: 9 },
  };
}

describe("/api/agui/observe?session reconnect cursor", () => {
  afterEach(() => {
    delete process.env.NEXUS_WEB_AUTH_MODE;
    vi.clearAllMocks();
    vi.resetModules();
  });

  it("uses afterId as the live stream cursor while suppressing snapshot replay", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    mocks.loadAgentSessionSnapshot.mockResolvedValueOnce(snapshot([]));
    mocks.materializedCursorForStreamAfter.mockResolvedValueOnce({ finalizedAt: 50, rowid: 5 });

    const { observeScoped } = await import("./agui.observe");
    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben&afterId=42"),
    );

    expect(response.status).toBe(200);
    expect(mocks.loadAgentSessionSnapshot).toHaveBeenCalledWith("s_ben", "ben", {
      historyTurns: 0,
    });
    expect(mocks.materializedCursorForStreamAfter).toHaveBeenCalledWith("s_ben", 42);
    expect(mocks.createStoreBackedAgentSessionRelay).toHaveBeenCalledWith("s_ben", {
      streamAfterId: 42,
      fallbackAfterCursor: { finalizedAt: 50, rowid: 5 },
    });
    expect(mocks.handleAgentSession).toHaveBeenCalledWith(expect.any(Request), {
      sessionName: "ben",
      initialEvents: [],
      createRelay: expect.any(Function),
    });
  });

  it("starts from the snapshot tail when no reconnect cursor is supplied", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const history = [{ type: "RUN_STARTED", threadId: "session:ben", runId: "r1" } as BaseEvent];
    mocks.loadAgentSessionSnapshot.mockResolvedValueOnce(snapshot(history));

    const { observeScoped } = await import("./agui.observe");
    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben"),
    );

    expect(response.status).toBe(200);
    expect(mocks.loadAgentSessionSnapshot).toHaveBeenCalledWith("s_ben", "ben", {
      historyTurns: undefined,
    });
    expect(mocks.createStoreBackedAgentSessionRelay).toHaveBeenCalledWith("s_ben", {
      streamAfterId: 7,
      fallbackAfterCursor: { finalizedAt: 100, rowid: 9 },
    });
    expect(mocks.handleAgentSession).toHaveBeenCalledWith(expect.any(Request), {
      sessionName: "ben",
      initialEvents: history,
      createRelay: expect.any(Function),
    });
  });

  it("honors historyTurns=0 for a cursor-less tail-only attach", async () => {
    // A recorder with no durable cursor can request only the live tail,
    // avoiding the cost of replaying the default history snapshot.
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    mocks.loadAgentSessionSnapshot.mockResolvedValueOnce(snapshot([]));

    const { observeScoped } = await import("./agui.observe");
    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben&historyTurns=0"),
    );

    expect(response.status).toBe(200);
    expect(mocks.loadAgentSessionSnapshot).toHaveBeenCalledWith("s_ben", "ben", {
      historyTurns: 0,
    });
    expect(mocks.createStoreBackedAgentSessionRelay).toHaveBeenCalledWith("s_ben", {
      streamAfterId: 7,
      fallbackAfterCursor: { finalizedAt: 100, rowid: 9 },
    });
    expect(mocks.handleAgentSession).toHaveBeenCalledWith(expect.any(Request), {
      sessionName: "ben",
      initialEvents: [],
      createRelay: expect.any(Function),
    });
  });

  it("checks stream-store readiness in parallel with snapshot hydration", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    let releaseSnapshot!: (value: ReturnType<typeof snapshot>) => void;
    mocks.loadAgentSessionSnapshot.mockImplementationOnce(() => new Promise((resolve) => {
      releaseSnapshot = resolve;
    }));
    mocks.streamStoreFileExists.mockResolvedValueOnce(true);

    const { observeScoped } = await import("./agui.observe");
    const response = observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben"),
    );

    await vi.waitFor(() => {
      expect(mocks.loadAgentSessionSnapshot).toHaveBeenCalledTimes(1);
      expect(mocks.streamStoreFileExists).toHaveBeenCalledTimes(1);
    });
    releaseSnapshot(snapshot([]));
    expect((await response).status).toBe(200);
  });

  it("does not fetch delegated grants when the caller is already the owner", async () => {
    process.env.NEXUS_WEB_AUTH_MODE = "local";
    const { localOperatorCaller } = await import("@server/auth/webAuthMode");
    const caller = localOperatorCaller();
    mocks.agentOwnerByName.mockResolvedValueOnce({
      agentId: "a_ben",
      name: "ben",
      project: "default",
      ownerName: caller.name,
      ownerProject: caller.project,
    } as never);
    mocks.loadAgentSessionSnapshot.mockResolvedValueOnce(snapshot([]));

    const { observeScoped } = await import("./agui.observe");
    expect((await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ben"),
    )).status).toBe(200);
    expect(mocks.agentAccessGrantsByAgentId).not.toHaveBeenCalled();
  });
});
