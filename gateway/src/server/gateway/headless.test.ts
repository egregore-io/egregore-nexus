import { createRequire } from "node:module";
import type { AddressInfo } from "node:net";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "@libsql/client";

import { describe, expect, it, vi } from "vitest";

import { removeTempPath } from "../../test/removeTempPath";
import { CURRENT_GATEWAY_SCHEMA_VERSION } from "../store/migrations";

vi.mock("../../routes/api/agui.observe", () => ({
  observeScoped: vi.fn(async () => json({ ok: true, route: "agent-session" })),
}));

import {
  createHeadlessGatewayServer,
  handleHeadlessGatewayRequest,
  migrateHeadlessGatewayStore,
  type HeadlessGatewayDispatchers,
} from "./headless";

interface TestWebSocket {
  once(event: "open", listener: () => void): void;
  once(event: "message", listener: (data: unknown) => void): void;
  once(event: "error", listener: (error: Error) => void): void;
  send(data: string): void;
  close(): void;
}

const { WebSocket } = createRequire(import.meta.url)("ws") as {
  WebSocket: new (
    url: string,
    protocols?: string[],
    options?: { headers?: Record<string, string> },
  ) => TestWebSocket;
};

function waitForWsFrame(
  socket: TestWebSocket,
  predicate: (frame: Record<string, unknown>) => boolean,
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("timed out waiting for WebSocket frame")), 2_000);
    const next = () => socket.once("message", (data) => {
      let frame: Record<string, unknown>;
      try {
        frame = JSON.parse(String(data)) as Record<string, unknown>;
      } catch (error) {
        clearTimeout(timeout);
        reject(error);
        return;
      }
      if (predicate(frame)) {
        clearTimeout(timeout);
        resolve(frame);
      } else {
        next();
      }
    });
    socket.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    next();
  });
}

function dispatchers(): HeadlessGatewayDispatchers {
  return {
    aguiObserve: vi.fn(async () => new Response("data: {}\n\n", {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    })),
    apiV1: vi.fn(async () => json({ ok: true, route: "api" })),
    networkMcp: vi.fn(async () => json({ ok: true, route: "mcp" })),
    webuiApi: vi.fn(async () => json({ ok: true, route: "webui-api" })),
  };
}

describe("headless gateway", () => {
  it("starts hooks before projection and closes both services", async () => {
    const calls: string[] = [];
    const afterReceipt = vi.fn(async () => undefined);
    const server = await createHeadlessGatewayServer(dispatchers(), {
      async startHooks() {
        calls.push("hooks:start");
        return { afterReceipt };
      },
      async startProjection(options) {
        calls.push("projection:start");
        expect(options.afterReceipt).toEqual(expect.any(Function));
        await options.afterReceipt?.({} as never);
        expect(afterReceipt).toHaveBeenCalledOnce();
      },
      async stopProjection() { calls.push("projection:stop"); },
      async stopHooks() { calls.push("hooks:stop"); },
      async stopConnection() { calls.push("connection:stop"); },
    });

    expect(calls).toEqual(["hooks:start", "projection:start"]);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    await server.shutdown();
    expect(calls).toEqual([
      "hooks:start",
      "projection:start",
      "projection:stop",
      "hooks:stop",
      "connection:stop",
    ]);
  });

  it("supports a v0.1.0 baseline-only activation step without starting the server", async () => {
    const dir = await mkdtemp(join(tmpdir(), "nexus-gateway-migrate-"));
    const path = join(dir, "gateway.db");
    try {
      const result = await migrateHeadlessGatewayStore({
        ...process.env,
        NEXUS_GATEWAY_DB: `file:${path}`,
      });
      expect(result).toEqual({ schemaVersion: CURRENT_GATEWAY_SCHEMA_VERSION });
      const db = createClient({ url: `file:${path}` });
      const rows = await db.execute(
        "SELECT version FROM gateway_schema_migrations ORDER BY version DESC LIMIT 1",
      );
      expect(Number(rows.rows[0]?.version)).toBe(result.schemaVersion);
      expect((await db.execute("SELECT COUNT(*) AS count FROM notifications")).rows[0])
        .toMatchObject({ count: 0 });
      db.close();
    } finally {
      await removeTempPath(dir, { recursive: true });
    }
  });

  it("streams the first observe frame before the upstream response closes", async () => {
    const deps = dispatchers();
    let closeUpstream: (() => void) | undefined;
    deps.aguiObserve = vi.fn(async () => {
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode("data: first\n\n"));
          closeUpstream = () => controller.close();
        },
      }), {
        headers: { "content-type": "text/event-stream" },
      });
    });
    const server = await createHeadlessGatewayServer(deps);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;

    try {
      const firstRead = fetch(`http://127.0.0.1:${port}/api/agui/observe?thread=streaming`)
        .then(async (response) => {
          const reader = response.body!.getReader();
          const chunk = await reader.read();
          await reader.cancel();
          return chunk;
        });
      const outcome = await new Promise<
        { type: "frame"; chunk: ReadableStreamReadResult<Uint8Array> }
        | { type: "blocked" }
      >((resolve) => {
        // This is a streaming-order contract, not a 100 ms performance benchmark. The complete
        // gateway suite shares two capped CPUs, so leave scheduler margin while still failing
        // decisively if the implementation buffers until the deliberately-open upstream closes.
        const timer = setTimeout(() => resolve({ type: "blocked" }), 2_000);
        void firstRead.then((chunk) => {
          clearTimeout(timer);
          resolve({ type: "frame", chunk });
        });
      });

      expect(outcome.type).toBe("frame");
      if (outcome.type === "frame") {
        expect(new TextDecoder().decode(outcome.chunk.value)).toContain("data: first");
      }
    } finally {
      closeUpstream?.();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  });

  it("accepts the public AG-UI WebSocket upgrade without the webconsole", async () => {
    const server = await createHeadlessGatewayServer(dispatchers());
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;
    const socket = new WebSocket(`ws://127.0.0.1:${port}/api/agui/ws`);

    try {
      await new Promise<void>((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
      });
      socket.send(JSON.stringify({ t: "ping" }));
      const frame = await new Promise<unknown>((resolve, reject) => {
        socket.once("message", (data) => resolve(JSON.parse(String(data))));
        socket.once("error", reject);
      });

      expect(frame).toEqual({ t: "pong" });
    } finally {
      socket.close();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  });

  it("binds browser cookie CSRF at upgrade before routing a WebSocket mutation", async () => {
    const deps = dispatchers();
    const routed: Request[] = [];
    deps.webuiApi = vi.fn(async (request: Request) => {
      routed.push(request);
      return json({
        ok: true,
        receipt: {
          commandId: "cmd_csrf",
          clientMessageId: "cm_csrf",
          sessionId: "s_otto",
          state: "queued",
          revision: 1,
          seq: 1,
        },
      }, 201);
    });
    const server = await createHeadlessGatewayServer(deps);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;
    const socket = new WebSocket(
      `ws://127.0.0.1:${port}/api/agui/ws`,
      ["nexus-v1", "nexus-csrf.csrf-ws"],
      { headers: { cookie: "nexus_human=human-1; nexus_csrf=csrf-ws" } },
    );

    try {
      await new Promise<void>((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
      });
      socket.send(JSON.stringify({
        t: "input",
        mode: "session",
        target: "otto",
        text: "hello",
        clientMessageId: "cm_csrf",
      }));
      const frame = await new Promise<unknown>((resolve, reject) => {
        socket.once("message", (data) => resolve(JSON.parse(String(data))));
        socket.once("error", reject);
      });

      expect(frame).toMatchObject({ t: "input.ack", commandId: "cmd_csrf" });
      expect(routed).toHaveLength(1);
      expect(routed[0]!.headers.get("x-nexus-csrf")).toBe("csrf-ws");
      expect(routed[0]!.headers.get("cookie")).toContain("nexus_csrf=csrf-ws");
    } finally {
      socket.close();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  });

  it("rejects a forged WebSocket CSRF header without the matching subprotocol", async () => {
    const previousMode = process.env.NEXUS_WEB_AUTH_MODE;
    process.env.NEXUS_WEB_AUTH_MODE = "remote";
    const deps = dispatchers();
    const server = await createHeadlessGatewayServer(deps);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;
    const socket = new WebSocket(
      `ws://127.0.0.1:${port}/api/agui/ws`,
      ["nexus-v1"],
      {
        headers: {
          cookie: "nexus_human=human-1; nexus_csrf=csrf-ws",
          "x-nexus-csrf": "csrf-ws",
        },
      },
    );

    try {
      await new Promise<void>((resolve, reject) => {
        socket.once("open", resolve);
        socket.once("error", reject);
      });
      socket.send(JSON.stringify({
        t: "input",
        mode: "session",
        target: "otto",
        text: "must not route",
        clientMessageId: "cm_forged",
      }));
      const frame = await new Promise<Record<string, unknown>>((resolve, reject) => {
        socket.once("message", (data) => resolve(JSON.parse(String(data))));
        socket.once("error", reject);
      });

      expect(frame).toMatchObject({
        t: "input.err",
        clientMessageId: "cm_forged",
        status: 403,
      });
      expect(deps.webuiApi).not.toHaveBeenCalled();
    } finally {
      socket.close();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
      if (previousMode === undefined) delete process.env.NEXUS_WEB_AUTH_MODE;
      else process.env.NEXUS_WEB_AUTH_MODE = previousMode;
    }
  });

  it("rejects every agent-session mutation surface without CSRF proof before ingress", async () => {
    const previousMode = process.env.NEXUS_WEB_AUTH_MODE;
    process.env.NEXUS_WEB_AUTH_MODE = "remote";
    const deps = dispatchers();
    const mutatingIngress: Request[] = [];
    let streamController: ReadableStreamDefaultController<Uint8Array> | undefined;
    deps.aguiObserve = vi.fn(async () => new Response(new ReadableStream<Uint8Array>({
      start(controller) {
        streamController = controller;
      },
    }), {
      status: 200,
      headers: {
        "content-type": "text/event-stream",
        "x-nexus-session-id": "s_otto",
        "x-nexus-agent-id": "a_otto",
        "x-nexus-agent-name": "otto",
      },
    }));
    deps.apiV1 = vi.fn(async (request: Request) => {
      if (request.method !== "GET") mutatingIngress.push(request);
      if (new URL(request.url).pathname === "/api/v1/members") {
        return Response.json([
          { name: "otto", agentId: "a_otto", sessionId: "s_otto", agent: "claude" },
        ]);
      }
      return json({ ok: true });
    });
    deps.webuiApi = vi.fn(async (request: Request) => {
      if (request.method !== "GET") mutatingIngress.push(request);
      const url = new URL(request.url);
      if (request.method === "GET" && url.searchParams.has("eventsAfter")) {
        return Response.json({ events: [], latestSeq: 0 });
      }
      if (request.method === "GET" && url.pathname === "/api/conversation/prompt") {
        return Response.json({
          target: "otto",
          sessionId: "s_otto",
          turnActive: false,
          steerCapability: "native_steer",
          seq: 0,
          revision: 0,
          commands: [],
        });
      }
      return Response.json({ ok: true }, { status: 201 });
    });
    const server = await createHeadlessGatewayServer(deps);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;
    const cases: Array<{
      label: string;
      frame: Record<string, unknown>;
      terminal: (frame: Record<string, unknown>) => boolean;
      expected: Record<string, unknown>;
    }> = [
      {
        label: "input",
        frame: { t: "input", text: "blocked", clientMessageId: "cm_csrf_input" },
        terminal: (frame) => frame.t === "input.err" && frame.status === 403,
        expected: { status: 403 },
      },
      {
        label: "steer",
        frame: { t: "steer", text: "blocked", clientMessageId: "cm_csrf_steer" },
        terminal: (frame) => frame.t === "steer.err" && frame.status === 403,
        expected: { status: 403 },
      },
      {
        label: "interrupt",
        frame: { t: "interrupt", clientMessageId: "cm_csrf_interrupt" },
        terminal: (frame) => frame.t === "interrupt.err" && frame.status === 403,
        expected: { status: 403 },
      },
      {
        label: "compact",
        frame: { t: "command", name: "compact", clientCommandId: "cc_csrf_compact" },
        terminal: (frame) => frame.t === "command.done" && frame.ok === false,
        expected: { ok: false, code: "unauthorized" },
      },
      {
        label: "harness command",
        frame: {
          t: "command",
          name: "model",
          args: { model: "sonnet" },
          clientCommandId: "cc_csrf_harness",
        },
        terminal: (frame) => frame.t === "command.done" && frame.ok === false,
        expected: { ok: false, code: "unauthorized" },
      },
      ...[
        ["redirect", { t: "queue.redirect", clientMutationId: "mut_csrf_redirect" }],
        ["cancel", {
          t: "queue.cancel",
          clientMutationId: "mut_csrf_cancel",
          commandId: "cmd_csrf_cancel",
        }],
        ["edit", {
          t: "queue.edit",
          clientMutationId: "mut_csrf_edit",
          commandId: "cmd_csrf_edit",
          text: "blocked edit",
        }],
        ["reorder", {
          t: "queue.reorder",
          clientMutationId: "mut_csrf_reorder",
          commandIds: ["cmd_csrf_reorder"],
          expectedRevisions: [1],
        }],
      ].map(([label, frame]) => ({
        label: `queue ${String(label)}`,
        frame: frame as Record<string, unknown>,
        terminal: (reply: Record<string, unknown>) =>
          reply.t === "queue.mutation.err" && reply.status === 403,
        expected: { status: 403 },
      })),
    ];

    try {
      for (const testCase of cases) {
        const socket = new WebSocket(
          `ws://127.0.0.1:${port}/api/agui/ws?session=otto&agentId=a_otto`,
          ["nexus-v1"],
          {
            headers: {
              cookie: "nexus_human=human-1; nexus_csrf=csrf-ws",
              "x-nexus-csrf": "csrf-ws",
            },
          },
        );
        await new Promise<void>((resolve, reject) => {
          socket.once("open", resolve);
          socket.once("error", reject);
        });
        const terminal = waitForWsFrame(socket, testCase.terminal);
        socket.send(JSON.stringify(testCase.frame));
        await expect(terminal, testCase.label).resolves.toEqual(
          expect.objectContaining(testCase.expected),
        );
        socket.close();
      }

      expect(mutatingIngress).toEqual([]);
    } finally {
      streamController?.close();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
      if (previousMode === undefined) delete process.env.NEXUS_WEB_AUTH_MODE;
      else process.env.NEXUS_WEB_AUTH_MODE = previousMode;
    }
  });

  it("serves the REST API without routing through the webconsole", async () => {
    const deps = dispatchers();
    const response = await handleHeadlessGatewayRequest(
      new Request("http://localhost:4100/api/v1/health"),
      deps,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true, route: "api" });
    expect(deps.apiV1).toHaveBeenCalledOnce();
    expect(deps.networkMcp).not.toHaveBeenCalled();
  });

  it("serves the AG-UI observe endpoint used by targeted WebSocket lanes", async () => {
    const deps = dispatchers();
    const response = await handleHeadlessGatewayRequest(
      new Request("http://localhost:4100/api/agui/observe?thread=design"),
      deps,
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(deps.aguiObserve).toHaveBeenCalledOnce();
    expect(deps.apiV1).not.toHaveBeenCalled();
    expect(deps.networkMcp).not.toHaveBeenCalled();
  });

  it("preserves the public agent-session observe route in API-only mode", async () => {
    const response = await handleHeadlessGatewayRequest(
      new Request("http://localhost:4100/api/agui/observe?session=codex-release"),
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true, route: "agent-session" });
  });

  it("serves network MCP through the bearer-to-Principal dispatch path", async () => {
    const deps = dispatchers();
    const response = await handleHeadlessGatewayRequest(
      new Request("http://localhost:4100/api/mcp", {
        method: "POST",
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize" }),
      }),
      deps,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true, route: "mcp" });
    expect(deps.networkMcp).toHaveBeenCalledOnce();
    expect(deps.apiV1).not.toHaveBeenCalled();
  });

  it.each([
    "/api/login",
    "/api/conversation/prompt",
    "/api/conversation/logs",
    "/api/conversation/steer",
    "/api/conversation/compact",
    "/api/conversation/interrupt",
    "/api/projects",
    "/api/projects/assign",
  ])("serves the WebUI Gateway edge %s in API-only mode", async (path) => {
    const deps = dispatchers();
    const response = await handleHeadlessGatewayRequest(
      new Request(`http://localhost:4100${path}`),
      deps,
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ ok: true, route: "webui-api" });
    expect(deps.webuiApi).toHaveBeenCalledOnce();
    expect(deps.apiV1).not.toHaveBeenCalled();
  });

  it("rejects cookie-backed compatibility mutations without matching CSRF", async () => {
    const deps = dispatchers();
    const rejected = await handleHeadlessGatewayRequest(
      new Request("http://localhost:4100/api/conversation/prompt", {
        method: "POST",
        headers: { cookie: "nexus_human=human-1; nexus_csrf=csrf-1" },
      }),
      deps,
      { NEXUS_WEB_AUTH_MODE: "remote" },
    );
    expect(rejected.status).toBe(403);
    expect(deps.webuiApi).not.toHaveBeenCalled();

    const accepted = await handleHeadlessGatewayRequest(
      new Request("http://localhost:4100/api/conversation/prompt", {
        method: "POST",
        headers: {
          cookie: "nexus_human=human-1; nexus_csrf=csrf-1",
          "x-nexus-csrf": "csrf-1",
        },
      }),
      deps,
      { NEXUS_WEB_AUTH_MODE: "remote" },
    );
    expect(accepted.status).toBe(200);
    expect(deps.webuiApi).toHaveBeenCalledOnce();
  });

  it("rejects webconsole routes in API-only mode", async () => {
    const deps = dispatchers();
    const response = await handleHeadlessGatewayRequest(
      new Request("http://localhost:4100/threads"),
      deps,
    );

    expect(response.status).toBe(404);
    expect(await response.json()).toMatchObject({
      error: { code: "not_found" },
    });
    expect(deps.apiV1).not.toHaveBeenCalled();
    expect(deps.networkMcp).not.toHaveBeenCalled();
  });
});

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}
