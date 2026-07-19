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
  WebSocket: new (url: string) => TestWebSocket;
};

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
