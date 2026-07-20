import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { once } from "node:events";
import { homedir } from "node:os";
import { join } from "node:path";

import { dispatchNetworkMcp } from "../../routes/api/mcp";
import { dispatchApiV1 } from "../../routes/api/v1/$";
import { observeScoped } from "../../routes/api/agui.observe";
import { attachAguiWsUpgrade } from "../agui/ws.mjs";
import { closeSharedDaemonPushConnector } from "../agui/daemonPushRelay.mjs";
import {
  createGatewayDeveloperEventSource,
  type GatewayDeveloperEventSource,
} from "../agui/gatewayDeveloperEvents";
import {
  startGatewayProjectionService,
  stopGatewayProjectionService,
  type GatewayProjectionServiceOptions,
} from "../projection/service";
import {
  startGatewayHookService,
  stopGatewayHookService,
  type GatewayHookService,
} from "../hooks/service";
import { dispatchWebuiApi, isWebuiApiPath } from "./webuiApi";
import { browserMutationCsrfFailure } from "../auth/browserMutationAuth.mjs";
import {
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
} from "../auth/webAuthMode";
import { createGatewayStore, getGatewayStore } from "../store/client";
import { gatewayStoreConfig } from "../store/config";
import { CURRENT_GATEWAY_SCHEMA_VERSION } from "../store/migrations";
import { createTransportHost, type TransportHost } from "../transport/host";

export {
  closeSharedDaemonPushConnector,
  startGatewayHookService,
  startGatewayProjectionService,
  stopGatewayHookService,
  stopGatewayProjectionService,
};

/** Apply the local canonical-store schema without starting REST, WS, MCP, or WebUI listeners. */
export async function migrateHeadlessGatewayStore(
  env: NodeJS.ProcessEnv = process.env,
): Promise<{ schemaVersion: number }> {
  const db = await createGatewayStore(gatewayStoreConfig(env));
  try {
    return { schemaVersion: CURRENT_GATEWAY_SCHEMA_VERSION };
  } finally {
    db.close();
  }
}

export interface HeadlessGatewayDispatchers {
  aguiObserve: (request: Request) => Promise<Response>;
  apiV1: (request: Request) => Promise<Response>;
  networkMcp: (request: Request) => Promise<Response>;
  webuiApi: (request: Request) => Promise<Response>;
}

export interface HeadlessGatewayLifecycle {
  startHooks(): Promise<Pick<GatewayHookService, "afterReceipt"> | undefined>;
  startProjection(options: GatewayProjectionServiceOptions): Promise<unknown>;
  stopProjection(): Promise<void>;
  stopHooks(): Promise<void>;
  startTransports?(): Promise<TransportHost | undefined>;
  stopTransports?(host: TransportHost | undefined): Promise<void>;
  stopConnection?(): Promise<void> | void;
}

export interface HeadlessGatewayWsOptions {
  fetchHandler: (request: Request) => Promise<Response>;
  developerEvents?: GatewayDeveloperEventSource;
}

const DEFAULT_LIFECYCLE: HeadlessGatewayLifecycle = {
  startHooks: startGatewayHookService,
  startProjection: startGatewayProjectionService,
  stopProjection: stopGatewayProjectionService,
  stopHooks: stopGatewayHookService,
  startTransports: async () => {
    const host = createTransportHost({
      nexusHome: process.env.NEXUS_HOME?.trim() || join(homedir(), ".nexus"),
      db: getGatewayStore,
    });
    await host.start();
    return host;
  },
  stopTransports: async (host) => host?.stop(),
  stopConnection: async () => closeSharedDaemonPushConnector(),
};

const DEFAULT_DISPATCHERS: HeadlessGatewayDispatchers = {
  aguiObserve: (request) => observeScoped(request, { canonicalDb: getGatewayStore }),
  apiV1: dispatchApiV1,
  networkMcp: dispatchNetworkMcp,
  webuiApi: dispatchWebuiApi,
};

/**
 * API-only gateway request handler. It intentionally exposes the REST spine and
 * authenticated network MCP while returning 404 for webconsole/UI routes.
 */
export async function handleHeadlessGatewayRequest(
  request: Request,
  dispatchers: HeadlessGatewayDispatchers = DEFAULT_DISPATCHERS,
  env: NodeJS.ProcessEnv = process.env,
): Promise<Response> {
  const csrfFailure = browserMutationCsrfFailure(request, {
    enforce: !isLocalOperatorWebAuthMode(webAuthModeFromEnv(env)),
  });
  if (csrfFailure) return csrfFailure;
  const url = new URL(request.url);
  if (url.pathname === "/api/mcp") {
    return dispatchers.networkMcp(request);
  }
  if (isWebuiApiPath(url.pathname)) {
    return dispatchers.webuiApi(request);
  }
  if (request.method === "GET" && url.pathname === "/api/agui/observe") {
    return dispatchers.aguiObserve(request);
  }
  if (url.pathname === "/api/v1" || url.pathname.startsWith("/api/v1/")) {
    return dispatchers.apiV1(request);
  }
  return json(
    {
      error: {
        code: "not_found",
        message: "headless Nexus gateway serves only /api/v1/*, /api/mcp, and AG-UI observe/WS",
      },
    },
    404,
  );
}

/** Guard a packaged TanStack handler with the same browser mutation contract as headless mode. */
export function guardGatewayBrowserRequest(
  request: Request,
  next: (request: Request) => Promise<Response>,
  env: NodeJS.ProcessEnv = process.env,
): Promise<Response> {
  const failure = browserMutationCsrfFailure(request, {
    enforce: !isLocalOperatorWebAuthMode(webAuthModeFromEnv(env)),
  });
  return failure ? Promise.resolve(failure) : next(request);
}

/**
 * Creates the independently distributed gateway server, including its public
 * AG-UI WebSocket upgrade lane. The WebUI remains a separate package.
 */
export async function createHeadlessGatewayServer(
  dispatchers: HeadlessGatewayDispatchers = DEFAULT_DISPATCHERS,
  lifecycle: HeadlessGatewayLifecycle = DEFAULT_LIFECYCLE,
) {
  const hooks = await lifecycle.startHooks();
  let transportHost: TransportHost | undefined;
  try {
    await lifecycle.startProjection({
      ...(hooks ? { afterReceipt: (event) => hooks.afterReceipt(event) } : {}),
    });
    transportHost = await lifecycle.startTransports?.();
  } catch (error) {
    await lifecycle.stopTransports?.(transportHost);
    await lifecycle.stopProjection().catch(() => undefined);
    await lifecycle.stopHooks();
    await lifecycle.stopConnection?.();
    throw error;
  }
  const fetchHandler = (request: Request) =>
    handleHeadlessGatewayRequest(request, dispatchers);
  const server = createServer((req, res) => {
    void handleNodeRequest(req, res, dispatchers);
  });
  await attachHeadlessGatewayWs(server, { fetchHandler });
  let cleanup: Promise<void> | undefined;
  const stopServices = () => cleanup ??= Promise.resolve()
    .then(() => lifecycle.stopTransports?.(transportHost))
    .then(() => lifecycle.stopProjection())
    .then(() => lifecycle.stopHooks())
    .then(() => lifecycle.stopConnection?.())
    .then(() => undefined);
  server.once("close", () => {
    void stopServices()
      .catch((error) => {
        process.stderr.write(`Nexus Gateway shutdown failed: ${String(error)}\n`);
      });
  });
  return Object.assign(server, {
    transportHost,
    async shutdown(): Promise<void> {
      if (server.listening) {
        await new Promise<void>((resolve, reject) => {
          server.close((error) => error ? reject(error) : resolve());
        });
      }
      await stopServices();
    },
  });
}

/** Attach the bundled AG-UI upgrade lane with Gateway-owned durable message subscriptions. */
export async function attachHeadlessGatewayWs(
  server: Server,
  options: HeadlessGatewayWsOptions,
) {
  const ownsDeveloperEvents = !options.developerEvents;
  const developerEvents = options.developerEvents ?? createGatewayDeveloperEventSource();
  const wss = await attachAguiWsUpgrade(server, {
    fetchHandler: options.fetchHandler,
    developerEvents,
  });
  if (ownsDeveloperEvents) {
    server.once("close", () => developerEvents.close());
  }
  return wss;
}

async function main(): Promise<void> {
  const port = parsePort(process.env.PORT ?? process.env.NEXUS_GATEWAY_PORT) ?? 4100;
  const host = process.env.HOST ?? process.env.NEXUS_GATEWAY_BIND ?? "127.0.0.1";
  const server = await createHeadlessGatewayServer();
  server.listen(port, host, () => {
    process.stdout.write(`Nexus headless gateway listening on http://${host}:${port}\n`);
  });
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, () => {
      void server.shutdown()
        .then(() => process.exit(0))
        .catch((error) => {
          process.stderr.write(`Nexus Gateway shutdown failed: ${String(error)}\n`);
          process.exit(1);
        });
    });
  }
}

async function handleNodeRequest(
  req: IncomingMessage,
  res: ServerResponse,
  dispatchers: HeadlessGatewayDispatchers,
): Promise<void> {
  try {
    const request = await nodeRequestToFetch(req);
    const response = await handleHeadlessGatewayRequest(request, dispatchers);
    await writeFetchResponse(res, response);
  } catch (err) {
    const message = err instanceof Error ? err.message : "gateway request failed";
    await writeFetchResponse(res, json({ error: { code: "internal_error", message } }, 500));
  }
}

async function nodeRequestToFetch(req: IncomingMessage): Promise<Request> {
  const host = req.headers.host ?? "127.0.0.1";
  const url = new URL(req.url ?? "/", `http://${host}`);
  const method = req.method ?? "GET";
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const entry of value) headers.append(key, entry);
    } else if (value !== undefined) {
      headers.set(key, value);
    }
  }

  const upper = method.toUpperCase();
  const hasBody = upper !== "GET" && upper !== "HEAD";
  return new Request(url, {
    method,
    headers,
    body: hasBody ? req : undefined,
    duplex: hasBody ? "half" : undefined,
  } as RequestInit & { duplex?: "half" });
}

async function writeFetchResponse(res: ServerResponse, response: Response): Promise<void> {
  res.statusCode = response.status;
  const setCookies =
    (response.headers as Headers & { getSetCookie?: () => string[] })
      .getSetCookie?.() ?? [];
  response.headers.forEach((value, key) => {
    if (key.toLowerCase() === "set-cookie") return;
    res.setHeader(key, value);
  });
  if (setCookies.length > 0) {
    res.setHeader("set-cookie", setCookies);
  }
  if (!response.body) {
    res.end();
    return;
  }

  const reader = response.body.getReader();
  try {
    while (!res.destroyed) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!res.write(value)) {
        await once(res, "drain");
      }
    }
  } finally {
    reader.releaseLock();
  }
  if (!res.destroyed) res.end();
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function parsePort(value: string | undefined): number | undefined {
  const cleaned = value?.trim();
  if (!cleaned) return undefined;
  const port = Number(cleaned);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`invalid gateway port: ${value}`);
  }
  return port;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  void main();
}
