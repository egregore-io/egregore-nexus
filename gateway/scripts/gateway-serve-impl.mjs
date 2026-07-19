#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { createReadStream, existsSync } from "node:fs";
import { cp, mkdir, readFile, rename, rm, stat, unlink, writeFile } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { homedir } from "node:os";
import { dirname, extname, join, relative, resolve, sep } from "node:path";
import { spawn } from "node:child_process";
import { Readable } from "node:stream";
import { fileURLToPath, pathToFileURL } from "node:url";

const DEFAULT_PORT = 4100;
const FALLBACK_PORTS = Array.from({ length: 11 }, (_, i) => 4100 + i);
const FRONTEND_DIR = dirname(dirname(fileURLToPath(import.meta.url)));
const DIST_CLIENT_DIR = join(FRONTEND_DIR, "dist/client");
const DIST_SERVER_ENTRY = join(FRONTEND_DIR, "dist/server/server.js");
// The directory static assets are actually served from — repointed to the run-scoped
// dist snapshot when the packaged gateway boots (see snapshotDistForRun).
let servingClientDir = DIST_CLIENT_DIR;
// The run-scoped dist snapshot dir, if this run made one (packaged mode only).
let runDistDir = null;
const GATEWAY_CLOSE_TIMEOUT_MS =
  parsePositiveInt(process.env.NEXUS_GATEWAY_CLOSE_TIMEOUT_MS) ?? 5_000;

// Route handlers already return failures to callers. Rejections escaping that boundary
// are logged without terminating the serving process.
process.on("unhandledRejection", (error) => {
  console.error("[gateway-serve] unhandledRejection (non-fatal):", error);
});

const apiOnly = process.argv.includes("--api-only") || truthy(process.env.NEXUS_GATEWAY_API_ONLY);
const discoveryMode = resolveDiscoveryMode(apiOnly);
const port = await resolvePort();
const runner = apiOnly ? startApiOnlyGateway(port) : await startPackagedGateway(port);

function childOptions(port, apiOnly) {
  return {
    cwd: FRONTEND_DIR,
    stdio: "inherit",
    env: {
      ...process.env,
      PORT: String(port),
      NEXUS_GATEWAY_PORT: String(port),
      NEXUS_GATEWAY_API_ONLY: apiOnly ? "1" : process.env.NEXUS_GATEWAY_API_ONLY,
    },
  };
}

function tsxCli() {
  return fileURLToPath(import.meta.resolve("tsx/cli"));
}

await waitForGateway(port);
const discoveryRecord = discoveryMode === "write"
  ? await writeGatewayDiscovery(port, process.pid)
  : undefined;

runner.onExit((code, signal) => {
  void cleanupAndExit(code, signal);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    void cleanupAndExit(0, signal);
  });
}

function startApiOnlyGateway(port) {
  const packagedEntry = join(FRONTEND_DIR, "dist-gateway/headless.mjs");
  const args = existsSync(packagedEntry)
    ? [packagedEntry]
    : [
        tsxCli(),
        "--tsconfig", join(FRONTEND_DIR, "tsconfig.json"),
        "src/server/gateway/headless.ts",
      ];
  const child = spawn(process.execPath, args, childOptions(port, true));
  let exited = false;
  const exitPromise = new Promise((resolve) => child.once("exit", resolve));
  child.once("exit", () => {
    exited = true;
  });
  return {
    pid: child.pid ?? process.pid,
    onExit(callback) {
      child.on("exit", callback);
    },
    async stop(signal) {
      if (exited) return;
      await stopChildWithDeadline(
        child,
        signal ?? "SIGTERM",
        GATEWAY_CLOSE_TIMEOUT_MS,
        () => exited,
        exitPromise,
      );
    },
  };
}

// Packaged mode serves a run-scoped snapshot because deploys rebuild `dist/` in place
// while Node resolves hashed asset imports lazily. API-only mode uses its dedicated
// headless entrypoint and does not read the client build.
async function snapshotDistForRun() {
  const dir = join(FRONTEND_DIR, `.dist-run-${process.pid}`);
  await rm(dir, { recursive: true, force: true });
  await cp(join(FRONTEND_DIR, "dist"), dir, { recursive: true });
  runDistDir = dir;
  return dir;
}

async function removeRunDistSnapshot() {
  if (!runDistDir) return;
  const dir = runDistDir;
  runDistDir = null;
  try {
    await rm(dir, { recursive: true, force: true });
  } catch (error) {
    console.error("[gateway-serve] failed to remove dist snapshot:", error);
  }
}

async function startPackagedGateway(port) {
  const { attachAguiWsUpgrade } = await import("../src/server/agui/ws.mjs");
  const projectionBundle = join(FRONTEND_DIR, "dist-gateway/headless.mjs");
  if (!existsSync(projectionBundle)) {
    throw new Error(
      "packaged Gateway projection consumer is missing; run npm run build:gateway-package",
    );
  }
  const {
    closeSharedDaemonPushConnector,
    startGatewayHookService,
    startGatewayProjectionService,
    stopGatewayHookService,
    stopGatewayProjectionService,
  } = await import(pathToFileURL(projectionBundle).href);
  const hooks = await startGatewayHookService();
  try {
    await startGatewayProjectionService({
      afterReceipt: (event) => hooks.afterReceipt(event),
    });
  } catch (error) {
    await stopGatewayHookService();
    closeSharedDaemonPushConnector();
    throw error;
  }
  const distDir = await snapshotDistForRun();
  servingClientDir = join(distDir, "client");
  const handler = await loadPackagedServerHandler(join(distDir, "server/server.js"));
  const host = clean(process.env.HOST) ?? clean(process.env.NEXUS_GATEWAY_BIND) ?? "127.0.0.1";
  const sockets = new Set();
  const server = createHttpServer((req, res) => {
    void handlePackagedRequest(handler, req, res);
  });
  server.on("connection", (socket) => trackSocket(sockets, socket));
  server.on("upgrade", (_req, socket) => trackSocket(sockets, socket));
  await attachAguiWsUpgrade(server, { fetchHandler: handler });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, resolve);
  });
  process.stdout.write(`Nexus packaged gateway listening on http://${host}:${port}\n`);
  return {
    pid: process.pid,
    onExit() {
      // The packaged gateway is this process; there is no child exit to mirror.
    },
    async stop() {
      await closeHttpServerWithDeadline(server, sockets, GATEWAY_CLOSE_TIMEOUT_MS);
      await stopGatewayProjectionService();
      await stopGatewayHookService();
      closeSharedDaemonPushConnector();
    },
  };
}

async function stopChildWithDeadline(child, signal, timeoutMs, isExited, exitPromise) {
  if (isExited()) return;
  child.kill(signal);
  let timedOut = false;
  await Promise.race([
    exitPromise,
    delay(timeoutMs).then(() => {
      timedOut = true;
    }),
  ]);
  if (timedOut && !isExited()) {
    child.kill("SIGKILL");
    await exitPromise;
  }
}

async function closeHttpServerWithDeadline(server, sockets, timeoutMs) {
  let closed = false;
  await new Promise((resolve) => {
    let timer;
    const done = () => {
      if (closed) return;
      closed = true;
      clearTimeout(timer);
      resolve();
    };
    timer = setTimeout(() => {
      server.closeAllConnections?.();
      destroyTrackedSockets(sockets);
      done();
    }, timeoutMs);
    try {
      server.close(done);
      server.closeIdleConnections?.();
    } catch {
      done();
    }
  });
}

function trackSocket(sockets, socket) {
  sockets.add(socket);
  socket.once("close", () => sockets.delete(socket));
}

function destroyTrackedSockets(sockets) {
  for (const socket of [...sockets]) {
    socket.destroy();
  }
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function loadPackagedServerHandler(serverEntry = DIST_SERVER_ENTRY) {
  const mod = await import(pathToFileURL(serverEntry).href);
  const entry = mod.default ?? mod.server ?? mod;
  const fetchHandler = typeof entry === "function" ? entry : entry?.fetch;
  if (typeof fetchHandler !== "function") {
    throw new Error(`packaged gateway entry has no fetch handler: ${serverEntry}`);
  }
  return fetchHandler.bind(entry);
}

async function handlePackagedRequest(handler, req, res) {
  try {
    if (await serveStaticAsset(req, res)) return;
    const request = await nodeRequestToFetch(req);
    const response = await handler(request);
    await writeFetchResponse(res, response);
  } catch (error) {
    const message = error instanceof Error ? error.message : "gateway request failed";
    await writeFetchResponse(res, json({ error: { code: "internal_error", message } }, 500));
  }
}

async function serveStaticAsset(req, res) {
  const method = req.method?.toUpperCase() ?? "GET";
  if (method !== "GET" && method !== "HEAD") return false;
  const url = new URL(req.url ?? "/", "http://localhost");
  if (url.pathname === "/") return false;
  let pathname;
  try {
    pathname = decodeURIComponent(url.pathname);
  } catch {
    return false;
  }
  const filePath = resolve(servingClientDir, pathname.replace(/^\/+/, ""));
  const clientRoot = resolve(servingClientDir);
  const rel = relative(clientRoot, filePath);
  if (rel.startsWith("..") || rel === "" || rel.includes(`..${sep}`)) return false;
  let fileStat;
  try {
    fileStat = await stat(filePath);
  } catch {
    return false;
  }
  if (!fileStat.isFile()) return false;

  res.statusCode = 200;
  res.setHeader("content-type", contentType(filePath));
  res.setHeader("content-length", String(fileStat.size));
  if (filePath.includes(`${sep}assets${sep}`)) {
    res.setHeader("cache-control", "public, max-age=31536000, immutable");
  }
  if (method === "HEAD") {
    res.end();
    return true;
  }
  const stream = createReadStream(filePath);
  stream.once("error", (error) => {
    res.destroy(error);
  });
  stream.pipe(res);
  return true;
}

async function nodeRequestToFetch(req) {
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
  });
}

async function writeFetchResponse(res, response) {
  res.statusCode = response.status;
  const setCookies = response.headers.getSetCookie();
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
  await new Promise((resolve, reject) => {
    const body = Readable.fromWeb(response.body);
    body.once("error", reject);
    res.once("error", reject);
    res.once("finish", resolve);
    body.pipe(res);
  });
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function contentType(path) {
  switch (extname(path).toLowerCase()) {
    case ".css":
      return "text/css; charset=utf-8";
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
      return "text/javascript; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    case ".wasm":
      return "application/wasm";
    default:
      return "application/octet-stream";
  }
}

async function resolvePort() {
  const explicit = parsePort(process.env.NEXUS_GATEWAY_PORT);
  if (explicit !== undefined) {
    if (!(await isPortAvailable(explicit))) {
      throw new Error(`NEXUS_GATEWAY_PORT ${explicit} is already in use`);
    }
    return explicit;
  }
  for (const candidate of FALLBACK_PORTS) {
    if (await isPortAvailable(candidate)) return candidate;
  }
  throw new Error("no free Nexus gateway port in range 4100-4110");
}

async function waitForGateway(port) {
  const healthUrl = `http://127.0.0.1:${port}/api/v1/health`;
  const capabilitiesUrl = `http://127.0.0.1:${port}/api/v1/capabilities`;
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      const health = await fetch(healthUrl);
      const capabilities = await fetch(capabilitiesUrl);
      if (health.ok && capabilities.status < 500) return;
    } catch {
      // Keep polling until the generated server finishes binding and its route table is ready.
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`gateway did not become ready on port ${port} (health + capabilities)`);
}

async function writeGatewayDiscovery(port, pid) {
  const home = process.env.NEXUS_HOME ?? join(homedir(), ".nexus");
  const instanceId = await loadOrCreateInstanceId(home);
  const record = {
    instanceId,
    url: `http://localhost:${port}`,
    port,
    authMode: resolveAuthMode(),
    pid,
    boundAt: Date.now(),
  };
  await atomicJsonWrite(join(home, "gateway.json"), record);
  return record;
}

async function removeGatewayDiscovery(record) {
  const home = process.env.NEXUS_HOME ?? join(homedir(), ".nexus");
  const path = join(home, "gateway.json");
  try {
    const current = JSON.parse(await readFile(path, "utf8"));
    if (
      current.instanceId === record.instanceId &&
      current.pid === record.pid &&
      current.port === record.port &&
      current.boundAt === record.boundAt
    ) {
      await unlink(path);
    }
  } catch {
    // Missing or malformed discovery needs no cleanup.
  }
}

async function cleanupAndExit(code, signal) {
  if (discoveryRecord) await removeGatewayDiscovery(discoveryRecord);
  await runner.stop(signal);
  await removeRunDistSnapshot();
  if (signal) process.kill(process.pid, signal);
  process.exit(code ?? 0);
}

function resolveDiscoveryMode(apiOnly) {
  const explicit = clean(valueForArg("--discovery")) ?? clean(process.env.NEXUS_GATEWAY_DISCOVERY);
  if (explicit) {
    if (explicit === "write" || explicit === "none") return explicit;
    throw new Error(`invalid gateway discovery mode: ${explicit}; expected write or none`);
  }
  return apiOnly ? "none" : "write";
}

function valueForArg(name) {
  const prefix = `${name}=`;
  const match = process.argv.find((arg) => arg.startsWith(prefix));
  return match ? match.slice(prefix.length) : undefined;
}

async function loadOrCreateInstanceId(home) {
  const path = join(home, "instance.json");
  try {
    const parsed = JSON.parse(await readFile(path, "utf8"));
    if (parsed.instanceId) return parsed.instanceId;
  } catch {
    // Missing or malformed instance files are repaired below.
  }
  const instanceId = `inst_${randomUUID().replaceAll("-", "")}`;
  await atomicJsonWrite(path, { instanceId });
  return instanceId;
}

async function atomicJsonWrite(path, value) {
  await mkdir(dirname(path), { recursive: true });
  const tmp = `${path}.tmp-${process.pid}-${Date.now()}`;
  await writeFile(tmp, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(tmp, path);
}

function resolveAuthMode() {
  const explicit = clean(process.env.NEXUS_WEB_AUTH_MODE);
  if (explicit) {
    const value = explicit.toLowerCase();
    if (value === "local" || value === "remote") return value;
    throw new Error(`invalid NEXUS_WEB_AUTH_MODE: ${explicit}; expected local or remote`);
  }
  if (truthy(process.env.NEXUS_WEB_ALLOW_REMOTE) || truthy(process.env.NEXUS_ALLOW_REMOTE)) {
    return "remote";
  }
  const publicUrl = clean(process.env.NEXUS_WEB_PUBLIC_URL) ?? clean(process.env.NEXUS_PUBLIC_URL);
  if (publicUrl && !isLoopbackUrl(publicUrl)) return "remote";
  const bind = clean(process.env.NEXUS_WEB_BIND) ?? clean(process.env.NEXUS_GATEWAY_BIND) ?? clean(process.env.HOST);
  if (bind && !isLoopbackBind(bind)) return "remote";
  return "local";
}

function parsePort(value) {
  const cleaned = clean(value);
  if (!cleaned) return undefined;
  const port = Number(cleaned);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`invalid NEXUS_GATEWAY_PORT: ${value}`);
  }
  return port;
}

function parsePositiveInt(value) {
  const cleaned = clean(value);
  if (!cleaned) return undefined;
  const parsed = Number(cleaned);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
}

function truthy(value) {
  const v = clean(value)?.toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function clean(value) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function isLoopbackUrl(raw) {
  try {
    const url = new URL(raw);
    return isLoopbackHost(url.hostname);
  } catch {
    return isLoopbackBind(raw);
  }
}

function isLoopbackBind(raw) {
  const host = parseBindHost(raw);
  return host ? isLoopbackHost(host) : true;
}

function parseBindHost(raw) {
  const value = raw.trim();
  if (!value) return undefined;
  if (value.startsWith("[")) {
    const end = value.indexOf("]");
    return end === -1 ? value : value.slice(0, end + 1);
  }
  const withoutProtocol = value.includes("://") ? new URL(value).hostname : value;
  const colon = withoutProtocol.indexOf(":");
  return colon === -1 ? withoutProtocol : withoutProtocol.slice(0, colon);
}

function isLoopbackHost(host) {
  return ["localhost", "127.0.0.1", "::1", "[::1]"].includes(host.toLowerCase());
}

function isPortAvailable(port) {
  return new Promise((resolve) => {
    const server = createNetServer();
    server.once("error", () => resolve(false));
    server.once("listening", () => server.close(() => resolve(true)));
    server.listen(port, "127.0.0.1");
  });
}
