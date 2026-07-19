#!/usr/bin/env node

import { createReadStream } from "node:fs";
import { access, realpath, stat } from "node:fs/promises";
import { createServer } from "node:http";
import { dirname, extname, join } from "node:path";
import { Readable } from "node:stream";
import { fileURLToPath } from "node:url";

import { removeOwnedDiscovery, writeDiscovery } from "../lib/lifecycle.mjs";

const args = process.argv.slice(2);
if (args.includes("--help")) {
  process.stdout.write(
    "Usage: nexus-webui [--host 127.0.0.1] [--port 4200] [--gateway-url http://127.0.0.1:4100] [--discovery <path>]\n",
  );
  process.exit(0);
}

const host = valueAfter("--host") ?? process.env.NEXUS_WEBUI_BIND ?? "127.0.0.1";
const port = parsePort(valueAfter("--port") ?? process.env.NEXUS_WEBUI_PORT ?? "4200");
const gateway = new URL(
  valueAfter("--gateway-url") ?? process.env.NEXUS_GATEWAY_URL ?? "http://127.0.0.1:4100",
);
const discoveryPath = valueAfter("--discovery") ?? process.env.NEXUS_WEBCONSOLE_DISCOVERY;
const executable = await realpath(fileURLToPath(import.meta.url));
const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const dist = process.env.NEXUS_WEBUI_DIST ?? join(packageRoot, "dist");
await access(join(dist, "index.html"));
const displayHost = host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
const url = `http://${displayHost}:${port}`;

const server = createServer((request, response) => {
  void dispatch(request, response).catch((error) => {
    if (!response.headersSent) response.writeHead(502, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: { code: "gateway_unavailable", message: String(error) } }));
  });
});
server.listen(port, host, async () => {
  if (discoveryPath) {
    await writeDiscovery(discoveryPath, {
      pid: process.pid,
      host,
      port,
      url,
      gatewayUrl: gateway.origin,
      startedAtMs: Date.now(),
      executable,
    });
  }
  process.stdout.write(`Nexus WebUI listening on ${url} (Gateway ${gateway.origin})\n`);
});
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => server.close(async () => {
    if (discoveryPath) await removeOwnedDiscovery(discoveryPath, process.pid);
    process.exit(0);
  }));
}

async function dispatch(request, response) {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? host}`);
  if (url.pathname === "/health") {
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      ok: true,
      service: "nexus-webui",
      pid: process.pid,
      host,
      port,
      url: `http://${displayHost}:${port}`,
      gateway: gateway.origin,
      executable,
    }));
    return;
  }
  if (url.pathname === "/api" || url.pathname.startsWith("/api/")) {
    await proxyGateway(request, response, url);
    return;
  }
  const relative = decodeURIComponent(url.pathname).split("/")
    .filter((segment) => segment && segment !== "." && segment !== "..")
    .join("/");
  const candidate = join(dist, relative || "index.html");
  const file = await regularFile(candidate) ? candidate : join(dist, "index.html");
  response.writeHead(200, { "content-type": contentType(file) });
  createReadStream(file).pipe(response);
}

async function proxyGateway(request, response, incoming) {
  const target = new URL(`${incoming.pathname}${incoming.search}`, gateway);
  const method = request.method ?? "GET";
  const body = method === "GET" || method === "HEAD" ? undefined : Readable.toWeb(request);
  const upstream = await fetch(target, {
    method,
    headers: request.headers,
    body,
    duplex: body ? "half" : undefined,
  });
  response.writeHead(upstream.status, Object.fromEntries(upstream.headers));
  if (upstream.body) Readable.fromWeb(upstream.body).pipe(response);
  else response.end();
}

async function regularFile(path) {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}

function valueAfter(name) {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : undefined;
}

function parsePort(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    throw new Error(`invalid WebUI port: ${value}`);
  }
  return parsed;
}

function contentType(path) {
  return ({
    ".css": "text/css; charset=utf-8",
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json",
    ".svg": "image/svg+xml",
  })[extname(path)] ?? "application/octet-stream";
}
