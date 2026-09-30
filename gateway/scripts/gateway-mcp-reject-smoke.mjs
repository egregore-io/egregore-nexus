#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

const discovery = await readDiscovery();
const url = new URL("/api/mcp", discovery.url);
const res = await fetch(url, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }),
});

if (res.status !== 401) {
  const body = await res.text();
  throw new Error(`expected unauthenticated network MCP connect to return 401; got ${res.status}: ${body}`);
}

console.log(`ok: unauthenticated network MCP rejected at ${url} with 401`);

async function readDiscovery() {
  const explicit = process.env.NEXUS_GATEWAY_URL?.trim();
  if (explicit) return { url: explicit };
  const home = process.env.NEXUS_HOME ?? join(homedir(), ".nexus");
  const discovery = JSON.parse(await readFile(join(home, "gateway.json"), "utf8"));
  if (!isPidAlive(discovery.pid)) {
    throw new Error(`gateway discovery points at dead pid ${discovery.pid}; set NEXUS_GATEWAY_URL or restart the gateway`);
  }
  const health = await fetch(new URL("/api/v1/health", discovery.url));
  if (!health.ok) {
    throw new Error(`gateway discovery failed health check at ${discovery.url}: ${health.status}`);
  }
  return discovery;
}

function isPidAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
