import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, unlink, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

export const DEFAULT_GATEWAY_PORT = 4100;
export const FALLBACK_GATEWAY_PORTS = Array.from({ length: 11 }, (_, i) => 4100 + i);

export type GatewayAuthMode = "local" | "remote";

export interface GatewayDiscovery {
  instanceId: string;
  url: string;
  port: number;
  authMode: GatewayAuthMode;
  pid: number;
  boundAt: number;
}

export interface GatewayPortEnv {
  NEXUS_GATEWAY_PORT?: string;
  NEXUS_WEB_AUTH_MODE?: string;
  NEXUS_WEB_ALLOW_REMOTE?: string;
  NEXUS_ALLOW_REMOTE?: string;
  NEXUS_WEB_BIND?: string;
  NEXUS_GATEWAY_BIND?: string;
  HOST?: string;
  NEXUS_WEB_PUBLIC_URL?: string;
  NEXUS_PUBLIC_URL?: string;
}

export interface WriteGatewayDiscoveryOptions {
  home?: string;
  port: number;
  authMode: GatewayAuthMode;
  host?: string;
}

interface RuntimeDeps {
  pid?: number;
  now?: () => number;
  randomId?: () => string;
  isPidAlive?: (pid: number) => boolean;
  fetch?: typeof fetch;
}

type PortProbe = (port: number) => Promise<boolean>;

/** Resolve the standalone gateway port. Default is :4100; fallback scan is :4101-:4110. */
export async function resolveGatewayPort(
  env: GatewayPortEnv = process.env,
  isAvailable: PortProbe = isPortAvailable,
): Promise<number> {
  const explicit = parsePort(env.NEXUS_GATEWAY_PORT);
  if (explicit !== undefined) {
    if (!(await isAvailable(explicit))) {
      throw new Error(`NEXUS_GATEWAY_PORT ${explicit} is already in use`);
    }
    return explicit;
  }

  for (const port of FALLBACK_GATEWAY_PORTS) {
    if (await isAvailable(port)) return port;
  }
  throw new Error("no free Nexus gateway port in range 4100-4110");
}

/** Write the gateway discovery record after the server has bound its port. */
export async function writeGatewayDiscovery(
  options: WriteGatewayDiscoveryOptions,
  deps: RuntimeDeps = {},
): Promise<GatewayDiscovery> {
  const home = options.home ?? nexusHome();
  const instanceId = await loadOrCreateInstanceId(home, deps);
  const record: GatewayDiscovery = {
    instanceId,
    url: `http://${options.host ?? "localhost"}:${options.port}`,
    port: options.port,
    authMode: options.authMode,
    pid: deps.pid ?? process.pid,
    boundAt: deps.now?.() ?? Date.now(),
  };
  await atomicJsonWrite(gatewayDiscoveryPath(home), record);
  return record;
}

export async function readGatewayDiscovery(home = nexusHome()): Promise<GatewayDiscovery> {
  return JSON.parse(await readFile(gatewayDiscoveryPath(home), "utf8")) as GatewayDiscovery;
}

/** Read discovery only if the recorded process and health endpoint are still live. */
export async function readLiveGatewayDiscovery(
  home = nexusHome(),
  deps: RuntimeDeps = {},
): Promise<GatewayDiscovery | null> {
  const record = await readGatewayDiscovery(home);
  const isPidAlive = deps.isPidAlive ?? defaultIsPidAlive;
  if (!isPidAlive(record.pid)) return null;
  const fetchImpl = deps.fetch ?? fetch;
  try {
    const res = await fetchImpl(new URL("/api/v1/health", record.url));
    return res.ok ? record : null;
  } catch {
    return null;
  }
}

/** Remove gateway.json on clean shutdown, but only if it still points at this process. */
export async function removeGatewayDiscovery(
  record: GatewayDiscovery,
  home = nexusHome(),
): Promise<void> {
  const path = gatewayDiscoveryPath(home);
  try {
    const current = JSON.parse(await readFile(path, "utf8")) as GatewayDiscovery;
    if (
      current.instanceId === record.instanceId &&
      current.pid === record.pid &&
      current.port === record.port &&
      current.boundAt === record.boundAt
    ) {
      await unlink(path);
    }
  } catch {
    // Missing or malformed discovery files need no cleanup.
  }
}

/** Resolve the discovery auth mode without importing route/auth modules into Vite config startup. */
export function resolveGatewayAuthMode(env: GatewayPortEnv = process.env): GatewayAuthMode {
  const explicit = clean(env.NEXUS_WEB_AUTH_MODE);
  if (explicit) {
    const value = explicit.toLowerCase();
    if (value === "local" || value === "remote") return value;
    throw new Error(`invalid NEXUS_WEB_AUTH_MODE: ${explicit}; expected local or remote`);
  }
  if (truthy(env.NEXUS_WEB_ALLOW_REMOTE) || truthy(env.NEXUS_ALLOW_REMOTE)) return "remote";
  const publicUrl = clean(env.NEXUS_WEB_PUBLIC_URL) ?? clean(env.NEXUS_PUBLIC_URL);
  if (publicUrl && !isLoopbackUrl(publicUrl)) return "remote";
  const bind = clean(env.NEXUS_WEB_BIND) ?? clean(env.NEXUS_GATEWAY_BIND) ?? clean(env.HOST);
  if (bind && !isLoopbackBind(bind)) return "remote";
  return "local";
}

export function gatewayDiscoveryPath(home = nexusHome()): string {
  return join(home, "gateway.json");
}

export function gatewayInstancePath(home = nexusHome()): string {
  return join(home, "instance.json");
}

export function nexusHome(): string {
  return process.env.NEXUS_HOME ?? join(homedir(), ".nexus");
}

async function loadOrCreateInstanceId(home: string, deps: RuntimeDeps): Promise<string> {
  const path = gatewayInstancePath(home);
  try {
    const parsed = JSON.parse(await readFile(path, "utf8")) as { instanceId?: string };
    if (parsed.instanceId) return parsed.instanceId;
  } catch {
    // Missing or malformed instance files are repaired below.
  }
  const instanceId = deps.randomId?.() ?? `inst_${randomUUID().replaceAll("-", "")}`;
  await atomicJsonWrite(path, { instanceId });
  return instanceId;
}

async function atomicJsonWrite(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tmp = `${path}.tmp-${process.pid}-${Date.now()}`;
  await writeFile(tmp, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(tmp, path);
}

async function isPortAvailable(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const server = createServer();
    server.once("error", () => resolve(false));
    server.once("listening", () => {
      server.close(() => resolve(true));
    });
    server.listen(port, "127.0.0.1");
  });
}

function parsePort(value: string | undefined): number | undefined {
  const cleaned = clean(value);
  if (!cleaned) return undefined;
  const port = Number(cleaned);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`invalid NEXUS_GATEWAY_PORT: ${value}`);
  }
  return port;
}

function truthy(value: string | undefined): boolean {
  const v = clean(value)?.toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function clean(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function isLoopbackUrl(raw: string): boolean {
  try {
    const url = new URL(raw);
    return isLoopbackHost(url.hostname);
  } catch {
    return isLoopbackBind(raw);
  }
}

function isLoopbackBind(raw: string): boolean {
  const host = parseBindHost(raw);
  return host ? isLoopbackHost(host) : true;
}

function parseBindHost(raw: string): string | undefined {
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

function isLoopbackHost(host: string): boolean {
  return new Set(["localhost", "127.0.0.1", "::1", "[::1]"]).has(host.toLowerCase());
}

function defaultIsPidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}
