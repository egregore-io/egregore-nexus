import { readFileSync } from "node:fs";
import { homedir, userInfo as osUserInfo } from "node:os";
import { join } from "node:path";

import type { GatewayCallerIdentity } from "@server/api/http";
import { localPrincipalAttributes } from "@server/auth/principal";

export type WebAuthFacet =
  | "local-operator"
  | "remote-human"
  | "remote-agent"
  | "peer";

export type WebAuthMode = WebAuthFacet | "local" | "remote";

/** Server-side configuration used to decide whether browser auth is required. */
export interface WebAuthEnv {
  NEXUS_WEB_AUTH_MODE?: string;
  NEXUS_WEB_ALLOW_REMOTE?: string;
  NEXUS_ALLOW_REMOTE?: string;
  NEXUS_WEB_BIND?: string;
  NEXUS_GATEWAY_BIND?: string;
  HOST?: string;
  NEXUS_WEB_PUBLIC_URL?: string;
  NEXUS_PUBLIC_URL?: string;
}

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);
const LOCAL_OPERATOR_MARKER = "local-operator";
const LEGACY_LOCAL_OPERATOR_NAME = "operator";

interface LocalOperatorNameDeps {
  env?: Record<string, string | undefined>;
  homeDir?: () => string;
  readFile?: (path: string) => string;
  userInfo?: () => { username?: string | null };
}

/**
 * Resolve the web-console auth facet from trusted server configuration.
 *
 * This deliberately ignores request-controlled headers. A remote deployment must
 * be selected by boot-time config such as an explicit auth mode, allow-remote
 * flag, public URL, or non-loopback bind.
 */
export function resolveWebAuthMode(
  env: WebAuthEnv = process.env,
): WebAuthFacet {
  const explicit = clean(env.NEXUS_WEB_AUTH_MODE);
  if (explicit) {
    switch (explicit.toLowerCase()) {
      case "local":
      case "local-operator":
        return "local-operator";
      case "remote":
      case "remote-human":
        return "remote-human";
      case "remote-agent":
        return "remote-agent";
      case "peer":
        throw new Error("NEXUS_WEB_AUTH_MODE=peer is not implemented");
      default:
        throw new Error(
          `invalid NEXUS_WEB_AUTH_MODE: ${explicit}; expected local-operator, remote-human, remote-agent, or peer`,
        );
    }
  }

  if (truthy(env.NEXUS_WEB_ALLOW_REMOTE) || truthy(env.NEXUS_ALLOW_REMOTE)) {
    return "remote-human";
  }

  const publicUrl = clean(env.NEXUS_WEB_PUBLIC_URL) ?? clean(env.NEXUS_PUBLIC_URL);
  if (publicUrl && !isLoopbackUrl(publicUrl)) return "remote-human";

  const bind = clean(env.NEXUS_WEB_BIND) ?? clean(env.NEXUS_GATEWAY_BIND) ?? clean(env.HOST);
  if (bind && !isLoopbackBind(bind)) return "remote-human";

  return "local-operator";
}

/**
 * Compatibility wrapper for route adapters. The optional second argument exists
 * only to make the anti-spoof invariant testable: headers are ignored.
 */
export function webAuthModeFromEnv(
  env: WebAuthEnv = process.env,
  _requestHeaders?: Headers,
): WebAuthFacet {
  return resolveWebAuthMode(env);
}

export function isLocalOperatorWebAuthMode(mode: WebAuthMode | undefined): boolean {
  return mode === "local" || mode === "local-operator";
}

/** The daemon-recognized zero-login local operator caller shape. */
export function localOperatorCaller(
  project = "default",
  nameDeps: LocalOperatorNameDeps = {},
): GatewayCallerIdentity {
  const name = resolveLocalOperatorName(nameDeps);
  return {
    id: `local:${project}:${LOCAL_OPERATOR_MARKER}`,
    name,
    project,
    ...localPrincipalAttributes(),
    sessionId: LOCAL_OPERATOR_MARKER,
    runtimeId: LOCAL_OPERATOR_MARKER,
  };
}

export function resolveLocalOperatorName(
  deps: LocalOperatorNameDeps = {},
): string {
  return (
    operatorConfigName(deps) ??
    windowsEnvUserName(deps.env ?? process.env) ??
    cleanName((deps.userInfo ?? safeOsUserInfo)().username) ??
    unixEnvUserName(deps.env ?? process.env) ??
    LEGACY_LOCAL_OPERATOR_NAME
  );
}

export function isLocalOperatorCaller(
  caller: GatewayCallerIdentity | undefined,
): boolean {
  return (
    caller?.sessionId === LOCAL_OPERATOR_MARKER &&
    caller.runtimeId === LOCAL_OPERATOR_MARKER &&
    caller.kind === "human" &&
    caller.tier === "admin" &&
    (caller.credentialFacet === "local" || caller.credentialFacet === "machine") &&
    !caller.clientKey
  );
}

/** Remote mode still leaves health probes and signed producer ingress outside human login. */
export function requiresHumanLogin(method: string, path: string): boolean {
  const upper = method.toUpperCase();
  const cleanPath = path.replace(/\/+$/, "") || "/";
  if (upper === "GET" && cleanPath === "/api/v1/health") return false;
  if (upper === "POST" && cleanPath === "/api/v1/notify") return false;
  if (upper === "POST" && /^\/api\/v1\/sources\/[^/]+\/push$/.test(cleanPath)) {
    return false;
  }
  return true;
}

function truthy(value: string | undefined): boolean {
  const v = clean(value)?.toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function clean(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function cleanName(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function operatorConfigName(deps: LocalOperatorNameDeps): string | undefined {
  const env = deps.env ?? process.env;
  const readFile = deps.readFile ?? ((path: string) => readFileSync(path, "utf8"));
  try {
    const raw = readFile(join(resolveNexusHome(env, deps.homeDir), "operator.json"));
    const parsed = JSON.parse(raw) as { name?: unknown };
    return cleanName(parsed.name);
  } catch {
    return undefined;
  }
}

function resolveNexusHome(
  env: Record<string, string | undefined>,
  homeDir: (() => string) | undefined,
): string {
  return clean(env.NEXUS_HOME) ?? join(homeDir?.() ?? homedir(), ".nexus");
}

function windowsEnvUserName(env: Record<string, string | undefined>): string | undefined {
  const username = cleanName(env.USERNAME);
  const domain = cleanName(env.USERDOMAIN);
  const computerName = cleanName(env.COMPUTERNAME);
  if (username && domain && domain !== computerName && !username.includes("\\")) {
    return `${domain}\\${username}`;
  }
  return username;
}

function unixEnvUserName(env: Record<string, string | undefined>): string | undefined {
  return cleanName(env.USER) ?? cleanName(env.LOGNAME);
}

function safeOsUserInfo(): { username?: string | null } {
  try {
    return osUserInfo();
  } catch {
    return {};
  }
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
  return LOCAL_HOSTS.has(host.toLowerCase());
}
