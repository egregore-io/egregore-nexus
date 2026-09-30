import { homedir } from "node:os";
import { join } from "node:path";

export interface GatewayStoreConfig {
  url: string;
}

export class GatewayStoreConfigError extends Error {
  override readonly name = "GatewayStoreConfigError";
}

/** Resolve the Gateway-owned canonical database. Network libSQL is outside the local cutoff. */
export function gatewayStoreConfig(
  env: NodeJS.ProcessEnv = process.env,
  home: string = homedir(),
): GatewayStoreConfig {
  const configured = env.NEXUS_GATEWAY_DB ?? env.NEXUS_WEBCONSOLE_DB;
  const raw = configured?.trim() || `file:${join(home, ".nexus", "gateway.db")}`;
  if (!raw.startsWith("file:")) {
    throw new GatewayStoreConfigError(
      "NEXUS_GATEWAY_DB must be a local file: URL for this release",
    );
  }

  const path = raw.slice("file:".length);
  if (!path) {
    throw new GatewayStoreConfigError("NEXUS_GATEWAY_DB file: URL must include a path");
  }
  if (path === "~") return { url: `file:${home}` };
  if (path.startsWith("~/")) return { url: `file:${join(home, path.slice(2))}` };
  return { url: `file:${path}` };
}
