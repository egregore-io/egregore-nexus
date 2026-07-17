import {
  callDaemonQuery,
  DaemonIpcError,
  type DaemonIpcCallOptions,
  type DaemonIpcCaller,
} from "@server/daemon/ipc";
import { GatewayError, type SourceRegistryReader } from "@server/api/http";
import type { SourceRow, SourceSecretRow } from "@server/read/queries";

type Query = (
  method: string,
  params: unknown,
  caller: DaemonIpcCaller,
  options: DaemonIpcCallOptions,
) => Promise<unknown>;

export interface DaemonSourceRegistryOptions {
  nexusHome?: string;
  query?: Query;
}

const gatewayCaller: DaemonIpcCaller = {
  name: "Nexus Gateway",
  project: "default",
  sessionId: "local-operator",
  runtimeId: "local-operator",
  kind: "human",
  tier: "admin",
};

/**
 * Typed identity seam for notification sources.
 *
 * Source registrations and plaintext credentials remain daemon-owned identity state. The Gateway
 * asks for source metadata/token over authenticated local IPC, then performs the public HMAC
 * validation itself. This avoids reopening the disabled generic daemon SQL read path or copying
 * credentials into the Gateway history database.
 */
export function createDaemonSourceRegistry(
  options: DaemonSourceRegistryOptions = {},
): SourceRegistryReader {
  const query: Query = options.query ?? ((method, params, caller, callOptions) =>
    callDaemonQuery(method, params, caller, callOptions));
  const callOptions: DaemonIpcCallOptions = options.nexusHome
    ? { nexusHome: options.nexusHome }
    : {};

  const call = async <T>(method: string, params: unknown): Promise<T | undefined> => {
    try {
      return await query(method, params, gatewayCaller, callOptions) as T;
    } catch (error) {
      if (error instanceof DaemonIpcError && error.code === -32004) return undefined;
      if (error instanceof DaemonIpcError) {
        throw new GatewayError(error.code ?? 503, error.message, error.data);
      }
      throw error;
    }
  };

  const show = (name: string) => call<SourceRow>("source.show", { name });

  return {
    async list() {
      return (await call<{ sources: SourceRow[] }>("source.list", {})) ?? { sources: [] };
    },
    show,
    async secret(name: string): Promise<SourceSecretRow | undefined> {
      const source = await show(name);
      if (!source) return undefined;
      if (!source.enabled) return { ...source, token: "" };
      const token = await call<{ name: string; token: string }>("source.token", { name });
      return token ? { ...source, token: token.token } : undefined;
    },
  };
}
