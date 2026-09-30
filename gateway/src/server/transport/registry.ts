export type GatewayTransportState =
  | "starting"
  | "running"
  | "backoff"
  | "disabled"
  | "stopped";

export interface GatewayTransportStateRow {
  name: string;
  state: GatewayTransportState;
}

interface GatewayTransportStateReader {
  states(): GatewayTransportStateRow[];
}

let activeTransportHost: GatewayTransportStateReader | undefined;

/** Publish the process-local host that owns the live transport capability registry. */
export function publishGatewayTransportHost(host: GatewayTransportStateReader): () => void {
  activeTransportHost = host;
  return () => {
    if (activeTransportHost === host) activeTransportHost = undefined;
  };
}

/** Read current providers from the live host; no host is truthfully an empty registry. */
export function gatewayTransportStates(): GatewayTransportStateRow[] {
  return activeTransportHost?.states() ?? [];
}
