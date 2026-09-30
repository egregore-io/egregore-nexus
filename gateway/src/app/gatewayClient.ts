const configuredGateway = (
  import.meta as unknown as { env?: Record<string, string | undefined> }
).env?.NEXUS_GATEWAY_URL?.trim();

/** Resolve every browser request against one Gateway origin, never daemon discovery. */
export function gatewayUrl(path: string, base = configuredGateway): string {
  if (!base) return path;
  return new URL(path, base.endsWith("/") ? base : `${base}/`).toString();
}

export function gatewayFetch(path: string, init?: RequestInit): Promise<Response> {
  return fetch(gatewayUrl(path), init);
}
