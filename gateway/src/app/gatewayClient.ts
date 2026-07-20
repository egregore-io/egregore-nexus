import {
  csrfWebSocketProtocol,
  NEXUS_CSRF_COOKIE,
  NEXUS_CSRF_HEADER,
} from "@shared/browserCsrf.mjs";

const configuredGateway = (
  import.meta as unknown as { env?: Record<string, string | undefined> }
).env?.NEXUS_GATEWAY_URL?.trim();
const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);

/** Resolve every browser request against one Gateway origin, never daemon discovery. */
export function gatewayUrl(path: string, base = configuredGateway): string {
  if (!base) return path;
  return new URL(path, base.endsWith("/") ? base : `${base}/`).toString();
}

export function gatewayFetch(path: string, init?: RequestInit): Promise<Response> {
  const headers = new Headers(init?.headers);
  const method = (init?.method ?? "GET").toUpperCase();
  // This header is connection-internal proof, never a caller override.
  headers.delete(NEXUS_CSRF_HEADER);
  if (!SAFE_METHODS.has(method)) {
    const csrf = browserCookie(NEXUS_CSRF_COOKIE);
    if (csrf) headers.set(NEXUS_CSRF_HEADER, csrf);
  }
  return fetch(gatewayUrl(path), {
    ...init,
    headers,
    // Human identity is cookie-backed. Silently accepting `omit` would turn a
    // browser mutation into a different principal at the Gateway boundary.
    credentials: "include",
  });
}

/** Browser WebSockets bind their readable CSRF cookie during the HTTP upgrade. */
export function gatewayWebSocketProtocols(): string[] {
  const token = browserCookie(NEXUS_CSRF_COOKIE);
  return token ? ["nexus-v1", csrfWebSocketProtocol(token)] : ["nexus-v1"];
}

function browserCookie(name: string): string | undefined {
  if (typeof document === "undefined") return undefined;
  for (const part of document.cookie.split(";")) {
    const index = part.indexOf("=");
    if (index <= 0 || part.slice(0, index).trim() !== name) continue;
    return part.slice(index + 1).trim() || undefined;
  }
  return undefined;
}
