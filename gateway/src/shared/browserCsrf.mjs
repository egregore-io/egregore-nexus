export const NEXUS_CSRF_COOKIE = "nexus_csrf";
export const NEXUS_CSRF_HEADER = "x-nexus-csrf";
export const NEXUS_CSRF_PROTOCOL_PREFIX = "nexus-csrf.";

const PROTOCOL_SAFE_TOKEN = /^[A-Za-z0-9._~-]+$/;

/** Encode a readable CSRF cookie as a standards-safe WebSocket subprotocol token. */
export function csrfWebSocketProtocol(token) {
  if (typeof token !== "string" || !PROTOCOL_SAFE_TOKEN.test(token)) {
    throw new Error("CSRF token must be WebSocket protocol-safe");
  }
  return `${NEXUS_CSRF_PROTOCOL_PREFIX}${token}`;
}
