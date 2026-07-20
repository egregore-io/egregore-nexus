import {
  csrfWebSocketProtocol,
  NEXUS_CSRF_COOKIE as CSRF_COOKIE,
  NEXUS_CSRF_HEADER as CSRF_HEADER,
  NEXUS_CSRF_PROTOCOL_PREFIX as CSRF_PROTOCOL_PREFIX,
} from "../../shared/browserCsrf.mjs";

const HUMAN_COOKIE = "nexus_human";
const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);
const CSRF_EXEMPT_PATHS = new Set(["/api/login"]);

export { csrfWebSocketProtocol };

/** Return a typed rejection when a cookie-authenticated browser mutation lacks CSRF proof. */
export function browserMutationCsrfFailure(request, options = {}) {
  if (options.enforce === false) return undefined;
  if (SAFE_METHODS.has(request.method.toUpperCase())) return undefined;
  if (CSRF_EXEMPT_PATHS.has(new URL(request.url).pathname)) return undefined;
  const cookies = parseCookieHeader(request.headers.get("cookie"));
  if (!cookies.has(HUMAN_COOKIE)) return undefined;
  const cookieToken = cookies.get(CSRF_COOKIE);
  const headerToken = request.headers.get(CSRF_HEADER);
  if (cookieToken && headerToken === cookieToken) return undefined;
  return new Response(JSON.stringify({
    error: { code: "forbidden", message: "missing or invalid CSRF token" },
  }), {
    status: 403,
    headers: { "content-type": "application/json" },
  });
}

/**
 * Bind a browser WebSocket handshake's double-submit proof to the connection Request.
 *
 * Browsers cannot set an arbitrary `x-nexus-csrf` upgrade header. They can request a
 * subprotocol, so the WebUI supplies `nexus-csrf.<token>` alongside its readable CSRF cookie.
 * Only a matching cookie/protocol pair becomes the internal header inherited by later writes.
 */
export function bindWebSocketCsrf(request) {
  const headers = new Headers(request.headers);
  // Never trust a network-supplied copy of the internal header. Only the
  // browser cookie + negotiated subprotocol pair below may mint it.
  headers.delete(CSRF_HEADER);
  const sanitized = new Request(request, { headers });
  const cookies = parseCookieHeader(request.headers.get("cookie"));
  const cookieToken = cookies.get(CSRF_COOKIE);
  if (!cookieToken) return sanitized;
  const protocols = (request.headers.get("sec-websocket-protocol") ?? "")
    .split(",")
    .map((value) => value.trim());
  const csrfProtocols = protocols.filter((value) => value.startsWith(CSRF_PROTOCOL_PREFIX));
  if (
    csrfProtocols.length !== 1 ||
    csrfProtocols[0].slice(CSRF_PROTOCOL_PREFIX.length) !== cookieToken
  ) {
    return sanitized;
  }
  headers.set(CSRF_HEADER, cookieToken);
  return new Request(request, { headers });
}

function parseCookieHeader(header) {
  const cookies = new Map();
  for (const part of (header ?? "").split(";")) {
    const index = part.indexOf("=");
    if (index <= 0) continue;
    const name = part.slice(0, index).trim();
    const value = part.slice(index + 1).trim();
    if (name) cookies.set(name, value);
  }
  return cookies;
}
