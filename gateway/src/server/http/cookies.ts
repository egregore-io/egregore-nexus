// Shared cookie-parsing utility. Used by the gateway routes (login.ts, api/v1/$.ts)
// that need to read a named cookie from the `Cookie` request header.

/**
 * Parse the `Cookie` request header (or `null`/`undefined` when absent) and
 * return a name→value Map. Accepts `string | null | undefined` so callers can
 * pass `request.headers.get("cookie")` directly (returns `null` when missing)
 * without a separate null-check.
 */
export function parseCookies(cookieHeader: string | null | undefined): Map<string, string> {
  const map = new Map<string, string>();
  if (!cookieHeader) return map;
  for (const part of cookieHeader.split(";")) {
    const eq = part.indexOf("=");
    if (eq === -1) continue;
    const key = part.slice(0, eq).trim();
    const val = part.slice(eq + 1).trim();
    if (key) map.set(key, val);
  }
  return map;
}
