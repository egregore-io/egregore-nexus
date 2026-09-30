// Session state: the caller's bound identity and bearer token.
// Exposes the token getter the server-side daemon client reads when
// constructing the `Authorization: Bearer <token>` header.
//
// Server-side the token comes from the request session, not this module-level
// holder — `client.ts` injects a request-scoped getter. This holder is the
// process default / test seam.
import type { Whoami } from "@shared/types";

export interface SessionState {
  token: string | null;
  me?: Whoami;
}

let session: SessionState = { token: null };

/** Read the current bearer token (or `null` when unauthenticated). */
export function getSessionToken(): string | null {
  return session.token;
}

/** Set the session (token + resolved identity). Replaces the held state. */
export function setSession(next: SessionState): void {
  session = next;
}

/** Clear the session back to unauthenticated. */
export function clearSession(): void {
  session = { token: null };
}
