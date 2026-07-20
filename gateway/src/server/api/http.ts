// Shared HTTP plumbing for the public API: the framework-agnostic request/
// response shapes, the dependency-injection bundle, the handler context, and the
// small result/error helpers. Kept in its own leaf module so `router.ts` and
// `handlers.ts` can both import it without a cycle.
import { z } from "zod";
import type { Client } from "@libsql/client";

import type { ReadDb } from "@drizzle/client";
import type { HumanReadDeliveryMarker } from "@server/delivery/humanRead";
import type { Ack, SendRequest, Kind, Locality, Tier } from "@shared/types";
import type { SourceRow, SourceSecretRow } from "@server/read/queries";

/** Credential facet that produced the gateway caller Principal. */
export type CredentialFacet =
  | "human"
  | "machine"
  | "remote-mcp"
  | "source"
  | "local";

/** Capability scope carried by a Principal; policy may also layer owner predicates over it. */
export type PrincipalScope = string;

/** Gateway caller Principal with durable fields needed for command-ingress writes. */
export interface GatewayCallerIdentity {
  id?: string;
  name: string;
  project: string;
  kind?: Kind;
  locality?: Locality;
  access?: string;
  principalId?: string;
  tier?: Tier;
  scopes?: PrincipalScope[];
  credentialFacet?: CredentialFacet;
  sessionId?: string;
  agentId?: string;
  runtimeId?: string;
  clientKey?: string;
  tokenId?: string;
  expiresAt?: number;
}

/** Gateway-local operational error used by command-ingress and API handlers. */
export class GatewayError extends Error {
  constructor(
    readonly code: number,
    message: string,
    readonly details?: unknown,
  ) {
    super(message);
    this.name = "GatewayError";
  }
}

/** Message Post write seam; production submits daemon command intents. */
export interface MessagePostSendOptions {
  /** Stable producer-supplied key used to dedupe retry/double-submit windows. */
  idempotencyKey?: string;
}

export interface MessagePostSender {
  send(
    req: SendRequest,
    caller: GatewayCallerIdentity,
    opts?: MessagePostSendOptions,
  ): Promise<Ack>;
}

/** Generic daemon command seam; production submits `command_intents` rows. */
export interface CommandIntentSender {
  submit<T = unknown>(
    kind: string,
    req: unknown,
    caller?: GatewayCallerIdentity,
    opts?: { idempotencyKey?: string },
  ): Promise<T>;
}

/** Daemon-owned notification-source identity view used by the Gateway edge. */
export interface SourceRegistryReader {
  list(): Promise<{ sources: SourceRow[] }>;
  show(name: string): Promise<SourceRow | undefined>;
  secret(name: string): Promise<SourceSecretRow | undefined>;
}

/** Gateway-local, read-only hook registry and attestation diagnostics. */
export interface HookDiagnosticsReader {
  list(includePrivate: boolean): Promise<unknown>;
  publicKey(): Promise<unknown>;
  audit(limit: number, includePrivate: boolean): Promise<unknown>;
}

/**
 * A plain inbound request — NOT the framework `Request`. The TanStack catch-all
 * route (`routes/api/v1/$.ts`) adapts the real request into this shape; tests
 * construct it directly. Keeping the router off the framework type is what makes
 * it unit-testable and portable.
 */
export interface ApiRequest {
  /** Upper-case HTTP method (GET/POST/DELETE/…). */
  method: string;
  /** The full path, e.g. `/api/v1/threads/design/history`. */
  path: string;
  /** Parsed query string as a flat string map. */
  query: Record<string, string | undefined>;
  /** Lower-cased header map (at least `authorization`). */
  headers: Record<string, string | undefined>;
  /**
   * Authenticated gateway caller, when a gateway session cookie resolves.
   * Read handlers use this for caller-relative views such as `/whoami`; command
   * handlers persist it on `command_intents` so the daemon worker can resolve
   * the same caller without gateway HTTP RPC.
   */
  caller?: GatewayCallerIdentity;
  /** The already-parsed JSON body (for POST/PUT/PATCH); `undefined` otherwise. */
  body?: unknown;
  /**
   * The raw body string (the exact bytes received before JSON.parse). Populated
   * by the TanStack catch-all adapter for body-carrying methods (POST/PUT/PATCH).
   * Required by the signed-push edge: HMAC is computed over the raw bytes so that
   * re-serialisation key-reordering never breaks the signature.
   */
  rawBody?: string;
}

/** The router's plain response — the catch-all route writes it to the framework. */
export interface ApiResponse {
  status: number;
  body: unknown;
}

/**
 * A LAZY getter for the read-only Drizzle handle. Read handlers call `deps.db()`
 * only when they actually need the read view; non-read routes (health, all
 * writes/ops) and any request that fails auth/validation never invoke it, so the
 * (potentially throwing / I/O-bound) read DB is never constructed for them. The
 * getter is async so a backing handle can be built/awaited on demand (and the
 * adapter memoizes it). Tests can pass `() => Promise.resolve(seededDb)`.
 */
export type ReadDbGetter = () => Promise<ReadDb> | ReadDb;

/**
 * The injected dependencies. Tests pass a seeded `db` getter plus optional
 * command seams; production passes command ingress for daemon-managed writes and
 * a getter over `createReadDb()`. This keeps the router pure and the translation
 * targets swappable.
 */
export interface ApiDeps {
  /** Shared secret used only at the public notification HMAC edge. Empty/unset disables ingress. */
  notifyHmacSecret?: string;
  /** Epoch-ms clock for timestamp freshness checks; injectable for deterministic tests. */
  now?: () => number;
  /** Optional Message Post sender for committed bus writes. */
  messagePost?: MessagePostSender;
  /** Optional generic command sender for migrated writes. */
  commands?: CommandIntentSender;
  /** Typed daemon identity seam for source metadata and HMAC credentials. */
  sourceRegistry?: SourceRegistryReader;
  /** Read-only diagnostics for the Gateway-owned message-hook service. */
  hooks?: HookDiagnosticsReader;
  /**
   * Lazy provider for the read-only Drizzle handle (display/query). Constructed
   * ONLY when a read handler calls it — never eagerly per request.
   */
  db: ReadDbGetter;
  /** Canonical Gateway store. When present, migrated public reads never touch the daemon view. */
  canonicalDb?: () => Promise<Client> | Client;
  /**
   * Optional daemon-DB write seam for human read receipt. Message-bearing GETs
   * call this after a successful read so human browser sessions drain their own
   * `in_flight` rows without adding writes to `server/read`.
   */
  humanReads?: HumanReadDeliveryMarker;
}

/** What every handler receives: the request, the deps, and matched path params. */
export interface HandlerCtx {
  req: ApiRequest;
  deps: ApiDeps;
  /** Path params extracted from the route pattern (e.g. `{ name: "design" }`). */
  params: Record<string, string>;
}

/** A route handler — thin: validate → (`commands` | `messagePost` | read-view) → result. */
export type Handler = (ctx: HandlerCtx) => Promise<ApiResponse> | ApiResponse;

// ── result helpers ────────────────────────────────────────────────────────────

/** A 2xx JSON result (defaults to 200). */
export function ok(body: unknown, status = 200): ApiResponse {
  return { status, body };
}

/** A structured error result. The body is a flat `{ error: { code, message } }`
 *  — never a stack, never internal detail. */
export function fail(status: number, message: string, code?: string): ApiResponse {
  return { status, body: { error: { code: code ?? httpCodeName(status), message } } };
}

function httpCodeName(status: number): string {
  switch (status) {
    case 400:
      return "bad_request";
    case 401:
      return "unauthorized";
    case 403:
      return "forbidden";
    case 404:
      return "not_found";
    case 409:
      return "conflict";
    case 422:
      return "unprocessable";
    case 500:
      return "internal_error";
    default:
      return status >= 500 ? "server_error" : "client_error";
  }
}

/**
 * Thrown by `parseBody` on a Zod validation failure so the router can map it to a
 * 400. Carries a compact, leak-free message (the flattened field issues).
 */
export class ValidationError extends Error {
  constructor(public issues: string) {
    super(`invalid request body: ${issues}`);
    this.name = "ValidationError";
  }
}

/**
 * Validate `ctx.req.body` against a Zod schema, returning the parsed value or
 * throwing `ValidationError` (→ 400). Centralized so every write handler is one
 * line of validation. The result is structurally the matching contract DTO.
 */
export function parseBody<T>(ctx: HandlerCtx, schema: z.ZodType<T>): T {
  const result = schema.safeParse(ctx.req.body ?? {});
  if (!result.success) {
    // Compact, deterministic, leak-free issue summary.
    const issues = result.error.issues
      .map((i) => `${i.path.join(".") || "<root>"}: ${i.message}`)
      .join("; ");
    throw new ValidationError(issues);
  }
  return result.data;
}
