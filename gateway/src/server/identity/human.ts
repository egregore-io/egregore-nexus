// Human session backend (Task 2a).
//
// `registerHuman` creates or verifies a web-console human account, enqueues a
// daemon-owned identity registration command (Kind::Human / Tier::Admin /
// Harness::Other),
// and persists a `human_session` row in the front door's own store
// (webconsole.db). The durable `human_user.client_key` is stable across browser
// sessions, so daemon registration resumes the same human identity instead of
// minting a new one on every login.
//
// `currentHuman` is the Principal resolver: given a cookie token it returns
// durable identity plus facet/tier/scope metadata. The gateway uses this for
// command-ingress caller attribution and policy.
//
// Dep-injection pattern (mirrors the conversation store): `genId` / `now` are
// injected via `deps` so unit tests can be fully deterministic without spying
// on globals.
import type { Client } from "@libsql/client";
import { randomBytes, scrypt as scryptCb, timingSafeEqual } from "node:crypto";
import { Harness, Tier, Kind, type RegisterResponse } from "@shared/types";
import type { CommandIntentSender, GatewayCallerIdentity } from "@server/api/http";
import { humanPrincipalAttributes } from "@server/auth/principal";
import { COMMAND_KINDS } from "@server/command/ingress";

const SCRYPT_N = 16_384;
const SCRYPT_R = 8;
const SCRYPT_P = 1;
const PASSWORD_KEY_BYTES = 32;

function normalizeName(name: string): string {
  return name.trim().toLowerCase();
}

function scryptPassword(
  password: string,
  salt: string,
  opts: { n?: number; r?: number; p?: number } = {},
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    scryptCb(
      password,
      salt,
      PASSWORD_KEY_BYTES,
      {
        N: opts.n ?? SCRYPT_N,
        r: opts.r ?? SCRYPT_R,
        p: opts.p ?? SCRYPT_P,
      },
      (err, derivedKey) => {
        if (err) reject(err);
        else resolve(derivedKey);
      },
    );
  });
}

async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(16).toString("hex");
  const derived = await scryptPassword(password, salt);
  return `scrypt$${SCRYPT_N}$${SCRYPT_R}$${SCRYPT_P}$${salt}$${derived.toString("hex")}`;
}

async function verifyPassword(password: string, encoded: string): Promise<boolean> {
  const parts = encoded.split("$");
  if (parts.length !== 6 || parts[0] !== "scrypt") return false;

  const [, nRaw, rRaw, pRaw, salt, hashHex] = parts;
  const n = Number(nRaw);
  const r = Number(rRaw);
  const p = Number(pRaw);
  if (!Number.isFinite(n) || !Number.isFinite(r) || !Number.isFinite(p)) {
    return false;
  }

  const expected = Buffer.from(hashHex ?? "", "hex");
  if (expected.length === 0) return false;
  const actual = await scryptPassword(password, salt ?? "", { n, r, p });
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

export class HumanAuthError extends Error {
  constructor(message = "invalid name or password") {
    super(message);
    this.name = "HumanAuthError";
  }
}

// ── Deps ─────────────────────────────────────────────────────────────────────

export interface HumanDeps {
  /** The front-door's writable libSQL client (webconsole.db). */
  db: Client;
  /** Store-backed command sender for daemon-owned identity registration. */
  commands: CommandIntentSender;
  /**
   * Random id generator — called twice per `registerHuman`: once for
   * `clientKey`, once for `cookieToken`.  Inject a deterministic mock in tests.
   */
  genId: () => string;
  /** Epoch-ms clock.  Inject in tests to avoid non-determinism. */
  now: () => number;
}

/** Deps for read-only `currentHuman` (no RPC needed). */
export interface ReadHumanDeps {
  db: Client;
}

// ── Types ────────────────────────────────────────────────────────────────────

export interface RegisterHumanInput {
  /** The display name the operator chose (must be unique on the daemon). */
  name: string;
  /** Password for the stable web-console human account. */
  password: string;
  /** Optional project; defaults to `"default"`. */
  project?: string;
}

export interface RegisterHumanResult {
  /** Opaque token to set as an HTTP-only cookie (`human_session.cookie_token`). */
  cookieToken: string;
  name: string;
  project: string;
  /** The daemon-assigned session id. */
  sessionId: string;
}

export interface HumanIdentity extends GatewayCallerIdentity {
  id: string;
  /** The daemon session id — carried so the gateway can resume the session. */
  sessionId: string;
  /** Stable daemon client key for this human account; carried onto command-intent caller metadata. */
  clientKey: string;
}

// ── registerHuman ────────────────────────────────────────────────────────────

/**
 * Register the web-console operator with the daemon as a Human and persist a
 * login session row.  Idempotent on `clientKey`: re-registering the same human
 * (same `clientKey`) is safe — the daemon reuses the existing session.
 */
export async function registerHuman(
  input: RegisterHumanInput,
  deps: HumanDeps,
): Promise<RegisterHumanResult> {
  const { db, commands, genId, now } = deps;
  const name = input.name.trim();
  const nameKey = normalizeName(name);
  if (!nameKey) throw new Error("name is required");
  if (!input.password) throw new HumanAuthError();

  const existing = await db.execute({
    sql: `SELECT name, password_hash, client_key, project, daemon_session_id
          FROM human_user
          WHERE name_key = ?
          LIMIT 1`,
    args: [nameKey],
  });

  let displayName = name;
  let project = input.project ?? "default";
  let clientKey: string;
  let previousSessionId: string | null = null;
  let newUserPasswordHash: string | null = null;

  const existingUser = existing.rows[0];
  if (existingUser) {
    const ok = await verifyPassword(input.password, String(existingUser.password_hash));
    if (!ok) throw new HumanAuthError();
    displayName = String(existingUser.name);
    project = String(existingUser.project);
    clientKey = String(existingUser.client_key);
    previousSessionId =
      existingUser.daemon_session_id == null
        ? null
        : String(existingUser.daemon_session_id);
  } else {
    clientKey = genId();
    newUserPasswordHash = await hashPassword(input.password);
  }

  // Generate a fresh browser-login cookie, while keeping the daemon client key stable.
  const cookieToken = genId();

  // Register with the daemon as a human operator through command ingress. The
  // daemon worker remains the identity gatekeeper; the gateway only enqueues.
  const response = await commands.submit<RegisterResponse>(
    COMMAND_KINDS.identityRegister,
    {
      name: displayName,
      harness: Harness.Other,
      harnessSessionId: `hs_${clientKey}`,
      project,
      clientKey,
      tier: Tier.Admin,
      kind: Kind.Human,
    },
    {
      name: displayName,
      project,
      kind: Kind.Human,
      tier: Tier.Admin,
      credentialFacet: "human",
      scopes: humanPrincipalAttributes().scopes,
      sessionId: previousSessionId ?? undefined,
      runtimeId: previousSessionId ?? undefined,
      clientKey,
    },
  );

  // A successful register always returns the bound session id; a missing one means the
  // registration did not actually bind, so fail loudly rather than persist an empty row.
  const rawSessionId =
    typeof response === "object" && response !== null && "sessionId" in response
      ? (response as { sessionId: unknown }).sessionId
      : undefined;
  const sessionId = typeof rawSessionId === "string" ? rawSessionId : "";
  if (!sessionId) {
    throw new Error(
      `registerHuman: daemon register returned no sessionId for "${displayName}" — registration did not bind`,
    );
  }

  const at = now();
  if (newUserPasswordHash) {
    await db.execute({
      sql: `INSERT INTO human_user
              (name_key, name, password_hash, client_key, project, daemon_session_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        nameKey,
        displayName,
        newUserPasswordHash,
        clientKey,
        project,
        sessionId || previousSessionId,
        at,
        at,
      ],
    });
  } else {
    await db.execute({
      sql: `UPDATE human_user
            SET daemon_session_id = ?, updated_at = ?
            WHERE name_key = ?`,
      args: [sessionId, at, nameKey],
    });
  }

  // Persist the session row.
  await db.execute({
    sql: `INSERT INTO human_session
            (cookie_token, name, client_key, project, daemon_session_id, created_at)
          VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT(cookie_token) DO UPDATE SET
            daemon_session_id = excluded.daemon_session_id`,
    args: [cookieToken, displayName, clientKey, project, sessionId, at],
  });

  return {
    cookieToken,
    name: displayName,
    project,
    sessionId: sessionId || previousSessionId || "",
  };
}

// ── currentHuman ─────────────────────────────────────────────────────────────

/**
 * Resolve a cookie token to the persisted human identity, or `null` if the
 * token is unknown / expired.  This is the `getCaller` source — wire it into
 * the daemon client for per-request attribution.
 */
export async function currentHuman(
  cookieToken: string,
  deps: ReadHumanDeps,
): Promise<HumanIdentity | null> {
  const { db } = deps;
  const res = await db.execute({
    sql: `SELECT name, project, daemon_session_id, client_key
          FROM human_session
          WHERE cookie_token = ?
          LIMIT 1`,
    args: [cookieToken],
  });

  const row = res.rows[0];
  if (!row) return null;

  return {
    id: `human:${String(row.project)}:${String(row.client_key)}`,
    name: String(row.name),
    project: String(row.project),
    ...humanPrincipalAttributes(),
    sessionId: String(row.daemon_session_id),
    clientKey: String(row.client_key),
  };
}
