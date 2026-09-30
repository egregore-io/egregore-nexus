import { createHash, randomBytes } from "node:crypto";

import type { Client } from "@libsql/client";
import { Kind, Tier } from "@shared/types";
import type {
  GatewayCallerIdentity,
  PrincipalScope,
} from "@server/api/http";
import {
  humanPrincipalAttributes,
  principalHasScope,
} from "@server/auth/principal";

const ACCESS_PREFIX = "nx_at";
const REFRESH_PREFIX = "nx_rt";
const DEFAULT_ACCESS_TTL_MS = 15 * 60_000;
const DEFAULT_REFRESH_TTL_MS = 30 * 24 * 60 * 60_000;

interface TokenRow {
  token_id: string;
  family_id: string;
  actor_name: string;
  actor_project: string;
  actor_kind: string;
  actor_tier: string;
  scopes_json: string;
  expires_at: number;
  refresh_expires_at: number;
  revoked_at: number | null;
}

export class BearerTokenError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "BearerTokenError";
  }
}

export interface BearerDeps {
  db: Client;
  now: () => number;
  genId?: () => string;
  randomSecret?: (prefix: string) => string;
}

export interface IssueBearerInput {
  actor: GatewayCallerIdentity;
  scopes: PrincipalScope[];
  ttlMs?: number;
  refreshTtlMs?: number;
}

export interface BearerTokenIssue {
  tokenId: string;
  familyId: string;
  accessToken: string;
  refreshToken: string;
  expiresAt: number;
  refreshExpiresAt: number;
  scopes: PrincipalScope[];
  tier: string;
}

export interface IssueOperatorBearerInput {
  name: string;
  project?: string;
  scopes: PrincipalScope[];
  ttlMs?: number;
  refreshTtlMs?: number;
}

export async function issueBearerToken(
  input: IssueBearerInput,
  deps: BearerDeps,
): Promise<BearerTokenIssue> {
  const now = deps.now();
  const scopes = normalizeScopes(input.scopes);
  if (scopes.length === 0) {
    throw new BearerTokenError(400, "at least one scope is required");
  }
  for (const scope of scopes) {
    if (!principalHasScope(input.actor, scope)) {
      throw new BearerTokenError(403, `scope not allowed: ${scope}`);
    }
  }

  const tokenId = `bt_${newId(deps)}`;
  const familyId = `bf_${newId(deps)}`;
  const accessToken = newSecret(ACCESS_PREFIX, deps);
  const refreshToken = newSecret(REFRESH_PREFIX, deps);
  const expiresAt = now + positive(input.ttlMs, DEFAULT_ACCESS_TTL_MS);
  const refreshExpiresAt = now + positive(input.refreshTtlMs, DEFAULT_REFRESH_TTL_MS);
  const tier = input.actor.tier ?? Tier.Agent;

  await deps.db.execute({
    sql: `INSERT INTO rest_bearer_token
            (token_id, family_id, actor_name, actor_project, actor_kind, actor_tier,
             scopes_json, access_hash, refresh_hash, expires_at, refresh_expires_at,
             revoked_at, last_used_at, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)`,
    args: [
      tokenId,
      familyId,
      input.actor.name,
      input.actor.project,
      input.actor.kind ?? Kind.Human,
      tier,
      JSON.stringify(scopes),
      hashToken(accessToken),
      hashToken(refreshToken),
      expiresAt,
      refreshExpiresAt,
      now,
    ],
  });

  return {
    tokenId,
    familyId,
    accessToken,
    refreshToken,
    expiresAt,
    refreshExpiresAt,
    scopes,
    tier,
  };
}

/**
 * Mint a local first-boot operator bearer for Lens-style bridges. The chosen
 * operator name is the bus identity carried by the token; callers still request
 * explicit scopes and receive an ordinary hashed REST bearer.
 */
export async function issueOperatorBearerToken(
  input: IssueOperatorBearerInput,
  deps: BearerDeps,
): Promise<BearerTokenIssue> {
  const name = input.name.trim();
  if (!name) {
    throw new BearerTokenError(400, "name is required");
  }
  const attrs = humanPrincipalAttributes();
  return issueBearerToken(
    {
      actor: {
        name,
        project: input.project?.trim() || "default",
        ...attrs,
      },
      scopes: input.scopes,
      ttlMs: input.ttlMs,
      refreshTtlMs: input.refreshTtlMs,
    },
    deps,
  );
}

export async function currentBearer(
  accessToken: string,
  deps: BearerDeps,
): Promise<GatewayCallerIdentity | null> {
  const row = await findByHash(deps.db, "access_hash", hashToken(accessToken));
  if (!row) return null;
  const now = deps.now();
  if (row.revoked_at != null || row.expires_at <= now) return null;

  await deps.db.execute({
    sql: "UPDATE rest_bearer_token SET last_used_at = ? WHERE token_id = ?",
    args: [now, row.token_id],
  });

  return rowToPrincipal(row);
}

export async function refreshBearerToken(
  refreshToken: string,
  deps: BearerDeps,
): Promise<BearerTokenIssue> {
  const row = await findByHash(deps.db, "refresh_hash", hashToken(refreshToken));
  if (!row) throw new BearerTokenError(401, "invalid refresh token");

  const now = deps.now();
  if (row.revoked_at != null) {
    await revokeFamily(deps.db, row.family_id, now);
    throw new BearerTokenError(401, "refresh token was already used or revoked");
  }
  if (row.refresh_expires_at <= now) {
    await revokeFamily(deps.db, row.family_id, now);
    throw new BearerTokenError(401, "refresh token expired");
  }

  await deps.db.execute({
    sql: "UPDATE rest_bearer_token SET revoked_at = ? WHERE token_id = ?",
    args: [now, row.token_id],
  });

  const accessToken = newSecret(ACCESS_PREFIX, deps);
  const nextRefreshToken = newSecret(REFRESH_PREFIX, deps);
  const tokenId = `bt_${newId(deps)}`;
  const scopes = parseScopes(row.scopes_json);
  const expiresAt = now + DEFAULT_ACCESS_TTL_MS;

  await deps.db.execute({
    sql: `INSERT INTO rest_bearer_token
            (token_id, family_id, actor_name, actor_project, actor_kind, actor_tier,
             scopes_json, access_hash, refresh_hash, expires_at, refresh_expires_at,
             revoked_at, last_used_at, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)`,
    args: [
      tokenId,
      row.family_id,
      row.actor_name,
      row.actor_project,
      row.actor_kind,
      row.actor_tier,
      JSON.stringify(scopes),
      hashToken(accessToken),
      hashToken(nextRefreshToken),
      expiresAt,
      row.refresh_expires_at,
      now,
    ],
  });

  return {
    tokenId,
    familyId: row.family_id,
    accessToken,
    refreshToken: nextRefreshToken,
    expiresAt,
    refreshExpiresAt: row.refresh_expires_at,
    scopes,
    tier: row.actor_tier,
  };
}

export async function revokeBearerToken(
  tokenId: string,
  deps: BearerDeps,
): Promise<boolean> {
  const now = deps.now();
  const res = await deps.db.execute({
    sql:
      "UPDATE rest_bearer_token SET revoked_at = COALESCE(revoked_at, ?) " +
      "WHERE token_id = ?",
    args: [now, tokenId],
  });
  return Number(res.rowsAffected) > 0;
}

async function findByHash(
  db: Client,
  column: "access_hash" | "refresh_hash",
  hash: string,
): Promise<TokenRow | null> {
  const res = await db.execute({
    sql:
      `SELECT token_id, family_id, actor_name, actor_project, actor_kind, actor_tier, ` +
      `scopes_json, expires_at, refresh_expires_at, revoked_at ` +
      `FROM rest_bearer_token WHERE ${column} = ? LIMIT 1`,
    args: [hash],
  });
  const row = res.rows[0];
  if (!row) return null;
  return {
    token_id: String(row.token_id),
    family_id: String(row.family_id),
    actor_name: String(row.actor_name),
    actor_project: String(row.actor_project),
    actor_kind: String(row.actor_kind),
    actor_tier: String(row.actor_tier),
    scopes_json: String(row.scopes_json),
    expires_at: Number(row.expires_at),
    refresh_expires_at: Number(row.refresh_expires_at),
    revoked_at: row.revoked_at == null ? null : Number(row.revoked_at),
  };
}

async function revokeFamily(db: Client, familyId: string, now: number): Promise<void> {
  await db.execute({
    sql:
      "UPDATE rest_bearer_token SET revoked_at = COALESCE(revoked_at, ?) " +
      "WHERE family_id = ?",
    args: [now, familyId],
  });
}

function rowToPrincipal(row: TokenRow): GatewayCallerIdentity {
  return {
    id: `bearer:${row.token_id}`,
    name: row.actor_name,
    project: row.actor_project,
    kind: parseKind(row.actor_kind),
    tier: parseTier(row.actor_tier),
    credentialFacet: "machine",
    scopes: parseScopes(row.scopes_json),
    tokenId: row.token_id,
    expiresAt: row.expires_at,
  };
}

function normalizeScopes(scopes: PrincipalScope[]): PrincipalScope[] {
  return [...new Set(scopes.map((s) => s.trim()).filter(Boolean))].sort();
}

function parseScopes(raw: string): PrincipalScope[] {
  try {
    const parsed = JSON.parse(raw) as unknown;
    return Array.isArray(parsed)
      ? normalizeScopes(parsed.filter((s): s is string => typeof s === "string"))
      : [];
  } catch {
    return [];
  }
}

function parseKind(value: string): Kind {
  return value === Kind.Agent || value === Kind.App || value === Kind.Notification
    ? value
    : Kind.Human;
}

function parseTier(value: string): Tier {
  return value === Tier.Admin ? Tier.Admin : Tier.Agent;
}

function positive(value: number | undefined, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? value
    : fallback;
}

function hashToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

function newId(deps: BearerDeps): string {
  return deps.genId?.() ?? randomBytes(12).toString("base64url");
}

function newSecret(prefix: string, deps: BearerDeps): string {
  return deps.randomSecret?.(prefix) ?? `${prefix}_${randomBytes(32).toString("base64url")}`;
}
