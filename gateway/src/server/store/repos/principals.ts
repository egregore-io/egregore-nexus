import { createHash } from "node:crypto";

import type { Client, Row } from "@libsql/client";

import { dottedEntityKind, parseEntityKind } from "@server/identity/entityKind";
import { Kind, Locality } from "@shared/types";

export interface PrincipalRow {
  principalId: string;
  kind: string;
  access: string;
  createdAt: number;
}

export interface BindHumanPrincipalInput {
  humanUserId: string;
}

export interface UpsertSubjectBindingInput {
  provider: string;
  externalUserId: string;
  displayName?: string;
  kind: string;
}

/** Stable migration/new-write account id derived from the already-unique durable client key. */
export function humanUserIdForClientKey(clientKey: string): string {
  return stableId("hu", "human-user", required(clientKey, "clientKey"));
}

/** Stable principal id for one immutable human account id. */
export function principalIdForHumanUserId(humanUserId: string): string {
  const id = required(humanUserId, "humanUserId");
  if (!/^hu_[a-f0-9]{24}$/.test(id)) throw new Error(`invalid humanUserId ${id}`);
  return `h_${id.slice(3)}`;
}

export async function principalById(
  db: Client,
  principalId: string,
): Promise<PrincipalRow | undefined> {
  const result = await db.execute({
    sql: `SELECT principal_id, kind, access, created_at
          FROM principals WHERE principal_id = ? LIMIT 1`,
    args: [principalId],
  });
  return result.rows[0] ? mapPrincipal(result.rows[0]) : undefined;
}

export async function principalByAlias(
  db: Client,
  alias: string,
): Promise<PrincipalRow | undefined> {
  const result = await db.execute({
    sql: `SELECT p.principal_id, p.kind, p.access, p.created_at
          FROM principal_aliases AS a
          JOIN principals AS p ON p.principal_id = a.principal_id
          WHERE a.alias = ? LIMIT 1`,
    args: [alias],
  });
  return result.rows[0] ? mapPrincipal(result.rows[0]) : undefined;
}

/** Bind one immutable human_user_id to exactly one local-human principal. */
export async function bindHumanPrincipal(
  db: Client,
  input: BindHumanPrincipalInput,
): Promise<PrincipalRow> {
  const humanUserId = required(input.humanUserId, "humanUserId");
  const principalId = principalIdForHumanUserId(humanUserId);
  const alias = `human:${humanUserId}`;
  const createdAt = Date.now();
  await db.batch(
    [
      {
        sql: `INSERT OR IGNORE INTO principals
                (principal_id, kind, access, created_at)
              VALUES (?, 'local.human', 'admin', ?)`,
        args: [principalId, createdAt],
      },
      {
        sql: `INSERT OR IGNORE INTO principal_aliases (principal_id, alias)
              VALUES (?, ?)`,
        args: [principalId, alias],
      },
    ],
    "write",
  );

  const bound = await principalByAlias(db, alias);
  if (!bound || bound.principalId !== principalId) {
    throw new Error(`human principal binding conflict for ${humanUserId}`);
  }
  requirePrincipalShape(bound, "local.human", "admin");
  return bound;
}

/** Find-or-create the principal bound to one provider subject. */
export async function upsertSubjectBinding(
  db: Client,
  input: UpsertSubjectBindingInput,
): Promise<PrincipalRow> {
  const provider = required(input.provider, "provider");
  const externalUserId = required(input.externalUserId, "externalUserId");
  const parsed = parseEntityKind(input.kind);
  if (parsed.locality !== Locality.External) {
    throw new Error(`subject principal must be external, got ${input.kind}`);
  }
  const kind = dottedEntityKind(parsed.locality, parsed.kind);
  const existing = await subjectPrincipal(db, provider, externalUserId);
  if (existing) {
    requirePrincipalShape(existing, kind, "guest");
    await updateSubjectDisplayName(db, provider, externalUserId, input.displayName);
    return existing;
  }

  const principalId = externalPrincipalIdForSubject(provider, externalUserId);
  const createdAt = Date.now();
  await db.batch(
    [
      {
        sql: `INSERT OR IGNORE INTO principals
                (principal_id, kind, access, created_at)
              VALUES (?, ?, 'guest', ?)`,
        args: [principalId, kind, createdAt],
      },
      {
        sql: `INSERT OR IGNORE INTO subject_bindings
                (provider, external_user_id, principal_id, display_name, created_at)
              VALUES (?, ?, ?, ?, ?)`,
        args: [provider, externalUserId, principalId, input.displayName ?? null, createdAt],
      },
    ],
    "write",
  );
  await updateSubjectDisplayName(db, provider, externalUserId, input.displayName);

  const bound = await subjectPrincipal(db, provider, externalUserId);
  if (!bound) throw new Error(`failed to bind subject ${provider}/${externalUserId}`);
  requirePrincipalShape(bound, kind, "guest");
  return bound;
}

async function subjectPrincipal(
  db: Client,
  provider: string,
  externalUserId: string,
): Promise<PrincipalRow | undefined> {
  const result = await db.execute({
    sql: `SELECT p.principal_id, p.kind, p.access, p.created_at
          FROM subject_bindings AS b
          JOIN principals AS p ON p.principal_id = b.principal_id
          WHERE b.provider = ? AND b.external_user_id = ? LIMIT 1`,
    args: [provider, externalUserId],
  });
  return result.rows[0] ? mapPrincipal(result.rows[0]) : undefined;
}

async function updateSubjectDisplayName(
  db: Client,
  provider: string,
  externalUserId: string,
  displayName: string | undefined,
): Promise<void> {
  if (displayName === undefined) return;
  await db.execute({
    sql: `UPDATE subject_bindings SET display_name = ?
          WHERE provider = ? AND external_user_id = ?`,
    args: [displayName, provider, externalUserId],
  });
}

export function externalPrincipalIdForSubject(provider: string, externalUserId: string): string {
  return stableId(
    "x",
    "external-subject",
    `${required(provider, "provider")}\0${required(externalUserId, "externalUserId")}`,
  );
}

function mapPrincipal(row: Row): PrincipalRow {
  return {
    principalId: String(row.principal_id),
    kind: String(row.kind),
    access: String(row.access),
    createdAt: Number(row.created_at),
  };
}

function requirePrincipalShape(row: PrincipalRow, kind: string, access: string): void {
  if (row.kind !== kind || row.access !== access) {
    throw new Error(
      `principal ${row.principalId} has ${row.kind}/${row.access}, expected ${kind}/${access}`,
    );
  }
}

function stableId(prefix: "hu" | "h" | "x", domain: string, value: string): string {
  const digest = createHash("sha256")
    .update(`nexus-v016:${domain}\0`, "utf8")
    .update(value, "utf8")
    .digest("hex")
    .slice(0, 24);
  return `${prefix}_${digest}`;
}

function required(value: string, field: string): string {
  const clean = value.trim();
  if (!clean) throw new Error(`${field} is required`);
  return clean;
}
