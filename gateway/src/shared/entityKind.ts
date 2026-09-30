import { Kind, Locality } from "@shared/types";
import type { MemberRow } from "@shared/readView";

export interface ParsedEntityKind {
  kind: Kind;
  locality: Locality;
}

export class EntityKindError extends Error {
  readonly code = "invalid_entity_kind";

  constructor(value: unknown) {
    super(`unknown entity kind ${JSON.stringify(value)}`);
    this.name = "EntityKindError";
  }
}

/** Parse canonical dotted kinds while preserving bare legacy values as local. */
export function parseEntityKind(
  value: unknown,
  parallelLocality?: unknown,
): ParsedEntityKind {
  if (typeof value !== "string" || !value) throw new EntityKindError(value);
  const parts = value.split(".");
  if (parts.length > 2) throw new EntityKindError(value);

  const kindToken = parts.length === 2 ? parts[1] : parts[0];
  const localityToken = parts.length === 2
    ? parts[0]
    : parallelLocality === undefined
      ? Locality.Local
      : parallelLocality;
  const kind = parseNature(kindToken);
  const locality = parseLocality(localityToken);
  if (parts.length === 2 && parallelLocality !== undefined && locality !== parallelLocality) {
    throw new EntityKindError({ value, locality: parallelLocality });
  }
  return { kind, locality };
}

export function dottedEntityKind(locality: Locality, kind: Kind): string {
  return `${locality}.${kind}`;
}

export function isAgentMember(member: Pick<MemberRow, "kind" | "agent">): boolean {
  if (member.kind === undefined) return Boolean(member.agent);
  try {
    return parseEntityKind(member.kind).kind === Kind.Agent;
  } catch {
    return false;
  }
}

function parseNature(value: unknown): Kind {
  switch (value) {
    case Kind.Agent:
    case Kind.Human:
    case Kind.Notification:
    case Kind.App:
      return value;
    default:
      throw new EntityKindError(value);
  }
}

function parseLocality(value: unknown): Locality {
  switch (value) {
    case Locality.Local:
    case Locality.External:
    case Locality.Trusted:
      return value;
    default:
      throw new EntityKindError(value);
  }
}
