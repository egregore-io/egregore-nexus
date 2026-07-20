import { Kind, Locality, Tier } from "@shared/types";
import type {
  CredentialFacet,
  GatewayCallerIdentity,
  PrincipalScope,
} from "@server/api/http";

export const AGENT_ATTACH_SCOPE: PrincipalScope = "agent:attach";

export const HUMAN_ADMIN_SCOPES: PrincipalScope[] = [
  "message:read",
  "message:send",
  "thread:read",
  "thread:write",
  "agent:read",
  AGENT_ATTACH_SCOPE,
  "agent:launch",
  "agent:admin",
  "runtime:register",
  "source:manage",
  "source:push",
  "search:read",
  "inbox:consume",
  "admin:*",
];

export interface PrincipalAttributes {
  kind: Kind;
  locality: Locality;
  tier: Tier;
  credentialFacet: CredentialFacet;
  scopes: PrincipalScope[];
}

export function humanPrincipalAttributes(): PrincipalAttributes {
  return {
    kind: Kind.Human,
    locality: Locality.Local,
    tier: Tier.Admin,
    credentialFacet: "human",
    scopes: [...HUMAN_ADMIN_SCOPES],
  };
}

export function localPrincipalAttributes(): PrincipalAttributes {
  return {
    kind: Kind.Human,
    locality: Locality.Local,
    tier: Tier.Admin,
    credentialFacet: "local",
    scopes: [...HUMAN_ADMIN_SCOPES],
  };
}

export interface PrincipalAuthorizationContext {
  /** Future owner/grant predicates can layer on top of scope+tier without rewriting route policy. */
  ownsResource?: boolean;
}

export function principalHasScope(
  principal: GatewayCallerIdentity | undefined,
  required: PrincipalScope,
  _ctx: PrincipalAuthorizationContext = {},
): boolean {
  const scopes = principal?.scopes ?? [];
  if (scopes.includes("admin:*")) return true;
  if (scopes.includes(required)) return true;

  const colon = required.indexOf(":");
  if (colon > 0 && scopes.includes(`${required.slice(0, colon)}:*`)) {
    return true;
  }

  return false;
}

export function principalMeetsTier(
  principal: GatewayCallerIdentity | undefined,
  required: Tier = Tier.Agent,
): boolean {
  return tierRank(principal?.tier ?? Tier.Agent) >= tierRank(required);
}

function tierRank(tier: Tier): number {
  return tier === Tier.Admin ? 2 : 1;
}
