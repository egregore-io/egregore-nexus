import { describe, expect, it } from "vitest";

import { canObserveAgentSession } from "./agui.observe";
import type { GatewayCallerIdentity } from "@server/api/http";
import type { AgentAccessGrantRow, AgentOwnerRow } from "@server/read/queries";
import { Kind, Tier } from "@shared/types";

function identity(
  name: string,
  project = "default",
  attrs: Partial<GatewayCallerIdentity> = {},
): GatewayCallerIdentity {
  return { name, project, ...attrs };
}

function ownedBy(
  name: string,
  project = "default",
  attrs: Partial<AgentOwnerRow> = {},
): AgentOwnerRow {
  return {
    agentId: "a_target",
    name: "target",
    project,
    ownerName: name,
    ownerProject: project,
    ownerSessionId: `s_${name}`,
    ...attrs,
  };
}

function grant(principalName: string, principalProject = "default", role = "viewer"): AgentAccessGrantRow {
  return {
    agentId: "a_target",
    principalProject,
    principalName,
    role,
  };
}

describe("canObserveAgentSession", () => {
  it("allows legacy rows without an owner", () => {
    expect(canObserveAgentSession(undefined, identity("operator"))).toBe(true);
    expect(
      canObserveAgentSession(
        { agentId: "a_target", name: "target", project: "default" },
        identity("operator"),
      ),
    ).toBe(true);
  });

  it("allows the owner to observe a managed agent session", () => {
    expect(canObserveAgentSession(ownedBy("operator"), identity("operator"))).toBe(true);
  });

  it("denies a non-owner even when they share the project", () => {
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice"))).toBe(false);
  });

  it("allows delegated viewer and co-owner grants to observe", () => {
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice"), [grant("alice")]))
      .toBe(true);
    expect(
      canObserveAgentSession(ownedBy("operator"), identity("cara"), [
        grant("cara", "default", "co_owner"),
      ]),
    ).toBe(true);
  });

  it("denies a revoked or wrong-project grant", () => {
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice"), [])).toBe(false);
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice", "ops"), [grant("alice")]))
      .toBe(false);
  });

  it("denies an owner name from another project", () => {
    expect(canObserveAgentSession(ownedBy("operator", "default"), identity("operator", "ops")))
      .toBe(false);
  });

  it("allows human admin observe override for protected managed targets", () => {
    const humanAdmin = identity("operator", "default", { kind: Kind.Human, tier: Tier.Admin });
    expect(canObserveAgentSession(ownedBy("owner", "default", { tier: "admin" }), humanAdmin))
      .toBe(true);
    expect(canObserveAgentSession(ownedBy("owner", "default", { role: "lead" }), humanAdmin))
      .toBe(true);
    expect(
      canObserveAgentSession(
        ownedBy("owner", "default", { sessionKind: "human", sessionTier: "admin" }),
        humanAdmin,
      ),
    ).toBe(true);
  });

  it("allows agent-admin observe override only for ordinary managed targets", () => {
    const agentAdmin = identity("agent-admin", "default", {
      kind: Kind.Agent,
      tier: Tier.Admin,
    });
    expect(canObserveAgentSession(ownedBy("owner"), agentAdmin)).toBe(true);
    expect(canObserveAgentSession(ownedBy("owner", "default", { tier: "admin" }), agentAdmin))
      .toBe(false);
    expect(canObserveAgentSession(ownedBy("owner", "default", { role: "lead" }), agentAdmin))
      .toBe(false);
    expect(canObserveAgentSession(ownedBy("owner", "default", { sessionKind: "human" }), agentAdmin))
      .toBe(false);
  });
});
