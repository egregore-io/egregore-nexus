import { afterEach, describe, expect, it, vi } from "vitest";
import { createClient } from "@libsql/client";

import { canObserveAgentSession, observeScoped } from "./agui.observe";
import type { GatewayCallerIdentity } from "@server/api/http";
import type { AgentAccessGrantRow, AgentOwnerRow } from "@server/read/queries";
import { Kind, Tier } from "@shared/types";
import { migrateGatewayStore } from "@server/store/migrations";
import { sessionFanout } from "@server/stream/sessionFanout";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function identity(
  name: string,
  project = "default",
  attrs: Partial<GatewayCallerIdentity> = {},
): GatewayCallerIdentity {
  return { name, project, ...attrs };
}

function ownedBy(
  name: string,
  _project = "default",
  attrs: Partial<AgentOwnerRow> = {},
): AgentOwnerRow {
  return {
    agentId: "a_target",
    name: "target",
    ownerName: name,
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
        { agentId: "a_target", name: "target" },
        identity("operator"),
      ),
    ).toBe(true);
  });

  it("allows the owner to observe a managed agent session", () => {
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("operator", "default", { sessionId: "s_operator" }),
    )).toBe(true);
  });

  it("uses a stable owner agent id before mutable owner-name metadata", () => {
    const owner = ownedBy("old-owner-name", "default", { ownerAgentId: "a_owner" });

    expect(canObserveAgentSession(
      owner,
      identity("renamed-owner", "other", { agentId: "a_owner" }),
    )).toBe(true);
    expect(canObserveAgentSession(
      owner,
      identity("old-owner-name", "default", { agentId: "a_intruder" }),
    )).toBe(false);
    expect(canObserveAgentSession(
      owner,
      identity("old-owner-name"),
    )).toBe(false);
  });

  it("uses a stable owner session id before mutable owner-name metadata", () => {
    const owner = ownedBy("old-owner-name", "default", { ownerSessionId: "s_owner" });

    expect(canObserveAgentSession(
      owner,
      identity("renamed-owner", "other", { sessionId: "s_owner" }),
    )).toBe(true);
    expect(canObserveAgentSession(
      owner,
      identity("old-owner-name", "default", { sessionId: "s_intruder" }),
    )).toBe(false);
    expect(canObserveAgentSession(owner, identity("old-owner-name"))).toBe(false);
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

  it("uses a stable delegated principal id before mutable grant-name metadata", () => {
    const stableGrant: AgentAccessGrantRow = {
      ...grant("old-delegate-name"),
      principalAgentId: "a_delegate",
    };

    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("renamed-delegate", "other", { agentId: "a_delegate" }),
      [stableGrant],
    )).toBe(true);
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("old-delegate-name", "default", { agentId: "a_intruder" }),
      [stableGrant],
    )).toBe(false);
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("old-delegate-name"),
      [stableGrant],
    )).toBe(false);
  });

  it("uses a stable delegated principal session before mutable grant-name metadata", () => {
    const sessionGrant: AgentAccessGrantRow = {
      ...grant("old-delegate-name"),
      principalSessionId: "s_delegate",
    };

    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("renamed-delegate", "other", { sessionId: "s_delegate" }),
      [sessionGrant],
    )).toBe(true);
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("old-delegate-name", "default", { sessionId: "s_intruder" }),
      [sessionGrant],
    )).toBe(false);
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("old-delegate-name"),
      [sessionGrant],
    )).toBe(false);
  });

  it("denies a revoked grant", () => {
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice"), [])).toBe(false);
  });

  it("treats project as metadata for owners and delegated viewers", () => {
    expect(canObserveAgentSession(
      ownedBy("operator", "default"),
      identity("operator", "ops", { sessionId: "s_operator" }),
    ))
      .toBe(true);
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice", "ops"), [grant("alice")]))
      .toBe(true);
  });

  it("allows human admin observe override for protected managed targets", () => {
    const humanAdmin = identity("operator", "default", { kind: Kind.Human, tier: Tier.Admin });
    expect(canObserveAgentSession(ownedBy("owner", "default", { tier: "admin" }), humanAdmin))
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
    expect(canObserveAgentSession(ownedBy("owner", "default", { sessionKind: "human" }), agentAdmin))
      .toBe(false);
  });
});

describe("observeScoped cursor compatibility", () => {
  it("preserves legacy afterId for explicit current-boot translation", async () => {
    vi.stubEnv("NEXUS_WEB_AUTH_MODE", "local");
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      {
        sql:
          "INSERT INTO identities (agent_id, name, metadata_json, updated_at) " +
          "VALUES (?, ?, ?, ?)",
        args: ["a_ada", "ada", "{}", 1],
      },
      {
        sql:
          "INSERT INTO runtime_descriptors " +
          "(runtime_id, agent_id, session_id, harness, mode, status, updated_at) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?)",
        args: ["r_ada", "a_ada", "s_ada", "codex", "headless", "online", 1],
      },
    ]);
    let captured: Record<string, unknown> | undefined;
    vi.spyOn(sessionFanout, "subscribe").mockImplementation((_sessionId, subscriber) => {
      captured = subscriber as unknown as Record<string, unknown>;
      return { ready: Promise.resolve(), close() {} };
    });

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ada&afterId=42"),
      {
        canonicalDb: () => db,
        submitCommand: vi.fn(async () => undefined),
      },
    );

    expect(response.status).toBe(200);
    expect(captured).toMatchObject({ after: undefined, legacyAfterId: "42" });
    await response.body?.cancel();
    db.close();
  });
});
