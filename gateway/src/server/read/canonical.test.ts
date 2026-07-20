import { createClient } from "@libsql/client";
import { describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../store/migrations";
import {
  canonicalDmHistory,
  canonicalAgentShow,
  canonicalAgentAccessGrantsByAgentId,
  canonicalAgentSessionTarget,
  canonicalMembers,
  canonicalMetadata,
  canonicalNotifications,
  canonicalRuntimes,
  canonicalThreadHistory,
  canonicalThreadMembers,
  canonicalThreads,
  canonicalTopics,
} from "./canonical";

describe("canonical Gateway REST reads", () => {
  it("serves threads, DMs, topics, notifications and opaque cursor pages", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      "INSERT INTO identities VALUES ('a_ada','ada',NULL,'agent','agent','{}',1)",
      "INSERT INTO identities VALUES ('a_blake','blake',NULL,'agent','agent','{}',1)",
      "INSERT INTO threads VALUES ('t_design','design',NULL,1,1)",
      "INSERT INTO thread_members VALUES ('t_design','a_ada',1,NULL)",
      "INSERT INTO topics VALUES ('builds',1,1)",
      "INSERT INTO topic_subscriptions VALUES ('builds','a_ada',NULL,'0',1,1)",
      `INSERT INTO bus_messages
        (message_id, kind, from_name, from_agent_id, to_name, to_agent_id,
         thread_id, topic, summary, body, provenance_json, created_at) VALUES
        ('m_t1','thread','ada','a_ada','design',NULL,'t_design',NULL,NULL,'first','{}',10),
        ('m_t2','thread','browser-user','a_browser','design',NULL,'t_design',NULL,NULL,'second',
         '{"from":"browser-user","kind":"human","project":"default","thread":"design"}',11),
        ('m_d1','dm','ada','a_ada','blake','a_blake',NULL,NULL,NULL,'dm','{}',12)`,
      "INSERT INTO notifications VALUES ('n_1','m_d1','build','{\"agentId\":\"a_blake\"}',NULL,'done',13)",
    ], "write");

    expect(await canonicalThreads(db)).toMatchObject([
      { name: "design", members: ["ada"], lastAt: 11 },
    ]);
    const page = await canonicalThreadHistory(db, "design", { limit: 1 });
    expect(page.rows).toMatchObject([{
      messageId: "m_t2",
      from: "browser-user",
      fromKind: "human",
      body: "second",
    }]);
    expect(page.after).toEqual(expect.any(String));
    expect((await canonicalThreadHistory(db, "design", {
      limit: 10,
      before: page.before,
    })).rows).toMatchObject([{ messageId: "m_t1" }]);
    expect((await canonicalDmHistory(db, "blake", { limit: 10 })).rows)
      .toMatchObject([{ messageId: "m_d1" }]);
    expect(await canonicalTopics(db)).toEqual([{ topic: "builds", subscribers: 1 }]);
    expect(await canonicalNotifications(db, { limit: 10 })).toMatchObject([
      { notifId: "n_1", source: "build", routedTo: ["a_blake"] },
    ]);
    db.close();
  });

  it("projects legacy-name thread memberships when no stable identity row is joined", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      "INSERT INTO threads VALUES ('t_lens','lens-check',NULL,1,1)",
      "INSERT INTO thread_members VALUES ('t_lens','legacy-name:Earl Tan',1,NULL)",
      "INSERT INTO thread_members VALUES ('t_lens','legacy-name:lens-codex',2,NULL)",
      "INSERT INTO identities VALUES ('a_claude','lens-claude',NULL,'agent','agent','{}',1)",
      "INSERT INTO thread_members VALUES ('t_lens','a_claude',3,NULL)",
    ], "write");

    const listed = await canonicalThreads(db);
    expect(listed).toHaveLength(1);
    expect(listed[0]?.name).toBe("lens-check");
    expect([...listed[0]!.members].sort()).toEqual([
      "Earl Tan", "lens-claude", "lens-codex",
    ]);
    await expect(canonicalThreadMembers(db, "lens-check")).resolves.toEqual({
      name: "lens-check",
      members: ["Earl Tan", "lens-codex", "lens-claude"],
    });
    db.close();
  });

  it("resolves an agent-session name from Gateway-owned identity/runtime state", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO identities VALUES
        ('a_codex','codex','alex','lead','admin',
         '{"project":"default","ownerProject":"default"}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_old','a_codex','s_old','codex','headless',NULL,NULL,NULL,'stopped',2),
        ('r_live','a_codex','s_live','codex','headless',NULL,NULL,NULL,'busy',3)`,
    ], "write");

    await expect(canonicalAgentSessionTarget(db, "codex")).resolves.toEqual({
      sessionId: "s_live",
      owner: expect.objectContaining({
        agentId: "a_codex",
        name: "codex",
        project: "default",
        ownerName: "alex",
        ownerProject: "default",
        role: "lead",
        tier: "admin",
      }),
    });
    db.close();
  });

  it("uses a stable agent id exclusively when display metadata is stale", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO identities VALUES
        ('a_real','current-name',NULL,'agent','agent','{}',1),
        ('a_other','stale-name',NULL,'agent','agent','{}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_real','a_real','s_real','codex','headless',NULL,NULL,NULL,'online',2),
        ('r_other','a_other','s_other','claude','headless',NULL,NULL,NULL,'online',2)`,
    ], "write");

    await expect(canonicalAgentSessionTarget(db, {
      agentId: "a_real",
      name: "stale-name",
    })).resolves.toEqual({
      sessionId: "s_real",
      owner: expect.objectContaining({ agentId: "a_real", name: "current-name" }),
    });
    db.close();
  });

  it("uses a stable session id exclusively when display metadata is stale", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO identities VALUES
        ('a_real','current-name',NULL,'agent','agent','{}',1),
        ('a_other','stale-name',NULL,'agent','agent','{}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_real','a_real','s_real','codex','headless',NULL,NULL,NULL,'online',2),
        ('r_other','a_other','s_other','claude','headless',NULL,NULL,NULL,'online',2)`,
    ], "write");

    await expect(canonicalAgentSessionTarget(db, {
      sessionId: "s_real",
      name: "stale-name",
    })).resolves.toEqual({
      sessionId: "s_real",
      owner: expect.objectContaining({ agentId: "a_real", name: "current-name" }),
    });
    db.close();
  });

  it("prefers an exact session id over a newer colliding runtime id", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO identities VALUES
        ('a_session_owner','session-owner',NULL,'agent','agent','{}',1),
        ('a_runtime_alias','runtime-alias',NULL,'agent','agent','{}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_session_owner','a_session_owner','s_collision','codex','headless',NULL,NULL,NULL,'online',2),
        ('s_collision','a_runtime_alias','s_alias','claude','headless',NULL,NULL,NULL,'online',99)`,
    ], "write");

    await expect(canonicalAgentSessionTarget(db, { sessionId: "s_collision" }))
      .resolves.toEqual({
        sessionId: "s_collision",
        owner: expect.objectContaining({ agentId: "a_session_owner", name: "session-owner" }),
      });
    db.close();
  });

  it("prefers an exact stable agent id over a colliding display alias on every agent read", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO identities VALUES
        ('a_collision','id-owner',NULL,'agent','agent','{"marker":"id"}',1),
        ('a_alias_owner','a_collision',NULL,'agent','agent','{"marker":"alias"}',2)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_id','a_collision','s_id','codex','headless',NULL,NULL,NULL,'online',2),
        ('r_alias','a_alias_owner','s_alias','claude','headless',NULL,NULL,NULL,'online',2)`,
    ], "write");

    await expect(canonicalAgentSessionTarget(db, "a_collision")).resolves.toMatchObject({
      sessionId: "s_id",
      owner: { agentId: "a_collision", name: "id-owner" },
    });
    await expect(canonicalAgentShow(db, "a_collision")).resolves.toMatchObject({
      agent: { agentId: "a_collision", name: "id-owner" },
    });
    await expect(canonicalAgentShow(db, "a_collision", { project: "other" }))
      .resolves.toMatchObject({
        agent: { agentId: "a_collision", name: "id-owner" },
      });
    await expect(canonicalMetadata(db, "agent", "a_collision")).resolves.toMatchObject({
      metadata: { marker: "id" },
    });
    db.close();
  });

  it("uses exact-id-first globally unique identity resolution for DM history", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO identities VALUES
        ('a_alias_owner','a_collision',NULL,'agent','agent','{}',1),
        ('a_collision','id-owner',NULL,'agent','agent','{}',2),
        ('a_duplicate_one','duplicate',NULL,'agent','agent','{}',3),
        ('a_duplicate_two','duplicate',NULL,'agent','agent','{}',4)`,
      `INSERT INTO bus_messages
        (message_id, kind, from_name, from_agent_id, to_name, to_agent_id,
         body, provenance_json, created_at) VALUES
        ('m_exact','dm','sender','a_sender','id-owner','a_collision','exact','{}',10),
        ('m_alias','dm','sender','a_sender','a_collision','a_alias_owner','alias','{}',11)`,
    ], "write");

    await expect(canonicalDmHistory(db, "a_collision", { limit: 10 }))
      .resolves.toMatchObject({ rows: [{ messageId: "m_exact" }] });
    await expect(canonicalDmHistory(db, "duplicate", { limit: 10 }))
      .rejects.toThrow("ambiguous agent name");
    db.close();
  });

  it("loads delegated agent-session grants from canonical identity projection metadata", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.execute({
      sql: `INSERT INTO identities VALUES (?, ?, ?, ?, ?, ?, ?)`,
      args: [
        "a_managed",
        "managed",
        "owner",
        "agent",
        "agent",
        JSON.stringify({
          accessGrants: [{
            principalProject: "stale-project",
            principalName: "stale-delegate",
            principalAgentId: "a_delegate",
            role: "viewer",
          }],
        }),
        1,
      ],
    });

    await expect(canonicalAgentAccessGrantsByAgentId(db, "a_managed"))
      .resolves.toEqual([{
        agentId: "a_managed",
        principalProject: "stale-project",
        principalName: "stale-delegate",
        principalAgentId: "a_delegate",
        role: "viewer",
      }]);
    db.close();
  });

  it("rejects an ambiguous projected agent name instead of taking limit one", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO identities VALUES
        ('a_duplicate_one','duplicate',NULL,'agent','agent','{}',1),
        ('a_duplicate_two','duplicate',NULL,'agent','agent','{}',2)`,
    ], "write");

    await expect(canonicalAgentSessionTarget(db, "duplicate"))
      .rejects.toThrow("ambiguous agent name");
    await expect(canonicalAgentShow(db, "duplicate"))
      .rejects.toThrow("ambiguous agent name");
    await expect(canonicalMetadata(db, "agent", "duplicate"))
      .rejects.toThrow("ambiguous agent name");
    db.close();
  });

  it("projects fleet members and runtimes from Gateway-owned identity state", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO identities VALUES
        ('a_ada','ada','alex','lead','admin',
         '{"project":"default","kind":"agent","currentWork":"shipping"}',1),
        ('a_blake','blake',NULL,'agent','agent',
         '{"project":"other","kind":"agent"}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_old','a_ada','s_old','codex','headless','acp','/old',NULL,'stopped',2),
        ('r_live','a_ada','s_live','codex','headed','tmux','/work',NULL,'busy',3),
        ('r_blake','a_blake','s_blake','claude','headless','acp','/work',NULL,'online',4)`,
    ], "write");

    await expect(canonicalMembers(db, {
      includeOffline: false,
      project: "default",
    })).resolves.toEqual([
      expect.objectContaining({
        name: "ada",
        agentId: "a_ada",
        sessionId: "s_live",
        agent: "codex",
        presence: "busy",
        currentWork: "shipping",
      }),
    ]);
    await expect(canonicalRuntimes(db, {
      includeStopped: true,
      project: "default",
    })).resolves.toEqual([
      expect.objectContaining({ runtimeId: "r_live", name: "ada", active: true }),
      expect.objectContaining({ runtimeId: "r_old", name: "ada", active: false }),
    ]);
    db.close();
  });
});
