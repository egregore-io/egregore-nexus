import { createClient } from "@libsql/client";
import { describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../store/migrations";
import {
  canonicalDmHistory,
  canonicalAgentSessionTarget,
  canonicalMembers,
  canonicalNotifications,
  canonicalRuntimes,
  canonicalThreadHistory,
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
      `INSERT INTO bus_messages VALUES
        ('m_t1','thread','ada','a_ada','design',NULL,'t_design',NULL,NULL,'first','{}',10),
        ('m_t2','thread','blake','a_blake','design',NULL,'t_design',NULL,NULL,'second','{}',11),
        ('m_d1','dm','ada','a_ada','blake','a_blake',NULL,NULL,NULL,'dm','{}',12)`,
      "INSERT INTO notifications VALUES ('n_1','m_d1','build','{\"agentId\":\"a_blake\"}',NULL,'done',13)",
    ], "write");

    expect(await canonicalThreads(db)).toMatchObject([
      { name: "design", members: ["ada"], lastAt: 11 },
    ]);
    const page = await canonicalThreadHistory(db, "design", { limit: 1 });
    expect(page.rows).toMatchObject([{ messageId: "m_t2", body: "second" }]);
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
