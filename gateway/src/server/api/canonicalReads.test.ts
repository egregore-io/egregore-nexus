import { createClient } from "@libsql/client";
import { randomUUID } from "node:crypto";
import { rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";

import { handle } from "./router";
import { migrateGatewayStore } from "../store/migrations";

describe("public canonical Gateway reads", () => {
  it("serves migrated REST resources without constructing the daemon read view", async () => {
    const path = join(tmpdir(), `nexus-canonical-api-${randomUUID()}.db`);
    const db = createClient({ url: `file:${path}` });
    await migrateGatewayStore(db);
    await db.batch([
      "INSERT INTO identities VALUES ('a_ada','ada',NULL,'agent','agent','{}',1)",
      "INSERT INTO identities VALUES ('a_operator','operator',NULL,'human','admin','{\"project\":\"default\",\"kind\":\"human\"}',1)",
      "INSERT INTO runtime_descriptors VALUES ('r_ada','a_ada','s_ada','codex','headless','acp','/work',NULL,'online',2)",
      "INSERT INTO threads VALUES ('t_design','design',NULL,1,1)",
      "INSERT INTO thread_members VALUES ('t_design','a_ada',1,NULL)",
      "INSERT INTO topics VALUES ('builds',1,1)",
      "INSERT INTO topic_subscriptions VALUES ('builds','a_ada',NULL,'0',1,1)",
      `INSERT INTO bus_messages VALUES
        ('m_thread','thread','ada','a_ada','design',NULL,'t_design',NULL,NULL,'thread row','{}',10),
        ('m_dm','dm','ada','a_ada','operator','a_operator',NULL,NULL,NULL,'dm row','{}',11)`,
      "INSERT INTO notifications VALUES ('n_1','m_dm','build','{\"topic\":\"builds\"}',NULL,'done',12)",
    ], "write");
    const legacyDb = vi.fn(() => { throw new Error("daemon read view must stay unopened"); });
    const deps = { db: legacyDb, canonicalDb: () => db };

    for (const pathName of [
      "/api/v1/health",
      "/api/v1/members?includeOffline=true",
      "/api/v1/runtimes?includeStopped=true",
      "/api/v1/runtimes?agent=ada&includeStopped=true",
      "/api/v1/agents/ada",
      "/api/v1/agents/ada/runtimes?includeStopped=true",
      "/api/v1/agents/ada/metadata",
      "/api/v1/sessions/s_ada/metadata",
      "/api/v1/threads/design/metadata",
      "/api/v1/messages/m_thread/metadata",
      "/api/v1/projects",
      "/api/v1/whoami",
      "/api/v1/routing-rules",
      "/api/v1/threads",
      "/api/v1/threads/design/header",
      "/api/v1/threads/design/members",
      "/api/v1/threads/design/history",
      "/api/v1/dms/ada/history",
      "/api/v1/history?thread=design",
      "/api/v1/search?q=thread",
      "/api/v1/messages/m_thread",
      "/api/v1/topics",
      "/api/v1/notifications",
    ]) {
      const url = new URL(pathName, "http://gateway.test");
      const query = Object.fromEntries(url.searchParams.entries());
      const response = await handle({
        method: "GET",
        path: url.pathname,
        query,
        headers: {},
        caller: { name: "operator", project: "default", sessionId: "s_operator" },
      }, deps);
      expect(response.status).toBe(200);
    }
    expect(legacyDb).not.toHaveBeenCalled();

    db.close();
    await rm(path, { force: true });
  });

  it("serves a projected thread message even when the Gateway missed the earlier thread registry event", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.execute(`INSERT INTO bus_messages VALUES
      ('m_late','thread','ada','a_ada','late-thread',NULL,'t_late',NULL,NULL,
       'arrived after Gateway boot','{}',10)`);

    const response = await handle({
      method: "GET",
      path: "/api/v1/threads/late-thread/history",
      query: {},
      headers: {},
      caller: { name: "operator", project: "default", sessionId: "s_operator" },
    }, { db: vi.fn(), canonicalDb: () => db });

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject([
      { messageId: "m_late", body: "arrived after Gateway boot" },
    ]);
    db.close();
  });

  it("keeps canonical search and message reads caller-scoped without the daemon read view", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      `INSERT INTO bus_messages VALUES
        ('m_public','thread','ada','a_ada','design',NULL,'t_design',NULL,NULL,
         'shared architecture note','{"project":"default","from":"ada","kind":"agent","thread":"design"}',10),
        ('m_private','dm','ada','a_ada','operator','a_operator',NULL,NULL,NULL,
         'private architecture note','{"project":"default","from":"ada","kind":"agent"}',11),
        ('m_hidden','dm','ada','a_ada','blake','a_blake',NULL,NULL,NULL,
         'hidden architecture note','{"project":"default","from":"ada","kind":"agent"}',12)`,
    ], "write");
    const legacyDb = vi.fn(() => { throw new Error("daemon read view must stay unopened"); });
    const deps = { db: legacyDb, canonicalDb: () => db };
    const caller = { name: "operator", project: "default", sessionId: "s_operator" };

    const search = await handle({
      method: "GET",
      path: "/api/v1/search",
      query: { q: "architecture" },
      headers: {},
      caller,
    }, deps);
    expect(search.status).toBe(200);
    expect(search.body).toMatchObject([
      { messageId: "m_private" },
      { messageId: "m_public" },
    ]);

    const visible = await handle({
      method: "GET",
      path: "/api/v1/messages/m_private",
      query: {},
      headers: {},
      caller,
    }, deps);
    expect(visible.status).toBe(200);
    expect(visible.body).toMatchObject({
      id: "m_private",
      project: "default",
      from: "ada",
      scope: "dm",
      body: "private architecture note",
    });

    const hidden = await handle({
      method: "GET",
      path: "/api/v1/messages/m_hidden",
      query: {},
      headers: {},
      caller,
    }, deps);
    expect(hidden.status).toBe(404);
    expect(legacyDb).not.toHaveBeenCalled();
    db.close();
  });

  it("accepts the local CLI me projection when no authenticated HTTP caller is attached", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.execute(`INSERT INTO bus_messages VALUES
      ('m_cli','dm','ada','a_ada','cli-agent','a_cli',NULL,NULL,NULL,
       'CLI-visible message','{"project":"default","from":"ada","kind":"agent"}',11)`);
    const deps = { db: vi.fn(), canonicalDb: () => db };
    const localOperator = { name: "operator", project: "default", sessionId: "s_operator" };

    const history = await handle({
      method: "GET",
      path: "/api/v1/history",
      query: { with: "ada", me: "cli-agent" },
      headers: {},
      caller: localOperator,
    }, deps);
    expect(history.status).toBe(200);
    expect(history.body).toMatchObject([{ messageId: "m_cli" }]);

    const message = await handle({
      method: "GET",
      path: "/api/v1/messages/m_cli",
      query: { me: "cli-agent" },
      headers: {},
      caller: localOperator,
    }, deps);
    expect(message.status).toBe(200);
    expect(message.body).toMatchObject({ id: "m_cli" });
    db.close();
  });
});
