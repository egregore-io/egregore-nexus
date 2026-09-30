import { createClient, type Client } from "@libsql/client";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  CURRENT_GATEWAY_SCHEMA_NAME,
  CURRENT_GATEWAY_SCHEMA_VERSION,
  migrateGatewayStore,
} from "./migrations";
import { GATEWAY_CANONICAL_TABLES } from "./schema";
import { currentHuman } from "../identity/human";

describe("Gateway v0.1.0 store baseline", () => {
  it("rolls back model columns when marker validation fails after an ignored write", async () => {
    const db = createClient({ url: ":memory:" });
    try {
      await migrateGatewayStore(db);
      await db.batch([
        "ALTER TABLE runtime_descriptors DROP COLUMN model_report_revision",
        "ALTER TABLE runtime_descriptors DROP COLUMN model_report_json",
        "UPDATE gateway_schema_migrations SET version=7,name='v0.1.6_transport_host'",
        "CREATE TRIGGER ignore_model_marker BEFORE UPDATE ON gateway_schema_migrations BEGIN SELECT RAISE(IGNORE); END",
      ], "write");
      await expect(migrateGatewayStore(db)).rejects.toThrow();
      expect(await columnNames(db, "runtime_descriptors")).not.toContain("model_report_revision");
      expect(await columnNames(db, "runtime_descriptors")).not.toContain("model_report_json");
      expect((await db.execute("SELECT version FROM gateway_schema_migrations")).rows[0]?.version).toBe(7);
      await db.execute("DROP TRIGGER ignore_model_marker");
      await migrateGatewayStore(db);
    } finally { db.close(); }
  });

  it("reopens persisted report columns and fails closed on corrupt revision/JSON pairs", async () => {
    const directory = mkdtempSync(join(tmpdir(), "nexus-model-migration-"));
    const url = `file:${join(directory, "gateway.db")}`;
    let db = createClient({ url });
    try {
      await migrateGatewayStore(db);
      await db.execute("INSERT INTO runtime_descriptors(runtime_id,agent_id,harness,mode,status,updated_at) VALUES('retain','a','test','headless','online',1)");
      db.close();
      db = createClient({ url });
      await migrateGatewayStore(db);
      expect((await db.execute("SELECT model_report_revision,model_report_json FROM runtime_descriptors")).rows).toMatchObject([{model_report_revision:0,model_report_json:null}]);
      await db.execute("UPDATE runtime_descriptors SET model_report_revision=2");
      await expect(migrateGatewayStore(db)).rejects.toThrow("invalid stored model report/revision pair");
      expect((await db.execute("SELECT model_report_revision FROM runtime_descriptors")).rows[0]?.model_report_revision).toBe(2);
    } finally { db.close(); rmSync(directory, {recursive:true,force:true}); }
  });

  it("adds model report columns to v7 atomically, preserving rows and reopening", async () => {
    const db = createClient({ url: ":memory:" });
    try {
      await migrateGatewayStore(db);
      expect(await columnNames(db, "runtime_descriptors")).toEqual(expect.arrayContaining(["model_report_revision", "model_report_json"]));
      await db.execute("ALTER TABLE runtime_descriptors DROP COLUMN model_report_revision");
      await db.execute("ALTER TABLE runtime_descriptors DROP COLUMN model_report_json");
      await db.execute("UPDATE gateway_schema_migrations SET version=7,name='v0.1.6_transport_host'");
      await db.execute("INSERT INTO runtime_descriptors(runtime_id,agent_id,harness,mode,status,updated_at) VALUES('retain','a','test','headless','online',1)");
      await db.execute("CREATE TRIGGER reject_model_marker BEFORE UPDATE ON gateway_schema_migrations BEGIN SELECT RAISE(ABORT,'model marker failure'); END");
      await expect(migrateGatewayStore(db)).rejects.toThrow("model marker failure");
      expect(await columnNames(db, "runtime_descriptors")).not.toContain("model_report_revision");
      expect((await db.execute("SELECT version FROM gateway_schema_migrations")).rows[0]?.version).toBe(7);
      await db.execute("DROP TRIGGER reject_model_marker");
      await migrateGatewayStore(db);
      await migrateGatewayStore(db);
      expect((await db.execute("SELECT runtime_id,model_report_revision,model_report_json FROM runtime_descriptors")).rows).toMatchObject([{runtime_id:"retain", model_report_revision:0, model_report_json:null}]);
    } finally { db.close(); }
  });

  it("creates one complete named baseline and reopens idempotently", async () => {
    const db = createClient({ url: ":memory:" });

    await migrateGatewayStore(db);
    await migrateGatewayStore(db);

    const tables = await db.execute(
      "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
    );
    const names = new Set(tables.rows.map((row) => String(row.name)));
    for (const table of GATEWAY_CANONICAL_TABLES) expect(names.has(table)).toBe(true);
    expect(names.has("projects")).toBe(false);
    expect(names.has("project_threads")).toBe(false);
    expect(names.has("hook_pipeline_evaluations")).toBe(true);
    expect(names.has("hook_handler_executions")).toBe(true);
    expect(names.has("hook_receipt_completion")).toBe(true);
    expect(await columnNames(db, "hook_pipeline_evaluations")).toContain("result_json");
    expect(await columnNames(db, "hook_handler_executions")).toContain("result_json");
    expect(await columnNames(db, "rest_bearer_token")).toEqual(expect.arrayContaining([
      "actor_session_id",
      "actor_agent_id",
      "actor_runtime_id",
      "actor_client_key",
    ]));

    const versions = await db.execute(
      "SELECT version, name FROM gateway_schema_migrations ORDER BY version",
    );
    expect(versions.rows).toMatchObject([
      { version: CURRENT_GATEWAY_SCHEMA_VERSION, name: CURRENT_GATEWAY_SCHEMA_NAME },
    ]);
    db.close();
  });

  it("upgrades the complete v0.1.0 baseline additively without losing data", async () => {
    const db = createClient({ url: ":memory:" });
    await createGatewayV1StoreForTest(db);
    await db.execute("INSERT INTO logs (ts, level, scope, message) VALUES (1, 'info', 'test', 'keep')");

    await migrateGatewayStore(db);

    expect(await singleText(db, "SELECT message FROM logs")).toBe("keep");
    expect(await tableExists(db, "hook_pipeline_evaluations")).toBe(true);
    const versions = await db.execute(
      "SELECT version, name FROM gateway_schema_migrations ORDER BY version",
    );
    expect(versions.rows).toMatchObject([
      { version: CURRENT_GATEWAY_SCHEMA_VERSION, name: CURRENT_GATEWAY_SCHEMA_NAME },
    ]);
    db.close();
  });

  it("backfills immutable human ids, principals, and live sessions without invalidating cookies", async () => {
    const db = createClient({ url: ":memory:" });
    await createGatewayV1StoreForTest(db);
    await db.execute({
      sql: `INSERT INTO human_user
              (name_key, name, password_hash, client_key, project, daemon_session_id,
               created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      args: ["legacy", "Legacy Human", "hash", "ck_legacy", "default", "s_legacy", 1, 1],
    });
    await db.execute({
      sql: `INSERT INTO human_session
              (cookie_token, name, client_key, project, daemon_session_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)`,
      args: ["cookie_legacy", "Legacy Human", "ck_legacy", "default", "s_legacy", 1],
    });

    await migrateGatewayStore(db);

    const user = (await db.execute(
      "SELECT human_user_id, name, client_key FROM human_user WHERE name_key = 'legacy'",
    )).rows[0]!;
    expect(String(user.human_user_id)).toMatch(/^hu_[a-f0-9]{24}$/);
    expect(user).toMatchObject({ name: "Legacy Human", client_key: "ck_legacy" });

    const session = (await db.execute(
      "SELECT human_user_id, principal_id, cookie_token FROM human_session WHERE cookie_token = 'cookie_legacy'",
    )).rows[0]!;
    expect(session.human_user_id).toBe(user.human_user_id);
    expect(String(session.principal_id)).toMatch(/^h_[a-f0-9]{24}$/);

    const principal = (await db.execute({
      sql: "SELECT principal_id, kind, access FROM principals WHERE principal_id = ?",
      args: [String(session.principal_id)],
    })).rows[0]!;
    expect(principal).toMatchObject({
      principal_id: session.principal_id,
      kind: "local.human",
      access: "admin",
    });

    await expect(currentHuman("cookie_legacy", { db })).resolves.toMatchObject({
      name: "Legacy Human",
      clientKey: "ck_legacy",
      sessionId: "s_legacy",
      humanUserId: user.human_user_id,
      principalId: session.principal_id,
    });
    db.close();
  });

  it("upgrades the receipt-era hook schema with replay results", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      "ALTER TABLE hook_pipeline_evaluations DROP COLUMN result_json",
      "ALTER TABLE runtime_descriptors DROP COLUMN model_report_revision",
      "ALTER TABLE runtime_descriptors DROP COLUMN model_report_json",
      "ALTER TABLE hook_handler_executions DROP COLUMN result_json",
      "ALTER TABLE rest_bearer_token DROP COLUMN actor_session_id",
      "ALTER TABLE rest_bearer_token DROP COLUMN actor_agent_id",
      "ALTER TABLE rest_bearer_token DROP COLUMN actor_runtime_id",
      "ALTER TABLE rest_bearer_token DROP COLUMN actor_client_key",
      `UPDATE gateway_schema_migrations
       SET version = 3, name = 'v0.1.5_message_hook_receipts'`,
    ], "write");

    await migrateGatewayStore(db);

    expect(await columnNames(db, "hook_pipeline_evaluations")).toContain("result_json");
    expect(await columnNames(db, "hook_handler_executions")).toContain("result_json");
    const marker = await db.execute("SELECT version, name FROM gateway_schema_migrations");
    expect(marker.rows).toMatchObject([
      { version: CURRENT_GATEWAY_SCHEMA_VERSION, name: CURRENT_GATEWAY_SCHEMA_NAME },
    ]);
    db.close();
  });

  it("upgrades resumable hooks with bearer authority fields", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      "ALTER TABLE runtime_descriptors DROP COLUMN model_report_revision",
      "ALTER TABLE runtime_descriptors DROP COLUMN model_report_json",
      "ALTER TABLE rest_bearer_token DROP COLUMN actor_session_id",
      "ALTER TABLE rest_bearer_token DROP COLUMN actor_agent_id",
      "ALTER TABLE rest_bearer_token DROP COLUMN actor_runtime_id",
      "ALTER TABLE rest_bearer_token DROP COLUMN actor_client_key",
      `UPDATE gateway_schema_migrations
       SET version = 4, name = 'v0.1.5_resumable_message_hooks'`,
    ], "write");

    await migrateGatewayStore(db);

    expect(await columnNames(db, "rest_bearer_token")).toEqual(expect.arrayContaining([
      "actor_session_id",
      "actor_agent_id",
      "actor_runtime_id",
      "actor_client_key",
    ]));
    const marker = await db.execute("SELECT version, name FROM gateway_schema_migrations");
    expect(marker.rows).toMatchObject([
      { version: CURRENT_GATEWAY_SCHEMA_VERSION, name: CURRENT_GATEWAY_SCHEMA_NAME },
    ]);
    db.close();
  });

  it("chains the v0.1.6 principal marker into the transport-host schema", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      "DROP TABLE transport_outbox",
      "ALTER TABLE runtime_descriptors DROP COLUMN model_report_revision",
      "ALTER TABLE runtime_descriptors DROP COLUMN model_report_json",
      "DROP TABLE transport_ingress",
      `UPDATE gateway_schema_migrations
       SET version = 6, name = 'v0.1.6_principals_and_transport_bindings'`,
    ], "write");

    await migrateGatewayStore(db);

    expect(await tableExists(db, "transport_ingress")).toBe(true);
    expect(await tableExists(db, "transport_outbox")).toBe(true);
    const marker = await db.execute("SELECT version, name FROM gateway_schema_migrations");
    expect(marker.rows).toMatchObject([
      { version: CURRENT_GATEWAY_SCHEMA_VERSION, name: CURRENT_GATEWAY_SCHEMA_NAME },
    ]);
    db.close();
  });

  it("rejects the pre-release v1/v2 ladder without changing it", async () => {
    const db = createClient({ url: ":memory:" });
    await db.batch(
      [
        `CREATE TABLE gateway_schema_migrations (
          version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at INTEGER NOT NULL
        )`,
        "INSERT INTO gateway_schema_migrations VALUES (1, 'canonical_gateway_store', 1)",
        "INSERT INTO gateway_schema_migrations VALUES (2, 'projection_quarantine', 2)",
        "CREATE TABLE pre_release_sentinel(value TEXT NOT NULL)",
        "INSERT INTO pre_release_sentinel VALUES ('preserve-me')",
      ],
      "write",
    );

    await expect(migrateGatewayStore(db)).rejects.toThrow(/pre-release.*fresh v0\.1\.0/i);
    expect(await singleText(db, "SELECT value FROM pre_release_sentinel")).toBe("preserve-me");
    const versions = await db.execute(
      "SELECT version, name FROM gateway_schema_migrations ORDER BY version",
    );
    expect(versions.rows).toHaveLength(2);
    db.close();
  });

  it("rejects unmarked non-empty stores without renaming or adding tables", async () => {
    const db = createClient({ url: ":memory:" });
    await db.batch(
      [
        `CREATE TABLE conversations (
          id TEXT PRIMARY KEY, kind TEXT NOT NULL, title TEXT, updated_at INTEGER NOT NULL
        )`,
        "INSERT INTO conversations VALUES ('dm:ada', 'dm', 'Ada', 10)",
      ],
      "write",
    );

    await expect(migrateGatewayStore(db)).rejects.toThrow(/unrecognized non-empty/i);
    expect(await tableExists(db, "conversations")).toBe(true);
    expect(await tableExists(db, "rendered_conversations")).toBe(false);
    expect(await tableExists(db, "gateway_schema_migrations")).toBe(false);
    db.close();
  });

  it("fails closed when a named baseline is missing a required object", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.execute("DROP TABLE bus_messages");

    await expect(migrateGatewayStore(db)).rejects.toThrow(
      /incomplete v0\.1\.0 Gateway schema.*bus_messages/i,
    );
    db.close();
  });
});

async function tableExists(db: Client, table: string): Promise<boolean> {
  const result = await db.execute({
    sql: "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
    args: [table],
  });
  return result.rows.length > 0;
}

async function singleText(db: Client, sql: string): Promise<string> {
  const result = await db.execute(sql);
  return String(result.rows[0]?.[0]);
}

async function columnNames(db: Client, table: string): Promise<string[]> {
  const result = await db.execute(`PRAGMA table_info(${table})`);
  return result.rows.map((row) => String(row.name));
}

async function createGatewayV1StoreForTest(db: Client): Promise<void> {
  const migrations = await import("./migrations");
  await migrations.createGatewayV1StoreForTest(db);
}
