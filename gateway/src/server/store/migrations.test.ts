import { createClient, type Client } from "@libsql/client";
import { describe, expect, it } from "vitest";

import {
  CURRENT_GATEWAY_SCHEMA_NAME,
  CURRENT_GATEWAY_SCHEMA_VERSION,
  migrateGatewayStore,
} from "./migrations";
import { GATEWAY_CANONICAL_TABLES } from "./schema";

describe("Gateway v0.1.0 store baseline", () => {
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

  it("upgrades the receipt-era hook schema with replay results", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      "ALTER TABLE hook_pipeline_evaluations DROP COLUMN result_json",
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
