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

    const versions = await db.execute(
      "SELECT version, name FROM gateway_schema_migrations ORDER BY version",
    );
    expect(versions.rows).toMatchObject([
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
