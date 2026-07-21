import { spawn } from "node:child_process";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, describe, expect, it } from "vitest";

import { removeTempPath } from "../../test/removeTempPath";
import { closeGatewayStore, createGatewayStore, getGatewayStore } from "./client";
import { CURRENT_GATEWAY_SCHEMA_VERSION } from "./migrations";

const temporaryDirectories: string[] = [];
const gatewayRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

afterEach(async () => {
  await closeGatewayStore();
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      removeTempPath(directory, { recursive: true }),
    ),
  );
});

async function temporaryStoreUrl(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "nexus-gateway-store-"));
  temporaryDirectories.push(directory);
  return `file:${join(directory, "gateway.db")}`;
}

describe("Gateway canonical store client", () => {
  it("opens one process-wide migrated client", async () => {
    const url = await temporaryStoreUrl();
    const first = await getGatewayStore({ NEXUS_GATEWAY_DB: url });
    const second = await getGatewayStore({ NEXUS_GATEWAY_DB: "file:/ignored-after-open.db" });

    expect(second).toBe(first);
    const schema = await first.execute(
      "SELECT version FROM gateway_schema_migrations ORDER BY version",
    );
    expect(schema.rows.map((row) => Number(row.version))).toEqual([
      CURRENT_GATEWAY_SCHEMA_VERSION,
    ]);
  });

  it("uses WAL and preserves canonical rows after close and reopen", async () => {
    const url = await temporaryStoreUrl();
    const first = await createGatewayStore({ url });
    const journal = await first.execute("PRAGMA journal_mode");
    expect(String(journal.rows[0]?.journal_mode).toLowerCase()).toBe("wal");
    await first.execute({
      sql: `INSERT INTO identities
            (agent_id, name, owner, role, tier, metadata_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
      args: ["a_ada", "ada", "alex", "agent", "Member", "{}", 1],
    });
    first.close();

    const reopened = await createGatewayStore({ url });
    const identities = await reopened.execute("SELECT agent_id, name FROM identities");
    expect(identities.rows).toMatchObject([{ agent_id: "a_ada", name: "ada" }]);
    reopened.close();
  });

  it("configures bounded writer waiting and normal synchronization", async () => {
    const url = await temporaryStoreUrl();
    const db = await createGatewayStore({ url });

    const busy = await db.execute("PRAGMA busy_timeout");
    const synchronous = await db.execute("PRAGMA synchronous");
    expect(Number(busy.rows[0]?.timeout)).toBe(5000);
    expect(Number(synchronous.rows[0]?.synchronous)).toBe(1);
    db.close();
  });

  it("queues same-process work without blocking the transaction owner", async () => {
    const url = await temporaryStoreUrl();
    const db = await createGatewayStore({ url });
    await db.execute("CREATE TABLE serialized_writes (value TEXT PRIMARY KEY)");

    const tx = await db.transaction("write");
    await tx.execute("INSERT INTO serialized_writes (value) VALUES ('transaction')");
    let concurrentState: "pending" | "fulfilled" | "rejected" = "pending";
    const concurrent = db.execute(
      "INSERT INTO serialized_writes (value) VALUES ('concurrent')",
    ).then(
      (result) => {
        concurrentState = "fulfilled";
        return result;
      },
      (error: unknown) => {
        concurrentState = "rejected";
        throw error;
      },
    );

    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(concurrentState).toBe("pending");
    await tx.commit();
    await expect(concurrent).resolves.toMatchObject({ rowsAffected: 1 });
    expect(concurrentState).toBe("fulfilled");

    const rows = await db.execute("SELECT value FROM serialized_writes ORDER BY value");
    expect(rows.rows.map((row) => String(row.value))).toEqual([
      "concurrent",
      "transaction",
    ]);
    db.close();
  });

  it("releases queued work after an interactive transaction is closed", async () => {
    const url = await temporaryStoreUrl();
    const db = await createGatewayStore({ url });
    await db.execute("CREATE TABLE closed_transactions (value TEXT PRIMARY KEY)");

    const rolledBack = await db.transaction("write");
    await rolledBack.execute(
      "INSERT INTO closed_transactions (value) VALUES ('rolled-back')",
    );
    const afterRollback = db.execute(
      "INSERT INTO closed_transactions (value) VALUES ('after-rollback')",
    );
    await rolledBack.rollback();
    await expect(afterRollback).resolves.toMatchObject({ rowsAffected: 1 });

    const closed = await db.transaction("write");
    await closed.execute("INSERT INTO closed_transactions (value) VALUES ('closed')");
    const afterClose = db.execute(
      "INSERT INTO closed_transactions (value) VALUES ('after-close')",
    );
    closed.close();
    await expect(afterClose).resolves.toMatchObject({ rowsAffected: 1 });

    const rows = await db.execute("SELECT value FROM closed_transactions ORDER BY value");
    expect(rows.rows.map((row) => String(row.value))).toEqual([
      "after-close",
      "after-rollback",
    ]);
    db.close();
  });

  it("waits for a concurrent initializer before applying WAL without retrying", async () => {
    const url = await temporaryStoreUrl();
    const lock = await holdExclusiveStoreLock(url);
    const startedAt = Date.now();
    try {
      const db = await createGatewayStore({ url });
      expect(Date.now() - startedAt).toBeGreaterThanOrEqual(250);
      const schema = await db.execute(
        "SELECT version FROM gateway_schema_migrations ORDER BY version",
      );
      expect(schema.rows.map((row) => Number(row.version))).toEqual([
        CURRENT_GATEWAY_SCHEMA_VERSION,
      ]);
      db.close();
    } finally {
      await lock.exit;
    }
  });
});

async function holdExclusiveStoreLock(url: string): Promise<{ exit: Promise<void> }> {
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import { createClient } from "@libsql/client";
       const db = createClient({ url: process.env.LOCK_URL });
       await db.execute("BEGIN EXCLUSIVE");
       await db.execute("PRAGMA user_version = 1");
       process.stdout.write("LOCKED\\n");
       await new Promise((resolve) => setTimeout(resolve, 750));
       await db.execute("ROLLBACK");
       db.close();`,
    ],
    {
      cwd: gatewayRoot,
      env: { ...process.env, LOCK_URL: url },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  const exit = new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`SQLite lock owner exited ${code ?? signal}: ${stderr}`));
    });
  });
  await new Promise<void>((resolve, reject) => {
    let stdout = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
      if (stdout.includes("LOCKED\n")) resolve();
    });
    void exit.catch(reject);
  });
  return { exit };
}
