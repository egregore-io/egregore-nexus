import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { chmod, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { createClient, type Client } from "@libsql/client";
import { afterEach, describe, expect, it } from "vitest";

import { createGatewayV1StoreForTest, migrateGatewayStore } from "./migrations";

const execFileAsync = promisify(execFile);
const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
const scriptPath = join(repositoryRoot, "scripts", "v016-identity-continuity.mjs");

const secrets = {
  humanClientKey: "ck_live_DO_NOT_LEAK",
  passwordHash: "argon_hash_DO_NOT_LEAK",
  cookie: "cookie_live_DO_NOT_LEAK",
  agentClientKey: "ck_agent_DO_NOT_LEAK",
  resumeKey: "resume_DO_NOT_LEAK",
} as const;

const cleanupRoots: string[] = [];

afterEach(async () => {
  await Promise.all(cleanupRoots.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

describe("v0.1.6 identity continuity operator gate", () => {
  it("snapshots the complete legacy identity rows read-only without leaking credentials", async () => {
    const fixture = await seedLegacyFixture();
    const beforeGateway = await fileDigest(fixture.gatewayDb);
    const beforeDaemon = await fileDigest(fixture.daemonDb);

    const first = await runContinuity("snapshot", fixture);
    const artifactText = await readFile(fixture.pre, "utf8");
    const artifact = JSON.parse(artifactText);

    expect(first.stdout).toBe("identity continuity snapshot written\n");
    expect(first.stderr).toBe("");
    expect((await stat(fixture.pre)).mode & 0o777).toBe(0o600);
    expect(await fileDigest(fixture.gatewayDb)).toBe(beforeGateway);
    expect(await fileDigest(fixture.daemonDb)).toBe(beforeDaemon);
    expect(artifact.schemaVersion).toBe("nexus-v016-identity-continuity/1");
    expect(artifact.stores.gateway.tables.human_user.columns).toEqual([
      "client_key",
      "created_at",
      "daemon_session_id",
      "name",
      "name_key",
      "password_hash",
      "project",
      "updated_at",
    ]);
    expect(artifact.stores.gateway.tables.human_session.columns).toEqual([
      "client_key",
      "cookie_token",
      "created_at",
      "daemon_session_id",
      "name",
      "project",
    ]);
    expect(artifact.stores.daemon.tables.agents.rows).toHaveLength(1);
    expect(artifact.stores.daemon.tables.identity_sessions.rows).toHaveLength(1);
    expect(artifact.stores.daemon.tables.native_thread_bindings.columns).toEqual([
      "agent_id",
      "created_at",
      "first_runtime_id",
      "harness",
      "last_runtime_id",
      "native_thread_id",
      "project",
      "released_at",
      "updated_at",
    ]);
    expect(
      artifact.stores.gateway.tables.human_user.rows[0].values.client_key,
    ).toEqual({ sha256: fingerprint("human_user", "client_key", secrets.humanClientKey) });

    for (const secret of Object.values(secrets)) {
      expect(artifactText).not.toContain(secret);
      expect(first.stdout).not.toContain(secret);
      expect(first.stderr).not.toContain(secret);
    }

    await runContinuity("snapshot", fixture);
    expect(await readFile(fixture.pre, "utf8")).toBe(artifactText);
  });

  it("verifies migrated identities and keeps minted ids stable across a second migration", async () => {
    const fixture = await seedLegacyFixture();
    await runContinuity("snapshot", fixture);
    await migrateFixture(fixture);

    const first = await runContinuity("verify", fixture);
    const firstIds = await readMintedIds(fixture.gatewayDb);
    const receiptText = await readFile(`${fixture.pre}.verified.json`, "utf8");

    expect(first.stdout).toBe("identity continuity verified\n");
    expect(first.stderr).toBe("");
    expect((await stat(`${fixture.pre}.verified.json`)).mode & 0o777).toBe(0o600);
    expect(firstIds.humanUserId).toMatch(/^hu_[a-f0-9]{24}$/);
    expect(firstIds.principalId).toBe(`h_${firstIds.humanUserId.slice(3)}`);

    await migrateFixture(fixture);
    const second = await runContinuity("verify", fixture);
    expect(second.stdout).toBe("identity continuity verified\n");
    expect(second.stderr).toBe("");
    expect(await readMintedIds(fixture.gatewayDb)).toEqual(firstIds);
    expect(await readFile(`${fixture.pre}.verified.json`, "utf8")).toBe(receiptText);

    for (const secret of Object.values(secrets)) {
      expect(receiptText).not.toContain(secret);
      expect(first.stdout).not.toContain(secret);
      expect(second.stdout).not.toContain(secret);
    }
  });

  it("fails closed with an exact location when a fingerprinted legacy value drifts", async () => {
    const fixture = await seedLegacyFixture();
    await runContinuity("snapshot", fixture);
    await migrateFixture(fixture);

    const db = createClient({ url: `file:${fixture.gatewayDb}` });
    await db.execute({
      sql: "UPDATE human_user SET client_key = ? WHERE name_key = ?",
      args: ["ck_mutated_DO_NOT_PRINT", "legacy"],
    });
    db.close();

    const failure = await runContinuityFailure("verify", fixture);
    expect(failure.code).toBe(1);
    expect(failure.stdout).toBe("");
    expect(failure.stderr).toContain(
      "continuity drift: table=human_user row=name_key:legacy column=client_key",
    );
    expect(failure.stderr).not.toContain(secrets.humanClientKey);
    expect(failure.stderr).not.toContain("ck_mutated_DO_NOT_PRINT");
  });
});

interface Fixture {
  root: string;
  gatewayDb: string;
  daemonDb: string;
  pre: string;
}

async function seedLegacyFixture(): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "nexus-v016-continuity-"));
  cleanupRoots.push(root);
  const fixture = {
    root,
    gatewayDb: join(root, "gateway.db"),
    daemonDb: join(root, "nexus.db"),
    pre: join(root, "pre.json"),
  };

  const gateway = createClient({ url: `file:${fixture.gatewayDb}` });
  await createGatewayV1StoreForTest(gateway);
  await gateway.batch([
    {
      sql: `INSERT INTO human_user
              (name_key, name, password_hash, client_key, project, daemon_session_id,
               created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        "legacy",
        "Legacy Human",
        secrets.passwordHash,
        secrets.humanClientKey,
        "default",
        "s_human",
        11,
        12,
      ],
    },
    {
      sql: `INSERT INTO human_session
              (cookie_token, name, client_key, project, daemon_session_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)`,
      args: [
        secrets.cookie,
        "Legacy Human",
        secrets.humanClientKey,
        "default",
        "s_human",
        13,
      ],
    },
  ], "write");
  gateway.close();

  const daemon = createClient({ url: `file:${fixture.daemonDb}` });
  await daemon.batch([
    `CREATE TABLE agents (
      agent_id TEXT PRIMARY KEY, project TEXT NOT NULL, name TEXT,
      default_harness TEXT, role TEXT, tier TEXT NOT NULL DEFAULT 'agent',
      disabled_at INTEGER, created_at INTEGER NOT NULL, metadata_json TEXT,
      owner_name TEXT, owner_project TEXT, owner_session_id TEXT,
      owner_agent_id TEXT, lifecycle_state TEXT, dead_reason TEXT
    )`,
    `CREATE TABLE identity_sessions (
      runtime_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, project TEXT NOT NULL,
      harness TEXT NOT NULL, mode TEXT NOT NULL, backend TEXT, cwd TEXT,
      native_resume_key TEXT, client_key TEXT, updated_at INTEGER NOT NULL
    )`,
    `CREATE TABLE native_thread_bindings (
      harness TEXT NOT NULL, native_thread_id TEXT NOT NULL, agent_id TEXT NOT NULL,
      project TEXT NOT NULL, first_runtime_id TEXT, last_runtime_id TEXT,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, released_at INTEGER,
      PRIMARY KEY (harness, native_thread_id)
    )`,
    {
      sql: `INSERT INTO agents
              (agent_id, project, name, default_harness, role, tier, disabled_at,
               created_at, metadata_json, owner_name, owner_project, owner_session_id,
               owner_agent_id, lifecycle_state, dead_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        "a_legacy",
        "default",
        "Legacy Agent",
        "claude",
        "worker",
        "agent",
        null,
        21,
        "{}",
        "Legacy Human",
        "default",
        "s_human",
        null,
        "registered",
        null,
      ],
    },
    {
      sql: `INSERT INTO identity_sessions
              (runtime_id, agent_id, project, harness, mode, backend, cwd,
               native_resume_key, client_key, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        "s_agent",
        "a_legacy",
        "default",
        "claude",
        "normal",
        "native",
        "/workspace",
        secrets.resumeKey,
        secrets.agentClientKey,
        22,
      ],
    },
    {
      sql: `INSERT INTO native_thread_bindings
              (harness, native_thread_id, agent_id, project, first_runtime_id,
               last_runtime_id, created_at, updated_at, released_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      args: ["claude", "thread_legacy", "a_legacy", "default", "s_agent", "s_agent", 23, 24, null],
    },
  ], "write");
  daemon.close();

  await chmod(fixture.gatewayDb, 0o600);
  await chmod(fixture.daemonDb, 0o600);
  return fixture;
}

async function migrateFixture(fixture: Fixture): Promise<void> {
  const gateway = createClient({ url: `file:${fixture.gatewayDb}` });
  await migrateGatewayStore(gateway);
  gateway.close();

  const daemon = createClient({ url: `file:${fixture.daemonDb}` });
  const columns = await daemon.execute("PRAGMA table_info(native_thread_bindings)");
  const names = new Set(columns.rows.map((row) => String(row.name)));
  if (names.has("harness")) {
    await daemon.batch([
      "ALTER TABLE native_thread_bindings RENAME COLUMN harness TO provider",
      "ALTER TABLE native_thread_bindings ADD COLUMN kind TEXT NOT NULL DEFAULT 'harness'",
    ], "write");
  }
  daemon.close();
}

async function readMintedIds(gatewayDb: string): Promise<{
  humanUserId: string;
  principalId: string;
}> {
  const db = createClient({ url: `file:${gatewayDb}` });
  const result = await db.execute(
    `SELECT u.human_user_id, s.principal_id
       FROM human_user u JOIN human_session s USING (human_user_id)
       WHERE u.name_key = 'legacy'`,
  );
  db.close();
  return {
    humanUserId: String(result.rows[0]?.human_user_id),
    principalId: String(result.rows[0]?.principal_id),
  };
}

async function runContinuity(
  command: "snapshot" | "verify",
  fixture: Fixture,
): Promise<{ stdout: string; stderr: string }> {
  const argument = command === "snapshot" ? "--out" : "--pre";
  return execFileAsync(process.execPath, [
    scriptPath,
    command,
    argument,
    fixture.pre,
    "--gateway-db",
    fixture.gatewayDb,
    "--daemon-db",
    fixture.daemonDb,
  ], { cwd: repositoryRoot, encoding: "utf8" });
}

async function runContinuityFailure(
  command: "snapshot" | "verify",
  fixture: Fixture,
): Promise<{ code: number; stdout: string; stderr: string }> {
  try {
    const result = await runContinuity(command, fixture);
    return { code: 0, ...result };
  } catch (error) {
    const failure = error as Error & { code?: number; stdout?: string; stderr?: string };
    return {
      code: Number(failure.code ?? -1),
      stdout: String(failure.stdout ?? ""),
      stderr: String(failure.stderr ?? ""),
    };
  }
}

function fingerprint(table: string, column: string, value: string): string {
  return createHash("sha256")
    .update(`nexus-v016-continuity|${table}.${column}|${value}`)
    .digest("hex");
}

async function fileDigest(path: string): Promise<string> {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}
