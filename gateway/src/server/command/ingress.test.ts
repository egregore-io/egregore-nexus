import { createClient, type Client, type InStatement } from "@libsql/client";
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import {
  COMMAND_KINDS,
  enqueueCommandIntent,
  submitCommandIntent,
  type CommandIngressOptions,
} from "./ingress";
import { localOperatorCaller } from "@server/auth/webAuthMode";
import { migrateGatewayStore } from "@server/store/migrations";
import { DaemonIpcError } from "@server/daemon/ipc";
import { Kind, Tier } from "@shared/types";

const DDL = `
CREATE TABLE IF NOT EXISTS command_intents (
  command_id        TEXT PRIMARY KEY,
  kind              TEXT NOT NULL,
  status            TEXT NOT NULL,
  project           TEXT NOT NULL,
  caller_name       TEXT NOT NULL,
  caller_session_id TEXT,
  caller_agent_id   TEXT,
  caller_runtime_id TEXT,
  caller_client_key TEXT,
  caller_kind       TEXT,
  caller_tier       TEXT,
  idempotency_key   TEXT,
  request_json      TEXT NOT NULL,
  result_json       TEXT,
  error_json        TEXT,
  attempts          INTEGER NOT NULL DEFAULT 0,
  revision          INTEGER NOT NULL DEFAULT 1,
  created_at        INTEGER NOT NULL,
  claimed_at        INTEGER,
  started_at        INTEGER,
  lease_until       INTEGER,
  completed_at      INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_command_intents_idempotency_scope
  ON command_intents(project, kind, COALESCE(caller_client_key, caller_session_id, caller_name), idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS command_intent_events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  project TEXT NOT NULL,
  session_id TEXT,
  command_id TEXT NOT NULL,
  client_message_id TEXT,
  state TEXT NOT NULL,
  mode TEXT NOT NULL,
  revision INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TRIGGER IF NOT EXISTS queue_insert BEFORE INSERT ON command_intents
WHEN NEW.kind IN ('harness.prompt', 'harness.steer')
BEGIN
  INSERT INTO command_intent_events
    (project, session_id, command_id, client_message_id, state, mode, revision, created_at)
  VALUES
    (NEW.project, 's_otto', NEW.command_id, json_extract(NEW.request_json, '$.clientMessageId'),
     'queued', CASE WHEN NEW.kind = 'harness.steer' THEN 'redirect' ELSE 'queue' END,
     NEW.revision, NEW.created_at);
END;
`;

async function makeDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await db.executeMultiple(DDL);
  return db;
}

async function complete(db: Client, commandId: string, result: unknown): Promise<void> {
  await db.execute({
    sql:
      "UPDATE command_intents SET status = 'done', result_json = ?, completed_at = ? " +
      "WHERE command_id = ?",
    args: [JSON.stringify(result), 2_000, commandId],
  });
}

describe("submitCommandIntent", () => {
  it("rebinds a persisted human once per daemon boot before durable acceptance", async () => {
    const calls: Array<{
      mode: "command" | "enqueue";
      kind: string;
      request: unknown;
      caller: unknown;
    }> = [];
    let daemonBootId = "boot_2";
    let registrations = 0;
    const options = {
      daemonBootId: async () => daemonBootId,
      daemonCommand: async (kind: string, request: unknown, caller: unknown) => {
        calls.push({ mode: "command", kind, request, caller });
        registrations += 1;
        return {
          sessionId: `s_rebound_${registrations}`,
          agentId: "a_human_earl",
        };
      },
      daemonEnqueue: async (kind: string, request: unknown, caller: unknown) => {
        calls.push({ mode: "enqueue", kind, request, caller });
        return {
          commandId: `cmd_prompt_${calls.length}`,
          status: "pending",
          createdAt: 1_000,
          revision: 1,
          sessionId: "s_target",
          seq: calls.length,
        };
      },
    } as unknown as CommandIngressOptions;
    const persistedHuman = {
      id: "human:default:nexus_ck_human",
      name: "earl",
      project: "default",
      kind: Kind.Human,
      tier: Tier.Admin,
      credentialFacet: "human" as const,
      sessionId: "s_from_previous_boot",
      runtimeId: "s_from_previous_boot",
      clientKey: "nexus_ck_human",
    };

    await enqueueCommandIntent(
      COMMAND_KINDS.harnessPrompt,
      { name: "fable", text: "first" },
      persistedHuman,
      options,
    );
    expect(calls).toEqual([
      expect.objectContaining({
        mode: "command",
        kind: COMMAND_KINDS.identityRegister,
        request: expect.objectContaining({
          name: "earl",
          project: "default",
          clientKey: "nexus_ck_human",
          kind: "human",
        }),
      }),
      expect.objectContaining({
        mode: "enqueue",
        kind: COMMAND_KINDS.harnessPrompt,
        caller: expect.objectContaining({
          sessionId: "s_rebound_1",
          runtimeId: "s_rebound_1",
          agentId: "a_human_earl",
          clientKey: "nexus_ck_human",
        }),
      }),
    ]);

    calls.length = 0;
    await enqueueCommandIntent(
      COMMAND_KINDS.harnessPrompt,
      { name: "fable", text: "same boot" },
      persistedHuman,
      options,
    );
    expect(calls).toEqual([
      expect.objectContaining({
        mode: "enqueue",
        kind: COMMAND_KINDS.harnessPrompt,
        caller: expect.objectContaining({ sessionId: "s_rebound_1" }),
      }),
    ]);

    calls.length = 0;
    daemonBootId = "boot_3";
    await enqueueCommandIntent(
      COMMAND_KINDS.harnessPrompt,
      { name: "fable", text: "next boot" },
      persistedHuman,
      options,
    );
    expect(calls).toEqual([
      expect.objectContaining({ mode: "command", kind: COMMAND_KINDS.identityRegister }),
      expect.objectContaining({
        mode: "enqueue",
        kind: COMMAND_KINDS.harnessPrompt,
        caller: expect.objectContaining({ sessionId: "s_rebound_2" }),
      }),
    ]);
  });

  it("coalesces concurrent first writes into one human rebind per daemon boot", async () => {
    let releaseRegistration!: () => void;
    const registrationReleased = new Promise<void>((resolve) => {
      releaseRegistration = resolve;
    });
    let registrationStarted!: () => void;
    const registrationObserved = new Promise<void>((resolve) => {
      registrationStarted = resolve;
    });
    const daemonCommand = vi.fn(async () => {
      registrationStarted();
      await registrationReleased;
      return { sessionId: "s_concurrent_human", agentId: "a_concurrent_human" };
    });
    let enqueueSeq = 0;
    const daemonEnqueue = vi.fn(async (
      _kind: string,
      _request: unknown,
      _caller: unknown,
    ) => ({
      commandId: `cmd_concurrent_${++enqueueSeq}`,
      status: "pending",
      createdAt: 1_000,
      revision: 1,
      sessionId: "s_target",
      seq: enqueueSeq,
    }));
    const options = {
      daemonBootId: async () => "boot_concurrent",
      daemonCommand,
      daemonEnqueue,
    } as unknown as CommandIngressOptions;
    const caller = {
      id: "human:default:nexus_ck_concurrent_human",
      name: "concurrent-human",
      project: "default",
      kind: Kind.Human,
      tier: Tier.Admin,
      credentialFacet: "human" as const,
      sessionId: "s_previous_boot",
      runtimeId: "s_previous_boot",
      clientKey: "nexus_ck_concurrent_human",
    };

    const first = enqueueCommandIntent(
      COMMAND_KINDS.harnessPrompt,
      { name: "fable", text: "first concurrent write" },
      caller,
      options,
    );
    await registrationObserved;
    const second = enqueueCommandIntent(
      COMMAND_KINDS.harnessPrompt,
      { name: "fable", text: "second concurrent write" },
      caller,
      options,
    );
    await vi.waitFor(() => expect(daemonCommand).toHaveBeenCalledTimes(1));
    releaseRegistration();

    await expect(Promise.all([first, second])).resolves.toHaveLength(2);
    expect(daemonCommand).toHaveBeenCalledTimes(1);
    expect(daemonEnqueue).toHaveBeenCalledTimes(2);
    for (const call of daemonEnqueue.mock.calls) {
      expect(call[2]).toEqual(expect.objectContaining({
        sessionId: "s_concurrent_human",
        runtimeId: "s_concurrent_human",
        agentId: "a_concurrent_human",
        clientKey: "nexus_ck_concurrent_human",
      }));
    }
  });

  it("fails before durable acceptance when the human rebind cannot reach the daemon", async () => {
    const daemonEnqueue = vi.fn(async () => ({
      commandId: "cmd_must_not_exist",
      status: "pending",
      createdAt: 1_000,
      revision: 1,
      seq: 1,
    }));

    await expect(enqueueCommandIntent(
      COMMAND_KINDS.harnessPrompt,
      { name: "fable", text: "do not accept this" },
      {
        name: "remote-human",
        project: "default",
        kind: Kind.Human,
        tier: Tier.Admin,
        credentialFacet: "human",
        sessionId: "s_stale",
        clientKey: "nexus_ck_unreachable_human",
      },
      {
        daemonBootId: async () => {
          throw new DaemonIpcError("Nexus daemon IPC endpoint is unavailable");
        },
        daemonEnqueue,
      },
    )).rejects.toMatchObject({ code: 503 });

    expect(daemonEnqueue).not.toHaveBeenCalled();
  });

  it("settles Gateway-local idempotency around daemon IPC and replays the original result", async () => {
    const ingressDb = createClient({ url: ":memory:" });
    await migrateGatewayStore(ingressDb);
    let calls = 0;
    const options = {
      ingressDb,
      genCommandId: () => "cmd_gateway_ingress",
      now: () => 1_000,
      daemonCommand: async () => {
        calls += 1;
        return { messageId: "m_once" };
      },
    };
    const first = await submitCommandIntent(
      COMMAND_KINDS.messagePostSend,
      { body: "once" },
      localOperatorCaller(),
      options,
      "client-once",
    );
    const replay = await submitCommandIntent(
      COMMAND_KINDS.messagePostSend,
      { body: "different retry bytes" },
      localOperatorCaller(),
      options,
      "client-once",
    );
    expect(first).toEqual({ messageId: "m_once" });
    expect(replay).toEqual(first);
    expect(calls).toBe(1);
  });

  it("maps an absent daemon to an explicit transport-unavailable 503 without terminal settlement", async () => {
    const ingressDb = createClient({ url: ":memory:" });
    await migrateGatewayStore(ingressDb);
    await expect(submitCommandIntent(
      COMMAND_KINDS.threadCreate,
      { name: "design" },
      localOperatorCaller(),
      {
        ingressDb,
        daemonCommand: async () => {
          throw new DaemonIpcError("Nexus daemon IPC endpoint is unavailable");
        },
      },
      "daemon-down",
    )).rejects.toMatchObject({ code: 503 });
    const rows = await ingressDb.execute("SELECT status FROM gateway_ingress");
    expect(rows.rows).toMatchObject([{ status: "pending" }]);
  });

  it("has no production direct-store constructor or legacy database routing", () => {
    const source = readFileSync("src/server/command/ingress.ts", "utf8");
    expect(source).not.toMatch(
      /createClient|NEXUS_DB_URL|NEXUS_DB_PATH|NEXUS_DB_WRITE_TOKEN|getCommandIngressDbLazy/,
    );
  });

  it("uses held daemon IPC in production instead of opening the canonical store", async () => {
    const calls: unknown[][] = [];
    const result = await submitCommandIntent(
      COMMAND_KINDS.threadCreate,
      { name: "backend", members: ["blake"] },
      localOperatorCaller(),
      {
        genCommandId: () => "cmd_daemon_submit",
        timeoutMs: 750,
        daemonCommand: async (...args) => {
          calls.push(args);
          return { created: true };
        },
      },
    );

    expect(result).toEqual({ created: true });
    expect(calls).toEqual([[
      "thread.create",
      { name: "backend", members: ["blake"] },
      expect.objectContaining({
        name: localOperatorCaller().name,
        sessionId: "local-operator",
        runtimeId: "local-operator",
        kind: "human",
        tier: "admin",
      }),
      expect.objectContaining({
        commandId: "cmd_daemon_submit",
        timeoutMs: 750,
      }),
    ]]);
  });

  it("uses daemon IPC acceptance mode for the durable queue receipt", async () => {
    const calls: unknown[][] = [];
    const receipt = {
      commandId: "cmd_daemon_enqueue",
      status: "pending",
      createdAt: 1_000,
      revision: 1,
      sessionId: "s_otto",
      seq: 7,
    };
    await expect(enqueueCommandIntent(
      COMMAND_KINDS.harnessPrompt,
      { name: "otto", text: "hello", clientMessageId: "cm_daemon" },
      localOperatorCaller(),
      {
        genCommandId: () => "cmd_daemon_enqueue",
        daemonEnqueue: async (...args) => {
          calls.push(args);
          return receipt;
        },
      },
      "cm_daemon",
    )).resolves.toEqual(receipt);
    expect(calls[0]?.[0]).toBe("harness.prompt");
    expect(calls[0]?.[3]).toEqual(expect.objectContaining({
      commandId: "cmd_daemon_enqueue",
      idempotencyKey: "cm_daemon",
    }));
  });

  it("returns the inserted queue receipt without follow-up selects", async () => {
    const db = await makeDb();
    const statements: string[] = [];
    const counted = new Proxy(db, {
      get(target, property) {
        if (property === "execute") {
          return async (statement: InStatement) => {
            statements.push(typeof statement === "string" ? statement : statement.sql);
            return target.execute(statement);
          };
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as Client;

    await expect(
      enqueueCommandIntent(
        COMMAND_KINDS.harnessPrompt,
        { name: "otto", text: "hello", clientMessageId: "cm_returning" },
        localOperatorCaller(),
        {
          db: counted,
          genCommandId: () => "cmd_returning",
          now: () => 1_000,
        },
      ),
    ).resolves.toEqual({
      commandId: "cmd_returning",
      status: "pending",
      createdAt: 1_000,
      revision: 1,
      sessionId: "s_otto",
      seq: 1,
    });

    expect(statements).toHaveLength(1);
    expect(statements[0]).toMatch(/INSERT[\s\S]+RETURNING/i);
  });

  it("batch-polls concurrent in-flight commands with one IN query per tick", async () => {
    const db = await makeDb();
    const pollStatements: string[] = [];
    const counted = new Proxy(db, {
      get(target, property) {
        if (property === "execute") {
          return async (statement: InStatement) => {
            const sql = typeof statement === "string" ? statement : statement.sql;
            if (/FROM command_intents WHERE command_id IN/i.test(sql)) pollStatements.push(sql);
            return target.execute(statement);
          };
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as Client;
    let sleeps = 0;
    const sleep = async () => {
      sleeps += 1;
      await Promise.all([
        complete(db, "cmd_batch_a", { value: "a" }),
        complete(db, "cmd_batch_b", { value: "b" }),
      ]);
    };

    const [a, b] = await Promise.all([
      submitCommandIntent(
        COMMAND_KINDS.threadCreate,
        { name: "a" },
        localOperatorCaller(),
        {
          db: counted,
          genCommandId: () => "cmd_batch_a",
          now: () => 1_000,
          sleep,
        },
      ),
      submitCommandIntent(
        COMMAND_KINDS.threadCreate,
        { name: "b" },
        localOperatorCaller(),
        {
          db: counted,
          genCommandId: () => "cmd_batch_b",
          now: () => 1_000,
          sleep,
        },
      ),
    ]);

    expect([a, b]).toEqual([{ value: "a" }, { value: "b" }]);
    expect(sleeps).toBe(1);
    expect(pollStatements).toHaveLength(2);
    expect(pollStatements.every((sql) => /IN \(\?, \?\)/.test(sql))).toBe(true);
  });

  it("rejects omitted callers instead of synthesizing a local operator", async () => {
    const db = await makeDb();

    await expect(
      submitCommandIntent(
        COMMAND_KINDS.threadCreate,
        { name: "backend", members: ["blake"] },
        undefined,
        {
          db,
          genCommandId: () => "cmd_thread_create_missing_caller",
          now: () => 1_000,
        },
      ),
    ).rejects.toThrow(/explicit Principal/);

    const rows = await db.execute("SELECT * FROM command_intents");
    expect(rows.rows).toHaveLength(0);
  });

  it("submits explicit local-mode commands as the local operator caller", async () => {
    const db = await makeDb();

    const result = await submitCommandIntent(
      COMMAND_KINDS.threadCreate,
      { name: "backend", members: ["blake"] },
      localOperatorCaller(),
      {
        db,
        genCommandId: () => "cmd_thread_create_1",
        now: () => 1_000,
        sleep: async () => {
          await complete(db, "cmd_thread_create_1", null);
        },
      },
    );

    expect(result).toBeNull();

    const rows = await db.execute("SELECT * FROM command_intents");
    expect(rows.rows).toHaveLength(1);
    const row = rows.rows[0]!;
    const local = localOperatorCaller();
    expect(row).toMatchObject({
      command_id: "cmd_thread_create_1",
      kind: "thread.create",
      status: "done",
      project: "default",
      caller_name: local.name,
      caller_session_id: "local-operator",
      caller_runtime_id: "local-operator",
      caller_client_key: null,
      caller_kind: "human",
      caller_tier: "admin",
      created_at: 1_000,
    });
    expect(JSON.parse(String(row.request_json))).toEqual({
      name: "backend",
      members: ["blake"],
    });
  });

  it("dedupes retry-prone writes with the same idempotency key", async () => {
    const db = await makeDb();
    const caller = {
      name: "operator",
      project: "default",
      sessionId: "s_operator",
      clientKey: "ck_operator",
    };

    let seq = 0;
    const first = submitCommandIntent(
      COMMAND_KINDS.messagePostSend,
      { to: { verb: "post", thread: "backend" }, body: "hello" },
      caller,
      {
        db,
        genCommandId: () => `cmd_retry_${++seq}`,
        now: () => 1_000,
        pollIntervalMs: 1,
        timeoutMs: 500,
        sleep: async () => {
          await complete(db, "cmd_retry_1", { messageId: "m_once", fanout: 1 });
        },
      },
      "web:post:backend:client-1",
    );
    const second = submitCommandIntent(
      COMMAND_KINDS.messagePostSend,
      { to: { verb: "post", thread: "backend" }, body: "hello" },
      caller,
      {
        db,
        genCommandId: () => `cmd_retry_${++seq}`,
        now: () => 1_001,
        pollIntervalMs: 1,
        timeoutMs: 500,
      },
      "web:post:backend:client-1",
    );

    await expect(first).resolves.toEqual({ messageId: "m_once", fanout: 1 });
    await expect(second).resolves.toEqual({ messageId: "m_once", fanout: 1 });

    const rows = await db.execute("SELECT * FROM command_intents");
    expect(rows.rows).toHaveLength(1);
    expect(rows.rows[0]).toMatchObject({
      command_id: "cmd_retry_1",
      idempotency_key: "web:post:backend:client-1",
    });
  });
});
