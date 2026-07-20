import { describe, expect, it, vi } from "vitest";
import { createClient, type Client } from "@libsql/client";

import { sendViaCommandIngress } from "./commandIngress";
import type { GatewayCallerIdentity } from "@server/api/http";
import type { SendRequest } from "@shared/types";

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
`;

async function makeDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await db.executeMultiple(DDL);
  return db;
}

const caller: GatewayCallerIdentity = {
  name: "Alex",
  project: "default",
  sessionId: "s_human",
  clientKey: "ck_human",
};

const req: SendRequest = {
  to: { verb: "post", thread: "design" },
  body: "hello through command ingress",
  summary: "hello",
  mention: ["bianca"],
};

async function complete(db: Client, commandId: string, result: unknown): Promise<void> {
  await db.execute({
    sql:
      "UPDATE command_intents SET status = 'done', result_json = ?, completed_at = ? " +
      "WHERE command_id = ?",
    args: [JSON.stringify(result), 2_000, commandId],
  });
}

describe("sendViaCommandIngress", () => {
  it("inserts a message.post.send command intent and returns the daemon-written Ack", async () => {
    const db = await makeDb();

    const ack = await sendViaCommandIngress(req, caller, {
      db,
      genCommandId: () => "cmd_test_1",
      now: () => 1_000,
      sleep: async () => {
        await complete(db, "cmd_test_1", { messageId: "m_done", fanout: 2 });
      },
    });

    expect(ack).toEqual({ messageId: "m_done" });

    const rows = await db.execute("SELECT * FROM command_intents");
    expect(rows.rows).toHaveLength(1);
    const row = rows.rows[0]!;
    expect(row).toMatchObject({
      command_id: "cmd_test_1",
      kind: "message.post.send",
      status: "done",
      project: "default",
      caller_name: "Alex",
      caller_session_id: "s_human",
      caller_agent_id: null,
      caller_runtime_id: "s_human",
      caller_client_key: "ck_human",
      caller_kind: "local.human",
      caller_tier: "admin",
      attempts: 0,
      created_at: 1_000,
    });
    expect(JSON.parse(String(row.request_json))).toEqual(req);
  });

  it("persists caller-provided idempotency keys on message post intents", async () => {
    const db = await makeDb();

    const ack = await sendViaCommandIngress(
      req,
      caller,
      {
        db,
        genCommandId: () => "cmd_idempotent",
        now: () => 1_000,
        sleep: async () => {
          await complete(db, "cmd_idempotent", { messageId: "m_done", fanout: 1 });
        },
      },
      { idempotencyKey: "web:post:design:client-1" },
    );

    expect(ack).toEqual({ messageId: "m_done" });
    const rows = await db.execute("SELECT * FROM command_intents");
    expect(rows.rows[0]).toMatchObject({
      command_id: "cmd_idempotent",
      idempotency_key: "web:post:design:client-1",
    });
  });

  it("rejects whitespace-only message bodies before inserting command intents", async () => {
    const db = await makeDb();

    await expect(
      sendViaCommandIngress({ ...req, body: " \n\t " }, caller, {
        db,
        genCommandId: () => "cmd_empty",
      }),
    ).rejects.toMatchObject({
      code: 400,
      message: expect.stringContaining("body"),
    });

    const rows = await db.execute("SELECT COUNT(*) AS count FROM command_intents");
    expect(Number(rows.rows[0]!.count)).toBe(0);
  });

  it("maps daemon error_json to GatewayError", async () => {
    const db = await makeDb();

    await expect(
      sendViaCommandIngress(req, caller, {
        db,
        genCommandId: () => "cmd_error",
        now: () => 1_000,
        sleep: async () => {
          await db.execute({
            sql:
              "UPDATE command_intents SET status = 'error', error_json = ? " +
              "WHERE command_id = ?",
            args: [
              JSON.stringify({ code: -32602, message: "invalid target" }),
              "cmd_error",
            ],
          });
        },
      }),
    ).rejects.toMatchObject({
      code: -32602,
      message: "invalid target",
    });
  });

  it("times out with a bounded poll loop", async () => {
    const db = await makeDb();
    let time = 0;

    await expect(
      sendViaCommandIngress(req, caller, {
        db,
        genCommandId: () => "cmd_timeout",
        timeoutMs: 50,
        now: () => {
          time += 100;
          return time;
        },
        sleep: vi.fn(async () => {}),
      }),
    ).rejects.toMatchObject({
      code: 504,
      message: expect.stringContaining("timed out"),
    });
  });

  it("requires durable human identity fields", async () => {
    await expect(
      sendViaCommandIngress(req, { name: "Alex", project: "default" }, { db: await makeDb() }),
    ).rejects.toMatchObject({
      code: 401,
      message: expect.stringContaining("logged-in human"),
    });
  });

  it("rejects a web caller with session id but no client key", async () => {
    await expect(
      sendViaCommandIngress(
        req,
        { name: "Alex", project: "default", sessionId: "s_human" },
        { db: await makeDb() },
      ),
    ).rejects.toMatchObject({
      code: 401,
      message: expect.stringContaining("logged-in human"),
    });
  });
});
