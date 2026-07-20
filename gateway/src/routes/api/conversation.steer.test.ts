import { createClient, type Client } from "@libsql/client";
import { describe, expect, it } from "vitest";

import { localOperatorCaller } from "@server/auth/webAuthMode";
import { handleConversationSteerPost } from "./conversation.steer";

const COMMAND_DDL = `
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
  caller_principal_id TEXT,
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

async function makeCommandDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await db.executeMultiple(COMMAND_DDL);
  return db;
}

describe("POST /api/conversation/steer", () => {
  it("waits for done and returns the native steer result with an idempotent command", async () => {
    const db = await makeCommandDb();
    const clock = [1_000, 1_000, 2_000];

    const res = await handleConversationSteerPost(
      new Request("http://localhost/api/conversation/steer", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          agentId: "a_ellie",
          text: "check the active migration first",
          clientMessageId: "cm_steer_1",
        }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db,
          genCommandId: () => "cmd_steer_done",
          timeoutMs: 50,
          pollIntervalMs: 1,
          now: () => clock.shift() ?? 2_000,
          sleep: async () => {
            await db.execute({
              sql:
                "UPDATE command_intents SET status = 'done', result_json = ?, completed_at = ? " +
                "WHERE command_id = ?",
              args: [
                JSON.stringify({ accepted: true, delivery: "steered", turnId: "turn_7" }),
                1_010,
                "cmd_steer_done",
              ],
            });
          },
        },
      },
    );

    expect(res.status).toBe(201);
    await expect(res.json()).resolves.toEqual({
      ok: true,
      result: { accepted: true, delivery: "steered", turnId: "turn_7" },
    });

    const rows = await db.execute("SELECT * FROM command_intents");
    const local = localOperatorCaller();
    expect(rows.rows).toHaveLength(1);
    expect(rows.rows[0]).toMatchObject({
      command_id: "cmd_steer_done",
      kind: "harness.steer",
      status: "done",
      caller_name: local.name,
      caller_session_id: "local-operator",
      idempotency_key: "cm_steer_1",
    });
    expect(JSON.parse(String(rows.rows[0]!.request_json))).toEqual({
      name: "a_ellie",
      agentId: "a_ellie",
      text: "check the active migration first",
      clientMessageId: "cm_steer_1",
    });
  });

  it("does not report a claimed steer as accepted", async () => {
    const db = await makeCommandDb();
    const clock = [1_000, 1_000, 2_000];

    const res = await handleConversationSteerPost(
      new Request("http://localhost/api/conversation/steer", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "ellie", text: "steer this turn" }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db,
          genCommandId: () => "cmd_steer_claimed",
          timeoutMs: 50,
          pollIntervalMs: 1,
          now: () => clock.shift() ?? 2_000,
          sleep: async () => {
            await db.execute({
              sql:
                "UPDATE command_intents SET status = 'claimed', claimed_at = ?, lease_until = ? " +
                "WHERE command_id = ?",
              args: [1_010, 61_010, "cmd_steer_claimed"],
            });
          },
        },
      },
    );

    expect(res.status).toBe(504);
    await expect(res.json()).resolves.toEqual({
      error: "command intent timed out after 50ms",
    });
  });

  it("maps an inactive-turn daemon rejection to an HTTP conflict", async () => {
    const db = await makeCommandDb();
    const clock = [1_000, 1_000, 2_000];

    const res = await handleConversationSteerPost(
      new Request("http://localhost/api/conversation/steer", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "ellie",
          text: "steer only if the turn is still active",
          clientMessageId: "cm_stale_steer",
        }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db,
          genCommandId: () => "cmd_stale_steer",
          timeoutMs: 50,
          pollIntervalMs: 1,
          now: () => clock.shift() ?? 2_000,
          sleep: async () => {
            await db.execute({
              sql:
                "UPDATE command_intents SET status = 'error', error_json = ?, completed_at = ? " +
                "WHERE command_id = ?",
              args: [
                JSON.stringify({
                  code: -32006,
                  message: "no active turn to steer",
                }),
                1_010,
                "cmd_stale_steer",
              ],
            });
          },
        },
      },
    );

    expect(res.status).toBe(409);
    await expect(res.json()).resolves.toEqual({
      error: "no active turn to steer",
    });
  });

  it("rejects unauthenticated remote requests before command ingress", async () => {
    const db = await makeCommandDb();
    const res = await handleConversationSteerPost(
      new Request("http://localhost/api/conversation/steer", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "ellie", text: "steer this turn" }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "remote" },
        commandIngress: { db },
      },
    );

    expect(res.status).toBe(401);
    await expect(res.json()).resolves.toEqual({ error: "not logged in" });
    const rows = await db.execute("SELECT command_id FROM command_intents");
    expect(rows.rows).toHaveLength(0);
  });
});
