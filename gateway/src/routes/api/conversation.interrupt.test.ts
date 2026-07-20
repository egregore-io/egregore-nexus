import { createClient, type Client } from "@libsql/client";
import { describe, expect, it } from "vitest";

import { localOperatorCaller } from "@server/auth/webAuthMode";
import { handleConversationInterruptPost } from "./conversation.interrupt";

const COMMAND_DDL = `
CREATE TABLE command_intents (
  command_id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
  project TEXT NOT NULL, caller_name TEXT NOT NULL, caller_session_id TEXT,
  caller_agent_id TEXT, caller_runtime_id TEXT, caller_client_key TEXT,
  caller_kind TEXT, caller_tier TEXT, idempotency_key TEXT, request_json TEXT NOT NULL,
  result_json TEXT, error_json TEXT, attempts INTEGER NOT NULL DEFAULT 0,
  revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, claimed_at INTEGER,
  started_at INTEGER, lease_until INTEGER, completed_at INTEGER
);
CREATE UNIQUE INDEX idx_command_intents_idempotency_scope
  ON command_intents(project, kind, COALESCE(caller_client_key, caller_session_id, caller_name), idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE TABLE command_intent_events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT NOT NULL, session_id TEXT,
  command_id TEXT NOT NULL, client_message_id TEXT, state TEXT NOT NULL, mode TEXT NOT NULL,
  revision INTEGER NOT NULL, created_at INTEGER NOT NULL
);`;

async function commandDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await db.executeMultiple(COMMAND_DDL);
  return db;
}

describe("POST /api/conversation/interrupt", () => {
  it("persists the authenticated caller and waits for adapter interrupt completion", async () => {
    const db = await commandDb();
    const clock = [1_000, 1_000, 2_000];
    const response = await handleConversationInterruptPost(
      new Request("http://localhost/api/conversation/interrupt", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "stale-name",
          agentId: "a_target",
          clientMessageId: "cm_interrupt",
        }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db,
          genCommandId: () => "cmd_interrupt",
          timeoutMs: 50,
          pollIntervalMs: 1,
          now: () => clock.shift() ?? 2_000,
          sleep: async () => {
            await db.execute({
              sql: "UPDATE command_intents SET status='done', result_json=?, completed_at=? WHERE command_id=?",
              args: [JSON.stringify({ interrupted: true }), 1_010, "cmd_interrupt"],
            });
          },
        },
      },
    );

    expect(response.status).toBe(201);
    await expect(response.json()).resolves.toEqual({
      ok: true,
      result: { interrupted: true },
    });
    const rows = await db.execute("SELECT * FROM command_intents");
    const local = localOperatorCaller();
    expect(rows.rows).toHaveLength(1);
    expect(rows.rows[0]).toMatchObject({
      command_id: "cmd_interrupt",
      kind: "harness.interrupt",
      caller_name: local.name,
      caller_session_id: "local-operator",
      caller_kind: "local.human",
      idempotency_key: "cm_interrupt",
      status: "done",
    });
    expect(JSON.parse(String(rows.rows[0]!.request_json))).toEqual({
      name: "stale-name",
      agentId: "a_target",
      clientMessageId: "cm_interrupt",
    });
  });

  it("rejects unauthenticated remote requests before durable ingress", async () => {
    const db = await commandDb();
    const response = await handleConversationInterruptPost(
      new Request("http://localhost/api/conversation/interrupt", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "target", clientMessageId: "cm_no_auth" }),
      }),
      { env: { NEXUS_WEB_AUTH_MODE: "remote" }, commandIngress: { db } },
    );

    expect(response.status).toBe(401);
    expect((await db.execute("SELECT command_id FROM command_intents")).rows).toHaveLength(0);
  });
});
