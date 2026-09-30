import { createClient, type Client } from "@libsql/client";
import { describe, expect, it } from "vitest";

import { localOperatorCaller } from "@server/auth/webAuthMode";
import { handleConversationPromptPost } from "./conversation.prompt";
import { handleConversationSteerPost } from "./conversation.steer";
import { handleConversationInterruptPost } from "./conversation.interrupt";

describe.each([
  ["prompt", handleConversationPromptPost],
  ["steer", handleConversationSteerPost],
  ["interrupt", handleConversationInterruptPost],
] as const)("exact HTTP %s", (kind, handle) => {
    const request = (extra: Record<string, unknown>) =>
    new Request(`http://localhost/api/conversation/${kind}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "target",
        agentId: "a_target",
        text: "input",
        ...extra,
      }),
    });

  it.each([null, [], "invalid", 3])(
    "rejects non-object JSON without effects: %j",
    async (body) => {
      const db = await makeCommandDb();
      try {
        const response = await handle(
          new Request(`http://localhost/api/conversation/${kind}`, {
            method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }), {
      env: { NEXUS_WEB_AUTH_MODE: "local" },
      commandIngress: { db, timeoutMs: 0 },
    });
    expect(response.status).toBe(400);
    expect(
          (await db.execute("SELECT * FROM command_intents")).rows).toHaveLength(0);
      } finally {
        db.close();
  }
    },
  );

  it.each([
    { expectedSessionId: null },
    { expectedSessionId: "" },
    { expectedSessionId: "  " },
      { expectedSessionId: {} },
    { expectedSessionId: "s_target", agentId: undefined },
    { expectedSessionId: "s_target", agentId: " " },
  ])("rejects malformed exact identity before effects: %j", async (extra) => {
    const db = await makeCommandDb();
    try {
      const response = await handle(request(extra),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db, timeoutMs: 0 },
      },
    );

    expect(response.status).toBe(400);
      expect(
        (await db.execute("SELECT * FROM command_intents")).rows,
      ).toHaveLength(0);
    } finally {
      db.close();
    }
  });

  it.each(["s_target", "s_foreign", undefined])(
    "carries the selector and checks actual response session %s", async (actualSession) => {
    const calls: Array<{ kind: string; request: unknown }> = [];
    const response = await handle(
        request({ expectedSessionId: "s_target" }),
        {
          env: { NEXUS_WEB_AUTH_MODE: "local" },
      commandIngress: {
            nexusHome: `/tmp/exact-http-${kind}-${actualSession}`,
            daemonBootId: async () => "boot_exact",
            daemonCommand: async (command, params) => {
              if (command === "identity.register")
                return { sessionId: "s_operator" };
              calls.push({ kind: command, request: params });
              return {
                accepted: true,
                delivered: true,
                sessionId: actualSession,
              };
            },
            daemonEnqueue: async (command, params) => {
              calls.push({ kind: command, request: params });
              return {
                commandId: "cmd_exact",
                status: "pending",
                createdAt: 1,
                revision: 1,
                seq: 1,
                sessionId: actualSession,
              };
            },
          },
        },
      );
      expect(calls).toEqual([
        {
          kind: `harness.${kind}`,
          request: expect.objectContaining({
            agentId: "a_target",
            expectedSessionId: "s_target",
          }),
        },
      ]);
      expect(response.status).toBe(actualSession === "s_target" ? 201 : 502);
      const body = await response.json();
      if (actualSession === "s_target")
        expect(body.receipt ?? body.result).toMatchObject({
          sessionId: "s_target",
        });
      else
        expect(body).toMatchObject({
          error: expect.stringMatching(/session/i),
        });
    },
  );
});

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

describe("POST /api/conversation/prompt", () => {
  it.each([
    { delivery: "auto" },
    { modelSelection: { modelId: "target-model", expectedSessionId: "s_target" } },
  ])("rejects unsupported delivery options instead of silently downgrading: %j", async (options) => {
    const db = await makeCommandDb();
    const response = await handleConversationPromptPost(new Request("http://localhost/api/conversation/prompt", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ agentId: "a_target", name: "target", text: "keep my intent", clientMessageId: "cm_options", ...options }),
    }), {
      env: { NEXUS_WEB_AUTH_MODE: "local" },
      commandIngress: { db, genCommandId: () => "cmd_options", now: () => 1_000 },
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ error: expect.stringContaining("not supported") });
    expect((await db.execute("SELECT * FROM command_intents")).rows).toHaveLength(0);
    db.close();
  });

  it("returns a stable durable queued receipt without claiming delivery", async () => {
    const db = await makeCommandDb();

    const res = await handleConversationPromptPost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "ellie", text: "hello" }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db,
          genCommandId: () => "cmd_prompt_claimed",
          now: () => 1_000,
        },
      },
    );

    expect(res.status).toBe(201);
    await expect(res.json()).resolves.toEqual({
      ok: true,
      receipt: {
        commandId: "cmd_prompt_claimed",
        state: "queued",
        revision: 1,
        seq: 0,
      },
    });

    const rows = await db.execute("SELECT * FROM command_intents");
    const local = localOperatorCaller();
    expect(rows.rows).toHaveLength(1);
    expect(rows.rows[0]).toMatchObject({
      command_id: "cmd_prompt_claimed",
      kind: "harness.prompt",
      status: "pending",
      caller_name: local.name,
      caller_session_id: "local-operator",
    });
  });

  it("acknowledges a durably queued prompt even while it remains unclaimed", async () => {
    const db = await makeCommandDb();
    const clock = [1_000, 2_000];

    const res = await handleConversationPromptPost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "ellie", text: "hello" }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db,
          genCommandId: () => "cmd_prompt_pending",
          timeoutMs: 50,
          now: () => clock.shift() ?? 2_000,
        },
      },
    );

    expect(res.status).toBe(201);
    await expect(res.json()).resolves.toEqual({
      ok: true,
      receipt: {
        commandId: "cmd_prompt_pending",
        state: "queued",
        revision: 1,
        seq: 0,
      },
    });

    const rows = await db.execute("SELECT status FROM command_intents");
    expect(rows.rows).toEqual([{ status: "pending" }]);
  });

  it("uses clientMessageId as the durable idempotency key", async () => {
    const db = await makeCommandDb();
    const ids = ["cmd_prompt_first", "cmd_prompt_retry"];
    const request = () =>
      new Request("http://localhost/api/conversation/prompt", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "ellie",
          text: "queued once",
          clientMessageId: "cm_prompt_1",
        }),
      });
    const deps = {
      env: { NEXUS_WEB_AUTH_MODE: "local" },
      commandIngress: {
        db,
        genCommandId: () => ids.shift() ?? "cmd_prompt_extra",
      },
    };

    const first = await handleConversationPromptPost(request(), deps);
    await db.execute(
      "UPDATE command_intents SET kind = 'harness.steer' WHERE command_id = 'cmd_prompt_first'",
    );
    const retry = await handleConversationPromptPost(request(), deps);

    expect(first.status).toBe(201);
    expect(retry.status).toBe(201);
    const rows = await db.execute(
      "SELECT command_id, kind, idempotency_key, status FROM command_intents",
    );
    expect(rows.rows).toEqual([
      {
        command_id: "cmd_prompt_first",
        kind: "harness.steer",
        idempotency_key: "cm_prompt_1",
        status: "pending",
      },
    ]);
  });

  it("preserves stable agentId in the queued prompt request", async () => {
    const db = await makeCommandDb();
    const clock = [1_000, 1_000, 2_000];

    const res = await handleConversationPromptPost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ agentId: "a_ellie", text: "hello" }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db,
          genCommandId: () => "cmd_prompt_id",
          timeoutMs: 50,
          pollIntervalMs: 1,
          now: () => clock.shift() ?? 2_000,
          sleep: async () => {
            await db.execute({
              sql:
                "UPDATE command_intents SET status = 'claimed', claimed_at = ?, " +
                "lease_until = ? WHERE command_id = ?",
              args: [1_010, 61_010, "cmd_prompt_id"],
            });
          },
        },
      },
    );

    expect(res.status).toBe(201);
    const rows = await db.execute("SELECT request_json FROM command_intents");
    expect(JSON.parse(String(rows.rows[0]!.request_json))).toEqual({
      name: "a_ellie",
      agentId: "a_ellie",
      text: "hello",
    });
  });

  it("does not infer agentId from an a_-prefixed name", async () => {
    const db = await makeCommandDb();

    const res = await handleConversationPromptPost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "a_team", text: "hello by name" }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        commandIngress: {
          db,
          genCommandId: () => "cmd_prompt_prefixed_name",
        },
      },
    );

    expect(res.status).toBe(201);
    const rows = await db.execute("SELECT request_json FROM command_intents");
    expect(JSON.parse(String(rows.rows[0]!.request_json))).toEqual({
      name: "a_team",
      text: "hello by name",
    });
  });
});
