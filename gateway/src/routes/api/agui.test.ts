import { describe, expect, it, vi } from "vitest";
import { spawn } from "node:child_process";
import { createClient, type Client } from "@libsql/client";

import type { RunDeps } from "@server/agui/run";
import { initSchema } from "@server/conversation/store";
import { registerHuman } from "@server/identity/human";
import type { CommandIntentSender } from "@server/api/http";
import { localOperatorCaller } from "@server/auth/webAuthMode";
import { makeRunWithHuman } from "./agui";

vi.mock("node:child_process", () => ({
  spawn: vi.fn(() => {
    throw new Error("CLI spawn should not be used by AG-UI route wiring");
  }),
}));

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

async function makeWebDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await initSchema(db);
  return db;
}

async function makeCommandDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await db.executeMultiple(COMMAND_DDL);
  return db;
}

function makeCommandMock(): CommandIntentSender {
  return {
    submit: vi.fn(async (_kind, request) => ({
      ok: true,
      sessionId: "sess_agui_human",
      name: (request as { name?: string })?.name ?? "unknown",
      token: "t",
    })) as CommandIntentSender["submit"],
  };
}

async function waitForCommand(db: Client) {
  for (let i = 0; i < 50; i++) {
    const res = await db.execute("SELECT * FROM command_intents LIMIT 1");
    if (res.rows[0]) return res.rows[0]!;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("timed out waiting for command_intents row");
}

async function completeCommand(db: Client, commandId: string): Promise<void> {
  await db.execute({
    sql:
      "UPDATE command_intents SET status = 'done', result_json = ?, completed_at = ? " +
      "WHERE command_id = ?",
    args: [JSON.stringify({ messageId: "m_agui", fanout: 1 }), 1_000_100, commandId],
  });
}

describe("/api/agui route wiring", () => {
  it("uses command ingress for the default human Message Post sender", async () => {
    const webDb = await makeWebDb();
    const commandDb = await makeCommandDb();
    const commands = makeCommandMock();
    let idSeq = 0;
    const { cookieToken } = await registerHuman(
      { name: "alice", password: "pw" },
      {
        db: webDb,
        commands,
        genId: () => `id_${++idSeq}`,
        now: () => 1_000_000,
      },
    );

    const handleRun = vi.fn(async (_request: Request, deps?: RunDeps) => {
      await deps?.sendMessage?.({
        to: { verb: "post", thread: "backend" },
        body: "hello from agui",
      });
      return new Response("ok", { status: 200 });
    });
    const runWithHuman = makeRunWithHuman({
      db: async () => webDb,
      commandIngressDb: () => commandDb,
      commandIngress: { pollIntervalMs: 1, timeoutMs: 500 },
      handleRun,
    });

    const request = new Request("http://localhost/api/agui?thread=backend", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}`,
      },
      body: JSON.stringify({ messages: [] }),
    });

    const pending = runWithHuman(request);
    const row = await waitForCommand(commandDb);
    await completeCommand(commandDb, String(row.command_id));
    const response = await pending;

    expect(response.status).toBe(200);
    expect(handleRun).toHaveBeenCalledOnce();
    expect(spawn).not.toHaveBeenCalled();
    expect(row).toMatchObject({
      kind: "message.post.send",
      status: "pending",
      project: "default",
      caller_name: "alice",
      caller_session_id: "sess_agui_human",
      caller_agent_id: null,
      caller_runtime_id: "sess_agui_human",
      caller_kind: "human",
      caller_tier: "admin",
    });
    expect(JSON.parse(String(row.request_json))).toEqual({
      to: { verb: "post", thread: "backend" },
      body: "hello from agui",
    });
  });

  it("allows local-mode AG-UI sends without a login and stamps the local operator", async () => {
    const webDb = await makeWebDb();
    const commandDb = await makeCommandDb();
    const handleRun = vi.fn(async (_request: Request, deps?: RunDeps) => {
      await deps?.sendMessage?.({
        to: { verb: "post", thread: "backend" },
        body: "hello from local agui",
      });
      return new Response("ok", { status: 200 });
    });
    const runWithHuman = makeRunWithHuman({
      db: async () => webDb,
      commandIngressDb: () => commandDb,
      commandIngress: { pollIntervalMs: 1, timeoutMs: 500 },
      handleRun,
      authMode: "local",
    });

    const request = new Request("http://localhost/api/agui?thread=backend", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ messages: [] }),
    });

    const pending = runWithHuman(request);
    const row = await waitForCommand(commandDb);
    await completeCommand(commandDb, String(row.command_id));
    const response = await pending;
    const local = localOperatorCaller();

    expect(response.status).toBe(200);
    expect(row).toMatchObject({
      kind: "message.post.send",
      project: "default",
      caller_name: local.name,
      caller_session_id: "local-operator",
      caller_runtime_id: "local-operator",
      caller_client_key: null,
      caller_kind: "human",
      caller_tier: "admin",
    });
  });

  it("rejects remote-mode AG-UI sends without a login", async () => {
    const webDb = await makeWebDb();
    const handleRun = vi.fn(async () => new Response("ok", { status: 200 }));
    const runWithHuman = makeRunWithHuman({
      db: async () => webDb,
      handleRun,
      authMode: "remote",
    });

    const request = new Request("http://localhost/api/agui?thread=backend", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        host: "localhost:5173",
        "x-forwarded-for": "127.0.0.1",
      },
      body: JSON.stringify({ messages: [] }),
    });

    const response = await runWithHuman(request);

    expect(response.status).toBe(401);
    expect(handleRun).not.toHaveBeenCalled();
  });
});
