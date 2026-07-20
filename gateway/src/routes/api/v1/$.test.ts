// Test: cookie→Message Post caller wiring in the /api/v1/$ chokepoint.
// Verifies that a request WITH a valid nexus_human cookie creates a
// command_intents row with human provenance; WITHOUT a valid cookie the message
// write is rejected instead of silently falling back to `operator`.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { spawn } from "node:child_process";
import { createHmac } from "node:crypto";
import { createClient, type Client } from "@libsql/client";

import { initSchema } from "@server/conversation/store";
import { seedDb } from "@drizzle/__mocks__/seedDb";
import { registerHuman } from "@server/identity/human";
import { issueBearerToken } from "@server/identity/bearer";
import type { CommandIntentSender, MessagePostSender } from "@server/api/http";
import { AGENT_ATTACH_SCOPE } from "@server/auth/principal";
import { localOperatorCaller } from "@server/auth/webAuthMode";
import { migrateGatewayStore } from "@server/store/migrations";
import { Kind, Tier } from "@shared/types";
// The chokepoint must expose a `makeDispatch` factory for DI; we test that.
import { makeDispatch } from "./$";

vi.mock("node:child_process", () => ({
  spawn: vi.fn(() => {
    throw new Error("CLI spawn should not be used by Message Post route wiring");
  }),
}));

// ── helpers ──────────────────────────────────────────────────────────────────

async function makeDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await initSchema(db);
  return db;
}

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

async function makeCommandDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await db.executeMultiple(COMMAND_DDL);
  return db;
}

async function makeCanonicalDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await migrateGatewayStore(db);
  return db;
}

function makeCommandCapture(): {
  commands: CommandIntentSender;
  calls: Array<{ kind: string; request: unknown }>;
} {
  const calls: Array<{ kind: string; request: unknown }> = [];
  return {
    calls,
    commands: {
      submit: vi.fn(async (kind, request) => {
        calls.push({ kind, request });
        // Return a shape compatible with registerHuman (needs sessionId).
        return {
          sessionId: "sess_mock_chokepoint",
          name: (request as { name?: string })?.name ?? "unknown",
          token: "t",
        };
      }) as CommandIntentSender["submit"],
    },
  };
}

const NOTIFY_HMAC_SECRET = "notify-machine-auth-secret";
const NOTIFY_TIMESTAMP = 1_784_000_000_000;

function signedNotifyRequest(options: {
  rawBody?: string;
  timestamp?: number;
  signature?: string;
  secret?: string;
} = {}): Request {
  const rawBody = options.rawBody ?? JSON.stringify({
    source: "ci",
    topic: "builds",
    payload: { status: "green" },
  });
  const timestamp = options.timestamp ?? NOTIFY_TIMESTAMP;
  const signature = options.signature ?? `sha256=${createHmac(
    "sha256",
    options.secret ?? NOTIFY_HMAC_SECRET,
  ).update(`${timestamp}.${rawBody}`).digest("hex")}`;
  return new Request("http://localhost/api/v1/notify", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-nexus-timestamp": String(timestamp),
      "x-nexus-signature": signature,
    },
    body: rawBody,
  });
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
    args: [JSON.stringify({ messageId: "m_command", fanout: 1 }), 1_000_100, commandId],
  });
}

describe("/api/v1/$ chokepoint — cookie→_caller wiring", () => {
  let db: Client;
  let commandDb: Client;

  beforeEach(async () => {
    db = await makeDb();
    commandDb = await makeCommandDb();
    vi.mocked(spawn).mockClear();
  });

  it("keeps remote-human health public without cookie, bearer, or scopes", async () => {
    const canonicalDb = await makeCanonicalDb();
    const dispatch = makeDispatch({
      db: async () => db,
      canonicalDb: () => canonicalDb,
      authMode: "remote",
    });

    const res = await dispatch(new Request("http://localhost/api/v1/health"));

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ status: "ok" });
    expect(spawn).not.toHaveBeenCalled();
  });

  it("writes a command intent with human identity when a valid nexus_human cookie is present", async () => {
    // Seed a human session so currentHuman resolves.
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    // Capture the cookie token from registerHuman's return — don't reconstruct it
    // (that would couple this test to registerHuman's internal genId call order).
    const { cookieToken } = await registerHuman({ name: "alice", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_${++idSeq}`,
      now: () => 1_000_000,
    });

    const dispatch = makeDispatch({
      db: async () => db,
      commandIngressDb: () => commandDb,
      commandIngress: { pollIntervalMs: 1, timeoutMs: 500 },
    });

    // POST /api/v1/messages is the real send route in the router's route table.
    const req = new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": "web:post:backend:client-1",
        cookie: `nexus_human=${cookieToken}`,
      },
      // POST /messages requires: { to: SendTarget, body: string }
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "hello" }),
    });

    const pending = dispatch(req);
    const row = await waitForCommand(commandDb);
    await completeCommand(commandDb, String(row.command_id));
    const res = await pending;

    expect(res.status).toBe(201);
    expect(spawn).not.toHaveBeenCalled();
    expect(row).toMatchObject({
      kind: "message.post.send",
      status: "pending",
      project: "default",
      caller_name: "alice",
      caller_session_id: "sess_mock_chokepoint",
      caller_agent_id: null,
      caller_runtime_id: "sess_mock_chokepoint",
      caller_kind: "human",
      caller_tier: "admin",
      idempotency_key: "web:post:backend:client-1",
    });
    expect(String(row.caller_client_key)).toMatch(/^id_/);
    expect(JSON.parse(String(row.request_json))).toEqual({
      to: { verb: "post", thread: "backend" },
      body: "hello",
      idempotencyKey: "web:post:backend:client-1",
    });
  });

  it("rebinds a persisted human before the first post after daemon replacement", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "test-user", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `rebind_id_${++idSeq}`,
      now: () => 1_000_000,
    });
    const calls: Array<{ kind: string; request: unknown; caller: unknown }> = [];

    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      commandIngress: {
        daemonBootId: async () => "boot_after_restart",
        daemonCommand: async (kind, request, caller) => {
          calls.push({ kind, request, caller });
          if (kind === "identity.register") {
            return { sessionId: "s_rebound_human", agentId: "a_human_fixture" };
          }
          return { messageId: "m_after_rebind", fanout: 1 };
        },
      },
    });

    const res = await dispatch(new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-rebind`,
        "x-nexus-csrf": "csrf-rebind",
      },
      body: JSON.stringify({
        to: { verb: "post", thread: "lens-live-blockers" },
        body: "first post after restart",
      }),
    }));

    expect(res.status).toBe(201);
    expect(calls).toEqual([
      expect.objectContaining({
        kind: "identity.register",
        request: expect.objectContaining({
          name: "test-user",
          clientKey: "rebind_id_1",
          kind: "human",
        }),
      }),
      expect.objectContaining({
        kind: "message.post.send",
        caller: expect.objectContaining({
          sessionId: "s_rebound_human",
          runtimeId: "s_rebound_human",
          clientKey: "rebind_id_1",
          kind: "human",
        }),
      }),
    ]);
    expect(calls[1]!.caller).not.toHaveProperty("agentId");
  });

  it("rejects message sends when there is no cookie", async () => {
    const dispatch = makeDispatch({
      db: async () => db,
      commandIngressDb: () => commandDb,
      authMode: "remote",
    });

    const req = new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "hello" }),
    });

    const res = await dispatch(req);

    expect(res.status).toBe(401);
    expect(spawn).not.toHaveBeenCalled();
  });

  it("authorizes a stable agent-session path by owner agent id and returns its canonical binding", async () => {
    const canonicalDb = await makeCanonicalDb();
    await canonicalDb.batch([
      `INSERT INTO identities VALUES
        ('a_target','current-target','old-owner-name','agent','agent',
         '{"ownerAgentId":"a_owner"}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_target','a_target','s_target','codex','headless',NULL,NULL,NULL,'online',2)`,
    ], "write");
    let idSeq = 0;
    const issued = await issueBearerToken(
      {
        actor: {
          name: "renamed-owner",
          agentId: "a_owner",
          project: "other-project",
          kind: Kind.Agent,
          tier: Tier.Agent,
          credentialFacet: "machine",
          scopes: ["message:read"],
        },
        scopes: ["message:read"],
      },
      {
        db,
        now: () => 2_000_000,
        genId: () => `session_owner_${++idSeq}`,
        randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
      },
    );
    const dispatch = makeDispatch({
      db: async () => db,
      canonicalDb: () => canonicalDb,
      authMode: "remote",
      now: () => 2_000_000,
    });

    const response = await dispatch(new Request(
      "http://localhost/api/v1/agent-sessions/s_target/events?view=agui",
      { headers: { authorization: `Bearer ${issued.accessToken}` } },
    ));

    expect(response.status).toBe(200);
    expect(response.headers.get("x-nexus-session-id")).toBe("s_target");
    expect(response.headers.get("x-nexus-agent-id")).toBe("a_target");
    expect(response.headers.get("x-nexus-agent-name")).toBe("current-target");
    await response.body?.cancel();
    canonicalDb.close();
  });

  it("warms the exact stable agent when an authorized canonical session lane reconnects", async () => {
    const canonicalDb = await makeCanonicalDb();
    await canonicalDb.batch([
      `INSERT INTO identities VALUES
        ('a_target','current-target','old-owner-name','agent','agent',
         '{"ownerAgentId":"a_owner"}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_target','a_target','s_target','codex','headless',NULL,NULL,NULL,'offline',2)`,
    ], "write");
    const submit = vi.fn(async () => ({ warmed: true }));
    const commands: CommandIntentSender = {
      submit: submit as CommandIntentSender["submit"],
    };
    const dispatch = makeDispatch({
      db: async () => db,
      canonicalDb: () => canonicalDb,
      authMode: "local",
      commands,
    });

    const response = await dispatch(new Request(
      "http://localhost/api/v1/agent-sessions/s_target/events?view=agui",
    ));

    expect(response.status).toBe(200);
    await vi.waitFor(() => expect(submit).toHaveBeenCalledOnce());
    expect(submit).toHaveBeenCalledWith(
      "harness.warm",
      { name: "current-target", agentId: "a_target" },
      localOperatorCaller(),
    );
    await response.body?.cancel();
    canonicalDb.close();
  });

  it("does not let a mutable owner-name match override a different stable owner id", async () => {
    const canonicalDb = await makeCanonicalDb();
    await canonicalDb.batch([
      `INSERT INTO identities VALUES
        ('a_target','current-target','old-owner-name','agent','agent',
         '{"ownerAgentId":"a_owner"}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_target','a_target','s_target','codex','headless',NULL,NULL,NULL,'online',2)`,
    ], "write");
    let idSeq = 0;
    const issued = await issueBearerToken(
      {
        actor: {
          name: "old-owner-name",
          agentId: "a_intruder",
          project: "default",
          kind: Kind.Agent,
          tier: Tier.Agent,
          credentialFacet: "machine",
          scopes: ["message:read"],
        },
        scopes: ["message:read"],
      },
      {
        db,
        now: () => 2_000_000,
        genId: () => `session_intruder_${++idSeq}`,
        randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
      },
    );
    const submit = vi.fn(async () => ({ warmed: true }));
    const commands: CommandIntentSender = {
      submit: submit as CommandIntentSender["submit"],
    };
    const dispatch = makeDispatch({
      db: async () => db,
      canonicalDb: () => canonicalDb,
      authMode: "remote",
      now: () => 2_000_000,
      commands,
    });

    const response = await dispatch(new Request(
      "http://localhost/api/v1/agent-sessions/s_target/events?view=agui",
      { headers: { authorization: `Bearer ${issued.accessToken}` } },
    ));

    expect(response.status).toBe(403);
    await expect(response.json()).resolves.toEqual({
      error: { code: "forbidden", message: "agent session owner required" },
    });
    expect(submit).not.toHaveBeenCalled();
    canonicalDb.close();
  });

  it("authorizes stable delegated access from the canonical Gateway grant snapshot", async () => {
    const canonicalDb = await makeCanonicalDb();
    await canonicalDb.batch([
      {
        sql: `INSERT INTO identities VALUES (?, ?, ?, ?, ?, ?, ?)`,
        args: [
          "a_target",
          "current-target",
          "owner",
          "agent",
          "agent",
          JSON.stringify({
            ownerAgentId: "a_owner",
            accessGrants: [{
              principalName: "old-delegate-name",
              principalAgentId: "a_delegate",
              principalProject: "stale-project",
              role: "viewer",
            }],
          }),
          1,
        ],
      },
      `INSERT INTO runtime_descriptors VALUES
        ('r_target','a_target','s_target','codex','headless',NULL,NULL,NULL,'online',2)`,
    ], "write");
    let idSeq = 0;
    const issued = await issueBearerToken(
      {
        actor: {
          name: "renamed-delegate",
          agentId: "a_delegate",
          project: "other-project",
          kind: Kind.Agent,
          tier: Tier.Agent,
          credentialFacet: "machine",
          scopes: ["message:read"],
        },
        scopes: ["message:read"],
      },
      {
        db,
        now: () => 2_000_000,
        genId: () => `session_delegate_${++idSeq}`,
        randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
      },
    );
    const dispatch = makeDispatch({
      db: async () => db,
      canonicalDb: () => canonicalDb,
      authMode: "remote",
      now: () => 2_000_000,
    });

    const response = await dispatch(new Request(
      "http://localhost/api/v1/agent-sessions/s_target/events?view=agui",
      { headers: { authorization: `Bearer ${issued.accessToken}` } },
    ));

    expect(response.status).toBe(200);
    expect(response.headers.get("x-nexus-agent-id")).toBe("a_target");
    await response.body?.cancel();
    canonicalDb.close();
  });

  it("authorizes the exact session id before a newer colliding runtime alias", async () => {
    const canonicalDb = await makeCanonicalDb();
    await canonicalDb.batch([
      `INSERT INTO identities VALUES
        ('a_exact_target','exact-target','old-owner','agent','agent',
         '{"ownerAgentId":"a_owner"}',1),
        ('a_runtime_alias','runtime-alias','alias-owner','agent','agent',
         '{"ownerAgentId":"a_intruder"}',1)`,
      `INSERT INTO runtime_descriptors VALUES
        ('r_exact','a_exact_target','s_collision','codex','headless',NULL,NULL,NULL,'online',2),
        ('s_collision','a_runtime_alias','s_alias','claude','headless',NULL,NULL,NULL,'online',99)`,
    ], "write");
    let idSeq = 0;
    const issued = await issueBearerToken(
      {
        actor: {
          name: "renamed-owner",
          agentId: "a_owner",
          project: "other-project",
          kind: Kind.Agent,
          tier: Tier.Agent,
          credentialFacet: "machine",
          scopes: ["message:read"],
        },
        scopes: ["message:read"],
      },
      {
        db,
        now: () => 2_000_000,
        genId: () => `session_collision_${++idSeq}`,
        randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
      },
    );
    const dispatch = makeDispatch({
      db: async () => db,
      canonicalDb: () => canonicalDb,
      authMode: "remote",
      now: () => 2_000_000,
    });

    const response = await dispatch(new Request(
      "http://localhost/api/v1/agent-sessions/s_collision/events?view=agui",
      { headers: { authorization: `Bearer ${issued.accessToken}` } },
    ));

    expect(response.status).toBe(200);
    expect(response.headers.get("x-nexus-agent-id")).toBe("a_exact_target");
    await response.body?.cancel();
    canonicalDb.close();
  });

  it("accepts valid signed notifications in remote mode without a human caller", async () => {
    const submit = vi.fn(async () => ({
      notifId: "n_machine_auth",
      routedTo: ["ben"],
      hmacOk: true,
    }));
    const commands: CommandIntentSender = {
      submit: submit as CommandIntentSender["submit"],
    };
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      commands,
      notifyHmacSecret: NOTIFY_HMAC_SECRET,
      now: () => NOTIFY_TIMESTAMP,
    });

    const res = await dispatch(signedNotifyRequest());

    expect(res.status).toBe(201);
    expect(submit).toHaveBeenCalledOnce();
    expect(submit).toHaveBeenCalledWith(
      "notification.notify",
      expect.objectContaining({
        rawBody: expect.stringContaining('"source":"ci"'),
        timestamp: String(NOTIFY_TIMESTAMP),
        signature: expect.stringMatching(/^sha256=[0-9a-f]{64}$/),
      }),
      {
        id: "notification:ci",
        name: "ci",
        project: "default",
        kind: Kind.Notification,
        tier: Tier.Agent,
        credentialFacet: "source",
        scopes: ["source:push"],
        sessionId: "notification:ci",
        runtimeId: "notification:ci",
      },
      {
        idempotencyKey: expect.stringMatching(
          /^notify:1784000000000:[0-9a-f]{64}$/,
        ),
      },
    );
  });

  it("rejects invalid, stale, and disabled signed notification ingress before command submit", async () => {
    const cases: Array<{
      name: string;
      request: Request;
      secret: string;
      now: number;
      status: number;
      message: string;
    }> = [
      {
        name: "invalid signature",
        request: signedNotifyRequest({ signature: `sha256=${"0".repeat(64)}` }),
        secret: NOTIFY_HMAC_SECRET,
        now: NOTIFY_TIMESTAMP,
        status: 401,
        message: "invalid notification signature",
      },
      {
        name: "stale timestamp",
        request: signedNotifyRequest(),
        secret: NOTIFY_HMAC_SECRET,
        now: NOTIFY_TIMESTAMP + 300_001,
        status: 401,
        message: "notification timestamp is outside the freshness window",
      },
      {
        name: "missing secret",
        request: signedNotifyRequest(),
        secret: "",
        now: NOTIFY_TIMESTAMP,
        status: 503,
        message: "notification HMAC secret is not configured",
      },
    ];

    for (const testCase of cases) {
      const submit = vi.fn(async () => ({ ok: true }));
      const commands: CommandIntentSender = {
        submit: submit as CommandIntentSender["submit"],
      };
      const dispatch = makeDispatch({
        db: async () => db,
        authMode: "remote",
        commands,
        notifyHmacSecret: testCase.secret,
        now: () => testCase.now,
      });

      const res = await dispatch(testCase.request);

      expect(res.status, testCase.name).toBe(testCase.status);
      await expect(res.json(), testCase.name).resolves.toMatchObject({
        error: { message: testCase.message },
      });
      expect(submit, testCase.name).not.toHaveBeenCalled();
    }
  });

  it("keeps neighboring message and source-management routes auth and scope gated", async () => {
    const submit = vi.fn(async () => ({ ok: true }));
    const commands: CommandIntentSender = {
      submit: submit as CommandIntentSender["submit"],
    };
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      commands,
      now: () => 2_000_000,
    });

    const unauthenticatedMessage = await dispatch(new Request(
      "http://localhost/api/v1/messages",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          to: { verb: "post", thread: "backend" },
          body: "blocked",
        }),
      },
    ));
    const unauthenticatedSource = await dispatch(new Request(
      "http://localhost/api/v1/sources",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "blocked" }),
      },
    ));

    expect(unauthenticatedMessage.status).toBe(401);
    expect(unauthenticatedSource.status).toBe(401);

    let idSeq = 0;
    const issued = await issueBearerToken(
      {
        actor: {
          name: "source-producer",
          project: "default",
          kind: Kind.Agent,
          tier: Tier.Agent,
          credentialFacet: "machine",
          scopes: ["source:push"],
        },
        scopes: ["source:push"],
      },
      {
        db,
        now: () => 2_000_000,
        genId: () => `notify_adjacent_${++idSeq}`,
        randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
      },
    );
    const scopedSourceManagement = await dispatch(new Request(
      "http://localhost/api/v1/sources",
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${issued.accessToken}`,
        },
        body: JSON.stringify({ name: "blocked" }),
      },
    ));

    expect(scopedSourceManagement.status).toBe(403);
    expect(submit).not.toHaveBeenCalled();
  });

  it("rejects message sends for an unknown/expired cookie token", async () => {
    const dispatch = makeDispatch({
      db: async () => db,
      commandIngressDb: () => commandDb,
      authMode: "remote",
    });

    const req = new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: "nexus_human=not_a_real_token; nexus_csrf=csrf-expired",
        "x-nexus-csrf": "csrf-expired",
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "hello" }),
    });

    const res = await dispatch(req);

    expect(res.status).toBe(401);
    expect(spawn).not.toHaveBeenCalled();
  });

  it("rejects a presented remote human cookie before auth when CSRF proof is absent", async () => {
    const dispatch = makeDispatch({
      db: async () => db,
      commandIngressDb: () => commandDb,
      authMode: "remote",
    });

    const res = await dispatch(new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: "nexus_human=not_a_real_token",
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "hello" }),
    }));

    expect(res.status).toBe(403);
    expect(spawn).not.toHaveBeenCalled();
  });

  it("allows local-mode message sends without a login and stamps the local operator", async () => {
    const dispatch = makeDispatch({
      db: async () => db,
      commandIngressDb: () => commandDb,
      commandIngress: { pollIntervalMs: 1, timeoutMs: 500 },
      authMode: "local",
    });

    const req = new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: "nexus_human=stale-local-cookie",
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "local hello" }),
    });

    const pending = dispatch(req);
    const row = await waitForCommand(commandDb);
    await completeCommand(commandDb, String(row.command_id));
    const res = await pending;
    const local = localOperatorCaller();

    expect(res.status).toBe(201);
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

  it("remote mode requires login even when request headers claim localhost", async () => {
    const dispatch = makeDispatch({
      db: async () => db,
      commandIngressDb: () => commandDb,
      authMode: "remote",
    });

    const req = new Request("http://localhost/api/v1/threads", {
      method: "GET",
      headers: {
        host: "localhost:5173",
        "x-forwarded-for": "127.0.0.1",
      },
    });

    const res = await dispatch(req);

    expect(res.status).toBe(401);
  });

  it("remote cookie-backed mutations require CSRF before reaching Message Post", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "carol", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_csrf_${++idSeq}`,
      now: () => 1_000_000,
    });
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_should_not_write", fanout: 1 })),
    };
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      messagePost,
    });

    const res = await dispatch(new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}`,
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "csrf gap" }),
    }));

    expect(res.status).toBe(403);
    expect(messagePost.send).not.toHaveBeenCalled();
  });

  it("issues scoped REST bearer tokens and uses them as machine Principals without CSRF", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "machine-owner", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_bearer_${++idSeq}`,
      now: () => 1_000_000,
    });
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_bearer_send", fanout: 1 })),
    };
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      messagePost,
      now: () => 2_000_000,
      genId: () => `bearer_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });

    const issue = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-1`,
        "x-nexus-csrf": "csrf-1",
      },
      body: JSON.stringify({ scopes: ["message:send"], ttlMs: 60_000 }),
    }));

    expect(issue.status).toBe(201);
    const issued = await issue.json() as { accessToken: string; tokenId: string };
    expect(issued.accessToken).toMatch(/^nx_at_/);

    const send = await dispatch(new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${issued.accessToken}`,
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "bearer send" }),
    }));

    expect(send.status).toBe(201);
    expect(messagePost.send).toHaveBeenCalledWith(
      { to: { verb: "post", thread: "backend" }, body: "bearer send" },
      expect.objectContaining({
        name: "machine-owner",
        credentialFacet: "machine",
        scopes: ["message:send"],
        tokenId: issued.tokenId,
      }),
      undefined,
    );

    const cookieConfused = await dispatch(new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${issued.accessToken}`,
        cookie: "nexus_human=stale-cookie; nexus_csrf=stale-csrf",
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "blocked" }),
    }));
    expect(cookieConfused.status).toBe(403);
    expect(messagePost.send).toHaveBeenCalledTimes(1);
  });

  it("denies REST bearer calls that lack the route scope before writing", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "read-only-owner", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_scope_${++idSeq}`,
      now: () => 1_000_000,
    });
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_should_not_write", fanout: 1 })),
    };
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      messagePost,
      now: () => 2_000_000,
      genId: () => `scope_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });

    const issue = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-2`,
        "x-nexus-csrf": "csrf-2",
      },
      body: JSON.stringify({ scopes: ["message:read"] }),
    }));
    const issued = await issue.json() as { accessToken: string };

    const send = await dispatch(new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${issued.accessToken}`,
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "blocked" }),
    }));

    expect(send.status).toBe(403);
    expect(messagePost.send).not.toHaveBeenCalled();
  });

  it("denies admin routes when a bearer has admin scope but only agent tier", async () => {
    let idSeq = 0;
    const issued = await issueBearerToken(
      {
        actor: {
          name: "agent-scope-owner",
          project: "default",
          kind: Kind.Agent,
          tier: Tier.Agent,
          credentialFacet: "machine",
          scopes: ["admin:*"],
        },
        scopes: ["admin:*"],
      },
      {
        db,
        now: () => 2_000_000,
        genId: () => `tier_${++idSeq}`,
        randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
      },
    );
    const commands: CommandIntentSender = {
      submit: vi.fn(async () => ({ ok: true })) as CommandIntentSender["submit"],
    };
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      commands,
      now: () => 2_000_000,
    });

    const res = await dispatch(new Request("http://localhost/api/v1/agents/alice", {
      method: "DELETE",
      headers: { authorization: `Bearer ${issued.accessToken}` },
    }));

    expect(res.status).toBe(403);
    await expect(res.json()).resolves.toEqual({
      error: { code: "forbidden", message: "requires admin tier" },
    });
    expect(commands.submit).not.toHaveBeenCalled();
  });

  it("denies bearer-token issuance when the caller scope is allowed but tier is not admin", async () => {
    let idSeq = 0;
    const issued = await issueBearerToken(
      {
        actor: {
          name: "agent-token-owner",
          project: "default",
          kind: Kind.Agent,
          tier: Tier.Agent,
          credentialFacet: "machine",
          scopes: ["message:send", "admin:*"],
        },
        scopes: ["message:send", "admin:*"],
      },
      {
        db,
        now: () => 2_000_000,
        genId: () => `issue_tier_${++idSeq}`,
        randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
      },
    );
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      now: () => 2_000_000,
    });

    const res = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${issued.accessToken}`,
      },
      body: JSON.stringify({ scopes: ["message:send"] }),
    }));

    expect(res.status).toBe(403);
    await expect(res.json()).resolves.toEqual({
      error: { code: "forbidden", message: "requires admin tier" },
    });
  });

  it("applies REST parity scopes to durable agent read routes", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "agent-reader-owner", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_agent_read_${++idSeq}`,
      now: () => 1_000_000,
    });
    const readDb = await seedDb();
    const dispatch = makeDispatch({
      db: async () => db,
      readDb: async () => readDb,
      authMode: "remote",
      now: () => 2_000_000,
      genId: () => `agent_read_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });

    const issueRead = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-agent-read`,
        "x-nexus-csrf": "csrf-agent-read",
      },
      body: JSON.stringify({ scopes: ["agent:read"] }),
    }));
    const readBearer = await issueRead.json() as { accessToken: string };

    const allowed = await dispatch(new Request("http://localhost/api/v1/agents/ben?project=nexus", {
      headers: { authorization: `Bearer ${readBearer.accessToken}` },
    }));
    expect(allowed.status).toBe(200);
    expect(await allowed.json()).toMatchObject({
      agent: { agentId: "a_ben", name: "ben" },
    });

    const issueMessageRead = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-agent-read`,
        "x-nexus-csrf": "csrf-agent-read",
      },
      body: JSON.stringify({ scopes: ["message:read"] }),
    }));
    const messageReadBearer = await issueMessageRead.json() as { accessToken: string };
    const denied = await dispatch(new Request("http://localhost/api/v1/agents/ben?project=nexus", {
      headers: { authorization: `Bearer ${messageReadBearer.accessToken}` },
    }));

    expect(denied.status).toBe(403);
  });

  it("requires agent:attach for terminal attach before reaching the router", async () => {
    let idSeq = 0;
    const issued = await issueBearerToken(
      {
        actor: {
          name: "terminal-reader",
          project: "default",
          kind: Kind.Agent,
          tier: Tier.Agent,
          credentialFacet: "machine",
          scopes: ["agent:read"],
        },
        scopes: ["agent:read"],
      },
      {
        db,
        now: () => 2_000_000,
        genId: () => `terminal_${++idSeq}`,
        randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
      },
    );
    const readDb = vi.fn(async () => seedDb());
    const dispatch = makeDispatch({
      db: async () => db,
      readDb,
      authMode: "remote",
      now: () => 2_000_000,
    });

    const res = await dispatch(new Request("http://localhost/api/v1/agents/ben/terminal", {
      headers: { authorization: `Bearer ${issued.accessToken}` },
    }));

    expect(res.status).toBe(403);
    await expect(res.json()).resolves.toEqual({
      error: { code: "forbidden", message: `missing required scope: ${AGENT_ATTACH_SCOPE}` },
    });
    expect(readDb).not.toHaveBeenCalled();
  });

  it("applies REST parity scopes before new write routes dispatch", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "route-scope-owner", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_route_scope_${++idSeq}`,
      now: () => 1_000_000,
    });
    const { commands, calls } = makeCommandCapture();
    const dispatch = makeDispatch({
      db: async () => db,
      commands,
      authMode: "remote",
      now: () => 2_000_000,
      genId: () => `route_scope_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });

    async function issue(scopes: string[]): Promise<string> {
      const res = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-route-scope`,
          "x-nexus-csrf": "csrf-route-scope",
        },
        body: JSON.stringify({ scopes }),
      }));
      expect(res.status).toBe(201);
      const issued = await res.json() as { accessToken: string };
      return issued.accessToken;
    }

    const agentAdmin = await issue(["agent:admin"]);
    const credential = await dispatch(new Request("http://localhost/api/v1/agents/ben/credentials", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${agentAdmin}`,
      },
      body: JSON.stringify({ label: "rest parity" }),
    }));
    expect(credential.status).toBe(201);

    const runtimeRegister = await issue(["runtime:register"]);
    const register = await dispatch(new Request("http://localhost/api/v1/register", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${runtimeRegister}`,
      },
      body: JSON.stringify({
        agentId: "a_ben",
        name: "ben",
        harness: "claude",
        harnessSessionId: "hs_ben",
        project: "nexus",
        clientKey: "ck_ben",
        runtimeCredential: "secret",
        tier: "agent",
      }),
    }));
    expect(register.status).toBe(201);

    const messageSend = await issue(["message:send"]);
    const rename = await dispatch(new Request("http://localhost/api/v1/rename", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${messageSend}`,
      },
      body: JSON.stringify({ name: "new-name" }),
    }));
    expect(rename.status).toBe(200);

    const threadWrite = await issue(["thread:write"]);
    const archive = await dispatch(new Request("http://localhost/api/v1/threads/design/archive", {
      method: "POST",
      headers: {
        authorization: `Bearer ${threadWrite}`,
      },
    }));
    expect(archive.status).toBe(200);

    const removeThread = await dispatch(new Request("http://localhost/api/v1/threads/design", {
      method: "DELETE",
      headers: {
        authorization: `Bearer ${threadWrite}`,
      },
    }));
    expect(removeThread.status).toBe(200);

    const messageRead = await issue(["message:read"]);
    const deniedCredential = await dispatch(new Request("http://localhost/api/v1/agents/ben/credentials", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${messageRead}`,
      },
      body: JSON.stringify({ label: "blocked" }),
    }));

    expect(deniedCredential.status).toBe(403);
    expect(calls.map((call) => call.kind)).toEqual([
      "agent.credential.create",
      "identity.register",
      "identity.rename",
      "thread.archive",
      "thread.delete",
    ]);
  });

  it("allows authenticated metadata writes without a route scope", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "metadata-owner", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_metadata_${++idSeq}`,
      now: () => 1_000_000,
    });
    const { commands, calls } = makeCommandCapture();
    const dispatch = makeDispatch({
      db: async () => db,
      commands,
      authMode: "remote",
      now: () => 2_000_000,
      genId: () => `metadata_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });

    const issuedRes = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-metadata`,
        "x-nexus-csrf": "csrf-metadata",
      },
      body: JSON.stringify({ scopes: ["source:push"] }),
    }));
    expect(issuedRes.status).toBe(201);
    const issued = await issuedRes.json() as { accessToken: string };

    const patch = await dispatch(new Request("http://localhost/api/v1/messages/m_seed_1/metadata", {
      method: "PATCH",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${issued.accessToken}`,
      },
      body: JSON.stringify({ metadata: { tool: "qa" } }),
    }));

    expect(patch.status).toBe(200);
    expect(calls).toEqual([
      {
        kind: "metadata.set",
        request: { entity: "message", id: "m_seed_1", metadata: { tool: "qa" } },
      },
    ]);

    const unauth = await dispatch(new Request("http://localhost/api/v1/messages/m_seed_1/metadata", {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ metadata: { tool: "qa" } }),
    }));
    expect(unauth.status).toBe(401);
  });

  it("expires REST bearer access tokens by expiresAt", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "expiring-owner", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_expire_${++idSeq}`,
      now: () => 1_000_000,
    });
    let now = 2_000_000;
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      now: () => now,
      genId: () => `expire_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });

    const issue = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-expire`,
        "x-nexus-csrf": "csrf-expire",
      },
      body: JSON.stringify({ scopes: ["message:read"], ttlMs: 1_000 }),
    }));
    const issued = await issue.json() as { accessToken: string };

    now += 1_001;
    const read = await dispatch(new Request("http://localhost/api/v1/threads", {
      headers: { authorization: `Bearer ${issued.accessToken}` },
    }));

    expect(read.status).toBe(401);
  });

  it("refresh rotates bearer tokens and revocation blocks the next bearer call", async () => {
    const { commands: seedCommands } = makeCommandCapture();
    let idSeq = 0;
    const { cookieToken } = await registerHuman({ name: "rotating-owner", password: "pw" }, {
      db,
      commands: seedCommands,
      genId: () => `id_rotate_${++idSeq}`,
      now: () => 1_000_000,
    });
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_rotated", fanout: 1 })),
    };
    let now = 2_000_000;
    const dispatch = makeDispatch({
      db: async () => db,
      authMode: "remote",
      messagePost,
      now: () => now,
      genId: () => `rotate_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });

    const issue = await dispatch(new Request("http://localhost/api/v1/auth/tokens", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-3`,
        "x-nexus-csrf": "csrf-3",
      },
      body: JSON.stringify({ scopes: ["message:send"], ttlMs: 60_000 }),
    }));
    const first = await issue.json() as {
      accessToken: string;
      refreshToken: string;
      tokenId: string;
    };

    now += 1_000;
    const refreshed = await dispatch(new Request("http://localhost/api/v1/auth/tokens/refresh", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ refreshToken: first.refreshToken }),
    }));

    expect(refreshed.status).toBe(201);
    const second = await refreshed.json() as { accessToken: string; tokenId: string };
    expect(second.accessToken).not.toBe(first.accessToken);

    const oldAccess = await dispatch(new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${first.accessToken}`,
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "old" }),
    }));
    expect(oldAccess.status).toBe(401);

    const revoke = await dispatch(new Request(`http://localhost/api/v1/auth/tokens/${second.tokenId}`, {
      method: "DELETE",
      headers: {
        cookie: `nexus_human=${cookieToken}; nexus_csrf=csrf-3`,
        "x-nexus-csrf": "csrf-3",
      },
    }));
    expect(revoke.status).toBe(200);

    const afterRevoke = await dispatch(new Request("http://localhost/api/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${second.accessToken}`,
      },
      body: JSON.stringify({ to: { verb: "post", thread: "backend" }, body: "revoked" }),
    }));
    expect(afterRevoke.status).toBe(401);
  });
});
