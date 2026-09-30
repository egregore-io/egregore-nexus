// TDD for POST /api/login.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { createClient, type Client } from "@libsql/client";

import { initSchema } from "@server/conversation/store";
import type { CommandIntentSender } from "@server/api/http";
import { localOperatorCaller } from "@server/auth/webAuthMode";

// ── helpers ──────────────────────────────────────────────────────────────────

async function makeDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await initSchema(db);
  return db;
}

function makeCommandMock(): CommandIntentSender {
  return {
    submit: vi.fn(async (_kind, request) => ({
      sessionId: "sess_mock_1",
      name: (request as { name?: string }).name ?? "unknown",
      token: "t",
    })) as CommandIntentSender["submit"],
  };
}

// ── We test the handler logic directly, not via HTTP.
// The handler factory is exported for DI; the Route export is framework glue.

import { makeLoginHandler, makeGetLoginHandler } from "./login";
import { currentHuman, registerHuman } from "@server/identity/human";

describe("POST /api/login", () => {
  let db: Client;
  let commands: CommandIntentSender;

  beforeEach(async () => {
    db = await makeDb();
    commands = makeCommandMock();
  });

  it("registers and returns Set-Cookie with the token", async () => {
    let idSeq = 0;
    const handler = makeLoginHandler({
      db: async () => db,
      commands,
      genId: () => `id_${++idSeq}`,
      now: () => 1_000_000,
    });

    const req = new Request("http://localhost/api/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "alex", password: "pw" }),
    });

    const res = await handler(req);
    expect(res.status).toBe(200);

    const body = (await res.json()) as { ok: boolean; csrfToken: string };
    expect(body.ok).toBe(true);
    expect(body.csrfToken).toMatch(/^id_/);

    const cookie = res.headers.get("set-cookie");
    expect(cookie).not.toBeNull();
    expect(cookie).toMatch(/nexus_human=/);
    expect(cookie).toMatch(/nexus_csrf=/);
    expect(cookie).toMatch(/HttpOnly/i);
    expect(cookie).toMatch(/SameSite=Lax/i);
    expect(cookie).toMatch(/Path=\//i);
  });

  it("returns 400 on missing name", async () => {
    const handler = makeLoginHandler({
      db: async () => db,
      commands,
      genId: () => "x",
      now: () => 0,
    });

    const req = new Request("http://localhost/api/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });

    const res = await handler(req);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/name/i);
  });

  it("returns 400 on missing password", async () => {
    const handler = makeLoginHandler({
      db: async () => db,
      commands,
      genId: () => "x",
      now: () => 0,
    });

    const req = new Request("http://localhost/api/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "alex" }),
    });

    const res = await handler(req);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/password/i);
  });

  it("returns 401 when an existing account password is wrong", async () => {
    let idSeq = 0;
    const handler = makeLoginHandler({
      db: async () => db,
      commands,
      genId: () => `id_${++idSeq}`,
      now: () => 1_000_000,
    });

    await handler(new Request("http://localhost/api/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "alex", password: "pw" }),
    }));

    const res = await handler(new Request("http://localhost/api/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "alex", password: "wrong" }),
    }));

    expect(res.status).toBe(401);
  });

  it("keeps the immutable human and principal ids across repeated login and rename", async () => {
    let idSeq = 0;
    const handler = makeLoginHandler({
      db: async () => db,
      commands,
      genId: () => `id_${++idSeq}`,
      now: () => 1_000_000,
    });
    const login = (name: string) => handler(new Request("http://localhost/api/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name, password: "pw" }),
    }));

    expect((await login("alex")).status).toBe(200);
    const account = (await db.execute(
      "SELECT human_user_id FROM human_user WHERE name_key = 'alex'",
    )).rows[0]!;
    await db.execute({
      sql: "UPDATE human_user SET name_key = ?, name = ? WHERE human_user_id = ?",
      args: ["alex-renamed", "Alex Renamed", String(account.human_user_id)],
    });
    expect((await login("alex-renamed")).status).toBe(200);

    const sessions = await db.execute(
      `SELECT human_user_id, principal_id FROM human_session
       ORDER BY cookie_token`,
    );
    expect(sessions.rows).toHaveLength(2);
    expect(new Set(sessions.rows.map((row) => String(row.human_user_id)))).toEqual(
      new Set([String(account.human_user_id)]),
    );
    expect(new Set(sessions.rows.map((row) => String(row.principal_id))).size).toBe(1);
    const principals = await db.execute("SELECT COUNT(*) AS n FROM principals");
    expect(Number(principals.rows[0]!.n)).toBe(1);
  });

  it("returns 400 on blank name", async () => {
    const handler = makeLoginHandler({
      db: async () => db,
      commands,
      genId: () => "x",
      now: () => 0,
    });

    const req = new Request("http://localhost/api/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "   " }),
    });

    const res = await handler(req);
    expect(res.status).toBe(400);
  });

  it("returns 400 on non-JSON body", async () => {
    const handler = makeLoginHandler({
      db: async () => db,
      commands,
      genId: () => "x",
      now: () => 0,
    });

    const req = new Request("http://localhost/api/login", {
      method: "POST",
      body: "not json",
    });

    const res = await handler(req);
    expect(res.status).toBe(400);
  });
});

describe("GET /api/login — session check", () => {
  let db: Client;
  let commands: CommandIntentSender;

  beforeEach(async () => {
    db = await makeDb();
    commands = makeCommandMock();
  });

  it("returns 200 with name+project when cookie is valid", async () => {
    // Seed a session
    let seq = 0;
    const humanDeps = {
      db,
      commands,
      genId: () => `id_get_${++seq}`,
      now: () => 1_000_000,
    };
    const { cookieToken } = await registerHuman({ name: "blake", password: "pw" }, humanDeps);

    const handler = makeGetLoginHandler({ db: async () => db });
    const req = new Request("http://localhost/api/login", {
      headers: { cookie: `nexus_human=${cookieToken}` },
    });

    const res = await handler(req);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { name: string; project: string };
    expect(body.name).toBe("blake");
    expect(body.project).toBe("default");
  });

  it("returns 401 in remote mode when no cookie is present", async () => {
    const handler = makeGetLoginHandler({ db: async () => db, authMode: "remote" });
    const req = new Request("http://localhost/api/login");
    const res = await handler(req);
    expect(res.status).toBe(401);
  });

  it("returns 401 in remote mode when cookie token is unknown", async () => {
    const handler = makeGetLoginHandler({ db: async () => db, authMode: "remote" });
    const req = new Request("http://localhost/api/login", {
      headers: { cookie: "nexus_human=bogus_token" },
    });
    const res = await handler(req);
    expect(res.status).toBe(401);
  });

  it("returns the local operator in local mode without requiring a cookie", async () => {
    const handler = makeGetLoginHandler({ db: async () => db, authMode: "local" });
    const req = new Request("http://localhost/api/login");

    const res = await handler(req);

    expect(res.status).toBe(200);
    const body = (await res.json()) as { name: string; project: string; mode: string };
    expect(body).toEqual({
      name: localOperatorCaller().name,
      project: "default",
      mode: "local",
    });
  });

  it("resolves a human login as a Principal with facet, tier, and scopes", async () => {
    let seq = 0;
    const { cookieToken } = await registerHuman(
      { name: "principal-human", password: "pw" },
      {
        db,
        commands,
        genId: () => `id_principal_${++seq}`,
        now: () => 1_000_000,
      },
    );

    const principal = await currentHuman(cookieToken, { db });

    expect(principal).toMatchObject({
      name: "principal-human",
      project: "default",
      kind: "human",
      tier: "admin",
      credentialFacet: "human",
      scopes: expect.arrayContaining(["message:send", "message:read"]),
    });
  });
});
