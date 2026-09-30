// TDD for the human session backend.
// Uses an in-memory libSQL DB and a mock command sender to verify
// registerHuman enqueues the correct daemon payload + persists a session row,
// and currentHuman reads it back (or returns null for an unknown token).
import { describe, it, expect, vi, beforeEach } from "vitest";
import { createClient, type Client } from "@libsql/client";

import { initSchema } from "@server/conversation/store";
import {
  registerHuman,
  currentHuman,
  type HumanDeps,
} from "./human";
import type { CommandIntentSender } from "@server/api/http";

// ── helpers ─────────────────────────────────────────────────────────────────

function makeDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  return initSchema(db).then(() => db);
}

function makeCommandMock(): {
  commands: CommandIntentSender;
  calls: Array<{ kind: string; request: unknown; caller: unknown }>;
} {
  const calls: Array<{ kind: string; request: unknown; caller: unknown }> = [];
  const commands: CommandIntentSender = {
    submit: vi.fn(async (kind, request, caller) => {
      calls.push({ kind, request, caller });
      // Return a minimal RegisterResponse shape.
      return { sessionId: "sess_mock_1", name: (request as { name?: string })?.name ?? "unknown", token: "t" };
    }) as CommandIntentSender["submit"],
  };
  return { commands, calls };
}

function makeDeps(
  db: Client,
  commands: CommandIntentSender,
  overrides?: Partial<HumanDeps>,
): HumanDeps {
  return {
    db,
    commands,
    genId: vi.fn().mockReturnValueOnce("ck_test_001").mockReturnValueOnce("tok_test_abc"),
    now: vi.fn().mockReturnValue(1_000_000),
    ...overrides,
  };
}

// ── tests ────────────────────────────────────────────────────────────────────

describe("registerHuman + currentHuman", () => {
  let db: Client;

  beforeEach(async () => {
    db = await makeDb();
  });

  it("calls the daemon register with a human-shaped payload", async () => {
    const { commands, calls } = makeCommandMock();
    const deps = makeDeps(db, commands);

    await registerHuman({ name: "alex", password: "pw" }, deps);

    expect(calls).toHaveLength(1);
    const { kind, request } = calls[0]!;
    expect(kind).toBe("identity.register");
    expect(request).toMatchObject({
      name: "alex",
      harness: "other",
      tier: "admin",
      kind: "human",
      project: "default",
    });
    // harnessSessionId is derived from clientKey
    expect((request as { harnessSessionId: string }).harnessSessionId).toMatch(/^hs_/);
    expect((request as { clientKey: string }).clientKey).toMatch(/^ck_/);
  });

  it("persists a human_session row and returns cookieToken", async () => {
    const { commands } = makeCommandMock();
    const deps = makeDeps(db, commands);

    const result = await registerHuman({ name: "alex", password: "pw" }, deps);

    expect(result.cookieToken).toBeTruthy();
    expect(result.name).toBe("alex");
    expect(result.project).toBe("default");
    expect(result.sessionId).toBeTruthy();
  });

  it("currentHuman returns the persisted row for a known token", async () => {
    const { commands } = makeCommandMock();
    const deps = makeDeps(db, commands);

    const { cookieToken } = await registerHuman({ name: "alex", password: "pw" }, deps);

    const human = await currentHuman(cookieToken, { db });
    expect(human).not.toBeNull();
    expect(human?.name).toBe("alex");
    expect(human?.project).toBe("default");
    expect(human?.sessionId).toBeTruthy();
  });

  it("currentHuman returns null for an unknown token", async () => {
    const result = await currentHuman("nope_not_a_token", { db });
    expect(result).toBeNull();
  });

  it("throws (does NOT persist) when the daemon register returns no sessionId", async () => {
    const calls: Array<{ kind: string; request: unknown }> = [];
    const commands: CommandIntentSender = {
      submit: vi.fn(async (kind, request) => {
        calls.push({ kind, request });
        return { name: (request as { name?: string })?.name ?? "x", token: "t" }; // no sessionId → did not bind
      }) as CommandIntentSender["submit"],
    };
    await expect(registerHuman({ name: "alex", password: "pw" }, makeDeps(db, commands))).rejects.toThrow(/sessionId/);
    // nothing was persisted
    const rows = await db.execute("SELECT COUNT(*) AS n FROM human_session");
    expect(Number(rows.rows[0]!.n)).toBe(0);
    const users = await db.execute("SELECT COUNT(*) AS n FROM human_user");
    expect(Number(users.rows[0]!.n)).toBe(0);
    const principals = await db.execute("SELECT COUNT(*) AS n FROM principals");
    expect(Number(principals.rows[0]!.n)).toBe(0);
  });

  it("uses injected genId and now (no Date.now / Math.random in unit)", async () => {
    const { commands, calls } = makeCommandMock();
    const genId = vi.fn()
      .mockReturnValueOnce("ck_deterministic")
      .mockReturnValueOnce("tok_deterministic");
    const now = vi.fn().mockReturnValue(42_000);

    await registerHuman({ name: "blake", password: "pw" }, makeDeps(db, commands, { genId, now }));

    // genId was called (at least twice: clientKey + cookieToken)
    expect(genId).toHaveBeenCalled();
    // now was called to set created_at
    expect(now).toHaveBeenCalled();
    // The clientKey sent to daemon matches what genId returned first
    expect((calls[0]!.request as { clientKey: string }).clientKey).toBe("ck_deterministic");
  });

  it("reuses the stable clientKey for later logins with the same password", async () => {
    const { commands, calls } = makeCommandMock();
    let seq = 0;
    const deps = makeDeps(db, commands, {
      genId: vi.fn(() => `id_${++seq}`),
      now: vi.fn().mockReturnValue(1_000_000),
    });

    const first = await registerHuman({ name: "alex", password: "pw" }, deps);
    const second = await registerHuman({ name: "ALEX", password: "pw" }, deps);

    expect(first.cookieToken).not.toBe(second.cookieToken);
    expect(calls).toHaveLength(2);
    const firstKey = (calls[0]!.request as { clientKey: string }).clientKey;
    const secondKey = (calls[1]!.request as { clientKey: string }).clientKey;
    expect(secondKey).toBe(firstKey);
  });

  it("keeps one immutable human and principal identity across login and rename", async () => {
    const { commands, calls } = makeCommandMock();
    let seq = 0;
    const deps = makeDeps(db, commands, {
      genId: vi.fn(() => `id_${++seq}`),
      now: vi.fn().mockReturnValue(1_000_000),
    });

    const first = await registerHuman({ name: "alex", password: "pw" }, deps);
    const firstIdentity = await currentHuman(first.cookieToken, { db });
    expect(firstIdentity?.humanUserId).toMatch(/^hu_[a-f0-9]{24}$/);
    expect(firstIdentity?.principalId).toMatch(/^h_[a-f0-9]{24}$/);
    expect(calls[0]?.caller).toMatchObject({ principalId: firstIdentity?.principalId });

    await db.execute({
      sql: "UPDATE human_user SET name_key = ?, name = ? WHERE human_user_id = ?",
      args: ["alex-renamed", "Alex Renamed", firstIdentity!.humanUserId],
    });
    const second = await registerHuman({ name: "alex-renamed", password: "pw" }, deps);
    const secondIdentity = await currentHuman(second.cookieToken, { db });

    expect(secondIdentity?.humanUserId).toBe(firstIdentity?.humanUserId);
    expect(secondIdentity?.principalId).toBe(firstIdentity?.principalId);
    const principals = await db.execute("SELECT COUNT(*) AS n FROM principals");
    expect(Number(principals.rows[0]!.n)).toBe(1);
  });

  it("rejects a wrong password without registering a daemon session", async () => {
    const { commands, calls } = makeCommandMock();
    const deps = makeDeps(db, commands, {
      genId: vi.fn()
        .mockReturnValueOnce("ck_first")
        .mockReturnValueOnce("tok_first")
        .mockReturnValueOnce("tok_second"),
    });

    await registerHuman({ name: "alex", password: "pw" }, deps);
    await expect(
      registerHuman({ name: "alex", password: "wrong" }, deps),
    ).rejects.toThrow(/invalid name or password/);

    expect(calls).toHaveLength(1);
  });

});
