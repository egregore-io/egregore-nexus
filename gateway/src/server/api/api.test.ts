// Task 9 — public HTTP API tests.
//
// Drives the framework-agnostic router directly with command-ingress and a
// seeded read-view. This is the load-bearing, framework-independent surface;
// the TanStack catch-all route is a thin adapter over exactly this function.
//
// The contract under test (per the plan + the OVERRIDE):
//   - migrated ingest/ops (POST/DELETE) → daemon `command_intents`;
//   - reads (GET) → the read-view (`queries.ts` over `db`), NOT the daemon;
//   - Principal auth at the edge (401), Zod body validation (400),
//     GatewayError → status, unknown route → 404;
//   - handlers never reach a DB write path.
import { createHmac } from "node:crypto";
import { describe, it, expect, beforeEach, vi } from "vitest";

import {
  handle,
  restCapabilityRoutes,
  type ApiRequest,
  type ApiDeps,
} from "./router";
import {
  GatewayError,
  type CommandIntentSender,
  type GatewayCallerIdentity,
  type HookDiagnosticsReader,
  type MessagePostSender,
} from "./http";
import { seedDb } from "@drizzle/__mocks__/seedDb";
import type { ReadDb } from "@drizzle/client";
import { COMMAND_KINDS } from "@server/command/ingress";
import {
  createHumanReadDeliveryMarker,
  type HumanReadDeliveryMarker,
} from "@server/delivery/humanRead";
import { Kind, Locality, Tier } from "@shared/types";

const API_KEY = "test-secret-key";
const NOTIFY_HMAC_SECRET = "notify-test-secret";
const API_CALLER_SESSION_ID = "s_api_test";
const API_CALLER: GatewayCallerIdentity = {
  name: "api-test",
  project: "nexus",
  sessionId: API_CALLER_SESSION_ID,
  runtimeId: API_CALLER_SESSION_ID,
  clientKey: "ck_api_test",
  kind: Kind.Human,
  tier: Tier.Admin,
  credentialFacet: "human",
  scopes: ["admin:*"],
};

interface CommandSpy extends CommandIntentSender {
  calls: Array<{ kind: string; req: unknown; caller: unknown; opts?: unknown }>;
}

type CommandResultFactory = (
  kind: string,
  req: unknown,
  caller: unknown,
) => unknown | Promise<unknown>;

function commandReq(req: unknown): Record<string, unknown> {
  return req && typeof req === "object" ? (req as Record<string, unknown>) : {};
}

function defaultCommandResult(kind: string, req: unknown): unknown {
  const body = commandReq(req);
  switch (kind) {
    case COMMAND_KINDS.messagePostSend:
      return { messageId: "m_command", fanout: 1 };
    case COMMAND_KINDS.metadataSet:
      return { entity: body.entity, id: body.id, metadata: body.metadata };
    case COMMAND_KINDS.notificationNotify:
      return { notifId: "n_command", routedTo: ["ben"], hmacOk: true };
    case COMMAND_KINDS.notificationSend:
      return { messageId: "m_notification", fanout: 1 };
    case COMMAND_KINDS.threadArchive:
      return { name: body.name, archived: true };
    case COMMAND_KINDS.threadDelete:
      return { name: body.name, deleted: true };
    case COMMAND_KINDS.threadRename:
      return { name: body.newName, previous: body.name };
    case COMMAND_KINDS.adminSpawn:
      return { sessionId: "s_spawn", name: body.name ?? "worker", status: "spawned" };
    case COMMAND_KINDS.adminRemove:
      return { name: body.name, status: body.kill ? "killed" : "removed" };
    case COMMAND_KINDS.adminEvict:
      return { name: body.name, status: "evicted" };
    case COMMAND_KINDS.adminDelete:
      return { name: body.name, status: "deleted" };
    case COMMAND_KINDS.adminAssignRole:
      return { name: body.name, role: body.role };
    case COMMAND_KINDS.adminAssignProject:
      return { name: body.name, project: body.project };
    case COMMAND_KINDS.adminGrantTier:
      return { name: body.name, tier: body.tier };
    case COMMAND_KINDS.agentGrantAccess:
      return {
        name: body.name,
        principal: body.principal,
        project: body.project ?? "default",
        role: body.role,
      };
    case COMMAND_KINDS.agentRevokeAccess:
      return {
        name: body.name,
        principal: body.principal,
        project: body.project ?? "default",
        revoked: true,
      };
    case COMMAND_KINDS.agentTransferOwner:
      return {
        name: body.name,
        previousOwner: "owner",
        previousProject: "default",
        owner: body.owner,
        project: body.project ?? "default",
      };
    case COMMAND_KINDS.agentCredentialCreate:
      return {
        credentialId: "cred_command",
        agentId: body.agentId ?? "a_ben",
        secret: "nexus_rt_secret",
        label: body.label,
        purpose: body.purpose,
        scopes: body.scopes ?? ["runtime:register"],
      };
    case COMMAND_KINDS.agentCredentialRevoke:
      return { credentialId: body.credentialId, revoked: true };
    case COMMAND_KINDS.identityRename:
      return { name: body.name, previous: "etan" };
    case COMMAND_KINDS.sourceRegister:
      return {
        source: {
          name: body.name,
          topic: body.topic ?? `${String(body.name)}.default`,
          enabled: true,
          createdAt: 1,
        },
        token: `src_${String(body.name)}token`,
      };
    case COMMAND_KINDS.sourceEnable:
    case COMMAND_KINDS.sourceDisable:
      return {
        name: body.name,
        topic: "default",
        enabled: kind === COMMAND_KINDS.sourceEnable,
        createdAt: 1,
      };
    case COMMAND_KINDS.sourceRotate:
      return { name: body.name, token: `src_${String(body.name)}rotated` };
    case COMMAND_KINDS.sourceRemove:
      return { name: body.name };
    case COMMAND_KINDS.sourcePush:
      return {
        topic: body.topic ?? `${String(body.source)}.default`,
        messageId: "m_push_test",
        queuedTo: 1,
      };
    default:
      return null;
  }
}

function makeCommandSpy(
  result: unknown | CommandResultFactory = defaultCommandResult,
): CommandSpy {
  const calls: CommandSpy["calls"] = [];
  return {
    calls,
    submit: vi.fn(async (kind, requestBody, caller, opts) => {
      calls.push({
        kind,
        req: requestBody,
        caller,
        ...(opts?.idempotencyKey ? { opts } : {}),
      });
      return typeof result === "function"
        ? result(kind, requestBody, caller)
        : result;
    }) as CommandIntentSender["submit"],
  };
}

let db: ReadDb;

beforeEach(async () => {
  db = await seedDb();
  process.env.NEXUS_API_KEY = API_KEY;
});

/** Build a request with a resolved gateway Principal by default. */
function req(overrides: Partial<ApiRequest>): ApiRequest {
  return {
    method: "GET",
    path: "/api/v1/health",
    query: {},
    headers: {},
    caller: API_CALLER,
    body: undefined,
    ...overrides,
  };
}

function deps(
  messagePost?: MessagePostSender,
  commands?: CommandIntentSender,
  humanReads?: HumanReadDeliveryMarker,
): ApiDeps {
  // `db` is now a LAZY getter (the $.ts adapter memoizes the real/mock handle and
  // only builds it when a read handler asks). Tests hand back the already-seeded
  // in-memory handle, so reads resolve it and non-read routes never call it.
  return {
    db: () => db,
    ...(messagePost ? { messagePost } : {}),
    commands: commands ?? makeCommandSpy(),
    ...(humanReads ? { humanReads } : {}),
  };
}

function signedNotifyRequest(opts: {
  rawBody: string;
  timestamp?: number;
  secret?: string;
  signature?: string;
}): ApiRequest {
  const timestamp = opts.timestamp ?? Date.now();
  const secret = opts.secret ?? NOTIFY_HMAC_SECRET;
  const signature = opts.signature ?? (
    "sha256=" + createHmac("sha256", secret)
      .update(`${timestamp}.${opts.rawBody}`)
      .digest("hex")
  );
  return req({
    method: "POST",
    path: "/api/v1/notify",
    headers: {
      "x-nexus-timestamp": String(timestamp),
      "x-nexus-signature": signature,
    },
    rawBody: opts.rawBody,
    body: JSON.parse(opts.rawBody) as unknown,
  });
}

describe("public API — writes/ops dispatch through the right transport", () => {
  it("POST /api/v1/notifications → submits the canonical explicit-target daemon request", async () => {
    const commands = makeCommandSpy({ messageId: "m_notification", fanout: 3 });
    const body = {
      target: { kind: "thread", thread: "release-gate" },
      source: "watchdog",
      body: "CHECKPOINT_READY",
      idempotencyKey: "checkpoint:1",
    };

    const res = await handle(
      req({ method: "POST", path: "/api/v1/notifications", body }),
      deps(undefined, commands),
    );

    expect(res).toEqual({ status: 201, body: { messageId: "m_notification" } });
    expect(commands.calls).toEqual([
      {
        kind: "notification.send",
        req: body,
        caller: API_CALLER,
        opts: { idempotencyKey: "checkpoint:1" },
      },
    ]);
    expect(res.body).not.toHaveProperty("fanout");
  });

  it("POST /api/v1/notifications → rejects an unauthenticated caller before daemon IPC", async () => {
    const commands = makeCommandSpy({ messageId: "m_never" });

    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/notifications",
        caller: undefined,
        body: {
          target: { kind: "agent", agentId: "a_target" },
          body: "must not cross ingress",
        },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(401);
    expect(commands.calls).toHaveLength(0);
  });

  it("POST /api/v1/notify → verifies the signed raw body and enqueues its durable envelope", async () => {
    const commands = makeCommandSpy();
    const rawBody = JSON.stringify({ source: "ci", topic: "builds", payload: { status: "green" } });
    const request = signedNotifyRequest({ rawBody, timestamp: 1_784_000_000_000 });
    const res = await handle(
      request,
      { ...deps(undefined, commands), notifyHmacSecret: NOTIFY_HMAC_SECRET, now: () => 1_784_000_000_000 },
    );

    expect([200, 201]).toContain(res.status);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.notificationNotify,
        req: {
          rawBody,
          timestamp: "1784000000000",
          signature: expect.stringMatching(/^sha256=[0-9a-f]{64}$/),
        },
        caller: {
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
        opts: {
          idempotencyKey: expect.stringMatching(/^notify:1784000000000:[0-9a-f]{64}$/),
        },
      },
    ]);
    const body = res.body as { notifId: string; routedTo: string[]; hmacOk: boolean };
    expect(typeof body.notifId).toBe("string");
    expect(Array.isArray(body.routedTo)).toBe(true);
    expect(body.hmacOk).toBe(true);
    expect(body.routedTo).toContain("ben");
  });

  it("POST /api/v1/notify → returns service unavailable without a configured HMAC secret", async () => {
    const commands = makeCommandSpy();
    const rawBody = JSON.stringify({ source: "ci", payload: { status: "green" } });

    const res = await handle(
      signedNotifyRequest({ rawBody }),
      { ...deps(undefined, commands), notifyHmacSecret: "" },
    );

    expect(res.status).toBe(503);
    expect(res.body).toMatchObject({ error: { code: "service_unavailable" } });
    expect(commands.calls).toHaveLength(0);
  });

  it("POST /api/v1/notify → rejects a stale timestamp before command ingress", async () => {
    const commands = makeCommandSpy();
    const rawBody = JSON.stringify({ source: "ci", payload: { status: "green" } });
    const timestamp = 1_784_000_000_000;

    const res = await handle(
      signedNotifyRequest({ rawBody, timestamp }),
      {
        ...deps(undefined, commands),
        notifyHmacSecret: NOTIFY_HMAC_SECRET,
        now: () => timestamp + 300_001,
      },
    );

    expect(res.status).toBe(401);
    expect(commands.calls).toHaveLength(0);
  });

  it("POST /api/v1/notify → rejects an invalid signature before command ingress", async () => {
    const commands = makeCommandSpy();
    const rawBody = JSON.stringify({ source: "ci", payload: { status: "green" } });

    const res = await handle(
      signedNotifyRequest({ rawBody, signature: `sha256=${"0".repeat(64)}` }),
      { ...deps(undefined, commands), notifyHmacSecret: NOTIFY_HMAC_SECRET },
    );

    expect(res.status).toBe(401);
    expect(commands.calls).toHaveLength(0);
  });

  it("POST /api/v1/messages → enqueues message.post.send when no Message Post sender is injected", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/messages",
        caller: { name: "etan", project: "default" },
        body: { to: { verb: "post", thread: "design" }, body: "hello thread" },
      }),
      deps(undefined, commands),
    );

    expect([200, 201]).toContain(res.status);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.messagePostSend,
        req: {
          to: { verb: "post", thread: "design" },
          body: "hello thread",
        },
        caller: { name: "etan", project: "default" },
      },
    ]);
    expect(res.body).toMatchObject({ messageId: expect.any(String) });
  });

  it("POST /api/v1/messages → preserves legacy names and canonical DM identities", async () => {
    const commands = makeCommandSpy();
    for (const to of [
      { verb: "dm", name: "ben" },
      { verb: "dm", agentId: "a_ben" },
      { verb: "dm", name: "stale-ben", agentId: "a_ben" },
    ] as const) {
      const res = await handle(
        req({
          method: "POST",
          path: "/api/v1/messages",
          caller: { name: "etan", project: "default" },
          body: { to, body: "hello" },
        }),
        deps(undefined, commands),
      );
      expect(res.status).toBe(201);
    }

    expect(commands.calls.map((call) => (call.req as { to: unknown }).to)).toEqual([
      { verb: "dm", name: "ben" },
      { verb: "dm", agentId: "a_ben" },
      { verb: "dm", name: "stale-ben", agentId: "a_ben" },
    ]);
  });

  it("POST /api/v1/messages → rejects an unaddressed DM before command ingress", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/messages",
        caller: { name: "etan", project: "default" },
        body: { to: { verb: "dm" }, body: "hello" },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(400);
    expect(commands.calls).toEqual([]);
  });

  it("POST /api/v1/messages → rejects empty and whitespace-only message bodies", async () => {
    const commands = makeCommandSpy();
    for (const [to, body] of [
      [{ verb: "dm", name: "ben" }, ""],
      [{ verb: "post", thread: "design" }, " \n\t "],
      [{ verb: "reply" }, "\n"],
    ] as const) {
      const res = await handle(
        req({
          method: "POST",
          path: "/api/v1/messages",
          caller: { name: "etan", project: "default" },
          body: { to, body },
        }),
        deps(undefined, commands),
      );

      expect(res.status).toBe(400);
      expect((res.body as { error: { message: string } }).error.message).toContain("body");
    }

    expect(commands.calls).toEqual([]);
  });

  it("POST /api/v1/messages → forwards the idempotency key to command ingress", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/messages",
        headers: { "idempotency-key": "web:post:design:client-1" },
        caller: { name: "etan", project: "default" },
        body: { to: { verb: "post", thread: "design" }, body: "hello thread" },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(201);
    expect(commands.calls[0]).toMatchObject({
      kind: COMMAND_KINDS.messagePostSend,
      req: {
        to: { verb: "post", thread: "design" },
        body: "hello thread",
        idempotencyKey: "web:post:design:client-1",
      },
      opts: { idempotencyKey: "web:post:design:client-1" },
    });
  });

  it("POST /api/v1/messages → uses the Message Post sender when injected", async () => {
    const sent: unknown[] = [];
    const messagePost: MessagePostSender = {
      send: async (body, caller, opts) => {
        sent.push({ body, caller, opts });
        return { messageId: "m_socket", fanout: 1 };
      },
    };
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/messages",
        caller: {
          name: "etan",
          project: "default",
          sessionId: "s_human",
          clientKey: "ck_human",
        },
        body: { to: { verb: "post", thread: "design" }, body: "hello thread" },
      }),
      deps(messagePost),
    );

    expect(res.status).toBe(201);
    expect(res.body).toEqual({ messageId: "m_socket" });
    expect(sent).toEqual([
      {
        body: {
          to: { verb: "post", thread: "design" },
          body: "hello thread",
        },
        caller: {
          name: "etan",
          project: "default",
          sessionId: "s_human",
          clientKey: "ck_human",
        },
      },
    ]);
  });

  it("POST /api/v1/messages → 401 without an authenticated caller", async () => {
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/messages",
        caller: undefined,
        body: { to: { verb: "post", thread: "design" }, body: "hello thread" },
      }),
      deps(),
    );

    expect(res.status).toBe(401);
  });

  it("PATCH /api/v1/messages/:id/metadata → enqueues metadata.set", async () => {
    const commands = makeCommandSpy();
    const metadata = { label: "launch-proof", arbitrary: ["json", 1] };
    const res = await handle(
      req({
        method: "PATCH",
        path: "/api/v1/messages/m_seed_1/metadata",
        caller: { name: "tooling", project: "nexus" },
        body: { metadata },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.metadataSet,
        req: { entity: "message", id: "m_seed_1", metadata },
        caller: { name: "tooling", project: "nexus" },
      },
    ]);
    expect(res.body).toEqual({ entity: "message", id: "m_seed_1", metadata });
  });

  it("GET /api/v1/hooks exposes redacted registry, public-key, and verified audit reads", async () => {
    const hooks: HookDiagnosticsReader = {
      list: vi.fn(async (includePrivate) => ({
        generation: "sha256:generation",
        includePrivate,
        hooks: [{ id: "redact", event: "before_send" }],
        errors: [],
      })),
      publicKey: vi.fn(async () => ({
        algorithm: "ed25519",
        keyId: "sha256:key",
        publicKey: "PUBLIC KEY",
      })),
      audit: vi.fn(async (limit, includePrivate) => ({
        limit,
        includePrivate,
        executions: [{ verified: true }],
      })),
    };
    const adminDeps = { ...deps(), hooks };

    const list = await handle(req({ path: "/api/v1/hooks" }), adminDeps);
    expect(list).toMatchObject({ status: 200, body: { includePrivate: true } });
    expect(hooks.list).toHaveBeenCalledWith(true);

    const agent = await handle(
      req({
        path: "/api/v1/hooks",
        caller: { ...API_CALLER, tier: Tier.Agent, scopes: ["message:read"] },
      }),
      adminDeps,
    );
    expect(agent).toMatchObject({ status: 200, body: { includePrivate: false } });
    expect(hooks.list).toHaveBeenLastCalledWith(false);

    await expect(handle(req({ path: "/api/v1/hooks/public-key" }), adminDeps))
      .resolves.toMatchObject({ status: 200, body: { algorithm: "ed25519" } });
    await expect(handle(req({ path: "/api/v1/hooks/audit", query: { limit: "7" } }), adminDeps))
      .resolves.toMatchObject({ status: 200, body: { limit: 7, includePrivate: true } });
  });
});

describe("public API — reads go to the read-view (NOT the daemon)", () => {
  it("GET /api/v1/threads → returns the seeded threads from the read-view", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/threads" }),
      deps(),
    );

    expect(res.status).toBe(200);
    const body = res.body as Array<{ name: string; members: string[] }>;
    const names = body.map((t) => t.name);
    expect(names).toContain("design");
    expect(names).toContain("ops");
    // DM threads are excluded by the read-view.
    expect(names).not.toContain("dm:etan:ben");
  });

  it("GET /api/v1/threads/:name/members → returns one thread roster from the read-view", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/threads/design/members" }),
      deps(),
    );

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      name: "design",
      members: ["ben", "blake", "etan"],
    });
  });

  it("GET /api/v1/threads/:name/header → returns metadata, roster presence, and active runtime count", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/threads/design/header" }),
      deps(),
    );

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      name: "design",
      topic: "Interface planning",
      description: "Product and console design discussions.",
      lastAt: 1_700_000_002_000,
      memberCount: 3,
      activeSessions: 2,
      members: [
        expect.objectContaining({ name: "ben", agent: "claude", presence: "online", sessionId: "s_ben", currentWork: "post-merge gate" }),
        expect.objectContaining({ name: "blake", agent: "codex", presence: "busy", sessionId: "s_blake", currentWork: "coordinating tasks" }),
        expect.objectContaining({ name: "etan", kind: "human", presence: "online", sessionId: "s_etan" }),
      ],
    });
  });

  it("GET /api/v1/messages/:id/metadata → returns the projected metadata bag", async () => {
    await db.$client.execute({
      sql: "UPDATE messages SET metadata_json = ? WHERE message_id = ?",
      args: [JSON.stringify({ source: "qa", flags: ["reviewed"] }), "m_seed_1"],
    });

    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/messages/m_seed_1/metadata",
        caller: { ...API_CALLER, project: "p_nexus" },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      entity: "message",
      id: "m_seed_1",
      metadata: { source: "qa", flags: ["reviewed"] },
    });
    expect(commands.calls).toEqual([]);
  });

  it("GET /api/v1/members → seeded roster from the read-view", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/members", query: { includeOffline: "true" } }),
      deps(),
    );

    expect(res.status).toBe(200);
    const body = res.body as Array<{ name: string }>;
    expect(body.map((m) => m.name).sort()).toEqual(["ben", "blake", "dylan", "etan"]);
  });

  it("GET /api/v1/members defaults to hiding explicit offline rows", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/members" }),
      deps(),
    );

    expect(res.status).toBe(200);
    const body = res.body as Array<{ name: string; presence: string }>;
    expect(body.map((m) => m.name).sort()).toEqual(["ben", "blake", "etan"]);
    expect(body.map((m) => m.name)).not.toContain("dylan");
  });

  it("GET /api/v1/members derives stale heartbeats as offline without losing audit visibility", async () => {
    await db.$client.execute({
      sql: "UPDATE sessions SET presence = 'online', last_heartbeat = ? WHERE name = 'ben'",
      args: [Date.now() - 60_000],
    });

    const online = await handle(
      req({ method: "GET", path: "/api/v1/members" }),
      deps(),
    );

    expect(online.status).toBe(200);
    const onlineBody = online.body as Array<{ name: string }>;
    expect(onlineBody.map((m) => m.name).sort()).toEqual(["blake", "etan"]);
    expect(onlineBody.map((m) => m.name)).not.toContain("ben");

    const all = await handle(
      req({ method: "GET", path: "/api/v1/members", query: { includeOffline: "true" } }),
      deps(),
    );

    expect(all.status).toBe(200);
    const allBody = all.body as Array<{ name: string; presence: string }>;
    expect(allBody.map((m) => m.name).sort()).toEqual(["ben", "blake", "dylan", "etan"]);
    expect(allBody.find((m) => m.name === "ben")?.presence).toBe("offline");
    expect(allBody.find((m) => m.name === "dylan")?.presence).toBe("offline");
  });

  it("GET /api/v1/whoami uses the authenticated caller when no name query is supplied", async () => {
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/whoami",
        caller: {
          name: "etan",
          project: "nexus",
          kind: Kind.Human,
          locality: Locality.External,
          access: "guest",
          principalId: "x_etan",
          tier: Tier.Admin,
          sessionId: "s_etan_browser",
          agentId: "a_etan_browser",
        },
      }),
      deps(),
    );

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      name: "etan",
      agentId: "a_etan_browser",
      kind: "human",
      locality: "external",
      entityKind: "external.human",
      access: "guest",
      principalId: "x_etan",
      tier: "admin",
      project: "nexus",
      presence: "online",
    });
  });

  it("GET /api/v1/whoami ignores a name query that targets another identity", async () => {
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/whoami",
        query: { name: "ben" },
        caller: {
          name: "etan",
          project: "nexus",
          kind: Kind.Human,
          tier: Tier.Admin,
          sessionId: "s_etan_browser",
        },
      }),
      deps(),
    );

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      name: "etan",
      kind: "human",
      project: "nexus",
    });
  });

  it("GET /api/v1/whoami works without a projected identity row", async () => {
    const readDb = vi.fn(() => db);
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/whoami",
        caller: {
          name: "browser-operator",
          project: "default",
          kind: Kind.Human,
          tier: Tier.Admin,
          sessionId: "s_browser_operator",
        },
      }),
      { db: readDb },
    );

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      name: "browser-operator",
      sessionId: "s_browser_operator",
      kind: "human",
      tier: "admin",
      project: "default",
    });
    expect(readDb).not.toHaveBeenCalled();
  });

  it("GET /api/v1/whoami without caller or name returns 401 instead of querying an empty name", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/whoami", caller: undefined }),
      deps(),
    );

    expect(res.status).toBe(401);
    expect(res.body).toMatchObject({
      error: { code: "unauthorized", message: "missing or invalid Principal" },
    });
  });

  it("GET /api/v1/search?q=... → seeded read-view search hits", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/search", query: { q: "hi" } }),
      deps(),
    );

    expect(res.status).toBe(200);
    const body = res.body as Array<{ from: string; snippet: string }>;
    expect(body.length).toBeGreaterThan(0);
    expect(body[0]!.from).toBe("ben");
    expect(body[0]!.snippet).toContain("hi");
  });

  it("GET /api/v1/search supports mode/scope/cursor filters and hides other DMs", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, topic, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?), " +
        "(?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_search_thread_recent",
        "ben",
        "thread",
        "design",
        "t_design",
        null,
        "RESTPARITY visible recent thread",
        "p_nexus",
        1_700_000_020_000,
        "m_search_dm_visible",
        "ben",
        "dm",
        "etan",
        null,
        null,
        "RESTPARITY visible dm",
        "nexus",
        1_700_000_020_001,
        "m_search_dm_hidden",
        "ben",
        "dm",
        "blake",
        null,
        null,
        "RESTPARITY hidden dm",
        "nexus",
        1_700_000_020_002,
        "m_search_topic",
        "ci",
        "topic",
        null,
        null,
        "deploys",
        "RESTPARITY visible topic",
        "nexus",
        1_700_000_020_003,
      ],
    });

    for (const [messageId, body] of [
      ["m_search_thread_recent", "RESTPARITY visible recent thread"],
      ["m_search_dm_visible", "RESTPARITY visible dm"],
      ["m_search_dm_hidden", "RESTPARITY hidden dm"],
      ["m_search_topic", "RESTPARITY visible topic"],
    ] as const) {
      const row = await db.$client.execute({
        sql: "SELECT rowid FROM messages WHERE message_id = ?",
        args: [messageId],
      });
      const rowid = Number((row.rows[0] as unknown as { rowid: number }).rowid);
      await db.$client.execute({
        sql: "INSERT INTO messages_fts (rowid, summary, body) VALUES (?, ?, ?)",
        args: [rowid, "", body],
      });
    }

    const threadOnly = await handle(
      req({
        method: "GET",
        path: "/api/v1/search",
        query: { q: "RESTPARITY", mode: "hybrid", thread: "design", since: "1700000010000", limit: "1" },
        caller: { name: "etan", project: "nexus" },
      }),
      deps(),
    );
    const dmVisible = await handle(
      req({
        method: "GET",
        path: "/api/v1/search",
        query: { q: "RESTPARITY", with: "ben", mode: "fts" },
        caller: { name: "etan", project: "nexus" },
      }),
      deps(),
    );
    const dmHidden = await handle(
      req({
        method: "GET",
        path: "/api/v1/search",
        query: { q: "RESTPARITY" },
        caller: { name: "etan", project: "nexus" },
      }),
      deps(),
    );
    const topicOnly = await handle(
      req({
        method: "GET",
        path: "/api/v1/search",
        query: { q: "RESTPARITY", topic: "deploys" },
        caller: { name: "etan", project: "nexus" },
      }),
      deps(),
    );

    expect(threadOnly.status).toBe(200);
    expect(threadOnly.body).toEqual([
      expect.objectContaining({ messageId: "m_search_thread_recent" }),
    ]);

    expect(dmVisible.status).toBe(200);
    expect(dmVisible.body).toContainEqual(
      expect.objectContaining({ messageId: "m_search_dm_visible" }),
    );

    expect(dmHidden.status).toBe(200);
    const visibleIds = (dmHidden.body as Array<{ messageId: string }>).map((h) => h.messageId);
    expect(visibleIds).toContain("m_search_dm_visible");
    expect(visibleIds).not.toContain("m_search_dm_hidden");
    expect(topicOnly.status).toBe(200);
    expect(topicOnly.body).toEqual([
      expect.objectContaining({ messageId: "m_search_topic" }),
    ]);
  });

  it("GET /api/v1/threads/:name/history → read-view thread history (path param)", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/threads/design/history" }),
      deps(),
    );
    expect(res.status).toBe(200);
    const body = res.body as Array<{ from: string; body: string }>;
    expect(body.length).toBe(2);
    expect(body[0]!.from).toBe("ben");
  });

  it("GET /api/v1/threads/:name/history marks returned rows delivered for the authenticated human session only", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state, delivered_at) " +
        "VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)",
      args: [
        "if_human_seed_1",
        "m_seed_1",
        API_CALLER_SESSION_ID,
        "pending",
        null,
        "if_human_seed_2",
        "m_seed_2",
        API_CALLER_SESSION_ID,
        "notified",
        null,
        "if_other_seed_1",
        "m_seed_1",
        "s_other_human",
        "pending",
        null,
      ],
    });

    const marker = createHumanReadDeliveryMarker(() => db.$client, {
      now: () => 1_700_000_500_000,
    });
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/threads/design/history",
        caller: API_CALLER,
      }),
      deps(undefined, undefined, marker),
    );

    expect(res.status).toBe(200);
    const delivered = await db.$client.execute({
      sql:
        "SELECT in_flight_id, state, delivered_at FROM in_flight " +
        "WHERE in_flight_id IN (?, ?, ?) ORDER BY in_flight_id",
      args: ["if_human_seed_1", "if_human_seed_2", "if_other_seed_1"],
    });
    expect(delivered.rows).toEqual([
      {
        in_flight_id: "if_human_seed_1",
        state: "delivered",
        delivered_at: 1_700_000_500_000,
      },
      {
        in_flight_id: "if_human_seed_2",
        state: "delivered",
        delivered_at: 1_700_000_500_000,
      },
      {
        in_flight_id: "if_other_seed_1",
        state: "pending",
        delivered_at: null,
      },
    ]);
  });

  it("GET /api/v1/dms/:name/history mark-on-read is scoped to the caller session, not query me", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_dm_human_read",
        "ben",
        "dm",
        API_CALLER.name,
        null,
        "human browser delivery",
        "nexus",
        1_700_000_006_000,
      ],
    });
    await db.$client.execute({
      sql:
        "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state, delivered_at) " +
        "VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)",
      args: [
        "if_dm_human",
        "m_dm_human_read",
        API_CALLER_SESSION_ID,
        "pending",
        null,
        "if_dm_other",
        "m_dm_human_read",
        "s_etan",
        "pending",
        null,
      ],
    });

    const marker = createHumanReadDeliveryMarker(() => db.$client, {
      now: () => 1_700_000_600_000,
    });
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/dms/ben/history",
        query: { me: "etan" },
        caller: API_CALLER,
      }),
      deps(undefined, undefined, marker),
    );

    expect(res.status).toBe(200);
    const rows = await db.$client.execute({
      sql:
        "SELECT in_flight_id, state, delivered_at FROM in_flight " +
        "WHERE in_flight_id IN (?, ?) ORDER BY in_flight_id",
      args: ["if_dm_human", "if_dm_other"],
    });
    expect(rows.rows).toEqual([
      {
        in_flight_id: "if_dm_human",
        state: "delivered",
        delivered_at: 1_700_000_600_000,
      },
      {
        in_flight_id: "if_dm_other",
        state: "pending",
        delivered_at: null,
      },
    ]);
  });

  it("GET /api/v1/messages/:id mark-on-read is idempotent and does not rewrite delivered_at", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state, delivered_at) " +
        "VALUES (?, ?, ?, ?, ?)",
      args: [
        "if_message_already",
        "m_seed_1",
        API_CALLER_SESSION_ID,
        "delivered",
        1_700_000_007_000,
      ],
    });

    const marker = createHumanReadDeliveryMarker(() => db.$client, {
      now: () => 1_700_000_700_000,
    });
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/messages/m_seed_1",
        caller: { ...API_CALLER, project: "p_nexus" },
      }),
      deps(undefined, undefined, marker),
    );

    expect(res.status).toBe(200);
    const rows = await db.$client.execute({
      sql:
        "SELECT state, delivered_at FROM in_flight " +
        "WHERE in_flight_id = ? LIMIT 1",
      args: ["if_message_already"],
    });
    expect(rows.rows[0]).toEqual({
      state: "delivered",
      delivered_at: 1_700_000_007_000,
    });
  });

  it("message-bearing reads fail open when the human receipt marker errors", async () => {
    const marker: HumanReadDeliveryMarker = {
      markDelivered: vi.fn(async () => {
        throw new Error("receipt db locked");
      }),
    };

    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/threads/design/history",
        caller: API_CALLER,
      }),
      deps(undefined, undefined, marker),
    );

    expect(res.status).toBe(200);
    expect(res.body).toEqual([
      expect.objectContaining({ messageId: "m_seed_1" }),
      expect.objectContaining({ messageId: "m_seed_2" }),
    ]);
    expect(marker.markDelivered).toHaveBeenCalledWith(
      API_CALLER_SESSION_ID,
      ["m_seed_1", "m_seed_2"],
    );
  });

  it("message-bearing reads fail open when the human receipt marker never settles", async () => {
    vi.useFakeTimers();
    let receiptStarted: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      receiptStarted = resolve;
    });
    const marker: HumanReadDeliveryMarker = {
      markDelivered: vi.fn(() => {
        receiptStarted?.();
        return new Promise<number>(() => {});
      }),
    };

    try {
      const pending = handle(
        req({
          method: "GET",
          path: "/api/v1/threads/design/history",
          caller: API_CALLER,
        }),
        deps(undefined, undefined, marker),
      );
      await started;
      await vi.advanceTimersByTimeAsync(250);
      const res = await pending;

      expect(res.status).toBe(200);
      expect(res.body).toEqual([
        expect.objectContaining({ messageId: "m_seed_1" }),
        expect.objectContaining({ messageId: "m_seed_2" }),
      ]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("GET history routes honor limit and before cursors", async () => {
    const threadPage = await handle(
      req({
        method: "GET",
        path: "/api/v1/threads/design/history",
        query: { limit: "1", before: "1700000002000" },
      }),
      deps(),
    );
    expect(threadPage.status).toBe(200);
    expect(threadPage.body).toEqual([
      expect.objectContaining({ from: "ben" }),
    ]);

    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_dm_cursor_old",
        "ben",
        "dm",
        "etan",
        null,
        "older cursor dm",
        "nexus",
        1_700_000_003_000,
        "m_dm_cursor_new",
        "ben",
        "dm",
        "etan",
        null,
        "newer cursor dm",
        "nexus",
        1_700_000_004_000,
      ],
    });
    const dmPage = await handle(
      req({
        method: "GET",
        path: "/api/v1/dms/ben/history",
        query: { limit: "1", before: "1700000004000" },
        caller: { name: "etan", project: "nexus" },
      }),
      deps(),
    );

    expect(dmPage.status).toBe(200);
    expect(dmPage.body).toEqual([
      expect.objectContaining({ messageId: "m_dm_cursor_old" }),
    ]);
  });

  it("GET thread history forwards the additive exact after cursor", async () => {
    const createdAt = 1_700_004_000_000;
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "VALUES (?, 'ben', 'thread', 'design', 't_design', 'first', 'nexus', ?), " +
        "(?, 'blake', 'thread', 'design', 't_design', 'second', 'nexus', ?)",
      args: ["m_api_cursor_first", createdAt, "m_api_cursor_second", createdAt],
    });
    const first = await db.$client.execute({
      sql: "SELECT rowid FROM messages WHERE message_id = ?",
      args: ["m_api_cursor_first"],
    });
    const afterRowid = Number(first.rows[0]?.rowid ?? 0);

    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/threads/design/history",
        query: { after: String(createdAt), afterRowid: String(afterRowid), limit: "10" },
      }),
      deps(),
    );

    expect(res.status).toBe(200);
    expect((res.body as Array<{ messageId: string }>).map((row) => row.messageId)).toEqual([
      "m_api_cursor_second",
    ]);
  });

  it("GET /api/v1/dms/:name/history → read-view DM history for the caller pair", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_dm_history",
        "ben",
        "dm",
        "etan",
        null,
        "stored DM history",
        "nexus",
        1_700_000_003_000,
      ],
    });

    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/dms/ben/history",
        caller: { name: "etan", project: "nexus" },
      }),
      deps(),
    );

    expect(res.status).toBe(200);
    const body = res.body as Array<{ messageId: string; from: string; body: string }>;
    expect(body).toContainEqual(
      expect.objectContaining({
        messageId: "m_dm_history",
        from: "ben",
        body: "stored DM history",
      }),
    );
  });

  it("GET /api/v1/dms/:name/history → local operator sees agent replies", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_operator_prompt",
        "operator",
        "dm",
        "roman",
        null,
        "human prompt from web",
        "default",
        1_700_000_004_000,
        "m_operator_reply",
        "roman",
        "dm",
        "operator",
        null,
        "reply visible in the web operator DM",
        "default",
        1_700_000_004_001,
      ],
    });

    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/dms/roman/history",
        caller: { name: "operator", project: "default" },
      }),
      deps(),
    );

    expect(res.status).toBe(200);
    const body = res.body as Array<{ messageId: string; from: string; body: string }>;
    expect(body).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          messageId: "m_operator_prompt",
          from: "operator",
          body: "human prompt from web",
        }),
        expect.objectContaining({
          messageId: "m_operator_reply",
          from: "roman",
          body: "reply visible in the web operator DM",
        }),
      ]),
    );
  });

  it("GET /api/v1/agents/:id → durable agent identity plus runtime read-view", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/agents/ben" }),
      deps(),
    );

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      agent: {
        agentId: "a_ben",
        name: "ben",
        project: "nexus",
        defaultHarness: "claude",
        role: "admin",
        tier: "agent",
        disabled: false,
        activeRuntime: {
          runtimeId: "s_ben",
          agentId: "a_ben",
          harness: "claude",
          presence: "online",
          active: true,
        },
      },
      runtimes: expect.arrayContaining([
        expect.objectContaining({
          runtimeId: "s_ben",
          agentId: "a_ben",
          harness: "claude",
          active: true,
        }),
      ]),
    });
  });

  it("GET /api/v1/agents/:id/runtimes → lists runtime rows with stopped rows opt-in", async () => {
    const active = await handle(
      req({ method: "GET", path: "/api/v1/agents/ben/runtimes" }),
      deps(),
    );
    const all = await handle(
      req({
        method: "GET",
        path: "/api/v1/agents/ben/runtimes",
        query: { includeStopped: "true" },
      }),
      deps(),
    );

    expect(active.status).toBe(200);
    expect(active.body).toMatchObject({
      agentId: "a_ben",
      runtimes: [
        expect.objectContaining({ runtimeId: "s_ben", active: true }),
      ],
    });

    expect(all.status).toBe(200);
    const body = all.body as { runtimes: Array<{ runtimeId: string; active: boolean }> };
    expect(body.runtimes.map((r) => r.runtimeId)).toEqual(["s_ben", "s_ben_old"]);
  });

  it("GET /api/v1/runtimes?agent=ben → top-level runtime parity route", async () => {
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/runtimes",
        query: { agent: "ben", includeStopped: "true" },
      }),
      deps(),
    );

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      agentId: "a_ben",
      runtimes: [
        expect.objectContaining({ runtimeId: "s_ben" }),
        expect.objectContaining({ runtimeId: "s_ben_old" }),
      ],
    });
  });

  it("GET /api/v1/runtimes prefers a stable agentId over stale name metadata", async () => {
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/runtimes",
        query: { name: "blake", agentId: "a_ben", includeStopped: "true" },
      }),
      deps(),
    );

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      agentId: "a_ben",
      runtimes: expect.arrayContaining([expect.objectContaining({ runtimeId: "s_ben" })]),
    });
  });

  it("GET /api/v1/runtimes with no target query → fleet-wide runtime list", async () => {
    const active = await handle(
      req({ method: "GET", path: "/api/v1/runtimes" }),
      deps(),
    );
    const all = await handle(
      req({
        method: "GET",
        path: "/api/v1/runtimes",
        query: { includeStopped: "true" },
      }),
      deps(),
    );

    expect(active.status).toBe(200);
    const activeBody = active.body as {
      runtimes: Array<{ runtimeId: string; name?: string; active: boolean }>;
    };
    expect(activeBody.runtimes.every((r) => r.active)).toBe(true);
    expect(activeBody.runtimes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ runtimeId: "s_ben", name: "ben", agentId: "a_ben" }),
      ]),
    );

    expect(all.status).toBe(200);
    const allBody = all.body as { runtimes: Array<{ runtimeId: string }> };
    expect(allBody.runtimes.map((r) => r.runtimeId)).toEqual(
      expect.arrayContaining(["s_ben", "s_ben_old"]),
    );
  });

  it("GET /api/v1/notifications → includes delivered source pushes", async () => {
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, thread_id, topic, summary, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_api_source_push",
        "github-ci",
        "topic",
        "builds",
        null,
        "builds",
        "build passed",
        "source push body",
        "p_nexus",
        1_700_000_010_000,
      ],
    });
    await db.$client.execute({
      sql:
        "INSERT INTO in_flight (in_flight_id, message_id, recipient_session, state) " +
        "VALUES (?, ?, ?, ?)",
      args: ["if_api_source_push_ben", "m_api_source_push", "s_ben", "pending"],
    });

    const res = await handle(
      req({ method: "GET", path: "/api/v1/notifications" }),
      deps(),
    );

    expect(res.status).toBe(200);
    const body = res.body as Array<{
      notifId: string;
      source?: string;
      topic?: string;
      routedTo: string[];
    }>;
    expect(body).toContainEqual(
      expect.objectContaining({
        notifId: "m_api_source_push",
        source: "github-ci",
        topic: "builds",
        routedTo: ["ben"],
      }),
    );
  });

  it("GET /api/v1/notifications accepts additive limit and before pagination", async () => {
    await db.$client.execute({
      sql:
        "WITH RECURSIVE nums(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM nums WHERE n < 9) " +
        "INSERT INTO notifications (notif_id, source, topic, hmac_ok, payload, routed_to, created_at) " +
        "SELECT 'n_api_page_' || n, 'ci', 'builds', 1, '{}', 'ben', 1700010000000 + n FROM nums",
      args: [],
    });

    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/notifications",
        query: { limit: "3", before: "1700010000008" },
      }),
      deps(),
    );

    expect(res.status).toBe(200);
    const body = res.body as Array<{ notifId: string; when: number }>;
    expect(body).toHaveLength(3);
    expect(body.every((row) => row.when < 1_700_010_000_008)).toBe(true);
  });
});

describe("public API — cross-cutting: auth, validation, errors", () => {
  it("a malformed notify body → 400 (Zod)", async () => {
    const res = await handle(
      req({ method: "POST", path: "/api/v1/notify", body: { topic: "builds" } }), // missing `source`
      deps(),
    );
    expect(res.status).toBe(400);
    // never dispatched to the daemon on a validation failure
  });

  it("a missing Principal → 401", async () => {
    const res = await handle(
      {
        method: "POST",
        path: "/api/v1/messages",
        query: {},
        headers: {}, // no Authorization
        body: { to: { verb: "dm", name: "ben" }, body: "hello" },
      },
      deps(),
    );
    expect(res.status).toBe(401);
  });

  it("an invalid bearer without a Principal → 401", async () => {
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/messages",
        caller: undefined,
        headers: { authorization: "Bearer wrong" },
        body: { to: { verb: "dm", name: "ben" }, body: "hello" },
      }),
      deps(),
    );
    expect(res.status).toBe(401);
  });

  it("legacy NEXUS_API_KEY bearer does not authorize REST after the Principal spine", async () => {
    const res = await handle(
      req({
        method: "GET",
        path: "/api/v1/threads",
        caller: undefined,
        headers: { authorization: `Bearer ${API_KEY}` },
      }),
      deps(),
    );

    expect(res.status).toBe(401);
  });

  it("no API key configured → keyless request is still rejected without a Principal", async () => {
    const saved = process.env.NEXUS_API_KEY;
    delete process.env.NEXUS_API_KEY;
    try {
      const res = await handle(
        {
          method: "GET",
          path: "/api/v1/threads",
          query: {},
          headers: {},
          body: undefined,
        },
        deps(),
      );
      expect(res.status).toBe(401);
    } finally {
      process.env.NEXUS_API_KEY = saved;
    }
  });

  it("an unknown path → 404", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/nonexistent" }),
      deps(),
    );
    expect(res.status).toBe(404);
  });

  it("GET /api/v1/health is unauthenticated and 200", async () => {
    const res = await handle(
      { method: "GET", path: "/api/v1/health", query: {}, headers: {}, body: undefined },
      deps(),
    );
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ status: "ok" });
  });

  it("a command-ingress GatewayError maps to an HTTP status (not a 500/stack leak)", async () => {
    const commands = makeCommandSpy(() => {
      throw new GatewayError(-32602, "invalid params");
    });
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/messages",
        body: { to: { verb: "dm", name: "ben" }, body: "hello" },
      }),
      deps(undefined, commands),
    );
    // -32602 is a client error → 4xx, never a leaked stack.
    expect(res.status).toBeGreaterThanOrEqual(400);
    expect(res.status).toBeLessThan(500);
    expect(JSON.stringify(res.body)).not.toMatch(/at .*\(.*:\d+:\d+\)/); // no stack frames
  });
});

describe("public API — EXTENDED surface dispatches to command ingress", () => {
  it("POST /api/v1/agents → launch (admin.spawn) with parsed params", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/agents",
        body: { kind: "claude", name: "worker-1", role: "agent" },
      }),
      deps(undefined, commands),
    );
    expect([200, 201]).toContain(res.status);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.adminSpawn,
        req: { kind: "claude", name: "worker-1", role: "agent" },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ sessionId: expect.any(String) });
  });

  it("POST /api/v1/topics/:name/subscribe → subscribe with the path-param topic", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "POST", path: "/api/v1/topics/builds/subscribe", body: {} }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.topicSubscribe,
        req: { topic: "builds" },
        caller: API_CALLER,
      },
    ]);
  });

  it("DELETE /api/v1/agents/:id → admin.remove with the path-param name (evict, no kill)", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "DELETE", path: "/api/v1/agents/blake" }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.adminRemove,
        req: { name: "blake", kill: false },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ name: "blake", status: "removed" });
  });

  it("DELETE /api/v1/agents/:id?kill=1 → admin.remove with kill:true", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "DELETE", path: "/api/v1/agents/dylan", query: { kill: "1" } }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.adminRemove,
        req: { name: "dylan", kill: true },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ name: "dylan", status: "killed" });
  });

  it("DELETE /api/v1/agents/:id?kill=true → admin.remove with kill:true", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "DELETE", path: "/api/v1/agents/dylan", query: { kill: "true" } }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.adminRemove,
        req: { name: "dylan", kill: true },
        caller: API_CALLER,
      },
    ]);
  });

  it("POST /api/v1/threads → enqueues thread.create with parsed body", async () => {
    const commands = makeCommandSpy(null);
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/threads",
        body: { name: "newchan", members: ["etan", "ben"] },
      }),
      deps(undefined, commands),
    );
    expect([200, 201]).toContain(res.status);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.threadCreate,
        req: { name: "newchan", members: ["etan", "ben"] },
        caller: API_CALLER,
      },
    ]);
  });

  it("POST /api/v1/threads/:name/join → enqueues thread.join with path param", async () => {
    const commands = makeCommandSpy(null);
    const res = await handle(
      req({ method: "POST", path: "/api/v1/threads/design/join", body: {} }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.threadJoin,
        req: { name: "design" },
        caller: API_CALLER,
      },
    ]);
  });

  it("thread leave/add-member/remove-member routes enqueue thread command intents", async () => {
    const commands = makeCommandSpy(null);

    const leave = await handle(
      req({ method: "POST", path: "/api/v1/threads/design/leave", body: {} }),
      deps(undefined, commands),
    );
    const add = await handle(
      req({
        method: "POST",
        path: "/api/v1/threads/design/members",
        body: { member: "blake" },
      }),
      deps(undefined, commands),
    );
    const remove = await handle(
      req({ method: "DELETE", path: "/api/v1/threads/design/members/blake" }),
      deps(undefined, commands),
    );

    expect(leave.status).toBe(200);
    expect(add.status).toBe(201);
    expect(remove.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.threadLeave,
        req: { name: "design" },
        caller: API_CALLER,
      },
      {
        kind: COMMAND_KINDS.threadAddMember,
        req: { name: "design", member: "blake" },
        caller: API_CALLER,
      },
      {
        kind: COMMAND_KINDS.threadRemoveMember,
        req: { name: "design", member: "blake" },
        caller: API_CALLER,
      },
    ]);
  });

  it("thread archive/delete routes enqueue thread lifecycle command intents", async () => {
    const commands = makeCommandSpy();

    const archive = await handle(
      req({ method: "POST", path: "/api/v1/threads/design/archive", body: {} }),
      deps(undefined, commands),
    );
    const remove = await handle(
      req({ method: "DELETE", path: "/api/v1/threads/design" }),
      deps(undefined, commands),
    );

    expect(archive.status).toBe(200);
    expect(remove.status).toBe(200);
    expect(archive.body).toEqual({ name: "design", archived: true });
    expect(remove.body).toEqual({ name: "design", deleted: true });
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.threadArchive,
        req: { name: "design" },
        caller: API_CALLER,
      },
      {
        kind: COMMAND_KINDS.threadDelete,
        req: { name: "design" },
        caller: API_CALLER,
      },
    ]);
  });

  it("PATCH /api/v1/threads/:name enqueues thread.rename", async () => {
    const commands = makeCommandSpy({ name: "research", previous: "design" });

    const res = await handle(
      req({
        method: "PATCH",
        path: "/api/v1/threads/design",
        body: { name: "research" },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ name: "research", previous: "design" });
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.threadRename,
        req: { name: "design", newName: "research" },
        caller: API_CALLER,
      },
    ]);
  });

  it("POST /api/v1/agents/:id/project → admin.assignProject with path-param name + body project", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/ben/project",
        body: { project: "lens" },
      }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.adminAssignProject,
        req: { name: "ben", project: "lens" },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ name: "ben", project: "lens" });
  });

  it("POST /api/v1/agents/:id/tier → admin.grantTier with path-param name + body tier", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/ben/tier",
        body: { tier: Tier.Admin },
      }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.adminGrantTier,
        req: { name: "ben", tier: Tier.Admin },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ name: "ben", tier: Tier.Admin });
  });

  it("POST /api/v1/agents/:id/access → agent.grantAccess with path-param name", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/ben/access",
        body: { principal: "alice", role: "viewer" },
      }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.agentGrantAccess,
        req: { name: "ben", principal: "alice", role: "viewer" },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ name: "ben", principal: "alice", role: "viewer" });
  });

  it("DELETE /api/v1/agents/:id/access/:principal → agent.revokeAccess", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "DELETE",
        path: "/api/v1/agents/ben/access/alice",
        query: { project: "ops" },
      }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.agentRevokeAccess,
        req: { name: "ben", principal: "alice", project: "ops" },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ name: "ben", principal: "alice", revoked: true });
  });

  it("POST /api/v1/agents/:id/owner → agent.transferOwner with path-param name", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/ben/owner",
        body: { owner: "alice", project: "ops" },
      }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.agentTransferOwner,
        req: { name: "ben", owner: "alice", project: "ops" },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ name: "ben", owner: "alice", project: "ops" });
  });

  it("agent operation routes preserve stable a_* ids in command payloads", async () => {
    const commands = makeCommandSpy();

    await handle(
      req({ method: "DELETE", path: "/api/v1/agents/a_ben", query: { kill: "1" } }),
      deps(undefined, commands),
    );
    await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/a_ben/project",
        body: { project: "lens" },
      }),
      deps(undefined, commands),
    );
    await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/a_ben/tier",
        body: { tier: Tier.Admin },
      }),
      deps(undefined, commands),
    );
    await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/a_ben/access",
        body: { principal: "a_alice", role: "viewer" },
      }),
      deps(undefined, commands),
    );
    await handle(
      req({
        method: "DELETE",
        path: "/api/v1/agents/a_ben/access/a_alice",
        query: { project: "ops" },
      }),
      deps(undefined, commands),
    );
    await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/a_ben/owner",
        body: { owner: "a_owner", project: "ops" },
      }),
      deps(undefined, commands),
    );

    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.adminRemove,
        req: { name: "a_ben", agentId: "a_ben", kill: true },
        caller: API_CALLER,
      },
      {
        kind: COMMAND_KINDS.adminAssignProject,
        req: { name: "a_ben", agentId: "a_ben", project: "lens" },
        caller: API_CALLER,
      },
      {
        kind: COMMAND_KINDS.adminGrantTier,
        req: { name: "a_ben", agentId: "a_ben", tier: Tier.Admin },
        caller: API_CALLER,
      },
      {
        kind: COMMAND_KINDS.agentGrantAccess,
        req: {
          name: "a_ben",
          agentId: "a_ben",
          principal: "a_alice",
          principalAgentId: "a_alice",
          role: "viewer",
        },
        caller: API_CALLER,
      },
      {
        kind: COMMAND_KINDS.agentRevokeAccess,
        req: {
          name: "a_ben",
          agentId: "a_ben",
          principal: "a_alice",
          principalAgentId: "a_alice",
          project: "ops",
        },
        caller: API_CALLER,
      },
      {
        kind: COMMAND_KINDS.agentTransferOwner,
        req: {
          name: "a_ben",
          agentId: "a_ben",
          owner: "a_owner",
          ownerAgentId: "a_owner",
          project: "ops",
        },
        caller: API_CALLER,
      },
    ]);
  });

  it("POST /api/v1/agents/:id/credentials → agent.credential.create with path identity", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/ben/credentials",
        body: { label: "headed codex", purpose: "runtime", scopes: ["runtime:register"] },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(201);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.agentCredentialCreate,
        req: {
          name: "ben",
          label: "headed codex",
          purpose: "runtime",
          scopes: ["runtime:register"],
        },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ credentialId: "cred_command", secret: expect.any(String) });
  });

  it("POST /api/v1/agents/:id/credentials defaults runtime registration scope", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/agents/a_ben/credentials",
        body: { label: "local" },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(201);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.agentCredentialCreate,
        req: {
          name: "a_ben",
          agentId: "a_ben",
          label: "local",
          scopes: ["runtime:register"],
        },
        caller: API_CALLER,
      },
    ]);
  });

  it("DELETE /api/v1/agents/:id/credentials/:credentialId → agent.credential.revoke", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "DELETE", path: "/api/v1/agents/ben/credentials/cred_ben_laptop" }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.agentCredentialRevoke,
        req: { credentialId: "cred_ben_laptop" },
        caller: API_CALLER,
      },
    ]);
    expect(res.body).toMatchObject({ credentialId: "cred_ben_laptop", revoked: true });
  });

  it("POST /api/v1/register accepts durable agentId + runtimeCredential", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/register",
        body: {
          agentId: "a_ben",
          name: "ben",
          harness: "claude",
          harnessSessionId: "hs_ben",
          project: "nexus",
          clientKey: "ck_ben",
          runtimeCredential: "nexus_rt_secret",
          tier: "agent",
          kind: "agent",
        },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(201);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.identityRegister,
        req: {
          agentId: "a_ben",
          name: "ben",
          harness: "claude",
          harnessSessionId: "hs_ben",
          project: "nexus",
          clientKey: "ck_ben",
          runtimeCredential: "nexus_rt_secret",
          tier: "agent",
          kind: "agent",
        },
        caller: API_CALLER,
      },
    ]);
  });

  it("POST /api/v1/rename → identity.rename for the authenticated caller", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/rename",
        caller: { name: "etan", project: "nexus" },
        body: { name: "alex" },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.identityRename,
        req: { name: "alex" },
        caller: { name: "etan", project: "nexus" },
      },
    ]);
    expect(res.body).toMatchObject({ name: "alex", previous: "etan" });
  });

  it("GET /api/v1/capabilities + /api/v1/openapi disclose the REST command spine", async () => {
    const registeredRoutes = restCapabilityRoutes();
    const capabilities = await handle(
      req({ method: "GET", path: "/api/v1/capabilities" }),
      deps(),
    );
    const openapi = await handle(
      req({ method: "GET", path: "/api/v1/openapi" }),
      deps(),
    );

    expect(capabilities.status).toBe(200);
    expect(capabilities.body).toMatchObject({
      version: 1,
      routes: expect.arrayContaining([
        expect.objectContaining({ method: "GET", path: "/api/v1/health", auth: false, read: "health" }),
        expect.objectContaining({ method: "GET", path: "/api/v1/search", read: "search.messages" }),
        expect.objectContaining({ method: "GET", path: "/api/v1/messages/{id}", read: "message.get" }),
        expect.objectContaining({ method: "POST", path: "/api/v1/auth/operator-token", credential: "auth.operatorToken" }),
        expect.objectContaining({ method: "POST", path: "/api/v1/auth/tokens", credential: "auth.token.issue" }),
        expect.objectContaining({ method: "POST", path: "/api/v1/auth/tokens/refresh", auth: false, credential: "auth.token.refresh" }),
        expect.objectContaining({ method: "DELETE", path: "/api/v1/auth/tokens/{tokenId}", credential: "auth.token.revoke" }),
        expect.objectContaining({ method: "GET", path: "/api/v1/threads/{name}/header", read: "thread.header" }),
        expect.objectContaining({ method: "GET", path: "/api/v1/threads/{name}/history" }),
        expect.objectContaining({ method: "GET", path: "/api/v1/threads/{name}/members", read: "thread.members" }),
        expect.objectContaining({ method: "GET", path: "/api/v1/agents/{id}" }),
        expect.objectContaining({ method: "POST", path: "/api/v1/agents", command: COMMAND_KINDS.adminSpawn }),
        expect.objectContaining({ method: "POST", path: "/api/v1/agents/{id}/credentials" }),
        expect.objectContaining({
          method: "DELETE",
          path: "/api/v1/agents/{id}",
          commands: [
            COMMAND_KINDS.adminRemove,
            COMMAND_KINDS.adminEvict,
            COMMAND_KINDS.adminDelete,
          ],
        }),
        expect.objectContaining({ method: "POST", path: "/api/v1/agents/{id}/tier", command: COMMAND_KINDS.adminGrantTier }),
        expect.objectContaining({ method: "POST", path: "/api/v1/agents/{id}/access", command: COMMAND_KINDS.agentGrantAccess }),
        expect.objectContaining({ method: "DELETE", path: "/api/v1/agents/{id}/access/{principal}", command: COMMAND_KINDS.agentRevokeAccess }),
        expect.objectContaining({ method: "POST", path: "/api/v1/agents/{id}/owner", command: COMMAND_KINDS.agentTransferOwner }),
        expect.objectContaining({ method: "POST", path: "/api/v1/admin/channel", command: COMMAND_KINDS.adminChannel }),
        expect.objectContaining({ method: "POST", path: "/api/v1/inbox/ack-threads", command: COMMAND_KINDS.inboxAckThreads }),
        expect.objectContaining({ method: "POST", path: "/api/v1/sources/{name}/push", auth: false, command: COMMAND_KINDS.sourcePush }),
        expect.objectContaining({ method: "POST", path: "/api/v1/threads/{name}/archive", command: COMMAND_KINDS.threadArchive }),
        expect.objectContaining({ method: "PATCH", path: "/api/v1/threads/{name}", command: COMMAND_KINDS.threadRename }),
        expect.objectContaining({ method: "DELETE", path: "/api/v1/threads/{name}", command: COMMAND_KINDS.threadDelete }),
        expect.objectContaining({ method: "POST", path: "/api/v1/routing-rules", status: 501 }),
      ]),
    });
    expect((capabilities.body as { routes: unknown[] }).routes).toEqual(registeredRoutes);

    expect(openapi.status).toBe(200);
    expect(openapi.body).toMatchObject({
      openapi: expect.stringMatching(/^3\./),
      paths: expect.objectContaining({
        "/api/v1/agents/{id}": expect.any(Object),
        "/api/v1/agents/{id}/credentials": expect.any(Object),
        "/api/v1/agents/{id}/tier": expect.any(Object),
        "/api/v1/agents/{id}/access": expect.any(Object),
        "/api/v1/agents/{id}/access/{principal}": expect.any(Object),
        "/api/v1/auth/operator-token": expect.any(Object),
        "/api/v1/auth/tokens": expect.any(Object),
        "/api/v1/auth/tokens/refresh": expect.any(Object),
        "/api/v1/auth/tokens/{tokenId}": expect.any(Object),
        "/api/v1/health": expect.any(Object),
        "/api/v1/inbox/ack-threads": expect.any(Object),
        "/api/v1/messages/{id}": expect.any(Object),
        "/api/v1/search": expect.any(Object),
        "/api/v1/sources/{name}/push": expect.any(Object),
        "/api/v1/threads/{name}/header": expect.any(Object),
        "/api/v1/threads/{name}/archive": expect.any(Object),
        "/api/v1/threads/{name}": expect.any(Object),
        "/api/v1/routing-rules": expect.any(Object),
      }),
    });
    const openApiPaths = (openapi.body as { paths: Record<string, Record<string, unknown>> }).paths;
    for (const route of registeredRoutes) {
      expect(openApiPaths[route.path]?.[route.method.toLowerCase()]).toBeTruthy();
    }
    expect(openApiPaths["/api/v1/threads/{name}"]).toEqual(
      expect.objectContaining({
        patch: expect.any(Object),
        delete: expect.any(Object),
      }),
    );
    expect(openApiPaths["/api/v1/agents/{id}"]).toEqual(
      expect.objectContaining({
        get: expect.any(Object),
        delete: expect.any(Object),
      }),
    );
  });

  it("POST /api/v1/routing-rules is explicitly 501 and never dispatches", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/routing-rules",
        body: { source: "ci", to: "ben" },
      }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(501);
    expect(res.body).toMatchObject({
      error: { code: "not_implemented" },
    });
    expect(commands.calls).toHaveLength(0);
  });
});

/** Build a signed push request using the same algorithm as the producer. */
function makeSignedPushReq(opts: {
  source: string;
  token: string;
  rawBody: string;
  tsOverride?: number;
  badSig?: boolean;
  topic?: string;
}): ApiRequest {
  const ts = opts.tsOverride ?? Date.now();
  const sig = opts.badSig
    ? "sha256=deadbeef"
    : "sha256=" + createHmac("sha256", opts.token).update(`${ts}.${opts.rawBody}`).digest("hex");
  return {
    method: "POST",
    path: `/api/v1/sources/${opts.source}/push`,
    query: opts.topic ? { topic: opts.topic } : {},
    headers: {
      authorization: `Bearer ${API_KEY}`,
      "x-nexus-timestamp": String(ts),
      "x-nexus-signature": sig,
      "content-type": "application/json",
    },
    body: JSON.parse(opts.rawBody) as unknown,
    rawBody: opts.rawBody,
  };
}

describe("POST /sources/:name/push — signed source push edge", () => {
  const SOURCE = "github-ci";
  const TOKEN = "src_github-citoken";
  const rawBody = JSON.stringify({ summary: "Deploy started", body: "web v1.4.2 → prod", meta: { service: "web" } });

  it("valid signature → 201 and source.push command is enqueued with correct params", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      makeSignedPushReq({ source: SOURCE, token: TOKEN, rawBody }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(201);
    expect(commands.calls).toHaveLength(1);
    expect(commands.calls[0]).toMatchObject({
      kind: COMMAND_KINDS.sourcePush,
      req: { source: SOURCE, body: "web v1.4.2 → prod" },
      caller: {
        name: SOURCE,
        project: "default",
        kind: Kind.Notification,
        tier: Tier.Agent,
        credentialFacet: "source",
        scopes: ["source:push"],
      },
      opts: {
        idempotencyKey: expect.stringMatching(/^source-push:github-ci:/),
      },
    });
  });

  it("identical signed replay submits the same durable idempotency key", async () => {
    const commands = makeCommandSpy();
    const signed = makeSignedPushReq({
      source: SOURCE,
      token: TOKEN,
      rawBody,
      tsOverride: Date.now(),
    });

    const first = await handle(signed, deps(undefined, commands));
    const replay = await handle(signed, deps(undefined, commands));

    expect(first.status).toBe(201);
    expect(replay.status).toBe(201);
    expect(commands.calls).toHaveLength(2);
    expect(commandReq(commands.calls[0]?.opts).idempotencyKey).toBe(
      commandReq(commands.calls[1]?.opts).idempotencyKey,
    );
  });

  it("valid signature with topic query → source.push carries the topic", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      makeSignedPushReq({ source: SOURCE, token: TOKEN, rawBody, topic: "deploys" }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(201);
    expect(commands.calls[0]).toMatchObject({
      kind: COMMAND_KINDS.sourcePush,
      req: { source: SOURCE, topic: "deploys" },
    });
  });

  it("bad signature → 401, push NOT called", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      makeSignedPushReq({ source: SOURCE, token: TOKEN, rawBody, badSig: true }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(401);
    expect(commands.calls).toHaveLength(0);
  });

  it("stale timestamp (now - 600s) → 401, push NOT called", async () => {
    const commands = makeCommandSpy();
    const staleTs = Math.floor(Date.now() / 1000) - 600;
    const res = await handle(
      makeSignedPushReq({ source: SOURCE, token: TOKEN, rawBody, tsOverride: staleTs }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(401);
    expect(commands.calls).toHaveLength(0);
  });

  it("missing timestamp header → 401, push NOT called", async () => {
    const commands = makeCommandSpy();
    const ts = Math.floor(Date.now() / 1000);
    const sig = "sha256=" + createHmac("sha256", TOKEN).update(`${ts}.${rawBody}`).digest("hex");
    const res = await handle(
      {
        method: "POST",
        path: `/api/v1/sources/${SOURCE}/push`,
        query: {},
        headers: {
          authorization: `Bearer ${API_KEY}`,
          "x-nexus-signature": sig,
        },
        body: JSON.parse(rawBody) as unknown,
        rawBody,
      },
      deps(undefined, commands),
    );

    expect(res.status).toBe(401);
    expect(commands.calls).toHaveLength(0);
  });

  it("missing source → 404, push NOT called", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      makeSignedPushReq({ source: "nonexistent", token: TOKEN, rawBody }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(404);
    expect(commands.calls).toHaveLength(0);
  });

  it("disabled source → 403, push NOT called", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      makeSignedPushReq({ source: "alerts-hook", token: "src_alerts-hooktoken", rawBody }),
      deps(undefined, commands),
    );

    expect(res.status).toBe(403);
    expect(commands.calls).toHaveLength(0);
  });
});

describe("public API — source management routes (task 8a)", () => {
  it("GET /api/v1/sources → reads the store projection", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/sources" }),
      deps(),
    );
    expect(res.status).toBe(200);
    const body = res.body as { sources: unknown[] };
    expect(Array.isArray(body.sources)).toBe(true);
    expect(body.sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "github-ci", enabled: true }),
        expect.objectContaining({ name: "alerts-hook", enabled: false }),
      ]),
    );
  });

  it("POST /api/v1/sources with name+topic → enqueues source.register", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/sources",
        body: { name: "myapp", topic: "deploys" },
      }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(201);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.sourceRegister,
        req: { name: "myapp", topic: "deploys" },
        caller: API_CALLER,
      },
    ]);
  });

  it("POST /api/v1/sources with name only (no topic) → enqueues source.register", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({
        method: "POST",
        path: "/api/v1/sources",
        body: { name: "myapp" },
      }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(201);
    expect(commands.calls).toEqual([
      {
        kind: COMMAND_KINDS.sourceRegister,
        req: { name: "myapp" },
        caller: API_CALLER,
      },
    ]);
  });

  it("POST /api/v1/sources with empty name → 400 (Zod)", async () => {
    const res = await handle(
      req({ method: "POST", path: "/api/v1/sources", body: { name: "" } }),
      deps(),
    );
    expect(res.status).toBe(400);
  });

  it("GET /api/v1/sources/:name → reads one source from the store projection", async () => {
    const res = await handle(
      req({ method: "GET", path: "/api/v1/sources/github-ci" }),
      deps(),
    );
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ name: "github-ci", topic: "builds", enabled: true });
  });

  it("POST /api/v1/sources/:name/enable → enqueues source.enable", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "POST", path: "/api/v1/sources/myapp/enable", body: {} }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      { kind: COMMAND_KINDS.sourceEnable, req: { name: "myapp" }, caller: API_CALLER },
    ]);
  });

  it("POST /api/v1/sources/:name/disable → enqueues source.disable", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "POST", path: "/api/v1/sources/myapp/disable", body: {} }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      { kind: COMMAND_KINDS.sourceDisable, req: { name: "myapp" }, caller: API_CALLER },
    ]);
  });

  it("POST /api/v1/sources/:name/rotate → enqueues source.rotate", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "POST", path: "/api/v1/sources/myapp/rotate", body: {} }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      { kind: COMMAND_KINDS.sourceRotate, req: { name: "myapp" }, caller: API_CALLER },
    ]);
    const body = res.body as { name: string; token: string };
    expect(typeof body.token).toBe("string");
  });

  it("DELETE /api/v1/sources/:name → enqueues source.remove", async () => {
    const commands = makeCommandSpy();
    const res = await handle(
      req({ method: "DELETE", path: "/api/v1/sources/myapp" }),
      deps(undefined, commands),
    );
    expect(res.status).toBe(200);
    expect(commands.calls).toEqual([
      { kind: COMMAND_KINDS.sourceRemove, req: { name: "myapp" }, caller: API_CALLER },
    ]);
  });

  it("GET /api/v1/sources/:name/push is NOT a management route (pattern mismatch)", async () => {
    // /sources/:name/push uses POST, not GET — this should 405 or 404 since there's no GET route for it
    const res = await handle(
      req({ method: "GET", path: "/api/v1/sources/myapp/push" }),
      deps(),
    );
    expect([404, 405]).toContain(res.status);
  });
});

describe("public API — thin: handlers never reach a DB write path", () => {
  it("a write call into the read DB handle is never reachable from a handler", async () => {
    // Spy the read handle's write-ish methods; assert none are invoked across a
    // representative spread of routes (reads + writes).
    const insertSpy = vi.spyOn(db as unknown as { insert: (...a: unknown[]) => unknown }, "insert");
    const updateSpy = vi.spyOn(db as unknown as { update: (...a: unknown[]) => unknown }, "update");
    const deleteSpy = vi.spyOn(db as unknown as { delete: (...a: unknown[]) => unknown }, "delete");

    await handle(req({ method: "GET", path: "/api/v1/threads" }), deps());
    await handle(req({ method: "GET", path: "/api/v1/members" }), deps());
    await handle(
      req({
        method: "POST",
        path: "/api/v1/notify",
        body: { source: "ci", payload: {} },
      }),
      deps(),
    );
    await handle(
      req({
        method: "POST",
        path: "/api/v1/messages",
        caller: { name: "etan", project: "default" },
        body: { to: { verb: "dm", name: "ben" }, body: "hi" },
      }),
      deps(),
    );

    expect(insertSpy).not.toHaveBeenCalled();
    expect(updateSpy).not.toHaveBeenCalled();
    expect(deleteSpy).not.toHaveBeenCalled();
  });
});
