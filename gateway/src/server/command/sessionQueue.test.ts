import { createClient, type Client, type InStatement, type Transaction } from "@libsql/client";
import { describe, expect, it, vi } from "vitest";

import {
  handleConversationQueueGet,
  handleConversationQueuePost,
} from "./sessionQueue";
import { CommandQueueState, SteerCapability } from "@shared/types";

const DDL = `
CREATE TABLE agent_runtimes (agent_id TEXT, runtime_id TEXT, active INTEGER, stopped_at INTEGER);
CREATE TABLE sessions (
  session_id TEXT PRIMARY KEY,
  agent_id TEXT,
  name TEXT,
  agent TEXT,
  transport TEXT,
  project TEXT,
  created_at INTEGER,
  client_key TEXT,
  kind TEXT,
  tier TEXT
);
CREATE TABLE agent_session_turns (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  status TEXT NOT NULL,
  first_stream_event_id INTEGER NOT NULL,
  last_stream_event_id INTEGER NOT NULL,
  started_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  finalized_at INTEGER
);
CREATE TABLE command_intents (
  command_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  project TEXT NOT NULL,
  caller_name TEXT NOT NULL,
  caller_session_id TEXT,
  caller_runtime_id TEXT,
  caller_client_key TEXT,
  caller_tier TEXT,
  caller_agent_id TEXT,
  caller_principal_id TEXT,
  caller_kind TEXT,
  request_json TEXT NOT NULL,
  error_json TEXT,
  revision INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL,
  claimed_at INTEGER,
  started_at INTEGER,
  lease_until INTEGER,
  completed_at INTEGER
);
CREATE TABLE command_intent_events (
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
CREATE TABLE command_queue_mutations (
  project TEXT NOT NULL,
  client_mutation_id TEXT NOT NULL,
  request_json TEXT NOT NULL,
  response_status INTEGER,
  response_json TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (project, client_mutation_id)
);
CREATE TRIGGER queue_insert BEFORE INSERT ON command_intents
WHEN NEW.kind IN ('harness.prompt', 'harness.steer')
BEGIN
  INSERT INTO command_intent_events
    (project, session_id, command_id, client_message_id, state, mode, revision, created_at)
  VALUES
    (NEW.project, 's_otto', NEW.command_id, json_extract(NEW.request_json, '$.clientMessageId'),
     'queued', CASE WHEN NEW.kind = 'harness.steer' THEN 'redirect' ELSE 'queue' END,
     NEW.revision, NEW.created_at);
END;
CREATE TRIGGER queue_update BEFORE UPDATE ON command_intents
WHEN NEW.revision != OLD.revision
BEGIN
  INSERT INTO command_intent_events
    (project, session_id, command_id, client_message_id, state, mode, revision, created_at)
  VALUES
    (NEW.project, 's_otto', NEW.command_id, json_extract(NEW.request_json, '$.clientMessageId'),
     CASE WHEN NEW.status = 'pending' THEN 'queued'
          WHEN NEW.status = 'cancelled' THEN 'cancelled'
          WHEN NEW.status = 'claimed' AND NEW.started_at IS NULL THEN 'claimed'
          WHEN NEW.status = 'claimed' THEN 'started'
          WHEN NEW.status = 'done' THEN 'completed' ELSE 'failed' END,
     CASE WHEN NEW.kind = 'harness.steer' THEN 'redirect' ELSE 'queue' END,
     NEW.revision, COALESCE(NEW.completed_at, NEW.started_at, NEW.claimed_at, NEW.created_at));
END;
`;

let activeDb: Client | undefined;

async function dbFor(
  harness = "codex",
  transport = "codex-appserver",
): Promise<Client> {
  // libSQL's bare `:memory:` URL is connection-local and therefore loses its schema when a
  // transaction checks out another connection. Keep one shared-memory client and clear it between
  // cases so the tests exercise the production write transaction without touching disk.
  if (!activeDb) {
    activeDb = createClient({ url: "file::memory:?cache=shared" });
    await activeDb.executeMultiple(DDL);
  } else {
    await activeDb.batch([
      "DELETE FROM command_queue_mutations",
      "DELETE FROM command_intent_events",
      "DELETE FROM command_intents",
      "DELETE FROM agent_session_turns",
      "DELETE FROM sessions",
      "DELETE FROM agent_runtimes",
      "DELETE FROM sqlite_sequence WHERE name = 'command_intent_events'",
    ]);
  }
  const db = activeDb;
  await db.execute({
    sql:
      "INSERT INTO sessions (session_id, agent_id, name, agent, transport, project, created_at) " +
      "VALUES (?, ?, ?, ?, ?, 'default', 1)",
    args: ["s_otto", "a_otto", "otto", harness, transport],
  });
  return db;
}

describe("exact queue mutation authority", () => {
  it("keeps a legacy original binding when only the retained identity owner remains", async () => {
    const db = await dbFor();
    await db.execute("INSERT INTO agent_runtimes VALUES ('a_otto', 's_otto', 0, 2)");
    await insertCommand(db, "cmd_retained_legacy", "error", 1);
    await db.execute({sql: "UPDATE command_intents SET request_json = ?", args:[JSON.stringify({agentId:"a_otto", text:"retained"})]});
    await db.execute("DELETE FROM sessions");
    const response = await handleConversationQueueGet(new Request("http://localhost/api/conversation/prompt?agentId=a_otto&expectedSessionId=s_otto"), deps(db));
    expect(await response.json()).toMatchObject({sessionId:"s_otto", commands:[{commandId:"cmd_retained_legacy", sessionId:"s_otto"}]});
  });
  it("scans ownerless and malformed legacy facts without inventing a lane gap", async () => {
    const db = await dbFor();
    await insertCommand(db, "cmd_ownerless", "error", 1);
    await db.execute("UPDATE sessions SET agent_id = NULL");
    for (let i = 0; i < 505; i++) {
      await insertCommand(db, `cmd_bad_${i}`, "pending", i + 2);
      await db.execute({
        sql: "UPDATE command_intents SET request_json = ? WHERE command_id = ?",
        args: [
          JSON.stringify({
            agentId: "a_otto",
            expectedSessionId: null,
            sessionId: "s_otto",
            text: "private",
          }),
          `cmd_bad_${i}`,
        ],
      });
    }
    const read = async (after: number) =>
      (
        await handleConversationQueueGet(
          new Request(
            `http://localhost/api/conversation/prompt?eventsAfter=${after}`,
          ),
          deps(db),
        )
      ).json();
    expect(await read(0)).toMatchObject({
      events: [],
      nextSeq: 500,
      gap: false,
    });
    expect(await read(500)).toMatchObject({
      events: [],
      nextSeq: 506,
      gap: false,
    });
  });

  it.each(["-32602", true, 2147483648, -2147483649])(
    "omits unstructured error code %s",
    async (code) => {
      const db = await dbFor();
      await insertCommand(db, "cmd_bad_code", "error", 1);
      await db.execute({
        sql: "UPDATE command_intents SET error_json = ?",
        args: [JSON.stringify({ code, message: "failure" })],
      });
    const response = await handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?name=otto"),
      deps(db),
    );
      const body = await response.json();
      expect(body.commands[0]).not.toHaveProperty("errorCode");
      expect(body.commands[0].correlationOwned).toBe(false);
    },
  );

  it("reads retained S1 without relabelling S2 or trusting forged pairs", async () => {
    const db = await dbFor();
    await db.executeMultiple(
      "INSERT INTO agent_runtimes VALUES ('a_otto','s_otto',0,2), ('a_otto','s_new',1,NULL), ('a_other','s_other',1,NULL); INSERT INTO sessions (session_id,agent_id,name,agent,transport,project,created_at) VALUES ('s_new','a_otto','otto-new','codex','codex-appserver','default',2);",
    );
    await insertCommand(db, "cmd_old", "error", 1);
    await db.execute({
      sql: "UPDATE command_intents SET request_json = ?, error_json = ? WHERE command_id = 'cmd_old'",
      args: [
        JSON.stringify({
          agentId: "a_otto",
        expectedSessionId: "s_otto",
          text: "original",
        }),
        JSON.stringify({ code: -32602, message: "stale" }),
      ],
    });
    for (let i = 0; i < 105; i++) {
      await insertCommand(db, `cmd_new_${i}`, "pending", 2 + i);
      await db.execute({
        sql: "UPDATE command_intents SET request_json = ? WHERE command_id = ?",
        args: [
          JSON.stringify({
            agentId: "a_otto",
            expectedSessionId: "s_new",
            text: "new",
          }),
          `cmd_new_${i}`,
        ],
      });
    }
    const response = await handleConversationQueueGet(
      new Request(
        "http://localhost/api/conversation/prompt?agentId=a_otto&expectedSessionId=s_otto",
      ),
      deps(db),
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      sessionId: "s_otto",
      commands: [
        {
          commandId: "cmd_old",
          sessionId: "s_otto",
          errorCode: -32602,
          correlationOwned: false,
        },
      ],
    });
    const foreign = await handleConversationQueueGet(
      new Request(
        "http://localhost/api/conversation/prompt?agentId=a_otto&expectedSessionId=s_other",
      ),
      deps(db),
    );
    expect(foreign.status).toBe(404);
    await db.execute(
      "UPDATE sessions SET agent_id = NULL WHERE session_id = 's_otto'",
    );
    const unowned = await handleConversationQueueGet(
      new Request(
        "http://localhost/api/conversation/prompt?agentId=a_otto&expectedSessionId=s_otto",
      ),
      deps(db),
    );
    expect(unowned.status).toBe(404);
    for (const query of [
      "agentId=a_otto&expectedSessionId=",
      "name=otto&expectedSessionId=s_otto",
    ]) {
      expect(
        (
          await handleConversationQueueGet(
            new Request(`http://localhost/api/conversation/prompt?${query}`),
            deps(db),
          )
        ).status,
      ).toBe(400);
    }
  });
  const request = (extra: Record<string, unknown> = {}) =>
    new Request("http://localhost/api/conversation/prompt", {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        agentId: "a_otto",
        expectedSessionId: "s_otto",
        action: "redirect_now",
        commandId: "cmd_exact",
        expectedRevision: 1,
        clientMutationId: "mut_exact",
        ...extra,
      }),
    });

  it.each([undefined, "s_foreign", "s_otto"])(
    "redirect requires the immutable original selector %s",
    async (original) => {
      const db = await dbFor();
      await db.execute(
        "INSERT INTO agent_runtimes VALUES ('a_otto', 's_otto', 1, NULL)",
      );
      await insertActiveTurn(db);
      await insertCommand(db, "cmd_exact", "pending", 1);
      const bytes = JSON.stringify(
        {
          name: "otto",
          agentId: "a_otto",
          expectedSessionId: original,
          text: "original",
          clientMessageId: "cm_exact",
        },
        null,
        2,
      );
      await db.execute({
        sql: "UPDATE command_intents SET request_json = ? WHERE command_id = 'cmd_exact'",
        args: [bytes],
      });
      const response = await handleConversationQueuePost(request(), deps(db));
      expect(response.status).toBe(original === "s_otto" ? 200 : 409);
      expect(
        (await db.execute("SELECT request_json FROM command_intents")).rows[0]
          ?.request_json,
      ).toBe(bytes);
    },
  );

  it("uses active runtime authority inside the transaction, after committed replay", async () => {
    const db = await dbFor();
    await db.execute(
      "INSERT INTO agent_runtimes VALUES ('a_otto', 's_otto', 1, NULL)",
    );
    await insertCommand(db, "cmd_exact", "pending", 1);
    const counted = countedDb(db);
    const first = await handleConversationQueuePost(
      request({ action: "cancel" }),
      deps(counted.db),
    );
    expect(first.status).toBe(200);
    const receipt = await first.json();
    const canonical = JSON.parse(
      String(
        (await db.execute("SELECT request_json FROM command_queue_mutations"))
          .rows[0]?.request_json,
      ),
    );
    expect(canonical.expectedSessionId).toBe("s_otto");
    expect(counted.direct.some((sql) => sql.includes("agent_runtimes"))).toBe(
      false,
    );
    expect(
      counted.transaction.some((sql) => sql.includes("agent_runtimes")),
    ).toBe(true);
    await db.execute("UPDATE agent_runtimes SET runtime_id = 's_new'");
    const replay = await handleConversationQueuePost(
      request({ action: "cancel" }),
      deps(db),
    );
    expect(await replay.json()).toEqual(receipt);
    const conflicting = await handleConversationQueuePost(
      request({ action: "cancel", expectedSessionId: "s_new" }),
      deps(db),
    );
    expect(conflicting.status).toBe(409);
    const stale = await handleConversationQueuePost(
      request({ action: "cancel", clientMutationId: "mut_new" }),
      deps(db),
    );
    expect(stale.status).toBe(409);
  });

  it.each([null, "", " ", {}])(
    "rejects malformed selector before daemon mutation: %j",
    async (expectedSessionId) => {
      const daemonQueueMutation = vi.fn();
      const response = await handleConversationQueuePost(
        request({ expectedSessionId }),
        { env: { NEXUS_WEB_AUTH_MODE: "local" }, daemonQueueMutation },
      );
      expect(response.status).toBe(400);
      expect(daemonQueueMutation).not.toHaveBeenCalled();
    },
  );

  it.each(["s_otto", "s_foreign", undefined])(
    "checks actual daemon mutation receipt session %s",
    async (sessionId) => {
      const daemonQueueMutation = vi.fn(async (_input: unknown) => ({
        status: 200,
        body: { sessionId, clientMutationId: "mut_exact" },
      }));
      const response = await handleConversationQueuePost(request(), {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        daemonQueueMutation,
      });
      expect(response.status).toBe(sessionId === "s_otto" ? 200 : 502);
      expect(daemonQueueMutation.mock.calls[0]?.[0]).toMatchObject({
        request: { expectedSessionId: "s_otto" },
      });
    },
  );
});

async function insertCommand(
  db: Client,
  id: string,
  status: string,
  createdAt: number,
  startedAt: number | null = null,
): Promise<void> {
  await db.execute({
    sql:
      "INSERT INTO command_intents (command_id, kind, status, project, caller_name, request_json, " +
      "created_at, claimed_at, started_at, lease_until, completed_at) " +
      "VALUES (?, 'harness.prompt', ?, 'default', 'Alex', ?, ?, ?, ?, ?, ?)",
    args: [
      id,
      status,
      JSON.stringify({
        name: "otto",
        text: `text ${id}`,
        clientMessageId: `cm_${id}`,
      }),
      createdAt,
      status === "claimed" ? createdAt + 1 : null,
      startedAt,
      status === "claimed" ? createdAt + 60_000 : null,
      ["done", "error", "cancelled"].includes(status) ? createdAt + 10 : null,
    ],
  });
}

async function insertActiveTurn(db: Client): Promise<void> {
  await db.execute(
    "INSERT INTO agent_session_turns VALUES " +
      "('turn_active', 's_otto', 'streaming', 1, 1, 1, 1, NULL)",
  );
}

const deps = (db: Client) => ({
  env: { NEXUS_WEB_AUTH_MODE: "local" },
  getWriteDb: async () => db,
  now: () => 9_000,
  observeTurn: (sessionId: string) => ({sessionId, state:"unknown" as any, steerCapability:SteerCapability.NativeSteer}),
});

function countedDb(db: Client): {
  db: Client;
  direct: string[];
  transaction: string[];
} {
  const direct: string[] = [];
  const transaction: string[] = [];
  const sqlOf = (statement: InStatement) =>
    typeof statement === "string" ? statement : statement.sql;
  const wrapTransaction = (tx: Transaction): Transaction =>
    new Proxy(tx, {
      get(target, property) {
        if (property === "execute") {
          return async (statement: InStatement) => {
            transaction.push(sqlOf(statement));
            return target.execute(statement);
          };
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
  const counted = new Proxy(db, {
    get(target, property) {
      if (property === "execute") {
        return async (statement: InStatement) => {
          direct.push(sqlOf(statement));
          return target.execute(statement);
        };
      }
      if (property === "transaction") {
        return async (...args: Parameters<Client["transaction"]>) =>
          wrapTransaction(await target.transaction(...args));
      }
      const value = Reflect.get(target, property, target) as unknown;
      return typeof value === "function" ? value.bind(target) : value;
    },
  }) as Client;
  return { db: counted, direct, transaction };
}

describe("durable session command queue", () => {
  it("does not infer redirect capability from stored Codex metadata", async () => {
    const db = await dbFor();
    await insertCommand(db, "cmd_no_cap", "pending", 1);
    await insertActiveTurn(db);
    const response = await handleConversationQueuePost(new Request("http://localhost/api/conversation/prompt", {
      method:"PATCH", headers:{"content-type":"application/json"},
      body:JSON.stringify({name:"otto", action:"redirect_now", clientMutationId:"no_cap", commandId:"cmd_no_cap", expectedRevision:1}),
    }), {env:{NEXUS_WEB_AUTH_MODE:"local"}, getWriteDb:async () => db});
    expect(response.status).toBe(409);
  });

  it("rejects foreign captured capability and replays a committed redirect after capability loss", async () => {
    const db = await dbFor();
    await insertCommand(db, "cmd_cap", "pending", 1);
    await insertActiveTurn(db);
    const request = (id:string) => new Request("http://localhost/api/conversation/prompt", {method:"PATCH", headers:{"content-type":"application/json"},
      body:JSON.stringify({name:"otto", action:"redirect_now", clientMutationId:id, commandId:"cmd_cap", expectedRevision:1})});
    const foreign = await handleConversationQueuePost(request("foreign"), {...deps(db), observeTurn:() => ({sessionId:"S_foreign", state:"unknown" as any, steerCapability:SteerCapability.NativeSteer})});
    expect(foreign.status).toBe(409);
    const accepted = await handleConversationQueuePost(request("accepted"), deps(db));
    expect(accepted.status).toBe(200);
    const replay = await handleConversationQueuePost(request("accepted"), {...deps(db), observeTurn:() => {throw new Error("adapter lost");}});
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual(await accepted.json());
  });

  it("sends production mutations to the daemon instead of opening a gateway transaction", async () => {
    const daemonQueueMutation = vi.fn(async () => ({
      status: 200,
      body: {
        clientMutationId: "mut_daemon",
        commandId: "cmd_daemon",
        state: "cancelled",
        steerCapability: "native_steer",
        seq: 9,
      },
    }));

    const response = await handleConversationQueuePost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "otto",
          action: "cancel",
          clientMutationId: "mut_daemon",
          commandId: "cmd_daemon",
          expectedRevision: 1,
        }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        daemonQueueMutation,
        now: () => 9_000,
      },
    );

    expect(response.status).toBe(200);
    expect(daemonQueueMutation).toHaveBeenCalledWith({
      project: "default",
      now: 9_000,
      request: expect.objectContaining({
        name: "otto",
        action: "cancel",
        clientMutationId: "mut_daemon",
      }),
    });
  });

  it("accepts a stable agentId as the complete production mutation target", async () => {
    const daemonQueueMutation = vi.fn(async () => ({
      status: 200,
      body: {
        clientMutationId: "mut_agent_id",
        commandId: "cmd_agent_id",
        state: "cancelled",
        steerCapability: "native_steer",
        seq: 10,
      },
    }));

    const response = await handleConversationQueuePost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          agentId: "a_otto",
          action: "cancel",
          clientMutationId: "mut_agent_id",
          commandId: "cmd_agent_id",
          expectedRevision: 1,
        }),
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        daemonQueueMutation,
        now: () => 9_000,
      },
    );

    expect(response.status).toBe(200);
    expect(daemonQueueMutation).toHaveBeenCalledWith({
      project: "default",
      now: 9_000,
      request: expect.objectContaining({
        agentId: "a_otto",
        action: "cancel",
        clientMutationId: "mut_agent_id",
      }),
    });
  });

  it("hydrates production snapshots through the typed daemon queue read", async () => {
    const daemonQueueRead = vi.fn(async () => ({
      target: "otto",
      sessionId: "s_otto",
      turnActive: false,
      steerCapability: SteerCapability.NativeSteer,
      seq: 12,
      revision: 12,
      commands: [],
    }));

    const response = await handleConversationQueueGet(
      new Request(
        "http://localhost/api/conversation/prompt?agentId=a_otto&expectedSessionId=s_otto",
      ),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        daemonQueueRead,
      },
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      target: "otto",
      sessionId: "s_otto",
      seq: 12,
      commands: [],
    });
    expect(daemonQueueRead).toHaveBeenCalledWith({
      project: "default",
      agentId: "a_otto",
        expectedSessionId: "s_otto",
      requester: expect.objectContaining({
        sessionId: "local-operator",
        kind: "human",
        tier: "admin",
      }),
    });
  });

  it("forwards exact original submission lookup without enqueueing", async () => {
    const daemonQueueRead = vi.fn(async () => ({ target: "otto", sessionId: "s_otto", turnActive: false, steerCapability: SteerCapability.None, seq: 12, revision: 12, commands: [] }));
    const response = await handleConversationQueueGet(new Request(
      "http://localhost/api/conversation/prompt?agentId=a_otto&expectedSessionId=s_otto&clientMessageId=cm_original",
    ), { env: { NEXUS_WEB_AUTH_MODE: "local" }, daemonQueueRead });
    expect(response.status).toBe(200);
    expect(daemonQueueRead).toHaveBeenCalledWith(expect.objectContaining({
      agentId: "a_otto", expectedSessionId: "s_otto", clientMessageId: "cm_original",
      requester: expect.objectContaining({ sessionId: "local-operator" }),
    }));
  });

  it.each([
    "agentId=a_otto&clientMessageId=cm_original",
    "agentId=a_otto&expectedSessionId=s_otto&clientMessageId=",
    "agentId=a_otto&expectedSessionId=s_otto&clientMessageId=a%0Ab",
    `agentId=a_otto&expectedSessionId=s_otto&clientMessageId=${"x".repeat(257)}`,
    "eventsAfter=1&clientMessageId=cm_original",
  ])("rejects unscoped or invalid submission lookup: %s", async (query) => {
    const daemonQueueRead = vi.fn();
    const response = await handleConversationQueueGet(new Request(
      `http://localhost/api/conversation/prompt?${query}`,
    ), { env: { NEXUS_WEB_AUTH_MODE: "local" }, daemonQueueRead });
    expect(response.status).toBe(400);
    expect(daemonQueueRead).not.toHaveBeenCalled();
  });

  it("reads production reconnect transitions through the typed daemon queue read", async () => {
    const daemonQueueRead = vi.fn(async () => ({
      events: [
        {
          seq: 13,
          sessionId: "s_otto",
          commandId: "cmd_done",
          clientMessageId: "cm_done",
          commandKind: "harness.prompt",
          callerName: "Operator",
          callerSessionId: "s_human",
          callerPrincipalId: "h_operator",
          callerKind: "human",
          state: CommandQueueState.Completed,
          mode: "queue",
          revision: 4,
        },
      ],
      nextSeq: 13,
      latestSeq: 13,
      gap: false,
    }));

    const response = await handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?eventsAfter=12"),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        daemonQueueRead,
      },
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      events: [{
        commandId: "cmd_done",
        state: "completed",
        callerPrincipalId: "h_operator",
      }],
      latestSeq: 13,
    });
    expect(daemonQueueRead).toHaveBeenCalledWith({
      project: "default",
      eventsAfter: 12,
      requester: expect.objectContaining({ sessionId: "local-operator" }),
    });
  });

  it("maps daemon queue read failures to a bad-gateway response", async () => {
    const response = await handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?name=otto"),
      {
        env: { NEXUS_WEB_AUTH_MODE: "local" },
        daemonQueueRead: vi.fn(async () => {
          throw new Error("daemon queue unavailable");
        }),
      },
    );

    expect(response.status).toBe(502);
    await expect(response.json()).resolves.toEqual({
      error: "daemon queue unavailable",
    });
  });

  it("keeps the reconnect cursor global when project metadata interleaves", async () => {
    const db = await dbFor();
    await insertCommand(db, "cmd_default", "pending", 10);
    await db.execute({
      sql:
        "INSERT INTO command_intents " +
        "(command_id, kind, status, project, caller_name, caller_session_id, caller_principal_id, caller_kind, " +
        "request_json, revision, created_at) VALUES (?, 'metadata.set', 'done', ?, ?, ?, ?, ?, '{}', 1, 11)",
      args: [
        "cmd_other",
        "other-metadata",
        "Other Operator",
        "s_other_human",
        "x_other",
        "external.human",
      ],
    });
    await db.execute({
      sql:
        "INSERT INTO command_intent_events " +
        "(project, session_id, command_id, client_message_id, state, mode, revision, created_at) " +
        "VALUES (?, ?, ?, ?, 'queued', 'queue', 1, 11)",
      args: [
        "other-metadata",
        "s_other",
        "cmd_other",
        "cm_other",
      ],
    });

    const response = await handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?eventsAfter=1"),
      deps(db),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      // An unrelated command's copied event session is not a validated session target.
      events: [],
      nextSeq: 2,
      latestSeq: 2,
      gap: false,
    });
  });

  it("hydrates with turn activity folded into runtime lookup and original ownership checked", async () => {
    const raw = await dbFor();
    await insertActiveTurn(raw);
    await insertCommand(raw, "cmd_counted", "pending", 10);
    const counted = countedDb(raw);

    const response = await handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?name=otto"),
      deps(counted.db),
    );

    expect(response.status).toBe(200);
    expect(counted.direct).toHaveLength(4);
    expect(counted.direct[0]).toMatch(/EXISTS[\s\S]+agent_session_turns/i);
  });

  it("returns CAS revision and queue cursor from the update without follow-up reads", async () => {
    const raw = await dbFor();
    await insertActiveTurn(raw);
    await insertCommand(raw, "cmd_returning", "pending", 10);
    const counted = countedDb(raw);

    const response = await handleConversationQueuePost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "otto",
          action: "redirect_now",
          clientMutationId: "mut_returning",
          commandId: "cmd_returning",
          expectedRevision: 1,
        }),
      }),
      deps(counted.db),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({ revision: 2, seq: 2 });
    expect(counted.transaction).toHaveLength(5);
    expect(counted.transaction[2]).toMatch(/EXISTS[\s\S]+agent_session_turns/i);
    expect(counted.transaction[3]).toMatch(/UPDATE[\s\S]+RETURNING revision[\s\S]+seq/i);
    expect(counted.transaction.some((sql) => /^SELECT revision/i.test(sql))).toBe(false);
    expect(counted.transaction.some((sql) => /SELECT COALESCE\(MAX\(seq\)/i.test(sql))).toBe(false);
  });

  it("keeps human-cookie identity in the gateway DB and queue truth in the daemon DB", async () => {
    const runtimeDb = await dbFor();
    await insertCommand(runtimeDb, "cmd_split_store", "pending", 10);
    const identityDb = createClient({ url: ":memory:" });
    await identityDb.executeMultiple(`
      CREATE TABLE human_user (
        name_key TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        client_key TEXT NOT NULL UNIQUE,
        project TEXT NOT NULL,
        daemon_session_id TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        human_user_id TEXT NOT NULL UNIQUE
      );
      CREATE TABLE human_session (
        cookie_token TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        client_key TEXT NOT NULL,
        project TEXT NOT NULL,
        daemon_session_id TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        human_user_id TEXT NOT NULL,
        principal_id TEXT NOT NULL
      );
      INSERT INTO human_user VALUES
        ('alex', 'Alex', 'unused', 'client-1', 'default', 'human-session-1', 1, 1,
         'hu_aaaaaaaaaaaaaaaaaaaaaaaa');
      INSERT INTO human_session VALUES
        ('cookie-1', 'Alex', 'client-1', 'default', 'human-session-1', 1,
         'hu_aaaaaaaaaaaaaaaaaaaaaaaa', 'h_aaaaaaaaaaaaaaaaaaaaaaaa');
    `);

    const response = await handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?name=otto", {
        headers: { cookie: "nexus_human=cookie-1" },
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "remote-human" },
        getWriteDb: async () => runtimeDb,
        getIdentityDb: async () => identityDb,
      },
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      sessionId: "s_otto",
      commands: [
        { commandId: "cmd_split_store", text: "text cmd_split_store" },
      ],
    });
    await runtimeDb.execute(
      "INSERT INTO sessions (session_id, name, project, client_key, kind, tier) VALUES ('human-new-boot', 'renamed', 'default', 'client-1', 'local.human', 'admin')",
    );
    await runtimeDb.execute(
      "UPDATE command_intents SET caller_client_key = 'client-1', caller_principal_id = 'h_aaaaaaaaaaaaaaaaaaaaaaaa', caller_kind = 'local.human', caller_tier = 'admin' WHERE command_id = 'cmd_split_store'",
    );
    const read = () => handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?name=otto", {
        headers: { cookie: "nexus_human=cookie-1" },
      }),
      {
        env: { NEXUS_WEB_AUTH_MODE: "remote-human" },
        getWriteDb: async () => runtimeDb,
        getIdentityDb: async () => identityDb,
      },
    );
    expect(await (await read()).json()).toMatchObject({
      commands: [{ correlationOwned: true }],
    });
    await identityDb.execute(
      "UPDATE human_session SET principal_id = 'h_wrong_account'",
    );
    expect((await read()).status).toBe(401);
    await identityDb.execute(
      "UPDATE human_session SET principal_id = 'h_aaaaaaaaaaaaaaaaaaaaaaaa'",
    );
    await identityDb.execute("UPDATE human_user SET project = 'other'");
    expect((await read()).status).toBe(401);
    identityDb.close();
  });

  it("hydrates stable IDs and turnActive from the persisted normalized turn", async () => {
    const db = await dbFor();
    await db.execute(
      "INSERT INTO agent_session_turns VALUES " +
        "('turn_1', 's_otto', 'streaming', 1, 2, 100, 101, NULL)",
    );
    await insertCommand(db, "cmd_queued", "pending", 10);
    await insertCommand(db, "cmd_started", "claimed", 20, 22);
    await db.execute(
      "UPDATE sessions SET name = 'otto-renamed' WHERE session_id = 's_otto'",
    );

    const response = await handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?name=otto-renamed"),
      deps(db),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      target: "otto-renamed",
      sessionId: "s_otto",
      turnActive: true,
      steerCapability: "native_steer",
      seq: 2,
      revision: 2,
      commands: [
        {
          commandId: "cmd_queued",
          clientMessageId: "cm_cmd_queued",
          sessionId: "s_otto",
          state: "queued",
          revision: 1,
          seq: 1,
        },
        {
          commandId: "cmd_started",
          clientMessageId: "cm_cmd_started",
          sessionId: "s_otto",
          state: "started",
          revision: 1,
          seq: 2,
        },
      ],
    });
  });

  it("does not fall back to a mutable name when a stable agentId is supplied", async () => {
    const db = await dbFor();
    await insertCommand(db, "cmd_wrong_identity", "pending", 10);

    const response = await handleConversationQueueGet(
      new Request(
        "http://localhost/api/conversation/prompt?name=otto&agentId=a_different",
      ),
      deps(db),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      target: "otto",
      turnActive: false,
      steerCapability: "none",
      commands: [],
    });
  });

  it("uses a valid stable agentId when the supplied display name is stale", async () => {
    const db = await dbFor();
    await insertCommand(db, "cmd_stable_identity", "pending", 10);

    const response = await handleConversationQueueGet(
      new Request(
        "http://localhost/api/conversation/prompt?name=stale-name&agentId=a_otto",
      ),
      deps(db),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      target: "otto",
      sessionId: "s_otto",
      commands: [{ commandId: "cmd_stable_identity" }],
    });
  });

  it("promotes the same pending row atomically and never inserts a second command", async () => {
    const db = await dbFor();
    await insertActiveTurn(db);
    await insertCommand(db, "cmd_promote", "pending", 10);

    const response = await handleConversationQueuePost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "otto",
          action: "redirect_now",
          clientMutationId: "mut_promote",
          commandId: "cmd_promote",
          expectedRevision: 1,
        }),
      }),
      deps(db),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      clientMutationId: "mut_promote",
      commandId: "cmd_promote",
      sessionId: "s_otto",
      state: "queued",
      steerCapability: "native_steer",
      revision: 2,
      seq: 2,
    });
    const rows = await db.execute(
      "SELECT command_id, kind, status FROM command_intents",
    );
    expect(rows.rows).toEqual([
      { command_id: "cmd_promote", kind: "harness.steer", status: "pending" },
    ]);
  });

  it("rejects redirect_now while no turn is active without changing the queued prompt", async () => {
    const db = await dbFor();
    await insertCommand(db, "cmd_inactive", "pending", 10);

    const response = await handleConversationQueuePost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "otto",
          action: "redirect_now",
          clientMutationId: "mut_inactive",
          commandId: "cmd_inactive",
          expectedRevision: 1,
        }),
      }),
      deps(db),
    );

    expect(response.status).toBe(409);
    await expect(response.json()).resolves.toMatchObject({
      clientMutationId: "mut_inactive",
      commandId: "cmd_inactive",
      error: "target has no active turn to redirect",
    });
    const rows = await db.execute(
      "SELECT kind, status, revision FROM command_intents WHERE command_id = 'cmd_inactive'",
    );
    expect(rows.rows).toEqual([
      { kind: "harness.prompt", status: "pending", revision: 1 },
    ]);
  });

  it("loses cleanly to a concurrent claim instead of cancel-plus-steer duplication", async () => {
    const db = await dbFor();
    await insertActiveTurn(db);
    await insertCommand(db, "cmd_claimed", "claimed", 10, 12);

    const response = await handleConversationQueuePost(
      new Request("http://localhost/api/conversation/prompt", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "otto",
          action: "redirect_now",
          clientMutationId: "mut_claimed",
          commandId: "cmd_claimed",
          expectedRevision: 1,
        }),
      }),
      deps(db),
    );

    expect(response.status).toBe(409);
    const rows = await db.execute(
      "SELECT command_id, kind, status FROM command_intents",
    );
    expect(rows.rows).toEqual([
      { command_id: "cmd_claimed", kind: "harness.prompt", status: "claimed" },
    ]);
  });

  it("replays an atomic redirect after ack loss without applying it twice", async () => {
    const db = await dbFor();
    await insertActiveTurn(db);
    await insertCommand(db, "cmd_retry", "pending", 10);
    const request = () =>
      new Request("http://localhost/api/conversation/prompt", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name: "otto",
          action: "redirect_now",
          clientMutationId: "mut_retry",
          commandId: "cmd_retry",
          expectedRevision: 1,
        }),
      });

    const first = await handleConversationQueuePost(request(), deps(db));
    const retry = await handleConversationQueuePost(request(), deps(db));
    expect(first.status).toBe(200);
    expect(retry.status).toBe(200);
    expect(await retry.json()).toEqual(await first.json());

    const command = await db.execute(
      "SELECT kind, revision FROM command_intents WHERE command_id = 'cmd_retry'",
    );
    expect(command.rows).toEqual([{ kind: "harness.steer", revision: 2 }]);
    const events = await db.execute(
      "SELECT state, mode, revision FROM command_intent_events " +
        "WHERE command_id = 'cmd_retry' ORDER BY seq",
    );
    expect(events.rows).toEqual([
      { state: "queued", mode: "queue", revision: 1 },
      { state: "queued", mode: "redirect", revision: 2 },
    ]);
  });

  it("rejects reuse of a clientMutationId for a different mutation", async () => {
    const db = await dbFor();
    await insertActiveTurn(db);
    await insertCommand(db, "cmd_reuse", "pending", 10);
    const post = (action: string) =>
      handleConversationQueuePost(
        new Request("http://localhost/api/conversation/prompt", {
          method: "PATCH",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            name: "otto",
            action,
            clientMutationId: "mut_reuse",
            commandId: "cmd_reuse",
            expectedRevision: 1,
          }),
        }),
        deps(db),
      );

    expect((await post("redirect_now")).status).toBe(200);
    const conflict = await post("cancel");
    expect(conflict.status).toBe(409);
    await expect(conflict.json()).resolves.toMatchObject({
      clientMutationId: "mut_reuse",
      commandId: "cmd_reuse",
      error: "clientMutationId was already used for a different mutation",
    });
  });

  it("advertises the observed interrupt-and-send adapter mode", async () => {
    const db = await dbFor("opencode", "pty");
    const response = await handleConversationQueueGet(
      new Request("http://localhost/api/conversation/prompt?name=otto"),
      {...deps(db), observeTurn:(sessionId) => ({sessionId, state:"unknown" as any, steerCapability:SteerCapability.InterruptAndSend})},
    );
    await expect(response.json()).resolves.toMatchObject({
      turnActive: false,
      steerCapability: "interrupt_and_send",
    });
  });
});
