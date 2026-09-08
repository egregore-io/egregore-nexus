import { afterEach, describe, expect, it, vi } from "vitest";
import { createClient } from "@libsql/client";

import { canObserveAgentSession, observeScoped } from "./agui.observe";
import type { GatewayCallerIdentity } from "@server/api/http";
import type { AgentAccessGrantRow, AgentOwnerRow } from "@server/read/queries";
import { Kind, Tier } from "@shared/types";
import { migrateGatewayStore } from "@server/store/migrations";
import { sessionFanout } from "@server/stream/sessionFanout";
import { EventEmitter } from "node:events";
import { CommandQueueHub, handleWs } from "@server/agui/ws.mjs";
import { handleConversationQueueGet } from "@server/command/sessionQueue";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function identity(
  name: string,
  project = "default",
  attrs: Partial<GatewayCallerIdentity> = {},
): GatewayCallerIdentity {
  return { name, project, ...attrs };
}

function ownedBy(
  name: string,
  _project = "default",
  attrs: Partial<AgentOwnerRow> = {},
): AgentOwnerRow {
  return {
    agentId: "a_target",
    name: "target",
    ownerName: name,
    ownerSessionId: `s_${name}`,
    ...attrs,
  };
}

function grant(principalName: string, principalProject = "default", role = "viewer"): AgentAccessGrantRow {
  return {
    agentId: "a_target",
    principalProject,
    principalName,
    role,
  };
}

describe("canObserveAgentSession", () => {
  it("allows legacy rows without an owner", () => {
    expect(canObserveAgentSession(undefined, identity("operator"))).toBe(true);
    expect(
      canObserveAgentSession(
        { agentId: "a_target", name: "target" },
        identity("operator"),
      ),
    ).toBe(true);
  });

  it("allows the owner to observe a managed agent session", () => {
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("operator", "default", { sessionId: "s_operator" }),
    )).toBe(true);
  });

  it("uses a stable owner agent id before mutable owner-name metadata", () => {
    const owner = ownedBy("old-owner-name", "default", { ownerAgentId: "a_owner" });

    expect(canObserveAgentSession(
      owner,
      identity("renamed-owner", "other", { agentId: "a_owner" }),
    )).toBe(true);
    expect(canObserveAgentSession(
      owner,
      identity("old-owner-name", "default", { agentId: "a_intruder" }),
    )).toBe(false);
    expect(canObserveAgentSession(
      owner,
      identity("old-owner-name"),
    )).toBe(false);
  });

  it("uses a stable owner session id before mutable owner-name metadata", () => {
    const owner = ownedBy("old-owner-name", "default", { ownerSessionId: "s_owner" });

    expect(canObserveAgentSession(
      owner,
      identity("renamed-owner", "other", { sessionId: "s_owner" }),
    )).toBe(true);
    expect(canObserveAgentSession(
      owner,
      identity("old-owner-name", "default", { sessionId: "s_intruder" }),
    )).toBe(false);
    expect(canObserveAgentSession(owner, identity("old-owner-name"))).toBe(false);
  });

  it("denies a non-owner even when they share the project", () => {
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice"))).toBe(false);
  });

  it("allows delegated viewer and co-owner grants to observe", () => {
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice"), [grant("alice")]))
      .toBe(true);
    expect(
      canObserveAgentSession(ownedBy("operator"), identity("cara"), [
        grant("cara", "default", "co_owner"),
      ]),
    ).toBe(true);
  });

  it("uses a stable delegated principal id before mutable grant-name metadata", () => {
    const stableGrant: AgentAccessGrantRow = {
      ...grant("old-delegate-name"),
      principalAgentId: "a_delegate",
    };

    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("renamed-delegate", "other", { agentId: "a_delegate" }),
      [stableGrant],
    )).toBe(true);
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("old-delegate-name", "default", { agentId: "a_intruder" }),
      [stableGrant],
    )).toBe(false);
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("old-delegate-name"),
      [stableGrant],
    )).toBe(false);
  });

  it("uses a stable delegated principal session before mutable grant-name metadata", () => {
    const sessionGrant: AgentAccessGrantRow = {
      ...grant("old-delegate-name"),
      principalSessionId: "s_delegate",
    };

    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("renamed-delegate", "other", { sessionId: "s_delegate" }),
      [sessionGrant],
    )).toBe(true);
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("old-delegate-name", "default", { sessionId: "s_intruder" }),
      [sessionGrant],
    )).toBe(false);
    expect(canObserveAgentSession(
      ownedBy("operator"),
      identity("old-delegate-name"),
      [sessionGrant],
    )).toBe(false);
  });

  it("denies a revoked grant", () => {
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice"), [])).toBe(false);
  });

  it("treats project as metadata for owners and delegated viewers", () => {
    expect(canObserveAgentSession(
      ownedBy("operator", "default"),
      identity("operator", "ops", { sessionId: "s_operator" }),
    ))
      .toBe(true);
    expect(canObserveAgentSession(ownedBy("operator"), identity("alice", "ops"), [grant("alice")]))
      .toBe(true);
  });

  it("allows human admin observe override for protected managed targets", () => {
    const humanAdmin = identity("operator", "default", { kind: Kind.Human, tier: Tier.Admin });
    expect(canObserveAgentSession(ownedBy("owner", "default", { tier: "admin" }), humanAdmin))
      .toBe(true);
    expect(
      canObserveAgentSession(
        ownedBy("owner", "default", { sessionKind: "human", sessionTier: "admin" }),
        humanAdmin,
      ),
    ).toBe(true);
  });

  it("allows agent-admin observe override only for ordinary managed targets", () => {
    const agentAdmin = identity("agent-admin", "default", {
      kind: Kind.Agent,
      tier: Tier.Admin,
    });
    expect(canObserveAgentSession(ownedBy("owner"), agentAdmin)).toBe(true);
    expect(canObserveAgentSession(ownedBy("owner", "default", { tier: "admin" }), agentAdmin))
      .toBe(false);
    expect(canObserveAgentSession(ownedBy("owner", "default", { sessionKind: "human" }), agentAdmin))
      .toBe(false);
  });
});

describe("observeScoped cursor compatibility", () => {
  it("opens retained exact S1 and active S2 through real HTTP/WS handlers without warming or crossing lanes", async () => {
    vi.stubEnv("NEXUS_WEB_AUTH_MODE", "local");
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.executeMultiple(`INSERT INTO identities (agent_id,name,metadata_json,updated_at) VALUES ('a_exact','renamed','{}',1), ('a_other','other','{}',1);
      INSERT INTO runtime_descriptors (runtime_id,agent_id,session_id,harness,mode,status,updated_at) VALUES
      ('r_old','a_exact','s_old','codex','headless','stopped',1), ('r_new','a_exact','s_new','codex','headless','online',2), ('s_collision','a_other','s_other','codex','headless','online',3);`);
    const subscribed: string[] = [];
    vi.spyOn(sessionFanout, "subscribe").mockImplementation((sessionId) => {
      subscribed.push(sessionId);
      return { ready: Promise.resolve(), close() {} };
    });
    const warm = vi.fn(async () => undefined);
    const reads: string[] = [];
    const hub = new CommandQueueHub({
      commandQueueEventPollMs: 1,
      commandQueueObservationPollMs: 60_000,
      fetchHandler: (request) =>
        handleConversationQueueGet(request, {
          env: { NEXUS_WEB_AUTH_MODE: "local" },
          daemonQueueRead: async (input) => {
            if (input.eventsAfter !== undefined)
              return {
                events:
                  input.eventsAfter < 2
                    ? [
                        {
                          seq: 1,
                          sessionId: "s_new",
                          commandId: "cmd_new",
                          clientMessageId: "cm_new",
                          commandKind: "harness.prompt",
                          callerName: "human",
                          state: "failed" as never,
                          mode: "queue",
                          revision: 3,
                          correlationOwned: true,
                        },
                        {
                          seq: 2,
                          sessionId: "s_old",
                          commandId: "cmd_old",
                          clientMessageId: "cm_old",
                          commandKind: "harness.prompt",
                          callerName: "human",
                          state: "failed" as never,
                          mode: "queue",
                          revision: 3,
                          correlationOwned: true,
                        },
                      ]
                    : [],
                nextSeq: 2,
                latestSeq: 2,
                gap: false,
              };
            expect(input.agentId).toBe("a_exact");
            reads.push(input.expectedSessionId!);
            const refreshed = reads.filter(session => session === input.expectedSessionId).length > 1;
            return {
              target: "renamed",
              sessionId: input.expectedSessionId,
              turnActive: false,
              steerCapability: "none" as never,
              seq: refreshed ? 2 : 0,
              revision: refreshed ? 3 : 0,
              commands: [],
            };
          },
        }),
    });
    class Socket extends EventEmitter {
      sent: string[] = [];
      send(value: string) {
        this.sent.push(value);
      }
      close() {
        this.emit("close");
      }
    }
    const sockets = [new Socket(), new Socket()];
    const controls = sockets.map((socket, index) =>
      handleWs(
        socket,
        new Request(
          `http://localhost/api/agui/ws?agentId=a_exact&expectedSessionId=${index ? "s_new" : "s_old"}`,
        ),
        {
          observe: (request) =>
            observeScoped(request, {
              canonicalDb: () => db,
              submitCommand: warm,
            }),
          commandQueueHub: hub,
        },
      ),
    );
    try {
      await vi.waitFor(() =>
        expect(
          sockets.every((socket) =>
            socket.sent.some(
              (raw) => JSON.parse(raw).t === "command.transition",
            ) && socket.sent.filter(raw => JSON.parse(raw).t === "queue.snapshot").length === 2,
          ),
        ).toBe(true),
      );
      for (const [index, socket] of sockets.entries()) {
        const frames = socket.sent.map((raw) => JSON.parse(raw));
        const expected = index ? "s_new" : "s_old";
        expect(frames[0]).toEqual({
          t: "session.bound",
          agentId: "a_exact",
          sessionId: expected,
        });
        expect(
          frames
            .filter((frame) => frame.t === "command.transition")
            .map((frame) => frame.sessionId),
        ).toEqual([expected]);
        expect(frames.filter(frame => frame.t === "queue.snapshot").map(frame => [frame.sessionId, frame.seq])).toEqual([[expected, 0], [expected, 2]]);
      }
      expect(reads.sort()).toEqual(["s_new", "s_new", "s_old", "s_old"]);
      expect(subscribed.sort()).toEqual(["s_new", "s_old"]);
      for (const [agent, session] of [
        ["a_other", "s_old"],
        ["a_other", "s_collision"],
      ]) {
        const response = await observeScoped(
          new Request(
            `http://localhost/api/agui/observe?agentId=${agent}&expectedSessionId=${session}`,
          ),
          { canonicalDb: () => db, submitCommand: warm },
        );

    expect(response.status).toBe(404);
      }
      expect(warm).not.toHaveBeenCalled();
    } finally {
      controls.forEach((control) => control.close());
      await Promise.all(controls.map((control) => control.closed));
      db.close();
    }
  });
  it("preserves legacy afterId for explicit current-boot translation", async () => {
    vi.stubEnv("NEXUS_WEB_AUTH_MODE", "local");
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    await db.batch([
      {
        sql:
          "INSERT INTO identities (agent_id, name, metadata_json, updated_at) " +
          "VALUES (?, ?, ?, ?)",
        args: ["a_ada", "ada", "{}", 1],
      },
      {
        sql:
          "INSERT INTO runtime_descriptors " +
          "(runtime_id, agent_id, session_id, harness, mode, status, updated_at) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?)",
        args: ["r_ada", "a_ada", "s_ada", "codex", "headless", "online", 1],
      },
    ]);
    let captured: Record<string, unknown> | undefined;
    vi.spyOn(sessionFanout, "subscribe").mockImplementation((_sessionId, subscriber) => {
      captured = subscriber as unknown as Record<string, unknown>;
      return { ready: Promise.resolve(), close() {} };
    });

    const response = await observeScoped(
      new Request("http://localhost/api/agui/observe?session=ada&afterId=42"),
      {
        canonicalDb: () => db,
        submitCommand: vi.fn(async () => undefined),
      },
    );

    expect(response.status).toBe(200);
    expect(captured).toMatchObject({ after: undefined, legacyAfterId: "42" });
    await response.body?.cancel();
    db.close();
  });
});
