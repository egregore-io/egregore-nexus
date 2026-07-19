import { createClient, type Client } from "@libsql/client";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { initSchema } from "@server/conversation/store";
import { issueBearerToken } from "@server/identity/bearer";
import type { GatewayCallerIdentity, MessagePostSender } from "@server/api/http";
import { Kind, Tier } from "@shared/types";
import { makeMcpDispatch } from "./mcp";
import { makeDispatch as makeApiDispatch } from "./v1/$";

async function makeDb(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await initSchema(db);
  return db;
}

const ADMIN_ACTOR: GatewayCallerIdentity = {
  name: "mcp-owner",
  project: "default",
  kind: Kind.Human,
  tier: Tier.Admin,
  credentialFacet: "human",
  scopes: ["admin:*"],
  sessionId: "s_mcp_owner",
  runtimeId: "s_mcp_owner",
};

async function bearer(
  db: Client,
  scopes: string[],
): Promise<{ accessToken: string; tokenId: string }> {
  let idSeq = 0;
  return issueBearerToken(
    {
      actor: ADMIN_ACTOR,
      scopes,
      ttlMs: 60_000,
    },
    {
      db,
      now: () => 1_000_000,
      genId: () => `mcp_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    },
  );
}

function rpc(method: string, params?: unknown): string {
  return JSON.stringify({ jsonrpc: "2.0", id: 1, method, params });
}

describe("/api/mcp network MCP", () => {
  let db: Client;

  beforeEach(async () => {
    db = await makeDb();
  });

  it("rejects missing and invalid bearer credentials before MCP dispatch", async () => {
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_never", fanout: 0 })),
    };
    const dispatch = makeMcpDispatch({
      db: async () => db,
      messagePost,
      now: () => 1_000_000,
    });

    const missing = await dispatch(new Request("http://localhost/api/mcp", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: rpc("tools/list"),
    }));
    expect(missing.status).toBe(401);

    const invalid = await dispatch(new Request("http://localhost/api/mcp", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: "Bearer wrong",
      },
      body: rpc("tools/list"),
    }));
    expect(invalid.status).toBe(401);
    expect(messagePost.send).not.toHaveBeenCalled();
  });

  it("lists only tools allowed by the bearer Principal scopes", async () => {
    const { accessToken } = await bearer(db, ["message:send"]);
    const dispatch = makeMcpDispatch({
      db: async () => db,
      now: () => 1_000_000,
    });

    const response = await dispatch(new Request("http://localhost/api/mcp", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${accessToken}`,
      },
      body: rpc("tools/list"),
    }));

    expect(response.status).toBe(200);
    const body = await response.json() as { result: { tools: Array<{ name: string }> } };
    const names = body.result.tools.map((tool) => tool.name);
    expect(names).toEqual(["dm", "post", "reply", "publish"]);
  });

  it("executes scoped Message Post tools as the bearer Principal", async () => {
    const { accessToken, tokenId } = await bearer(db, ["message:send"]);
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_mcp_post", fanout: 2 })),
    };
    const dispatch = makeMcpDispatch({
      db: async () => db,
      messagePost,
      now: () => 1_000_000,
    });

    const response = await dispatch(new Request("http://localhost/api/mcp", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${accessToken}`,
      },
      body: rpc("tools/call", {
        name: "post",
        arguments: { thread: "backend", message: "hello from network mcp" },
      }),
    }));

    expect(response.status).toBe(200);
    const body = await response.json() as {
      result: { content: Array<{ text: string }>; isError: boolean };
    };
    expect(body.result.isError).toBe(false);
    expect(JSON.parse(body.result.content[0]!.text)).toEqual({
      messageId: "m_mcp_post",
    });
    expect(messagePost.send).toHaveBeenCalledWith(
      {
        to: { verb: "post", thread: "backend" },
        body: "hello from network mcp",
      },
      expect.objectContaining({
        name: "mcp-owner",
        credentialFacet: "machine",
        tokenId,
        scopes: ["message:send"],
      }),
    );
  });

  it("returns an MCP tool error for empty or whitespace-only message bodies", async () => {
    const { accessToken } = await bearer(db, ["message:send"]);
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_never", fanout: 0 })),
    };
    const dispatch = makeMcpDispatch({
      db: async () => db,
      messagePost,
      now: () => 1_000_000,
    });

    for (const [name, args] of [
      ["dm", { to: "roman", message: "" }],
      ["post", { thread: "backend", message: " \n\t " }],
      ["reply", { message: "\n" }],
    ] as const) {
      const response = await dispatch(new Request("http://localhost/api/mcp", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${accessToken}`,
        },
        body: rpc("tools/call", { name, arguments: args }),
      }));

      expect(response.status).toBe(200);
      const body = await response.json() as {
        result: { isError: boolean; content: Array<{ text: string }> };
      };
      expect(body.result.isError).toBe(true);
      expect(body.result.content[0]!.text).toContain("body");
    }

    expect(messagePost.send).not.toHaveBeenCalled();
  });

  it("uses the first-boot operator bearer as the named human Principal on MCP posts", async () => {
    let idSeq = 0;
    const apiDispatch = makeApiDispatch({
      db: async () => db,
      authMode: "local",
      now: () => 2_000_000,
      genId: () => `lens_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });

    const issue = await apiDispatch(new Request("http://localhost/api/v1/auth/operator-token", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "Alex Morgan",
        scopes: ["message:send"],
        ttlMs: 60_000,
      }),
    }));

    expect(issue.status).toBe(201);
    const issued = await issue.json() as { accessToken: string; tokenId: string };
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_lens_operator", fanout: 3 })),
    };
    const mcpDispatch = makeMcpDispatch({
      db: async () => db,
      messagePost,
      now: () => 2_000_001,
    });

    const response = await mcpDispatch(new Request("http://localhost/api/mcp", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${issued.accessToken}`,
      },
      body: rpc("tools/call", {
        name: "dm",
        arguments: { to: "roman", message: "from the Lens operator" },
      }),
    }));

    expect(response.status).toBe(200);
    expect(messagePost.send).toHaveBeenCalledWith(
      {
        to: { verb: "dm", name: "roman" },
        body: "from the Lens operator",
      },
      expect.objectContaining({
        name: "Alex Morgan",
        project: "default",
        kind: Kind.Human,
        tier: Tier.Admin,
        credentialFacet: "machine",
        tokenId: issued.tokenId,
        scopes: ["message:send"],
      }),
    );
    const caller = vi.mocked(messagePost.send).mock.calls[0]![1];
    expect(caller.name).not.toBe("operator");
    expect(caller.name).not.toBe("lens-bridge");
  });

  it("carries first-boot operator authority through command ingress", async () => {
    let idSeq = 0;
    const apiDispatch = makeApiDispatch({
      db: async () => db,
      authMode: "local",
      now: () => 2_000_000,
      genId: () => `operator_authority_${++idSeq}`,
      randomSecret: (prefix) => `${prefix}_secret_${++idSeq}`,
    });
    const issue = await apiDispatch(new Request("http://localhost/api/v1/auth/operator-token", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "Alex Morgan",
        scopes: ["message:send"],
        ttlMs: 60_000,
      }),
    }));
    const issued = await issue.json() as { accessToken: string };
    const daemonCommand = vi.fn(async () => ({ messageId: "m_operator_authority" }));
    const mcpDispatch = makeMcpDispatch({
      db: async () => db,
      now: () => 2_000_001,
      commandIngress: { daemonCommand },
    });

    const response = await mcpDispatch(new Request("http://localhost/api/mcp", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${issued.accessToken}`,
      },
      body: rpc("tools/call", {
        name: "post",
        arguments: { thread: "backend", message: "operator authority survives" },
      }),
    }));

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      result: { isError: false },
    });
    expect(daemonCommand).toHaveBeenCalledWith(
      "message.post.send",
      {
        to: { verb: "post", thread: "backend" },
        body: "operator authority survives",
      },
      expect.objectContaining({
        name: "Alex Morgan",
        project: "default",
        sessionId: "local-operator",
        runtimeId: "local-operator",
        kind: Kind.Human,
        tier: Tier.Admin,
      }),
      expect.any(Object),
    );
  });

  it("keeps operator bearer bootstrap local-only and named", async () => {
    const remoteDispatch = makeApiDispatch({
      db: async () => db,
      authMode: "remote",
    });
    const remote = await remoteDispatch(new Request("http://localhost/api/v1/auth/operator-token", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "Alex Morgan", scopes: ["message:send"] }),
    }));
    expect(remote.status).toBe(401);

    const localDispatch = makeApiDispatch({
      db: async () => db,
      authMode: "local",
    });
    const blankName = await localDispatch(new Request("http://localhost/api/v1/auth/operator-token", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: "   ", scopes: ["message:send"] }),
    }));
    expect(blankName.status).toBe(400);
  });

  it("returns an MCP tool error when the bearer lacks the tool scope", async () => {
    const { accessToken } = await bearer(db, ["message:read"]);
    const messagePost: MessagePostSender = {
      send: vi.fn(async () => ({ messageId: "m_never", fanout: 0 })),
    };
    const dispatch = makeMcpDispatch({
      db: async () => db,
      messagePost,
      now: () => 1_000_000,
    });

    const response = await dispatch(new Request("http://localhost/api/mcp", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${accessToken}`,
      },
      body: rpc("tools/call", {
        name: "post",
        arguments: { thread: "backend", message: "blocked" },
      }),
    }));

    expect(response.status).toBe(200);
    const body = await response.json() as { result: { isError: boolean; content: Array<{ text: string }> } };
    expect(body.result.isError).toBe(true);
    expect(body.result.content[0]!.text).toContain("missing required scope: message:send");
    expect(messagePost.send).not.toHaveBeenCalled();
  });
});
