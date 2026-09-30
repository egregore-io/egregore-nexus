import { createFileRoute } from "@tanstack/react-router";

import { handleNetworkMcp } from "@server/mcp/http";
import type {
  ApiDeps,
  GatewayCallerIdentity,
  MessagePostSender,
  CommandIntentSender,
} from "@server/api/http";
import { createReadDb, type ReadDb } from "@drizzle/client";
import { createConversationStore } from "@server/conversation/store";
import {
  createCommandIngressSender,
  type CommandIngressSenderOptions,
} from "@server/messagePost/commandIngress";
import {
  createCommandIngressSubmitter,
  type CommandIngressOptions,
} from "@server/command/ingress";
import type { Client } from "@libsql/client";
import {
  bearerFromAuthorization,
  identityFromBearer,
  type DispatchDeps,
} from "./v1/$";

export interface McpDispatchDeps {
  db: () => Promise<Client>;
  readDb?: () => Promise<ReadDb> | ReadDb;
  messagePost?: MessagePostSender;
  commands?: CommandIntentSender;
  commandIngressDb?: () => Promise<Client> | Client;
  commandIngress?: Omit<CommandIngressOptions, "db">;
  now?: () => number;
}

let readDbPromise: Promise<ReadDb> | undefined;
function getReadDbLazy(): Promise<ReadDb> {
  if (!readDbPromise) {
    readDbPromise = Promise.resolve(createReadDb());
    readDbPromise.catch(() => {
      readDbPromise = undefined;
    });
  }
  return readDbPromise;
}

let writeDbPromise: Promise<Client> | undefined;
function getWriteDbLazy(): Promise<Client> {
  if (!writeDbPromise) {
    writeDbPromise = createConversationStore();
    writeDbPromise.catch(() => {
      writeDbPromise = undefined;
    });
  }
  return writeDbPromise;
}

const realMcpDeps: McpDispatchDeps = {
  db: () => getWriteDbLazy(),
};

export function makeMcpDispatch(deps: McpDispatchDeps) {
  const commandIngressOptions: CommandIngressSenderOptions = {
    ...deps.commandIngress,
    ...(deps.commandIngressDb ? { db: deps.commandIngressDb } : {}),
  };
  const commands =
    deps.commands ??
    createCommandIngressSubmitter(commandIngressOptions);
  const messagePost =
    deps.messagePost ??
    createCommandIngressSender(commandIngressOptions);

  return async function dispatch(request: Request): Promise<Response> {
    const caller = await bearerPrincipal(request, deps);
    if (!caller) {
      return json({ error: { code: "unauthorized", message: "missing or invalid Principal" } }, 401);
    }

    let body: unknown;
    try {
      body = await request.json();
    } catch {
      body = undefined;
    }

    const mcpDeps: ApiDeps = {
      db: deps.readDb ?? getReadDbLazy,
      messagePost,
      commands,
    };
    const res = await handleNetworkMcp(body, caller, mcpDeps);
    return json(res.body, res.status);
  };
}

async function bearerPrincipal(
  request: Request,
  deps: McpDispatchDeps,
): Promise<GatewayCallerIdentity | null> {
  const bearerToken = bearerFromAuthorization(request.headers.get("authorization"));
  if (!bearerToken) return null;
  const dispatchDeps: DispatchDeps = {
    db: deps.db,
    now: deps.now,
  };
  return identityFromBearer(bearerToken, dispatchDeps);
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

// Exported so the standalone headless gateway can serve network MCP with the
// same bearer-to-Principal resolution as the webconsole route.
export const dispatchNetworkMcp = makeMcpDispatch(realMcpDeps);

export const Route = createFileRoute("/api/mcp")({
  server: {
    handlers: {
      POST: ({ request }) => dispatchNetworkMcp(request),
    },
  },
});
