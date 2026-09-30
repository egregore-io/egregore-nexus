import type {
  ApiDeps,
  ApiResponse,
  GatewayCallerIdentity,
  PrincipalScope,
} from "@server/api/http";
import { fail } from "@server/api/http";
import { principalHasScope, principalMeetsTier } from "@server/auth/principal";
import { COMMAND_KINDS } from "@server/command/ingress";
import { assertSendRequestBody } from "@server/messagePost/validate";
import { Tier, type Ack, type SendRequest } from "@shared/types";

interface JsonRpcRequest {
  jsonrpc?: string;
  id?: unknown;
  method?: string;
  params?: unknown;
}

interface McpTool {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  scope: PrincipalScope;
  tier?: Tier;
}

const TOOLS: McpTool[] = [
  {
    name: "dm",
    description: "Send a private direct message to another agent or human by name.",
    scope: "message:send",
    inputSchema: {
      type: "object",
      required: ["to", "message"],
      properties: {
        to: { type: "string" },
        message: { type: "string" },
        summary: { type: "string" },
      },
    },
  },
  {
    name: "post",
    description: "Post a message to a named thread.",
    scope: "message:send",
    inputSchema: {
      type: "object",
      required: ["thread", "message"],
      properties: {
        thread: { type: "string" },
        message: { type: "string" },
        summary: { type: "string" },
      },
    },
  },
  {
    name: "reply",
    description: "Reply into the current conversation context.",
    scope: "message:send",
    inputSchema: {
      type: "object",
      required: ["message"],
      properties: {
        message: { type: "string" },
        summary: { type: "string" },
      },
    },
  },
  {
    name: "publish",
    description: "Publish a message to a topic.",
    scope: "message:send",
    inputSchema: {
      type: "object",
      required: ["topic", "message"],
      properties: {
        topic: { type: "string" },
        message: { type: "string" },
        summary: { type: "string" },
      },
    },
  },
];

/** Network MCP JSON-RPC endpoint over the gateway Principal spine. */
export async function handleNetworkMcp(
  rpc: unknown,
  caller: GatewayCallerIdentity | undefined,
  deps: ApiDeps,
): Promise<ApiResponse> {
  if (!caller) return fail(401, "missing or invalid Principal");
  const req = rpcRequest(rpc);
  if (!req.method) return jsonRpcError(req.id, -32600, "missing method");

  switch (req.method) {
    case "initialize":
      return jsonRpcResult(req.id, {
        protocolVersion: "2024-11-05",
        serverInfo: { name: "nexus-gateway", version: "0.0.0" },
        capabilities: { tools: {} },
      });
    case "tools/list":
      return jsonRpcResult(req.id, {
        tools: visibleTools(caller).map(({ scope: _scope, tier: _tier, ...tool }) => tool),
      });
    case "tools/call":
      return toolCall(req.id, req.params, caller, deps);
    default:
      return jsonRpcError(req.id, -32601, `method not found: '${req.method}'`);
  }
}

function visibleTools(caller: GatewayCallerIdentity): McpTool[] {
  return TOOLS.filter((tool) => allowed(caller, tool));
}

async function toolCall(
  id: unknown,
  params: unknown,
  caller: GatewayCallerIdentity,
  deps: ApiDeps,
): Promise<ApiResponse> {
  const record = objectParam(params);
  const name = stringField(record.name);
  if (!name) return jsonRpcError(id, -32602, "missing 'name' in tools/call params");

  const tool = TOOLS.find((candidate) => candidate.name === name);
  if (!tool) return jsonRpcError(id, -32601, `unknown tool '${name}'`);
  if (!allowed(caller, tool)) {
    return jsonRpcToolResult(id, `missing required scope: ${tool.scope}`, true);
  }

  const args = objectParam(record.arguments);
  try {
    const result = await executeTool(tool.name, args, caller, deps);
    return jsonRpcToolResult(id, JSON.stringify(result, null, 2), false);
  } catch (err) {
    const message = err instanceof Error ? err.message : "tool call failed";
    return jsonRpcToolResult(id, message, true);
  }
}

async function executeTool(
  name: string,
  args: Record<string, unknown>,
  caller: GatewayCallerIdentity,
  deps: ApiDeps,
): Promise<unknown> {
  switch (name) {
    case "dm":
      return sendMessage(
        {
          to: { verb: "dm", name: requiredString(args, "to") },
          body: requiredMessage(args, "message"),
          summary: stringField(args.summary),
        },
        caller,
        deps,
      );
    case "post":
      return sendMessage(
        {
          to: { verb: "post", thread: requiredString(args, "thread") },
          body: requiredMessage(args, "message"),
          summary: stringField(args.summary),
        },
        caller,
        deps,
      );
    case "reply":
      return sendMessage(
        {
          to: { verb: "reply" },
          body: requiredMessage(args, "message"),
          summary: stringField(args.summary),
        },
        caller,
        deps,
      );
    case "publish":
      return sendMessage(
        {
          to: { verb: "publish", topic: requiredString(args, "topic") },
          body: requiredMessage(args, "message"),
          summary: stringField(args.summary),
        },
        caller,
        deps,
      );
    default:
      throw new Error(`tool '${name}' is listed but not implemented by network MCP yet`);
  }
}

async function sendMessage(
  req: SendRequest,
  caller: GatewayCallerIdentity,
  deps: ApiDeps,
): Promise<Ack> {
  assertSendRequestBody(req);
  const ack = deps.messagePost
    ? await deps.messagePost.send(req, caller)
    : deps.commands
      ? await deps.commands.submit<Ack>(COMMAND_KINDS.messagePostSend, req, caller)
      : null;
  if (!ack) throw new Error("command ingress is not configured");
  return { messageId: ack.messageId };
}

function allowed(caller: GatewayCallerIdentity, tool: McpTool): boolean {
  return principalMeetsTier(caller, tool.tier ?? Tier.Agent) && principalHasScope(caller, tool.scope);
}

function rpcRequest(value: unknown): JsonRpcRequest {
  return value && typeof value === "object" ? (value as JsonRpcRequest) : {};
}

function objectParam(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function stringField(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function requiredString(args: Record<string, unknown>, key: string): string {
  const value = stringField(args[key]);
  if (!value) throw new Error(`missing required argument '${key}'`);
  return value;
}

function requiredMessage(args: Record<string, unknown>, key: string): string {
  const value = stringField(args[key]);
  if (value === undefined) throw new Error(`missing required argument '${key}'`);
  return value;
}

function jsonRpcResult(id: unknown, result: unknown): ApiResponse {
  return { status: 200, body: { jsonrpc: "2.0", id: id ?? null, result } };
}

function jsonRpcError(id: unknown, code: number, message: string): ApiResponse {
  return {
    status: 200,
    body: { jsonrpc: "2.0", id: id ?? null, error: { code, message } },
  };
}

function jsonRpcToolResult(id: unknown, text: string, isError: boolean): ApiResponse {
  return jsonRpcResult(id, {
    content: [{ type: "text", text }],
    isError,
  });
}
