import {
  HookAction,
  type GatewayHookCapabilities,
  type HookEvaluationRequest,
  type HookEvaluationResponse,
} from "@shared/types";

import { createBeforeSendAdapter } from "./eventAdapter";
import type { HookEngine } from "./engine";

export interface GatewayHookConnection {
  registerHooks(
    capabilities: GatewayHookCapabilities,
    handler: (request: HookEvaluationRequest) => Promise<HookEvaluationResponse>,
  ): () => void;
}

export interface GatewayHookBridgeOptions {
  connection: GatewayHookConnection;
  engine: HookEngine;
  generation: () => string;
}

export function registerGatewayHookBridge(options: GatewayHookBridgeOptions): () => void {
  return options.connection.registerHooks(
    {
      protocolVersion: 1,
      generation: options.generation(),
      events: ["before_send"],
    },
    async (request) => evaluate(options.engine, request),
  );
}

async function evaluate(
  engine: HookEngine,
  request: HookEvaluationRequest,
): Promise<HookEvaluationResponse> {
  if (request.event !== "before_send") {
    throw new Error(`unsupported hook event ${request.event}`);
  }
  const outcome = await engine.run(
    createBeforeSendAdapter(),
    request.request.message,
    {
      evaluationId: request.request.evaluationId,
      messageId: request.request.evaluationId,
    },
  );
  return {
    event: "before_send",
    result: {
      evaluationId: request.request.evaluationId,
      action: outcome.action === "reject" ? HookAction.Reject : HookAction.Continue,
      message: outcome.message,
      ...(outcome.timing ? { timing: outcome.timing } : {}),
      executedBy: [...outcome.executedBy],
    },
  };
}
