import { describe, expect, it } from "vitest";

import {
  HookAction,
  type GatewayHookCapabilities,
  type HookEvaluationRequest,
  type HookEvaluationResponse,
} from "@shared/types";

import { HookEngine } from "./engine";
import { registerGatewayHookBridge, type GatewayHookConnection } from "./bridge";

describe("Gateway hook bridge", () => {
  it("registers the active generation and maps before_send through the generic engine", async () => {
    let capabilities: GatewayHookCapabilities | undefined;
    let handler:
      | ((request: HookEvaluationRequest) => Promise<HookEvaluationResponse>)
      | undefined;
    let closed = false;
    const connection: GatewayHookConnection = {
      registerHooks(nextCapabilities, nextHandler) {
        capabilities = nextCapabilities;
        handler = nextHandler;
        return () => {
          closed = true;
        };
      },
    };
    const engine = new HookEngine({
      snapshot: () => ({
        generation: "sha256:generation",
        createdAt: 1,
        hooks: [],
      }),
      runner: { run: async () => ({ action: "continue" }) },
    });

    const close = registerGatewayHookBridge({
      connection,
      engine,
      generation: () => "sha256:generation",
    });
    expect(capabilities).toEqual({
      protocolVersion: 1,
      generation: "sha256:generation",
      events: ["before_send"],
    });

    await expect(
      handler?.({
        event: "before_send",
        request: {
          evaluationId: "he_message",
          message: {
            sender: { name: "fixture-sender" },
            target: { verb: "post", thread: "release" },
            body: "hello",
            mention: [],
            metadata: {},
          },
        },
      }),
    ).resolves.toEqual({
      event: "before_send",
      result: {
        evaluationId: "he_message",
        action: HookAction.Continue,
        message: {
          sender: { name: "fixture-sender" },
          target: { verb: "post", thread: "release" },
          body: "hello",
          mention: [],
          metadata: {},
        },
        executedBy: [],
      },
    });

    close();
    expect(closed).toBe(true);
  });

  it("rejects events that are not advertised yet", async () => {
    let handler:
      | ((request: HookEvaluationRequest) => Promise<HookEvaluationResponse>)
      | undefined;
    const connection: GatewayHookConnection = {
      registerHooks(_capabilities, nextHandler) {
        handler = nextHandler;
        return () => {};
      },
    };
    const engine = new HookEngine({
      snapshot: () => ({ generation: "generation", createdAt: 1, hooks: [] }),
      runner: { run: async () => ({}) },
    });
    registerGatewayHookBridge({ connection, engine, generation: () => "generation" });

    await expect(
      handler?.({
        event: "after_receipt",
        request: {
          invocationId: "hi_receipt",
          message: {
            sender: { name: "fixture-sender" },
            target: { verb: "post", thread: "release" },
            body: "hello",
          },
          receipt: { messageId: "m_message" },
        },
      }),
    ).rejects.toThrow(/unsupported hook event after_receipt/);
  });
});
