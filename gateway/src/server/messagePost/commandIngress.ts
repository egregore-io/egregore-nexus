// Store-backed Message Post command ingress.
//
// Message Post is one command-intent kind. This module keeps the typed
// MessagePostSender seam while the shared command submitter handles the
// daemon-owned `command_intents` queue.
import { GatewayError } from "@server/api/http";
import type {
  GatewayCallerIdentity,
  MessagePostSender,
  MessagePostSendOptions,
} from "@server/api/http";
import { isLocalOperatorCaller } from "@server/auth/webAuthMode";
import type { Ack, SendRequest } from "@shared/types";
import {
  COMMAND_KINDS,
  submitCommandIntent,
  type CommandIngressOptions,
} from "@server/command/ingress";
import { assertSendRequestBody } from "@server/messagePost/validate";

export type CommandIngressSenderOptions = CommandIngressOptions;

/** Build a Message Post sender that submits daemon command intents. */
export function createCommandIngressSender(
  opts: CommandIngressSenderOptions = {},
): MessagePostSender {
  return {
    send: (req, caller, sendOpts) =>
      sendViaCommandIngress(req, caller, opts, sendOpts),
  };
}

/** Submit one Message Post command intent and return the daemon-written Ack. */
export async function sendViaCommandIngress(
  req: SendRequest,
  caller: GatewayCallerIdentity,
  opts: CommandIngressSenderOptions = {},
  sendOpts: MessagePostSendOptions = {},
): Promise<Ack> {
  requireHumanCaller(caller);
  assertSendRequestBody(req);
  return parseAck(
    await submitCommandIntent<unknown>(
      COMMAND_KINDS.messagePostSend,
      req,
      caller,
      opts,
      sendOpts.idempotencyKey,
    ),
  );
}

function requireHumanCaller(
  caller: GatewayCallerIdentity,
): asserts caller is GatewayCallerIdentity & { sessionId: string } {
  if (isLocalOperatorCaller(caller)) return;
  if (
    caller.sessionId?.startsWith("transport:") &&
    caller.kind === "human" &&
    caller.locality === "external" &&
    caller.access === "guest" &&
    caller.principalId?.startsWith("x_") &&
    caller.tier === "agent" &&
    caller.credentialFacet === "source" &&
    !caller.agentId &&
    !caller.clientKey &&
    (!caller.runtimeId || caller.runtimeId === caller.sessionId)
  ) return;
  if (!caller.sessionId || !caller.clientKey) {
    throw new GatewayError(401, "message post requires a logged-in human identity");
  }
}

function parseAck(raw: unknown): Ack {
  try {
    const value = raw as Partial<Ack>;
    if (!value || typeof value.messageId !== "string") {
      throw new Error("missing messageId");
    }
    return { messageId: value.messageId };
  } catch {
    throw new GatewayError(502, "message post command returned invalid Ack");
  }
}
