import type {
  DeliveryTiming,
  HookAfterReceiptRequest,
  HookAfterReceiptResult,
  HookExecutedBy,
  HookMessage,
} from "@shared/types";

import { isPlainObject, mergeHookMetadata } from "./merge";

export const HOOK_PROTOCOL = "nexus.hooks/v1" as const;

export interface HookInvocation {
  protocol: typeof HOOK_PROTOCOL;
  invocationId: string;
  event: string;
  handler: {
    hookId: string;
    entrypoint: string;
    runtime: string;
  };
  executedBy: readonly HookExecutedBy[];
  message: HookMessage;
  [key: string]: unknown;
}

export interface HookProgramResult {
  action?: "continue" | "reject";
  message?: {
    body?: string;
    summary?: string | null;
    mention?: string[];
  };
  metadata?: Record<string, unknown>;
  timing?: DeliveryTiming;
}

export interface HookApplyResult<State> {
  state: State;
  stop: boolean;
}

export interface HookEventAdapter<Input, State, Output> {
  readonly event: string;
  readonly blocking: boolean;
  initial(input: Input): State;
  payload(state: State): Record<string, unknown>;
  apply(state: State, rawResult: unknown): HookApplyResult<State>;
  executions?(state: State): readonly HookExecutedBy[];
  attachExecution?(state: State, execution: HookExecutedBy): State;
  finish(state: State): Output;
}

export interface BeforeSendOutcome {
  action: "continue" | "reject";
  message: HookMessage;
  timing?: DeliveryTiming;
  executedBy: readonly HookExecutedBy[];
}

interface BeforeSendState extends BeforeSendOutcome {}

export function createBeforeSendAdapter(): HookEventAdapter<
  HookMessage,
  BeforeSendState,
  BeforeSendOutcome
> {
  return {
    event: "before_send",
    blocking: true,
    initial: (message) => ({
      action: "continue",
      message: cloneMessage(message),
      executedBy: [],
    }),
    payload: (state) => ({ message: cloneMessage(state.message) }),
    executions: (state) => state.executedBy,
    attachExecution: (state, execution) => ({
      ...state,
      executedBy: appendExecution(state.executedBy, execution),
    }),
    apply(state, rawResult) {
      const result = parseBeforeSendResult(rawResult);
      const message = applyMessagePatch(state.message, result);
      const timing = result.timing ?? state.timing;
      const action = result.action ?? "continue";
      return {
        state: {
          action,
          message,
          ...(timing ? { timing } : {}),
          executedBy: state.executedBy,
        },
        stop: action === "reject",
      };
    },
    finish: (state) => state,
  };
}

interface AfterReceiptState {
  invocationId: string;
  message: HookMessage;
  receipt: HookAfterReceiptRequest["receipt"];
  metadata: Record<string, unknown>;
  executedBy: readonly HookExecutedBy[];
}

export function createAfterReceiptAdapter(): HookEventAdapter<
  HookAfterReceiptRequest,
  AfterReceiptState,
  HookAfterReceiptResult
> {
  return {
    event: "after_receipt",
    blocking: false,
    initial: (request) => ({
      invocationId: request.invocationId,
      message: cloneMessage(request.message),
      receipt: structuredClone(request.receipt),
      metadata: {},
      executedBy: [...(request.executedBy ?? [])],
    }),
    payload: (state) => ({
      message: cloneMessage(state.message),
      receipt: structuredClone(state.receipt),
    }),
    executions: (state) => state.executedBy,
    attachExecution: (state, execution) => ({
      ...state,
      executedBy: appendExecution(state.executedBy, execution),
    }),
    apply(state, rawResult) {
      if (!isPlainObject(rawResult)) {
        throw new HookResultValidationError("hook result must be an object");
      }
      requireKnownFields(rawResult, new Set(["metadata"]), "result");
      if (rawResult.metadata !== undefined && !isPlainObject(rawResult.metadata)) {
        throw new HookResultValidationError("result.metadata must be an object");
      }
      return {
        state: {
          ...state,
          metadata: rawResult.metadata
            ? mergeHookMetadata(state.metadata, rawResult.metadata)
            : state.metadata,
        },
        stop: false,
      };
    },
    finish: (state) => ({
      invocationId: state.invocationId,
      metadata: state.metadata,
      executedBy: [...state.executedBy],
    }),
  };
}

export class HookResultValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "HookResultValidationError";
  }
}

function parseBeforeSendResult(raw: unknown): HookProgramResult {
  if (!isPlainObject(raw)) throw new HookResultValidationError("hook result must be an object");
  requireKnownFields(raw, new Set(["action", "message", "metadata", "timing"]), "result");

  const action = raw.action;
  if (action !== undefined && action !== "continue" && action !== "reject") {
    throw new HookResultValidationError("result.action is not allowed");
  }
  const timing = raw.timing;
  if (
    timing !== undefined &&
    timing !== "interrupt" &&
    timing !== "yield_turn" &&
    timing !== "after_tool_loop"
  ) {
    throw new HookResultValidationError("result.timing is not allowed");
  }

  let message: HookProgramResult["message"];
  if (raw.message !== undefined) {
    if (!isPlainObject(raw.message)) {
      throw new HookResultValidationError("result.message must be an object");
    }
    requireKnownFields(raw.message, new Set(["body", "summary", "mention"]), "message");
    if (raw.message.body !== undefined && typeof raw.message.body !== "string") {
      throw new HookResultValidationError("message.body is not allowed");
    }
    if (
      raw.message.summary !== undefined &&
      raw.message.summary !== null &&
      typeof raw.message.summary !== "string"
    ) {
      throw new HookResultValidationError("message.summary is not allowed");
    }
    if (
      raw.message.mention !== undefined &&
      (!Array.isArray(raw.message.mention) ||
        raw.message.mention.some((entry) => typeof entry !== "string"))
    ) {
      throw new HookResultValidationError("message.mention is not allowed");
    }
    message = raw.message as HookProgramResult["message"];
  }

  let metadata: Record<string, unknown> | undefined;
  if (raw.metadata !== undefined) {
    if (!isPlainObject(raw.metadata)) {
      throw new HookResultValidationError("result.metadata must be an object");
    }
    metadata = raw.metadata;
  }

  return {
    ...(action ? { action } : {}),
    ...(message ? { message } : {}),
    ...(metadata ? { metadata } : {}),
    ...(timing ? { timing: timing as DeliveryTiming } : {}),
  };
}

function applyMessagePatch(message: HookMessage, result: HookProgramResult): HookMessage {
  const next = cloneMessage(message);
  if (result.message?.body !== undefined) next.body = result.message.body;
  if (result.message && Object.hasOwn(result.message, "summary")) {
    if (result.message.summary === null) delete next.summary;
    else next.summary = result.message.summary;
  }
  if (result.message?.mention !== undefined) next.mention = [...result.message.mention];
  if (result.metadata) {
    next.metadata = mergeHookMetadata(next.metadata ?? {}, result.metadata);
  }
  return next;
}

function cloneMessage(message: HookMessage): HookMessage {
  return {
    sender: { ...message.sender },
    target: structuredClone(message.target),
    body: message.body,
    ...(message.summary !== undefined ? { summary: message.summary } : {}),
    mention: [...(message.mention ?? [])],
    metadata: structuredClone(message.metadata ?? {}),
  };
}

function appendExecution(
  values: readonly HookExecutedBy[],
  execution: HookExecutedBy,
): HookExecutedBy[] {
  return values.some((value) => value.invocationId === execution.invocationId)
    ? [...values]
    : [...values, execution];
}

function requireKnownFields(
  value: Record<string, unknown>,
  fields: ReadonlySet<string>,
  label: string,
): void {
  const unknown = Object.keys(value).find((field) => !fields.has(field));
  if (unknown) throw new HookResultValidationError(`${label} field ${unknown} is not allowed`);
}
