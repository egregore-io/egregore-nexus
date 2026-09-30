import { createHash } from "node:crypto";

import type { HookExecutedBy, HookMessage } from "@shared/types";

import type {
  HookEventAdapter,
  HookInvocation,
} from "./eventAdapter";
import { HOOK_PROTOCOL } from "./eventAdapter";
import type { HookManifest, HookRegistrySnapshot } from "./types";

export interface HookRunner {
  run(hook: HookManifest, invocation: HookInvocation): Promise<unknown>;
}

export interface HookRunnerExecution {
  readonly kind: "nexus.hook-runner-execution";
  readonly output: unknown;
  readonly executedBy: HookExecutedBy;
}

export class HookRunnerFailure extends Error {
  constructor(
    message: string,
    readonly executedBy: HookExecutedBy,
    cause?: unknown,
  ) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = "HookRunnerFailure";
  }
}

export interface HookRunContext {
  evaluationId: string;
  messageId: string;
}

export interface HookPipelineObserver {
  begin(input: {
    context: HookRunContext;
    event: string;
    generation: string;
    originalMessage: HookMessage;
    startedAt: number;
  }): Promise<{
    context?: HookRunContext;
    generation?: string;
    completedOutput?: unknown;
  } | void>;
  complete(context: HookRunContext, completedAt: number, output: unknown): Promise<void>;
  fail(context: HookRunContext, completedAt: number): Promise<void>;
}

export interface HookEngineOptions {
  snapshot: () => HookRegistrySnapshot;
  runner: HookRunner;
  invocationId?: (
    generation: string,
    hook: HookManifest,
    index: number,
    context?: HookRunContext,
  ) => string;
  observer?: HookPipelineObserver;
  now?: () => number;
}

export class HookEngine {
  readonly #snapshot: () => HookRegistrySnapshot;
  readonly #runner: HookRunner;
  readonly #invocationId: NonNullable<HookEngineOptions["invocationId"]>;
  readonly #observer?: HookPipelineObserver;
  readonly #now: () => number;
  readonly #running = new Map<string, Promise<unknown>>();

  constructor(options: HookEngineOptions) {
    this.#snapshot = options.snapshot;
    this.#runner = options.runner;
    this.#invocationId = options.invocationId ?? stableInvocationId;
    this.#observer = options.observer;
    this.#now = options.now ?? Date.now;
  }

  async settled(): Promise<void> {
    await Promise.allSettled([...this.#running.values()]);
  }

  async run<Input, State, Output>(
    adapter: HookEventAdapter<Input, State, Output>,
    input: Input,
    context?: HookRunContext,
  ): Promise<Output> {
    if (!context) return this.#runOnce(adapter, input, context);
    const key = `${adapter.event}\0${context.evaluationId}`;
    const existing = this.#running.get(key);
    if (existing) return existing as Promise<Output>;
    const active = this.#runOnce(adapter, input, context);
    const tracked = active.finally(() => {
      if (this.#running.get(key) === tracked) this.#running.delete(key);
    });
    this.#running.set(key, tracked);
    return tracked;
  }

  async #runOnce<Input, State, Output>(
    adapter: HookEventAdapter<Input, State, Output>,
    input: Input,
    context?: HookRunContext,
  ): Promise<Output> {
    const snapshot = this.#snapshot();
    const hooks = snapshot.hooks.filter((hook) => hook.enabled && hook.event === adapter.event);
    let state = adapter.initial(input);
    let generation = snapshot.generation;
    let runContext = context;

    if (context && this.#observer) {
      const payload = adapter.payload(state);
      const checkpoint = await this.#observer.begin({
        context,
        event: adapter.event,
        generation: snapshot.generation,
        originalMessage: (payload.message ?? {}) as HookMessage,
        startedAt: this.#now(),
      });
      if (checkpoint?.context) runContext = checkpoint.context;
      if (checkpoint?.generation) generation = checkpoint.generation;
      if (checkpoint && "completedOutput" in checkpoint) {
        return checkpoint.completedOutput as Output;
      }
      if (generation !== snapshot.generation) {
        throw new Error(
          `pinned hook generation is unavailable for evaluation ${runContext?.evaluationId}`,
        );
      }
    }

    try {
      for (const [index, hook] of hooks.entries()) {
        const payload = adapter.payload(state);
        const invocation: HookInvocation = {
          protocol: HOOK_PROTOCOL,
          invocationId: this.#invocationId(generation, hook, index, runContext),
          event: adapter.event,
          handler: {
            hookId: hook.id,
            entrypoint: hook.handler.entrypoint,
            runtime: hook.handler.run[0] ?? hook.handler.kind,
          },
          executedBy: [...(adapter.executions?.(state) ?? [])],
          message: (payload.message ?? {}) as HookInvocation["message"],
          ...payload,
          ...(runContext ? {
            evaluationId: runContext.evaluationId,
            messageId: runContext.messageId,
          } : {}),
        };
        try {
          const raw = await this.#runner.run(hook, invocation);
          const execution = isHookRunnerExecution(raw) ? raw : undefined;
          const applied = adapter.apply(state, execution ? execution.output : raw);
          state = execution && adapter.attachExecution
            ? adapter.attachExecution(applied.state, execution.executedBy)
            : applied.state;
          if (applied.stop) break;
        } catch (error) {
          if (error instanceof HookRunnerFailure && adapter.attachExecution) {
            state = adapter.attachExecution(state, error.executedBy);
          }
          if (hook.onFailure === "continue") continue;
          throw error;
        }
      }

      const output = adapter.finish(state);
      if (runContext && this.#observer) {
        await this.#observer.complete(runContext, this.#now(), output);
      }
      return output;
    } catch (error) {
      if (runContext && this.#observer) await this.#observer.fail(runContext, this.#now());
      throw error;
    }
  }
}

function stableInvocationId(
  generation: string,
  hook: HookManifest,
  index: number,
  context?: HookRunContext,
): string {
  const scope = context?.evaluationId ?? "unscoped";
  const digest = createHash("sha256")
    .update(`${scope}\0${generation}\0${hook.id}\0${index}`)
    .digest("hex");
  return `hi_${digest}`;
}

export function hookRunnerExecution(
  output: unknown,
  executedBy: HookExecutedBy,
): HookRunnerExecution {
  return { kind: "nexus.hook-runner-execution", output, executedBy };
}

function isHookRunnerExecution(value: unknown): value is HookRunnerExecution {
  return Boolean(
    value &&
    typeof value === "object" &&
    "kind" in value &&
    value.kind === "nexus.hook-runner-execution" &&
    "executedBy" in value,
  );
}
