import { homedir } from "node:os";
import { join } from "node:path";

import type { Client } from "@libsql/client";
import type {
  HookAfterReceiptRequest,
  HookExecutedBy,
} from "@shared/types";

import {
  sharedDaemonPushConnector,
  type DaemonPushConnection,
} from "../agui/daemonPushRelay.mjs";
import type { CanonicalProjectionEvent } from "../projection/apply";
import { getGatewayStore } from "../store/client";
import {
  AfterReceiptProcessor,
  createDaemonHookMetadataMerger,
} from "./afterReceipt";
import {
  registerGatewayHookBridge,
  type GatewayHookConnection as BridgeConnection,
} from "./bridge";
import {
  HookEngine,
  HookRunnerFailure,
  hookRunnerExecution,
  type HookPipelineObserver,
  type HookRunner,
} from "./engine";
import { createAfterReceiptAdapter } from "./eventAdapter";
import { LocalCommandRunner } from "./localCommandRunner";
import { isPlainObject } from "./merge";
import { HookRegistry } from "./registry";
import {
  digestHookArtifact,
  HookSigner,
  verifyHookExecution,
} from "./signing";
import {
  HookAuditStore,
  type HookExecutionRecord,
  type HookReceiptRecord,
} from "./store";
import type { HookManifest, HookRegistryReload } from "./types";
import { setGatewayHookDiagnostics } from "./diagnostics";
import { HOOK_EVENTS } from "./events";

const SUPPORTED_EVENTS = new Set<string>(HOOK_EVENTS);

export interface GatewayHookConnection extends BridgeConnection {
  ready: Promise<void>;
  close(): void;
}

export interface GatewayHookServiceOptions {
  db: Client;
  nexusHome: string;
  hooksDirectory?: string;
  connection?: GatewayHookConnection;
  runner?: HookRunner;
  mergeDaemon?: ConstructorParameters<typeof AfterReceiptProcessor>[1]["mergeDaemon"];
  now?: () => number;
}

export class GatewayHookService {
  #closeBridge?: () => void;
  #closeRegistryListener?: () => void;
  #closed = false;
  #receiptRetry: Promise<number> = Promise.resolve(0);

  constructor(
    readonly registry: HookRegistry,
    readonly signer: HookSigner,
    readonly auditStore: HookAuditStore,
    readonly engine: HookEngine,
    readonly receiptProcessor: AfterReceiptProcessor,
    private readonly connection?: GatewayHookConnection,
  ) {}

  async start(): Promise<void> {
    this.#closeRegistryListener = this.registry.onReload((reload) => {
      if (reload.activated) this.#registerBridge();
    });
    await this.registry.start();
    if (!this.#closeBridge) this.#registerBridge();
    await this.connection?.ready;
    this.#receiptRetry = this.receiptProcessor.retryPending().catch((error) => {
      process.stderr.write(`Gateway hook receipt retry failed: ${String(error)}\n`);
      return 0;
    });
  }

  generation(): string {
    return this.registry.snapshot().generation;
  }

  errors(): readonly string[] {
    return this.registry.errors();
  }

  reload(): Promise<HookRegistryReload> {
    return this.registry.reload();
  }

  afterReceipt(event: CanonicalProjectionEvent): Promise<unknown> {
    return this.receiptProcessor.accept(event);
  }

  async list(includePrivate: boolean): Promise<Record<string, unknown>> {
    const snapshot = this.registry.snapshot();
    return {
      generation: snapshot.generation,
      createdAt: snapshot.createdAt,
      errors: includePrivate
        ? [...this.registry.errors()]
        : publicRegistryErrors(this.registry.errors()),
      includePrivate,
      hooks: snapshot.hooks.map((hook) => hookView(hook, includePrivate)),
    };
  }

  async publicKey(): Promise<Record<string, unknown>> {
    return {
      algorithm: "ed25519",
      keyId: this.signer.keyId,
      publicKey: this.signer.publicKeyPem(),
    };
  }

  async audit(limit: number, includePrivate = true): Promise<Record<string, unknown>> {
    const executions = (await this.auditStore.executions(limit)).map((execution) => {
      const executedBy = execution.executedBy;
      const view = {
        ...execution,
        verified: isHookExecutedBy(executedBy)
          ? verifyHookExecution(executedBy, execution.event, this.signer.publicKey)
          : false,
      };
      if (includePrivate) return view;
      const { result: _privateResult, ...redacted } = view;
      return redacted;
    });
    return {
      keyId: this.signer.keyId,
      limit: normalizedLimit(limit),
      includePrivate,
      executions,
      pendingReceipts: includePrivate
        ? await this.auditStore.pendingReceipts()
        : (await this.auditStore.pendingReceipts()).map(publicReceiptView),
    };
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#closeBridge?.();
    this.#closeBridge = undefined;
    this.#closeRegistryListener?.();
    this.#closeRegistryListener = undefined;
    this.registry.close();
    await this.registry.settled();
    await this.engine.settled();
    await this.#receiptRetry;
    await this.receiptProcessor.settled();
  }

  #registerBridge(): void {
    if (!this.connection) return;
    const previous = this.#closeBridge;
    const next = registerGatewayHookBridge({
      connection: this.connection,
      engine: this.engine,
      generation: () => this.generation(),
    });
    this.#closeBridge = next;
    previous?.();
  }
}

export async function createGatewayHookService(
  options: GatewayHookServiceOptions,
): Promise<GatewayHookService> {
  const now = options.now ?? Date.now;
  const registry = new HookRegistry({
    directory: options.hooksDirectory ?? join(options.nexusHome, "gateway", "hooks.d"),
    supportedEvents: SUPPORTED_EVENTS,
    now,
  });
  const signer = await HookSigner.loadOrCreate(join(options.nexusHome, "gateway", "keys"));
  const auditStore = new HookAuditStore(options.db);
  const observer = auditObserver(auditStore);
  const runner = new AttestedHookRunner(
    options.runner ?? new LocalCommandRunner(),
    auditStore,
    signer,
    now,
  );
  const engine = new HookEngine({
    snapshot: () => registry.snapshot(),
    runner,
    observer,
    now,
  });
  const receiptProcessor = new AfterReceiptProcessor(options.db, {
    execute: (request) => executeAfterReceipt(engine, request),
    mergeDaemon: options.mergeDaemon ?? createDaemonHookMetadataMerger({
      nexusHome: options.nexusHome,
    }),
    generation: () => registry.snapshot().generation,
    now,
  });
  const service = new GatewayHookService(
    registry,
    signer,
    auditStore,
    engine,
    receiptProcessor,
    options.connection,
  );
  await service.start();
  return service;
}

class AttestedHookRunner implements HookRunner {
  constructor(
    private readonly runner: HookRunner,
    private readonly store: HookAuditStore,
    private readonly signer: HookSigner,
    private readonly now: () => number,
  ) {}

  async run(hook: HookManifest, invocation: Parameters<HookRunner["run"]>[1]): Promise<unknown> {
    const existing = await this.store.execution(invocation.invocationId);
    if (existing) {
      requireInvocationMatch(existing, hook, invocation);
      if (existing.outcome === "completed") {
        if (!("result" in existing) || !isHookExecutedBy(existing.executedBy)) {
          throw new Error(`completed hook invocation ${invocation.invocationId} is incomplete`);
        }
        return hookRunnerExecution(existing.result, existing.executedBy);
      }
      if (existing.outcome === "running") {
        throw new Error(
          `hook invocation ${invocation.invocationId} has an ambiguous interrupted execution`,
        );
      }
    }
    const startedAt = this.now();
    let artifactDigest: string;
    try {
      artifactDigest = await digestHookArtifact(hook);
    } catch (error) {
      const completedAt = this.now();
      const executedBy = this.signer.attest({
        invocationId: invocation.invocationId,
        hook,
        event: invocation.event,
        artifactDigest: "unavailable",
        startedAt,
        completedAt,
        outcome: failureOutcome(hook),
      });
      await this.store.recordExecution({
        invocationId: invocation.invocationId,
        evaluationId: evaluationId(invocation),
        hookId: hook.id,
        event: invocation.event,
        artifactDigest: "unavailable",
        startedAt,
        completedAt,
        outcome: failureOutcome(hook),
        executedBy,
      });
      throw new HookRunnerFailure(errorMessage(error), executedBy, error);
    }

    await this.store.startExecution({
      invocationId: invocation.invocationId,
      evaluationId: evaluationId(invocation),
      hookId: hook.id,
      event: invocation.event,
      artifactDigest,
      startedAt,
    });

    let output: unknown;
    try {
      output = await this.runner.run(hook, invocation);
    } catch (error) {
      const completedAt = this.now();
      const executedBy = this.signer.attest({
        invocationId: invocation.invocationId,
        hook,
        event: invocation.event,
        artifactDigest,
        startedAt,
        completedAt,
        outcome: failureOutcome(hook),
      });
      await this.store.recordExecution({
        invocationId: invocation.invocationId,
        evaluationId: evaluationId(invocation),
        hookId: hook.id,
        event: invocation.event,
        artifactDigest,
        startedAt,
        completedAt,
        outcome: failureOutcome(hook),
        executedBy,
      });
      throw new HookRunnerFailure(errorMessage(error), executedBy, error);
    }

    const completedAt = this.now();
    const executedBy = this.signer.attest({
      invocationId: invocation.invocationId,
      hook,
      event: invocation.event,
      artifactDigest,
      startedAt,
      completedAt,
      outcome: "completed",
    });
    await this.store.recordExecution({
      invocationId: invocation.invocationId,
      evaluationId: evaluationId(invocation),
      hookId: hook.id,
      event: invocation.event,
      artifactDigest,
      startedAt,
      completedAt,
      outcome: "completed",
      executedBy,
      result: output,
    });
    return hookRunnerExecution(output, executedBy);
  }
}

function auditObserver(store: HookAuditStore): HookPipelineObserver {
  return {
    async begin(input) {
      const record = await store.beginEvaluation({
        evaluationId: input.context.evaluationId,
        messageId: input.context.messageId,
        event: input.event,
        registryGeneration: input.generation,
        originalMessage: input.originalMessage,
        startedAt: input.startedAt,
      });
      const checkpoint = {
        context: {
          evaluationId: record.evaluationId,
          messageId: record.messageId,
        },
        generation: record.registryGeneration,
      };
      if (record.state !== "completed") return checkpoint;
      if (!("completedResult" in record)) {
        throw new Error(`completed hook evaluation ${record.evaluationId} has no stored result`);
      }
      return { ...checkpoint, completedOutput: record.completedResult };
    },
    complete: (context, completedAt, output) =>
      store.completeEvaluation(context.evaluationId, completedAt, output),
    fail: (context, completedAt) => store.failEvaluation(context.evaluationId, completedAt),
  };
}

function executeAfterReceipt(engine: HookEngine, request: HookAfterReceiptRequest) {
  return engine.run(createAfterReceiptAdapter(), request, {
    evaluationId: request.invocationId,
    messageId: request.receipt.messageId,
  });
}

function evaluationId(invocation: { evaluationId?: unknown; invocationId: string }): string {
  return typeof invocation.evaluationId === "string"
    ? invocation.evaluationId
    : invocation.invocationId;
}

function hookView(hook: HookManifest, includePrivate: boolean): Record<string, unknown> {
  if (includePrivate) return structuredClone(hook) as unknown as Record<string, unknown>;
  return {
    version: hook.version,
    id: hook.id,
    event: hook.event,
    order: hook.order,
    timeoutMs: hook.timeoutMs,
    onFailure: hook.onFailure,
    enabled: hook.enabled,
    handler: { kind: hook.handler.kind },
  };
}

function isHookExecutedBy(value: unknown): value is HookExecutedBy {
  return isPlainObject(value) && typeof value.invocationId === "string";
}

function normalizedLimit(limit: number): number {
  return Number.isSafeInteger(limit) && limit > 0 ? Math.min(limit, 500) : 100;
}

function publicRegistryErrors(errors: readonly string[]): string[] {
  return errors.length === 0 ? [] : [`${errors.length} hook manifest error(s)`];
}

function publicReceiptView(receipt: HookReceiptRecord) {
  return {
    messageId: receipt.messageId,
    invocationId: receipt.invocationId,
    state: receipt.state,
    claimedAt: receipt.claimedAt,
    attempts: receipt.attempts,
  };
}

function failureOutcome(hook: HookManifest): "continued_failure" | "failed" {
  return hook.onFailure === "continue" ? "continued_failure" : "failed";
}

function requireInvocationMatch(
  execution: HookExecutionRecord,
  hook: HookManifest,
  invocation: Parameters<HookRunner["run"]>[1],
): void {
  if (
    execution.evaluationId !== evaluationId(invocation) ||
    execution.hookId !== hook.id ||
    execution.event !== invocation.event
  ) {
    throw new Error(`hook invocation ID collision for ${invocation.invocationId}`);
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function resolveNexusHome(env: NodeJS.ProcessEnv = process.env): string {
  return env.NEXUS_HOME?.trim() || join(homedir(), ".nexus");
}

let processService: Promise<GatewayHookService> | undefined;

export function startGatewayHookService(): Promise<GatewayHookService> {
  processService ??= startProcessHookService();
  return processService;
}

export async function stopGatewayHookService(): Promise<void> {
  const pending = processService;
  processService = undefined;
  setGatewayHookDiagnostics(undefined);
  await (await pending)?.close();
}

async function startProcessHookService(): Promise<GatewayHookService> {
  const connection = sharedDaemonPushConnector() as DaemonPushConnection | undefined;
  const service = await createGatewayHookService({
    db: await getGatewayStore(),
    nexusHome: resolveNexusHome(),
    ...(connection?.registerHooks ? { connection: connection as GatewayHookConnection } : {}),
  });
  setGatewayHookDiagnostics(service);
  return service;
}
