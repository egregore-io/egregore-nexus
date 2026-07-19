import { createHash } from "node:crypto";

import type { Client } from "@libsql/client";
import type {
  HookAfterReceiptRequest,
  HookAfterReceiptResult,
  HookExecutedBy,
  HookMessage,
  MessageMetadataMergeRequest,
  SendTarget,
} from "@shared/types";

import type { CanonicalProjectionEvent } from "../projection/apply";
import { callDaemonQuery, type DaemonIpcCaller } from "../daemon/ipc";
import { isPlainObject, mergeHookMetadata } from "./merge";
import { HookAuditStore, type HookReceiptRecord } from "./store";

export type AfterReceiptStatus = "ignored" | "pending" | "completed";

export interface AfterReceiptProcessorOptions {
  execute(request: HookAfterReceiptRequest): Promise<HookAfterReceiptResult>;
  mergeDaemon(request: MessageMetadataMergeRequest): Promise<unknown>;
  now?: () => number;
  generation?: () => string;
}

const gatewayCaller: DaemonIpcCaller = {
  name: "Nexus Gateway",
  project: "default",
  sessionId: "local-operator",
  runtimeId: "local-operator",
  kind: "human",
  tier: "admin",
};

export function createDaemonHookMetadataMerger(options: { nexusHome?: string } = {}) {
  return (request: MessageMetadataMergeRequest): Promise<unknown> =>
    callDaemonQuery("hook.metadata.merge", request, gatewayCaller, {
      ...(options.nexusHome ? { nexusHome: options.nexusHome } : {}),
    });
}

export class AfterReceiptProcessor {
  readonly #audit: HookAuditStore;
  readonly #now: () => number;
  readonly #running = new Map<string, Promise<AfterReceiptStatus>>();

  constructor(
    private readonly db: Client,
    private readonly options: AfterReceiptProcessorOptions,
  ) {
    this.#audit = new HookAuditStore(db);
    this.#now = options.now ?? Date.now;
  }

  async accept(event: CanonicalProjectionEvent): Promise<AfterReceiptStatus> {
    if (event.kind !== "message.accepted") return "ignored";
    const messageId = requiredString(event.payload, "messageId");
    const invocationId = receiptInvocationId(event.eventId, messageId);
    const original = projectionMessage(event.payload);
    await this.#audit.beginEvaluation({
      evaluationId: invocationId,
      messageId,
      event: "after_receipt",
      registryGeneration: this.options.generation?.() ?? "projection-v1",
      originalMessage: original,
      startedAt: this.#now(),
    });
    await this.#audit.claimReceipt(messageId, invocationId, this.#now());
    return this.#exclusive(messageId, () => this.#process(messageId));
  }

  async retryPending(): Promise<number> {
    let completed = 0;
    for (const receipt of await this.#audit.pendingReceipts()) {
      if (await this.#exclusive(receipt.messageId, () => this.#process(receipt.messageId)) === "completed") {
        completed += 1;
      }
    }
    return completed;
  }

  async settled(): Promise<void> {
    await Promise.allSettled([...this.#running.values()]);
  }

  async #process(messageId: string): Promise<AfterReceiptStatus> {
    const receipt = await this.#audit.receipt(messageId);
    if (!receipt) return "ignored";
    if (receipt.state === "completed") return "completed";
    if (receipt.state === "merge_pending") return this.#mergeDaemon(receipt);

    const evaluation = await this.#audit.evaluation(messageId, "after_receipt");
    if (!evaluation) {
      await this.#audit.failReceipt(messageId, "after_receipt evaluation is missing");
      return "pending";
    }
    await this.#audit.startReceiptAttempt(messageId);
    const existingExecutedBy = metadataExecutions(evaluation.originalMessage.metadata ?? {});
    let outcome: HookAfterReceiptResult;
    try {
      outcome = await this.options.execute({
        invocationId: receipt.invocationId,
        message: evaluation.originalMessage,
        receipt: { messageId },
        executedBy: existingExecutedBy,
      });
      if (outcome.invocationId !== receipt.invocationId) {
        throw new Error("after_receipt result invocationId does not match the request");
      }
    } catch (error) {
      const message = errorMessage(error);
      await this.#audit.failReceipt(messageId, message);
      return "pending";
    }

    const patch = outcome.metadata ?? {};
    const executedBy = uniqueExecutions(outcome.executedBy ?? []);
    try {
      await this.#completeGatewayMetadata(messageId, patch, executedBy);
    } catch (error) {
      await this.#audit.failReceipt(messageId, errorMessage(error));
      return "pending";
    }
    const completed = await this.#audit.receipt(messageId);
    return completed ? this.#mergeDaemon(completed) : "pending";
  }

  async #completeGatewayMetadata(
    messageId: string,
    hookPatch: Record<string, unknown>,
    executedBy: readonly HookExecutedBy[],
  ): Promise<void> {
    const tx = await this.db.transaction("write");
    try {
      const rows = await tx.execute({
        sql: "SELECT metadata_json FROM bus_messages WHERE message_id = ?",
        args: [messageId],
      });
      if (!rows.rows[0]) throw new Error(`canonical Gateway message is missing: ${messageId}`);
      const base = parseObject(rows.rows[0].metadata_json);
      const metadata = withExecutions(mergeHookMetadata(base, hookPatch), executedBy);
      const daemonPatch = withExecutions(structuredClone(hookPatch), executedBy, base);
      await tx.execute({
        sql: "UPDATE bus_messages SET metadata_json = ? WHERE message_id = ?",
        args: [JSON.stringify(metadata), messageId],
      });
      await tx.execute({
        sql: `UPDATE hook_receipt_completion
              SET state = 'merge_pending', completed_at = ?, metadata_patch_json = ?,
                  executed_by_json = ?, last_error = NULL
              WHERE message_id = ?`,
        args: [this.#now(), JSON.stringify(daemonPatch), JSON.stringify(executedBy), messageId],
      });
      await tx.commit();
    } catch (error) {
      try {
        await tx.rollback();
      } catch {
        // The original completion failure remains the useful error.
      }
      throw error;
    }
  }

  async #mergeDaemon(receipt: HookReceiptRecord): Promise<AfterReceiptStatus> {
    if (!receipt.metadataPatch) {
      await this.#audit.failReceipt(receipt.messageId, "after_receipt metadata patch is missing");
      return "pending";
    }
    try {
      await this.options.mergeDaemon({
        messageId: receipt.messageId,
        invocationId: receipt.invocationId,
        metadata: receipt.metadataPatch,
      });
      await this.#audit.acknowledgeReceiptMerge(receipt.messageId, this.#now());
      return "completed";
    } catch (error) {
      await this.#audit.failReceiptMerge(receipt.messageId, errorMessage(error));
      return "pending";
    }
  }

  #exclusive(messageId: string, run: () => Promise<AfterReceiptStatus>): Promise<AfterReceiptStatus> {
    const existing = this.#running.get(messageId);
    if (existing) return existing;
    const active = run().finally(() => this.#running.delete(messageId));
    this.#running.set(messageId, active);
    return active;
  }
}

function receiptInvocationId(eventId: string, messageId: string): string {
  return `hr_${createHash("sha256").update(`${eventId}\0${messageId}`).digest("hex")}`;
}

function projectionMessage(payload: unknown): HookMessage {
  const value = isPlainObject(payload) ? payload : {};
  const provenance = isPlainObject(value.provenance) ? value.provenance : {};
  const scope = requiredString(value, "scope");
  return {
    sender: {
      ...(typeof value.fromAgentId === "string" ? { agentId: value.fromAgentId } : {}),
      name: requiredString(value, "fromName"),
    },
    target: projectionTarget(scope, value, provenance),
    body: requiredString(value, "body"),
    ...(typeof value.summary === "string" ? { summary: value.summary } : {}),
    mention: Array.isArray(value.mention)
      ? value.mention.filter((entry): entry is string => typeof entry === "string")
      : [],
    metadata: parseObject(value.metadata),
  };
}

function projectionTarget(
  scope: string,
  value: Record<string, unknown>,
  provenance: Record<string, unknown>,
): SendTarget {
  if (scope === "dm") {
    return {
      verb: "dm",
      ...(typeof value.toName === "string" ? { name: value.toName } : {}),
      ...(typeof value.toAgentId === "string" ? { agentId: value.toAgentId } : {}),
    };
  }
  if (scope === "thread") {
    return { verb: "post", thread: firstString(value.threadName, provenance.thread, value.threadId) };
  }
  if (scope === "topic") {
    return { verb: "publish", topic: firstString(value.topic, provenance.topic) };
  }
  throw new Error(`unsupported accepted-message scope ${scope}`);
}

function withExecutions(
  metadata: Record<string, unknown>,
  additions: readonly HookExecutedBy[],
  provenanceBase: Record<string, unknown> = metadata,
): Record<string, unknown> {
  if (additions.length === 0) return metadata;
  const existing = metadataExecutions(provenanceBase);
  const executedBy = uniqueExecutions([...existing, ...additions]);
  const nexus = isPlainObject(metadata._nexus) ? structuredClone(metadata._nexus) : {};
  const hooks = isPlainObject(nexus.hooks) ? nexus.hooks : {};
  hooks.executedBy = executedBy;
  nexus.hooks = hooks;
  metadata._nexus = nexus;
  return metadata;
}

function metadataExecutions(metadata: Record<string, unknown>): HookExecutedBy[] {
  const nexus = isPlainObject(metadata._nexus) ? metadata._nexus : {};
  const hooks = isPlainObject(nexus.hooks) ? nexus.hooks : {};
  return Array.isArray(hooks.executedBy)
    ? hooks.executedBy.filter(isHookExecution)
    : [];
}

function uniqueExecutions(values: readonly HookExecutedBy[]): HookExecutedBy[] {
  const seen = new Set<string>();
  return values.filter((value) => {
    if (seen.has(value.invocationId)) return false;
    seen.add(value.invocationId);
    return true;
  });
}

function isHookExecution(value: unknown): value is HookExecutedBy {
  return isPlainObject(value) && typeof value.invocationId === "string";
}

function parseObject(value: unknown): Record<string, unknown> {
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return isPlainObject(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }
  return isPlainObject(value) ? structuredClone(value) : {};
}

function requiredString(value: unknown, key: string): string {
  if (!isPlainObject(value) || typeof value[key] !== "string" || value[key].length === 0) {
    throw new Error(`accepted message requires ${key}`);
  }
  return value[key];
}

function firstString(...values: unknown[]): string {
  const value = values.find((entry) => typeof entry === "string" && entry.length > 0);
  if (typeof value !== "string") throw new Error("accepted message target is missing");
  return value;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
