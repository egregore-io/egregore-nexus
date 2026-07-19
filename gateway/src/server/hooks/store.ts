import type { Client } from "@libsql/client";
import type { HookMessage } from "@shared/types";

export interface HookEvaluationInput {
  evaluationId: string;
  messageId: string;
  event: string;
  registryGeneration: string;
  originalMessage: HookMessage;
  startedAt: number;
}

export interface HookEvaluationRecord extends HookEvaluationInput {
  state: string;
  completedAt?: number;
  completedResult?: unknown;
}

export interface HookExecutionInput {
  invocationId: string;
  evaluationId: string;
  hookId: string;
  event: string;
  artifactDigest: string;
  startedAt: number;
  completedAt: number;
  outcome: string;
  executedBy: unknown;
  result?: unknown;
}

export interface HookExecutionRecord extends HookExecutionInput {}

export type HookExecutionStart = Pick<
  HookExecutionInput,
  "invocationId" | "evaluationId" | "hookId" | "event" | "artifactDigest" | "startedAt"
>;

export interface HookReceiptRecord {
  messageId: string;
  invocationId: string;
  state: string;
  claimedAt: number;
  completedAt?: number;
  metadataPatch?: Record<string, unknown>;
  executedBy?: unknown[];
  mergeAckedAt?: number;
  lastError?: string;
  attempts: number;
}

export class HookAuditStore {
  constructor(readonly db: Client) {}

  async beginEvaluation(input: HookEvaluationInput): Promise<HookEvaluationRecord> {
    await this.db.execute({
      sql: `INSERT INTO hook_pipeline_evaluations (
              evaluation_id, message_id, event, registry_generation,
              original_message_json, state, started_at
            ) VALUES (?, ?, ?, ?, ?, 'running', ?)
            ON CONFLICT DO NOTHING`,
      args: [
        input.evaluationId,
        input.messageId,
        input.event,
        input.registryGeneration,
        JSON.stringify(input.originalMessage),
        input.startedAt,
      ],
    });
    const result = await this.db.execute({
      sql: `SELECT evaluation_id, message_id, event, registry_generation,
                   original_message_json, state, started_at, completed_at, result_json
            FROM hook_pipeline_evaluations
            WHERE message_id = ? AND event = ?`,
      args: [input.messageId, input.event],
    });
    const row = result.rows[0];
    if (!row) {
      throw new Error(`hook evaluation ID collision for ${input.evaluationId}`);
    }
    const record = evaluationRecord(row);
    if (record.state === "failed") {
      await this.db.execute({
        sql: `UPDATE hook_pipeline_evaluations
              SET state = 'running', completed_at = NULL, result_json = NULL
              WHERE evaluation_id = ? AND state = 'failed'`,
        args: [record.evaluationId],
      });
      const { completedAt: _completedAt, completedResult: _completedResult, ...retry } = record;
      return { ...retry, state: "running" };
    }
    return record;
  }

  async completeEvaluation(
    evaluationId: string,
    completedAt: number,
    result: unknown,
  ): Promise<void> {
    await this.db.execute({
      sql: `UPDATE hook_pipeline_evaluations
            SET state = 'completed', completed_at = ?, result_json = ?
            WHERE evaluation_id = ?`,
      args: [completedAt, JSON.stringify(result), evaluationId],
    });
  }

  async failEvaluation(evaluationId: string, completedAt: number): Promise<void> {
    await this.db.execute({
      sql: `UPDATE hook_pipeline_evaluations
            SET state = 'failed', completed_at = ?
            WHERE evaluation_id = ?`,
      args: [completedAt, evaluationId],
    });
  }

  async evaluation(messageId: string, event: string): Promise<HookEvaluationRecord | undefined> {
    const result = await this.db.execute({
      sql: `SELECT evaluation_id, message_id, event, registry_generation,
                   original_message_json, state, started_at, completed_at, result_json
            FROM hook_pipeline_evaluations WHERE message_id = ? AND event = ?`,
      args: [messageId, event],
    });
    const row = result.rows[0];
    if (!row) return undefined;
    return evaluationRecord(row);
  }

  async recordExecution(input: HookExecutionInput): Promise<void> {
    const existing = await this.execution(input.invocationId);
    if (existing) {
      requireSameInvocation(existing, input);
      if (existing.outcome === "completed") {
        requireIdenticalCompletedInvocation(existing, input);
        return;
      }
      await this.db.execute({
        sql: `UPDATE hook_handler_executions
              SET artifact_digest = ?, started_at = ?, completed_at = ?, outcome = ?,
                  executed_by_json = ?, result_json = ?
              WHERE invocation_id = ?`,
        args: [
          input.artifactDigest,
          input.startedAt,
          input.completedAt,
          input.outcome,
          JSON.stringify(input.executedBy),
          input.result === undefined ? null : JSON.stringify(input.result),
          input.invocationId,
        ],
      });
      return;
    }
    await this.db.execute({
      sql: `INSERT INTO hook_handler_executions (
              invocation_id, evaluation_id, hook_id, event, artifact_digest,
              started_at, completed_at, outcome, executed_by_json, result_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      args: [
        input.invocationId,
        input.evaluationId,
        input.hookId,
        input.event,
        input.artifactDigest,
        input.startedAt,
        input.completedAt,
        input.outcome,
        JSON.stringify(input.executedBy),
        input.result === undefined ? null : JSON.stringify(input.result),
      ],
    });
  }

  async startExecution(input: HookExecutionStart): Promise<void> {
    const existing = await this.execution(input.invocationId);
    if (existing) {
      requireSameInvocation(existing, input);
      if (existing.outcome !== "failed" && existing.outcome !== "continued_failure") {
        throw new Error(`hook invocation ${input.invocationId} is already ${existing.outcome}`);
      }
      await this.db.execute({
        sql: `UPDATE hook_handler_executions
              SET artifact_digest = ?, started_at = ?, completed_at = ?, outcome = 'running',
                  executed_by_json = '{}', result_json = NULL
              WHERE invocation_id = ? AND outcome IN ('failed', 'continued_failure')`,
        args: [input.artifactDigest, input.startedAt, input.startedAt, input.invocationId],
      });
      return;
    }
    await this.db.execute({
      sql: `INSERT INTO hook_handler_executions (
              invocation_id, evaluation_id, hook_id, event, artifact_digest,
              started_at, completed_at, outcome, executed_by_json, result_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'running', '{}', NULL)`,
      args: [
        input.invocationId,
        input.evaluationId,
        input.hookId,
        input.event,
        input.artifactDigest,
        input.startedAt,
        input.startedAt,
      ],
    });
  }

  async execution(invocationId: string): Promise<HookExecutionRecord | undefined> {
    const result = await this.db.execute({
      sql: `SELECT invocation_id, evaluation_id, hook_id, event, artifact_digest,
                   started_at, completed_at, outcome, executed_by_json, result_json
            FROM hook_handler_executions WHERE invocation_id = ?`,
      args: [invocationId],
    });
    return result.rows[0] ? executionRecord(result.rows[0]) : undefined;
  }

  async executions(limit = 100): Promise<HookExecutionRecord[]> {
    const result = await this.db.execute({
      sql: `SELECT invocation_id, evaluation_id, hook_id, event, artifact_digest,
                   started_at, completed_at, outcome, executed_by_json, result_json
            FROM hook_handler_executions
            ORDER BY started_at DESC, invocation_id DESC
            LIMIT ?`,
      args: [boundedLimit(limit)],
    });
    return result.rows.map(executionRecord);
  }

  async claimReceipt(messageId: string, invocationId: string, claimedAt: number): Promise<boolean> {
    const result = await this.db.execute({
      sql: `INSERT INTO hook_receipt_completion (message_id, invocation_id, claimed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(message_id) DO NOTHING`,
      args: [messageId, invocationId, claimedAt],
    });
    return result.rowsAffected === 1;
  }

  async receipt(messageId: string): Promise<HookReceiptRecord | undefined> {
    const result = await this.db.execute({
      sql: `SELECT message_id, invocation_id, state, claimed_at, completed_at,
                   metadata_patch_json, executed_by_json, merge_acked_at,
                   last_error, attempts
            FROM hook_receipt_completion WHERE message_id = ?`,
      args: [messageId],
    });
    return result.rows[0] ? receiptRecord(result.rows[0]) : undefined;
  }

  async pendingReceipts(): Promise<HookReceiptRecord[]> {
    const result = await this.db.execute(
      `SELECT message_id, invocation_id, state, claimed_at, completed_at,
              metadata_patch_json, executed_by_json, merge_acked_at,
              last_error, attempts
       FROM hook_receipt_completion WHERE state != 'completed'
       ORDER BY claimed_at, message_id`,
    );
    return result.rows.map(receiptRecord);
  }

  async startReceiptAttempt(messageId: string): Promise<void> {
    await this.db.execute({
      sql: `UPDATE hook_receipt_completion
            SET state = 'running', attempts = attempts + 1, last_error = NULL
            WHERE message_id = ?`,
      args: [messageId],
    });
  }

  async failReceipt(messageId: string, error: string): Promise<void> {
    await this.db.execute({
      sql: `UPDATE hook_receipt_completion SET state = 'failed', last_error = ?
            WHERE message_id = ?`,
      args: [error, messageId],
    });
  }

  async completeReceipt(
    messageId: string,
    completedAt: number,
    metadataPatch: Record<string, unknown>,
    executedBy: readonly unknown[],
  ): Promise<void> {
    await this.db.execute({
      sql: `UPDATE hook_receipt_completion
            SET state = 'merge_pending', completed_at = ?, metadata_patch_json = ?,
                executed_by_json = ?, last_error = NULL
            WHERE message_id = ?`,
      args: [completedAt, JSON.stringify(metadataPatch), JSON.stringify(executedBy), messageId],
    });
  }

  async failReceiptMerge(messageId: string, error: string): Promise<void> {
    await this.db.execute({
      sql: `UPDATE hook_receipt_completion
            SET state = 'merge_pending', last_error = ? WHERE message_id = ?`,
      args: [error, messageId],
    });
  }

  async acknowledgeReceiptMerge(messageId: string, acknowledgedAt: number): Promise<void> {
    await this.db.execute({
      sql: `UPDATE hook_receipt_completion
            SET state = 'completed', merge_acked_at = ?, last_error = NULL
            WHERE message_id = ?`,
      args: [acknowledgedAt, messageId],
    });
  }
}

function evaluationRecord(row: Record<string, unknown>): HookEvaluationRecord {
  return {
    evaluationId: String(row.evaluation_id),
    messageId: String(row.message_id),
    event: String(row.event),
    registryGeneration: String(row.registry_generation),
    originalMessage: JSON.parse(String(row.original_message_json)) as HookMessage,
    state: String(row.state),
    startedAt: Number(row.started_at),
    ...(row.completed_at === null ? {} : { completedAt: Number(row.completed_at) }),
    ...(row.result_json === null ? {} : { completedResult: JSON.parse(String(row.result_json)) }),
  };
}

function executionRecord(row: Record<string, unknown>): HookExecutionRecord {
  return {
    invocationId: String(row.invocation_id),
    evaluationId: String(row.evaluation_id),
    hookId: String(row.hook_id),
    event: String(row.event),
    artifactDigest: String(row.artifact_digest),
    startedAt: Number(row.started_at),
    completedAt: Number(row.completed_at),
    outcome: String(row.outcome),
    executedBy: JSON.parse(String(row.executed_by_json)) as unknown,
    ...(row.result_json === null ? {} : { result: JSON.parse(String(row.result_json)) }),
  };
}

function requireSameInvocation(
  existing: HookExecutionRecord,
  input: Pick<HookExecutionInput, "invocationId" | "evaluationId" | "hookId" | "event">,
): void {
  if (
    existing.evaluationId !== input.evaluationId ||
    existing.hookId !== input.hookId ||
    existing.event !== input.event
  ) {
    throw new Error(`hook invocation ID collision for ${input.invocationId}`);
  }
}

function requireIdenticalCompletedInvocation(
  existing: HookExecutionRecord,
  input: HookExecutionInput,
): void {
  if (
    existing.artifactDigest !== input.artifactDigest ||
    existing.outcome !== input.outcome ||
    JSON.stringify(existing.executedBy) !== JSON.stringify(input.executedBy) ||
    JSON.stringify(existing.result) !== JSON.stringify(input.result)
  ) {
    throw new Error(`hook invocation ID collision for ${input.invocationId}`);
  }
}

function boundedLimit(value: number): number {
  return Number.isSafeInteger(value) && value > 0 ? Math.min(value, 500) : 100;
}

function receiptRecord(row: Record<string, unknown>): HookReceiptRecord {
  return {
    messageId: String(row.message_id),
    invocationId: String(row.invocation_id),
    state: String(row.state),
    claimedAt: Number(row.claimed_at),
    ...(row.completed_at === null ? {} : { completedAt: Number(row.completed_at) }),
    ...(row.metadata_patch_json === null
      ? {}
      : { metadataPatch: JSON.parse(String(row.metadata_patch_json)) as Record<string, unknown> }),
    ...(row.executed_by_json === null
      ? {}
      : { executedBy: JSON.parse(String(row.executed_by_json)) as unknown[] }),
    ...(row.merge_acked_at === null ? {} : { mergeAckedAt: Number(row.merge_acked_at) }),
    ...(row.last_error === null ? {} : { lastError: String(row.last_error) }),
    attempts: Number(row.attempts),
  };
}
