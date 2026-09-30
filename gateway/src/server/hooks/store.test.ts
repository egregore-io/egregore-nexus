import { createClient } from "@libsql/client";
import { describe, expect, it } from "vitest";

import { migrateGatewayStore } from "../store/migrations";
import { HookAuditStore } from "./store";

describe("Gateway hook audit store", () => {
  it("stores the original message once and reuses it across retries", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    const store = new HookAuditStore(db);
    const original = {
      sender: { agentId: "a_fixture_sender", name: "fixture-sender" },
      target: { verb: "post", thread: "release" } as const,
      body: "original",
      mention: [],
      metadata: { stable: true },
    };

    const first = await store.beginEvaluation({
      evaluationId: "he_message_before_send",
      messageId: "m_message",
      event: "before_send",
      registryGeneration: "sha256:generation",
      originalMessage: original,
      startedAt: 10,
    });
    const retry = await store.beginEvaluation({
      evaluationId: "he_message_before_send",
      messageId: "m_message",
      event: "before_send",
      registryGeneration: "sha256:generation",
      originalMessage: { ...original, body: "must-not-overwrite" },
      startedAt: 20,
    });

    expect(first.originalMessage).toEqual(original);
    expect(retry.originalMessage).toEqual(original);
    expect(await count(db, "hook_pipeline_evaluations")).toBe(1);
    db.close();
  });

  it("records each invocation once and rejects an ID collision", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    const store = new HookAuditStore(db);
    await store.beginEvaluation({
      evaluationId: "he_message_before_send",
      messageId: "m_message",
      event: "before_send",
      registryGeneration: "sha256:generation",
      originalMessage: {
        sender: { name: "fixture-sender" },
        target: { verb: "dm", name: "fable" },
        body: "hello",
        mention: [],
        metadata: {},
      },
      startedAt: 10,
    });
    const execution = {
      invocationId: "hi_stable",
      evaluationId: "he_message_before_send",
      hookId: "redact",
      event: "before_send",
      artifactDigest: "sha256:artifact",
      startedAt: 11,
      completedAt: 12,
      outcome: "completed",
      executedBy: { hookId: "redact", invocationId: "hi_stable" },
      result: { metadata: { redacted: true } },
    };

    await store.recordExecution(execution);
    await store.recordExecution(execution);
    await expect(store.recordExecution({ ...execution, hookId: "collision" })).rejects.toThrow(
      /invocation ID collision/i,
    );
    expect(await count(db, "hook_handler_executions")).toBe(1);
    await expect(store.execution("hi_stable")).resolves.toMatchObject({
      outcome: "completed",
      result: { metadata: { redacted: true } },
    });
    db.close();
  });

  it("persists and replays a completed pipeline result", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    const store = new HookAuditStore(db);
    const input = {
      evaluationId: "he_replay",
      messageId: "m_replay",
      event: "before_send",
      registryGeneration: "sha256:generation",
      originalMessage: {
        sender: { name: "fixture-sender" },
        target: { verb: "post", thread: "release" } as const,
        body: "original",
        mention: [],
        metadata: {},
      },
      startedAt: 10,
    };
    await store.beginEvaluation(input);
    await store.completeEvaluation("he_replay", 20, { action: "continue", body: "done" });

    await expect(store.beginEvaluation({
      ...input,
      originalMessage: { ...input.originalMessage, body: "must-not-replace" },
      startedAt: 30,
    })).resolves.toMatchObject({
      state: "completed",
      completedResult: { action: "continue", body: "done" },
      originalMessage: { body: "original" },
    });
    db.close();
  });

  it("claims after-receipt processing once per canonical message", async () => {
    const db = createClient({ url: ":memory:" });
    await migrateGatewayStore(db);
    const store = new HookAuditStore(db);

    await expect(store.claimReceipt("m_once", "hi_receipt_1", 100)).resolves.toBe(true);
    await expect(store.claimReceipt("m_once", "hi_receipt_2", 200)).resolves.toBe(false);
    expect(await count(db, "hook_receipt_completion")).toBe(1);
    db.close();
  });
});

async function count(db: ReturnType<typeof createClient>, table: string): Promise<number> {
  const result = await db.execute(`SELECT COUNT(*) AS count FROM ${table}`);
  return Number(result.rows[0]?.count ?? 0);
}
