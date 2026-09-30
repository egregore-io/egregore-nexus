import { createClient, type Client } from "@libsql/client";
import type {
  HookAfterReceiptRequest,
  HookAfterReceiptResult,
  HookExecutedBy,
} from "@shared/types";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { removeTempPath } from "../../test/removeTempPath";
import { migrateGatewayStore } from "../store/migrations";
import { applyCanonicalProjection, type CanonicalProjectionEvent } from "../projection/apply";
import { AfterReceiptProcessor } from "./afterReceipt";

const accepted: CanonicalProjectionEvent = {
  eventId: "message:m_after",
  daemonEpoch: "boot-1",
  seq: 1,
  occurredAt: 10,
  kind: "message.accepted",
  version: 1,
  payload: {
    messageId: "m_after",
    scope: "thread",
    fromName: "fixture-sender",
    fromAgentId: "a_fixture_sender",
    threadId: "t_release",
    threadName: "release",
    body: "ship it",
    mention: ["fable"],
    metadata: {
      nested: { before: true },
      _nexus: { hooks: { executedBy: [] } },
    },
    createdAt: 10,
  },
};

const execution: HookExecutedBy = {
  hookId: "index-release",
  entrypoint: "main",
  runtime: "node",
  artifactDigest: "sha256:artifact",
  invocationId: "hi_after_index",
  outcome: "success",
  attestation: { algorithm: "ed25519", signature: "signed" },
};

describe("after_receipt processing", () => {
  let db: Client;
  let dbPath: string;

  beforeEach(async () => {
    dbPath = join(tmpdir(), `nexus-after-receipt-${randomUUID()}.db`);
    db = createClient({ url: `file:${dbPath}` });
    await migrateGatewayStore(db);
    await applyCanonicalProjection(db, accepted);
  });

  afterEach(async () => {
    db.close();
    await removeTempPath(dbPath);
  });

  it("runs one stable invocation per accepted message and retries only the daemon merge", async () => {
    const requests: HookAfterReceiptRequest[] = [];
    const execute = vi.fn(async (request: HookAfterReceiptRequest): Promise<HookAfterReceiptResult> => {
      requests.push(request);
      return {
        invocationId: request.invocationId,
        metadata: { nested: { after: true }, indexed: true },
        executedBy: [execution],
      };
    });
    const mergeDaemon = vi
      .fn()
      .mockRejectedValueOnce(new Error("daemon unavailable"))
      .mockResolvedValueOnce(undefined);
    const processor = new AfterReceiptProcessor(db, {
      execute,
      mergeDaemon,
      now: () => 20,
    });

    await expect(processor.accept(accepted)).resolves.toBe("pending");
    expect(execute).toHaveBeenCalledTimes(1);
    expect(requests[0]).toMatchObject({
      message: {
        sender: { agentId: "a_fixture_sender", name: "fixture-sender" },
        target: { verb: "post", thread: "release" },
        mention: ["fable"],
      },
      receipt: { messageId: "m_after" },
    });

    const canonical = await db.execute(
      "SELECT metadata_json FROM bus_messages WHERE message_id = 'm_after'",
    );
    expect(JSON.parse(String(canonical.rows[0]?.metadata_json))).toEqual({
      nested: { before: true, after: true },
      indexed: true,
      _nexus: { hooks: { executedBy: [execution] } },
    });
    expect((await db.execute(
      "SELECT state, last_error FROM hook_receipt_completion WHERE message_id = 'm_after'",
    )).rows[0]).toMatchObject({ state: "merge_pending", last_error: "daemon unavailable" });

    const reconnected = new AfterReceiptProcessor(db, {
      execute,
      mergeDaemon,
      now: () => 20,
    });
    await expect(reconnected.accept(accepted)).resolves.toBe("completed");
    expect(execute).toHaveBeenCalledTimes(1);
    expect(mergeDaemon).toHaveBeenCalledTimes(2);
    expect(mergeDaemon.mock.calls[0]?.[0]).toEqual(mergeDaemon.mock.calls[1]?.[0]);
    expect((await db.execute(
      "SELECT state, merge_acked_at FROM hook_receipt_completion WHERE message_id = 'm_after'",
    )).rows[0]).toMatchObject({ state: "completed", merge_acked_at: 20 });
    const replayed = JSON.parse(String((await db.execute(
      "SELECT metadata_json FROM bus_messages WHERE message_id = 'm_after'",
    )).rows[0]?.metadata_json));
    expect(replayed._nexus.hooks.executedBy).toHaveLength(1);
  });

  it("keeps a failed side effect visible and retries with the same invocation identity", async () => {
    const ids: string[] = [];
    const execute = vi
      .fn(async (request: HookAfterReceiptRequest): Promise<HookAfterReceiptResult> => {
        ids.push(request.invocationId);
        if (ids.length === 1) throw new Error("hook process exited 1");
        return { invocationId: request.invocationId, metadata: {}, executedBy: [] };
      });
    const processor = new AfterReceiptProcessor(db, {
      execute,
      mergeDaemon: vi.fn(async () => undefined),
      now: () => 30,
    });

    await expect(processor.accept(accepted)).resolves.toBe("pending");
    expect((await db.execute(
      "SELECT state, last_error FROM hook_receipt_completion WHERE message_id = 'm_after'",
    )).rows[0]).toMatchObject({ state: "failed", last_error: "hook process exited 1" });

    await expect(processor.retryPending()).resolves.toBe(1);
    expect(ids).toHaveLength(2);
    expect(ids[0]).toBe(ids[1]);
  });

  it("ignores delivery outcomes because receipts are message-scoped, not recipient-scoped", async () => {
    const execute = vi.fn();
    const processor = new AfterReceiptProcessor(db, {
      execute,
      mergeDaemon: vi.fn(),
    });
    await expect(processor.accept({ ...accepted, kind: "delivery.settled" })).resolves.toBe(
      "ignored",
    );
    expect(execute).not.toHaveBeenCalled();
  });
});
