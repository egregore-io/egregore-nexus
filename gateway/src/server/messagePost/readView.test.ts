import { afterEach, describe, it, expect, vi } from "vitest";

import { seedDb } from "@drizzle/__mocks__/seedDb";
import type { ReadDb } from "@drizzle/client";
import { GatewayChangeBus } from "@server/store/changeBus";
import {
  createReadViewMessagePostRegistry,
  messagesForTargetAfter,
  type MessagePostMessage,
} from "@server/messagePost/readView";

afterEach(() => {
  vi.useRealTimers();
});

async function insertThreadMessage(
  db: ReadDb,
  args: {
    id: string;
    thread: string;
    body: string;
    createdAt: number;
  },
): Promise<number> {
  await db.$client.execute({
    sql:
      "INSERT INTO messages " +
      "(message_id, from_name, kind, to_name, thread_id, body, project, created_at) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    args: [
      args.id,
      "ben",
      "thread",
      args.thread,
      `t_${args.thread}`,
      args.body,
      "nexus",
      args.createdAt,
    ],
  });
  const row = await db.$client.execute({
    sql: "SELECT rowid FROM messages WHERE message_id = ?",
    args: [args.id],
  });
  return Number((row.rows[0] as unknown as { rowid: number }).rowid);
}

describe("Message Post read-view tail cursor", () => {
  it("uses agentId rather than mismatched display metadata for a DM tail", async () => {
    const db = await seedDb();
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, from_agent_id, kind, to_name, to_agent_id, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_right", "renamed", "a_ben", "dm", "operator", null, "right", "nexus", 100,
        "m_wrong", "stale-ben", "a_other", "dm", "operator", null, "wrong", "nexus", 101,
      ],
    });

    const rows = await messagesForTargetAfter(
      db,
      { verb: "dm", name: "stale-ben", agentId: "a_ben" },
      { after: 0, project: "nexus", selfName: "operator" },
    );

    expect(rows.map((row) => row.id)).toEqual(["m_right"]);
  });

  it("treats project as metadata rather than a DM transport boundary", async () => {
    const db = await seedDb();
    await db.$client.execute({
      sql:
        "INSERT INTO messages " +
        "(message_id, from_name, kind, to_name, body, project, created_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
      args: ["m_cross_metadata", "ben", "dm", "operator", "arrives", "old-label", 100],
    });

    const rows = await messagesForTargetAfter(
      db,
      { verb: "dm", name: "ben" },
      { after: 0, project: "new-label", selfName: "operator" },
    );

    expect(rows.map((row) => row.id)).toEqual(["m_cross_metadata"]);
  });

  it("resumes within the same millisecond using rowid as the tie-breaker", async () => {
    const db = await seedDb();
    const createdAt = 1_782_976_900_000;
    const firstRowid = await insertThreadMessage(db, {
      id: "m_ws3_same_ms_first",
      thread: "ws3-cursor",
      body: "first same-ms row",
      createdAt,
    });
    const secondRowid = await insertThreadMessage(db, {
      id: "m_ws3_same_ms_second",
      thread: "ws3-cursor",
      body: "second same-ms row",
      createdAt,
    });
    await insertThreadMessage(db, {
      id: "m_ws3_later",
      thread: "ws3-cursor",
      body: "later row",
      createdAt: createdAt + 1,
    });

    const rows = await messagesForTargetAfter(
      db,
      { verb: "post", thread: "ws3-cursor" },
      { after: createdAt, afterRowid: firstRowid },
    );

    expect(rows.map((row) => row.id)).toEqual(["m_ws3_same_ms_second", "m_ws3_later"]);
    expect(rows[0]?.cursor).toEqual({ createdAt, rowid: secondRowid });
  });

  it("keeps legacy timestamp-only cursor semantics for older clients", async () => {
    const db = await seedDb();
    const createdAt = 1_782_976_910_000;
    await insertThreadMessage(db, {
      id: "m_ws3_legacy_same_ms",
      thread: "ws3-legacy-cursor",
      body: "same-ms row",
      createdAt,
    });
    await insertThreadMessage(db, {
      id: "m_ws3_legacy_later",
      thread: "ws3-legacy-cursor",
      body: "later row",
      createdAt: createdAt + 1,
    });

    const rows = await messagesForTargetAfter(
      db,
      { verb: "post", thread: "ws3-legacy-cursor" },
      { after: createdAt },
    );

    expect(rows.map((row) => row.id)).toEqual(["m_ws3_legacy_later"]);
  });
});

describe("shared Message Post poller registry", () => {
  it("does not query on a periodic timer and re-reads only after post-commit invalidation", async () => {
    vi.useFakeTimers();
    const changeBus = new GatewayChangeBus();
    const fetchMessages = vi.fn(async () => [] as MessagePostMessage[]);
    const registry = createReadViewMessagePostRegistry({
      fetchMessages,
      changeBus,
      now: () => Date.now(),
    });
    const source = registry.createSource({
      target: { verb: "post", thread: "design" },
      onMessage: vi.fn(),
    });
    await source.ready;
    expect(fetchMessages).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(10_000);
    expect(fetchMessages).toHaveBeenCalledTimes(1);

    changeBus.publish("thread-name:design");
    await vi.waitFor(() => expect(fetchMessages).toHaveBeenCalledTimes(2));
    source.close();
  });

  it("uses one poll query for concurrent observers of one canonical target", async () => {
    const fetchMessages = vi.fn(async () => [] as MessagePostMessage[]);
    const registry = createReadViewMessagePostRegistry({ fetchMessages, now: () => 100 });
    const sources = Array.from({ length: 8 }, () => registry.createSource({
      target: { verb: "post", thread: "design" },
      project: "display-only",
      pollIntervalMs: 60_000,
      onMessage: vi.fn(),
    }));

    await Promise.all(sources.map((source) => source.ready));

    expect(fetchMessages).toHaveBeenCalledTimes(1);
    expect(fetchMessages).toHaveBeenCalledWith(
      { verb: "post", thread: "design" },
      expect.objectContaining({ after: 99, project: undefined }),
    );
    expect(registry.activePollerCount()).toBe(1);

    for (const source of sources) source.close();
    expect(registry.activePollerCount()).toBe(0);
  });

  it("keeps a paused subscriber cursor in place and resumes without affecting peers", async () => {
    vi.useFakeTimers();
    let calls = 0;
    const row = {
      id: "m_backpressure",
      from: "ada",
      body: "bounded",
      scope: "thread",
      project: "metadata",
      provenance: "agent",
      createdAt: 101,
      cursor: { createdAt: 101, rowid: 9 },
    } as unknown as MessagePostMessage;
    const fetchMessages = vi.fn(async () => (++calls === 1 ? [row] : []));
    const registry = createReadViewMessagePostRegistry({ fetchMessages, now: () => 100 });
    const blocked = vi.fn(() => false);
    const peer = vi.fn(() => true);
    const first = registry.createSource({
      target: { verb: "post", thread: "design" },
      after: 99,
      pollIntervalMs: 1_000,
      onMessage: blocked,
    });
    const second = registry.createSource({
      target: { verb: "post", thread: "design" },
      after: 99,
      pollIntervalMs: 1_000,
      onMessage: peer,
    });
    await Promise.all([first.ready, second.ready]);

    expect(blocked).toHaveBeenCalledTimes(1);
    expect(peer).toHaveBeenCalledTimes(1);
    first.resume?.();
    await vi.runOnlyPendingTimersAsync();

    expect(registry.activePollerCount()).toBe(1);
    first.close();
    second.close();
  });

  it("emits heartbeats from the shared poll cadence", async () => {
    vi.useFakeTimers();
    const heartbeat = vi.fn();
    const registry = createReadViewMessagePostRegistry({
      fetchMessages: async () => [],
      now: () => Date.now(),
    });
    const source = registry.createSource({
      target: { verb: "post", thread: "design" },
      pollIntervalMs: 1_000,
      heartbeatIntervalMs: 2_000,
      onMessage: vi.fn(),
      onHeartbeat: heartbeat,
    });
    await source.ready;

    await vi.advanceTimersByTimeAsync(2_100);
    expect(heartbeat).toHaveBeenCalledTimes(1);
    source.close();
  });

  it("releases every poller after reconnect churn", async () => {
    const fetchMessages = vi.fn(async () => [] as MessagePostMessage[]);
    const registry = createReadViewMessagePostRegistry({ fetchMessages, now: () => 100 });

    for (let cycle = 0; cycle < 50; cycle++) {
      const sources = Array.from({ length: 12 }, () => registry.createSource({
        target: { verb: "post", thread: "design" },
        pollIntervalMs: 60_000,
        onMessage: vi.fn(),
      }));
      await Promise.all(sources.map((source) => source.ready));
      expect(registry.activePollerCount()).toBe(1);
      for (const source of sources) source.close();
      expect(registry.activePollerCount()).toBe(0);
    }

    expect(fetchMessages).toHaveBeenCalledTimes(50);
  });
});
