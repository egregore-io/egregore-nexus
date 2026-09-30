// Round-trip tests for the web console's conversation store (its own read-write
// libSQL DB). Uses an in-memory libSQL DB so nothing touches the real file.
import { describe, it, expect, beforeEach } from "vitest";
import { createClient, type Client } from "@libsql/client";

import {
  appendLog,
  appendLogs,
  initSchema,
  saveMessage,
  getMessages,
  getLogs,
  conversationDbConfig,
  pruneWebconsoleOperationalRows,
  type StoredMessage,
} from "./store";
import { GatewayStoreConfigError } from "../store/config";

function msg(over: Partial<StoredMessage>): StoredMessage {
  return {
    id: "m1",
    conversationId: "dm:ada",
    role: "you",
    author: "operator",
    content: JSON.stringify({ blocks: [{ b: "p", runs: [{ t: "text", v: "hi" }] }] }),
    status: "final",
    createdAt: 1000,
    ...over,
  };
}

describe("conversation store — persist + read back (the refresh path)", () => {
  let db: Client;
  beforeEach(async () => {
    db = createClient({ url: ":memory:" });
    await initSchema(db);
  });

  it("persists messages and reads them back in chronological order", async () => {
    await saveMessage(db, msg({ id: "m1", role: "you", content: '"hello"', createdAt: 1 }));
    await saveMessage(db, msg({ id: "m2", role: "agent", author: "ada", content: '"hi there"', createdAt: 2 }));

    const rows = await getMessages(db, "dm:ada");
    expect(rows.map((r) => r.id)).toEqual(["m1", "m2"]);
    expect(rows[0]?.role).toBe("you");
    expect(rows[1]?.role).toBe("agent");
    expect(rows[1]?.author).toBe("ada");
  });

  it("limits backlog to the latest rows while preserving display order", async () => {
    await saveMessage(db, msg({ id: "old", content: '"old"', createdAt: 1 }));
    await saveMessage(db, msg({ id: "middle", content: '"middle"', createdAt: 2 }));
    await saveMessage(db, msg({ id: "new", content: '"new"', createdAt: 3 }));

    const rows = await getMessages(db, "dm:ada", 2);
    expect(rows.map((r) => r.id)).toEqual(["middle", "new"]);
  });

  it("upserts a streaming message in place (same id → content/status replaced)", async () => {
    await saveMessage(db, msg({ id: "t1", role: "agent", content: '"Par"', status: "streaming" }));
    await saveMessage(db, msg({ id: "t1", role: "agent", content: '"Paris"', status: "final" }));

    const rows = await getMessages(db, "dm:ada");
    expect(rows).toHaveLength(1); // not duplicated
    expect(rows[0]?.content).toBe('"Paris"');
    expect(rows[0]?.status).toBe("final");
  });

  it("scopes messages to their conversation (no cross-DM leakage)", async () => {
    await saveMessage(db, msg({ id: "a", conversationId: "dm:ada", content: '"to ada"' }));
    await saveMessage(db, msg({ id: "b", conversationId: "dm:ben", content: '"to ben"' }));

    expect((await getMessages(db, "dm:ada")).map((r) => r.id)).toEqual(["a"]);
    expect((await getMessages(db, "dm:ben")).map((r) => r.id)).toEqual(["b"]);
  });

  it("delegates configuration to the local canonical Gateway store", () => {
    expect(conversationDbConfig({}).url).toMatch(/\/\.nexus\/gateway\.db$/);
    expect(
      conversationDbConfig({
        NEXUS_GATEWAY_DB: "file:/tmp/gateway.db",
        NEXUS_WEBCONSOLE_DB: "file:/tmp/legacy.db",
      }),
    ).toEqual({ url: "file:/tmp/gateway.db" });
    expect(() =>
      conversationDbConfig({ NEXUS_WEBCONSOLE_DB: "libsql://x.turso.io" }),
    ).toThrow(GatewayStoreConfigError);
  });

  it("uses rendered cache tables inside the canonical schema", async () => {
    const tables = await db.execute(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'rendered_%' ORDER BY name",
    );
    expect(tables.rows.map((row) => String(row.name))).toEqual([
      "rendered_conversations",
      "rendered_messages",
    ]);
  });

  it("prunes webconsole operational logs without deleting message cache rows", async () => {
    await appendLog(db, {
      ts: 800,
      level: "debug",
      scope: "observe",
      message: "old operational log",
    });
    await appendLog(db, {
      ts: 950,
      level: "debug",
      scope: "observe",
      message: "recent operational log",
    });
    await saveMessage(db, msg({ id: "old-message-cache", content: '"keep"', createdAt: 100 }));

    const sweep = await pruneWebconsoleOperationalRows(db, {
      retentionMs: 100,
      sizeThresholdMb: 1,
      now: 1000,
    });

    expect(sweep.logs).toBe(1);
    expect((await getLogs(db)).map((row) => row.message)).toEqual(["recent operational log"]);
    expect((await getMessages(db, "dm:ada")).map((row) => row.id)).toEqual([
      "old-message-cache",
    ]);
  });

  it("appends a request's log lines in one write batch", async () => {
    const calls: Array<{ count: number; mode: string | undefined }> = [];
    const counted = new Proxy(db, {
      get(target, property) {
        if (property === "batch") {
          return async (
            statements: Parameters<Client["batch"]>[0],
            mode?: Parameters<Client["batch"]>[1],
          ) => {
            calls.push({ count: statements.length, mode });
            return target.batch(statements, mode);
          };
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as Client;

    await appendLogs(counted, [
      { ts: 1, level: "info", scope: "send", message: "one" },
      { ts: 2, level: "warn", scope: "send", message: "two" },
    ]);

    expect(calls).toEqual([{ count: 2, mode: "write" }]);
    expect((await getLogs(db)).map((row) => row.message)).toEqual(["one", "two"]);
  });
});
