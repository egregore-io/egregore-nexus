import { createClient } from "@libsql/client";
import { describe, expect, it } from "vitest";

import { readDb } from "@drizzle/client";

import { createDeveloperEventSource } from "./developerEvents";

async function memoryDb() {
  const client = createClient({ url: ":memory:" });
  await client.batch([
    `CREATE TABLE developer_events (
      topic TEXT NOT NULL,
      seq INTEGER NOT NULL,
      kind TEXT NOT NULL,
      message_id TEXT,
      thread_name TEXT,
      dm_name TEXT,
      from_name TEXT,
      agent_name TEXT,
      session_id TEXT,
      lifecycle TEXT,
      current_work TEXT,
      data_json TEXT,
      created_at INTEGER NOT NULL,
      PRIMARY KEY(topic, seq)
    )`,
    `INSERT INTO developer_events
      (topic, seq, kind, agent_name, session_id, lifecycle, current_work, data_json, created_at)
      VALUES
      ('sys.agent.lifecycle', 1, 'agent_lifecycle', 'ada', 's_ada', 'started', NULL, NULL, 100),
      ('sys.agent.lifecycle', 2, 'agent_lifecycle', 'ada', 's_ada', 'current_work', 'release preparation', NULL, 101),
      ('sys.agent.lifecycle', 3, 'agent_lifecycle', 'ada-renamed', 's_ada', 'rename', NULL, '{"oldName":"ada","newName":"ada-renamed"}', 102),
      ('sys.thread.ops', 1, 'action', NULL, NULL, NULL, NULL, '{"action":"thread.join","member":"ada"}', 103),
      ('sys.inbox.ada', 1, 'message', NULL, NULL, NULL, NULL, NULL, 103)`,
  ]);
  return readDb(client);
}

describe("developer event read projection", () => {
  it("maps rows after a per-topic sequence cursor", async () => {
    const source = createDeveloperEventSource(await memoryDb());

    await expect(source.since("sys.agent.lifecycle", 1)).resolves.toEqual([
      {
        kind: "agent_lifecycle",
        topic: "sys.agent.lifecycle",
        seq: 2,
        ts: 101,
        agent: "ada",
        sessionId: "s_ada",
        lifecycle: "current_work",
        currentWork: "release preparation",
      },
      {
        kind: "agent_lifecycle",
        topic: "sys.agent.lifecycle",
        seq: 3,
        ts: 102,
        agent: "ada-renamed",
        sessionId: "s_ada",
        lifecycle: "rename",
        data: { oldName: "ada", newName: "ada-renamed" },
      },
    ]);
    await expect(source.since("sys.agent.lifecycle", 2)).resolves.toEqual([
      {
        kind: "agent_lifecycle",
        topic: "sys.agent.lifecycle",
        seq: 3,
        ts: 102,
        agent: "ada-renamed",
        sessionId: "s_ada",
        lifecycle: "rename",
        data: { oldName: "ada", newName: "ada-renamed" },
      },
    ]);
    await expect(source.since("sys.thread.ops", 0)).resolves.toEqual([
      {
        kind: "action",
        topic: "sys.thread.ops",
        seq: 1,
        ts: 103,
        data: { action: "thread.join", member: "ada" },
      },
    ]);
  });
});
