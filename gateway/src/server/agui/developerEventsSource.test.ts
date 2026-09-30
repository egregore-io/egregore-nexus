import { readFileSync } from "node:fs";

import { expect, it, vi } from "vitest";

import { createDeveloperEventSource } from "./developerEventsSource.mjs";

it("loads durable developer events through daemon IPC in the plain-Node gateway", async () => {
  const query = vi.fn(async () => ({
    columns: [
      "topic", "seq", "kind", "message_id", "thread_name", "dm_name", "from_name",
      "agent_name", "session_id", "lifecycle", "current_work", "data_json", "created_at",
    ],
    rows: [[
      "sys.fleet.status", 7, "agent_lifecycle", null, null, null, "daemon", "otto",
      "s_otto", "online", "testing", '{"presence":"online"}', 9_000,
    ]],
    rowsAffected: 0,
  }));
  const source = createDeveloperEventSource({ query });

  await expect(source.since("sys.fleet.status", 6)).resolves.toEqual([{
    kind: "agent_lifecycle",
    topic: "sys.fleet.status",
    seq: 7,
    ts: 9_000,
    from: "daemon",
    agent: "otto",
    sessionId: "s_otto",
    lifecycle: "online",
    currentWork: "testing",
    data: { presence: "online" },
  }]);
  expect(query).toHaveBeenCalledWith("local.store.read", {
    sql: expect.stringContaining("FROM developer_events"),
    args: ["sys.fleet.status", 6],
  });

  const sourceText = readFileSync("src/server/agui/developerEventsSource.mjs", "utf8");
  expect(sourceText).not.toMatch(/@libsql\/client|NEXUS_DB_URL|NEXUS_DB_PATH|query_only/);
});
