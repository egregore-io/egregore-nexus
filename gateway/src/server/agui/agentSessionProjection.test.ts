import { createClient, type Client } from "@libsql/client";
import { EventType } from "@ag-ui/client";
import { describe, expect, it } from "vitest";

import {
  createMaterializedTurnRelay,
  loadAgentSessionSnapshot,
  materializedCursorForStreamAfter,
} from "@server/agui/agentSessionProjection";
import type { WsEvent } from "@shared/types";

async function createStore(): Promise<Client> {
  const db = createClient({ url: ":memory:" });
  await db.batch([
    `CREATE TABLE agent_session_turns (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      status TEXT NOT NULL,
      first_stream_event_id INTEGER NOT NULL,
      last_stream_event_id INTEGER NOT NULL,
      started_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      finalized_at INTEGER
    )`,
    `CREATE TABLE agent_session_messages (
      id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      turn_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL,
      role TEXT NOT NULL,
      author TEXT,
      content_json TEXT NOT NULL,
      status TEXT NOT NULL,
      first_stream_event_id INTEGER NOT NULL,
      last_stream_event_id INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      finalized_at INTEGER,
      UNIQUE(turn_id, ordinal)
    )`,
  ]);
  return db;
}

function types(events: Array<Record<string, unknown>>): string[] {
  return events.map((ev) => String(ev.type));
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 200,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("timed out waiting for predicate");
}

describe("agent session materialized projection", () => {
  it("hydrates finalized turns as AG-UI and returns the last finalized stream cursor", async () => {
    const db = await createStore();
    await db.batch([
      {
        sql:
          "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
          "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        args: ["turn_final", "s_ada", "final", 1, 7, 100, 120, 120],
      },
      {
        sql:
          "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
          "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        args: ["turn_live", "s_ada", "streaming", 8, 9, 130, 135, null],
      },
      {
        sql:
          "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
          "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        args: [
          "m_user",
          "s_ada",
          "turn_final",
          0,
          "user",
          null,
          JSON.stringify({
            schema: 1,
            blocks: [{
              type: "text",
              text: "check this",
              id: "initial-prompt:s_ada",
              clientMessageId: "initial-prompt:s_ada",
              source: "initial_prompt",
              name: "ada",
              harness: "codex",
              runtimeId: "s_ada",
            }],
          }),
          "final",
          1,
          1,
          100,
          100,
          120,
        ],
      },
      {
        sql:
          "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
          "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        args: [
          "m_assistant",
          "s_ada",
          "turn_final",
          1,
          "assistant",
          null,
          JSON.stringify({
            schema: 1,
            blocks: [
              { type: "thinking", text: "looking" },
              { type: "text", text: "done" },
            ],
          }),
          "final",
          2,
          6,
          101,
          119,
          120,
        ],
      },
    ]);

    const snapshot = await loadAgentSessionSnapshot("s_ada", "ada", { client: db });
    const eventRecords = snapshot.events as Array<Record<string, unknown>>;

    expect(snapshot.tailAfterId).toBe(7);
    expect(eventRecords[0]?.threadId).toBe("session:ada");
    expect(types(eventRecords)).toEqual([
      EventType.RUN_STARTED,
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
      EventType.REASONING_MESSAGE_START,
      EventType.REASONING_MESSAGE_CONTENT,
      EventType.REASONING_MESSAGE_END,
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TEXT_MESSAGE_END,
      EventType.RUN_FINISHED,
    ]);
    expect(eventRecords.find((ev) => ev.type === EventType.TEXT_MESSAGE_CONTENT)?.delta).toBe(
      "check this",
    );
    expect(eventRecords[1]).toMatchObject({
      type: EventType.TEXT_MESSAGE_START,
      messageId: "initial-prompt:s_ada",
      role: "user",
      source: "initial_prompt",
      name: "ada",
      harness: "codex",
      runtimeId: "s_ada",
    });
    expect(eventRecords[2]).toMatchObject({
      type: EventType.TEXT_MESSAGE_CONTENT,
      messageId: "initial-prompt:s_ada",
      delta: "check this",
      source: "initial_prompt",
      harness: "codex",
      runtimeId: "s_ada",
    });
    expect(
      eventRecords
        .filter((ev) => ev.type === EventType.TEXT_MESSAGE_CONTENT)
        .map((ev) => ev.delta),
    ).toContain("done");
  });

  it("replays one native text item around its interleaved tool facts", async () => {
    const db = await createStore();
    await db.batch([
      {
        sql:
          "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
          "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        args: ["turn_item", "s_codex", "final", 1, 4, 100, 120, 120],
      },
      {
        sql:
          "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
          "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        args: [
          "m_item",
          "s_codex",
          "turn_item",
          0,
          "assistant",
          null,
          JSON.stringify({
            schema: 1,
            blocks: [
              { type: "text", text: "checking the focused test", itemId: "am1" },
              {
                type: "tool_call",
                id: "tc1",
                tool: "shell",
                name: "find focused test",
                status: "completed",
                output: "focused.test.ts",
              },
            ],
          }),
          "final",
          1,
          3,
          100,
          119,
          120,
        ],
      },
    ]);

    const snapshot = await loadAgentSessionSnapshot("s_codex", "codex", { client: db });
    const events = snapshot.events as Array<Record<string, unknown>>;

    expect(types(events)).toEqual([
      EventType.RUN_STARTED,
      EventType.TEXT_MESSAGE_START,
      EventType.TEXT_MESSAGE_CONTENT,
      EventType.TOOL_CALL_START,
      EventType.TOOL_CALL_RESULT,
      EventType.TEXT_MESSAGE_END,
      EventType.RUN_FINISHED,
    ]);
    expect(events.filter((event) => event.type === EventType.TEXT_MESSAGE_START)).toEqual([
      expect.objectContaining({ messageId: "am1", role: "assistant" }),
    ]);
    expect(events.filter((event) => event.type === EventType.TEXT_MESSAGE_END)).toEqual([
      expect.objectContaining({ messageId: "am1" }),
    ]);
    expect(events.find((event) => event.type === EventType.TEXT_MESSAGE_CONTENT)).toMatchObject({
      messageId: "am1",
      delta: "checking the focused test",
    });
  });

  it("does not reuse generated AG-UI message ids across materialized turns", async () => {
    const db = await createStore();
    const insertTurn =
      "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
      "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    const insertMessage =
      "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
      "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    await db.batch([
      { sql: insertTurn, args: ["turn_one", "s_ada", "final", 10, 12, 100, 120, 120] },
      {
        sql: insertMessage,
        args: [
          "m_one", "s_ada", "turn_one", 0, "assistant", null,
          JSON.stringify({ schema: 1, blocks: [{ type: "text", text: "first" }] }),
          "final", 10, 12, 100, 119, 120,
        ],
      },
      { sql: insertTurn, args: ["turn_two", "s_ada", "final", 20, 22, 130, 150, 150] },
      {
        sql: insertMessage,
        args: [
          "m_two", "s_ada", "turn_two", 0, "assistant", null,
          JSON.stringify({ schema: 1, blocks: [{ type: "text", text: "second" }] }),
          "final", 20, 22, 130, 149, 150,
        ],
      },
    ]);

    const snapshot = await loadAgentSessionSnapshot("s_ada", "ada", { client: db });
    const startsAndEnds = (snapshot.events as Array<Record<string, unknown>>)
      .filter((ev) => ev.type === EventType.TEXT_MESSAGE_START || ev.type === EventType.TEXT_MESSAGE_END)
      .map((ev) => String(ev.messageId));

    expect(startsAndEnds).toEqual([
      "s_ada:stream:10:msg:1",
      "s_ada:stream:10:msg:1",
      "s_ada:stream:20:msg:1",
      "s_ada:stream:20:msg:1",
    ]);
    expect(new Set(startsAndEnds).size).toBe(2);
  });

  it("tails only turns finalized after the snapshot cursor, closing each with turn_end", async () => {
    const db = await createStore();
    const insertTurn =
      "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
      "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    const insertMessage =
      "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
      "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    await db.batch([
      { sql: insertTurn, args: ["turn_old", "s_ada", "final", 1, 3, 100, 120, 120] },
      {
        sql: insertMessage,
        args: [
          "m_old", "s_ada", "turn_old", 0, "assistant", null,
          JSON.stringify({ schema: 1, blocks: [{ type: "text", text: "old" }] }),
          "final", 1, 3, 100, 119, 120,
        ],
      },
    ]);

    const snapshot = await loadAgentSessionSnapshot("s_ada", "ada", { client: db });

    const seen: WsEvent[] = [];
    const relay = createMaterializedTurnRelay("s_ada", {
      client: db,
      afterCursor: snapshot.tailCursor,
      pollMs: 5,
    })({
      onEvent: (ev) => seen.push(ev),
    });

    // A turn finalized AFTER the snapshot cursor must stream out; the old one must not.
    await db.batch([
      { sql: insertTurn, args: ["turn_new", "s_ada", "final", 4, 6, 130, 140, 140] },
      {
        sql: insertMessage,
        args: [
          "m_new", "s_ada", "turn_new", 0, "assistant", null,
          JSON.stringify({ schema: 1, blocks: [{ type: "text", text: "new" }] }),
          "final", 4, 6, 130, 139, 140,
        ],
      },
    ]);

    await waitFor(() => seen.length === 2);
    relay.close();

    expect(seen[0]).toMatchObject({
      type: "agent.update",
      sessionId: "s_ada",
      kind: "text",
      data: { text: "new" },
    });
    expect(seen[1]).toMatchObject({
      type: "agent.update",
      sessionId: "s_ada",
      kind: "turn_end",
    });
  });

  // --- C-TOOL v1 (docs/tool-call-contract.md): replay must be lossless ------

  it("replays tool blocks with the canonical name and single-encoded structured args", async () => {
    const db = await createStore();
    await db.batch([
      {
        sql:
          "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
          "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        args: ["turn_t", "s_tool", "final", 1, 4, 100, 119, 120],
      },
      {
        sql:
          "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
          "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        args: [
          "m_tool",
          "s_tool",
          "turn_t",
          0,
          "assistant",
          null,
          JSON.stringify({
            schema: 1,
            blocks: [
              {
                type: "tool_call",
                id: "tc_modern",
                tool: "shell",
                name: "/usr/bin/zsh -lc 'ls'",
                status: "completed",
                input: { command: "ls", cwd: "/tmp" },
                argsJson: "{\n  \"command\": \"ls\",\n  \"cwd\": \"/tmp\"\n}",
                output: "listing",
              },
              {
                type: "tool_call",
                id: "tc_legacy",
                name: "write",
                status: "completed",
                argsJson: "{\"file_path\": \"/tmp/a.md\"}",
                output: "ok",
              },
            ],
          }),
          "final",
          2,
          4,
          101,
          119,
          120,
        ],
      },
    ]);

    const snapshot = await loadAgentSessionSnapshot("s_tool", "tool", { client: db });
    const eventRecords = snapshot.events as Array<Record<string, unknown>>;

    const starts = eventRecords.filter((ev) => ev.type === EventType.TOOL_CALL_START);
    expect(starts.map((ev) => ev.toolCallName)).toEqual(["shell", "write"]);

    const args = eventRecords.filter((ev) => ev.type === EventType.TOOL_CALL_ARGS);
    expect(args).toHaveLength(2);
    // The fidelity law: parsing the delta returns the ORIGINAL OBJECT — a doubly-encoded
    // string here would discard the structured argument object.
    expect(JSON.parse(String(args[0]?.delta))).toEqual({ command: "ls", cwd: "/tmp" });
    expect(JSON.parse(String(args[1]?.delta))).toEqual({ file_path: "/tmp/a.md" });

    const results = eventRecords.filter((ev) => ev.type === EventType.TOOL_CALL_RESULT);
    expect(results.map((ev) => ev.content)).toEqual(["listing", "ok"]);
  });

  it("finds the materialized fallback cursor before a reconnect stream id", async () => {
    const db = await createStore();
    const insertTurn =
      "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
      "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    await db.batch([
      { sql: insertTurn, args: ["turn_old", "s_iris", "final", 10, 20, 100, 120, 120] },
      { sql: insertTurn, args: ["turn_partial", "s_iris", "final", 21, 30, 130, 140, 140] },
      { sql: insertTurn, args: ["turn_later", "s_iris", "final", 31, 40, 150, 160, 160] },
    ]);

    expect(await materializedCursorForStreamAfter("s_iris", 25, { client: db })).toEqual({
      finalizedAt: 120,
      rowid: 1,
    });
    expect(await materializedCursorForStreamAfter("s_iris", 30, { client: db })).toEqual({
      finalizedAt: 140,
      rowid: 2,
    });
  });

  it("skips a not-yet-finalized turn until it finalizes", async () => {
    const db = await createStore();
    const insertTurn =
      "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
      "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    const insertMessage =
      "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
      "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    await db.batch([
      { sql: insertTurn, args: ["turn_live", "s_ada", "streaming", 1, 2, 100, 110, null] },
      {
        sql: insertMessage,
        args: [
          "m_live", "s_ada", "turn_live", 0, "assistant", null,
          JSON.stringify({ schema: 1, blocks: [{ type: "text", text: "streaming" }] }),
          "streaming", 1, 2, 100, 110, null,
        ],
      },
    ]);

    const seen: WsEvent[] = [];
    const relay = createMaterializedTurnRelay("s_ada", {
      client: db,
      pollMs: 5,
    })({
      onEvent: (ev) => seen.push(ev),
    });

    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(seen).toHaveLength(0);

    await db.execute({
      sql: "UPDATE agent_session_turns SET finalized_at = ?, status = 'final' WHERE id = ?",
      args: [150, "turn_live"],
    });

    await waitFor(() => seen.length === 2);
    relay.close();

    expect(seen[0]).toMatchObject({ kind: "text", data: { text: "streaming" } });
    expect(seen[1]).toMatchObject({ kind: "turn_end" });
  });
});
