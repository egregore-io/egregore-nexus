import { createClient, type Client } from "@libsql/client";
import { EventType } from "@ag-ui/client";
import { describe, expect, it } from "vitest";

import {
  createMaterializedTurnRelay,
  loadAgentSessionSnapshot,
  materializedCursorForStreamAfter,
} from "@server/agui/agentSessionProjection";
import type { WsEvent } from "@shared/types";
import { acpToAguiEvents, newBracket } from "@server/agui/mapAgentUpdate";

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
  it.each(["assistant", "user"])("keeps every materialized %s block distinct with stable snapshot and relay identities", async (role) => {
    const db = await createStore();
    await db.batch([
      {sql:"INSERT INTO agent_session_turns VALUES (?, ?, ?, ?, ?, ?, ?, ?)", args:["turn_blocks","s_blocks","final",10,18,100,120,120]},
      {sql:"INSERT INTO agent_session_messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", args:[
        "row_blocks","s_blocks","turn_blocks",0,role,null,
        JSON.stringify({schema:1,blocks:[{type:"text",text:"equal"},
          {type:"tool_call",id:"call",tool:"bash",status:"completed",output:"ok"},
          {type:"text",text:"equal"},{type:"text",text:"adjacent"},
          {type:"text",itemId:"native_exact",clientMessageId:"native_exact",text:"native"}]}),"final",10,18,100,119,120,
      ]},
    ]);
    const snapshot = await loadAgentSessionSnapshot("s_blocks","blocks",{client:db});
    const contents = (snapshot.events as Array<Record<string,unknown>>).filter(e=>e.type===EventType.TEXT_MESSAGE_CONTENT);
    expect(contents.map(e=>e.delta)).toEqual(["equal","equal","adjacent","native"]);
    expect(new Set(contents.map(e=>e.messageId)).size).toBe(4);
    expect(contents[3]?.messageId).toBe("native_exact");
    expect(contents[3]?.nexusMessageOrigin).toBeUndefined();
    expect(contents.slice(0,3).map(e=>(e.nexusMessageOrigin as {materializedBlock:string}).materializedBlock))
      .toEqual([0,2,3].map(index=>JSON.stringify(["s_blocks","row_blocks",index])));
    const again = await loadAgentSessionSnapshot("s_blocks","blocks",{client:db});
    expect(again.events).toEqual(snapshot.events);
    let bracket = newBracket();
    const replay: Array<Record<string,unknown>> = [];
    const handle = createMaterializedTurnRelay("s_blocks",{client:db,pollMs:5})({
      onEvent(event) {
        if(event.type!=="agent.update") return;
        const mapped=acpToAguiEvents(event,bracket); bracket=mapped.bracket;
        replay.push(...mapped.events as unknown as Array<Record<string,unknown>>);
      },
    });
    try {
      await waitFor(()=>replay.filter(e=>e.type===EventType.TEXT_MESSAGE_CONTENT).length===4);
      expect(replay.filter(e=>e.type===EventType.TEXT_MESSAGE_CONTENT)).toEqual(contents);
    } finally { handle.close(); db.close(); }
  });

  it("replays authenticated human provenance from durable user_input blocks", async () => {
    const db = await createStore();
    await db.batch([
      {
        sql:
          "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
          "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        args: ["turn_human", "s_ben", "final", 1, 2, 100, 120, 120],
      },
      {
        sql:
          "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
          "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        args: [
          "m_human",
          "s_ben",
          "turn_human",
          0,
          "user",
          "alice",
          JSON.stringify({
            schema: 1,
            blocks: [{
              type: "text",
              text: "authenticated replay",
              id: "you:replay:1",
              clientMessageId: "you:replay:1",
              name: "alice",
              kind: "human",
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
    ]);

    const snapshot = await loadAgentSessionSnapshot("s_ben", "ben", { client: db });
    const start = (snapshot.events as Array<Record<string, unknown>>).find(
      (event) => event.type === EventType.TEXT_MESSAGE_START && event.role === "user",
    );

    expect(start).toMatchObject({
      messageId: "you:replay:1",
      name: "alice",
      kind: "human",
    });
    expect(start).not.toMatchObject({ name: "ben" });
  });

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
      `nexus-materialized:${encodeURIComponent(JSON.stringify(["s_ada","m_one",0]))}:msg`,
      `nexus-materialized:${encodeURIComponent(JSON.stringify(["s_ada","m_one",0]))}:msg`,
      `nexus-materialized:${encodeURIComponent(JSON.stringify(["s_ada","m_two",0]))}:msg`,
      `nexus-materialized:${encodeURIComponent(JSON.stringify(["s_ada","m_two",0]))}:msg`,
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

  it.each([
    { label: "completed nonempty output", status: "completed", output: "ok" },
    { label: "completed empty output", status: "completed", output: "" },
    { label: "failed nonempty output", status: "failed", output: "native error" },
    { label: "failed empty output", status: "failed", output: "" },
    { label: "completed absent output", status: "completed", output: undefined },
    { label: "completed null output", status: "completed", output: null },
    { label: "failed absent output", status: "failed", output: undefined },
    { label: "absent status and output", status: undefined, output: undefined },
  ])("preserves $label through legacy materialized replay", async ({ status, output }) => {
    const toolCall = {
      id: "tc_output",
      tool: "shell",
      input: { command: "true" },
      ...(status === undefined ? {} : { status }),
      ...(output === undefined ? {} : { output }),
    };
    const expectedResults = typeof output !== "string" ? [] : [{
      type: EventType.TOOL_CALL_RESULT,
      messageId: "tc_output:result",
      toolCallId: "tc_output",
      content: output,
      role: "tool",
      status,
      append: false,
    }];

    // The mapper already distinguishes an explicit empty result from no result.
    // Replay must preserve that distinction, even when the enclosing turn is final.
    const direct = acpToAguiEvents({
      type: "agent.update",
      sessionId: "s_output",
      kind: "tool_call",
      data: toolCall,
    }, newBracket());
    expect(direct.events.filter((event) => event.type === EventType.TOOL_CALL_RESULT))
      .toEqual(expectedResults);

    const db = await createStore();
    try {
      await db.batch([
        {
          sql: "INSERT INTO agent_session_turns VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
          args: ["turn_output", "s_output", "final", 1, 2, 100, 120, 120],
        },
        {
          sql: "INSERT INTO agent_session_messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
          args: [
            "m_output", "s_output", "turn_output", 0, "assistant", null,
            JSON.stringify({ schema: 1, blocks: [{ type: "tool_call", ...toolCall }] }),
            "final", 1, 2, 100, 120, 120,
          ],
        },
      ]);

      // Exercise the actual materialized projection -> AG-UI mapper composition.
      const snapshot = await loadAgentSessionSnapshot("s_output", "output", { client: db });
      expect(snapshot.events.filter((event) => event.type === EventType.TOOL_CALL_START))
        .toEqual([{
          type: EventType.TOOL_CALL_START,
          toolCallId: "tc_output",
          toolCallName: "shell",
          streamEventId: 1,
        }]);
      expect(snapshot.events.filter((event) => event.type === EventType.TOOL_CALL_RESULT))
        .toEqual(expectedResults.map((result) => ({ ...result, streamEventId: 1 })));
    } finally {
      db.close();
    }
  });

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
