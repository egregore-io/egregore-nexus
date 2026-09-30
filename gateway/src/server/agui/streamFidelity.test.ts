// C-TOOL v1 fidelity gate (docs/tool-call-contract.md).
//
// The invariant that keeps tool-call args real everywhere: for the same logical tool
// call, the LIVE lane (agent.update → acpToAguiEvents) and the REPLAY lane (materialized
// block → projection → acpToAguiEvents) must yield the same canonical toolCallName and an
// args payload that JSON-parses back to the same object. A doubly-encoded string or a
// title-as-name regression on EITHER lane fails this suite.
//
// One fixture per producer lane (acp/codex shell call, claude Write, opencode bash).
// When you add a harness, add its fixture here (docs/adding-a-harness.md).
import { createClient } from "@libsql/client";
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";
import { describe, expect, it } from "vitest";

import {
  acpToAguiEvents,
  newBracket,
  type AgentUpdateEvent,
  type AguiBracket,
} from "@server/agui/mapAgentUpdate";
import { loadAgentSessionSnapshot } from "@server/agui/agentSessionProjection";

interface ToolFixture {
  lane: string;
  tool: string;
  title: string;
  input: Record<string, unknown>;
  output: string;
}

// The shapes each producer lane emits AFTER its C-TOOL conversion (asserted by that
// lane's own unit tests — nexus-acp-stream, nexus-harness-codex, nexus-transcript,
// nexus-agent adapters). This suite pins the downstream halves to them.
const FIXTURES: ToolFixture[] = [
  {
    lane: "codex commandExecution",
    tool: "shell",
    title: "/usr/bin/zsh -lc 'cargo test'",
    input: { command: "cargo test", cwd: "/home/user/proj" },
    output: "ok. 12 passed",
  },
  {
    lane: "claude Write",
    tool: "Write",
    title: "Write",
    input: { file_path: "/tmp/proof.md", content: "# proof" },
    output: "wrote 7 bytes",
  },
  {
    lane: "opencode bash",
    tool: "bash",
    title: "bash",
    input: { command: "pwd" },
    output: "/tmp\n",
  },
];

function liveEvents(fixture: ToolFixture, id: string): BaseEvent[] {
  const updates: AgentUpdateEvent[] = [
    {
      type: "agent.update",
      sessionId: "s_fid",
      kind: "tool_call",
      data: { id, tool: fixture.tool, title: fixture.title, status: "in_progress", input: fixture.input },
    } as AgentUpdateEvent,
    {
      type: "agent.update",
      sessionId: "s_fid",
      kind: "tool_call",
      data: { id, status: "completed", content: fixture.output },
    } as AgentUpdateEvent,
  ];
  let bracket: AguiBracket = newBracket();
  const out: BaseEvent[] = [];
  for (const u of updates) {
    const mapped = acpToAguiEvents(u, bracket);
    bracket = mapped.bracket;
    out.push(...mapped.events);
  }
  return out;
}

async function replayEvents(fixture: ToolFixture, id: string): Promise<BaseEvent[]> {
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
      finalized_at INTEGER
    )`,
  ]);
  // The block shape the daemon materializer persists for this call (see
  // agent_session_materializer.rs merge_tool_call + its integration test).
  const block = {
    type: "tool_call",
    id,
    tool: fixture.tool,
    name: fixture.title,
    status: "completed",
    input: fixture.input,
    argsJson: JSON.stringify(fixture.input, null, 2),
    output: fixture.output,
  };
  await db.batch([
    {
      sql:
        "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
        "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      args: ["turn_f", "s_fid", "final", 1, 3, 100, 119, 120],
    },
    {
      sql:
        "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
        "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      args: [
        "m_f",
        "s_fid",
        "turn_f",
        0,
        "assistant",
        null,
        JSON.stringify({ schema: 1, blocks: [block] }),
        "final",
        1,
        3,
        101,
        119,
        120,
      ],
    },
  ]);
  const snapshot = await loadAgentSessionSnapshot("s_fid", "fid", { client: db });
  return snapshot.events;
}

function toolFacts(events: BaseEvent[]) {
  const records = events as Array<Record<string, unknown>>;
  const start = records.find((e) => e.type === EventType.TOOL_CALL_START);
  const args = records.find((e) => e.type === EventType.TOOL_CALL_ARGS);
  const result = records.find((e) => e.type === EventType.TOOL_CALL_RESULT);
  return {
    name: start?.toolCallName,
    args: args ? (JSON.parse(String(args.delta)) as unknown) : undefined,
    result: result?.content,
  };
}

describe("stream fidelity gate — live and replayed tool calls are equivalent", () => {
  for (const fixture of FIXTURES) {
    it(`${fixture.lane}: canonical name + parseable args survive both lanes`, async () => {
      const live = toolFacts(liveEvents(fixture, "tc_fid"));
      const replay = toolFacts(await replayEvents(fixture, "tc_fid"));

      // Canonical machine name on both lanes — never the display title.
      expect(live.name).toBe(fixture.tool);
      expect(replay.name).toBe(fixture.tool);

      // The fidelity law: args parse back to the ORIGINAL input object on both lanes.
      expect(live.args).toEqual(fixture.input);
      expect(replay.args).toEqual(fixture.input);

      // The result content survives both lanes.
      expect(live.result).toBe(fixture.output);
      expect(replay.result).toBe(fixture.output);
    });
  }

  it("a legacy argsJson-only block (pre-contract store) still replays parseable args", async () => {
    const db = createClient({ url: ":memory:" });
    await db.batch([
      `CREATE TABLE agent_session_turns (
        id TEXT PRIMARY KEY, session_id TEXT NOT NULL, status TEXT NOT NULL,
        first_stream_event_id INTEGER NOT NULL, last_stream_event_id INTEGER NOT NULL,
        started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, finalized_at INTEGER
      )`,
      `CREATE TABLE agent_session_messages (
        id TEXT PRIMARY KEY, session_id TEXT NOT NULL, turn_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL, role TEXT NOT NULL, author TEXT, content_json TEXT NOT NULL,
        status TEXT NOT NULL, first_stream_event_id INTEGER NOT NULL,
        last_stream_event_id INTEGER NOT NULL, created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL, finalized_at INTEGER
      )`,
    ]);
    await db.batch([
      {
        sql:
          "INSERT INTO agent_session_turns (id, session_id, status, first_stream_event_id, " +
          "last_stream_event_id, started_at, updated_at, finalized_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        args: ["turn_l", "s_leg", "final", 1, 2, 100, 119, 120],
      },
      {
        sql:
          "INSERT INTO agent_session_messages (id, session_id, turn_id, ordinal, role, author, " +
          "content_json, status, first_stream_event_id, last_stream_event_id, created_at, updated_at, finalized_at) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        args: [
          "m_l",
          "s_leg",
          "turn_l",
          0,
          "assistant",
          null,
          JSON.stringify({
            schema: 1,
            blocks: [
              {
                type: "tool_call",
                id: "tc_leg",
                name: "write",
                status: "completed",
                argsJson: '{"file_path": "/tmp/a.md"}',
              },
            ],
          }),
          "final",
          1,
          2,
          101,
          119,
          120,
        ],
      },
    ]);
    const snapshot = await loadAgentSessionSnapshot("s_leg", "leg", { client: db });
    const facts = toolFacts(snapshot.events);
    expect(facts.args).toEqual({ file_path: "/tmp/a.md" });
  });
});
