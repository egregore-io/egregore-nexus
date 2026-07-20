// Materialized `/agent` session history for the AG-UI observe endpoint.
//
// The daemon writes raw token/activity frames to `stream_events` and folds finalized turns into
// `agent_session_turns` + `agent_session_messages`. This module hydrates a bounded finalized
// history from that compact projection, then lets the raw stream relay tail only rows after the
// last finalized turn. That keeps refresh/reconnect from replaying the entire raw token log.
import type { BaseEvent } from "@ag-ui/client";
import type { Client, Row } from "@libsql/client";

import { getReadDb } from "@drizzle/client";
import type { AgentUpdateEvent, AguiBracket } from "@server/agui/mapAgentUpdate";
import {
  acpToAguiEvents,
  closeRun,
  newBracket,
} from "@server/agui/mapAgentUpdate";
import {
  agentSessionThreadId,
  runFinished,
  runStarted,
} from "@server/agui/_sseCore";
import type { RelayFactory } from "@server/agui/run";

const DEFAULT_HISTORY_TURNS = 100;

interface MaterializedRow {
  id: string;
  sessionId: string;
  turnId: string;
  ordinal: number;
  role: string;
  author?: string;
  contentJson: string;
  firstStreamEventId: number;
}

interface MaterializedBlock {
  type?: unknown;
  text?: unknown;
  id?: unknown;
  itemId?: unknown;
  item_id?: unknown;
  source?: unknown;
  name?: unknown;
  kind?: unknown;
  title?: unknown;
  harness?: unknown;
  runtimeId?: unknown;
  runtime_id?: unknown;
  status?: unknown;
  tool?: unknown;
  input?: unknown;
  argsJson?: unknown;
  output?: unknown;
  entries?: unknown;
  clientMessageId?: unknown;
  client_message_id?: unknown;
}

interface MaterializedContent {
  blocks?: unknown[];
}

export interface AgentSessionSnapshot {
  /** AG-UI frames for the bounded finalized history. */
  events: BaseEvent[];
  /** Raw `stream_events.id` cursor the live relay should start after. */
  tailAfterId: number;
  /** Finalize-order cursor (`finalized_at`, `rowid`) the materialized-turn relay starts after. */
  tailCursor: TurnCursor;
}

/** Compound finalize-order cursor: `finalized_at` with `rowid` as the tiebreak. */
export interface TurnCursor {
  finalizedAt: number;
  rowid: number;
}

/** Metadata for one finalized materialized turn before its rows are replayed. */
export interface MaterializedTurnInfo {
  id: string;
  finalizedAt: number;
  rowid: number;
  lastStreamEventId: number;
}

function cell(row: Row, key: string): unknown {
  return (row as Record<string, unknown>)[key];
}

function asString(value: unknown): string {
  if (typeof value === "string") return value;
  if (value === null || value === undefined) return "";
  return String(value);
}

function asNumber(value: unknown): number {
  if (typeof value === "number") return value;
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseContent(json: string): MaterializedContent {
  try {
    const value = JSON.parse(json) as unknown;
    return typeof value === "object" && value !== null
      ? (value as MaterializedContent)
      : {};
  } catch {
    return {};
  }
}

function blockObject(value: unknown): MaterializedBlock | null {
  return typeof value === "object" && value !== null
    ? (value as MaterializedBlock)
    : null;
}

function update(
  sessionId: string,
  kind: AgentUpdateEvent["kind"],
  data: unknown,
): AgentUpdateEvent {
  return { type: "agent.update", sessionId, kind, data } as AgentUpdateEvent;
}

function eventsForBlock(row: MaterializedRow, block: MaterializedBlock): AgentUpdateEvent[] {
  const kind = asString(block.type);
  if (row.role === "user") {
    if (kind !== "text") return [];
    const data: Record<string, unknown> = { text: asString(block.text) };
    const clientMessageId = asString(block.clientMessageId) || asString(block.client_message_id) || asString(block.id);
    if (clientMessageId) data.clientMessageId = clientMessageId;
    const source = asString(block.source);
    if (source) data.source = source;
    const name = asString(block.name);
    if (name) data.name = name;
    const actorKind = asString(block.kind);
    if (actorKind) data.kind = actorKind;
    const harness = asString(block.harness);
    if (harness) data.harness = harness;
    const runtimeId = asString(block.runtimeId) || asString(block.runtime_id);
    if (runtimeId) data.runtimeId = runtimeId;
    return [update(row.sessionId, "user_input", data)];
  }

  if (kind === "text") {
    const data: Record<string, unknown> = { text: asString(block.text) };
    const itemId = asString(block.itemId) || asString(block.item_id);
    if (itemId) data.itemId = itemId;
    return [update(row.sessionId, "text", data)];
  }
  if (kind === "thinking") {
    const data: Record<string, unknown> = { text: asString(block.text) };
    const itemId = asString(block.itemId) || asString(block.item_id);
    if (itemId) data.itemId = itemId;
    return [update(row.sessionId, "thinking", data)];
  }
  if (kind === "plan") {
    return [
      update(row.sessionId, "plan", {
        entries: Array.isArray(block.entries) ? block.entries : undefined,
        text: asString(block.text),
      }),
    ];
  }
  if (kind === "tool_call") {
    const id = asString(block.id) || `${row.id}:tool`;
    const data: Record<string, unknown> = {
      id,
      title: asString(block.name) || asString(block.title) || "tool",
    };
    // C-TOOL v1 (docs/tool-call-contract.md): replay is lossless — the canonical `tool`
    // name and the STRUCTURED `input` object come back exactly as the live stream carried
    // them. Blocks that predate the structured field fall back to parsing the display
    // string (never re-stringifying it — that double-encode blanked every replayed arg).
    const tool = asString(block.tool);
    if (tool) data.tool = tool;
    const status = asString(block.status);
    if (status) data.status = status;
    const input =
      block.input !== undefined ? block.input : parseJsonMaybe(asString(block.argsJson));
    if (input !== undefined && input !== null) data.input = input;
    const output = asString(block.output);
    if (output) data.content = output;
    return [update(row.sessionId, "tool_call", data)];
  }
  return [];
}

/** Parse JSON text to a value; `undefined` when absent or not valid JSON. */
function parseJsonMaybe(text: string): unknown {
  if (!text) return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return undefined;
  }
}

function rowToAgentUpdates(row: MaterializedRow): AgentUpdateEvent[] {
  const content = parseContent(row.contentJson);
  const out: AgentUpdateEvent[] = [];
  for (const raw of content.blocks ?? []) {
    const block = blockObject(raw);
    if (!block) continue;
    for (const ev of eventsForBlock(row, block)) {
      // Stamp the materialized row's stream anchor so history replay carries the
      // same streamEventId field the live tail does (consumers dedupe on it).
      const data = ev.data as Record<string, unknown> | undefined;
      if (data && typeof data === "object" && data.streamEventId === undefined) {
        data.streamEventId = row.firstStreamEventId;
      }
      out.push(ev);
    }
  }
  return out;
}

function mapTurn(
  sessionName: string,
  turnId: string,
  rows: MaterializedRow[],
): BaseEvent[] {
  const threadId = agentSessionThreadId(sessionName);
  // The turn id itself is the runId — the SAME id the live lane derives for this turn
  // (turn_<sessionId>_<firstStreamEventId>), so a replayed turn is idempotent for
  // id-keyed consumers instead of arriving under a second identity.
  const runId = turnId;
  let bracket: AguiBracket = newBracket();
  const body: BaseEvent[] = [];

  for (const row of rows) {
    for (const ev of rowToAgentUpdates(row)) {
      const mapped = acpToAguiEvents(ev, bracket);
      bracket = mapped.bracket;
      body.push(...mapped.events);
    }
  }

  if (body.length === 0) return [];
  return [runStarted(threadId, runId), ...body, ...closeRun(bracket), runFinished(threadId, runId)];
}

function rowFromDb(row: Row): MaterializedRow {
  const author = asString(cell(row, "author"));
  return {
    id: asString(cell(row, "id")),
    sessionId: asString(cell(row, "session_id")),
    turnId: asString(cell(row, "turn_id")),
    ordinal: asNumber(cell(row, "ordinal")),
    role: asString(cell(row, "role")),
    author: author || undefined,
    contentJson: asString(cell(row, "content_json")),
    firstStreamEventId: asNumber(cell(row, "first_stream_event_id")),
  };
}

function mapRows(sessionName: string, rows: MaterializedRow[]): BaseEvent[] {
  const byTurn = new Map<string, MaterializedRow[]>();
  for (const row of rows) {
    const bucket = byTurn.get(row.turnId) ?? [];
    bucket.push(row);
    byTurn.set(row.turnId, bucket);
  }

  const out: BaseEvent[] = [];
  for (const [turnId, turnRows] of byTurn) {
    turnRows.sort((a, b) => a.ordinal - b.ordinal);
    out.push(...mapTurn(sessionName, turnId, turnRows));
  }
  return out;
}

/**
 * Load bounded finalized `/agent` history and the raw stream cursor to continue from.
 *
 * The live cursor is the last finalized turn's stream id, not the global materialization cursor:
 * the global cursor may sit inside an active turn. Starting after the last finalized turn lets an
 * in-flight turn replay from its beginning and then continue correctly.
 */
export async function loadAgentSessionSnapshot(
  sessionId: string,
  sessionName: string,
  deps: { client?: Client; historyTurns?: number } = {},
): Promise<AgentSessionSnapshot> {
  const client = deps.client ?? getReadDb().$client;
  const historyTurns = deps.historyTurns ?? DEFAULT_HISTORY_TURNS;
  // The tail cursor is the (finalized_at, rowid) pair of the LAST-finalized turn —
  // both columns from the same row, so the compound comparison in the relay never
  // skips a same-millisecond tie.
  const cursorResult = await client.execute({
    sql:
      "SELECT COALESCE(last_stream_event_id, 0) AS tail_after_id, " +
      "COALESCE(finalized_at, 0) AS tail_finalized_at, " +
      "COALESCE(rowid, 0) AS tail_rowid " +
      "FROM agent_session_turns WHERE session_id = ? AND finalized_at IS NOT NULL " +
      "ORDER BY finalized_at DESC, rowid DESC LIMIT 1",
    args: [sessionId],
  });
  const tailAfterId = asNumber(cursorResult.rows[0]?.tail_after_id);
  const tailCursor: TurnCursor = {
    finalizedAt: asNumber(cursorResult.rows[0]?.tail_finalized_at),
    rowid: asNumber(cursorResult.rows[0]?.tail_rowid),
  };

  if (historyTurns <= 0) return { events: [], tailAfterId, tailCursor };

  const result = await client.execute({
    sql:
      "SELECT m.id, m.session_id, m.turn_id, m.ordinal, m.role, m.author, m.content_json, m.first_stream_event_id " +
      "FROM agent_session_messages m " +
      "JOIN agent_session_turns t ON t.id = m.turn_id " +
      "WHERE m.session_id = ? AND t.finalized_at IS NOT NULL AND t.id IN (" +
      "  SELECT id FROM agent_session_turns WHERE session_id = ? AND finalized_at IS NOT NULL " +
      "  ORDER BY started_at DESC, id DESC LIMIT ?" +
      ") ORDER BY t.started_at ASC, m.ordinal ASC",
    args: [sessionId, sessionId, historyTurns],
  });

  const rows = result.rows.map(rowFromDb);
  return {
    events: mapRows(sessionName, rows),
    tailAfterId,
    tailCursor,
  };
}

/**
 * Find the finalized-turn cursor that is safe for a reconnect carrying a raw
 * stream id. If the id sits inside a finalized turn, return the previous
 * finalized turn so the materialized relay replays and repairs that turn.
 */
export async function materializedCursorForStreamAfter(
  sessionId: string,
  afterId: number,
  deps: { client?: Client } = {},
): Promise<TurnCursor> {
  if (!Number.isFinite(afterId) || afterId <= 0) return { finalizedAt: 0, rowid: 0 };
  const client = deps.client ?? getReadDb().$client;
  const result = await client.execute({
    sql:
      "SELECT COALESCE(finalized_at, 0) AS finalized_at, COALESCE(rowid, 0) AS turn_rowid " +
      "FROM agent_session_turns " +
      "WHERE session_id = ? AND finalized_at IS NOT NULL AND last_stream_event_id <= ? " +
      "ORDER BY last_stream_event_id DESC, finalized_at DESC, rowid DESC LIMIT 1",
    args: [sessionId, Math.floor(afterId)],
  });
  return {
    finalizedAt: asNumber(result.rows[0]?.finalized_at),
    rowid: asNumber(result.rows[0]?.turn_rowid),
  };
}

// --- live tail: materialized-turn relay --------------------------------------
//
// The daemon holds raw token frames in its PRIVATE in-memory `mem.stream_events`
// (the ephemeral-stream migration drops `main.stream_events` from the file DB),
// so a cross-process gateway can never tail raw frames. What IS durable and
// continuously updated in the file DB is the finalized-turn projection
// (`agent_session_turns` + `agent_session_messages`) — so the live lane tails
// finalize-order turns and emits each one's `agent.update` events followed by a
// `turn_end` marker. `observeAgentSession` brackets each turn into its own
// RUN_STARTED…RUN_FINISHED. Turn-level (not token-level) liveness: token
// streaming needs a daemon-served feed (#28 native_forwarder territory).

/** Poll cadence for newly finalized turns. */
const DEFAULT_TURN_POLL_MS = 500;

function turnEndEvent(sessionId: string): AgentUpdateEvent {
  return update(sessionId, "turn_end", {});
}

/**
 * Build a `RelayFactory` that tails newly FINALIZED turns for one session from
 * the materialized projection, in finalize order, starting after `afterCursor`
 * (the snapshot's `tailCursor`). Emits the same per-block `agent.update` events
 * the snapshot mapper derives, plus a `turn_end` per turn.
 */
export function createMaterializedTurnRelay(
  sessionId: string,
  deps: {
    client?: Client;
    pollMs?: number;
    afterCursor?: TurnCursor;
    shouldSkipTurn?: (turn: MaterializedTurnInfo) => boolean;
    onTurnEmitted?: (cursor: TurnCursor, turn: MaterializedTurnInfo) => void;
  } = {},
): RelayFactory {
  const pollMs = deps.pollMs ?? DEFAULT_TURN_POLL_MS;
  return ({ onEvent, onError = () => {} }) => {
    const client = deps.client ?? getReadDb().$client;
    let cursor: TurnCursor = deps.afterCursor ?? { finalizedAt: 0, rowid: 0 };
    let closed = false;
    let polling = false;
    let paused = false;
    let pending: {
      events: AgentUpdateEvent[];
      index: number;
      turn: MaterializedTurnInfo;
    } | undefined;

    const emitPending = (): boolean => {
      if (!pending) return true;
      while (pending.index < pending.events.length) {
        if (onEvent(pending.events[pending.index]!) === false) {
          paused = true;
          return false;
        }
        pending.index += 1;
      }
      const completed = pending.turn;
      cursor = { finalizedAt: completed.finalizedAt, rowid: completed.rowid };
      deps.onTurnEmitted?.(cursor, completed);
      pending = undefined;
      return true;
    };

    const drainNew = async (): Promise<void> => {
      if (!emitPending()) return;
      const turns = await client.execute({
        sql:
          "SELECT id, rowid AS turn_rowid, finalized_at, last_stream_event_id " +
          "FROM agent_session_turns " +
          "WHERE session_id = ? AND finalized_at IS NOT NULL " +
          "AND (finalized_at > ? OR (finalized_at = ? AND rowid > ?)) " +
          "ORDER BY finalized_at ASC, rowid ASC",
        args: [sessionId, cursor.finalizedAt, cursor.finalizedAt, cursor.rowid],
      });
      for (const turn of turns.rows) {
        if (closed) return;
        const turnInfo: MaterializedTurnInfo = {
          id: asString(cell(turn, "id")),
          finalizedAt: asNumber(cell(turn, "finalized_at")),
          rowid: asNumber(cell(turn, "turn_rowid")),
          lastStreamEventId: asNumber(cell(turn, "last_stream_event_id")),
        };
        if (deps.shouldSkipTurn?.(turnInfo)) {
          cursor = { finalizedAt: turnInfo.finalizedAt, rowid: turnInfo.rowid };
          continue;
        }
        const rows = await client.execute({
          sql:
            "SELECT id, session_id, turn_id, ordinal, role, author, content_json, first_stream_event_id " +
            "FROM agent_session_messages WHERE turn_id = ? ORDER BY ordinal ASC",
          args: [turnInfo.id],
        });
        pending = {
          events: [
            ...rows.rows.map(rowFromDb).flatMap(rowToAgentUpdates),
            turnEndEvent(sessionId),
          ],
          index: 0,
          turn: turnInfo,
        };
        if (!emitPending()) return;
      }
    };

    const tick = async (): Promise<void> => {
      if (closed || paused || polling) return;
      polling = true;
      try {
        await drainNew();
      } catch (err) {
        onError(err); // transient read error — next tick retries (durable projection, no loss)
      } finally {
        polling = false;
      }
    };

    void tick();
    const timer = setInterval(() => void tick(), pollMs);

    return {
      ready: Promise.resolve(),
      pause() {
        paused = true;
      },
      resume() {
        if (closed || !paused) return;
        paused = false;
        void tick();
      },
      close() {
        if (closed) return;
        closed = true;
        clearInterval(timer);
      },
    };
  };
}
