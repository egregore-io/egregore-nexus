// Lane B renderer — maps ONE contract `agent.update` event (`AgentUpdateEvent`)
// to AG-UI `BaseEvent`s, carrying a per-run `bracket` so successive updates
// open/close text & reasoning message blocks correctly, preserve stable native
// message identity across interleaved tools, and merge tool calls by id.
// `closeRun(bracket)` ENDs whatever is still open at turn-end.
//
// The event vocabulary is byte-compatible with egregore-lens's `aguiReader.ts`:
// TEXT_MESSAGE_START/CONTENT/END, REASONING_MESSAGE_START/CONTENT/END,
// TOOL_CALL_START/ARGS/RESULT. `plan` has no native AG-UI event → STATE_DELTA (a
// JSON-Patch the reader treats as a state event); `commands` → CUSTOM
// `nexus.commands` (the reader ignores CUSTOM names other than `lens.component`,
// so nothing is dropped). Run lifecycle (RUN_STARTED/FINISHED/ERROR) is emitted
// by the endpoint orchestrator, not here.
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";
import type { WsEvent } from "@shared/types";

/** The contract `agent.update` wire event — the input this mapper consumes. */
export type AgentUpdateEvent = Extract<WsEvent, { type: "agent.update" }>;

// Internal projection metadata, not a property accepted from native wire data.
// The materialized relay passes these objects directly to the mapper; object
// spreads preserve the symbol, while serializing a raw wire event cannot forge it.
const materializedBlock = Symbol("nexus.materializedBlock");
type MaterializedUpdate = AgentUpdateEvent & { [materializedBlock]?: string };
export function withMaterializedBlock(ev: AgentUpdateEvent, rowId: string, index: number): AgentUpdateEvent {
  return { ...ev, [materializedBlock]: JSON.stringify([ev.sessionId, rowId, index]) } as MaterializedUpdate;
}

/** CUSTOM event name carrying a harness's advertised slash commands. */
export const NEXUS_COMMANDS_EVENT = "nexus.commands";

// --- bracket state ----------------------------------------------------------

/** Which streaming message channel is currently open (at most one). */
type OpenChannel = "text" | "reasoning" | null;

interface GeneratedMessageOrigin {
  kind: "generated";
  sessionId: string;
  sourceEventId: number;
  channel: "text" | "reasoning" | "user";
  materializedBlock?: string;
}

/**
 * Per-run bracketing state. `messageId` is the id of the currently-open text /
 * reasoning message (AG-UI brackets a message by a stable id across its
 * START/CONTENT/END). `openToolIds` tracks tool-call ids already STARTed so a
 * later update for the same id merges (ARGS/RESULT) instead of re-opening.
 */
export interface AguiBracket {
  openChannel: OpenChannel;
  messageId: string | null;
  /** Native harness item id for the open message, when the stream supplies one. */
  messageItemId: string | null;
  messageOrigin: GeneratedMessageOrigin | null;
  messageBlockId: string | null;
  openToolIds: Set<string>;
  userInputEchoes: Map<string, string | null>;
  /** Monotonic counter so each new message block gets a fresh id. */
  seq: number;
}

/** A fresh, empty bracket for the start of a run. */
export function newBracket(): AguiBracket {
  return {
    openChannel: null,
    messageId: null,
    messageItemId: null,
    messageOrigin: null,
    messageBlockId: null,
    openToolIds: new Set(),
    userInputEchoes: new Map(),
    seq: 0,
  };
}

// --- event constructors (typed; widened to BaseEvent) -----------------------

const textStart = (messageId: string): BaseEvent =>
  ({ type: EventType.TEXT_MESSAGE_START, messageId, role: "assistant" }) as BaseEvent;
const userStart = (messageId: string): BaseEvent =>
  ({ type: EventType.TEXT_MESSAGE_START, messageId, role: "user" }) as BaseEvent;
const textContent = (messageId: string, delta: string): BaseEvent =>
  ({ type: EventType.TEXT_MESSAGE_CONTENT, messageId, delta }) as BaseEvent;
const textEnd = (messageId: string): BaseEvent =>
  ({ type: EventType.TEXT_MESSAGE_END, messageId }) as BaseEvent;

const reasoningStart = (messageId: string): BaseEvent =>
  ({ type: EventType.REASONING_MESSAGE_START, messageId, role: "reasoning" }) as BaseEvent;
const reasoningContent = (messageId: string, delta: string): BaseEvent =>
  ({ type: EventType.REASONING_MESSAGE_CONTENT, messageId, delta }) as BaseEvent;
const reasoningEnd = (messageId: string): BaseEvent =>
  ({ type: EventType.REASONING_MESSAGE_END, messageId }) as BaseEvent;

const toolStart = (toolCallId: string, toolCallName: string): BaseEvent =>
  ({ type: EventType.TOOL_CALL_START, toolCallId, toolCallName }) as BaseEvent;
const toolArgs = (toolCallId: string, delta: string): BaseEvent =>
  // Canonical input is a whole structured value, not a string fragment. Older
  // consumers must be paired before rollout; absence of append remains a delta.
  ({ type: EventType.TOOL_CALL_ARGS, toolCallId, delta, append: false }) as BaseEvent;
const toolResult = (
  toolCallId: string,
  content: string,
  messageId: string,
  status?: string,
  append?: boolean,
): BaseEvent => {
  const ev: Record<string, unknown> = {
    type: EventType.TOOL_CALL_RESULT,
    messageId,
    toolCallId,
    content,
    role: "tool",
  };
  if (status !== undefined) ev.status = status;
  if (append !== undefined) ev.append = append;
  return ev as BaseEvent;
};

const stateDelta = (delta: unknown[]): BaseEvent =>
  ({ type: EventType.STATE_DELTA, delta }) as BaseEvent;
const customEvent = (name: string, value: unknown): BaseEvent =>
  ({ type: EventType.CUSTOM, name, value }) as BaseEvent;

// --- helpers ----------------------------------------------------------------

const asRecord = (v: unknown): Record<string, unknown> =>
  typeof v === "object" && v !== null ? (v as Record<string, unknown>) : {};

const str = (v: unknown): string | undefined =>
  typeof v === "string" ? v : undefined;

const has = (obj: Record<string, unknown>, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(obj, key);

function userInputMetadata(data: Record<string, unknown>): Record<string, unknown> {
  const metadata: Record<string, unknown> = {};
  const source = str(data.source);
  if (source) metadata.source = source;
  const name = str(data.name);
  if (name) metadata.name = name;
  const kind = str(data.kind);
  if (kind) metadata.kind = kind;
  const harness = str(data.harness);
  if (harness) metadata.harness = harness;
  const runtimeId = str(data.runtimeId) ?? str(data.runtime_id);
  if (runtimeId) metadata.runtimeId = runtimeId;
  return metadata;
}

function streamEventId(data: Record<string, unknown>): number | undefined {
  const raw = data.streamEventId ?? data.stream_event_id;
  const n = typeof raw === "number" ? raw : Number(raw);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

function withStreamEventId(events: BaseEvent[], data: Record<string, unknown>): BaseEvent[] {
  const id = streamEventId(data);
  if (id === undefined) return events;
  return events.map((ev) => ({ ...(ev as Record<string, unknown>), streamEventId: id }) as unknown as BaseEvent);
}

function generatedMessageId(
  sessionId: string,
  data: Record<string, unknown>,
  kind: "msg" | "user",
  seq: number,
  blockId?: string,
): string {
  if (blockId !== undefined) return `nexus-materialized:${encodeURIComponent(blockId)}:${kind}`;
  const id = streamEventId(data);
  const scope = id === undefined ? sessionId : `${sessionId}:stream:${id}`;
  // A retained source event opens at most one block of a given kind. Its identity
  // must not depend on how many earlier blocks this particular replay observed.
  // Epoch/authority remain carried by the enclosing exact-source stream.
  return `${scope}:${kind}:${id === undefined ? seq : 1}`;
}

function generatedOrigin(
  sessionId: string,
  data: Record<string, unknown>,
  channel: GeneratedMessageOrigin["channel"],
  blockId?: string,
): GeneratedMessageOrigin | null {
  const sourceEventId = streamEventId(data);
  return sourceEventId === undefined ? null : {
    kind: "generated", sessionId, sourceEventId, channel,
    ...(blockId !== undefined ? {materializedBlock: blockId} : {}),
  };
}

function withOrigin(event: BaseEvent, origin: GeneratedMessageOrigin | null): BaseEvent {
  return origin ? { ...event, nexusMessageOrigin: origin } as BaseEvent : event;
}

/** Convert tool output payloads into displayable text without dropping structured JSON. */
function toolContent(v: unknown): string | undefined {
  if (v === undefined || v === null) return undefined;
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

/** Close whatever message channel is open, appending its END event. */
function endOpenChannel(b: AguiBracket, out: BaseEvent[]): void {
  if (b.openChannel === "text" && b.messageId) out.push(withOrigin(textEnd(b.messageId), b.messageOrigin));
  else if (b.openChannel === "reasoning" && b.messageId)
    out.push(withOrigin(reasoningEnd(b.messageId), b.messageOrigin));
  b.openChannel = null;
  b.messageId = null;
  b.messageItemId = null;
  b.messageOrigin = null;
  b.messageBlockId = null;
}

// --- OUT mapping ------------------------------------------------------------

/**
 * Map one `agent.update` → AG-UI events, threading the bracket. Opens a block on
 * a channel transition, CONTENT on each delta, and ENDs the previous block on a
 * channel change. A tool call closes legacy unkeyed messages, but a text message
 * carrying a stable native `data.itemId` stays bracketed across interleaved tool
 * events. Tool calls merge by `data.id` — START once, then ARGS / RESULT on later
 * updates for that id.
 *
 * Returns the events to emit + the advanced bracket (caller threads it forward).
 */
export function acpToAguiEvents(
  ev: AgentUpdateEvent,
  bracket: AguiBracket,
): { events: BaseEvent[]; bracket: AguiBracket } {
  // Clone so the function is pure w.r.t. the input bracket value.
  const b: AguiBracket = {
    openChannel: bracket.openChannel,
    messageId: bracket.messageId,
    messageItemId: bracket.messageItemId,
    messageOrigin: bracket.messageOrigin,
    messageBlockId: bracket.messageBlockId,
    openToolIds: new Set(bracket.openToolIds),
    userInputEchoes: new Map(bracket.userInputEchoes),
    seq: bracket.seq,
  };
  const out: BaseEvent[] = [];
  const data = asRecord(ev.data);
  const blockId = (ev as MaterializedUpdate)[materializedBlock];

  switch (ev.kind) {
    case "text":
    case "thinking": {
      const channel: OpenChannel = ev.kind === "text" ? "text" : "reasoning";
      const itemId = str(data.itemId) ?? str(data.item_id) ?? null;
      // A channel switch closes the previous message block first.
      if (b.openChannel && b.openChannel !== channel) endOpenChannel(b, out);
      // Native item ids distinguish adjacent messages even when their AG-UI channel is the same.
      if (b.openChannel === channel && b.messageItemId !== itemId) endOpenChannel(b, out);
      if (b.openChannel === channel && itemId === null && b.messageBlockId !== (blockId ?? null)) endOpenChannel(b, out);
      if (b.openChannel !== channel) {
        const messageId = itemId ?? generatedMessageId(ev.sessionId, data, "msg", ++b.seq, blockId);
        b.openChannel = channel;
        b.messageId = messageId;
        b.messageItemId = itemId;
        b.messageBlockId = itemId === null ? blockId ?? null : null;
        b.messageOrigin = itemId === null ? generatedOrigin(ev.sessionId, data, channel, blockId) : null;
        out.push(withOrigin(channel === "text" ? textStart(messageId) : reasoningStart(messageId), b.messageOrigin));
      }
      const delta = str(data.text) ?? "";
      const messageId = b.messageId as string;
      out.push(
        withOrigin(channel === "text"
          ? textContent(messageId, delta)
          : reasoningContent(messageId, delta), b.messageOrigin),
      );
      break;
    }

    case "tool_call": {
      // Stable native text items may contain tool activity between deltas. Keep that message open
      // so the tool events retain their temporal position without splitting one assistant item
      // into multiple bubbles. Legacy/unkeyed text and reasoning retain their prior semantics.
      if (b.openChannel && !(b.openChannel === "text" && b.messageItemId)) {
        endOpenChannel(b, out);
      }
      const id = str(data.id) ?? str(data.toolCallId);
      if (!id) break; // can't key a tool call without an id — drop defensively
      if (!b.openToolIds.has(id)) {
        // C-TOOL v1: `tool` is the canonical machine name; `title` is display-only
        // (for ACP harnesses it is the whole command line) and only a fallback here.
        const name = str(data.tool) ?? str(data.title) ?? str(data.kind) ?? "tool";
        b.openToolIds.add(id);
        out.push(toolStart(id, name));
      }
      // Structured args (`input`, an object) → ARGS, stringified exactly ONCE — the
      // consumer must get JSON that parses back to the original object.
      if (has(data, "input") && data.input !== null) {
        out.push(toolArgs(id, JSON.stringify(data.input)));
      }
      const outputSource = has(data, "content")
        ? data.content
        : has(data, "output")
          ? data.output
          : undefined;
      const content = toolContent(outputSource);
      if (content !== undefined) {
        const status = str(data.status);
        const append = status === "in_progress" && !has(data, "content");
        // RESULT requires a messageId; tie it to the tool-call id deterministically.
        out.push(toolResult(id, content, `${id}:result`, status, append));
      }
      break;
    }

    case "plan": {
      // No native AG-UI plan event → a JSON-Patch STATE_DELTA replacing `/plan`.
      // The lens reader recognizes STATE_DELTA as a state event (tolerated).
      const entries = Array.isArray(data.entries) ? data.entries : [];
      out.push(stateDelta([{ op: "replace", path: "/plan", value: entries }]));
      break;
    }

    case "commands": {
      // Out of the core render path → a CUSTOM fact (reader ignores unknown names).
      const commands = Array.isArray(data.commands) ? data.commands : [];
      out.push(customEvent(NEXUS_COMMANDS_EVENT, { sessionId: ev.sessionId, commands }));
      break;
    }

    case "user_input": {
      // A user-role turn the harness received — the operator's typed input (from this web view,
      // the TUI, or another operator) or a daemon-injected batch. Emit a COMPLETE user-role message
      // block (start→content→end) so it renders as the user's line. This is what makes the web view
      // and the `nexus attach` TUI mirror each other: it rides the same session stream.
      if (b.openChannel) endOpenChannel(b, out);
      const text = str(data.text) ?? "";
      const explicitMessageId =
        str(data.clientMessageId) ?? str(data.client_message_id) ?? null;
      const scopedMessageId = explicitMessageId ?? (blockId === undefined ? null
        : generatedMessageId(ev.sessionId, data, "user", 0, blockId));
      const seenMessageId = b.userInputEchoes.get(text);
      if (
        seenMessageId !== undefined &&
        (scopedMessageId === null || seenMessageId === null || scopedMessageId === seenMessageId)
      ) {
        if (seenMessageId === null && explicitMessageId !== null) {
          b.userInputEchoes.set(text, explicitMessageId);
        }
        break;
      }
      const messageId =
        scopedMessageId ?? generatedMessageId(ev.sessionId, data, "user", ++b.seq);
      b.userInputEchoes.set(text, messageId);
      const metadata = userInputMetadata(data);
      const origin = explicitMessageId === null ? generatedOrigin(ev.sessionId, data, "user", blockId) : null;
      out.push(
        withOrigin({ ...(userStart(messageId) as Record<string, unknown>), ...metadata } as unknown as BaseEvent, origin),
        withOrigin({ ...(textContent(messageId, text) as Record<string, unknown>), ...metadata } as unknown as BaseEvent, origin),
        withOrigin({ ...(textEnd(messageId) as Record<string, unknown>), ...metadata } as unknown as BaseEvent, origin),
      );
      break;
    }

    case "turn_end": {
      // Pure lifecycle marker (the turn is complete) — no renderable AG-UI event. The observe
      // orchestrator detects it (isTurnEnd) and emits `closeRun` + RUN_FINISHED, which closes the
      // streaming row. Nothing to map here.
      break;
    }

    default: {
      // Exhaustiveness guard — a new kind would surface here at compile time.
      const _never: never = ev.kind;
      void _never;
    }
  }

  return { events: withStreamEventId(out, data), bracket: b };
}

/** END any still-open message block at turn-end. Tool calls need no END here. */
export function closeRun(bracket: AguiBracket): BaseEvent[] {
  const out: BaseEvent[] = [];
  if (bracket.openChannel === "text" && bracket.messageId)
    out.push(withOrigin(textEnd(bracket.messageId), bracket.messageOrigin));
  else if (bracket.openChannel === "reasoning" && bracket.messageId)
    out.push(withOrigin(reasoningEnd(bracket.messageId), bracket.messageOrigin));
  return out;
}
