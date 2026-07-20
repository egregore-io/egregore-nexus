// Task 4 (plan 60) — the web console's AG-UI consumer: a pure reducer that folds
// decoded AG-UI `BaseEvent`s into the EXISTING `PaneMessage[]`/`Block[]` model,
// plus the `useAguiConversation` hook that wires the live `observe` stream into
// it (SSE by default, WebSocket when the gateway transport flag is enabled).
//
// The conversation render — egregore-lens AND our own web console — consumes the
// AG-UI `observe` stream (plan 60 T3). This file is OUR consumer: it reuses the
// daemon→AG-UI mapping (`@server/agui`) end-to-end, then translates the AG-UI
// vocabulary into the prototype's view-model so the SAME `Thread`/`MessageParts`
// renderers draw it — no new renderers. SSE keeps REST actions; WS can carry
// input envelopes over the active stream socket with REST fallback.
import { useCallback, useEffect, useRef, useState } from "react";
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";

import type { SendTarget } from "@shared/types";
import { logEvent } from "@app/log";
import {
  gatewayFetch,
  gatewayUrl,
  gatewayWebSocketProtocols,
} from "@app/gatewayClient";

import type { Block, ChipLabel, Inline, PaneMessage } from "./types";

type PendingEcho = {
  id: string;
  text: string;
  reconcileBy: "id" | "text";
};

// --- the agent identity a live row is attributed to -------------------------

/**
 * How a live turn is labelled in the thread. AG-UI `observe` does not yet carry
 * the agent's display identity (it is keyed by run/thread), so the caller (the
 * route, which knows the conversation) supplies it; defaults to a neutral agent.
 */
export interface AguiAgent {
  who: string;
  glyph: string;
  chip?: ChipLabel;
  presence?: PaneMessage["presence"];
}

const DEFAULT_AGENT: Required<Pick<AguiAgent, "who" | "glyph" | "chip">> = {
  who: "agent",
  glyph: "◆",
  chip: "agent",
};

// --- reducer state ----------------------------------------------------------

/**
 * The reducer's accumulator. `messages` is the render-ready `PaneMessage[]`
 * (each AG-UI run becomes one agent row). The rest is per-run bracketing so
 * deltas land in the right block: `openRunId` is the row currently accumulating,
 * `openTextMessageId` is the AG-UI message id whose text feeds the trailing `p`
 * block, and `toolBlockIndex` maps a `toolCallId` → its block index in the row so
 * later ARGS/RESULT updates merge in place instead of appending a new block.
 */
export interface AguiConversationState {
  messages: PaneMessage[];
  agent: Required<Pick<AguiAgent, "who" | "glyph" | "chip">> & {
    presence?: PaneMessage["presence"];
  };
  /** Authenticated viewer identity used to distinguish self from other streamed humans. */
  you: { who: string; glyph: string };
  openRunId: string | null;
  /** Index of the open run's row in `messages` (-1 when none). */
  openIndex: number;
  /** The AG-UI text message id currently feeding the trailing `p` block. */
  openTextMessageId: string | null;
  /** Stable run id currently being ignored because it replays an already-rendered row. */
  ignoredRunId: string | null;
  /** The AG-UI reasoning message id currently feeding the trailing thinking block. */
  openReasoningMessageId: string | null;
  /**
   * The AG-UI message id of a `role:"user"` message currently being buffered, and its accumulated
   * text. `observe` now echoes the operator's input back as a user-role message (the session stream
   * mirrors TUI ↔ web); we buffer it and decide on END whether it duplicates our own optimistic echo.
   */
  openUserMessageId: string | null;
  openUserCommittedMessageId: string | null;
  openUserName: string | null;
  openUserKind: string | null;
  userBuffer: string;
  /**
   * Optimistic `you` rows (`appendUserMessage`) awaiting their stream copy. Agent-session prompts
   * reconcile by client-generated id; Message Post still reconciles by committed text until its
   * REST send returns the daemon message id.
   */
  pendingEchoes: PendingEcho[];
  /** toolCallId → block index within the open row (for in-place merge). */
  toolBlockIndex: Map<string, number>;
}

/** A fresh reducer state. Pass an `agent` to attribute live rows; `you` labels operator rows. */
export function newConversationState(
  agent?: AguiAgent,
  you?: { who: string; glyph: string },
): AguiConversationState {
  return {
    messages: [],
    agent: {
      who: agent?.who ?? DEFAULT_AGENT.who,
      glyph: agent?.glyph ?? DEFAULT_AGENT.glyph,
      chip: agent?.chip ?? DEFAULT_AGENT.chip,
      presence: agent?.presence,
    },
    you: { who: you?.who ?? "you", glyph: you?.glyph ?? "Y" },
    openRunId: null,
    openIndex: -1,
    openTextMessageId: null,
    ignoredRunId: null,
    openReasoningMessageId: null,
    openUserMessageId: null,
    openUserCommittedMessageId: null,
    openUserName: null,
    openUserKind: null,
    userBuffer: "",
    pendingEchoes: [],
    toolBlockIndex: new Map(),
  };
}

// --- helpers ----------------------------------------------------------------

const field = (ev: BaseEvent, key: string): unknown =>
  (ev as BaseEvent & Record<string, unknown>)[key];
const str = (v: unknown): string => (typeof v === "string" ? v : "");

function positiveMetadataNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return null;
}

function committedMessageIdFromStart(ev: BaseEvent): string | null {
  const messageId = str(field(ev, "messageId"));
  if (!messageId) return null;
  return positiveMetadataNumber(field(ev, "createdAt")) ? messageId : null;
}

/** Map a tool-call status hint to the prototype's short status label. */
function toolStatusLabel(hasResult: boolean, status?: string): string | undefined {
  if (status === "completed") return "ok";
  if (status === "failed" || status === "error") return "error";
  if (status === "in_progress") return undefined;
  return hasResult ? "ok" : undefined;
}

/**
 * Open a new agent row for a run and make it the accumulation target. Reuses the
 * descriptor's identity; the row starts streaming (cleared on RUN_FINISHED).
 */
function openRun(state: AguiConversationState, runId: string): AguiConversationState {
  const row: PaneMessage = {
    id: `agui:${runId}`,
    who: state.agent.who,
    glyph: state.agent.glyph,
    chip: state.agent.chip,
    presence: state.agent.presence,
    streaming: true,
    blocks: [],
  };
  const messages = [...state.messages, row];
  return {
    ...state,
    messages,
    openRunId: runId,
    openIndex: messages.length - 1,
    openTextMessageId: null,
    ignoredRunId: null,
    openReasoningMessageId: null,
    openUserMessageId: null,
    openUserCommittedMessageId: null,
    openUserName: null,
    openUserKind: null,
    userBuffer: "",
    toolBlockIndex: new Map(),
  };
}

function resetOpenRun(state: AguiConversationState, runId: string): AguiConversationState {
  const row = openRow(state);
  if (!row) return openRun(state, runId);
  const messages = state.messages.slice();
  messages[state.openIndex] = {
    ...row,
    id: `agui:${runId}`,
    streaming: true,
    blocks: [],
  };
  return {
    ...state,
    messages,
    openRunId: runId,
    openTextMessageId: null,
    ignoredRunId: null,
    openReasoningMessageId: null,
    openUserMessageId: null,
    openUserCommittedMessageId: null,
    openUserName: null,
    openUserKind: null,
    userBuffer: "",
    toolBlockIndex: new Map(),
  };
}

/** The open run's row, or null when no run is accumulating. */
function openRow(state: AguiConversationState): PaneMessage | null {
  if (state.openIndex < 0) return null;
  return state.messages[state.openIndex] ?? null;
}

function ignoreOpenRun(state: AguiConversationState): AguiConversationState {
  const row = openRow(state);
  const messages = state.messages.slice();
  if (row && row.blocks.length === 0) {
    messages.splice(state.openIndex, 1);
  }
  return {
    ...state,
    messages,
    ignoredRunId: state.openRunId,
    openRunId: null,
    openIndex: -1,
    openTextMessageId: null,
    openReasoningMessageId: null,
    openUserMessageId: null,
    openUserCommittedMessageId: null,
    openUserName: null,
    openUserKind: null,
    userBuffer: "",
    toolBlockIndex: new Map(),
  };
}

/**
 * Append the operator's own message as a closed "you" row (the optimistic echo
 * of a composer send — `observe` only carries agent turns, never the operator's
 * input, so we render it locally). Closes any open run so the agent's reply opens
 * a fresh row after it. `who`/`glyph` come from the operator identity.
 */
type OptimisticUserMessage = {
  id: string;
  who: string;
  glyph: string;
  text: string;
  time?: string;
  reconcileBy?: "id" | "text";
};

function appendOptimisticUserState(
  state: AguiConversationState,
  args: OptimisticUserMessage,
): AguiConversationState {
  const row: PaneMessage = {
    id: args.id,
    who: args.who,
    glyph: args.glyph,
    chip: "you",
    isYou: true,
    time: args.time,
    blocks: [{ b: "p", runs: [{ t: "text", v: args.text }] }],
  };
  return {
    ...state,
    messages: [...state.messages, row],
    // Remember the row so the stream's echo is recognised as the same message and dropped instead
    // of rendering a second bubble.
    pendingEchoes: [
      ...state.pendingEchoes,
      { id: args.id, text: args.text, reconcileBy: args.reconcileBy ?? "text" },
    ],
  };
}

export function appendUserMessage(
  state: AguiConversationState,
  args: OptimisticUserMessage,
): AguiConversationState {
  return {
    ...appendOptimisticUserState(state, args),
    openRunId: null,
    openIndex: -1,
    openTextMessageId: null,
    ignoredRunId: null,
    openReasoningMessageId: null,
    openUserMessageId: null,
    openUserCommittedMessageId: null,
    openUserName: null,
    openUserKind: null,
    userBuffer: "",
    toolBlockIndex: new Map(),
  };
}

/**
 * Insert a closed attributed user row carrying `text`. Placed BEFORE the open agent run (if any)
 * so input stays before its reply. Authenticated self is `you`; a differently named human remains
 * `human`; legacy events without provenance retain the historical self fallback.
 */
function insertUserRow(
  state: AguiConversationState,
  id: string,
  text: string,
  opts: { durableId?: boolean; name?: string; kind?: string } = {},
): AguiConversationState {
  const attributedOther = Boolean(
    opts.name && opts.kind && (opts.kind !== "human" || opts.name !== state.you.who),
  );
  const isYou = !attributedOther;
  const who = isYou ? state.you.who : opts.name!;
  const row: PaneMessage = {
    id: opts.durableId ? id : `agui-user:${id}`,
    who,
    glyph: isYou ? state.you.glyph : who.charAt(0).toUpperCase() || "?",
    chip: isYou ? "you" : opts.kind === "human" ? "human" : "agent",
    ...(isYou ? { isYou: true } : {}),
    blocks: [{ b: "p", runs: [{ t: "text", v: text }] }],
  };
  if (state.openIndex < 0) {
    return { ...state, messages: [...state.messages, row] };
  }
  const messages = state.messages.slice();
  messages.splice(state.openIndex, 0, row);
  return { ...state, messages, openIndex: state.openIndex + 1 };
}

/** Compare committed Message Post echoes until that lane returns a daemon id at send-time. */
function normalizedEchoText(text: string): string {
  return text.trim().replace(/\s+/g, " ");
}

/** Replace the open row's blocks (immutably) and return the new state. */
function withOpenBlocks(
  state: AguiConversationState,
  blocks: Block[],
): AguiConversationState {
  const row = openRow(state);
  if (!row) return state;
  const messages = state.messages.slice();
  messages[state.openIndex] = { ...row, blocks };
  return { ...state, messages };
}

// --- the reducer ------------------------------------------------------------

/**
 * Fold ONE decoded AG-UI event into the conversation. Mirrors egregore-lens's
 * `applyAguiEvent` dispatch, but the sink is the prototype view-model:
 *   - RUN_STARTED                  → open a new agent row (streaming)
 *   - REASONING_MESSAGE_*          → a `thinking` block (deltas concatenated)
 *   - TEXT_MESSAGE_*               → a `p` block of text runs (deltas concatenated)
 *   - TOOL_CALL_START/ARGS/RESULT  → a `toolcall` block merged by toolCallId
 *   - RUN_FINISHED / RUN_ERROR     → stop streaming the open row
 * Unknown events (STATE_DELTA, CUSTOM, …) pass through untouched — same as lens.
 */
export function reduceAguiEvents(
  state: AguiConversationState,
  ev: BaseEvent,
): AguiConversationState {
  if (state.ignoredRunId) {
    if (ev.type === EventType.RUN_FINISHED || ev.type === EventType.RUN_ERROR) {
      return { ...state, ignoredRunId: null };
    }
    return state;
  }

  switch (ev.type) {
    case EventType.RUN_STARTED: {
      const runId = str(field(ev, "runId")) || `run_${state.messages.length}`;
      if (state.openRunId === runId) return resetOpenRun(state, runId);
      if (state.openRunId === null && state.messages.some((m) => m.id === `agui:${runId}`)) {
        return { ...state, ignoredRunId: runId };
      }
      return openRun(state, runId);
    }

    case EventType.RUN_FINISHED:
    case EventType.RUN_ERROR: {
      const row = openRow(state);
      if (!row) return state;
      const messages = state.messages.slice();
      // If the run produced no renderable agent content (e.g. the turn only carried the operator's
      // input, which we divert to a `you` row), drop the empty agent bubble instead of leaving it.
      if (row.blocks.length === 0) {
        messages.splice(state.openIndex, 1);
      } else {
        messages[state.openIndex] = { ...row, streaming: false };
      }
      return {
        ...state,
        messages,
        openRunId: null,
        openIndex: -1,
        openTextMessageId: null,
        ignoredRunId: null,
        openReasoningMessageId: null,
        openUserMessageId: null,
        openUserCommittedMessageId: null,
        openUserName: null,
        openUserKind: null,
        userBuffer: "",
        toolBlockIndex: new Map(),
      };
    }

    case EventType.REASONING_MESSAGE_START: {
      const row = openRow(state);
      if (!row) return state;
      // Begin a fresh thinking block for this reasoning message.
      const messageId = str(field(ev, "messageId"));
      const blocks = [...row.blocks, { b: "thinking", text: "" } as Block];
      return { ...withOpenBlocks(state, blocks), openReasoningMessageId: messageId };
    }

    case EventType.REASONING_MESSAGE_CONTENT: {
      const row = openRow(state);
      if (!row) return state;
      const delta = str(field(ev, "delta"));
      const blocks = row.blocks.slice();
      // Append to the trailing thinking block (open one if a tool interrupted it).
      const last = blocks[blocks.length - 1];
      if (last && last.b === "thinking") {
        blocks[blocks.length - 1] = { b: "thinking", text: last.text + delta };
      } else {
        blocks.push({ b: "thinking", text: delta });
      }
      return withOpenBlocks(state, blocks);
    }

    case EventType.REASONING_MESSAGE_END: {
      return { ...state, openReasoningMessageId: null };
    }

    case EventType.TEXT_MESSAGE_START: {
      const messageId = str(field(ev, "messageId"));
      const committedMessageId = committedMessageIdFromStart(ev);
      // A `role:"user"` message is input echoed by the session stream, never assistant text.
      // Buffer it separately; on END, reconcile authenticated self with an optimistic echo or
      // render the preserved actor as a distinct row. Do NOT fold it into the target's run.
      if (str(field(ev, "role")) === "user") {
        if (
          committedMessageId &&
          state.messages.some((m, index) => index !== state.openIndex && m.id === committedMessageId)
        ) {
          return ignoreOpenRun(state);
        }
        return {
          ...state,
          openUserMessageId: messageId,
          openUserCommittedMessageId: committedMessageId,
          openUserName: str(field(ev, "name")) || null,
          openUserKind: str(field(ev, "kind")) || null,
          userBuffer: "",
        };
      }
      const row = openRow(state);
      if (!row) return state;
      if (
        committedMessageId &&
        state.messages.some((m, index) => index !== state.openIndex && m.id === committedMessageId)
      ) {
        return ignoreOpenRun(state);
      }
      // A new text message starts a fresh `p` block.
      const blocks = [...row.blocks, { b: "p", runs: [] as Inline[] } as Block];
      // Provenance: the committed sender rides on the START event as `name`
      // (Lane A / Message Post). Attribute this row to its real author instead
      // of the single global agent identity, so the UI shows who is speaking.
      const author = str(field(ev, "name"));
      const authorIsHuman = str(field(ev, "kind")) === "human";
      if ((author && author !== row.who) || (authorIsHuman && row.chip !== "human")) {
        const messages = state.messages.slice();
        messages[state.openIndex] = {
          ...row,
          id: committedMessageId ?? row.id,
          who: author || row.who,
          glyph: author ? author.charAt(0).toUpperCase() : row.glyph,
          chip: authorIsHuman ? "human" : row.chip,
          blocks,
        };
        return { ...state, messages, openTextMessageId: messageId };
      }
      const messages = state.messages.slice();
      messages[state.openIndex] = {
        ...row,
        id: committedMessageId ?? row.id,
        blocks,
      };
      return { ...state, messages, openTextMessageId: messageId };
    }

    case EventType.TEXT_MESSAGE_CONTENT: {
      const messageId = str(field(ev, "messageId"));
      const delta = str(field(ev, "delta"));
      // Accumulate the buffered user message rather than the agent row.
      if (state.openUserMessageId && messageId === state.openUserMessageId) {
        return { ...state, userBuffer: state.userBuffer + delta };
      }
      const row = openRow(state);
      if (!row) return state;
      const blocks = row.blocks.slice();
      const last = blocks[blocks.length - 1];
      if (last && last.b === "p") {
        // Concatenate into the trailing text run (or open one).
        const runs = last.runs.slice();
        const lastRun = runs[runs.length - 1];
        if (lastRun && lastRun.t === "text") {
          runs[runs.length - 1] = { t: "text", v: lastRun.v + delta };
        } else {
          runs.push({ t: "text", v: delta });
        }
        blocks[blocks.length - 1] = { b: "p", runs };
      } else {
        blocks.push({ b: "p", runs: [{ t: "text", v: delta }] });
      }
      return withOpenBlocks(state, blocks);
    }

    case EventType.TEXT_MESSAGE_END: {
      const messageId = str(field(ev, "messageId"));
      if (state.openUserMessageId && messageId === state.openUserMessageId) {
        const text = state.userBuffer;
        const normalizedText = normalizedEchoText(text);
        const streamedIsSelf =
          !state.openUserKind ||
          (state.openUserKind === "human" &&
            (!state.openUserName || state.openUserName === state.you.who));
        const echoIdx = streamedIsSelf
          ? state.pendingEchoes.findIndex((pending) => {
              if (pending.reconcileBy === "id") return pending.id === messageId;
              return normalizedEchoText(pending.text) === normalizedText;
            })
          : -1;
        if (echoIdx >= 0) {
          // Dup of our optimistic `you` echo → drop the stream copy, consume the pending entry.
          const pendingEchoes = state.pendingEchoes.slice();
          const pending = pendingEchoes[echoIdx]!;
          pendingEchoes.splice(echoIdx, 1);
          const messages =
            pending.reconcileBy === "text" && messageId
              ? state.messages.map((m) => (m.id === pending.id ? { ...m, id: messageId } : m))
              : state.messages;
          return {
            ...state,
            messages,
            openUserMessageId: null,
            openUserCommittedMessageId: null,
            openUserName: null,
            openUserKind: null,
            userBuffer: "",
            pendingEchoes,
          };
        }
        // No optimistic self echo: render the preserved actor as self, human, or agent.
        const committedMessageId = state.openUserCommittedMessageId;
        return {
          ...insertUserRow(state, committedMessageId ?? messageId, text, {
            durableId: !!committedMessageId,
            name: state.openUserName ?? undefined,
            kind: state.openUserKind ?? undefined,
          }),
          openUserMessageId: null,
          openUserCommittedMessageId: null,
          openUserName: null,
          openUserKind: null,
          userBuffer: "",
        };
      }
      return { ...state, openTextMessageId: null };
    }

    case EventType.TOOL_CALL_START: {
      const row = openRow(state);
      if (!row) return state;
      const toolCallId = str(field(ev, "toolCallId"));
      const name = str(field(ev, "toolCallName")) || "tool";
      if (state.toolBlockIndex.has(toolCallId)) return state; // already open — merge later
      const blocks = [...row.blocks, { b: "toolcall", name } as Block];
      const toolBlockIndex = new Map(state.toolBlockIndex);
      toolBlockIndex.set(toolCallId, blocks.length - 1);
      return { ...withOpenBlocks(state, blocks), toolBlockIndex };
    }

    case EventType.TOOL_CALL_ARGS: {
      // Args are accumulated but not surfaced as a render block (the prototype
      // tool-call card shows name/status/output, not raw args). Kept a no-op on
      // the view-model so the merge-by-id contract holds without UI noise.
      return state;
    }

    case EventType.TOOL_CALL_RESULT: {
      const row = openRow(state);
      if (!row) return state;
      const toolCallId = str(field(ev, "toolCallId"));
      const content = str(field(ev, "content"));
      const append = field(ev, "append") === true;
      const status = str(field(ev, "status"));
      const idx = state.toolBlockIndex.get(toolCallId);
      const blocks = row.blocks.slice();
      if (idx === undefined) {
        // RESULT without a prior START — synthesize the block defensively.
        blocks.push({ b: "toolcall", name: "tool", status: toolStatusLabel(true, status), output: content });
        const toolBlockIndex = new Map(state.toolBlockIndex);
        toolBlockIndex.set(toolCallId, blocks.length - 1);
        return { ...withOpenBlocks(state, blocks), toolBlockIndex };
      }
      const existing = blocks[idx];
      if (existing && existing.b === "toolcall") {
        const output = append && existing.output !== undefined ? existing.output + content : content;
        blocks[idx] = {
          ...existing,
          status: toolStatusLabel(true, status),
          output,
        };
      }
      return withOpenBlocks(state, blocks);
    }

    default:
      // STATE_DELTA / CUSTOM (nexus.plan / nexus.commands) / lifecycle we don't
      // render here — pass through, exactly like the lens reader's default.
      return state;
  }
}

// --- SSE decoding (the wire framing `@ag-ui/encoder` emits) ------------------

/**
 * Parse the AG-UI SSE wire framing (`data: <json>\n\n`, the bytes
 * `@ag-ui/encoder` writes and `@ag-ui/client` reads) into events. The browser
 * `EventSource` already splits frames and hands us each `data` payload via
 * `onmessage`, so this just JSON-parses one payload; exported for the hook + a
 * raw-text fallback. Malformed payloads are skipped (never throw mid-stream).
 */
export function parseAguiData(data: string): BaseEvent | null {
  const json = data.trim();
  if (!json) return null;
  try {
    return JSON.parse(json) as BaseEvent;
  } catch {
    return null;
  }
}

// --- the React hook ---------------------------------------------------------

export interface UseAguiConversationOptions {
  /**
   * The thread to watch (the `?thread=` the `observe` endpoint scopes to). When
   * omitted, the hook stays idle and the caller renders an empty state or its
   * explicitly supplied messages.
   */
  thread?: string;
  /** Identity to attribute live agent rows to (route-supplied). */
  agent?: AguiAgent;
  /**
   * Client-side scope filter. The Phase-3 `observe` endpoint is accept-all (it
   * follows the whole bus), so the hook filters decoded runs to this thread/
   * session here. Defaults to accept-all when unset; pass a predicate to drop
   * other threads' runs until the endpoint scopes server-side.
   */
  accept?: (ev: BaseEvent) => boolean;
  /**
   * Open an AG-UI source for a URL. Defaults to SSE unless `NEXUS_WEB_TRANSPORT=ws`;
   * tests inject a fake. Returning `null` (no browser transport, e.g. SSR) keeps the hook idle.
   */
  openSource?: (url: string) => AguiEventSource | null;
  /**
   * Daemon boot epoch observed by an outer poller. When it changes while the same pane is open,
   * the hook re-subscribes from its last observed cursor instead of waiting for a manual refresh.
   */
  observeEpoch?: string | number | null;
  /** Delay before explicitly reopening `observe` after a transport error. Tests set this to `0`. */
  reconnectDelayMs?: number;
  /**
   * Poll cadence for committed Message Post history while a channel/DM pane is open.
   * This is a read-view fallback for missed/stalled observe frames; agent-session panes do not use
   * it because their source of truth is the session stream, not message history.
   */
  historyPollIntervalMs?: number;
  /**
   * Where the Message Post composer's `send` posts (channels → post, DMs → dm).
   * Agent-session sends leave this unset and provide `sessionName` instead.
   * When both are omitted, `send` is a no-op (read-only pane).
   */
  target?: SendTarget;
  /** Agent-session name for direct session input. Never treated as a Message Post target. */
  sessionName?: string;
  /** Stable session identity used only for the Gateway agent-session stream. */
  sessionStreamId?: string;
  /** The operator identity for the optimistic "you" bubble (defaults to "you"). */
  you?: { who: string; glyph: string };
  /**
   * Explicit send mode — controls which path `send()` takes:
   *   - `"bus"` (default): posts via `POST /api/v1/messages`. Used by channel
   *     (`/c`) and DM (`/dm`) routes — operator DMs go via the daemon bus.
   *   - `"session"`: posts directly to a harness session via
   *     `POST /api/conversation/prompt` (`postPrompt`). Used by `/agent`.
   */
  sendMode?: "bus" | "session";
  /** POST one bus message. Defaults to a real `fetch`; tests inject. */
  postRun?: (target: SendTarget, text: string) => Promise<void>;
  /** Direct harness-session prompt (name, text). Defaults to a real `fetch`; tests inject. */
  postPrompt?: (name: string, text: string, clientMessageId?: string) => Promise<void>;
  /**
   * The conversation key (`dm:<name>` / channel name). When set, the pane reads a bounded
   * canonical daemon backlog on mount and while the live stream is unavailable.
   */
  conversationId?: string;
  /** Load the bounded backlog. Defaults to a real `fetch`; tests inject. */
  loadBacklog?: (
    conversationId: string,
    you?: { who: string; glyph: string },
    cursor?: { after?: number | string; afterRowid?: number; waitMs?: number; signal?: AbortSignal },
  ) => Promise<PaneMessage[]>;
}

const BACKLOG_PAGE_SIZE = 100;
const HISTORY_REFETCH_MS = 2500;

export interface HistoryBacklogRow {
  messageId: string;
  from: string;
  fromKind?: string;
  when?: number;
  body: string;
  cursor?: { createdAt: number; rowid: number; opaque?: string };
}

/** Map canonical Gateway history rows into the same pane rows the live reducer emits. */
export function historyRowsToPaneMessages(
  rows: HistoryBacklogRow[],
  you?: { who: string; glyph: string },
): PaneMessage[] {
  return rows.map((row) => {
    const isYou = !!you && row.from.toLowerCase() === you.who.toLowerCase();
    const who = isYou ? you.who : row.from;
    return {
      id: row.messageId,
      who,
      glyph: isYou ? you.glyph : (who.trim()[0] ?? "?").toUpperCase(),
      chip: isYou ? "you" : row.fromKind === "human" ? "human" : "agent",
      ...(isYou ? { isYou: true } : {}),
      ...(row.cursor ? { messageCursor: row.cursor } : {}),
      blocks: [{ b: "p", runs: [{ t: "text", v: row.body }] }],
    };
  });
}

/** Merge initial history with any live rows that arrived before the history request resolved. */
export function mergeBacklogMessages(backlog: PaneMessage[], live: PaneMessage[]): PaneMessage[] {
  const seen = new Set(backlog.map((m) => m.id));
  return [...backlog, ...live.filter((m) => !seen.has(m.id))];
}

/**
 * Return the DM partner encoded in a web-console conversation id (`dm:<name>` today, with support
 * for the older `dm:<me>:<name>` pair shape). `null` means the id is not a DM cache key.
 */
function dmPartnerFromConversationId(conversationId: string): string | null {
  if (!conversationId.startsWith("dm:")) return null;
  const parts = conversationId.slice(3).split(":").filter(Boolean);
  return parts[parts.length - 1] ?? null;
}

/** Real backlog read: GET one canonical Gateway history page → `PaneMessage[]`. */
async function defaultLoadBacklog(
  conversationId: string,
  you?: { who: string; glyph: string },
  cursor?: { after?: number | string; afterRowid?: number; waitMs?: number; signal?: AbortSignal },
): Promise<PaneMessage[]> {
  const params = new URLSearchParams({ limit: String(BACKLOG_PAGE_SIZE) });
  if (cursor?.after) params.set("after", typeof cursor.after === "number" ? String(Math.floor(cursor.after)) : cursor.after);
  if (cursor?.afterRowid && cursor.afterRowid > 0) {
    params.set("afterRowid", String(Math.floor(cursor.afterRowid)));
  }
  if (cursor?.waitMs && cursor.waitMs > 0) params.set("waitMs", String(Math.min(30_000, Math.floor(cursor.waitMs))));
  const dmPartner = dmPartnerFromConversationId(conversationId);
  if (dmPartner) {
    const history = await gatewayFetch(
      `/api/v1/dms/${encodeURIComponent(dmPartner)}/history?${params.toString()}`,
      { signal: cursor?.signal },
    );
    if (history.ok) {
      const rows = (await history.json()) as HistoryBacklogRow[];
      return historyRowsToPaneMessages(rows, you);
    }
  } else {
    const history = await gatewayFetch(
      `/api/v1/threads/${encodeURIComponent(conversationId)}/history?${params.toString()}`,
      { signal: cursor?.signal },
    );
    if (history.ok) {
      const rows = (await history.json()) as HistoryBacklogRow[];
      return historyRowsToPaneMessages(rows, you);
    }
  }

  return [];
}

export type AguiTransport = "sse" | "ws";
export type AguiInputFrame =
  | {
      /** Stable session-path sockets derive their immutable target server-side. */
      t: "input";
      text: string;
      clientMessageId: string;
    }
  | {
      t: "input";
      mode: "session";
      target: string;
      text: string;
      clientMessageId: string;
    }
  | {
      t: "input";
      mode: "bus";
      target: SendTarget;
      text: string;
      clientMessageId: string;
    };

type AguiControlFrame =
  | { t: "pong" }
  | { t: "input.ack"; clientMessageId?: string; delivered?: boolean }
  | { t: "input.err"; clientMessageId?: string; message?: string; error?: string };

/** The minimal AG-UI source surface the hook uses (so tests can fake it). */
export interface AguiEventSource {
  onmessage: ((ev: { data: string }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  /**
   * Optional bidirectional ingress. Return `false` when no live socket is available so the hook
   * can fall back to the existing POST path. Reject only when the server actively rejects input.
   */
  sendInput?: (frame: AguiInputFrame) => Promise<boolean>;
  close(): void;
}

/** Build the `observe` URL for an agent session (Lane B). */
export function observeSessionUrl(sessionId: string, after?: string): string {
  const params = new URLSearchParams({ view: "agui" });
  if (after) params.set("after", after);
  return `/api/v1/agent-sessions/${encodeURIComponent(sessionId)}/events?${params.toString()}`;
}

/** Compatibility lane for links that predate stable session ids. */
export function observeLegacySessionUrl(name: string, afterId?: number): string {
  const params = new URLSearchParams({ session: name });
  if (afterId && afterId > 0) params.set("afterId", String(afterId));
  return `/api/agui/observe?${params.toString()}`;
}

/** Stable key for the active Message Post target, used to rebind reused pane instances. */
function targetSubscriptionKey(target: SendTarget | undefined): string {
  switch (target?.verb) {
    case "dm":
      return `dm:${target.agentId ?? target.name ?? "unknown"}`;
    case "publish":
      return `publish:${target.topic}`;
    case "post":
      return `post:${target.thread}`;
    case "reply":
      return "reply";
    default:
      return "none";
  }
}

/** POST a bus message; the committed message renders back through `observe`. */
async function defaultPostRun(target: SendTarget, text: string): Promise<void> {
  const res = await gatewayFetch("/api/v1/messages", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "idempotency-key": messagePostIdempotencyKey(target, text),
    },
    body: JSON.stringify({ to: target, body: text, mention: [] }),
  });
  if (!res.ok) throw new Error(`message → HTTP ${res.status}`);
}

function messagePostIdempotencyKey(target: SendTarget, text: string): string {
  const random =
    globalThis.crypto?.randomUUID?.() ??
    `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `web:${targetSubscriptionKey(target)}:${random}:${text.length}`;
}

/**
 * DIRECT operator→agent session send: POST the agent name + text to the daemon's
 * `prompt` (injects into the harness session, NO bus). The reply streams over the WS
 * → `observe` renders it. This is the AionUi `sendMessage` half.
 */
async function defaultPostPrompt(name: string, text: string, clientMessageId?: string): Promise<void> {
  const res = await gatewayFetch("/api/conversation/prompt", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name, text, ...(clientMessageId ? { clientMessageId } : {}) }),
  });
  if (!res.ok) throw new Error(`prompt → HTTP ${res.status}`);
}

export function resolveAguiTransport(raw?: string | null): AguiTransport {
  const value = (raw ?? aguiTransportEnv()).trim().toLowerCase();
  return value === "ws" ? "ws" : "sse";
}

function aguiTransportEnv(): string {
  const metaEnv = (import.meta as unknown as { env?: Record<string, string | undefined> }).env;
  return metaEnv?.NEXUS_WEB_TRANSPORT ?? "";
}

export function observeWebSocketUrl(
  observe: string,
  locationLike: Pick<Location, "protocol" | "host"> | URL = globalThis.location,
): string {
  const base =
    locationLike instanceof URL
      ? locationLike
      : new URL(`${locationLike.protocol}//${locationLike.host}`);
  const url = new URL(observe, base);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = url.pathname.replace(/\/api\/agui\/observe$/, "/api/agui/ws");
  return url.toString();
}

type PendingSocketInput = {
  resolve: (value: boolean) => void;
  reject: (reason?: unknown) => void;
};

const SESSION_BACKPRESSURE_CLOSE_CODE = 1013;
const SESSION_BACKPRESSURE_CLOSE_PREFIX = "session.bp:";
const SESSION_BACKPRESSURE_CURSOR = /^[A-Za-z0-9._~:%+\/=\-]+$/;

type SessionBackpressureResume = { cursor: string | null };

export class AguiWebSocketCloseError extends Error {
  readonly code: number;
  readonly reason: string;
  readonly backpressureResume: SessionBackpressureResume | null;

  constructor(code: number, reason: string) {
    super(reason ? `AG-UI WebSocket closed (${code}): ${reason}` : `AG-UI WebSocket closed (${code})`);
    this.name = "AguiWebSocketCloseError";
    this.code = code;
    this.reason = reason;
    this.backpressureResume = sessionBackpressureResume(code, reason);
  }
}

function sessionBackpressureResume(code: number, reason: string): SessionBackpressureResume | null {
  if (code !== SESSION_BACKPRESSURE_CLOSE_CODE || !reason.startsWith(SESSION_BACKPRESSURE_CLOSE_PREFIX)) {
    return null;
  }
  const cursor = reason.slice(SESSION_BACKPRESSURE_CLOSE_PREFIX.length);
  if (cursor === "none") return { cursor: null };
  if (!cursor || cursor.length > 112 || !SESSION_BACKPRESSURE_CURSOR.test(cursor)) return null;
  return { cursor };
}

function backpressureResumeFromError(error: unknown): SessionBackpressureResume | null {
  return error instanceof AguiWebSocketCloseError ? error.backpressureResume : null;
}

export class AguiWebSocketSource implements AguiEventSource {
  onmessage: ((ev: { data: string }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;

  private readonly socket: WebSocket;
  private readonly pending = new Map<string, PendingSocketInput>();
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private missedPongs = 0;
  private closed = false;

  constructor(observeUrl: string, socketUrl: string = observeWebSocketUrl(observeUrl)) {
    this.socket = new WebSocket(socketUrl, gatewayWebSocketProtocols());
    this.socket.addEventListener("open", () => {
      this.missedPongs = 0;
      this.heartbeatTimer = setInterval(() => this.ping(), 15_000);
    });
    this.socket.addEventListener("message", (event) => this.handleMessage(event.data));
    this.socket.addEventListener("error", (event) => this.reportError(event));
    this.socket.addEventListener("close", (event) => {
      const error = new AguiWebSocketCloseError(event.code, event.reason);
      this.failPending(error);
      if (!this.closed) this.reportError(error);
    });
  }

  async sendInput(frame: AguiInputFrame): Promise<boolean> {
    if (this.closed || this.socket.readyState !== WebSocket.OPEN) return false;
    return new Promise<boolean>((resolve, reject) => {
      this.pending.set(frame.clientMessageId, { resolve, reject });
      try {
        this.socket.send(JSON.stringify(frame));
      } catch (err) {
        this.pending.delete(frame.clientMessageId);
        reject(err);
      }
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.failPending(new Error("AG-UI source closed"));
    this.socket.close();
  }

  private ping(): void {
    if (this.closed) return;
    if (this.socket.readyState !== WebSocket.OPEN) return;
    this.missedPongs += 1;
    if (this.missedPongs > 2) {
      this.closed = true;
      this.socket.close();
      this.reportError(new Error("AG-UI WebSocket heartbeat missed two pongs"));
      return;
    }
    this.socket.send(JSON.stringify({ t: "ping" }));
  }

  private handleMessage(data: unknown): void {
    if (typeof data !== "string") return;
    const control = parseAguiControlFrame(data);
    if (control) {
      this.handleControl(control);
      return;
    }
    this.onmessage?.({ data });
  }

  private handleControl(frame: AguiControlFrame): void {
    if (frame.t === "pong") {
      this.missedPongs = 0;
      return;
    }
    if (!frame.clientMessageId) return;
    const pending = this.pending.get(frame.clientMessageId);
    if (!pending) return;
    this.pending.delete(frame.clientMessageId);
    if (frame.t === "input.ack") {
      pending.resolve(frame.delivered !== false);
    } else {
      pending.reject(new Error(frame.message ?? frame.error ?? "input rejected"));
    }
  }

  private reportError(err: unknown): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.onerror?.(err);
  }

  private failPending(err: Error): void {
    for (const pending of this.pending.values()) pending.reject(err);
    this.pending.clear();
  }
}

function parseAguiControlFrame(data: string): AguiControlFrame | null {
  try {
    const parsed = JSON.parse(data) as unknown;
    if (!parsed || typeof parsed !== "object") return null;
    const record = parsed as Record<string, unknown>;
    if (record.t === "pong") return { t: "pong" };
    if (record.t === "input.ack") {
      return {
        t: "input.ack",
        ...(typeof record.clientMessageId === "string" ? { clientMessageId: record.clientMessageId } : {}),
        ...(typeof record.delivered === "boolean" ? { delivered: record.delivered } : {}),
      };
    }
    if (record.t === "input.err") {
      return {
        t: "input.err",
        ...(typeof record.clientMessageId === "string" ? { clientMessageId: record.clientMessageId } : {}),
        ...(typeof record.message === "string" ? { message: record.message } : {}),
        ...(typeof record.error === "string" ? { error: record.error } : {}),
      };
    }
    return null;
  } catch {
    return null;
  }
}

export function openAguiSource(url: string, transport: AguiTransport = resolveAguiTransport()): AguiEventSource | null {
  url = gatewayUrl(url);
  if (transport === "ws") {
    if (typeof WebSocket === "undefined") return null;
    return new AguiWebSocketSource(url);
  }
  if (typeof EventSource === "undefined") return null;
  return new EventSource(url) as unknown as AguiEventSource;
}

const defaultOpenSource = (url: string): AguiEventSource | null => openAguiSource(url);

function positiveNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return null;
}

/** Pull reconnect cursors out of metadata the server attaches to AG-UI frames. */
export function observeCursorsFromEvent(
  ev: BaseEvent,
): { messageAfter?: number; messageAfterRowid?: number; sessionAfterId?: number; sessionCursor?: string } {
  const record = ev as BaseEvent & Record<string, unknown>;
  const cursor = record.cursor;
  if (typeof cursor === "string" && cursor) return { sessionCursor: cursor };
  if (cursor && typeof cursor === "object") {
    const cursorRecord = cursor as Record<string, unknown>;
    const messageAfter = positiveNumber(cursorRecord.createdAt);
    const messageAfterRowid = positiveNumber(cursorRecord.rowid);
    if (messageAfter && messageAfterRowid) {
      return { messageAfter, messageAfterRowid };
    }
  }
  const messageAfter =
    positiveNumber(record.createdAt) ??
    positiveNumber(record.createdAtMs) ??
    positiveNumber(record.cursor);
  const sessionAfterId =
    positiveNumber(record.streamEventId) ??
    positiveNumber(record.stream_event_id) ??
    positiveNumber(record.afterId);
  return {
    ...(messageAfter ? { messageAfter } : {}),
    ...(sessionAfterId ? { sessionAfterId } : {}),
  };
}

/**
 * Subscribe to a thread's live AG-UI `observe` stream and reduce it into the
 * prototype `PaneMessage[]`. Returns `{ messages, live, send }`: `live` is true
 * once a stream is open; `send(text)` posts to the gateway REST message edge for
 * the configured `target`, optimistically echoing the operator's "you" bubble —
 * the committed bus message streams back through the same `observe` stream.
 */
export function useAguiConversation(
  options: UseAguiConversationOptions = {},
): { messages: PaneMessage[]; live: boolean; send: (text: string) => Promise<void> } {
  const {
    thread,
    agent,
    accept,
    openSource = defaultOpenSource,
    target,
    sessionName,
    sessionStreamId,
    you,
    sendMode = "bus",
    postRun = defaultPostRun,
    postPrompt = defaultPostPrompt,
    conversationId,
    loadBacklog = defaultLoadBacklog,
    observeEpoch,
    reconnectDelayMs = 750,
    historyPollIntervalMs: _historyPollIntervalMs = HISTORY_REFETCH_MS,
  } = options;
  const [messages, setMessages] = useState<PaneMessage[]>([]);
  const [live, setLive] = useState(false);
  // Hold the reducer state across frames without re-subscribing on each event.
  const stateRef = useRef<AguiConversationState>(newConversationState(agent, you));
  // Monotonic id source for optimistic "you" rows (no Date.now in render path).
  const sendSeq = useRef(0);
  const messageAfterRef = useRef(0);
  const messageAfterRowidRef = useRef(0);
  const messageAfterOpaqueRef = useRef("");
  const sessionAfterIdRef = useRef(0);
  const sessionAfterCursorRef = useRef("");
  const sessionPendingCursorRef = useRef("");
  const sessionCursorCheckpointRef = useRef<AguiConversationState | null>(null);
  const sourceRef = useRef<AguiEventSource | null>(null);
  const subscriptionKeyRef = useRef<string | null>(null);
  const observeEpochRef = useRef<string | number | null | undefined>(undefined);
  const targetKey = targetSubscriptionKey(target);

  useEffect(() => {
    if (!thread) {
      setLive(false);
      setMessages([]);
      return;
    }
    const subscriptionKey = JSON.stringify({
      thread,
      conversationId,
      targetKey,
      sessionName,
      sessionStreamId,
      sendMode,
    });
    const newSubscription = subscriptionKeyRef.current !== subscriptionKey;
    subscriptionKeyRef.current = subscriptionKey;
    const epochChanged = observeEpochRef.current !== observeEpoch;
    observeEpochRef.current = observeEpoch;
    const isSessionObserve = sendMode === "session" && !!sessionName;
    if (newSubscription) {
      stateRef.current = newConversationState(agent, you);
      messageAfterRef.current = 0;
      messageAfterRowidRef.current = 0;
      messageAfterOpaqueRef.current = "";
      sessionAfterIdRef.current = 0;
      sessionAfterCursorRef.current = "";
      sessionPendingCursorRef.current = "";
      sessionCursorCheckpointRef.current = null;
      setMessages([]);
    } else if (isSessionObserve && epochChanged) {
      // `streamEventId` belongs to the daemon's tmpfs stream-store file, whose
      // row ids reset on daemon restart. Keep rendered rows, but let the new
      // subscription rehydrate from durable materialized history + a fresh live
      // cursor instead of carrying a stale volatile afterId into the new store.
      sessionAfterIdRef.current = 0;
      sessionAfterCursorRef.current = "";
      sessionPendingCursorRef.current = "";
      sessionCursorCheckpointRef.current = null;
    }
    let cancelled = false;
    let source: AguiEventSource | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let nextReconnectDelayMs = reconnectDelayMs;
    const historyAbort = new AbortController();
    const currentObserveUrl = (): string => sessionStreamId
      ? observeSessionUrl(sessionStreamId, sessionAfterCursorRef.current)
      : observeLegacySessionUrl(sessionName!, sessionAfterIdRef.current);
    let renderFrame: number | undefined;
    let renderTimer: ReturnType<typeof setTimeout> | undefined;
    let pendingMessages: PaneMessage[] | null = null;

    const cancelRenderFlush = (): void => {
      if (renderFrame !== undefined && typeof window !== "undefined") {
        window.cancelAnimationFrame(renderFrame);
      }
      if (renderTimer) clearTimeout(renderTimer);
      renderFrame = undefined;
      renderTimer = undefined;
      pendingMessages = null;
    };
    const flushRender = (): void => {
      if (cancelled) return;
      renderFrame = undefined;
      renderTimer = undefined;
      const rows = pendingMessages;
      pendingMessages = null;
      if (rows) setMessages(rows);
    };
    const scheduleRenderFlush = (): void => {
      if (renderFrame !== undefined || renderTimer !== undefined) return;
      if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
        renderFrame = window.requestAnimationFrame(flushRender);
      } else {
        renderTimer = setTimeout(flushRender, 16);
      }
    };
    const isBurstStreamEvent = (ev: BaseEvent): boolean =>
      ev.type === EventType.TEXT_MESSAGE_CONTENT ||
      ev.type === EventType.REASONING_MESSAGE_CONTENT ||
      ev.type === EventType.TOOL_CALL_ARGS;
    const commitConversationState = (ev: BaseEvent): void => {
      pendingMessages = stateRef.current.messages;
      if (isBurstStreamEvent(ev)) {
        scheduleRenderFlush();
      } else {
        flushRender();
      }
    };

    logEvent("observe", "info", `subscribe thread=${thread}`, { conversationId });

    // Replay canonical daemon history first, THEN open the live stream so new turns append after
    // it. The fallback timer is active only while no live source owns freshness.
    const useBacklog = !!conversationId;
    const sameMessageWindow = (left: PaneMessage[], right: PaneMessage[]): boolean =>
      left.length === right.length &&
      (left.length === 0 || left[left.length - 1]?.id === right[right.length - 1]?.id);
    const refreshBacklog = async (waitMs = 0): Promise<void> => {
      if (!useBacklog) return;
      const cursor = messageAfterOpaqueRef.current || messageAfterRef.current > 0
        ? {
            after: messageAfterOpaqueRef.current || messageAfterRef.current,
            ...(messageAfterRowidRef.current > 0
              ? { afterRowid: messageAfterRowidRef.current }
              : {}),
            ...(waitMs > 0 ? { waitMs } : {}),
            signal: historyAbort.signal,
          }
        : undefined;
      await loadBacklog(conversationId!, you, cursor).then((backlog) => {
        if (cancelled || backlog.length === 0) return;
        for (const m of backlog) {
          const next = m.messageCursor;
          if (!next) continue;
          if (next.opaque) messageAfterOpaqueRef.current = next.opaque;
          if (
            next.createdAt > messageAfterRef.current ||
            (next.createdAt === messageAfterRef.current && next.rowid > messageAfterRowidRef.current)
          ) {
            messageAfterRef.current = next.createdAt;
            messageAfterRowidRef.current = next.rowid;
          }
        }
        const merged = mergeBacklogMessages(backlog, stateRef.current.messages);
        if (sameMessageWindow(merged, stateRef.current.messages)) return;
        stateRef.current = { ...stateRef.current, messages: merged };
        setMessages(merged);
        logEvent("backlog", "info", `replayed ${backlog.length} message(s)`, { conversationId });
      }).catch(() => {
        /* abort/reconnect retries from the last durable cursor */
      });
    };

    const runHistoryLongPoll = async (): Promise<void> => {
      if (!useBacklog || isSessionObserve) return;
      while (!cancelled) {
        await refreshBacklog(messageAfterOpaqueRef.current ? 30_000 : 0);
        if (!messageAfterOpaqueRef.current) await new Promise((resolve) => setTimeout(resolve, 250));
      }
    };

    const connect = (): void => {
      if (cancelled) return;
      source = openSource(currentObserveUrl());
      sourceRef.current = source;
      if (!source) {
        // No browser transport (SSR / unsupported) → stay idle (backlog, if any, stands).
        setLive(false);
        logEvent("observe", "warn", "no AG-UI source (SSR/idle)", { conversationId });
        return;
      }
      setLive(true);

      source.onmessage = (e) => {
        const ev = parseAguiData(e.data);
        if (!ev) return;
        nextReconnectDelayMs = reconnectDelayMs;
        const cursors = observeCursorsFromEvent(ev);
        if (cursors.messageAfter) {
          if (
            cursors.messageAfter > messageAfterRef.current ||
            (cursors.messageAfter === messageAfterRef.current &&
              (cursors.messageAfterRowid ?? 0) > messageAfterRowidRef.current)
          ) {
            messageAfterRef.current = cursors.messageAfter;
            messageAfterRowidRef.current = cursors.messageAfterRowid ?? 0;
          }
        }
        if (cursors.sessionAfterId) {
          sessionAfterIdRef.current = Math.max(sessionAfterIdRef.current, cursors.sessionAfterId);
        }
        if (cursors.sessionCursor) {
          if (cursors.sessionCursor !== sessionPendingCursorRef.current) {
            sessionPendingCursorRef.current = cursors.sessionCursor;
            sessionCursorCheckpointRef.current = stateRef.current;
          }
          sessionAfterCursorRef.current = cursors.sessionCursor;
        }
        if (accept && !accept(ev)) return;
        // Log EVERY streamed chunk — type + the text/reasoning delta when present —
        // so the LOGS drawer shows the realtime stream arriving token-by-token.
        const evt = ev as { type?: string; delta?: string };
        logEvent("stream", "debug", `chunk ${String(evt.type)}`, {
          conversationId,
          data: typeof evt.delta === "string" ? { delta: evt.delta } : undefined,
          persist: false,
        });
        stateRef.current = reduceAguiEvents(stateRef.current, ev);
        // Token-sized deltas render at most once per frame. Boundary events
        // flush immediately so completed turns and test-driven synchronous folds settle.
        commitConversationState(ev);
      };
      source.onerror = (error) => {
        if (cancelled) return;
        const resume = isSessionObserve ? backpressureResumeFromError(error) : null;
        if (resume && sessionCursorCheckpointRef.current) {
          cancelRenderFlush();
          stateRef.current = sessionCursorCheckpointRef.current;
          sessionAfterCursorRef.current = resume.cursor ?? "";
          sessionPendingCursorRef.current = "";
          sessionCursorCheckpointRef.current = null;
          setMessages(stateRef.current.messages);
        } else {
          flushRender();
        }
        logEvent("observe", "warn", "stream error (reconnecting)", { conversationId });
        source?.close();
        if (sourceRef.current === source) sourceRef.current = null;
        source = null;
        setLive(false);
        const delay = Math.min(nextReconnectDelayMs, 10_000);
        nextReconnectDelayMs = Math.min(Math.max(nextReconnectDelayMs * 2, reconnectDelayMs), 10_000);
        reconnectTimer = setTimeout(connect, delay);
      };
    };

    if (isSessionObserve) connect();
    else {
      setLive(true);
      void runHistoryLongPoll();
    }

    return () => {
      cancelled = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      historyAbort.abort();
      cancelRenderFlush();
      source?.close();
      if (sourceRef.current === source) sourceRef.current = null;
    };
    // `agent`/`accept` deps are captured at subscribe time. Re-subscribe when the
    // watched thread OR active target changes so reused panes cannot keep tailing the old target.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [thread, conversationId, targetKey, sessionName, sessionStreamId, sendMode, observeEpoch]);

  const send = useCallback(
    async (text: string): Promise<void> => {
      const body = text.trim();
      if (!body) return;
      // Route send based on the EXPLICIT sendMode, not the target verb:
      //   "session" → POST /api/conversation/prompt (harness session input, /agent view).
      //   "bus" (default) → POST /api/v1/messages (daemon bus, /c and /dm routes).
      const isSession = sendMode === "session";
      if (!isSession && !target) return;
      if (isSession && !sessionName) return;
      logEvent("composer", "info", `send → ${isSession ? "session prompt" : "/api/v1/messages"}`, {
        conversationId,
        data: { chars: body.length, target: target?.verb, sessionName, sendMode },
      });
      // Optimistic "you" echo into the shared reducer (observe never carries the
      // operator's own input). The id is unique across reloads (the sequence
      // resets on mount, so include a timestamp) to avoid colliding with a
      // restored backlog row.
      const id = `you:${Date.now()}:${++sendSeq.current}`;
      const optimisticInput: OptimisticUserMessage = {
        id,
        who: you?.who ?? "you",
        glyph: you?.glyph ?? "Y",
        text: body,
        reconcileBy: isSession ? "id" : "text",
      };
      stateRef.current = appendUserMessage(stateRef.current, optimisticInput);
      // A 1013 resume rolls the stream reducer back to the state immediately before the
      // server's unfinished cursor group. Local input can be accepted after that checkpoint,
      // so mirror the optimistic mutation into the checkpoint; replay then restores only the
      // stream portion while retaining the accepted bubble and its pending echo reconciliation.
      if (sessionCursorCheckpointRef.current) {
        sessionCursorCheckpointRef.current = appendOptimisticUserState(
          sessionCursorCheckpointRef.current,
          optimisticInput,
        );
      }
      setMessages(stateRef.current.messages);
      try {
        const activeSource = sourceRef.current;
        if (activeSource?.sendInput) {
          const delivered = await activeSource.sendInput(
            isSession
              ? sessionStreamId
                ? {
                    t: "input",
                    text: body,
                    clientMessageId: id,
                  }
                : {
                    t: "input",
                    mode: "session",
                    target: sessionName!,
                    text: body,
                    clientMessageId: id,
                  }
              : {
                  t: "input",
                  mode: "bus",
                  target: target!,
                  text: body,
                  clientMessageId: id,
                },
          );
          if (delivered) {
            logEvent("run", "info", `delivered over WebSocket (${isSession ? "session prompt" : "message"})`, {
              conversationId,
            });
            return;
          }
        }
        if (isSession) {
          await postPrompt(sessionName!, body, id); // direct harness session input
        } else {
          await postRun(target!, body);
        }
        logEvent("run", "info", `delivered (${isSession ? "session prompt" : "message"})`, { conversationId });
      } catch (err) {
        logEvent("run", "error", `send failed: ${err instanceof Error ? err.message : String(err)}`, {
          conversationId,
        });
      }
    },
    [
      target,
      sessionName,
      sessionStreamId,
      sendMode,
      you?.who,
      you?.glyph,
      postRun,
      postPrompt,
      conversationId,
    ],
  );

  return { messages, live, send };
}

// --- useAgentSession --------------------------------------------------------

export interface UseAgentSessionOptions {
  /** Mutable display name used only for attribution and the explicit legacy lookup fallback. */
  name: string;
  /** Durable Nexus session id from the route handle. Stable-path sockets bind all writes to it. */
  sessionId?: string;
  /** Identity to attribute live agent rows to. */
  agent?: AguiAgent;
  /** Operator identity for the optimistic "you" bubble. */
  you?: { who: string; glyph: string };
  /**
   * Open an AG-UI source for a URL. Defaults to browser `EventSource` unless
   * `NEXUS_WEB_TRANSPORT=ws`; tests inject a fake. The hook always opens
   * the stable session path when `sessionId` exists, otherwise the explicit
   * compatibility `?session=<name>` route.
   */
  openSource?: (url: string) => AguiEventSource | null;
  /** Direct harness-session prompt (name, text). Defaults to a real `fetch`; tests inject. */
  postPrompt?: (name: string, text: string) => Promise<void>;
  /** Daemon boot epoch observed by the caller; changing it forces a cursor-resume reconnect. */
  observeEpoch?: string | number | null;
  /** Delay before explicitly reopening `observe` after a transport error. Tests set this to `0`. */
  reconnectDelayMs?: number;
}

/**
 * Subscribe to an agent's stable session stream (Lane B) and
 * expose a session-input composer (`POST /api/conversation/prompt`). This is
 * the `/agent/<handle>` route's data hook: it observes `agent.update` activity
 * (NOT `message.created`) and injects operator input directly into the harness
 * session — NOT the bus. The hook is a thin wrapper around `useAguiConversation`
 * with the observe URL bound to `sessionId` whenever one exists. Name-only
 * links use the compatibility route and targeted frame shape. It never
 * fabricates a DM `SendTarget`.
 */
export function useAgentSession(
  options: UseAgentSessionOptions,
): { messages: PaneMessage[]; live: boolean; send: (text: string) => Promise<void> } {
  const { name, sessionId, agent, you, openSource, postPrompt, observeEpoch, reconnectDelayMs } = options;

  return useAguiConversation({
    thread: name,
    agent,
    you,
    // Agent-session input is a direct session prompt, never a Message Post DM target.
    sessionName: name,
    sessionStreamId: sessionId,
    sendMode: "session",
    openSource,
    postPrompt,
    observeEpoch,
    reconnectDelayMs,
  });
}
