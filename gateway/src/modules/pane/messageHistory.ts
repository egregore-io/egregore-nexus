import { useCallback, useEffect, useRef, useState } from "react";

import { gatewayFetch } from "@app/gatewayClient";
import type { Ack, SendTarget } from "@shared/types";

import type { PaneMessage } from "./types";

export interface GatewayHistoryRow {
  messageId: string;
  from: string;
  fromKind?: string;
  when?: number;
  body: string;
  cursor?: { createdAt: number; rowid: number; opaque?: string };
}

export interface GatewayHistoryCursor {
  after: string;
  waitMs: number;
  signal?: AbortSignal;
}

export type GatewayHistoryLoader = (
  conversationId: string,
  cursor?: GatewayHistoryCursor,
) => Promise<GatewayHistoryRow[]>;

export type GatewayMessagePoster = (
  target: SendTarget,
  text: string,
  clientMessageId: string,
) => Promise<Ack>;

export interface UseGatewayMessageHistoryOptions {
  conversationId: string;
  target: SendTarget;
  you?: { who: string; glyph: string };
  loadHistory?: GatewayHistoryLoader;
  postMessage?: GatewayMessagePoster;
}

const PAGE_SIZE = 100;
const LONG_POLL_MS = 30_000;
const RETRY_MS = 1_000;

/** The sole browser history edge for ordinary thread and DM conversations. */
export function historyPath(
  conversationId: string,
  cursor?: { after?: string; waitMs?: number },
): string {
  const params = new URLSearchParams({ limit: String(PAGE_SIZE) });
  if (cursor?.after) params.set("after", cursor.after);
  if (cursor?.waitMs) params.set("waitMs", String(Math.min(LONG_POLL_MS, cursor.waitMs)));
  const dm = conversationId.startsWith("dm:") ? conversationId.slice(3) : undefined;
  const resource = dm
    ? `/api/v1/dms/${encodeURIComponent(dm)}/history`
    : `/api/v1/threads/${encodeURIComponent(conversationId)}/history`;
  return `${resource}?${params.toString()}`;
}

export function historyRowsToPaneMessages(
  rows: GatewayHistoryRow[],
  you?: { who: string; glyph: string },
): PaneMessage[] {
  return rows.map((row) => {
    const isYou = !!you && row.from === you.who;
    return {
      id: row.messageId,
      who: row.from,
      glyph: isYou ? you.glyph : row.from.charAt(0).toUpperCase() || "?",
      chip: isYou ? "you" : row.fromKind === "human" ? "human" : "agent",
      ...(isYou ? { isYou: true } : {}),
      time: row.when ? new Date(row.when).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : undefined,
      ...(row.cursor ? { messageCursor: row.cursor } : {}),
      blocks: [{ b: "p", runs: [{ t: "text", v: row.body }] }],
    } satisfies PaneMessage;
  });
}

export function mergeHistoryMessages(
  canonical: PaneMessage[],
  current: PaneMessage[],
): PaneMessage[] {
  const canonicalIds = new Set(canonical.map((row) => row.id));
  return [...current.filter((row) => !canonicalIds.has(row.id)), ...canonical];
}

async function defaultLoadHistory(
  conversationId: string,
  cursor?: GatewayHistoryCursor,
): Promise<GatewayHistoryRow[]> {
  const response = await gatewayFetch(historyPath(conversationId, cursor), {
    signal: cursor?.signal,
  });
  if (!response.ok) throw new Error(`Gateway history failed (${response.status})`);
  return response.json() as Promise<GatewayHistoryRow[]>;
}

async function defaultPostMessage(
  target: SendTarget,
  text: string,
  clientMessageId: string,
): Promise<Ack> {
  const response = await gatewayFetch("/api/v1/messages", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "idempotency-key": clientMessageId,
    },
    body: JSON.stringify({ to: target, body: text, mention: [], idempotencyKey: clientMessageId }),
  });
  if (!response.ok) throw new Error(`Gateway message send failed (${response.status})`);
  return response.json() as Promise<Ack>;
}

function latestCursor(rows: GatewayHistoryRow[], fallback: string): string {
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    const cursor = rows[index]?.cursor?.opaque;
    if (cursor) return cursor;
  }
  return fallback;
}

function abortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

/**
 * Ordinary messages use one bounded latest read followed by cancellable cursor long-polls.
 * There is deliberately no WebSocket/EventSource input in this hook.
 */
export function useGatewayMessageHistory(
  options: UseGatewayMessageHistoryOptions,
): { messages: PaneMessage[]; live: boolean; send: (text: string) => Promise<void> } {
  const {
    conversationId,
    target,
    you,
    loadHistory = defaultLoadHistory,
    postMessage = defaultPostMessage,
  } = options;
  const [messages, setMessages] = useState<PaneMessage[]>([]);
  const [live, setLive] = useState(false);
  const rowsRef = useRef<PaneMessage[]>([]);
  const sendSequence = useRef(0);

  useEffect(() => {
    const lifecycle = new AbortController();
    let request: AbortController | undefined;
    let cursor = "";
    rowsRef.current = [];
    setMessages([]);
    setLive(false);

    const apply = (rows: GatewayHistoryRow[]) => {
      cursor = latestCursor(rows, cursor || "origin");
      if (rows.length === 0) return;
      const next = mergeHistoryMessages(historyRowsToPaneMessages(rows, you), rowsRef.current);
      rowsRef.current = next;
      setMessages(next);
    };

    const loop = async () => {
      let hydrated = false;
      while (!lifecycle.signal.aborted) {
        request = new AbortController();
        const abort = () => request?.abort();
        lifecycle.signal.addEventListener("abort", abort, { once: true });
        try {
          try {
            const rows = hydrated
              ? await loadHistory(conversationId, {
                  after: cursor || "origin",
                  waitMs: LONG_POLL_MS,
                  signal: request.signal,
                })
              : await loadHistory(conversationId);
            apply(rows);
            hydrated = true;
            setLive(true);
          } catch (error) {
            if (lifecycle.signal.aborted) return;
            if (!abortError(error)) {
              setLive(false);
              await new Promise((resolve) => setTimeout(resolve, RETRY_MS));
            }
          } finally {
            lifecycle.signal.removeEventListener("abort", abort);
          }
        } finally {
          if (request?.signal.aborted) request = undefined;
        }
      }
    };

    const refreshNow = () => request?.abort();
    window.addEventListener("focus", refreshNow);
    window.addEventListener("online", refreshNow);
    void loop();

    return () => {
      lifecycle.abort();
      request?.abort();
      window.removeEventListener("focus", refreshNow);
      window.removeEventListener("online", refreshNow);
    };
  }, [conversationId, loadHistory, you?.who, you?.glyph]);

  const send = useCallback(async (text: string) => {
    const body = text.trim();
    if (!body) return;
    const clientMessageId = `web:${Date.now()}:${++sendSequence.current}`;
    const optimistic: PaneMessage = {
      id: clientMessageId,
      who: you?.who ?? "you",
      glyph: you?.glyph ?? "Y",
      chip: "you",
      isYou: true,
      blocks: [{ b: "p", runs: [{ t: "text", v: body }] }],
    };
    rowsRef.current = [...rowsRef.current, optimistic];
    setMessages(rowsRef.current);
    const ack = await postMessage(target, body, clientMessageId);
    rowsRef.current = rowsRef.current.map((row) => row.id === clientMessageId
      ? { ...row, id: ack.messageId }
      : row);
    setMessages(rowsRef.current);
  }, [postMessage, target, you?.glyph, you?.who]);

  return { messages, live, send };
}
