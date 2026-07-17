import { watch, type FSWatcher } from "node:fs";
import { stat } from "node:fs/promises";
import { homedir } from "node:os";
import { basename, dirname, join } from "node:path";

import { createClient, type Client, type Row } from "@libsql/client";

import {
  createDaemonPushAgentSessionRelay,
  sharedDaemonPushConnector,
  type DaemonPushConnector,
} from "@server/agui/daemonPushRelay.mjs";
import {
  createMaterializedTurnRelay,
  type TurnCursor,
} from "@server/agui/agentSessionProjection";
import type { RelayFactory, RealtimeRelayOptions } from "@server/agui/relayTypes";
import type { WsEvent } from "@shared/types";

const STREAM_STORE_FILE = "nexus-stream.db";
const DEFAULT_HEARTBEAT_MS = 1500;

type WatchFactory = (
  path: string,
  listener: (eventType: string, filename: string | Buffer | null) => void,
) => FSWatcherLike;

interface FSWatcherLike {
  close(): void;
}

export interface StreamStoreDoorbell {
  subscribe(listener: () => void): () => void;
  close(): void;
}

interface DoorbellDeps {
  watch?: WatchFactory;
  debounceMs?: number;
}

interface StreamStoreRelayDeps {
  storePath?: string;
  client?: Client;
  clientFactory?: (storePath: string) => Client;
  epochFactory?: (storePath: string) => Promise<string | null>;
  doorbell?: StreamStoreDoorbell;
  heartbeatMs?: number;
  /** Backwards-compatible test alias for the heartbeat cadence. */
  pollMs?: number;
  afterId?: number;
  onTurnEnd?: (streamEventId: number) => void;
}

interface StoreBackedRelayDeps {
  storePath?: string;
  doorbell?: StreamStoreDoorbell;
  streamHeartbeatMs?: number;
  streamAfterId?: number;
  fallbackAfterCursor?: TurnCursor;
  fallbackClient?: Client;
  fallbackPollMs?: number;
  daemonPushConnector?: DaemonPushConnector;
}

export interface RawStreamChunk {
  id: number;
  sessionId: string;
  chunkBase64: string;
  encoding: "base64";
}

interface RawStreamDeps {
  storePath?: string;
  doorbell?: StreamStoreDoorbell;
  clientFactory?: (storePath: string) => Client;
  heartbeatMs?: number;
  afterId?: number;
}

interface TailState {
  client?: Client;
  epoch?: string;
  cursor: number;
  openedOnce: boolean;
}

function cleanEnv(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function expandTilde(path: string): string {
  if (path === "~") return homedir();
  if (path.startsWith("~/")) return join(homedir(), path.slice(2));
  return path;
}

function stripFilePrefix(path: string): string {
  if (!path.startsWith("file:")) return path;
  const raw = path.slice("file:".length);
  return raw.startsWith("///") ? `/${raw.slice(3)}` : raw;
}

/** Resolve the gateway-side path to the daemon-owned tmpfs stream store. */
export function resolveStreamStorePath(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = cleanEnv(env.NEXUS_STREAM_DB_PATH);
  if (explicit) return expandTilde(stripFilePrefix(explicit));
  const runtimeDir = cleanEnv(env.XDG_RUNTIME_DIR);
  if (runtimeDir) return join(runtimeDir, STREAM_STORE_FILE);
  return join("/dev/shm", STREAM_STORE_FILE);
}

export async function streamStoreFileExists(storePath = resolveStreamStorePath()): Promise<boolean> {
  try {
    const stats = await stat(storePath);
    return stats.isFile();
  } catch {
    return false;
  }
}

function fileUrl(path: string): string {
  return `file:${path}`;
}

function cell(row: Row, key: string): unknown {
  return (row as Record<string, unknown>)[key];
}

function asNumber(value: unknown): number {
  if (typeof value === "number") return value;
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function asString(value: unknown): string {
  if (typeof value === "string") return value;
  if (value === null || value === undefined) return "";
  return String(value);
}

function parseJson(value: string): unknown {
  if (!value) return {};
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return {};
  }
}

function rawToBase64(value: unknown): string {
  if (value instanceof Uint8Array) return Buffer.from(value).toString("base64");
  if (value instanceof ArrayBuffer) return Buffer.from(value).toString("base64");
  if (ArrayBuffer.isView(value)) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("base64");
  }
  if (Object.prototype.toString.call(value) === "[object ArrayBuffer]") {
    return Buffer.from(value as ArrayBuffer).toString("base64");
  }
  return Buffer.from(asString(value)).toString("base64");
}

function filenameString(filename: string | Buffer | null): string | null {
  if (typeof filename === "string") return filename;
  if (filename instanceof Buffer) return filename.toString();
  return null;
}

function isStreamStoreDoorbellFile(filename: string | null, storePath: string): boolean {
  if (!filename) return true;
  const base = basename(storePath);
  return filename === base || filename === `${base}-wal`;
}

/** A Level-2 directory doorbell: fs.watch on the store directory, debounced. */
export function createStreamStoreDoorbell(
  storePath = resolveStreamStorePath(),
  deps: DoorbellDeps = {},
): StreamStoreDoorbell {
  const listeners = new Set<() => void>();
  const watchFactory = deps.watch ?? ((path, listener) => watch(path, listener));
  const debounceMs = deps.debounceMs ?? 5;
  let closed = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let watcher: FSWatcherLike | undefined;

  const notify = (): void => {
    if (closed) return;
    for (const listener of [...listeners]) listener();
  };
  const schedule = (): void => {
    if (closed) return;
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = undefined;
      notify();
    }, debounceMs);
  };

  try {
    watcher = watchFactory(dirname(storePath), (_event, filename) => {
      if (isStreamStoreDoorbellFile(filenameString(filename), storePath)) schedule();
    });
  } catch {
    watcher = undefined;
  }

  return {
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    close() {
      closed = true;
      listeners.clear();
      if (timer) clearTimeout(timer);
      watcher?.close();
    },
  };
}

const sharedDoorbells = new Map<string, StreamStoreDoorbell>();

function sharedDoorbell(storePath: string): StreamStoreDoorbell {
  const key = dirname(storePath);
  const existing = sharedDoorbells.get(key);
  if (existing) return existing;
  const created = createStreamStoreDoorbell(storePath);
  sharedDoorbells.set(key, created);
  return created;
}

async function storeEpoch(storePath: string): Promise<string | null> {
  try {
    const stats = await stat(storePath);
    if (!stats.isFile()) return null;
    return `${stats.dev}:${stats.ino}`;
  } catch {
    return null;
  }
}

function createReadClient(storePath: string): Client {
  return createClient({ url: fileUrl(storePath) });
}

function releaseTailClient(state: TailState): void {
  state.client?.close();
  state.client = undefined;
}

function resetTailClient(state: TailState): void {
  releaseTailClient(state);
  state.epoch = undefined;
}

async function ensureClient(
  state: TailState,
  storePath: string,
  firstCursor: number,
  clientFactory: (storePath: string) => Client = createReadClient,
  epochFactory: (storePath: string) => Promise<string | null> = storeEpoch,
): Promise<Client | null> {
  const epoch = await epochFactory(storePath);
  if (!epoch) {
    resetTailClient(state);
    return null;
  }
  if (state.epoch !== epoch) {
    releaseTailClient(state);
    state.cursor = state.openedOnce ? 0 : firstCursor;
    state.epoch = epoch;
    state.openedOnce = true;
  }
  state.client ??= clientFactory(storePath);
  return state.client;
}

function streamRowToEvent(row: Row): { event: WsEvent; id: number; kind: string } {
  const id = asNumber(cell(row, "id"));
  const sessionId = asString(cell(row, "session_id"));
  const kind = asString(cell(row, "kind"));
  const data = parseJson(asString(cell(row, "data")));
  // Stamp the store row id into the payload so downstream mapping (and the live
  // run-id derivation in agentSession) can key on the same id the materializer uses.
  if (data && typeof data === "object" && !Array.isArray(data)) {
    (data as Record<string, unknown>).streamEventId = id;
  }
  return {
    id,
    kind,
    event: { type: "agent.update", sessionId, kind, data } as WsEvent,
  };
}

/**
 * Tail lane 1 (`stream_events`) from the named stream store for one session.
 * The gateway only reads the store; the daemon remains the sole stream writer.
 */
export function createStreamStoreRelay(
  sessionId: string,
  deps: StreamStoreRelayDeps = {},
): RelayFactory {
  const storePath = deps.storePath ?? resolveStreamStorePath();
  const doorbell = deps.doorbell ?? sharedDoorbell(storePath);
  const heartbeatMs = deps.heartbeatMs ?? deps.pollMs ?? DEFAULT_HEARTBEAT_MS;
  const afterId = deps.afterId ?? 0;

  return ({ onEvent, onError = () => {} }: RealtimeRelayOptions) => {
    const state: TailState = { cursor: afterId, openedOnce: false };
    let closed = false;
    let draining = false;
    let queued = false;
    let paused = false;

    const drainOnce = async (): Promise<void> => {
      const client = deps.client
        ?? (await ensureClient(
          state,
          storePath,
          afterId,
          deps.clientFactory,
          deps.epochFactory,
        ));
      if (!client) return;
      try {
        const rows = await client.execute({
          sql:
            "SELECT id, session_id, kind, data FROM stream_events " +
            "WHERE session_id = ? AND id > ? ORDER BY id ASC",
          args: [sessionId, state.cursor],
        });
        for (const row of rows.rows) {
          if (closed || paused) return;
          const mapped = streamRowToEvent(row);
          if (onEvent(mapped.event) === false) {
            paused = true;
            return;
          }
          state.cursor = mapped.id;
          if (mapped.kind === "turn_end") deps.onTurnEnd?.(mapped.id);
        }
      } finally {
        if (!deps.client) releaseTailClient(state);
      }
    };

    const drain = async (): Promise<void> => {
      if (closed || paused) return;
      if (draining) {
        queued = true;
        return;
      }
      draining = true;
      try {
        do {
          queued = false;
          await drainOnce();
        } while (queued && !closed);
      } catch (err) {
        onError(err);
      } finally {
        draining = false;
      }
    };

    const wake = (): void => {
      void drain();
    };
    const unsubscribe = doorbell.subscribe(wake);
    const timer = setInterval(wake, heartbeatMs);
    const ready = drain();

    return {
      ready,
      pause() {
        paused = true;
      },
      resume() {
        if (closed || !paused) return;
        paused = false;
        void drain();
      },
      close() {
        if (closed) return;
        closed = true;
        unsubscribe();
        clearInterval(timer);
        if (!deps.client) resetTailClient(state);
      },
    };
  };
}

/**
 * Prefer live lane-1 streaming and keep the materialized turn relay as fallback-only.
 * A streamed `turn_end` suppresses the finalized materialized turn with the same stream id range.
 */
export function createStoreBackedAgentSessionRelay(
  sessionId: string,
  deps: StoreBackedRelayDeps = {},
): RelayFactory {
  let streamedDoneThroughId = 0;
  const streamRelay = createStreamStoreRelay(sessionId, {
    storePath: deps.storePath,
    doorbell: deps.doorbell,
    heartbeatMs: deps.streamHeartbeatMs,
    afterId: deps.streamAfterId,
  });
  const daemonPushConnector = deps.daemonPushConnector ?? sharedDaemonPushConnector;
  const daemonPushRelay = daemonPushConnector
    ? createDaemonPushAgentSessionRelay(sessionId, {
        connector: daemonPushConnector,
        afterId: deps.streamAfterId,
      })
    : undefined;
  let fallbackCursor = deps.fallbackAfterCursor ?? { finalizedAt: 0, rowid: 0 };
  const createFallbackRelay = (): RelayFactory =>
    createMaterializedTurnRelay(sessionId, {
      client: deps.fallbackClient,
      pollMs: deps.fallbackPollMs ?? DEFAULT_HEARTBEAT_MS,
      afterCursor: fallbackCursor,
      shouldSkipTurn: (turn) =>
        turn.lastStreamEventId > 0 && turn.lastStreamEventId <= streamedDoneThroughId,
      onTurnEmitted: (cursor) => {
        fallbackCursor = cursor;
      },
    });

  return (opts) => {
    let closed = false;
    let silenceTimer: ReturnType<typeof setTimeout> | undefined;
    let fallback: ReturnType<RelayFactory> | undefined;
    let storeRepair: ReturnType<RelayFactory> | undefined;

    const stopFallback = (): void => {
      fallback?.close();
      fallback = undefined;
    };
    const stopStoreRepair = (): void => {
      storeRepair?.close();
      storeRepair = undefined;
    };
    const startFallback = (): void => {
      if (closed || fallback) return;
      fallback = createFallbackRelay()(opts);
      void fallback.ready;
    };
    const armSilenceFallback = (): void => {
      if (closed) return;
      if (silenceTimer) clearTimeout(silenceTimer);
      silenceTimer = setTimeout(startFallback, deps.fallbackPollMs ?? DEFAULT_HEARTBEAT_MS);
    };
    const handleLiveEvent = (event: WsEvent): boolean => {
      // Both live transports stamp the durable stream row id into `data`. Advance the completed
      // cursor here, after the daemon-push/store choice, so the materialized silence fallback
      // cannot replay a turn that already closed over either live lane.
      const accepted = opts.onEvent(event);
      if (accepted === false) return false;
      if (event.type === "agent.update" && event.kind === "turn_end") {
        const data = event.data as Record<string, unknown> | undefined;
        const rawId = data?.streamEventId ?? data?.stream_event_id;
        const id = typeof rawId === "number" ? rawId : Number(rawId);
        if (Number.isFinite(id) && id > 0) {
          streamedDoneThroughId = Math.max(streamedDoneThroughId, id);
        }
      }
      stopFallback();
      armSilenceFallback();
      return true;
    };
    const startStoreRepair = (): void => {
      if (closed || storeRepair) return;
      storeRepair = streamRelay({
        ...opts,
        onEvent: handleLiveEvent,
      });
      void storeRepair.ready;
    };

    let stream: ReturnType<RelayFactory> | undefined;
    const streamOpts = {
      ...opts,
      onEvent(event: WsEvent) {
        stopStoreRepair();
        return handleLiveEvent(event);
      },
      onGap() {
        startStoreRepair();
      },
    };
    stream = daemonPushRelay ? daemonPushRelay(streamOpts) : streamRelay(streamOpts);
    armSilenceFallback();

    return {
      ready: stream.ready,
      pause() {
        stream?.pause?.();
        storeRepair?.pause?.();
        fallback?.pause?.();
      },
      resume() {
        stream?.resume?.();
        storeRepair?.resume?.();
        fallback?.resume?.();
      },
      close() {
        closed = true;
        if (silenceTimer) clearTimeout(silenceTimer);
        stream?.close();
        stopStoreRepair();
        stopFallback();
      },
    };
  };
}

function rawRowToChunk(row: Row): RawStreamChunk {
  return {
    id: asNumber(cell(row, "id")),
    sessionId: asString(cell(row, "session_id")),
    chunkBase64: rawToBase64(cell(row, "chunk")),
    encoding: "base64",
  };
}

function encodeRawSse(chunk: RawStreamChunk): string {
  return `id: ${chunk.id}\nevent: raw\ndata: ${JSON.stringify(chunk)}\n\n`;
}

/** Raw terminal-output lane as SSE, for terminal-style consumers. */
export function observeRawStream(
  sessionId: string,
  deps: RawStreamDeps = {},
): ReadableStream<Uint8Array> {
  const storePath = deps.storePath ?? resolveStreamStorePath();
  const doorbell = deps.doorbell ?? sharedDoorbell(storePath);
  const heartbeatMs = deps.heartbeatMs ?? DEFAULT_HEARTBEAT_MS;
  const afterId = deps.afterId ?? 0;
  const textEncoder = new TextEncoder();
  let cleanup = (): void => {};

  return new ReadableStream<Uint8Array>({
    start(controller) {
      const state: TailState = { cursor: afterId, openedOnce: false };
      let closed = false;
      let draining = false;
      let queued = false;

      const drainOnce = async (): Promise<void> => {
        const client = await ensureClient(state, storePath, afterId, deps.clientFactory);
        if (!client) return;
        try {
          const rows = await client.execute({
            sql:
              "SELECT id, session_id, chunk FROM stream_raw " +
              "WHERE session_id = ? AND id > ? ORDER BY id ASC",
            args: [sessionId, state.cursor],
          });
          for (const row of rows.rows) {
            if (closed) return;
            const chunk = rawRowToChunk(row);
            controller.enqueue(textEncoder.encode(encodeRawSse(chunk)));
            state.cursor = chunk.id;
          }
        } finally {
          releaseTailClient(state);
        }
      };

      const drain = async (): Promise<void> => {
        if (closed) return;
        if (draining) {
          queued = true;
          return;
        }
        draining = true;
        try {
          do {
            queued = false;
            await drainOnce();
          } while (queued && !closed);
        } catch (err) {
          controller.enqueue(
            textEncoder.encode(`event: error\ndata: ${JSON.stringify(String(err))}\n\n`),
          );
        } finally {
          draining = false;
        }
      };

      const wake = (): void => {
        void drain();
      };
      const unsubscribe = doorbell.subscribe(wake);
      const timer = setInterval(wake, heartbeatMs);
      cleanup = () => {
        closed = true;
        unsubscribe();
        clearInterval(timer);
        resetTailClient(state);
      };
      void drain();
    },
    cancel() {
      cleanup();
    },
  });
}
