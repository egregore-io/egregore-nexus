import type { WsEvent } from "@shared/types";
import {
  sharedDaemonPushConnector,
  type DaemonPushConnection,
} from "../agui/daemonPushRelay.mjs";

export type SessionStreamView = "nexus" | "agui" | "terminal";

export type SessionFanoutFrame =
  | { lane: "agent"; epoch: string; id: number; cursor: string; event: WsEvent }
  | { lane: "terminal"; epoch: string; id: number; cursor: string; chunkBase64: string; encoding: "base64" }
  | { lane: "gap"; epoch: string; id: number; cursor: string; reason: string };

export interface FanoutSubscriber {
  view: SessionStreamView;
  after?: string;
  /** Pre-opaque compatibility cursor, translated only within the current daemon boot. */
  legacyAfterId?: string;
  onFrame(frame: SessionFanoutFrame): boolean | void;
  onError?(error: unknown): void;
}

export interface SessionFanoutSubscription {
  ready: Promise<void>;
  close(): void;
}

export interface SessionUpstream {
  ready: Promise<void>;
  /** Current daemon boot observed from the authenticated push handshake. */
  daemonBootId?(): string | undefined;
  close(): void;
}

export type SessionUpstreamFactory = (
  sessionId: string,
  handlers: { onFrame(frame: SessionFanoutFrame): void; onError(error: unknown): void },
) => SessionUpstream;

interface SessionState {
  upstream: SessionUpstream;
  ring: SessionFanoutFrame[];
  subscribers: Set<SubscriberState>;
  droppedThrough: Map<string, number>;
  daemonBootId?: string;
}

export interface SessionCursor {
  daemonBootId: string;
  id: number;
}

interface SubscriberState {
  subscriber: FanoutSubscriber;
  cursor?: SessionCursor;
  initialized: boolean;
  closed: boolean;
}

/** One bounded, process-wide semantic fanout per agent session. */
export class SessionFanoutHub {
  private readonly sessions = new Map<string, SessionState>();

  constructor(
    private readonly upstream: SessionUpstreamFactory = daemonSessionUpstream,
    private readonly ringLimit = 512,
  ) {}

  subscribe(sessionId: string, subscriber: FanoutSubscriber): SessionFanoutSubscription {
    const state = this.sessions.get(sessionId) ?? this.openSession(sessionId);
    const tracked: SubscriberState = {
      subscriber,
      initialized: false,
      closed: false,
    };
    state.subscribers.add(tracked);
    const ready = state.upstream.ready.then(() => {
      if (tracked.closed) return;
      state.daemonBootId = state.upstream.daemonBootId?.() ?? state.daemonBootId;
      replay(state, tracked);
      tracked.initialized = true;
    }).catch((error) => {
      subscriber.onError?.(error);
      throw error;
    });
    return {
      ready,
      close: () => {
        tracked.closed = true;
        state.subscribers.delete(tracked);
        if (state.subscribers.size === 0) {
          state.upstream.close();
          this.sessions.delete(sessionId);
        }
      },
    };
  }

  upstreamCount(): number {
    return this.sessions.size;
  }

  close(): void {
    for (const state of this.sessions.values()) state.upstream.close();
    this.sessions.clear();
  }

  private openSession(sessionId: string): SessionState {
    const state: SessionState = {
      upstream: undefined as unknown as SessionUpstream,
      ring: [],
      subscribers: new Set<SubscriberState>(),
      droppedThrough: new Map<string, number>(),
    };
    state.upstream = this.upstream(sessionId, {
      onFrame: (frame) => {
        if (state.daemonBootId && state.daemonBootId !== frame.epoch) {
          state.droppedThrough.clear();
        }
        state.daemonBootId = frame.epoch;
        state.ring.push(frame);
        if (state.ring.length > this.ringLimit) {
          const dropped = state.ring.splice(0, state.ring.length - this.ringLimit);
          for (const old of dropped) recordDropped(state, old);
        }
        for (const tracked of [...state.subscribers]) {
          if (!tracked.initialized || tracked.closed) continue;
          deliverLive(state, tracked, frame);
        }
      },
      onError: (error) => {
        for (const tracked of state.subscribers) tracked.subscriber.onError?.(error);
      },
    });
    this.sessions.set(sessionId, state);
    return state;
  }
}

export const sessionFanout = new SessionFanoutHub();

function replay(state: SessionState, tracked: SubscriberState): void {
  const { subscriber } = tracked;
  const boot = currentBootId(state);
  const requested = requestedCursor(subscriber, boot);
  if (requested === "malformed") {
    emitGap(state, tracked, boot, "malformed_cursor");
    return;
  }
  if (requested && requested.daemonBootId !== boot) {
    emitGap(state, tracked, boot, "boot_mismatch");
    return;
  }
  const visible = currentVisibleFrames(state, subscriber.view, boot);
  if (requested && visible.length === 0) {
    // The daemon sends `ready` before its durable catch-up rows. Keep filtering at the caller's
    // accepted boundary while making the transient empty-ring ambiguity explicit to the client.
    emitGap(state, tracked, boot, "empty_ring", requested.id);
    return;
  }
  if (requested) {
    const droppedThrough = state.droppedThrough.get(floorKey(subscriber.view, boot)) ?? 0;
    if (requested.id < droppedThrough) {
      emitGap(state, tracked, boot, "stale_cursor");
      return;
    }
    const latest = visible.at(-1)!.id;
    if (requested.id > latest) {
      emitGap(state, tracked, boot, "ahead_cursor");
      return;
    }
    tracked.cursor = requested;
  }
  for (const frame of visible) deliverFrame(state, tracked, frame);
}

function visibleTo(view: SessionStreamView, frame: SessionFanoutFrame): boolean {
  if (frame.lane === "gap") return true;
  return view === "terminal" ? frame.lane === "terminal" : frame.lane === "agent";
}

function currentBootId(state: SessionState): string {
  return state.upstream.daemonBootId?.()
    ?? state.daemonBootId
    ?? state.ring.at(-1)?.epoch
    ?? "unavailable";
}

function requestedCursor(
  subscriber: FanoutSubscriber,
  daemonBootId: string,
): SessionCursor | "malformed" | undefined {
  if (subscriber.after !== undefined && subscriber.legacyAfterId !== undefined) {
    return "malformed";
  }
  if (subscriber.after !== undefined) {
    return decodeCursor(subscriber.after) ?? "malformed";
  }
  if (subscriber.legacyAfterId === undefined) return undefined;
  if (!/^(0|[1-9]\d*)$/.test(subscriber.legacyAfterId)) return "malformed";
  const id = Number(subscriber.legacyAfterId);
  return Number.isSafeInteger(id) ? { daemonBootId, id } : "malformed";
}

function currentVisibleFrames(
  state: SessionState,
  view: SessionStreamView,
  daemonBootId: string,
): SessionFanoutFrame[] {
  return state.ring.filter(
    (frame) => frame.epoch === daemonBootId && visibleTo(view, frame),
  );
}

function floorKey(view: SessionStreamView, daemonBootId: string): string {
  return `${daemonBootId}:${view === "terminal" ? "terminal" : "agent"}`;
}

function recordDropped(state: SessionState, frame: SessionFanoutFrame): void {
  if (frame.lane === "gap") return;
  const view: SessionStreamView = frame.lane === "terminal" ? "terminal" : "nexus";
  const key = floorKey(view, frame.epoch);
  state.droppedThrough.set(key, Math.max(state.droppedThrough.get(key) ?? 0, frame.id));
}

function latestVisibleId(
  state: SessionState,
  view: SessionStreamView,
  daemonBootId: string,
): number {
  return currentVisibleFrames(state, view, daemonBootId)
    .reduce((latest, frame) => Math.max(latest, frame.id), 0);
}

function emitGap(
  state: SessionState,
  tracked: SubscriberState,
  daemonBootId: string,
  reason: string,
  resumeId?: number,
): void {
  const id = resumeId ?? latestVisibleId(state, tracked.subscriber.view, daemonBootId);
  const gap: SessionFanoutFrame = {
    lane: "gap",
    epoch: daemonBootId,
    id,
    cursor: encodeCursor(daemonBootId, id),
    reason,
  };
  if (deliverToSubscriber(state, tracked, gap)) {
    tracked.cursor = { daemonBootId, id };
  }
}

function deliverLive(
  state: SessionState,
  tracked: SubscriberState,
  frame: SessionFanoutFrame,
): void {
  if (!visibleTo(tracked.subscriber.view, frame)) return;
  if (tracked.cursor && tracked.cursor.daemonBootId !== frame.epoch) {
    emitGap(state, tracked, frame.epoch, "boot_mismatch");
    return;
  }
  deliverFrame(state, tracked, frame);
}

function deliverFrame(
  state: SessionState,
  tracked: SubscriberState,
  frame: SessionFanoutFrame,
): void {
  if (!visibleTo(tracked.subscriber.view, frame)) return;
  if (frame.lane !== "gap" && tracked.cursor) {
    if (tracked.cursor.daemonBootId !== frame.epoch) {
      emitGap(state, tracked, frame.epoch, "boot_mismatch");
      return;
    }
    if (frame.id <= tracked.cursor.id) return;
  }
  if (deliverToSubscriber(state, tracked, frame)) {
    tracked.cursor = { daemonBootId: frame.epoch, id: frame.id };
  }
}

function deliverToSubscriber(
  state: SessionState,
  tracked: SubscriberState,
  frame: SessionFanoutFrame,
): boolean {
  try {
    if (tracked.subscriber.onFrame(frame) !== false) return true;
  } catch (error) {
    tracked.subscriber.onError?.(error);
  }
  tracked.closed = true;
  state.subscribers.delete(tracked);
  return false;
}

export function encodeCursor(epoch: string, id: number): string {
  return Buffer.from(JSON.stringify({ v: 1, daemonBootId: epoch, id })).toString("base64url");
}

export function decodeCursor(value: string | undefined): SessionCursor | null {
  if (!value) return null;
  try {
    const parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as Record<string, unknown>;
    return parsed.v === 1
      && typeof parsed.daemonBootId === "string"
      && parsed.daemonBootId.length > 0
      && Number.isSafeInteger(parsed.id)
      && Number(parsed.id) >= 0
      ? { daemonBootId: parsed.daemonBootId, id: Number(parsed.id) }
      : null;
  } catch {
    return null;
  }
}

function daemonSessionUpstream(
  sessionId: string,
  handlers: { onFrame(frame: SessionFanoutFrame): void; onError(error: unknown): void },
): SessionUpstream {
  const connection = sharedDaemonPushConnector() as DaemonPushConnection | undefined;
  if (!connection) return { ready: Promise.resolve(), close() {} };
  const epoch = () => connection.daemonBootId || "unavailable";
  const unsubAgent = connection.subscribe({ lane: "agent", sessionId, afterId: 0 }, {
    onFrame: (raw) => {
      const frame = raw as Record<string, unknown>;
      if (frame.t === "gap") {
        const boot = epoch();
        const id = Number.isSafeInteger(frame.afterId) && Number(frame.afterId) >= 0
          ? Number(frame.afterId)
          : 0;
        handlers.onFrame({
          lane: "gap",
          epoch: boot,
          id,
          cursor: encodeCursor(boot, id),
          reason: "daemon_gap",
        });
        return;
      }
      if (frame.t !== "agent.update") return;
      const id = Number(frame.streamEventId);
      const boot = epoch();
      const data = frame.data && typeof frame.data === "object" ? { ...(frame.data as Record<string, unknown>), streamEventId: id } : { streamEventId: id };
      handlers.onFrame({
        lane: "agent", epoch: boot, id, cursor: encodeCursor(boot, id),
        event: { type: "agent.update", sessionId, kind: String(frame.kind), data } as WsEvent,
      });
    },
    onError: handlers.onError,
  });
  const unsubRaw = connection.subscribe({ lane: "raw", sessionId, afterId: 0 }, {
    onFrame: (raw) => {
      const frame = raw as Record<string, unknown>;
      if (frame.t !== "raw") return;
      const id = Number(frame.streamRawId);
      const boot = epoch();
      handlers.onFrame({
        lane: "terminal", epoch: boot, id, cursor: encodeCursor(boot, id),
        chunkBase64: String(frame.chunkBase64), encoding: "base64",
      });
    },
    onError: handlers.onError,
  });
  return {
    ready: connection.ready,
    daemonBootId: epoch,
    close() { unsubAgent(); unsubRaw(); },
  };
}
