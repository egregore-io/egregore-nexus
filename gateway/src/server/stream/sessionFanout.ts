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
  onFrame(frame: SessionFanoutFrame): boolean | void;
  onError?(error: unknown): void;
}

export interface SessionFanoutSubscription {
  ready: Promise<void>;
  close(): void;
}

export interface SessionUpstream {
  ready: Promise<void>;
  close(): void;
}

export type SessionUpstreamFactory = (
  sessionId: string,
  handlers: { onFrame(frame: SessionFanoutFrame): void; onError(error: unknown): void },
) => SessionUpstream;

interface SessionState {
  upstream: SessionUpstream;
  ring: SessionFanoutFrame[];
  subscribers: Set<FanoutSubscriber>;
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
    state.subscribers.add(subscriber);
    replay(state, subscriber);
    return {
      ready: state.upstream.ready,
      close: () => {
        state.subscribers.delete(subscriber);
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
      subscribers: new Set<FanoutSubscriber>(),
    };
    state.upstream = this.upstream(sessionId, {
      onFrame: (frame) => {
        state.ring.push(frame);
        if (state.ring.length > this.ringLimit) state.ring.splice(0, state.ring.length - this.ringLimit);
        for (const subscriber of [...state.subscribers]) {
          if (!visibleTo(subscriber.view, frame)) continue;
          try {
            if (subscriber.onFrame(frame) === false) state.subscribers.delete(subscriber);
          } catch (error) {
            state.subscribers.delete(subscriber);
            subscriber.onError?.(error);
          }
        }
      },
      onError: (error) => {
        for (const subscriber of state.subscribers) subscriber.onError?.(error);
      },
    });
    this.sessions.set(sessionId, state);
    return state;
  }
}

export const sessionFanout = new SessionFanoutHub();

function replay(state: SessionState, subscriber: FanoutSubscriber): void {
  const cursor = decodeCursor(subscriber.after);
  if (subscriber.after && !cursor) {
    subscriber.onFrame({ lane: "gap", epoch: "current", id: 0, cursor: encodeCursor("current", 0), reason: "stale_cursor" });
    return;
  }
  const visible = state.ring.filter((frame) => visibleTo(subscriber.view, frame));
  if (cursor && visible.length > 0 && visible[0]!.epoch !== cursor.epoch) {
    subscriber.onFrame({ lane: "gap", epoch: visible.at(-1)!.epoch, id: 0, cursor: encodeCursor(visible.at(-1)!.epoch, 0), reason: "epoch_changed" });
    return;
  }
  for (const frame of visible) {
    if (!cursor || frame.id > cursor.id) subscriber.onFrame(frame);
  }
}

function visibleTo(view: SessionStreamView, frame: SessionFanoutFrame): boolean {
  if (frame.lane === "gap") return true;
  return view === "terminal" ? frame.lane === "terminal" : frame.lane === "agent";
}

export function encodeCursor(epoch: string, id: number): string {
  return Buffer.from(JSON.stringify({ v: 1, epoch, id })).toString("base64url");
}

function decodeCursor(value: string | undefined): { epoch: string; id: number } | null {
  if (!value) return null;
  try {
    const parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as Record<string, unknown>;
    return parsed.v === 1 && typeof parsed.epoch === "string" && Number.isInteger(parsed.id)
      ? { epoch: parsed.epoch, id: Number(parsed.id) }
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
  const epoch = "daemon-current";
  const unsubAgent = connection.subscribe({ lane: "agent", sessionId, afterId: 0 }, {
    onFrame: (raw) => {
      const frame = raw as Record<string, unknown>;
      if (frame.t === "gap") {
        handlers.onFrame({ lane: "gap", epoch, id: 0, cursor: encodeCursor(epoch, 0), reason: "daemon_gap" });
        return;
      }
      if (frame.t !== "agent.update") return;
      const id = Number(frame.streamEventId);
      const data = frame.data && typeof frame.data === "object" ? { ...(frame.data as Record<string, unknown>), streamEventId: id } : { streamEventId: id };
      handlers.onFrame({
        lane: "agent", epoch, id, cursor: encodeCursor(epoch, id),
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
      handlers.onFrame({
        lane: "terminal", epoch, id, cursor: encodeCursor(epoch, id),
        chunkBase64: String(frame.chunkBase64), encoding: "base64",
      });
    },
    onError: handlers.onError,
  });
  return { ready: connection.ready, close() { unsubAgent(); unsubRaw(); } };
}
