// Shared AG-UI SSE streaming helpers — no imports from other agui/ files.
// Consumed by run.ts and messagePost.ts; kept here to avoid circular deps.
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";
import type {
  RealtimeRelay,
  RealtimeRelayOptions,
  RelayFactory,
} from "@server/agui/relayTypes";
import type { SendRequest, SendTarget, WsEvent } from "@shared/types";
import type { MessagePostSourceFactory } from "@server/messagePost/readView";

/** The minimal encoder surface we use — `EventEncoder` from `@ag-ui/encoder`. */
export interface SseEncoder {
  /** Frame one event as an SSE record (`data: <json>\n\n`). */
  encodeSSE(event: BaseEvent): string;
}

export type { RealtimeRelay, RealtimeRelayOptions, RelayFactory };

/**
 * Dependencies for the AG-UI orchestrators. Message Post uses command ingress
 * plus a store read-view source. Agent Session uses an explicit
 * store-projection relay supplied by the route after resolving the session id.
 */
export interface RunDeps {
  /**
   * Message Post write seam. Production gateway routes use the local
   * command-intent ingress here.
   * Tests can inject a recorder; production route adapters pass the resolved
   * Principal explicitly before command ingress writes.
   */
  sendMessage?: (req: SendRequest) => Promise<unknown>;
  /** Build a session-scoped event relay. Agent Session routes must supply this explicitly. */
  createRelay?: RelayFactory;
  /**
   * Lane A committed-message source. Production uses the read-view poller so
   * `?thread=`/`?dm=`/`?topic=` observe reads committed store rows.
   */
  createMessageSource?: MessagePostSourceFactory;
  /** Project scope for read-view Message Post observe. */
  project?: string;
  /**
   * Message Post observe cursor. `messageAfterRowid`, when present, disambiguates rows committed
   * in the same millisecond as `messageAfter`.
   */
  messageAfter?: number;
  messageAfterRowid?: number;
  /** Poll interval for read-view Message Post observe. */
  pollIntervalMs?: number;
  /** SSE comment heartbeat cadence for idle Agent Session streams. */
  heartbeatIntervalMs?: number;
  /** SSE encoder. Defaults to a fresh `EventEncoder`. */
  encoder?: SseEncoder;
  /**
   * AG-UI frames emitted before the live relay starts. Agent Session uses this for compact,
   * materialized history so reconnects do not replay the full raw stream log.
   */
  initialEvents?: BaseEvent[];
  /**
   * Filter: does this session event belong to the watched run/thread? Defaults
   * to "accept all" because Agent Session relays are already scoped to a single
   * materialized session stream.
   */
  matchEvent?: (ev: WsEvent) => boolean;
  /**
   * Turn-end signal for Agent Session. Message Post paths use committed
   * messages directly and ignore this hook.
   */
  isTurnEnd?: (ev: WsEvent) => boolean;
  /** The watcher's own name; their posts render with role `user`. */
  selfName?: string;
}

// --- run-lifecycle event constructors (widened to BaseEvent) ----------------

export const runStarted = (threadId: string, runId: string): BaseEvent =>
  ({ type: EventType.RUN_STARTED, threadId, runId }) as BaseEvent;
export const runFinished = (threadId: string, runId: string): BaseEvent =>
  ({ type: EventType.RUN_FINISHED, threadId, runId }) as BaseEvent;
export const runError = (message: string): BaseEvent =>
  ({ type: EventType.RUN_ERROR, message }) as BaseEvent;

/** Derive a stable thread id label from the send target (for RUN_* ids). */
export function targetThreadId(target: SendTarget): string {
  switch (target.verb) {
    case "post":
      return `post:${target.thread}`;
    case "dm":
      return `dm:${target.agentId ?? target.name ?? "unknown"}`;
    case "publish":
      return `publish:${target.topic}`;
    case "reply":
      return "reply";
    default: {
      const _never: never = target;
      void _never;
      return "thread";
    }
  }
}

/** Derive the AG-UI thread id label for an Agent Session stream. */
export function agentSessionThreadId(sessionName: string): string {
  return `session:${sessionName}`;
}

let runSeq = 0;
export const nextRunId = (): string => `run_${Date.now()}_${++runSeq}`;

// --- shared streaming core --------------------------------------------------

/**
 * Build the `ReadableStream<Uint8Array>` body. `onStart` runs once the relay is
 * subscribed (it does the injected send for `run`, nothing for `observe`).
 * `onEvent` maps each relay frame to AG-UI events (enqueued as encoded SSE) and
 * may signal the stream is complete by returning `true` (used by `run` at
 * turn-end). Errors anywhere → a RUN_ERROR frame, then close.
 */
export function buildSseStream(args: {
  encoder: SseEncoder;
  createRelay: RelayFactory;
  /** First frames to emit synchronously (e.g. RUN_STARTED for `run`). */
  initial: BaseEvent[];
  /** Async work after the relay is ready (e.g. the `send` call). May throw. */
  onStart?: () => Promise<void>;
  /** Map one relay frame → events to emit; return true to END the stream. */
  onEvent: (ev: WsEvent, emit: (events: BaseEvent[]) => void) => boolean;
  /**
   * When true, a relay transport error does NOT fail/close the SSE — the relay reconnects
   * underneath and events resume on the SAME stream. Used by `observe` (a long-lived watch that
   * must survive daemon restarts / blips); `run` leaves it false so a dropped single turn surfaces
   * as RUN_ERROR.
   */
  tolerateRelayError?: boolean;
  heartbeatIntervalMs?: number;
}): ReadableStream<Uint8Array> {
  const {
    encoder,
    createRelay,
    initial,
    onStart,
    onEvent,
    tolerateRelayError = false,
    heartbeatIntervalMs = 20_000,
  } = args;
  const textEncoder = new TextEncoder();
  let relay: RealtimeRelay | null = null;
  let finished = false;
  let heartbeatTimer: ReturnType<typeof setInterval> | undefined;

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      const hasCapacity = (): boolean =>
        controller.desiredSize === null || controller.desiredSize > 0;
      const emit = (events: BaseEvent[]): void => {
        for (const ev of events) {
          controller.enqueue(textEncoder.encode(encoder.encodeSSE(ev)));
        }
      };
      const fail = (err: unknown): void => {
        if (finished) return;
        finished = true;
        if (heartbeatTimer) clearInterval(heartbeatTimer);
        const message = err instanceof Error ? err.message : String(err);
        emit([runError(message)]);
        relay?.close();
        controller.close();
      };
      const end = (): void => {
        if (finished) return;
        finished = true;
        if (heartbeatTimer) clearInterval(heartbeatTimer);
        relay?.close();
        controller.close();
      };

      // Frames known before any I/O (e.g. RUN_STARTED).
      emit(initial);

      // Subscribe the relay first so no turn-start frames are missed between the
      // `send` and the first event. The orchestrator owns the one connection.
      relay = createRelay({
        onEvent: (ev) => {
          if (finished) return true;
          if (!hasCapacity() && relay?.pause && relay.resume) return false;
          try {
            if (onEvent(ev, emit)) end();
            if (!hasCapacity()) relay?.pause?.();
            return true;
          } catch (err) {
            fail(err);
            return true;
          }
        },
        onError: (err) => {
          // For a long-lived observe, a transport error is transient: the relay reconnects on
          // its own, so keep the SSE open and let events resume. For a single run, fail it.
          if (tolerateRelayError) return;
          fail(err);
        },
      });

      heartbeatTimer = setInterval(() => {
        if (finished) return;
        if (!hasCapacity()) {
          relay?.pause?.();
          return;
        }
        controller.enqueue(textEncoder.encode(": ping\n\n"));
        if (!hasCapacity()) relay?.pause?.();
      }, Math.max(1, heartbeatIntervalMs));

      try {
        await relay.ready;
        if (finished) return;
        if (onStart) await onStart();
      } catch (err) {
        fail(err);
      }
    },
    pull() {
      relay?.resume?.();
    },
    cancel() {
      // Consumer went away (closed tab / aborted fetch) — release the relay.
      finished = true;
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      relay?.close();
    },
  });
}
