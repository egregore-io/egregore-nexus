// Lane B: Agent Session pipeline.
//
// `observeAgentSession(sessionName, deps)` renders the captured session stream from
// the durable store projection (`agent.update` → AG-UI via `acpToAguiEvents`).
// It is the non-post arm used by `/agent`, extracted here so Lane B is
// structurally isolated from Lane A. It NEVER reads or renders
// `message.created` — that is Lane A's domain.
//
// Turn-end: the `agent.update` `turn_end` marker ONLY (not `message.created`).
// A `message.created` event is silently ignored — neither rendered nor used as
// a turn-end signal. `user_input` DOES render here — it is either direct operator
// input or the daemon's session-visible projection of a bus/steer delivery.
import { EventEncoder } from "@ag-ui/encoder";
import type { BaseEvent } from "@ag-ui/client";
import type { WsEvent } from "@shared/types";
import {
  acpToAguiEvents,
  closeRun,
  newBracket,
  type AgentUpdateEvent,
  type AguiBracket,
} from "@server/agui/mapAgentUpdate";
import {
  buildSseStream,
  runStarted,
  runFinished,
  runError,
  nextRunId,
  agentSessionThreadId,
} from "@server/agui/_sseCore";
import type { RunDeps, SseEncoder } from "@server/agui/_sseCore";

const acceptAll = (_ev: WsEvent): boolean => true;

/** Lane B turn-end: the `agent.update` `turn_end` marker ONLY. */
const agentUpdateTurnEnd = (ev: WsEvent): boolean =>
  ev.type === "agent.update" && ev.kind === "turn_end";

const isAgentUpdate = (ev: WsEvent): ev is AgentUpdateEvent =>
  ev.type === "agent.update";

/** A renderable agent update (everything except the pure `turn_end` lifecycle marker). */
const isRenderableUpdate = (ev: WsEvent): ev is AgentUpdateEvent =>
  isAgentUpdate(ev) && ev.kind !== "turn_end";

/**
 * Derive the SAME turn id the daemon materializer will assign this turn
 * (`turn_<sessionId>_<firstStreamEventId>`), so the live run and its later
 * history replay share one runId and replay is idempotent for id-keyed
 * consumers. Falls back to an ephemeral id when the relay didn't stamp a
 * stream event id (e.g. the materialized-turn fallback relay).
 */
const deriveTurnRunId = (ev: AgentUpdateEvent): string | null => {
  const data = ev.data as Record<string, unknown> | undefined;
  const raw = data?.streamEventId ?? data?.stream_event_id;
  const id = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isFinite(id) || !ev.sessionId) return null;
  return `turn_${ev.sessionId}_${id}`;
};

function errorStream(encoder: SseEncoder, message: string): ReadableStream<Uint8Array> {
  const textEncoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      const event: BaseEvent = runError(message);
      controller.enqueue(textEncoder.encode(encoder.encodeSSE(event)));
      controller.close();
    },
  });
}

/**
 * WATCH a target's agent session stream as an AG-UI run stream — Lane B only.
 *
 * Emits compact materialized history first when supplied by the route, then maps
 * live `agent.update` events to AG-UI frames, wrapping EACH observed turn in its
 * own RUN_STARTED…RUN_FINISHED with a fresh bracket. The live run is opened
 * lazily on the first renderable update (a `turn_end` marker with no prior
 * content does NOT emit an empty RUN_STARTED).
 *
 * Raw `message.created` events are silently ignored — Lane B never acts on Lane A
 * signals directly. When bus/steer input should be visible in the session transcript, the daemon
 * mirrors it as `agent.update:user_input` before harness injection.
 */
export function observeAgentSession(
  sessionName: string,
  deps: RunDeps = {},
): ReadableStream<Uint8Array> {
  const encoder = deps.encoder ?? new EventEncoder();
  const createRelay = deps.createRelay;
  if (!createRelay) {
    return errorStream(
      encoder,
      "agent session observe requires a store-backed session relay",
    );
  }
  const matchEvent = deps.matchEvent ?? acceptAll;
  const isTurnEnd = deps.isTurnEnd ?? agentUpdateTurnEnd;
  const threadId = agentSessionThreadId(sessionName);
  const initialEvents = deps.initialEvents ?? [];

  let bracket: AguiBracket = newBracket();
  let runOpen = false;
  let runId = "";

  return buildSseStream({
    encoder,
    createRelay,
    initial: initialEvents,
    tolerateRelayError: true,
    heartbeatIntervalMs: deps.heartbeatIntervalMs,
    onEvent: (ev, emit) => {
      // Lane B purity: message.created is NEVER acted upon here.
      if (ev.type === "message.created") return false;

      if (!matchEvent(ev)) return false;

      // Open the run lazily on the first RENDERABLE update; the `turn_end` marker must NOT open a
      // run (it only closes one), else an end with no prior content emits an empty RUN_STARTED.
      if (isRenderableUpdate(ev)) {
        if (!runOpen) {
          runOpen = true;
          runId = deriveTurnRunId(ev) ?? nextRunId();
          bracket = newBracket();
          emit([runStarted(threadId, runId)]);
        }
        const out = acpToAguiEvents(ev, bracket);
        bracket = out.bracket;
        emit(out.events);
      }
      if (isTurnEnd(ev) && runOpen) {
        emit([...closeRun(bracket), runFinished(threadId, runId)]);
        runOpen = false; // the next turn opens a fresh run.
      }
      return false; // observe follows the target forever; never END here.
    },
  });
}
