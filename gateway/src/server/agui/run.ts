// the AG-UI run orchestrator (the streaming half of the
// gateway's AG-UI surface). Pure translation + lifecycle wiring. Two entry
// points produce AG-UI SSE byte streams that `@ag-ui/client` decodes verbatim
// (the same stream egregore-lens reads):
//
//   run(input, target, deps)  — DRIVE a turn: emit RUN_STARTED, send the
//     in-mapped message through the Message Post write seam, then render the
//     committed Message Post row from the store read view before RUN_FINISHED.
//     Any error → RUN_ERROR.
//   observe(target, deps)     — WATCH the bus read view: NO send, one
//     RUN_STARTED/FINISHED pair per committed message.
//
// `deps` are injectable so the whole path is unit-testable with a recording
// send function + a hand-driven read-view source (see run.test.ts).
import { EventEncoder } from "@ag-ui/encoder";
import type { BaseEvent, RunAgentInput } from "@ag-ui/client";
import { runInputToSend } from "@server/agui/map";
import { messageToAguiEvents } from "@server/agui/mapMessage";
import type { SendTarget } from "@shared/types";
import { COMMAND_KINDS, submitCommandIntent } from "@server/command/ingress";
import {
  buildSseStream,
  runStarted,
  runFinished,
  runError,
  nextRunId,
  targetThreadId,
} from "@server/agui/_sseCore";
import type { SseEncoder, RelayFactory, RunDeps } from "@server/agui/_sseCore";
import { observeMessagePost } from "@server/agui/messagePost";
import {
  createReadViewMessagePostSource,
  type MessagePostSource,
} from "@server/messagePost/readView";

// The SSE byte encoder lives in `@ag-ui/encoder` (`EventEncoder.encodeSSE`); the
// `EventType` enum + event TS shapes come from `@ag-ui/client` (one source — the
// same package egregore-lens decodes with). Re-export `RunAgentInput` for callers.
export type { RunAgentInput };

// Re-export the shared surface so existing callers of run.ts don't need updating.
export type { SseEncoder, RelayFactory, RunDeps };
export { buildSseStream, runStarted, runFinished, nextRunId, targetThreadId };

// --- run: drive one turn ----------------------------------------------------

/**
 * DRIVE a turn as an AG-UI run over SSE. Emits RUN_STARTED, sends the in-mapped
 * user message through `deps.sendMessage` (or command-ingress), then tails the
 * store read view for the committed Message Post row. The returned stream ends
 * after the first committed row is rendered.
 */
export function run(
  input: RunAgentInput,
  target: SendTarget,
  deps: RunDeps = {},
): ReadableStream<Uint8Array> {
  const sendMessage =
    deps.sendMessage ??
    ((req) => submitCommandIntent(COMMAND_KINDS.messagePostSend, req));
  const encoder = deps.encoder ?? new EventEncoder();
  const createMessageSource =
    deps.createMessageSource ?? createReadViewMessagePostSource;
  const selfName = deps.selfName;

  const threadId = input.threadId || targetThreadId(target);
  const runId = input.runId || nextRunId();
  const textEncoder = new TextEncoder();
  let source: MessagePostSource | null = null;
  let finished = false;

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
        const message = err instanceof Error ? err.message : String(err);
        emit([runError(message)]);
        source?.close();
        controller.close();
      };
      const finish = (): void => {
        if (finished) return;
        finished = true;
        source?.close();
        controller.close();
      };

      emit([runStarted(threadId, runId)]);

      try {
        source = createMessageSource({
          target,
          selfName,
          project: deps.project,
          pollIntervalMs: deps.pollIntervalMs,
          onMessage: (msg) => {
            if (finished) return true;
            if (!hasCapacity() && source?.pause && source.resume) return false;
            const body = messageToAguiEvents(msg, selfName);
            emit([...body, runFinished(threadId, runId)]);
            finish();
            return true;
          },
          onError: fail,
        });
        await source.ready;
        if (finished) return;
        await sendMessage(runInputToSend(input, target));
      } catch (err) {
        fail(err);
      }
    },
    pull() {
      source?.resume?.();
    },
    cancel() {
      finished = true;
      source?.close();
    },
  });
}

// --- observe: watch a thread's runs -----------------------------------------

/**
 * WATCH a bus target's runs as an AG-UI stream — read-only, never self-ends.
 * Renders committed `message.created` events (NOT `agent.update`s): this is
 * Lane A (Message Post). The consumer closes the stream (cancel) when it stops
 * watching.
 *
 * Routing:
 *   post / dm / publish  → observeMessagePost (Lane A: committed message.created only).
 *                          `/dm` is Lane A — operator DMs go via the bus, not ACP direct.
 *   (agent session)       → observeAgentSession (Lane B) is reachable ONLY via
 *                          handleAgentSession (?session=), NOT via this function.
 */
export function observe(
  target: SendTarget,
  deps: RunDeps = {},
): ReadableStream<Uint8Array> {
  // All bus targets (post / dm / publish) use Lane A: committed message.created.
  // `observeAgentSession` (Lane B) is reached via handleAgentSession (?session=), not here.
  return observeMessagePost(target, deps);
}
