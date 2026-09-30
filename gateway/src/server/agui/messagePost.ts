// Lane A: Message Post pipeline.
//
// `observeMessagePost(target, deps)` renders committed Message Post rows for ANY
// bus target (post / dm / publish) — never emits an `agent.update`-derived frame.
// It is extracted from the `verb === "post"` branch of `observe()` in run.ts and
// serves as the single Lane A implementation; `observe()` delegates to it.
import { EventEncoder } from "@ag-ui/encoder";
import type { BaseEvent } from "@ag-ui/client";
import type { SendTarget, Message } from "@shared/types";
import { messageToAguiEvents } from "@server/agui/mapMessage";
import {
  createReadViewMessagePostSource,
  type MessagePostSource,
} from "@server/messagePost/readView";
import {
  runStarted,
  runFinished,
  nextRunId,
  targetThreadId,
  runError,
} from "@server/agui/_sseCore";
import type { RunDeps } from "@server/agui/_sseCore";

const OPEN_COMMENT = ": nexus-message-post-observe-open\n\n";

/**
 * WATCH any bus target for committed messages — Lane A only.
 *
 * Renders each committed row as a complete AG-UI run
 * (RUN_STARTED → TEXT_MESSAGE_START/CONTENT/END → RUN_FINISHED), in commit
 * order. `agent.update` events are not part of this source. The stream never
 * self-ends; the consumer cancels when done.
 */
export function observeMessagePost(
  target: SendTarget,
  deps: RunDeps = {},
): ReadableStream<Uint8Array> {
  const encoder = deps.encoder ?? new EventEncoder();
  const createMessageSource =
    deps.createMessageSource ?? createReadViewMessagePostSource;
  const selfName = deps.selfName;
  const threadId = targetThreadId(target);
  const textEncoder = new TextEncoder();
  let source: MessagePostSource | null = null;
  let finished = false;

  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(textEncoder.encode(OPEN_COMMENT));

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

      const render = (msg: Message): boolean => {
        if (finished) return true;
        if (!hasCapacity() && source?.pause && source.resume) return false;
        try {
          const body = messageToAguiEvents(msg, selfName);
          if (body.length === 0) return true;
          const runId = nextRunId();
          emit([runStarted(threadId, runId), ...body, runFinished(threadId, runId)]);
          if (!hasCapacity()) source?.pause?.();
        } catch {
          // mapping error → skip this message, keep the watch alive.
        }
        return true;
      };

      try {
        source = createMessageSource({
          target,
          selfName,
          project: deps.project,
          after: deps.messageAfter,
          afterRowid: deps.messageAfterRowid,
          pollIntervalMs: deps.pollIntervalMs,
          onHeartbeat: () => {
            if (!hasCapacity()) return false;
            controller.enqueue(textEncoder.encode(": ping\n\n"));
            if (!hasCapacity()) source?.pause?.();
            return true;
          },
          onMessage: render,
          // A transient read-view miss should not tear down the browser SSE; the
          // source retries on its own polling cadence.
          onError: () => {},
        });
        source.ready.catch(fail);
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
