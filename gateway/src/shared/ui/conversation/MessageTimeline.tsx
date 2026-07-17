// MessageTimeline — the shared conversation primitive (threads + DMs reuse it).
//
// Renders messages oldest→newest (newest at the bottom), grouped into runs by
// the same author, with a provenance chip per group (the web console duality) and
// per-message `pending`/`failed` affordances. The list is a polite live-region
// (`role="log"` + `aria-live="polite"`) so screen readers announce new lines
// without stealing focus. All motion is `motion-reduce`-safe.
//
// NOTE: this is the non-virtualized baseline. Task 14 layers `@tanstack/react-
// virtual` over the same grouping for long histories; the public props here are
// the stable contract.
import { useMemo } from "react";
import type { ReactNode } from "react";

import type { MessageVM } from "@shared/types";

import { cn } from "../cn";
import { Avatar } from "../components/Avatar";
import { ProvenanceChip } from "./ProvenanceChip";

export interface MessageTimelineProps {
  messages: MessageVM[];
  /** Resolve a custom avatar for a sender name; falls back to initials. */
  avatarFor?: (name: string) => ReactNode;
  /** Retry handler for a failed optimistic message. */
  onRetry?: (message: MessageVM) => void;
  /** Rendered when there are no messages yet. */
  emptyState?: ReactNode;
  className?: string;
  "aria-label"?: string;
}

interface Group {
  key: string;
  from: string;
  messages: MessageVM[];
}

/** Collapse consecutive same-author messages into render groups. */
function groupByAuthor(messages: MessageVM[]): Group[] {
  const groups: Group[] = [];
  for (const m of messages) {
    const last = groups[groups.length - 1];
    if (last && last.from === m.from) {
      last.messages.push(m);
    } else {
      groups.push({ key: m.id || m.tempId || `${m.from}-${m.createdAt}`, from: m.from, messages: [m] });
    }
  }
  return groups;
}

function formatTime(ms: number): string {
  if (!ms) return "";
  try {
    return new Date(ms).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return "";
  }
}

export function MessageTimeline({
  messages,
  avatarFor,
  onRetry,
  emptyState,
  className,
  "aria-label": ariaLabel = "Conversation",
}: MessageTimelineProps) {
  const groups = useMemo(() => groupByAuthor(messages), [messages]);

  return (
    <div
      role="log"
      aria-live="polite"
      aria-relevant="additions text"
      aria-label={ariaLabel}
      className={cn(
        "flex w-full flex-col gap-5 px-4 py-4",
        "mx-auto max-w-chat",
        className,
      )}
    >
      {groups.length === 0
        ? emptyState ?? <TimelineEmpty />
        : groups.map((group) => {
            const head = group.messages[0]!;
            return (
              <article key={group.key} className="flex gap-3" data-message-group>
                <div className="shrink-0 pt-0.5">
                  {avatarFor ? (
                    avatarFor(group.from)
                  ) : (
                    <Avatar
                      name={group.from}
                      kind={head.provenance.kind}
                      size="md"
                    />
                  )}
                </div>
                <div className="min-w-0 flex-1">
                  <header className="mb-1 flex items-center gap-2">
                    <ProvenanceChip provenance={head.provenance} />
                    <time
                      className="text-[10px] tabular-nums text-text-muted/70"
                      dateTime={head.createdAt ? new Date(head.createdAt).toISOString() : undefined}
                    >
                      {formatTime(head.createdAt)}
                    </time>
                  </header>
                  <div className="flex flex-col gap-1">
                    {group.messages.map((m) => (
                      <MessageLine key={m.id || m.tempId} message={m} onRetry={onRetry} />
                    ))}
                  </div>
                </div>
              </article>
            );
          })}
    </div>
  );
}

function MessageLine({
  message,
  onRetry,
}: {
  message: MessageVM;
  onRetry?: (m: MessageVM) => void;
}) {
  const { pending, failed, body } = message;
  return (
    <div
      data-message-id={message.id || message.tempId}
      data-pending={pending ? "" : undefined}
      data-failed={failed ? "" : undefined}
      className={cn(
        "group/msg whitespace-pre-wrap break-words text-[15px] leading-[1.5] text-[color:var(--lens-text-normal)]",
        pending && "opacity-55",
        failed && "text-status-danger",
      )}
    >
      {body}
      {pending && (
        <span className="ml-2 align-middle text-[10px] uppercase tracking-wide text-text-muted">
          sending…
        </span>
      )}
      {failed && (
        <span className="ml-2 align-middle text-[11px]">
          <span className="text-status-danger">failed</span>
          {onRetry && (
            <button
              type="button"
              onClick={() => onRetry(message)}
              className="ml-1 rounded px-1 text-text-muted underline-offset-2 outline-none hover:text-text-normal hover:underline focus-visible:ring-2 focus-visible:ring-white/20"
            >
              retry
            </button>
          )}
        </span>
      )}
    </div>
  );
}

function TimelineEmpty() {
  return (
    <div className="flex flex-1 select-none items-center justify-center py-16 text-[13px] text-text-muted">
      No messages yet.
    </div>
  );
}
