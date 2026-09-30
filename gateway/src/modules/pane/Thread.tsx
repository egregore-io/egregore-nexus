// Thread — the scrolling message log of a conversation pane.
// Renders day separators and message rows (avatar gutter + head +
// rich body), centered to the reading max-width. It is a polite live-region so
// new lines are announced without stealing focus. Composes ui-kit conversation
// primitives (Chip, Avatar, Streamdown markdown, code/mention/streaming/toolcall
// parts) — not raw markup. Tokens only.
import { useEffect, useRef, useState } from "react";
import { ArrowDown } from "lucide-react";
import { Streamdown } from "streamdown";

import {
  Caret,
  Chip,
  CodeBlock,
  InlineCode,
  Mention,
  StreamingIndicator,
  Thinking,
  ToolCall,
  cn,
} from "@shared/ui";

import { isDaySep, type Block, type Inline, type PaneMessage, type ThreadItem } from "./types";

export type ThreadTextRenderer = "inline" | "streamdown";

const AUTO_SCROLL_RESUME_THRESHOLD_PX = 8;

export interface ThreadProps {
  items: ThreadItem[];
  "aria-label"?: string;
  /** Paragraph renderer for this lane. Defaults to Streamdown markdown. */
  textRenderer?: ThreadTextRenderer;
  /** Keep the newest rendered message in view as live stream rows arrive. */
  autoScroll?: boolean;
}

export function Thread({
  items,
  "aria-label": ariaLabel = "Messages",
  textRenderer = "streamdown",
  autoScroll = true,
}: ThreadProps) {
  const logRef = useRef<HTMLDivElement>(null);
  const stickToBottomRef = useRef(true);
  const lastScrollTopRef = useRef(0);
  const previousItemCountRef = useRef(items.length);
  const [unreadBelow, setUnreadBelow] = useState(0);

  const updateStickiness = () => {
    const log = logRef.current;
    if (!log) return;
    const scrollingUp = log.scrollTop < lastScrollTopRef.current;
    const distanceFromBottom = log.scrollHeight - log.scrollTop - log.clientHeight;
    if (scrollingUp) {
      stickToBottomRef.current = false;
    } else if (distanceFromBottom <= AUTO_SCROLL_RESUME_THRESHOLD_PX) {
      stickToBottomRef.current = true;
      setUnreadBelow(0);
    }
    lastScrollTopRef.current = log.scrollTop;
  };

  useEffect(() => {
    const added = Math.max(0, items.length - previousItemCountRef.current);
    previousItemCountRef.current = items.length;
    if (!autoScroll) return;
    if (!stickToBottomRef.current) {
      if (added > 0) setUnreadBelow((count) => count + added);
      return;
    }
    const log = logRef.current;
    if (!log) return;
    log.scrollTop = log.scrollHeight;
    lastScrollTopRef.current = log.scrollTop;
    setUnreadBelow(0);
  }, [autoScroll, items]);

  const jumpToLatest = () => {
    const log = logRef.current;
    if (!log) return;
    stickToBottomRef.current = true;
    log.scrollTop = log.scrollHeight;
    lastScrollTopRef.current = log.scrollTop;
    setUnreadBelow(0);
    log.focus();
  };

  return (
    <div className="relative flex min-h-0 flex-1">
      <div
        ref={logRef}
        role="log"
        tabIndex={-1}
        aria-live="polite"
        aria-relevant="additions text"
        aria-label={ariaLabel}
        onWheel={(event) => {
          if (event.deltaY < 0) stickToBottomRef.current = false;
        }}
        onScroll={updateStickiness}
        className="flex-1 overflow-y-auto py-[18px] pb-2 lens-scroll"
      >
        {items.map((item, i) =>
          isDaySep(item) ? (
            <DaySeparator key={`sep-${i}`} label={item.sep} />
          ) : (
            <MessageRow key={item.id} msg={item} textRenderer={textRenderer} />
          ),
        )}
      </div>
      {autoScroll && unreadBelow > 0 && (
        <button
          type="button"
          aria-label={`Jump to latest messages, ${unreadBelow} unread`}
          onClick={jumpToLatest}
          className={cn(
            "absolute bottom-4 left-1/2 z-10 flex -translate-x-1/2 items-center gap-2",
            "rounded-pill border border-border-strong bg-surface-raised px-3 py-2",
            "text-[12px] font-semibold text-text-normal shadow-[0_12px_32px_rgba(0,0,0,0.45)]",
            "outline-none transition-colors hover:border-[color:var(--lens-focus-halo)] hover:bg-bg-hover",
            "focus-visible:ring-2 focus-visible:ring-white/20",
          )}
        >
          <ArrowDown aria-hidden="true" size={14} strokeWidth={2} />
          <span>{unreadBelow}</span>
        </button>
      )}
    </div>
  );
}

function DaySeparator({ label }: { label: string }) {
  return (
    <div className="mx-auto mb-3.5 flex max-w-chat items-center gap-3 px-6 text-text-faint">
      <span className="h-px flex-1 bg-border-subtle" />
      <span className="text-[11px] font-medium tracking-[0.04em]">{label}</span>
      <span className="h-px flex-1 bg-border-subtle" />
    </div>
  );
}

const presenceRing: Record<string, string> = {
  online: "after:bg-online",
  busy: "after:bg-busy",
  offline: "after:bg-offline",
};

function MessageRow({ msg, textRenderer }: { msg: PaneMessage; textRenderer: ThreadTextRenderer }) {
  const presenceKey = String(msg.presence ?? "");
  return (
    <article
      className={cn(
        "mx-auto grid max-w-chat px-6 py-[7px]",
        "grid-cols-[52px_1fr] hover:bg-[color:var(--lens-fill-hover)]",
      )}
    >
      {/* avatar gutter */}
      <div className="pt-0.5">
        <span
          className={cn(
            "relative grid h-[30px] w-[30px] place-items-center rounded-btn border text-[12px] font-semibold",
            msg.isYou
              ? "border-border-subtle bg-bg-hover text-text-normal"
              : "border-border-subtle bg-surface-raised text-text-read",
            !msg.isYou &&
              msg.presence &&
              cn(
                "after:absolute after:-bottom-0.5 after:-right-0.5 after:h-2 after:w-2 after:rounded-full after:border-2 after:border-bg-primary",
                presenceRing[presenceKey] ?? "after:bg-offline",
              ),
          )}
        >
          {msg.glyph}
        </span>
      </div>

      {/* body */}
      <div className="min-w-0">
        <div className="mb-0.5 flex items-baseline gap-2">
          <span className="text-[14px] font-semibold text-text-normal">
            {msg.who}
          </span>
          <Chip variant={msg.chip === "you" ? "you" : "default"}>{msg.chip}</Chip>
          {msg.streaming ? (
            <StreamingIndicator />
          ) : (
            msg.time && (
              <time className="ml-0.5 text-[11px] text-text-faint">
                {msg.time}
              </time>
            )
          )}
        </div>
        {msg.blocks.map((block, i) => (
          <BlockView
            key={i}
            block={block}
            streaming={msg.streaming && i === msg.blocks.length - 1}
            textRenderer={textRenderer}
          />
        ))}
      </div>
    </article>
  );
}

function BlockView({
  block,
  streaming,
  textRenderer,
}: {
  block: Block;
  streaming?: boolean;
  textRenderer: ThreadTextRenderer;
}) {
  switch (block.b) {
    case "p":
      if (textRenderer === "streamdown") {
        return (
          <div className="nexus-streamdown text-[15px] text-text-read">
            <Streamdown
              className="nexus-streamdown-body"
              mode={streaming ? "streaming" : "static"}
              parseIncompleteMarkdown={streaming}
              skipHtml
              controls={false}
              lineNumbers={false}
            >
              {markdownFromRuns(block.runs)}
            </Streamdown>
            {streaming && <Caret />}
          </div>
        );
      }
      return (
        <p className="text-[15px] text-text-read">
          {block.runs.map((run, i) => (
            <InlineRun key={i} run={run} />
          ))}
          {streaming && <Caret />}
        </p>
      );
    case "code":
      return <CodeBlock lines={block.lines} />;
    case "thinking":
      return <Thinking>{block.text}</Thinking>;
    case "toolcall":
      return (
        <ToolCall
          icon={block.icon}
          name={block.name}
          status={block.status}
          output={block.output}
        />
      );
  }
}

function InlineRun({ run }: { run: Inline }) {
  switch (run.t) {
    case "text":
      return <>{run.v}</>;
    case "code":
      return <InlineCode>{run.v}</InlineCode>;
    case "mention":
      return <Mention>@{run.v}</Mention>;
  }
}

function markdownFromRuns(runs: Inline[]): string {
  return runs.map(markdownFromRun).join("");
}

function markdownFromRun(run: Inline): string {
  switch (run.t) {
    case "text":
      return run.v;
    case "code":
      return inlineCodeMarkdown(run.v);
    case "mention":
      return `@${run.v}`;
  }
}

function inlineCodeMarkdown(value: string): string {
  const longestFence =
    value.match(/`+/g)?.reduce((longest, fence) => Math.max(longest, fence.length), 0) ?? 0;
  const fence = "`".repeat(longestFence + 1);
  return `${fence}${value}${fence}`;
}
