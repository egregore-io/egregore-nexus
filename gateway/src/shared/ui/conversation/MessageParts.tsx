// MessageParts — the rich in-message blocks the web console thread renders:
// inline code, @mentions, a code block with line numbers, the live-streaming
// caret + indicator, agent "thinking" text, and a tool-call block. Each uses
// tokens only (white-label). These compose
// into the thread/message components rather than being inlined as raw markup.
import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../cn";

// ── inline code ─────────────────────────────────────────────────────────────
export function InlineCode({ children }: { children: ReactNode }) {
  return (
    <code className="rounded-2 border border-border-subtle bg-surface-inset px-[5px] py-px font-mono text-[12.5px] text-text-normal">
      {children}
    </code>
  );
}

// ── @mention ────────────────────────────────────────────────────────────────
export function Mention({ children }: { children: ReactNode }) {
  return (
    <span className="rounded-2 bg-[color:var(--lens-fill-active)] px-[3px] font-medium text-text-normal">
      {children}
    </span>
  );
}

// ── code block (line-numbered) ──────────────────────────────────────────────
export interface CodeBlockProps {
  /** One entry per line; line numbers are rendered automatically. */
  lines: string[];
  className?: string;
}

export function CodeBlock({ lines, className }: CodeBlockProps) {
  return (
    <pre
      className={cn(
        "mb-0.5 mt-2 overflow-x-auto rounded-bubble border border-border-subtle bg-bg-tertiary",
        "px-3 py-2.5 font-mono text-[12.5px] leading-[1.55] text-text-read",
        className,
      )}
    >
      {lines.map((line, i) => (
        <div key={i}>
          <span className="mr-3 select-none text-text-faint">{i + 1}</span>
          {line}
        </div>
      ))}
    </pre>
  );
}

// ── live streaming ──────────────────────────────────────────────────────────
export function StreamingIndicator() {
  return (
    <span className="inline-flex items-center gap-1.5 text-[11px] text-online">
      <span className="h-1.5 w-1.5 rounded-full bg-online [animation:lens-blink_1.1s_steps(2,start)_infinite]" />
      streaming
    </span>
  );
}

/** The blinking caret appended to a streaming message body. */
export function Caret() {
  return (
    <span
      aria-hidden="true"
      className="ml-0.5 inline-block h-[15px] w-[7px] align-text-bottom bg-text-muted [animation:lens-blink_1s_steps(2,start)_infinite]"
    />
  );
}

// ── agent thinking ──────────────────────────────────────────────────────────
export function Thinking({
  children,
  className,
  ...rest
}: HTMLAttributes<HTMLParagraphElement>) {
  return (
    <p
      className={cn("text-[13px] italic text-text-muted", className)}
      {...rest}
    >
      {children}
    </p>
  );
}

// ── tool-call block ─────────────────────────────────────────────────────────
export interface ToolCallProps {
  /** Mono glyph in the bar (e.g. "$"). */
  icon?: string;
  /** The command/tool name. */
  name: string;
  /** Status label (e.g. "ok"). */
  status?: string;
  /** The tool output, shown below a hairline. */
  output?: ReactNode;
}

export function ToolCall({ icon = "$", name, status, output }: ToolCallProps) {
  return (
    <div className="mb-0.5 mt-2 overflow-hidden rounded-btn border border-border-subtle bg-surface-inset">
      <div className="flex items-center gap-2 px-[11px] py-[7px] text-[12px]">
        <span className="font-mono text-text-muted">{icon}</span>
        <span className="font-mono text-text-normal">{name}</span>
        {status && (
          <span className="ml-auto rounded-2 border border-border-subtle px-[5px] text-[10px] text-online">
            {status}
          </span>
        )}
      </div>
      {output !== undefined && (
        <div className="whitespace-pre-wrap break-words border-t border-border-subtle px-[11px] pb-[9px] pt-2 font-mono text-[12px] text-text-muted">
          {output}
        </div>
      )}
    </div>
  );
}
