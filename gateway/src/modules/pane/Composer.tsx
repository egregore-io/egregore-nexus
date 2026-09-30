// Composer — the message input at the foot of a pane.
//
// A contenteditable field (empty-state placeholder via `data-placeholder`) and
// a Send button. Centered to the reading max-width. Tokens only; the editor is
// a labelled textbox for a11y.
//
// When `onSend` is provided the composer is live: Enter (without Shift) or the
// Send button submits the trimmed text, clears the editor, and disables while the
// send is in flight. Without `onSend` it stays inert (stories/tests).
import { useRef, useState } from "react";

import { cn } from "@shared/ui";

export interface ComposerProps {
  /** Placeholder shown when the editor is empty. */
  placeholder: string;
  /** Accessible label for the composer form. */
  label: string;
  /** Send the typed text. Omitted → the composer is inert. */
  onSend?: (text: string) => void | Promise<void>;
}

export function Composer({ placeholder, label, onSend }: ComposerProps) {
  const editorRef = useRef<HTMLDivElement>(null);
  // Synchronous in-flight guard. `sending` (state) can't gate double-submits: two
  // events in the same tick (keydown + form submit, IME commit + Enter) both read
  // the stale `sending === false` from their closure and both fire → duplicate
  // post. A ref flips synchronously, before any await, so the second call bails.
  const inFlightRef = useRef(false);
  const [sending, setSending] = useState(false);

  async function submit() {
    if (!onSend || inFlightRef.current) return;
    const el = editorRef.current;
    const text = (el?.textContent ?? "").trim();
    if (!text) return;
    inFlightRef.current = true;
    // Clear + KEEP FOCUS immediately (optimistic, AionUi-style): the editor stays
    // editable and focused so the operator can keep typing — the reply streams in via
    // `observe`, so there's nothing to wait for. We do NOT flip `contentEditable` off
    // (that blurs the field and the caret never comes back → "send drops out of the box").
    if (el) {
      el.textContent = "";
      el.focus();
    }
    setSending(true);
    try {
      await onSend(text);
    } finally {
      inFlightRef.current = false;
      setSending(false);
      // Re-assert focus after the in-flight re-render settles (belt-and-suspenders for
      // browsers that move focus when the surrounding tree re-renders on send).
      editorRef.current?.focus();
    }
  }

  return (
    <form
      aria-label={label}
      className="mx-auto mb-[18px] mt-1 w-full max-w-chat px-6"
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
    >
      <div
        ref={editorRef}
        contentEditable
        role="textbox"
        aria-multiline="true"
        aria-label={placeholder}
        suppressContentEditableWarning
        data-placeholder={placeholder}
        onKeyDown={(e) => {
          // Enter sends; Shift+Enter inserts a newline (only when live).
          if (onSend && e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            void submit();
          }
        }}
        className={cn(
          "min-h-[44px] rounded-bubble border border-border-strong bg-surface-inset px-3.5 py-[11px]",
          "text-[15px] text-text-read outline-none",
          "focus:border-[color:var(--lens-focus-halo)]",
          "empty:before:text-text-faint empty:before:content-[attr(data-placeholder)]",
        )}
      />
      <div className="mt-2 flex justify-end">
        <button
          type="submit"
          disabled={!onSend || sending}
          className={cn(
            "rounded-pill border border-border-subtle bg-[color:var(--lens-fill-active)] px-4 py-[7px]",
            "text-[13px] font-semibold text-text-normal outline-none transition-colors",
            "hover:border-[color:var(--lens-focus-halo)] hover:bg-bg-hover",
            "focus-visible:ring-2 focus-visible:ring-white/20",
            "disabled:cursor-not-allowed disabled:opacity-50",
          )}
        >
          {sending ? "Sending…" : "Send"}
        </button>
      </div>
    </form>
  );
}
