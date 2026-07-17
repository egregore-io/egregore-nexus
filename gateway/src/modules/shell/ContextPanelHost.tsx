// ContextPanelHost — the right-hand context panel (prototype "RIGHT CONTEXT
// PANEL"). A hairline-separated rail at `--ctx-w`; feature routes portal a scoped
// ctx-view (members / agent details / routing rules / admin tiers) into its inner
// node. When a route provides nothing, a calm idle state shows. Rendered as an
// <aside> complementary landmark. Tokens only.
import { useCallback } from "react";

import { cn } from "@shared/ui";

export interface ContextPanelHostProps {
  /** Receives the inner content node so routes can portal their ctx-view in. */
  onNode: (node: HTMLElement | null) => void;
}

export function ContextPanelHost({ onNode }: ContextPanelHostProps) {
  // Callback ref → report the live node up to the shell for the context portal.
  const ref = useCallback(
    (node: HTMLDivElement | null) => onNode(node),
    [onNode],
  );

  return (
    <aside
      aria-label="Details"
      className="relative overflow-y-auto border-l border-border-subtle bg-bg-secondary px-3.5 py-3.5 lens-scroll"
    >
      {/* Idle state — hidden once the portal node has any content. */}
      <div
        className={cn(
          "pointer-events-none absolute inset-0 flex flex-col items-center justify-center gap-2 px-4 text-center",
          "has-[+_[data-ctx-slot]:not(:empty)]:hidden",
        )}
      >
        <div
          aria-hidden="true"
          className="h-8 w-8 rounded-full border border-dashed border-border-strong"
        />
        <p className="text-[12px] text-text-muted">
          Select a channel, agent, or feed to see details here.
        </p>
      </div>

      {/* Portal target for route-provided context. */}
      <div ref={ref} data-ctx-slot className="relative" />
    </aside>
  );
}
