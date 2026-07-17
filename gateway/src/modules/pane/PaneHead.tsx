// PaneHead — the header strip of a conversation pane (prototype `.pane__head`).
//
// Left: an optional hash/glyph or a presence dot, the title, and a quiet topic.
// Right: an optional facepile plus route-owned actions. Hairline bottom border,
// fixed height from the prototype. Tokens only.
import type { ReactNode } from "react";

import { Facepile, PresenceDot, cn } from "@shared/ui";
import type { Face, PresenceValue } from "@shared/ui";

export interface PaneHeadProps {
  title: string;
  topic?: string;
  /** A leading glyph ("#" / "≋" / "⌥"). Mutually exclusive with `presence`. */
  glyph?: string;
  /** A leading presence dot (DMs). Mutually exclusive with `glyph`. */
  presence?: PresenceValue;
  faces?: Face[];
  /** Route-owned controls, such as thread archive/delete. */
  actions?: ReactNode;
  className?: string;
}

export function PaneHead({
  title,
  topic,
  glyph,
  presence,
  faces,
  actions,
  className,
}: PaneHeadProps) {
  const hasRightRail = (faces && faces.length > 0) || actions;

  return (
    <div
      className={cn(
        "flex h-[50px] shrink-0 items-center justify-between gap-3 border-b border-border-subtle px-[18px]",
        className,
      )}
    >
      <div className="flex min-w-0 items-baseline gap-2.5">
        {glyph && (
          <span className="text-[18px] font-semibold text-text-faint">
            {glyph}
          </span>
        )}
        {presence && (
          <PresenceDot presence={presence} className="self-center" />
        )}
        <h1 className="truncate text-[18px] font-semibold tracking-[-0.01em] text-text-normal">
          {title}
        </h1>
        {topic && (
          <span className="truncate text-[12px] text-text-muted">{topic}</span>
        )}
      </div>
      {hasRightRail && (
        <div className="flex shrink-0 items-center gap-2.5">
          {faces && faces.length > 0 && <Facepile faces={faces} />}
          {actions}
        </div>
      )}
    </div>
  );
}
