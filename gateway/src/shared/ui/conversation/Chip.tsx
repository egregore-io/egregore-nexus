// Chip — the small metadata tag next to a message author (prototype `.chip`).
//
// Variants: the default quiet tag (e.g. "agent" / "notification") and the `you`
// emphasis (filled with the active-fill). Tokens only.
import type { HTMLAttributes } from "react";

import { cn } from "../cn";

export type ChipVariant = "default" | "you";

export interface ChipProps extends HTMLAttributes<HTMLSpanElement> {
  variant?: ChipVariant;
}

export function Chip({ variant = "default", className, ...rest }: ChipProps) {
  return (
    <span
      className={cn(
        "rounded-2 border border-border-subtle px-[5px] text-[10px] tracking-[0.03em]",
        variant === "you"
          ? "bg-[color:var(--lens-fill-active)] text-text-normal"
          : "text-text-muted",
        className,
      )}
      {...rest}
    />
  );
}
