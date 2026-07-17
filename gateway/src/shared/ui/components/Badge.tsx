// Badge — compact count/label pill.
//
// Used for nav unread counts, mention counts, role chips, and status tags. Stays
// monochrome by default (a hairline-bordered translucent chip); `tone` opts into
// the accent or a desaturated status color when a signal needs to read.
import type { HTMLAttributes } from "react";

import { cn } from "../cn";

export type BadgeTone = "neutral" | "accent" | "mention" | "online" | "busy";

export interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone;
}

const tones: Record<BadgeTone, string> = {
  neutral: "bg-white/[0.06] text-text-muted",
  accent: "bg-accent text-white",
  // Mentions are the one place we let a count glow: solid desaturated terracotta.
  mention: "bg-status-danger text-black",
  online:
    "bg-status-online/15 text-status-online ring-1 ring-inset ring-status-online/30",
  busy: "bg-status-busy/15 text-status-busy ring-1 ring-inset ring-status-busy/30",
};

export function Badge({ tone = "neutral", className, ...rest }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex min-w-[1.25rem] items-center justify-center rounded-full px-1.5 " +
          "text-[10px] font-semibold leading-[1.4] tabular-nums",
        tones[tone],
        className,
      )}
      {...rest}
    />
  );
}
