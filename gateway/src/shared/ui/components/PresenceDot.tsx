// PresenceDot — a small status indicator keyed off the contract `Presence`.
//
// Color comes from the themeable `--lens-status-*` tokens. The dot carries an
// accessible label so screen readers announce presence without relying on color
// alone (a11y: never color-only signalling).
import type { HTMLAttributes } from "react";

import type { Presence } from "@shared/types";

import { cn } from "../cn";

export type PresenceValue = Presence | "online" | "busy" | "offline";

export interface PresenceDotProps extends HTMLAttributes<HTMLSpanElement> {
  presence: PresenceValue;
  size?: "sm" | "md";
}

const colorFor: Record<string, string> = {
  online: "bg-status-online",
  busy: "bg-status-busy",
  offline: "bg-status-offline",
};

const labelFor: Record<string, string> = {
  online: "Online",
  busy: "Busy",
  offline: "Offline",
};

export function PresenceDot({
  presence,
  size = "md",
  className,
  ...rest
}: PresenceDotProps) {
  const key = String(presence);
  const dim = size === "sm" ? "h-1.5 w-1.5" : "h-2 w-2";
  return (
    <span
      role="img"
      aria-label={labelFor[key] ?? key}
      className={cn(
        "inline-block shrink-0 rounded-full",
        // Ring the dot in the canvas color so it reads cleanly when overlaid on
        // an avatar; offline gets no ring to recede.
        key !== "offline" && "ring-2 ring-black/80",
        colorFor[key] ?? "bg-status-offline",
        dim,
        className,
      )}
      {...rest}
    />
  );
}
