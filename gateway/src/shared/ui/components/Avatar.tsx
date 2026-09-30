// Avatar — initials chip for members/agents.
//
// No image dependency (and no Radix avatar package): we render initials on a
// flat translucent surface with a hairline ring, matching the monochrome
// Obsidian Void palette. An optional `presence` overlays a PresenceDot.
import { useMemo } from "react";
import type { HTMLAttributes } from "react";

import { cn } from "../cn";
import { PresenceDot, type PresenceValue } from "./PresenceDot";

export type AvatarSize = "xs" | "sm" | "md" | "lg";

export interface AvatarProps extends HTMLAttributes<HTMLSpanElement> {
  /** Display name — initials are derived from it. */
  name: string;
  /** Mark agents (vs humans) with a subtly distinct ring. */
  kind?: "agent" | "human" | "notification" | "app";
  presence?: PresenceValue;
  size?: AvatarSize;
}

const sizeClasses: Record<AvatarSize, string> = {
  xs: "h-5 w-5 text-[9px]",
  sm: "h-6 w-6 text-[10px]",
  md: "h-8 w-8 text-[12px]",
  lg: "h-10 w-10 text-[14px]",
};

function initials(name: string): string {
  const parts = name.trim().split(/[\s._-]+/).filter(Boolean);
  if (parts.length === 0) return "?";
  if (parts.length === 1) return parts[0]!.slice(0, 2).toUpperCase();
  return (parts[0]![0]! + parts[parts.length - 1]![0]!).toUpperCase();
}

export function Avatar({
  name,
  kind = "human",
  presence,
  size = "md",
  className,
  ...rest
}: AvatarProps) {
  const text = useMemo(() => initials(name), [name]);
  // Agents read with a brighter hairline; humans recede slightly. Both stay
  // monochrome (white-alpha) to honor the palette.
  const ring = kind === "agent" ? "ring-white/[0.16]" : "ring-white/[0.08]";
  return (
    <span
      className={cn("relative inline-flex shrink-0", className)}
      {...rest}
    >
      <span
        aria-hidden="true"
        className={cn(
          "inline-flex items-center justify-center rounded-full",
          "bg-white/[0.05] font-semibold text-text-normal ring-1",
          ring,
          sizeClasses[size],
        )}
      >
        {text}
      </span>
      {presence !== undefined && (
        <PresenceDot
          presence={presence}
          size="sm"
          className="absolute -bottom-0.5 -right-0.5"
        />
      )}
    </span>
  );
}
