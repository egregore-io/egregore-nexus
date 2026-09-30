// ScrollArea — a scroll container with the recessive Obsidian Void scrollbar.
//
// No Radix scroll-area dependency: native overflow + the `.lens-scroll` utility
// (tokens.css) gives a thin, faint thumb that brightens on hover. Defaults to
// vertical scrolling; pass `axis` for horizontal/both.
import { forwardRef } from "react";
import type { HTMLAttributes } from "react";

import { cn } from "../cn";

export interface ScrollAreaProps extends HTMLAttributes<HTMLDivElement> {
  axis?: "y" | "x" | "both";
}

const axisClasses: Record<NonNullable<ScrollAreaProps["axis"]>, string> = {
  y: "overflow-y-auto overflow-x-hidden",
  x: "overflow-x-auto overflow-y-hidden",
  both: "overflow-auto",
};

export const ScrollArea = forwardRef<HTMLDivElement, ScrollAreaProps>(
  function ScrollArea({ axis = "y", className, ...rest }, ref) {
    return (
      <div
        ref={ref}
        className={cn("lens-scroll min-h-0 min-w-0", axisClasses[axis], className)}
        {...rest}
      />
    );
  },
);
