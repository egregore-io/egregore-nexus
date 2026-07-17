// Tooltip — Radix-backed, Obsidian Void styled.
//
// A floating overlay surface (DESIGN.md "Overlay Floating" `#111`) with a
// hairline border and no heavy shadow. `TooltipProvider` is mounted once at the
// app root; individual tooltips wrap a trigger element via `asChild`.
import * as RadixTooltip from "@radix-ui/react-tooltip";
import type { ReactNode } from "react";

import { cn } from "../cn";

export const TooltipProvider = RadixTooltip.Provider;

export interface TooltipProps {
  /** The tooltip text/content. */
  content: ReactNode;
  /** The trigger element (rendered via Radix `asChild`). */
  children: ReactNode;
  side?: RadixTooltip.TooltipContentProps["side"];
  align?: RadixTooltip.TooltipContentProps["align"];
  /** Open delay in ms (defaults to a snappy 200ms). */
  delayDuration?: number;
}

export function Tooltip({
  content,
  children,
  side = "right",
  align = "center",
  delayDuration = 200,
}: TooltipProps) {
  return (
    <RadixTooltip.Root delayDuration={delayDuration}>
      <RadixTooltip.Trigger asChild>{children}</RadixTooltip.Trigger>
      <RadixTooltip.Portal>
        <RadixTooltip.Content
          side={side}
          align={align}
          sideOffset={6}
          className={cn(
            "z-[var(--lens-z-overlay,60)] select-none rounded-md px-2 py-1",
            "bg-overlay text-[12px] font-medium text-text-normal",
            "border border-white/[0.08]",
            // Token-driven fade — no animation-plugin dependency, reduced-motion safe.
            "origin-[var(--radix-tooltip-content-transform-origin)]",
            "transition-opacity duration-150 motion-reduce:transition-none",
            "data-[state=closed]:opacity-0 data-[state=delayed-open]:opacity-100",
          )}
        >
          {content}
        </RadixTooltip.Content>
      </RadixTooltip.Portal>
    </RadixTooltip.Root>
  );
}
