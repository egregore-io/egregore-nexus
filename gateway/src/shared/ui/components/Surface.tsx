// Surface / Panel — the Obsidian Void container primitives.
//
// Depth comes from flat token fills + HAIRLINE borders (DESIGN.md §1), never
// drop shadows. `Surface` is the base box; `Panel` adds an optional hairline
// header row. `tone` picks the fill from the `--lens-*` background scale.
import { forwardRef } from "react";
import type { HTMLAttributes, ReactNode } from "react";

import { cn } from "../cn";

export type SurfaceTone =
  | "primary" // pure canvas
  | "secondary" // rails / panels
  | "tertiary" // cards / inner containers
  | "inset" // embedded regions (inputs)
  | "raised"; // raised controls / headers

export interface SurfaceProps extends HTMLAttributes<HTMLDivElement> {
  tone?: SurfaceTone;
  /** Render the hairline border (default true). */
  bordered?: boolean;
  /** Apply the standard `--lens-radius` rounding (default true). */
  rounded?: boolean;
}

const tones: Record<SurfaceTone, string> = {
  primary: "bg-primary",
  secondary: "bg-secondary",
  tertiary: "bg-tertiary",
  inset: "bg-surface-inset",
  raised: "bg-surface-raised",
};

export const Surface = forwardRef<HTMLDivElement, SurfaceProps>(function Surface(
  { tone = "tertiary", bordered = true, rounded = true, className, ...rest },
  ref,
) {
  return (
    <div
      ref={ref}
      className={cn(
        tones[tone],
        bordered && "border border-white/[0.06]",
        rounded && "rounded-lens",
        className,
      )}
      {...rest}
    />
  );
});

export interface PanelProps extends SurfaceProps {
  /** Optional header content rendered above a hairline divider. */
  header?: ReactNode;
  /** Optional trailing controls aligned to the right of the header. */
  headerActions?: ReactNode;
}

export const Panel = forwardRef<HTMLDivElement, PanelProps>(function Panel(
  { header, headerActions, children, className, tone = "secondary", ...rest },
  ref,
) {
  return (
    <Surface
      ref={ref}
      tone={tone}
      className={cn("flex min-h-0 flex-col overflow-hidden", className)}
      {...rest}
    >
      {header !== undefined && (
        <div className="flex h-11 shrink-0 items-center justify-between gap-3 border-b border-white/[0.06] px-3.5">
          <div className="min-w-0 truncate text-[13px] font-semibold text-text-normal">
            {header}
          </div>
          {headerActions !== undefined && (
            <div className="flex shrink-0 items-center gap-1">
              {headerActions}
            </div>
          )}
        </div>
      )}
      {children}
    </Surface>
  );
});
