// SidebarItem — the nav row primitive (channels, DMs, members, sections).
//
// Workspace sidebar rows: inactive = muted gray on a
// transparent backdrop; hover = white text + a faint fill; active = white text,
// a hairline border, and the `bg-white/[0.04]` active overlay; 6px radius.
//
// The visual contract is exported as `sidebarItemClasses()` so routed rows can
// apply the exact same styling to a TanStack `<Link>` while `SidebarItem` itself
// renders an accessible `<button>` for non-navigational rows.
import { forwardRef } from "react";
import type { ButtonHTMLAttributes, ReactNode } from "react";

import { cn } from "../cn";

export interface SidebarItemVisualOptions {
  active?: boolean;
  /** Indent for nested rows (e.g. channels under a section). */
  indented?: boolean;
}

/** The shared row classes — reuse on a `<Link>` to keep routed rows identical. */
export function sidebarItemClasses({
  active,
  indented,
}: SidebarItemVisualOptions = {}): string {
  return cn(
    "group/item flex h-8 w-full items-center gap-2 rounded-md px-2 text-left",
    "text-[13px] font-medium leading-none outline-none transition-colors duration-100",
    "motion-reduce:transition-none focus-visible:ring-2 focus-visible:ring-white/20",
    indented && "pl-3",
    active
      ? "border border-white/[0.06] bg-white/[0.04] text-text-normal"
      : "border border-transparent text-text-muted hover:bg-white/[0.025] hover:text-text-normal",
  );
}

export interface SidebarItemProps
  extends ButtonHTMLAttributes<HTMLButtonElement>,
    SidebarItemVisualOptions {
  /** Leading icon (lucide glyph or PresenceDot). */
  icon?: ReactNode;
  /** Trailing slot — typically a count Badge. */
  trailing?: ReactNode;
}

export const SidebarItem = forwardRef<HTMLButtonElement, SidebarItemProps>(
  function SidebarItem(
    { active, indented, icon, trailing, children, className, type, ...rest },
    ref,
  ) {
    return (
      <button
        ref={ref}
        type={type ?? "button"}
        aria-current={active ? "page" : undefined}
        className={cn(sidebarItemClasses({ active, indented }), className)}
        {...rest}
      >
        {icon !== undefined && (
          <span className="flex h-4 w-4 shrink-0 items-center justify-center text-[15px] text-current opacity-90 [&_svg]:h-4 [&_svg]:w-4">
            {icon}
          </span>
        )}
        <span className="min-w-0 flex-1 truncate">{children}</span>
        {trailing !== undefined && (
          <span className="ml-auto flex shrink-0 items-center">{trailing}</span>
        )}
      </button>
    );
  },
);
