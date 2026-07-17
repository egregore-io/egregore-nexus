// IconButton — square, icon-only control (top-bar actions, row affordances).
//
// Requires an accessible `label` (mapped to aria-label) since it has no visible
// text. Same Obsidian Void treatment as Button's ghost/secondary variants.
import { forwardRef } from "react";
import type { ButtonHTMLAttributes, ReactNode } from "react";

import { cn } from "../cn";

export type IconButtonVariant = "ghost" | "secondary";
export type IconButtonSize = "sm" | "md";

export interface IconButtonProps
  extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children"> {
  /** Accessible name — required (icon-only controls have no visible text). */
  label: string;
  variant?: IconButtonVariant;
  size?: IconButtonSize;
  children: ReactNode;
}

const base =
  "inline-flex shrink-0 items-center justify-center rounded-lens outline-none " +
  "transition-colors duration-150 motion-reduce:transition-none " +
  "focus-visible:ring-2 focus-visible:ring-white/20 " +
  "disabled:pointer-events-none disabled:opacity-45 " +
  "[&_svg]:h-[1.05em] [&_svg]:w-[1.05em]";

const variants: Record<IconButtonVariant, string> = {
  ghost:
    "bg-transparent text-text-muted hover:bg-white/[0.04] hover:text-text-normal",
  secondary:
    "bg-surface-raised text-text-normal border border-white/[0.08] " +
    "hover:bg-bg-hover hover:border-white/[0.14]",
};

const sizes: Record<IconButtonSize, string> = {
  sm: "h-7 w-7 text-[15px]",
  md: "h-8 w-8 text-[17px]",
};

export const IconButton = forwardRef<HTMLButtonElement, IconButtonProps>(
  function IconButton(
    { label, variant = "ghost", size = "md", className, type, children, ...rest },
    ref,
  ) {
    return (
      <button
        ref={ref}
        type={type ?? "button"}
        aria-label={label}
        title={label}
        className={cn(base, variants[variant], sizes[size], className)}
        {...rest}
      >
        {children}
      </button>
    );
  },
);
