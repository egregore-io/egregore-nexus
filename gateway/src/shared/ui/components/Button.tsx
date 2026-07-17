// Button — Obsidian Void primitive.
//
// Token-driven (no hard-coded hex). Variants map to the DESIGN.md roles:
//  - primary   : the accent CTA (solid `--lens-accent`), white text.
//  - secondary : a raised surface with a hairline border (the default control).
//  - ghost     : transparent, reveals the active-overlay fill on hover/focus.
//  - danger    : a desaturated terracotta CTA for destructive actions.
// Focus uses the muted white halo (DESIGN §2 "Focus Halo"), never a saturated
// color. Motion is short and `motion-reduce`-safe.
import { forwardRef } from "react";
import type { ButtonHTMLAttributes } from "react";

import { cn } from "../cn";

export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger";
export type ButtonSize = "sm" | "md" | "lg";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
}

const base =
  "inline-flex select-none items-center justify-center gap-2 whitespace-nowrap rounded-lens " +
  "font-medium tracking-[-0.005em] outline-none transition-colors duration-150 " +
  "motion-reduce:transition-none " +
  "focus-visible:ring-2 focus-visible:ring-white/20 focus-visible:ring-offset-0 " +
  "disabled:pointer-events-none disabled:opacity-45";

const variants: Record<ButtonVariant, string> = {
  primary:
    "bg-accent text-white hover:bg-accent-hover active:bg-accent-hover/90",
  secondary:
    "bg-surface-raised text-text-normal border border-white/[0.08] " +
    "hover:bg-bg-hover hover:border-white/[0.14] active:bg-bg-hover",
  ghost:
    "bg-transparent text-text-muted hover:bg-white/[0.04] hover:text-text-normal " +
    "active:bg-white/[0.06]",
  danger:
    "bg-transparent text-[color:var(--lens-text-normal)] border border-white/[0.10] " +
    "hover:border-white/20 hover:bg-white/[0.04]",
};

const sizes: Record<ButtonSize, string> = {
  sm: "h-7 px-2.5 text-[12px]",
  md: "h-8 px-3 text-[13px]",
  lg: "h-9 px-4 text-ui",
};

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  function Button(
    { variant = "secondary", size = "md", className, type, ...rest },
    ref,
  ) {
    return (
      <button
        ref={ref}
        type={type ?? "button"}
        className={cn(base, variants[variant], sizes[size], className)}
        {...rest}
      />
    );
  },
);
