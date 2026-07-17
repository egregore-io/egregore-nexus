/*
 * Token contract (TS) — Obsidian Void.
 *
 * Tailwind v4 is configured CSS-first (see `src/shared/ui/tokens.css`, which
 * imports Tailwind and maps the `--lens-*` tokens into the theme via `@theme`).
 * Under v4 there is no JS theme to extend at config time; this module is kept as
 * the single TS source of truth for the token NAMES so code can reference the
 * contract instead of re-typing CSS-variable strings. Values are the prototype's
 * (gateway/prototype/styles.css), the look source of truth.
 */

/** The `--lens-*` CSS custom properties that make up the Obsidian Void theme. */
export const lensTokens = {
  // canvas
  bgPrimary: "--lens-bg-primary",
  bgSecondary: "--lens-bg-secondary",
  bgTertiary: "--lens-bg-tertiary",
  bgHover: "--lens-bg-hover",
  surfaceRaised: "--lens-surface-raised",
  surfaceInset: "--lens-surface-inset",
  overlay: "--lens-overlay",
  // text
  textNormal: "--lens-text-normal",
  textRead: "--lens-text-read",
  textMuted: "--lens-text-muted",
  textFaint: "--lens-text-faint",
  // borders / fills
  borderSubtle: "--lens-border-subtle",
  borderStrong: "--lens-border-strong",
  fillActive: "--lens-fill-active",
  fillHover: "--lens-fill-hover",
  focusHalo: "--lens-focus-halo",
  // state hues
  online: "--lens-online",
  busy: "--lens-busy",
  offline: "--lens-offline",
  alert: "--lens-alert",
  accent: "--lens-accent",
  // type
  fontUi: "--lens-font-ui",
  fontMono: "--lens-font-mono",
  // depth
  shadowElevated: "--lens-shadow-elevated",
} as const;

export type LensTokenName = (typeof lensTokens)[keyof typeof lensTokens];

/** `lensVar("bgPrimary")` → `"var(--lens-bg-primary)"` for inline-style use. */
export function lensVar(token: keyof typeof lensTokens): string {
  return `var(${lensTokens[token]})`;
}
