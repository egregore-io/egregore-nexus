// NexusMark — the official Nexus mark + the travel loader.
//
// Nexus mark contract:
//   - The loader is ONLY the "travel" variant (dots glide straight across the
//     mark between corners) and it is used ONLY for loading states.
//   - Everywhere else the mark is STATIC: the dot constellation is randomized
//     once per page load and never animates or re-rolls in place.
//
// Rendering is delegated to the canonical framework-free `/nexus-mark.js`
// (vendored verbatim from the design export; loaded from the root document
// head). These wrappers only mount/unmount it so the mark stays byte-identical
// to canon. Because the mark is randomized, it renders client-side only — an
// empty box on the server — since SSR markup could never match the client roll.
import { useEffect, useRef } from "react";
import type { HTMLAttributes } from "react";

import { cn } from "../cn";

type NexusMarkGlobal = {
  render: (el: HTMLElement, opts?: Record<string, unknown>) => void;
  loader: (
    el: HTMLElement,
    opts?: Record<string, unknown>,
  ) => { stop: () => void; el: HTMLElement };
};

declare global {
  interface Window {
    NexusMark?: NexusMarkGlobal;
  }
}

/**
 * Run `fn` once the canonical script has landed (it loads async from the
 * document head, so a component can mount first). Returns a disposer.
 */
function whenReady(
  fn: (nm: NexusMarkGlobal) => void | (() => void),
): () => void {
  let raf = 0;
  let cleanup: (() => void) | void;
  const tick = () => {
    const nm = window.NexusMark;
    if (nm) cleanup = fn(nm);
    else raf = requestAnimationFrame(tick);
  };
  tick();
  return () => {
    if (raf) cancelAnimationFrame(raf);
    if (cleanup) cleanup();
  };
}

export interface NexusMarkProps extends HTMLAttributes<HTMLSpanElement> {
  /** Rendered box in px (the mark is square, 64-unit viewBox). */
  size?: number;
  /** 0..1 — chance each of the 12 corners lights up. Canon default 0.45. */
  density?: number;
  /** Pin a reproducible constellation. Omit for a fresh roll per page load. */
  seed?: string | number;
}

/**
 * The static mark. Inherits color via `currentColor` — set a text-* class.
 * One random constellation per mount; changes only on refresh, by canon.
 */
export function NexusMark({
  size = 16,
  density,
  seed,
  className,
  ...rest
}: NexusMarkProps) {
  const ref = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    return whenReady((nm) => {
      nm.render(el, {
        size,
        ...(density != null ? { density } : null),
        ...(seed != null ? { seed } : null),
      });
    });
  }, [size, density, seed]);

  return (
    <span
      ref={ref}
      aria-hidden="true"
      className={cn("inline-block shrink-0 leading-none", className)}
      style={{ width: size, height: size }}
      {...rest}
    />
  );
}

export interface NexusLoaderProps extends HTMLAttributes<HTMLSpanElement> {
  /** Rendered box in px. */
  size?: number;
  /** Dot pool — a number, or "min-max" to randomize the pool per run. */
  count?: number | string;
  /** Animation speed multiplier. Canon default 1. */
  speed?: number;
  /** Accessible label announced to screen readers. */
  label?: string;
}

/**
 * The loading mark — travel variant only (canon). Dots glide straight across
 * the mark between corners; the animation stops and cleans up on unmount.
 */
export function NexusLoader({
  size = 20,
  count,
  speed,
  label = "Loading",
  className,
  ...rest
}: NexusLoaderProps) {
  const ref = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    return whenReady((nm) => {
      const ctl = nm.loader(el, {
        mode: "travel",
        size,
        ...(count != null ? { count } : null),
        ...(speed != null ? { speed } : null),
      });
      return () => ctl.stop();
    });
  }, [size, count, speed]);

  return (
    <span
      ref={ref}
      role="status"
      aria-label={label}
      className={cn("inline-block shrink-0 leading-none", className)}
      style={{ width: size, height: size }}
      {...rest}
    />
  );
}
