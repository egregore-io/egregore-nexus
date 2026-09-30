// Tiny class-name joiner for the ui-kit. Wraps `clsx` so primitives have one
// import for conditional/merged className composition. No tailwind-merge dep —
// the kit composes deliberately (base classes first, caller `className` last) so
// last-wins ordering is enough for our utility usage.
import clsx, { type ClassValue } from "clsx";

export function cn(...inputs: ClassValue[]): string {
  return clsx(inputs);
}
