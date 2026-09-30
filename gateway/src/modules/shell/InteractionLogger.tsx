// Global interaction logger — "if it moves it has a log". Mounted once at the
// shell. Captures, into the durable webconsole log stream:
//   - route changes (a pane/conversation opening)
//   - every click (the element's role + label/text, so the log reads like a
//     trail of what the operator actually did)
// Domain events (send/run/observe/persist/errors) are logged at their source.
import { useEffect } from "react";
import { useRouterState } from "@tanstack/react-router";

import { logEvent } from "@app/log";

/** A short, human label for a clicked element (button text, aria-label, link). */
function describe(el: Element | null): string | null {
  let n: Element | null = el;
  for (let i = 0; n && i < 4; i++, n = n.parentElement) {
    const aria = n.getAttribute("aria-label");
    if (aria) return aria;
    const role = n.getAttribute("role");
    const tag = n.tagName.toLowerCase();
    if (tag === "button" || tag === "a" || role === "button" || role === "link") {
      const text = (n.textContent ?? "").trim().replace(/\s+/g, " ").slice(0, 40);
      const href = n.getAttribute("href");
      return text || href || `${tag}${role ? `[${role}]` : ""}`;
    }
  }
  return null;
}

export function InteractionLogger(): null {
  const pathname = useRouterState({ select: (s) => s.location.pathname });

  // Pane / route opens.
  useEffect(() => {
    logEvent("nav", "info", `open ${pathname}`, { data: { pathname } });
  }, [pathname]);

  // Every click (capture phase so it fires even if handlers stop propagation).
  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      const label = describe(e.target as Element);
      if (label) logEvent("click", "debug", label);
    };
    document.addEventListener("click", onClick, true);
    return () => document.removeEventListener("click", onClick, true);
  }, []);

  return null;
}
