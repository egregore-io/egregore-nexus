// Context-panel slot — lets a feature route publish its right-panel content to
// the shell's <aside> while rendering its pane into the routed <main>.
//
// TanStack's nested layout renders the shell once (in __root) with the routed
// <Outlet/> in <main>; the active route also wants to fill the separate context
// column. We bridge that with a portal: the host registers a target node via
// context, and a route renders <ContextPanelContent> which portals its children
// into that node. No business logic — pure layout plumbing. SSR-safe: the portal
// only mounts once the target node exists (effect-driven), so the first server
// render simply omits the context (the host shows its idle state).
import {
  createContext,
  useContext,
  useEffect,
  useState,
} from "react";
import type { ReactNode } from "react";
import { createPortal } from "react-dom";

const ContextSlotNode = createContext<HTMLElement | null>(null);

/** Provided by the host; carries the live target node for the context portal. */
export function ContextSlotProvider({
  node,
  children,
}: {
  node: HTMLElement | null;
  children: ReactNode;
}) {
  return (
    <ContextSlotNode.Provider value={node}>{children}</ContextSlotNode.Provider>
  );
}

/**
 * Render right-panel content from a route. Portals into the host's context node
 * once it exists. While there is no node (first paint / SSR), it renders nothing
 * and the host shows its idle state.
 */
export function ContextPanelContent({ children }: { children: ReactNode }) {
  const node = useContext(ContextSlotNode);
  // Re-render on mount so the (effect-set) node from the provider is picked up.
  const [, setReady] = useState(false);
  useEffect(() => setReady(true), []);
  if (!node) return null;
  return createPortal(children, node);
}
