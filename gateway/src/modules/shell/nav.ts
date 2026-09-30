// Shell navigation model — the typed shapes for the rail/top-bar IA.
//
// These are the contract the presentational shell (Sidebar/TopBar) renders and
// that `useShellNav` populates from the live read-view (threads/members/projects/
// whoami). This file is types only — there is no seeded/static content; runtime
// data is fetched live.
import type { PresenceValue } from "@shared/ui";

// ── presence / kind primitives ──────────────────────────────────────────────

export type AgentKind = "agent" | "human" | "notification" | "app";

// ── left rail: grouped IA ───────────────────────────────────────────────────

/** A channel row under the "Channels" group ("#name" + optional count). */
export interface ChannelNavItem {
  id: string;
  /** View key matching the route parameter. */
  name: string;
  /** Unread/activity count shown faint. */
  count?: number;
  /** Render the count as an alert pill (terracotta-tinted) instead of faint. */
  alert?: boolean;
}

/** A direct-message row ("• name agent"). */
export interface DmNavItem {
  id: string;
  name: string;
  /** The daemon-owned session this agent is bound to — addresses `/agent/<name>:<session_id>`. */
  sessionId: string;
  kind: AgentKind;
  presence: PresenceValue;
  /** The kind label shown faint on the right (e.g. "agent"). */
  kindLabel?: string;
}

/** A team row (square team glyph + name). */
export interface TeamNavItem {
  id: string;
  name: string;
}

/** The signed-in operator shown in the rail footer. */
export interface MeIdentity {
  name: string;
  role: string;
  presence: PresenceValue;
}

/** The active project shown in the top-bar switcher. */
export interface ProjectNavItem {
  projectId: string;
  name: string;
  presence: PresenceValue;
}
