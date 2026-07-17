// TopBar — the global header (prototype "GLOBAL HEADER").
//
// Regions, left→right: brand mark + NEXUS wordmark · rail toggle · project switcher
// (status dot + name + chevron) · global search input with a `/` kbd hint ·
// right side: ctx toggle + "N agents live" pill (pulsing dot) + notifications bell
// with a count badge. Nothing here holds business logic — it renders seeded nav state
// and raises affordances the shell/routes own. Tokens only (white-label).
import { Link } from "@tanstack/react-router";
import { useEffect, useRef } from "react";

import { NexusMark, cn } from "@shared/ui";
import { useUiPrefsStore } from "@app/uiPrefs";

import type { ProjectNavItem } from "./nav";
import { ProjectSwitcher } from "./Sidebar/ProjectSwitcher";

export interface PanelToggleProps {
  panel: "rail" | "ctx";
  label: string;
  collapsed: boolean;
  onToggle: () => void;
}

/**
 * PanelToggle — icon button that collapses/expands the rail or context panel.
 * SVG preserved from the original Nexus prototype.
 */
export function PanelToggle({ panel, label, collapsed, onToggle }: PanelToggleProps) {
  return (
    <button
      type="button"
      className={cn(
        "icon-btn panel-toggle",
        "grid h-[30px] w-[30px] place-items-center rounded-btn text-text-muted outline-none transition-colors",
        "hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal",
        "focus-visible:ring-2 focus-visible:ring-white/20",
      )}
      data-panel-toggle={panel}
      aria-label={label}
      aria-pressed={collapsed}
      aria-expanded={!collapsed}
      title={`${label} ( ${panel === "rail" ? "[" : "]"} )`}
      onClick={onToggle}
    >
      {/* SVG preserved from the original Nexus prototype. */}
      <svg viewBox="0 0 18 18" aria-hidden="true" className="h-[17px] w-[17px]">
        <rect x="2.5" y="3.5" width="13" height="11" rx="2" fill="none" stroke="currentColor" strokeWidth="1.2" />
        <rect className="panel-toggle__bar" x="2.5" y="3.5" width="4.5" height="11" rx="2" fill="currentColor" />
        <path d="M7 3.8v10.4" fill="none" stroke="currentColor" strokeWidth="1.2" />
      </svg>
    </button>
  );
}

export interface TopBarProps {
  /** Live count of agents online (members with an `agent` brain). */
  agentsLive?: number;
  /** Notification badge count. */
  notifications?: number;
  /** Live projects for the switcher (real read-view). */
  projects?: ProjectNavItem[];
  /** The active project id (= name) to highlight in the switcher. */
  activeProjectId?: string;
  /** Called when the user picks a project — filters the rail to that group ("" = All threads). */
  onSelectProject?: (projectId: string) => void;
  /** Called when the user picks "New project" in the switcher. */
  onAddProject?: () => void;
  /** Rail collapse state — drives the rail PanelToggle. */
  railCollapsed?: boolean;
  /** Called when rail PanelToggle is clicked. */
  onToggleRail?: () => void;
  /** Context panel collapse state — drives the ctx PanelToggle. */
  ctxCollapsed?: boolean;
  /** Called when ctx PanelToggle is clicked. */
  onToggleCtx?: () => void;
}

export function TopBar({
  agentsLive = 0,
  notifications = 0,
  projects = [],
  activeProjectId,
  onSelectProject,
  onAddProject,
  railCollapsed = false,
  onToggleRail,
  ctxCollapsed = false,
  onToggleCtx,
}: TopBarProps) {
  const searchRef = useRef<HTMLInputElement>(null);
  const searchCollapsed = useUiPrefsStore((s) => s.collapsed.search);
  const setCollapsed = useUiPrefsStore((s) => s.setCollapsed);

  // "/" expands + focuses search, unless already typing in a field.
  // When collapsed, pressing "/" sets collapsed.search=false then focuses.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const el = e.target as HTMLElement | null;
      const typing =
        el?.isContentEditable ||
        el?.tagName === "INPUT" ||
        el?.tagName === "TEXTAREA";
      if (e.key === "/" && !typing) {
        e.preventDefault();
        setCollapsed("search", false);
        // Focus runs after state update via a microtask so the input is visible.
        Promise.resolve().then(() => searchRef.current?.focus());
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [setCollapsed]);

  return (
    <header
      className={cn(
        "sticky top-0 z-[100] flex items-center gap-3.5 px-3",
        "border-b border-border-subtle bg-surface-inset",
      )}
      style={{ height: "var(--top-h)" }}
    >
      {/* Brand */}
      <div className="flex items-center gap-2 pr-1">
        <BrandMark />
        <span className="text-[13px] font-bold tracking-[0.14em] text-text-normal">
          NEXUS
        </span>
      </div>

      {/* Rail toggle */}
      {onToggleRail && (
        <PanelToggle
          panel="rail"
          label="Toggle sidebar"
          collapsed={railCollapsed}
          onToggle={onToggleRail}
        />
      )}

      {/* Project switcher */}
      <ProjectSwitcher
        projects={projects}
        activeProjectId={activeProjectId}
        onSelect={onSelectProject}
        onAddProject={onAddProject}
      />

      {/* Global search — collapses to an icon-button; "/" or click expands */}
      {searchCollapsed ? (
        <button
          type="button"
          aria-label="Search"
          aria-expanded={false}
          className={cn(
            "icon-btn grid h-[30px] w-[30px] place-items-center rounded-btn text-text-muted outline-none transition-colors",
            "hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal",
            "focus-visible:ring-2 focus-visible:ring-white/20",
          )}
          onClick={() => {
            setCollapsed("search", false);
            Promise.resolve().then(() => searchRef.current?.focus());
          }}
        >
          <SearchIcon />
        </button>
      ) : (
        <div
          className={cn(
            "flex h-[30px] max-w-[520px] flex-1 items-center gap-2 rounded-btn px-2.5",
            "border border-border-subtle bg-surface-inset",
            "focus-within:border-[color:var(--lens-focus-halo)]",
          )}
        >
          <SearchIcon />
          <input
            ref={searchRef}
            type="search"
            aria-label="Search"
            placeholder="Search threads, agents, history…"
            className="flex-1 border-0 bg-transparent text-[13px] text-text-read outline-none placeholder:text-text-muted"
            onKeyDown={(e) => {
              if (e.key === "Escape") {
                e.currentTarget.value = "";
                setCollapsed("search", true);
              }
            }}
            onBlur={(e) => {
              if (!e.currentTarget.value.trim()) setCollapsed("search", true);
            }}
          />
          <kbd className="rounded-2 border border-border-strong bg-surface-inset px-[5px] py-px font-mono text-[10px] text-text-muted">
            /
          </kbd>
        </div>
      )}

      {/* Right side */}
      <div className="ml-auto flex items-center gap-2.5">
        {onToggleCtx && (
          <PanelToggle
            panel="ctx"
            label="Toggle context panel"
            collapsed={ctxCollapsed}
            onToggle={onToggleCtx}
          />
        )}

        <span
          className={cn(
            "inline-flex items-center gap-[7px] rounded-pill px-2.5 py-1",
            "border border-border-subtle text-[11px] font-medium tracking-[0.02em] text-text-muted",
          )}
        >
          <span className="h-1.5 w-1.5 rounded-full bg-online [animation:lens-blink_1.1s_steps(2,start)_infinite]" />
          {agentsLive} agents live
        </span>

        <Link
          to="/pub"
          aria-label="Notifications"
          className={cn(
            "relative grid h-[30px] w-[30px] place-items-center rounded-btn text-text-muted outline-none",
            "transition-colors hover:bg-[color:var(--lens-fill-active)] hover:text-text-normal",
            "focus-visible:ring-2 focus-visible:ring-white/20",
          )}
        >
          <BellIcon />
          {notifications > 0 && (
            <span
              className={cn(
                "absolute right-px top-0.5 grid h-[14px] min-w-[14px] place-items-center rounded-pill px-[3px]",
                "bg-alert text-[9px] font-bold text-white",
              )}
            >
              {notifications}
            </span>
          )}
        </Link>
      </div>
    </header>
  );
}

function BrandMark() {
  // The canonical Nexus mark is static, with dots randomized once per
  // page load (re-roll on refresh only — never re-rolled on click/navigation).
  // Replaces the old egregore-sigil CSS mask. Inherits text-normal via
  // currentColor so it adapts light/dark without a second asset.
  return (
    <NexusMark size={15} aria-hidden="true" className="text-text-normal" />
  );
}

function SearchIcon() {
  return (
    <svg
      viewBox="0 0 16 16"
      aria-hidden="true"
      className="h-[15px] w-[15px] shrink-0 text-text-muted"
    >
      <circle cx="7" cy="7" r="4.2" fill="none" stroke="currentColor" strokeWidth="1.2" />
      <path d="M10.2 10.2 13 13" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
    </svg>
  );
}

function BellIcon() {
  return (
    <svg viewBox="0 0 18 18" aria-hidden="true" className="h-[17px] w-[17px]">
      <path
        d="M9 3a3.5 3.5 0 0 0-3.5 3.5c0 3-1.2 4-1.2 4h9.4s-1.2-1-1.2-4A3.5 3.5 0 0 0 9 3Z"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinejoin="round"
      />
      <path
        d="M7.6 13.2a1.5 1.5 0 0 0 2.8 0"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinecap="round"
      />
    </svg>
  );
}
