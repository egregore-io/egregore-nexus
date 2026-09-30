// AppShell — the overall operator frame (web console).
//
// Layout: a sticky TopBar across the top, then a
// three-column shell grid — [ rail (Sidebar) | main pane (router Outlet) |
// context panel ]. Grid widths come from the `--rail-w` / `--ctx-w` / `--top-h`
// tokens so a theme can re-scale the frame. The shell holds no business logic.
//
// Feature views render their pane into <main> via the Outlet and (optionally)
// publish their right-panel content into the context <aside> via the context
// slot — the host exposes its inner node and routes portal into it.
//
// The TopBar + Sidebar are fed LIVE read-view data (real threads/members/projects/
// whoami) via `useShellNav` over the public REST API + TanStack Query — no seed.
// They stay presentational (props in); this is the live container that wires them.
import { Outlet } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import type { ReactNode } from "react";

import { useActiveProject, useSetActiveProject } from "@app/activeProject";
import { useApplyAppearance } from "@app/applyAppearance";
import { useProjectGroups } from "@app/projects";
import { useCollapsed, useTogglePanel } from "@app/uiPrefs";

import { ContextPanelHost } from "./ContextPanelHost";
import { ContextSlotProvider } from "./contextSlot";
import { NewThreadDialog } from "./NewThreadDialog";
import { SettingsModal } from "./SettingsModal";
import { Sidebar } from "./Sidebar/Sidebar";
import { TopBar } from "./TopBar";
import { LogDrawer } from "./LogDrawer";
import { InteractionLogger } from "./InteractionLogger";
import { useThreadNotifications } from "./useThreadNotifications";
import {
  useAgentsLive,
  useChannels,
  useDms,
  useMe,
  useTeams,
} from "./useShellNav";

export interface AppShellProps {
  /** Override the routed Outlet (used by tests). */
  children?: ReactNode;
}

export function AppShell({ children }: AppShellProps) {
  const [contextNode, setContextNode] = useState<HTMLElement | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [newThreadOpen, setNewThreadOpen] = useState(false);

  // Collapse state (persisted via uiPrefs zustand store).
  const railCollapsed = useCollapsed("rail");
  const ctxCollapsed = useCollapsed("ctx");
  const togglePanel = useTogglePanel();

  // Re-apply persisted appearance tokens to the document on mount (so
  // remembered values survive navigation / page refresh).
  useApplyAppearance();

  // Native browser toasts when a subscribed thread gains a message while the
  // tab is hidden/unfocused (rides the polled threads read-view; inert unless
  // Notification.permission is already "granted").
  useThreadNotifications();

  // Active project GROUP (persisted; null = "All threads"). A project is a console-only grouping
  // of threads owned by the console — see `@app/projects`.
  const activeProject = useActiveProject();
  const setActiveProject = useSetActiveProject();

  // Live read-view → the rail + top-bar IA (real data; empty/loading otherwise).
  const channels = useChannels();
  const dms = useDms();
  const groups = useProjectGroups();
  const me = useMe();
  const agentsLive = useAgentsLive();
  const teams = useTeams();
  const navLoading = channels.isLoading || dms.isLoading;

  // Switcher rows come from the gateway project groups (console-only).
  const switcherProjects = (groups.data ?? []).map((g) => ({
    projectId: g.id,
    name: g.name,
    presence: "online" as const,
  }));
  const activeGroup = activeProject
    ? groups.data?.find((g) => g.id === activeProject)
    : undefined;
  // Filter the channel rail to the active group's members; "All" shows every thread. If the active
  // group id no longer exists (deleted), fall through to "All" rather than hiding everything.
  const allChannels = channels.data ?? [];
  const visibleChannels = activeGroup
    ? allChannels.filter((c) => activeGroup.threads.includes(c.name))
    : allChannels;
  const dmNames = (dms.data ?? []).map((d) => d.name);

  // Keyboard shortcuts: "[" toggles the rail, "]" toggles the context panel.
  // Guard: do not fire while typing in input/textarea/select or contenteditable.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key !== "[" && e.key !== "]") return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const t = e.target as HTMLElement | null;
      if (
        t &&
        (t.isContentEditable || (t.matches && t.matches("input, textarea, select")))
      )
        return;
      e.preventDefault();
      togglePanel(e.key === "[" ? "rail" : "ctx");
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [togglePanel]);

  // Compute grid template — collapsed column becomes "0" so the grid tracks
  // vanish without touching the rail/ctx render tree.
  const railW = railCollapsed ? "0" : "var(--rail-w)";
  const ctxW = ctxCollapsed ? "0" : "var(--ctx-w)";

  return (
    <div className="flex h-screen min-h-0 w-full flex-col overflow-hidden bg-bg-primary text-text-read">
      <TopBar
        agentsLive={agentsLive.data ?? 0}
        projects={switcherProjects}
        activeProjectId={activeProject ?? undefined}
        onSelectProject={(id) => setActiveProject(id || null)}
        railCollapsed={railCollapsed}
        onToggleRail={() => togglePanel("rail")}
        ctxCollapsed={ctxCollapsed}
        onToggleCtx={() => togglePanel("ctx")}
      />
      <div
        className="grid min-h-0 flex-1"
        style={{ gridTemplateColumns: `${railW} minmax(0, 1fr) ${ctxW}` }}
      >
        {!railCollapsed && (
          <Sidebar
            channels={visibleChannels}
            dms={dms.data ?? []}
            teams={teams}
            me={me.data}
            loading={navLoading}
            onSettings={() => setSettingsOpen(true)}
            onCreateChannel={() => setNewThreadOpen(true)}
            projects={switcherProjects.map((p) => ({ id: p.projectId, name: p.name }))}
          />
        )}
        <ContextSlotProvider node={contextNode}>
          <main
            className="flex min-w-0 flex-col overflow-hidden bg-bg-primary"
            aria-label="Conversation"
          >
            {children ?? <Outlet />}
          </main>
          {!ctxCollapsed && <ContextPanelHost onNode={setContextNode} />}
        </ContextSlotProvider>
      </div>

      <LogDrawer />
      <InteractionLogger />
      <SettingsModal open={settingsOpen} onOpenChange={setSettingsOpen} />
      <NewThreadDialog
        open={newThreadOpen}
        onOpenChange={setNewThreadOpen}
        candidates={dmNames}
        activeProjectName={activeGroup?.name}
      />
    </div>
  );
}
