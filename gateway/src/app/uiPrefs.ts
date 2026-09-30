// Persisted UI preferences for the web console: panel collapse state, appearance
// (accent + backdrop dim), and the pinned projects that become tabs. Mirrors the
// SSR-safe persist pattern in `activeProject.ts`. localStorage key `nexus.ui`.
import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";
import { useShallow } from "zustand/react/shallow";

export type Panel = "rail" | "ctx" | "search";
export type Accent = "mono" | "green" | "amber" | "ember";
export type BackdropDim = 0.45 | 0.66 | 0.85;

interface UiPrefsState {
  collapsed: Record<Panel, boolean>;
  accent: Accent;
  backdropDim: BackdropDim;
  pinnedProjects: string[];
  togglePanel: (p: Panel) => void;
  setCollapsed: (p: Panel, v: boolean) => void;
  setAccent: (a: Accent) => void;
  setBackdropDim: (d: BackdropDim) => void;
  togglePin: (project: string) => void;
  isPinned: (project: string) => boolean;
}

const noopStorage = { getItem: () => null, setItem: () => {}, removeItem: () => {} };

export const useUiPrefsStore = create<UiPrefsState>()(
  persist(
    (set, get) => ({
      // Search starts collapsed.
      collapsed: { rail: false, ctx: false, search: true },
      accent: "mono",
      backdropDim: 0.66,
      pinnedProjects: [],
      togglePanel: (p) =>
        set((s) => ({ collapsed: { ...s.collapsed, [p]: !s.collapsed[p] } })),
      setCollapsed: (p, v) =>
        set((s) => ({ collapsed: { ...s.collapsed, [p]: v } })),
      setAccent: (accent) => set({ accent }),
      setBackdropDim: (backdropDim) => set({ backdropDim }),
      togglePin: (project) =>
        set((s) => ({
          pinnedProjects: s.pinnedProjects.includes(project)
            ? s.pinnedProjects.filter((p) => p !== project)
            : [...s.pinnedProjects, project],
        })),
      isPinned: (project) => get().pinnedProjects.includes(project),
    }),
    {
      name: "nexus.ui",
      storage: createJSONStorage(() =>
        typeof window !== "undefined" ? window.localStorage : noopStorage,
      ),
    },
  ),
);

export const usePinnedProjects = () => useUiPrefsStore((s) => s.pinnedProjects);
export const useTogglePin = () => useUiPrefsStore((s) => s.togglePin);
export const useCollapsed = (p: Panel) => useUiPrefsStore((s) => s.collapsed[p]);
export const useTogglePanel = () => useUiPrefsStore((s) => s.togglePanel);
export const useAppearance = () =>
  useUiPrefsStore(useShallow((s) => ({ accent: s.accent, backdropDim: s.backdropDim })));
export const useSetAppearance = () =>
  useUiPrefsStore(useShallow((s) => ({ setAccent: s.setAccent, setBackdropDim: s.setBackdropDim })));
