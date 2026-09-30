// Active-project store — which project GROUP the rail is filtered to. A project is a
// console-only grouping of threads owned by the console (see `@app/projects`); this store holds
// the selected project's id and persists it to localStorage so the selection survives reloads.
//
// The value is the project group's **id** (from `/api/projects`), or `null` meaning "All threads"
// (no group filter — every thread is shown). Read it with `useActiveProject()`; the rail filters
// channels to the active group's members (see `AppShell`).
import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

interface ActiveProjectState {
  /** The active project group id, or null for "All threads". */
  activeProject: string | null;
  setActiveProject: (id: string | null) => void;
}

/** A no-op storage for SSR (no `window`/`localStorage` on the server). */
const noopStorage = {
  getItem: () => null,
  setItem: () => {},
  removeItem: () => {},
};

export const useActiveProjectStore = create<ActiveProjectState>()(
  persist(
    (set) => ({
      activeProject: null,
      setActiveProject: (id) => set({ activeProject: id }),
    }),
    {
      name: "nexus.activeProject",
      storage: createJSONStorage(() =>
        typeof window !== "undefined" ? window.localStorage : noopStorage,
      ),
    },
  ),
);

/** The active project group id (or null for "All threads"). */
export const useActiveProject = (): string | null =>
  useActiveProjectStore((s) => s.activeProject);

/** The setter for the active project group (pass null for "All threads"). */
export const useSetActiveProject = (): ((id: string | null) => void) =>
  useActiveProjectStore((s) => s.setActiveProject);
