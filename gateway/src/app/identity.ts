// Operator identity store — persists the operator's registered name to
// localStorage so the console knows who it is across reloads.
//
// `null` means no operator has been registered yet in this browser session.
// Use `useRegisterOperator()` to POST to /api/v1/register and set the name.
import { useMutation } from "@tanstack/react-query";
import { create } from "zustand";

import { gatewayFetch } from "./gatewayClient";
import { createJSONStorage, persist } from "zustand/middleware";

/** The single backend workspace the console's operator + agents live in. Projects are a
 * console-only grouping now (see `@app/projects`), so the daemon scope is one stable value. */
const WORKSPACE = "default";

export interface IdentityState {
  name: string | null;
  setName: (n: string) => void;
  forget: () => void;
}

/** A no-op storage for SSR (no `window`/`localStorage` on the server). */
const noopStorage = {
  getItem: () => null,
  setItem: () => {},
  removeItem: () => {},
};

export const useIdentityStore = create<IdentityState>()(
  persist(
    (set) => ({
      name: null,
      setName: (n) => set({ name: n }),
      forget: () => set({ name: null }),
    }),
    {
      name: "nexus.identity",
      storage: createJSONStorage(() =>
        typeof window !== "undefined" ? window.localStorage : noopStorage,
      ),
    },
  ),
);

/** The current operator name (or null). */
export const useIdentityName = (): string | null =>
  useIdentityStore((s) => s.name);

/** POSTs the operator to /api/v1/register and persists the name on success. */
export function useRegisterOperator() {
  const setName = useIdentityStore((s) => s.setName);
  return useMutation({
    mutationFn: async (name: string) => {
      const res = await gatewayFetch("/api/v1/register", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name,
          harness: "other",
          harnessSessionId: `op:${name}`,
          project: WORKSPACE,
          clientKey: `op:${name}`,
          tier: "admin",
          kind: "app",
          role: "operator",
        }),
      });
      if (!res.ok) throw new Error(`register → HTTP ${res.status}`);
      return res.json();
    },
    onSuccess: (_d, name) => setName(name),
  });
}
