// Browser identity is resolved exclusively by the authenticated Gateway
// principal exposed at /api/v1/whoami. The old persisted registration store is
// retained only for compatibility with the legacy register mutation; no shell,
// pane, or session attribution reads it as identity authority.
import { useMutation, useQuery } from "@tanstack/react-query";
import { create } from "zustand";

import { gatewayFetch } from "./gatewayClient";
import { qk } from "@shared/queryKeys";
import type { WhoamiRow } from "@shared/readView";
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

const WHOAMI_STALE_MS = 15_000;

/** Resolve the optional authenticated browser principal from the Gateway. */
async function fetchCurrentIdentity(): Promise<WhoamiRow | null> {
  const res = await gatewayFetch("/api/v1/whoami", {
    headers: { accept: "application/json" },
  });
  if (res.status === 401) return null;
  if (!res.ok) throw new Error(`read-view /api/v1/whoami → HTTP ${res.status}`);
  return (await res.json()) as WhoamiRow;
}

/** The shared browser query used by every current-identity consumer. */
export function useCurrentIdentity() {
  return useQuery({
    queryKey: qk.whoami(),
    queryFn: fetchCurrentIdentity,
    staleTime: WHOAMI_STALE_MS,
  });
}

/** The authenticated operator name (or null while logged out/loading). */
export const useIdentityName = (): string | null =>
  useCurrentIdentity().data?.name ?? null;

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
