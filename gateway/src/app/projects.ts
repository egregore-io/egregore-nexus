// Compatibility hooks for the removed webconsole project-group feature. Project is now only the
// daemon scope string; these hooks keep older UI flows harmless while returning no groups.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { gatewayFetch } from "@app/gatewayClient";
import { qk } from "@shared/queryKeys";

/** One project group plus the thread names assigned to it (mirror of the server shape). */
export interface ProjectGroup {
  id: string;
  name: string;
  createdAt: number;
  threads: string[];
}

/** TanStack Query key for the gateway project-group list. */
export const PROJECT_GROUPS_KEY = ["projectGroups"] as const;

async function getJson<T>(path: string): Promise<T> {
  const res = await gatewayFetch(path, { headers: { accept: "application/json" } });
  if (!res.ok) throw new Error(`${path} → HTTP ${res.status}`);
  return (await res.json()) as T;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const res = await gatewayFetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path} → HTTP ${res.status}`);
  return (await res.json()) as T;
}

/** Every project group (with its assigned thread names). */
export function useProjectGroups() {
  return useQuery({
    queryKey: PROJECT_GROUPS_KEY,
    queryFn: () => getJson<{ projects: ProjectGroup[] }>("/api/projects"),
    select: (r) => r.projects,
    staleTime: 15_000,
  });
}

/** Project groups are no longer supported. */
export function useCreateProjectGroup() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      postJson<{ project: ProjectGroup }>("/api/projects", { name }).then((r) => r.project),
    onSuccess: () => void qc.invalidateQueries({ queryKey: PROJECT_GROUPS_KEY }),
  });
}

/** Old project assignment calls are ignored. */
export function useAssignThread() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (args: { threadName: string; projectId: string | null }) =>
      postJson<{ ok: true; ignored?: true }>("/api/projects/assign", args),
    onSuccess: () => void qc.invalidateQueries({ queryKey: PROJECT_GROUPS_KEY }),
  });
}

/**
 * Create a thread through the gateway's `/api/v1/threads` command-intent path. The old
 * `projectId` argument is ignored because project groups are no longer a UI primitive.
 */
export function useCreateThread() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      name: string;
      members?: string[];
      projectId?: string | null;
    }) => {
      await postJson<unknown>("/api/v1/threads", {
        name: args.name,
        members: args.members ?? [],
      });
      return { name: args.name };
    },
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: qk.threads() });
      void qc.invalidateQueries({ queryKey: PROJECT_GROUPS_KEY });
    },
  });
}
