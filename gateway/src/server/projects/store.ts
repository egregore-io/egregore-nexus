// Compatibility helpers for the removed webconsole project-group feature.
// Project is now a plain daemon scope label, not a persisted gateway grouping.
import type { Client } from "@libsql/client";

import { getConversationStore } from "@server/conversation/store";

/** One project group plus the thread names assigned to it. */
export interface ProjectGroup {
  id: string;
  name: string;
  createdAt: number;
  /** Thread names assigned to this project (the daemon's thread NAME = the sidebar key). */
  threads: string[];
}

/** The shared read-write store (schema ensured on first use). */
export function projectsStore(): Promise<Client> {
  return getConversationStore();
}

/** Project groups no longer exist; keep the read route stable while returning none. */
export async function listProjects(_db: Client): Promise<ProjectGroup[]> {
  return [];
}

/** Project groups are no longer supported. */
export async function createProject(
  _db: Client,
  _args: { id: string; name: string; createdAt: number },
): Promise<ProjectGroup> {
  throw new Error("project groups are no longer supported");
}

/** Project groups are no longer supported. */
export async function deleteProject(_db: Client, _id: string): Promise<void> {
  throw new Error("project groups are no longer supported");
}

/**
 * Old clients may still send an assignment after creating a thread. Treat it as a
 * no-op so stale localStorage/UI state cannot break thread creation.
 */
export async function assignThread(
  _db: Client,
  _args: { threadName: string; projectId: string | null; createdAt: number },
): Promise<void> {
  return;
}
