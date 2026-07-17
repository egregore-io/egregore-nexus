/** Stable browser/server cache keys. They carry no storage or process dependency. */
export const qk = {
  messages: (scope: string) => ["messages", scope] as const,
  members: (scope: string) => ["members", scope] as const,
  threads: () => ["threads"] as const,
  topics: () => ["topics"] as const,
  routingRules: () => ["routingRules"] as const,
  search: (query: string, filters?: unknown) => ["search", query, filters] as const,
  whoami: () => ["whoami"] as const,
  projects: () => ["projects"] as const,
  history: (scope: string) => ["history", scope] as const,
  notifications: () => ["notifications"] as const,
  sources: () => ["sources"] as const,
};

export type QueryKeyFactory = typeof qk;
