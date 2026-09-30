export interface PlainNodeDaemonQueryOptions {
  nexusHome?: string;
  requestId?: string;
  timeoutMs?: number;
  caller?: {
    name?: string;
    project: string;
    sessionId?: string;
    agentId?: string;
    runtimeId?: string;
    clientKey?: string;
    kind: "agent" | "human" | "app" | "notification";
    tier: "agent" | "admin";
  };
}

export function callDaemonQuery<T = unknown>(
  method: string,
  params: unknown,
  options?: PlainNodeDaemonQueryOptions,
): Promise<T>;
