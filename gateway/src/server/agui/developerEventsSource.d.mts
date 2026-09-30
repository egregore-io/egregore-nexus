export interface DeveloperEventEnvelope {
  kind: "message" | "agent_lifecycle" | "action";
  topic: string;
  seq: number;
  ts: number;
  thread?: string;
  dm?: string;
  from?: string;
  messageId?: string;
  agent?: string;
  sessionId?: string;
  lifecycle?: string;
  currentWork?: string;
  data?: unknown;
}

export interface DeveloperEventSource {
  since(topic: string, afterSeq: number): Promise<DeveloperEventEnvelope[]>;
}

export interface DeveloperEventSourceOptions {
  query?: (
    method: string,
    params: { sql: string; args: unknown[] },
  ) => Promise<{ columns?: string[]; rows?: unknown[][] }>;
  execute?: (params: {
    sql: string;
    args: unknown[];
  }) => Promise<{ columns?: string[]; rows?: unknown[][] }>;
}

export function createDeveloperEventSource(
  options?: DeveloperEventSourceOptions,
): DeveloperEventSource;
