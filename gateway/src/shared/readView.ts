/** Browser-facing Gateway REST DTOs. No server implementation types cross this boundary. */
export interface MemberRow {
  name: string;
  sessionId: string;
  agentId?: string;
  agent?: string;
  kind?: string;
  tier?: string;
  presence: string;
  currentWork?: string;
}

export interface ThreadRow {
  name: string;
  topic?: string;
  description?: string;
  members: string[];
  lastAt?: number;
  latestSeq?: number;
}

export interface HistoryRow {
  messageId: string;
  from: string;
  when: number;
  summary?: string;
  body: string;
  cursor?: { createdAt: number; rowid: number; opaque?: string };
}

export interface RouteRuleRow {
  source?: string;
  topic?: string;
  to: string;
}

export interface NotificationRow {
  notifId: string;
  source?: string;
  topic?: string;
  hmacOk: boolean;
  routedTo: string[];
  when: number;
}

export interface ProjectRow {
  projectId: string;
  name: string;
  createdBy?: string;
  rootPath?: string;
  createdAt?: number;
}

export interface WhoamiRow {
  agentId?: string;
  name: string;
  sessionId: string;
  kind: string;
  tier: string;
  presence: string;
}
