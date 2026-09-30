// Seed fixtures for the in-process read-view DB. Kept deliberately consistent
// with the public contract shapes so gateway route tests and UI tests exercise
// the same DTOs the product reads from the store.
import { Kind, Scope, Presence, Tier } from "@shared/types";
import type {
  MemberSummary,
  ThreadSummary,
  TopicSummary,
  Project,
  Source,
  Whoami,
  Message,
  RouteRule,
  HistoryEntry,
  SearchHit,
} from "@shared/types";

export const PROJECT_ID = "p_nexus";
export const PROJECT_NAME = "nexus";

export interface MockSeed {
  project: Project;
  whoami: Whoami;
  members: MemberSummary[];
  threads: ThreadSummary[];
  topics: TopicSummary[];
  messages: Message[];
  routingRules: RouteRule[];
  history: HistoryEntry[];
  searchHits: SearchHit[];
  /** thread name -> member names. */
  threadMembers: Record<string, string[]>;
  /** registered notification sources. */
  sources: Source[];
}

/** Return a fresh, deep-cloned seed so each test DB starts clean. */
export function makeSeed(): MockSeed {
  const now = 1_700_000_000_000;

  const project: Project = {
    projectId: PROJECT_ID,
    name: PROJECT_NAME,
    createdBy: "erin",
    rootPath: "/home/erin/projects/nexus",
    createdAt: now,
  };

  const whoami: Whoami = {
    name: "erin",
    sessionId: "s_erin",
    role: "owner",
    tier: Tier.Admin,
    project: PROJECT_NAME,
    presence: Presence.Online,
  };

  const members: MemberSummary[] = [
    {
      name: "erin",
      sessionId: "s_erin",
      agent: undefined,
      role: "owner",
      presence: Presence.Online,
      currentWork: undefined,
    },
    {
      name: "ben",
      sessionId: "s_ben",
      agent: "claude",
      role: "admin",
      presence: Presence.Online,
      currentWork: "post-merge gate",
    },
    {
      name: "blake",
      sessionId: "s_blake",
      agent: "codex",
      role: "agent",
      presence: Presence.Busy,
      currentWork: "coordinating tasks",
    },
    {
      name: "dylan",
      sessionId: "s_dylan",
      agent: "claude",
      role: "agent",
      presence: Presence.Offline,
      currentWork: undefined,
    },
  ];

  const designMembers = ["erin", "ben", "blake"];
  const opsMembers = ["erin", "ben"];
  const threadMembers: Record<string, string[]> = {
    design: designMembers,
    ops: opsMembers,
    "dm:erin:ben": ["erin", "ben"],
  };

  const threads: ThreadSummary[] = [
    {
      name: "design",
      topic: "Interface planning",
      description: "Product and console design discussions.",
      members: designMembers,
      lastAt: now + 2_000,
    },
    { name: "ops", topic: undefined, description: undefined, members: opsMembers, lastAt: now + 1_000 },
  ];

  const topics: TopicSummary[] = [
    { topic: "builds", subscribers: 2 },
    { topic: "alerts", subscribers: 1 },
  ];

  const messages: Message[] = [
    {
      id: "m_seed_1",
      project: PROJECT_ID,
      from: "ben",
      scope: Scope.Thread,
      thread: "t_design",
      topic: undefined,
      body: "hi team, kicking off the gateway",
      summary: "kickoff",
      provenance: {
        from: "ben",
        kind: Kind.Agent,
        thread: "design",
        topic: undefined,
        stamp: undefined,
      },
      createdAt: now + 1_000,
    },
    {
      id: "m_seed_2",
      project: PROJECT_ID,
      from: "blake",
      scope: Scope.Thread,
      thread: "t_design",
      topic: undefined,
      body: "ack - taking the read-view",
      summary: undefined,
      provenance: {
        from: "blake",
        kind: Kind.Agent,
        thread: "design",
        topic: undefined,
        stamp: undefined,
      },
      createdAt: now + 2_000,
    },
  ];

  const routingRules: RouteRule[] = [
    { source: "ci", topic: "builds", to: "ben" },
  ];

  const history: HistoryEntry[] = messages.map((m) => ({
    from: m.from,
    when: m.createdAt,
    summary: m.summary,
    body: m.body,
  }));

  const searchHits: SearchHit[] = [
    {
      messageId: "m_seed_1",
      from: "ben",
      when: now + 1_000,
      snippet: "hi team, kicking off the gateway",
      score: 0.92,
    },
  ];

  const sources: Source[] = [
    { name: "github-ci", topic: "builds", enabled: true, createdAt: now, lastFiredAt: undefined },
    { name: "alerts-hook", topic: "alerts", enabled: false, createdAt: now, lastFiredAt: undefined },
  ];

  return {
    project,
    whoami,
    members,
    threads,
    topics,
    messages,
    routingRules,
    history,
    searchHits,
    threadMembers,
    sources,
  };
}
