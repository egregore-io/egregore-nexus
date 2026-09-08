// Durable Agent Session queue read/mutation surface.
//
// `command_intents` remains the only queue authority. This route projects those rows for reconnect
// hydration and performs pending-row mutations with compare-and-set SQL, so the UI never owns a
// second queue and promote-to-steer never decomposes into cancel-plus-send.
import type { Client, Transaction } from "@libsql/client";

import { getConversationStore } from "@server/conversation/store";
import { callDaemonQuery, type DaemonIpcCaller } from "@server/daemon/ipc";
import { currentHuman } from "@server/identity/human";
import { principalIdForHumanUserId } from "@server/store/repos/principals";
import type { GatewayCallerIdentity } from "@server/api/http";
import { parseEntityKind } from "@server/identity/entityKind";
import { parseCookies } from "@server/http/cookies";
import {
  localOperatorCaller,
  isLocalOperatorWebAuthMode,
  webAuthModeFromEnv,
} from "@server/auth/webAuthMode";
import {
  CommandQueueAction,
  CommandQueueState,
  SteerCapability,
  TurnState,
  type CommandQueueEntry,
  type CommandQueueMutationRequest,
  type CommandQueueSnapshot,
  type CommandQueueTransition,
} from "@shared/types";

const PROMPT = "harness.prompt";
const STEER = "harness.steer";
const MAX_QUEUE_ROWS = 100;

/** Omission retains legacy lookup; an explicit selector must be a complete stable pair. */
export function exactSessionError(value: unknown): string | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value))
    return "body must be a JSON object";
  const body = value as Record<string, unknown>;
  if (!("expectedSessionId" in body)) return undefined;
  if (
    typeof body.expectedSessionId !== "string" ||
    !body.expectedSessionId.trim() ||
    typeof body.agentId !== "string" ||
    !body.agentId.trim()
  ) {
    return "expectedSessionId requires a non-empty session id and stable agentId";
  }
  return undefined;
}

export function exactSessionResultError(
  expectedSessionId: unknown,
  result: { sessionId?: unknown } | undefined,
): string | undefined {
  if (
    expectedSessionId !== undefined &&
    result?.sessionId !== expectedSessionId
  ) {
    return "command acknowledgement session mismatch; outcome for the requested session is unconfirmed";
  }
  return undefined;
}

function getIdentityDbLazy(): Promise<Client> {
  return getConversationStore();
}

interface QueueMutationInput {
  project: string;
  now: number;
  request: CommandQueueMutationRequest;
}

interface QueueMutationWireResponse {
  status: number;
  body: Record<string, unknown>;
}

type QueueReadWireResponse =
  | CommandQueueSnapshot
  | {
  events: CommandQueueTransition[];
  nextSeq: number;
  latestSeq: number;
  gap: boolean;
};

interface QueueReadInput {
  project: string;
  name?: string;
  agentId?: string;
  expectedSessionId?: string;
  requester?: DaemonIpcCaller;
  eventsAfter?: number;
}

const gatewayCaller: DaemonIpcCaller = {
  name: "Nexus Gateway",
  project: "default",
  sessionId: "local-operator",
  runtimeId: "local-operator",
  kind: "human",
  tier: "admin",
};

export interface ConversationQueueDeps {
  env?: NodeJS.ProcessEnv;
  /** Explicit direct-store seam for unit tests; production never opens this client. */
  getWriteDb?: () => Promise<Client>;
  /** Gateway-owned human-cookie store; never used for queue/runtime rows. */
  getIdentityDb?: () => Promise<Client>;
  /** Test seam for the daemon-owned atomic mutation RPC. */
  daemonQueueMutation?: (
    input: QueueMutationInput,
  ) => Promise<QueueMutationWireResponse>;
  /** Test seam for daemon-owned queue snapshots and reconnect transitions. */
  daemonQueueRead?: (input: QueueReadInput) => Promise<QueueReadWireResponse>;
  /** Injected direct-DB fixture only. Production observation belongs to the daemon adapter. */
  observeTurn?: (sessionId: string) => NonNullable<CommandQueueSnapshot["observation"]>;
  nexusHome?: string;
  now?: () => number;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function authorizedProject(
  request: Request,
  deps: ConversationQueueDeps,
): Promise<{ project: string; requester: DaemonIpcCaller } | Response> {
  const mode = webAuthModeFromEnv(deps.env ?? process.env);
  const token = parseCookies(request.headers.get("cookie")).get("nexus_human");
  const identityDb = token
    ? await (deps.getIdentityDb ?? getIdentityDbLazy)()
    : undefined;
  const cookieIdentity = token
    ? await currentHuman(token, {
        db: identityDb!,
      }).catch(() => null)
    : null;
  if (cookieIdentity) {
    // A cookie row cannot choose a different account principal or project.
    const account = await identityDb!.execute({
      sql: "SELECT project FROM human_user WHERE client_key = ? AND human_user_id = ?",
      args: [cookieIdentity.clientKey!, cookieIdentity.humanUserId],
    });
    let consistent = false;
    try {
      consistent =
        account.rows.length === 1 &&
        account.rows[0]?.project === cookieIdentity.project &&
        principalIdForHumanUserId(cookieIdentity.humanUserId) ===
          cookieIdentity.principalId;
    } catch {
      /* Malformed stored account identity is not requester evidence. */
    }
    if (!consistent) return json({ error: "invalid requester identity" }, 401);
  }
  const identity =
    cookieIdentity ??
    (isLocalOperatorWebAuthMode(mode) ? localOperatorCaller() : null);
  if (!identity) return json({ error: "not logged in" }, 401);
  return { project: identity.project, requester: queueRequester(identity) };
}

function queueRequester(identity: GatewayCallerIdentity): DaemonIpcCaller {
  return {
    name: identity.name,
    project: identity.project,
    kind: identity.kind ?? "human",
    tier: identity.tier ?? "admin",
    ...(identity.locality ? { locality: identity.locality } : {}),
    ...(identity.principalId ? { principalId: identity.principalId } : {}),
    ...(identity.clientKey
      ? { clientKey: identity.clientKey }
      : {
          ...(identity.sessionId ? { sessionId: identity.sessionId } : {}),
          ...(identity.runtimeId ? { runtimeId: identity.runtimeId } : {}),
        }),
  };
}

async function daemonQueueMutation(
  input: QueueMutationInput,
  deps: ConversationQueueDeps,
): Promise<QueueMutationWireResponse> {
  if (deps.daemonQueueMutation) return deps.daemonQueueMutation(input);
  return callDaemonQuery<QueueMutationWireResponse>(
    "local.sessionQueue.mutate",
    input,
    { ...gatewayCaller, project: input.project },
    {
      ...(deps.nexusHome ? { nexusHome: deps.nexusHome } : {}),
    },
  );
}

async function daemonQueueRead(
  input: QueueReadInput,
  deps: ConversationQueueDeps,
): Promise<QueueReadWireResponse> {
  if (deps.daemonQueueRead) return deps.daemonQueueRead(input);
  return callDaemonQuery<QueueReadWireResponse>(
    "local.sessionQueue.read",
    input,
    { ...gatewayCaller, project: input.project },
    {
      ...(deps.nexusHome ? { nexusHome: deps.nexusHome } : {}),
    },
  );
}

type QueueTarget = {
  name?: string;
  agentId?: string;
  expectedSessionId?: string;
};

function targetFromUrl(request: Request): QueueTarget | Response {
  const url = new URL(request.url);
  const name = url.searchParams.get("name")?.trim();
  const agentId = url.searchParams.get("agentId")?.trim() || undefined;
  const expectedSessionId = url.searchParams.has("expectedSessionId")
    ? url.searchParams.get("expectedSessionId")!
    : undefined;
  if (
    expectedSessionId !== undefined &&
    (!expectedSessionId.trim() ||
      !agentId ||
      url.searchParams.getAll("expectedSessionId").length !== 1 ||
      url.searchParams.getAll("agentId").length !== 1)
  ) {
    return json(
      {
        error: "expectedSessionId requires a non-empty session id and stable agentId",
      },
      400,
    );
  }
  if (!name && !agentId) {
    return json({ error: "name or agentId is required" }, 400);
  }
  return {
    ...(name ? { name } : {}),
    ...(agentId ? { agentId } : {}),
    ...(
    expectedSessionId !== undefined ? { expectedSessionId } : {}),
  };
}

function isResponse(value: unknown): value is Response {
  return value instanceof Response;
}

interface TargetRuntime {
  sessionId?: string;
  name?: string;
  harness?: string;
  transport?: string;
  turnActive: boolean;
  steerCapability: SteerCapability;
}

type SqlExecutor = Pick<Client, "execute"> | Pick<Transaction, "execute">;

async function targetRuntime(
  db: SqlExecutor,
  target: QueueTarget,
  readOnly = false,
): Promise<TargetRuntime> {
  const result = await db.execute({
    sql:
      "SELECT s.session_id, s.agent, s.transport, EXISTS(" +
      "SELECT 1 FROM agent_session_turns t WHERE t.session_id = s.session_id " +
      "AND t.status = 'streaming' AND t.finalized_at IS NULL LIMIT 1" +
      ") AS turn_active, s.name FROM sessions s WHERE " +
      (readOnly && target.expectedSessionId
        ? "s.agent_id = ? AND s.session_id = ? "
        :
      "((? IS NOT NULL AND s.agent_id = ?) OR (? IS NULL AND s.name = ?)) ") +
      (target.expectedSessionId && !readOnly
        ? "AND s.session_id = ? AND EXISTS (SELECT 1 FROM agent_runtimes r WHERE r.agent_id = s.agent_id AND r.runtime_id = s.session_id AND r.active = 1 AND r.stopped_at IS NULL) "
        : "") +
      "ORDER BY s.created_at DESC LIMIT 1",
    args:
      readOnly && target.expectedSessionId
        ? [target.agentId!, target.expectedSessionId]
        : [
      target.agentId ?? null,
      target.agentId ?? null,
      target.agentId ?? null,
      target.name ?? null,
      ...(target.expectedSessionId ? [target.expectedSessionId] : []),
    ],
  });
  const row = result.rows[0];
  const sessionId =
    typeof row?.session_id === "string" ? row.session_id : undefined;
  const name = typeof row?.name === "string" ? row.name : undefined;
  const harness = typeof row?.agent === "string" ? row.agent : undefined;
  const transport =
    typeof row?.transport === "string" ? row.transport : undefined;
  const turnActive = Number(row?.turn_active ?? 0) === 1;
  const steerCapability = SteerCapability.None;
  return { sessionId, name, harness, transport, turnActive, steerCapability };
}

function targetPredicate(
  target: QueueTarget,
  sessionId?: string,
): { sql: string; args: Array<string | null> } {
  const direct = target.agentId
    ? {
        sql: "json_extract(request_json, '$.agentId') = ?",
        args: [target.agentId],
      }
    : {
        sql:
          "json_extract(request_json, '$.agentId') IS NULL " +
          "AND json_extract(request_json, '$.name') = ?",
        args: [target.name ?? ""],
      };
  if (!sessionId) return direct;
  return {
    sql:
      `(${direct.sql} OR command_id IN (` +
      "SELECT command_id FROM command_intent_events WHERE session_id = ?))",
    args: [...direct.args, sessionId],
  };
}

async function queueEntries(
  db: SqlExecutor,
  target: QueueTarget,
  sessionId?: string,
  requester?: DaemonIpcCaller,
): Promise<CommandQueueEntry[]> {
  const predicate = targetPredicate(target, sessionId);
  const entries: CommandQueueEntry[] = [];
  for (let offset = 0; ; offset += MAX_QUEUE_ROWS) {
    const result = await db.execute({
      sql:
        "SELECT command_intents.*, (SELECT session_id FROM command_intent_events e WHERE e.command_id = command_intents.command_id ORDER BY seq LIMIT 1) AS original_session, COALESCE((SELECT MAX(seq) FROM command_intent_events e " +
      "WHERE e.command_id = command_intents.command_id), 0) AS seq " +
      "FROM command_intents WHERE kind IN (?, ?) AND " +
        predicate.sql +
        " " +
      "ORDER BY CASE WHEN status IN ('pending', 'claimed') THEN 0 ELSE 1 END, created_at ASC " +
        "LIMIT ? OFFSET ?",
      args: [PROMPT, STEER, ...predicate.args, MAX_QUEUE_ROWS, offset],
    });
    for (const row of result.rows) {
      const binding = await commandSessionBinding(
        db,
        parseObject(row.request_json),
        row.original_session,
      );
      if (!binding || binding !== sessionId) continue;
      entries.push({
        ...queueEntry(row, binding),
        correlationOwned: correlationOwned(row, requester),
      });
      if (entries.length === MAX_QUEUE_ROWS) return entries;
    }
    if (result.rows.length < MAX_QUEUE_ROWS) return entries;
  }
}

async function retainedOwned(
  db: SqlExecutor,
  agentId: string,
  sessionId: string,
): Promise<boolean> {
  const result = await db.execute({
    sql: "SELECT r.agent_id, s.session_id AS descriptive_session, s.agent_id AS descriptive_agent FROM agent_runtimes r LEFT JOIN sessions s ON s.session_id = r.runtime_id WHERE r.runtime_id = ?",
    args: [sessionId],
  });
  return (
    result.rows.length === 1 &&
    result.rows[0]?.agent_id === agentId &&
    (result.rows[0]?.descriptive_session == null ||
      result.rows[0]?.descriptive_agent === agentId)
  );
}

async function commandSessionBinding(
  db: SqlExecutor,
  request: Record<string, unknown>,
  original: unknown,
): Promise<string | undefined> {
  const agent =
    typeof request.agentId === "string" && request.agentId.trim()
      ? request.agentId
      : undefined;
  if ("expectedSessionId" in request) {
    const expected = request.expectedSessionId;
    return agent &&
      typeof expected === "string" &&
      expected.trim() &&
      (await retainedOwned(db, agent, expected))
      ? expected
      : undefined;
  }
  if (typeof original !== "string" || !original) return undefined;
  const result = await db.execute({
    sql: "SELECT s.*, r.agent_id AS runtime_agent FROM sessions s LEFT JOIN agent_runtimes r ON r.runtime_id = s.session_id WHERE s.session_id = ?",
    args: [original],
  });
  const row = result.rows[0];
  if (!row && agent)
    return (await retainedOwned(db, agent, original)) ? original : undefined;
  if (
    !row ||
    typeof row.agent_id !== "string" ||
    !row.agent_id.trim() ||
    (row.runtime_agent != null && row.runtime_agent !== row.agent_id)
  )
    return undefined;
  if (agent) return row.agent_id === agent ? original : undefined;
  // An omitted sessionId leaves the first journal row as the enqueue-time
  // target binding. Its retained owner survives descriptive name changes.
  return typeof request.name === "string" &&
    request.name.trim() &&
    (request.name === row.name || !("sessionId" in request))
    ? original
    : undefined;
}

function correlationOwned(
  row: Record<string, unknown>,
  requester?: DaemonIpcCaller,
): boolean {
  if (
    !requester ||
    row.project !== requester.project ||
    requester.kind !== "human" ||
    row.caller_tier !== requester.tier
  )
    return false;
  try {
    const kind = parseEntityKind(row.caller_kind);
    if (
      kind.kind !== requester.kind ||
      kind.locality !== (requester.locality ?? "local")
    )
      return false;
  } catch {
    return false;
  }
  if (requester.clientKey)
    return (
      row.caller_client_key === requester.clientKey &&
      Boolean(requester.principalId) &&
      row.caller_principal_id === requester.principalId
    );
  return (
    requester.tier === "admin" &&
    requester.sessionId === "local-operator" &&
    (!requester.runtimeId || requester.runtimeId === "local-operator") &&
    row.caller_session_id === "local-operator" &&
    (row.caller_runtime_id == null ||
      row.caller_runtime_id === "local-operator") &&
    row.caller_client_key == null
  );
}

async function registeredReadRequester(
  db: SqlExecutor,
  evidence: DaemonIpcCaller,
): Promise<DaemonIpcCaller | undefined> {
  if (!evidence.clientKey) return evidence;
  if (!evidence.principalId || evidence.kind !== "human") return undefined;
  const result = await db.execute({
    sql: "SELECT * FROM sessions WHERE client_key = ?",
    args: [evidence.clientKey],
  });
  const row = result.rows[0];
  if (
    result.rows.length !== 1 ||
    row?.project !== evidence.project ||
    !["human", "local.human"].includes(String(row.kind)) ||
    row.tier !== evidence.tier
  )
    return undefined;
  return evidence;
}

function queueEntry(
  row: Record<string, unknown>,
  sessionId?: string,
): CommandQueueEntry {
  const request = parseObject(row.request_json);
  return {
    commandId: String(row.command_id),
    clientMessageId:
      typeof request.clientMessageId === "string"
        ? request.clientMessageId
        : undefined,
    sessionId,
    text: typeof request.text === "string" ? request.text : "",
    state: queueState(String(row.status), row.started_at),
    mode: row.kind === STEER ? "redirect" : "queue",
    revision: Number(row.revision ?? 1),
    seq: Number(row.seq ?? 0),
    createdAt: Number(row.created_at ?? 0),
    claimedAt: nullableNumber(row.claimed_at),
    startedAt: nullableNumber(row.started_at),
    completedAt: nullableNumber(row.completed_at),
    error: errorMessage(row.error_json),
    errorCode: structuredErrorCode(row.error_json),
  };
}

function structuredErrorCode(raw: unknown): number | undefined {
  const code = parseObject(raw).code;
  return typeof code === "number" &&
    Number.isInteger(code) &&
    code >= -2147483648 &&
    code <= 2147483647
    ? code
    : undefined;
}

async function latestQueueSeq(
  db: SqlExecutor,
): Promise<number> {
  const result = await db.execute({
    sql: "SELECT COALESCE(MAX(seq), 0) AS seq FROM command_intent_events",
    args: [],
  });
  return Number(result.rows[0]?.seq ?? 0);
}

async function queueEvents(
  db: SqlExecutor,
  afterSeq: number,
  requester?: DaemonIpcCaller,
): Promise<{
  events: CommandQueueTransition[]; scannedSeq: number }> {
  const result = await db.execute({
    sql:
      "SELECT c.*, e.seq, e.session_id, e.command_id, e.client_message_id, " +
      "(SELECT session_id FROM command_intent_events first WHERE first.command_id = c.command_id ORDER BY seq LIMIT 1) AS original_session, " +
      "e.state, e.mode, e.revision FROM command_intent_events e " +
      "LEFT JOIN command_intents c ON c.command_id = e.command_id " +
      "WHERE e.seq > ? ORDER BY e.seq LIMIT 500",
    args: [afterSeq],
  });
  const events: CommandQueueTransition[] = [];
  for (const row of result.rows) {
    const sessionId = await commandSessionBinding(
      db,
      parseObject(row.request_json),
      row.original_session,
    );
    if (!sessionId) continue;
    events.push({
      seq: Number(row.seq),
      sessionId,
    commandId: String(row.command_id),
    clientMessageId:
      typeof row.client_message_id === "string"
        ? row.client_message_id
        : undefined,
    commandKind: String(row.kind),
    callerName: String(row.caller_name),
    callerSessionId:
      typeof row.caller_session_id === "string" ? row.caller_session_id : undefined,
    callerAgentId:
      typeof row.caller_agent_id === "string" ? row.caller_agent_id : undefined,
    callerPrincipalId:
      typeof row.caller_principal_id === "string" ? row.caller_principal_id : undefined,
    callerKind: typeof row.caller_kind === "string" ? row.caller_kind : undefined,
    state: String(row.state) as CommandQueueState,
      mode: String(row.mode),
    revision: Number(row.revision),
      correlationOwned: correlationOwned(row, requester),
    });
  }
  return { events, scannedSeq: Number(result.rows.at(-1)?.seq ?? afterSeq) };
}

function queueState(status: string, startedAt?: unknown): CommandQueueState {
  switch (status) {
    case "pending":
      return CommandQueueState.Queued;
    case "claimed":
      return startedAt == null
        ? CommandQueueState.Claimed
        : CommandQueueState.Started;
    case "done":
      return CommandQueueState.Completed;
    case "error":
      return CommandQueueState.Failed;
    case "cancelled":
      return CommandQueueState.Cancelled;
    default:
      return CommandQueueState.Failed;
  }
}

function parseObject(raw: unknown): Record<string, unknown> {
  if (typeof raw !== "string") return {};
  try {
    const parsed = JSON.parse(raw) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}

function nullableNumber(value: unknown): number | undefined {
  return value == null ? undefined : Number(value);
}

function errorMessage(raw: unknown): string | undefined {
  const value = parseObject(raw);
  return typeof value.message === "string" ? value.message : undefined;
}

function mutationResponse(
  runtime: TargetRuntime,
  clientMutationId: string,
  state: CommandQueueState,
  seq: number,
  commandId?: string,
  revision?: number,
): Record<string, unknown> {
  return {
    clientMutationId,
    ...(commandId ? { commandId } : {}),
    ...(runtime.sessionId ? { sessionId: runtime.sessionId } : {}),
    state,
    steerCapability: runtime.steerCapability,
    ...(revision !== undefined ? { revision } : {}),
    seq,
  };
}

function mutationErrorBody(
  body: Partial<CommandQueueMutationRequest>,
  error: string,
): Record<string, unknown> {
  return {
    ...(body.clientMutationId
      ? { clientMutationId: body.clientMutationId }
      : {}),
    ...(body.commandId ? { commandId: body.commandId } : {}),
    error,
  };
}

function mutationError(
  body: Partial<CommandQueueMutationRequest>,
  error: string,
  status: number,
): Response {
  return json(mutationErrorBody(body, error), status);
}

function canonicalMutationRequest(
  body: CommandQueueMutationRequest,
  target: QueueTarget,
): string {
  return JSON.stringify({
    name: target.name,
    agentId: target.agentId ?? null,
    ...(target.expectedSessionId
      ? { expectedSessionId: target.expectedSessionId }
      : {}),
    action: body.action,
    commandId: body.commandId ?? null,
    expectedRevision: body.expectedRevision ?? null,
    text: body.text ?? null,
    commandIds: body.commandIds ?? [],
    expectedRevisions: [...(body.expectedRevisions ?? [])].sort((left, right) =>
      left.commandId.localeCompare(right.commandId),
    ),
  });
}

/** Hydrate the canonical session queue and persisted turn-active state. */
export async function handleConversationQueueGet(
  request: Request,
  deps: ConversationQueueDeps = {},
): Promise<Response> {
  const auth = await authorizedProject(request, deps);
  if (isResponse(auth)) return auth;
  const url = new URL(request.url);
  if (url.searchParams.has("eventsAfter")) {
    if (url.searchParams.has("expectedSessionId"))
      return json(
        {
          error: "eventsAfter is a global read and cannot select an exact lane",
        },
        400,
      );
    const afterSeq = Number(url.searchParams.get("eventsAfter"));
    if (!Number.isInteger(afterSeq) || afterSeq < 0) {
      return json({ error: "eventsAfter must be a non-negative integer" }, 400);
    }
    if (!deps.getWriteDb) {
      try {
        return json(
          await daemonQueueRead({ ...auth, eventsAfter: afterSeq }, deps),
        );
      } catch (error) {
        return json({ error: error instanceof Error ? error.message : String(error) }, 502);
      }
    }
    const db = await deps.getWriteDb();
    const requester = await registeredReadRequester(db, auth.requester);
    const [bounds, events] = await Promise.all([db.execute({
      sql:
        "SELECT COALESCE(MIN(CASE WHEN seq > ? THEN seq END), 0) AS next_seq, " +
        "COALESCE(MAX(seq), 0) AS max_seq FROM command_intent_events",
      args: [afterSeq],
    }),
      queueEvents(db, afterSeq, requester),
    ]);
    const nextSeq = Number(bounds.rows[0]?.next_seq ?? 0);
    const latestSeq = Number(bounds.rows[0]?.max_seq ?? 0);
    const gap = afterSeq > 0 && nextSeq > afterSeq + 1;
    return json({
      events: gap ? [] : events.events,
      nextSeq: gap ? nextSeq : events.scannedSeq,
      latestSeq,
      gap,
    });
  }
  const target = targetFromUrl(request);
  if (isResponse(target)) return target;
  if (!deps.getWriteDb) {
    try {
      return json(
        await daemonQueueRead(
          {
            ...auth,
        ...(target.name ? { name: target.name } : {}),
        ...(target.agentId ? { agentId: target.agentId } : {}),
    ...(target.expectedSessionId
      ? { expectedSessionId: target.expectedSessionId }
      : {}),
          },
          deps,
        ),
      );
    } catch (error) {
        return json({ error: error instanceof Error ? error.message : String(error) }, 502);
      }
  }
    const db = await deps.getWriteDb();
  const requester = await registeredReadRequester(db, auth.requester);
  if (
    target.expectedSessionId &&
    !(await retainedOwned(db, target.agentId!, target.expectedSessionId))
  ) {
    return json({ error: "exact session is not retained by this agent" }, 404);
  }
  const [runtime, seq] = await Promise.all([
    targetRuntime(db, target, true),
    latestQueueSeq(db),
  ]);
  if (target.expectedSessionId) runtime.sessionId = target.expectedSessionId;
  const observation = runtime.sessionId
    ? deps.observeTurn?.(runtime.sessionId) ?? {
        sessionId: runtime.sessionId, state: TurnState.Unknown, steerCapability: SteerCapability.None,
      }
    : undefined;
  const exactObservation = observation?.sessionId === runtime.sessionId ? observation : undefined;
  const snapshot: CommandQueueSnapshot = {
    target: runtime.name ?? target.name ?? target.agentId ?? "",
    sessionId: runtime.sessionId,
    turnActive: runtime.turnActive,
    steerCapability: exactObservation?.steerCapability ?? SteerCapability.None,
    seq,
    revision: seq,
    commands: await queueEntries(db, target, runtime.sessionId, requester),
    ...(exactObservation ? {observation: exactObservation} : {}),
  };
  if (exactObservation?.state === "nativeOpen") snapshot.turnActive = true;
  if (exactObservation?.state === "verifiedIdle") snapshot.turnActive = false;
  return json(snapshot);
}

/** Apply one compare-and-set mutation to the daemon-owned pending queue. */
export async function handleConversationQueuePost(
  request: Request,
  deps: ConversationQueueDeps = {},
): Promise<Response> {
  let body: CommandQueueMutationRequest;
  try {
    body = (await request.json()) as CommandQueueMutationRequest;
  } catch {
    return json({ error: "body must be JSON" }, 400);
  }
  const selectorError = exactSessionError(body);
  if (selectorError) return json({ error: selectorError }, 400);
  if (!body.name?.trim() && !body.agentId) {
    return mutationError(body, "name or agentId is required", 400);
  }
  if (!body.clientMutationId?.trim()) {
    return mutationError(body, "clientMutationId is required", 400);
  }
  const target: QueueTarget = {
    ...(body.name?.trim() ? { name: body.name.trim() } : {}),
    ...(body.agentId ? { agentId: String(body.agentId) } : {}),
    ...(body.expectedSessionId
      ? { expectedSessionId: body.expectedSessionId }
      : {}),
  };
  const auth = await authorizedProject(request, deps);
  if (isResponse(auth)) return auth;
  const now = (deps.now ?? Date.now)();
  if (!deps.getWriteDb) {
    try {
      const result = await daemonQueueMutation(
        { project: auth.project, now, request: body },
        deps,
      );
      const resultError =
        result.status >= 200 && result.status < 300
          ? exactSessionResultError(body.expectedSessionId, result.body)
          : undefined;
      if (resultError) return mutationError(body, resultError, 502);
      return json(result.body, result.status);
    } catch (error) {
      return mutationError(
        body,
        error instanceof Error ? error.message : String(error),
        502,
      );
    }
  }
  const db = await deps.getWriteDb();
  const requestJson = canonicalMutationRequest(body, target);
  let captured: CommandQueueSnapshot["observation"];
  try {
    const selected = target.expectedSessionId
      ? {sessionId:target.expectedSessionId} : await targetRuntime(db, target);
    captured = selected.sessionId ? deps.observeTurn?.(selected.sessionId) : undefined;
  } catch { /* Missing private evidence must not prevent committed replay. */ }
  const tx = await db.transaction("write");
  let transactionClosed = false;

  try {
    const replay = await tx.execute({
      sql:
        "SELECT request_json, response_status, response_json FROM command_queue_mutations " +
        "WHERE project = ? AND client_mutation_id = ? LIMIT 1",
      args: [auth.project, body.clientMutationId],
    });
    const previous = replay.rows[0];
    if (previous) {
      await tx.rollback();
      transactionClosed = true;
      if (String(previous.request_json) !== requestJson) {
        return mutationError(
          body,
          "clientMutationId was already used for a different mutation",
          409,
        );
      }
      if (typeof previous.response_json !== "string") {
        return mutationError(
          body,
          "queue mutation is still being committed",
          409,
        );
      }
      return json(
        JSON.parse(previous.response_json) as unknown,
        Number(previous.response_status ?? 200),
      );
    }
    await tx.execute({
      sql:
        "INSERT INTO command_queue_mutations " +
        "(project, client_mutation_id, request_json, response_status, response_json, created_at) " +
        "VALUES (?, ?, ?, NULL, NULL, ?)",
      args: [auth.project, body.clientMutationId, requestJson, now],
    });
    const runtime = await targetRuntime(tx, target);
    runtime.steerCapability = captured?.sessionId === runtime.sessionId
      ? captured?.steerCapability ?? SteerCapability.None : SteerCapability.None;
    const predicate = targetPredicate(target, runtime.sessionId);
    if (target.expectedSessionId) {
      // Conversion cannot grant an old row authority that was absent at its original enqueue.
      const requireOriginal = body.action === CommandQueueAction.RedirectNow;
      predicate.sql =
        `(${predicate.sql}) AND (` +
        (requireOriginal
          ? ""
          : "json_type(request_json, '$.expectedSessionId') IS NULL OR ") +
        "json_extract(request_json, '$.expectedSessionId') = ?)";
      predicate.args.push(target.expectedSessionId);
    }

    const finish = async (
      responseBody: Record<string, unknown>,
      status = 200,
    ): Promise<Response> => {
      await tx.execute({
        sql:
          "UPDATE command_queue_mutations SET response_status = ?, response_json = ? " +
          "WHERE project = ? AND client_mutation_id = ?",
        args: [
          status,
          JSON.stringify(responseBody),
          auth.project,
          body.clientMutationId,
        ],
      });
      await tx.commit();
      transactionClosed = true;
      return json(responseBody, status);
    };
    const fail = (error: string, status: number) =>
      finish(mutationErrorBody(body, error), status);

    if (
      target.expectedSessionId &&
      runtime.sessionId !== target.expectedSessionId
    ) {
      return await fail(
        "expected session is not the active owned runtime binding",
        409,
      );
    }

    switch (body.action) {
      case CommandQueueAction.RedirectNow: {
        if (!body.commandId || body.expectedRevision == null) {
          return await fail("commandId and expectedRevision are required", 400);
        }
        if (runtime.steerCapability === SteerCapability.None) {
          return await fail(
            "target harness cannot redirect an active turn",
            409,
          );
        }
        if (!runtime.turnActive) {
          return await fail("target has no active turn to redirect", 409);
        }
        const changed = await tx.execute({
          sql:
            "UPDATE command_intents SET kind = ?, revision = revision + 1 " +
            "WHERE command_id = ? AND project = ? AND kind = ? AND status = 'pending' " +
            "AND revision = ? AND " +
            predicate.sql +
            " RETURNING revision, COALESCE((SELECT MAX(seq) FROM command_intent_events " +
            "WHERE project = ?), 0) AS seq",
          args: [
            STEER,
            body.commandId,
            auth.project,
            PROMPT,
            body.expectedRevision,
            ...predicate.args,
            auth.project,
          ],
        });
        // libSQL reports rowsAffected=0 for UPDATE ... RETURNING; the returned row is the CAS proof.
        if (changed.rows.length !== 1) {
          return await fail(
            "command revision changed or command is no longer queued",
            409,
          );
        }
        return await finish(
          mutationResponse(
            runtime,
            body.clientMutationId,
            CommandQueueState.Queued,
            Number(changed.rows[0]?.seq ?? 0),
            body.commandId,
            Number(changed.rows[0]?.revision ?? body.expectedRevision + 1),
          ),
        );
      }
      case CommandQueueAction.Cancel: {
        if (!body.commandId || body.expectedRevision == null) {
          return await fail("commandId and expectedRevision are required", 400);
        }
        const changed = await tx.execute({
          sql:
            "UPDATE command_intents SET status = 'cancelled', revision = revision + 1, " +
            "completed_at = ?, lease_until = NULL WHERE command_id = ? AND project = ? " +
            "AND kind = ? AND status = 'pending' AND revision = ? AND " +
            predicate.sql +
            " RETURNING revision, COALESCE((SELECT MAX(seq) FROM command_intent_events " +
            "WHERE project = ?), 0) AS seq",
          args: [
            now,
            body.commandId,
            auth.project,
            PROMPT,
            body.expectedRevision,
            ...predicate.args,
            auth.project,
          ],
        });
        if (changed.rows.length !== 1) {
          return await fail(
            "command revision changed or command is no longer queued",
            409,
          );
        }
        return await finish(
          mutationResponse(
            runtime,
            body.clientMutationId,
            CommandQueueState.Cancelled,
            Number(changed.rows[0]?.seq ?? 0),
            body.commandId,
            Number(changed.rows[0]?.revision ?? body.expectedRevision + 1),
          ),
        );
      }
      case CommandQueueAction.Edit: {
        if (
          !body.commandId ||
          body.expectedRevision == null ||
          !body.text?.trim()
        ) {
          return await fail(
            "commandId, expectedRevision, and text are required",
            400,
          );
        }
        const changed = await tx.execute({
          sql:
            "UPDATE command_intents SET request_json = json_set(request_json, '$.text', ?), " +
            "revision = revision + 1 WHERE command_id = ? AND project = ? AND kind = ? " +
            "AND status = 'pending' AND revision = ? AND " +
            predicate.sql +
            " RETURNING revision, COALESCE((SELECT MAX(seq) FROM command_intent_events " +
            "WHERE project = ?), 0) AS seq",
          args: [
            body.text,
            body.commandId,
            auth.project,
            PROMPT,
            body.expectedRevision,
            ...predicate.args,
            auth.project,
          ],
        });
        if (changed.rows.length !== 1) {
          return await fail(
            "command revision changed or command is no longer queued",
            409,
          );
        }
        return await finish(
          mutationResponse(
            runtime,
            body.clientMutationId,
            CommandQueueState.Queued,
            Number(changed.rows[0]?.seq ?? 0),
            body.commandId,
            Number(changed.rows[0]?.revision ?? body.expectedRevision + 1),
          ),
        );
      }
      case CommandQueueAction.Reorder: {
        const requestedIds = body.commandIds ?? [];
        const ids = [...new Set(requestedIds)];
        if (ids.length === 0 || ids.length !== requestedIds.length) {
          return await fail("commandIds must be a non-empty unique list", 400);
        }
        const expected = new Map(
          (body.expectedRevisions ?? []).map((entry) => [
            entry.commandId,
            entry.revision,
          ]),
        );
        if (
          expected.size !== ids.length ||
          ids.some((id) => !expected.has(id))
        ) {
          return await fail(
            "expectedRevisions must cover every commandId",
            400,
          );
        }
        const placeholders = ids.map(() => "?").join(", ");
        const rows = await tx.execute({
          sql:
            `SELECT command_id, created_at, revision FROM command_intents WHERE command_id IN (${placeholders}) ` +
            "AND project = ? AND kind = ? AND status = 'pending' AND " +
            predicate.sql,
          args: [...ids, auth.project, PROMPT, ...predicate.args],
        });
        if (rows.rows.length !== ids.length) {
          return await fail("one or more commands are no longer queued", 409);
        }
        if (
          rows.rows.some(
            (row) =>
              Number(row.revision) !== expected.get(String(row.command_id)),
          )
        ) {
          return await fail("one or more command revisions changed", 409);
        }
        const base = Math.min(
          ...rows.rows.map((row) => Number(row.created_at ?? now)),
        );
        const orderCase = ids.map(() => "WHEN ? THEN ?").join(" ");
        const expectedPairs = ids
          .map(() => "(command_id = ? AND revision = ?)")
          .join(" OR ");
        const changed = await tx.execute({
          sql:
            `UPDATE command_intents SET created_at = CASE command_id ${orderCase} ELSE created_at END, ` +
            "revision = revision + 1 WHERE project = ? AND kind = ? AND status = 'pending' " +
            "AND " +
            predicate.sql +
            ` AND command_id IN (${placeholders}) ` +
            `AND ? = (SELECT COUNT(*) FROM command_intents WHERE project = ? AND kind = ? ` +
            `AND status = 'pending' AND (${expectedPairs})) ` +
            "RETURNING revision, COALESCE((SELECT MAX(seq) FROM command_intent_events " +
            "WHERE project = ?), 0) AS seq",
          args: [
            ...ids.flatMap((id, index) => [id, base + index]),
            auth.project,
            PROMPT,
            ...predicate.args,
            ...ids,
            ids.length,
            auth.project,
            PROMPT,
            ...ids.flatMap((id) => [id, expected.get(id)!]),
            auth.project,
          ],
        });
        if (changed.rows.length !== ids.length) {
          return await fail("one or more command revisions changed", 409);
        }
        return await finish(
          mutationResponse(
            runtime,
            body.clientMutationId,
            CommandQueueState.Queued,
            Math.max(...changed.rows.map((row) => Number(row.seq ?? 0))),
          ),
        );
      }
      default:
        return await fail(`unsupported action: ${String(body.action)}`, 400);
    }
  } catch (error) {
    if (!transactionClosed) {
      await tx.rollback().catch(() => undefined);
      transactionClosed = true;
    }
    return mutationError(
      body,
      error instanceof Error ? error.message : String(error),
      502,
    );
  }
}
