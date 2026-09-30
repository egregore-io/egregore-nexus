// Daemon-owned command ingress.
//
// Production gateway producers submit over the daemon's boot-scoped local IPC
// endpoint. A direct Client can be injected only by focused unit tests exercising
// the legacy row/receipt algorithms without opening the canonical store.
import { randomUUID } from "node:crypto";

import type { Client } from "@libsql/client";

import { GatewayError } from "@server/api/http";
import {
  callDaemonCommand,
  callDaemonEnqueue,
  DaemonIpcError,
  readDaemonBootId,
  type DaemonIpcCaller,
  type DaemonIpcCallOptions,
  type DaemonIpcCommandOptions,
} from "@server/daemon/ipc";
import type {
  CommandIntentSender,
  GatewayCallerIdentity,
} from "@server/api/http";
import { Kind, Tier } from "@shared/types";
import { beginIngress, settleIngress, type GatewayIngressRow } from "@server/store/repos/ingress";

export const COMMAND_KINDS = {
  messagePostSend: "message.post.send",
  metadataSet: "metadata.set",
  notificationNotify: "notification.notify",
  notificationSend: "notification.send",
  inboxConsume: "inbox.consume",
  inboxAck: "inbox.ack",
  inboxAckThreads: "inbox.ack_threads",
  identityRegister: "identity.register",
  identityRename: "identity.rename",
  presenceStatus: "presence.status",
  presenceHeartbeat: "presence.heartbeat",
  topicSubscribe: "topic.subscribe",
  topicUnsubscribe: "topic.unsubscribe",
  threadCreate: "thread.create",
  threadJoin: "thread.join",
  threadLeave: "thread.leave",
  threadArchive: "thread.archive",
  threadDelete: "thread.delete",
  threadRename: "thread.rename",
  threadAddMember: "thread.add_member",
  threadRemoveMember: "thread.remove_member",
  harnessPrompt: "harness.prompt",
  harnessSteer: "harness.steer",
  harnessInterrupt: "harness.interrupt",
  harnessCompact: "harness.compact",
  harnessWarm: "harness.warm",
  adminSpawn: "admin.spawn",
  adminRemove: "admin.remove",
  adminEvict: "admin.evict",
  adminDelete: "admin.delete",
  adminAssignRole: "admin.assign_role",
  adminAssignProject: "admin.assign_project",
  adminGrantTier: "admin.grant_tier",
  adminChannel: "admin.channel",
  adminRoute: "admin.route",
  adminMonitor: "admin.monitor",
  agentGrantAccess: "agent.grant_access",
  agentRevokeAccess: "agent.revoke_access",
  agentTransferOwner: "agent.transfer_owner",
  agentCredentialCreate: "agent.credential.create",
  agentCredentialRevoke: "agent.credential.revoke",
  sourceRegister: "source.register",
  sourceEnable: "source.enable",
  sourceDisable: "source.disable",
  sourceRotate: "source.rotate",
  sourceRemove: "source.remove",
  sourcePush: "source.push",
} as const;

type DbProvider = Client | (() => Client | Promise<Client>);

export interface CommandIngressOptions {
  /** Explicit direct-store unit-test seam; production must leave this unset. */
  db?: DbProvider;
  /** Gateway-local idempotency journal around daemon IPC. */
  ingressDb?: DbProvider;
  /** Bounded wait for the daemon worker to complete the row. */
  timeoutMs?: number;
  /** Poll cadence while waiting for `done` or `error`. */
  pollIntervalMs?: number;
  /** Test seam for deterministic command ids. */
  genCommandId?: () => string;
  /** Test seam for deterministic timeouts. */
  now?: () => number;
  /** Test seam that avoids real timers in direct unit tests. */
  sleep?: (ms: number) => Promise<void>;
  /** Return this result instead of timing out when the command is in the named status. */
  timeoutResultsByStatus?: Record<string, unknown>;
  /** Nexus home containing the daemon's boot-scoped IPC manifest. */
  nexusHome?: string;
  /** Test seam for a held daemon command call. Production uses `callDaemonCommand`. */
  daemonCommand?: (
    kind: string,
    params: unknown,
    caller: DaemonIpcCaller,
    options: DaemonIpcCommandOptions,
  ) => Promise<unknown>;
  /** Test seam for a durable-acceptance daemon call. Production uses `callDaemonEnqueue`. */
  daemonEnqueue?: (
    kind: string,
    params: unknown,
    caller: DaemonIpcCaller,
    options: DaemonIpcCommandOptions,
  ) => Promise<unknown>;
  /** Test seam for the daemon boot epoch used by human-principal rebinding. */
  daemonBootId?: (options?: DaemonIpcCallOptions) => Promise<string>;
}

interface CommandPollRow {
  status: string;
  revision: number;
  resultJson?: string;
  errorJson?: string;
}

/** Stable receipt returned as soon as a command row exists durably. */
export interface CommandEnqueueReceipt {
  commandId: string;
  status: string;
  createdAt: number;
  revision: number;
  sessionId?: string;
  seq: number;
}

interface CommandCallerRow {
  project: string;
  name: string;
  sessionId?: string;
  agentId?: string;
  runtimeId?: string;
  clientKey?: string;
  kind: string;
  tier: string;
}

const DEFAULT_TIMEOUT_MS = 10_000;
const DEFAULT_POLL_INTERVAL_MS = 100;

interface HumanDaemonBinding {
  bootId: string;
  sessionId: string;
}

const humanDaemonBindings = new Map<string, HumanDaemonBinding>();
const pendingHumanRebinds = new Map<string, Promise<HumanDaemonBinding>>();

/** Build a generic command sender backed by `command_intents`. */
export function createCommandIngressSubmitter(
  opts: CommandIngressOptions = {},
): CommandIntentSender {
  return {
    submit: (kind, req, caller, submitOpts) =>
      submitCommandIntent(kind, req, caller, opts, submitOpts?.idempotencyKey),
  };
}

/**
 * Submit one daemon command intent and return the daemon-written result.
 *
 * Callers must pass an explicit Principal. Local zero-login is represented by
 * `localOperatorCaller()` in the local web facet, not by a hidden fallback here.
 */
export async function submitCommandIntent<T = unknown>(
  kind: string,
  req: unknown,
  caller?: GatewayCallerIdentity,
  opts: CommandIngressOptions = {},
  idempotencyKey?: string,
): Promise<T> {
  const currentCaller = await ensureHumanCallerForDaemonBoot(kind, caller, opts);
  const commandCaller = callerRow(currentCaller);
  if (!opts.db) {
    const invoke = opts.daemonCommand ?? callDaemonCommand;
    const commandId = opts.genCommandId?.() ?? `cmd_${randomUUID()}`;
    if (opts.ingressDb && idempotencyKey) {
      return submitThroughGatewayIngress<T>({
        db: await resolveDb(opts.ingressDb),
        kind,
        req,
        caller: commandCaller,
        idempotencyKey,
        commandId,
        now: opts.now ?? Date.now,
        invoke: (stableCommandId) => invoke(kind, req, daemonCaller(commandCaller), {
          commandId: stableCommandId,
          idempotencyKey,
          ...(opts.nexusHome ? { nexusHome: opts.nexusHome } : {}),
          timeoutMs: opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,
        }) as Promise<T>,
      });
    }
    try {
      return await invoke(kind, req, daemonCaller(commandCaller), {
        commandId,
        ...(idempotencyKey ? { idempotencyKey } : {}),
        ...(opts.nexusHome ? { nexusHome: opts.nexusHome } : {}),
        timeoutMs: opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,
      }) as T;
    } catch (error) {
      throw gatewayIpcError(error);
    }
  }
  const db = await resolveDb(opts.db);
  const receipt = await enqueueCommandIntent(kind, req, caller, { ...opts, db }, idempotencyKey);
  return pollForResult<T>(db, receipt.commandId, receipt.createdAt, opts);
}

/**
 * Persist one command intent and return its stable row identity without waiting for execution.
 * Retry-prone callers pass `idempotencyKey`; a replay returns the original command id and current
 * status instead of inserting a client-side shadow queue row.
 */
export async function enqueueCommandIntent(
  kind: string,
  req: unknown,
  caller?: GatewayCallerIdentity,
  opts: CommandIngressOptions = {},
  idempotencyKey?: string,
): Promise<CommandEnqueueReceipt> {
  const now = opts.now ?? Date.now;
  const createdAt = now();
  const commandId = opts.genCommandId?.() ?? `cmd_${randomUUID()}`;
  const currentCaller = await ensureHumanCallerForDaemonBoot(kind, caller, opts);
  const commandCaller = callerRow(currentCaller);
  if (!opts.db) {
    const invoke = opts.daemonEnqueue ?? callDaemonEnqueue;
    if (opts.ingressDb && idempotencyKey) {
      return submitThroughGatewayIngress<CommandEnqueueReceipt>({
        db: await resolveDb(opts.ingressDb),
        kind,
        req,
        caller: commandCaller,
        idempotencyKey,
        commandId,
        now,
        invoke: (stableCommandId) => invoke(kind, req, daemonCaller(commandCaller), {
          commandId: stableCommandId,
          idempotencyKey,
          ...(opts.nexusHome ? { nexusHome: opts.nexusHome } : {}),
          timeoutMs: opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,
        }) as Promise<CommandEnqueueReceipt>,
      });
    }
    try {
      return await invoke(kind, req, daemonCaller(commandCaller), {
        commandId,
        ...(idempotencyKey ? { idempotencyKey } : {}),
        ...(opts.nexusHome ? { nexusHome: opts.nexusHome } : {}),
        timeoutMs: opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,
      }) as CommandEnqueueReceipt;
    } catch (error) {
      throw gatewayIpcError(error);
    }
  }
  const db = await resolveDb(opts.db);
  return insertCommandIntent(db, {
    commandId,
    kind,
    req,
    commandCaller,
    createdAt,
    idempotencyKey: cleanEnv(idempotencyKey),
  });
}

async function ensureHumanCallerForDaemonBoot(
  commandKind: string,
  caller: GatewayCallerIdentity | undefined,
  opts: CommandIngressOptions,
): Promise<GatewayCallerIdentity | undefined> {
  if (
    opts.db ||
    commandKind === COMMAND_KINDS.identityRegister ||
    caller?.credentialFacet !== "human"
  ) {
    return caller;
  }
  if (!caller.clientKey) {
    throw new GatewayError(401, "human command caller is missing its stable client key");
  }

  let bootId: string;
  try {
    bootId = await (opts.daemonBootId ?? readDaemonBootId)(
      opts.nexusHome ? { nexusHome: opts.nexusHome } : {},
    );
  } catch (error) {
    throw gatewayIpcError(error);
  }
  const bindingKey = `${opts.nexusHome ?? "<default>"}:${caller.clientKey}`;
  const cached = humanDaemonBindings.get(bindingKey);
  const binding = cached?.bootId === bootId
    ? cached
    : await rebindHumanCaller(bindingKey, bootId, caller, opts);

  const reboundCaller = {
    ...caller,
    sessionId: binding.sessionId,
    runtimeId: binding.sessionId,
  };
  delete reboundCaller.agentId;
  return reboundCaller;
}

async function rebindHumanCaller(
  bindingKey: string,
  bootId: string,
  caller: GatewayCallerIdentity,
  opts: CommandIngressOptions,
): Promise<HumanDaemonBinding> {
  const pendingKey = `${bindingKey}:${bootId}`;
  const existing = pendingHumanRebinds.get(pendingKey);
  if (existing) return existing;

  const pending = registerHumanCaller(bootId, caller, opts);
  pendingHumanRebinds.set(pendingKey, pending);
  try {
    const binding = await pending;
    humanDaemonBindings.set(bindingKey, binding);
    return binding;
  } finally {
    pendingHumanRebinds.delete(pendingKey);
  }
}

async function registerHumanCaller(
  bootId: string,
  caller: GatewayCallerIdentity,
  opts: CommandIngressOptions,
): Promise<HumanDaemonBinding> {
  const invoke = opts.daemonCommand ?? callDaemonCommand;
  const clientKey = caller.clientKey as string;
  try {
    const response = await invoke(
      COMMAND_KINDS.identityRegister,
      {
        name: caller.name,
        harness: "other",
        harnessSessionId: `hs_${clientKey}`,
        project: caller.project,
        clientKey,
        tier: caller.tier ?? Tier.Admin,
        kind: Kind.Human,
      },
      daemonCaller(callerRow(caller)),
      {
        commandId: `cmd_${randomUUID()}`,
        ...(opts.nexusHome ? { nexusHome: opts.nexusHome } : {}),
        timeoutMs: opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,
      },
    ) as { sessionId?: unknown };
    if (typeof response?.sessionId !== "string" || !response.sessionId) {
      throw new GatewayError(502, "daemon human rebind returned no sessionId");
    }
    return {
      bootId,
      sessionId: response.sessionId,
    };
  } catch (error) {
    throw gatewayIpcError(error);
  }
}

interface GatewayIngressSubmission<T> {
  db: Client;
  kind: string;
  req: unknown;
  caller: CommandCallerRow;
  idempotencyKey: string;
  commandId: string;
  now: () => number;
  invoke: (commandId: string) => Promise<T>;
}

async function submitThroughGatewayIngress<T>(input: GatewayIngressSubmission<T>): Promise<T> {
  const key = ingressScope(input);
  const begun = await beginIngress(input.db, {
    idempotencyKey: key,
    commandId: input.commandId,
    request: { kind: input.kind, params: input.req },
    now: input.now(),
  });
  if (!begun.created) {
    if (begun.row.status === "accepted") return begun.row.result as T;
    if (begun.row.status === "rejected") throwIngressError(begun.row);
  }
  const commandId = begun.row.commandId ?? input.commandId;
  try {
    const result = await input.invoke(commandId);
    await settleIngress(input.db, key, {
      commandId,
      status: "accepted",
      result,
      now: input.now(),
    });
    return result;
  } catch (error) {
    const mapped = gatewayIpcError(error);
    if (isTerminalDaemonRejection(error)) {
      await settleIngress(input.db, key, {
        commandId,
        status: "rejected",
        error: ingressError(mapped),
        now: input.now(),
      });
    }
    throw mapped;
  }
}

function ingressScope(input: GatewayIngressSubmission<unknown>): string {
  const principal = input.caller.clientKey ?? input.caller.sessionId ?? input.caller.agentId ?? input.caller.name;
  return `${input.kind}:${principal}:${input.idempotencyKey}`;
}

function isTerminalDaemonRejection(error: unknown): boolean {
  return error instanceof DaemonIpcError && error.code !== undefined;
}

function ingressError(error: unknown): { code: number; message: string; details?: unknown } {
  if (error instanceof GatewayError) {
    return { code: error.code, message: error.message, ...(error.details === undefined ? {} : { details: error.details }) };
  }
  return { code: 502, message: error instanceof Error ? error.message : String(error) };
}

function throwIngressError(row: GatewayIngressRow): never {
  const error = row.error as { code?: unknown; message?: unknown; details?: unknown } | undefined;
  throw new GatewayError(
    typeof error?.code === "number" ? error.code : 502,
    typeof error?.message === "string" ? error.message : "daemon command was rejected",
    error?.details,
  );
}

interface CommandInsert {
  commandId: string;
  kind: string;
  req: unknown;
  commandCaller: CommandCallerRow;
  createdAt: number;
  idempotencyKey?: string;
}

async function insertCommandIntent(
  db: Client,
  insert: CommandInsert,
): Promise<CommandEnqueueReceipt> {
  if (insert.idempotencyKey) {
    const existing = await findIdempotentCommand(db, insert);
    if (existing) return existing;
  }
  try {
    const inserted = await db.execute({
      sql:
        "INSERT INTO command_intents " +
        "(command_id, kind, status, project, caller_name, caller_session_id, " +
        "caller_agent_id, caller_runtime_id, caller_client_key, caller_kind, caller_tier, " +
        "idempotency_key, request_json, result_json, error_json, attempts, created_at, claimed_at, started_at, lease_until, " +
        "completed_at) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, ?, " +
        "NULL, NULL, NULL, NULL) " +
        "RETURNING command_id, status, created_at, revision, " +
        "(SELECT session_id FROM command_intent_events e WHERE e.command_id = command_intents.command_id " +
        "ORDER BY seq DESC LIMIT 1) AS session_id, " +
        "COALESCE((SELECT seq FROM command_intent_events e WHERE e.command_id = command_intents.command_id " +
        "ORDER BY seq DESC LIMIT 1), 0) AS seq",
      args: [
        insert.commandId,
        insert.kind,
        insert.commandCaller.project,
        insert.commandCaller.name,
        insert.commandCaller.sessionId ?? null,
        insert.commandCaller.agentId ?? null,
        insert.commandCaller.runtimeId ?? insert.commandCaller.sessionId ?? null,
        insert.commandCaller.clientKey ?? null,
        insert.commandCaller.kind,
        insert.commandCaller.tier,
        insert.idempotencyKey ?? null,
        JSON.stringify(insert.req),
        insert.createdAt,
      ],
    });
    const row = inserted.rows[0];
    if (!row) throw new GatewayError(502, `command intent disappeared: ${insert.commandId}`);
    return commandReceipt(row as Record<string, unknown>);
  } catch (error) {
    if (!insert.idempotencyKey || !isUniqueConstraint(error)) throw error;
  }

  const existing = await findIdempotentCommand(db, insert);
  if (existing) return existing;
  throw errorFromUniqueMiss();
}

async function findIdempotentCommand(
  db: Client,
  insert: CommandInsert,
): Promise<CommandEnqueueReceipt | undefined> {
  if (!insert.idempotencyKey) return undefined;
  // Queue→redirect promotion changes the authoritative row kind in place. Treat prompt and steer
  // as one composer idempotency domain so an ACK-loss retry of the original prompt resolves the
  // promoted row instead of inserting a duplicate boundary turn.
  const kinds = insert.kind === COMMAND_KINDS.harnessPrompt || insert.kind === COMMAND_KINDS.harnessSteer
    ? [COMMAND_KINDS.harnessPrompt, COMMAND_KINDS.harnessSteer]
    : [insert.kind];
  const kindPlaceholders = kinds.map(() => "?").join(", ");
  const existing = await db.execute({
    sql:
      "SELECT command_id, status, created_at, revision, " +
      "(SELECT session_id FROM command_intent_events e WHERE e.command_id = command_intents.command_id " +
      "ORDER BY seq DESC LIMIT 1) AS session_id, " +
      "COALESCE((SELECT seq FROM command_intent_events e WHERE e.command_id = command_intents.command_id " +
      "ORDER BY seq DESC LIMIT 1), 0) AS seq " +
      `FROM command_intents WHERE project = ? AND kind IN (${kindPlaceholders}) ` +
      "AND COALESCE(caller_client_key, caller_session_id, caller_name) = COALESCE(?, ?, ?) " +
      "AND idempotency_key = ? LIMIT 1",
    args: [
      insert.commandCaller.project,
      ...kinds,
      insert.commandCaller.clientKey ?? null,
      insert.commandCaller.sessionId ?? null,
      insert.commandCaller.name,
      insert.idempotencyKey,
    ],
  });
  const row = existing.rows[0];
  return row ? commandReceipt(row as Record<string, unknown>) : undefined;
}

function commandReceipt(row: Record<string, unknown>): CommandEnqueueReceipt {
  const commandId = String(row.command_id ?? "");
  if (!commandId) throw new GatewayError(502, "command intent returned no command id");
  return {
    commandId,
    status: String(row.status ?? "pending"),
    createdAt: Number(row.created_at ?? 0),
    revision: Number(row.revision ?? 1),
    ...(typeof row.session_id === "string" ? { sessionId: row.session_id } : {}),
    seq: Number(row.seq ?? 0),
  };
}

function isUniqueConstraint(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.toLowerCase().includes("unique");
}

function errorFromUniqueMiss(): Error {
  return new Error("idempotent command insert collided but existing row was not found");
}

async function resolveDb(db: DbProvider): Promise<Client> {
  return typeof db === "function" ? db() : db;
}

function callerRow(caller: GatewayCallerIdentity | undefined): CommandCallerRow {
  if (!caller) {
    throw new GatewayError(401, "command intent requires an explicit Principal");
  }
  return {
    project: caller.project,
    name: caller.name,
    sessionId: caller.sessionId,
    agentId: caller.agentId,
    runtimeId: caller.runtimeId ?? caller.sessionId,
    clientKey: caller.clientKey,
    kind: caller.kind ?? Kind.Human,
    tier: caller.tier ?? Tier.Admin,
  };
}

function daemonCaller(caller: CommandCallerRow): DaemonIpcCaller {
  return {
    name: caller.name,
    project: caller.project,
    ...(caller.sessionId ? { sessionId: caller.sessionId } : {}),
    ...(caller.agentId ? { agentId: caller.agentId } : {}),
    ...(caller.runtimeId ? { runtimeId: caller.runtimeId } : {}),
    ...(caller.clientKey ? { clientKey: caller.clientKey } : {}),
    kind: caller.kind as DaemonIpcCaller["kind"],
    tier: caller.tier as DaemonIpcCaller["tier"],
  };
}

function gatewayIpcError(error: unknown): unknown {
  if (!(error instanceof DaemonIpcError)) return error;
  const status = error.code === undefined
    ? 503
    : error.code === -32602
    ? 400
    : error.code === -32001
      ? 401
      : error.code === -32003
        ? 404
        : error.code === -32004
          ? 403
          : 502;
  return new GatewayError(status, error.message);
}

async function pollForResult<T>(
  db: Client,
  commandId: string,
  startedAt: number,
  opts: CommandIngressOptions,
): Promise<T> {
  return pollHub(db).wait<T>(commandId, startedAt, opts);
}

interface PollWaiter {
  commandId: string;
  deadline: number;
  timeoutMs: number;
  pollIntervalMs: number;
  now: () => number;
  sleep: (ms: number) => Promise<void>;
  timeoutResultsByStatus?: Record<string, unknown>;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}

const commandPollHubs = new WeakMap<Client, CommandPollHub>();

function pollHub(db: Client): CommandPollHub {
  let hub = commandPollHubs.get(db);
  if (!hub) {
    hub = new CommandPollHub(db);
    commandPollHubs.set(db, hub);
  }
  return hub;
}

class CommandPollHub {
  private readonly waiters = new Map<string, Set<PollWaiter>>();
  private running = false;

  constructor(private readonly db: Client) {}

  wait<T>(
    commandId: string,
    startedAt: number,
    opts: CommandIngressOptions,
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
      const waiter: PollWaiter = {
        commandId,
        deadline: startedAt + timeoutMs,
        timeoutMs,
        pollIntervalMs: opts.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
        now: opts.now ?? Date.now,
        sleep: opts.sleep ?? delay,
        timeoutResultsByStatus: opts.timeoutResultsByStatus,
        resolve: resolve as (value: unknown) => void,
        reject,
      };
      const commandWaiters = this.waiters.get(commandId) ?? new Set<PollWaiter>();
      commandWaiters.add(waiter);
      this.waiters.set(commandId, commandWaiters);
      this.start();
    });
  }

  private start(): void {
    if (this.running) return;
    this.running = true;
    void this.run();
  }

  private async run(): Promise<void> {
    // Coalesce submissions that complete their INSERTs in the same event-loop turn.
    await delay(0);
    try {
      while (this.waiters.size > 0) {
        const commandIds = [...this.waiters.keys()];
        const rows = await loadCommands(this.db, commandIds);
        for (const commandId of commandIds) {
          const commandWaiters = this.waiters.get(commandId);
          if (!commandWaiters) continue;
          const row = rows.get(commandId);
          for (const waiter of [...commandWaiters]) {
            if (!row) {
              this.settle(waiter, undefined, new GatewayError(502, `command intent disappeared: ${commandId}`));
            } else if (row.status === "done") {
              this.settle(waiter, parseCommandResult(row.resultJson));
            } else if (row.status === "error") {
              try {
                throwCommandError(row.errorJson);
              } catch (error) {
                this.settle(waiter, undefined, error);
              }
            } else if (waiter.now() >= waiter.deadline) {
              if (Object.hasOwn(waiter.timeoutResultsByStatus ?? {}, row.status)) {
                this.settle(waiter, waiter.timeoutResultsByStatus?.[row.status]);
              } else {
                this.settle(
                  waiter,
                  undefined,
                  new GatewayError(504, `command intent timed out after ${waiter.timeoutMs}ms`),
                );
              }
            }
          }
        }
        if (this.waiters.size === 0) break;

        const sleepers = new Map<(ms: number) => Promise<void>, number>();
        for (const commandWaiters of this.waiters.values()) {
          for (const waiter of commandWaiters) {
            sleepers.set(
              waiter.sleep,
              Math.min(sleepers.get(waiter.sleep) ?? Number.POSITIVE_INFINITY, waiter.pollIntervalMs),
            );
          }
        }
        await Promise.all([...sleepers].map(([sleep, ms]) => sleep(ms)));
      }
    } catch (error) {
      for (const commandWaiters of this.waiters.values()) {
        for (const waiter of commandWaiters) waiter.reject(error);
      }
      this.waiters.clear();
    } finally {
      this.running = false;
      if (this.waiters.size > 0) this.start();
    }
  }

  private settle(waiter: PollWaiter, value?: unknown, error?: unknown): void {
    const commandWaiters = this.waiters.get(waiter.commandId);
    commandWaiters?.delete(waiter);
    if (commandWaiters?.size === 0) this.waiters.delete(waiter.commandId);
    if (error !== undefined) waiter.reject(error);
    else waiter.resolve(value);
  }
}

async function loadCommands(
  db: Client,
  commandIds: readonly string[],
): Promise<Map<string, CommandPollRow>> {
  if (commandIds.length === 0) return new Map();
  const placeholders = commandIds.map(() => "?").join(", ");
  const res = await db.execute({
    sql:
      "SELECT command_id, status, revision, result_json, error_json " +
      `FROM command_intents WHERE command_id IN (${placeholders})`,
    args: [...commandIds],
  });
  const rows = new Map<string, CommandPollRow>();
  for (const row of res.rows) {
    rows.set(String(row.command_id), {
      status: String(row.status),
      revision: Number(row.revision ?? 1),
      resultJson: row.result_json == null ? undefined : String(row.result_json),
      errorJson: row.error_json == null ? undefined : String(row.error_json),
    });
  }
  return rows;
}

function parseCommandResult<T>(raw: string | undefined): T {
  if (!raw) return undefined as T;
  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new GatewayError(502, "command intent returned invalid JSON");
  }
}

function throwCommandError(raw: string | undefined): never {
  let parsed: unknown = undefined;
  try {
    parsed = raw ? JSON.parse(raw) : undefined;
  } catch {
    parsed = undefined;
  }
  const error = parsed as { code?: unknown; message?: unknown } | undefined;
  const code = typeof error?.code === "number" ? error.code : 502;
  const message =
    typeof error?.message === "string"
      ? error.message
      : "command intent failed";
  throw new GatewayError(code, message, parsed);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cleanEnv(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
