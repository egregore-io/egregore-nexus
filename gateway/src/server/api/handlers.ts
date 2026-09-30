// The public-API handlers — each a THIN translator. A handler is EXACTLY one of:
//   • a command-ingress write → `deps.commands.submit(...)`,
//   • a Message Post write → `deps.messagePost.send(...)` when injected,
//   • a read               → a read-view query over the lazily-resolved handle
//     (`await deps.db()` — built only when a read actually runs).
// Message-bearing reads also call the narrow `humanReads` receipt seam, when
// injected, so human browser sessions can drain their own delivery queue without
// putting writes in `server/read`. Path params (`:name`, `:id`) arrive in
// `ctx.params`; the request body is validated with the matching Zod schema (→ 400
// on malformed).
//
// Command-ingress params are contract-shaped: if a Zod schema stops matching the
// DTO the daemon expects, this file fails to compile instead of silently
// drifting from the wire contract.
import { createHmac, timingSafeEqual } from "node:crypto";
import type { ApiDeps, GatewayCallerIdentity, Handler } from "./http";
import { ok, fail, parseBody, GatewayError } from "./http";
import {
  notifySchema,
  notifySendSchema,
  sendSchema,
  spawnSchema,
  agentCredentialCreateSchema,
  assignRoleSchema,
  assignProjectSchema,
  grantTierSchema,
  agentAccessGrantSchema,
  agentOwnerTransferSchema,
  renameSchema,
  createThreadSchema,
  addThreadMemberSchema,
  statusSchema,
  subscribeSchema,
  consumeSchema,
  ackSchema,
  ackThreadsSchema,
  registerSchema,
  channelSchema,
  routeForwardSchema,
  monitorSchema,
  routeRuleSchema,
  sourceRegisterSchema,
  sourcePushSchema,
  metadataSchema,
  renameThreadSchema,
} from "./schemas";
import {
  listThreads,
  threadHistory,
  dmHistory,
  listMembers,
  searchMessages,
  listTopics,
  listNotifications,
  listRoutingRules,
  listProjects,
  threadHeader,
  threadMembersFor,
  listSources,
  sourceByName,
  sourceSecretByName,
  agentShow,
  agentRuntimesFor,
  listRuntimes,
} from "@server/read/queries";
import { messageById } from "@server/messagePost/readView";
import {
  entityMetadata,
  type MetadataEntityKind,
} from "@server/read/metadata";
import { COMMAND_KINDS } from "@server/command/ingress";
import {
  canonicalDmHistory,
  canonicalAgentRuntimesFor,
  canonicalAgentShow,
  canonicalHistory,
  canonicalMembers,
  canonicalMetadata,
  canonicalMessageById,
  canonicalNotifications,
  canonicalProjects,
  canonicalRoutingRules,
  canonicalRuntimes,
  canonicalSearch,
  canonicalThreadHistory,
  canonicalThreadHeader,
  canonicalThreadMembers,
  canonicalThreads,
  canonicalTopics,
} from "@server/read/canonical";
import { longPollCanonicalPage } from "@server/messagePost/longPoll";
import { gatewayChangeBus } from "@server/store/changeBus";
import {
  Kind,
  Tier,
  type Ack,
  type NotifySendRequest,
  type SendRequest,
} from "@shared/types";

/** Maximum time an authorized read may wait for its best-effort human delivery receipt. */
const HUMAN_READ_RECEIPT_TIMEOUT_MS = 250;

// Small helpers for query parsing (flat string map → typed-ish values).
function intParam(v: string | undefined): number | undefined {
  if (v === undefined || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}
function boolParam(v: string | undefined): boolean | undefined {
  if (v === undefined) return undefined;
  return v === "true" || v === "1";
}

function requireCommands(deps: Parameters<Handler>[0]["deps"]) {
  if (!deps.commands) {
    throw new GatewayError(500, "command ingress is not configured");
  }
  return deps.commands;
}

function requireHooks(deps: ApiDeps) {
  if (!deps.hooks) throw new GatewayError(503, "message hook service is unavailable");
  return deps.hooks;
}

// ════════════════════════════════════════════════════════════════════════════
// READS (GET → read-view over the lazily-resolved read handle). The read DB is
// obtained on demand via `await deps.db()` — so a read route is the ONLY thing
// that ever constructs it. Never call rpc; never write.
// ════════════════════════════════════════════════════════════════════════════

export const getThreads: Handler = async ({ deps }) =>
  ok(deps.canonicalDb
    ? await canonicalThreads(await deps.canonicalDb())
    : await listThreads(await deps.db()));

export const getThreadHistory: Handler = async ({ deps, params, req }) => {
  if (deps.canonicalDb) {
    const db = await deps.canonicalDb();
    const read = () => canonicalThreadHistory(db, params.name!, {
      limit: intParam(req.query.limit) ?? 50,
      before: req.query.before,
      after: req.query.after,
    });
    const page = await longPollCanonicalPage({
      key: `thread-name:${params.name!}`,
      after: req.query.after,
      waitMs: intParam(req.query.waitMs) ?? 0,
      bus: gatewayChangeBus,
      read,
    });
    return withHumanReadReceipt(deps, req, page.rows);
  }
  return withHumanReadReceipt(
    deps, req, await threadHistory(await deps.db(), {
      thread: params.name!,
      limit: intParam(req.query.limit),
      before: intParam(req.query.before),
      after: intParam(req.query.after),
      afterRowid: intParam(req.query.afterRowid),
    }),
  );
};

export const getHooks: Handler = async ({ deps, req }) =>
  ok(await requireHooks(deps).list(req.caller?.tier === Tier.Admin));

export const getHookPublicKey: Handler = async ({ deps }) =>
  ok(await requireHooks(deps).publicKey());

export const getHookAudit: Handler = async ({ deps, req }) =>
  ok(await requireHooks(deps).audit(
    intParam(req.query.limit) ?? 100,
    req.caller?.tier === Tier.Admin,
  ));

export const getDmHistory: Handler = async ({ deps, params, req }) => {
  if (deps.canonicalDb) {
    const db = await deps.canonicalDb();
    const read = () => canonicalDmHistory(db, params.name!, {
      limit: intParam(req.query.limit) ?? 50,
      before: req.query.before,
      after: req.query.after,
    });
    const page = await longPollCanonicalPage({
      key: `dm-name:${params.name!}`,
      after: req.query.after,
      waitMs: intParam(req.query.waitMs) ?? 0,
      bus: gatewayChangeBus,
      read,
    });
    return withHumanReadReceipt(deps, req, page.rows);
  }
  return withHumanReadReceipt(
    deps, req, await dmHistory(await deps.db(), {
      with: params.name!,
      me: req.caller?.name ?? req.query.me,
      project: req.query.project ?? req.caller?.project,
      limit: intParam(req.query.limit),
      before: intParam(req.query.before),
      after: intParam(req.query.after),
      afterRowid: intParam(req.query.afterRowid),
    }),
  );
};

export const getMembers: Handler = async ({ deps, req }) =>
  ok(
    deps.canonicalDb ? await canonicalMembers(await deps.canonicalDb(), {
      includeOffline: boolParam(req.query.includeOffline) ?? false,
      project: req.query.project,
    }) : await listMembers(await deps.db(), {
      includeOffline: boolParam(req.query.includeOffline) ?? false,
      project: req.query.project,
    }),
  );

export const getSearch: Handler = async ({ deps, req }) => {
  const query = req.query.q ?? req.query.query ?? "";
  const caller = req.query.me ?? req.caller?.name;
  return ok(
    deps.canonicalDb ? await canonicalSearch(await deps.canonicalDb(), {
      query,
      mode: req.query.mode ?? (req.query.semantic ? "semantic" : req.query.hybrid ? "hybrid" : "fts"),
      thread: req.query.thread,
      topic: req.query.topic,
      with: req.query.with,
      since: intParam(req.query.since),
      caller,
      limit: intParam(req.query.limit) ?? 50,
    }) : await searchMessages(await deps.db(), {
      query,
      mode: req.query.mode ?? (req.query.semantic ? "semantic" : req.query.hybrid ? "hybrid" : "fts"),
      thread: req.query.thread,
      topic: req.query.topic,
      with: req.query.with,
      since: intParam(req.query.since),
      project: req.query.project ?? req.caller?.project,
      caller: req.caller?.name,
      limit: intParam(req.query.limit),
    }),
  );
};

export const getHistory: Handler = async ({ deps, req }) => {
  if (!deps.canonicalDb) return fail(503, "Gateway canonical history is unavailable");
  return ok(await canonicalHistory(await deps.canonicalDb(), {
    caller: req.query.me ?? req.caller?.name,
    thread: req.query.thread,
    with: req.query.with,
    topic: req.query.topic,
    limit: intParam(req.query.limit) ?? 50,
    before: intParam(req.query.before),
  }));
};

export const getTopics: Handler = async ({ deps }) =>
  ok(deps.canonicalDb
    ? await canonicalTopics(await deps.canonicalDb())
    : await listTopics(await deps.db()));

export const getNotifications: Handler = async ({ deps, req }) =>
  ok(deps.canonicalDb
    ? await canonicalNotifications(await deps.canonicalDb(), {
      limit: intParam(req.query.limit) ?? 100,
      before: intParam(req.query.before),
    })
    : await listNotifications(await deps.db(), {
      limit: intParam(req.query.limit),
      before: intParam(req.query.before),
    }));

export const getRoutingRules: Handler = async ({ deps }) =>
  ok(deps.canonicalDb
    ? await canonicalRoutingRules(await deps.canonicalDb())
    : await listRoutingRules(await deps.db()));

export const getProjects: Handler = async ({ deps }) =>
  ok(deps.canonicalDb
    ? await canonicalProjects(await deps.canonicalDb())
    : await listProjects(await deps.db()));

export const getWhoami: Handler = async ({ req }) => {
  const caller = req.caller;
  if (!caller) return fail(401, "not logged in", "unauthorized");
  return ok({
    name: caller.name,
    ...(caller.agentId ? { agentId: caller.agentId } : {}),
    sessionId: caller.sessionId ?? "",
    kind: caller.kind ?? Kind.Human,
    tier: caller.tier ?? Tier.Agent,
    project: caller.project,
    presence: "online",
  });
};

// ════════════════════════════════════════════════════════════════════════════
// WRITES / OPS. Public REST writes enqueue daemon command intents. Handlers
// never write canonical tables directly.
// ════════════════════════════════════════════════════════════════════════════

// --- notifications / messages ---

export const postNotify: Handler = async ({ deps, ...rest }) => {
  // Preserve the public API's field-level 400 for malformed JSON shapes. Signature verification
  // still happens before command ingress or any durable write.
  parseBody({ deps, ...rest }, notifySchema);
  const secret = deps.notifyHmacSecret?.trim();
  if (!secret) {
    return fail(503, "notification HMAC secret is not configured", "service_unavailable");
  }
  const timestamp = rest.req.headers["x-nexus-timestamp"]?.trim();
  const signature = rest.req.headers["x-nexus-signature"]?.trim().toLowerCase();
  const rawBody = rest.req.rawBody;
  if (!timestamp || !/^\d+$/.test(timestamp) || !rawBody || !signature) {
    return fail(401, "missing or invalid notification signature");
  }
  const timestampMs = Number(timestamp);
  if (!Number.isSafeInteger(timestampMs) || timestampMs < 1_000_000_000_000) {
    return fail(401, "notification timestamp must be Unix milliseconds");
  }
  const now = deps.now ?? Date.now;
  if (Math.abs(now() - timestampMs) > 300_000) {
    return fail(401, "notification timestamp is outside the freshness window");
  }
  if (!/^sha256=[0-9a-f]{64}$/.test(signature)) {
    return fail(401, "invalid notification signature");
  }
  const expected = "sha256=" + createHmac("sha256", secret)
    .update(`${timestamp}.${rawBody}`)
    .digest("hex");
  const expectedBytes = Buffer.from(expected, "utf8");
  const signatureBytes = Buffer.from(signature, "utf8");
  if (
    expectedBytes.length !== signatureBytes.length ||
    !timingSafeEqual(expectedBytes, signatureBytes)
  ) {
    return fail(401, "invalid notification signature");
  }
  let signedBody: unknown;
  try {
    signedBody = JSON.parse(rawBody) as unknown;
  } catch {
    return fail(400, "signed notification body is not valid JSON");
  }
  const body = parseBody(
    { deps, ...rest, req: { ...rest.req, body: signedBody } },
    notifySchema,
  );
  const digest = signature.slice("sha256=".length);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.notificationNotify,
      { rawBody, timestamp, signature },
      signedNotificationCaller(body.source),
      { idempotencyKey: `notify:${timestamp}:${digest}` },
    ),
    201,
  );
};

export const postNotification: Handler = async ({ deps, ...rest }) => {
  if (!rest.req.caller) return fail(401, "not logged in", "unauthorized");
  const body = parseBody({ deps, ...rest }, notifySendSchema) as NotifySendRequest;
  const opts = body.idempotencyKey
    ? { idempotencyKey: body.idempotencyKey }
    : undefined;
  const ack = await requireCommands(deps).submit<Ack>(
    COMMAND_KINDS.notificationSend,
    body,
    rest.req.caller,
    opts,
  );
  return ok({ messageId: ack.messageId }, 201);
};

function signedNotificationCaller(source: string): GatewayCallerIdentity {
  return {
    id: `notification:${source}`,
    name: source,
    project: "default",
    kind: Kind.Notification,
    tier: Tier.Agent,
    credentialFacet: "source",
    scopes: ["source:push"],
    sessionId: `notification:${source}`,
    runtimeId: `notification:${source}`,
  };
}

export const postMessage: Handler = async ({ deps, ...rest }) => {
  if (!rest.req.caller) return fail(401, "not logged in", "unauthorized");
  const body = parseBody({ deps, ...rest }, sendSchema);
  const idempotencyKey = messagePostIdempotencyKey(rest.req);
  const idempotencyOpts = idempotencyKey ? { idempotencyKey } : undefined;
  const requestBody = idempotencyKey && !body.idempotencyKey
    ? { ...body, idempotencyKey }
    : body;
  // `to` is the SendTarget union; the parsed shape is the contract SendRequest.
  const ack = deps.messagePost
    ? await deps.messagePost.send(
      requestBody as SendRequest,
      rest.req.caller,
      idempotencyOpts,
    )
    : await requireCommands(deps).submit<Ack>(
      COMMAND_KINDS.messagePostSend,
      requestBody as SendRequest,
      rest.req.caller,
      idempotencyOpts,
    );
  return ok({ messageId: ack.messageId }, 201);
};

function messagePostIdempotencyKey(req: { headers: Record<string, string | undefined>; body?: unknown }): string | undefined {
  const header = req.headers["idempotency-key"]?.trim();
  if (header) return header;
  if (req.body && typeof req.body === "object") {
    const key = (req.body as { idempotencyKey?: unknown }).idempotencyKey;
    if (typeof key === "string" && key.trim()) return key.trim();
  }
  return undefined;
}

export const getMessage: Handler = async ({ deps, params, req }) => {
  const message = deps.canonicalDb
    ? await canonicalMessageById(
      await deps.canonicalDb(),
      params.id!,
      req.query.me ?? req.caller?.name,
    )
    : await messageById(await deps.db(), params.id!, req.caller?.project);
  if (!message) return fail(404, `message not found: ${params.id}`);
  await markHumanReadMessages(deps, req, [message.id]);
  return ok(message);
};

async function withHumanReadReceipt<T extends { messageId: string }>(
  deps: ApiDeps,
  req: Parameters<Handler>[0]["req"],
  rows: T[],
) {
  await markHumanReadMessages(
    deps,
    req,
    rows.map((row) => row.messageId),
  );
  return ok(rows);
}

async function markHumanReadMessages(
  deps: ApiDeps,
  req: Parameters<Handler>[0]["req"],
  messageIds: readonly string[],
) {
  const sessionId = req.caller?.sessionId;
  if (!sessionId || !deps.humanReads || messageIds.length === 0) return;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      deps.humanReads.markDelivered(sessionId, messageIds),
      new Promise<void>((resolve) => {
        timeout = setTimeout(resolve, HUMAN_READ_RECEIPT_TIMEOUT_MS);
      }),
    ]);
  } catch {
    // Receipt writes are best-effort: a DB lock or transient gateway write
    // failure must not make the already-authorized message read disappear from
    // the human UI. The next successful read can mark the same ids delivered.
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

async function getMetadata(
  deps: Parameters<Handler>[0]["deps"],
  req: Parameters<Handler>[0]["req"],
  entity: MetadataEntityKind,
  id: string,
) {
  const row = deps.canonicalDb
    ? await canonicalMetadata(await deps.canonicalDb(), entity, id)
    : await entityMetadata(await deps.db(), entity, id, req.caller?.project);
  return row ? ok(row) : fail(404, `${entity} metadata target not found: ${id}`);
}

async function patchMetadata(
  ctx: Parameters<Handler>[0],
  entity: MetadataEntityKind,
  id: string,
) {
  if (!ctx.req.caller) return fail(401, "not logged in", "unauthorized");
  const body = parseBody(ctx, metadataSchema);
  return ok(
    await requireCommands(ctx.deps).submit(
      COMMAND_KINDS.metadataSet,
      { entity, id, metadata: body.metadata },
      ctx.req.caller,
    ),
  );
}

export const getMessageMetadata: Handler = async ({ deps, params, req }) =>
  getMetadata(deps, req, "message", params.id!);

export const getSessionMetadata: Handler = async ({ deps, params, req }) =>
  getMetadata(deps, req, "session", params.id!);

export const getThreadMetadata: Handler = async ({ deps, params, req }) =>
  getMetadata(deps, req, "thread", params.name!);

export const getAgentMetadata: Handler = async ({ deps, params, req }) =>
  getMetadata(deps, req, "agent", params.id!);

export const patchMessageMetadata: Handler = async (ctx) =>
  patchMetadata(ctx, "message", ctx.params.id!);

export const patchSessionMetadata: Handler = async (ctx) =>
  patchMetadata(ctx, "session", ctx.params.id!);

export const patchThreadMetadata: Handler = async (ctx) =>
  patchMetadata(ctx, "thread", ctx.params.name!);

export const patchAgentMetadata: Handler = async (ctx) =>
  patchMetadata(ctx, "agent", ctx.params.id!);

// --- agents (admin spawn/remove/assign-role) ---

export const getAgent: Handler = async ({ deps, params, req }) => {
  const row = deps.canonicalDb ? await canonicalAgentShow(await deps.canonicalDb(), params.id!, {
    includeStopped: boolParam(req.query.includeStopped) ?? true,
  }) : await agentShow(await deps.db(), params.id!, {
    includeStopped: boolParam(req.query.includeStopped) ?? true,
  });
  return row ? ok(row) : fail(404, `agent not found: ${params.id!}`);
};

export const getAgentRuntimes: Handler = async ({ deps, params, req }) => {
  const row = deps.canonicalDb ? await canonicalAgentRuntimesFor(await deps.canonicalDb(), params.id!, {
    includeStopped: boolParam(req.query.includeStopped) ?? false,
  }) : await agentRuntimesFor(await deps.db(), {
    id: params.id!,
    includeStopped: boolParam(req.query.includeStopped) ?? false,
  });
  return row ? ok(row) : fail(404, `agent not found: ${params.id!}`);
};

export const getRuntimes: Handler = async ({ deps, req }) => {
  const id = req.query.agentId ?? req.query.agent ?? req.query.name;
  // No target query = the fleet view: every runtime across all agents.
  if (!id) {
    const runtimes = deps.canonicalDb ? await canonicalRuntimes(await deps.canonicalDb(), {
      project: req.query.project,
      includeStopped: boolParam(req.query.includeStopped) ?? false,
    }) : await listRuntimes(await deps.db(), {
      project: req.query.project,
      includeStopped: boolParam(req.query.includeStopped) ?? false,
    });
    return ok({ runtimes });
  }
  const row = deps.canonicalDb ? await canonicalAgentRuntimesFor(await deps.canonicalDb(), id, {
    includeStopped: boolParam(req.query.includeStopped) ?? false,
  }) : await agentRuntimesFor(await deps.db(), {
    id,
    includeStopped: boolParam(req.query.includeStopped) ?? false,
  });
  return row ? ok(row) : fail(404, `agent not found: ${id}`);
};

export const postAgent: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, spawnSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.adminSpawn,
      body,
      rest.req.caller,
    ),
    201,
  );
};

function agentCommandTarget(id: string): { name: string; agentId?: string } {
  return id.startsWith("a_") ? { name: id, agentId: id } : { name: id };
}

export const deleteAgent: Handler = async ({ deps, params, req }) => {
  const id = params.id!;
  const target = agentCommandTarget(id);
  // Three distinct lifecycle ops via query flag:
  //   ?delete=1 → purge everywhere (daemon store + the web console's own Turso conversation)
  //   ?evict=1  → leave every thread (session + process kept)
  //   default / ?kill=1 → kill the process (kill=1) or detach (kill=0); record kept, resumable
  const commands = requireCommands(deps);
  if (boolParam(req.query.delete)) {
    const result = await commands.submit(
      COMMAND_KINDS.adminDelete,
      target,
      req.caller,
    );
    try {
      const { getConversationStore, deleteConversation } = await import("@server/conversation/store");
      await deleteConversation(await getConversationStore(), `dm:${id}`);
    } catch {
      /* daemon purge already done; the console-store purge is best-effort */
    }
    return ok(result);
  }
  if (boolParam(req.query.evict)) {
    return ok(
      await commands.submit(
        COMMAND_KINDS.adminEvict,
        target,
        req.caller,
      ),
    );
  }
  const kill = boolParam(req.query.kill) ?? false;
  return ok(
    await commands.submit(
      COMMAND_KINDS.adminRemove,
      { ...target, kill },
      req.caller,
    ),
  );
};

export const postAgentRole: Handler = async ({ deps, params, ...rest }) => {
  const { role } = parseBody({ deps, params, ...rest }, assignRoleSchema);
  const target = agentCommandTarget(params.id!);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.adminAssignRole,
      { ...target, role },
      rest.req.caller,
    ),
  );
};

export const postAgentProject: Handler = async ({ deps, params, ...rest }) => {
  const { project } = parseBody({ deps, params, ...rest }, assignProjectSchema);
  const target = agentCommandTarget(params.id!);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.adminAssignProject,
      { ...target, project },
      rest.req.caller,
    ),
  );
};

export const postAgentTier: Handler = async ({ deps, params, ...rest }) => {
  const { tier } = parseBody({ deps, params, ...rest }, grantTierSchema);
  const target = agentCommandTarget(params.id!);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.adminGrantTier,
      { ...target, tier },
      rest.req.caller,
    ),
  );
};

export const postAgentAccess: Handler = async ({ deps, params, ...rest }) => {
  const body = parseBody({ deps, params, ...rest }, agentAccessGrantSchema);
  const target = agentCommandTarget(params.id!);
  const principal =
    body.principal.startsWith("a_") ? { principalAgentId: body.principal } : {};
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.agentGrantAccess,
      { ...target, ...body, ...principal },
      rest.req.caller,
    ),
  );
};

export const deleteAgentAccess: Handler = async ({ deps, params, req }) => {
  const target = agentCommandTarget(params.id!);
  const principal = params.principal!;
  const principalTarget = principal.startsWith("a_") ? { principalAgentId: principal } : {};
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.agentRevokeAccess,
      {
        ...target,
        principal,
        ...principalTarget,
        project: req.query.project,
      },
      req.caller,
    ),
  );
};

export const postAgentOwner: Handler = async ({ deps, params, ...rest }) => {
  const body = parseBody({ deps, params, ...rest }, agentOwnerTransferSchema);
  const target = agentCommandTarget(params.id!);
  const owner = body.owner.startsWith("a_") ? { ownerAgentId: body.owner } : {};
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.agentTransferOwner,
      { ...target, ...body, ...owner },
      rest.req.caller,
    ),
  );
};

export const postAgentCredential: Handler = async ({ deps, params, ...rest }) => {
  const body = parseBody({ deps, params, ...rest }, agentCredentialCreateSchema);
  const target = agentCommandTarget(params.id!);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.agentCredentialCreate,
      {
        ...target,
        ...body,
        scopes: body.scopes?.length ? body.scopes : ["runtime:register"],
      },
      rest.req.caller,
    ),
    201,
  );
};

export const deleteAgentCredential: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.agentCredentialRevoke,
      { credentialId: params.credentialId! },
      req.caller,
    ),
  );

// --- threads ---

export const postThread: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, createThreadSchema);
  return ok(
    await requireCommands(deps).submit<null>(
      COMMAND_KINDS.threadCreate,
      body,
      rest.req.caller,
    ),
    201,
  );
};

export const postThreadJoin: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit<null>(
      COMMAND_KINDS.threadJoin,
      { name: params.name! },
      req.caller,
    ),
  );

export const postThreadLeave: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit<null>(
      COMMAND_KINDS.threadLeave,
      { name: params.name! },
      req.caller,
    ),
  );

export const postThreadArchive: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit<null>(
      COMMAND_KINDS.threadArchive,
      { name: params.name! },
      req.caller,
    ),
  );

export const deleteThread: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit<null>(
      COMMAND_KINDS.threadDelete,
      { name: params.name! },
      req.caller,
    ),
  );

export const patchThread: Handler = async ({ deps, params, ...rest }) => {
  const { name: newName } = parseBody({ deps, params, ...rest }, renameThreadSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.threadRename,
      { name: params.name!, newName },
      rest.req.caller,
    ),
  );
};

export const getThreadMembers: Handler = async ({ deps, params }) => {
  const row = deps.canonicalDb
    ? await canonicalThreadMembers(await deps.canonicalDb(), params.name!)
    : await threadMembersFor(await deps.db(), { thread: params.name! });
  return row ? ok(row) : fail(404, `thread not found: ${params.name!}`);
};

export const getThreadHeader: Handler = async ({ deps, params }) => {
  const row = deps.canonicalDb
    ? await canonicalThreadHeader(await deps.canonicalDb(), params.name!)
    : await threadHeader(await deps.db(), { thread: params.name! });
  return row ? ok(row) : fail(404, `thread not found: ${params.name!}`);
};

export const postThreadMember: Handler = async ({ deps, params, ...rest }) => {
  const { member } = parseBody({ deps, params, ...rest }, addThreadMemberSchema);
  return ok(
    await requireCommands(deps).submit<null>(
      COMMAND_KINDS.threadAddMember,
      { name: params.name!, member },
      rest.req.caller,
    ),
    201,
  );
};

export const deleteThreadMember: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit<null>(
      COMMAND_KINDS.threadRemoveMember,
      { name: params.name!, member: params.member! },
      req.caller,
    ),
  );

// --- topics / pubsub ---

export const postTopicSubscribe: Handler = async ({ deps, params, ...rest }) => {
  const { group } = parseBody({ deps, params, ...rest }, subscribeSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.topicSubscribe,
      { topic: params.name!, group },
      rest.req.caller,
    ),
  );
};

export const postTopicUnsubscribe: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.topicUnsubscribe,
      { topic: params.name! },
      req.caller,
    ),
  );

// --- presence / lifecycle ---

export const postStatus: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, statusSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.presenceStatus,
      body,
      rest.req.caller,
    ),
  );
};

export const postHeartbeat: Handler = async ({ deps, req }) =>
  // HeartbeatRequest is an empty body; the registry expects the empty object.
  ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.presenceHeartbeat,
      {},
      req.caller,
    ),
  );

// --- inbox (consume / ack) ---

export const postConsume: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, consumeSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.inboxConsume,
      body,
      rest.req.caller,
    ),
  );
};

export const postAck: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, ackSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.inboxAck,
      body,
      rest.req.caller,
    ),
  );
};

export const postAckThreads: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, ackThreadsSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.inboxAckThreads,
      body,
      rest.req.caller,
    ),
  );
};

// --- registration ---

export const postRegister: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, registerSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.identityRegister,
      body,
      rest.req.caller,
    ),
    201,
  );
};

export const postRename: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, renameSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.identityRename,
      body,
      rest.req.caller,
    ),
  );
};

// --- routing rules (standing rule) ---

export const postRoutingRule: Handler = async ({ deps, ...rest }) => {
  parseBody({ deps, ...rest }, routeRuleSchema);
  return fail(
    501,
    "standing routing rules are not wired to command ingress yet",
    "not_implemented",
  );
};

// --- admin (channel / route / monitor) ---

export const postAdminChannel: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, channelSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.adminChannel,
      body,
      rest.req.caller,
    ),
  );
};

export const postAdminRoute: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, routeForwardSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.adminRoute,
      body,
      rest.req.caller,
    ),
  );
};

export const postAdminMonitor: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, monitorSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.adminMonitor,
      body,
      rest.req.caller,
    ),
  );
};

// --- notification source management ---

export const getSources: Handler = async ({ deps }) =>
  ok(deps.sourceRegistry
    ? await deps.sourceRegistry.list()
    : await listSources(await deps.db()));

export const postSource: Handler = async ({ deps, ...rest }) => {
  const body = parseBody({ deps, ...rest }, sourceRegisterSchema);
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.sourceRegister,
      body,
      rest.req.caller,
    ),
    201,
  );
};

export const getSource: Handler = async ({ deps, params }) => {
  const source = deps.sourceRegistry
    ? await deps.sourceRegistry.show(params.name!)
    : await sourceByName(await deps.db(), params.name!);
  return source ? ok(source) : fail(404, `source not found: ${params.name!}`);
};

export const postSourceEnable: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.sourceEnable,
      { name: params.name! },
      req.caller,
    ),
  );

export const postSourceDisable: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.sourceDisable,
      { name: params.name! },
      req.caller,
    ),
  );

export const postSourceRotate: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.sourceRotate,
      { name: params.name! },
      req.caller,
    ),
  );

export const deleteSource: Handler = async ({ deps, params, req }) =>
  ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.sourceRemove,
      { name: params.name! },
      req.caller,
    ),
  );

// --- notification sources (signed push) ---

/**
 * POST /sources/:name/push
 *
 * Authenticated push from an external source. The gateway (not the daemon) owns
 * HMAC verification: it reads the source's plaintext token from the daemon-owned
 * store projection, then verifies the producer's signature itself. Only on a
 * valid, fresh signature does it enqueue a `source.push` command intent.
 *
 * Wire contract (producer does):
 *   raw = json.dumps({summary, body, meta})
 *   sig = hmac_sha256(TOKEN, f"{ts}.{raw}").hexdigest()
 *   POST with X-Nexus-Timestamp: ts, X-Nexus-Signature: sha256=<sig>
 *   Content-Type: application/json, request body = raw
 */
export const postSourcePush: Handler = async ({ deps, params, req }) => {
  const name = params.name!;
  const topic = req.query.topic;
  const tsHeader = req.headers["x-nexus-timestamp"];
  const sigHeader = req.headers["x-nexus-signature"];
  const rawBody = req.rawBody;

  // 1. Freshness check: timestamp must be present, numeric, and within 5 minutes.
  if (!tsHeader || !/^\d+$/.test(tsHeader)) {
    return fail(401, "missing or invalid X-Nexus-Timestamp");
  }
  const ts = parseInt(tsHeader, 10);
  // Public v0.1 uses Unix milliseconds, matching Nexus store/event timestamps. Accept legacy
  // second stamps during the compatibility window, but never rewrite the signed value.
  const timestampMs = ts < 1_000_000_000_000 ? ts * 1000 : ts;
  if (Math.abs(Date.now() - timestampMs) > 300_000) {
    return fail(401, "timestamp out of window (replay rejected)");
  }

  // 2. Fetch the source token from the read store before any signature work.
  const source = deps.sourceRegistry
    ? await deps.sourceRegistry.secret(name)
    : await sourceSecretByName(await deps.db(), name);
  if (!source) return fail(404, "source not found");
  if (!source.enabled) return fail(403, "source disabled");
  const token = source.token;

  // 3. HMAC verification (gateway-side; constant-time compare).
  if (!sigHeader || !rawBody) {
    return fail(401, "missing signature or body");
  }
  const expected = "sha256=" + createHmac("sha256", token).update(`${ts}.${rawBody}`).digest("hex");
  // timingSafeEqual requires equal-length Buffers; mismatched length is also a failure.
  const eBuf = Buffer.from(expected, "utf8");
  const sBuf = Buffer.from(sigHeader, "utf8");
  const signatureOk = eBuf.length === sBuf.length && timingSafeEqual(eBuf, sBuf);
  if (!signatureOk) {
    return fail(401, "invalid signature");
  }

  // 4. Parse + validate the raw body with Zod (after signature is confirmed valid).
  let parsed: { summary?: string; body: string; meta?: unknown };
  try {
    const jsonBody = JSON.parse(rawBody) as unknown;
    const result = sourcePushSchema.safeParse(jsonBody);
    if (!result.success) {
      return fail(400, "invalid push body");
    }
    parsed = result.data;
  } catch {
    return fail(400, "push body is not valid JSON");
  }

  // 5. Enqueue the daemon source.push command.
  return ok(
    await requireCommands(deps).submit(
      COMMAND_KINDS.sourcePush,
      {
        source: name,
        topic: topic || undefined,
        summary: parsed.summary,
        body: parsed.body,
        meta: parsed.meta,
      },
      sourcePushCaller(name),
      {
        idempotencyKey: `source-push:${name}:${tsHeader}:${sigHeader}`,
      },
    ),
    201,
  );
};

function sourcePushCaller(name: string): GatewayCallerIdentity {
  return {
    id: `source:${name}`,
    name,
    project: "default",
    kind: Kind.Notification,
    tier: Tier.Agent,
    credentialFacet: "source",
    scopes: ["source:push"],
    sessionId: `source:${name}`,
    runtimeId: `source:${name}`,
  };
}
