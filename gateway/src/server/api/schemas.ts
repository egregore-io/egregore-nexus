// Zod request schemas for the public HTTP API. Built to be CONSISTENT with the
// generated contract DTOs (`@shared/types` / `contracts.gen.ts`) — they validate
// the untrusted external body/params and parse them into exactly the shape the
// matching daemon command expects. They are validators only: no business logic.
//
// `z.infer<typeof X>` is structurally assignable to the contract DTO; the handler
// layer asserts that with a `satisfies`-style cast at the call site so a drift
// between a schema and its contract DTO is a compile error.
import { z } from "zod";

import {
  StatusState,
  ChannelOp,
  Tier,
  Kind,
  AgentAccessRole,
} from "@shared/types";
import { EMPTY_MESSAGE_BODY, hasMessageBody } from "@server/messagePost/validate";

// ── enums (mirror the contract string enums) ──────────────────────────────────
const harnessSchema = z
  .string()
  .regex(/^[a-z][a-z0-9_-]{0,63}$/, "invalid harness id");
const statusStateSchema = z.nativeEnum(StatusState);
const channelOpSchema = z.nativeEnum(ChannelOp);
const tierSchema = z.nativeEnum(Tier);
const kindSchema = z.nativeEnum(Kind);
const agentAccessRoleSchema = z.nativeEnum(AgentAccessRole);

// ── SendTarget (internally-tagged union on `verb`) ────────────────────────────
export const sendTargetSchema = z
  .discriminatedUnion("verb", [
    z.object({
      verb: z.literal("dm"),
      name: z.string().min(1).optional(),
      agentId: z.string().min(1).optional(),
    }),
    z.object({ verb: z.literal("post"), thread: z.string().min(1) }),
    z.object({ verb: z.literal("publish"), topic: z.string().min(1) }),
    z.object({ verb: z.literal("reply") }),
  ])
  .superRefine((target, context) => {
    if (target.verb === "dm" && !target.name && !target.agentId) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "dm target requires name or agentId",
      });
    }
  });

// ── writes / ops (POST/DELETE bodies -> daemon command params) ────────────────

/** POST /notify → notify */
export const notifySchema = z.object({
  source: z.string().min(1),
  topic: z.string().min(1).optional(),
  // Opaque producer payload (contract: `any`). Required field, any JSON value.
  payload: z.unknown(),
});

/** POST /notifications → one explicitly targeted notification. */
export const notifyTargetSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("auto"), value: z.string().min(1) }),
  z.object({ kind: z.literal("agent"), agentId: z.string().min(1) }),
  z.object({ kind: z.literal("name"), name: z.string().min(1) }),
  z.object({ kind: z.literal("group"), group: z.string().min(1) }),
  z.object({ kind: z.literal("thread"), thread: z.string().min(1) }),
]);

export const notifySendSchema = z.object({
  target: notifyTargetSchema,
  source: z.string().min(1).optional(),
  body: z.string().refine(hasMessageBody, EMPTY_MESSAGE_BODY),
  idempotencyKey: z.string().min(1).optional(),
});

/** POST /messages → send */
export const sendSchema = z.object({
  to: sendTargetSchema,
  summary: z.string().optional(),
  body: z.string().refine(hasMessageBody, EMPTY_MESSAGE_BODY),
  mention: z.array(z.string()).optional(),
  idempotencyKey: z.string().min(1).optional(),
});

/** PATCH /{entity}/:id/metadata -> metadata.set. */
export const metadataSchema = z.object({
  metadata: z.unknown(),
});

/** POST /agents → launch (admin.spawn) */
export const spawnSchema = z.object({
  kind: harnessSchema,
  name: z.string().min(1).optional(),
  cwd: z.string().optional(),
  project: z.string().optional(),
  role: z.string().optional(),
});

/** POST /agents/:id/role → admin.assignRole (name comes from the path param) */
export const assignRoleSchema = z.object({
  role: z.string().min(1),
});

/** POST /agents/:id/project → admin.assignProject (name comes from the path param) */
export const assignProjectSchema = z.object({
  project: z.string().min(1),
});

/** POST /agents/:id/tier → admin.grantTier (name comes from the path param). */
export const grantTierSchema = z.object({
  tier: tierSchema,
});

/** POST /agents/:id/access → agent.grantAccess (name comes from the path param). */
export const agentAccessGrantSchema = z.object({
  principal: z.string().min(1),
  project: z.string().min(1).optional(),
  role: agentAccessRoleSchema,
});

/** POST /agents/:id/owner → agent.transferOwner (name comes from the path param). */
export const agentOwnerTransferSchema = z.object({
  owner: z.string().min(1),
  project: z.string().min(1).optional(),
});

/** POST /agents/:id/credentials → agent.credential.create. */
export const agentCredentialCreateSchema = z.object({
  label: z.string().min(1).optional(),
  purpose: z.string().min(1).optional(),
  scopes: z.array(z.string().min(1)).optional(),
});

/** POST /rename → identity.rename. */
export const renameSchema = z.object({
  name: z.string().min(1),
});

/** POST /threads -> thread.create */
export const createThreadSchema = z.object({
  name: z.string().min(1),
  members: z.array(z.string()).optional(),
});

/** PATCH /threads/:name -> thread.rename (current name comes from the path). */
export const renameThreadSchema = z.object({
  name: z.string().min(1),
});

/** POST /threads/:name/members -> thread.add_member (thread name comes from the path). */
export const addThreadMemberSchema = z.object({
  member: z.string().min(1),
});

/** POST /status → status */
export const statusSchema = z.object({
  state: statusStateSchema.optional(),
  work: z.string().optional(),
});

/** POST /topics/:name/subscribe → subscribe (topic from the path param) */
export const subscribeSchema = z.object({
  group: z.string().optional(),
});

/** POST /inbox/consume → consume */
export const consumeSchema = z.object({
  timeoutMs: z.number().int().nonnegative().optional(),
  max: z.number().int().positive().optional(),
});

/** POST /inbox/ack → ack */
export const ackSchema = z.object({
  messageId: z.string().min(1),
});

/** POST /inbox/ack-threads → ackThreads */
export const ackThreadsSchema = z.object({
  messageIds: z.array(z.string().min(1)),
});

/** POST /register → register */
export const registerSchema = z.object({
  agentId: z.string().min(1).optional(),
  name: z.string().min(1),
  harness: harnessSchema,
  harnessSessionId: z.string().min(1),
  project: z.string().min(1),
  clientKey: z.string().min(1),
  runtimeCredential: z.string().min(1).optional(),
  tier: tierSchema,
  kind: kindSchema.optional(),
  role: z.string().optional(),
  cwd: z.string().optional(),
});

/** POST /admin/channel → admin.channel */
export const channelSchema = z.object({
  op: channelOpSchema,
  topic: z.string().min(1),
  source: z.string().optional(),
});

/** POST /admin/route → admin.route (ad-hoc one-shot forward) */
export const routeForwardSchema = z.object({
  notif: z.string().min(1),
  to: z.string().min(1),
});

/** POST /admin/monitor → admin.monitor */
export const monitorSchema = z.object({
  follow: z.boolean(),
  scope: z.string().optional(),
});

/** POST /admin/transport/secrets — the value is carried only in the request body. */
export const transportSecretSchema = z.object({
  key: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/),
  value: z.string().min(1),
});

/** POST /routing-rules → route (standing rule; at least one of source/topic). */
export const routeRuleSchema = z
  .object({
    source: z.string().min(1).optional(),
    topic: z.string().min(1).optional(),
    to: z.string().min(1),
  })
  .refine((r) => r.source !== undefined || r.topic !== undefined, {
    message: "a route rule needs at least one of `source` or `topic`",
  });

/** POST /sources — register a new notification source. */
export const sourceRegisterSchema = z.object({
  name: z.string().min(1),
  topic: z.string().min(1).optional(),
});

/** POST /sources/:name/push — the raw body parsed as JSON (validated after HMAC). */
export const sourcePushSchema = z.object({
  summary: z.string().optional(),
  body: z.string().min(1),
  meta: z.unknown().optional(),
});

// Inferred output types (used by handlers for typed command-intent params).
export type SourceRegisterBody = z.infer<typeof sourceRegisterSchema>;
export type SourcePushBody = z.infer<typeof sourcePushSchema>;
export type NotifyBody = z.infer<typeof notifySchema>;
export type SendBody = z.infer<typeof sendSchema>;
export type SpawnBody = z.infer<typeof spawnSchema>;
export type AssignRoleBody = z.infer<typeof assignRoleSchema>;
export type AssignProjectBody = z.infer<typeof assignProjectSchema>;
export type GrantTierBody = z.infer<typeof grantTierSchema>;
export type AgentAccessGrantBody = z.infer<typeof agentAccessGrantSchema>;
export type AgentOwnerTransferBody = z.infer<typeof agentOwnerTransferSchema>;
export type AgentCredentialCreateBody = z.infer<typeof agentCredentialCreateSchema>;
export type RenameBody = z.infer<typeof renameSchema>;
export type CreateThreadBody = z.infer<typeof createThreadSchema>;
export type RenameThreadBody = z.infer<typeof renameThreadSchema>;
export type StatusBody = z.infer<typeof statusSchema>;
export type SubscribeBody = z.infer<typeof subscribeSchema>;
export type ConsumeBody = z.infer<typeof consumeSchema>;
export type AckBody = z.infer<typeof ackSchema>;
export type AckThreadsBody = z.infer<typeof ackThreadsSchema>;
export type RegisterBody = z.infer<typeof registerSchema>;
export type ChannelBody = z.infer<typeof channelSchema>;
export type RouteForwardBody = z.infer<typeof routeForwardSchema>;
export type MonitorBody = z.infer<typeof monitorSchema>;
export type RouteRuleBody = z.infer<typeof routeRuleSchema>;
