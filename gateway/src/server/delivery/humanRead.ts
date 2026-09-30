// Human read delivery marker.
//
// Agent sessions mark `in_flight` rows delivered from their daemon-owned realtime
// loop after a prompt is injected. Human web sessions do not have that loop:
// receiving a message means the authenticated browser fetched a message-bearing
// gateway read. Keep that write out of `server/read` so the display query layer
// remains SELECT-only.
import type { Client } from "@libsql/client";

import { callDaemonQuery, type DaemonIpcCaller } from "@server/daemon/ipc";

type ClientProvider = Client | (() => Client | Promise<Client>);

export interface HumanReadDeliveryMarker {
  markDelivered(sessionId: string, messageIds: readonly string[]): Promise<number>;
}

export interface HumanReadDeliveryOptions {
  now?: () => number;
  nexusHome?: string;
  daemonMark?: (request: HumanReadSettlementRequest) => Promise<{ changed: number }>;
}

interface HumanReadSettlementRequest {
  sessionId: string;
  messageIds: string[];
  now: number;
}

const gatewayCaller: DaemonIpcCaller = {
  name: "Nexus Gateway",
  project: "default",
  sessionId: "local-operator",
  runtimeId: "local-operator",
  kind: "human",
  tier: "admin",
};

/** Build a marker backed by the daemon DB client. */
export function createHumanReadDeliveryMarker(
  db?: ClientProvider,
  opts: HumanReadDeliveryOptions = {},
): HumanReadDeliveryMarker {
  return {
    markDelivered: async (sessionId, messageIds) => {
      if (db) {
        return markHumanReadDelivered(await resolveClient(db), {
          sessionId,
          messageIds,
          now: opts.now ?? Date.now,
        });
      }
      const normalized = normalizeReceipt(sessionId, messageIds);
      if (!normalized) return 0;
      const request: HumanReadSettlementRequest = {
        ...normalized,
        now: (opts.now ?? Date.now)(),
      };
      const result = opts.daemonMark
        ? await opts.daemonMark(request)
        : await callDaemonQuery<{ changed: number }>(
            "local.humanRead.markDelivered",
            request,
            gatewayCaller,
            { ...(opts.nexusHome ? { nexusHome: opts.nexusHome } : {}) },
          );
      return Number(result.changed ?? 0);
    },
  };
}

/**
 * Mark exactly the authenticated human session's visible messages delivered.
 *
 * The update is idempotent: already-delivered/acked rows are left as-is, and an
 * empty or duplicate message-id list is a no-op. The `recipient_session` filter
 * is the security boundary; callers cannot drain another browser/runtime by
 * passing query params such as `?me=`.
 */
export async function markHumanReadDelivered(
  db: Client,
  args: {
    sessionId: string;
    messageIds: readonly string[];
    now?: () => number;
  },
): Promise<number> {
  const sessionId = args.sessionId.trim();
  if (!sessionId) return 0;
  const ids = Array.from(new Set(args.messageIds.map((id) => id.trim()).filter(Boolean)));
  if (ids.length === 0) return 0;

  const placeholders = ids.map(() => "?").join(", ");
  const deliveredAt = (args.now ?? Date.now)();
  const result = await db.execute({
    sql:
      "UPDATE in_flight " +
      "SET state = 'delivered', delivered_at = COALESCE(delivered_at, ?) " +
      `WHERE recipient_session = ? AND message_id IN (${placeholders}) ` +
      "AND state IN ('pending', 'notified')",
    args: [deliveredAt, sessionId, ...ids],
  });
  return Number(result.rowsAffected ?? 0);
}

function normalizeReceipt(
  sessionId: string,
  messageIds: readonly string[],
): { sessionId: string; messageIds: string[] } | undefined {
  const session = sessionId.trim();
  const ids = Array.from(new Set(messageIds.map((id) => id.trim()).filter(Boolean)));
  return session && ids.length > 0 ? { sessionId: session, messageIds: ids } : undefined;
}

async function resolveClient(db: ClientProvider): Promise<Client> {
  return typeof db === "function" ? await db() : db;
}
