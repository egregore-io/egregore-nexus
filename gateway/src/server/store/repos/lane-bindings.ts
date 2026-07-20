import type { Client, Row } from "@libsql/client";

export type LaneKind = "thread" | "dm";

export interface LaneBindingRow {
  provider: string;
  externalChatId: string;
  laneKind: LaneKind;
  laneName: string;
}

export class LaneBindingConflictError extends Error {
  readonly code = "transport_lane_conflict";

  constructor(
    readonly provider: string,
    readonly externalChatId: string,
    readonly current: LaneBindingRow,
    readonly requested: LaneBindingRow,
  ) {
    super(
      `${provider}/${externalChatId} is already bound to ` +
        `${current.laneKind}/${current.laneName}`,
    );
    this.name = "LaneBindingConflictError";
  }
}

/** Idempotently bind a provider chat, refusing implicit rebinds. */
export async function bindLane(db: Client, input: LaneBindingRow): Promise<LaneBindingRow> {
  const row = validated(input);
  await db.execute({
    sql: `INSERT OR IGNORE INTO transport_lane_bindings
            (provider, external_chat_id, lane_kind, lane_name, created_at)
          VALUES (?, ?, ?, ?, ?)`,
    args: [row.provider, row.externalChatId, row.laneKind, row.laneName, Date.now()],
  });
  const current = await laneForChat(db, row.provider, row.externalChatId);
  if (!current) throw new Error(`failed to bind chat ${row.provider}/${row.externalChatId}`);
  if (current.laneKind !== row.laneKind || current.laneName !== row.laneName) {
    throw new LaneBindingConflictError(row.provider, row.externalChatId, current, row);
  }
  return current;
}

export async function laneForChat(
  db: Client,
  provider: string,
  externalChatId: string,
): Promise<LaneBindingRow | undefined> {
  const result = await db.execute({
    sql: `SELECT provider, external_chat_id, lane_kind, lane_name
          FROM transport_lane_bindings
          WHERE provider = ? AND external_chat_id = ? LIMIT 1`,
    args: [provider, externalChatId],
  });
  return result.rows[0] ? mapLane(result.rows[0]) : undefined;
}

/** Contract-G outbound fan-out query: chats are membership, not agent rows. */
export async function chatsForLane(
  db: Client,
  laneKind: LaneKind,
  laneName: string,
): Promise<LaneBindingRow[]> {
  const result = await db.execute({
    sql: `SELECT provider, external_chat_id, lane_kind, lane_name
          FROM transport_lane_bindings
          WHERE lane_kind = ? AND lane_name = ?
          ORDER BY provider, external_chat_id`,
    args: [laneKind, laneName],
  });
  return result.rows.map(mapLane);
}

function validated(input: LaneBindingRow): LaneBindingRow {
  const provider = required(input.provider, "provider");
  const externalChatId = required(input.externalChatId, "externalChatId");
  const laneName = required(input.laneName, "laneName");
  if (input.laneKind !== "thread" && input.laneKind !== "dm") {
    throw new Error(`unsupported lane kind ${String(input.laneKind)}`);
  }
  return { provider, externalChatId, laneKind: input.laneKind, laneName };
}

function mapLane(row: Row): LaneBindingRow {
  const laneKind = String(row.lane_kind);
  if (laneKind !== "thread" && laneKind !== "dm") {
    throw new Error(`invalid stored lane kind ${laneKind}`);
  }
  return {
    provider: String(row.provider),
    externalChatId: String(row.external_chat_id),
    laneKind,
    laneName: String(row.lane_name),
  };
}

function required(value: string, field: string): string {
  const clean = value.trim();
  if (!clean) throw new Error(`${field} is required`);
  return clean;
}
