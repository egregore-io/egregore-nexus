/**
 * Plain-Node durable developer-event source — the `developerEvents.ts` twin for the
 * packaged/plain serve path, which cannot load drizzle-backed TypeScript (the
 * 188×-ERR_MODULE_NOT_FOUND class from 2026-07-09: durable `sys.*` subscribes answered
 * "durable developer events unavailable on this gateway build" whenever the gateway ran via
 * `scripts/gateway-serve.mjs`). Same read-only contract: `since(topic, afterSeq)` over the
 * daemon-owned `developer_events` rows, mapped to the wire envelope. Never opens the store.
 *
 * Precedent for the .mjs twin pattern: `daemonPushRelay.mjs`.
 */
import { callDaemonQuery } from "../daemon/ipcQuery.mjs";

const SINCE_SQL =
  "SELECT topic, seq, kind, message_id, thread_name, dm_name, from_name, agent_name, " +
  "session_id, lifecycle, current_work, data_json, created_at " +
  "FROM developer_events WHERE topic = ? AND seq > ? ORDER BY seq ASC";

/** Row → wire envelope; mirrors developerEvents.ts `rowToEnvelope`. */
function rowToEnvelope(row) {
  const topic = row.topic ?? "";
  const seq = Number(row.seq ?? 0);
  const ts = Number(row.created_at ?? 0);
  if (!topic || seq <= 0 || ts <= 0) return undefined;
  let data;
  if (row.data_json) {
    try {
      data = JSON.parse(row.data_json);
    } catch {
      data = undefined;
    }
  }
  const kind = row.kind === "agent_lifecycle"
    ? "agent_lifecycle"
    : row.kind === "action"
      ? "action"
      : "message";
  return {
    kind,
    topic,
    seq,
    ts,
    ...(row.thread_name ? { thread: row.thread_name } : {}),
    ...(row.dm_name ? { dm: row.dm_name } : {}),
    ...(row.from_name ? { from: row.from_name } : {}),
    ...(row.message_id ? { messageId: row.message_id } : {}),
    ...(row.agent_name ? { agent: row.agent_name } : {}),
    ...(row.session_id ? { sessionId: row.session_id } : {}),
    ...(row.lifecycle ? { lifecycle: row.lifecycle } : {}),
    ...(row.current_work ? { currentWork: row.current_work } : {}),
    ...(data !== undefined ? { data } : {}),
  };
}

/** Same shape as developerEvents.ts; query/client injection is retained only as a test seam. */
export function createDeveloperEventSource(options = {}) {
  const query = typeof options.query === "function"
    ? options.query
    : typeof options.execute === "function"
      ? async (_method, params) => options.execute(params)
      : callDaemonQuery;
  return {
    async since(topic, afterSeq) {
      const result = await query("local.store.read", {
        sql: SINCE_SQL,
        args: [topic, afterSeq],
      });
      return namedRows(result).map(rowToEnvelope).filter((row) => row !== undefined);
    },
  };
}

function namedRows(result) {
  const columns = Array.isArray(result?.columns) ? result.columns : [];
  return (result?.rows ?? []).map((row) => {
    if (!Array.isArray(row)) return row;
    return Object.fromEntries(columns.map((column, index) => [column, row[index]]));
  });
}
