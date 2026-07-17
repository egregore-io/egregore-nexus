import type { GatewayChangeBus } from "../store/changeBus";

export interface CanonicalLongPollPage<Row = unknown> {
  rows: Row[];
  before?: string;
  after?: string;
  rebased: boolean;
}

export interface CanonicalLongPollOptions<Row> {
  key: string;
  after?: string;
  waitMs: number;
  bus: GatewayChangeBus;
  read: () => Promise<CanonicalLongPollPage<Row>>;
  signal?: AbortSignal;
}

/**
 * Cursor-backed long poll with no database polling loop.
 *
 * The second read closes the commit-before-subscribe race: the revision is
 * captured first, durable state is checked again, and only then do we wait for
 * a process-local post-commit invalidation before performing the final read.
 */
export async function longPollCanonicalPage<Row>(
  options: CanonicalLongPollOptions<Row>,
): Promise<CanonicalLongPollPage<Row>> {
  const initial = await options.read();
  if (initial.rows.length > 0 || !options.after || options.waitMs <= 0) return initial;

  const revision = options.bus.revision(options.key);
  const raceClosingRead = await options.read();
  if (raceClosingRead.rows.length > 0 || options.signal?.aborted) return raceClosingRead;

  await options.bus.waitForChange(options.key, revision, {
    timeoutMs: Math.max(0, Math.min(30_000, Math.trunc(options.waitMs))),
    signal: options.signal,
  });
  return options.read();
}
