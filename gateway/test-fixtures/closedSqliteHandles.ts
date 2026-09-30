import { setImmediate } from "node:timers/promises";
import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";

/**
 * Test teardown only: libsql 0.5.29 retains finalized statements until V8 collection, even after
 * Client.close(). Windows refuses unlink while those native handles remain. Collect only after
 * callers have closed every client; this is not a production close guarantee or an unlink retry.
 * https://github.com/tursodatabase/libsql-client-ts/issues/350
 */
export async function collectClosedSqliteHandles(): Promise<void> {
  if (process.platform !== "win32") return;
  setFlagsFromString("--expose-gc");
  const collect = runInNewContext("gc") as () => void;
  collect();
  await setImmediate();
}
