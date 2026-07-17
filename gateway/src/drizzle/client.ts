// Read-only Drizzle view for gateway display projections. The gateway owns no canonical database
// connection: its libSQL-compatible client sends SELECT frames over the daemon's local IPC, and
// the daemon executes them on its sole embedded connection. Tests may still bind `readDb()` to an
// injected in-memory libSQL client.
//
// `schema.gen.ts` is the introspected (`drizzle-kit pull`) mirror of the
// Rust-owned tables; in tests/dev we run the same DDL on an in-memory libSQL DB.
import { drizzle } from "drizzle-orm/libsql/node";
import type { Client } from "@libsql/client";

import { schema } from "./schema.gen";
import { createDaemonReadClient } from "@server/daemon/readClient";

/** The full Drizzle DB handle bound to the introspected schema. */
export type Db = ReturnType<typeof makeDrizzle>;

/**
 * The READ handle we expose to the rest of the gateway. It is the Drizzle DB
 * with the mutating builders (`insert`/`update`/`delete`) erased from the type —
 * a write is a compile error, not a runtime surprise. `.select(...)`, `.query`,
 * `.$client` (for read-only raw SQL like FTS5) remain.
 */
export type ReadDb = Omit<Db, "insert" | "update" | "delete">;

function makeDrizzle(client: Client) {
  return drizzle(client, { schema });
}

/**
 * Wrap a raw libSQL `Client` into the read-only Drizzle handle. Used by both the
 * production client below and the in-memory `seedDb()` in tests, so they run the
 * exact same read code path.
 */
export function readDb(client: Client): ReadDb {
  return makeDrizzle(client) as ReadDb;
}

/**
 * Build the production read view over the daemon's boot-scoped local IPC endpoint. Legacy DB
 * location/token variables are deliberately ignored; only the daemon opens the embedded store.
 */
export function createReadDb(env: NodeJS.ProcessEnv = process.env): ReadDb {
  return readDb(createDaemonReadClient({
    ...(env.NEXUS_HOME?.trim() ? { nexusHome: env.NEXUS_HOME.trim() } : {}),
  }));
}

// Lazily-constructed production singleton so importing this module (e.g. from a
// test that only needs `readDb`/types) does not require Turso env to be present.
let _db: ReadDb | undefined;

/** The shared production read handle (constructed once, on first use). */
export function getReadDb(): ReadDb {
  if (!_db) _db = createReadDb();
  return _db;
}
