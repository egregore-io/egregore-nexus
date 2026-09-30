import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";

import { createClient, type Client } from "@libsql/client";

import { gatewayStoreConfig, type GatewayStoreConfig } from "./config";
import { migrateGatewayStore } from "./migrations";

let sharedStore: Promise<Client> | undefined;

/** Open a local Gateway store, configure SQLite for one writer, and apply migrations. */
export async function createGatewayStore(config: GatewayStoreConfig): Promise<Client> {
  await ensureParentDirectory(config.url);
  const db = createClient(config);
  try {
    await db.execute("PRAGMA journal_mode = WAL");
    await db.execute("PRAGMA busy_timeout = 5000");
    await db.execute("PRAGMA synchronous = NORMAL");
    await migrateGatewayStore(db);
    return db;
  } catch (error) {
    db.close();
    throw error;
  }
}

/** Return the sole process-wide Gateway database client. */
export function getGatewayStore(env: NodeJS.ProcessEnv = process.env): Promise<Client> {
  sharedStore ??= createGatewayStore(gatewayStoreConfig(env));
  return sharedStore;
}

/** Close the process-wide store during Gateway shutdown or hot reload. */
export async function closeGatewayStore(): Promise<void> {
  const pending = sharedStore;
  sharedStore = undefined;
  if (!pending) return;
  const db = await pending;
  db.close();
}

async function ensureParentDirectory(url: string): Promise<void> {
  const path = url.slice("file:".length);
  await mkdir(dirname(path), { recursive: true });
}
