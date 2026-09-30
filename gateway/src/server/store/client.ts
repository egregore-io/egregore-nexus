import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";

import {
  createClient,
  type Client,
  type InArgs,
  type InStatement,
  type Replicated,
  type ResultSet,
  type Transaction,
  type TransactionMode,
} from "@libsql/client";

import { gatewayStoreConfig, type GatewayStoreConfig } from "./config";
import { migrateGatewayStore } from "./migrations";

let sharedStore: Promise<Client> | undefined;

/** Open a local Gateway store, configure SQLite for one writer, and apply migrations. */
export async function createGatewayStore(config: GatewayStoreConfig): Promise<Client> {
  await ensureParentDirectory(config.url);
  const db = createClient(config);
  try {
    await db.execute("PRAGMA busy_timeout = 5000");
    await db.execute("PRAGMA journal_mode = WAL");
    await db.execute("PRAGMA synchronous = NORMAL");
    await migrateGatewayStore(db);
    return new SerializedGatewayClient(db);
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

/**
 * Keep the process's logical SQLite connections behind one async boundary.
 *
 * The local libSQL client detaches the connection used by an interactive
 * transaction and lazily opens another for concurrent client calls. SQLite's
 * busy wait is synchronous, so letting that second connection contend with a
 * transaction held by this same event loop can prevent the holder from ever
 * reaching commit. Queueing here leaves the event loop free and keeps the sole
 * Gateway writer deterministic.
 */
class SerializedGatewayClient implements Client {
  readonly #gate = new AsyncSerialGate();

  constructor(private readonly client: Client) {}

  execute(stmt: InStatement): Promise<ResultSet>;
  execute(sql: string, args?: InArgs): Promise<ResultSet>;
  execute(stmt: InStatement, args?: InArgs): Promise<ResultSet> {
    return this.#gate.run(() =>
      typeof stmt === "string" ? this.client.execute(stmt, args) : this.client.execute(stmt)
    );
  }

  batch(
    stmts: Array<InStatement | [string, InArgs?]>,
    mode?: TransactionMode,
  ): Promise<Array<ResultSet>> {
    return this.#gate.run(() => this.client.batch(stmts, mode));
  }

  migrate(stmts: InStatement[]): Promise<ResultSet[]> {
    return this.#gate.run(() => this.client.migrate(stmts));
  }

  async transaction(mode?: TransactionMode): Promise<Transaction> {
    const release = await this.#gate.acquire();
    try {
      const transaction = mode === undefined
        ? await this.client.transaction()
        : await this.client.transaction(mode);
      return serializeTransaction(transaction, release);
    } catch (error) {
      release();
      throw error;
    }
  }

  executeMultiple(sql: string): Promise<void> {
    return this.#gate.run(() => this.client.executeMultiple(sql));
  }

  sync(): Promise<Replicated> {
    return this.#gate.run(() => this.client.sync());
  }

  close(): void {
    this.client.close();
  }

  reconnect(): void {
    this.client.reconnect();
  }

  get closed(): boolean {
    return this.client.closed;
  }

  get protocol(): string {
    return this.client.protocol;
  }
}

class AsyncSerialGate {
  #tail = Promise.resolve();

  async acquire(): Promise<() => void> {
    let releaseTurn!: () => void;
    const turn = new Promise<void>((resolve) => {
      releaseTurn = resolve;
    });
    const previous = this.#tail;
    this.#tail = previous.then(() => turn);
    await previous;

    let released = false;
    return () => {
      if (released) return;
      released = true;
      releaseTurn();
    };
  }

  async run<T>(operation: () => Promise<T>): Promise<T> {
    const release = await this.acquire();
    try {
      return await operation();
    } finally {
      release();
    }
  }
}

function serializeTransaction(
  transaction: Transaction,
  releaseGate: () => void,
): Transaction {
  let released = false;
  const release = () => {
    if (released) return;
    released = true;
    releaseGate();
  };
  const operation = async <T>(run: () => Promise<T>): Promise<T> => {
    try {
      return await run();
    } finally {
      if (transaction.closed) release();
    }
  };
  const terminal = async (run: () => Promise<void>): Promise<void> => {
    try {
      await run();
    } finally {
      try {
        if (!transaction.closed) transaction.close();
      } finally {
        release();
      }
    }
  };

  return {
    execute: (stmt) => operation(() => transaction.execute(stmt)),
    batch: (stmts) => operation(() => transaction.batch(stmts)),
    executeMultiple: (sql) => operation(() => transaction.executeMultiple(sql)),
    rollback: () => terminal(() => transaction.rollback()),
    commit: () => terminal(() => transaction.commit()),
    close: () => {
      try {
        transaction.close();
      } finally {
        release();
      }
    },
    get closed() {
      return transaction.closed;
    },
  };
}
