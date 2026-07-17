type ChangeListener = (revision: number) => void;

export interface WaitForChangeOptions {
  timeoutMs: number;
  signal?: AbortSignal;
}

/**
 * Process-local invalidation signal for Gateway projections.
 *
 * Durable truth stays in SQLite; this bus only avoids polling after a committed
 * repository write. A missed signal is harmless because every reader carries a
 * durable cursor and performs a bounded read before waiting again.
 */
export class GatewayChangeBus {
  private readonly revisions = new Map<string, number>();
  private readonly listeners = new Map<string, Set<ChangeListener>>();
  private readonly waiters = new Map<string, number>();

  subscribe(key: string, listener: ChangeListener): () => void {
    const listeners = this.listeners.get(key) ?? new Set<ChangeListener>();
    listeners.add(listener);
    this.listeners.set(key, listeners);
    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) this.listeners.delete(key);
    };
  }

  publish(key: string): number {
    const revision = (this.revisions.get(key) ?? 0) + 1;
    this.revisions.set(key, revision);
    for (const listener of [...(this.listeners.get(key) ?? [])]) {
      listener(revision);
    }
    return revision;
  }

  revision(key: string): number {
    return this.revisions.get(key) ?? 0;
  }

  waiterCount(key: string): number {
    return this.waiters.get(key) ?? 0;
  }

  waitForChange(
    key: string,
    afterRevision: number,
    options: WaitForChangeOptions,
  ): Promise<number> {
    const current = this.revision(key);
    if (current > afterRevision || options.signal?.aborted) {
      return Promise.resolve(current);
    }

    this.waiters.set(key, this.waiterCount(key) + 1);
    return new Promise((resolve) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const finish = (revision: number) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        unsubscribe();
        options.signal?.removeEventListener("abort", onAbort);
        const remaining = this.waiterCount(key) - 1;
        if (remaining > 0) this.waiters.set(key, remaining);
        else this.waiters.delete(key);
        resolve(revision);
      };
      const onAbort = () => finish(this.revision(key));
      const unsubscribe = this.subscribe(key, finish);
      options.signal?.addEventListener("abort", onAbort, { once: true });
      timer = setTimeout(() => finish(this.revision(key)), Math.max(0, options.timeoutMs));
    });
  }
}

/** One invalidation bus for the Gateway process. Durable state remains in SQLite. */
export const gatewayChangeBus = new GatewayChangeBus();
