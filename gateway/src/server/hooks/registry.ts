import { createHash } from "node:crypto";
import { watch as watchDirectory } from "node:fs";
import { mkdir, readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

import { loadHookManifest } from "./manifest";
import type { HookManifest, HookRegistryReload, HookRegistrySnapshot } from "./types";

export interface HookRegistryOptions {
  directory: string;
  supportedEvents: ReadonlySet<string>;
  now?: () => number;
  debounceMs?: number;
  watch?: HookWatchFactory;
  schedule?: HookReloadScheduler;
}

export type HookWatchFactory = (
  directory: string,
  listener: () => void,
) => { close(): void };

export type HookReloadScheduler = (
  callback: () => void,
  delayMs: number,
) => { cancel(): void };

export class HookRegistry {
  readonly #directory: string;
  readonly #supportedEvents: ReadonlySet<string>;
  readonly #now: () => number;
  readonly #debounceMs: number;
  readonly #watch: HookWatchFactory;
  readonly #schedule: HookReloadScheduler;
  #snapshot: HookRegistrySnapshot;
  #errors: readonly string[] = [];
  #watcher?: { close(): void };
  #scheduled?: { cancel(): void };
  #reloadChain: Promise<unknown> = Promise.resolve();
  readonly #listeners = new Set<(reload: HookRegistryReload) => void>();

  constructor(options: HookRegistryOptions) {
    this.#directory = options.directory;
    this.#supportedEvents = options.supportedEvents;
    this.#now = options.now ?? Date.now;
    this.#debounceMs = options.debounceMs ?? 100;
    this.#watch = options.watch ?? defaultWatch;
    this.#schedule = options.schedule ?? defaultSchedule;
    this.#snapshot = snapshot([], this.#now());
  }

  snapshot(): HookRegistrySnapshot {
    return this.#snapshot;
  }

  errors(): readonly string[] {
    return this.#errors;
  }

  onReload(listener: (reload: HookRegistryReload) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  async start(): Promise<void> {
    if (this.#watcher) return;
    await this.reload();
    this.#watcher = this.#watch(this.#directory, () => this.#queueReload());
  }

  async settled(): Promise<void> {
    await this.#reloadChain;
  }

  close(): void {
    this.#scheduled?.cancel();
    this.#scheduled = undefined;
    this.#watcher?.close();
    this.#watcher = undefined;
  }

  async reload(): Promise<HookRegistryReload> {
    await mkdir(this.#directory, { recursive: true });
    const entries = await readdir(this.#directory, { withFileTypes: true });
    const manifestEntries = entries
      .filter((entry) => entry.name.endsWith(".toml"))
      .sort((left, right) => left.name.localeCompare(right.name));
    const errors: string[] = [];
    const hooks: HookManifest[] = [];

    for (const entry of manifestEntries) {
      const path = join(this.#directory, entry.name);
      if (!entry.isFile()) {
        errors.push(`${path}: manifest must be a regular file`);
        continue;
      }
      try {
        hooks.push(await loadHookManifest(path, this.#supportedEvents));
      } catch (error) {
        errors.push(error instanceof Error ? error.message : String(error));
      }
    }

    const ids = new Set<string>();
    for (const hook of hooks) {
      if (ids.has(hook.id)) errors.push(`duplicate hook id ${hook.id}`);
      ids.add(hook.id);
    }

    if (errors.length > 0) {
      this.#errors = Object.freeze(errors);
      return this.#emit({ activated: false, snapshot: this.#snapshot, errors: this.#errors });
    }

    hooks.sort((left, right) => left.order - right.order || left.id.localeCompare(right.id));
    let artifactDigests: string[];
    try {
      artifactDigests = await Promise.all(hooks.map(async (hook) =>
        createHash("sha256").update(await readFile(hook.handler.entry)).digest("hex")
      ));
    } catch (error) {
      this.#errors = Object.freeze([`hook artifact changed during reload: ${String(error)}`]);
      return this.#emit({ activated: false, snapshot: this.#snapshot, errors: this.#errors });
    }
    this.#snapshot = snapshot(hooks, this.#now(), artifactDigests);
    this.#errors = [];
    return this.#emit({ activated: true, snapshot: this.#snapshot, errors: [] });
  }

  #emit(reload: HookRegistryReload): HookRegistryReload {
    for (const listener of this.#listeners) {
      try {
        listener(reload);
      } catch (error) {
        process.stderr.write(`Gateway hook reload listener failed: ${String(error)}\n`);
      }
    }
    return reload;
  }

  #queueReload(): void {
    this.#scheduled?.cancel();
    this.#scheduled = this.#schedule(() => {
      this.#scheduled = undefined;
      this.#reloadChain = this.#reloadChain.then(() => this.reload()).catch(() => undefined);
    }, this.#debounceMs);
  }
}

function snapshot(
  hooks: HookManifest[],
  createdAt: number,
  artifactDigests: readonly string[] = [],
): HookRegistrySnapshot {
  const frozenHooks = Object.freeze(hooks.map((hook) => Object.freeze(hook)));
  const canonical = JSON.stringify({ hooks: frozenHooks, artifactDigests });
  return Object.freeze({
    generation: `sha256:${createHash("sha256").update(canonical).digest("hex")}`,
    hooks: frozenHooks,
    createdAt,
  });
}

function defaultWatch(directory: string, listener: () => void): { close(): void } {
  return watchDirectory(directory, () => listener());
}

function defaultSchedule(callback: () => void, delayMs: number): { cancel(): void } {
  const handle = setTimeout(callback, delayMs);
  handle.unref();
  return { cancel: () => clearTimeout(handle) };
}
