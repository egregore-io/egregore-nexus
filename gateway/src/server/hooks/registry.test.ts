import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { HookRegistry } from "./registry";

const homes: string[] = [];

afterEach(async () => {
  await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

async function hookDirectory(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "nexus-hook-registry-"));
  homes.push(root);
  await writeFile(join(root, "hook.sh"), "#!/bin/sh\nprintf '{}\\n'\n", "utf8");
  return root;
}

async function writeManifest(
  directory: string,
  filename: string,
  values: { id: string; order: number; event?: string },
): Promise<void> {
  await writeFile(
    join(directory, filename),
    `
version = 1
id = "${values.id}"
event = "${values.event ?? "before_send"}"
order = ${values.order}

[handler]
kind = "local"
entry = "hook.sh"
run = ["{entry}"]
`,
    "utf8",
  );
}

describe("Gateway hook registry", () => {
  it("sorts one atomic generation by order then id", async () => {
    const directory = await hookDirectory();
    await writeManifest(directory, "030-zeta.toml", { id: "zeta", order: 30 });
    await writeManifest(directory, "020-beta.toml", { id: "beta", order: 20 });
    await writeManifest(directory, "020-alpha.toml", { id: "alpha", order: 20 });

    const registry = new HookRegistry({
      directory,
      supportedEvents: new Set(["before_send", "after_receipt"]),
    });
    const result = await registry.reload();

    expect(result.activated).toBe(true);
    expect(result.errors).toEqual([]);
    expect(registry.snapshot().hooks.map((hook) => hook.id)).toEqual(["alpha", "beta", "zeta"]);
    expect(registry.snapshot().hooks[0]?.manifestPath).toBe(join(directory, "020-alpha.toml"));
    expect(registry.snapshot().generation).toMatch(/^sha256:/);
  });

  it("keeps the last valid generation when a replacement is malformed", async () => {
    const directory = await hookDirectory();
    await writeManifest(directory, "010-stable.toml", { id: "stable", order: 10 });
    const registry = new HookRegistry({
      directory,
      supportedEvents: new Set(["before_send"]),
    });
    await registry.reload();
    const stable = registry.snapshot();

    await writeFile(join(directory, "010-stable.toml"), "not valid toml = [", "utf8");
    const rejected = await registry.reload();

    expect(rejected.activated).toBe(false);
    expect(rejected.errors).toHaveLength(1);
    expect(registry.snapshot()).toBe(stable);
  });

  it("changes generation when a handler artifact changes", async () => {
    const directory = await hookDirectory();
    await writeManifest(directory, "010-stable.toml", { id: "stable", order: 10 });
    const registry = new HookRegistry({
      directory,
      supportedEvents: new Set(["before_send"]),
    });
    await registry.reload();
    const first = registry.snapshot().generation;

    await writeFile(join(directory, "hook.sh"), "#!/bin/sh\nprintf '{\"changed\":true}\\n'\n", "utf8");
    await registry.reload();

    expect(registry.snapshot().generation).not.toBe(first);
  });

  it("rejects duplicate ids without activating a partial set", async () => {
    const directory = await hookDirectory();
    await writeManifest(directory, "010-one.toml", { id: "same", order: 10 });
    await writeManifest(directory, "020-two.toml", { id: "same", order: 20 });
    const registry = new HookRegistry({
      directory,
      supportedEvents: new Set(["before_send"]),
    });

    const result = await registry.reload();
    expect(result.activated).toBe(false);
    expect(result.errors[0]).toContain("duplicate hook id same");
    expect(registry.snapshot().hooks).toEqual([]);
  });

  it("debounces file events and atomically reloads the active generation", async () => {
    const directory = await hookDirectory();
    await writeManifest(directory, "010-live.toml", { id: "live", order: 10 });
    let onChange: (() => void) | undefined;
    let scheduled: (() => void) | undefined;
    let cancelled = 0;
    let watcherClosed = false;
    const registry = new HookRegistry({
      directory,
      supportedEvents: new Set(["before_send"]),
      watch: (_directory, listener) => {
        onChange = listener;
        return { close: () => (watcherClosed = true) };
      },
      schedule: (callback) => {
        scheduled = callback;
        return { cancel: () => cancelled++ };
      },
    });

    await registry.start();
    const firstGeneration = registry.snapshot().generation;
    await writeManifest(directory, "010-live.toml", { id: "live", order: 50 });
    onChange?.();
    onChange?.();
    expect(cancelled).toBe(1);
    expect(scheduled).toBeTypeOf("function");
    scheduled?.();
    await registry.settled();

    expect(registry.snapshot().generation).not.toBe(firstGeneration);
    expect(registry.snapshot().hooks[0]?.order).toBe(50);
    registry.close();
    expect(watcherClosed).toBe(true);
  });
});
