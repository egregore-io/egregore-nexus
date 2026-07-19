import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { loadHookManifest } from "./manifest";

const homes: string[] = [];

afterEach(async () => {
  await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});

async function fixture(manifest: string): Promise<{ root: string; path: string }> {
  const root = await mkdtemp(join(tmpdir(), "nexus-hook-manifest-"));
  homes.push(root);
  const path = join(root, "010-example.toml");
  await writeFile(path, manifest, "utf8");
  await writeFile(join(root, "hook.py"), "print('{}')\n", "utf8");
  return { root, path };
}

describe("Gateway hook manifests", () => {
  it("parses a local hook and resolves its entry without invoking a shell", async () => {
    const { root, path } = await fixture(`
version = 1
id = "redact-secrets"
event = "before_send"
order = 10
timeout_ms = 500
on_failure = "reject"

[handler]
kind = "local"
entry = "./hook.py"
entrypoint = "main"
run = ["python3", "{entry}"]
pass_env = ["SAFE_FLAG"]
`);

    await expect(
      loadHookManifest(path, new Set(["before_send", "after_receipt"])),
    ).resolves.toEqual({
      version: 1,
      id: "redact-secrets",
      event: "before_send",
      order: 10,
      timeoutMs: 500,
      onFailure: "reject",
      enabled: true,
      manifestPath: path,
      handler: {
        kind: "local",
        entry: join(root, "hook.py"),
        entrypoint: "main",
        run: ["python3", join(root, "hook.py")],
        passEnv: ["SAFE_FLAG"],
      },
    });
  });

  it("applies bounded defaults", async () => {
    const { path } = await fixture(`
version = 1
id = "observe"
event = "after_receipt"

[handler]
kind = "local"
entry = "hook.py"
run = ["{entry}"]
`);

    const loaded = await loadHookManifest(path, new Set(["after_receipt"]));
    expect(loaded).toMatchObject({
      order: 0,
      timeoutMs: 1000,
      onFailure: "continue",
      enabled: true,
      handler: { entrypoint: "main", passEnv: [] },
    });
  });

  it("rejects path-shaped provenance entrypoints", async () => {
    const { path } = await fixture(`
version = 1
id = "unsafe-entrypoint"
event = "before_send"

[handler]
kind = "local"
entry = "hook.py"
entrypoint = "/private/main"
run = ["python3", "{entry}"]
`);

    await expect(loadHookManifest(path, new Set(["before_send"]))).rejects.toThrow(
      /handler\.entrypoint.*logical identifier/i,
    );
  });

  it.each([
    ["unknown event", 'event = "before_send"', 'event = "agent_token"', "unsupported hook event"],
    ["unsupported version", "version = 1", "version = 2", "version must be 1"],
    ["excessive timeout", "timeout_ms = 1000", "timeout_ms = 30001", "timeout_ms must be between"],
  ])("rejects %s", async (_label, search, replacement, message) => {
    const { path } = await fixture(`
version = 1
id = "invalid"
event = "before_send"
timeout_ms = 1000

[handler]
kind = "local"
entry = "hook.py"
run = ["{entry}"]
`);
    const original = await (await import("node:fs/promises")).readFile(path, "utf8");
    await writeFile(
      path,
      original.replace(search, replacement),
      "utf8",
    );
    await expect(loadHookManifest(path, new Set(["before_send"]))).rejects.toThrow(message);
  });

  it("rejects fail-closed policy on an observational event", async () => {
    const { path } = await fixture(`
version = 1
id = "invalid-observer"
event = "after_receipt"
on_failure = "reject"

[handler]
kind = "local"
entry = "hook.py"
run = ["{entry}"]
`);
    await expect(loadHookManifest(path, new Set(["after_receipt"]))).rejects.toThrow(
      "reject is valid only for before_send",
    );
  });

  it("rejects a symlinked entry artifact", async () => {
    const { root, path } = await fixture(`
version = 1
id = "linked"
event = "before_send"

[handler]
kind = "local"
entry = "linked.py"
run = ["{entry}"]
`);
    await symlink(join(root, "hook.py"), join(root, "linked.py"));
    await expect(loadHookManifest(path, new Set(["before_send"]))).rejects.toThrow("regular file");
  });
});
