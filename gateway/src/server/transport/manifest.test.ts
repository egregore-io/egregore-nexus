import { chmod, mkdir, mkdtemp, rename, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";
import { windowsFixtureAcl } from "../../../test-fixtures/windowsTransportAuthority";

import {
  loadTransportManifest,
  revalidateTransportManifest,
} from "./manifest";

const roots: string[] = [];

afterEach(async () => {
  const { rm } = await import("node:fs/promises");
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("transport manifest authority", { timeout: process.platform === "win32" ? 60_000 : 5_000 }, () => {
  it("loads only a contained, owned, immutable manifest and entry", async () => {
    const fixture = await makeFixture();
    const loaded = await loadTransportManifest("fake", { nexusHome: fixture.home });

    expect(loaded).toMatchObject({
      name: "fake",
      provider: "fake",
      args: ["--mode", "test"],
      config: { greeting: "hello" },
      secretRefs: { FAKE_TOKEN: "fake.token" },
    });
    expect(Object.isFrozen(loaded)).toBe(true);
    expect(Object.isFrozen(loaded.args)).toBe(true);
    expect(Object.isFrozen(loaded.config)).toBe(true);
    expect(Object.isFrozen(loaded.secretRefs)).toBe(true);
    await expect(revalidateTransportManifest(loaded)).resolves.toBeUndefined();
  });

  it("refuses containment escapes, symlinks, unsafe ownership/mode, and unknown keys", async () => {
    const fixture = await makeFixture();
    await writeFile(fixture.manifest, manifestText("../outside.mjs"), { mode: 0o600 });
    await expect(loadTransportManifest("fake", { nexusHome: fixture.home }))
      .rejects.toThrow(/contained|entry/i);

    await writeFile(fixture.manifest, `${manifestText("fake-bridge.mjs")}\nunknown = true\n`, { mode: 0o600 });
    await expect(loadTransportManifest("fake", { nexusHome: fixture.home }))
      .rejects.toThrow(/unknown/i);

    await writeFile(fixture.manifest, manifestText("fake-bridge.mjs"));
    if (process.platform === "win32") await windowsFixtureAcl(fixture.manifest, "writable");
    else await chmod(fixture.manifest, 0o622);
    await expect(loadTransportManifest("fake", { nexusHome: fixture.home }))
      .rejects.toThrow(/writable|mode|mutation/i);

    if (process.platform === "win32") {
      await windowsFixtureAcl(fixture.manifest, "secure");
      await windowsFixtureAcl(fixture.manifest, "wrong-owner");
      await expect(loadTransportManifest("fake", { nexusHome: fixture.home })).rejects.toThrow(/owner/i);
      await windowsFixtureAcl(fixture.manifest, "secure");
    } else {
      await chmod(fixture.manifest, 0o600);
      await expect(loadTransportManifest("fake", {
        nexusHome: fixture.home, expectedUid: (process.getuid?.() ?? 1000) + 1,
      })).rejects.toThrow(/owner/i);
    }

    await rename(fixture.entry, `${fixture.entry}.real`);
    if (process.platform === "win32") {
      const target = join(fixture.dir, "junction-target");
      await mkdir(target);
      await symlink(target, fixture.entry, "junction");
    } else await symlink(`${fixture.entry}.real`, fixture.entry);
    await expect(loadTransportManifest("fake", { nexusHome: fixture.home }))
      .rejects.toThrow(/symlink|no-follow|reparse/i);
  });

  it("fails spawn-time revalidation after manifest or entry replacement", async () => {
    const fixture = await makeFixture();
    const loaded = await loadTransportManifest("fake", { nexusHome: fixture.home });

    await writeFile(`${fixture.entry}.swap`, "#!/usr/bin/env node\nprocess.exit(2);\n", { mode: 0o700 });
    await rename(`${fixture.entry}.swap`, fixture.entry);
    if (process.platform === "win32") await windowsFixtureAcl(fixture.entry, "secure");
    await expect(revalidateTransportManifest(loaded)).rejects.toThrow(/changed|identity|hash/i);

    const loadedAgain = await loadTransportManifest("fake", { nexusHome: fixture.home });
    await writeFile(`${fixture.manifest}.swap`, manifestText("fake-bridge.mjs").replace("hello", "bye"), { mode: 0o600 });
    await rename(`${fixture.manifest}.swap`, fixture.manifest);
    if (process.platform === "win32") await windowsFixtureAcl(fixture.manifest, "secure");
    await expect(revalidateTransportManifest(loadedAgain)).rejects.toThrow(/changed|identity|hash/i);
  });

  it("rechecks directory authority before spawn even when both file bodies are unchanged", async () => {
    const fixture = await makeFixture();
    const loaded = await loadTransportManifest("fake", { nexusHome: fixture.home });
    if (process.platform === "win32") await windowsFixtureAcl(fixture.dir, "writable");
    else await chmod(fixture.dir, 0o722);
    await expect(revalidateTransportManifest(loaded)).rejects.toThrow(/mutation|writable|mode/i);
  });
});

async function makeFixture() {
  const root = await mkdtemp(join(tmpdir(), "nexus-transport-manifest-"));
  roots.push(root);
  const home = join(root, "home");
  const dir = join(home, "gateway", "transports.d");
  await mkdir(dir, { recursive: true, mode: 0o700 });
  await chmod(home, 0o700);
  await chmod(join(home, "gateway"), 0o700);
  await chmod(dir, 0o700);
  const entry = join(dir, "fake-bridge.mjs");
  const manifest = join(dir, "fake.toml");
  await writeFile(entry, "#!/usr/bin/env node\nprocess.exit(0);\n", { mode: 0o700 });
  await writeFile(manifest, manifestText("fake-bridge.mjs"), { mode: 0o600 });
  if (process.platform === "win32") await windowsFixtureAcl(home, "secure-tree");
  return { home, dir, entry, manifest };
}

function manifestText(entry: string): string {
  return [
    'name = "fake"',
    'provider = "fake"',
    `entry = ${JSON.stringify(entry)}`,
    'args = ["--mode", "test"]',
    '[config]',
    'greeting = "hello"',
    '[secretRefs]',
    'FAKE_TOKEN = "fake.token"',
    "",
  ].join("\n");
}
