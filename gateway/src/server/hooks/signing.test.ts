import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  HOOK_PRIVATE_KEY_FILE,
  HookSigner,
  digestHookArtifact,
  verifyHookExecution,
} from "./signing";
import type { HookManifest } from "./types";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((path) => rm(path, { recursive: true })));
});

describe("hook execution signing", () => {
  it("persists an owner-only Ed25519 key and signs the canonical execution tuple", async () => {
    const directory = await temporaryDirectory();
    const entry = join(directory, "hook.py");
    await writeFile(entry, "print('one')\n");
    const hook = manifest(entry);
    const signer = await HookSigner.loadOrCreate(join(directory, "keys"));
    const artifactDigest = await digestHookArtifact(hook);

    const executedBy = signer.attest({
      invocationId: "hi_stable",
      hook,
      event: "before_send",
      artifactDigest,
      startedAt: 10,
      completedAt: 20,
      outcome: "continue",
    });

    expect(executedBy).toMatchObject({
      hookId: "redact",
      entrypoint: "main",
      runtime: "python3",
      artifactDigest,
      invocationId: "hi_stable",
      outcome: "continue",
      attestation: {
        algorithm: "ed25519",
        keyId: signer.keyId,
        startedAt: 10,
        completedAt: 20,
      },
    });
    expect(verifyHookExecution(executedBy, "before_send", signer.publicKey)).toBe(true);
    expect(
      verifyHookExecution({ ...executedBy, outcome: "tampered" }, "before_send", signer.publicKey),
    ).toBe(false);
    if (process.platform !== "win32") {
      expect((await stat(join(directory, "keys", HOOK_PRIVATE_KEY_FILE))).mode & 0o777).toBe(0o600);
    }
    expect(await readFile(join(directory, "keys", HOOK_PRIVATE_KEY_FILE), "utf8")).toContain(
      "PRIVATE KEY",
    );
    expect(JSON.stringify(executedBy)).not.toContain("PRIVATE KEY");

    const reopened = await HookSigner.loadOrCreate(join(directory, "keys"));
    expect(reopened.keyId).toBe(signer.keyId);
    expect(verifyHookExecution(executedBy, "before_send", reopened.publicKey)).toBe(true);
  });

  it("changes the artifact digest when executable bytes change", async () => {
    const directory = await temporaryDirectory();
    const entry = join(directory, "hook.py");
    await writeFile(entry, "print('one')\n");
    const hook = manifest(entry);
    const first = await digestHookArtifact(hook);

    await writeFile(entry, "print('two')\n");

    expect(await digestHookArtifact(hook)).not.toBe(first);
  });
});

function manifest(entry: string): HookManifest {
  return {
    version: 1,
    id: "redact",
    event: "before_send",
    order: 0,
    timeoutMs: 1_000,
    onFailure: "reject",
    enabled: true,
    manifestPath: join(join(entry, ".."), "redact.toml"),
    handler: {
      kind: "local",
      entry,
      entrypoint: "main",
      run: ["python3", entry],
      passEnv: [],
    },
  };
}

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "nexus-hook-signing-"));
  temporaryDirectories.push(directory);
  return directory;
}
