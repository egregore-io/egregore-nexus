import {
  createHash,
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  sign,
  verify,
  type KeyObject,
} from "node:crypto";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";

import type { HookExecutedBy } from "@shared/types";

import { isPlainObject } from "./merge";
import type { HookManifest } from "./types";

export const HOOK_PRIVATE_KEY_FILE = "message-hooks-ed25519-private.pem";

export interface HookAttestationInput {
  invocationId: string;
  hook: HookManifest;
  event: string;
  artifactDigest: string;
  startedAt: number;
  completedAt: number;
  outcome: string;
}

interface HookAttestation {
  algorithm: "ed25519";
  keyId: string;
  startedAt: number;
  completedAt: number;
  signature: string;
}

export class HookSigner {
  readonly #privateKey: KeyObject;
  readonly publicKey: KeyObject;
  readonly keyId: string;

  private constructor(privateKey: KeyObject) {
    this.#privateKey = privateKey;
    this.publicKey = createPublicKey(privateKey);
    const publicDer = this.publicKey.export({ type: "spki", format: "der" });
    this.keyId = `sha256:${createHash("sha256").update(publicDer).digest("hex")}`;
  }

  static async loadOrCreate(keysDirectory: string): Promise<HookSigner> {
    await mkdir(keysDirectory, { recursive: true, mode: 0o700 });
    await chmod(keysDirectory, 0o700);
    const privatePath = join(keysDirectory, HOOK_PRIVATE_KEY_FILE);
    let pem: string;
    try {
      pem = await readFile(privatePath, "utf8");
    } catch (error) {
      if (!hasCode(error, "ENOENT")) throw error;
      const generated = String(
        generateKeyPairSync("ed25519").privateKey.export({
          type: "pkcs8",
          format: "pem",
        }),
      );
      try {
        await writeFile(privatePath, generated, { encoding: "utf8", flag: "wx", mode: 0o600 });
        pem = generated;
      } catch (writeError) {
        if (!hasCode(writeError, "EEXIST")) throw writeError;
        pem = await readFile(privatePath, "utf8");
      }
    }
    await chmod(privatePath, 0o600);
    return new HookSigner(createPrivateKey(pem));
  }

  attest(input: HookAttestationInput): HookExecutedBy {
    const attestation: HookAttestation = {
      algorithm: "ed25519",
      keyId: this.keyId,
      startedAt: input.startedAt,
      completedAt: input.completedAt,
      signature: sign(null, canonicalExecution(input, this.keyId), this.#privateKey).toString(
        "base64url",
      ),
    };
    return {
      hookId: input.hook.id,
      entrypoint: input.hook.handler.entrypoint,
      runtime: runtimeIdentifier(input.hook),
      artifactDigest: input.artifactDigest,
      invocationId: input.invocationId,
      outcome: input.outcome,
      attestation,
    };
  }

  publicKeyPem(): string {
    return String(this.publicKey.export({ type: "spki", format: "pem" }));
  }
}

export async function digestHookArtifact(hook: HookManifest): Promise<string> {
  const bytes = await readFile(hook.handler.entry);
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

export function verifyHookExecution(
  executedBy: HookExecutedBy,
  event: string,
  publicKey: KeyObject,
): boolean {
  if (!isAttestation(executedBy.attestation)) return false;
  const input: HookAttestationInput = {
    invocationId: executedBy.invocationId,
    hook: {
      version: 1,
      id: executedBy.hookId,
      event,
      order: 0,
      timeoutMs: 1,
      onFailure: "continue",
      enabled: true,
      manifestPath: "",
      handler: {
        kind: "local",
        entry: "",
        entrypoint: executedBy.entrypoint,
        run: [executedBy.runtime],
        passEnv: [],
      },
    },
    event,
    artifactDigest: executedBy.artifactDigest,
    startedAt: executedBy.attestation.startedAt,
    completedAt: executedBy.attestation.completedAt,
    outcome: executedBy.outcome,
  };
  try {
    return verify(
      null,
      canonicalExecution(input, executedBy.attestation.keyId),
      publicKey,
      Buffer.from(executedBy.attestation.signature, "base64url"),
    );
  } catch {
    return false;
  }
}

function canonicalExecution(input: HookAttestationInput, keyId: string): Buffer {
  return Buffer.from(
    JSON.stringify([
      "nexus.message-hook-attestation/v1",
      keyId,
      input.invocationId,
      input.hook.id,
      input.hook.handler.entrypoint,
      runtimeIdentifier(input.hook),
      input.event,
      input.artifactDigest,
      input.startedAt,
      input.completedAt,
      input.outcome,
    ]),
  );
}

function runtimeIdentifier(hook: HookManifest): string {
  return basename(hook.handler.run[0] ?? hook.handler.kind);
}

function isAttestation(value: unknown): value is HookAttestation {
  return (
    isPlainObject(value) &&
    value.algorithm === "ed25519" &&
    typeof value.keyId === "string" &&
    typeof value.startedAt === "number" &&
    typeof value.completedAt === "number" &&
    typeof value.signature === "string"
  );
}

function hasCode(error: unknown, code: string): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === code;
}
