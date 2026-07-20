import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, open } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";

import { parse } from "smol-toml";

const NAME_PATTERN = /^[a-z0-9][a-z0-9_-]{0,63}$/;
const PROVIDER_PATTERN = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const ENV_PATTERN = /^[A-Z_][A-Z0-9_]*$/;
const SECRET_KEY_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const ROOT_KEYS = new Set(["name", "provider", "entry", "args", "config", "secretRefs"]);

interface FileAuthority {
  path: string;
  dev: number;
  ino: number;
  uid: number;
  mode: number;
  size: number;
  sha256: string;
}

export interface LoadedTransportManifest {
  readonly name: string;
  readonly provider: string;
  readonly manifestPath: string;
  readonly entryPath: string;
  readonly args: readonly string[];
  readonly config: Readonly<Record<string, unknown>>;
  readonly secretRefs: Readonly<Record<string, string>>;
  readonly authority: Readonly<{
    expectedUid?: number;
    manifest: FileAuthority;
    entry: FileAuthority;
  }>;
}

export interface LoadTransportManifestOptions {
  nexusHome: string;
  expectedUid?: number;
}

export class TransportManifestError extends Error {
  constructor(readonly path: string, message: string) {
    super(`${path}: ${message}`);
    this.name = "TransportManifestError";
  }
}

export async function loadTransportManifest(
  name: string,
  options: LoadTransportManifestOptions,
): Promise<LoadedTransportManifest> {
  if (!NAME_PATTERN.test(name)) throw new TransportManifestError(name, "invalid manifest name");
  const home = resolve(options.nexusHome);
  const directory = join(home, "gateway", "transports.d");
  const manifestPath = join(directory, `${name}.toml`);
  const expectedUid = options.expectedUid ?? process.getuid?.();
  await validateDirectoryChain(home, directory, expectedUid);
  const manifest = await captureRegularFile(manifestPath, expectedUid, "manifest");

  let parsed: unknown;
  try {
    parsed = parse((await readCaptured(manifest)).toString("utf8"));
  } catch (error) {
    throw new TransportManifestError(manifestPath, `invalid TOML: ${errorMessage(error)}`);
  }
  const root = record(parsed, manifestPath, "manifest root");
  for (const key of Object.keys(root)) {
    if (!ROOT_KEYS.has(key)) throw new TransportManifestError(manifestPath, `unknown key ${key}`);
  }
  const manifestName = string(root.name, manifestPath, "name");
  if (manifestName !== name) throw new TransportManifestError(manifestPath, "name must match filename");
  const provider = string(root.provider, manifestPath, "provider");
  if (!PROVIDER_PATTERN.test(provider)) {
    throw new TransportManifestError(manifestPath, "provider is invalid");
  }
  const entryValue = string(root.entry, manifestPath, "entry");
  if (isAbsolute(entryValue)) {
    throw new TransportManifestError(manifestPath, "entry must be contained and relative");
  }
  const entryPath = resolve(directory, entryValue);
  if (entryPath === directory || !entryPath.startsWith(`${directory}${sep}`)) {
    throw new TransportManifestError(manifestPath, "entry escapes the contained transport directory");
  }
  await validateDirectoryChain(directory, resolve(entryPath, ".."), expectedUid);
  const entry = await captureRegularFile(entryPath, expectedUid, "entry");
  if ((entry.mode & 0o111) === 0) {
    throw new TransportManifestError(entryPath, "entry must be executable");
  }

  const args = stringArray(root.args ?? [], manifestPath, "args");
  const config = root.config === undefined
    ? {}
    : cloneRecord(record(root.config, manifestPath, "config"));
  const rawRefs = root.secretRefs === undefined
    ? {}
    : record(root.secretRefs, manifestPath, "secretRefs");
  const secretRefs: Record<string, string> = {};
  for (const [environmentName, value] of Object.entries(rawRefs)) {
    if (!ENV_PATTERN.test(environmentName)) {
      throw new TransportManifestError(manifestPath, `invalid secretRefs name ${environmentName}`);
    }
    const key = string(value, manifestPath, `secretRefs.${environmentName}`);
    if (!SECRET_KEY_PATTERN.test(key)) {
      throw new TransportManifestError(manifestPath, `invalid secret key ${key}`);
    }
    secretRefs[environmentName] = key;
  }

  return deepFreeze({
    name,
    provider,
    manifestPath,
    entryPath,
    args,
    config,
    secretRefs,
    authority: { expectedUid, manifest, entry },
  });
}

export async function revalidateTransportManifest(
  manifest: LoadedTransportManifest,
): Promise<void> {
  const currentManifest = await captureRegularFile(
    manifest.manifestPath,
    manifest.authority.expectedUid,
    "manifest",
  );
  const currentEntry = await captureRegularFile(
    manifest.entryPath,
    manifest.authority.expectedUid,
    "entry",
  );
  requireSameAuthority(manifest.authority.manifest, currentManifest, "manifest");
  requireSameAuthority(manifest.authority.entry, currentEntry, "entry");
}

async function validateDirectoryChain(
  root: string,
  target: string,
  expectedUid: number | undefined,
): Promise<void> {
  const from = resolve(root);
  const to = resolve(target);
  const rel = relative(from, to);
  if (rel.startsWith("..") || isAbsolute(rel)) {
    throw new TransportManifestError(target, "path escapes authority root");
  }
  const components = [from];
  let cursor = from;
  for (const part of rel.split(sep).filter(Boolean)) {
    cursor = join(cursor, part);
    components.push(cursor);
  }
  for (const path of components) {
    const stat = await safeLstat(path, "directory");
    if (stat.isSymbolicLink()) throw new TransportManifestError(path, "no-follow ancestry refused symlink");
    if (!stat.isDirectory()) throw new TransportManifestError(path, "authority component is not a directory");
    requireOwnershipAndMode(path, stat.uid, stat.mode, expectedUid);
  }
}

async function captureRegularFile(
  path: string,
  expectedUid: number | undefined,
  label: string,
): Promise<FileAuthority> {
  const before = await safeLstat(path, label);
  if (before.isSymbolicLink()) throw new TransportManifestError(path, `${label} no-follow refused symlink`);
  if (!before.isFile()) throw new TransportManifestError(path, `${label} must be a regular file`);
  requireOwnershipAndMode(path, before.uid, before.mode, expectedUid);
  let handle;
  try {
    handle = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    const fdStat = await handle.stat();
    if (!fdStat.isFile() || fdStat.dev !== before.dev || fdStat.ino !== before.ino) {
      throw new TransportManifestError(path, `${label} identity changed while opening`);
    }
    const bytes = await handle.readFile();
    const after = await lstat(path);
    if (after.isSymbolicLink() || after.dev !== fdStat.dev || after.ino !== fdStat.ino) {
      throw new TransportManifestError(path, `${label} identity changed while reading`);
    }
    return {
      path,
      dev: fdStat.dev,
      ino: fdStat.ino,
      uid: fdStat.uid,
      mode: fdStat.mode & 0o777,
      size: bytes.byteLength,
      sha256: createHash("sha256").update(bytes).digest("hex"),
    };
  } catch (error) {
    if (error instanceof TransportManifestError) throw error;
    throw new TransportManifestError(path, `${label} no-follow read failed: ${errorMessage(error)}`);
  } finally {
    await handle?.close();
  }
}

async function readCaptured(authority: FileAuthority): Promise<Buffer> {
  const handle = await open(authority.path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const stat = await handle.stat();
    if (stat.dev !== authority.dev || stat.ino !== authority.ino) {
      throw new TransportManifestError(authority.path, "manifest identity changed before parse");
    }
    const bytes = await handle.readFile();
    const hash = createHash("sha256").update(bytes).digest("hex");
    if (hash !== authority.sha256) {
      throw new TransportManifestError(authority.path, "manifest hash changed before parse");
    }
    return bytes;
  } finally {
    await handle.close();
  }
}

function requireSameAuthority(before: FileAuthority, after: FileAuthority, label: string): void {
  if (
    before.dev !== after.dev ||
    before.ino !== after.ino ||
    before.uid !== after.uid ||
    before.mode !== after.mode ||
    before.size !== after.size ||
    before.sha256 !== after.sha256
  ) {
    throw new TransportManifestError(before.path, `${label} identity or hash changed before spawn`);
  }
}

function requireOwnershipAndMode(
  path: string,
  actualUid: number,
  mode: number,
  expectedUid: number | undefined,
): void {
  if (expectedUid !== undefined && actualUid !== expectedUid) {
    throw new TransportManifestError(path, `wrong owner uid ${actualUid}; expected ${expectedUid}`);
  }
  if ((mode & 0o022) !== 0) {
    throw new TransportManifestError(path, "group/world-writable mode refused");
  }
}

async function safeLstat(path: string, label: string) {
  try {
    return await lstat(path);
  } catch (error) {
    throw new TransportManifestError(path, `${label} is unavailable: ${errorMessage(error)}`);
  }
}

function record(value: unknown, path: string, field: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TransportManifestError(path, `${field} must be a table`);
  }
  return value as Record<string, unknown>;
}

function cloneRecord(value: Record<string, unknown>): Record<string, unknown> {
  return structuredClone(value) as Record<string, unknown>;
}

function string(value: unknown, path: string, field: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new TransportManifestError(path, `${field} must be a non-empty string`);
  }
  return value.trim();
}

function stringArray(value: unknown, path: string, field: string): string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw new TransportManifestError(path, `${field} must be an array of strings`);
  }
  return [...value];
}

function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value as Record<string, unknown>)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
