import { lstat, readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

import { parse } from "smol-toml";

import type { HookFailurePolicy, HookManifest } from "./types";

const ID_PATTERN = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const ENV_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const ENTRYPOINT_PATTERN = /^[A-Za-z_][A-Za-z0-9_.:-]{0,127}$/;

export class HookManifestError extends Error {
  constructor(
    readonly manifestPath: string,
    message: string,
  ) {
    super(`${manifestPath}: ${message}`);
    this.name = "HookManifestError";
  }
}

export async function loadHookManifest(
  manifestPath: string,
  supportedEvents: ReadonlySet<string>,
): Promise<HookManifest> {
  await requireRegularFile(manifestPath, manifestPath, "manifest");
  let raw: unknown;
  try {
    raw = parse(await readFile(manifestPath, "utf8"));
  } catch (error) {
    throw manifestError(manifestPath, `invalid TOML: ${errorMessage(error)}`);
  }
  const table = requireRecord(raw, manifestPath, "manifest root");
  const version = requireInteger(table.version, manifestPath, "version");
  if (version !== 1) throw manifestError(manifestPath, "version must be 1");

  const id = requireString(table.id, manifestPath, "id");
  if (!ID_PATTERN.test(id)) {
    throw manifestError(manifestPath, "id must match [a-z0-9][a-z0-9._-]{0,63}");
  }
  const event = requireString(table.event, manifestPath, "event");
  if (!supportedEvents.has(event)) {
    throw manifestError(manifestPath, `unsupported hook event ${JSON.stringify(event)}`);
  }

  const order = optionalInteger(table.order, 0, manifestPath, "order");
  const timeoutMs = optionalInteger(table.timeout_ms, 1_000, manifestPath, "timeout_ms");
  if (timeoutMs < 1 || timeoutMs > 30_000) {
    throw manifestError(manifestPath, "timeout_ms must be between 1 and 30000");
  }
  const onFailure = optionalEnum<HookFailurePolicy>(
    table.on_failure,
    "continue",
    ["continue", "reject"],
    manifestPath,
    "on_failure",
  );
  if (event !== "before_send" && onFailure === "reject") {
    throw manifestError(manifestPath, "on_failure=reject is valid only for before_send");
  }
  const enabled = optionalBoolean(table.enabled, true, manifestPath, "enabled");

  const handler = requireRecord(table.handler, manifestPath, "handler");
  const kind = requireString(handler.kind, manifestPath, "handler.kind");
  if (kind !== "local") throw manifestError(manifestPath, "handler.kind must be local");
  const relativeEntry = requireString(handler.entry, manifestPath, "handler.entry");
  const entry = resolve(dirname(manifestPath), relativeEntry);
  await requireRegularFile(entry, manifestPath, "handler.entry");
  const entrypoint = optionalString(handler.entrypoint, "main", manifestPath, "handler.entrypoint");
  if (!ENTRYPOINT_PATTERN.test(entrypoint)) {
    throw manifestError(manifestPath, "handler.entrypoint must be a logical identifier");
  }
  const run = requireStringArray(handler.run, manifestPath, "handler.run");
  if (run.length === 0) throw manifestError(manifestPath, "handler.run must not be empty");
  const passEnv = optionalStringArray(handler.pass_env, [], manifestPath, "handler.pass_env");
  const invalidEnv = passEnv.find((name) => !ENV_PATTERN.test(name));
  if (invalidEnv) {
    throw manifestError(manifestPath, `handler.pass_env contains invalid name ${invalidEnv}`);
  }

  return {
    version: 1,
    id,
    event,
    order,
    timeoutMs,
    onFailure,
    enabled,
    manifestPath,
    handler: {
      kind: "local",
      entry,
      entrypoint,
      run: run.map((argument) => argument.replaceAll("{entry}", entry)),
      passEnv,
    },
  };
}

async function requireRegularFile(
  path: string,
  manifestPath: string,
  field: string,
): Promise<void> {
  try {
    const stat = await lstat(path);
    if (stat.isSymbolicLink() || !stat.isFile()) {
      throw manifestError(manifestPath, `${field} must resolve to a regular file`);
    }
  } catch (error) {
    if (error instanceof HookManifestError) throw error;
    throw manifestError(manifestPath, `${field} must resolve to a regular file`);
  }
}

function requireRecord(value: unknown, path: string, field: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw manifestError(path, `${field} must be a table`);
  }
  return value as Record<string, unknown>;
}

function requireString(value: unknown, path: string, field: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw manifestError(path, `${field} must be a non-empty string`);
  }
  return value;
}

function optionalString(value: unknown, fallback: string, path: string, field: string): string {
  return value === undefined ? fallback : requireString(value, path, field);
}

function requireInteger(value: unknown, path: string, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw manifestError(path, `${field} must be an integer`);
  }
  return value;
}

function optionalInteger(
  value: unknown,
  fallback: number,
  path: string,
  field: string,
): number {
  return value === undefined ? fallback : requireInteger(value, path, field);
}

function optionalBoolean(
  value: unknown,
  fallback: boolean,
  path: string,
  field: string,
): boolean {
  if (value === undefined) return fallback;
  if (typeof value !== "boolean") throw manifestError(path, `${field} must be a boolean`);
  return value;
}

function requireStringArray(value: unknown, path: string, field: string): string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw manifestError(path, `${field} must be an array of strings`);
  }
  return [...value] as string[];
}

function optionalStringArray(
  value: unknown,
  fallback: string[],
  path: string,
  field: string,
): string[] {
  return value === undefined ? fallback : requireStringArray(value, path, field);
}

function optionalEnum<T extends string>(
  value: unknown,
  fallback: T,
  values: readonly T[],
  path: string,
  field: string,
): T {
  if (value === undefined) return fallback;
  if (typeof value !== "string" || !values.includes(value as T)) {
    throw manifestError(path, `${field} must be one of ${values.join(", ")}`);
  }
  return value as T;
}

function manifestError(path: string, message: string): HookManifestError {
  return new HookManifestError(path, message);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
