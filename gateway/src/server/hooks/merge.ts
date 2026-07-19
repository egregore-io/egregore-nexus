export function mergeHookMetadata(
  base: Record<string, unknown>,
  patch: Record<string, unknown>,
): Record<string, unknown> {
  if (Object.hasOwn(patch, "_nexus")) {
    throw new HookMetadataError("metadata._nexus is reserved for Gateway provenance");
  }
  return mergeObjects(base, patch);
}

export class HookMetadataError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "HookMetadataError";
  }
}

function mergeObjects(
  base: Record<string, unknown>,
  patch: Record<string, unknown>,
): Record<string, unknown> {
  const result = cloneObject(base);
  for (const [key, value] of Object.entries(patch)) {
    const current = result[key];
    result[key] = isPlainObject(current) && isPlainObject(value)
      ? mergeObjects(current, value)
      : cloneJson(value);
  }
  return result;
}

function cloneObject(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, cloneJson(entry)]));
}

function cloneJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(cloneJson);
  if (isPlainObject(value)) return cloneObject(value);
  return value;
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
