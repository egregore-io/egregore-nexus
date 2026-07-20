import type { Client } from "@libsql/client";

export interface TransportSecretMutation {
  key: string;
  updatedAt: number;
}

export async function setTransportSecret(
  db: Client,
  key: string,
  value: string,
  now = Date.now(),
): Promise<TransportSecretMutation> {
  const cleanKey = validSecretKey(key);
  if (!value) throw new Error("transport secret value is required");
  await db.execute({
    sql: `INSERT INTO transport_secrets (key, value, updated_at) VALUES (?, ?, ?)
          ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
    args: [cleanKey, value, now],
  });
  return { key: cleanKey, updatedAt: now };
}

export async function removeTransportSecret(
  db: Client,
  key: string,
): Promise<{ key: string; removed: boolean }> {
  const cleanKey = validSecretKey(key);
  const result = await db.execute({
    sql: "DELETE FROM transport_secrets WHERE key = ?",
    args: [cleanKey],
  });
  return { key: cleanKey, removed: result.rowsAffected > 0 };
}

export async function resolveTransportSecret(
  db: Client,
  key: string,
): Promise<string | undefined> {
  const result = await db.execute({
    sql: "SELECT value FROM transport_secrets WHERE key = ? LIMIT 1",
    args: [validSecretKey(key)],
  });
  const value = result.rows[0]?.value;
  return value === undefined || value === null ? undefined : String(value);
}

export async function resolveTransportSecretRefs(
  db: Client,
  refs: Readonly<Record<string, string>>,
): Promise<Record<string, string>> {
  const resolved: Record<string, string> = {};
  for (const [environmentName, secretKey] of Object.entries(refs)) {
    if (!/^[A-Z_][A-Z0-9_]*$/.test(environmentName)) {
      throw new Error(`invalid transport secret environment name ${environmentName}`);
    }
    const value = await resolveTransportSecret(db, secretKey);
    if (value === undefined) throw new Error(`missing transport secret ${secretKey}`);
    resolved[environmentName] = value;
  }
  return resolved;
}

function validSecretKey(value: string): string {
  const key = value.trim();
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(key)) {
    throw new Error("transport secret key is invalid");
  }
  return key;
}
