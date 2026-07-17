import type { Client, Row } from "@libsql/client";

export interface GatewayNotification {
  notificationId: string;
  messageId: string;
  source?: string;
  target: Record<string, unknown>;
  summary?: string;
  body: string;
  createdAt: number;
}

export async function insertNotification(
  db: Client,
  notification: GatewayNotification,
): Promise<"inserted" | "duplicate"> {
  const result = await db.execute({
    sql: `INSERT OR IGNORE INTO notifications
          (notification_id, message_id, source, target_json, summary, body, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?)`,
    args: [
      notification.notificationId,
      notification.messageId,
      notification.source ?? null,
      JSON.stringify(notification.target),
      notification.summary ?? null,
      notification.body,
      notification.createdAt,
    ],
  });
  return result.rowsAffected === 0 ? "duplicate" : "inserted";
}

export async function getNotification(
  db: Client,
  notificationId: string,
): Promise<GatewayNotification | null> {
  const result = await db.execute({
    sql: "SELECT * FROM notifications WHERE notification_id = ? LIMIT 1",
    args: [notificationId],
  });
  return result.rows[0] ? mapNotification(result.rows[0]) : null;
}

function mapNotification(row: Row): GatewayNotification {
  return {
    notificationId: String(row.notification_id),
    messageId: String(row.message_id),
    source: optionalString(row.source),
    target: JSON.parse(String(row.target_json)) as Record<string, unknown>,
    summary: optionalString(row.summary),
    body: String(row.body),
    createdAt: Number(row.created_at),
  };
}

function optionalString(value: unknown): string | undefined {
  return value === null || value === undefined ? undefined : String(value);
}
