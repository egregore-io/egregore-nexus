// Lane A renderer — maps ONE committed bus message (`Message`) to a complete
// AG-UI text message (START/CONTENT/END). This is the Message Post lane: a
// thread view shows its committed messages, not any agent's raw activity.
//
// Role is `user` for the watcher's own posts, `assistant` for everyone else.
// An empty body renders nothing. The committed sender (`msg.from`) rides along
// on the START event as `name` so the client can attribute each row to its real
// author (provenance) instead of a single generic "agent".
import { EventType } from "@ag-ui/client";
import type { BaseEvent } from "@ag-ui/client";
import type { Message } from "@shared/types";

// --- event constructors (typed; widened to BaseEvent) -----------------------

interface MessageCursorCarrier {
  cursor?: {
    createdAt?: number;
    rowid?: number;
  };
}

function messageCursor(msg: Message): { createdAt: number; rowid: number } | undefined {
  const cursor = (msg as Message & MessageCursorCarrier).cursor;
  if (!cursor) return undefined;
  const createdAt = cursor.createdAt;
  const rowid = cursor.rowid;
  if (typeof createdAt !== "number" || typeof rowid !== "number") return undefined;
  if (!Number.isFinite(createdAt) || !Number.isFinite(rowid)) return undefined;
  if (createdAt <= 0 || rowid <= 0) return undefined;
  return { createdAt, rowid };
}

const textStart = (
  messageId: string,
  name: string,
  createdAt: number,
  cursor?: { createdAt: number; rowid: number },
): BaseEvent =>
  ({
    type: EventType.TEXT_MESSAGE_START,
    messageId,
    role: "assistant",
    name,
    createdAt,
    ...(cursor ? { cursor } : {}),
  }) as BaseEvent;
const userStart = (
  messageId: string,
  createdAt: number,
  cursor?: { createdAt: number; rowid: number },
): BaseEvent =>
  ({
    type: EventType.TEXT_MESSAGE_START,
    messageId,
    role: "user",
    createdAt,
    ...(cursor ? { cursor } : {}),
  }) as BaseEvent;
const textContent = (messageId: string, delta: string): BaseEvent =>
  ({ type: EventType.TEXT_MESSAGE_CONTENT, messageId, delta }) as BaseEvent;
const textEnd = (messageId: string): BaseEvent =>
  ({ type: EventType.TEXT_MESSAGE_END, messageId }) as BaseEvent;

/**
 * Map ONE committed bus message (`read({ id })` result) to a complete AG-UI text
 * message — START/CONTENT/END keyed by the message id. This is the Lane A render:
 * a thread shows its committed messages, not any agent's raw activity. Role is
 * `user` for the watcher's own posts, `assistant` for everyone else. An empty body
 * renders nothing.
 */
export function messageToAguiEvents(msg: Message, selfName?: string): BaseEvent[] {
  if (!msg.body.trim()) return [];
  const isSelf = selfName != null && msg.from === selfName;
  const cursor = messageCursor(msg);
  const start = isSelf
    ? userStart(msg.id, msg.createdAt, cursor)
    : textStart(msg.id, msg.from, msg.createdAt, cursor);
  return [start, textContent(msg.id, msg.body), textEnd(msg.id)];
}
