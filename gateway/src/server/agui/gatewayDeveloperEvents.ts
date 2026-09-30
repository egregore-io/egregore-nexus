import type { Client, Row } from "@libsql/client";

import { DeveloperEventKind, type DeveloperEventEnvelope } from "@shared/types";
import { getGatewayStore } from "@server/store/client";
import { gatewayChangeBus, type GatewayChangeBus } from "@server/store/changeBus";

const THREAD_TOPIC_PREFIX = "sys.message.thread.";
const DM_TOPIC_PREFIX = "sys.dm.";
const READ_LIMIT = 200;

export interface GatewayDeveloperEventHandlers {
  onEvent(event: DeveloperEventEnvelope): boolean | void;
  onError?(error: unknown): void;
}

export interface GatewayDeveloperEventSubscription {
  ready: Promise<void>;
  pause(): void;
  resume(): void;
  close(): void;
}

export interface GatewayDeveloperEventSource {
  subscribe(
    topic: string,
    afterSeq: number,
    handlers: GatewayDeveloperEventHandlers,
  ): GatewayDeveloperEventSubscription;
  activeTopicCount(): number;
  close(): void;
}

export interface GatewayDeveloperEventSourceOptions {
  db?: () => Promise<Client>;
  changeBus?: GatewayChangeBus;
  onRead?: (topic: string, afterSeq: number) => void;
}

interface TopicDescriptor {
  topic: string;
  kind: "thread" | "dm";
  name: string;
  changeKey: string;
}

interface Subscriber {
  id: number;
  cursor: number;
  paused: boolean;
  closed: boolean;
  readyResolved: boolean;
  resolveReady(): void;
  handlers: GatewayDeveloperEventHandlers;
}

/**
 * Gateway-owned, post-commit developer events for durable thread and DM messages.
 *
 * SQLite remains the replay authority. The process-local change bus is only a wake-up signal,
 * so subscribing before the first read closes the read/wait race without introducing a timer.
 */
export function createGatewayDeveloperEventSource(
  options: GatewayDeveloperEventSourceOptions = {},
): GatewayDeveloperEventSource {
  const db = options.db ?? getGatewayStore;
  const changeBus = options.changeBus ?? gatewayChangeBus;
  const readers = new Map<string, ReturnType<typeof createReader>>();
  let subscriberSeq = 0;
  let closed = false;

  function createReader(descriptor: TopicDescriptor) {
    const subscribers = new Map<number, Subscriber>();
    let reading = false;
    let scheduled = false;
    let rerun = false;
    let stopped = false;

    const requestRead = () => {
      if (stopped || subscribers.size === 0) return;
      if (reading) {
        rerun = true;
        return;
      }
      if (scheduled) return;
      scheduled = true;
      queueMicrotask(() => {
        scheduled = false;
        void read();
      });
    };

    const read = async () => {
      if (stopped || reading || subscribers.size === 0) return;
      reading = true;
      const included = [...subscribers.values()].filter((subscriber) => !subscriber.closed);
      const readable = included.filter((subscriber) => !subscriber.paused);
      try {
        if (readable.length > 0) {
          const cursor = Math.min(...readable.map((subscriber) => subscriber.cursor));
          options.onRead?.(descriptor.topic, cursor);
          const events = await readEvents(await db(), descriptor, cursor);
          for (const event of events) {
            for (const subscriber of included) {
              if (subscriber.closed || subscriber.paused || event.seq <= subscriber.cursor) continue;
              try {
                const accepted = subscriber.handlers.onEvent(event);
                if (accepted === false) {
                  subscriber.paused = true;
                  continue;
                }
                subscriber.cursor = event.seq;
              } catch (error) {
                subscriber.paused = true;
                subscriber.handlers.onError?.(error);
              }
            }
          }
          if (events.length === READ_LIMIT) rerun = true;
        }
      } catch (error) {
        for (const subscriber of readable) {
          subscriber.paused = true;
          subscriber.handlers.onError?.(error);
        }
      } finally {
        for (const subscriber of included) {
          if (subscriber.readyResolved) continue;
          subscriber.readyResolved = true;
          subscriber.resolveReady();
        }
        reading = false;
        if (rerun && !stopped && subscribers.size > 0) {
          rerun = false;
          requestRead();
        }
      }
    };

    const unsubscribeChangeBus = changeBus.subscribe(descriptor.changeKey, requestRead);

    return {
      add(afterSeq: number, handlers: GatewayDeveloperEventHandlers) {
        const id = ++subscriberSeq;
        let resolveReady: () => void = () => undefined;
        const ready = new Promise<void>((resolve) => {
          resolveReady = resolve;
        });
        const subscriber: Subscriber = {
          id,
          cursor: afterSeq,
          paused: false,
          closed: false,
          readyResolved: false,
          resolveReady,
          handlers,
        };
        subscribers.set(id, subscriber);
        requestRead();
        return {
          ready,
          pause() {
            subscriber.paused = true;
          },
          resume() {
            if (subscriber.closed || !subscriber.paused) return;
            subscriber.paused = false;
            requestRead();
          },
          close() {
            if (subscriber.closed) return;
            subscriber.closed = true;
            subscribers.delete(id);
            if (!subscriber.readyResolved) subscriber.resolveReady();
            if (subscribers.size === 0) {
              stopped = true;
              unsubscribeChangeBus();
              readers.delete(descriptor.topic);
            }
          },
        } satisfies GatewayDeveloperEventSubscription;
      },
      stop() {
        if (stopped) return;
        stopped = true;
        unsubscribeChangeBus();
        for (const subscriber of subscribers.values()) {
          subscriber.closed = true;
          if (!subscriber.readyResolved) subscriber.resolveReady();
        }
        subscribers.clear();
      },
    };
  }

  return {
    subscribe(topic, afterSeq, handlers) {
      if (closed) throw new Error("Gateway developer event source is closed");
      if (!Number.isInteger(afterSeq) || afterSeq < 0) {
        throw new Error("Gateway developer event cursor must be a non-negative integer");
      }
      const descriptor = describeTopic(topic);
      let reader = readers.get(topic);
      if (!reader) {
        reader = createReader(descriptor);
        readers.set(topic, reader);
      }
      return reader.add(afterSeq, handlers);
    },
    activeTopicCount() {
      return readers.size;
    },
    close() {
      if (closed) return;
      closed = true;
      for (const reader of readers.values()) reader.stop();
      readers.clear();
    },
  };
}

function describeTopic(topic: string): TopicDescriptor {
  if (topic.startsWith(THREAD_TOPIC_PREFIX) && topic.length > THREAD_TOPIC_PREFIX.length) {
    const name = topic.slice(THREAD_TOPIC_PREFIX.length);
    return { topic, kind: "thread", name, changeKey: `thread-name:${name}` };
  }
  if (topic.startsWith(DM_TOPIC_PREFIX) && topic.length > DM_TOPIC_PREFIX.length) {
    const name = topic.slice(DM_TOPIC_PREFIX.length);
    return { topic, kind: "dm", name, changeKey: `dm-name:${name}` };
  }
  throw new Error(`${topic} is not a Gateway message topic`);
}

async function readEvents(
  db: Client,
  descriptor: TopicDescriptor,
  afterSeq: number,
): Promise<DeveloperEventEnvelope[]> {
  const targetClause = descriptor.kind === "thread"
    ? "kind = 'thread' AND (thread_id = ? OR to_name = ?)"
    : "kind = 'dm' AND (from_name = ? OR to_name = ?)";
  const result = await db.execute({
    sql: `SELECT rowid, message_id, from_name, created_at
          FROM bus_messages
          WHERE rowid > ? AND ${targetClause}
          ORDER BY rowid ASC LIMIT ?`,
    args: [afterSeq, descriptor.name, descriptor.name, READ_LIMIT],
  });
  return result.rows.map((row) => eventFromRow(row, descriptor));
}

function eventFromRow(row: Row, descriptor: TopicDescriptor): DeveloperEventEnvelope {
  const seq = Number(row.rowid);
  const ts = Number(row.created_at);
  const from = row.from_name === null || row.from_name === undefined
    ? undefined
    : String(row.from_name);
  return {
    kind: DeveloperEventKind.Message,
    topic: descriptor.topic,
    seq,
    ts,
    ...(descriptor.kind === "thread" ? { thread: descriptor.name } : { dm: descriptor.name }),
    ...(from ? { from } : {}),
    messageId: String(row.message_id),
  };
}
