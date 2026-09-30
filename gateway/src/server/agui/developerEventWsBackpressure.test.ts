import { createClient, type Client } from "@libsql/client";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { removeTempPath } from "../../test/removeTempPath";
import { applyCanonicalProjection } from "../projection/apply";
import { GatewayChangeBus } from "../store/changeBus";
import { migrateGatewayStore } from "../store/migrations";
import { createGatewayDeveloperEventSource } from "./gatewayDeveloperEvents";
import { handleWs } from "./ws.mjs";

const MAX_BUFFERED_AMOUNT = 1024 * 1024;

class FakeSocket extends EventEmitter {
  sent: string[] = [];
  closed: { code?: number; reason?: string } | null = null;
  bufferedAmount = 0;
  afterSend?: (payload: string) => void;

  send(data: string | Buffer) {
    const payload = String(data);
    this.sent.push(payload);
    this.afterSend?.(payload);
  }

  close(code?: number, reason?: string) {
    if (this.closed) return;
    this.closed = { code, reason };
    this.emit("close");
  }
}

function acceptedThreadMessage(seq: number, thread: string, messageId: string) {
  return {
    eventId: `message:${messageId}`,
    daemonEpoch: "boot-developer-backpressure",
    seq,
    occurredAt: 1_780_000_000_000 + seq,
    kind: "message.accepted" as const,
    version: 1,
    payload: {
      messageId,
      scope: "thread",
      threadId: thread,
      toName: thread,
      fromName: "Ada",
      body: `body:${messageId}`,
      createdAt: 1_780_000_000_000 + seq,
    },
  };
}

function developerEvents(socket: FakeSocket) {
  return socket.sent
    .map((payload) => JSON.parse(payload))
    .filter((frame) => frame.type === "developer.event")
    .map((frame) => frame.event as { topic: string; seq: number; messageId: string });
}

function subscribe(socket: FakeSocket, topic: string, afterSeq: number) {
  socket.emit("message", JSON.stringify({ t: "subscribe", topic, afterSeq }));
}

describe("developer-event WebSocket backpressure", () => {
  let db: Client;
  let dbPath: string;

  beforeEach(async () => {
    dbPath = join(tmpdir(), `nexus-developer-ws-backpressure-${randomUUID()}.db`);
    db = createClient({ url: `file:${dbPath}` });
    await migrateGatewayStore(db);
  });

  afterEach(async () => {
    db.close();
    await removeTempPath(dbPath);
  });

  it("closes only the slow subscriber with its last accepted cursor and replays exactly once", async () => {
    const changeBus = new GatewayChangeBus();
    const source = createGatewayDeveloperEventSource({ db: async () => db, changeBus });

    await applyCanonicalProjection(db, acceptedThreadMessage(1, "design", "m_design_1"));
    await applyCanonicalProjection(db, acceptedThreadMessage(2, "design", "m_design_2"));
    await applyCanonicalProjection(db, acceptedThreadMessage(3, "design", "m_design_3"));
    await applyCanonicalProjection(db, acceptedThreadMessage(4, "ops", "m_ops_1"));

    const slow = new FakeSocket();
    slow.afterSend = (payload) => {
      if (JSON.parse(payload).type === "developer.event") {
        // The next frame itself crosses the limit. A preflight that checks only the
        // already-buffered bytes would incorrectly accept it.
        slow.bufferedAmount = MAX_BUFFERED_AMOUNT - 1;
      }
    };
    const fast = new FakeSocket();
    const isolated = new FakeSocket();
    const slowControl = handleWs(slow, new Request("http://localhost/api/agui/ws"), {
      developerEvents: source,
    });
    const fastControl = handleWs(fast, new Request("http://localhost/api/agui/ws"), {
      developerEvents: source,
    });
    const isolatedControl = handleWs(isolated, new Request("http://localhost/api/agui/ws"), {
      developerEvents: source,
    });

    subscribe(slow, "sys.message.thread.design", 0);
    subscribe(fast, "sys.message.thread.design", 0);
    subscribe(isolated, "sys.message.thread.ops", 0);

    await vi.waitFor(() => {
      expect(slow.closed).toEqual({ code: 1013, reason: "developer.backpressure:1" });
    });
    await slowControl.closed;
    await vi.waitFor(() => {
      expect(developerEvents(fast).map((event) => event.seq)).toEqual([1, 2, 3]);
      expect(developerEvents(isolated).map((event) => event.seq)).toEqual([4]);
    });
    expect(developerEvents(slow).map((event) => event.seq)).toEqual([1]);

    const cursor = Number(slow.closed?.reason?.split(":").at(-1));
    const resumed = new FakeSocket();
    const resumedControl = handleWs(resumed, new Request("http://localhost/api/agui/ws"), {
      developerEvents: source,
    });
    subscribe(resumed, "sys.message.thread.design", cursor);
    await vi.waitFor(() => {
      expect(developerEvents(resumed).map((event) => event.seq)).toEqual([2, 3]);
    });

    // Duplicate wakes do not duplicate accepted rows, and the isolated topic remains isolated.
    changeBus.publish("thread-name:design");
    changeBus.publish("thread-name:design");
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(developerEvents(resumed).map((event) => event.seq)).toEqual([2, 3]);
    expect(developerEvents(fast).map((event) => event.seq)).toEqual([1, 2, 3]);
    expect(developerEvents(isolated).map((event) => event.seq)).toEqual([4]);

    await applyCanonicalProjection(db, acceptedThreadMessage(5, "design", "m_design_4"));
    changeBus.publish("thread-name:design");
    await vi.waitFor(() => {
      expect(developerEvents(fast).map((event) => event.seq)).toEqual([1, 2, 3, 5]);
      expect(developerEvents(resumed).map((event) => event.seq)).toEqual([2, 3, 5]);
    });
    expect(developerEvents(isolated).map((event) => event.seq)).toEqual([4]);

    fastControl.close();
    isolatedControl.close();
    resumedControl.close();
    await Promise.all([fastControl.closed, isolatedControl.closed, resumedControl.closed]);
    source.close();
  });
});
