import { createClient, type Client } from "@libsql/client";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { DeveloperEventEnvelope } from "@shared/types";
import { removeTempPath } from "../../test/removeTempPath";
import { applyCanonicalProjection } from "../projection/apply";
import type { ProjectionFrame } from "../projection/consumer";
import { GatewayProjectionService } from "../projection/service";
import { GatewayChangeBus } from "../store/changeBus";
import { migrateGatewayStore } from "../store/migrations";
import { createGatewayDeveloperEventSource } from "./gatewayDeveloperEvents";

function acceptedMessage(
  seq: number,
  payload: Record<string, unknown>,
) {
  return {
    eventId: `message:${String(payload.messageId)}`,
    daemonEpoch: "boot-events",
    seq,
    occurredAt: 1_780_000_000_000 + seq,
    kind: "message.accepted" as const,
    version: 1,
    payload,
  };
}

describe("Gateway-owned developer message events", () => {
  let db: Client;
  let dbPath: string;

  beforeEach(async () => {
    dbPath = join(tmpdir(), `nexus-gateway-developer-events-${randomUUID()}.db`);
    db = createClient({ url: `file:${dbPath}` });
    await migrateGatewayStore(db);
  });

  afterEach(async () => {
    db.close();
    await removeTempPath(dbPath);
  });

  it("replays and pushes thread events from committed bus_messages without a timer", async () => {
    const changeBus = new GatewayChangeBus();
    const onRead = vi.fn();
    const source = createGatewayDeveloperEventSource({ db: async () => db, changeBus, onRead });
    await applyCanonicalProjection(db, acceptedMessage(1, {
      messageId: "m_before_subscribe",
      scope: "thread",
      threadId: "design",
      toName: "design",
      fromName: "Ada",
      body: "metadata envelopes must not expose this body",
      createdAt: 1_780_000_000_001,
    }));

    const events: DeveloperEventEnvelope[] = [];
    const subscription = source.subscribe("sys.message.thread.design", 0, {
      onEvent: (event) => {
        events.push(event);
        return true;
      },
      onError: (error) => {
        throw error;
      },
    });
    await subscription.ready;
    expect(events).toEqual([{
      kind: "message",
      topic: "sys.message.thread.design",
      seq: 1,
      ts: 1_780_000_000_001,
      thread: "design",
      from: "Ada",
      messageId: "m_before_subscribe",
    }]);
    expect(JSON.stringify(events)).not.toContain("metadata envelopes must not expose this body");

    await applyCanonicalProjection(db, acceptedMessage(2, {
      messageId: "m_after_subscribe",
      scope: "thread",
      threadId: "design",
      toName: "design",
      fromName: "Bob",
      body: "new message",
      createdAt: 1_780_000_000_002,
    }));
    changeBus.publish("thread-name:design");
    await vi.waitFor(() => expect(events).toHaveLength(2));
    expect(events[1]).toMatchObject({
      topic: "sys.message.thread.design",
      seq: 2,
      from: "Bob",
      messageId: "m_after_subscribe",
    });

    const callsAfterDelivery = onRead.mock.calls.length;
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(onRead).toHaveBeenCalledTimes(callsAfterDelivery);

    subscription.close();
    source.close();
  });

  it("wakes from the projection service only after message acceptance commits", async () => {
    const changeBus = new GatewayChangeBus();
    let onFrame: ((frame: ProjectionFrame) => void) | undefined;
    const ackProjection = vi.fn(async () => undefined);
    const projection = new GatewayProjectionService(db, {
      ready: Promise.resolve(),
      subscribeProjections: (handlers) => {
        onFrame = handlers.onFrame;
        return () => undefined;
      },
      ackProjection,
      close: vi.fn(),
    }, changeBus);
    await projection.start();
    const source = createGatewayDeveloperEventSource({ db: async () => db, changeBus });
    const events: string[] = [];
    const subscription = source.subscribe("sys.message.thread.design", 0, {
      onEvent: (event) => {
        events.push(event.messageId ?? "");
        return true;
      },
    });
    await subscription.ready;

    onFrame?.({ t: "projection", event: acceptedMessage(1, {
      messageId: "m_projection_wake",
      scope: "thread",
      threadId: "design",
      toName: "design",
      fromName: "Ada",
      body: "post-commit only",
    }) });
    await vi.waitFor(() => expect(events).toEqual(["m_projection_wake"]));
    expect(ackProjection).toHaveBeenCalledTimes(1);

    subscription.close();
    source.close();
    await projection.close();
  });

  it("shares one exact-topic reader, isolates topics, and suppresses duplicate wakes", async () => {
    const changeBus = new GatewayChangeBus();
    const source = createGatewayDeveloperEventSource({ db: async () => db, changeBus });
    const first: string[] = [];
    const second: string[] = [];
    const other: string[] = [];
    const subscriptions = [
      source.subscribe("sys.message.thread.design", 0, {
        onEvent: (event) => {
          first.push(event.messageId ?? "");
          return true;
        },
      }),
      source.subscribe("sys.message.thread.design", 0, {
        onEvent: (event) => {
          second.push(event.messageId ?? "");
          return true;
        },
      }),
      source.subscribe("sys.message.thread.ops", 0, {
        onEvent: (event) => {
          other.push(event.messageId ?? "");
          return true;
        },
      }),
    ];
    await Promise.all(subscriptions.map((subscription) => subscription.ready));
    expect(source.activeTopicCount()).toBe(2);

    await applyCanonicalProjection(db, acceptedMessage(1, {
      messageId: "m_design_once",
      scope: "thread",
      threadId: "design",
      toName: "design",
      fromName: "Ada",
      body: "once",
    }));
    changeBus.publish("thread-name:design");
    await vi.waitFor(() => expect(first).toEqual(["m_design_once"]));
    expect(second).toEqual(["m_design_once"]);
    expect(other).toEqual([]);

    changeBus.publish("thread-name:design");
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(first).toEqual(["m_design_once"]);
    expect(second).toEqual(["m_design_once"]);

    for (const subscription of subscriptions) subscription.close();
    expect(source.activeTopicCount()).toBe(0);
    source.close();
  });

  it("resumes from the last accepted global row cursor", async () => {
    await applyCanonicalProjection(db, acceptedMessage(1, {
      messageId: "m_seen",
      scope: "thread",
      threadId: "design",
      toName: "design",
      fromName: "Ada",
      body: "seen",
    }));
    await applyCanonicalProjection(db, acceptedMessage(2, {
      messageId: "m_unseen",
      scope: "thread",
      threadId: "design",
      toName: "design",
      fromName: "Bob",
      body: "unseen",
    }));
    const source = createGatewayDeveloperEventSource({
      db: async () => db,
      changeBus: new GatewayChangeBus(),
    });
    const events: string[] = [];
    const subscription = source.subscribe("sys.message.thread.design", 1, {
      onEvent: (event) => {
        events.push(event.messageId ?? "");
        return true;
      },
    });
    await subscription.ready;
    expect(events).toEqual(["m_unseen"]);

    subscription.close();
    source.close();
  });

  it("does not advance a subscriber cursor until its delivery callback accepts", async () => {
    await applyCanonicalProjection(db, acceptedMessage(1, {
      messageId: "m_backpressured",
      scope: "thread",
      threadId: "design",
      toName: "design",
      fromName: "Ada",
      body: "retry me",
    }));
    const changeBus = new GatewayChangeBus();
    const source = createGatewayDeveloperEventSource({ db: async () => db, changeBus });
    const attempts: string[] = [];
    let accept = false;
    const subscription = source.subscribe("sys.message.thread.design", 0, {
      onEvent: (event) => {
        attempts.push(event.messageId ?? "");
        return accept;
      },
    });
    await subscription.ready;
    expect(attempts).toEqual(["m_backpressured"]);

    await applyCanonicalProjection(db, acceptedMessage(2, {
      messageId: "m_after_backpressure",
      scope: "thread",
      threadId: "design",
      toName: "design",
      fromName: "Bob",
      body: "after",
    }));
    changeBus.publish("thread-name:design");
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(attempts).toEqual(["m_backpressured"]);

    accept = true;
    subscription.resume();
    await vi.waitFor(() => expect(attempts).toEqual([
      "m_backpressured",
      "m_backpressured",
      "m_after_backpressure",
    ]));

    subscription.close();
    source.close();
  });

  it("maps DM topics to either side of the canonical conversation", async () => {
    await applyCanonicalProjection(db, acceptedMessage(1, {
      messageId: "m_dm",
      scope: "dm",
      fromName: "Ada",
      toName: "earl",
      body: "private body",
      createdAt: 1_780_000_000_001,
    }));
    const source = createGatewayDeveloperEventSource({
      db: async () => db,
      changeBus: new GatewayChangeBus(),
    });
    const earl: DeveloperEventEnvelope[] = [];
    const ada: DeveloperEventEnvelope[] = [];
    const unknown: DeveloperEventEnvelope[] = [];
    const subscriptions = [
      source.subscribe("sys.dm.earl", 0, { onEvent: (event) => (earl.push(event), true) }),
      source.subscribe("sys.dm.Ada", 0, { onEvent: (event) => (ada.push(event), true) }),
      source.subscribe("sys.dm.unknown", 0, { onEvent: (event) => (unknown.push(event), true) }),
    ];
    await Promise.all(subscriptions.map((subscription) => subscription.ready));

    expect(earl).toEqual([expect.objectContaining({
      topic: "sys.dm.earl",
      dm: "earl",
      from: "Ada",
      messageId: "m_dm",
    })]);
    expect(ada).toEqual([expect.objectContaining({
      topic: "sys.dm.Ada",
      dm: "Ada",
      from: "Ada",
      messageId: "m_dm",
    })]);
    expect(unknown).toEqual([]);

    for (const subscription of subscriptions) subscription.close();
    source.close();
  });

  it("rejects non-message developer topics instead of falling back to daemon store reads", () => {
    const source = createGatewayDeveloperEventSource({
      db: async () => db,
      changeBus: new GatewayChangeBus(),
    });
    expect(() => source.subscribe("sys.agent.lifecycle", 0, { onEvent: () => true }))
      .toThrow(/Gateway message topic/);
    source.close();
  });
});
