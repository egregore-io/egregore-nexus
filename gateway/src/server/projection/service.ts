import type { Client } from "@libsql/client";

import {
  sharedDaemonPushConnector,
  type DaemonPushConnection,
} from "../agui/daemonPushRelay.mjs";
import { getGatewayStore } from "../store/client";
import { gatewayChangeBus, type GatewayChangeBus } from "../store/changeBus";
import { GatewayProjectionConsumer, type ProjectionFrame } from "./consumer";
import type { CanonicalProjectionEvent } from "./apply";

export interface GatewayProjectionConnection {
  ready: Promise<void>;
  subscribeProjections(handlers: {
    onFrame: (frame: ProjectionFrame) => void;
    onError: (error: unknown) => void;
  }): () => void;
  ackProjection(ack: { daemonEpoch: string; throughSeq: number }): Promise<unknown> | unknown;
  close(): void;
}

export interface GatewayProjectionServiceOptions {
  afterReceipt?: (event: CanonicalProjectionEvent) => Promise<unknown>;
}

/** One projection subscription for the Gateway process, independent of browsers and requests. */
export class GatewayProjectionService {
  private started?: Promise<void>;
  private unsubscribe?: () => void;
  private chain: Promise<void> = Promise.resolve();

  constructor(
    db: Client,
    private readonly connection: GatewayProjectionConnection,
    changeBus: GatewayChangeBus = gatewayChangeBus,
    options: GatewayProjectionServiceOptions = {},
  ) {
    this.consumer = new GatewayProjectionConsumer(db, {
      ack: (ack) => Promise.resolve(connection.ackProjection(ack)),
    }, {
      afterCommit: async (event, result) => {
        if (result !== "applied") return;
        if (event.kind === "message.accepted") await options.afterReceipt?.(event);
        for (const key of projectionChangeKeys(event)) changeBus.publish(key);
      },
    });
  }

  private readonly consumer: GatewayProjectionConsumer;

  start(): Promise<void> {
    this.started ??= this.startOnce();
    return this.started;
  }

  private async startOnce(): Promise<void> {
    this.unsubscribe = this.connection.subscribeProjections({
      onFrame: (frame) => {
        this.chain = this.chain
          .then(() => this.consumer.handleFrame(frame))
          .catch((error) => {
            // Keep later frames serviceable. Malformed domain events are quarantined by the
            // consumer; this catch is for transport/store failures that should reconnect.
            process.stderr.write(`Gateway projection consumer error: ${String(error)}\n`);
          });
      },
      onError: (error) => {
        process.stderr.write(`Gateway projection transport error: ${String(error)}\n`);
      },
    });
    await this.connection.ready;
  }

  async close(): Promise<void> {
    this.unsubscribe?.();
    this.unsubscribe = undefined;
    await this.chain;
  }
}

function projectionChangeKeys(event: CanonicalProjectionEvent): string[] {
  if (event.kind !== "message.accepted" && event.kind !== "notification.emitted") return [];
  if (!event.payload || typeof event.payload !== "object" || Array.isArray(event.payload)) return [];
  const payload = event.payload as Record<string, unknown>;
  const keys: string[] = [];
  addKey(keys, "thread", payload.threadId);
  addKey(keys, "thread-name", payload.threadName ?? payload.thread ?? payload.toName);
  addKey(keys, "topic", payload.topic);
  if (event.kind === "message.accepted") {
    addKey(keys, "dm", payload.fromAgentId);
    addKey(keys, "dm", payload.toAgentId);
    addKey(keys, "dm-name", payload.fromName);
    addKey(keys, "dm-name", payload.toName);
  }
  if (event.kind === "notification.emitted") keys.push("notifications");
  return [...new Set(keys)];
}

function addKey(keys: string[], prefix: string, value: unknown): void {
  if (typeof value === "string" && value.length > 0) keys.push(`${prefix}:${value}`);
}

let processService: Promise<GatewayProjectionService | undefined> | undefined;

/** Start the process singleton when the local daemon stream endpoint is installed. */
export function startGatewayProjectionService(
  options: GatewayProjectionServiceOptions = {},
): Promise<GatewayProjectionService | undefined> {
  processService ??= startProcessService(options);
  return processService;
}

export async function stopGatewayProjectionService(): Promise<void> {
  const service = await processService;
  processService = undefined;
  await service?.close();
}

async function startProcessService(
  options: GatewayProjectionServiceOptions,
): Promise<GatewayProjectionService | undefined> {
  const connection = sharedDaemonPushConnector() as DaemonPushConnection | undefined;
  if (!connection?.subscribeProjections || !connection.ackProjection) return undefined;
  const projectionConnection: GatewayProjectionConnection = {
    ready: connection.ready,
    subscribeProjections: (handlers) => connection.subscribeProjections!(handlers),
    ackProjection: (ack) => connection.ackProjection!(ack),
    close: () => undefined,
  };
  const service = new GatewayProjectionService(
    await getGatewayStore(),
    projectionConnection,
    gatewayChangeBus,
    options,
  );
  await service.start();
  return service;
}
