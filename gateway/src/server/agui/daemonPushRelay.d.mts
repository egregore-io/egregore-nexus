// Type declarations for daemonPushRelay.mjs (plain-JS so scripts/gateway-serve.mjs can
// import the WS upgrade stack with plain Node — same convention as ws.mjs/ws.d.mts).

import type { RelayFactory } from "@server/agui/relayTypes";
import type {
  DeveloperEventEnvelope,
  GatewayHookCapabilities,
  GatewayProjectionAck,
  GatewayProjectionEvent,
  HookEvaluationRequest,
  HookEvaluationResponse,
} from "@shared/types";

export type DaemonPushLane = "agent" | "raw" | "developer_event";

export interface DaemonPushSubscription {
  lane: DaemonPushLane;
  sessionId: string;
  afterId: number;
}

export interface DaemonPushProjectionFrame {
  t: "projection";
  event: GatewayProjectionEvent;
}

export interface DaemonPushProjectionAckFrame {
  t: "projection.ack";
  ack: GatewayProjectionAck;
}

export interface DaemonPushProjectionGapFrame {
  t: "projection.gap";
  gap: {
    daemonEpoch: string;
    afterSeq?: number;
    throughSeq?: number;
    reason: string;
    recordedAt?: number;
  };
}

export interface DaemonPushConnection {
  ready: Promise<void>;
  /** Authenticated daemon boot currently backing this connection. */
  readonly daemonBootId?: string;
  subscribe(
    subscription: DaemonPushSubscription,
    handlers: {
      onFrame: (frame: unknown) => void;
      onError: (error: unknown) => void;
    },
  ): () => void;
  subscribeProjections?(handlers: {
    onFrame: (frame: DaemonPushProjectionFrame | DaemonPushProjectionGapFrame) => void;
    onError: (error: unknown) => void;
  }): () => void;
  ackProjection?(ack: GatewayProjectionAck): Promise<void>;
  registerHooks?(
    capabilities: GatewayHookCapabilities,
    handler: (request: HookEvaluationRequest) => Promise<HookEvaluationResponse> | HookEvaluationResponse,
  ): () => void;
  close(): void;
}

export type DaemonPushConnector = () => DaemonPushConnection | undefined;

export interface DaemonPushDeveloperEventSource {
  subscribe(
    topic: string,
    afterSeq: number,
    handlers: {
      onEvent: (event: DeveloperEventEnvelope) => void;
      onGap?: (frame: unknown) => void;
      onError?: (error: unknown) => void;
    },
  ): () => void;
}

export interface DaemonPushRelayDeps {
  connector?: DaemonPushConnector;
  afterId?: number;
}

export declare function createDaemonPushAgentSessionRelay(
  sessionId: string,
  deps?: DaemonPushRelayDeps,
): RelayFactory;

export declare function createDaemonPushDeveloperEventSource(
  sessionId: string,
  deps?: DaemonPushRelayDeps,
): DaemonPushDeveloperEventSource | undefined;

export declare function sharedDaemonPushConnector(): DaemonPushConnection | undefined;

export declare function closeSharedDaemonPushConnector(): void;

export declare function resetSharedDaemonPushConnectorForTests(): void;

export declare function gatewayStreamEndpointManifestPath(env?: NodeJS.ProcessEnv): string;
