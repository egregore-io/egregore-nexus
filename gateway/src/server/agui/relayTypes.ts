import type { WsEvent } from "@shared/types";

/** Options for a session-scoped AG-UI event relay. */
export interface RealtimeRelayOptions {
  /** Called with each projected session event. */
  /** Return false to retain the relay cursor until the consumer resumes. */
  onEvent: (ev: WsEvent) => unknown;
  /** Called when a live source detects a cursor gap and needs store-backed repair. */
  onGap?: () => void;
  /** Optional error sink for transient projection/read errors. */
  onError?: (err: unknown) => void;
}

/** Minimal relay handle consumed by the AG-UI SSE core. */
export interface RealtimeRelay {
  /** Resolves once the relay is subscribed and ready to emit events. */
  ready: Promise<void>;
  pause?(): void;
  resume?(): void;
  /** Tear down the source subscription. Idempotent. */
  close(): void;
}

/** Factory shape for session-scoped event relays. */
export type RelayFactory = (opts: RealtimeRelayOptions) => RealtimeRelay;
