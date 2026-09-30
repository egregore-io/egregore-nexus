export interface AguiWsHandle {
  closed: Promise<void>;
  close(code?: number, reason?: string): void;
}

export interface AguiWsSocket {
  bufferedAmount?: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  on?(event: "message", handler: (data: string | Buffer) => void): unknown;
  on?(event: "close" | "error", handler: () => void): unknown;
}

export interface AguiWsInput {
  mode: "session" | "bus";
  /** Session targets carry the canonical agentId/expectedSessionId pair; bus targets are unchanged. */
  target: unknown;
  text: string;
  clientMessageId?: string;
}

export interface AguiWsSteerInput {
  /** Canonical session target including agentId and expectedSessionId. */
  target: unknown;
  text: string;
  clientMessageId?: string;
}

export interface AguiWsInterruptInput {
  /** Canonical session target including agentId and expectedSessionId. */
  target: unknown;
  clientMessageId: string;
}

/** Transport binding only; not an AG-UI run event or native readiness/admission proof. */
export interface AguiWsSessionBound {
  t: "session.bound";
  agentId: string;
  sessionId: string;
}

export interface AguiWsDeveloperEventSource {
  subscribe(
    topic: string,
    afterSeq: number,
    handlers: {
      onEvent: (event: any) => boolean | void;
      onError?: (error: unknown) => void;
    },
  ): {
    ready: Promise<void>;
    pause?(): void;
    resume?(): void;
    close(): void;
  };
}

export interface AguiWsDaemonToolCallEventSource {
  subscribe(
    topic: string,
    afterSeq: number,
    handlers: {
      onEvent: (event: unknown) => void;
      onGap?: (frame: unknown) => void;
      onError?: (error: unknown) => void;
    },
  ): (() => void) | undefined;
}

export interface AguiWsObserveFrameObserver {
  observeAguiFrame(payload: string): void;
}

export interface AguiWsPumpOptions {
  /** Apply the bounded, resumable agent-session WebSocket outbound contract. */
  session?: boolean;
}

export interface AguiWsDeps {
  runtimeSnapshots?: import("./runtimeSnapshots").RuntimeSnapshotSource;
  fetchHandler?: (request: Request) => Promise<Response>;
  observe?: (request: Request) => Promise<Response>;
  sessionInput?: (input: AguiWsInput, request: Request) => Promise<Response>;
  busInput?: (input: AguiWsInput, request: Request) => Promise<Response>;
  steerInput?: (input: AguiWsSteerInput, request: Request) => Promise<Response>;
  interruptInput?: (input: AguiWsInterruptInput, request: Request) => Promise<Response>;
  /** Session name → harness kind (`claude`|`codex`|…) for the command catalog.
   *  Defaults to a `/api/v1/members` lookup through `fetchHandler`. */
  resolveHarness?: (
    target: string | { name?: string; agentId?: string },
    request: Request,
  ) => Promise<string | undefined>;
  developerEvents?: AguiWsDeveloperEventSource;
  daemonToolCallEvents?: AguiWsDaemonToolCallEventSource | null;
  /** Ephemeral `sys.fleet.status` push source (agent presence/activity/spawned/removed plus the
   *  ordered `resync` reconciliation boundary on every subscribe/reconnect). Defaults to the
   *  daemon push socket under the pseudo session id `fleet`. */
  daemonFleetStatusEvents?: AguiWsDaemonToolCallEventSource | null;
  /** One gateway-wide poll cadence for the daemon-owned transition projection. */
  commandQueueEventPollMs?: number;
  commandQueueObservationPollMs?: number;
  commandQueueHub?: CommandQueueHub;
}

export interface CommandQueueHubHandlers {
  onSnapshot(snapshot: unknown): void;
  onTransition(transition: unknown): void;
  onError(
    error: string,
    details: {
      phase: "subscribe" | "hydrate" | "refresh" | "events";
      fatal: boolean;
    },
  ): void;
  onRestored?(seq: number): void;
}

export class CommandQueueHub {
  constructor(deps: AguiWsDeps);
  subscribe(
    request: Request,
    target:
      string | { name?: string; agentId?: string; expectedSessionId?: string },
    handlers: CommandQueueHubHandlers,
  ): () => void;
}

export function handleWs(
  socket: AguiWsSocket,
  request: Request,
  deps?: AguiWsDeps,
): AguiWsHandle;

export function attachAguiWsUpgrade(
  server: unknown,
  options?: AguiWsDeps,
): Promise<unknown>;

export function toObserveRequest(request: Request): Request;

export function pumpSseResponseToSocket(
  response: Response,
  socket: AguiWsSocket,
  signal?: AbortSignal,
  observer?: AguiWsObserveFrameObserver,
  options?: AguiWsPumpOptions,
): Promise<void>;
