// Daemon push relay — carries daemon-origin agent updates and ephemeral developer
// events over the daemon→gateway platform-local push endpoint. The shared
// connection survives daemon replacement by rereading the manifest with bounded
// backoff, discarding socket-local framing bytes, and resubscribing from its latest
// acknowledged cursors.
//
// Plain-JS module (.mjs + .d.mts, same convention as ws.mjs): scripts/gateway-serve.mjs
// imports the WS upgrade stack with plain Node — no TypeScript, no path aliases — so
// everything reachable from ws.mjs at module load must stay plain-Node loadable.
// Types live in daemonPushRelay.d.mts.

import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { createConnection } from "node:net";

let sharedConnection;
const RECONNECT_BASE_MS = 100;
const RECONNECT_MAX_MS = 5_000;

export function createDaemonPushAgentSessionRelay(sessionId, deps = {}) {
  return ({ onEvent, onGap = () => {} }) => {
    const connection = deps.connector?.();
    if (!connection) {
      onGap();
      return {
        ready: Promise.resolve(),
        close() {},
      };
    }
    let gapSignaled = false;
    let closed = false;
    let paused = false;
    let cursor = deps.afterId ?? 0;
    let unsubscribe = () => {};
    const signalGap = () => {
      if (gapSignaled) return;
      gapSignaled = true;
      onGap();
    };

    const subscribe = () => {
      if (closed || paused) return;
      unsubscribe = connection.subscribe(
        {
          lane: "agent",
          sessionId,
          afterId: cursor,
        },
        {
          onFrame(frame) {
            const mapped = daemonPushFrameToWsEvent(frame, sessionId);
            if (mapped === "gap") {
              onGap();
              return;
            }
            if (!mapped) return;
            if (onEvent(mapped) === false) {
              paused = true;
              unsubscribe();
              return;
            }
            const rawId = mapped.data?.streamEventId ?? mapped.data?.stream_event_id;
            const id = typeof rawId === "number" ? rawId : Number(rawId);
            if (Number.isFinite(id) && id > 0) cursor = Math.max(cursor, id);
          },
          onError() {
            signalGap();
          },
        },
      );
    };
    subscribe();

    return {
      ready: connection.ready.catch(() => {
        signalGap();
      }),
      pause() {
        if (closed || paused) return;
        paused = true;
        unsubscribe();
      },
      resume() {
        if (closed || !paused) return;
        paused = false;
        subscribe();
      },
      close() {
        closed = true;
        unsubscribe();
      },
    };
  };
}

export function createDaemonPushDeveloperEventSource(sessionId, deps = {}) {
  const connection = (deps.connector ?? sharedDaemonPushConnector)?.();
  if (!connection) return undefined;
  return {
    subscribe(topic, afterSeq, handlers) {
      const unsubscribe = connection.subscribe(
        {
          lane: "developer_event",
          sessionId,
          afterId: afterSeq,
        },
        {
          onFrame(frame) {
            const event = daemonPushFrameToDeveloperEvent(frame, sessionId, topic);
            if (event === "gap") {
              handlers.onGap?.(frame);
              return;
            }
            if (event) handlers.onEvent(event);
          },
          onError(error) {
            handlers.onError?.(error);
          },
        },
      );
      void connection.ready.catch((error) => handlers.onError?.(error));
      return unsubscribe;
    },
  };
}

export function sharedDaemonPushConnector() {
  if (sharedConnection) return sharedConnection;
  sharedConnection = createManifestDaemonPushConnection();
  return sharedConnection;
}

export function closeSharedDaemonPushConnector() {
  sharedConnection?.close();
  sharedConnection = undefined;
}

export function resetSharedDaemonPushConnectorForTests() {
  closeSharedDaemonPushConnector();
}

export function gatewayStreamEndpointManifestPath(env = process.env) {
  const home = env.NEXUS_HOME?.trim() || join(homedir(), ".nexus");
  return join(home, "gateway-stream-endpoint.json");
}

/** A separately owned connection; its subscription cursors never share the live fanout's tail. */
export function createManifestDaemonPushConnection(deps = {}) {
  const manifestPath = gatewayStreamEndpointManifestPath();
  if (!existsSync(manifestPath)) return undefined;
  return new DaemonPushConnection(manifestPath, deps.readManifest);
}

class DaemonPushConnection {
  #socket;
  #buffer = Buffer.alloc(0);
  #connected = false;
  #closed = false;
  #manifest;
  #daemonBootId;
  #manifestPath;
  #readManifest;
  #subscriptions = new Map();
  #projectionHandlers = new Set();
  #hookProvider;
  #hookReadiness;
  #opening = false;
  #reconnectAttempt = 0;
  #reconnectTimer;
  #paused = false;
  #consumeControl = () => false;

  constructor(manifestPath, readManifest = (path) => readFile(path, "utf8")) {
    this.#manifestPath = manifestPath;
    this.#readManifest = readManifest;
    this.ready = this.#connect().catch((error) => {
      this.#broadcastError(error);
      this.#scheduleReconnect();
      throw error;
    });
  }

  get daemonBootId() {
    return this.#daemonBootId ?? this.#manifest?.daemonBootId;
  }

  subscribe(subscription, handlers) {
    const key = subscriptionKey(subscription);
    let entry = this.#subscriptions.get(key);
    const alreadySubscribed = Boolean(entry);
    if (!entry) {
      entry = { subscription, handlers: new Set() };
      this.#subscriptions.set(key, entry);
    }
    entry.handlers.add(handlers);
    const fleet = subscription.lane === "developer_event" && subscription.sessionId === "fleet";
    if (alreadySubscribed && fleet) {
      // The upstream fleet lane is shared. A browser-local subscribe must reconcile only that new
      // handler; asking the daemon to replay would fan its resync out to every existing browser.
      const cursor = entry.subscription.afterId ?? 0;
      queueMicrotask(() => {
        if (entry?.handlers.has(handlers)) handlers.onFrame(fleetResyncFrame(cursor));
      });
    } else if (this.#connected) {
      this.#writeFrame({ t: "subscribe", ...subscription });
    }
    return () => {
      entry?.handlers.delete(handlers);
      if (entry && entry.handlers.size === 0) {
        this.#subscriptions.delete(key);
        if (this.#connected) {
          this.#writeFrame({
            t: "unsubscribe",
            lane: subscription.lane,
            sessionId: subscription.sessionId,
          });
        }
      }
    };
  }

  subscribeProjections(handlers) {
    this.#projectionHandlers.add(handlers);
    return () => this.#projectionHandlers.delete(handlers);
  }

  async ackProjection(ack) {
    if (!this.#connected) throw new Error("daemon projection connection is not ready");
    this.#writeFrame({ t: "projection.ack", ack });
  }

  registerHooks(capabilities, handler) {
    if (!capabilities || typeof capabilities !== "object") {
      throw new Error("hook capabilities are required");
    }
    if (typeof handler !== "function") throw new Error("hook handler is required");
    const registration = { capabilities, handler };
    this.#hookProvider = registration;
    this.#hookReadiness?.resolve();
    this.ready = new Promise((resolve) => {
      this.#hookReadiness = { registration, resolve };
    });
    if (this.#connected) this.#socket?.destroy();
    return () => {
      if (this.#hookProvider !== registration) return;
      this.#hookProvider = undefined;
      if (this.#connected) this.#socket?.destroy();
    };
  }

  /** Flow control for an independently owned reader; never pause the shared fanout connector. */
  pause() {
    if (this.#closed) return;
    this.#paused = true;
    this.#socket?.pause();
  }

  resume() {
    if (this.#closed || !this.#paused) return;
    this.#paused = false;
    // Finish already-read framing bytes before admitting another socket chunk. A downstream
    // callback may pause us again in the middle of this buffer.
    this.#onData(Buffer.alloc(0));
    if (!this.#paused && !this.#closed) this.#socket?.resume();
  }

  close() {
    this.#closed = true;
    if (this.#reconnectTimer) clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = undefined;
    this.#socket?.destroy();
    this.#socket = undefined;
    this.#buffer = Buffer.alloc(0);
    this.#subscriptions.clear();
    this.#projectionHandlers.clear();
    this.#hookProvider = undefined;
    this.#hookReadiness?.resolve();
    this.#hookReadiness = undefined;
  }

  async #connect() {
    if (this.#closed || this.#connected || this.#opening) return;
    this.#opening = true;
    try {
      // The daemon replaces this boot-scoped manifest on every restart. Always reread it instead of
      // retaining the old socket path/token in the long-lived gateway process.
      const nextManifest = JSON.parse(await this.#readManifest(this.#manifestPath));
      if (this.#closed) return;
      const previousBoot = this.#manifest?.daemonBootId;
      if (previousBoot && nextManifest.daemonBootId && previousBoot !== nextManifest.daemonBootId) {
        for (const entry of this.#subscriptions.values()) {
          // Fleet has an explicit connection-local resync boundary that resets an ahead cursor.
          // Other boot-scoped developer rings and the replaced agent stream store restart at zero.
          if (entry.subscription.lane !== "developer_event" || entry.subscription.sessionId !== "fleet") {
            entry.subscription.afterId = 0;
          }
        }
      }
      this.#manifest = nextManifest;
      this.#daemonBootId = typeof nextManifest.daemonBootId === "string"
        ? nextManifest.daemonBootId
        : undefined;
      this.#buffer = Buffer.alloc(0);
      await this.#openSocket();
      this.#reconnectAttempt = 0;
    } finally {
      this.#opening = false;
    }
  }

  async #openSocket() {
    if (this.#closed) return;
    await new Promise((resolve, reject) => {
      const socket = createConnection({ path: this.#manifest.path });
      this.#socket = socket;
      let opened = false;
      let protocolReady = false;
      let helloHookProvider;
      socket.once("connect", () => {
        opened = true;
        this.#connected = true;
        helloHookProvider = this.#hookProvider;
        this.#writeFrame({
          t: "hello",
          version: 1,
          token: this.#manifest.token,
          subscriptions: [...this.#subscriptions.values()].map((entry) => entry.subscription),
          ...(helloHookProvider ? { hooks: helloHookProvider.capabilities } : {}),
        });
      });
      socket.once("error", (error) => {
        if (!protocolReady) reject(error);
      });
      socket.on("data", (chunk) => this.#onData(chunk, (frame) => {
        if (frame?.t === "ready") {
          if (typeof frame.daemonBootId !== "string" || !frame.daemonBootId) {
            const error = new Error("daemon push ready frame is missing daemonBootId");
            this.#broadcastError(error);
            reject(error);
            socket.destroy();
            return true;
          }
          this.#daemonBootId = frame.daemonBootId;
          protocolReady = true;
          const hookReadiness = this.#hookReadiness;
          if (hookReadiness && hookReadiness.registration === helloHookProvider) {
            hookReadiness.resolve();
            this.#hookReadiness = undefined;
          }
          resolve();
          return true;
        }
        if (frame?.t === "error" && !protocolReady) {
          const error = new Error(frame.message || frame.code || "daemon push handshake rejected");
          this.#broadcastError(error);
          reject(error);
          socket.destroy();
          return true;
        }
        return false;
      }));
      socket.on("error", (error) => {
        if (opened) this.#broadcastError(error);
      });
      socket.on("close", () => {
        if (this.#socket !== socket) return;
        this.#connected = false;
        this.#socket = undefined;
        // Length-prefixed bytes are boot/socket scoped. A partial old frame must never prefix the
        // first frame from the replacement daemon endpoint.
        this.#buffer = Buffer.alloc(0);
        if (!protocolReady) reject(new Error("daemon push closed before ready"));
        if (opened && !this.#closed) this.#scheduleReconnect();
      });
    });
  }

  #scheduleReconnect() {
    if (this.#closed || this.#connected || this.#reconnectTimer) return;
    if (this.#opening) {
      this.#reconnectTimer = setTimeout(() => {
        this.#reconnectTimer = undefined;
        this.#scheduleReconnect();
      }, 0);
      this.#reconnectTimer.unref?.();
      return;
    }
    const exponent = Math.min(this.#reconnectAttempt, 8);
    const delay = Math.min(RECONNECT_BASE_MS * (2 ** exponent), RECONNECT_MAX_MS);
    this.#reconnectAttempt += 1;
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = undefined;
      void this.#connect().catch((error) => {
        this.#broadcastError(error);
        this.#scheduleReconnect();
      });
    }, delay);
    this.#reconnectTimer.unref?.();
  }

  #onData(chunk, consumeControl) {
    if (consumeControl) this.#consumeControl = consumeControl;
    this.#buffer = Buffer.concat([this.#buffer, chunk]);
    while (!this.#paused && !this.#closed && this.#buffer.length >= 4) {
      const len = this.#buffer.readUInt32BE(0);
      if (this.#buffer.length < len + 4) return;
      const payload = this.#buffer.subarray(4, len + 4);
      this.#buffer = this.#buffer.subarray(len + 4);
      let frame;
      try {
        frame = JSON.parse(payload.toString("utf8"));
      } catch (error) {
        this.#broadcastError(error);
        continue;
      }
      if (!this.#consumeControl(frame)) this.#routeFrame(frame);
    }
  }

  #routeFrame(frame) {
    if (!frame || typeof frame !== "object") return;
    if (frame.t === "hook.evaluate") {
      this.#handleHookEvaluation(frame);
      return;
    }
    if (frame.t === "projection" || frame.t === "projection.gap") {
      for (const handlers of [...this.#projectionHandlers]) handlers.onFrame(frame);
      return;
    }
    if (frame.t !== "agent.update" && frame.t !== "raw" && frame.t !== "developer.event" && frame.t !== "gap") return;
    if (!frame.sessionId) return;
    const lane = frame.t === "developer.event" || frame.lane === "developer_event"
      ? "developer_event"
      : frame.t === "raw" || frame.lane === "raw"
        ? "raw"
        : "agent";
    const key = subscriptionKey({ lane, sessionId: frame.sessionId });
    const entry = this.#subscriptions.get(key);
    if (!entry) return;
    if (frame.t === "agent.update" || frame.t === "raw") {
      const cursor = frame.t === "raw" ? frame.streamRawId : frame.streamEventId;
      if (
        Number.isInteger(cursor)
        && cursor > 0
        && cursor <= (entry.subscription.afterId ?? 0)
      ) return;
    }
    this.#advanceCursor(entry, frame);
    for (const handlers of [...entry.handlers]) handlers.onFrame(frame);
  }

  #handleHookEvaluation(frame) {
    const provider = this.#hookProvider;
    const correlationId = frame.evaluation?.correlationId;
    const request = frame.evaluation?.request;
    if (!provider || typeof correlationId !== "string" || !request || typeof request !== "object") {
      return;
    }
    Promise.resolve()
      .then(() => provider.handler(request))
      .then((result) => {
        if (this.#hookProvider !== provider) return;
        this.#writeFrame({ t: "hook.result", correlationId, result });
      })
      .catch((error) => {
        if (this.#hookProvider !== provider) return;
        this.#writeFrame({
          t: "hook.result",
          correlationId,
          error: {
            code: typeof error?.code === "string" ? error.code : "hook_failed",
            message: error instanceof Error ? error.message : String(error),
            retryable: error?.retryable === true,
          },
        });
      });
  }

  #advanceCursor(entry, frame) {
    let cursor;
    if (frame.t === "agent.update") cursor = frame.streamEventId;
    else if (frame.t === "raw") cursor = frame.streamRawId;
    else if (frame.t === "developer.event") cursor = frame.event?.seq;
    if (!Number.isInteger(cursor) || cursor < 0) return;
    const fleetResync = frame.t === "developer.event"
      && frame.sessionId === "fleet"
      && frame.event?.lifecycle === "resync";
    entry.subscription.afterId = fleetResync
      ? cursor
      : Math.max(entry.subscription.afterId ?? 0, cursor);
  }

  #writeFrame(frame) {
    if (!this.#socket || !this.#connected) return;
    const payload = Buffer.from(JSON.stringify(frame), "utf8");
    const header = Buffer.alloc(4);
    header.writeUInt32BE(payload.length, 0);
    this.#socket.write(Buffer.concat([header, payload]));
  }

  #broadcastError(error) {
    for (const handlers of this.#projectionHandlers) handlers.onError(error);
    for (const entry of this.#subscriptions.values()) {
      for (const handlers of entry.handlers) handlers.onError(error);
    }
  }
}

function daemonPushFrameToWsEvent(frame, sessionId) {
  if (!frame || typeof frame !== "object") return undefined;
  if (frame.t === "gap" && frame.lane === "agent" && frame.sessionId === sessionId) {
    return "gap";
  }
  if (frame.t !== "agent.update" || frame.sessionId !== sessionId) return undefined;
  if (!frame.kind) return undefined;
  const data = normalizeData(frame.data);
  if (typeof frame.streamEventId === "number") {
    data.streamEventId = frame.streamEventId;
  }
  return {
    type: "agent.update",
    sessionId,
    kind: frame.kind,
    data,
  };
}

function daemonPushFrameToDeveloperEvent(frame, sessionId, topic) {
  if (!frame || typeof frame !== "object") return undefined;
  if (frame.t === "gap" && frame.lane === "developer_event" && frame.sessionId === sessionId) {
    return "gap";
  }
  if (frame.t !== "developer.event" || frame.sessionId !== sessionId) return undefined;
  const event = frame.event;
  if (!event || typeof event !== "object" || event.topic !== topic) return undefined;
  return event;
}

function normalizeData(data) {
  if (data && typeof data === "object" && !Array.isArray(data)) {
    return { ...data };
  }
  return {};
}

function subscriptionKey(subscription) {
  return `${subscription.lane}:${subscription.sessionId}`;
}

function fleetResyncFrame(seq) {
  return {
    t: "developer.event",
    sessionId: "fleet",
    event: {
      kind: "agent_lifecycle",
      topic: "sys.fleet.status",
      seq,
      ts: Date.now(),
      sessionId: "fleet",
      lifecycle: "resync",
      data: { reason: "subscribe", source: "members" },
    },
  };
}
