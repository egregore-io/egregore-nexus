import { createDaemonPushDeveloperEventSource } from "./daemonPushRelay.mjs";
import {
  catalogForHarness,
  findCommand,
  gatewayVerbPath,
  renderHarnessInvocation,
  validateCommandArgs,
} from "./commandRegistry.mjs";
import { bindWebSocketCsrf } from "../auth/browserMutationAuth.mjs";

const AGUI_WS_PATH = "/api/agui/ws";
const SESSION_EVENTS_PATH = /^\/api\/v1\/agent-sessions\/[^/]+\/events$/;
const MAX_BUFFERED_AMOUNT = 1024 * 1024;
const MAX_SESSION_CONTROL_FRAME_BYTES = 8 * 1024 * 1024;
const SESSION_BACKPRESSURE_REASON = "session.bp:";
const DEVELOPER_BACKPRESSURE_REASON = "developer.backpressure:";
const TOOL_CALL_TOPIC_PREFIX = "sys.agent.";
const TOOL_CALL_TOPIC_SUFFIX = ".tool_call";
// Fleet-wide agent status (presence/spawned/removed) — ephemeral daemon push events
// published under the pseudo session id "fleet". Any WS client may subscribe; fleet
// status is discovery-level data.
const FLEET_STATUS_TOPIC = "sys.fleet.status";
const FLEET_SESSION_KEY = "fleet";
const TOOL_CALL_RING_LIMIT = 256;
const TOOL_CALL_NAME_LIMIT = 1024;
const TOOL_CALL_START = "TOOL_CALL_START";
const TOOL_CALL_RESULT = "TOOL_CALL_RESULT";
const DEFAULT_COMMAND_QUEUE_EVENT_POLL_MS = 100;
const SESSION_OUTBOUND = new WeakMap();

export function handleWs(socket, request, deps = {}) {
  const observe = deps.observe ?? ((req) => routeThroughFetchHandler(deps, req));
  const sessionInput = deps.sessionInput ?? defaultSessionInput(deps);
  const busInput = deps.busInput ?? defaultBusInput(deps);
  const steerInput = deps.steerInput ?? defaultSteerInput(deps);
  const interruptInput = deps.interruptInput ?? defaultInterruptInput(deps);
  const sessionLane = new SessionLaneBinding(request);
  const sessionOutbound = sessionLane.isSessionLane
    ? {
        socket,
        observer: undefined,
        session: true,
        stopped: false,
        lastAcceptedCursor: undefined,
        pendingAcceptedCursor: undefined,
      }
    : undefined;
  if (sessionOutbound) SESSION_OUTBOUND.set(socket, sessionOutbound);
  const commands = new CommandFrames(socket, request, deps, sessionInput, sessionLane);
  const commandQueue = new SessionCommandQueue(socket, request, deps, sessionLane);
  const subscriptions = new DeveloperEventSubscriptions(socket, request, deps);
  const abort = new AbortController();
  let finished = false;
  let resolveClosed;
  const closed = new Promise((resolve) => {
    resolveClosed = resolve;
  });

  const finish = () => {
    if (finished) return;
    finished = true;
    sessionLane.fail("agent-session socket closed before lane binding completed");
    abort.abort();
    subscriptions.close();
    commandQueue.close();
    resolveClosed?.();
  };
  const close = (code, reason) => {
    if (!finished) {
      try {
        socket.close(code, reason);
      } catch {
        // The peer may already be gone; the cleanup below is enough.
      }
    }
    finish();
  };

  socket.on?.("close", finish);
  socket.on?.("error", finish);
  socket.on?.("message", (data) => {
    void handleClientFrame(String(data), {
      socket,
      request,
      sessionInput,
      busInput,
      steerInput,
      interruptInput,
      subscriptions,
      commands,
      commandQueue,
      sessionLane,
    });
  });

  void (async () => {
    // Dedicated events lane: a socket with no
    // observe target is legal — it carries only subscribe/command frames, no content
    // stream. Without this, a bare socket died 1011 "missing target".
    if (!hasObserveTarget(request)) return;
    try {
      const response = await observe(toObserveRequest(request));
      if (!response.ok) {
        sessionLane.fail(await errorReason(response));
        close(closeCodeForStatus(response.status), await errorReason(response));
        return;
      }
      const boundTarget = sessionLane.bindResponse(response);
      if (boundTarget) {
        commandQueue.start(boundTarget);
        subscriptions.bindSession(boundTarget.name);
      }
      await pumpSseResponseToSocket(response, socket, abort.signal, subscriptions, {
        session: Boolean(sessionLaneTargetFromRequest(request) || sessionIdFromPath(request)),
        outbound: sessionOutbound,
      });
      close(1000, "observe ended");
    } catch (error) {
      sessionLane.fail(messageForError(error));
      if (!abort.signal.aborted) close(1011, messageForError(error));
    }
  })();

  return { closed, close };
}

function hasObserveTarget(request) {
  const url = new URL(request.url);
  if (SESSION_EVENTS_PATH.test(url.pathname)) return true;
  const params = url.searchParams;
  return ["session", "agentId", "thread", "dm", "topic"].some((key) => params.get(key));
}

export async function attachAguiWsUpgrade(server, options = {}) {
  const { WebSocketServer } = await import("ws");
  const wss = new WebSocketServer({ noServer: true });
  // ONE watcher per gateway process. Lane sockets subscribe to this hub; they never create their
  // own timers or DB polls. The hub reads the append-only transition projection with a monotonic
  // cursor and fans events out by exact durable session id.
  const commandQueueHub = options.commandQueueHub ?? (
    typeof options.fetchHandler === "function" ? new CommandQueueHub(options) : undefined
  );
  const socketOptions = { ...options, commandQueueHub };
  server.on("upgrade", (req, socket, head) => {
    const request = bindWebSocketCsrf(nodeUpgradeRequestToFetch(req));
    const url = new URL(request.url);
    if (url.pathname !== AGUI_WS_PATH && !SESSION_EVENTS_PATH.test(url.pathname)) {
      // This is the server's only upgrade handler: fail wrong-path upgrades
      // fast instead of leaving the client hanging on an unanswered upgrade.
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      handleWs(ws, request, socketOptions);
    });
  });
  return wss;
}

export function toObserveRequest(request) {
  const url = new URL(request.url);
  if (SESSION_EVENTS_PATH.test(url.pathname)) return request;
  url.pathname = "/api/agui/observe";
  return new Request(url, {
    method: "GET",
    headers: request.headers,
    signal: request.signal,
  });
}

export async function pumpSseResponseToSocket(response, socket, signal, observer, options = {}) {
  if (!response.body) return;
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const outbound = options.outbound ?? {
    socket,
    observer,
    session: options.session === true,
    stopped: false,
    lastAcceptedCursor: undefined,
    pendingAcceptedCursor: undefined,
  };
  outbound.observer = observer;
  let pending = "";
  for (;;) {
    if (signal?.aborted) {
      await reader.cancel().catch(() => {});
      return;
    }
    const { value, done } = await reader.read();
    if (done) break;
    if (!value) continue;
    pending += decoder.decode(value, { stream: true });
    pending = flushSseBlocks(pending, outbound);
    if (
      !outbound.stopped
      && outbound.session
      && pending.length > 0
      && (outbound.socket.bufferedAmount ?? 0) + Buffer.byteLength(pending, "utf8")
        > MAX_BUFFERED_AMOUNT
    ) {
      closeOutboundBackpressure(outbound);
    }
    if (outbound.stopped) {
      await reader.cancel().catch(() => {});
      return;
    }
  }
  pending += decoder.decode();
  flushSseBlocks(`${pending}\n\n`, outbound);
}

function flushSseBlocks(text, outbound) {
  let start = 0;
  const payloads = [];
  for (;;) {
    const next = text.indexOf("\n\n", start);
    if (next === -1) break;
    const payload = ssePayload(text.slice(start, next));
    if (payload !== undefined) payloads.push(payload);
    start = next + 2;
  }
  if (!emitSsePayloads(payloads, outbound)) return "";
  return text.slice(start);
}

function ssePayload(block) {
  const data = [];
  for (const line of block.split(/\r?\n/)) {
    if (!line.startsWith("data:")) continue;
    data.push(line.slice(5).trimStart());
  }
  return data.length > 0 ? data.join("\n") : undefined;
}

function emitSsePayloads(payloads, outbound) {
  if (payloads.length === 0) return true;
  if (outbound.session) {
    let start = 0;
    while (start < payloads.length) {
      const cursor = opaqueCursorFromPayload(payloads[start]);
      let end = start + 1;
      while (end < payloads.length && opaqueCursorFromPayload(payloads[end]) === cursor) end += 1;

      // A semantic cursor can produce multiple projection siblings, and transport readers may
      // split those siblings across chunks. Only a later distinct cursor proves the prior cursor
      // complete. Keeping one cursor pending makes a 1013 reconnect replay the whole unfinished
      // sibling group instead of skipping a frame that never reached the socket.
      if (
        cursor
        && outbound.pendingAcceptedCursor
        && cursor !== outbound.pendingAcceptedCursor
      ) {
        outbound.lastAcceptedCursor = outbound.pendingAcceptedCursor;
      }

      const group = payloads.slice(start, end);
      const groupBytes = group.reduce(
        (total, payload) => total + Buffer.byteLength(payload, "utf8"),
        0,
      );
      if ((outbound.socket.bufferedAmount ?? 0) + groupBytes > MAX_BUFFERED_AMOUNT) {
        closeOutboundBackpressure(outbound);
        return false;
      }
      for (const payload of group) {
        outbound.socket.send(payload);
        outbound.observer?.observeAguiFrame?.(payload);
      }
      if (cursor) outbound.pendingAcceptedCursor = cursor;
      start = end;
    }
    return true;
  }
  for (const payload of payloads) {
    if (!emitLegacySsePayload(payload, outbound)) return false;
  }
  return true;
}

function emitLegacySsePayload(payload, outbound) {
  const bufferedAmount = outbound.socket.bufferedAmount ?? 0;
  if (bufferedAmount > MAX_BUFFERED_AMOUNT) {
    closeOutboundBackpressure(outbound);
    return false;
  }
  outbound.socket.send(payload);
  outbound.observer?.observeAguiFrame?.(payload);
  return true;
}

function closeOutboundBackpressure(outbound) {
  if (outbound.stopped) return;
  outbound.stopped = true;
  outbound.socket.close?.(
    1013,
    outbound.session
      ? `${SESSION_BACKPRESSURE_REASON}${outbound.lastAcceptedCursor ?? "none"}`
      : "ag-ui websocket backpressure",
  );
}

function opaqueCursorFromPayload(payload) {
  try {
    const cursor = JSON.parse(payload)?.cursor;
    return typeof cursor === "string" && cursor.length > 0 ? cursor : undefined;
  } catch {
    return undefined;
  }
}

async function handleClientFrame(
  raw,
  {
    socket,
    request,
    sessionInput,
    busInput,
    steerInput,
    interruptInput,
    subscriptions,
    commands,
    commandQueue,
    sessionLane,
  },
) {
  let frame;
  try {
    frame = JSON.parse(raw);
  } catch {
    sendJson(socket, { t: "input.err", error: "frame must be JSON" });
    return;
  }
  if (frame?.t === "ping") {
    sendJson(socket, { t: "pong" });
    return;
  }
  if (frame?.t === "subscribe") {
    subscriptions.subscribe(frame);
    return;
  }
  if (frame?.t === "unsubscribe") {
    subscriptions.unsubscribe(frame);
    return;
  }
  if (frame?.t === "commands.list") {
    await commands.list(frame);
    return;
  }
  if (frame?.t === "command") {
    await commands.invoke(frame);
    return;
  }
  if (["queue.redirect", "queue.cancel", "queue.edit", "queue.reorder"].includes(frame?.t)) {
    await commandQueue.mutate(frame);
    return;
  }
  if (frame?.t === "steer") {
    const clientMessageId = stringField(frame.clientMessageId);
    try {
      const scoped = sessionLane.isSessionLane
        ? { ...frame, target: sessionTargetWireValue(await sessionLane.targetForFrame(frame)) }
        : withSocketScopedSteerDefaults(frame, request);
      const steer = normalizeSteerFrame(scoped);
      const response = await steerInput(steer, request);
      if (!response.ok) {
        sendJson(socket, {
          t: "steer.err",
          clientMessageId,
          status: response.status,
          error: await errorReason(response),
        });
        return;
      }
      const body = await response.json();
      if (!body?.result || typeof body.result !== "object" || Array.isArray(body.result)) {
        throw new Error("steer response is missing its result");
      }
      sendJson(socket, { t: "steer.ack", clientMessageId, ...body.result });
    } catch (error) {
      sendJson(socket, {
        t: "steer.err",
        clientMessageId,
        error: messageForError(error),
      });
    }
    return;
  }
  if (frame?.t === "interrupt") {
    const clientMessageId = stringField(frame.clientMessageId);
    try {
      if (!clientMessageId) throw new Error("interrupt.clientMessageId is required");
      const target = sessionLane.isSessionLane
        ? await sessionLane.targetForFrame(frame)
        : normalizeSessionLookup(frame.target, frame.agentId)
          ?? sessionTargetFromRequest(request);
      if (!target) throw new Error("interrupt requires a target session");
      const response = await interruptInput({ target: sessionTargetWireValue(target), clientMessageId }, request);
      if (!response.ok) {
        sendJson(socket, {
          t: "interrupt.err",
          clientMessageId,
          status: response.status,
          error: await errorReason(response),
        });
        return;
      }
      const body = await response.json();
      if (!body?.result || typeof body.result !== "object" || Array.isArray(body.result)) {
        throw new Error("interrupt response is missing its result");
      }
      sendJson(socket, { t: "interrupt.ack", clientMessageId, ...body.result });
    } catch (error) {
      sendJson(socket, {
        t: "interrupt.err",
        clientMessageId,
        error: messageForError(error),
      });
    }
    return;
  }
  if (frame?.t !== "input") {
    sendJson(socket, { t: "input.err", error: `unsupported frame: ${String(frame?.t)}` });
    return;
  }

  const clientMessageId = stringField(frame.clientMessageId);
  try {
    if (sessionLane.isSessionLane && frame.mode === "bus") {
      throw new Error("bus input cannot be sent on an agent-session lane");
    }
    const scoped = sessionLane.isSessionLane
      ? {
          ...frame,
          mode: "session",
          target: sessionTargetWireValue(await sessionLane.targetForFrame(frame)),
        }
      : withSocketScopedInputDefaults(frame, request);
    const input = normalizeInputFrame(scoped);
    const response = input.mode === "session"
      ? await sessionInput(input, request)
      : await busInput(input, request);
    if (!response.ok) {
      sendJson(socket, {
        t: "input.err",
        clientMessageId,
        status: response.status,
        error: await errorReason(response),
      });
      return;
    }
    if (input.mode === "session") {
      const body = await response.json();
      const receipt = normalizeQueueReceipt(body?.receipt, clientMessageId);
      commandQueue.observeReceipt(receipt);
      sendJson(socket, { t: "input.ack", ...receipt });
    } else {
      sendJson(socket, { t: "input.ack", clientMessageId, delivered: true });
    }
  } catch (error) {
    sendJson(socket, {
      t: "input.err",
      clientMessageId,
      error: messageForError(error),
    });
  }
}

function normalizeQueueReceipt(value, fallbackClientMessageId) {
  const commandId = stringField(value?.commandId);
  if (!commandId) throw new Error("session input response is missing commandId");
  const state = commandQueueState(value?.state);
  if (!state) throw new Error("session input response is missing queue state");
  const clientMessageId = stringField(value?.clientMessageId) ?? fallbackClientMessageId;
  const sessionId = stringField(value?.sessionId);
  const revision = nonNegativeInteger(value?.revision);
  const seq = nonNegativeInteger(value?.seq);
  if (revision === undefined || seq === undefined) {
    throw new Error("session input response is missing revision/seq");
  }
  return {
    commandId,
    ...(clientMessageId ? { clientMessageId } : {}),
    ...(sessionId ? { sessionId } : {}),
    state,
    revision,
    seq,
  };
}

function completedCommandReceipt(transition) {
  if (transition?.state !== "completed") return undefined;
  const commandKind = stringField(transition.commandKind);
  const commandId = stringField(transition.commandId);
  const clientId = stringField(transition.clientMessageId);
  const sessionId = stringField(transition.sessionId);
  const callerName = stringField(transition.callerName);
  const callerSessionId = stringField(transition.callerSessionId);
  const callerAgentId = stringField(transition.callerAgentId);
  const callerKind = stringField(transition.callerKind);
  const revision = nonNegativeInteger(transition.revision);
  const seq = nonNegativeInteger(transition.seq);
  const callerId = callerAgentId ?? callerSessionId;
  if (!commandKind || !commandId || !clientId || !sessionId || !callerName
      || !callerSessionId || !callerKind || !callerId
      || revision === undefined || seq === undefined) {
    return undefined;
  }
  return {
    commandKind,
    commandId,
    clientId,
    sessionId,
    state: "completed",
    revision,
    seq,
    callerId,
    callerKind,
    callerName,
    callerSessionId,
    ...(callerAgentId ? { callerAgentId } : {}),
  };
}

// One lane's subscription/mutation facade. All event reads live in CommandQueueHub below.
class SessionCommandQueue {
  #socket;
  #request;
  #deps;
  #target;
  #unsubscribe;
  #sessionLane;

  constructor(socket, request, deps, sessionLane) {
    this.#socket = socket;
    this.#request = request;
    this.#deps = deps;
    this.#target = sessionTargetFromRequest(request);
    this.#sessionLane = sessionLane;
  }

  start(boundTarget) {
    if (boundTarget) this.#target = boundTarget;
    if (!this.#target || !this.#deps.commandQueueHub) return;
    this.#unsubscribe = this.#deps.commandQueueHub.subscribe(
      this.#request,
      this.#target,
      {
        onSnapshot: (snapshot) => sendJson(this.#socket, { t: "queue.snapshot", ...snapshot }),
        onTransition: (transition) => {
          sendJson(this.#socket, { t: "command.transition", ...transition });
          const receipt = completedCommandReceipt(transition);
          if (receipt) sendJson(this.#socket, { t: "command.receipt", ...receipt });
        },
        onError: (error, details = {}) => sendJson(this.#socket, {
          t: "queue.err",
          error,
          phase: details.phase ?? "events",
          fatal: details.fatal === true,
        }),
        onRestored: (seq) => sendJson(this.#socket, { t: "queue.restored", seq }),
      },
    );
  }

  close() {
    this.#unsubscribe?.();
    this.#unsubscribe = undefined;
  }

  observeReceipt(_receipt) {}

  async mutate(frame) {
    const action = {
      "queue.redirect": "redirect_now",
      "queue.cancel": "cancel",
      "queue.edit": "edit",
      "queue.reorder": "reorder",
    }[frame.t];
    const clientMutationId = stringField(frame.clientMutationId);
    if (!clientMutationId) {
      sendJson(this.#socket, {
        t: "queue.mutation.err",
        error: "clientMutationId is required",
      });
      return;
    }
    try {
      const target = this.#sessionLane.isSessionLane
        ? await this.#sessionLane.targetForFrame(frame)
        : this.#target;
      if (!target) {
        throw new Error("queue operations require a session socket");
      }
      const response = await routeThroughFetchHandler(
        this.#deps,
        requestWithJson(this.#request, "/api/conversation/prompt", {
          ...sessionTargetRequestBody(target),
          action,
          clientMutationId,
          ...(stringField(frame.commandId) ? { commandId: stringField(frame.commandId) } : {}),
          ...(nonNegativeInteger(frame.expectedRevision) !== undefined
            ? { expectedRevision: nonNegativeInteger(frame.expectedRevision) }
            : {}),
          ...(stringField(frame.text) ? { text: stringField(frame.text) } : {}),
          ...(Array.isArray(frame.commandIds) ? { commandIds: frame.commandIds } : {}),
          ...(Array.isArray(frame.expectedRevisions)
            ? { expectedRevisions: frame.expectedRevisions }
            : {}),
        }, undefined, "PATCH"),
      );
      if (!response.ok) {
        sendJson(this.#socket, {
          t: "queue.mutation.err",
          action,
          clientMutationId,
          ...(stringField(frame.commandId) ? { commandId: stringField(frame.commandId) } : {}),
          status: response.status,
          error: await errorReason(response),
        });
        return;
      }
      const result = await response.json();
      sendJson(this.#socket, { t: "queue.mutation.ack", action, ...result });
    } catch (error) {
      sendJson(this.#socket, {
        t: "queue.mutation.err",
        action,
        clientMutationId,
        error: messageForError(error),
      });
    }
  }
}

/** Gateway-wide bounded watcher over the durable command transition log. */
export class CommandQueueHub {
  #deps;
  #subscribers = new Set();
  #groups = new Map();
  #pollMs;

  constructor(deps) {
    this.#deps = deps;
    this.#pollMs = positiveInteger(deps.commandQueueEventPollMs)
      ?? DEFAULT_COMMAND_QUEUE_EVENT_POLL_MS;
  }

  subscribe(request, target, handlers) {
    const normalizedTarget = normalizeSessionLookup(target);
    if (!normalizedTarget) {
      handlers.onError(
        "queue subscription requires a session name or agentId",
        { phase: "subscribe", fatal: true },
      );
      return () => {};
    }
    const authKey = commandQueueAuthKey(request);
    let group = this.#groups.get(authKey);
    if (!group) {
      group = {
        request,
        cursor: 0,
        ready: false,
        generation: 0,
        timer: undefined,
        polling: false,
        wakeAfterPoll: false,
        pendingRefreshes: new Map(),
      };
      this.#groups.set(authKey, group);
    }
    // A subscriber may hydrate to an older global cursor than already-mounted
    // siblings. Pause this auth group until every active snapshot settles, then
    // resume from their minimum; per-subscriber cursors suppress safe replay.
    group.generation += 1;
    group.ready = false;
    if (group.timer) {
      clearTimeout(group.timer);
      group.timer = undefined;
    }
    const subscriber = {
      request,
      target: normalizedTarget,
      handlers,
      authKey,
      sessionId: undefined,
      cursor: 0,
      ready: false,
      degradedPhases: new Set(),
      buffered: [],
      failed: false,
      closed: false,
      hydrationGeneration: 1,
    };
    this.#subscribers.add(subscriber);
    void this.#hydrate(subscriber, group, "subscribe", subscriber.hydrationGeneration);
    return () => {
      subscriber.closed = true;
      this.#subscribers.delete(subscriber);
      const remaining = [...this.#subscribers].filter((entry) => (
        entry.authKey === authKey && !entry.closed
      ));
      if (remaining.length === 0) {
        if (group.timer) clearTimeout(group.timer);
        group.pendingRefreshes.clear();
        this.#groups.delete(authKey);
      } else {
        group.generation += 1;
        group.request = remaining[0].request;
        this.#reconcileGroup(authKey, group);
      }
    };
  }

  #queueError(subscriber, error, phase, fatal) {
    if (fatal) subscriber.failed = true;
    if (!fatal) subscriber.degradedPhases.add(phase);
    subscriber.handlers.onError(messageForError(error), { phase, fatal });
  }

  #queueRestored(subscriber, phase, seq) {
    if (subscriber.failed) return;
    if (!subscriber.degradedPhases.delete(phase)) return;
    if (subscriber.degradedPhases.size === 0) subscriber.handlers.onRestored?.(seq);
  }

  #reconcileGroup(authKey, group, delay = 0) {
    if (this.#groups.get(authKey) !== group) return;
    const active = [...this.#subscribers].filter((subscriber) => (
      subscriber.authKey === authKey && !subscriber.closed && !subscriber.failed
    ));
    if (active.length === 0 || active.some((subscriber) => !subscriber.ready)) {
      group.ready = false;
      return;
    }
    group.cursor = Math.min(...active.map((subscriber) => subscriber.cursor));
    group.ready = true;
    this.#scheduleGroup(authKey, group, delay);
  }

  async #hydrate(subscriber, group, phase, generation) {
    try {
      const snapshot = await this.#loadSnapshot(subscriber.request, subscriber.target);
      if (
        subscriber.closed
        || this.#groups.get(subscriber.authKey) !== group
        || subscriber.hydrationGeneration !== generation
      ) return;
      subscriber.sessionId = stringField(snapshot?.sessionId);
      subscriber.handlers.onSnapshot(snapshot);
      const snapshotSeq = nonNegativeInteger(snapshot?.seq) ?? 0;
      subscriber.cursor = snapshotSeq;
      subscriber.ready = true;
      for (const event of subscriber.buffered) {
        const seq = nonNegativeInteger(event?.seq);
        if (seq === undefined || seq <= subscriber.cursor) continue;
        subscriber.cursor = seq;
        subscriber.handlers.onTransition(event);
      }
      subscriber.buffered = [];
      subscriber.failed = false;
      if (phase === "hydrate" && subscriber.degradedPhases.size > 0) {
        subscriber.degradedPhases.clear();
        subscriber.handlers.onRestored?.(subscriber.cursor);
      }
      this.#reconcileGroup(
        subscriber.authKey,
        group,
        phase === "hydrate" ? this.#pollMs : 0,
      );
    } catch (error) {
      if (
        !subscriber.closed
        && this.#groups.get(subscriber.authKey) === group
        && subscriber.hydrationGeneration === generation
      ) {
        subscriber.ready = false;
        this.#queueError(subscriber, error, phase, true);
        this.#reconcileGroup(subscriber.authKey, group);
      }
    }
  }

  async #loadSnapshot(request, target) {
    const url = new URL(request.url);
    url.pathname = "/api/conversation/prompt";
    url.search = "";
    if (target.name) url.searchParams.set("name", target.name);
    if (target.agentId) url.searchParams.set("agentId", target.agentId);
    const response = await routeThroughFetchHandler(
      this.#deps,
      new Request(url, { method: "GET", headers: request.headers }),
    );
    if (!response.ok) throw new Error(await errorReason(response));
    return response.json();
  }

  async #refreshGroupSubscribers(group, generation, subscribers) {
    const active = [...subscribers].filter((subscriber) => (
      !subscriber.closed && !subscriber.failed && subscriber.ready
    ));
    if (active.length === 0) return "done";
    try {
      // All entries share auth + target. Fetch once, then fan the authoritative projection out to
      // every mounted client. This is event-driven reconciliation, not another polling lane.
      const snapshot = await this.#loadSnapshot(active[0].request, active[0].target);
      if (
        this.#groups.get(active[0].authKey) !== group
        || group.generation !== generation
        || !group.ready
      ) return "stale";
      const sessionId = stringField(snapshot?.sessionId);
      for (const subscriber of active) {
        if (subscriber.closed) continue;
        subscriber.sessionId = sessionId;
        subscriber.handlers.onSnapshot(snapshot);
        this.#queueRestored(
          subscriber,
          "refresh",
          nonNegativeInteger(snapshot?.seq) ?? subscriber.cursor,
        );
      }
      return "done";
    } catch (error) {
      if (
        this.#groups.get(active[0].authKey) !== group
        || group.generation !== generation
        || !group.ready
      ) return "stale";
      for (const subscriber of active) {
        if (!subscriber.closed) this.#queueError(subscriber, error, "refresh", false);
      }
      return "retry";
    }
  }

  #scheduleGroup(authKey, group, delay) {
    if (this.#groups.get(authKey) !== group || !group.ready) return;
    if (group.polling) {
      if (delay === 0) group.wakeAfterPoll = true;
      return;
    }
    if (group.timer) {
      if (delay !== 0) return;
      clearTimeout(group.timer);
    }
    group.timer = setTimeout(() => {
      group.timer = undefined;
      void this.#pollGroup(authKey, group);
    }, delay);
  }

  async #pollGroup(authKey, group) {
    if (
      group.polling
      || !group.ready
      || this.#groups.get(authKey) !== group
    ) return;
    group.polling = true;
    const generation = group.generation;
    const requestCursor = group.cursor;
    let caughtUp = true;
    try {
      const url = new URL(group.request.url);
      url.pathname = "/api/conversation/prompt";
      url.search = "";
      url.searchParams.set("eventsAfter", String(requestCursor));
      const response = await routeThroughFetchHandler(
        this.#deps,
        new Request(url, { method: "GET", headers: group.request.headers }),
      );
      if (!response.ok) throw new Error(await errorReason(response));
      const body = await response.json();
      if (
        this.#groups.get(authKey) !== group
        || group.generation !== generation
        || !group.ready
        || group.cursor !== requestCursor
      ) return;
      if (body?.gap === true) {
        group.generation += 1;
        group.ready = false;
        group.pendingRefreshes.clear();
        const affected = [...this.#subscribers].filter((entry) => (
          entry.authKey === authKey && !entry.closed && !entry.failed
        ));
        for (const subscriber of affected) {
          subscriber.ready = false;
          subscriber.buffered = [];
          subscriber.hydrationGeneration += 1;
          void this.#hydrate(
            subscriber,
            group,
            "hydrate",
            subscriber.hydrationGeneration,
          );
        }
        return;
      }
      const events = Array.isArray(body?.events) ? body.events : [];
      for (const event of events) {
        const seq = nonNegativeInteger(event?.seq);
        if (seq === undefined || seq <= group.cursor) continue;
        group.cursor = seq;
        for (const subscriber of this.#subscribers) {
          if (subscriber.authKey !== authKey || subscriber.closed || subscriber.failed) continue;
          if (stringField(event?.sessionId) !== subscriber.sessionId) continue;
          if (seq <= subscriber.cursor) continue;
          if (subscriber.ready) {
            subscriber.cursor = seq;
            subscriber.handlers.onTransition(event);
            // A queued transition can mean new, edited, reordered, or redirected content. The
            // transition remains the lifecycle fact; one coalesced snapshot supplies text/order
            // that intentionally do not bloat every event row.
            if (commandQueueState(event?.state) === "queued") {
              const key = sessionTargetKey(subscriber.target);
              let entries = group.pendingRefreshes.get(key);
              if (!entries) {
                entries = new Set();
                group.pendingRefreshes.set(key, entries);
              }
              entries.add(subscriber);
            }
          } else {
            subscriber.buffered.push(event);
          }
        }
      }
      const latest = nonNegativeInteger(body?.latestSeq) ?? group.cursor;
      if (events.length === 0) group.cursor = Math.max(group.cursor, latest);
      if (group.cursor < latest) caughtUp = false;

      for (const [key, subscribers] of group.pendingRefreshes) {
        const result = await this.#refreshGroupSubscribers(group, generation, subscribers);
        if (result === "stale") return;
        if (result === "done") group.pendingRefreshes.delete(key);
      }
      for (const subscriber of this.#subscribers) {
        if (subscriber.authKey !== authKey || subscriber.closed || subscriber.failed) continue;
        this.#queueRestored(subscriber, "events", group.cursor);
      }
    } catch (error) {
      if (
        this.#groups.get(authKey) === group
        && group.generation === generation
        && group.ready
      ) {
        for (const subscriber of this.#subscribers) {
          if (subscriber.authKey !== authKey || subscriber.closed || subscriber.failed) continue;
          this.#queueError(subscriber, error, "events", false);
        }
      }
    } finally {
      group.polling = false;
      if (this.#groups.get(authKey) === group && group.ready) {
        const delay = group.wakeAfterPoll ? 0 : (caughtUp ? this.#pollMs : 0);
        group.wakeAfterPoll = false;
        this.#scheduleGroup(authKey, group, delay);
      }
    }
  }
}

function commandQueueAuthKey(request) {
  const cookie = request.headers.get("cookie");
  if (cookie) return `cookie\u0000${cookie}`;
  const authorization = request.headers.get("authorization");
  if (authorization) return `authorization\u0000${authorization}`;
  return "local";
}

function commandQueueState(value) {
  return ["queued", "claimed", "started", "completed", "failed", "cancelled"].includes(value)
    ? value
    : undefined;
}

function positiveInteger(value) {
  return Number.isInteger(value) && value > 0 ? value : undefined;
}

// SPEC-ws-full-command-access §13 (lens repo). Structured command surface on
// the socket: `commands.list` answers with the session's static catalog;
// `command` validates against it and dispatches EXACTLY once — gateway verbs
// to their REST handler, harness commands rendered to native slash text and
// delivered through the same prompt ingress passthrough input uses. The
// per-connection replay map is the server-side injection-once guard: a
// duplicate `clientCommandId` (client bug, reconnect replay) re-sends the
// original terminal frames and never re-dispatches.
const COMMAND_REPLAY_LIMIT = 128;

class CommandFrames {
  #socket;
  #request;
  #deps;
  #sessionInput;
  #sessionLane;
  #replay = new Map();

  constructor(socket, request, deps, sessionInput, sessionLane) {
    this.#socket = socket;
    this.#request = request;
    this.#deps = deps;
    this.#sessionInput = sessionInput;
    this.#sessionLane = sessionLane;
  }

  async list(frame) {
    let target;
    try {
      target = this.#sessionLane.isSessionLane
        ? await this.#sessionLane.targetForFrame(frame)
        : normalizeSessionLookup(frame.target, frame.agentId)
          ?? sessionTargetFromRequest(this.#request);
    } catch (error) {
      sendJson(this.#socket, {
        t: "commands.err",
        ...(stringField(frame.clientCommandId)
          ? { clientCommandId: stringField(frame.clientCommandId) }
          : {}),
        error: messageForError(error),
        code: "invalid_args",
      });
      return;
    }
    if (!target) {
      sendJson(this.#socket, {
        t: "commands.err",
        ...(stringField(frame.clientCommandId)
          ? { clientCommandId: stringField(frame.clientCommandId) }
          : {}),
        error: "commands.list requires a target session",
        code: "invalid_args",
      });
      return;
    }
    const harness = await this.#harnessFor(target);
    sendJson(this.#socket, {
      t: "commands.catalog",
      target: sessionTargetLabel(target),
      ...(target.agentId ? { agentId: target.agentId } : {}),
      ...catalogForHarness(harness),
    });
  }

  async invoke(frame) {
    const clientCommandId = stringField(frame.clientCommandId);
    if (!clientCommandId) {
      sendJson(this.#socket, {
        t: "command.err",
        error: "command.clientCommandId is required",
        code: "invalid_args",
      });
      return;
    }
    const known = this.#replay.get(clientCommandId);
    if (known) {
      // In-flight duplicate: the original dispatch will emit the terminal
      // frames once; sending nothing here is what makes execution single.
      if (known !== PENDING_COMMAND) {
        for (const reply of known) sendJson(this.#socket, reply);
      }
      return;
    }
    this.#remember(clientCommandId, PENDING_COMMAND);

    const err = (error, code) =>
      this.#finish(clientCommandId, [{ t: "command.err", clientCommandId, error, code }]);

    let target;
    try {
      target = this.#sessionLane.isSessionLane
        ? await this.#sessionLane.targetForFrame(frame)
        : normalizeSessionLookup(frame.target, frame.agentId)
          ?? sessionTargetFromRequest(this.#request);
    } catch (error) {
      return err(messageForError(error), "invalid_args");
    }
    if (!target) return err("command requires a target session", "invalid_args");
    const name = stringField(frame.name);
    if (!name) return err("command.name is required", "invalid_args");

    const harness = await this.#harnessFor(target);
    const descriptor = findCommand(harness, name);
    if (!descriptor) return err(`unknown command: ${name}`, "unknown_command");
    const invalid = validateCommandArgs(descriptor, frame.args);
    if (invalid) return err(invalid, "invalid_args");

    // Spec §13.5: ack = accepted-for-execution (validation passed), sent
    // BEFORE dispatch; done = dispatch resolved, ok mirroring its success.
    // err is terminal only for pre-acceptance failures above.
    const ack = { t: "command.ack", clientCommandId };
    sendJson(this.#socket, ack);
    let done;
    try {
      const response = descriptor.class === "gateway"
        ? await routeThroughFetchHandler(
            this.#deps,
            requestWithJson(
              this.#request,
              gatewayVerbPath(name),
              {
                ...sessionTargetRequestBody(target),
                clientMessageId: clientCommandId,
              },
            ),
          )
        : await this.#sessionInput(
            {
              mode: "session",
              target: sessionTargetWireValue(target),
              text: renderHarnessInvocation(descriptor, frame.args),
              clientMessageId: clientCommandId,
            },
            this.#request,
          );
      done = response.ok
        ? { t: "command.done", clientCommandId, ok: true }
        : {
            t: "command.done",
            clientCommandId,
            ok: false,
            error: await errorReason(response),
            code: commandCodeForStatus(response.status),
          };
    } catch (error) {
      done = {
        t: "command.done",
        clientCommandId,
        ok: false,
        error: messageForError(error),
        code: "upstream_error",
      };
    }
    this.#remember(clientCommandId, [ack, done]);
    sendJson(this.#socket, done);
  }

  async #harnessFor(target) {
    const resolve = this.#deps.resolveHarness ?? defaultResolveHarness(this.#deps);
    try {
      return await resolve(sessionTargetWireValue(target), this.#request);
    } catch {
      return undefined;
    }
  }

  #finish(clientCommandId, replies) {
    this.#remember(clientCommandId, replies);
    for (const reply of replies) sendJson(this.#socket, reply);
  }

  #remember(clientCommandId, replies) {
    this.#replay.delete(clientCommandId);
    if (this.#replay.size >= COMMAND_REPLAY_LIMIT) {
      const oldest = this.#replay.keys().next().value;
      if (oldest !== undefined) this.#replay.delete(oldest);
    }
    this.#replay.set(clientCommandId, replies);
  }
}

const PENDING_COMMAND = Symbol("pending command");

function defaultResolveHarness(deps) {
  return async (target, request) => {
    const lookup = normalizeSessionLookup(target);
    if (!lookup) return undefined;
    const url = new URL(request.url);
    url.pathname = "/api/v1/members";
    url.search = "?includeOffline=true";
    const response = await routeThroughFetchHandler(
      deps,
      new Request(url, { method: "GET", headers: request.headers }),
    );
    if (!response.ok) return undefined;
    const members = await response.json();
    if (!Array.isArray(members)) return undefined;
    const member = lookup.agentId
      ? members.find((row) => row?.agentId === lookup.agentId)
      : members.find((row) => row?.name === lookup.name);
    return typeof member?.agent === "string" ? member.agent : undefined;
  };
}

function commandCodeForStatus(status) {
  return status === 401 || status === 403 ? "unauthorized" : "upstream_error";
}

class DeveloperEventSubscriptions {
  #socket;
  #source;
  #topics = new Map();
  #sessionName;
  #toolCallRows = new Map();
  #toolCallNames = new Map();
  #toolCallSeq = new Map();
  #daemonToolCallSource;
  #daemonToolCallActiveTopics = new Set();
  #daemonFleetSource;
  #closed = false;
  #deps;

  constructor(socket, request, deps) {
    this.#socket = socket;
    this.#sessionName = sessionNameFromRequest(request);
    this.#deps = deps;
    this.#source = deps.developerEvents;
    this.#daemonToolCallSource = Object.prototype.hasOwnProperty.call(deps, "daemonToolCallEvents")
      ? deps.daemonToolCallEvents
      : (this.#sessionName ? createDaemonPushDeveloperEventSource(this.#sessionName) : undefined);
    this.#daemonFleetSource = Object.prototype.hasOwnProperty.call(deps, "daemonFleetStatusEvents")
      ? deps.daemonFleetStatusEvents
      : createDaemonPushDeveloperEventSource(FLEET_SESSION_KEY);
  }

  bindSession(name) {
    const canonicalName = stringField(name);
    if (!canonicalName) return;
    this.#sessionName = canonicalName;
    if (!Object.prototype.hasOwnProperty.call(this.#deps, "daemonToolCallEvents")) {
      this.#daemonToolCallSource = createDaemonPushDeveloperEventSource(canonicalName);
    }
  }

  subscribe(frame) {
    const topic = normalizeDeveloperTopic(frame.topic);
    if (!topic) {
      sendJson(this.#socket, { t: "subscribe.err", error: "subscribe.topic must be a sys.* topic" });
      return;
    }
    const afterSeq = nonNegativeInteger(frame.afterSeq);
    if (afterSeq === undefined) {
      sendJson(this.#socket, { t: "subscribe.err", topic, error: "subscribe.afterSeq must be a non-negative integer" });
      return;
    }

    const toolCallAgent = toolCallTopicAgent(topic);
    if (toolCallAgent) {
      if (!this.#sessionName || toolCallAgent !== this.#sessionName) {
        sendJson(this.#socket, {
          t: "subscribe.err",
          topic,
          error: `tool_call subscriptions require matching ?session=${toolCallAgent}`,
        });
        return;
      }
      this.#subscribeToolCalls(topic, afterSeq);
      return;
    }

    if (topic === FLEET_STATUS_TOPIC) {
      this.#subscribeFleetStatus(topic, afterSeq);
      return;
    }

    if (!this.#source || typeof this.#source.subscribe !== "function") {
      sendJson(this.#socket, {
        t: "subscribe.err",
        topic,
        error: "Gateway message event source is unavailable on this gateway build",
      });
      return;
    }
    this.#stopTopic(topic, false);
    const state = { topic, cursor: afterSeq, active: true, ephemeral: false };
    this.#topics.set(topic, state);
    sendJson(this.#socket, { t: "subscribe.ack", topic, afterSeq });
    try {
      const subscription = this.#source.subscribe(topic, afterSeq, {
        onEvent: (event) => {
          if (!state.active || this.#closed) return false;
          const seq = Number(event?.seq ?? 0);
          if (!Number.isInteger(seq) || seq <= state.cursor) return true;
          return this.#sendEvent(state, event);
        },
        onError: (error) => {
          if (!state.active || this.#closed) return;
          sendJson(this.#socket, {
            t: "subscribe.err",
            topic,
            error: messageForError(error),
          });
          this.#stopTopic(topic, false);
        },
      });
      state.daemonUnsubscribe = () => subscription.close();
    } catch (error) {
      this.#stopTopic(topic, false);
      sendJson(this.#socket, {
        t: "subscribe.err",
        topic,
        error: messageForError(error),
      });
    }
  }

  unsubscribe(frame) {
    const topic = normalizeDeveloperTopic(frame.topic);
    if (!topic) return;
    this.#stopTopic(topic, true);
  }

  #stopTopic(topic, acknowledge) {
    const state = this.#topics.get(topic);
    if (!state) return;
    state.active = false;
    state.daemonUnsubscribe?.();
    this.#topics.delete(topic);
    if (toolCallTopicAgent(topic)) this.#daemonToolCallActiveTopics.delete(topic);
    if (acknowledge) sendJson(this.#socket, { t: "unsubscribe.ack", topic });
  }

  close() {
    this.#closed = true;
    for (const state of this.#topics.values()) {
      state.active = false;
      state.daemonUnsubscribe?.();
    }
    this.#topics.clear();
    this.#daemonToolCallActiveTopics.clear();
  }

  observeAguiFrame(payload) {
    if (this.#closed || !this.#sessionName) return;
    const topic = toolCallTopic(this.#sessionName);
    if (this.#daemonToolCallActiveTopics.has(topic)) return;
    let frame;
    try {
      frame = JSON.parse(payload);
    } catch {
      return;
    }
    const event = this.#toolCallEventFromAgui(frame);
    if (!event) return;
    const rows = this.#rowsForTopic(event.topic);
    rows.push(event);
    if (rows.length > TOOL_CALL_RING_LIMIT) rows.splice(0, rows.length - TOOL_CALL_RING_LIMIT);
    const state = this.#topics.get(event.topic);
    if (!state?.active || state.cursor >= event.seq) return;
    this.#sendEvent(state, event);
  }

  #subscribeToolCalls(topic, afterSeq) {
    this.#stopTopic(topic, false);
    const state = {
      topic,
      cursor: afterSeq,
      active: true,
      ephemeral: true,
      daemonUnsubscribe: undefined,
    };
    this.#topics.set(topic, state);
    sendJson(this.#socket, { t: "subscribe.ack", topic, afterSeq });
    state.daemonUnsubscribe = this.#daemonToolCallSource?.subscribe(topic, afterSeq, {
      onEvent: (event) => {
        if (!state.active || this.#closed) return;
        this.#daemonToolCallActiveTopics.add(topic);
        const seq = Number(event.seq ?? 0);
        if (seq <= state.cursor) return;
        this.#sendEvent(state, event);
      },
      onGap: (frame) => {
        if (!state.active || this.#closed) return;
        const retry = frame && typeof frame === "object" ? frame.retry : undefined;
        const after = frame && typeof frame === "object" ? frame.afterId : undefined;
        sendJson(this.#socket, {
          t: "subscribe.gap",
          topic,
          afterSeq: Number.isInteger(after) ? after : state.cursor,
          retry: typeof retry === "string" ? retry : "ephemeral",
        });
      },
      onError: () => {
        // Keep the socket-local AG-UI derivation fallback alive for old/stale daemon push paths.
      },
    });
    for (const event of this.#rowsForTopic(topic)) {
      if (!state.active || this.#closed) return;
      if (Number(event.seq ?? 0) <= state.cursor) continue;
      if (!this.#sendEvent(state, event)) return;
    }
  }

  // Fleet status rides the daemon push socket only — there is no socket-local AG-UI
  // derivation fallback (unlike tool calls), because the events originate in the daemon,
  // not in this connection's own stream. The daemon terminates every replay with an ordered
  // lifecycle=resync event; forwarding it verbatim is the current-state reconciliation contract.
  #subscribeFleetStatus(topic, afterSeq) {
    if (!this.#daemonFleetSource) {
      sendJson(this.#socket, {
        t: "subscribe.err",
        topic,
        error: "fleet status push unavailable on this gateway",
      });
      return;
    }
    this.#stopTopic(topic, false);
    const state = {
      topic,
      cursor: afterSeq,
      active: true,
      ephemeral: true,
      daemonUnsubscribe: undefined,
    };
    this.#topics.set(topic, state);
    sendJson(this.#socket, { t: "subscribe.ack", topic, afterSeq });
    state.daemonUnsubscribe = this.#daemonFleetSource.subscribe(topic, afterSeq, {
      onEvent: (event) => {
        if (!state.active || this.#closed) return;
        const seq = Number(event.seq ?? 0);
        const isResync = event?.lifecycle === "resync";
        // Fleet seqs are daemon-boot scoped. The ordered resync boundary is deliberately allowed
        // to move an ahead-of-boot cursor backwards; otherwise every event in the new boot would
        // be suppressed forever by a cursor retained from the old one.
        if (!isResync && seq <= state.cursor) return;
        this.#sendEvent(state, event);
      },
      onGap: (frame) => {
        if (!state.active || this.#closed) return;
        const retry = frame && typeof frame === "object" ? frame.retry : undefined;
        const after = frame && typeof frame === "object" ? frame.afterId : undefined;
        sendJson(this.#socket, {
          t: "subscribe.gap",
          topic,
          afterSeq: Number.isInteger(after) ? after : state.cursor,
          retry: typeof retry === "string" ? retry : "ephemeral",
        });
      },
      onError: () => {
        // The daemon push connection reports errors per-subscription; the client's
        // reconnect loop re-subscribes with its cursor, so nothing to do here.
      },
    });
  }

  #sendEvent(state, event) {
    const payload = JSON.stringify({ type: "developer.event", event });
    const bufferedAmount = this.#socket.bufferedAmount ?? 0;
    if (bufferedAmount + Buffer.byteLength(payload, "utf8") > MAX_BUFFERED_AMOUNT) {
      this.#socket.close?.(
        1013,
        `${DEVELOPER_BACKPRESSURE_REASON}${state.cursor}`,
      );
      return false;
    }
    this.#socket.send(payload);
    state.cursor = Number(event.seq);
    return true;
  }

  #toolCallEventFromAgui(frame) {
    if (!frame || typeof frame !== "object") return undefined;
    const type = frame.type;
    if (type !== TOOL_CALL_START && type !== TOOL_CALL_RESULT) return undefined;
    const toolCallId = stringField(frame.toolCallId);
    if (!toolCallId) return undefined;
    const topic = toolCallTopic(this.#sessionName);
    let tool = stringField(frame.toolCallName) ?? stringField(frame.toolName) ?? stringField(frame.name);
    if (tool) {
      this.#rememberToolCallName(toolCallId, tool);
    } else {
      tool = this.#toolCallNames.get(toolCallId) ?? "tool";
    }

    let phase;
    let ok;
    let clearToolName = false;
    if (type === TOOL_CALL_START) {
      phase = "pre";
      ok = true;
    } else {
      const status = stringField(frame.status);
      if (isInProgressToolResult(frame, status)) return undefined;
      phase = "post";
      ok = !isFailedToolStatus(status);
      clearToolName = true;
    }

    const seq = (this.#toolCallSeq.get(topic) ?? 0) + 1;
    this.#toolCallSeq.set(topic, seq);
    const event = {
      kind: "tool_call",
      topic,
      seq,
      ts: Date.now(),
      agent: this.#sessionName,
      tool,
      phase,
      ok,
    };
    if (clearToolName) this.#toolCallNames.delete(toolCallId);
    return event;
  }

  #rememberToolCallName(toolCallId, tool) {
    if (this.#toolCallNames.has(toolCallId)) {
      this.#toolCallNames.delete(toolCallId);
    } else if (this.#toolCallNames.size >= TOOL_CALL_NAME_LIMIT) {
      const oldest = this.#toolCallNames.keys().next().value;
      if (oldest !== undefined) this.#toolCallNames.delete(oldest);
    }
    this.#toolCallNames.set(toolCallId, tool);
  }

  #rowsForTopic(topic) {
    let rows = this.#toolCallRows.get(topic);
    if (!rows) {
      rows = [];
      this.#toolCallRows.set(topic, rows);
    }
    return rows;
  }
}

function normalizeDeveloperTopic(value) {
  const topic = stringField(value);
  if (!topic || !topic.startsWith("sys.")) return undefined;
  return topic;
}

function sessionNameFromRequest(request) {
  const session = new URL(request.url).searchParams.get("session");
  return stringField(session);
}

function sessionTargetFromRequest(request) {
  const params = new URL(request.url).searchParams;
  return normalizeSessionLookup(
    params.get("session"),
    params.get("agentId"),
  );
}

class SessionLaneBinding {
  #request;
  #pathSessionId;
  #fallbackTarget;
  #resolve;
  #settled = false;
  #ready;

  constructor(request) {
    this.#request = request;
    this.#pathSessionId = sessionIdFromPath(request);
    this.#fallbackTarget = sessionLaneTargetFromRequest(request);
    this.isSessionLane = Boolean(this.#pathSessionId || this.#fallbackTarget);
    this.#ready = this.isSessionLane
      ? new Promise((resolve) => { this.#resolve = resolve; })
      : Promise.resolve({ target: undefined });
  }

  bindResponse(response) {
    if (!this.isSessionLane || this.#settled) return undefined;
    const responseSessionId = stringField(response.headers.get("x-nexus-session-id"));
    const responseTarget = normalizeSessionLookup(
      response.headers.get("x-nexus-agent-name"),
      response.headers.get("x-nexus-agent-id"),
    );
    if (this.#pathSessionId) {
      if (!responseSessionId || responseSessionId !== this.#pathSessionId || !responseTarget?.agentId) {
        const error = "agent-session response is missing its canonical lane binding";
        this.fail(error);
        throw new Error(error);
      }
    }
    const target = responseTarget ?? this.#fallbackTarget;
    if (!target) {
      const error = "agent-session lane target is not materialized";
      this.fail(error);
      throw new Error(error);
    }
    this.#settled = true;
    this.#resolve?.({
      target,
      sessionId: responseSessionId ?? this.#pathSessionId,
      canonical: Boolean(responseTarget),
    });
    return target;
  }

  fail(error) {
    if (!this.isSessionLane || this.#settled) return;
    this.#settled = true;
    this.#resolve?.({ error });
  }

  async targetForFrame(frame) {
    const state = await this.#ready;
    if (state.error) throw new Error(state.error);
    const canonical = state.target;
    if (!canonical) throw new Error("agent-session lane target is not materialized");
    const explicit = normalizeSessionLookup(frame?.target, frame?.agentId);
    if (!explicit) return canonical;
    if (canonical.agentId && explicit.agentId) {
      if (canonical.agentId !== explicit.agentId) {
        throw new Error("frame target does not match the bound agent-session lane");
      }
      return canonical;
    }
    if (explicit.agentId && !canonical.agentId) {
      if (!state.canonical && explicit.name === canonical.name) return explicit;
      throw new Error("frame stable agent id cannot be verified on this session lane");
    }
    if (explicit.name && explicit.name !== canonical.name) {
      throw new Error("frame target does not match the bound agent-session lane");
    }
    return canonical;
  }
}

function sessionLaneTargetFromRequest(request) {
  const params = new URL(request.url).searchParams;
  const session = stringField(params.get("session"));
  const agentId = stringField(params.get("agentId"));
  if (session) return normalizeSessionLookup(session, agentId);
  if (!agentId || params.get("thread") || params.get("dm") || params.get("topic")) return undefined;
  return { agentId };
}

function sessionIdFromPath(request) {
  const match = /^\/api\/v1\/agent-sessions\/([^/]+)\/events$/.exec(new URL(request.url).pathname);
  if (!match) return undefined;
  try {
    return stringField(decodeURIComponent(match[1]));
  } catch {
    return undefined;
  }
}

function normalizeSessionLookup(value, explicitAgentId) {
  const name = typeof value === "string"
    ? stringField(value)
    : stringField(value?.name);
  const agentId = stringField(explicitAgentId) ?? stringField(value?.agentId);
  if (!name && !agentId) return undefined;
  return {
    ...(name ? { name } : {}),
    ...(agentId ? { agentId } : {}),
  };
}

function sessionTargetLabel(target) {
  return target.name ?? target.agentId;
}

function sessionTargetWireValue(target) {
  return target.agentId ? target : target.name;
}

function sessionTargetRequestBody(target) {
  return {
    ...(target.name ? { name: target.name } : {}),
    ...(target.agentId ? { agentId: target.agentId } : {}),
  };
}

function sessionTargetKey(target) {
  return target.agentId ? `agent:${target.agentId}` : `name:${target.name}`;
}

function toolCallTopic(agent) {
  return `${TOOL_CALL_TOPIC_PREFIX}${agent}${TOOL_CALL_TOPIC_SUFFIX}`;
}

function toolCallTopicAgent(topic) {
  if (!topic.startsWith(TOOL_CALL_TOPIC_PREFIX) || !topic.endsWith(TOOL_CALL_TOPIC_SUFFIX)) {
    return undefined;
  }
  const agent = topic.slice(TOOL_CALL_TOPIC_PREFIX.length, -TOOL_CALL_TOPIC_SUFFIX.length);
  return stringField(agent);
}

function isInProgressToolResult(frame, status) {
  return frame.append === true || status === "in_progress" || status === "running";
}

function isFailedToolStatus(status) {
  return status === "failed" || status === "error" || status === "cancelled" || status === "canceled";
}

function nonNegativeInteger(value) {
  if (value === undefined || value === null) return 0;
  const number = typeof value === "string" && value.trim() ? Number(value) : value;
  if (!Number.isInteger(number) || number < 0) return undefined;
  return number;
}

// Pre-command-surface WS clients (the Lens nexus plugin's observe socket) send bare
// `{t:"input", text, agentId?}` frames with no mode/target — the socket's own query
// (?session= / ?thread= / ?dm= / ?topic=) already carries the intent. Default those
// legacy frames from the connection scope instead of rejecting them.
function withSocketScopedInputDefaults(frame, request) {
  if (frame.mode) return frame;
  const params = new URL(request.url).searchParams;
  const session = params.get("session");
  const queryAgentId = stringField(params.get("agentId"));
  const thread = params.get("thread");
  const dm = params.get("dm");
  const topic = params.get("topic");
  const idOnlySession = queryAgentId && !session && !thread && !dm && !topic;
  if (session || idOnlySession) {
    if (frame.target) return { ...frame, mode: "session" };
    const agentId = stringField(frame.agentId) ?? queryAgentId;
    return {
      ...frame,
      mode: "session",
      target: agentId
        ? { ...(session ? { name: session } : {}), agentId }
        : session,
    };
  }
  if (frame.target) return { ...frame, mode: "bus" };
  if (thread) return { ...frame, mode: "bus", target: { verb: "post", thread } };
  if (queryAgentId) {
    return {
      ...frame,
      mode: "bus",
      target: { verb: "dm", agentId: queryAgentId, ...(dm ? { name: dm } : {}) },
    };
  }
  if (dm) return { ...frame, mode: "bus", target: { verb: "dm", name: dm } };
  if (topic) return { ...frame, mode: "bus", target: { verb: "publish", topic } };
  return frame;
}

// Steering is an explicit Agent Session operation, never a Message Post send. A session-scoped
// socket supplies the same target default as legacy input frames while keeping normal input on
// its existing prompt/bus routing paths.
function withSocketScopedSteerDefaults(frame, request) {
  if (frame.target) return frame;
  const params = new URL(request.url).searchParams;
  const session = params.get("session");
  const agentId = stringField(frame.agentId) ?? stringField(params.get("agentId"));
  if (!session && !agentId) return frame;
  return {
    ...frame,
    target: agentId
      ? { ...(session ? { name: session } : {}), agentId }
      : session,
  };
}

function normalizeSteerFrame(frame) {
  const text = stringField(frame.text);
  if (!text) throw new Error("steer.text is required");
  return {
    target: normalizeSessionTarget(frame.target),
    text,
    clientMessageId: stringField(frame.clientMessageId),
  };
}

function normalizeInputFrame(frame) {
  const mode = frame.mode === "bus" ? "bus" : frame.mode === "session" ? "session" : undefined;
  const text = stringField(frame.text);
  if (!mode) throw new Error("input.mode must be session or bus");
  if (!text) throw new Error("input.text is required");
  if (mode === "session") {
    const target = normalizeSessionTarget(frame.target);
    return {
      mode,
      target,
      text,
      clientMessageId: stringField(frame.clientMessageId),
    };
  }
  return {
    mode,
    target: normalizeBusTarget(frame.target),
    text,
    clientMessageId: stringField(frame.clientMessageId),
  };
}

function normalizeSessionTarget(target) {
  if (typeof target === "string" && target.trim()) return target.trim();
  if (target && typeof target === "object") {
    const name = stringField(target.name);
    const agentId = stringField(target.agentId);
    if (name || agentId) {
      return {
        ...(name ? { name } : {}),
        ...(agentId ? { agentId } : {}),
      };
    }
  }
  throw new Error("session input target must be a name or {name,agentId}");
}

function normalizeBusTarget(target) {
  if (typeof target === "string" && target.trim()) {
    return { verb: "post", thread: target.trim() };
  }
  if (!target || typeof target !== "object") {
    throw new Error("bus input target must be a SendTarget");
  }
  switch (target.verb) {
    case "post": {
      const thread = stringField(target.thread);
      if (!thread) break;
      return { verb: "post", thread };
    }
    case "dm": {
      const name = stringField(target.name);
      const agentId = stringField(target.agentId);
      if (!name && !agentId) break;
      return {
        verb: "dm",
        ...(name ? { name } : {}),
        ...(agentId ? { agentId } : {}),
      };
    }
    case "publish": {
      const topic = stringField(target.topic);
      if (!topic) break;
      return { verb: "publish", topic };
    }
    case "reply":
      return { verb: "reply" };
    default:
      break;
  }
  throw new Error("bus input target must be a valid SendTarget");
}

function defaultSessionInput(deps) {
  return async (input, request) => {
    const target = typeof input.target === "string" ? { name: input.target } : input.target;
    return routeThroughFetchHandler(
      deps,
      requestWithJson(request, "/api/conversation/prompt", {
        name: target.name ?? target.agentId,
        ...(target.agentId ? { agentId: target.agentId } : {}),
        text: input.text,
        ...(input.clientMessageId ? { clientMessageId: input.clientMessageId } : {}),
      }),
    );
  };
}

function defaultBusInput(deps) {
  return async (input, request) => {
    return routeThroughFetchHandler(
      deps,
      requestWithJson(
        request,
        "/api/v1/messages",
        {
          to: input.target,
          body: input.text,
          ...(input.clientMessageId ? { idempotencyKey: input.clientMessageId } : {}),
        },
        input.clientMessageId,
      ),
    );
  };
}

function defaultSteerInput(deps) {
  return async (input, request) => {
    const target = typeof input.target === "string" ? { name: input.target } : input.target;
    return routeThroughFetchHandler(
      deps,
      requestWithJson(request, "/api/conversation/steer", {
        name: target.name ?? target.agentId,
        ...(target.agentId ? { agentId: target.agentId } : {}),
        text: input.text,
        ...(input.clientMessageId ? { clientMessageId: input.clientMessageId } : {}),
      }),
    );
  };
}

function defaultInterruptInput(deps) {
  return async (input, request) => {
    const target = typeof input.target === "string" ? { name: input.target } : input.target;
    return routeThroughFetchHandler(
      deps,
      requestWithJson(request, "/api/conversation/interrupt", {
        name: target.name ?? target.agentId,
        ...(target.agentId ? { agentId: target.agentId } : {}),
        clientMessageId: input.clientMessageId,
      }),
    );
  };
}

function requestWithJson(parent, path, body, idempotencyKey, method = "POST") {
  const url = new URL(parent.url);
  url.pathname = path;
  url.search = "";
  const headers = new Headers(parent.headers);
  headers.set("content-type", "application/json");
  if (idempotencyKey) headers.set("idempotency-key", idempotencyKey);
  return new Request(url, {
    method,
    headers,
    body: JSON.stringify(body),
  });
}

function nodeUpgradeRequestToFetch(req) {
  const host = req.headers.host ?? "127.0.0.1";
  const url = new URL(req.url ?? "/", `http://${host}`);
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const entry of value) headers.append(key, entry);
    } else if (value !== undefined) {
      headers.set(key, value);
    }
  }
  return new Request(url, { method: "GET", headers });
}

function routeThroughFetchHandler(deps, request) {
  if (typeof deps.fetchHandler !== "function") {
    throw new Error("AG-UI websocket fetch handler is not configured");
  }
  return deps.fetchHandler(request);
}

function sendJson(socket, body) {
  const payload = JSON.stringify(body);
  const outbound = SESSION_OUTBOUND.get(socket);
  if (outbound?.session) {
    if (outbound.stopped) return false;
    const payloadBytes = Buffer.byteLength(payload, "utf8");
    if (
      payloadBytes > MAX_SESSION_CONTROL_FRAME_BYTES
      || (socket.bufferedAmount ?? 0) > MAX_BUFFERED_AMOUNT
    ) {
      closeOutboundBackpressure(outbound);
      return false;
    }
  }
  socket.send(payload);
  return true;
}

async function errorReason(response) {
  try {
    const body = await response.clone().json();
    const error = body?.error;
    if (typeof error === "string") return error;
    if (error && typeof error.message === "string") return error.message;
  } catch {
    // Fall through to status text.
  }
  return response.statusText || `HTTP ${response.status}`;
}

function closeCodeForStatus(status) {
  if (status === 401) return 4401;
  if (status === 403) return 4403;
  return 1011;
}

function messageForError(error) {
  return error instanceof Error ? error.message : String(error);
}

function stringField(value) {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}
