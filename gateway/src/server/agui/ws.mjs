import { createDaemonPushDeveloperEventSource } from "./daemonPushRelay.mjs";
import {
  catalogForHarness,
  findCommand,
  gatewayVerbPath,
  renderHarnessInvocation,
  validateCommandArgs,
} from "./commandRegistry.mjs";

const AGUI_WS_PATH = "/api/agui/ws";
const SESSION_EVENTS_PATH = /^\/api\/v1\/agent-sessions\/[^/]+\/events$/;
const MAX_BUFFERED_AMOUNT = 1024 * 1024;
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

export function handleWs(socket, request, deps = {}) {
  const observe = deps.observe ?? ((req) => routeThroughFetchHandler(deps, req));
  const sessionInput = deps.sessionInput ?? defaultSessionInput(deps);
  const busInput = deps.busInput ?? defaultBusInput(deps);
  const steerInput = deps.steerInput ?? defaultSteerInput(deps);
  const commands = new CommandFrames(socket, request, deps, sessionInput);
  const commandQueue = new SessionCommandQueue(socket, request, deps);
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
      subscriptions,
      commands,
      commandQueue,
    });
  });
  commandQueue.start();

  void (async () => {
    // Dedicated events lane: a socket with no
    // observe target is legal — it carries only subscribe/command frames, no content
    // stream. Without this, a bare socket died 1011 "missing target".
    if (!hasObserveTarget(request)) return;
    try {
      const response = await observe(toObserveRequest(request));
      if (!response.ok) {
        close(closeCodeForStatus(response.status), await errorReason(response));
        return;
      }
      await pumpSseResponseToSocket(response, socket, abort.signal, subscriptions);
      close(1000, "observe ended");
    } catch (error) {
      if (!abort.signal.aborted) close(1011, messageForError(error));
    }
  })();

  return { closed, close };
}

function hasObserveTarget(request) {
  const url = new URL(request.url);
  if (SESSION_EVENTS_PATH.test(url.pathname)) return true;
  const params = url.searchParams;
  return ["session", "thread", "dm", "topic"].some((key) => params.get(key));
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
    const request = nodeUpgradeRequestToFetch(req);
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

export async function pumpSseResponseToSocket(response, socket, signal, observer) {
  if (!response.body) return;
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
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
    pending = flushSseBlocks(pending, socket, observer);
  }
  pending += decoder.decode();
  flushSseBlocks(`${pending}\n\n`, socket, observer);
}

function flushSseBlocks(text, socket, observer) {
  let start = 0;
  for (;;) {
    const next = text.indexOf("\n\n", start);
    if (next === -1) break;
    emitSseBlock(text.slice(start, next), socket, observer);
    start = next + 2;
  }
  return text.slice(start);
}

function emitSseBlock(block, socket, observer) {
  const data = [];
  for (const line of block.split(/\r?\n/)) {
    if (!line.startsWith("data:")) continue;
    data.push(line.slice(5).trimStart());
  }
  if (data.length === 0) return;
  if ((socket.bufferedAmount ?? 0) > MAX_BUFFERED_AMOUNT) {
    socket.close?.(1013, "ag-ui websocket backpressure");
    return;
  }
  const payload = data.join("\n");
  socket.send(payload);
  observer?.observeAguiFrame?.(payload);
}

async function handleClientFrame(
  raw,
  { socket, request, sessionInput, busInput, steerInput, subscriptions, commands, commandQueue },
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
      const steer = normalizeSteerFrame(withSocketScopedSteerDefaults(frame, request));
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
  if (frame?.t !== "input") {
    sendJson(socket, { t: "input.err", error: `unsupported frame: ${String(frame?.t)}` });
    return;
  }

  const clientMessageId = stringField(frame.clientMessageId);
  try {
    const input = normalizeInputFrame(withSocketScopedInputDefaults(frame, request));
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

// One lane's subscription/mutation facade. All event reads live in CommandQueueHub below.
class SessionCommandQueue {
  #socket;
  #request;
  #deps;
  #target;
  #unsubscribe;

  constructor(socket, request, deps) {
    this.#socket = socket;
    this.#request = request;
    this.#deps = deps;
    this.#target = sessionNameFromRequest(request);
  }

  start() {
    if (!this.#target || !this.#deps.commandQueueHub) return;
    this.#unsubscribe = this.#deps.commandQueueHub.subscribe(
      this.#request,
      this.#target,
      {
        onSnapshot: (snapshot) => sendJson(this.#socket, { t: "queue.snapshot", ...snapshot }),
        onTransition: (transition) => {
          sendJson(this.#socket, { t: "command.transition", ...transition });
        },
        onError: (error) => sendJson(this.#socket, { t: "queue.err", error }),
      },
    );
  }

  close() {
    this.#unsubscribe?.();
    this.#unsubscribe = undefined;
  }

  observeReceipt(_receipt) {}

  async mutate(frame) {
    if (!this.#target) {
      sendJson(this.#socket, { t: "queue.mutation.err", error: "queue operations require a session socket" });
      return;
    }
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
      const response = await routeThroughFetchHandler(
        this.#deps,
        requestWithJson(this.#request, "/api/conversation/prompt", {
          name: this.#target,
          ...(stringField(frame.agentId) ? { agentId: stringField(frame.agentId) } : {}),
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
  #pendingRefreshes = new Map();
  #timer;
  #polling = false;
  #pollMs;

  constructor(deps) {
    this.#deps = deps;
    this.#pollMs = positiveInteger(deps.commandQueueEventPollMs)
      ?? DEFAULT_COMMAND_QUEUE_EVENT_POLL_MS;
  }

  subscribe(request, target, handlers) {
    const authKey = request.headers.get("cookie") ?? "local";
    let group = this.#groups.get(authKey);
    if (!group) {
      group = { request, cursor: 0, ready: false };
      this.#groups.set(authKey, group);
    }
    const subscriber = {
      request,
      target,
      handlers,
      authKey,
      sessionId: undefined,
      cursor: 0,
      ready: false,
      buffered: [],
      closed: false,
    };
    this.#subscribers.add(subscriber);
    void this.#hydrate(subscriber, group);
    return () => {
      subscriber.closed = true;
      this.#subscribers.delete(subscriber);
      if (![...this.#subscribers].some((entry) => entry.authKey === authKey)) {
        this.#groups.delete(authKey);
      }
      if (this.#subscribers.size === 0 && this.#timer) {
        clearTimeout(this.#timer);
        this.#timer = undefined;
      }
      if (this.#subscribers.size === 0) this.#pendingRefreshes.clear();
    };
  }

  async #hydrate(subscriber, group) {
    try {
      const snapshot = await this.#loadSnapshot(subscriber.request, subscriber.target);
      if (subscriber.closed) return;
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
      if (!group.ready) {
        group.cursor = snapshotSeq;
        group.ready = true;
      }
      this.#schedule(0);
    } catch (error) {
      if (!subscriber.closed) subscriber.handlers.onError(messageForError(error));
    }
  }

  async #loadSnapshot(request, target) {
    const url = new URL(request.url);
    url.pathname = "/api/conversation/prompt";
    url.search = "";
    url.searchParams.set("name", target);
    const response = await routeThroughFetchHandler(
      this.#deps,
      new Request(url, { method: "GET", headers: request.headers }),
    );
    if (!response.ok) throw new Error(await errorReason(response));
    return response.json();
  }

  async #refreshSubscribers(subscribers) {
    const active = [...subscribers].filter((subscriber) => !subscriber.closed);
    if (active.length === 0) return true;
    try {
      // All entries share auth + target. Fetch once, then fan the authoritative projection out to
      // every mounted client. This is event-driven reconciliation, not another polling lane.
      const snapshot = await this.#loadSnapshot(active[0].request, active[0].target);
      const sessionId = stringField(snapshot?.sessionId);
      for (const subscriber of active) {
        if (subscriber.closed) continue;
        subscriber.sessionId = sessionId;
        subscriber.handlers.onSnapshot(snapshot);
      }
      return true;
    } catch (error) {
      for (const subscriber of active) {
        if (!subscriber.closed) subscriber.handlers.onError(messageForError(error));
      }
      return false;
    }
  }

  #schedule(delay) {
    if (this.#timer || this.#polling || this.#subscribers.size === 0) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.#poll();
    }, delay);
  }

  async #poll() {
    if (this.#polling) return;
    this.#polling = true;
    let caughtUp = true;
    try {
      for (const [authKey, group] of this.#groups) {
        if (!group.ready) continue;
        const url = new URL(group.request.url);
        url.pathname = "/api/conversation/prompt";
        url.search = "";
        url.searchParams.set("eventsAfter", String(group.cursor));
        const response = await routeThroughFetchHandler(
          this.#deps,
          new Request(url, { method: "GET", headers: group.request.headers }),
        );
        if (!response.ok) throw new Error(await errorReason(response));
        const body = await response.json();
        if (body?.gap === true) {
          group.ready = false;
          const affected = [...this.#subscribers].filter((entry) => entry.authKey === authKey);
          for (const subscriber of affected) {
            subscriber.ready = false;
            subscriber.buffered = [];
            void this.#hydrate(subscriber, group);
          }
          continue;
        }
        const events = Array.isArray(body?.events) ? body.events : [];
        for (const event of events) {
          const seq = nonNegativeInteger(event?.seq);
          if (seq === undefined || seq <= group.cursor) continue;
          group.cursor = seq;
          for (const subscriber of this.#subscribers) {
            if (subscriber.authKey !== authKey || subscriber.closed) continue;
            if (stringField(event?.sessionId) !== subscriber.sessionId) continue;
            if (seq <= subscriber.cursor) continue;
            if (subscriber.ready) {
              subscriber.cursor = seq;
              subscriber.handlers.onTransition(event);
              // A queued transition can mean new, edited, reordered, or redirected content. The
              // transition remains the lifecycle fact; one coalesced snapshot supplies text/order
              // that intentionally do not bloat every event row.
              if (commandQueueState(event?.state) === "queued") {
                const key = `${subscriber.authKey}\u0000${subscriber.target}`;
                let entries = this.#pendingRefreshes.get(key);
                if (!entries) {
                  entries = new Set();
                  this.#pendingRefreshes.set(key, entries);
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
      }
      for (const [key, subscribers] of this.#pendingRefreshes) {
        if (await this.#refreshSubscribers(subscribers)) {
          this.#pendingRefreshes.delete(key);
        }
      }
    } catch (error) {
      for (const subscriber of this.#subscribers) {
        if (!subscriber.closed) subscriber.handlers.onError(messageForError(error));
      }
    } finally {
      this.#polling = false;
      this.#schedule(caughtUp ? this.#pollMs : 0);
    }
  }
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
  #replay = new Map();

  constructor(socket, request, deps, sessionInput) {
    this.#socket = socket;
    this.#request = request;
    this.#deps = deps;
    this.#sessionInput = sessionInput;
  }

  async list(frame) {
    const target = stringField(frame.target) ?? sessionNameFromRequest(this.#request);
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
    sendJson(this.#socket, { t: "commands.catalog", target, ...catalogForHarness(harness) });
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

    const target = stringField(frame.target) ?? sessionNameFromRequest(this.#request);
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
            requestWithJson(this.#request, gatewayVerbPath(name), { name: target }),
          )
        : await this.#sessionInput(
            {
              mode: "session",
              target,
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
      return await resolve(target, this.#request);
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
    const member = members.find((row) => row?.name === target);
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

  constructor(socket, request, deps) {
    this.#socket = socket;
    this.#sessionName = sessionNameFromRequest(request);
    this.#source = deps.developerEvents;
    this.#daemonToolCallSource = Object.prototype.hasOwnProperty.call(deps, "daemonToolCallEvents")
      ? deps.daemonToolCallEvents
      : (this.#sessionName ? createDaemonPushDeveloperEventSource(this.#sessionName) : undefined);
    this.#daemonFleetSource = Object.prototype.hasOwnProperty.call(deps, "daemonFleetStatusEvents")
      ? deps.daemonFleetStatusEvents
      : createDaemonPushDeveloperEventSource(FLEET_SESSION_KEY);
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
          if ((this.#socket.bufferedAmount ?? 0) > MAX_BUFFERED_AMOUNT) {
            this.#socket.close?.(1013, "developer event websocket backpressure");
            return false;
          }
          sendJson(this.#socket, { type: "developer.event", event });
          state.cursor = seq;
          return true;
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
    sendJson(this.#socket, { type: "developer.event", event });
    state.cursor = event.seq;
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
        sendJson(this.#socket, { type: "developer.event", event });
        state.cursor = seq;
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
      sendJson(this.#socket, { type: "developer.event", event });
      state.cursor = event.seq;
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
        sendJson(this.#socket, { type: "developer.event", event });
        state.cursor = seq;
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
  if (session) {
    if (frame.target) return { ...frame, mode: "session" };
    const agentId = stringField(frame.agentId);
    return {
      ...frame,
      mode: "session",
      target: agentId ? { name: session, agentId } : session,
    };
  }
  if (frame.target) return { ...frame, mode: "bus" };
  const thread = params.get("thread");
  if (thread) return { ...frame, mode: "bus", target: { verb: "post", thread } };
  const dm = params.get("dm");
  const targetAgentId = params.get("agentId");
  if (targetAgentId) {
    return {
      ...frame,
      mode: "bus",
      target: { verb: "dm", agentId: targetAgentId, ...(dm ? { name: dm } : {}) },
    };
  }
  if (dm) return { ...frame, mode: "bus", target: { verb: "dm", name: dm } };
  const topic = params.get("topic");
  if (topic) return { ...frame, mode: "bus", target: { verb: "publish", topic } };
  return frame;
}

// Steering is an explicit Agent Session operation, never a Message Post send. A session-scoped
// socket supplies the same target default as legacy input frames while keeping normal input on
// its existing prompt/bus routing paths.
function withSocketScopedSteerDefaults(frame, request) {
  if (frame.target) return frame;
  const session = new URL(request.url).searchParams.get("session");
  if (!session) return frame;
  const agentId = stringField(frame.agentId);
  return {
    ...frame,
    target: agentId ? { name: session, agentId } : session,
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
    if (name || agentId) return { name, agentId };
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
  socket.send(JSON.stringify(body));
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
