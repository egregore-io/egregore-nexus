import { randomBytes } from "node:crypto";

const guard = globalThis.__nexusOpenCodePlugin ??= {};

function log(message) {
  process.stderr.write(`[nexus-opencode-plugin] ${message}\n`);
}

export const nexus = async () => {
  if (guard.hooks) return guard.hooks;

  const bridgeUrl = process.env.NEXUS_OPENCODE_BRIDGE_URL?.trim();
  const bridgeToken = process.env.NEXUS_OPENCODE_BRIDGE_TOKEN?.trim();
  const serverUrl = process.env.NEXUS_OPENCODE_SERVER_URL?.trim();
  const serverUser = process.env.OPENCODE_SERVER_USERNAME?.trim() || "opencode";
  const serverPassword = process.env.OPENCODE_SERVER_PASSWORD?.trim();
  const name = process.env.NEXUS_NAME?.trim() || process.env.NEXUS_AGENT_ID?.trim();
  const project = process.env.NEXUS_PROJECT?.trim() || "default";
  const explicitSession = process.env.NEXUS_OPENCODE_SESSION_ID?.trim();
  const configuredPromptModel = process.env.NEXUS_OPENCODE_PROMPT_MODEL?.trim();
  const configuredPromptAgent = process.env.NEXUS_OPENCODE_PROMPT_AGENT?.trim();

  if (!bridgeUrl || !bridgeToken || !serverUrl || !serverPassword || !name) {
    log("missing Nexus/OpenCode identity; plugin inert");
    return {};
  }

  const serverAuth = `Basic ${Buffer.from(`${serverUser}:${serverPassword}`).toString("base64")}`;
  let sessionID;
  let busy = false;
  let nativeBusy = false;
  let activeTurn;
  let deferredTurn;
  let pollStarted = false;
  const roles = new Map();
  const partText = new Map();
  const canonicalInputs = new Map();
  let messageClock = 0n;
  let turnEndEmitted = false;
  let promptContext;
  let telemetrySequence = 0;
  let telemetryTail = Promise.resolve();
  let telemetryPending = 0;
  let telemetryBytes = 0;
  let telemetryStopped = false;
  let telemetryRoot;

  async function bridge(path, init = {}, timeoutMs = 30_000) {
    const res = await fetch(`${bridgeUrl}${path}`, {
      ...init,
      signal: init.signal ?? AbortSignal.timeout(timeoutMs),
      headers: {
        authorization: `Bearer ${bridgeToken}`,
        "content-type": "application/json",
        ...(init.headers ?? {}),
      },
    });
    if (res.status === 204) return undefined;
    if (!res.ok) throw new Error(`Nexus bridge HTTP ${res.status} ${res.statusText} for ${path}`);
    return await res.json();
  }

  async function opencode(path, init = {}, timeoutMs = 30_000) {
    const res = await fetch(`${serverUrl}${path}`, {
      ...init,
      signal: init.signal ?? AbortSignal.timeout(timeoutMs),
      headers: {
        authorization: serverAuth,
        "content-type": "application/json",
        ...(init.headers ?? {}),
      },
    });
    if (res.status === 204) return undefined;
    if (!res.ok) throw new Error(`OpenCode HTTP ${res.status} ${res.statusText} for ${path}`);
    return await res.json();
  }

  function parseModelRef(value) {
    if (!value || !value.includes("/")) return undefined;
    const [providerID, ...rest] = value.split("/");
    const modelID = rest.join("/");
    if (!providerID || !modelID) return undefined;
    return { providerID, modelID };
  }

  function firstProviderModel(provider) {
    const models = provider?.models ?? {};
    const first = Object.values(models)[0];
    if (typeof first === "string") return first;
    if (first?.id) return first.id;
    return Object.keys(models)[0];
  }

  async function resolvePromptContext() {
    if (promptContext) return promptContext;
    promptContext = (async () => {
      let cfg = {};
      try {
        cfg = await opencode("/config", {}, 10_000) ?? {};
      } catch (error) {
        log(`config lookup failed: ${error.message}`);
      }
      let model = parseModelRef(configuredPromptModel) ?? parseModelRef(cfg.model);
      if (!model) {
        try {
          const result = await opencode("/config/providers", {}, 10_000);
          const providers = Array.isArray(result?.providers) ? result.providers : [];
          for (const provider of providers) {
            const providerID = provider?.id;
            if (!providerID) continue;
            const modelID = result?.default?.[providerID] ?? firstProviderModel(provider);
            if (modelID) {
              model = { providerID, modelID };
              break;
            }
          }
        } catch (error) {
          log(`provider lookup failed: ${error.message}`);
        }
      }
      return {
        agent: configuredPromptAgent || cfg.default_agent || "build",
        model,
      };
    })();
    return promptContext;
  }

  async function emit(kind, data = {}) {
    try {
      await bridge("/event", { method: "POST", body: JSON.stringify({ kind, data }) }, 10_000);
    } catch (error) {
      log(`event bridge failed: ${error.message}`);
    }
  }

  async function emitTurnEnd() {
    if (turnEndEmitted) return;
    turnEndEmitted = true;
    await emit("turn_end", {});
  }

  function ours(id) {
    return Boolean(id && telemetryRoot && id === telemetryRoot);
  }

  const sessionReady = (async () => {
    if (explicitSession) {
      sessionID = explicitSession;
      telemetryRoot = explicitSession;
      process.stderr.write(`[nexus-opencode-session] ${sessionID}\n`);
      return sessionID;
    }
    try {
      const res = await opencode("/session", {
        method: "POST",
        body: JSON.stringify({ title: `nexus:${project}:${name}` }),
      }, 10_000);
      if (res?.id) {
        sessionID = res.id;
        telemetryRoot = res.id;
        process.stderr.write(`[nexus-opencode-session] ${sessionID}\n`);
      }
    } catch (error) {
      log(`session create failed: ${error.message}`);
    }
    return sessionID;
  })();

  async function stopTelemetry() {
    if (telemetryStopped) return;
    telemetryStopped = true;
    const root = await sessionReady;
    if (!root) return;
    try { await bridge("/model", {method:"POST", body:JSON.stringify({info:{sessionID:root}, telemetry:{unavailable:true}})}, 1_000); }
    catch (error) { log(`telemetry owner unavailable: ${error.message}`); }
  }

  // Preserve callback order across capacity reads, including user rows/removals that determine
  // the native 100-row window. Bounds fail closed; this is not a second prompt/execution queue.
  function queueTelemetry(original, sequence, removed = false) {
    if (telemetryStopped || !telemetryRoot || original.sessionID !== telemetryRoot) return Promise.resolve();
    let raw;
    try {
      raw = JSON.stringify({sessionID:original.sessionID,id:original.id,role:original.role,
        providerID:original.providerID,modelID:original.modelID,finish:original.finish,tokens:original.tokens});
    } catch { return stopTelemetry(); }
    const bytes = Buffer.byteLength(raw);
    if (telemetryPending >= 256 || bytes > 1_048_576 - telemetryBytes) return stopTelemetry();
    const info = JSON.parse(raw);
    telemetryPending++;
    telemetryBytes += bytes;
    const pending = telemetryTail.then(async () => {
      const root = await sessionReady;
      if (telemetryStopped || !root || info.sessionID !== root) return;
      let contextCapacity = null;
      if (!removed && info.role === "assistant" && info.tokens?.output > 0 && typeof info.providerID === "string" && typeof info.modelID === "string") {
        try {
          const result = await opencode("/config/providers", {}, 1_000);
          const provider = result?.providers?.find((provider) => provider?.id === info.providerID);
          contextCapacity = provider?.models?.[info.modelID]?.limit?.context ?? null;
        } catch (error) { log(`context capacity unavailable: ${error.message}`); }
      }
      if (telemetryStopped) return;
      await bridge("/model", {method:"POST", body:JSON.stringify({info,telemetry:{sequence,contextCapacity,removed}})}, 1_000);
    }).catch(async (error) => {
      log(`telemetry unavailable: ${error.message}`);
      await stopTelemetry();
    }).finally(() => {telemetryPending--; telemetryBytes -= bytes;});
    telemetryTail = pending;
    return pending;
  }

  async function ensureSession() {
    return await sessionReady;
  }

  function nextMessageId() {
    // OpenCode 1.17.17 accepts client-supplied IDs. Preserve its chronological
    // 48-bit timestamp/counter prefix and 14-character random suffix shape.
    const now = BigInt(Date.now()) * 4096n;
    messageClock = now > messageClock ? now : messageClock + 1n;
    return `msg_${messageClock.toString(16).padStart(12, "0").slice(-12)}${randomBytes(7).toString("hex")}`;
  }

  function observeUserReceipt(info) {
    const turn = activeTurn;
    if (turn?.phase !== "submitted" || !turn.nativeBinding || info.role !== "user" ||
        info.sessionID !== turn.nativeBinding.sessionID || info.id !== turn.nativeBinding.messageID) return;
    if (!turn.receipt) {
      // message.updated is post-persistence; chat.message is deliberately not used.
      turn.receipt = bridge(`/turn/${encodeURIComponent(turn.id)}/accepted`, {
        method: "POST", body: JSON.stringify(turn.nativeBinding),
      }, 10_000).then(result => result?.canonicalEcho === true);
      canonicalInputs.set(info.id, turn.receipt);
      while (canonicalInputs.size > 1024) canonicalInputs.delete(canonicalInputs.keys().next().value);
      // Retain rejection for exact later consumers without an unhandled-promise warning.
      void turn.receipt.catch(error => log(`native input receipt unavailable: ${error.message}`));
    }
    return turn.receipt;
  }

  async function completeActiveTurn() {
    if (!busy && !activeTurn) return;
    const turn = activeTurn;
    if (turn && turn.phase !== "submitted") {
      // Local activity may end while our claimed command is still resolving
      // configuration/binding. That lifecycle does not belong to the command.
      busy = nativeBusy;
      await emitTurnEnd();
      await submitPreparedTurn(turn);
      return;
    }
    // A delayed idle from prior local work is not this prompt's completion.
    // Post-persistence identity is the first eligible native turn evidence.
    if (turn && !turn.receipt) return;
    if (turn?.receipt) {
      try { await turn.receipt; }
      catch (error) { await failActiveTurn(error, undefined, turn); return; }
    }
    if (activeTurn !== turn) return;
    activeTurn = undefined;
    busy = false;
    await emitTurnEnd();
    if (turn) {
      try {
        await bridge(`/turn/${encodeURIComponent(turn.id)}/complete`, { method: "POST", body: "{}" }, 10_000);
      } catch (error) {
        log(`turn complete failed: ${error.message}`);
      }
    }
    queueMicrotask(() => void pollTurns());
  }

  async function failActiveTurn(error, providerError, turn = activeTurn) {
    if (activeTurn !== turn) return;
    activeTurn = undefined;
    // A failed command request does not prove native activity has stopped.
    busy = nativeBusy;
    if (turn) {
      try {
        const payload = { error: error.message || String(error) };
        if (providerError) payload.providerError = providerError;
        await bridge(`/turn/${encodeURIComponent(turn.id)}/error`, {
          method: "POST",
          body: JSON.stringify(payload),
        }, 10_000);
      } catch (bridgeError) {
        log(`turn error report failed: ${bridgeError.message}`);
      }
    }
    setTimeout(() => void pollTurns(), 1_000).unref?.();
  }

  async function drive(turn) {
    if (busy || nativeBusy || activeTurn) {
      // A bridge long-poll can already be outstanding when OpenCode begins a local/setup turn.
      // The bridge has transferred ownership of this turn to the plugin, so returning here would
      // strand the daemon's completion waiter forever. Retain it and submit it after the native
      // session reports idle.
      deferredTurn = turn;
      return;
    }
    const captured = {...turn, phase: "setup"};
    activeTurn = captured;
    busy = true;
    turnEndEmitted = false;
    try {
      const id = await ensureSession();
      if (activeTurn !== captured) return;
      if (!id) throw new Error("OpenCode session is not ready");
      const prompt = await resolvePromptContext();
      if (activeTurn !== captured) return;
      const nativeBinding = {sessionID: id, messageID: nextMessageId()};
      captured.nativeBinding = nativeBinding;
      const body = {
        messageID: nativeBinding.messageID,
        agent: prompt.agent,
        parts: [{ type: "text", text: turn.text }],
      };
      if (prompt.model) body.model = prompt.model;
      await bridge(`/turn/${encodeURIComponent(turn.id)}/bind`, {
        method: "POST", body: JSON.stringify(nativeBinding),
      }, 10_000);
      if (activeTurn !== captured) return;
      captured.body = body;
      captured.phase = "ready";
      await submitPreparedTurn(captured);
    } catch (error) {
      // Never retry a possibly submitted native prompt under a fresh identity.
      await failActiveTurn(error, undefined, captured);
    }
  }

  async function submitPreparedTurn(turn) {
    if (activeTurn !== turn || turn.phase !== "ready" || nativeBusy) return;
    // Mark before the request so concurrent idle notifications cannot submit
    // the same immutable prepared prompt twice. No post-attempt retry exists.
    turn.phase = "submitted";
    busy = true;
    turnEndEmitted = false;
    try {
      await opencode(`/session/${encodeURIComponent(turn.nativeBinding.sessionID)}/prompt_async`, {
        method: "POST", body: JSON.stringify(turn.body),
      }, 10_000);
    } catch (error) {
      await failActiveTurn(error, undefined, turn);
    }
  }

  async function pollTurns() {
    if (pollStarted || busy || nativeBusy || activeTurn) return;
    pollStarted = true;
    try {
      while (!busy && !nativeBusy && !activeTurn) {
        const turn = deferredTurn ?? await bridge("/turn/next", {}, 35_000);
        if (!turn) continue;
        if (turn === deferredTurn) deferredTurn = undefined;
        await drive(turn);
      }
    } catch (error) {
      if (!busy && !nativeBusy && !activeTurn) setTimeout(() => void pollTurns(), 1_000).unref?.();
    } finally {
      pollStarted = false;
    }
  }

  function textDelta(part) {
    const key = `${part.messageID ?? part.messageId ?? ""}:${part.id ?? ""}`;
    const text = typeof part.text === "string" ? part.text : typeof part.content === "string" ? part.content : "";
    const prior = partText.get(key) ?? "";
    partText.set(key, text);
    return text.startsWith(prior) ? text.slice(prior.length) : text;
  }

  async function observePart(part) {
    if (!ours(part.sessionID ?? part.sessionId)) return;
    // A revoked/uncertain command receipt does not revoke this root's native
    // display. Preserve ordering when available, then continue actual output.
    if (activeTurn?.receipt) {
      try { await activeTurn.receipt; }
      catch { /* No canonical admission claim and no native input replay. */ }
    }
    const type = part.type;
    if (type === "text" || type === "reasoning") {
      const messageId = part.messageID ?? part.messageId;
      const role = roles.get(messageId);
      if (role !== "user" && role !== "assistant") return;
      const delta = textDelta(part);
      if (!delta) return;
      if (role === "user") {
        try { if (await canonicalInputs.get(messageId)) return; }
        catch {
          // The canonical echo may already have happened before its HTTP
          // response was lost. Do not fabricate a second input representation.
          return;
        }
        await emit("user_input", { text: delta, clientMessageId: messageId });
      } else {
        await emit(type === "reasoning" ? "thinking" : "text", {
          text: delta, itemId: part.id ?? messageId, nativeMessageId: messageId,
        });
      }
      return;
    }
    if (type === "tool") {
      // C-TOOL v1 (docs/tool-call-contract.md): `tool` = the registered machine name,
      // `input` = the structured args value; keys are omitted when absent, never null-padded.
      const toolName = String(part.tool ?? part.name ?? "tool");
      const payload = {
        id: String(part.callID ?? part.toolCallID ?? part.toolCallId ?? part.id ?? "tool"),
        tool: toolName,
        title: toolName,
        status: (part.state?.status ?? part.status) === "error"
          ? "failed" : (part.state?.status ?? part.status ?? "in_progress"),
      };
      const input = part.input ?? part.arguments ?? part.state?.input;
      if (input != null) payload.input = input;
      const output = part.output ?? part.result ?? part.state?.output;
      const error = part.state?.error;
      if (payload.status === "failed" && error != null) payload.content = error;
      else if (output != null) payload.content = output;
      await emit("tool_call", payload);
      return;
    }
    if (type === "step-finish") {
      await emitTurnEnd();
    }
  }

  function eventErrorMessage(properties) {
    const error = properties?.error;
    if (!error) return properties?.message ?? "OpenCode session error";
    return error.data?.message ?? error.message ?? error.name ?? JSON.stringify(error);
  }

  function firstPresent(...values) {
    for (const value of values) {
      if (value !== undefined && value !== null && value !== "") return value;
    }
    return undefined;
  }

  function assignIfPresent(target, key, ...values) {
    const value = firstPresent(...values);
    if (value !== undefined) target[key] = value;
  }

  function eventProviderError(properties) {
    const error = properties?.error ?? {};
    const data = error.data ?? properties?.data ?? {};
    const nested = data.providerError ?? data.provider_error ?? properties?.providerError ?? properties?.provider_error ?? {};
    const providerError = {};
    assignIfPresent(providerError, "reason", nested.reason, data.reason, error.reason);
    assignIfPresent(providerError, "code", nested.code, data.code, error.code);
    assignIfPresent(providerError, "type", nested.type, data.type, error.type);
    assignIfPresent(providerError, "errorCode", nested.errorCode, nested.error_code, data.errorCode, data.error_code, error.errorCode, error.error_code);
    assignIfPresent(providerError, "providerErrorCode", nested.providerErrorCode, nested.provider_error_code, data.providerErrorCode, data.provider_error_code);
    assignIfPresent(providerError, "status", nested.status, data.status, error.status);
    assignIfPresent(providerError, "statusCode", nested.statusCode, nested.status_code, data.statusCode, data.status_code, error.statusCode, error.status_code);
    assignIfPresent(providerError, "httpStatusCode", nested.httpStatusCode, nested.http_status_code, data.httpStatusCode, data.http_status_code, error.httpStatusCode, error.http_status_code);
    assignIfPresent(providerError, "retryAfterMs", nested.retryAfterMs, nested.retry_after_ms, data.retryAfterMs, data.retry_after_ms);
    assignIfPresent(providerError, "retryAfter", nested.retryAfter, nested.retry_after, nested["retry-after"], data.retryAfter, data.retry_after, data["retry-after"]);
    assignIfPresent(providerError, "resetAt", nested.resetAt, nested.reset_at, data.resetAt, data.reset_at);
    assignIfPresent(providerError, "resetsAt", nested.resetsAt, nested.resets_at, data.resetsAt, data.resets_at);
    assignIfPresent(providerError, "provider", nested.provider, data.provider, error.provider);
    assignIfPresent(providerError, "model", nested.model, data.model, error.model);
    return Object.keys(providerError).length ? providerError : undefined;
  }

  const hooks = {
    "chat.message": async (input) => {
      await sessionReady;
      if (!ours(input.sessionID)) return;
      busy = true;
      nativeBusy = true;
      turnEndEmitted = false;
    },
    event: async ({ event }) => {
      switch (event.type) {
        case "session.created":
          if (!event.properties?.info?.parentID && event.properties?.info?.id) sessionID = event.properties.info.id;
          break;
        case "message.updated": {
          const info = event.properties?.info ?? {};
          if (ours(info.sessionID) && info.id && info.role) roles.set(info.id, info.role);
          const receipt = ours(info.sessionID) ? observeUserReceipt(info) : undefined;
          // Capture before any asynchronous lookup; same-ID older responses cannot overwrite newer.
          const sequence = ++telemetrySequence;
          const telemetry = info.role === "assistant" || info.role === "user"
            ? queueTelemetry(info, sequence) : Promise.resolve();
          // Forward original native metadata, never requested prompt configuration. The bridge's
          // immutable ready-handshake root owns attribution independently of mutable display state.
          if (info.role === "assistant") {
            try { await bridge("/model", { method: "POST", body: JSON.stringify(info) }); }
            catch (error) { log(`model metadata unavailable: ${error}`); }
          }
          await telemetry;
          if (receipt) await receipt;
          break;
        }
        case "message.removed": {
          const sequence = ++telemetrySequence;
          const info = {sessionID: event.properties?.sessionID, id: event.properties?.messageID};
          await queueTelemetry(info, sequence, true);
          break;
        }
        case "message.part.updated":
          await observePart(event.properties?.part ?? {});
          break;
        case "session.status":
          if (!ours(event.properties?.sessionID)) return;
          if (event.properties?.status?.type === "busy") {
            busy = true;
            nativeBusy = true;
          } else if (event.properties?.status?.type === "idle") {
            nativeBusy = false;
            if (activeTurn) await completeActiveTurn();
            else {
              busy = false;
              queueMicrotask(() => void pollTurns());
            }
          }
          break;
        case "session.idle":
          if (!ours(event.properties?.sessionID)) return;
          nativeBusy = false;
          await completeActiveTurn();
          break;
        case "session.error":
          if (event.properties?.sessionID && !ours(event.properties.sessionID)) return;
          nativeBusy = false;
          if (activeTurn && activeTurn.phase !== "submitted") {
            busy = false;
            await submitPreparedTurn(activeTurn);
            break;
          }
          await failActiveTurn(
            new Error(eventErrorMessage(event.properties)),
            eventProviderError(event.properties),
          );
          break;
      }
    },
    "tool.execute.before": async (input) => {
      if (!ours(input.sessionID)) return;
      // Tool parts carry their native identity, inputs and lifecycle. This hook
      // is not model reasoning and must not fabricate an assistant text row.
    },
    dispose: async () => {
      await stopTelemetry();
      busy = false;
      nativeBusy = false;
      activeTurn = undefined;
      deferredTurn = undefined;
    },
  };

  guard.hooks = hooks;
  void pollTurns();
  log(`ready for ${project}:${name}`);
  return hooks;
};
