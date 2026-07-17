//! Loopback bridge for headed OpenCode's native plugin.
//!
//! Headed OpenCode is not driven by tmux keystrokes. The daemon binds this bridge as the
//! [`HarnessInput`](nexus_pty::HarnessInput) for the OpenCode session, while an in-process
//! `@opencode-ai/plugin` polls the loopback endpoint for pending turns, submits them through
//! OpenCode's native `prompt_async`, and reports completion on `session.idle`. Because
//! [`send_turn`](OpenCodePluginInput::send_turn) only returns after that completion callback, the
//! existing realtime drain loop keeps its crash-safe "ack after turn finished" semantics. The plugin
//! sends explicit OpenCode prompt agent/model metadata on each turn so resumed sessions do not fall
//! back to a stale model from their prior history. It also treats OpenCode's generic idle status as
//! a completion/poll-resume signal so a stale native `busy` flag cannot leave Nexus turns queued.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use nexus_contracts::events::{AgentUpdateKind, WsEvent};
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_pty::{HarnessInput, TurnCompletionEvidence};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Notify};
use uuid::Uuid;

pub(crate) const OPENCODE_PROVIDER_ERROR_PREFIX: &str = "__nexus_opencode_provider_error__:";

/// Runtime knobs for the OpenCode plugin bridge.
#[derive(Debug, Clone)]
pub struct OpenCodePluginBridgeOptions {
    /// Maximum time the daemon waits for the plugin to report turn completion. Production launches
    /// use a long timeout; tests tighten it so a broken completion path fails quickly.
    pub turn_timeout: Duration,
}

impl Default for OpenCodePluginBridgeOptions {
    fn default() -> Self {
        Self {
            turn_timeout: Duration::from_secs(20 * 60),
        }
    }
}

/// The loopback endpoint and bearer token injected into the OpenCode plugin process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodePluginEndpoint {
    base_url: String,
    token: String,
}

/// Runtime files generated for a headed OpenCode launch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodePluginFiles {
    /// Native plugin loaded by `opencode serve` through `OPENCODE_CONFIG_CONTENT`.
    pub plugin_path: PathBuf,
    /// Node launcher that starts `opencode serve` and attaches the foreground TUI to the owned
    /// session.
    pub serve_path: PathBuf,
    /// Per-Nexus-session OpenCode data root. Fresh launches point `opencode serve` at an isolated
    /// DB here so parallel agents do not fight over a SQLite lock; explicit `--session` resumes
    /// leave OpenCode on its native store so the existing `ses_*` can be found.
    pub data_dir: PathBuf,
    /// Handshake file written by the serve shim once `opencode serve` has created/adopted the
    /// plugin-owned session and the foreground `opencode attach` TUI can be opened.
    pub ready_path: PathBuf,
}

impl OpenCodePluginEndpoint {
    /// Base URL for the loopback bridge, for example `http://127.0.0.1:43123`.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Bearer token required on every plugin request.
    pub fn token(&self) -> &str {
        &self.token
    }
}

/// Live bridge handle. Dropping it shuts down the loopback server and marks the bound input dead.
pub struct OpenCodePluginBridge {
    endpoint: OpenCodePluginEndpoint,
    input: Arc<OpenCodePluginInput>,
    state: Arc<BridgeState>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

impl OpenCodePluginBridge {
    /// Start a token-authenticated loopback bridge for one headed OpenCode Nexus session.
    pub async fn start(
        session_id: SessionId,
        events: Arc<dyn EventSink>,
        options: OpenCodePluginBridgeOptions,
    ) -> Result<Self, OpenCodePluginBridgeError> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let endpoint = OpenCodePluginEndpoint {
            base_url: format!("http://{addr}"),
            token: Uuid::new_v4().to_string(),
        };
        let state = Arc::new(BridgeState {
            session_id,
            token: endpoint.token.clone(),
            events,
            turn_timeout: options.turn_timeout,
            queue: Mutex::new(VecDeque::new()),
            active: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            notify: Notify::new(),
            alive: AtomicBool::new(true),
        });
        let app = Router::new()
            .route("/turn/next", get(next_turn))
            .route("/turn/:id/complete", post(complete_turn))
            .route("/turn/:id/error", post(error_turn))
            .route("/event", post(plugin_event))
            .with_state(state.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                tracing::warn!(target: "nexus::opencode_plugin_bridge", error = %err, "bridge server exited with error");
            }
        });
        let input = Arc::new(OpenCodePluginInput {
            state: state.clone(),
        });
        Ok(Self {
            endpoint,
            input,
            state,
            shutdown: Mutex::new(Some(shutdown_tx)),
        })
    }

    /// Loopback address and bearer token that the native plugin must use.
    pub fn endpoint(&self) -> &OpenCodePluginEndpoint {
        &self.endpoint
    }

    /// Bound transport input. The daemon registers this under the session's [`PtyTransport`].
    pub fn input(&self) -> Arc<OpenCodePluginInput> {
        self.input.clone()
    }

    /// Stop the loopback server. Best-effort; also wakes any long-polling plugin request.
    pub fn shutdown(&self) {
        self.state.alive.store(false, Ordering::SeqCst);
        self.state.notify.notify_waiters();
        if let Some(tx) = self.shutdown.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for OpenCodePluginBridge {
    fn drop(&mut self) {
        self.state.alive.store(false, Ordering::SeqCst);
        self.state.notify.notify_waiters();
        if let Some(tx) = self.shutdown.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

/// Write the launch-local native plugin and serve shim for one OpenCode runtime.
///
/// The generated JavaScript is intentionally self-contained: no npm package is needed from Nexus.
/// OpenCode loads the plugin through its normal `plugin` config field, and the shim uses only Node
/// built-ins plus the installed `opencode` binary. The shim isolates fresh session storage, but
/// does not set `OPENCODE_DB` for operator-supplied explicit `-s/--session` resumes so native
/// OpenCode can resolve the requested session from its normal store. Daemon restart revives pass
/// `NEXUS_OPENCODE_RESUME_ISOLATED=1` so the stored native session id is resolved in the same
/// launch-local DB instead.
pub fn write_opencode_plugin_files(
    state_dir: &Path,
    session: &SessionId,
) -> Result<OpenCodePluginFiles, std::io::Error> {
    let root = state_dir.join("opencode-plugin-sessions").join(&session.0);
    let data_dir = root.join("opencode-home");
    std::fs::create_dir_all(&data_dir)?;
    let plugin_path = root.join("nexus-opencode-plugin.mjs");
    let serve_path = root.join("nexus-opencode-serve.mjs");
    let ready_path = root.join("ready.json");
    std::fs::write(&plugin_path, OPENCODE_PLUGIN_JS)?;
    std::fs::write(&serve_path, OPENCODE_SERVE_JS)?;
    Ok(OpenCodePluginFiles {
        plugin_path,
        serve_path,
        data_dir,
        ready_path,
    })
}

/// `HarnessInput` implementation that waits for the OpenCode plugin to finish each turn.
pub struct OpenCodePluginInput {
    state: Arc<BridgeState>,
}

#[async_trait]
impl HarnessInput for OpenCodePluginInput {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        if !self.is_alive() {
            return Err("opencode plugin bridge is not alive".to_string());
        }
        let turn_id = self
            .state
            .next_id
            .fetch_add(1, Ordering::SeqCst)
            .to_string();
        let (tx, rx) = oneshot::channel();
        {
            let mut queue = self.state.queue.lock().unwrap();
            queue.push_back(QueuedTurn {
                id: turn_id,
                text: text.to_string(),
                completion: tx,
            });
        }
        self.state.notify.notify_one();
        match tokio::time::timeout(self.state.turn_timeout, rx).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_closed)) => Err("opencode plugin bridge completion channel closed".to_string()),
            Err(_elapsed) => Err("opencode plugin turn timed out".to_string()),
        }
    }

    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        TurnCompletionEvidence::Terminal
    }

    fn is_alive(&self) -> bool {
        self.state.alive.load(Ordering::SeqCst)
    }
}

struct BridgeState {
    session_id: SessionId,
    token: String,
    events: Arc<dyn EventSink>,
    turn_timeout: Duration,
    queue: Mutex<VecDeque<QueuedTurn>>,
    active: Mutex<HashMap<String, oneshot::Sender<Result<(), String>>>>,
    next_id: AtomicU64,
    notify: Notify,
    alive: AtomicBool,
}

struct QueuedTurn {
    id: String,
    text: String,
    completion: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PluginTurn {
    id: String,
    text: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PluginTurnError {
    error: Option<String>,
    provider_error: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct PluginEvent {
    kind: AgentUpdateKind,
    #[serde(default)]
    data: Value,
}

/// Errors returned when the loopback server cannot start.
#[derive(Debug, Error)]
pub enum OpenCodePluginBridgeError {
    #[error("opencode plugin bridge io error: {0}")]
    Io(#[from] std::io::Error),
}

async fn next_turn(State(state): State<Arc<BridgeState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let timeout = tokio::time::sleep(Duration::from_secs(25));
    tokio::pin!(timeout);
    loop {
        if !state.alive.load(Ordering::SeqCst) {
            return StatusCode::NO_CONTENT.into_response();
        }
        let turn = {
            let mut queue = state.queue.lock().unwrap();
            queue.pop_front()
        };
        if let Some(turn) = turn {
            state
                .active
                .lock()
                .unwrap()
                .insert(turn.id.clone(), turn.completion);
            return Json(PluginTurn {
                id: turn.id,
                text: turn.text,
            })
            .into_response();
        }
        tokio::select! {
            _ = state.notify.notified() => {}
            _ = &mut timeout => return StatusCode::NO_CONTENT.into_response(),
        }
    }
}

async fn complete_turn(
    State(state): State<Arc<BridgeState>>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Some(tx) = state.active.lock().unwrap().remove(&id) {
        let _ = tx.send(Ok(()));
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown turn" })),
        )
            .into_response()
    }
}

async fn error_turn(
    State(state): State<Arc<BridgeState>>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Json(body): Json<PluginTurnError>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Some(tx) = state.active.lock().unwrap().remove(&id) {
        let message = plugin_turn_error_message(body);
        let _ = tx.send(Err(message));
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "unknown turn" })),
        )
            .into_response()
    }
}

fn plugin_turn_error_message(body: PluginTurnError) -> String {
    let message = body
        .error
        .unwrap_or_else(|| "opencode plugin turn failed".to_string());
    if let Some(provider_error) = body.provider_error.filter(|value| !value.is_null()) {
        return format!(
            "{OPENCODE_PROVIDER_ERROR_PREFIX}{}",
            json!({
                "error": message,
                "providerError": provider_error,
            })
        );
    }
    message
}

async fn plugin_event(
    State(state): State<Arc<BridgeState>>,
    headers: HeaderMap,
    Json(body): Json<PluginEvent>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    state
        .events
        .emit(WsEvent::AgentUpdate {
            session_id: state.session_id.clone(),
            kind: body.kind,
            data: body.data,
        })
        .await;
    StatusCode::NO_CONTENT.into_response()
}

fn authorized(state: &BridgeState, headers: &HeaderMap) -> bool {
    bearer_token(headers)
        .or_else(|| header_value(headers, "x-nexus-token"))
        .is_some_and(|token| token == state.token)
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = header_value(headers, "authorization")?;
    value.strip_prefix("Bearer ")
}

fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

const OPENCODE_PLUGIN_JS: &str = r##"
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
  let activeTurn;
  let deferredTurn;
  let pollStarted = false;
  const roles = new Map();
  const partText = new Map();
  let turnEndEmitted = false;
  let promptContext;

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
    if (!id) return true;
    if (!sessionID) sessionID = id;
    return id === sessionID;
  }

  const sessionReady = (async () => {
    if (explicitSession) {
      sessionID = explicitSession;
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
        process.stderr.write(`[nexus-opencode-session] ${sessionID}\n`);
      }
    } catch (error) {
      log(`session create failed: ${error.message}`);
    }
    return sessionID;
  })();

  async function ensureSession() {
    return sessionID ?? await sessionReady;
  }

  async function completeActiveTurn() {
    if (!busy && !activeTurn) return;
    const turn = activeTurn;
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

  async function failActiveTurn(error, providerError) {
    const turn = activeTurn;
    activeTurn = undefined;
    busy = false;
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
    if (busy || activeTurn) {
      // A bridge long-poll can already be outstanding when OpenCode begins a local/setup turn.
      // The bridge has transferred ownership of this turn to the plugin, so returning here would
      // strand the daemon's completion waiter forever. Retain it and submit it after the native
      // session reports idle.
      deferredTurn = turn;
      return;
    }
    const id = await ensureSession();
    if (!id) throw new Error("OpenCode session is not ready");
    const prompt = await resolvePromptContext();
    activeTurn = turn;
    busy = true;
    turnEndEmitted = false;
    const body = {
      agent: prompt.agent,
      parts: [{ type: "text", text: turn.text }],
    };
    if (prompt.model) body.model = prompt.model;
    await opencode(`/session/${encodeURIComponent(id)}/prompt_async`, {
      method: "POST",
      body: JSON.stringify(body),
    }, 10_000);
  }

  async function pollTurns() {
    if (pollStarted || busy || activeTurn) return;
    pollStarted = true;
    try {
      while (!busy && !activeTurn) {
        const turn = deferredTurn ?? await bridge("/turn/next", {}, 35_000);
        if (!turn) continue;
        if (turn === deferredTurn) deferredTurn = undefined;
        await drive(turn);
      }
    } catch (error) {
      if (!busy && !activeTurn) setTimeout(() => void pollTurns(), 1_000).unref?.();
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
    const type = part.type;
    if (type === "text" || type === "reasoning") {
      const delta = textDelta(part);
      if (!delta) return;
      const role = roles.get(part.messageID ?? part.messageId);
      if (role === "user") await emit("user_input", { text: delta });
      else await emit(type === "reasoning" ? "thinking" : "text", { text: delta });
      return;
    }
    if (type === "tool") {
      // C-TOOL v1 (docs/tool-call-contract.md): `tool` = the registered machine name,
      // `input` = the structured args value; keys are omitted when absent, never null-padded.
      const toolName = String(part.tool ?? part.name ?? "tool");
      const payload = {
        id: String(part.id ?? part.toolCallID ?? part.toolCallId ?? "tool"),
        tool: toolName,
        title: toolName,
        status: part.state?.status ?? part.status ?? "in_progress",
      };
      const input = part.input ?? part.arguments ?? part.state?.input;
      if (input != null) payload.input = input;
      const output = part.output ?? part.result ?? part.state?.output;
      if (output != null) payload.content = output;
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
      if (!ours(input.sessionID)) return;
      busy = true;
      turnEndEmitted = false;
    },
    event: async ({ event }) => {
      switch (event.type) {
        case "session.created":
          if (!event.properties?.info?.parentID && event.properties?.info?.id) sessionID = event.properties.info.id;
          break;
        case "message.updated": {
          const info = event.properties?.info ?? {};
          if (info.id && info.role) roles.set(info.id, info.role);
          break;
        }
        case "message.part.updated":
          await observePart(event.properties?.part ?? {});
          break;
        case "session.status":
          if (!ours(event.properties?.sessionID)) return;
          if (event.properties?.status?.type === "busy") {
            busy = true;
          } else if (event.properties?.status?.type === "idle") {
            if (activeTurn) await completeActiveTurn();
            else {
              busy = false;
              queueMicrotask(() => void pollTurns());
            }
          }
          break;
        case "session.idle":
          if (!ours(event.properties?.sessionID)) return;
          await completeActiveTurn();
          break;
        case "session.error":
          if (event.properties?.sessionID && !ours(event.properties.sessionID)) return;
          await failActiveTurn(
            new Error(eventErrorMessage(event.properties)),
            eventProviderError(event.properties),
          );
          break;
      }
    },
    "tool.execute.before": async (input) => {
      if (!ours(input.sessionID)) return;
      await emit("thinking", { text: `Running tool: ${input.tool}` });
    },
    dispose: async () => {
      busy = false;
      activeTurn = undefined;
      deferredTurn = undefined;
    },
  };

  guard.hooks = hooks;
  void pollTurns();
  log(`ready for ${project}:${name}`);
  return hooks;
};
"##;

const OPENCODE_SERVE_JS: &str = r##"
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { once } from "node:events";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { delimiter, join } from "node:path";

function resolveOpencodeBin() {
  const override = process.env.NEXUS_OPENCODE_BIN?.trim();
  if (override) return override;
  const platform = { linux: "linux", darwin: "darwin", win32: "windows" }[process.platform];
  const arch = { x64: "x64", arm64: "arm64" }[process.arch];
  if (!platform || !arch) return process.platform === "win32" ? "opencode.exe" : "opencode";
  const binary = platform === "windows" ? "opencode.exe" : "opencode";
  const packageBase = `opencode-${platform}-${arch}`;
  const provider = packageBase.slice(0, packageBase.indexOf("-"));
  const providerPackage = `${provider}-ai`;
  const stagedBinary = `${provider}.exe`;
  const packages = [packageBase, `${packageBase}-baseline`];
  for (const dir of (process.env.PATH ?? "").split(delimiter)) {
    if (!dir) continue;
    const moduleRoots = [join(dir, "node_modules"), join(dir, "..", "lib", "node_modules")];
    const candidates = [join(dir, binary)];
    for (const modules of moduleRoots) {
      candidates.push(join(modules, providerPackage, "bin", stagedBinary));
      for (const packageName of packages) {
        candidates.push(join(modules, packageName, "bin", binary));
        candidates.push(join(modules, "opencode-ai", "node_modules", packageName, "bin", binary));
      }
    }
    for (const exe of candidates) {
      if (existsSync(exe)) return exe;
    }
  }
  return binary;
}

async function freePort() {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function killServe(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  const dead = await Promise.race([
    once(child, "exit").then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), 3000)),
  ]);
  if (!dead) {
    child.kill("SIGKILL");
    await once(child, "exit");
  }
}

function splitNativeArgs(args) {
  const out = { model: undefined, agent: undefined, session: undefined, attachArgs: [] };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    const next = args[i + 1];
    if ((arg === "--model" || arg === "-m") && next) {
      out.model = next;
      i += 1;
      continue;
    }
    if (arg.startsWith("--model=")) {
      out.model = arg.slice("--model=".length);
      continue;
    }
    if (arg === "--agent" && next) {
      out.agent = next;
      i += 1;
      continue;
    }
    if (arg.startsWith("--agent=")) {
      out.agent = arg.slice("--agent=".length);
      continue;
    }
    if ((arg === "--session" || arg === "-s") && next) {
      out.session = next;
      i += 1;
      continue;
    }
    if (arg.startsWith("--session=")) {
      out.session = arg.slice("--session=".length);
      continue;
    }
    // The attached TUI is only a viewer for the plugin-owned session. Keep UI-native flags that
    // attach supports; do not pass session/model/prompt flags that apply to a normal launch and
    // would either duplicate `--session` or be rejected by the attach subcommand.
    if (arg === "--mini" || arg === "--no-replay") {
      out.attachArgs.push(arg);
      continue;
    }
    if (arg === "--replay-limit" && next) {
      out.attachArgs.push(arg, next);
      i += 1;
      continue;
    }
    if (arg.startsWith("--replay-limit=")) {
      out.attachArgs.push(arg);
    }
  }
  return out;
}

async function main() {
  const nativeArgs = process.argv.slice(2);
  const native = splitNativeArgs(nativeArgs);
  const pluginPath = process.env.NEXUS_OPENCODE_PLUGIN_PATH?.trim();
  const dataRoot = process.env.NEXUS_OPENCODE_HOME?.trim();
  const readyPath = process.env.NEXUS_OPENCODE_READY_PATH?.trim();
  if (!pluginPath) throw new Error("NEXUS_OPENCODE_PLUGIN_PATH is required");
  if (!dataRoot) throw new Error("NEXUS_OPENCODE_HOME is required");

  const bin = resolveOpencodeBin();
  const port = String(await freePort());
  const url = `http://127.0.0.1:${port}`;
  const secret = randomBytes(24).toString("hex");
  const username = "opencode";
  mkdirSync(dataRoot, { recursive: true });
  const resumeIsolated = process.env.NEXUS_OPENCODE_RESUME_ISOLATED === "1";
  const isolatedStore = !native.session || resumeIsolated;
  const dbPath = join(dataRoot, "opencode.db");
  const pidFile = join(dataRoot, "serve.pid");
  const baseEnv = { ...process.env };
  if (!isolatedStore) delete baseEnv.OPENCODE_DB;

  if (existsSync(pidFile)) {
    const pid = Number(readFileSync(pidFile, "utf8"));
    try {
      process.kill(pid, 0);
      throw new Error(`opencode serve already running for this Nexus session (pid ${pid})`);
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
      rmSync(pidFile, { force: true });
    }
  }

  const config = {
    $schema: "https://opencode.ai/config.json",
    permission: "allow",
    plugin: [pluginPath],
  };
  if (native.model) {
    config.model = native.model;
    config.agent = { nexus: { mode: "primary", model: native.model } };
    config.default_agent = "nexus";
  }
  if (native.agent) config.default_agent = native.agent;

  const serve = spawn(bin, ["serve", "--hostname", "127.0.0.1", "--port", port], {
    env: {
      ...baseEnv,
      NEXUS_OPENCODE_SERVER_URL: url,
      NEXUS_OPENCODE_SESSION_ID: native.session ?? "",
      OPENCODE_SERVER_USERNAME: username,
      OPENCODE_SERVER_PASSWORD: secret,
      NEXUS_OPENCODE_PROMPT_MODEL: native.model ?? "",
      NEXUS_OPENCODE_PROMPT_AGENT: native.agent ?? (native.model ? "nexus" : ""),
      ...(isolatedStore ? { OPENCODE_DB: dbPath } : {}),
      OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  writeFileSync(pidFile, String(serve.pid));
  serve.on("exit", () => rmSync(pidFile, { force: true }));

  let sessionId;
  let attached = false;
  let onSession;
  const scan = (data) => {
    if (!attached) process.stderr.write(data);
    if (!sessionId) {
      const match = data.toString().match(/\[nexus-opencode-session\] (\S+)/);
      if (match) {
        sessionId = match[1];
        onSession?.(sessionId);
      }
    }
  };
  serve.stdout?.on("data", scan);
  serve.stderr?.on("data", scan);
  serve.on("exit", (code, signal) => {
    if (!attached) process.exit(code ?? (signal ? 1 : 0));
  });

  const auth = `Basic ${Buffer.from(`${username}:${secret}`).toString("base64")}`;
  void (async () => {
    for (let i = 0; i < 300 && !sessionId; i += 1) {
      try {
        await fetch(`${url}/session`, { headers: { authorization: auth }, signal: AbortSignal.timeout(1500) });
      } catch {
        // Serve not ready yet, or an early request hung during boot.
      }
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  })();

  const id = await new Promise((resolve) => {
    if (sessionId) return resolve(sessionId);
    onSession = resolve;
    setTimeout(() => resolve(sessionId), 60_000);
  });
  if (!id) {
    process.stderr.write(`[nexus-opencode-serve] session never came up; aborting\n`);
    await killServe(serve);
    process.exit(1);
  }
  const tuiEnv = {
    ...baseEnv,
    OPENCODE_SERVER_USERNAME: username,
    OPENCODE_SERVER_PASSWORD: secret,
    ...(isolatedStore ? { OPENCODE_DB: dbPath } : {}),
  };
  delete tuiEnv.OPENCODE_CONFIG_CONTENT;
  delete tuiEnv.NEXUS_OPENCODE_BRIDGE_URL;
  delete tuiEnv.NEXUS_OPENCODE_BRIDGE_TOKEN;
  delete tuiEnv.NEXUS_OPENCODE_PLUGIN_PATH;
  delete tuiEnv.NEXUS_OPENCODE_SERVER_URL;

  attached = true;
  const tui = spawn(bin, ["attach", url, "--session", id, "--password", secret, ...native.attachArgs], {
    env: tuiEnv,
    stdio: "inherit",
  });

  for (const sig of ["SIGINT", "SIGTERM"]) {
    process.on(sig, () => {
      tui.kill(sig);
      serve.kill(sig);
    });
  }
  tui.on("exit", (code, signal) => {
    void killServe(serve).then(() => process.exit(code ?? (signal ? 1 : 0)));
  });
  await new Promise((resolve) => setTimeout(resolve, 1000));
  if (
    serve.exitCode !== null ||
    serve.signalCode !== null ||
    tui.exitCode !== null ||
    tui.signalCode !== null
  ) {
    await killServe(serve);
    process.exit(tui.exitCode ?? serve.exitCode ?? 1);
  }
  if (readyPath) {
    const readyTempPath = `${readyPath}.tmp-${process.pid}`;
    writeFileSync(readyTempPath, JSON.stringify({ sessionId: id, url, pid: serve.pid }) + "\n");
    renameSync(readyTempPath, readyPath);
  }
}

void main();
"##;

#[allow(dead_code)]
fn _assert_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OpenCodePluginBridge>();
    assert_send_sync::<OpenCodePluginInput>();
    assert_send_sync::<SocketAddr>();
}
