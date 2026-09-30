//! Loopback bridge for headed OpenCode's native plugin.
//!
//! The headed runtime is not driven by tmux keystrokes. The daemon binds this bridge as the
//! [`HarnessInput`](nexus_pty::HarnessInput) for the OpenCode session, while an in-process
//! `@opencode-ai/plugin` polls the loopback endpoint for pending turns, submits them through
//! OpenCode's native `prompt_async`, and reports completion on `session.idle`. Because
//! [`send_turn`](OpenCodePluginInput::send_turn) only returns after that completion callback, the
//! existing realtime drain loop keeps its crash-safe "ack after turn finished" semantics. The plugin
//! sends explicit prompt agent/model metadata on each turn so resumed sessions do not fall
//! back to a stale model from their prior history. It also treats OpenCode's generic idle status as
//! a completion/poll-resume signal so a stale native `busy` flag cannot leave Nexus turns queued.

use std::collections::{HashMap, HashSet, VecDeque};
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
use nexus_agent::adapter::opencode::{native, telemetry::OpenCodeTelemetry};
use nexus_agent::adapter::NativeModelReporting;
use nexus_contracts::events::ChildStream;
use nexus_contracts::events::{AgentUpdateKind, WsEvent};
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_pty::{HarnessInput, TurnAcceptanceObserver, TurnCompletionEvidence};
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
    ready_owner: String,
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

    /// Opaque per-launch ready-file association. This is not an authentication/observer token.
    pub(crate) fn ready_owner(&self) -> &str {
        &self.ready_owner
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
    /// Capture a provisional model owner before polling native setup. The ready handshake, not an
    /// incoming message, subsequently binds its root. Root activation remains the caller's job.
    pub async fn start_observed(
        session_id: SessionId,
        events: Arc<dyn EventSink>,
        options: OpenCodePluginBridgeOptions,
        reporting: NativeModelReporting,
    ) -> Result<Self, OpenCodePluginBridgeError> {
        let reporter = ModelReporter {
            reporting,
            root: None,
            seen: HashSet::new(),
            seen_bytes: 0,
            telemetry: Default::default(),
            telemetry_sequences: HashMap::new(),
            telemetry_id_bytes: 0,
        };
        if reporter.reporting.profile().backend().as_str() != "opencode.plugin"
            || !reporter
                .reporting
                .sink()
                .accepts_profile(reporter.reporting.profile().identity())
        {
            return Err(OpenCodePluginBridgeError::Model(
                "captured model profile is unavailable".into(),
            ));
        }
        Self::start_inner(session_id, events, options, Some(reporter)).await
    }

    /// Bind only the exact root read from this launch's ready handshake; never a model event.
    /// Bind the native root child events must name. Immutable: a second call with the same
    /// root is a no-op that returns true, a different root or an empty one is refused, and a
    /// dead bridge binds nothing.
    pub fn bind_child_root(&self, root: &str) -> bool {
        if !self.state.alive.load(Ordering::SeqCst) || root.trim().is_empty() {
            return false;
        }
        let mut bound = self.state.child_root.lock().unwrap();
        match bound.as_deref() {
            Some(existing) => existing == root,
            None => {
                *bound = Some(root.to_string());
                true
            }
        }
    }

    pub fn bind_model_root(&self, root: &str) -> bool {
        // The launch handshake binds one root for every lane this bridge publishes.
        self.bind_child_root(root);
        let mut reporter = self.state.model.lock().unwrap();
        let Some(model) = reporter.as_mut() else {
            return false;
        };
        if !self.state.alive.load(Ordering::SeqCst)
            || root.trim().is_empty()
            || model.root.as_deref().is_some_and(|old| old != root)
            || !model.reporting.sink().bind_native_root(root)
        {
            return false;
        }
        model.root = Some(root.into());
        true
    }

    /// Exclude reporting shutdown while a synchronous activation callback checks captured ownership.
    /// The supervisor additionally excludes replacement of this bridge in its runtime registry.
    pub fn with_current_model_binding<T>(
        &self,
        reporting: &NativeModelReporting,
        callback: impl FnOnce(&str) -> T,
    ) -> Option<T> {
        let guard = self.state.model.lock().unwrap();
        let model = guard.as_ref()?;
        if !self.state.alive.load(Ordering::SeqCst)
            || !model.reporting.same_owner(reporting)
            || !reporting
                .sink()
                .accepts_profile(reporting.profile().identity())
        {
            return None;
        }
        Some(callback(model.root.as_deref()?))
    }
    /// Start a token-authenticated loopback bridge for one headed OpenCode Nexus session.
    pub async fn start(
        session_id: SessionId,
        events: Arc<dyn EventSink>,
        options: OpenCodePluginBridgeOptions,
    ) -> Result<Self, OpenCodePluginBridgeError> {
        Self::start_inner(session_id, events, options, None).await
    }

    async fn start_inner(
        session_id: SessionId,
        events: Arc<dyn EventSink>,
        options: OpenCodePluginBridgeOptions,
        model: Option<ModelReporter>,
    ) -> Result<Self, OpenCodePluginBridgeError> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let endpoint = OpenCodePluginEndpoint {
            base_url: format!("http://{addr}"),
            token: Uuid::new_v4().to_string(),
            ready_owner: Uuid::new_v4().to_string(),
        };
        let state = Arc::new(BridgeState {
            session_id,
            token: endpoint.token.clone(),
            events,
            turn_timeout: options.turn_timeout,
            turns: Mutex::new(Turns::default()),
            next_id: AtomicU64::new(1),
            notify: Notify::new(),
            alive: AtomicBool::new(true),
            model: Mutex::new(model),
            child_root: Mutex::new(None),
        });
        let app = Router::new()
            .route("/turn/next", get(next_turn))
            .route("/turn/:id/bind", post(bind_turn))
            .route("/turn/:id/accepted", post(accept_turn))
            .route("/turn/:id/complete", post(complete_turn))
            .route("/turn/:id/error", post(error_turn))
            .route("/event", post(plugin_event))
            .route("/child", post(plugin_child_event))
            .route("/model", post(plugin_model))
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
        {
            let mut model = self.state.model.lock().unwrap();
            self.state.alive.store(false, Ordering::SeqCst);
            model.take(); // Revoke before releasing native reporting exclusion.
        }
        self.state.notify.notify_waiters();
        let mut turns = self.state.turns.lock().unwrap();
        turns.queue.clear();
        turns.active.clear();
        drop(turns);
        if let Some(tx) = self.shutdown.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for OpenCodePluginBridge {
    fn drop(&mut self) {
        self.shutdown();
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
        self.send_captured(text, None).await
    }

    async fn send_turn_observed(
        &self,
        text: &str,
        observer: Arc<dyn TurnAcceptanceObserver>,
    ) -> Result<(), String> {
        // Only the bound native persisted-user callback admits the display echo.
        // Terminal completion must not publish a second, late synthetic input.
        self.send_captured(text, Some(observer)).await
    }

    fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        TurnCompletionEvidence::Terminal
    }

    fn is_alive(&self) -> bool {
        self.state.alive.load(Ordering::SeqCst)
    }
}

impl OpenCodePluginInput {
    async fn send_captured(
        &self,
        text: &str,
        observer: Option<Arc<dyn TurnAcceptanceObserver>>,
    ) -> Result<(), String> {
        if !self.is_alive() {
            return Err("opencode plugin bridge is not alive".to_string());
        }
        let turn_id = self
            .state
            .next_id
            .fetch_add(1, Ordering::SeqCst)
            .to_string();
        let (tx, rx) = oneshot::channel();
        let _claim = PendingTurn {
            state: self.state.clone(),
            id: turn_id.clone(),
        };
        {
            let mut turns = self.state.turns.lock().unwrap();
            if !self.is_alive() {
                return Err("opencode plugin bridge is not alive".into());
            }
            turns.queue.push_back(QueuedTurn {
                id: turn_id,
                text: text.to_string(),
                completion: tx,
                observer,
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
}

struct PendingTurn {
    state: Arc<BridgeState>,
    id: String,
}
impl Drop for PendingTurn {
    fn drop(&mut self) {
        let mut turns = self.state.turns.lock().unwrap();
        turns.queue.retain(|turn| turn.id != self.id);
        turns.active.remove(&self.id);
    }
}

#[derive(Default)]
struct Turns {
    queue: VecDeque<QueuedTurn>,
    active: HashMap<String, ActiveTurn>,
}

struct ActiveTurn {
    completion: oneshot::Sender<Result<(), String>>,
    observer: Option<Arc<dyn TurnAcceptanceObserver>>,
    binding: Option<NativeInputBinding>,
    accepting: bool,
    accepted: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct NativeInputBinding {
    #[serde(rename = "sessionID")]
    session_id: String,
    #[serde(rename = "messageID")]
    message_id: String,
}

struct BridgeState {
    session_id: SessionId,
    token: String,
    events: Arc<dyn EventSink>,
    turn_timeout: Duration,
    turns: Mutex<Turns>,
    next_id: AtomicU64,
    notify: Notify,
    alive: AtomicBool,
    model: Mutex<Option<ModelReporter>>,
    /// The immutable native root this bridge publishes children for, bound once by the
    /// launch handshake (`bind_child_root`, also set by `bind_model_root`). Child events name
    /// their root in the body, and the route accepts only this one.
    child_root: Mutex<Option<String>>,
}

struct ModelReporter {
    reporting: NativeModelReporting,
    root: Option<String>,
    seen: HashSet<String>,
    seen_bytes: usize,
    telemetry: OpenCodeTelemetry,
    // Private event order, including removal tombstones. Not a native token counter or turn ID.
    telemetry_sequences: HashMap<String, u64>,
    telemetry_id_bytes: usize,
}
impl Drop for ModelReporter {
    fn drop(&mut self) {
        self.reporting.sink().revoke();
    }
}

impl ModelReporter {
    fn observe(&mut self, info: &Value) -> bool {
        use nexus_contracts::model_report::ModelEvidenceValue;
        if !self
            .reporting
            .sink()
            .accepts_profile(self.reporting.profile().identity())
        {
            return false;
        }
        let Some(root) = self.root.as_deref() else {
            return true;
        };
        if let Some(envelope) = info.get("telemetry") {
            let Some(native) = info.get("info") else {
                return true;
            };
            if native.get("sessionID").and_then(Value::as_str) != Some(root) {
                return true;
            }
            if envelope.get("unavailable") == Some(&Value::Bool(true)) {
                return false;
            }
            let Some(id) = native
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| nexus_contracts::telemetry::TelemetryId::new(*id).is_ok())
            else {
                return true;
            };
            let Some(sequence) = envelope
                .get("sequence")
                .and_then(Value::as_u64)
                .filter(|n| *n > 0 && *n <= 9_007_199_254_740_991)
            else {
                return true;
            };
            if self
                .telemetry_sequences
                .get(id)
                .is_some_and(|old| *old >= sequence)
            {
                return true;
            }
            if !self.telemetry_sequences.contains_key(id) {
                if self.telemetry_sequences.len() >= 4096
                    || id.len() > 262144usize.saturating_sub(self.telemetry_id_bytes)
                {
                    return false;
                }
                self.telemetry_id_bytes += id.len();
            }
            self.telemetry_sequences.insert(id.to_owned(), sequence);
            let updates = if envelope.get("removed") == Some(&Value::Bool(true)) {
                self.telemetry.remove(id, root)
            } else {
                self.telemetry.observe(
                    native,
                    &envelope["contextCapacity"],
                    root,
                    nexus_common::now(),
                )
            };
            for update in updates {
                let Some(profile) = self.reporting.profile().telemetry() else {
                    continue;
                };
                let capability = match &update {
                    nexus_contracts::telemetry::NativeTelemetryUpdate::Usage { .. } => {
                        profile.usage()
                    }
                    nexus_contracts::telemetry::NativeTelemetryUpdate::Context { .. } => {
                        profile.context()
                    }
                    _ => continue,
                };
                if capability.capability() == nexus_contracts::ModelEvidenceCapability::Supported
                    && !self.reporting.sink().observe_telemetry(update)
                {
                    return false;
                }
            }
            return true;
        }
        // Once this message's selection is accepted, later partial snapshots of that same
        // message must not clear it or refresh it as new evidence.
        if info.get("sessionID").and_then(Value::as_str) == Some(root)
            && info
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| self.seen.contains(id))
        {
            return true;
        }
        let Some(update) = native::selected_model(info, root, nexus_common::now()) else {
            return true;
        };
        let message = match &update.value {
            ModelEvidenceValue::Observed(value) => value.native_message_id.clone(),
            _ => None,
        };
        if let Some(message) = message.as_ref() {
            if self.seen.contains(message) {
                return true;
            }
            if self.seen.len() >= 4096
                || message.len() > 262144usize.saturating_sub(self.seen_bytes)
            {
                return false;
            }
        }
        if !self.reporting.sink().observe(update) {
            return false;
        }
        if let Some(message) = message {
            self.seen_bytes += message.len();
            self.seen.insert(message);
        }
        true
    }
}

struct QueuedTurn {
    id: String,
    text: String,
    completion: oneshot::Sender<Result<(), String>>,
    observer: Option<Arc<dyn TurnAcceptanceObserver>>,
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
    #[error("native model reporting: {0}")]
    Model(String),
}

async fn plugin_model(
    State(state): State<Arc<BridgeState>>,
    headers: HeaderMap,
    Json(info): Json<Value>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let mut model = state.model.lock().unwrap();
    if state.alive.load(Ordering::SeqCst)
        && model.as_mut().is_some_and(|model| !model.observe(&info))
    {
        model.take();
    }
    StatusCode::NO_CONTENT.into_response()
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
        {
            let mut turns = state.turns.lock().unwrap();
            if !state.alive.load(Ordering::SeqCst) {
                return StatusCode::NO_CONTENT.into_response();
            }
            if let Some(turn) = turns.queue.pop_front() {
                turns.active.insert(
                    turn.id.clone(),
                    ActiveTurn {
                        completion: turn.completion,
                        observer: turn.observer,
                        binding: None,
                        accepting: false,
                        accepted: false,
                    },
                );
                return Json(PluginTurn {
                    id: turn.id,
                    text: turn.text,
                })
                .into_response();
            }
        }
        tokio::select! {
            _ = state.notify.notified() => {}
            _ = &mut timeout => return StatusCode::NO_CONTENT.into_response(),
        }
    }
}

async fn bind_turn(
    State(state): State<Arc<BridgeState>>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Json(binding): Json<NativeInputBinding>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if !binding.session_id.starts_with("ses_")
        || !binding.message_id.starts_with("msg_")
        || binding.session_id.len() > 4096
        || binding.message_id.len() > 4096
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let mut turns = state.turns.lock().unwrap();
    let Some(turn) = turns.active.get_mut(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !state.alive.load(Ordering::SeqCst)
        || turn.binding.as_ref().is_some_and(|old| old != &binding)
    {
        return StatusCode::CONFLICT.into_response();
    }
    turn.binding = Some(binding);
    StatusCode::NO_CONTENT.into_response()
}

async fn accept_turn(
    State(state): State<Arc<BridgeState>>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Json(binding): Json<NativeInputBinding>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (observer, canonical_echo) = {
        let mut turns = state.turns.lock().unwrap();
        let Some(turn) = turns.active.get_mut(&id) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if !state.alive.load(Ordering::SeqCst) || turn.binding.as_ref() != Some(&binding) {
            return StatusCode::CONFLICT.into_response();
        }
        if turn.accepting {
            // Never replay the observer or acknowledge its still-pending effect.
            return StatusCode::CONFLICT.into_response();
        }
        let observer = if turn.accepted {
            None
        } else {
            turn.observer.clone()
        };
        turn.accepting = true;
        (observer, turn.observer.is_some())
    };
    // Admission linearizes under the exact captured turn lock; no await holds it.
    if let Some(observer) = observer {
        observer.accepted().await;
    }
    {
        let mut turns = state.turns.lock().unwrap();
        let Some(turn) = turns.active.get_mut(&id) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if !state.alive.load(Ordering::SeqCst) || turn.binding.as_ref() != Some(&binding) {
            return StatusCode::CONFLICT.into_response();
        }
        turn.accepting = false;
        turn.accepted = true;
    }
    Json(json!({"canonicalEcho": canonical_echo})).into_response()
}

async fn complete_turn(
    State(state): State<Arc<BridgeState>>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let turn = {
        let mut turns = state.turns.lock().unwrap();
        if turns.active.get(&id).is_some_and(|turn| turn.accepting) {
            return StatusCode::CONFLICT.into_response();
        }
        turns.active.remove(&id)
    };
    if let Some(turn) = turn {
        let outcome = if turn.observer.is_some() && !turn.accepted {
            Err("opencode completed without a captured native input receipt".to_string())
        } else {
            Ok(())
        };
        let _ = turn.completion.send(outcome);
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
    if let Some(turn) = state.turns.lock().unwrap().active.remove(&id) {
        let message = plugin_turn_error_message(body);
        let _ = turn.completion.send(Err(message));
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

/// One native child stream update from the plugin: a subagent session's part, status or idle,
/// attributed by the plugin (harness-owned) and routed to the child lane, never the parent's.
#[derive(Debug, Deserialize)]
struct PluginChildEvent {
    child: ChildStream,
    kind: AgentUpdateKind,
    #[serde(rename = "sourceRef")]
    source_ref: String,
    #[serde(default)]
    data: Value,
}

async fn plugin_child_event(
    State(state): State<Arc<BridgeState>>,
    headers: HeaderMap,
    Json(body): Json<PluginChildEvent>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if body.child.harness != nexus_harness_opencode::HARNESS_ID
        || body.child.root.is_empty()
        || body.source_ref.is_empty()
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    // Child publication is bound to the launch-captured root, never to the caller's claim.
    let bound = state.child_root.lock().unwrap().clone();
    match bound.as_deref() {
        None => {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": "root_unbound" })),
            )
                .into_response();
        }
        Some(root) if root != body.child.root => {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": "root_mismatch" })),
            )
                .into_response();
        }
        Some(_) => {}
    }
    state
        .events
        .emit(WsEvent::ChildAgentUpdate {
            session_id: state.session_id.clone(),
            child: body.child,
            kind: body.kind,
            source_ref: body.source_ref,
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

const OPENCODE_PLUGIN_JS: &str = native::PLUGIN_SOURCE;

const OPENCODE_SERVE_JS: &str = nexus_harness_opencode::SERVE_SOURCE;

#[allow(dead_code)]
fn _assert_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OpenCodePluginBridge>();
    assert_send_sync::<OpenCodePluginInput>();
    assert_send_sync::<SocketAddr>();
}
