//! Hermes headed gateway bridge.
//!
//! Headed Hermes is not driven by tmux keystrokes. Nexus launches `hermes gateway run` with an
//! runtime profile under the machine Hermes root that contains the Nexus gateway platform plugin,
//! then talks to that plugin over a launch-local socket. Delivery is one-at-a-time and acked only after the plugin
//! surfaces the message into the gateway turn; streaming comes back as `agent_update` frames.
//! The generated profile also gives Hermes a model-facing Nexus identity hint, so a fresh launch
//! knows its assigned bus name/session before it has seen thread traffic.

use std::fs;
use std::io::{BufRead, BufReader, Write};
#[cfg(windows)]
use std::net::{TcpListener as BridgeListener, TcpStream as BridgeStream};
#[cfg(unix)]
use std::os::unix::net::{UnixListener as BridgeListener, UnixStream as BridgeStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use nexus_agent::adapter::hermes::skill::install_bus_skill_in_home;
use nexus_common::NexusError;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::{AgentUpdateKind, WsEvent};
use nexus_dispatch::Bell;
use nexus_pty::TurnCompletionEvidence;
use serde_json::{json, Value};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Bound gateway startup/subscription independently from model inference.
const ADAPTER_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Keep the Hermes lane closed while its provider/model turn is running. This is a lane-readiness
/// bound, not a transport-delivery deadline: delivery settles earlier, when Hermes reports that it
/// has admitted the message into background processing.
const TURN_COMPLETION_TIMEOUT: Duration = Duration::from_secs(600);

/// Files and socket metadata for one launch-local Hermes gateway profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermesGatewayProfile {
    pub home: PathBuf,
    /// Machine Hermes root used as the source for provider config and Hermes' global auth fallback.
    pub source_home: PathBuf,
    pub bridge_socket: PathBuf,
    pub bridge_token: String,
    /// Daemon-assigned bus name surfaced to Hermes in the launch-local platform prompt.
    pub nexus_name: String,
    /// Daemon-owned session id surfaced beside the bus name so Hermes can verify with `whoami`.
    pub session_id: SessionId,
}

/// Write an isolated Hermes profile with the Nexus platform plugin enabled.
pub fn write_hermes_gateway_profile(profile: &HermesGatewayProfile) -> Result<(), NexusError> {
    let plugin_dir = profile.home.join("plugins/nexus");
    fs::create_dir_all(&plugin_dir)
        .map_err(|e| NexusError::Store(format!("create Hermes plugin dir: {e}")))?;
    fs::write(
        profile.home.join("config.yaml"),
        hermes_config_yaml(&profile.source_home)?,
    )
    .map_err(|e| NexusError::Store(format!("write Hermes config.yaml: {e}")))?;
    install_bus_skill_in_home(&profile.home)
        .map_err(|e| NexusError::Store(format!("install Hermes nexus-bus skill: {e}")))?;
    fs::write(plugin_dir.join("plugin.yaml"), PLUGIN_YAML)
        .map_err(|e| NexusError::Store(format!("write Hermes plugin.yaml: {e}")))?;
    fs::write(plugin_dir.join("__init__.py"), plugin_init(profile))
        .map_err(|e| NexusError::Store(format!("write Hermes __init__.py: {e}")))?;
    fs::write(plugin_dir.join("adapter.py"), PLUGIN_ADAPTER)
        .map_err(|e| NexusError::Store(format!("write Hermes adapter.py: {e}")))?;
    fs::write(plugin_dir.join("bridge_client.py"), PLUGIN_BRIDGE_CLIENT)
        .map_err(|e| NexusError::Store(format!("write Hermes bridge_client.py: {e}")))?;
    Ok(())
}

fn hermes_config_yaml(source_home: &Path) -> Result<String, NexusError> {
    let inherited = inherited_hermes_config_yaml(source_home)?;
    Ok(format!(
        r#"# Nexus-managed Hermes profile; regenerated on launch.
{inherited}plugins:
  enabled: [nexus]
platforms:
  nexus:
    enabled: true
approvals:
  mode: off
"#
    ))
}

fn inherited_hermes_config_yaml(source_home: &Path) -> Result<String, NexusError> {
    let source_config = source_home.join("config.yaml");
    if !source_config.is_file() {
        return Ok(String::new());
    }
    let raw = fs::read_to_string(&source_config)
        .map_err(|e| NexusError::Store(format!("read Hermes source config.yaml: {e}")))?;
    let parsed: serde_yaml::Value = serde_yaml::from_str(&raw)
        .map_err(|e| NexusError::Store(format!("parse Hermes source config.yaml: {e}")))?;
    let Some(mapping) = parsed.as_mapping() else {
        return Ok(String::new());
    };
    let mut inherited = serde_yaml::Mapping::new();
    for key in [
        "model",
        "providers",
        "custom_providers",
        "fallback_providers",
        "credential_pool_strategies",
        "provider_routing",
    ] {
        let yaml_key = serde_yaml::Value::String(key.to_string());
        if let Some(value) = mapping.get(&yaml_key) {
            inherited.insert(yaml_key, value.clone());
        }
    }
    if inherited.is_empty() {
        return Ok(String::new());
    }
    let rendered = serde_yaml::to_string(&inherited)
        .map_err(|e| NexusError::Store(format!("render Hermes inherited config: {e}")))?;
    Ok(rendered.trim_start_matches("---\n").to_string())
}

fn plugin_init(profile: &HermesGatewayProfile) -> String {
    let hint = format!(
        "You are {} (session {}). Never sign messages as anyone else. If unsure, use the launch-pinned Nexus CLI to run `nexus whoami`. You are connected to the local Nexus bus. Plain final responses are observation-only and are never delivered to the sender. When an inbound <nexus-batch> explicitly requests a reply, use the Nexus bus skill exactly once to reply, DM, or post to the supplied destination, then finish locally. Do not send anything for traffic that does not request a response.",
        profile.nexus_name, profile.session_id.0
    );
    let hint_literal =
        serde_json::to_string(&hint).expect("platform hint string should serialize to JSON");
    format!(
        r#"from .adapter import NexusAdapter
import os

def register(ctx):
    os.environ.setdefault("NEXUS_ALLOW_ALL_USERS", "true")
    os.environ.setdefault("NEXUS_HOME_CHANNEL", "nexus")
    ctx.register_platform(
        name="nexus",
        label="Nexus",
        adapter_factory=lambda cfg: NexusAdapter(cfg),
        check_fn=lambda: True,
        allow_all_env="NEXUS_ALLOW_ALL_USERS",
        platform_hint={hint_literal},
        emoji="◈",
        max_message_length=8000,
    )
"#
    )
}

#[derive(Default)]
struct BridgeState {
    adapter: Option<BridgeStream>,
    awaiting_id: Option<String>,
    processing_started_id: Option<String>,
    delivered_id: Option<String>,
    completion_guard: Option<(String, OwnedMutexGuard<()>)>,
}

/// Persistent local-socket bridge used by the Hermes gateway plugin.
pub struct HermesGatewayBridge {
    state: Arc<(Mutex<BridgeState>, Condvar)>,
    endpoint: String,
    /// Hermes gateway treats a new platform message as an interrupt. Keep the native lane FIFO and
    /// allow the next bus turn onto the socket only after the previous completion callback acks.
    delivery: Arc<AsyncMutex<()>>,
}

impl HermesGatewayBridge {
    /// Start the local bridge. Unix uses a filesystem socket; Windows uses an ephemeral loopback
    /// TCP listener. One plugin connection is active at a time; a new subscribe
    /// frame supersedes the previous connection.
    pub fn start(
        session: SessionId,
        socket_path: PathBuf,
        token: String,
        events: Arc<dyn EventSink>,
        bell: Bell,
    ) -> Result<Arc<Self>, NexusError> {
        let (listener, endpoint) = bind_bridge_listener(&socket_path)?;
        let state = Arc::new((Mutex::new(BridgeState::default()), Condvar::new()));
        let handle = tokio::runtime::Handle::try_current().ok();
        let bridge = Arc::new(Self {
            state: state.clone(),
            endpoint,
            delivery: Arc::new(AsyncMutex::new(())),
        });
        thread::Builder::new()
            .name(format!("nexus-hermes-bridge-{}", session.0))
            .spawn(move || {
                for stream in listener.incoming().flatten() {
                    let state = state.clone();
                    let token = token.clone();
                    let session = session.clone();
                    let events = events.clone();
                    let bell = bell.clone();
                    let handle = handle.clone();
                    thread::Builder::new()
                        .name(format!("nexus-hermes-bridge-client-{}", session.0))
                        .spawn(move || {
                            handle_client(stream, state, token, session, events, bell, handle);
                        })
                        .ok();
                }
            })
            .map_err(|e| NexusError::Store(format!("spawn Hermes bridge thread: {e}")))?;
        Ok(bridge)
    }

    /// Connection endpoint passed to the generated Hermes plugin.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Send a rendered Nexus turn to the connected Hermes plugin and wait until it reports the turn
    /// entered Hermes background processing. The lane remains closed until the terminal callback
    /// (or its bounded timeout), but a provider stall after context admission is not transport loss.
    pub async fn send_rendered_turn(&self, text: &str) -> Result<(), String> {
        let delivery = self.delivery.clone().lock_owned().await;
        let text = text.to_string();
        let state = self.state.clone();
        let id = tokio::task::spawn_blocking(move || send_blocking(state, &text))
            .await
            .map_err(|e| format!("Hermes bridge worker panicked: {e}"))??;

        let mut delivery = Some(delivery);
        {
            let (lock, _) = &*self.state;
            let mut guard = lock.lock().unwrap();
            if guard.delivered_id.as_deref() != Some(id.as_str()) {
                guard.completion_guard = Some((
                    id.clone(),
                    delivery
                        .take()
                        .expect("Hermes delivery guard must be owned"),
                ));
            }
        }
        drop(delivery);

        let state = self.state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(TURN_COMPLETION_TIMEOUT).await;
            let (lock, cv) = &*state;
            let mut guard = lock.lock().unwrap();
            let timed_out = guard
                .completion_guard
                .as_ref()
                .is_some_and(|(guarded_id, _)| guarded_id == &id);
            if timed_out {
                guard.completion_guard.take();
                if guard.awaiting_id.as_deref() == Some(id.as_str()) {
                    guard.awaiting_id = None;
                }
                cv.notify_all();
                tracing::warn!(turn_id = %id, "Hermes provider turn exceeded lane-readiness timeout; releasing queued delivery without revoking context acceptance");
            }
        });
        Ok(())
    }

    /// The generated Hermes adapter sends `processing_started` from Hermes' lifecycle hook after
    /// the message leaves its platform queue and enters background processing. This is stronger
    /// than terminal input bytes, but deliberately independent from provider/model completion.
    pub fn turn_completion_evidence(&self) -> TurnCompletionEvidence {
        TurnCompletionEvidence::ContextAccepted
    }
}

#[cfg(unix)]
fn bind_bridge_listener(socket_path: &Path) -> Result<(BridgeListener, String), NexusError> {
    if socket_path.exists() {
        let _ = fs::remove_file(socket_path);
    }
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| NexusError::Store(format!("create Hermes bridge dir: {e}")))?;
    }
    let listener = BridgeListener::bind(socket_path)
        .map_err(|e| NexusError::Store(format!("bind Hermes bridge socket: {e}")))?;
    Ok((listener, socket_path.to_string_lossy().into_owned()))
}

#[cfg(windows)]
fn bind_bridge_listener(_socket_path: &Path) -> Result<(BridgeListener, String), NexusError> {
    let listener = BridgeListener::bind(("127.0.0.1", 0))
        .map_err(|e| NexusError::Store(format!("bind Hermes bridge loopback socket: {e}")))?;
    let address = listener
        .local_addr()
        .map_err(|e| NexusError::Store(format!("read Hermes bridge loopback address: {e}")))?;
    Ok((listener, format!("tcp://{address}")))
}

fn send_blocking(state: Arc<(Mutex<BridgeState>, Condvar)>, text: &str) -> Result<String, String> {
    let id = format!(
        "nexus_turn_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    );
    let frame = json!({ "t": "incoming", "id": id, "text": text });
    let (lock, cv) = &*state;
    let mut guard = lock.lock().unwrap();
    let connect_deadline = Instant::now() + ADAPTER_CONNECT_TIMEOUT;
    while guard.adapter.is_none() && Instant::now() < connect_deadline {
        let remaining = connect_deadline.saturating_duration_since(Instant::now());
        let (next, timeout) = cv.wait_timeout(guard, remaining).unwrap();
        guard = next;
        if timeout.timed_out() {
            break;
        }
    }
    let Some(adapter) = guard.adapter.as_mut() else {
        return Err("Hermes gateway bridge has no subscribed adapter".into());
    };
    adapter
        .write_all(format!("{frame}\n").as_bytes())
        .map_err(|e| format!("Hermes gateway bridge write failed: {e}"))?;
    adapter
        .flush()
        .map_err(|e| format!("Hermes gateway bridge flush failed: {e}"))?;
    guard.awaiting_id = Some(id.clone());
    guard.processing_started_id = None;
    guard.delivered_id = None;
    let completion_deadline = Instant::now() + TURN_COMPLETION_TIMEOUT;
    while Instant::now() < completion_deadline {
        let remaining = completion_deadline.saturating_duration_since(Instant::now());
        let (next, timeout) = cv.wait_timeout(guard, remaining).unwrap();
        guard = next;
        if guard.processing_started_id.as_deref() == Some(id.as_str()) {
            return Ok(id);
        }
        if timeout.timed_out() {
            break;
        }
    }
    guard.awaiting_id = None;
    Err("Hermes gateway bridge processing start timed out".into())
}

fn handle_client(
    stream: BridgeStream,
    state: Arc<(Mutex<BridgeState>, Condvar)>,
    token: String,
    session: SessionId,
    events: Arc<dyn EventSink>,
    bell: Bell,
    handle: Option<tokio::runtime::Handle>,
) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        let Ok(n) = reader.read_line(&mut line) else {
            break;
        };
        if n == 0 {
            break;
        }
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if frame.get("token").and_then(Value::as_str) != Some(token.as_str()) {
            continue;
        }
        match frame.get("t").and_then(Value::as_str) {
            Some("subscribe") => {
                if let Ok(writer) = reader.get_ref().try_clone() {
                    let (lock, cv) = &*state;
                    lock.lock().unwrap().adapter = Some(writer);
                    cv.notify_all();
                }
            }
            Some("processing_started") => {
                if let Some(id) = frame.get("id").and_then(Value::as_str) {
                    let (lock, cv) = &*state;
                    let mut guard = lock.lock().unwrap();
                    if guard.awaiting_id.as_deref() == Some(id) {
                        guard.processing_started_id = Some(id.to_string());
                        cv.notify_all();
                    }
                }
            }
            Some("delivered") => {
                if let Some(id) = frame.get("id").and_then(Value::as_str) {
                    let (lock, cv) = &*state;
                    let mut guard = lock.lock().unwrap();
                    if guard.awaiting_id.as_deref() == Some(id) {
                        // Backward-compatible with an adapter generated by an older daemon: its
                        // terminal receipt is also sufficient context-acceptance evidence.
                        guard.processing_started_id = Some(id.to_string());
                        guard.delivered_id = Some(id.to_string());
                        if guard
                            .completion_guard
                            .as_ref()
                            .is_some_and(|(guarded_id, _)| guarded_id == id)
                        {
                            guard.completion_guard.take();
                        }
                        guard.awaiting_id = None;
                        cv.notify_all();
                    }
                }
            }
            Some("agent_update") => {
                let Some(kind) = agent_update_kind(frame.get("kind").and_then(Value::as_str))
                else {
                    continue;
                };
                let data = frame.get("data").cloned().unwrap_or_else(|| json!({}));
                let event = WsEvent::AgentUpdate {
                    session_id: session.clone(),
                    kind,
                    data,
                };
                if kind == AgentUpdateKind::TurnEnd {
                    bell.ring(&session);
                }
                if let Some(handle) = &handle {
                    let events = events.clone();
                    handle.spawn(async move {
                        events.emit(event).await;
                    });
                }
            }
            _ => {}
        }
    }
}

fn agent_update_kind(kind: Option<&str>) -> Option<AgentUpdateKind> {
    match kind? {
        "user_input" | "userInput" => Some(AgentUpdateKind::UserInput),
        "text" => Some(AgentUpdateKind::Text),
        "thinking" => Some(AgentUpdateKind::Thinking),
        "tool_call" | "toolCall" => Some(AgentUpdateKind::ToolCall),
        "turn_end" | "turnEnd" => Some(AgentUpdateKind::TurnEnd),
        _ => None,
    }
}

const PLUGIN_YAML: &str = r#"name: nexus
label: Nexus
kind: platform
version: 0.1.0
description: Connect Hermes gateway sessions to the local Nexus daemon.
author: Egregore
"#;

const PLUGIN_ADAPTER: &str = r#"from __future__ import annotations
import asyncio, uuid
from typing import Any, Optional
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult
from gateway.config import Platform, PlatformConfig
from .bridge_client import get_client

class NexusAdapter(BasePlatformAdapter):
    def __init__(self, config: PlatformConfig) -> None:
        super().__init__(config, Platform("nexus"))
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._client = get_client()
        self._startup_ready = asyncio.Event()
        self._startup_task: Optional[asyncio.Task] = None

    async def connect(self, is_reconnect: bool = False) -> bool:
        self._loop = asyncio.get_running_loop()
        self._startup_ready.clear()
        self._client.start(self._on_incoming)
        self._startup_task = asyncio.create_task(self._wait_for_startup_settlement())
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        if self._startup_task is not None:
            self._startup_task.cancel()
            self._startup_task = None
        self._client.close()
        self._mark_disconnected()

    def _gateway_startup_is_busy(self) -> bool:
        # Hermes can auto-resume an interrupted turn after its platform adapter connects. During
        # that window BasePlatformAdapter has a one-message pending slot: feeding a Nexus turn into
        # it can replace an earlier turn before either reaches model context. Keep the turn on the
        # lossless Nexus bridge until startup restore and its adapter-owned processing task settle.
        handler_owner = getattr(self._message_handler, "__self__", None)
        restoring = bool(getattr(handler_owner, "_startup_restore_in_progress", False))
        return restoring or bool(self._active_sessions)

    async def _wait_for_startup_settlement(self) -> None:
        while self._gateway_startup_is_busy():
            await asyncio.sleep(0.05)
        # Require a stable idle observation. The startup restore task and its adapter task are
        # scheduled independently in Hermes, so a single idle sample can fall between them.
        await asyncio.sleep(0.10)
        while self._gateway_startup_is_busy():
            await asyncio.sleep(0.05)
        self._startup_ready.set()

    async def send(self, chat_id: str, content: str, reply_to: Any = None, metadata: Any = None) -> SendResult:
        self._client.agent_update("text", {"text": content})
        return SendResult(success=True, message_id=uuid.uuid4().hex)

    async def edit_message(self, chat_id: str, message_id: str, content: str, finalize: bool = False) -> SendResult:
        self._client.agent_update("text", {"text": content, "message_id": message_id, "finalize": finalize})
        if finalize:
            self._client.agent_update("turn_end", {"reason": "gateway_stream_complete", "message_id": message_id})
        return SendResult(success=True, message_id=message_id)

    async def get_chat_info(self, chat_id: str) -> dict:
        return {"name": "Nexus", "type": "dm"}

    def _on_incoming(self, msg: dict) -> None:
        loop = self._loop
        if loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._inject(msg), loop)

    async def on_processing_start(self, event: MessageEvent) -> None:
        # Hermes has removed this event from its platform queue and is about to
        # build/run the agent turn. This is the transport settlement boundary.
        if event.message_id:
            self._client.processing_started(event.message_id)

    async def on_processing_complete(self, event: MessageEvent, outcome: Any) -> None:
        # BasePlatformAdapter.handle_message returns as soon as it schedules the
        # background turn. Acking that future lets the next Nexus message interrupt
        # Hermes while it is still generating. This hook is the real turn boundary:
        # it runs after model processing and final platform delivery have settled.
        if event.message_id:
            self._client.delivered(event.message_id)

    async def _inject(self, msg: dict) -> None:
        await self._startup_ready.wait()
        source = self.build_source(chat_id="nexus", chat_name="Nexus", chat_type="dm", user_id="nexus", user_name="Nexus")
        await self.handle_message(MessageEvent(text=msg.get("text") or "", message_type=MessageType.TEXT, source=source, message_id=msg.get("id")))
"#;

const PLUGIN_BRIDGE_CLIENT: &str = r#"from __future__ import annotations
import json, os, socket, threading, time
from typing import Callable, Optional

class BridgeClient:
    def __init__(self, endpoint: str, token: str) -> None:
        self._endpoint = endpoint
        self._token = token
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._reader: Optional[threading.Thread] = None
        self._on_incoming: Optional[Callable[[dict], None]] = None

    def start(self, on_incoming: Callable[[dict], None]) -> None:
        self._on_incoming = on_incoming
        if self._reader is None:
            self._reader = threading.Thread(target=self._run, name="nexus-bridge", daemon=True)
            self._reader.start()

    def _run(self) -> None:
        buf = b""
        while not self._stop.is_set():
            if self._sock is None:
                self._connect()
                if self._sock is None:
                    continue
                self._send({"t": "subscribe"})
            try:
                data = self._sock.recv(65536)
            except OSError:
                data = b""
            if not data:
                with self._lock:
                    self._sock = None
                continue
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    frame = json.loads(line)
                except ValueError:
                    continue
                if frame.get("t") == "incoming" and self._on_incoming:
                    self._on_incoming(frame)

    def _connect(self) -> None:
        while not self._stop.is_set():
            try:
                endpoint = self._endpoint
                if endpoint.startswith("tcp://"):
                    host, port = endpoint.removeprefix("tcp://").rsplit(":", 1)
                    s = socket.create_connection((host, int(port)))
                else:
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.connect(endpoint)
                with self._lock:
                    self._sock = s
                return
            except OSError:
                time.sleep(1.0)

    def _send(self, frame: dict) -> None:
        frame["token"] = self._token
        data = (json.dumps(frame) + "\n").encode()
        with self._lock:
            if self._sock is None:
                return
            try:
                self._sock.sendall(data)
            except OSError:
                self._sock = None

    def delivered(self, msg_id: str) -> None:
        self._send({"t": "delivered", "id": msg_id})

    def processing_started(self, msg_id: str) -> None:
        self._send({"t": "processing_started", "id": msg_id})

    def agent_update(self, kind: str, data: dict) -> None:
        self._send({"t": "agent_update", "kind": kind, "data": data})

    def close(self) -> None:
        self._stop.set()
        with self._lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass
                self._sock = None

_client: Optional[BridgeClient] = None

def get_client() -> BridgeClient:
    global _client
    if _client is None:
        endpoint = os.environ["NEXUS_HERMES_BRIDGE_SOCKET"]
        token = os.environ["NEXUS_HERMES_BRIDGE_TOKEN"]
        _client = BridgeClient(endpoint, token)
    return _client
"#;
