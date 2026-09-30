use std::io::{BufRead, BufReader, Read, Write};
#[cfg(windows)]
use std::net::TcpStream as BridgeTestStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream as BridgeTestStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use nexus::daemon::hermes_gateway::{
    write_hermes_gateway_profile, HermesGatewayBridge, HermesGatewayProfile,
};
use nexus_common::provenance::render_batch;
use nexus_contracts::batch::{BatchCounts, BatchMessage, NexusBatch};
use nexus_contracts::ids::{MessageId, SessionId};
use nexus_contracts::{AgentUpdateKind, EventSink, WsEvent};
use nexus_contracts::{Kind, Scope};
use nexus_dispatch::Bell;
use nexus_pty::TurnCompletionEvidence;
use tempfile::tempdir;

#[path = "support/hermes_parked_timing.rs"]
mod hermes_parked_timing;

struct NativeCapture {
    profile: nexus_contracts::model_report::ModelProfileIdentity,
    closed: std::sync::atomic::AtomicBool,
    root: Mutex<Option<String>>,
    updates: Mutex<Vec<nexus_contracts::model_report::NativeModelUpdate>>,
    telemetry: Mutex<Vec<nexus_contracts::telemetry::NativeTelemetryUpdate>>,
}
impl nexus_contracts::model_report::ModelObservationSink for NativeCapture {
    fn accepts_profile(
        &self,
        profile: &nexus_contracts::model_report::ModelProfileIdentity,
    ) -> bool {
        !self.closed.load(std::sync::atomic::Ordering::SeqCst) && self.profile.matches(profile)
    }
    fn bind_native_root(&self, root: &str) -> bool {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return false;
        }
        let mut current = self.root.lock().unwrap();
        if current.as_deref().is_some_and(|old| old != root) {
            return false;
        }
        *current = Some(root.into());
        true
    }
    fn observe(&self, update: nexus_contracts::model_report::NativeModelUpdate) -> bool {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst)
            || self.root.lock().unwrap().as_deref() != Some(&update.native_session_id)
        {
            return false;
        }
        self.updates.lock().unwrap().push(update);
        true
    }
    fn revoke(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn observe_telemetry(&self, update: nexus_contracts::telemetry::NativeTelemetryUpdate) -> bool {
        let nexus_contracts::telemetry::NativeTelemetryUpdate::Usage {
            native_session_id, ..
        } = &update
        else {
            return false;
        };
        if self.closed.load(std::sync::atomic::Ordering::SeqCst)
            || self.root.lock().unwrap().as_deref() != Some(native_session_id)
        {
            return false;
        }
        self.telemetry.lock().unwrap().push(update);
        true
    }
}
fn native_capture() -> (
    Arc<NativeCapture>,
    nexus_agent::adapter::NativeModelReporting,
) {
    let profile = nexus_agent::adapter::hermes::native::model_profile();
    let sink = Arc::new(NativeCapture {
        profile: profile.identity().clone(),
        closed: false.into(),
        root: Mutex::new(None),
        updates: Mutex::new(vec![]),
        telemetry: Mutex::new(vec![]),
    });
    let reporting = profile.capture(sink.clone()).unwrap();
    (sink, reporting)
}
fn model_frame(root: &str, sequence: u64, model: &str) -> serde_json::Value {
    serde_json::json!({"t":"model_source", "token":"model-token", "sequence":sequence,
        "event":"agent:start", "context":{"platform":"nexus","user_id":"nexus",
        "chat_id":"nexus","thread_id":"","chat_type":"dm","session_id":root},
        "row":{"id":root,"model":model,"parent_session_id":null,"model_config":"{}"}})
}
fn send_model(bridge: &HermesGatewayBridge, frame: serde_json::Value) -> bool {
    let mut stream = connect_bridge(bridge);
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    writeln!(stream, "{frame}").unwrap();
    let mut reply = String::new();
    if BufReader::new(stream).read_line(&mut reply).is_err() {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(&reply)
        .ok()
        .is_some_and(|value| value["accepted"] == true)
}

#[tokio::test(flavor = "multi_thread")]
async fn native_usage_is_exact_row_cumulative_replacement_not_context_or_total() {
    use nexus_contracts::telemetry::{
        NativeTelemetryUpdate, NativeTelemetryValue, TokenUsageScope,
    };
    let dir = tempdir().unwrap();
    let (sink, reporting) = native_capture();
    let bridge = HermesGatewayBridge::start_observed(
        SessionId("s_usage".into()),
        dir.path().join("bridge.sock"),
        "model-token".into(),
        Arc::new(CaptureSink::default()),
        Bell::new(),
        reporting,
    )
    .unwrap();
    for (index, input) in [120, 120, 12, 0].into_iter().enumerate() {
        let mut frame = model_frame("root", index as u64 + 1, "configured");
        frame["row"]["usage"] = serde_json::json!({"api_call_count":3,"input_tokens":input,
            "output_tokens":30,"cache_read_tokens":80,"cache_write_tokens":20,"reasoning_tokens":12});
        assert!(send_model(&bridge, frame));
        let values = sink.telemetry.lock().unwrap();
        assert_eq!(
            values.len(),
            index + 1,
            "each native row replaces independently"
        );
        let NativeTelemetryUpdate::Usage {
            native_session_id,
            value: NativeTelemetryValue::Observed(value),
        } = &values[index]
        else {
            panic!("native row must supply usage");
        };
        assert_eq!(native_session_id, "root");
        assert_eq!(value.scope, TokenUsageScope::SessionCumulative);
        assert_eq!(value.input_tokens.unwrap().get(), input);
        assert_eq!(value.cache_read_tokens.unwrap().get(), 80);
        assert_eq!(value.cache_write_tokens.unwrap().get(), 20);
        assert_eq!(value.reasoning_tokens.unwrap().get(), 12);
        assert!(value.total_tokens.is_none());
        assert!(
            value.native_turn_id.is_none() && value.reset_id.is_none() && value.model.is_none()
        );
    }
    for (index, usage, invalid) in [
        (5, serde_json::Value::Null, false),
        (6, serde_json::json!({"api_call_count":0}), false),
        (
            7,
            serde_json::json!({"api_call_count":1,"input_tokens":-1}),
            true,
        ),
    ] {
        let mut frame = model_frame("root", index, "configured");
        frame["row"]["usage"] = usage;
        assert!(send_model(&bridge, frame));
        let values = sink.telemetry.lock().unwrap();
        let NativeTelemetryUpdate::Usage { value, .. } = values.last().unwrap() else {
            panic!("usage only")
        };
        assert_eq!(matches!(value, NativeTelemetryValue::Invalid), invalid);
        assert_eq!(matches!(value, NativeTelemetryValue::Unknown), !invalid);
    }
    let before = sink.telemetry.lock().unwrap().len();
    let mut foreign = model_frame("root", 8, "child");
    foreign["row"]["parent_session_id"] = serde_json::json!("parent");
    assert!(!send_model(&bridge, foreign));
    assert_eq!(sink.telemetry.lock().unwrap().len(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn native_model_source_requires_exact_hook_root_and_rejects_replay_or_child() {
    let dir = tempdir().unwrap();
    let (sink, reporting) = native_capture();
    let bridge = HermesGatewayBridge::start_observed(
        SessionId("s_model".into()),
        dir.path().join("bridge.sock"),
        "model-token".into(),
        Arc::new(CaptureSink::default()),
        Bell::new(),
        reporting,
    )
    .unwrap();
    let mut foreign_source = model_frame("foreign", 1, "wrong");
    foreign_source["context"]["chat_id"] = serde_json::json!("other");
    assert!(!send_model(&bridge, foreign_source));
    assert!(sink.root.lock().unwrap().is_none());
    let mut absent = model_frame("root", 1, "configured");
    absent["row"] = serde_json::Value::Null;
    assert!(
        send_model(&bridge, absent),
        "positive hook selects root before DB row exists"
    );
    assert_eq!(sink.root.lock().unwrap().as_deref(), Some("root"));
    assert!(matches!(
        sink.updates
            .lock()
            .unwrap()
            .last()
            .map(|update| &update.value),
        Some(nexus_contracts::model_report::ModelEvidenceValue::Unknown(
            _
        ))
    ));
    assert!(send_model(&bridge, model_frame("root", 2, "configured")));
    assert!(!send_model(&bridge, model_frame("root", 2, "stale-repeat")));
    let mut unavailable = model_frame("root", 3, "unused");
    unavailable["row"] = serde_json::Value::Null;
    assert!(send_model(&bridge, unavailable));
    assert!(matches!(
        sink.updates
            .lock()
            .unwrap()
            .last()
            .map(|update| &update.value),
        Some(nexus_contracts::model_report::ModelEvidenceValue::Unknown(
            _
        ))
    ));
    let mut child = model_frame("root", 4, "child");
    child["row"]["id"] = serde_json::json!("child");
    assert!(!send_model(&bridge, child));
    assert_eq!(sink.updates.lock().unwrap().len(), 3);
    assert!(send_model(
        &bridge,
        model_frame("root", 5, "configured-new")
    ));
    assert!(!send_model(
        &bridge,
        model_frame("replacement", 6, "new-root")
    ));
    assert!(sink.closed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!send_model(&bridge, model_frame("root", 7, "late-old")));
    assert_eq!(sink.updates.lock().unwrap().len(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn native_model_bridge_drop_revokes_captured_sink_with_retained_listener() {
    let dir = tempdir().unwrap();
    let (sink, reporting) = native_capture();
    let bridge = HermesGatewayBridge::start_observed(
        SessionId("s_drop".into()),
        dir.path().join("bridge.sock"),
        "model-token".into(),
        Arc::new(CaptureSink::default()),
        Bell::new(),
        reporting,
    )
    .unwrap();
    drop(bridge);
    assert!(
        sink.closed.load(std::sync::atomic::Ordering::SeqCst),
        "bridge lifetime owns captured reporter"
    );
}

#[derive(Clone, Default)]
struct CaptureSink {
    events: Arc<Mutex<Vec<WsEvent>>>,
}

#[async_trait]
impl EventSink for CaptureSink {
    async fn emit(&self, event: WsEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hermes_gateway_bridge_delivers_batch_and_acks_on_surface() {
    let dir = tempdir().unwrap();
    let socket = dir.path().join("bridge.sock");
    let session = SessionId("s_hermes_gateway".into());
    let sink = CaptureSink::default();
    let bridge = HermesGatewayBridge::start(
        session,
        socket.clone(),
        "tok_test".into(),
        Arc::new(sink.clone()),
        Bell::new(),
    )
    .unwrap();
    assert_eq!(
        bridge.turn_completion_evidence(),
        TurnCompletionEvidence::ContextAccepted,
        "the generated adapter acknowledges only after Hermes starts processing the message"
    );

    let mut plugin = connect_bridge(&bridge);
    plugin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    plugin
        .write_all(br#"{"t":"subscribe","token":"tok_test"}"#)
        .unwrap();
    plugin.write_all(b"\n").unwrap();

    let batch = NexusBatch {
        counts: BatchCounts {
            dms: 1,
            thread: 0,
            total: 1,
        },
        dms: vec![BatchMessage {
            id: MessageId("m1".into()),
            from: "operator".into(),
            kind: Kind::Human,
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "hello hermes".into(),
            truncated: false,
        }],
        threads: Vec::new(),
        dm_message_ids: vec![MessageId("m1".into())],
        thread_message_ids: Vec::new(),
        message_ids: vec![MessageId("m1".into())],
    };
    let expected = render_batch(&batch);
    let send_bridge = bridge.clone();
    let send_expected = expected.clone();
    let send = tokio::spawn(async move { send_bridge.send_rendered_turn(&send_expected).await });

    let mut reader = BufReader::new(plugin.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.contains(r#""t":"incoming""#), "{line}");
    assert!(line.contains("hello hermes"), "{line}");

    let id = extract_json_string(&line, "id").expect("incoming id");
    writeln!(
        plugin,
        r#"{{"t":"delivered","id":"{id}","token":"tok_test"}}"#
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(2), send)
        .await
        .expect("delivery ack timeout")
        .expect("send task should not panic")
        .expect("send should ack");
}

#[tokio::test(flavor = "multi_thread")]
async fn hermes_gateway_bridge_settles_when_processing_starts_before_provider_completion() {
    let dir = tempdir().unwrap();
    let socket = dir.path().join("bridge.sock");
    let bridge = HermesGatewayBridge::start(
        SessionId("s_hermes_processing_started".into()),
        socket.clone(),
        "tok_processing_started".into(),
        Arc::new(CaptureSink::default()),
        Bell::new(),
    )
    .unwrap();

    let mut plugin = connect_bridge(&bridge);
    plugin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    writeln!(
        plugin,
        r#"{{"t":"subscribe","token":"tok_processing_started"}}"#
    )
    .unwrap();

    let send_bridge = bridge.clone();
    let send =
        tokio::spawn(async move { send_bridge.send_rendered_turn("provider may stall").await });
    let mut reader = BufReader::new(plugin.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let id = extract_json_string(&line, "id").expect("incoming id");

    writeln!(
        plugin,
        r#"{{"t":"processing_started","id":"{id}","token":"tok_processing_started"}}"#
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(2), send)
        .await
        .expect("processing-start receipt must settle before provider completion")
        .expect("send task should not panic")
        .expect("processing-start receipt should settle delivery");
}

#[tokio::test(flavor = "multi_thread")]
async fn hermes_gateway_bridge_serializes_turns_until_terminal_after_context_acceptance() {
    let dir = tempdir().unwrap();
    let socket = dir.path().join("bridge.sock");
    let bridge = HermesGatewayBridge::start(
        SessionId("s_hermes_serial".into()),
        socket.clone(),
        "tok_serial".into(),
        Arc::new(CaptureSink::default()),
        Bell::new(),
    )
    .unwrap();

    let mut plugin = connect_bridge(&bridge);
    plugin
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    writeln!(plugin, r#"{{"t":"subscribe","token":"tok_serial"}}"#).unwrap();

    let first_bridge = bridge.clone();
    let first = tokio::spawn(async move { first_bridge.send_rendered_turn("first").await });
    let mut reader = BufReader::new(plugin.try_clone().unwrap());
    let mut first_line = String::new();
    reader.read_line(&mut first_line).unwrap();
    assert!(first_line.contains("first"), "{first_line}");

    let first_id = extract_json_string(&first_line, "id").expect("first incoming id");
    writeln!(
        plugin,
        r#"{{"t":"processing_started","id":"{first_id}","token":"tok_serial"}}"#
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("first processing-start receipt timeout")
        .expect("first send task should not panic")
        .expect("first processing-start receipt should settle transport");

    let second_bridge = bridge.clone();
    let second = tokio::spawn(async move { second_bridge.send_rendered_turn("second").await });
    let mut premature = String::new();
    let premature_read = reader.read_line(&mut premature);
    assert!(
        premature_read.is_err(),
        "second turn reached busy Hermes before the first delivery settled: {premature}"
    );

    writeln!(
        plugin,
        r#"{{"t":"delivered","id":"{first_id}","token":"tok_serial"}}"#
    )
    .unwrap();

    plugin
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut second_line = String::new();
    reader.read_line(&mut second_line).unwrap();
    assert!(second_line.contains("second"), "{second_line}");
    let second_id = extract_json_string(&second_line, "id").expect("second incoming id");
    writeln!(
        plugin,
        r#"{{"t":"processing_started","id":"{second_id}","token":"tok_serial"}}"#
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("second processing-start receipt timeout")
        .expect("second send task should not panic")
        .expect("second processing-start receipt should settle transport");
    writeln!(
        plugin,
        r#"{{"t":"delivered","id":"{second_id}","token":"tok_serial"}}"#
    )
    .unwrap();
}

#[tokio::test]
async fn hermes_gateway_bridge_projects_agent_updates_from_plugin_events() {
    let dir = tempdir().unwrap();
    let socket = dir.path().join("bridge.sock");
    let session = SessionId("s_hermes_stream".into());
    let sink = CaptureSink::default();
    let bridge = HermesGatewayBridge::start(
        session.clone(),
        socket.clone(),
        "tok_stream".into(),
        Arc::new(sink.clone()),
        Bell::new(),
    )
    .unwrap();

    let mut plugin = connect_bridge(&bridge);
    writeln!(plugin, r#"{{"t":"subscribe","token":"tok_stream"}}"#).unwrap();
    writeln!(
        plugin,
        r#"{{"t":"agent_update","token":"tok_stream","kind":"text","data":{{"text":"hi from hermes"}}}}"#
    )
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let events = sink.events.lock().unwrap().clone();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0],
        WsEvent::AgentUpdate {
            session_id,
            kind: AgentUpdateKind::Text,
            data,
        } if session_id == &session && data["text"] == "hi from hermes"
    ));
}

#[test]
fn hermes_gateway_profile_is_isolated_and_loads_nexus_platform() {
    let dir = tempdir().unwrap();
    let source_home = dir.path().join("source-hermes");
    std::fs::create_dir_all(&source_home).unwrap();
    std::fs::write(
        source_home.join("config.yaml"),
        concat!(
            "model:\n  default: gpt-5.5\n  provider: openai-codex\n",
            "providers: {}\n",
            "fallback_providers: []\n",
            "plugins:\n  enabled: [other]\n",
            "platforms:\n  slack:\n    enabled: true\n",
        ),
    )
    .unwrap();
    std::fs::write(
        source_home.join("auth.json"),
        r#"{"providers":{"openai-codex":{}}}"#,
    )
    .unwrap();
    let profile = HermesGatewayProfile {
        home: source_home.join("profiles/nexus-test"),
        source_home,
        bridge_socket: dir.path().join("bridge.sock"),
        bridge_token: "tok_profile".into(),
        nexus_name: "hermes-one".into(),
        session_id: SessionId("s_hermes_one".into()),
    };

    write_hermes_gateway_profile(&profile).unwrap();

    let config = std::fs::read_to_string(profile.home.join("config.yaml")).unwrap();
    let init = std::fs::read_to_string(profile.home.join("plugins/nexus/__init__.py")).unwrap();
    let adapter = std::fs::read_to_string(profile.home.join("plugins/nexus/adapter.py")).unwrap();
    let bridge_client =
        std::fs::read_to_string(profile.home.join("plugins/nexus/bridge_client.py")).unwrap();
    assert!(config.contains("enabled: [nexus]"));
    assert!(
        config.lines().any(|line| line == "platforms:")
            && config.lines().any(|line| line == "  nexus:")
            && config.lines().any(|line| line == "    enabled: true"),
        "Hermes v0.17 reads enabled adapters from top-level platforms, not gateway.platforms"
    );
    assert!(config.contains("model:\n  default: gpt-5.5\n  provider: openai-codex"));
    assert!(config.contains("providers: {}"));
    assert!(config.contains("fallback_providers: []"));
    assert!(!config.contains("enabled: [other]"));
    assert!(!config.contains("slack:"));
    assert!(
        !profile.home.join("auth.json").exists(),
        "Nexus runtime profiles must use Hermes global machine auth without copying auth.json"
    );
    let skill = std::fs::read_to_string(profile.home.join("skills/nexus-bus/SKILL.md"))
        .expect("headed Hermes profile must install its launch-pinned Nexus bus skill");
    assert!(skill.contains("post <thread>"));
    assert!(skill.contains("dm <name>"));
    assert!(
        skill.contains(std::env::current_exe().unwrap().to_string_lossy().as_ref()),
        "headed Hermes terminal tools sanitize PATH, so the isolated skill must pin this Nexus binary"
    );
    assert!(
        !config.contains("gateway:\n  platforms:"),
        "gateway.platforms is ignored by Hermes v0.17 and leaves the Nexus adapter disabled"
    );
    assert!(config.contains("approvals:"));
    assert!(config.contains("mode: off"));
    assert!(profile.home.join("plugins/nexus/plugin.yaml").is_file());
    assert!(profile.home.join("plugins/nexus/adapter.py").is_file());
    assert!(profile
        .home
        .join("plugins/nexus/bridge_client.py")
        .is_file());
    assert!(
        !profile.home.join("plugins/nexus/hooks.py").exists(),
        "Hermes message.start/delta/complete are stream events, not plugin hooks"
    );
    assert!(
        !init.contains("register_hook"),
        "the plugin must not register nonexistent message stream hooks"
    );
    assert!(
        init.contains("ctx.register_platform(")
            && init.contains("name=\"nexus\"")
            && init.contains("adapter_factory=lambda cfg: NexusAdapter(cfg)")
            && init.contains("check_fn=lambda: True"),
        "Hermes v0.17 platform plugins must register with the keyword platform API"
    );
    assert!(init.contains("You are hermes-one (session s_hermes_one)"));
    assert!(init.contains("Never sign messages as anyone else"));
    assert!(init.contains("nexus whoami"));
    assert!(
        adapter.contains("async def edit_message"),
        "streaming deltas reach platform adapters through edit_message"
    );
    assert!(
        adapter.contains("async def connect(self, is_reconnect: bool = False)"),
        "Hermes 0.18 calls platform connect with the reconnect-state keyword; the optional argument preserves older Hermes compatibility"
    );
    assert!(
        adapter.contains("async def on_processing_start")
            && adapter.contains("self._client.processing_started(event.message_id)"),
        "the bridge delivery receipt must come from Hermes' background-processing boundary"
    );
    assert!(
        adapter.contains("async def on_processing_complete")
            && adapter.contains("self._client.delivered(event.message_id)"),
        "Hermes terminal completion must still release the serialized lane"
    );
    assert!(
        !adapter.contains("fut.add_done_callback"),
        "handle_message only schedules Hermes' background turn, so its future is not completion evidence"
    );
    assert!(
        adapter.contains("self._startup_ready = asyncio.Event()")
            && adapter.contains("async def _wait_for_startup_settlement")
            && adapter.contains("while self._gateway_startup_is_busy():")
            && adapter.contains("await self._startup_ready.wait()"),
        concat!(
            "Nexus input must stay on the lossless bridge until Hermes finishes restart auto-resume; ",
            "feeding it into Hermes' single pending-message slot during startup can overwrite it"
        )
    );
    assert!(
        bridge_client.contains("endpoint.startswith(\"tcp://\")")
            && bridge_client.contains("socket.create_connection((host, int(port)))"),
        "the generated Hermes bridge client must connect to the loopback TCP endpoint used on Windows"
    );
}

#[test]
fn hermes_gateway_keeps_connect_and_turn_completion_deadlines_separate() {
    let source = include_str!("../src/daemon/hermes_gateway.rs");

    assert!(source.contains("const ADAPTER_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);"));
    assert!(source.contains("const TURN_COMPLETION_TIMEOUT: Duration = Duration::from_secs(600);"));
    assert!(source.contains("Instant::now() + TURN_COMPLETION_TIMEOUT"));
}

fn extract_json_string(line: &str, field: &str) -> Option<String> {
    let marker = format!(r#""{field}":""#);
    let start = line.find(&marker)? + marker.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

#[cfg(unix)]
fn connect_bridge(bridge: &HermesGatewayBridge) -> BridgeTestStream {
    BridgeTestStream::connect(bridge.endpoint()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn native_model_generated_hook_reads_only_framework_selected_row() {
    let dir = tempdir().unwrap();
    let home = dir.path().join("home");
    let (sink, reporting) = native_capture();
    let bridge = HermesGatewayBridge::start_observed(
        SessionId("s_hook".into()),
        dir.path().join("bridge.sock"),
        "model-token".into(),
        Arc::new(CaptureSink::default()),
        Bell::new(),
        reporting,
    )
    .unwrap();
    write_hermes_gateway_profile(&HermesGatewayProfile {
        home: home.clone(),
        source_home: dir.path().join("source"),
        bridge_socket: dir.path().join("bridge.sock"),
        bridge_token: "model-token".into(),
        nexus_name: "test".into(),
        session_id: SessionId("s_hook".into()),
    })
    .unwrap();
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/model_reporting/native.json");
    let python = if cfg!(windows) {
        "python.exe"
    } else {
        "python3"
    };
    let output = std::process::Command::new(python).arg("-c").arg(r#"
import asyncio, importlib.util, json, os, pathlib, sqlite3, sys
home=pathlib.Path(os.environ['HERMES_HOME'])
spec=importlib.util.spec_from_file_location('captured_hook',home/'hooks/nexus-model/handler.py')
hook=importlib.util.module_from_spec(spec); spec.loader.exec_module(hook)
db=sqlite3.connect(home/'state.db')
db.execute('CREATE TABLE sessions(id TEXT PRIMARY KEY, model TEXT, model_config TEXT, parent_session_id TEXT, ended_at REAL)')
for event in json.loads(pathlib.Path(sys.argv[1]).read_text())['rows']['hermes.headed']['events']:
    row=event['payload']
    db.execute('INSERT INTO sessions VALUES(?,?,?,?,?)',(row['id'],row['model'],row['model_config'],row['parent_session_id'],row['ended_at']))
db.commit()
ctx={'platform':'nexus','user_id':'nexus','chat_id':'nexus','thread_id':'','chat_type':'dm','session_id':'fixture-root'}
async def run():
    await hook.handle('agent:start',{**ctx,'chat_id':'foreign','session_id':'fixture-foreign'})
    await hook.handle('agent:start',ctx)
    for column in ('input_tokens','output_tokens','cache_read_tokens','cache_write_tokens','reasoning_tokens','api_call_count'):
        db.execute('ALTER TABLE sessions ADD COLUMN '+column+' INTEGER DEFAULT 0')
    db.execute('UPDATE sessions SET input_tokens=120,output_tokens=30,cache_read_tokens=80,cache_write_tokens=20,reasoning_tokens=12,api_call_count=3 WHERE id=?',('fixture-root',)); db.commit()
    db.execute('UPDATE sessions SET model=? WHERE id=?',('opaque/next','fixture-root')); db.commit()
    await hook.handle('agent:end',ctx)
    # A later root cannot turn a stale launch into a new native owner.
    await hook.handle('agent:start',{**ctx,'session_id':'replacement'})
asyncio.run(run())
"#).arg(fixture).env("HERMES_HOME",&home).env("NEXUS_HERMES_BRIDGE_SOCKET",bridge.endpoint())
        .env("NEXUS_HERMES_BRIDGE_TOKEN","model-token").output().expect("python3 is required for generated hook gate");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let updates = sink.updates.lock().unwrap();
    assert_eq!(
        updates.len(),
        2,
        "actual generated hook must publish both exact native row snapshots"
    );
    for update in updates.iter() {
        assert_eq!(update.native_session_id, "fixture-root");
        assert_eq!(
            update.field,
            nexus_contracts::model_report::ModelEvidenceField::Configured
        );
    }
    let nexus_contracts::model_report::ModelEvidenceValue::Observed(last) = &updates[1].value
    else {
        panic!("native configured row");
    };
    assert_eq!(last.model_id, "opaque/next");
    let telemetry = sink.telemetry.lock().unwrap();
    assert_eq!(
        telemetry.len(),
        2,
        "old-schema model read survives; upgraded row supplies usage"
    );
    assert!(matches!(
        &telemetry[0],
        nexus_contracts::telemetry::NativeTelemetryUpdate::Usage {
            value: nexus_contracts::telemetry::NativeTelemetryValue::Unknown,
            ..
        }
    ));
    let nexus_contracts::telemetry::NativeTelemetryUpdate::Usage {
        value: nexus_contracts::telemetry::NativeTelemetryValue::Observed(value),
        ..
    } = &telemetry[1]
    else {
        panic!("actual exact-row query must capture usage")
    };
    assert_eq!(value.input_tokens.unwrap().get(), 120);
    assert_eq!(value.cache_write_tokens.unwrap().get(), 20);
    assert!(sink.closed.load(std::sync::atomic::Ordering::SeqCst));
}

#[cfg(windows)]
fn connect_bridge(bridge: &HermesGatewayBridge) -> BridgeTestStream {
    let address = bridge
        .endpoint()
        .strip_prefix("tcp://")
        .expect("Windows Hermes bridge endpoint must be loopback TCP");
    BridgeTestStream::connect(address).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn native_model_parked_hook_cannot_publish_into_replacement_bridge() {
    let dir = tempdir().unwrap();
    let home = dir.path().join("home");
    let socket = dir.path().join("bridge.sock");
    let (old, reporting) = native_capture();
    let bridge = HermesGatewayBridge::start_observed(
        SessionId("s_replace".into()),
        socket.clone(),
        "old-token".into(),
        Arc::new(CaptureSink::default()),
        Bell::new(),
        reporting,
    )
    .unwrap();
    write_hermes_gateway_profile(&HermesGatewayProfile {
        home: home.clone(),
        source_home: dir.path().join("source"),
        bridge_socket: socket.clone(),
        bridge_token: "old-token".into(),
        nexus_name: "test".into(),
        session_id: SessionId("s_replace".into()),
    })
    .unwrap();
    let python = if cfg!(windows) {
        "python.exe"
    } else {
        "python3"
    };
    let timing = hermes_parked_timing::Timeline::before_spawn();
    let child = std::process::Command::new(python).arg("-c").arg(r#"
import sys, time
started=time.monotonic()
def phase(name):
    print('parked-hook phase='+name+' elapsed='+str(time.monotonic()-started),file=sys.stderr,flush=True)
phase('python-started')
import asyncio, importlib.util, os, pathlib, sqlite3
phase('imports-complete')
home=pathlib.Path(os.environ['HERMES_HOME'])
db=sqlite3.connect(home/'state.db')
db.execute('CREATE TABLE sessions(id TEXT PRIMARY KEY, model TEXT, model_config TEXT, parent_session_id TEXT, ended_at REAL)')
db.execute('INSERT INTO sessions VALUES(?,?,?,?,?)',('old-root','old-model','{}',None,None)); db.commit(); db.close()
phase('database-ready')
spec=importlib.util.spec_from_file_location('captured_hook',home/'hooks/nexus-model/handler.py')
hook=importlib.util.module_from_spec(spec); spec.loader.exec_module(hook)
phase('hook-imported')
read=hook._row
def parked(root):
    phase('row-read-entered')
    row=read(root)
    assert row is not None and row['id']=='old-root' and row['model']=='old-model', 'exact fixture row unavailable'
    phase('row-read-complete')
    print('exact native row captured',flush=True)
    assert sys.stdin.readline().strip()=='release'
    phase('release-received')
    return row
hook._row=parked
phase('handle-entered')
asyncio.run(hook.handle('agent:start',{'platform':'nexus','user_id':'nexus','chat_id':'nexus','thread_id':'','chat_type':'dm','session_id':'old-root'}))
phase('handle-complete')
"#).env("HERMES_HOME", &home).env("NEXUS_HERMES_BRIDGE_SOCKET",bridge.endpoint())
        .env("NEXUS_HERMES_BRIDGE_TOKEN","old-token")
        .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped()).spawn().expect("python3 is required");
    timing.record("spawn-return");
    struct ChildGuard(std::process::Child, hermes_parked_timing::Timeline);
    impl ChildGuard {
        fn stop(&mut self) {
            self.1
                .record(&format!("owned-child-kill-request pid={}", self.0.id()));
            let _ = self.0.kill();
            self.1.record("owned-child-kill-return");
            let _ = self.0.wait();
            self.1.record("owned-child-wait-return");
        }
    }
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            self.stop();
        }
    }
    let mut child = ChildGuard(child, timing.clone());
    // Drain concurrently so a diagnostic pipe cannot itself stall the child.
    // Keep only a bounded tail; these are hermetic fixture phases, never provider data.
    let captured_timing = timing.clone();
    let mut error_output = child.0.stderr.take().unwrap();
    let stderr_reader = std::thread::spawn(move || {
        let mut chunk = [0_u8; 1024];
        loop {
            match error_output.read(&mut chunk) {
                Ok(0) => break,
                Ok(count) => captured_timing.stderr_chunk(&chunk[..count]),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    });
    let output = child.0.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let stdout_reader = std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(output).read_line(&mut line).map(|_| line);
        let _ = tx.send(result);
    });
    let captured = timing.receive_marker(&rx);
    if !matches!(&captured, Ok(Ok(line)) if line.trim() == "exact native row captured") {
        let status = child.0.try_wait();
        child.stop();
        let _ = stdout_reader.join();
        let _ = stderr_reader.join();
        panic!(
            "parked hook did not capture its exact row within the unchanged 3s watchdog; marker={captured:?}; child_status={status:?}; {}",
            timing.report(),
        );
    }
    drop(bridge);
    assert!(old.closed.load(std::sync::atomic::Ordering::SeqCst));
    let (new, reporting) = native_capture();
    let replacement = HermesGatewayBridge::start_observed(
        SessionId("s_replace".into()),
        socket,
        "model-token".into(),
        Arc::new(CaptureSink::default()),
        Bell::new(),
        reporting,
    )
    .unwrap();
    // Windows uses a different ephemeral endpoint; Unix deliberately reuses the socket path.
    // Both retain OLD's captured token, never replacing it with NEW's authority.
    writeln!(child.0.stdin.take().unwrap(), "release").unwrap();
    timing.record("release-sent");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    timing.record("finish-watchdog-start");
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            timing.record("child-exit-observed");
            break status;
        }
        if std::time::Instant::now() >= deadline {
            timing.record("finish-watchdog-timeout");
            child.stop();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            panic!(
                "parked hook did not finish within the unchanged 3s watchdog; {}",
                timing.report(),
            );
        }
        tokio::task::yield_now().await;
    };
    let _ = stdout_reader.join();
    let _ = stderr_reader.join();
    eprintln!("parked-hook timing: {}", timing.report());
    assert!(
        status.success(),
        "parked hook failed after release: status={status}; {}",
        timing.report(),
    );
    assert!(old.updates.lock().unwrap().is_empty());
    assert!(new.updates.lock().unwrap().is_empty());
    assert!(!new.closed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(send_model(
        &replacement,
        model_frame("new-root", 1, "new-model")
    ));
    assert_eq!(new.updates.lock().unwrap().len(), 1);
}
