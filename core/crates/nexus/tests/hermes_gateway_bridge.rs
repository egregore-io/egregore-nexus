use std::io::{BufRead, BufReader, Write};
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
        auto_reply_note: None,
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

#[cfg(windows)]
fn connect_bridge(bridge: &HermesGatewayBridge) -> BridgeTestStream {
    let address = bridge
        .endpoint()
        .strip_prefix("tcp://")
        .expect("Windows Hermes bridge endpoint must be loopback TCP");
    BridgeTestStream::connect(address).unwrap()
}
