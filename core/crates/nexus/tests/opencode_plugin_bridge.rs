use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use nexus::daemon::opencode_plugin_bridge::{
    write_opencode_plugin_files, OpenCodePluginBridge, OpenCodePluginBridgeOptions,
};
use nexus_contracts::events::{AgentUpdateKind, WsEvent};
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_pty::HarnessInput;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn node_available() -> bool {
    std::process::Command::new("node")
        .arg("--version")
        .output()
        .is_ok()
}

#[derive(Default)]
struct CaptureSink {
    events: Mutex<Vec<WsEvent>>,
}

#[async_trait]
impl EventSink for CaptureSink {
    async fn emit(&self, event: WsEvent) {
        self.events.lock().unwrap().push(event);
    }
}

impl CaptureSink {
    fn events(&self) -> Vec<WsEvent> {
        self.events.lock().unwrap().clone()
    }
}

struct NativeCapture {
    profile: nexus_contracts::model_report::ModelProfileIdentity,
    closed: std::sync::atomic::AtomicBool,
    root: Mutex<Option<String>>,
    updates: Mutex<Vec<nexus_contracts::model_report::NativeModelUpdate>>,
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
}
fn native_capture() -> (
    Arc<NativeCapture>,
    nexus_agent::adapter::NativeModelReporting,
) {
    let profile = nexus_agent::adapter::opencode::native::model_profile();
    let sink = Arc::new(NativeCapture {
        profile: profile.identity().clone(),
        closed: false.into(),
        root: Mutex::new(None),
        updates: Mutex::new(vec![]),
    });
    let reporting = profile.capture(sink.clone()).unwrap();
    (sink, reporting)
}

#[tokio::test]
async fn model_bridge_requires_captured_root_and_bounds_owner_lifetime() {
    use nexus_contracts::model_report::ModelEvidenceValue;
    let (sink, reporting) = native_capture();
    let bridge = OpenCodePluginBridge::start_observed(
        SessionId("s_model".into()),
        Arc::new(CaptureSink::default()),
        Default::default(),
        reporting.clone(),
    )
    .await
    .unwrap();
    let info = json!({"sessionID":"ses_exact","id":"msg_one","role":"assistant","modelID":"native-selected","providerID":"opaque"});
    bridge
        .http_json(
            "POST",
            "/model",
            Some(info.clone()),
            bridge.endpoint().token(),
        )
        .await;
    assert!(
        sink.updates.lock().unwrap().is_empty(),
        "events cannot supply the root"
    );
    assert!(bridge.bind_model_root("ses_exact"));
    assert!(!bridge.bind_model_root("ses_replacement"));
    assert_eq!(
        bridge.with_current_model_binding(&reporting, |root| root.to_owned()),
        Some("ses_exact".into())
    );
    let (_, foreign) = native_capture();
    assert_eq!(
        bridge.with_current_model_binding(&foreign, |_| panic!("foreign callback")),
        None::<()>
    );
    let (status, _) = bridge
        .http_json("POST", "/model", Some(info.clone()), "wrong-token")
        .await;
    assert_eq!(status, 401);
    let mut child = info.clone();
    child["sessionID"] = json!("ses_child");
    bridge
        .http_json("POST", "/model", Some(child), bridge.endpoint().token())
        .await;
    assert!(sink.updates.lock().unwrap().is_empty());
    for _ in 0..2 {
        assert_eq!(
            bridge
                .http_json(
                    "POST",
                    "/model",
                    Some(info.clone()),
                    bridge.endpoint().token()
                )
                .await
                .0,
            204
        );
    }
    let mut repeated_without_model = info.clone();
    repeated_without_model
        .as_object_mut()
        .unwrap()
        .remove("modelID");
    bridge
        .http_json(
            "POST",
            "/model",
            Some(repeated_without_model),
            bridge.endpoint().token(),
        )
        .await;
    let updates = sink.updates.lock().unwrap().clone();
    assert_eq!(
        updates.len(),
        1,
        "repeated cumulative message is not new evidence"
    );
    assert!(
        matches!(&updates[0].value, ModelEvidenceValue::Observed(value) if value.model_id == "native-selected")
    );
    bridge.shutdown();
    assert!(sink.closed.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        bridge.with_current_model_binding(&reporting, |_| true),
        None
    );
    let (_new_sink, new) = native_capture();
    assert!(new.sink().accepts_profile(new.profile().identity()));
}

#[tokio::test]
async fn model_bridge_replay_exhaustion_and_drop_close_only_captured_reporting() {
    let (sink, reporting) = native_capture();
    let bridge = OpenCodePluginBridge::start_observed(
        SessionId("s_exhaust".into()),
        Arc::new(CaptureSink::default()),
        Default::default(),
        reporting,
    )
    .await
    .unwrap();
    assert!(bridge.bind_model_root("ses_exact"));
    let input = bridge.input();
    bridge.http_json("POST", "/model", Some(json!({"sessionID":"ses_exact","id":"m".repeat(262145),"role":"assistant","modelID":"native"})), bridge.endpoint().token()).await;
    assert!(sink.closed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(sink.updates.lock().unwrap().is_empty());
    assert!(
        input.is_alive(),
        "model exhaustion does not change delivery policy"
    );
    let (other, reporting) = native_capture();
    let other_bridge = OpenCodePluginBridge::start_observed(
        SessionId("s_drop".into()),
        Arc::new(CaptureSink::default()),
        Default::default(),
        reporting,
    )
    .await
    .unwrap();
    let retained_input = other_bridge.input();
    drop(other_bridge);
    assert!(other.closed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!retained_input.is_alive());
}

#[tokio::test]
async fn model_generated_plugin_forwards_original_metadata_to_actual_bridge() {
    // Node is required for this integration gate: never silently turn it into a passing skip.
    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_generated_model".into())).unwrap();
    let (sink, reporting) = native_capture();
    let bridge = OpenCodePluginBridge::start_observed(
        SessionId("s_generated_model".into()),
        Arc::new(CaptureSink::default()),
        Default::default(),
        reporting,
    )
    .await
    .unwrap();
    assert!(bridge.bind_model_root("ses_exact"));
    let script = r#"
import { pathToFileURL } from 'node:url';
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, init) => {
  if (String(url).endsWith('/turn/next')) return new Promise(() => {});
  if (String(url).startsWith('http://native.invalid/')) return new Response(JSON.stringify({id:'ses_exact'}), {status:200});
  return realFetch(url, init);
};
const { nexus } = await import(pathToFileURL(process.argv[1]).href);
const hooks = await nexus();
await hooks.event({event:{type:'session.created',properties:{info:{id:'ses_foreign'}}}});
await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_foreign',id:'m_child',role:'assistant',modelID:'child'}}}});
await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_exact',id:'m_real',role:'assistant',modelID:'selected-opaque',providerID:'native-provider',time:{created:123}}}}});
process.exit(0);
"#;
    let output = tokio::process::Command::new("node")
        .args(["--input-type=module", "--eval", script])
        .arg(&files.plugin_path)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("NEXUS_OPENCODE_BRIDGE_URL", bridge.endpoint().base_url())
        .env("NEXUS_OPENCODE_BRIDGE_TOKEN", bridge.endpoint().token())
        .env("NEXUS_OPENCODE_SERVER_URL", "http://native.invalid")
        .env("OPENCODE_SERVER_PASSWORD", "fixture")
        .env("NEXUS_NAME", "fixture")
        .env("NEXUS_OPENCODE_PROMPT_MODEL", "must-not-report/request")
        .kill_on_drop(true)
        .output()
        .await
        .expect("required Node fixture runtime");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let updates = sink.updates.lock().unwrap();
    assert_eq!(updates.len(), 1);
    let nexus_contracts::model_report::ModelEvidenceValue::Observed(value) = &updates[0].value
    else {
        panic!("observed")
    };
    assert_eq!(value.model_id, "selected-opaque");
    assert_eq!(value.provider_id.as_deref(), Some("native-provider"));
    assert_eq!(value.native_message_id.as_deref(), Some("m_real"));
}

#[tokio::test]
async fn send_turn_waits_until_plugin_reports_completion() {
    let sink = Arc::new(CaptureSink::default());
    let bridge = OpenCodePluginBridge::start(
        SessionId("s_opencode_plugin_waits".into()),
        sink,
        OpenCodePluginBridgeOptions {
            turn_timeout: Duration::from_secs(2),
        },
    )
    .await
    .expect("bridge starts");

    let input = bridge.input();
    let pending = tokio::spawn(async move { input.send_turn("hello opencode").await });

    let (_status, body) = bridge
        .http_json("GET", "/turn/next", None, bridge.endpoint().token())
        .await;
    assert_eq!(body["text"], "hello opencode");
    let turn_id = body["id"].as_str().expect("turn id").to_string();

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !pending.is_finished(),
        "delivery must stay pending until the plugin reports turn completion"
    );

    let (status, _body) = bridge
        .http_json(
            "POST",
            &format!("/turn/{turn_id}/complete"),
            Some(json!({})),
            bridge.endpoint().token(),
        )
        .await;
    assert_eq!(status, 204);
    pending
        .await
        .expect("send task joins")
        .expect("turn completes");
}

#[tokio::test]
async fn send_turn_preserves_structured_provider_error_payload() {
    let sink = Arc::new(CaptureSink::default());
    let bridge = OpenCodePluginBridge::start(
        SessionId("s_opencode_plugin_provider_error".into()),
        sink,
        OpenCodePluginBridgeOptions {
            turn_timeout: Duration::from_secs(2),
        },
    )
    .await
    .expect("bridge starts");

    let input = bridge.input();
    let pending = tokio::spawn(async move { input.send_turn("hit provider limit").await });

    let (_status, body) = bridge
        .http_json("GET", "/turn/next", None, bridge.endpoint().token())
        .await;
    let turn_id = body["id"].as_str().expect("turn id").to_string();

    let (status, _body) = bridge
        .http_json(
            "POST",
            &format!("/turn/{turn_id}/error"),
            Some(json!({
                "error": "OpenCode provider rejected the turn",
                "providerError": {
                    "reason": "rate_limit",
                    "retryAfterMs": 1200,
                    "provider": "openrouter",
                    "model": "free-model"
                }
            })),
            bridge.endpoint().token(),
        )
        .await;
    assert_eq!(status, 204);

    let err = pending
        .await
        .expect("send task joins")
        .expect_err("provider error completes the turn with a structured error");
    let payload = err
        .strip_prefix("__nexus_opencode_provider_error__:")
        .expect("structured bridge errors use the classifier sentinel");
    let payload: Value = serde_json::from_str(payload).expect("structured provider-error json");
    assert_eq!(payload["error"], "OpenCode provider rejected the turn");
    assert_eq!(payload["providerError"]["reason"], "rate_limit");
    assert_eq!(payload["providerError"]["provider"], "openrouter");
    assert_eq!(payload["providerError"]["model"], "free-model");
}

#[tokio::test]
async fn plugin_events_emit_agent_updates() {
    let sink = Arc::new(CaptureSink::default());
    let bridge = OpenCodePluginBridge::start(
        SessionId("s_opencode_plugin_events".into()),
        sink.clone(),
        OpenCodePluginBridgeOptions {
            turn_timeout: Duration::from_secs(2),
        },
    )
    .await
    .expect("bridge starts");

    let (status, _body) = bridge
        .http_json(
            "POST",
            "/event",
            Some(json!({
                "kind": "text",
                "data": { "text": "streamed delta" }
            })),
            bridge.endpoint().token(),
        )
        .await;
    assert_eq!(status, 204);

    assert_eq!(
        sink.events(),
        vec![WsEvent::AgentUpdate {
            session_id: SessionId("s_opencode_plugin_events".into()),
            kind: AgentUpdateKind::Text,
            data: json!({ "text": "streamed delta" }),
        }]
    );
}

#[tokio::test]
async fn plugin_bridge_rejects_bad_tokens() {
    let sink = Arc::new(CaptureSink::default());
    let bridge = OpenCodePluginBridge::start(
        SessionId("s_opencode_plugin_auth".into()),
        sink,
        OpenCodePluginBridgeOptions {
            turn_timeout: Duration::from_secs(2),
        },
    )
    .await
    .expect("bridge starts");

    let (status, _body) = bridge
        .http_json("GET", "/turn/next", None, "wrong-token")
        .await;
    assert_eq!(status, 401);
}

#[test]
fn generated_assets_use_opencode_serve_attach_and_plugin_events() {
    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_opencode_assets".into())).unwrap();

    let plugin = std::fs::read_to_string(files.plugin_path).unwrap();
    let serve = std::fs::read_to_string(files.serve_path).unwrap();

    assert!(files.ready_path.ends_with("ready.json"));
    assert!(plugin.contains("/prompt_async"));
    assert!(plugin.contains("resolvePromptContext()"));
    assert!(plugin.contains("body.model = prompt.model"));
    assert!(plugin.contains("eventProviderError(event.properties)"));
    assert!(plugin.contains("providerError"));
    assert!(plugin.contains("process.env.NEXUS_NAME?.trim() || process.env.NEXUS_AGENT_ID?.trim()"));
    assert!(plugin.contains("eventErrorMessage(event.properties)"));
    assert!(plugin.contains("message.part.updated"));
    assert!(plugin.contains("event.properties?.status?.type === \"idle\""));
    assert!(plugin.contains("queueMicrotask(() => void pollTurns())"));
    assert!(plugin.contains("/turn/next"));
    assert!(plugin.contains("/event"));
    assert!(serve.contains("NEXUS_OPENCODE_READY_PATH"));
    assert!(serve.contains("opencode-${platform}-${arch}"));
    assert!(serve.contains("const provider = packageBase.slice(0, packageBase.indexOf(\"-\"))"));
    assert!(serve.contains("join(modules, providerPackage, \"bin\", stagedBinary)"));
    assert!(serve.contains("writeFileSync(readyTempPath"));
    assert!(serve.contains("renameSync(readyTempPath, readyPath)"));
    assert!(serve.contains("const readyTempPath = `${readyPath}.tmp-${process.pid}`"));
    assert!(
        serve.find("writeFileSync(readyTempPath").unwrap()
            > serve.find("const tui = spawn").unwrap(),
        "serve shim must report ready only after the foreground attach process starts"
    );
    assert!(serve.contains("\"serve\", \"--hostname\", \"127.0.0.1\""));
    assert!(serve.contains("OPENCODE_CONFIG_CONTENT"));
    assert!(serve.contains("splitNativeArgs(nativeArgs)"));
    assert!(serve.contains("config.model = native.model"));
    assert!(serve.contains("\"attach\", url, \"--session\", id"));
    assert!(serve.contains("...native.attachArgs"));
    assert!(serve
        .contains("const resumeIsolated = process.env.NEXUS_OPENCODE_RESUME_ISOLATED === \"1\""));
    assert!(serve.contains("const isolatedStore = !native.session || resumeIsolated"));
    assert!(serve.contains("if (!isolatedStore) delete baseEnv.OPENCODE_DB"));
    assert!(serve.contains("...(isolatedStore ? { OPENCODE_DB: dbPath } : {})"));
    assert!(serve.contains("NEXUS_OPENCODE_SESSION_ID: native.session ?? \"\""));
    assert!(serve.contains("NEXUS_OPENCODE_PROMPT_MODEL: native.model ?? \"\""));
    assert!(serve.contains(
        "NEXUS_OPENCODE_PROMPT_AGENT: native.agent ?? (native.model ? \"nexus\" : \"\")"
    ));
    assert!(serve.contains("delete tuiEnv.OPENCODE_CONFIG_CONTENT"));
}

#[test]
fn generated_assets_are_valid_node_modules_when_node_is_available() {
    if !node_available() {
        eprintln!("[skip] node is not available");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_opencode_assets_node".into()))
            .unwrap();

    for path in [files.plugin_path, files.serve_path] {
        let output = std::process::Command::new("node")
            .arg("--check")
            .arg(&path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "node --check failed for {}:\n{}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn generated_plugin_defers_a_claimed_turn_that_arrives_while_opencode_is_busy() {
    if !node_available() {
        eprintln!("[skip] node is not available");
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_opencode_busy_arrival".into()))
            .unwrap();
    let driver = tmp.path().join("busy-arrival-regression.mjs");
    std::fs::write(
        &driver,
        r#"
import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";

process.env.NEXUS_OPENCODE_BRIDGE_URL = "http://bridge";
process.env.NEXUS_OPENCODE_BRIDGE_TOKEN = "bridge-token";
process.env.NEXUS_OPENCODE_SERVER_URL = "http://opencode";
process.env.OPENCODE_SERVER_PASSWORD = "server-password";
process.env.NEXUS_NAME = "busy-arrival";
process.env.NEXUS_PROJECT = "test";
process.env.NEXUS_OPENCODE_PROMPT_MODEL = "opencode-go/deepseek-v4-flash";

let resolveFirstTurn;
let turnPolls = 0;
const prompts = [];
const completions = [];

function json(value) {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

globalThis.fetch = async (input, init = {}) => {
  const url = String(input);
  if (url === "http://opencode/session" && init.method === "POST") {
    return json({ id: "oc-session" });
  }
  if (url === "http://opencode/config") {
    return json({ default_agent: "nexus" });
  }
  if (url === "http://bridge/turn/next") {
    turnPolls += 1;
    if (turnPolls === 1) {
      return await new Promise((resolve) => { resolveFirstTurn = resolve; });
    }
    return await new Promise(() => {});
  }
  if (url === "http://opencode/session/oc-session/prompt_async") {
    prompts.push(JSON.parse(init.body));
    return new Response(null, { status: 204 });
  }
  if (url === "http://bridge/turn/turn-1/complete") {
    completions.push(url);
    return new Response(null, { status: 204 });
  }
  if (url === "http://bridge/event") {
    return new Response(null, { status: 204 });
  }
  throw new Error(`unexpected fetch ${init.method ?? "GET"} ${url}`);
};

const { nexus } = await import(pathToFileURL(process.argv[2]).href);
const hooks = await nexus();

for (let i = 0; i < 100 && !resolveFirstTurn; i += 1) {
  await new Promise((resolve) => setTimeout(resolve, 5));
}
assert.ok(resolveFirstTurn, "plugin started its bridge long-poll");

await hooks["chat.message"]({ sessionID: "oc-session" });
resolveFirstTurn(json({ id: "turn-1", text: "opening while setup is busy" }));
await new Promise((resolve) => setTimeout(resolve, 25));
assert.equal(prompts.length, 0, "claimed turn waits while the native session is busy");

await hooks.event({
  event: {
    type: "session.status",
    properties: { sessionID: "oc-session", status: { type: "idle" } },
  },
});

for (let i = 0; i < 100 && prompts.length === 0; i += 1) {
  await new Promise((resolve) => setTimeout(resolve, 5));
}
assert.equal(prompts.length, 1, "deferred turn is submitted after OpenCode becomes idle");
assert.equal(prompts[0].parts[0].text, "opening while setup is busy");

await hooks.event({
  event: {
    type: "session.status",
    properties: { sessionID: "oc-session", status: { type: "idle" } },
  },
});
assert.equal(completions.length, 1, "turn completes only after its native idle event");
process.exit(0);
"#,
    )
    .unwrap();

    let output = std::process::Command::new("node")
        .arg(&driver)
        .arg(&files.plugin_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "busy-arrival plugin regression failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

trait BridgeHttpExt {
    async fn http_json(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        token: &str,
    ) -> (u16, Value);
}

impl BridgeHttpExt for OpenCodePluginBridge {
    async fn http_json(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        token: &str,
    ) -> (u16, Value) {
        let body = body.map(|v| v.to_string()).unwrap_or_default();
        let address = self
            .endpoint()
            .base_url()
            .strip_prefix("http://")
            .expect("loopback http url")
            .to_string();
        let mut stream = TcpStream::connect(address).await.expect("connect bridge");
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: bridge\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read response");
        let response = String::from_utf8(response).expect("utf8 response");
        let status = response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .expect("status code");
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .filter(|body| !body.trim().is_empty())
            .map(|body| serde_json::from_str(body).expect("json body"))
            .unwrap_or(Value::Null);
        (status, body)
    }
}
