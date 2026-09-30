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
