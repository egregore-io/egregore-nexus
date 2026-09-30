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
        if self.closed.load(std::sync::atomic::Ordering::SeqCst)
            || self.root.lock().unwrap().as_deref() != Some(update.native_session_id())
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
    let profile = nexus_agent::adapter::opencode::native::model_profile();
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
let holdCapacity = false;
let releaseCapacity;
let capacityEntered;
const entered = new Promise(resolve => {capacityEntered = resolve;});
const held = new Promise(resolve => {releaseCapacity = resolve;});
let overtaken = 0;
globalThis.fetch = async (url, init) => {
  if (String(url).endsWith('/turn/next')) return new Promise(() => {});
  if (String(url).endsWith('/config/providers')) {
    if (holdCapacity) {capacityEntered(); await held;}
    return new Response(JSON.stringify({providers:[{id:'native-provider',models:{'selected-opaque':{limit:{context:200000}}}}]}), {status:200});
  }
  if (holdCapacity && String(url).endsWith('/model') && JSON.parse(init.body)?.telemetry && JSON.parse(init.body)?.info?.id?.startsWith('z')) overtaken++;
  if (String(url).startsWith('http://native.invalid/')) return new Response(JSON.stringify({id:'ses_exact'}), {status:200});
  return realFetch(url, init);
};
const { nexus } = await import(pathToFileURL(process.argv[1]).href);
const hooks = await nexus();
await hooks.event({event:{type:'session.created',properties:{info:{id:'ses_foreign'}}}});
await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_foreign',id:'m_child',role:'assistant',modelID:'child'}}}});
await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_foreign',id:'m_child_large',role:'user',tokens:{padding:'x'.repeat(1048576)}}}}});
await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_exact',id:'m_real',role:'assistant',modelID:'selected-opaque',providerID:'native-provider',time:{created:123}}}}});
await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_exact',id:'m_real',role:'assistant',modelID:'selected-opaque',providerID:'native-provider',time:{created:123},finish:'stop',tokens:{input:40000,output:1000,reasoning:100,cache:{read:800,write:100}}}}}});
holdCapacity = true;
const pending = [hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_exact',id:'m_aaa',role:'assistant',modelID:'selected-opaque',providerID:'native-provider',finish:'stop',tokens:{input:40000,output:1000,reasoning:100,cache:{read:800,write:100}}}}}})];
await Promise.race([entered, new Promise((_,reject)=>setTimeout(()=>reject(new Error('root telemetry unavailable after foreign callback')),1000))]);
for (let i=0;i<100;i++) pending.push(hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_exact',id:`z${String(i).padStart(3,'0')}`,role:'user'}}}}));
pending.push(hooks.event({event:{type:'message.removed',properties:{sessionID:'ses_exact',messageID:'z099'}}}));
await new Promise(resolve=>setTimeout(resolve,50));
if (overtaken !== 0) throw new Error('capacity lookup allowed later native retention changes to overtake');
holdCapacity = false;
releaseCapacity();
await Promise.all(pending);
await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:'ses_exact',id:'z_limit',role:'user',tokens:{padding:'x'.repeat(1048576)}}}}});
process.exit(0);
"#;
    let output = tokio::process::Command::new("node")
        .args(["--input-type=module", "--eval", script])
        .arg(&files.plugin_path)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        // Windows Node initializes its crypto provider from the OS installation path.
        // Retain only that required OS variable, not the operator's harness environment.
        .envs(std::env::vars_os().filter(|(name, _)| {
            cfg!(windows) && name.to_string_lossy().eq_ignore_ascii_case("SystemRoot")
        }))
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
    assert_eq!(updates.len(), 2);
    let nexus_contracts::model_report::ModelEvidenceValue::Observed(value) = &updates[0].value
    else {
        panic!("observed")
    };
    assert_eq!(value.model_id, "selected-opaque");
    assert_eq!(value.provider_id.as_deref(), Some("native-provider"));
    assert_eq!(value.native_message_id.as_deref(), Some("m_real"));
    let values = sink.telemetry.lock().unwrap();
    assert!(
        values.len() >= 4,
        "same-message model dedupe must not suppress telemetry or eviction"
    );
    assert!(
        matches!(
            values.last().unwrap(),
            nexus_contracts::telemetry::NativeTelemetryUpdate::Context {
                value: nexus_contracts::telemetry::NativeTelemetryValue::Unknown,
                ..
            }
        ),
        "evicted delayed assistant must not reappear"
    );
    let nexus_contracts::telemetry::NativeTelemetryUpdate::Context {
        value: nexus_contracts::telemetry::NativeTelemetryValue::Observed(context),
        ..
    } = &values[1]
    else {
        panic!("native context")
    };
    assert_eq!(context.used_tokens.as_ref().unwrap().value.get(), 42000);
    assert_eq!(
        context
            .effective_capacity_tokens
            .as_ref()
            .unwrap()
            .value
            .get(),
        200000
    );
    assert_eq!(
        context.remaining_percent.as_ref().unwrap().value.get(),
        79.0
    );
    assert!(
        sink.closed.load(std::sync::atomic::Ordering::SeqCst),
        "bounded telemetry overflow revokes reporting"
    );
    assert!(
        bridge.input().is_alive(),
        "reporting exhaustion does not interrupt native input"
    );
}

#[tokio::test]
async fn telemetry_bridge_fences_reordered_same_message_and_removal_without_model_authority() {
    use nexus_contracts::telemetry::{NativeTelemetryUpdate, NativeTelemetryValue};
    let (sink, reporting) = native_capture();
    let bridge = OpenCodePluginBridge::start_observed(
        SessionId("s_telemetry".into()),
        Arc::new(CaptureSink::default()),
        Default::default(),
        reporting,
    )
    .await
    .unwrap();
    let sample = |root: &str, id: &str, sequence, input| json!({"info":{"sessionID":root,"id":id,"role":"assistant","finish":"stop","tokens":{"input":input,"output":30,"reasoning":12,"cache":{"read":80,"write":20}}},"telemetry":{"sequence":sequence,"contextCapacity":1000}});
    bridge
        .http_json(
            "POST",
            "/model",
            Some(sample("ses_exact", "msg_b", 1, 1)),
            bridge.endpoint().token(),
        )
        .await;
    assert!(sink.telemetry.lock().unwrap().is_empty());
    assert!(bridge.bind_model_root("ses_exact"));
    bridge
        .http_json(
            "POST",
            "/model",
            Some(sample("ses_child", "msg_b", 900, 900)),
            bridge.endpoint().token(),
        )
        .await;
    bridge
        .http_json(
            "POST",
            "/model",
            Some(sample("ses_exact", "msg_b", 2, 200)),
            "wrong-token",
        )
        .await;
    assert!(sink.telemetry.lock().unwrap().is_empty());
    for value in [
        sample("ses_exact", "msg_b", 2, 200),
        sample("ses_exact", "msg_b", 1, 900),
        sample("ses_exact", "msg_b", 2, 900),
        sample("ses_exact", "msg_a", 1, 100),
    ] {
        bridge
            .http_json("POST", "/model", Some(value), bridge.endpoint().token())
            .await;
    }
    assert_eq!(sink.telemetry.lock().unwrap().len(), 2);
    assert!(
        sink.updates.lock().unwrap().is_empty(),
        "telemetry never invents model selection"
    );
    bridge.http_json("POST", "/model", Some(json!({"info":{"sessionID":"ses_exact","id":"msg_b"},"telemetry":{"sequence":3,"removed":true}})), bridge.endpoint().token()).await;
    bridge
        .http_json(
            "POST",
            "/model",
            Some(sample("ses_exact", "msg_b", 2, 900)),
            bridge.endpoint().token(),
        )
        .await;
    let values = sink.telemetry.lock().unwrap();
    assert_eq!(
        values.len(),
        4,
        "late capacity reply cannot resurrect removed sample"
    );
    let NativeTelemetryUpdate::Usage {
        value: NativeTelemetryValue::Observed(usage),
        ..
    } = &values[2]
    else {
        panic!("fallback usage")
    };
    assert_eq!(usage.input_tokens.unwrap().get(), 100);
    drop(values);
    bridge.shutdown();
    assert!(sink.closed.load(std::sync::atomic::Ordering::SeqCst));
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

#[derive(Default)]
struct AcceptedCount(std::sync::atomic::AtomicUsize);

#[async_trait]
impl nexus_pty::TurnAcceptanceObserver for AcceptedCount {
    async fn accepted(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn generated_plugin_real_bridge_emits_one_ordered_echo_per_intentional_input() {
    struct Echo {
        sink: Arc<CaptureSink>,
        client_id: String,
    }
    #[async_trait]
    impl nexus_pty::TurnAcceptanceObserver for Echo {
        async fn accepted(&self) {
            self.sink
                .emit(WsEvent::AgentUpdate {
                    session_id: SessionId("s_composed_receipt".into()),
                    kind: AgentUpdateKind::UserInput,
                    data: json!({"text":"same intentional text", "clientMessageId":self.client_id}),
                })
                .await;
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_composed_receipt".into())).unwrap();
    let sink = Arc::new(CaptureSink::default());
    let bridge = OpenCodePluginBridge::start(
        SessionId("s_composed_receipt".into()),
        sink.clone(),
        Default::default(),
    )
    .await
    .unwrap();
    let input = bridge.input();
    let captured_sink = sink.clone();
    let sends = tokio::spawn(async move {
        for index in 0..2 {
            input
                .send_turn_observed(
                    "same intentional text",
                    Arc::new(Echo {
                        sink: captured_sink.clone(),
                        client_id: format!("client-{index}"),
                    }),
                )
                .await
                .unwrap();
        }
    });
    let script = r#"
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
const realFetch = globalThis.fetch, prompts = [];
globalThis.fetch = async (url, init={}) => {
  if (!String(url).startsWith('http://native.invalid')) return realFetch(url,init);
  if (String(url).endsWith('/config')) return new Response(JSON.stringify({default_agent:'build'}));
  if (String(url).endsWith('/prompt_async')) { prompts.push(JSON.parse(init.body)); return new Response(null,{status:204}); }
  throw Error('unexpected native fixture route');
};
const hooks = await (await import(pathToFileURL(process.argv[1]).href)).nexus();
const event = (type,properties) => hooks.event({event:{type,properties}});
for(let n=0;n<2;n++) {
  for(let i=0;i<400 && prompts.length<=n;i++) await new Promise(r=>setTimeout(r,5));
  assert.equal(prompts.length,n+1);
  const id=prompts[n].messageID;
  assert.equal(prompts[n].parts[0].text,'same intentional text');
  await event('message.updated',{info:{sessionID:'ses_exact',id,role:'user'}});
  await event('message.updated',{info:{sessionID:'ses_exact',id,role:'user'}});
  await event('message.part.updated',{part:{sessionID:'ses_exact',id:`prt_user${n}`,messageID:id,type:'text',text:'same intentional text'}});
  await event('message.updated',{info:{sessionID:'ses_exact',id:`msg_reply${n}`,role:'assistant'}});
  await event('message.part.updated',{part:{sessionID:'ses_exact',id:`prt_reply${n}`,messageID:`msg_reply${n}`,type:'text',text:`reply-${n}`}});
  await event('session.idle',{sessionID:'ses_exact'});
}
assert.notEqual(prompts[0].messageID,prompts[1].messageID);
process.exit(0);
"#;
    let output = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("node")
            .args(["--input-type=module", "--eval", script])
            .arg(&files.plugin_path)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .envs(std::env::vars_os().filter(|(name, _)| {
                cfg!(windows) && name.to_string_lossy().eq_ignore_ascii_case("SystemRoot")
            }))
            .env("NEXUS_OPENCODE_BRIDGE_URL", bridge.endpoint().base_url())
            .env("NEXUS_OPENCODE_BRIDGE_TOKEN", bridge.endpoint().token())
            .env("NEXUS_OPENCODE_SERVER_URL", "http://native.invalid")
            .env("NEXUS_OPENCODE_SESSION_ID", "ses_exact")
            .env("OPENCODE_SERVER_PASSWORD", "fixture")
            .env("NEXUS_NAME", "fixture")
            .env("NEXUS_OPENCODE_PROMPT_MODEL", "fixture/model")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    tokio::time::timeout(Duration::from_secs(2), sends)
        .await
        .unwrap()
        .unwrap();
    let events = sink.events();
    let kinds: Vec<_> = events
        .iter()
        .map(|event| match event {
            WsEvent::AgentUpdate { kind, .. } => *kind,
            _ => panic!("unexpected event"),
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd,
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd
        ]
    );
    for (index, offset) in [0, 3].into_iter().enumerate() {
        let WsEvent::AgentUpdate { data, .. } = &events[offset] else {
            unreachable!()
        };
        assert_eq!(data["clientMessageId"], format!("client-{index}"));
        let WsEvent::AgentUpdate { data, .. } = &events[offset + 1] else {
            unreachable!()
        };
        assert_eq!(data["text"], format!("reply-{index}"));
        assert_eq!(data["itemId"], format!("prt_reply{index}"));
    }
}

#[tokio::test]
async fn pending_canonical_echo_cannot_be_bypassed_by_duplicate_or_completion() {
    struct HeldAcceptance {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        calls: std::sync::atomic::AtomicUsize,
    }
    #[async_trait]
    impl nexus_pty::TurnAcceptanceObserver for HeldAcceptance {
        async fn accepted(&self) {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
    let bridge = Arc::new(
        OpenCodePluginBridge::start(
            SessionId("s_receipt_held".into()),
            Arc::new(CaptureSink::default()),
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let observer = Arc::new(HeldAcceptance {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let input = bridge.input();
    let captured = observer.clone();
    let pending = tokio::spawn(async move { input.send_turn_observed("held", captured).await });
    let (_, turn) = bridge
        .http_json("GET", "/turn/next", None, bridge.endpoint().token())
        .await;
    let path = format!("/turn/{}", turn["id"].as_str().unwrap());
    let binding = json!({"sessionID":"ses_root", "messageID":"msg_held"});
    assert_eq!(
        bridge
            .http_json(
                "POST",
                &format!("{path}/bind"),
                Some(binding.clone()),
                bridge.endpoint().token()
            )
            .await
            .0,
        204
    );
    let first_bridge = bridge.clone();
    let first_path = path.clone();
    let first_binding = binding.clone();
    let first = tokio::spawn(async move {
        first_bridge
            .http_json(
                "POST",
                &format!("{first_path}/accepted"),
                Some(first_binding),
                first_bridge.endpoint().token(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), observer.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        bridge
            .http_json(
                "POST",
                &format!("{path}/accepted"),
                Some(binding.clone()),
                bridge.endpoint().token()
            )
            .await
            .0,
        409,
        "an in-progress callback is not a completed canonical echo"
    );
    assert_eq!(
        bridge
            .http_json(
                "POST",
                &format!("{path}/complete"),
                Some(json!({})),
                bridge.endpoint().token()
            )
            .await
            .0,
        409,
        "turn completion cannot overtake the held echo"
    );
    assert!(!pending.is_finished());
    assert_eq!(observer.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    observer.release.notify_one();
    assert_eq!(first.await.unwrap().0, 200);
    assert_eq!(
        bridge
            .http_json(
                "POST",
                &format!("{path}/accepted"),
                Some(binding),
                bridge.endpoint().token()
            )
            .await
            .0,
        200
    );
    assert_eq!(observer.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        bridge
            .http_json(
                "POST",
                &format!("{path}/complete"),
                Some(json!({})),
                bridge.endpoint().token()
            )
            .await
            .0,
        204
    );
    pending.await.unwrap().unwrap();
}

#[tokio::test]
async fn observed_input_receipt_precedes_completion_and_is_exact_once() {
    let bridge = OpenCodePluginBridge::start(
        SessionId("s_receipt".into()),
        Arc::new(CaptureSink::default()),
        Default::default(),
    )
    .await
    .unwrap();
    let input = bridge.input();
    let count = Arc::new(AcceptedCount::default());
    let observer = count.clone();
    let pending =
        tokio::spawn(async move { input.send_turn_observed("same text", observer).await });
    let (_, turn) = bridge
        .http_json("GET", "/turn/next", None, bridge.endpoint().token())
        .await;
    let path = format!("/turn/{}", turn["id"].as_str().unwrap());
    let binding = json!({"sessionID":"ses_root", "messageID":"msg_exact"});
    let (status, _) = bridge
        .http_json(
            "POST",
            &format!("{path}/bind"),
            Some(binding.clone()),
            bridge.endpoint().token(),
        )
        .await;
    assert_eq!(
        status, 204,
        "native identity is bound before prompt submission"
    );
    for bad in [
        json!({"sessionID":"ses_foreign", "messageID":"msg_exact"}),
        json!({"sessionID":"ses_root", "messageID":"msg_other"}),
    ] {
        let (status, _) = bridge
            .http_json(
                "POST",
                &format!("{path}/accepted"),
                Some(bad),
                bridge.endpoint().token(),
            )
            .await;
        assert_eq!(status, 409);
    }
    for _ in 0..2 {
        let (status, receipt) = bridge
            .http_json(
                "POST",
                &format!("{path}/accepted"),
                Some(binding.clone()),
                bridge.endpoint().token(),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(receipt["canonicalEcho"], true);
    }
    assert_eq!(count.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!pending.is_finished(), "admission is not turn completion");
    let (status, _) = bridge
        .http_json(
            "POST",
            &format!("{path}/complete"),
            Some(json!({})),
            bridge.endpoint().token(),
        )
        .await;
    assert_eq!(status, 204);
    pending.await.unwrap().unwrap();
    assert_eq!(
        count.0.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "no late synthetic echo"
    );
}

#[tokio::test]
async fn dropped_input_revokes_only_its_captured_receipt_without_resending() {
    let bridge = OpenCodePluginBridge::start(
        SessionId("s_receipt_drop".into()),
        Arc::new(CaptureSink::default()),
        Default::default(),
    )
    .await
    .unwrap();
    let old_count = Arc::new(AcceptedCount::default());
    let input = bridge.input();
    let observer = old_count.clone();
    let old = tokio::spawn(async move { input.send_turn_observed("same text", observer).await });
    let (_, turn) = bridge
        .http_json("GET", "/turn/next", None, bridge.endpoint().token())
        .await;
    let old_path = format!("/turn/{}", turn["id"].as_str().unwrap());
    let binding = json!({"sessionID":"ses_root", "messageID":"msg_old"});
    assert_eq!(
        bridge
            .http_json(
                "POST",
                &format!("{old_path}/bind"),
                Some(binding.clone()),
                bridge.endpoint().token()
            )
            .await
            .0,
        204
    );
    old.abort();
    assert!(old.await.unwrap_err().is_cancelled());
    let input = bridge.input();
    let next = tokio::spawn(async move { input.send_turn("same text").await });
    let (_, new_turn) = bridge
        .http_json("GET", "/turn/next", None, bridge.endpoint().token())
        .await;
    assert_ne!(
        new_turn["id"], turn["id"],
        "intentional repeated text is a new submission"
    );
    assert_eq!(
        bridge
            .http_json(
                "POST",
                &format!("{old_path}/accepted"),
                Some(binding),
                bridge.endpoint().token()
            )
            .await
            .0,
        404
    );
    assert_eq!(old_count.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        bridge
            .http_json(
                "POST",
                &format!("/turn/{}/complete", new_turn["id"].as_str().unwrap()),
                Some(json!({})),
                bridge.endpoint().token()
            )
            .await
            .0,
        204
    );
    next.await.unwrap().unwrap();
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
fn generated_serve_pins_mcp_to_each_captured_launch_without_project_writes() {
    if !node_available() {
        eprintln!("[skip] node is not available");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_mcp_capture".into())).unwrap();
    let project_config = tmp.path().join("opencode.json");
    let stale =
        r#"{"mcp":{"nexus-bus":{"type":"local","command":["stale","--client-key","other-key"]}}}"#;
    std::fs::write(&project_config, stale).unwrap();
    let driver = tmp.path().join("mcp-capture.mjs");
    std::fs::write(&driver, r#"
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const mode = process.argv[3];
Object.assign(process.env, {
  NEXUS_OPENCODE_PLUGIN_PATH:process.argv[2], NEXUS_OPENCODE_HOME:process.argv[4],
  NEXUS_OPENCODE_BIN:'fixture-native', NEXUS_CLI:'/fixture with spaces/nexus',
  NEXUS_NAME:mode === 'unnamed' ? '' : `name-${mode}`, NEXUS_AGENT_ID:'agent-unnamed',
  NEXUS_PROJECT:'fixture project', NEXUS_CLIENT_KEY:`key-${mode}`, NEXUS_AGENT:'opencode',
  NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL:'', NEXUS_SKIP_AGENT_HOOK_INSTALL:'',
});
if (mode === 'missing') delete process.env.NEXUS_CLIENT_KEY;
if (mode === 'skip') process.env.NEXUS_SKIP_AGENT_HOOK_INSTALL = '1';
// Execute the actual generated launcher; substitute only native process spawn.
// No native executable, provider, project configuration write or MCP call occurs.
globalThis.capture = (_bin, _args, options) => {
  assert.notEqual(mode, 'missing', 'incomplete caller must fail before native spawn');
  const config = JSON.parse(options.env.OPENCODE_CONFIG_CONTENT);
  if (mode === 'skip') assert.equal(config.mcp, undefined);
  else assert.deepEqual(config.mcp?.['nexus-bus'], {
    type:'local', enabled:true,
    command:['/fixture with spaces/nexus','mcp','--as',mode === 'unnamed' ? 'agent-unnamed' : `name-${mode}`,
             '--project','fixture project','--client-key',`key-${mode}`,'--agent','opencode'],
  });
  throw new Error('fixture captured spawn');
};
process.on('unhandledRejection', error => {
  const expected = mode === 'missing' ? 'captured Nexus MCP identity is incomplete' : 'fixture captured spawn';
  if (error.message !== expected) { console.error(error.message); process.exit(1); }
  process.exit(0);
});
const script = readFileSync(process.argv[2], 'utf8').replace(
  'import { spawn } from "node:child_process";', 'const spawn = globalThis.capture;',
);
await import(`data:text/javascript;base64,${Buffer.from(script).toString('base64')}`);
"#).unwrap();
    for mode in ["first", "second", "unnamed", "missing", "skip"] {
        let output = std::process::Command::new("node")
            .arg(&driver)
            .arg(&files.serve_path)
            .arg(mode)
            .arg(tmp.path().join(mode))
            .current_dir(tmp.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "mode {mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(std::fs::read_to_string(&project_config).unwrap(), stale);
    }
}

#[test]
fn generated_plugin_preserves_native_tool_identity_error_and_activity() {
    if !node_available() {
        eprintln!("[skip] node is not available");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_opencode_tool_fidelity".into()))
            .unwrap();
    let driver = tmp.path().join("tool-fidelity.mjs");
    std::fs::write(&driver, r#"
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
Object.assign(process.env, {
  NEXUS_OPENCODE_BRIDGE_URL:'http://bridge', NEXUS_OPENCODE_BRIDGE_TOKEN:'fixture',
  NEXUS_OPENCODE_SERVER_URL:'http://native', OPENCODE_SERVER_PASSWORD:'fixture',
  NEXUS_OPENCODE_SESSION_ID:'native-root', NEXUS_NAME:'tool-fixture',
});
const events = [];
globalThis.fetch = async (url, init = {}) => {
  if (url === 'http://bridge/turn/next') return new Promise(() => {});
  if (url === 'http://bridge/event') {
    events.push(JSON.parse(init.body));
    return new Response(null, {status:204});
  }
  if (url === 'http://bridge/model') return new Response(null, {status:204});
  throw new Error(`unexpected fixture fetch ${url}`);
};
const hooks = await (await import(pathToFileURL(process.argv[2]).href)).nexus();
const part = async value => hooks.event({event:{type:'message.part.updated',properties:{part:{
  sessionID:'native-root',messageID:'native-message',type:'tool',tool:'bash',...value,
}}}});
await hooks['tool.execute.before']({sessionID:'native-root',tool:'bash',callID:'call-native'});
await part({id:'part-storage',callID:'call-native',state:{status:'running',input:{command:'pwd'}}});
await part({id:'part-storage',callID:'call-native',state:{status:'completed',input:{command:'pwd'},output:'native output'}});
await part({id:'part-error',callID:'call-error',state:{status:'error',input:{},error:'native permission error'}});
await part({id:'part-foreign',callID:'call-foreign',sessionID:'foreign-root',state:{status:'completed',output:'foreign'}});
const failures = [];
function check(name, fn) { try { fn(); } catch (error) { failures.push(`${name}: ${error.message}`); } }
check('no synthetic assistant prose', () => assert.equal(events.some(e => e.kind === 'thinking'), false));
const tools = events.filter(e => e.kind === 'tool_call');
check('native call identity', () => assert.deepEqual(tools.map(e => e.data.id), ['call-native','call-native','call-error']));
check('native result preserved', () => assert.equal(tools[1]?.data.content, 'native output'));
check('native failure preserved', () => assert.deepEqual(
  {status:tools[2]?.data.status, content:tools[2]?.data.content},
  {status:'failed',content:'native permission error'},
));
check('foreign source refused', () => assert.equal(tools.length, 3));
await hooks.dispose();
assert.deepEqual(failures, []);
"#).unwrap();
    let output = std::process::Command::new("node")
        .arg(&driver)
        .arg(&files.plugin_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
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
  if (url === "http://bridge/turn/turn-1/bind") {
    assert.equal(JSON.parse(init.body).sessionID, "oc-session");
    return new Response(null, { status: 204 });
  }
  if (url === "http://bridge/turn/turn-1/accepted") return json({canonicalEcho:true});
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

await hooks.event({event:{type:"message.updated",properties:{info:{
 sessionID:"oc-session",id:prompts[0].messageID,role:"user"
}}}});

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

#[test]
fn generated_plugin_failed_request_while_native_busy_does_not_spin_or_claim_successor() {
    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_failed_busy".into())).unwrap();
    let script = r#"
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
Object.assign(process.env,{NEXUS_OPENCODE_BRIDGE_URL:'http://bridge',NEXUS_OPENCODE_BRIDGE_TOKEN:'fixture',
 NEXUS_OPENCODE_SERVER_URL:'http://native',OPENCODE_SERVER_PASSWORD:'fixture',NEXUS_NAME:'fixture',
 NEXUS_OPENCODE_SESSION_ID:'ses_exact',NEXUS_OPENCODE_PROMPT_MODEL:'fixture/model'});
const stage=process.argv[2], prompts=[], errors=[], violations=[];
const json=x=>new Response(JSON.stringify(x)), empty=()=>new Response(null,{status:204});
let polls=0, poll, release, localBusy=false;
globalThis.fetch=async(input,init={})=>{
 const url=String(input), body=init.body?JSON.parse(init.body):undefined;
 if(url.endsWith('/turn/next')) {
   polls++;
   if(polls===1) return new Promise(r=>{poll=r;});
   if(localBusy) {
     violations.push('claimed successor while native busy');
     // Diagnostic refusal avoids hanging the fixture in the old microtask spin.
     throw Error('native busy claim rejected by fixture');
   }
   if(polls===2) return json({id:'B',text:'B'});
   return new Promise(()=>{});
 }
 if(url.endsWith('/config')) return json({default_agent:'build'});
 if(url.endsWith('/bind')) {
   if(stage==='bind'&&url.endsWith('/A/bind')) return new Promise(r=>{release=()=>r(new Response(null,{status:500}));});
   return empty();
 }
 if(url.endsWith('/prompt_async')) {
   prompts.push(body);
   if(stage==='prompt'&&prompts.length===1) return new Promise(r=>{release=()=>r(new Response(null,{status:500}));});
   return empty();
 }
 if(url.endsWith('/error')) {errors.push(url);return empty();}
 if(url.endsWith('/accepted')) return json({canonicalEcho:true});
 if(url.endsWith('/event')||url.endsWith('/complete')||url.endsWith('/model')) return empty();
 throw Error('unexpected '+url);
};
const hooks=await(await import(pathToFileURL(process.argv[1]).href)).nexus();
const wait=async p=>{for(let i=0;i<400&&!p();i++) await new Promise(r=>setTimeout(r,5));assert.ok(p());};
const event=(type,properties)=>hooks.event({event:{type,properties}});
await wait(()=>poll);poll(json({id:'A',text:'A'}));await wait(()=>release);
localBusy=true;await event('session.status',{sessionID:'ses_exact',status:{type:'busy'}});
release();await wait(()=>errors.length===1);
await new Promise(r=>setTimeout(r,1150));
assert.deepEqual(violations,[]);assert.equal(polls,1);
assert.equal(prompts.length,stage==='prompt'?1:0,'no successor submitted while busy');
localBusy=false;await event('session.idle',{sessionID:'ses_exact'});
await wait(()=>prompts.some(p=>p.parts[0].text==='B'));
const b=prompts.find(p=>p.parts[0].text==='B');
await event('message.updated',{info:{sessionID:'ses_exact',id:b.messageID,role:'user'}});
await event('session.idle',{sessionID:'ses_exact'});
assert.equal(prompts.filter(p=>p.parts[0].text==='B').length,1);
assert.deepEqual(errors,['http://bridge/turn/A/error']);
process.exit(0);
"#;
    let mut failures = Vec::new();
    for stage in ["prompt", "bind"] {
        let output = std::process::Command::new("node")
            .args(["--input-type=module", "--eval", script])
            .arg(&files.plugin_path)
            .arg(stage)
            .output()
            .unwrap();
        if !output.status.success() {
            failures.push(format!(
                "{stage}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn generated_plugin_local_lifecycle_during_setup_retains_unsubmitted_prompt() {
    let tmp = tempfile::tempdir().unwrap();
    let files = write_opencode_plugin_files(tmp.path(), &SessionId("s_setup".into())).unwrap();
    let script = r#"
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
Object.assign(process.env,{NEXUS_OPENCODE_BRIDGE_URL:'http://bridge',NEXUS_OPENCODE_BRIDGE_TOKEN:'fixture',
 NEXUS_OPENCODE_SERVER_URL:'http://native',OPENCODE_SERVER_PASSWORD:'fixture',NEXUS_NAME:'fixture',
 NEXUS_OPENCODE_SESSION_ID:'ses_exact',NEXUS_OPENCODE_PROMPT_MODEL:'fixture/model'});
const [stage, ending]=process.argv.slice(2), prompts=[], errors=[], completions=[];
const json=x=>new Response(JSON.stringify(x)), empty=()=>new Response(null,{status:204});
let poll, release;
globalThis.fetch=async(input,init={})=>{
 const url=String(input), body=init.body?JSON.parse(init.body):undefined;
 if(url.endsWith('/turn/next')) return new Promise(r=>{poll=r;});
 if(url.endsWith('/config')) return stage==='config'?new Promise(r=>{release=()=>r(json({default_agent:'build'}));}):json({default_agent:'build'});
 if(url.endsWith('/bind')) return stage==='bind'?new Promise(r=>{release=()=>r(empty());}):empty();
 if(url.endsWith('/prompt_async')) {prompts.push(body);return empty();}
 if(url.endsWith('/accepted')) return json({canonicalEcho:true});
 if(url.endsWith('/error')) {errors.push(url);return empty();}
 if(url.endsWith('/complete')) {completions.push(url);return empty();}
 if(url.endsWith('/event')||url.endsWith('/model')) return empty();
 throw Error('unexpected '+url);
};
const hooks=await(await import(pathToFileURL(process.argv[1]).href)).nexus();
const wait=async p=>{for(let i=0;i<400&&!p();i++) await new Promise(r=>setTimeout(r,5));assert.ok(p());};
const event=(type,properties)=>hooks.event({event:{type,properties}});
await wait(()=>poll);poll(json({id:'A',text:'retained once'}));await wait(()=>release);
await event('session.status',{sessionID:'ses_exact',status:{type:'busy'}});
if(ending!=='held-busy') {
 if(ending==='error') await event('session.error',{sessionID:'ses_exact',error:{message:'local failure'}});
 else await event('session.status',{sessionID:'ses_exact',status:{type:'idle'}});
 await event('session.idle',{sessionID:'ses_exact'});
}
assert.deepEqual(completions,[],'local idle cannot complete setup A');
assert.deepEqual(errors,[],'local error cannot fail setup A');
assert.equal(prompts.length,0);
release();
if(ending==='held-busy') {
 await new Promise(r=>setTimeout(r,30));
 assert.equal(prompts.length,0,'retain prepared A while local turn remains busy');
 await event('session.status',{sessionID:'ses_exact',status:{type:'idle'}});
}
await wait(()=>prompts.length===1);
assert.equal(prompts[0].parts[0].text,'retained once');
await event('session.idle',{sessionID:'ses_exact'});
assert.deepEqual(completions,[],'an idle before exact persistence cannot settle submitted A');
await event('message.updated',{info:{sessionID:'ses_exact',id:prompts[0].messageID,role:'user'}});
await event('session.idle',{sessionID:'ses_exact'});
assert.deepEqual(completions,['http://bridge/turn/A/complete']);
assert.deepEqual(errors,[]);assert.equal(prompts.length,1);
process.exit(0);
"#;
    let mut failures = Vec::new();
    for stage in ["config", "bind"] {
        for ending in ["idle", "error", "held-busy"] {
            let output = std::process::Command::new("node")
                .args(["--input-type=module", "--eval", script])
                .arg(&files.plugin_path)
                .arg(stage)
                .arg(ending)
                .output()
                .unwrap();
            if !output.status.success() {
                failures.push(format!(
                    "{stage}/{ending}: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn generated_plugin_late_receipt_cannot_settle_replacement_or_hide_native_output() {
    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_late_receipt".into())).unwrap();
    let script = r#"
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
Object.assign(process.env,{NEXUS_OPENCODE_BRIDGE_URL:'http://bridge',NEXUS_OPENCODE_BRIDGE_TOKEN:'fixture',
 NEXUS_OPENCODE_SERVER_URL:'http://native',OPENCODE_SERVER_PASSWORD:'fixture',NEXUS_NAME:'fixture',
 NEXUS_OPENCODE_SESSION_ID:'ses_exact',NEXUS_OPENCODE_PROMPT_MODEL:'fixture/model'});
const mode=process.argv[2], prompts=[], errors=[], completions=[], emitted=[];
const json=x=>new Response(JSON.stringify(x)), empty=()=>new Response(null,{status:204});
let poll, releaseA;
globalThis.fetch=async(input,init={})=>{
 const url=String(input), body=init.body?JSON.parse(init.body):undefined;
 if(url.endsWith('/turn/next')) return new Promise(r=>{poll=r;});
 if(url.endsWith('/config')) return json({default_agent:'build'});
 if(url.endsWith('/prompt_async')) {prompts.push(body);return empty();}
 if(url.endsWith('/bind')||url.endsWith('/model')) return empty();
 if(url.endsWith('/A/accepted')) return mode==='lost'?new Response(null,{status:404}):new Promise(r=>{releaseA=r;});
 if(url.endsWith('/B/accepted')) return json({canonicalEcho:true});
 if(url.endsWith('/error')) {errors.push(url);return empty();}
 if(url.endsWith('/complete')) {completions.push(url);return empty();}
 if(url.endsWith('/event')) {emitted.push(body);return empty();}
 throw Error('unexpected '+url);
};
const hooks=await(await import(pathToFileURL(process.argv[1]).href)).nexus();
const wait=async p=>{for(let i=0;i<400&&!p();i++) await new Promise(r=>setTimeout(r,5));assert.ok(p());};
const event=(type,properties)=>hooks.event({event:{type,properties}});
await wait(()=>poll); let p=poll;poll=undefined;p(json({id:'A',text:'A'}));await wait(()=>prompts.length===1);
const receipt=event('message.updated',{info:{sessionID:'ses_exact',id:prompts[0].messageID,role:'user'}}).catch(()=>{});
if(mode==='lost') {
 await receipt;
 await event('message.updated',{info:{sessionID:'ses_exact',id:'msg_native',role:'assistant'}});
 for(const part of [{id:'prt_text',type:'text',text:'still processing'},
   {id:'prt_thought',type:'reasoning',text:'still thinking'},
   {id:'prt_tool',type:'tool',callID:'call_real',tool:'bash',state:{status:'completed',output:'real output'}}])
   await event('message.part.updated',{part:{...part,sessionID:'ses_exact',messageID:'msg_native'}});
 assert.deepEqual(emitted.map(x=>x.kind),['text','thinking','tool_call']);
 assert.equal(emitted[2].data.content,'real output');
 assert.equal(completions.length,0);
} else {
 await wait(()=>releaseA);
 const idleA=event('session.idle',{sessionID:'ses_exact'});
 await event('session.error',{sessionID:'ses_exact',error:{message:'A ended'}});
 await event('session.status',{sessionID:'ses_exact',status:{type:'idle'}});
 await wait(()=>poll);p=poll;poll=undefined;p(json({id:'B',text:'B'}));await wait(()=>prompts.length===2);
 releaseA(mode==='reject'?new Response(null,{status:404}):json({canonicalEcho:true}));
 await Promise.all([receipt,idleA]);
 assert.deepEqual(errors,['http://bridge/turn/A/error'],'late A cannot fail B');
 assert.deepEqual(completions,[],'late A cannot clear B or declare completion');
 await event('message.updated',{info:{sessionID:'ses_exact',id:prompts[1].messageID,role:'user'}});
 await event('session.idle',{sessionID:'ses_exact'});
 assert.deepEqual(completions,['http://bridge/turn/B/complete']);
}
process.exit(0);
"#;
    let mut failures = Vec::new();
    for mode in ["reject", "resolve", "lost"] {
        let output = std::process::Command::new("node")
            .args(["--input-type=module", "--eval", script])
            .arg(&files.plugin_path)
            .arg(mode)
            .output()
            .unwrap();
        if !output.status.success() {
            failures.push(format!(
                "{mode}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn generated_plugin_correlates_persisted_user_identity_not_equal_text() {
    if !node_available() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let files =
        write_opencode_plugin_files(tmp.path(), &SessionId("s_native_receipt".into())).unwrap();
    let driver = tmp.path().join("native-receipt.mjs");
    std::fs::write(&driver, r#"
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
Object.assign(process.env, {NEXUS_OPENCODE_BRIDGE_URL:'http://bridge', NEXUS_OPENCODE_BRIDGE_TOKEN:'fixture',
  NEXUS_OPENCODE_SERVER_URL:'http://native', OPENCODE_SERVER_PASSWORD:'fixture', NEXUS_NAME:'fixture',
  NEXUS_OPENCODE_SESSION_ID:'ses_exact', NEXUS_OPENCODE_PROMPT_MODEL:'fixture/model'});
const json = value => new Response(JSON.stringify(value), {status:200});
const empty = () => new Response(null, {status:204});
const calls = [], prompts = [], emitted = [];
let poll, bound;
globalThis.fetch = async (input, init={}) => {
  const url = String(input), body = init.body ? JSON.parse(init.body) : undefined;
  if (url.endsWith('/turn/next')) return new Promise(resolve => { poll=resolve; });
  if (url.endsWith('/config')) return json({default_agent:'build'});
  if (url.endsWith('/bind')) { bound=body; calls.push('bound'); return empty(); }
  if (url.endsWith('/prompt_async')) { prompts.push(body); calls.push('submitted'); return empty(); }
  if (url.endsWith('/accepted')) { assert.deepEqual(body,bound); calls.push('accepted'); return json({canonicalEcho:true}); }
  if (url.endsWith('/event')) { emitted.push(body); return empty(); }
  if (url.endsWith('/model') || url.endsWith('/complete')) return empty();
  throw Error(`unexpected ${url}`);
};
const hooks = await (await import(pathToFileURL(process.argv[2]).href)).nexus();
const wait = async predicate => { for(let i=0;i<100&&!predicate();i++) await new Promise(r=>setTimeout(r,5)); assert.ok(predicate()); };
await wait(()=>poll);
const event = async (type,properties) => hooks.event({event:{type,properties}});
const ids=[];
for (let n=1;n<=2;n++) {
  const next=poll; poll=undefined;
  next(json({id:`turn-${n}`,text:'same intentional text'}));
  await wait(()=>prompts.length===n);
  const id=prompts[n-1].messageID; ids.push(id);
  assert.match(id,/^msg_[0-9a-f]{12}[A-Za-z0-9]{14}$/);
  assert.deepEqual(bound,{sessionID:'ses_exact',messageID:id});
  assert.equal(calls.filter(x=>x==='accepted').length,n-1,'HTTP submission is not native receipt');
  await hooks['chat.message']({sessionID:'ses_exact',messageID:id});
  assert.equal(calls.filter(x=>x==='accepted').length,n-1,'pre-persistence hook is not native receipt');
  await event('message.updated',{info:{sessionID:'ses_foreign',id,role:'user'}});
  assert.equal(calls.filter(x=>x==='accepted').length,n-1);
  await event('message.updated',{info:{sessionID:'ses_exact',id,role:'user'}});
  await event('message.updated',{info:{sessionID:'ses_exact',id,role:'user'}});
  await event('message.part.updated',{part:{sessionID:'ses_exact',id:`prt_${n}`,messageID:id,type:'text',text:'same intentional text'}});
  assert.equal(calls.filter(x=>x==='accepted').length,n);
  assert.equal(emitted.filter(x=>x.kind==='user_input').length,0,'owned echo replaced by canonical admission only');
  await event('session.idle',{sessionID:'ses_exact'});
  await wait(()=>poll);
}
assert.notEqual(ids[0],ids[1]);
await event('message.updated',{info:{sessionID:'ses_exact',id:'msg_manual',role:'user'}});
await event('message.part.updated',{part:{sessionID:'ses_exact',id:'prt_manual',messageID:'msg_manual',type:'text',text:'same intentional text'}});
assert.equal(emitted.filter(x=>x.kind==='user_input').length,1,'equal manual input remains visible');
assert.equal(emitted.find(x=>x.kind==='user_input').data.clientMessageId,'msg_manual');
assert.deepEqual(calls.slice(0,3),['bound','submitted','accepted']);
process.exit(0);
"#).unwrap();
    let out = std::process::Command::new("node")
        .arg(&driver)
        .arg(files.plugin_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
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
