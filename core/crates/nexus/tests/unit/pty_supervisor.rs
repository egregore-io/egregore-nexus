use super::*;

#[path = "claude_receipt_suffix.rs"]
mod claude_receipt_suffix;

#[path = "claude_bus_queue.rs"]
mod claude_bus_queue;

#[cfg(unix)]
#[path = "opencode_revival.rs"]
mod opencode_revival;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::daemon::gateway_stream_socket::{GatewayStreamFrame, GatewayStreamPublisher};
use async_trait::async_trait;
use nexus_contracts::batch::{BatchCounts, NexusBatch};
use nexus_contracts::ports::AgentTurnExecutionPort;
use nexus_harness_claude::native::{
    forwarder::ClaudeHookObservationSink,
    transcript::{parse_hook_record, ClaudeHookRecord},
};
use nexus_harness_core::{native_harness_program, NativeProcessPlatform};

mod model_projection_artifacts {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/model_projection_artifacts.rs"
    ));
}

mod claude_operator_live {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/claude_operator_live.rs"
    ));
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn headed_model_projection_hermes_claim_precedes_native_and_publishes_after_live_setup() {
    use crate::daemon::gateway_stream_socket::{GatewayStreamFrame, GatewayStreamPublisher};
    use nexus_store::repos::AgentRuntimes;
    use std::os::unix::fs::PermissionsExt;
    for mode in ["success", "cancel", "ledger-error"] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let fake = dir.path().join("hermes");
        std::fs::write(&fake, r#"#!/usr/bin/env python3
import asyncio, importlib.util, json, os, pathlib, socket, sqlite3, time
home=pathlib.Path(os.environ['HERMES_HOME']); gates=pathlib.Path(os.environ['HOME'])
s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); s.connect(os.environ['NEXUS_HERMES_BRIDGE_SOCKET'])
s.sendall((json.dumps({'t':'subscribe','token':os.environ['NEXUS_HERMES_BRIDGE_TOKEN']})+'\n').encode())
(gates/'witness').write_text(json.dumps({'session':os.environ['NEXUS_SESSION_ID']}))
if (gates/'cancel').exists():
    incoming=json.loads(s.makefile('rb').readline())
    assert incoming['t']=='incoming'
    (gates/'input-entered').write_text('captured input')
    while not (gates/'release').exists(): time.sleep(.01)
    for kind in ['processing_started','delivered']:
        s.sendall((json.dumps({'t':kind,'id':incoming['id'],'token':os.environ['NEXUS_HERMES_BRIDGE_TOKEN']})+'\n').encode())
    s.sendall((json.dumps({'t':'model_source','token':os.environ['NEXUS_HERMES_BRIDGE_TOKEN']})+'\n').encode())
    assert s.makefile('rb').readline()
    (gates/'released').write_text('bridge processed receipt')
while not (gates/'emit').exists(): time.sleep(.01)
db=sqlite3.connect(home/'state.db')
db.execute('CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, model TEXT, model_config TEXT, parent_session_id TEXT, ended_at REAL, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, api_call_count INTEGER)')
db.execute('INSERT OR REPLACE INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?,?)',('native-root','native-configured','{}',None,None,120,30,80,20,12,3)); db.commit(); db.close()
spec=importlib.util.spec_from_file_location('model_hook',home/'hooks/nexus-model/handler.py')
hook=importlib.util.module_from_spec(spec); spec.loader.exec_module(hook)
asyncio.run(hook.handle('agent:start',{'platform':'nexus','user_id':'nexus','chat_id':'nexus','chat_type':'dm','thread_id':'','session_id':'native-root'}))
while True: time.sleep(1)
"#).unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = format!("{}:/usr/bin:/bin", dir.path().display());
        if mode == "cancel" {
            std::fs::write(dir.path().join("cancel"), b"").unwrap();
        }
        let _env = crate::cli::ambient::TestEnvGuard::new(&[
            ("HOME", dir.path().to_str()),
            ("HERMES_HOME", source.to_str()),
            ("PATH", Some(&path)),
        ]);
        let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
        store.migrate().await.unwrap();
        let publisher = GatewayStreamPublisher::new(128);
        let mut frames = publisher.subscribe();
        let state = crate::daemon::AppState::wire_pty_with_gateway_stream(
            store.clone(),
            &nexus_common::Config::default(),
            Some(publisher),
        );
        state.wait_for_runtime_identity_ready().await.unwrap();
        struct Cleanup(crate::daemon::AppState, std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                if self.1.join("input-entered").exists() {
                    let _ = std::fs::write(self.1.join("release"), b"");
                    let deadline = std::time::Instant::now() + Duration::from_secs(2);
                    while !self.1.join("released").exists() && std::time::Instant::now() < deadline
                    {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
                self.0.pty_supervisor().unwrap().kill_all();
            }
        }
        let _cleanup = Cleanup(state.clone(), dir.path().to_owned());
        if mode == "ledger-error" {
            store.identity_conn().execute_batch("CREATE TRIGGER model_ledger_error BEFORE UPDATE OF os_pid ON agent_runtimes WHEN NEW.os_pid IS NOT NULL BEGIN SELECT RAISE(ABORT,'native ledger rejected'); END;").await.unwrap();
        }
        let req = serde_json::from_value(serde_json::json!({"kind":"hermes","name":"native-model","backend":"pty","headless":false,"cwd":dir.path().to_str().unwrap(),"initialPrompt":if mode == "cancel" {Some("fixture input")} else {None}})).unwrap();
        let launched = state.clone();
        let task = tokio::spawn(async move { launched.launch_agent(req, "default", None).await });
        let native: serde_json::Value = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(bytes) = std::fs::read(dir.path().join("witness")) {
                    if let Ok(value) = serde_json::from_slice(&bytes) {
                        break value;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual generated gateway process subscribed");
        let session = SessionId(native["session"].as_str().unwrap().into());
        let staged = AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap();
        if mode != "ledger-error" {
            assert!(
                staged
                    .as_ref()
                    .is_some_and(|row| row.model_observer_token.is_some()),
                "native setup must be preceded by captured model claim: {mode}"
            );
        }
        // The ledger failure can already have revoked its committed claim before the fake
        // process writes the witness. Success/cancel above establish pre-native claim ordering.
        assert!(!staged.unwrap().model_report.unwrap().observer_active);
        if mode == "cancel" {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !dir.path().join("input-entered").exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("actual native input entered before cancellation");
            assert!(!task.is_finished());
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            let result = task.await.unwrap();
            if mode == "ledger-error" {
                assert!(result
                    .unwrap_err()
                    .message
                    .contains("native ledger rejected"));
            } else {
                result.unwrap();
            }
        }
        if mode != "success" {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let row = AgentRuntimes::new(&store)
                        .find_by_runtime_id(&session.0)
                        .await
                        .unwrap()
                        .unwrap();
                    if row.model_observer_token.is_none() {
                        assert!(!row.model_report.unwrap().observer_active);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("captured failed launch revokes without shutdown");
        } else {
            std::fs::write(dir.path().join("emit"), b"").unwrap();
            let fresh_frame = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let GatewayStreamFrame::Projection { event } = frames.recv().await.unwrap() {
                        if event.payload["runtimeId"] == session.0
                            && event.payload["modelReport"]["configured"]["observation"]["modelId"]
                                == "native-configured"
                            && event.payload["modelReport"]["telemetry"]["usage"]["observation"]
                                ["inputTokens"]
                                == 120
                        {
                            assert_eq!(event.payload["modelReport"]["observerActive"], true);
                            assert_eq!(event.payload["modelReport"]["backend"], "hermes.gateway");
                            break event.payload;
                        }
                    }
                }
            })
            .await
            .expect("native hook reaches canonical Rust publisher");
            let first = AgentRuntimes::new(&store)
                .find_by_runtime_id(&session.0)
                .await
                .unwrap()
                .unwrap();
            state
                .ensure_harness_live("native-model", "default")
                .await
                .unwrap();
            assert_eq!(
                AgentRuntimes::new(&store)
                    .find_by_runtime_id(&session.0)
                    .await
                    .unwrap()
                    .unwrap()
                    .model_observer_token,
                first.model_observer_token
            );
            state.pty_supervisor().unwrap().kill(&session);
            state.presence.materialize_offline(&session).await.unwrap();
            state
                .ensure_harness_live("native-model", "default")
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let row = AgentRuntimes::new(&store)
                        .find_by_runtime_id(&session.0)
                        .await
                        .unwrap()
                        .unwrap();
                    if row.model_observer_token.is_some()
                        && row.model_observer_token != first.model_observer_token
                        && row
                            .model_report
                            .as_ref()
                            .is_some_and(|report| report.observer_active)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cold native resume owns a fresh observed claim");
            let mut newer = model_projection_artifacts::next(
                &mut frames,
                &session.0,
                fresh_frame["modelReport"]["reportRevision"]
                    .as_u64()
                    .unwrap(),
                true,
                "configured",
            )
            .await;
            while newer["modelReport"]["telemetry"]["usage"]["observation"]["inputTokens"] != 120 {
                newer = model_projection_artifacts::next(
                    &mut frames,
                    &session.0,
                    newer["modelReport"]["reportRevision"].as_u64().unwrap(),
                    true,
                    "configured",
                )
                .await;
            }
            state.presence.materialize_offline(&session).await.unwrap();
            let stopped = model_projection_artifacts::next(
                &mut frames,
                &session.0,
                newer["modelReport"]["reportRevision"].as_u64().unwrap(),
                false,
                "configured",
            )
            .await;
            for body in [&fresh_frame, &newer, &stopped] {
                let usage = &body["modelReport"]["telemetry"]["usage"]["observation"];
                assert_eq!(usage["scope"], "sessionCumulative");
                assert_eq!(usage["inputTokens"], 120);
                assert_eq!(usage["cacheReadTokens"], 80);
                assert_eq!(usage["cacheWriteTokens"], 20);
                assert!(usage.get("totalTokens").is_none());
                assert!(usage.get("resetId").is_none());
                assert_eq!(
                    body["modelReport"]["telemetry"]["context"]["capability"],
                    "unsupported"
                );
            }
            model_projection_artifacts::export("hermes", "headed", &fresh_frame, &newer, &stopped);
        }
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn headed_model_projection_opencode_stages_and_publishes_fresh_resume_offline() {
    opencode_model_root_fixture("success").await;
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn opencode_model_root_cancel_and_required_ledger_error_do_not_activate() {
    for mode in ["cancel", "ledger-error"] {
        opencode_model_root_fixture(mode).await;
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn opencode_model_root_cold_resume_rejects_wrong_root_and_retains_liveness_cause() {
    for mode in ["resume-owner", "resume-error", "resume-mismatch"] {
        opencode_model_root_fixture(mode).await;
    }
}

#[cfg(unix)]
async fn opencode_model_root_fixture(mode: &str) {
    use crate::daemon::gateway_stream_socket::{GatewayStreamFrame, GatewayStreamPublisher};
    use nexus_store::repos::AgentRuntimes;
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let fake = dir.path().join("native-viewer");
    std::fs::write(&fake, r#"#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const home = process.env.HOME;
const root = fs.existsSync(path.join(home,'wrong-root')) ? 'ses_wrong' : 'ses_exact';
const wait = p => new Promise(resolve => { const t = setInterval(() => {if(fs.existsSync(p)){clearInterval(t);resolve();}},5); });
(async () => {
  fs.writeFileSync(path.join(home,'witness'), JSON.stringify({session:process.env.NEXUS_SESSION_ID,pid:process.pid}));
  await wait(path.join(home,'go'));
  fs.writeFileSync(process.env.NEXUS_OPENCODE_READY_PATH,JSON.stringify({sessionId:root,pid:process.pid,readyOwner:fs.existsSync(path.join(home,'wrong-owner'))?'foreign-owner':process.env.NEXUS_NATIVE_READY_OWNER}));
  process.env.NEXUS_OPENCODE_SERVER_URL = 'http://native.invalid';
  process.env.OPENCODE_SERVER_PASSWORD = 'fixture';
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    if (String(url).endsWith('/turn/next')) return new Promise(() => {});
    if (String(url).endsWith('/config/providers')) return new Response(JSON.stringify({providers:[{id:'native-provider',models:{'opaque-selected':{limit:{context:200000}}}}]}),{status:200});
    if (String(url).startsWith('http://native.invalid/')) return new Response(JSON.stringify({id:root}),{status:200});
    return realFetch(url,init);
  };
  const {nexus} = await import(pathToFileURL(process.env.NEXUS_OPENCODE_PLUGIN_PATH).href);
  const hooks = await nexus();
  await wait(path.join(home,'emit'));
  await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:root,id:'native-message',role:'assistant',modelID:'opaque-selected',providerID:'native-provider',time:{created:123}}}}});
  await hooks.event({event:{type:'message.updated',properties:{info:{sessionID:root,id:'native-message',role:'assistant',modelID:'opaque-selected',providerID:'native-provider',finish:'stop',tokens:{input:40000,output:1000,reasoning:100,cache:{read:800,write:100}}}}}});
  setInterval(()=>{},1000);
})().catch(e=>{console.error(e);process.exit(1)});
"#).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let _env = crate::cli::ambient::TestEnvGuard::new(&[
        ("HOME", dir.path().to_str()),
        ("NEXUS_NODE_BIN", fake.to_str()),
        ("NEXUS_OPENCODE_BIN", fake.to_str()),
    ]);
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(128);
    let mut frames = publisher.subscribe();
    let state = crate::daemon::AppState::wire_pty_with_gateway_stream(
        store.clone(),
        &nexus_common::Config::default(),
        Some(publisher),
    );
    state.wait_for_runtime_identity_ready().await.unwrap();
    struct Cleanup(crate::daemon::AppState);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.pty_supervisor().unwrap().kill_all();
        }
    }
    let _cleanup = Cleanup(state.clone());
    let req: nexus_contracts::SpawnRequest = serde_json::from_value(serde_json::json!({"kind":"opencode","name":"model-root","backend":"pty","headless":false,"cwd":dir.path().to_str().unwrap()})).unwrap();
    let launched = state.clone();
    let task = tokio::spawn(async move { launched.launch_agent(req, "default", None).await });
    let witness = dir.path().join("witness");
    let native: serde_json::Value = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(bytes) = std::fs::read(&witness) {
                if let Ok(value) = serde_json::from_slice(&bytes) {
                    break value;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual fake native launch reached ready gate");
    let session = SessionId(native["session"].as_str().unwrap().into());
    let staged = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap();
    assert!(
        staged
            .as_ref()
            .is_some_and(|r| r.model_observer_token.is_some()),
        "claim committed before native launch"
    );
    assert!(!staged.unwrap().model_report.unwrap().observer_active);
    if mode == "cancel" {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    } else {
        if mode == "ledger-error" {
            store.identity_conn().execute_batch("CREATE TRIGGER native_model_ledger_error BEFORE UPDATE OF os_pid ON agent_runtimes WHEN NEW.os_pid IS NOT NULL BEGIN SELECT RAISE(ABORT,'captured ledger failure'); END;").await.unwrap();
        }
        std::fs::write(dir.path().join("go"), b"").unwrap();
        let result = task.await.unwrap();
        if mode == "ledger-error" {
            let error = result.unwrap_err();
            assert!(
                error
                    .message
                    .contains("opencode process-ledger persistence failed"),
                "{error:?}"
            );
            assert!(error.message.contains("captured ledger failure"));
            assert!(
                state
                    .pty_supervisor()
                    .unwrap()
                    .has_opencode_plugin(&session),
                "failure is after native binding returned"
            );
        } else {
            result.unwrap();
        }
    }
    if matches!(mode, "cancel" | "ledger-error") {
        let row = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let row = AgentRuntimes::new(&store)
                    .find_by_runtime_id(&session.0)
                    .await
                    .unwrap()
                    .unwrap();
                if row.model_observer_token.is_none() {
                    break row;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("captured launch failure revokes before coordinator shutdown");
        assert!(!row.model_report.unwrap().observer_active);
        state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
        return;
    }
    std::fs::write(dir.path().join("emit"), b"").unwrap();
    let fresh_frame = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let GatewayStreamFrame::Projection { event } = frames.recv().await.unwrap() {
                if event.payload["runtimeId"] == session.0
                    && event.payload["modelReport"]["turnSelected"]["observation"]["modelId"]
                        == "opaque-selected"
                    && event.payload["modelReport"]["telemetry"]["context"]["observation"]
                        ["usedTokens"]["value"]
                        == 42000
                {
                    break event.payload;
                }
            }
        }
    })
    .await
    .expect("actual generated plugin through root and canonical publisher");
    assert_eq!(fresh_frame["modelReport"]["observerActive"], true);
    assert_eq!(fresh_frame["modelReport"]["backend"], "opencode.plugin");
    let first = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    state
        .ensure_opencode_plugin_live("model-root", "default")
        .await
        .unwrap();
    assert_eq!(
        AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
            .unwrap()
            .model_observer_token,
        first.model_observer_token,
        "hot path retains owner"
    );
    state.pty_supervisor().unwrap().kill(&session);
    state.presence.materialize_offline(&session).await.unwrap();
    std::fs::remove_file(&witness).unwrap();
    std::fs::remove_file(dir.path().join("emit")).unwrap();
    if mode.starts_with("resume-") {
        if mode == "resume-error" {
            store.conn.execute_batch("CREATE TRIGGER native_model_presence_error BEFORE UPDATE OF last_heartbeat ON sessions BEGIN SELECT RAISE(ABORT,'captured native heartbeat failure'); END;").await.unwrap();
        } else if mode == "resume-owner" {
            std::fs::write(dir.path().join("wrong-owner"), b"").unwrap();
        } else {
            std::fs::write(dir.path().join("wrong-root"), b"").unwrap();
        }
        let error = state
            .ensure_opencode_plugin_live("model-root", "default")
            .await
            .unwrap_err();
        let expected = if mode == "resume-error" {
            "captured native heartbeat failure"
        } else {
            "native ready root"
        };
        assert!(error.message.contains(expected), "{error:?}");
        state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
        let failed = AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
            .unwrap();
        assert!(failed.model_observer_token.is_none());
        assert!(!failed.model_report.unwrap().observer_active);
        return;
    }
    state
        .ensure_opencode_plugin_live("model-root", "default")
        .await
        .unwrap();
    let resumed = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        resumed.model_observer_token, first.model_observer_token,
        "cold resume captures new ownership"
    );
    std::fs::write(dir.path().join("emit"), b"").unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = AgentRuntimes::new(&store)
                .find_by_runtime_id(&session.0)
                .await
                .unwrap()
                .unwrap();
            if row.model_report.as_ref().is_some_and(|r| {
                r.observer_active
                    && matches!(
                        r.turn_selected,
                        nexus_contracts::ModelEvidenceSlot::Observed { .. }
                    )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut newer = model_projection_artifacts::next(
        &mut frames,
        &session.0,
        fresh_frame["modelReport"]["reportRevision"]
            .as_u64()
            .unwrap(),
        true,
        "turnSelected",
    )
    .await;
    while newer["modelReport"]["telemetry"]["context"]["observation"]["usedTokens"]["value"]
        != 42000
    {
        newer = model_projection_artifacts::next(
            &mut frames,
            &session.0,
            newer["modelReport"]["reportRevision"].as_u64().unwrap(),
            true,
            "turnSelected",
        )
        .await;
    }
    state.presence.materialize_offline(&session).await.unwrap();
    let offline = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let GatewayStreamFrame::Projection { event } = frames.recv().await.unwrap() {
                if event.payload["runtimeId"] == session.0
                    && event.payload["modelReport"]["reportRevision"].as_i64()
                        == Some(offline.model_report_revision)
                    && event.payload["active"] == false
                {
                    break event.payload;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(frame["active"], false);
    assert_eq!(frame["stoppedAt"], serde_json::json!(offline.stopped_at));
    assert_eq!(
        frame["modelReport"],
        serde_json::to_value(offline.model_report).unwrap()
    );
    model_projection_artifacts::export("opencode", "headed", &fresh_frame, &newer, &frame);
    state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}

struct ClaudeModelCapture {
    identity: nexus_contracts::model_report::ModelProfileIdentity,
    updates: Mutex<Vec<nexus_contracts::model_report::NativeModelUpdate>>,
    closed: std::sync::atomic::AtomicBool,
    telemetry: Mutex<Vec<nexus_contracts::telemetry::NativeTelemetryUpdate>>,
}
impl nexus_contracts::model_report::ModelObservationSink for ClaudeModelCapture {
    fn accepts_profile(
        &self,
        identity: &nexus_contracts::model_report::ModelProfileIdentity,
    ) -> bool {
        self.identity.matches(identity) && !self.closed.load(Ordering::SeqCst)
    }
    fn bind_native_root(&self, root: &str) -> bool {
        root == "native" && !self.closed.load(Ordering::SeqCst)
    }
    fn observe(&self, update: nexus_contracts::model_report::NativeModelUpdate) -> bool {
        if self.closed.load(Ordering::SeqCst) {
            return false;
        }
        self.updates.lock().unwrap().push(update);
        true
    }
    fn revoke(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
    fn observe_telemetry(&self, update: nexus_contracts::telemetry::NativeTelemetryUpdate) -> bool {
        if self.closed.load(Ordering::SeqCst) {
            return false;
        }
        self.telemetry.lock().unwrap().push(update);
        true
    }
}

fn claude_model_capture() -> (
    Arc<ClaudeModelCapture>,
    nexus_agent::adapter::NativeModelReporting,
) {
    let profile = nexus_harness_claude::native::model_reporting::profile();
    let sink = Arc::new(ClaudeModelCapture {
        identity: profile.identity().clone(),
        updates: Mutex::new(Vec::new()),
        closed: std::sync::atomic::AtomicBool::new(false),
        telemetry: Mutex::new(Vec::new()),
    });
    let pair = profile.capture(sink.clone()).unwrap();
    (sink, pair)
}

#[test]
fn claude_response_usage_has_independent_api_message_dedupe_under_captured_source() {
    use nexus_contracts::telemetry::{NativeTelemetryUpdate, NativeTelemetryValue};
    use nexus_harness_claude::native::model_reporting::ClaudeTranscriptRead;
    let dir = temp_test_dir("claude-usage-owner");
    let path = dir.join("transcript.jsonl");
    std::fs::write(&path, b"").unwrap();
    let owner = ClaudeTurnCompletion::new(Some("native".into()), None);
    let (sink, pair) = claude_model_capture();
    assert!(owner.attach_model_reporting(pair, claude_model_source(&path)));
    let mut bytes = String::new();
    for (index, root, id, input, stop) in [
        (0, "native", "api_one", 120, false),
        (1, "native", "api_one", 120, true),
        (2, "native", "api_one", 999, true),
        (3, "child", "api_two", 999, true),
        (4, "native", "api_two", 12, true),
    ] {
        let row = serde_json::json!({"type":"assistant","isSidechain":false,"sessionId":root,"uuid":format!("block-{index}"),"message":{"type":"message","role":"assistant","id":id,"model":"response-native","stop_reason":if stop {Some("end_turn")} else {None},"usage":{"input_tokens":input,"output_tokens":30,"cache_read_input_tokens":80,"cache_creation_input_tokens":20}}});
        bytes.push_str(&serde_json::to_string(&row).unwrap());
        bytes.push('\n');
        std::fs::write(&path, &bytes).unwrap();
        let read = ClaudeTranscriptRead::open(&path).unwrap();
        owner.observe_models(&path, &read.response_models_after(0), Some(&read), true);
        let expected = match index {
            0 => 0,
            1..=3 => 1,
            _ => 2,
        };
        assert_eq!(
            sink.telemetry.lock().unwrap().len(),
            expected,
            "row {index}: model dedupe must not suppress usage or count content blocks twice"
        );
    }
    let values = sink.telemetry.lock().unwrap();
    let NativeTelemetryUpdate::Usage {
        value: NativeTelemetryValue::Observed(usage),
        ..
    } = values.last().unwrap()
    else {
        panic!("usage")
    };
    assert_eq!(usage.input_tokens.unwrap().get(), 12);
    drop(values);
    owner.close_model_reporting();
    assert!(sink.closed.load(Ordering::SeqCst));
}

fn claude_model_source(
    path: &std::path::Path,
) -> crate::daemon::claude_native_forwarder::ClaudeModelSource {
    crate::daemon::claude_native_forwarder::ClaudeModelSource::Ready(
        nexus_harness_claude::native::model_reporting::capture_response_source(
            &path.with_extension("no-hooks"),
            0,
            Some("native"),
            Some(("native", path)),
        )
        .unwrap()
        .unwrap(),
    )
}

struct ClaudeSourceInput(Arc<AtomicUsize>);
#[async_trait]
impl HarnessInput for ClaudeSourceInput {
    async fn send_turn(&self, _text: &str) -> Result<(), String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err("input reached".into())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn claude_model_pending_source_fences_both_input_paths_and_preserves_legacy() {
    for observed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let paths = ClaudeNativeBridgePaths::new(dir.path(), &SessionId("pending-source".into()));
        std::fs::create_dir_all(&paths.bridge_dir).unwrap();
        let owner = Arc::new(ClaudeTurnCompletion::new(
            None,
            Some(paths.hook_log_path.clone()),
        ));
        let (sink, pair) = claude_model_capture();
        let source = owner.capture_model_source(&paths, None).unwrap();
        assert!(owner.attach_model_reporting(pair, source));
        let calls = Arc::new(AtomicUsize::new(0));
        let harness = ClaudeNativeHarness {
            input: Arc::new(ClaudeSourceInput(calls.clone())),
            completion: owner.clone(),
        };
        let accept = Arc::new(CountAcceptance::default());
        let mut send = Box::pin(async {
            if observed {
                harness.send_turn_observed("prepared", accept.clone()).await
            } else {
                harness.send_turn("prepared").await
            }
        });
        assert!(
            futures::poll!(&mut send).is_pending(),
            "source must be ready before input handoff"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(accept.count.load(Ordering::SeqCst), 0);
        let path = paths.bridge_dir.join("native.jsonl");
        std::fs::write(&paths.hook_log_path,serde_json::json!({"event":"SessionStart","session_id":"native","transcript_path":path}).to_string()).unwrap();
        owner.observe_hooks(&[], Some(0), true);
        assert_eq!(send.await.unwrap_err(), "input reached");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            sink.updates.lock().unwrap().is_empty(),
            "source readiness is not model evidence"
        );
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let legacy = ClaudeNativeHarness {
        input: Arc::new(ClaudeSourceInput(calls.clone())),
        completion: Arc::new(ClaudeTurnCompletion::default()),
    };
    assert_eq!(
        legacy.send_turn("legacy").await.unwrap_err(),
        "input reached"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn headed_model_projection_claude_attachment_reports_only_new_response() {
    for case in ["resume", "fresh", "delayed", "shutdown-pending"] {
        claude_model_actual_attachment(case).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn claude_model_rejected_admission_preserves_display_but_rejects_modeled_input() {
    claude_model_actual_attachment("rejected-admission").await;
}

#[tokio::test(flavor = "current_thread")]
async fn claude_model_pending_source_timeout_cancel_replacement_and_loss_revoke_only_old() {
    for reason in [
        "timeout",
        "cancel",
        "replaced",
        "forwarder-closed",
        "claim-closed",
        "setup-canceled",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let paths = ClaudeNativeBridgePaths::new(dir.path(), &SessionId("pending-loss".into()));
        let owner = Arc::new(ClaudeTurnCompletion::new(None, None));
        let (sink, pair) = claude_model_capture();
        let source = owner.capture_model_source(&paths, None).unwrap();
        assert!(owner.attach_model_reporting(pair, source));
        let (new_sink, new_pair) = claude_model_capture();
        let new_owner = ClaudeTurnCompletion::new(Some("native".into()), None);
        assert!(new_owner
            .attach_model_reporting(new_pair, claude_model_source(&dir.path().join("new.jsonl"))));
        if reason == "timeout" {
            assert!(owner
                .wait_for_model_source(Duration::ZERO)
                .await
                .unwrap_err()
                .contains("timed out"));
        } else {
            let mut wait = Box::pin(owner.wait_for_model_source(Duration::from_secs(2)));
            assert!(futures::poll!(&mut wait).is_pending());
            match reason {
                "cancel" => drop(wait),
                "replaced" => {
                    owner.invalidate();
                    assert!(wait.await.unwrap_err().contains("replaced"));
                }
                "forwarder-closed" => {
                    owner.close_model_reporting();
                    assert!(wait.await.unwrap_err().contains("closed"));
                }
                "claim-closed" => {
                    sink.closed.store(true, Ordering::SeqCst);
                    assert!(wait.await.unwrap_err().contains("unavailable"));
                }
                "setup-canceled" => {
                    drop(owner.begin_model_source());
                    assert!(wait.await.unwrap_err().contains("canceled"));
                }
                _ => unreachable!(),
            }
        }
        assert!(
            sink.closed.load(Ordering::SeqCst),
            "{reason}: OLD pending claim must close"
        );
        assert!(
            !new_sink.closed.load(Ordering::SeqCst),
            "{reason}: NEW claim must remain usable"
        );
        new_owner
            .wait_for_model_source(Duration::ZERO)
            .await
            .unwrap();
        assert!(
            owner.wait_for_model_source(Duration::ZERO).await.is_err(),
            "{reason}: cannot fall through after loss"
        );
    }
}

async fn claude_model_actual_attachment(case: &str) {
    let fresh = case != "resume";
    let delayed = matches!(case, "delayed" | "shutdown-pending" | "rejected-admission");
    let shutdown_pending = case == "shutdown-pending";
    use crate::daemon::gateway_stream_socket::{GatewayStreamFrame, GatewayStreamPublisher};
    use nexus_store::repos::AgentRuntimes;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(128);
    let mut frames = publisher.subscribe();
    let state = crate::daemon::AppState::wire_pty_with_gateway_stream(
        store.clone(),
        &nexus_common::Config::default(),
        Some(publisher),
    );
    state.wait_for_runtime_identity_ready().await.unwrap();
    // Actual harness descriptor + explicitly captured sidecar owner: real admission,
    // poll forwarder and publisher, not an operator/native process launch or boot adoption.
    let mut request = shared_activation_request("claude-model-root");
    request.harness = nexus_contracts::HarnessId::new("claude").unwrap();
    let session = state.identity.register(request).await.unwrap().session_id;
    let paths = ClaudeNativeBridgePaths::new(dir.path(), &session);
    std::fs::create_dir_all(&paths.bridge_dir).unwrap();
    let owner = if fresh {
        let owner = Arc::new(ClaudeTurnCompletion::new(
            None,
            Some(paths.hook_log_path.clone()),
        ));
        state
            .pty_supervisor()
            .unwrap()
            .claude_completions
            .lock()
            .unwrap()
            .insert(session.clone(), owner.clone());
        owner
    } else {
        seed_managed_claude_owner(state.pty_supervisor().unwrap(), &session)
    };
    let path = paths.bridge_dir.join("actual-native.jsonl");
    let mut row = serde_json::json!({"type":"assistant","isSidechain":false,"sessionId":"native","message":{"type":"message","role":"assistant","id":"history","model":"old","content":[]}});
    let history = if fresh {
        String::new()
    } else {
        row.to_string()
    };
    if !fresh {
        std::fs::write(&path, &history).unwrap();
    }
    if !delayed {
        std::fs::write(
        &paths.hook_log_path,
        serde_json::json!({"event":"SessionStart","session_id":"native","transcript_path":path})
            .to_string(),
    )
    .unwrap();
    }
    ClaudeRuntimeStateRepo::new(&store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: session.clone(),
            bridge_dir: paths.bridge_dir.clone(),
            claude_session_id: (!fresh).then(|| "native".into()),
            launch_cwd: dir.path().into(),
            transcript_path: Some(path.clone()),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    if case == "rejected-admission" {
        state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
    }
    state
        .spawn_claude_native_forwarder_if_needed(&session)
        .await;
    let claimed = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    if case == "rejected-admission" {
        assert!(
            claimed.model_observer_token.is_none(),
            "closed coordinator cannot grant a model claim"
        );
        assert!(
            owner.wait_for_model_source(Duration::ZERO).await.is_err(),
            "model-enabled input cannot fall back after admission rejection"
        );
        std::fs::write(&paths.message_delta_log_path,r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"display remains independent"}}"#).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let GatewayStreamFrame::AgentUpdate {
                    session_id,
                    kind,
                    data,
                    ..
                } = frames.recv().await.unwrap()
                {
                    if session_id == session.0 && kind == nexus_contracts::AgentUpdateKind::Text {
                        assert_eq!(data["text"], "display remains independent");
                        break;
                    }
                }
            }
        })
        .await
        .expect("rejected model attachment must not suppress native display forwarding");
        owner.invalidate();
        return;
    }
    assert!(
        claimed.model_observer_token.is_some(),
        "actual attachment must own a committed claim before native output"
    );
    assert!(!claimed.model_report.as_ref().unwrap().observer_active);
    if shutdown_pending {
        let mut ready = Box::pin(owner.wait_for_model_source(Duration::from_secs(2)));
        assert!(futures::poll!(&mut ready).is_pending());
        state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
        let error = ready.await.unwrap_err();
        assert!(
            error.contains("unavailable") || error.contains("closed"),
            "{error}"
        );
        assert!(
            owner.is_current(),
            "model shutdown is not native-owner invalidation"
        );
        owner.invalidate();
        return;
    }
    if delayed {
        let mut ready = Box::pin(owner.wait_for_model_source(Duration::from_secs(2)));
        assert!(futures::poll!(&mut ready).is_pending());
        std::fs::write(&paths.hook_log_path, serde_json::json!({"event":"SessionStart","session_id":"native","transcript_path":path}).to_string()).unwrap();
        ready.await.unwrap();
    }
    row["message"]["id"] = serde_json::json!("unknown");
    row["message"].as_object_mut().unwrap().remove("model");
    let unknown = format!("{history}\n{row}");
    std::fs::write(&path, &unknown).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let native = ClaudeRuntimeStateRepo::new(&store)
                .find_by_runtime_id(&session)
                .await
                .unwrap()
                .unwrap();
            if native.transcript_cursor >= unknown.len() as i64 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap()
            .unwrap()
            .model_report
            .unwrap()
            .observer_active,
        "Unknown cannot activate reporting"
    );
    row["message"]["id"] = serde_json::json!("fresh-response");
    row["message"]["model"] = serde_json::json!("claude-native-response");
    row["message"]["stop_reason"] = serde_json::json!("end_turn");
    row["message"]["usage"] = serde_json::json!({"input_tokens":120,"output_tokens":30,"cache_read_input_tokens":80,"cache_creation_input_tokens":20,"output_tokens_details":{"thinking_tokens":12}});
    std::fs::write(&path, format!("{unknown}\n{row}")).unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let GatewayStreamFrame::Projection { event } = frames.recv().await.unwrap() {
                let payload = event.payload;
                if payload["runtimeId"] == session.0
                    && payload["modelReport"]["responseReported"]["observation"]["modelId"]
                        == "claude-native-response"
                    && payload["modelReport"]["telemetry"]["usage"]["observation"]["inputTokens"]
                        == 120
                {
                    break payload;
                }
            }
        }
    })
    .await
    .expect("actual Claude forwarder must publish its accepted response model");
    let durable = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame["agentId"], durable.agent_id);
    assert_eq!(
        frame["modelReport"],
        serde_json::to_value(durable.model_report.unwrap()).unwrap()
    );
    assert_eq!(frame["modelReport"]["observerActive"], true);
    assert_eq!(
        frame["modelReport"]["responseReported"]["observation"]["nativeSessionId"],
        "native"
    );
    assert_eq!(
        frame["modelReport"]["responseReported"]["observation"]["nativeMessageId"],
        "fresh-response"
    );
    assert!(frame.get("modelObserverToken").is_none());
    let newer = if case == "fresh" {
        let first_record = row.to_string();
        row["message"]["id"] = serde_json::json!("second-response");
        row["message"]["model"] = serde_json::json!("claude-next-response");
        std::fs::write(&path, format!("{unknown}\n{first_record}\n{row}")).unwrap();
        Some(
            model_projection_artifacts::next(
                &mut frames,
                &session.0,
                frame["modelReport"]["reportRevision"].as_u64().unwrap(),
                true,
                "responseReported",
            )
            .await,
        )
    } else {
        None
    };
    owner.invalidate();
    state.presence.materialize_offline(&session).await.unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let GatewayStreamFrame::Projection { event } = frames.recv().await.unwrap() {
                if event.payload["runtimeId"] == session.0 && event.payload["presence"] == "offline"
                {
                    break event.payload;
                }
            }
        }
    })
    .await
    .unwrap();
    let durable = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stopped["active"], false);
    assert_eq!(stopped["stoppedAt"], durable.stopped_at.unwrap());
    assert_eq!(
        stopped["modelReport"],
        serde_json::to_value(durable.model_report.unwrap()).unwrap()
    );
    if let Some(newer) = newer {
        assert_eq!(
            newer["modelReport"]["responseReported"]["observation"]["modelId"],
            "claude-next-response"
        );
        model_projection_artifacts::export("claude", "headed", &frame, &newer, &stopped);
    }
    state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn claude_model_forwarder_cancelled_before_first_poll_closes_only_captured_reporting() {
    use crate::daemon::claude_native_forwarder::spawn_claude_native_forwarder_with_tool_events;
    let dir = tempfile::tempdir().unwrap();
    let session = SessionId("claude-model-cancel".into());
    let paths = ClaudeNativeBridgePaths::new(dir.path(), &session);
    std::fs::create_dir_all(&paths.bridge_dir).unwrap();
    let path = paths.bridge_dir.join("transcript.jsonl");
    std::fs::write(&path, "").unwrap();
    let owner = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let (sink, pair) = claude_model_capture();
    assert!(owner.attach_model_reporting(pair, claude_model_source(&path)));
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    let worker = spawn_claude_native_forwarder_with_tool_events(
        store,
        session,
        paths,
        Arc::new(DiscardClaudeDisplay),
        Bell::new(),
        250,
        None,
        Some(owner.clone()),
    );
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert!(
        sink.closed.load(Ordering::SeqCst),
        "unpolled task cancellation must drop its captured reporter"
    );
    assert!(
        owner.is_current(),
        "model revocation does not fabricate native process death"
    );
}

#[test]
fn claude_model_completion_fences_history_replay_foreign_root_and_replaced_owner() {
    use nexus_harness_claude::native::model_reporting::{
        ClaudeResponseModel, ClaudeTranscriptRead,
    };
    let dir = temp_test_dir("claude-model-owner");
    let path = dir.join("transcript.jsonl");
    std::fs::write(&path, b"history").unwrap();
    let initial = ClaudeTranscriptRead::open(&path).unwrap();
    let owner = ClaudeTurnCompletion::new(Some("native".into()), None);
    let (sink, pair) = claude_model_capture();
    assert!(owner.attach_model_reporting(pair.clone(), claude_model_source(&path)));
    assert!(
        !owner.attach_model_reporting(pair, claude_model_source(&path)),
        "one source lifetime per native owner"
    );
    let model = ClaudeResponseModel {
        native_session_id: "native".into(),
        native_message_id: "msg-1".into(),
        model: Ok(Some("response-exact".into())),
        native_reported_at: Some(1),
        usage: None,
    };
    owner.observe_models(&path, &[(model.clone(), 7)], Some(&initial), true);
    assert!(
        sink.updates.lock().unwrap().is_empty(),
        "captured EOF excludes resumed history"
    );
    std::fs::write(&path, b"history new one").unwrap();
    let next = ClaudeTranscriptRead::open(&path).unwrap();
    let mut foreign = model.clone();
    foreign.native_session_id = "child".into();
    owner.observe_models(
        &path,
        &[(foreign, 8), (model.clone(), 15)],
        Some(&next),
        true,
    );
    assert_eq!(sink.updates.lock().unwrap().len(), 1);
    std::fs::write(&path, b"history new one replay").unwrap();
    let replay = ClaudeTranscriptRead::open(&path).unwrap();
    owner.observe_models(&path, &[(model.clone(), 22)], Some(&replay), true);
    assert_eq!(
        sink.updates.lock().unwrap().len(),
        1,
        "same native message cannot become a new response"
    );
    owner.invalidate();
    assert!(sink.closed.load(Ordering::SeqCst));
    owner.observe_models(&path, &[(model, 22)], Some(&replay), true);
    assert_eq!(sink.updates.lock().unwrap().len(), 1);
    drop((initial, next, replay, owner));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn claude_model_completion_source_loss_revoke_is_sticky_and_drop_closes() {
    use nexus_harness_claude::native::model_reporting::ClaudeTranscriptRead;
    let dir = temp_test_dir("claude-model-source");
    let path = dir.join("transcript.jsonl");
    std::fs::write(&path, b"old").unwrap();
    let initial = ClaudeTranscriptRead::open(&path).unwrap();
    let owner = ClaudeTurnCompletion::new(Some("native".into()), None);
    let (sink, pair) = claude_model_capture();
    assert!(owner.attach_model_reporting(pair, claude_model_source(&path)));
    std::fs::rename(&path, dir.join("old.jsonl")).unwrap();
    std::fs::write(&path, b"old new").unwrap();
    let replacement = ClaudeTranscriptRead::open(&path).unwrap();
    owner.observe_models(&path, &[], Some(&replacement), true);
    assert!(
        sink.closed.load(Ordering::SeqCst),
        "same-path replacement closes captured reporting, not activity policy"
    );
    assert!(owner.is_current());
    let (new_sink, new_pair) = claude_model_capture();
    assert!(!owner.attach_model_reporting(new_pair.clone(), claude_model_source(&path)));
    let next_owner = ClaudeTurnCompletion::new(Some("native".into()), None);
    assert!(next_owner.attach_model_reporting(new_pair, claude_model_source(&path)));
    drop(next_owner);
    assert!(new_sink.closed.load(Ordering::SeqCst));
    drop((owner, initial, replacement));
    std::fs::remove_dir_all(dir).unwrap();
}

fn native_hook(kind: &str, prompt: &str, offset: u64) -> ClaudeHookRecord {
    parse_hook_record(
        &serde_json::json!({"event": kind, "session_id": "native", "prompt_id": "A", "prompt": prompt}),
        offset,
    )
}

struct LivenessProbeInput {
    alive: bool,
}

struct SubmissionSignalWriter {
    completion: Arc<ClaudeTurnCompletion>,
    writes: Arc<Mutex<Vec<Vec<u8>>>>,
}

struct StructuredAcceptedInput {
    completion: Arc<ClaudeTurnCompletion>,
    emit_native_receipt: bool,
}

#[async_trait]
impl HarnessInput for StructuredAcceptedInput {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        if self.emit_native_receipt {
            let submit = native_hook("UserPromptSubmit", text, 1);
            self.completion.observe_hooks(
                &[submit.clone(), native_hook("Stop", text, 2)],
                Some(2),
                true,
            );
            self.completion.accept_native_user_input(&submit).await;
        }
        self.completion.signal();
        Ok(())
    }
}

#[derive(Default)]
struct CountAcceptance {
    count: AtomicUsize,
}

#[async_trait]
impl TurnAcceptanceObserver for CountAcceptance {
    async fn accepted(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

impl nexus_pty::TerminalWriter for SubmissionSignalWriter {
    fn write_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        self.writes.lock().unwrap().push(bytes.to_vec());
        if bytes == b"\r" {
            self.completion.signal_submission();
        }
        Ok(())
    }
}

struct SubmissionSignalTerminal {
    output: tokio::sync::broadcast::Sender<Vec<u8>>,
    writer: Arc<SubmissionSignalWriter>,
}

fn echoing_pty_program() -> &'static str {
    #[cfg(windows)]
    {
        "cmd.exe"
    }
    #[cfg(not(windows))]
    {
        "cat"
    }
}

impl TerminalBackend for SubmissionSignalTerminal {
    fn attach(&self) -> nexus_pty::TerminalAttachment {
        nexus_pty::TerminalAttachment {
            reader: self.output.subscribe(),
            writer: self.writer.clone(),
        }
    }

    fn resize(&self, _cols: u16, _rows: u16) -> Result<(), String> {
        Ok(())
    }

    fn current_size(&self) -> Option<(u16, u16)> {
        Some((120, 30))
    }
}

#[async_trait]
impl HarnessInput for LivenessProbeInput {
    async fn send_turn(&self, _text: &str) -> Result<(), String> {
        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.alive
    }
}

#[test]
fn opencode_plugin_input_is_dead_when_its_headed_runtime_exits() {
    let bridge: Arc<dyn HarnessInput> = Arc::new(LivenessProbeInput { alive: true });
    let runtime: Arc<dyn HarnessInput> = Arc::new(LivenessProbeInput { alive: false });

    let input = OpenCodeHeadedHarness::new(bridge, runtime);

    assert!(
        !input.is_alive(),
        "a live loopback bridge cannot keep an exited OpenCode TUI online"
    );
}

#[test]
fn opencode_viewer_backend_preserves_requested_mode() {
    assert_eq!(opencode_viewer_backend_kind("pty").unwrap(), "raw");
    assert_eq!(opencode_viewer_backend_kind("tmux").unwrap(), "tmux");
    assert!(opencode_viewer_backend_kind("screen").is_err());
}

#[tokio::test]
async fn opencode_headed_observed_receipt_precedes_completion_without_late_echo() {
    use crate::daemon::opencode_plugin_bridge::OpenCodePluginBridge;
    use serde_json::{json, Value};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn request(
        bridge: &OpenCodePluginBridge,
        method: &str,
        path: &str,
        body: Value,
    ) -> (u16, Value) {
        let body = body.to_string();
        let mut stream = tokio::net::TcpStream::connect(
            bridge
                .endpoint()
                .base_url()
                .strip_prefix("http://")
                .unwrap(),
        )
        .await
        .unwrap();
        stream.write_all(format!(
            "{method} {path} HTTP/1.1\r\nHost: bridge\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            bridge.endpoint().token(), body.len()
        ).as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let status = response.split_whitespace().nth(1).unwrap().parse().unwrap();
        let body = response.split_once("\r\n\r\n").unwrap().1;
        (
            status,
            if body.is_empty() {
                Value::Null
            } else {
                serde_json::from_str(body).unwrap()
            },
        )
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        let bridge = OpenCodePluginBridge::start(
            SessionId("s_headed_receipt_fixture".into()),
            Arc::new(DiscardClaudeDisplay),
            Default::default(),
        )
        .await
        .unwrap();
        let input = Arc::new(OpenCodeHeadedHarness::new(
            bridge.input(),
            Arc::new(LivenessProbeInput { alive: true }),
        ));
        let count = Arc::new(CountAcceptance::default());
        // Equal text is two intentional submissions, not a content-deduplication key.
        for ordinal in 1..=2 {
            let (input, observer) = (input.clone(), count.clone());
            let pending =
                tokio::spawn(
                    async move { input.send_turn_observed("still there?", observer).await },
                );
            let (status, turn) = request(&bridge, "GET", "/turn/next", Value::Null).await;
            assert_eq!(status, 200);
            let path = format!("/turn/{}", turn["id"].as_str().unwrap());
            let binding = json!({"sessionID":"ses_exact", "messageID":format!("msg_{ordinal}")});
            assert_eq!(
                request(&bridge, "POST", &format!("{path}/bind"), binding.clone())
                    .await
                    .0,
                204
            );
            for _ in 0..2 {
                let (status, receipt) = request(
                    &bridge,
                    "POST",
                    &format!("{path}/accepted"),
                    binding.clone(),
                )
                .await;
                assert_eq!(status, 200);
                assert_eq!(
                    receipt["canonicalEcho"], true,
                    "headed wrapper must pass acceptance authority to the bridge"
                );
            }
            assert_eq!(count.count.load(Ordering::SeqCst), ordinal);
            assert!(
                !pending.is_finished(),
                "native admission must precede completion"
            );
            assert_eq!(
                request(&bridge, "POST", &format!("{path}/complete"), json!({}))
                    .await
                    .0,
                204
            );
            pending.await.unwrap().unwrap();
            assert_eq!(
                count.count.load(Ordering::SeqCst),
                ordinal,
                "completion must not emit another user echo"
            );
        }
        bridge.shutdown();
    })
    .await
    .expect("headed receipt fixture timed out");
}

#[test]
fn opencode_native_viewer_does_not_use_daemon_terminal_query_replies() {
    assert!(!raw_pty_query_responder(HeadedRuntimeKind::OpenCodePlugin));
    assert!(raw_pty_query_responder(HeadedRuntimeKind::ClaudeNative));
    assert!(raw_pty_query_responder(HeadedRuntimeKind::HermesGateway));
}

#[test]
fn claude_raw_prompt_detection_tracks_a_wrapped_live_draft() {
    let live = "status\n❯ existential humanist; begin from Nietzschean self-overcoming.\n  This is explicitly a load-test debate.\n  If a Nexus action fails, stop and wait for the operator.\n────────────────────────────────────────\n⏵⏵ bypass permissions on";
    assert!(claude_raw_prompt_rendered(live));
    assert!(claude_raw_draft_in_input_box(
        live,
        "You are Simone, the claude/pty participant",
        "stop and wait for the operator."
    ));

    let settled = "❯ stop and wait for the operator.\nassistant response\n────────────────────────────────────────\n❯\n────────────────────────────────────────\n⏵⏵ bypass permissions on";
    assert!(claude_raw_prompt_rendered(settled));
    assert!(!claude_raw_draft_in_input_box(
        settled,
        "You are Simone, the claude/pty participant",
        "stop and wait for the operator."
    ));
}

#[tokio::test]
async fn claude_raw_submit_accepts_structured_hook_evidence_while_queue_preview_remains() {
    let completion = Arc::new(ClaudeTurnCompletion::default());
    let writes = Arc::new(Mutex::new(Vec::new()));
    let (output, _keepalive) = tokio::sync::broadcast::channel(16);
    let backend = Arc::new(SubmissionSignalTerminal {
        output: output.clone(),
        writer: Arc::new(SubmissionSignalWriter {
            completion: completion.clone(),
            writes: writes.clone(),
        }),
    });
    let terminal = ScreenModelBackend::wrap(backend as Arc<dyn TerminalBackend>);
    output
        .send(
            b"tool call running\r\n\xe2\x9d\xaf queued during a tool loop\r\n------------------------\r\n"
                .to_vec(),
        )
        .unwrap();
    let ready_deadline = Instant::now() + Duration::from_secs(1);
    while !terminal.contents().contains("queued during a tool loop")
        && Instant::now() < ready_deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let input = ClaudeRawPtyInput {
        input: Arc::new(LivenessProbeInput { alive: true }),
        terminal,
        completion: completion.clone(),
    };
    completion.observe_hooks(&[native_hook("UserPromptSubmit", "old", 1)], Some(1), true);
    tokio::time::timeout(
        Duration::from_secs(2),
        input.submit_prompt("queued during a tool loop"),
    )
    .await
    .expect("structured hook acceptance should settle before the screen preview disappears")
    .expect("Claude raw PTY submission should succeed");

    assert!(writes.lock().unwrap().iter().any(|bytes| bytes == b"\r"));
    assert_eq!(
        &writes.lock().unwrap()[..2],
        &[vec![0x01], vec![0x0b]],
        "editing keys must not be a single pasted control chunk"
    );
    assert!(
        !writes
            .lock()
            .unwrap()
            .iter()
            .any(|bytes| bytes.contains(&3)),
        "normal submission never sends Ctrl-C"
    );
}

#[tokio::test]
async fn claude_raw_input_rechecks_manual_activity_before_terminal_write() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let writes = Arc::new(Mutex::new(Vec::new()));
    let (output, _keepalive) = tokio::sync::broadcast::channel(16);
    let backend = Arc::new(SubmissionSignalTerminal {
        output: output.clone(),
        writer: Arc::new(SubmissionSignalWriter {
            completion: completion.clone(),
            writes: writes.clone(),
        }),
    });
    let terminal = ScreenModelBackend::wrap(backend);
    let input = ClaudeRawPtyInput {
        input: Arc::new(LivenessProbeInput { alive: true }),
        terminal: terminal.clone(),
        completion: completion.clone(),
    };
    // Native input arrived while the raw path was waiting for its terminal readiness projection.
    completion.observe_hooks(
        &[native_hook("UserPromptSubmit", "manual", 1)],
        Some(1),
        true,
    );
    output
        .send(
            "❯ queued during a tool loop\r\n------------------------\r\n"
                .as_bytes()
                .to_vec(),
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while !terminal.contents().contains("queued during") {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        input.send_turn("queued during a tool loop"),
    )
    .await
    .unwrap();
    assert!(
        result.is_err(),
        "newly observed manual work must reject native admission"
    );
    assert!(writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn claude_observed_turn_uses_exact_native_input_as_its_only_acceptance_boundary() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let input = ClaudeNativeHarness {
        input: Arc::new(StructuredAcceptedInput {
            completion: completion.clone(),
            emit_native_receipt: true,
        }),
        completion,
    };
    let observer = Arc::new(CountAcceptance::default());

    input
        .send_turn_observed("one canonical input", observer.clone())
        .await
        .expect("the matching native receipt should settle the observed turn");

    assert_eq!(observer.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn claude_observed_turn_rejects_terminal_without_exact_native_input_receipt() {
    let completion = Arc::new(ClaudeTurnCompletion::default());
    let input = ClaudeNativeHarness {
        input: Arc::new(StructuredAcceptedInput {
            completion: completion.clone(),
            emit_native_receipt: false,
        }),
        completion,
    };
    let observer = Arc::new(CountAcceptance::default());

    tokio::time::timeout(
        Duration::from_millis(25),
        input.send_turn_observed("missing native receipt", observer.clone()),
    )
    .await
    .expect_err("a terminal hook alone cannot prove which prompt Claude admitted");
    assert_eq!(observer.count.load(Ordering::SeqCst), 0);
}

struct BlockAcceptance {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

struct QueuedOperatorInput {
    completion: Arc<ClaudeTurnCompletion>,
    calls: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl HarnessInput for QueuedOperatorInput {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        self.calls.lock().unwrap().push(format!("input:{text}"));
        let submit = parse_hook_record(
            &serde_json::json!({
                "event": "UserPromptSubmit", "session_id": "native", "prompt_id": "B", "prompt": text
            }),
            2,
        );
        self.completion
            .observe_hooks(&[submit.clone()], Some(2), true);
        assert!(self.completion.accept_native_user_input(&submit).await);
        Ok(())
    }

    async fn interrupt_active_turn(&self) -> Result<(), String> {
        self.calls.lock().unwrap().push("interrupt".into());
        // Ctrl-C delivery does not synchronously generate a Stop hook.
        assert!(self.completion.has_open_turn());
        Ok(())
    }
}

#[tokio::test]
async fn claude_operator_input_submits_during_native_work_without_interrupt_or_stop() {
    check_claude_operator_input(false).await;
}

#[tokio::test]
async fn claude_operator_redirect_interrupts_then_submits_without_old_stop() {
    check_claude_operator_input(true).await;
}

async fn check_claude_operator_input(redirect: bool) {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    completion.observe_hooks(&[native_hook("UserPromptSubmit", "old", 1)], Some(1), true);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let transport = PtyTransport::default();
    let session = SessionId("isolated-claude-operator-input".into());
    transport.bind(
        session.clone(),
        Arc::new(ClaudeNativeHarness {
            input: Arc::new(QueuedOperatorInput {
                completion: completion.clone(),
                calls: calls.clone(),
            }),
            completion: completion.clone(),
        }),
    );
    assert!(transport.accepts_prompt_while_busy(&session));
    assert!(!transport.accepts_prompt_while_busy(&SessionId("foreign".into())));
    let event = nexus_contracts::WsEvent::AgentUpdate {
        session_id: session.clone(),
        kind: nexus_contracts::AgentUpdateKind::UserInput,
        data: serde_json::json!({"text":"new", "clientMessageId":"isolated-input"}),
    };
    let events = Arc::new(DiscardClaudeDisplay);
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        if redirect {
            transport
                .steer_observed(&session, "new".into(), events, event)
                .await
                .map(|_| ())
        } else {
            transport
                .prompt_observed(&session, "new".into(), events, event)
                .await
        }
    })
    .await
    .expect("operator acceptance must not wait for native Stop");
    result.expect("active Claude must accept operator input through its native queue");
    let expected = if redirect {
        vec!["interrupt", "input:new"]
    } else {
        vec!["input:new"]
    };
    assert_eq!(*calls.lock().unwrap(), expected);
    assert!(
        completion.has_open_turn(),
        "input receipt must not synthesize turn completion"
    );
}

#[tokio::test]
async fn claude_operator_plain_prompt_uses_native_acceptance_without_stop() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    completion.observe_hooks(&[native_hook("UserPromptSubmit", "old", 1)], Some(1), true);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let transport = PtyTransport::default();
    let session = SessionId("isolated-plain-operator".into());
    transport.bind(
        session.clone(),
        Arc::new(ClaudeNativeHarness {
            input: Arc::new(QueuedOperatorInput {
                completion: completion.clone(),
                calls: calls.clone(),
            }),
            completion: completion.clone(),
        }),
    );
    tokio::time::timeout(
        Duration::from_secs(1),
        transport.prompt(&session, "plain".into()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(*calls.lock().unwrap(), ["input:plain"]);
    assert!(completion.has_open_turn());
}

#[tokio::test]
async fn claude_operator_receipt_does_not_accept_stop_only() {
    struct StopOnlyInput(Arc<ClaudeTurnCompletion>);
    #[async_trait]
    impl HarnessInput for StopOnlyInput {
        async fn send_turn(&self, _: &str) -> Result<(), String> {
            self.0
                .observe_hooks(&[native_hook("Stop", "", 1)], Some(1), true);
            Ok(())
        }
    }
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let input = ClaudeNativeHarness {
        input: Arc::new(StopOnlyInput(completion.clone())),
        completion,
    };
    let observer = Arc::new(CountAcceptance::default());
    tokio::time::timeout(
        Duration::from_millis(25),
        input.submit_prompt_observed("unproven", observer.clone(), false),
    )
    .await
    .expect_err("Stop cannot substitute for an exact submit receipt");
    assert_eq!(observer.count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn claude_operator_overlap_preserves_exact_bus_terminal_authority() {
    for same_native_turn in [false, true] {
        let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
        let bus = completion.register_accepted_input("bus", Arc::new(CountAcceptance::default()));
        let old = native_hook("UserPromptSubmit", "bus", 1);
        completion.observe_hooks(&[old.clone()], Some(1), true);
        assert!(completion.accept_native_user_input(&old).await);
        let operator =
            completion.register_accepted_input("operator", Arc::new(CountAcceptance::default()));
        let id = if same_native_turn { "A" } else { "B" };
        let new = parse_hook_record(
            &serde_json::json!({"event":"UserPromptSubmit","session_id":"native","prompt_id":id,"prompt":"operator"}),
            2,
        );
        completion.observe_hooks(&[new.clone()], Some(2), true);
        assert!(completion.accept_native_user_input(&new).await);
        operator
            .wait_accepted(Duration::from_millis(10))
            .await
            .unwrap();
        assert!(completion.has_open_turn());
        let stop = parse_hook_record(
            &serde_json::json!({"event":"Stop","session_id":"native","prompt_id":id}),
            3,
        );
        completion.observe_hooks(&[stop], Some(3), true);
        operator.wait(Duration::from_millis(10)).await.unwrap();
        assert_eq!(
            bus.wait(Duration::from_millis(10)).await.is_ok(),
            same_native_turn,
            "NEW Stop cannot settle interrupted OLD bus turn"
        );
        assert!(!completion.has_open_turn());
        assert!(!completion.is_unknown());
    }
}

struct NativeWriteBarrier {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[tokio::test]
async fn claude_operator_cancel_after_write_releases_receipt_and_next_input_lane() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let writer = Arc::new(NativeWriteBarrier {
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let input = Arc::new(ClaudeNativeHarness {
        input: writer.clone(),
        completion: completion.clone(),
    });
    let abandoned_observer = Arc::new(CountAcceptance::default());
    let first_input = input.clone();
    let first_observer = abandoned_observer.clone();
    let first = tokio::spawn(async move {
        first_input
            .submit_prompt_observed("abandoned", first_observer, false)
            .await
    });
    writer.entered.acquire().await.unwrap().forget();
    writer.release.add_permits(1);
    drop(
        tokio::time::timeout(Duration::from_secs(1), completion.lock_input())
            .await
            .unwrap(),
    );
    assert!(
        !first.is_finished(),
        "write returned, exact receipt still pending"
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let late = native_hook("UserPromptSubmit", "abandoned", 1);
    completion.observe_hooks(&[late.clone()], Some(1), true);
    assert!(
        !completion.accept_native_user_input(&late).await,
        "cancelled caller cannot acquire a late accepted event"
    );
    assert_eq!(abandoned_observer.count.load(Ordering::SeqCst), 0);
    let next_observer = Arc::new(CountAcceptance::default());
    let next_input = input.clone();
    let observed = next_observer.clone();
    let next = tokio::spawn(async move {
        next_input
            .submit_prompt_observed("next", observed, false)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), writer.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    writer.release.add_permits(1);
    let receipt = native_hook("UserPromptSubmit", "next", 2);
    completion.observe_hooks(&[receipt.clone()], Some(2), true);
    assert!(completion.accept_native_user_input(&receipt).await);
    tokio::time::timeout(Duration::from_secs(1), next)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(next_observer.count.load(Ordering::SeqCst), 1);
    assert!(
        completion.has_open_turn(),
        "cancelled API future does not erase native work"
    );
}

#[tokio::test]
async fn claude_operator_waiting_lane_rechecks_replacement_before_interrupt_or_write() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let writer = Arc::new(NativeWriteBarrier {
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let input = Arc::new(ClaudeNativeHarness {
        input: writer.clone(),
        completion: completion.clone(),
    });
    let first_input = input.clone();
    let first = tokio::spawn(async move {
        first_input
            .submit_prompt_observed("first", Arc::new(CountAcceptance::default()), false)
            .await
    });
    writer.entered.acquire().await.unwrap().forget();
    let mut second = Box::pin(input.submit_prompt_observed(
        "second",
        Arc::new(CountAcceptance::default()),
        true,
    ));
    assert!(
        std::future::Future::poll(
            second.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending(),
        "redirect must wait for captured input lane"
    );
    completion.invalidate();
    writer.release.add_permits(1);
    assert!(first.await.unwrap().unwrap_err().contains("replaced"));
    assert!(second.await.unwrap_err().contains("replaced"));
    assert_eq!(
        writer.entered.available_permits(),
        0,
        "no second native write"
    );
}

#[async_trait]
impl HarnessInput for NativeWriteBarrier {
    async fn send_turn(&self, _: &str) -> Result<(), String> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Ok(())
    }
}

#[async_trait]
impl TurnAcceptanceObserver for BlockAcceptance {
    async fn accepted(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
}

#[tokio::test]
async fn claude_same_pass_terminal_waits_for_its_blocked_accepted_callback() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let writer = Arc::new(NativeWriteBarrier {
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let input = Arc::new(ClaudeNativeHarness {
        input: writer.clone(),
        completion: completion.clone(),
    });
    let observer = Arc::new(BlockAcceptance {
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let mut call = {
        let observer = observer.clone();
        tokio::spawn(async move { input.send_turn_observed("text", observer).await })
    };
    writer.entered.acquire().await.unwrap().forget();
    let submit = native_hook("UserPromptSubmit", "text", 1);
    completion.observe_hooks(
        &[submit.clone(), native_hook("Stop", "text", 2)],
        Some(2),
        true,
    );
    let accept = {
        let completion = completion.clone();
        tokio::spawn(async move { completion.accept_native_user_input(&submit).await })
    };
    observer.entered.acquire().await.unwrap().forget();
    writer.release.add_permits(1);
    let prematurely_finished = tokio::time::timeout(Duration::from_millis(25), &mut call)
        .await
        .is_ok();
    observer.release.add_permits(1);
    accept.await.unwrap();
    assert!(
        !prematurely_finished,
        "receipt callback completion is part of successful wrapper settlement"
    );
    if !prematurely_finished {
        call.await.unwrap().unwrap();
    }
}

#[test]
fn claude_cold_resume_waits_for_a_new_session_start_record() {
    let supervisor = PtySupervisor::new();
    let session = SessionId("s_claude_cold_ready".into());
    let dir = tempfile::tempdir().unwrap();
    let hook_log = dir.path().join("hooks.jsonl");
    std::fs::write(&hook_log, "{\"event\":\"SessionStart\",\"payload\":{}}\n").unwrap();
    let offset = std::fs::metadata(&hook_log).unwrap().len();
    supervisor
        .claude_startup_markers
        .lock()
        .unwrap()
        .insert(session.clone(), (hook_log.clone(), offset));
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(hook_log)
            .unwrap();
        writeln!(file, "{{\"event\":\"SessionStart\",\"payload\":{{}}}}").unwrap();
    });

    supervisor.wait_for_claude_startup(&session).unwrap();
}

#[test]
fn claude_teardown_invalidates_old_owner_and_replacement_is_fresh() {
    let supervisor = PtySupervisor::new();
    let session = SessionId("owner-replacement".into());
    let old = supervisor.claude_turn_completion(&session);
    supervisor.kill(&session);
    let new = supervisor.claude_turn_completion(&session);
    assert!(
        !old.is_current(),
        "teardown must invalidate outstanding captured owners"
    );
    assert_ne!(old.owner_id(), new.owner_id());
}

#[test]
fn claude_same_session_binding_capture_replaces_owner_without_waiting_for_teardown() {
    let supervisor = PtySupervisor::new();
    let session = SessionId(format!("fixture-{}", uuid::Uuid::new_v4()));
    let old = supervisor
        .capture_claude_binding(
            &session,
            HeadedRuntimeKind::ClaudeNative,
            &["--resume".into(), "old-native".into()],
        )
        .unwrap();
    let new = supervisor
        .capture_claude_binding(
            &session,
            HeadedRuntimeKind::ClaudeNative,
            &["--resume".into(), "new-native".into()],
        )
        .unwrap();
    assert!(!old.is_current());
    assert!(new.is_current());
    assert_ne!(old.owner_id(), new.owner_id());
    assert_eq!(
        supervisor.claude_turn_completion(&session).owner_id(),
        new.owner_id()
    );
    // A late old attachment must not obtain replacement authority.
    old.attach_resume_identity(Some("wrong".into()));
    assert!(!old.is_current());
    assert_eq!(
        supervisor.claude_turn_completion(&session).owner_id(),
        new.owner_id()
    );
    assert!(supervisor
        .bind_claude_input(&session, old, Arc::new(LivenessProbeInput { alive: true }))
        .is_err());
    assert!(new.is_current());
    assert!(supervisor
        .with_claude_owner(&session, &new, || ())
        .is_some());
}

#[tokio::test]
async fn manual_claude_submit_blocks_transport_until_matching_stop_not_tool_output() {
    let transport = PtyTransport::default();
    let session = SessionId("manual-open".into());
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    transport.bind(
        session.clone(),
        Arc::new(ClaudeNativeHarness {
            input: Arc::new(LivenessProbeInput { alive: true }),
            completion: completion.clone(),
        }),
    );
    let initial = transport.observe_turn(&session);
    assert_eq!(initial.state, nexus_contracts::TurnState::Unknown);
    assert_eq!(initial, transport.observe_turn(&session));
    assert!(transport.active_turn_sessions().is_empty());
    tokio::time::timeout(
        Duration::from_secs(1),
        transport.wait_for_turn_completion(&session),
    )
    .await
    .unwrap()
    .unwrap();
    completion.observe_hooks(
        &[native_hook("UserPromptSubmit", "manual", 1)],
        Some(1),
        true,
    );
    assert_eq!(transport.active_turn_sessions(), vec![session.clone()]);
    let open = transport.observe_turn(&session);
    assert_eq!(open.state, nexus_contracts::TurnState::NativeOpen);
    assert_eq!(open.steer_capability, transport.steer_capability(&session));
    assert_ne!(initial.stamp, open.stamp);
    completion.observe_hooks(&[native_hook("PostToolUse", "manual", 2)], Some(2), true);
    assert_eq!(transport.active_turn_sessions(), vec![session.clone()]);
    assert!(tokio::time::timeout(
        Duration::from_millis(25),
        transport.wait_for_turn_completion(&session)
    )
    .await
    .is_err());
    completion.observe_hooks(&[native_hook("Stop", "manual", 3)], Some(3), true);
    transport.wait_for_turn_completion(&session).await.unwrap();
    assert!(transport.active_turn_sessions().is_empty());
    let idle = transport.observe_turn(&session);
    assert_eq!(idle.state, nexus_contracts::TurnState::VerifiedIdle);
    assert_eq!(idle, transport.observe_turn(&session));
    completion.invalidate();
    assert_eq!(
        transport.observe_turn(&session).state,
        nexus_contracts::TurnState::Unavailable
    );
}

#[tokio::test]
async fn claude_receipt_requires_exact_offset_native_session_and_live_owner() {
    let dir = temp_test_dir("receipt-provenance");
    let log = dir.join("hooks.jsonl");
    let completion = Arc::new(ClaudeTurnCompletion::new(
        Some("native".into()),
        Some(log.clone()),
    ));
    // The historical identical text is on disk before registration, but not yet ingested.
    std::fs::write(&log, vec![b' '; 100]).unwrap();
    let observer = Arc::new(CountAcceptance::default());
    let accepted = completion.register_accepted_input("same", observer.clone());
    let old = native_hook("UserPromptSubmit", "same", 50);
    let mut foreign = native_hook("UserPromptSubmit", "same", 110);
    foreign.session_id = Some("foreign".into());
    let current = native_hook("UserPromptSubmit", "same", 120);
    completion.observe_hooks(
        &[old.clone(), foreign.clone(), current.clone()],
        Some(120),
        true,
    );
    assert!(
        !completion.accept_native_user_input(&old).await,
        "historical text cannot consume a registration authorized by the later record"
    );
    assert!(!completion.accept_native_user_input(&foreign).await);
    assert!(!accepted.was_accepted());
    assert!(completion.accept_native_user_input(&current).await);
    assert!(accepted.was_accepted());
    assert_eq!(observer.count.load(Ordering::SeqCst), 1);
    let retired = completion.register_accepted_input("late", observer.clone());
    completion.invalidate();
    let late = native_hook("UserPromptSubmit", "late", 130);
    completion.observe_hooks(&[late.clone()], Some(130), true);
    assert!(!completion.accept_native_user_input(&late).await);
    assert!(!retired.was_accepted());
}

#[test]
fn claude_replay_truncation_missing_and_invalid_evidence_cannot_clear_open_work() {
    let completion = ClaudeTurnCompletion::new(Some("native".into()), None);
    completion.observe_hooks(
        &[native_hook("UserPromptSubmit", "manual", 100)],
        Some(100),
        true,
    );
    completion.observe_hooks(
        &[
            native_hook("UserPromptSubmit", "old", 1),
            native_hook("Stop", "old", 2),
        ],
        Some(100),
        true,
    );
    assert!(
        completion.has_open_turn(),
        "replayed old Submit/Stop cannot replace newer work"
    );
    let terminal = native_hook("Stop", "manual", 90);
    completion.observe_hooks(&[terminal.clone()], Some(100), true);
    assert!(completion.has_open_turn());
    completion.observe_hooks(&[terminal], Some(90), true);
    assert!(completion.has_open_turn());
    assert!(completion.is_unknown());
    completion.observe_hooks(&[], None, false);
    assert!(completion.has_open_turn());
    let mut invalid = native_hook("Stop", "manual", 110);
    invalid.valid = false;
    completion.observe_hooks(&[invalid], Some(110), true);
    assert!(completion.has_open_turn());
    let mut unidentified = native_hook("Stop", "manual", 120);
    unidentified.prompt_id = None;
    completion.observe_hooks(&[unidentified], Some(120), true);
    assert!(
        completion.has_open_turn(),
        "absent prompt ids cannot invent a matching terminal"
    );
}

#[test]
fn claude_valid_submit_before_partial_tail_still_establishes_open_work() {
    let completion = ClaudeTurnCompletion::new(Some("native".into()), None);
    completion.observe_hooks(
        &[native_hook("UserPromptSubmit", "manual", 100)],
        Some(110),
        false,
    );
    assert!(
        completion.has_open_turn(),
        "partial trailing JSON cannot discard a fully parsed submit prefix"
    );
    assert!(completion.is_unknown());
}

#[test]
fn claude_complete_stop_before_partial_tail_retains_terminal_authority() {
    let completion = ClaudeTurnCompletion::new(Some("native".into()), None);
    completion.observe_hooks(
        &[native_hook("UserPromptSubmit", "manual", 100)],
        Some(100),
        true,
    );
    completion.observe_hooks(&[native_hook("Stop", "manual", 200)], Some(210), false);
    assert!(
        !completion.has_open_turn(),
        "complete Stop is authoritative independently of a later partial JSON suffix"
    );
    assert_eq!(completion.snapshot(), 1);
    assert!(
        completion.is_unknown(),
        "the unfinished suffix still limits subsequent freshness"
    );
}

#[test]
fn claude_fresh_launch_cannot_inherit_stored_native_session_identity() {
    let completion = ClaudeTurnCompletion::new(None, None);
    completion.attach_resume_identity(Some("old-native".into()));
    let start = parse_hook_record(
        &serde_json::json!({"event": "SessionStart", "session_id": "native"}),
        1,
    );
    completion.observe_hooks(
        &[start, native_hook("UserPromptSubmit", "manual", 2)],
        Some(2),
        true,
    );
    assert!(
        completion.has_open_turn(),
        "fresh SessionStart must establish NEW, not stale persisted resume metadata"
    );
}

#[tokio::test]
async fn claude_initial_registration_learns_identity_only_from_new_valid_session_start() {
    let completion = Arc::new(ClaudeTurnCompletion::new(None, None));
    let observer = Arc::new(CountAcceptance::default());
    let registration = completion.register_accepted_input("initial", observer.clone());
    let start = parse_hook_record(
        &serde_json::json!({"event": "SessionStart", "session_id": "native"}),
        1,
    );
    let submit = native_hook("UserPromptSubmit", "initial", 2);
    completion.observe_hooks(
        &[start, submit.clone(), native_hook("Stop", "initial", 3)],
        Some(3),
        true,
    );
    assert!(completion.accept_native_user_input(&submit).await);
    registration.wait(Duration::from_millis(25)).await.unwrap();
    assert_eq!(observer.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn claude_initial_registration_accepts_fresh_start_written_before_registration() {
    let dir = temp_test_dir("start-before-registration");
    let path = dir.join("hooks");
    let completion = Arc::new(ClaudeTurnCompletion::new(None, Some(path.clone())));
    let start = serde_json::json!({"event": "SessionStart", "session_id": "native"});
    let text = start.to_string();
    std::fs::write(&path, &text).unwrap();
    let offset = text.len() as u64;
    let observer = Arc::new(CountAcceptance::default());
    let registration = completion.register_accepted_input("initial", observer.clone());
    let submit = native_hook("UserPromptSubmit", "initial", offset + 1);
    completion.observe_hooks(
        &[
            parse_hook_record(&start, offset),
            submit.clone(),
            native_hook("Stop", "initial", offset + 2),
        ],
        Some(offset + 2),
        true,
    );
    assert!(
        completion.accept_native_user_input(&submit).await,
        "the binding floor, not the prompt-write floor, validates SessionStart lineage"
    );
    registration.wait(Duration::from_millis(25)).await.unwrap();
}

#[test]
fn claude_optional_ids_allow_unambiguous_single_turn_but_not_overlap_or_conflict() {
    for ids in [(None, None), (Some("A"), None), (None, Some("A"))] {
        let completion = ClaudeTurnCompletion::new(Some("native".into()), None);
        let mut submit = native_hook("UserPromptSubmit", "manual", 1);
        submit.prompt_id = ids.0.map(str::to_string);
        let mut stop = native_hook("Stop", "manual", 2);
        stop.prompt_id = ids.1.map(str::to_string);
        completion.observe_hooks(&[submit, stop], Some(2), true);
        assert_eq!(
            completion.observe_turn().state,
            nexus_contracts::TurnState::VerifiedIdle
        );
        assert!(
            !completion.has_open_turn(),
            "single native turn with optional ids {ids:?} must retain supported Stop semantics"
        );
    }
    for conflicting in [false, true] {
        let completion = ClaudeTurnCompletion::new(Some("native".into()), None);
        let mut a = native_hook("UserPromptSubmit", "A", 1);
        a.prompt_id = None;
        let mut b = native_hook("UserPromptSubmit", "B", 2);
        b.prompt_id = None;
        if conflicting {
            b.valid = false;
        }
        let mut stop = native_hook("Stop", "A", 3);
        stop.prompt_id = None;
        completion.observe_hooks(&[a, b, stop], Some(3), true);
        assert!(
            completion.has_open_turn(),
            "overlap/conflict cannot be guessed away without matching ids"
        );
        assert!(completion.is_unknown());
        assert_eq!(
            completion.observe_turn().state,
            nexus_contracts::TurnState::Unknown
        );
    }
}

#[tokio::test]
async fn claude_old_waiter_retains_terminal_when_next_manual_turn_opens() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let observer = Arc::new(CountAcceptance::default());
    let accepted = completion.register_accepted_input("A", observer);
    let submit = native_hook("UserPromptSubmit", "A", 1);
    let mut next = native_hook("UserPromptSubmit", "B", 3);
    next.prompt_id = Some("B".into());
    completion.observe_hooks(
        &[submit.clone(), native_hook("Stop", "A", 2), next],
        Some(3),
        true,
    );
    assert!(completion.has_open_turn());
    completion.accept_native_user_input(&submit).await;
    accepted.wait(Duration::from_millis(25)).await.unwrap();
    assert!(
        completion.has_open_turn(),
        "settling A cannot make manual B idle"
    );
}

struct WrittenHarness {
    input: Arc<dyn HarnessInput>,
    written: tokio::sync::Semaphore,
}

#[tokio::test]
async fn manual_no_id_hooks_close_transport_through_real_forwarder_pass() {
    use nexus_harness_claude::native::forwarder::forward_once_with_observations;
    use std::io::Write;
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId(format!("fixture-{}", uuid::Uuid::new_v4()));
    let dir = temp_test_dir("manual-no-id");
    let paths = ClaudeNativeBridgePaths::new(&dir, &session);
    std::fs::create_dir_all(&paths.bridge_dir).unwrap();
    let completion = Arc::new(ClaudeTurnCompletion::new(
        Some("native".into()),
        Some(paths.hook_log_path.clone()),
    ));
    ClaudeRuntimeStateRepo::new(&store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: session.clone(),
            bridge_dir: paths.bridge_dir.clone(),
            claude_session_id: Some("native".into()),
            launch_cwd: dir,
            transcript_path: Some(paths.bridge_dir.join("transcript.jsonl")),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    let transport = PtyTransport::default();
    transport.bind(
        session.clone(),
        Arc::new(ClaudeNativeHarness {
            input: Arc::new(LivenessProbeInput { alive: true }),
            completion: completion.clone(),
        }),
    );
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.hook_log_path)
        .unwrap();
    for (payload, open) in [
        (
            serde_json::json!({"hook_event_name": "UserPromptSubmit", "session_id": "native", "prompt": "manual"}),
            true,
        ),
        (
            serde_json::json!({"hook_event_name": "PostToolUse", "session_id": "native", "tool_name": "Read", "tool_use_id": "tool"}),
            true,
        ),
        (
            serde_json::json!({"hook_event_name": "Stop", "session_id": "native", "last_assistant_message": "done"}),
            false,
        ),
    ] {
        writeln!(
            file,
            "{}",
            serde_json::json!({"event": payload["hook_event_name"], "payload": payload})
        )
        .unwrap();
        forward_once_with_observations(
            store.clone(),
            session.clone(),
            paths.clone(),
            Arc::new(DiscardClaudeDisplay),
            None,
            Some(completion.clone()),
        )
        .await
        .unwrap();
        assert_eq!(
            transport.active_turn_sessions().contains(&session),
            open,
            "native optional-id payload must retain singleton lifecycle semantics"
        );
    }
    transport.wait_for_turn_completion(&session).await.unwrap();
}

#[async_trait]
impl HarnessInput for WrittenHarness {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        self.input.send_turn(text).await?;
        self.written.add_permits(1);
        Ok(())
    }
}

struct DiscardClaudeDisplay;

// Initialized-coordinator AppState fixture, not production boot/adoption coverage. No background
// loops, provider/native processes, endpoint handles, or operator files are created.
struct ShutdownModelFixture {
    state: crate::daemon::AppState,
    reporting: Arc<crate::daemon::model_reporting::ModelReporting>,
    identity_gate: Arc<tokio::sync::Mutex<()>>,
    _dir: tempfile::TempDir,
}

impl ShutdownModelFixture {
    async fn new(events: Arc<dyn EventSink>) -> Self {
        use crate::daemon::app::WsSink;
        use nexus_store::repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime};
        let dir = tempfile::tempdir().unwrap();
        let daemon =
            nexus_store::DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
                .await
                .unwrap();
        let identity_gate = daemon.identity().write_lock();
        let store = Arc::new(daemon.compatibility_store());
        let reporting = Arc::new(crate::daemon::model_reporting::ModelReporting::new(
            store.clone(),
            events.clone(),
        ));
        reporting.initialize().await.unwrap();
        Agents::new(&store)
            .create(NewAgent {
                agent_id: "a_shutdown".into(),
                project: "default".into(),
                name: None,
                default_harness: None,
                role: None,
                tier: None,
                owner: None,
            })
            .await
            .unwrap();
        AgentRuntimes::new(&store)
            .create(NewAgentRuntime {
                runtime_id: "s_shutdown".into(),
                agent_id: "a_shutdown".into(),
                harness: "other".into(),
                cwd: None,
                transport: None,
                presence: Some("online".into()),
                active: true,
            })
            .await
            .unwrap();
        let config = nexus_common::Config::default();
        let identity = Arc::new(nexus_identity::Identity::new(
            store.clone(),
            events.clone(),
            &config,
        ));
        let agent = Arc::new(PtyTransport::default());
        let realtime = Arc::new(nexus_dispatch::DispatchService::new(
            nexus_dispatch::ServiceDeps {
                store: store.clone(),
                bell: Bell::new(),
                registry: nexus_dispatch::AgentRegistry::new(),
                project: "default".into(),
                drain_limit: 100,
                preview_chars: 100,
            },
        ));
        let bus = Arc::new(nexus_bus::Bus::new(
            store.clone(),
            realtime.clone(),
            identity.clone(),
            events.clone(),
        ));
        let search = Arc::new(nexus_search::Search::new(store.clone(), identity.clone()));
        let notify = Arc::new(nexus_notify::Notify::new(
            store.clone(),
            bus.clone(),
            events.clone(),
            nexus_notify::RoutingRules::new(vec![]),
        ));
        let admin = Arc::new(nexus_admin::Admin::new(
            identity.clone(),
            agent.clone(),
            notify.clone(),
            events,
        ));
        let state = crate::daemon::AppState::new_with_model_reporting(
            store.clone(),
            WsSink::new(16, Some(store)),
            identity,
            agent,
            realtime,
            bus,
            search,
            notify,
            admin,
            "default".into(),
            reporting.clone(),
        );
        Self {
            _dir: dir,
            state,
            reporting,
            identity_gate,
        }
    }

    fn reserve(
        &self,
    ) -> Result<crate::daemon::model_reporting::ModelObserverHandle, nexus_common::NexusError> {
        use crate::daemon::model_reporting::ModelCapabilityProfile;
        use nexus_contracts::model_report::{ModelEvidenceCapability, ModelReportBackend};
        self.reporting.reserve(
            "a_shutdown".into(),
            SessionId("s_shutdown".into()),
            ModelReportBackend::new("fixture/shutdown").unwrap(),
            ModelCapabilityProfile {
                configured: ModelEvidenceCapability::Supported,
                turn_selected: ModelEvidenceCapability::Unsupported,
                response_reported: ModelEvidenceCapability::Unsupported,
            },
        )
    }

    async fn wait_for_store_waiter(&self, baseline: usize) {
        // The only spawned task is the real model worker. begin_write_txn clones this Arc for
        // lock_owned before awaiting its held mutex; the extra owner proves entry into that gate.
        tokio::time::timeout(Duration::from_secs(2), async {
            while Arc::strong_count(&self.identity_gate) <= baseline {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("model worker never reached the held identity write gate");
    }

    async fn assert_drained_row(&self, revision: i64) {
        let row = nexus_store::repos::AgentRuntimes::new(&self.state.store)
            .find_by_runtime_id("s_shutdown")
            .await
            .unwrap()
            .unwrap();
        assert!(row.model_observer_token.is_none());
        assert_eq!(row.model_report_revision, revision);
        assert!(!row.model_report.unwrap().observer_active);
    }
}

async fn assert_shutdown_waiter_is_bounded_and_cancellable(fixture: &ShutdownModelFixture) {
    let mut abandoned = Box::pin(
        fixture
            .state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2)),
    );
    assert!(
        futures::poll!(&mut abandoned).is_pending(),
        "actual owned work must keep AppState drain pending"
    );
    assert!(
        fixture.reserve().is_err(),
        "first poll must close this SAME coordinator's admission"
    );
    drop(abandoned);
    let error = fixture
        .state
        .drain_model_reporting_for_shutdown(Duration::ZERO)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("shutdown incomplete"), "{error}");
}

#[tokio::test]
async fn app_model_shutdown_retains_parked_claim_after_waiter_cancellation_and_timeout() {
    use nexus_contracts::model_report::ModelObservationSink;
    let fixture = ShutdownModelFixture::new(Arc::new(DiscardClaudeDisplay)).await;
    let observer = fixture.reserve().unwrap();
    let gate = fixture.identity_gate.clone().lock_owned().await;
    let baseline = Arc::strong_count(&fixture.identity_gate);
    let mut claim = Box::pin(fixture.reporting.commit_claim(&observer));
    assert!(futures::poll!(&mut claim).is_pending());
    fixture.wait_for_store_waiter(baseline).await;
    assert_shutdown_waiter_is_bounded_and_cancellable(&fixture).await;
    assert!(!observer.bind_native_root("late"));
    drop(gate);
    assert!(!claim.await.unwrap());
    fixture
        .state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    fixture.assert_drained_row(2).await;
}

#[tokio::test]
async fn app_model_shutdown_retains_parked_apply_after_waiter_cancellation_and_timeout() {
    use nexus_contracts::model_report::ModelObservationSink;
    let fixture = ShutdownModelFixture::new(Arc::new(DiscardClaudeDisplay)).await;
    let observer = fixture.reserve().unwrap();
    assert!(observer.bind_native_root("native"));
    assert!(fixture.reporting.commit_claim(&observer).await.unwrap());
    let gate = fixture.identity_gate.clone().lock_owned().await;
    let baseline = Arc::strong_count(&fixture.identity_gate);
    assert!(fixture.reporting.activate(&observer, "native"));
    fixture.wait_for_store_waiter(baseline).await;
    assert_shutdown_waiter_is_bounded_and_cancellable(&fixture).await;
    assert!(!observer.bind_native_root("native"));
    drop(gate);
    fixture
        .state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    fixture.assert_drained_row(3).await;
}

#[derive(Default)]
struct ShutdownPublicationGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: AtomicUsize,
}

#[async_trait]
impl EventSink for ShutdownPublicationGate {
    async fn emit(&self, _: nexus_contracts::WsEvent) {
        panic!("unexpected fixture event");
    }

    async fn project_runtime_binding(&self, _: &SessionId, _: &nexus_contracts::AgentId) {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

#[tokio::test]
async fn app_model_shutdown_retains_parked_publication_after_waiter_cancellation_and_timeout() {
    let events = Arc::new(ShutdownPublicationGate::default());
    let fixture = ShutdownModelFixture::new(events.clone()).await;
    let observer = fixture.reserve().unwrap();
    let mut claim = Box::pin(fixture.reporting.commit_claim(&observer));
    assert!(futures::poll!(&mut claim).is_pending());
    tokio::time::timeout(Duration::from_secs(2), events.entered.notified())
        .await
        .unwrap();
    assert_shutdown_waiter_is_bounded_and_cancellable(&fixture).await;
    events.release.notify_one();
    assert!(!claim.await.unwrap());
    fixture
        .state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    fixture.assert_drained_row(2).await;
    assert_eq!(events.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn app_model_shutdown_retains_settlement_failure_for_later_waiters() {
    let fixture = ShutdownModelFixture::new(Arc::new(DiscardClaudeDisplay)).await;
    let observer = fixture.reserve().unwrap();
    assert!(fixture.reporting.commit_claim(&observer).await.unwrap());
    fixture.state.store.identity_conn().execute_batch(
        "CREATE TRIGGER reject_shutdown_revoke BEFORE UPDATE OF model_observer_token ON agent_runtimes WHEN NEW.model_observer_token IS NULL BEGIN SELECT RAISE(FAIL, 'shutdown-revoke-marker'); END;"
    ).await.unwrap();
    for _ in 0..2 {
        let error = fixture
            .state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("shutdown-revoke-marker"),
            "{error}"
        );
    }
    assert!(fixture.reserve().is_err());
    let row = nexus_store::repos::AgentRuntimes::new(&fixture.state.store)
        .find_by_runtime_id("s_shutdown")
        .await
        .unwrap()
        .unwrap();
    assert!(
        row.model_observer_token.is_some(),
        "failed revoke is not rollback/success proof"
    );
}

#[tokio::test]
async fn app_model_shutdown_admission_stays_open_through_empty_or_failed_transport_sweep() {
    for list_fails in [false, true] {
        let fixture = ShutdownModelFixture::new(Arc::new(DiscardClaudeDisplay)).await;
        fixture.state.begin_command_worker_shutdown().await;
        if list_fails {
            fixture
                .state
                .store
                .conn
                .execute_batch("DROP TABLE sessions")
                .await
                .unwrap();
            assert!(nexus_store::repos::Sessions::new(&fixture.state.store)
                .list_all()
                .await
                .is_err());
        }
        assert_eq!(
            fixture.state.teardown_owned_transports_for_shutdown().await,
            0
        );
        let observer = fixture
            .reserve()
            .expect("delivery fence/sweep must not close model admission");
        assert!(fixture.reporting.commit_claim(&observer).await.unwrap());
        fixture
            .state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
        assert!(fixture.reserve().is_err());
        fixture.assert_drained_row(2).await;
    }
}

// Real coordinator/presence/wiring, but no provider, native process, or operator-home access.
async fn managed_attachment_fixture() -> (
    tempfile::TempDir,
    crate::daemon::app::LoopWiring,
    Arc<crate::daemon::model_reporting::ModelReporting>,
) {
    use crate::daemon::services::presence::{PresenceWriter, TransportRegistry};
    use nexus_store::repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime};
    let dir = tempfile::tempdir().unwrap();
    let daemon = nexus_store::DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_attachment".into(),
            project: "default".into(),
            name: None,
            default_harness: None,
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: "s_attachment".into(),
            agent_id: "a_attachment".into(),
            harness: "other".into(),
            cwd: None,
            transport: None,
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
    let events: Arc<dyn EventSink> = Arc::new(DiscardClaudeDisplay);
    let reporting = Arc::new(crate::daemon::model_reporting::ModelReporting::new(
        store.clone(),
        events.clone(),
    ));
    reporting.initialize().await.unwrap();
    let presence = PresenceWriter::new(store.clone(), events.clone(), TransportRegistry::new())
        .with_model_reporting(reporting.clone());
    let wiring = crate::daemon::app::LoopWiring {
        store,
        bell: Bell::new(),
        registry: nexus_dispatch::AgentRegistry::new(),
        events,
        turn_exec: Arc::new(PtyTransport::default()),
        gateway_stream: None,
        drain_limit: 100,
        preview_chars: 100,
        spawned: Default::default(),
        shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        native_forwarders: Default::default(),
        raw_stream_writers: Default::default(),
        presence,
    };
    (dir, wiring, reporting)
}

#[derive(Debug, Clone, Copy)]
enum ManagedAttachment {
    EventLoop,
    RawStream,
    Native,
    PresenceWriter,
}

async fn attach_managed(
    wiring: &crate::daemon::app::LoopWiring,
    session: &SessionId,
    kind: ManagedAttachment,
) {
    match kind {
        ManagedAttachment::EventLoop => wiring.spawn_loop(session, "default").await,
        ManagedAttachment::RawStream => {
            let (_sender, output) = broadcast::channel(1);
            wiring.spawn_raw_stream_writer(session, output).await;
        }
        ManagedAttachment::Native => assert!(wiring.claim_native_forwarder("inert", session).await),
        ManagedAttachment::PresenceWriter => wiring
            .presence
            .mark_transport_present(
                session,
                crate::daemon::services::presence::TransportHandle::EventLoop,
            )
            .await
            .unwrap(),
    }
}

fn assert_no_managed_attachment(wiring: &crate::daemon::app::LoopWiring, session: &SessionId) {
    assert!(
        !wiring.presence.registry().is_present(session),
        "transport published before presence admission"
    );
    assert!(
        wiring.spawned.lock().unwrap().is_empty(),
        "event loop map published before presence admission"
    );
    assert!(
        wiring.raw_stream_writers.lock().unwrap().is_empty(),
        "raw writer map published before presence admission"
    );
    assert!(
        wiring.native_forwarders.lock().unwrap().is_empty(),
        "native map published before presence admission"
    );
}

async fn offline_blocks_managed_attachment(kind: ManagedAttachment) {
    use crate::daemon::model_reporting::ModelCapabilityProfile;
    use nexus_contracts::model_report::{
        ModelEvidenceCapability, ModelObservationSink, ModelReportBackend,
    };
    use nexus_store::repos::AgentRuntimes;
    let (_dir, wiring, reporting) = managed_attachment_fixture().await;
    let session = SessionId("s_attachment".into());
    let old = reporting
        .reserve(
            "a_attachment".into(),
            session.clone(),
            ModelReportBackend::new("fixture/opaque").unwrap(),
            ModelCapabilityProfile {
                configured: ModelEvidenceCapability::Supported,
                turn_selected: ModelEvidenceCapability::Unsupported,
                response_reported: ModelEvidenceCapability::Supported,
            },
        )
        .unwrap();
    assert!(reporting.commit_claim(&old).await.unwrap());
    assert!(old.bind_native_root("old-native"));
    assert!(reporting.activate(&old, "old-native"));
    let before = AgentRuntimes::new(&wiring.store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert!(before.model_observer_token.is_some());
    let identity = wiring
        .store
        .begin_identity_write_txn("park_attachment_offline_stop")
        .await
        .unwrap();
    let mut offline = Box::pin(wiring.presence.materialize_offline(&session));
    assert!(futures::poll!(&mut offline).is_pending());
    assert!(
        !old.bind_native_root("old-native"),
        "offline admission closes the actual local observer"
    );
    let mut attachment = Box::pin(attach_managed(&wiring, &session, kind));
    let first_poll = futures::poll!(&mut attachment);
    assert_no_managed_attachment(&wiring, &session);
    assert!(
        first_poll.is_pending(),
        "{kind:?} must await the tracked offline transition"
    );
    drop(offline);
    assert!(
        futures::poll!(&mut attachment).is_pending(),
        "cancelled offline caller must not release presence"
    );
    assert_no_managed_attachment(&wiring, &session);
    let parked = AgentRuntimes::new(&wiring.store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert!(parked.active);
    assert_eq!(parked.model_observer_token, before.model_observer_token);
    drop(identity);
    tokio::time::timeout(Duration::from_secs(2), attachment)
        .await
        .unwrap();
    assert!(wiring.presence.registry().is_present(&session));
    let after = AgentRuntimes::new(&wiring.store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.active,
        matches!(kind, ManagedAttachment::PresenceWriter)
    );
    assert!(after.model_observer_token.is_none());
    if matches!(kind, ManagedAttachment::PresenceWriter) {
        assert_eq!(after.presence.as_deref(), Some("online"));
        assert!(after.stopped_at.is_none());
    } else {
        assert_eq!(after.presence.as_deref(), Some("offline"));
        assert!(after.stopped_at.is_some());
    }
    assert!(!old.bind_native_root("old-native"));
    wiring.teardown_session_transports(&session);
    wiring.release_native_forwarder("inert", &session);
    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test]
async fn managed_attachment_event_loop_waits_for_cancelled_offline_settlement() {
    offline_blocks_managed_attachment(ManagedAttachment::EventLoop).await;
}

#[tokio::test]
async fn managed_attachment_raw_writer_waits_for_cancelled_offline_settlement() {
    offline_blocks_managed_attachment(ManagedAttachment::RawStream).await;
}

#[tokio::test]
async fn managed_attachment_native_waits_for_cancelled_offline_settlement() {
    offline_blocks_managed_attachment(ManagedAttachment::Native).await;
}

#[tokio::test]
async fn managed_attachment_presence_writer_waits_for_cancelled_offline_settlement() {
    offline_blocks_managed_attachment(ManagedAttachment::PresenceWriter).await;
}

#[tokio::test]
async fn managed_attachment_cancelled_before_guard_publishes_nothing() {
    for kind in [
        ManagedAttachment::EventLoop,
        ManagedAttachment::RawStream,
        ManagedAttachment::Native,
        ManagedAttachment::PresenceWriter,
    ] {
        let (_dir, wiring, reporting) = managed_attachment_fixture().await;
        let session = SessionId("s_attachment".into());
        let transition = wiring.store.lock_presence_transition().await;
        let mut attachment = Box::pin(attach_managed(&wiring, &session, kind));
        let first_poll = futures::poll!(&mut attachment);
        assert_no_managed_attachment(&wiring, &session);
        assert!(first_poll.is_pending());
        drop(attachment);
        drop(transition);
        assert_no_managed_attachment(&wiring, &session);
        reporting.shutdown(Duration::from_secs(2)).await.unwrap();
    }
}

#[tokio::test]
async fn managed_attachment_captured_old_owner_is_revalidated_after_presence_wait() {
    let (_dir, wiring, reporting) = managed_attachment_fixture().await;
    let supervisor = PtySupervisor::new();
    let session = SessionId("s_attachment".into());
    let old = seed_managed_claude_owner(&supervisor, &session);
    let transition = wiring.store.lock_presence_transition().await;
    // The actual owner is captured outside the future's presence wait, just as adoption does.
    let mut claim = Box::pin(async {
        let presence = wiring.store.lock_presence_transition().await;
        let claimed = supervisor
            .with_claude_owner(&session, &old, || {
                wiring.claim_native_forwarder_for_owner(
                    "claude",
                    &session,
                    old.owner_id(),
                    &presence,
                )
            })
            .unwrap_or(false);
        drop(presence);
        claimed
    });
    assert!(futures::poll!(&mut claim).is_pending());
    assert_no_managed_attachment(&wiring, &session);
    let new = seed_managed_claude_owner(&supervisor, &session);
    assert_ne!(old.owner_id(), new.owner_id());
    drop(transition);
    assert!(
        !claim.await,
        "OLD must never borrow replacement owner authority"
    );
    assert_no_managed_attachment(&wiring, &session);
    assert!(new.is_current());
    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
}

fn shared_activation_request(key: &str) -> nexus_contracts::register::RegisterRequest {
    nexus_contracts::register::RegisterRequest {
        name: Some(key.into()),
        agent_id: None,
        harness: nexus_contracts::HarnessId::new("other").unwrap(),
        harness_session_id: format!("fixture-{key}"),
        project: "default".into(),
        client_key: key.into(),
        runtime_credential: None,
        tier: nexus_contracts::Tier::Agent,
        kind: None,
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

// Boot-empty actual AppState, then fixture-only generic rows. No PTY supervisor, native
// adoption/forwarder, provider process, or production all-native boot guarantee is exercised.
#[tokio::test(flavor = "current_thread")]
async fn managed_full_purge_app_cancellation_before_presence_has_no_effects() {
    use nexus_store::repos::{AgentRuntimes, Sessions};
    let (_dir, state, _identity_gate, session) = outer_activation_fixture().await;
    let before = Sessions::new(&state.store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    let runtime = AgentRuntimes::new(&state.store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap();
    let caller = state
        .identity
        .resolve("default", "outer-tail")
        .await
        .unwrap();
    let agent = before.agent_id.clone().map(nexus_contracts::AgentId);
    let guard = state.store.lock_presence_transition().await;
    let mut delete = Box::pin(state.delete_agent(&caller, agent.as_ref(), "outer-tail"));
    witness_combined_presence_wait(&guard, delete.as_mut()).await;
    drop(delete);
    assert_eq!(
        Sessions::new(&state.store)
            .find_by_session_id(&session)
            .await
            .unwrap(),
        Some(before)
    );
    assert_eq!(
        AgentRuntimes::new(&state.store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap(),
        runtime
    );
    drop(guard);
    assert_eq!(
        state
            .delete_agent(&caller, agent.as_ref(), "outer-tail")
            .await
            .unwrap()
            .status,
        "deleted"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn managed_full_purge_app_shutdown_never_uses_legacy_fallback() {
    managed_full_purge_app_case(1).await;
}

#[tokio::test(flavor = "current_thread")]
async fn managed_full_purge_app_reports_committed_identity_on_transport_error() {
    managed_full_purge_app_case(2).await;
}

#[tokio::test(flavor = "current_thread")]
async fn managed_full_purge_app_ignored_native_clear_vetoes_identity_delete() {
    managed_full_purge_app_case(3).await;
}

#[tokio::test(flavor = "current_thread")]
async fn managed_full_purge_app_changed_native_clear_vetoes_identity_delete() {
    managed_full_purge_app_case(4).await;
}

#[tokio::test(flavor = "current_thread")]
async fn managed_full_purge_app_success_advances_native_clear() {
    managed_full_purge_app_case(0).await;
}

async fn managed_full_purge_app_case(mode: u8) {
    use nexus_store::repos::{AgentRuntimes, Sessions};
    let (_dir, state, _identity_gate, session) = outer_activation_fixture().await;
    let before = Sessions::new(&state.store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert!(before.harness_session_id.is_some());
    let caller = state
        .identity
        .resolve("default", "outer-tail")
        .await
        .unwrap();
    let agent = before.agent_id.clone().map(nexus_contracts::AgentId);
    match mode {
        1 => state.drain_model_reporting_for_shutdown(Duration::from_secs(2)).await.unwrap(),
        2 => state.store.conn.execute_batch("CREATE TRIGGER full_app_delete_fail BEFORE DELETE ON sessions BEGIN SELECT RAISE(ABORT,'app transport delete trigger'); END;").await.unwrap(),
        3 => state.store.conn.execute_batch("CREATE TRIGGER full_app_clear_ignore BEFORE UPDATE OF harness_session_id ON sessions BEGIN SELECT RAISE(IGNORE); END;").await.unwrap(),
        4 => state.store.conn.execute_batch("CREATE TRIGGER full_app_clear_change AFTER UPDATE OF harness_session_id ON sessions BEGIN UPDATE sessions SET client_key='replacement-key' WHERE session_id=NEW.session_id; END;").await.unwrap(),
        _ => {},
    }
    let result = state
        .delete_agent(&caller, agent.as_ref(), "outer-tail")
        .await;
    let runtime = AgentRuntimes::new(&state.store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap();
    let current = Sessions::new(&state.store)
        .find_by_session_id(&session)
        .await
        .unwrap();
    if mode == 0 {
        assert_eq!(result.unwrap().status, "deleted");
        assert!(runtime.is_none());
        assert!(current.is_none());
    } else {
        let error = result
            .expect_err("managed rejection must not report deleted")
            .message;
        assert!(
            current.is_some(),
            "failed purge lost selected transport row"
        );
        match mode {
            1 => {
                assert!(error.contains("not admitting purge"), "{error}");
                assert!(runtime.is_some());
            }
            2 => {
                assert!(error.contains("app transport delete trigger"), "{error}");
                assert!(
                    error.contains("identity purge committed"),
                    "partial outcome lost: {error}"
                );
                assert!(runtime.is_none());
            }
            _ => {
                assert!(error.contains("selection changed"), "{error}");
                assert!(runtime.is_some());
            }
        }
        if mode == 3 {
            assert_eq!(
                current.unwrap().harness_session_id,
                before.harness_session_id
            );
        }
    }
    // This fixture has no native supervisor or opened adapter. It proves the actual managed
    // AppState caller and durable partial outcome, not native kill/callback settlement.
}

async fn outer_activation_fixture() -> (
    tempfile::TempDir,
    crate::daemon::AppState,
    Arc<tokio::sync::Mutex<()>>,
    SessionId,
) {
    let dir = tempfile::tempdir().unwrap();
    let daemon = nexus_store::DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    let identity_gate = daemon.identity().write_lock();
    let store = Arc::new(daemon.compatibility_store());
    let executor = Arc::new(PtyTransport::default());
    let state = crate::daemon::AppState::wire_with_turn_exec(
        store.clone(),
        &nexus_common::Config::default(),
        executor.clone(),
    );
    // Capture before the current-thread executor can poll boot. Its cloned AppState owns two
    // executor Arcs (agent and LoopWiring.turn_exec), released only when spawn_boot_respawn's
    // task exits. The keeper and periodic reconciler retain their Arcs; empty boot creates no
    // event loops or recipient tasks. This witnesses boot completion, not just ingress readiness.
    assert!(matches!(
        tokio::runtime::Handle::current().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread
    ));
    let boot_owned = Arc::strong_count(&executor);
    tokio::time::timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&executor) == boot_owned {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("empty boot task must release its executor ownership");
    assert_eq!(Arc::strong_count(&executor), boot_owned - 2);
    state.wait_for_runtime_identity_ready().await.unwrap();
    assert!(state.pty_supervisor().is_none());
    let session = state
        .identity
        .register(shared_activation_request("outer-tail"))
        .await
        .unwrap()
        .session_id;
    store.conn.execute(
        "UPDATE sessions SET transport='pty',presence='offline',last_heartbeat=1 WHERE session_id=?1",
        libsql::params![session.0.clone()],
    ).await.unwrap();
    store.identity_conn().execute(
        "UPDATE agent_runtimes SET transport='pty',presence='offline',last_heartbeat=1,stopped_at=9 WHERE runtime_id=?1",
        libsql::params![session.0.clone()],
    ).await.unwrap();
    assert!(!state.presence.registry().is_present(&session));
    (dir, state, identity_gate, session)
}

// Capture the actual initiating adoption warning, whose public caller intentionally returns ().
// Locks are held only during synchronous event recording, never across a test await.
struct OuterActivationWarnings(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for OuterActivationWarnings {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                write!(&mut self.0, "{}={value:?};", field.name()).unwrap();
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

async fn outer_activation_adoption(fail: bool) {
    use nexus_store::repos::{AgentRuntimes, Sessions};
    use tracing::instrument::WithSubscriber;
    let (_dir, state, identity_gate, session) = outer_activation_fixture().await;
    let store = &state.store;
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET stopped_at=NULL WHERE runtime_id=?1",
            libsql::params![session.0.clone()],
        )
        .await
        .unwrap();
    let selected = AgentRuntimes::new(store)
        .list_active_by_transport("pty")
        .await
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].runtime_id, session.0);
    assert_eq!(selected[0].harness, "other");
    assert!(Sessions::new(store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap()
        .is_agent());
    if fail {
        store.identity_conn().execute_batch(
            "CREATE TRIGGER outer_activation_fail BEFORE UPDATE OF active ON agent_runtimes BEGIN SELECT RAISE(ABORT,'outer adoption activation trigger'); END;",
        ).await.unwrap();
    }
    let before = AgentRuntimes::new(store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    let held = identity_gate.clone().lock_owned().await;
    let baseline = Arc::strong_count(&identity_gate);
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let mut adoption = Box::pin(
        state
            .adopt_active_pty_runtimes()
            .with_subscriber(OuterActivationWarnings(warnings.clone())),
    );
    // Only the submitted managed activation worker can clone this identity gate now: boot is
    // ready, fixture writes are finished, and no event loop has been spawned. Transport Session
    // writes use the OTHER gate. The owned guard is unwind-safe and released before settlement.
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = &mut adoption => panic!("adoption settled before actual managed activation wait"),
            _ = async {
                while Arc::strong_count(&identity_gate) == baseline {
                    tokio::task::yield_now().await;
                }
            } => {}
        }
    })
    .await
    .expect("adoption never submitted activation to the identity write gate");
    assert_eq!(Arc::strong_count(&identity_gate), baseline + 1);
    let registry = state.presence.registry();
    assert!(
        registry.is_present(&session),
        "first EventLoop attachment precedes activation"
    );
    let first_attachment = registry.snapshot();
    drop(held);
    adoption.await;
    let after = AgentRuntimes::new(store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    let row = Sessions::new(store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.presence.as_deref(), Some("online"));
    assert!(
        row.last_heartbeat.unwrap() > 1,
        "preceding Session writes are not rolled back"
    );
    let warnings = warnings.lock().unwrap().clone();
    if fail {
        assert!(
            warnings.iter().any(|warning| warning
                .contains("failed to materialize adopted PTY runtime presence")
                && warning.contains("outer adoption activation trigger")
                && warning.contains("NotCommitted")
                && warning.contains(
                    "Session heartbeat/conditional presence writes may already have committed"
                )),
            "initiating activation must reach and report the real failing trigger: {warnings:?}"
        );
        assert_eq!(
            after, before,
            "runtime activation failure preserves runtime projection"
        );
        assert!(registry.matches_captured(&session, &first_attachment),
            "failed managed activation must preserve the FIRST attachment, skipping later spawn_loop replacement");
        // The injected activation failure must not also fault shutdown's unrelated stop write.
        store
            .identity_conn()
            .execute_batch("DROP TRIGGER outer_activation_fail")
            .await
            .unwrap();
    } else {
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(after.presence.as_deref(), Some("online"));
        assert_eq!(after.stopped_at, None);
        assert!(
            !registry.matches_captured(&session, &first_attachment),
            "successful same-branch adoption must execute later spawn_loop replacement"
        );
        assert!(registry.is_present(&session));
    }
    state.teardown_owned_transports_for_shutdown().await;
    state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn outer_activation_adoption_failure_skips_later_loop_replacement() {
    outer_activation_adoption(true).await;
}

#[tokio::test]
async fn outer_activation_adoption_success_replaces_first_attachment() {
    outer_activation_adoption(false).await;
}

async fn outer_activation_resume(closed: bool) {
    use crate::daemon::services::presence::TransportHandle;
    use nexus_store::repos::{AgentRuntimes, Sessions};
    let (_dir, state, _identity_gate, session) = outer_activation_fixture().await;
    let registry = state.presence.registry();
    registry.attach(&session, TransportHandle::EventLoop);
    registry.attach(&session, TransportHandle::RawStream);
    let prior = registry.snapshot();
    let before = AgentRuntimes::new(&state.store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap();
    if closed {
        state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
    }
    let result = state
        .finish_codex_appserver_resume("outer-tail", &session, "default", false)
        .await;
    let after = AgentRuntimes::new(&state.store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap();
    if closed {
        let error = result
            .expect_err("closed activation must fail actual resume finish")
            .to_string();
        assert!(
            error.contains("model reporting is not admitting activation")
                && error.contains("activation rejected before store")
                && error.contains(
                    "Session heartbeat/conditional presence writes may already have committed"
                ),
            "{error}"
        );
        assert_eq!(after, before);
        assert!(
            registry.matches_captured(&session, &prior),
            "resume failure must not publish wakeable tail"
        );
    } else {
        assert_eq!(result.unwrap(), session);
        assert_eq!(after.unwrap().presence.as_deref(), Some("online"));
        assert!(
            !registry.matches_captured(&session, &prior),
            "normal resume must make the agent wakeable"
        );
    }
    let row = Sessions::new(&state.store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.presence.as_deref(), Some("online"));
    assert!(row.last_heartbeat.unwrap() > 1);
    state.teardown_owned_transports_for_shutdown().await;
    state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn outer_activation_resume_closed_preserves_prior_attachment() {
    outer_activation_resume(true).await;
}

#[tokio::test]
async fn outer_activation_resume_success_restores_wakeable_tail() {
    outer_activation_resume(false).await;
}

async fn outer_activation_resume_with_failed_session_telemetry(fail_activation: bool) {
    use crate::daemon::services::presence::TransportHandle;
    use nexus_store::repos::{AgentRuntimes, Sessions};
    use tracing::instrument::WithSubscriber;
    let (_dir, state, _identity_gate, session) = outer_activation_fixture().await;
    let store = &state.store;
    let before = AgentRuntimes::new(store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    // One actual runtime, no sibling lifecycle appends: only Session's online transition fails.
    assert!(AgentRuntimes::new(store)
        .active_sibling_runtime_pairs(&before.agent_id, &session.0)
        .await
        .unwrap()
        .is_empty());
    store.conn.execute_batch("CREATE TRIGGER fail_session_resume_telemetry BEFORE INSERT ON developer_events WHEN NEW.lifecycle='started' BEGIN SELECT RAISE(ABORT,'Session resume telemetry failure'); END;").await.unwrap();
    if fail_activation {
        store.identity_conn().execute_batch("CREATE TRIGGER fail_resume_activation BEFORE UPDATE OF active ON agent_runtimes BEGIN SELECT RAISE(ABORT,'required resume activation failure'); END;").await.unwrap();
    }
    let registry = state.presence.registry();
    registry.attach(&session, TransportHandle::EventLoop);
    registry.attach(&session, TransportHandle::RawStream);
    let prior = registry.snapshot();
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let result = state
        .finish_codex_appserver_resume("outer-tail", &session, "default", false)
        .with_subscriber(OuterActivationWarnings(warnings.clone()))
        .await;
    let after = AgentRuntimes::new(store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    if fail_activation {
        let error = result
            .expect_err("required activation failure must still suppress wakeable tail")
            .to_string();
        assert!(
            error.contains("required resume activation failure")
                && error.contains("NotCommitted")
                && error.contains(
                    "Session heartbeat/conditional presence writes may already have committed"
                ),
            "{error}"
        );
        assert!(
            !error.contains("Session resume telemetry failure"),
            "{error}"
        );
        assert_eq!(after, before);
        assert!(registry.matches_captured(&session, &prior));
    } else {
        assert_eq!(
            result.expect("Session telemetry must not prevent required activation"),
            session
        );
        assert_eq!(after.presence.as_deref(), Some("online"));
        assert!(after.active);
        assert_eq!(after.stopped_at, None);
        assert!(after.last_heartbeat.unwrap() > 1);
        assert!(!registry.matches_captured(&session, &prior));
    }
    let warnings = warnings.lock().unwrap().clone();
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("Session resume telemetry failure")),
        "actual Session append failure must be reported best-effort: {warnings:?}"
    );
    let row = Sessions::new(store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.presence.as_deref(), Some("online"));
    assert!(row.last_heartbeat.unwrap() > 1);
    store
        .conn
        .execute_batch("DROP TRIGGER fail_session_resume_telemetry")
        .await
        .unwrap();
    if fail_activation {
        store
            .identity_conn()
            .execute_batch("DROP TRIGGER fail_resume_activation")
            .await
            .unwrap();
    }
    state.teardown_owned_transports_for_shutdown().await;
    state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn outer_activation_resume_tolerates_session_telemetry_before_real_activation() {
    outer_activation_resume_with_failed_session_telemetry(false).await;
}

#[tokio::test]
async fn outer_activation_resume_session_telemetry_does_not_mask_activation_failure() {
    outer_activation_resume_with_failed_session_telemetry(true).await;
}

async fn outer_activation_resume_required_projection_failure(status_read: bool) {
    use crate::daemon::services::presence::TransportHandle;
    use nexus_store::repos::AgentRuntimes;
    let (_dir, state, _identity_gate, session) = outer_activation_fixture().await;
    let store = &state.store;
    if status_read {
        // Lookup initially succeeds; only the later status read sees the invalid kind. The
        // Session UPDATE and actual activation/runtime projection are allowed to commit first.
        store.conn.execute_batch("CREATE TRIGGER fail_resume_status_read AFTER UPDATE OF presence ON sessions BEGIN UPDATE sessions SET kind='invalid-resume-status-kind' WHERE session_id=NEW.session_id; END;").await.unwrap();
    } else {
        store.identity_conn().execute_batch("CREATE TRIGGER fail_resume_runtime_presence BEFORE UPDATE OF presence ON agent_runtimes BEGIN SELECT RAISE(ABORT,'required resume runtime presence failure'); END;").await.unwrap();
    }
    let registry = state.presence.registry();
    registry.attach(&session, TransportHandle::EventLoop);
    let prior = registry.snapshot();
    let error = state
        .finish_codex_appserver_resume("outer-tail", &session, "default", false)
        .await
        .expect_err("required projection error must stop the wakeable tail")
        .to_string();
    let runtime = AgentRuntimes::new(store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert!(runtime.active);
    assert_eq!(
        runtime.stopped_at, None,
        "earlier activation is not rolled back"
    );
    assert!(registry.matches_captured(&session, &prior));
    if status_read {
        assert!(
            error.contains("unknown stored session kind")
                && error.contains("invalid-resume-status-kind"),
            "{error}"
        );
        assert_eq!(runtime.presence.as_deref(), Some("online"));
        assert!(runtime.last_heartbeat.unwrap() > 1);
        store
            .conn
            .execute_batch(
                "DROP TRIGGER fail_resume_status_read; UPDATE sessions SET kind='agent';",
            )
            .await
            .unwrap();
    } else {
        assert!(
            error.contains("required resume runtime presence failure"),
            "{error}"
        );
        assert_eq!(runtime.presence.as_deref(), Some("offline"));
        assert_eq!(runtime.last_heartbeat, Some(1));
        store
            .identity_conn()
            .execute_batch("DROP TRIGGER fail_resume_runtime_presence")
            .await
            .unwrap();
    }
    let row = nexus_store::repos::Sessions::new(store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.presence.as_deref(), Some("online"));
    assert!(row.last_heartbeat.unwrap() > 1);
    state.teardown_owned_transports_for_shutdown().await;
    state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn outer_activation_resume_runtime_presence_failure_skips_wakeable_tail() {
    outer_activation_resume_required_projection_failure(false).await;
}

#[tokio::test]
async fn outer_activation_resume_status_read_failure_skips_wakeable_tail() {
    outer_activation_resume_required_projection_failure(true).await;
}

async fn shared_activation_root(root: u8, identity: bool) {
    use nexus_store::repos::{AgentRuntimes, Sessions};
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        nexus_store::Store::open(dir.path().join("fixture.db").to_str().unwrap())
            .await
            .unwrap(),
    );
    store.migrate().await.unwrap();
    let config = nexus_common::Config::default();
    // All constructors are inert: empty runtime worklists, no native open/launch/bind.
    let state = match root {
        0 => crate::daemon::AppState::wire_with_registry(
            store.clone(),
            &config,
            nexus_agent::AdapterRegistry::new(),
        ),
        1 => crate::daemon::AppState::wire_with_turn_exec(
            store.clone(),
            &config,
            Arc::new(PtyTransport::default()),
        ),
        _ => crate::daemon::AppState::wire_pty_with_gateway_stream(store.clone(), &config, None),
    };
    state.wait_for_runtime_identity_ready().await.unwrap();
    let registered = state
        .identity
        .register(shared_activation_request("before-close"))
        .await
        .unwrap();
    let session = registered.session_id;
    state.presence.materialize_online(&session).await.unwrap();
    state
        .presence
        .restore_online_on_activity(&session)
        .await
        .unwrap();
    state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    if identity {
        let error = state
            .identity
            .register(shared_activation_request("after-close"))
            .await
            .expect_err("root Identity must share closed coordinator")
            .to_string();
        assert!(
            error.contains("not admitting activation"),
            "managed rejection cause: {error}"
        );
        // Fresh registration staged the compatibility row before managed activation rejection.
        let staged = Sessions::new(&store)
            .find_by_client_key("default", "after-close")
            .await
            .unwrap()
            .expect("managed rejection retains staged registration");
        assert!(AgentRuntimes::new(&store)
            .find_by_runtime_id(&staged.session_id.0)
            .await
            .unwrap()
            .is_none());
    } else {
        for activity in [false, true] {
            store
                .conn
                .execute(
                    "UPDATE sessions SET presence='offline',last_heartbeat=1 WHERE session_id=?1",
                    libsql::params![session.0.clone()],
                )
                .await
                .unwrap();
            store.identity_conn().execute("UPDATE agent_runtimes SET presence='offline',last_heartbeat=1,stopped_at=9 WHERE runtime_id=?1", libsql::params![session.0.clone()]).await.unwrap();
            let result = if activity {
                state.presence.restore_online_on_activity(&session).await
            } else {
                state.presence.materialize_online(&session).await
            };
            let error = result
                .expect_err("root Presence must share closed coordinator")
                .to_string();
            assert!(
                error.contains("not admitting activation")
                    && error.contains("Session")
                    && error.contains("already"),
                "{error}"
            );
            let row = Sessions::new(&store)
                .find_by_session_id(&session)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.presence.as_deref(), Some("online"));
            assert!(row.last_heartbeat.unwrap() > 1);
            let runtime = AgentRuntimes::new(&store)
                .find_by_runtime_id(&session.0)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(runtime.presence.as_deref(), Some("offline"));
            assert_eq!(runtime.last_heartbeat, Some(1));
            assert_eq!(runtime.stopped_at, Some(9));
        }
    }
}

#[tokio::test]
async fn shared_activation_generated_late_failure_retains_coupled_state() {
    use nexus_store::repos::{AgentRuntimes, Agents, Sessions};
    for root in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            nexus_store::Store::open(dir.path().join("fixture.db").to_str().unwrap())
                .await
                .unwrap(),
        );
        store.migrate().await.unwrap();
        let config = nexus_common::Config::default();
        let state = match root {
            0 => crate::daemon::AppState::wire_with_registry(
                store.clone(),
                &config,
                nexus_agent::AdapterRegistry::new(),
            ),
            1 => crate::daemon::AppState::wire_with_turn_exec(
                store.clone(),
                &config,
                Arc::new(PtyTransport::default()),
            ),
            _ => {
                crate::daemon::AppState::wire_pty_with_gateway_stream(store.clone(), &config, None)
            }
        };
        state.wait_for_runtime_identity_ready().await.unwrap();
        store.conn.execute_batch("CREATE TRIGGER shared_activation_stamp_fail BEFORE UPDATE OF agent_id ON sessions WHEN OLD.client_key='late-stamp' BEGIN SELECT RAISE(ABORT,'original late generated stamp failure'); END;").await.unwrap();
        let error = state
            .identity
            .register(shared_activation_request("late-stamp"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("original late generated stamp failure"),
            "{error}"
        );
        let session = Sessions::new(&store)
            .find_by_client_key("default", "late-stamp")
            .await
            .unwrap()
            .expect("Session must survive already-applied runtime activation");
        let runtime = AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.session_id.0)
            .await
            .unwrap()
            .expect("committed generated runtime must survive late stamp failure");
        assert!(runtime.active);
        assert_eq!(runtime.agent_id, format!("a_{}", session.session_id.0));
        assert!(Agents::new(&store)
            .find_by_id(&runtime.agent_id)
            .await
            .unwrap()
            .is_some());
        assert!(
            session.agent_id.is_none(),
            "the rejected stamp is not repaired"
        );
        assert!(
            error.contains("retained"),
            "partial registration disposition: {error}"
        );
        state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn shared_activation_registry_identity() {
    shared_activation_root(0, true).await;
}
#[tokio::test]
async fn shared_activation_registry_presence() {
    shared_activation_root(0, false).await;
}
#[tokio::test]
async fn shared_activation_turn_exec_identity() {
    shared_activation_root(1, true).await;
}
#[tokio::test]
async fn shared_activation_turn_exec_presence() {
    shared_activation_root(1, false).await;
}
#[tokio::test]
async fn shared_activation_pty_identity() {
    shared_activation_root(2, true).await;
}
#[tokio::test]
async fn shared_activation_pty_presence() {
    shared_activation_root(2, false).await;
}

// Fixture initial/replacement state only: production binding construction derives a hook path
// from HOME, so do not invoke it in these hermetic attachment tests. Actual owner revalidation and
// AppState adoption still run through production code; native binding construction is not tested.
fn seed_managed_claude_owner(
    supervisor: &PtySupervisor,
    session: &SessionId,
) -> Arc<ClaudeTurnCompletion> {
    let owner = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    if let Some(old) = supervisor
        .claude_completions
        .lock()
        .unwrap()
        .insert(session.clone(), owner.clone())
    {
        old.invalidate();
    }
    owner
}

#[tokio::test]
async fn managed_attachment_actual_wakeable_helper_waits_and_cancels_before_publication() {
    // Exercise the actual AppState helper without exposing its private loop/dispatch registry.
    // This observes attachment publication; registry Idle/Paused ordering is a source-level pin.
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = crate::daemon::AppState::wire_with_turn_exec(
        store.clone(),
        &nexus_common::Config::default(),
        Arc::new(PtyTransport::default()),
    );
    state.wait_for_runtime_identity_ready().await.unwrap();
    let session = SessionId("s_wakeable_attachment".into());
    for paused in [false, true] {
        let transition = store.lock_presence_transition().await;
        let mut wake = Box::pin(state.make_live_agent_wakeable(&session, "default", paused));
        assert!(futures::poll!(&mut wake).is_pending());
        assert!(!state.presence.registry().is_present(&session));
        drop(wake);
        drop(transition);
        assert!(!state.presence.registry().is_present(&session));
    }
    tokio::time::timeout(
        Duration::from_secs(2),
        state.make_live_agent_wakeable(&session, "default", false),
    )
    .await
    .unwrap();
    assert!(state.presence.registry().is_present(&session));
    state.teardown_owned_transports_for_shutdown().await;
}

#[derive(Debug, Clone, Copy)]
enum ManagedBindingBranch {
    SameSession,
    DeadName,
    FreshName,
}

// On a current-thread executor, the only code running between these Arc counts is the target's
// synchronous poll. Store::lock_presence_transition clones this underlying mutex Arc at entry;
// Arc<Store> clones and repository reads do not. Keeping the guard proves a real queued wait.
async fn witness_combined_presence_wait(
    held: &tokio::sync::OwnedMutexGuard<()>,
    mut target: std::pin::Pin<&mut impl std::future::Future>,
) {
    assert!(matches!(
        tokio::runtime::Handle::current().runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread
    ));
    let mutex = tokio::sync::OwnedMutexGuard::mutex(held);
    tokio::time::timeout(
        Duration::from_secs(2),
        futures::future::poll_fn(|cx| {
            let before = Arc::strong_count(mutex);
            // Prevent Tokio's semaphore budget check from yielding before FIFO enqueue.
            let polled = std::future::Future::poll(
                std::pin::pin!(tokio::task::unconstrained(target.as_mut())),
                cx,
            );
            assert!(
                polled.is_pending(),
                "target settled before entering held presence gate"
            );
            let after = Arc::strong_count(mutex);
            if after == before {
                return std::task::Poll::Pending;
            }
            assert_eq!(
                after,
                before + 1,
                "target must retain exactly one presence-wait Arc"
            );
            std::task::Poll::Ready(())
        }),
    )
    .await
    .expect("target never entered actual presence gate");
}

#[tokio::test(flavor = "current_thread")]
async fn combined_stale_whole_caller_preserves_equal_handle_replacement() {
    use crate::daemon::services::presence::TransportHandle;
    use nexus_store::repos::{AgentRuntimes, Sessions};
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        nexus_store::Store::open(dir.path().join("combined.db").to_str().unwrap())
            .await
            .unwrap(),
    );
    store.migrate().await.unwrap();
    let state = crate::daemon::AppState::wire_with_turn_exec(
        store.clone(),
        &nexus_common::Config::default(),
        Arc::new(PtyTransport::default()),
    );
    state.wait_for_runtime_identity_ready().await.unwrap();
    let session = SessionId("s_combined".into());
    state
        .bind_member(
            &session,
            "a_combined",
            Some("combined"),
            "default",
            nexus_contracts::HarnessId::new("other").unwrap(),
            None,
            "key",
            None,
            "pty",
            None,
        )
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE sessions SET created_at=1,last_heartbeat=1 WHERE session_id='s_combined'",
            (),
        )
        .await
        .unwrap();
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET started_at=1,last_heartbeat=1 WHERE runtime_id='s_combined'",
            (),
        )
        .await
        .unwrap();
    let registry = state.presence.registry();
    registry.attach(&session, TransportHandle::RawStream);
    let before = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap();
    let transition = store.lock_presence_transition().await;
    let mut stale = Box::pin(state.reconcile_stale_presence_once());
    witness_combined_presence_wait(&transition, stale.as_mut()).await;
    // Replacement under the same real admission gate deliberately leaves OLD heartbeat intact.
    registry.attach(&session, TransportHandle::RawStream);
    drop(transition);
    stale.await.unwrap();
    assert!(
        registry.is_present(&session),
        "both stale phases must preserve NEW attachment"
    );
    assert_eq!(
        AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap(),
        before,
        "runtime-only trailing sweep must not undo compatibility skip"
    );
    assert_eq!(
        Sessions::new(&store)
            .find_by_session_id(&session)
            .await
            .unwrap()
            .unwrap()
            .presence
            .as_deref(),
        Some("online")
    );
    state.teardown_owned_transports_for_shutdown().await;
}

async fn combined_actual_bind_race(stale_wins: bool) {
    use nexus_store::repos::{AgentRuntimes, Sessions};
    let dir = tempfile::tempdir().unwrap();
    let daemon = nexus_store::DaemonStore::open(dir.path().join("bind-race.db").to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    let state = crate::daemon::AppState::wire_with_turn_exec(
        store.clone(),
        &nexus_common::Config::default(),
        Arc::new(PtyTransport::default()),
    );
    state.wait_for_runtime_identity_ready().await.unwrap();
    let session = SessionId("s_combined_bind".into());
    let bind = || {
        state.bind_member(
            &session,
            "a_combined_bind",
            Some("combined-bind"),
            "default",
            nexus_contracts::HarnessId::new("other").unwrap(),
            None,
            "new-key",
            None,
            "pty",
            None,
        )
    };
    bind().await.unwrap();
    store
        .conn
        .execute("UPDATE sessions SET created_at=1,last_heartbeat=1", ())
        .await
        .unwrap();
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET started_at=1,last_heartbeat=1",
            (),
        )
        .await
        .unwrap();
    if stale_wins {
        let gate = store
            .begin_identity_write_txn("combined_bind_stale_wins")
            .await
            .unwrap();
        let mut stale = Box::pin(state.reconcile_stale_presence_once());
        assert!(futures::poll!(&mut stale).is_pending());
        // Durable first-store commit is an entered handshake, not a timed scheduling guess.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                assert!(futures::poll!(&mut stale).is_pending());
                if Sessions::new(&store)
                    .find_by_session_id(&session)
                    .await
                    .unwrap()
                    .unwrap()
                    .presence
                    .as_deref()
                    == Some("offline")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut new_bind = Box::pin(bind());
        assert!(futures::poll!(&mut new_bind).is_pending());
        drop(stale);
        assert!(
            futures::poll!(&mut new_bind).is_pending(),
            "owned stale settlement must retain presence after caller cancellation"
        );
        gate.commit().await.unwrap();
        new_bind.await.unwrap();
    } else {
        let guard = store.lock_presence_transition().await;
        let mut new_bind = Box::pin(bind());
        witness_combined_presence_wait(&guard, new_bind.as_mut()).await;
        let mut stale = Box::pin(state.reconcile_stale_presence_once());
        witness_combined_presence_wait(&guard, stale.as_mut()).await;
        drop(guard);
        new_bind.await.unwrap();
        let before = AgentRuntimes::new(&store)
            .find_by_runtime_id(&session.0)
            .await
            .unwrap();
        let before_session = Sessions::new(&store)
            .find_by_session_id(&session)
            .await
            .unwrap();
        stale.await.unwrap();
        assert_eq!(
            AgentRuntimes::new(&store)
                .find_by_runtime_id(&session.0)
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            Sessions::new(&store)
                .find_by_session_id(&session)
                .await
                .unwrap(),
            before_session
        );
    }
    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert!(runtime.active);
    assert_eq!(runtime.presence.as_deref(), Some("online"));
    state.teardown_owned_transports_for_shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn combined_stale_actual_new_bind_wins_zero_effects() {
    combined_actual_bind_race(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn combined_stale_actual_bind_waits_through_cancelled_owned_settlement() {
    combined_actual_bind_race(true).await;
}

async fn managed_binding_rows(
    store: &nexus_store::Store,
) -> (
    Vec<nexus_store::types::SessionRow>,
    Vec<nexus_store::types::AgentRow>,
    Vec<nexus_store::types::AgentRuntimeRow>,
) {
    use nexus_store::repos::{AgentRuntimes, Agents, Sessions};
    (
        Sessions::new(store).list_all().await.unwrap(),
        Agents::new(store).list(None, true).await.unwrap(),
        AgentRuntimes::new(store)
            .list_for_agent("a_binding", true)
            .await
            .unwrap(),
    )
}

async fn managed_binding_waits_for_presence(branch: ManagedBindingBranch, cancel: bool) {
    use nexus_contracts::Presence;
    use nexus_store::repos::{
        AgentRuntimes, Agents, NewAgent, NewAgentRuntime, NewSession, Sessions,
    };

    // Real AppState, disposable store, and an empty inert transport: no native owner, launch,
    // provider metadata, or process-global HOME changes are needed to exercise bind_member.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        nexus_store::Store::open(dir.path().join("binding.db").to_str().unwrap())
            .await
            .unwrap(),
    );
    store.migrate().await.unwrap();
    let state = crate::daemon::AppState::wire_with_turn_exec(
        store.clone(),
        &nexus_common::Config::default(),
        Arc::new(PtyTransport::default()),
    );
    state.wait_for_runtime_identity_ready().await.unwrap();
    let session = SessionId("s_binding".into());
    let fossil = SessionId("s_binding_fossil".into());
    let sessions = Sessions::new(&store);
    if !matches!(branch, ManagedBindingBranch::FreshName) {
        let existing = if matches!(branch, ManagedBindingBranch::DeadName) {
            &fossil
        } else {
            &session
        };
        sessions
            .create(NewSession {
                session_id: existing.clone(),
                name: Some("binding".into()),
                agent: Some("other".into()),
                kind: "agent".into(),
                role: None,
                tier: "agent".into(),
                harness_session_id: None,
                client_key: Some("old-key".into()),
                cwd: None,
                project: "default".into(),
                transport: None,
            })
            .await
            .unwrap();
        // Keep the background stale sweep from racing fixture rows while the waiter is driven.
        sessions.touch_heartbeat(existing).await.unwrap();
        sessions
            .set_presence(existing, Presence::Offline)
            .await
            .unwrap();
        if matches!(branch, ManagedBindingBranch::DeadName) {
            Agents::new(&store)
                .create(NewAgent {
                    agent_id: "a_binding".into(),
                    project: "default".into(),
                    name: Some("binding".into()),
                    default_harness: Some("other".into()),
                    role: None,
                    tier: None,
                    owner: None,
                })
                .await
                .unwrap();
            sessions.set_agent_id(existing, "a_binding").await.unwrap();
            AgentRuntimes::new(&store)
                .create(NewAgentRuntime {
                    runtime_id: fossil.0.clone(),
                    agent_id: "a_binding".into(),
                    harness: "other".into(),
                    cwd: None,
                    transport: None,
                    presence: Some("offline".into()),
                    active: false,
                })
                .await
                .unwrap();
        } else {
            assert!(sessions
                .find_by_session_id(existing)
                .await
                .unwrap()
                .unwrap()
                .agent_id
                .is_none());
        }
    }
    let before = managed_binding_rows(&store).await;
    let bind = || {
        state.bind_member(
            &session,
            "a_binding",
            Some("binding"),
            "default",
            nexus_contracts::HarnessId::new("other").unwrap(),
            None,
            "new-key",
            None,
            "pty",
            None,
        )
    };
    let transition = store.lock_presence_transition().await;
    let mut pending = Box::pin(bind());
    assert!(futures::poll!(&mut pending).is_pending());
    // Drive through any asynchronous store reads: a single Pending poll could precede the bug.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(
        managed_binding_rows(&store).await,
        before,
        "{branch:?}: binding changed rows before presence admission"
    );
    assert!(!state.presence.registry().is_present(&session));
    if cancel {
        drop(pending);
        drop(transition);
        tokio::task::yield_now().await;
        let control = store.lock_presence_transition().await;
        assert_eq!(managed_binding_rows(&store).await, before);
        assert!(!state.presence.registry().is_present(&session));
        drop(control);
        // A cancelled waiter must not strand admission for an independent ordinary call.
        tokio::time::timeout(Duration::from_secs(2), bind())
            .await
            .expect("independent binding must complete after cancellation")
            .unwrap();
    } else {
        drop(transition);
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .expect("admitted binding must not recursively acquire presence")
            .unwrap();
    }
    let row = sessions
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.agent_id.as_deref(), Some("a_binding"));
    assert_eq!(row.name.as_deref(), Some("binding"));
    assert_eq!(row.project, "default");
    assert_eq!(row.transport.as_deref(), Some("pty"));
    assert_eq!(row.presence.as_deref(), Some("online"));
    assert!(row.last_heartbeat.is_some());
    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(runtime.agent_id, "a_binding");
    assert_eq!(runtime.presence.as_deref(), Some("online"));
    assert!(runtime.active);
    let agents = Agents::new(&store).list(None, true).await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].agent_id, "a_binding");
    assert_eq!(agents[0].name.as_deref(), Some("binding"));
    assert_eq!(sessions.list_all().await.unwrap().len(), 1);
    if matches!(branch, ManagedBindingBranch::DeadName) {
        assert_eq!(agents, before.1, "reuse must retain the stable identity");
        assert!(sessions
            .find_by_session_id(&fossil)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            AgentRuntimes::new(&store)
                .find_by_runtime_id(&fossil.0)
                .await
                .unwrap(),
            before.2.first().cloned(),
            "name reuse must preserve the old inactive runtime fossil"
        );
    }
    state.teardown_owned_transports_for_shutdown().await;
}

#[tokio::test]
async fn managed_binding_same_session_waits_before_identity_and_runtime_creation() {
    managed_binding_waits_for_presence(ManagedBindingBranch::SameSession, false).await;
}

#[tokio::test]
async fn managed_binding_dead_name_waits_before_rebinding() {
    managed_binding_waits_for_presence(ManagedBindingBranch::DeadName, false).await;
}

#[tokio::test]
async fn managed_binding_fresh_name_waits_before_creation() {
    managed_binding_waits_for_presence(ManagedBindingBranch::FreshName, false).await;
}

#[tokio::test]
async fn managed_binding_cancelled_before_admission_has_no_effects() {
    for branch in [
        ManagedBindingBranch::SameSession,
        ManagedBindingBranch::DeadName,
        ManagedBindingBranch::FreshName,
    ] {
        managed_binding_waits_for_presence(branch, true).await;
    }
}

#[tokio::test]
async fn managed_attachment_actual_adoption_captures_owner_before_presence_wait() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = crate::daemon::AppState::wire_pty(store.clone(), &nexus_common::Config::default());
    state.wait_for_runtime_identity_ready().await.unwrap();
    let supervisor = state.pty_supervisor().unwrap();
    let session = SessionId("s_adoption_attachment".into());
    // Even an incorrect late-capture implementation can only read this disposable native path.
    let paths = ClaudeNativeBridgePaths::new(dir.path(), &session);
    std::fs::create_dir_all(&paths.bridge_dir).unwrap();
    std::fs::write(&paths.hook_log_path, "").unwrap();
    ClaudeRuntimeStateRepo::new(&store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: session.clone(),
            bridge_dir: paths.bridge_dir.clone(),
            claude_session_id: Some("native".into()),
            launch_cwd: dir.path().to_path_buf(),
            transcript_path: Some(paths.bridge_dir.join("transcript.jsonl")),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    let old = seed_managed_claude_owner(supervisor, &session);
    let transition = store.lock_presence_transition().await;
    let mut adoption = Box::pin(state.spawn_claude_native_forwarder_if_needed(&session));
    assert!(futures::poll!(&mut adoption).is_pending());
    assert!(!state.presence.registry().is_present(&session));
    let new = seed_managed_claude_owner(supervisor, &session);
    assert_ne!(old.owner_id(), new.owner_id());
    drop(transition);
    assert!(
        futures::poll!(&mut adoption).is_ready(),
        "obsolete adoption must return before native setup/I/O"
    );
    assert!(
        !state.presence.registry().is_present(&session),
        "late OLD adoption must not claim using NEW's owner"
    );
    assert!(new.is_current());

    let transition = store.lock_presence_transition().await;
    let mut cancelled = Box::pin(state.spawn_claude_native_forwarder_if_needed(&session));
    assert!(futures::poll!(&mut cancelled).is_pending());
    drop(cancelled);
    drop(transition);
    assert!(!state.presence.registry().is_present(&session));
}

#[cfg(unix)]
#[derive(Default)]
struct ClaudeFixtureProcesses {
    raw: Option<Arc<PtySession>>,
    tmux: Option<Arc<TmuxHarness>>,
}

#[cfg(unix)]
impl Drop for ClaudeFixtureProcesses {
    fn drop(&mut self) {
        if let Some(raw) = &self.raw {
            let _ = raw.kill();
        }
        if let Some(tmux) = &self.tmux {
            let _ = tmux.kill();
        }
    }
}
#[async_trait]
impl EventSink for DiscardClaudeDisplay {
    async fn emit(&self, _: nexus_contracts::WsEvent) {}
}

#[tokio::test]
async fn claude_new_forwarder_claim_survives_late_old_track_and_cleanup() {
    use crate::daemon::claude_native_forwarder::spawn_claude_native_forwarder_with_tool_events;
    use std::io::Write;
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let state = crate::daemon::AppState::wire_pty(store.clone(), &nexus_common::Config::default());
    let supervisor = state.pty_supervisor().unwrap();
    let wiring = crate::daemon::app::LoopWiring {
        store: store.clone(),
        bell: Bell::new(),
        registry: nexus_dispatch::AgentRegistry::new(),
        events: Arc::new(DiscardClaudeDisplay),
        turn_exec: state.agent.clone(),
        gateway_stream: None,
        drain_limit: 100,
        preview_chars: 100,
        spawned: Default::default(),
        shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        native_forwarders: Default::default(),
        raw_stream_writers: Default::default(),
        presence: state.presence.clone(),
    };
    let session = SessionId(format!("fixture-{}", uuid::Uuid::new_v4()));
    let args = ["--resume".into(), "native".into()];
    let old = supervisor
        .capture_claude_binding(&session, HeadedRuntimeKind::ClaudeNative, &args)
        .unwrap();
    let transition = wiring.store.lock_presence_transition().await;
    assert!(wiring.claim_native_forwarder_for_owner(
        "claude",
        &session,
        old.owner_id(),
        &transition
    ));
    // OLD is parked after the real reservation and before attachment. NEW must replace it.
    let new = supervisor
        .capture_claude_binding(&session, HeadedRuntimeKind::ClaudeNative, &args)
        .unwrap();
    assert!(
        wiring.claim_native_forwarder_for_owner("claude", &session, new.owner_id(), &transition),
        "a stale reserved slot must not strand NEW without an observer"
    );
    drop(transition);
    let dir = temp_test_dir("forwarder-claim");
    let paths = ClaudeNativeBridgePaths::new(&dir, &session);
    std::fs::create_dir_all(&paths.bridge_dir).unwrap();
    std::fs::write(&paths.hook_log_path, "").unwrap();
    new.attach_hook_source(paths.hook_log_path.clone());
    ClaudeRuntimeStateRepo::new(&store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: session.clone(),
            bridge_dir: paths.bridge_dir.clone(),
            claude_session_id: Some("native".into()),
            launch_cwd: dir,
            transcript_path: Some(paths.bridge_dir.join("transcript.jsonl")),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    let handle = spawn_claude_native_forwarder_with_tool_events(
        store,
        session.clone(),
        paths.clone(),
        Arc::new(DiscardClaudeDisplay),
        Bell::new(),
        5,
        None,
        Some(new.clone()),
    );
    let live = handle.abort_handle();
    wiring.track_native_forwarder_for_owner("claude", &session, new.owner_id(), handle);
    let obsolete = tokio::spawn(std::future::pending());
    let obsolete_status = obsolete.abort_handle();
    wiring.track_native_forwarder_for_owner("claude", &session, old.owner_id(), obsolete);
    wiring.release_native_forwarder_for_owner("claude", &session, old.owner_id());
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(paths.hook_log_path)
        .unwrap();
    writeln!(file, "{{\"event\":\"UserPromptSubmit\",\"session_id\":\"native\",\"prompt_id\":\"A\",\"prompt\":\"manual\"}}").unwrap();
    new.wait_for_submission_after(0, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(new.has_open_turn());
    assert!(
        !live.is_finished(),
        "OLD cleanup must not abort NEW's native forwarder"
    );
    assert!(
        obsolete_status.is_finished(),
        "late OLD attachment is aborted"
    );
    writeln!(
        file,
        "{{\"event\":\"Stop\",\"session_id\":\"native\",\"prompt_id\":\"A\"}}"
    )
    .unwrap();
    new.wait_after(0, Duration::from_secs(1)).await.unwrap();
    assert!(!new.has_open_turn());
    wiring.release_native_forwarder_for_owner("claude", &session, new.owner_id());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_raw_and_tmux_claude_wrappers_require_fresh_native_receipt_and_matching_terminal() {
    use nexus_harness_claude::native::forwarder::forward_once_with_observations;
    use std::io::Write;
    for (backend, prompt_ids) in [
        ("raw", true),
        ("tmux", true),
        ("raw", false),
        ("tmux", false),
    ] {
        let dir = temp_test_dir(backend);
        let session = SessionId(format!("fixture-{}", uuid::Uuid::new_v4()));
        let paths = ClaudeNativeBridgePaths::new(&dir, &session);
        std::fs::create_dir_all(&paths.bridge_dir).unwrap();
        let historical = "{\"event\":\"UserPromptSubmit\",\"session_id\":\"native\",\"prompt_id\":\"historical\",\"prompt\":\"same\"}\n";
        std::fs::write(&paths.hook_log_path, historical).unwrap();
        let completion = Arc::new(ClaudeTurnCompletion::new(
            Some("native".into()),
            Some(paths.hook_log_path.clone()),
        ));
        // Disposable shell paints a readiness glyph. It is not Claude or provider proof.
        let script = r"while :; do printf '\033[2J\033[H❯\n'; sleep 0.05; done";
        let mut processes = ClaudeFixtureProcesses::default();
        let native: Arc<dyn HarnessInput> = if backend == "raw" {
            let mut command = CommandBuilder::new("sh");
            command.args(["-c", script]);
            let pty = Arc::new(
                PtySession::spawn(
                    command,
                    PtySize {
                        rows: 24,
                        cols: 80,
                        pixel_width: 0,
                        pixel_height: 0,
                    },
                )
                .unwrap(),
            );
            let terminal = ScreenModelBackend::wrap(pty.clone());
            processes.raw = Some(pty.clone());
            Arc::new(ClaudeRawPtyInput {
                input: pty,
                terminal,
                completion: completion.clone(),
            })
        } else {
            let harness = Arc::new(
                TmuxHarness::launch(
                    &session.0,
                    "sh",
                    &["-c".into(), script.into()],
                    dir.to_str().unwrap(),
                    80,
                    24,
                )
                .unwrap(),
            );
            processes.tmux = Some(harness.clone());
            harness
        };
        let writer = Arc::new(WrittenHarness {
            input: native,
            written: tokio::sync::Semaphore::new(0),
        });
        let input = Arc::new(ClaudeNativeHarness {
            input: writer.clone(),
            completion: completion.clone(),
        });
        let observer = Arc::new(CountAcceptance::default());
        let mut call = {
            let input = input.clone();
            let observer = observer.clone();
            tokio::spawn(async move { input.send_turn_observed("same", observer).await })
        };
        tokio::time::timeout(Duration::from_secs(15), writer.written.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
        store.migrate().await.unwrap();
        ClaudeRuntimeStateRepo::new(&store)
            .upsert_launch(ClaudeRuntimeLaunch {
                runtime_id: session.clone(),
                bridge_dir: paths.bridge_dir.clone(),
                claude_session_id: Some("native".into()),
                launch_cwd: dir.clone(),
                transcript_path: Some(paths.bridge_dir.join("transcript.jsonl")),
                bridge_pid: None,
                hook_pids_json: None,
            })
            .await
            .unwrap();
        let mut hooks = std::fs::OpenOptions::new()
            .append(true)
            .open(&paths.hook_log_path)
            .unwrap();
        writeln!(
            hooks,
            "{{\"event\":\"Stop\",\"session_id\":\"native\",\"prompt_id\":\"A\"}}"
        )
        .unwrap();
        forward_once_with_observations(
            store.clone(),
            session.clone(),
            paths.clone(),
            Arc::new(DiscardClaudeDisplay),
            None,
            Some(completion.clone()),
        )
        .await
        .unwrap();
        assert_eq!(
            observer.count.load(Ordering::SeqCst),
            0,
            "{backend}: historical same-text receipt is not this write"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut call)
                .await
                .is_err(),
            "{backend}: terminal-only evidence cannot settle the wrapper"
        );
        let mut submit = serde_json::json!({"event": "UserPromptSubmit", "session_id": "native", "prompt_id": "A", "prompt": "same"});
        let mut stop =
            serde_json::json!({"event": "Stop", "session_id": "native", "prompt_id": "A"});
        if !prompt_ids {
            submit.as_object_mut().unwrap().remove("prompt_id");
            stop.as_object_mut().unwrap().remove("prompt_id");
        }
        writeln!(hooks, "{submit}\n{stop}").unwrap();
        forward_once_with_observations(
            store,
            session.clone(),
            paths,
            Arc::new(DiscardClaudeDisplay),
            None,
            Some(completion.clone()),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut call)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(observer.count.load(Ordering::SeqCst), 1);
        let transport = PtyTransport::default();
        transport.bind(session.clone(), input);
        let new = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
        transport.bind(
            session,
            Arc::new(ClaudeNativeHarness {
                input: writer,
                completion: new.clone(),
            }),
        );
        assert!(!completion.is_current());
        assert!(new.is_current());
        assert_ne!(completion.owner_id(), new.owner_id());
        drop(processes);
    }
}

#[test]
fn merge_claude_trust_adds_cwd_and_preserves_other_state() {
    let existing = serde_json::json!({
        "someTopLevel": 42,
        "projects": { "/home/x": { "hasTrustDialogAccepted": true, "other": 1 } }
    });
    let out = merge_claude_trust(existing, "/home/agent/ada");
    // Untouched: top-level state + the other project's entry.
    assert_eq!(out["someTopLevel"], 42);
    assert_eq!(out["projects"]["/home/x"]["other"], 1);
    assert_eq!(out["projects"]["/home/x"]["hasTrustDialogAccepted"], true);
    // Added: the new cwd is trusted + onboarded.
    assert_eq!(
        out["projects"]["/home/agent/ada"]["hasTrustDialogAccepted"],
        true
    );
    assert_eq!(
        out["projects"]["/home/agent/ada"]["hasCompletedProjectOnboarding"],
        true
    );
}

#[test]
fn claude_user_config_path_honors_custom_config_dir() {
    assert_eq!(
        claude_user_config_path(Some("/tmp/claude-config"), Some("/home/agent")),
        PathBuf::from("/tmp/claude-config/.claude.json")
    );
    assert_eq!(
        claude_user_config_path(None, Some("/home/agent")),
        PathBuf::from("/home/agent/.claude.json")
    );
}

#[test]
fn claude_headed_env_re_exports_custom_config_after_tmux_sanitization() {
    let mut env = vec![("NEXUS_PROJECT".to_string(), "matrix".to_string())];
    append_claude_config_env_from(
        &mut env,
        HeadedRuntimeKind::ClaudeNative,
        Some("/tmp/claude-config"),
    );
    assert!(env
        .iter()
        .any(|(key, value)| { key == "CLAUDE_CONFIG_DIR" && value == "/tmp/claude-config" }));

    let mut non_claude = Vec::new();
    append_claude_config_env_from(
        &mut non_claude,
        HeadedRuntimeKind::CodexAppServer,
        Some("/tmp/claude-config"),
    );
    assert!(non_claude.is_empty());
}

#[test]
fn hermes_machine_home_collapses_runtime_profile_to_global_root() {
    let fixture = tempfile::tempdir().unwrap();
    let home = fixture.path().join("operator");
    let profile = home.join(".hermes/profiles/nexus-s_agent");
    assert_eq!(
        resolve_machine_hermes_home(None, Some(&profile), Some(&home)).unwrap(),
        home.join(".hermes")
    );

    let custom_root = fixture.path().join("hermes-data");
    let custom_profile = custom_root.join("profiles/nexus-s_agent");
    assert_eq!(
        resolve_machine_hermes_home(None, Some(&custom_profile), Some(&home)).unwrap(),
        custom_root
    );
}

#[test]
fn hermes_machine_home_preserves_explicit_root_and_home_default() {
    let fixture = tempfile::tempdir().unwrap();
    let home = fixture.path().join("operator");
    let explicit = fixture.path().join("hermes-data");
    assert_eq!(
        resolve_machine_hermes_home(None, Some(&explicit), Some(&home)).unwrap(),
        explicit
    );
    assert_eq!(
        resolve_machine_hermes_home(None, None, Some(&home)).unwrap(),
        home.join(".hermes")
    );
}

#[test]
fn harness_program_maps_each_tui_binary() {
    let platform = NativeProcessPlatform::current();
    assert_eq!(
        harness_program(&hid("claude")),
        native_harness_program("claude", platform)
    );
    assert_eq!(
        harness_program(&hid("codex")),
        native_harness_program("codex", platform)
    );
    assert_eq!(
        harness_program(&hid("opencode")),
        native_harness_program("opencode", platform)
    );
    assert_eq!(
        harness_program(&hid("hermes")),
        native_harness_program("hermes", platform)
    );
    assert_eq!(harness_program(&hid("pi")), None);
    assert_eq!(harness_program(&hid("other")), None);
}

#[test]
fn headed_harness_profiles_route_runtime_identity() {
    assert_eq!(harness_agent_token(&hid("claude")), "claude");
    assert_eq!(
        headed_runtime_kind(&hid("claude")),
        HeadedRuntimeKind::ClaudeNative
    );
    assert_eq!(harness_agent_token(&hid("codex")), "codex");
    assert_eq!(
        headed_runtime_kind(&hid("codex")),
        HeadedRuntimeKind::CodexAppServer
    );
    assert_eq!(harness_agent_token(&hid("opencode")), "opencode");
    assert_eq!(
        headed_runtime_kind(&hid("opencode")),
        HeadedRuntimeKind::OpenCodePlugin
    );
    assert_eq!(harness_agent_token(&hid("hermes")), "hermes");
    assert_eq!(
        headed_runtime_kind(&hid("hermes")),
        HeadedRuntimeKind::HermesGateway
    );
    assert_eq!(harness_agent_token(&hid("pi")), "pi");
    assert_eq!(headed_runtime_kind(&hid("pi")), HeadedRuntimeKind::Screen);
    assert_eq!(harness_agent_token(&hid("other")), "other");
    assert_eq!(
        headed_runtime_kind(&hid("other")),
        HeadedRuntimeKind::Screen
    );
}

#[test]
fn opencode_binary_preflight_reports_missing_path() {
    let err = resolve_opencode_executable(None, Some(""))
        .expect_err("empty PATH should not resolve opencode");
    assert!(err.contains("OpenCode executable"));
    assert!(err.contains("NEXUS_OPENCODE_BIN"));
}

#[test]
fn opencode_binary_preflight_resolves_path_and_override() {
    let tmp = tempfile::tempdir().unwrap();
    let bin = tmp.path().join(if cfg!(windows) {
        "opencode.exe"
    } else {
        "opencode"
    });
    std::fs::write(&bin, "fixture").unwrap();
    let path_env = tmp.path().to_string_lossy();

    assert_eq!(
        resolve_opencode_executable(None, Some(&path_env)).unwrap(),
        bin.to_string_lossy()
    );
    assert_eq!(
        resolve_opencode_executable(Some("opencode"), Some(&path_env)).unwrap(),
        bin.to_string_lossy()
    );
    assert_eq!(
        resolve_opencode_executable(Some(bin.to_string_lossy().as_ref()), Some("")).unwrap(),
        bin.to_string_lossy()
    );
}

#[test]
fn opencode_launch_env_reaches_generated_shim_without_ambient_cli() {
    // Other harness fixtures temporarily replace PATH. Hold the shared guard while
    // capturing and executing Node so this test cannot inherit their toolchain.
    let _env = crate::cli::ambient::TestEnvGuard::new(&[]);
    for (opt_outs, expected_skip) in [
        ([false, false], ""),
        ([true, false], "NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL"),
        ([false, true], "NEXUS_SKIP_AGENT_HOOK_INSTALL"),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let session = SessionId("s_launch_env".into());
        let files = write_opencode_plugin_files(tmp.path(), &session).unwrap();
        let mut env = opencode_plugin_env(
            &session,
            "a_exact",
            Some("exact"),
            "project",
            "fixture-key",
            "/captured daemon/nexus",
            opt_outs,
        );
        assert_eq!(
            env.iter()
                .find(|(key, _)| key == "NEXUS_CLI")
                .map(|(_, value)| value.as_str()),
            Some("/captured daemon/nexus")
        );
        env.extend([
            (
                "NEXUS_OPENCODE_PLUGIN_PATH".into(),
                files.plugin_path.to_string_lossy().into_owned(),
            ),
            (
                "NEXUS_OPENCODE_HOME".into(),
                tmp.path().join("home").to_string_lossy().into_owned(),
            ),
            ("NEXUS_OPENCODE_BIN".into(), "fixture-native".into()),
        ]);
        let driver = tmp.path().join("launch-env.mjs");
        std::fs::write(&driver, r#"
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
globalThis.capture=(_bin,_args,options)=>{
 const expected=process.argv[3], config=JSON.parse(options.env.OPENCODE_CONFIG_CONTENT);
 for(const key of ['NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL','NEXUS_SKIP_AGENT_HOOK_INSTALL'])
   assert.equal(options.env[key],key===expected?'1':'0','captured opt-out survives launch');
 if(expected) assert.equal(config.mcp?.['nexus-bus'],undefined,'explicit opt-out does not install inline MCP');
 else assert.deepEqual(config.mcp['nexus-bus'].command,['/captured daemon/nexus','mcp','--as','exact','--project','project','--client-key','fixture-key','--agent','opencode']);
 process.exit(0);
};
const source=readFileSync(process.argv[2],'utf8').replace('import { spawn } from "node:child_process";','const spawn=globalThis.capture;');
await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
"#).unwrap();
        let args = vec![
            driver.to_string_lossy().into_owned(),
            files.serve_path.to_string_lossy().into_owned(),
            expected_skip.into(),
        ];
        let mut raw = std::process::Command::new("node");
        raw.env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .envs(std::env::vars_os().filter(|(name, _)| {
                cfg!(windows) && name.to_string_lossy().eq_ignore_ascii_case("SystemRoot")
            }))
            .env("NEXUS_CLI", "ambient-wrong")
            .env("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL", "1")
            .env("NEXUS_SKIP_AGENT_HOOK_INSTALL", "1")
            .envs(env.iter().cloned())
            .args(&args);
        let output = raw.output().unwrap();
        assert!(
            output.status.success(),
            "raw: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        #[cfg(unix)]
        {
            let shell = nexus_pty::tmux_launch_shell_command(
                "node",
                &args,
                tmp.path().to_str().unwrap(),
                &env,
            );
            let output = std::process::Command::new("/bin/sh")
                .args(["-c", &shell])
                .env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap_or_default())
                .env("NEXUS_CLI", "ambient-wrong")
                .env("NEXUS_CLIENT_KEY", "wrong-key")
                .env("NEXUS_SKIP_AGENT_BOOTSTRAP_INSTALL", "1")
                .env("NEXUS_SKIP_AGENT_HOOK_INSTALL", "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "tmux shell: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}

#[test]
fn codex_appserver_identity_is_runtime_key_backed_for_mcp_and_shell() {
    let session = SessionId("s_codex_runtime".into());
    let bus = codex_appserver_bus_mcp("/usr/bin/nexus", "ada", "lens", "ck_secret");
    assert_eq!(bus.command, "/usr/bin/nexus");
    assert!(bus
        .args
        .windows(2)
        .any(|w| w[0] == "--client-key" && w[1] == "ck_secret"));
    assert!(bus
        .args
        .windows(2)
        .any(|w| w[0] == "--agent" && w[1] == "codex"));

    assert!(
        !bus.args.iter().any(|arg| arg == "--socket"),
        "store-backed MCP must not carry a daemon socket arg: {:?}",
        bus.args
    );

    let env = codex_appserver_env(
        &session,
        "a_s_codex_runtime",
        Some("ada"),
        "lens",
        "ck_secret",
        "/opt/nexus/bin/nexus",
    );
    assert!(env
        .iter()
        .any(|(k, v)| k == "NEXUS_SESSION_ID" && v == "s_codex_runtime"));
    assert!(env
        .iter()
        .any(|(k, v)| k == "NEXUS_AGENT_ID" && v == "a_s_codex_runtime"));
    assert!(env.iter().any(|(k, v)| k == "NEXUS_NAME" && v == "ada"));
    assert!(env
        .iter()
        .any(|(k, v)| k == "NEXUS_CLIENT_KEY" && v == "ck_secret"));
    assert!(env.iter().any(|(k, v)| k == "NEXUS_AGENT" && v == "codex"));
    assert!(env
        .iter()
        .any(|(k, v)| k == "NEXUS_CLI" && v == "/opt/nexus/bin/nexus"));
    assert!(env
        .iter()
        .any(|(k, v)| k == "PATH" && v.starts_with("/opt/nexus/bin")));
}

#[test]
fn nexus_runtime_env_exports_stable_session_id() {
    let env = nexus_runtime_env(
        &SessionId("s_stable_runtime".into()),
        "a_s_stable_runtime",
        None,
        "lens",
        "ck_secret",
        "codex",
    );
    assert!(env
        .iter()
        .any(|(k, v)| k == "NEXUS_SESSION_ID" && v == "s_stable_runtime"));
    assert!(env
        .iter()
        .any(|(k, v)| k == "NEXUS_AGENT_ID" && v == "a_s_stable_runtime"));
    assert!(env
        .iter()
        .any(|(k, v)| k == "NEXUS_CLIENT_KEY" && v == "ck_secret"));
    assert!(env.iter().any(|(k, v)| k == "NEXUS_PROJECT" && v == "lens"));
    assert!(env.iter().any(|(k, v)| k == "NEXUS_AGENT" && v == "codex"));
    assert!(env.iter().any(|(k, v)| k == "NEXUS_TIER" && v.is_empty()));
    assert!(
        !env.iter().any(|(k, _)| k == "NEXUS_NAME"),
        "staged launch env must not fabricate NEXUS_NAME: {env:?}"
    );
}

#[test]
fn nexus_runtime_env_preserves_only_daemon_ipc_coordinates() {
    let _env = crate::cli::ambient::TestEnvGuard::new(&[
        ("NEXUS_HOME", Some("/tmp/nexus-home")),
        ("NEXUS_DB_URL", Some("http://127.0.0.1:8086")),
        ("NEXUS_DB_AUTH_TOKEN", Some("db-secret")),
        ("NEXUS_STREAM_DB_PATH", Some("/dev/shm/nexus-stream.db")),
        ("NEXUS_NO_AUTOSTART", Some("1")),
        ("TOKIO_WORKER_THREADS", Some("2")),
    ]);
    let env = nexus_runtime_env(
        &SessionId("s_transport".into()),
        "a_s_transport",
        Some("ada"),
        "matrix",
        "ck_transport",
        "opencode",
    );

    for (key, value) in [
        ("NEXUS_HOME", "/tmp/nexus-home"),
        ("NEXUS_NO_AUTOSTART", "1"),
        ("TOKIO_WORKER_THREADS", "2"),
    ] {
        assert!(
            env.iter()
                .any(|(got_key, got_value)| { got_key == key && got_value == value }),
            "missing {key}={value:?} from headed runtime env"
        );
    }
    for key in [
        "NEXUS_DB_URL",
        "NEXUS_DB_AUTH_TOKEN",
        "NEXUS_STREAM_DB_PATH",
    ] {
        assert!(
            !env.iter().any(|(got_key, _)| got_key == key),
            "legacy direct-store coordinate leaked into headed runtime: {key}"
        );
    }
}

#[test]
fn command_builder_scrubs_inherited_nexus_identity_env() {
    let _env = crate::cli::ambient::TestEnvGuard::new(&[
        ("NEXUS_NAME", Some("remy")),
        ("NEXUS_CLIENT_KEY", Some("wrong-key")),
        ("NEXUS_AGENT_ID", Some("a_wrong")),
        ("NEXUS_SESSION_ID", Some("s_wrong")),
        ("NEXUS_TIER", Some("admin")),
        ("CLAUDE_CODE_SESSION_ID", Some("claude-wrong")),
    ]);
    let mut cmd = CommandBuilder::new("codex");
    scrub_inherited_nexus_identity_env(&mut cmd);

    for key in [
        "NEXUS_NAME",
        "NEXUS_CLIENT_KEY",
        "NEXUS_AGENT_ID",
        "NEXUS_SESSION_ID",
        "NEXUS_TIER",
        "CLAUDE_CODE_SESSION_ID",
    ] {
        assert!(cmd.get_env(key).is_none(), "{key} leaked into raw pty env");
    }
}

#[tokio::test]
async fn claude_native_runtime_writes_session_identity_manifest() {
    let supervisor = PtySupervisor::new();
    let session = SessionId("s_claude_identity".into());
    let state_dir = temp_test_dir("claude-identity");

    let paths = supervisor
        .prepare_claude_native_runtime(
            &session,
            "violet",
            Some("violet"),
            "default",
            "ck_violet",
            "/usr/bin/nexus",
            "/tmp/violet",
            &state_dir,
            None,
        )
        .await
        .unwrap();

    let body = std::fs::read_to_string(paths.identity_path).unwrap();
    assert!(body.contains("NEXUS_BRIDGE_NAME='violet'"));
    assert!(body.contains("NEXUS_BRIDGE_SESSION_ID='s_claude_identity'"));
}

#[tokio::test]
async fn codex_bridge_options_carry_runtime_store() {
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let supervisor = PtySupervisor::with_runtime_store(store.clone());

    let opts = supervisor.codex_bridge_options("ada", None, Vec::new(), None, false);

    assert!(opts
        .runtime_store
        .as_ref()
        .is_some_and(|got| Arc::ptr_eq(got, &store)));
    assert!(
        opts.create_thread_if_missing,
        "fresh headed Codex must be injectable before the operator types in the TUI"
    );
}

#[tokio::test]
async fn codex_bridge_options_carry_gateway_tool_observation_sink() {
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let publisher = GatewayStreamPublisher::new(16);
    let mut rx = publisher.subscribe();
    let supervisor = PtySupervisor::with_runtime_store_and_gateway_stream(store, Some(publisher));

    let opts = supervisor.codex_bridge_options("ada", None, Vec::new(), None, false);
    let sink = opts
        .tool_observations
        .as_ref()
        .expect("Codex bridge options should carry a tool observation sink");

    sink.publish_tool_call(
        &SessionId("s_codex_tool".into()),
        ToolCallObservation {
            tool_call_id: Some("cmd1".to_string()),
            tool: "Bash".to_string(),
            phase: nexus_transcript::ToolCallPhase::Pre,
            ok: true,
        },
    );

    let frame = rx.try_recv().expect("gateway stream frame should publish");
    match frame {
        crate::daemon::gateway_stream_socket::GatewayStreamFrame::DeveloperEvent {
            session_id,
            event,
        } => {
            assert_eq!(session_id, "s_codex_tool");
            assert_eq!(event.agent.as_deref(), Some("ada"));
            assert_eq!(
                event.session_id.as_ref().map(|id| id.0.as_str()),
                Some("s_codex_tool")
            );
            assert_eq!(event.tool.as_deref(), Some("Bash"));
        }
        other => panic!("expected developer.event frame, got {other:?}"),
    }
}

#[tokio::test]
async fn persist_codex_tmux_state_updates_runtime_state() {
    let store = Arc::new(nexus_store::Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let supervisor = PtySupervisor::with_runtime_store(store.clone());
    let session = SessionId("s_codex_tmux".into());
    let repo = nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&store);
    repo.upsert_launch(nexus_harness_codex::storage::CodexRuntimeLaunch {
        runtime_id: session.clone(),
        codex_thread_id: None,
        codex_home: std::path::PathBuf::from("/tmp/codex-home"),
        app_server_sock: std::path::PathBuf::from("/tmp/codex.sock"),
        app_server_pid: None,
        mcp_sidecar_pids_json: None,
        app_server_adopted: true,
    })
    .await
    .unwrap();

    supervisor
        .persist_codex_tmux_state(&session, Some("/tmp/tmux.sock"), "nexus-s_codex_tmux")
        .await
        .unwrap();

    let state = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(state.tmux_socket.as_deref(), Some("/tmp/tmux.sock"));
    assert_eq!(state.tmux_session.as_deref(), Some("nexus-s_codex_tmux"));
}

#[tokio::test]
async fn launch_spawns_binds_and_routes_inject_through_the_pty() {
    // The platform's stdin pager echoes PTY input back out as an offline harness stand-in.
    let supervisor = PtySupervisor::new();
    let session = SessionId("s_cat_launch".into());
    let pty = supervisor
        .launch(
            &session,
            echoing_pty_program(),
            "ada",
            Some("ada"),
            "default",
            "nexus_ck_cat",
            "/usr/bin/nexus",
            "/tmp",
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .unwrap();

    let mut out = pty.subscribe();

    let batch = NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: vec![],
        threads: vec![],
        dm_message_ids: vec![],
        thread_message_ids: vec![],
        message_ids: vec![],
    };
    // The cloned transport is bound to the same PTY, so inject reaches it.
    supervisor
        .transport()
        .inject_turn(&session, &batch)
        .await
        .unwrap();

    let saw_envelope = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut buf = Vec::new();
        while let Ok(c) = out.recv().await {
            buf.extend_from_slice(&c);
            if String::from_utf8_lossy(&buf).contains("nexus-batch") {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    assert!(
        saw_envelope,
        "inject_turn must write the rendered <nexus-batch> into the bound PTY"
    );
}

fn temp_test_dir(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-pty-supervisor-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn launch_stores_raw_runtime_behind_pty_backend_handle() {
    let supervisor = PtySupervisor::new();
    let session = SessionId("s_cat_backend_handle".into());
    supervisor
        .launch(
            &session,
            echoing_pty_program(),
            "ada",
            Some("ada"),
            "default",
            "nexus_ck_cat",
            "/usr/bin/nexus",
            "/tmp",
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(supervisor.pty_backend_kind(&session), Some("raw"));
    assert!(
        supervisor.pty_output(&session).is_some(),
        "raw PTY backend handle should remain available through the backend map"
    );

    let mut out = supervisor
        .pty_output(&session)
        .expect("raw backend handle should expose output");
    let batch = NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: vec![],
        threads: vec![],
        dm_message_ids: vec![],
        thread_message_ids: vec![],
        message_ids: vec![],
    };
    supervisor
        .transport()
        .inject_turn(&session, &batch)
        .await
        .unwrap();

    let saw_envelope = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut buf = Vec::new();
        while let Ok(c) = out.recv().await {
            buf.extend_from_slice(&c);
            if String::from_utf8_lossy(&buf).contains("nexus-batch") {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);

    assert!(saw_envelope, "backend output should expose PTY bytes");
}

#[tokio::test]
async fn raw_pty_bind_publishes_endpoint_manifest_and_kill_removes_it() {
    use crate::daemon::terminal_socket::{
        read_terminal_endpoint_manifest, terminal_endpoint_manifest_path,
    };

    let dir = std::env::temp_dir().join(format!("nexus-manifest-test-{}", std::process::id()));
    let dir_s = dir.display().to_string();
    let _env = crate::cli::ambient::TestEnvGuard::new(&[(
        "NEXUS_TERMINAL_MANIFEST_DIR",
        Some(dir_s.as_str()),
    )]);

    let supervisor = PtySupervisor::new();
    let session = SessionId("s_manifest_cat".into());
    supervisor
        .launch(
            &session,
            echoing_pty_program(),
            "ada",
            Some("ada"),
            "default",
            "nexus_ck_manifest",
            "/usr/bin/nexus",
            "/tmp",
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .unwrap();

    let manifest = read_terminal_endpoint_manifest(&session)
        .expect("raw PTY bind must publish an endpoint manifest for local attach tooling");
    let live = supervisor.terminal_endpoint(&session).unwrap();
    assert_eq!(
        manifest.session_id, session,
        "manifest session id must identify the requested runtime"
    );
    assert_eq!(
        live.session_id, session,
        "live endpoint session id must identify the bound runtime"
    );
    assert_eq!(
        manifest.path, live.path,
        "manifest path mirrors the live endpoint"
    );
    assert_eq!(
        manifest.token, live.token,
        "manifest token mirrors the live endpoint"
    );

    supervisor.kill(&session);
    assert!(
        !terminal_endpoint_manifest_path(&session).exists(),
        "kill must remove the endpoint manifest so attach fails loud, not stale"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn terminal_endpoint_manifest_rejects_wrong_session_id() {
    use crate::daemon::terminal_socket::{
        read_terminal_endpoint_manifest, terminal_endpoint_manifest_path,
    };

    let dir = std::env::temp_dir().join(format!("nexus-manifest-mismatch-{}", std::process::id()));
    let dir_s = dir.display().to_string();
    let _env = crate::cli::ambient::TestEnvGuard::new(&[(
        "NEXUS_TERMINAL_MANIFEST_DIR",
        Some(dir_s.as_str()),
    )]);
    let session = SessionId("s_manifest_expected".into());
    let path = terminal_endpoint_manifest_path(&session);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        serde_json::json!({
            "session_id": "s_manifest_other",
            "path": "/tmp/nexus-terminal-wrong.sock",
            "token": "token"
        })
        .to_string(),
    )
    .unwrap();

    assert!(
        read_terminal_endpoint_manifest(&session).is_none(),
        "manifest reader must reject a manifest whose embedded session id does not match"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[tokio::test]
async fn launch_exposes_terminal_socket_for_raw_pty_runtime() {
    use crate::daemon::terminal_socket::{
        read_terminal_frame, write_terminal_frame, TerminalFrame,
    };
    use tokio::net::UnixStream;

    let supervisor = PtySupervisor::new();
    let session = SessionId("s_terminal_cat".into());
    supervisor
        .launch(
            &session,
            echoing_pty_program(),
            "ada",
            Some("ada"),
            "default",
            "nexus_ck_cat",
            "/usr/bin/nexus",
            "/tmp",
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .unwrap();

    let endpoint = supervisor
        .terminal_endpoint(&session)
        .expect("raw PTY launch should expose terminal endpoint");
    let mut stream = UnixStream::connect(&endpoint.path).await.unwrap();
    write_terminal_frame(&mut stream, TerminalFrame::Auth(endpoint.token.clone()))
        .await
        .unwrap();
    let hello = read_terminal_frame(&mut stream).await.unwrap();
    assert!(
        matches!(hello, TerminalFrame::Hello { ref session_id } if session_id == &session.0),
        "terminal socket must identify the session before raw bytes flow, got {hello:?}"
    );
    write_terminal_frame(
        &mut stream,
        TerminalFrame::Input(b"socket-terminal\n".to_vec()),
    )
    .await
    .unwrap();

    let saw_echo = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if let TerminalFrame::Output(bytes) = read_terminal_frame(&mut stream).await.unwrap() {
                if String::from_utf8_lossy(&bytes).contains("socket-terminal") {
                    return true;
                }
            }
        }
    })
    .await
    .unwrap_or(false);

    assert!(saw_echo, "terminal socket must pump backend output");
    write_terminal_frame(
        &mut stream,
        TerminalFrame::Resize {
            cols: 100,
            rows: 30,
        },
    )
    .await
    .unwrap();
}

#[test]
fn terminal_endpoint_is_absent_without_a_pty_backend() {
    let supervisor = PtySupervisor::new();
    let session = SessionId("s_headless".into());

    assert!(
        supervisor.terminal_endpoint(&session).is_none(),
        "headless/ACP-style sessions must not expose terminal endpoints"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn terminal_socket_rejects_bad_auth_token() {
    use crate::daemon::terminal_socket::{
        read_terminal_frame, write_terminal_frame, TerminalFrame,
    };
    use tokio::net::UnixStream;

    let supervisor = PtySupervisor::new();
    let session = SessionId("s_terminal_auth".into());
    supervisor
        .launch(
            &session,
            "cat",
            "ada",
            Some("ada"),
            "default",
            "nexus_ck_cat",
            "/usr/bin/nexus",
            "/tmp",
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .unwrap();

    let endpoint = supervisor
        .terminal_endpoint(&session)
        .expect("raw PTY launch should expose terminal endpoint");
    let mut stream = UnixStream::connect(&endpoint.path).await.unwrap();
    write_terminal_frame(&mut stream, TerminalFrame::Auth("wrong".into()))
        .await
        .unwrap();

    let frame = read_terminal_frame(&mut stream).await.unwrap();
    assert!(
        matches!(frame, TerminalFrame::Error(ref msg) if msg.contains("unauthorized")),
        "bad token should get an unauthorized error frame, got {frame:?}"
    );
}

#[tokio::test]
async fn kill_removes_terminal_socket_endpoint() {
    let supervisor = PtySupervisor::new();
    let session = SessionId("s_terminal_kill".into());
    supervisor
        .launch(
            &session,
            echoing_pty_program(),
            "ada",
            Some("ada"),
            "default",
            "nexus_ck_cat",
            "/usr/bin/nexus",
            "/tmp",
            PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .await
        .unwrap();

    let endpoint = supervisor
        .terminal_endpoint(&session)
        .expect("raw PTY launch should expose terminal endpoint");
    #[cfg(unix)]
    assert!(endpoint.path.exists());
    #[cfg(windows)]
    assert!(endpoint.path.to_string_lossy().starts_with(r"\\.\pipe\"));
    assert_eq!(supervisor.pty_backend_kind(&session), Some("raw"));

    assert!(supervisor.kill(&session));

    assert!(supervisor.terminal_endpoint(&session).is_none());
    assert_eq!(supervisor.pty_backend_kind(&session), None);
    #[cfg(unix)]
    assert!(
        !endpoint.path.exists(),
        "terminal socket file should be removed when the runtime is killed"
    );
}

/// A validated [`nexus_contracts::HarnessId`] from a literal (panics on invalid — test-only).
fn hid(s: &str) -> nexus_contracts::HarnessId {
    nexus_contracts::HarnessId::new(s).expect("valid harness id literal")
}

/// A program that prints one line to its terminal and exits non-zero — a stand-in for the
/// OpenCode serve shim crashing during boot.
fn crashing_pty_command(message: &str) -> CommandBuilder {
    #[cfg(windows)]
    {
        let mut cmd = CommandBuilder::new("cmd.exe");
        cmd.args(["/D", "/C", &format!("echo {message}& exit 1")]);
        cmd
    }
    #[cfg(not(windows))]
    {
        let mut cmd = CommandBuilder::new("sh");
        cmd.args(["-c", &format!("echo '{message}'; exit 1")]);
        cmd
    }
}

#[tokio::test]
async fn raw_opencode_ready_wait_reports_last_screen_output_when_shim_exits() {
    let size = PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    };
    let pty =
        Arc::new(PtySession::spawn(crashing_pty_command("serve boom: ENOENT"), size).unwrap());
    let screen = ScreenModelBackend::wrap(pty.clone());
    let ready_path = std::env::temp_dir().join(format!(
        "nexus-opencode-ready-never-{}.json",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&ready_path);

    let err = wait_for_opencode_plugin_ready_raw(
        pty.as_ref(),
        screen.as_ref(),
        &ready_path,
        Duration::from_secs(10),
    )
    .await
    .expect_err("a shim that exits before writing the ready file must fail the launch");

    assert!(
        err.starts_with(
            "raw PTY exited before OpenCode plugin reported ready; last screen output: "
        ),
        "{err}"
    );
    assert!(
        err.contains("serve boom: ENOENT"),
        "the operator-facing error must carry what the shim printed: {err}"
    );
}

#[test]
fn last_screen_output_is_omitted_when_the_screen_is_blank() {
    assert_eq!(
        with_last_screen_output("raw PTY exited", "  \n \n"),
        "raw PTY exited"
    );
    assert_eq!(
        with_last_screen_output("raw PTY exited", "\n  boom  \n"),
        "raw PTY exited; last screen output: boom"
    );
}

// Actual managed AppState + Agent + coordinator, with an inert metadata-emitting adapter.
// These are root ownership gates, not native executable/decoder evidence.
struct RootModelAdapter {
    carrier: Mutex<Option<nexus_agent::adapter::AdapterModelReporting>>,
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    park: std::sync::atomic::AtomicBool,
    missing_root: std::sync::atomic::AtomicBool,
}

impl RootModelAdapter {
    fn carrier(&self) -> nexus_agent::adapter::AdapterModelReporting {
        self.carrier
            .lock()
            .unwrap()
            .clone()
            .expect("actual root injects captured reporting")
    }
    fn update() -> nexus_contracts::model_report::NativeModelUpdate {
        use nexus_contracts::model_report::*;
        NativeModelUpdate {
            native_session_id: "root-native".into(),
            field: ModelEvidenceField::Configured,
            value: ModelEvidenceValue::Observed(ModelObservation {
                model_id: "opaque-root-model".into(),
                provider_id: None,
                source: ModelObservationSource::new("root-fixture-config").unwrap(),
                observed_at: 1,
                native_session_id: Some("root-native".into()),
                native_turn_id: None,
                native_message_id: None,
                native_reported_at: None,
            }),
        }
    }
}

#[async_trait]
impl nexus_agent::Adapter for RootModelAdapter {
    async fn open_session(&self) -> Result<(), nexus_common::NexusError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let carrier = self.carrier.lock().unwrap().clone();
        if let Some(carrier) = carrier {
            if !self.missing_root.load(Ordering::SeqCst) {
                assert!(carrier.sink().bind_native_root("root-native"));
                assert!(carrier.sink().observe(Self::update()));
            }
        }
        self.entered.notify_one();
        if self.park.load(Ordering::SeqCst) {
            self.release.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn resume(&self, _: &str) -> Result<(), nexus_common::NexusError> {
        self.open_session().await
    }
    async fn inject(&self, _: String) -> Result<(), nexus_agent::AdapterInjectError> {
        Ok(())
    }
    async fn stream_updates(
        &self,
    ) -> Result<Vec<nexus_agent::StreamEvent>, nexus_common::NexusError> {
        Ok(vec![])
    }
    async fn acp_session_id(&self) -> Option<String> {
        (!self.missing_root.load(Ordering::SeqCst)).then(|| "root-native".into())
    }
}

struct RootModelFixture {
    state: crate::daemon::AppState,
    native: Arc<RootModelAdapter>,
    session: SessionId,
    _dir: tempfile::TempDir,
}

impl RootModelFixture {
    async fn new(observed: bool) -> Self {
        use nexus_agent::adapter::{AcpModelMetadataDialect, AdapterModelReportingProfile};
        use nexus_contracts::{
            ModelEvidenceCapability, ModelObservationSource, ModelReportBackend,
        };
        let dir = tempfile::tempdir().unwrap();
        let daemon =
            nexus_store::DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
                .await
                .unwrap();
        let store = Arc::new(daemon.compatibility_store());
        let native = Arc::new(RootModelAdapter {
            carrier: Mutex::new(None),
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            park: std::sync::atomic::AtomicBool::new(false),
            missing_root: std::sync::atomic::AtomicBool::new(false),
        });
        let mut registry = nexus_agent::AdapterRegistry::new();
        let n = native.clone();
        let factory: nexus_agent::AdapterFactory = Arc::new(move |ctx| {
            *n.carrier.lock().unwrap() = ctx.model_reporting;
            n.clone()
        });
        if observed {
            registry.register_observed(
                &hid("other"),
                factory,
                AdapterModelReportingProfile::new(
                    ModelReportBackend::new("root-fixture").unwrap(),
                    ModelEvidenceCapability::Supported,
                    ModelEvidenceCapability::Unsupported,
                    ModelEvidenceCapability::Unsupported,
                    AcpModelMetadataDialect::ConfigOptions {
                        source: ModelObservationSource::new("root-fixture-config").unwrap(),
                    },
                )
                .unwrap(),
            );
        } else {
            registry.register(&hid("other"), factory);
        }
        let state = crate::daemon::AppState::wire_with_registry(
            store,
            &nexus_common::Config::default(),
            registry,
        );
        state.wait_for_runtime_identity_ready().await.unwrap();
        let mut req = shared_activation_request("root-model");
        req.cwd = Some(dir.path().to_string_lossy().into_owned());
        let session = state.identity.register(req).await.unwrap().session_id;
        state.presence.materialize_offline(&session).await.unwrap();
        Self {
            state,
            native,
            session,
            _dir: dir,
        }
    }
    async fn row(&self) -> nexus_store::types::SessionRow {
        nexus_store::repos::Sessions::new(&self.state.store)
            .find_by_session_id(&self.session)
            .await
            .unwrap()
            .unwrap()
    }
    async fn runtime(&self) -> nexus_store::types::AgentRuntimeRow {
        nexus_store::repos::AgentRuntimes::new(&self.state.store)
            .find_by_runtime_id(&self.session.0)
            .await
            .unwrap()
            .unwrap()
    }
    async fn drain(&self) {
        self.state
            .drain_model_reporting_for_shutdown(Duration::from_secs(2))
            .await
            .unwrap();
    }
    async fn opened(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.native.entered.notified())
            .await
            .expect("actual native open entered");
    }
}

#[tokio::test]
async fn root_model_stages_before_native_then_activates_exact_success_and_hot_reuses() {
    let f = RootModelFixture::new(true).await;
    f.native.park.store(true, Ordering::SeqCst);
    let state = f.state.clone();
    let row = f.row().await;
    let task = tokio::spawn(async move { state.ensure_live_row(row).await });
    f.opened().await;
    let carrier = f.native.carrier();
    let staged = f.runtime().await;
    assert!(staged.model_observer_token.is_some());
    let report = staged.model_report.unwrap();
    assert!(!report.observer_active);
    assert!(matches!(
        report.configured,
        nexus_contracts::ModelEvidenceSlot::Unknown { .. }
    ));
    // This caller reaches the existing per-runtime revive mutex before any DB/constructor work.
    let mut contender = Box::pin(f.state.ensure_live_row(f.row().await));
    assert!(futures::poll!(contender.as_mut()).is_pending());
    f.native.release.add_permits(1);
    task.await.unwrap().unwrap();
    contender.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if f.runtime().await.model_report.is_some_and(|r| {
                r.observer_active
                    && matches!(
                        r.configured,
                        nexus_contracts::ModelEvidenceSlot::Observed { .. }
                    )
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("activated native evidence persisted");
    let before = f.runtime().await;
    f.state.ensure_live_row(f.row().await).await.unwrap();
    let after = f.runtime().await;
    assert_eq!(f.native.calls.load(Ordering::SeqCst), 1);
    assert_eq!(before.model_observer_token, after.model_observer_token);
    assert_eq!(before.model_report_revision, after.model_report_revision);
    assert!(carrier.sink().accepts_profile(carrier.profile().identity()));
    f.drain().await;
}

#[tokio::test]
async fn root_model_cancel_native_open_revokes_and_cleans_staged_claim() {
    let f = RootModelFixture::new(true).await;
    f.native.park.store(true, Ordering::SeqCst);
    let state = f.state.clone();
    let row = f.row().await;
    let task = tokio::spawn(async move { state.ensure_live_row(row).await });
    f.opened().await;
    let carrier = f.native.carrier();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!carrier.sink().observe(RootModelAdapter::update()));
    f.drain().await;
    let row = f.runtime().await;
    assert!(row.model_observer_token.is_none());
    assert!(!row.model_report.unwrap().observer_active);
}

#[tokio::test]
async fn root_model_required_liveness_failure_closes_returned_open_owner() {
    let f = RootModelFixture::new(true).await;
    f.state.store.conn.execute_batch("CREATE TRIGGER root_model_online_fail BEFORE UPDATE OF presence ON sessions WHEN NEW.presence='online' BEGIN SELECT RAISE(ABORT,'root required online failure'); END;").await.unwrap();
    let error = f.state.ensure_live_row(f.row().await).await.unwrap_err();
    assert!(error.message.contains("root required online failure"));
    let carrier = f.native.carrier();
    assert!(!carrier.sink().observe(RootModelAdapter::update()));
    f.drain().await;
    assert!(!f.runtime().await.model_report.unwrap().observer_active);
}

#[tokio::test]
async fn root_model_missing_native_root_rejects_activation_without_guessing() {
    let f = RootModelFixture::new(true).await;
    f.native.missing_root.store(true, Ordering::SeqCst);
    let error = f
        .state
        .ensure_live_row(f.row().await)
        .await
        .expect_err("missing root cannot authorize activation");
    assert!(error.message.contains("native root"));
    let carrier = f.native.carrier();
    assert!(!carrier.sink().bind_native_root("invented"));
    f.drain().await;
    assert!(!f.runtime().await.model_report.unwrap().observer_active);
}

#[tokio::test]
async fn root_model_shutdown_rejects_before_factory_but_legacy_profile_stays_absent() {
    let f = RootModelFixture::new(true).await;
    f.drain().await;
    let mut row = f.row().await;
    let cwd = f._dir.path().join("must-not-create");
    row.cwd = Some(cwd.to_string_lossy().into_owned());
    assert!(f.state.ensure_live_row(row).await.is_err());
    assert!(
        !cwd.exists(),
        "closed admission precedes context filesystem effects"
    );
    assert_eq!(f.native.calls.load(Ordering::SeqCst), 0);
    let legacy = RootModelFixture::new(false).await;
    legacy
        .state
        .ensure_live_row(legacy.row().await)
        .await
        .unwrap();
    assert!(legacy.native.carrier.lock().unwrap().is_none());
    assert!(legacy.runtime().await.model_report.is_none());
    legacy.drain().await;
}

#[tokio::test]
async fn root_model_fresh_launch_uses_same_captured_reporting_path() {
    let f = RootModelFixture::new(true).await;
    let req: nexus_contracts::SpawnRequest = serde_json::from_value(serde_json::json!({
        "kind":"other", "name":"fresh-root-model", "headless":true,
        "cwd":f._dir.path().to_str().unwrap(), "project":"default"
    }))
    .unwrap();
    let response = f.state.launch_agent(req, "default", None).await.unwrap();
    let carrier = f.native.carrier();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let row = nexus_store::repos::AgentRuntimes::new(&f.state.store)
                .find_by_runtime_id(&response.session_id.0)
                .await
                .unwrap()
                .unwrap();
            if row.model_report.is_some_and(|r| r.observer_active) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fresh root activated");
    assert!(carrier.sink().observe(RootModelAdapter::update()));
    f.drain().await;
}

#[tokio::test(flavor = "current_thread")]
async fn root_model_cancel_after_agent_return_before_liveness_closes_outer_owner() {
    let f = RootModelFixture::new(true).await;
    f.native.park.store(true, Ordering::SeqCst);
    let mut pending = Box::pin(f.state.ensure_live_row(f.row().await));
    tokio::select! {
        result = pending.as_mut() => panic!("open settled before native gate: {result:?}"),
        _ = f.opened() => {}
    }
    let carrier = f.native.carrier();
    let held = f.state.store.lock_presence_transition().await;
    f.native.release.add_permits(1);
    // Exact +1 mutex-Arc witness during ONLY the target poll. This path's first new presence
    // acquisition is mark_rebound_agent_live, after Agent returned/disarmed its inner guard.
    witness_combined_presence_wait(&held, pending.as_mut()).await;
    assert_eq!(f.state.agent.is_harness_alive(&f.session), Some(true));
    assert!(carrier.sink().accepts_profile(carrier.profile().identity()));
    drop(pending);
    assert!(!carrier.sink().observe(RootModelAdapter::update()));
    assert_eq!(
        f.state.agent.is_harness_alive(&f.session),
        Some(true),
        "native binding is not rolled back"
    );
    drop(held);
    f.drain().await;
    assert!(!f.runtime().await.model_report.unwrap().observer_active);
}

#[tokio::test]
async fn root_model_failed_claim_never_enters_factory_and_retains_original_error() {
    let f = RootModelFixture::new(true).await;
    f.state.store.identity_conn().execute_batch("CREATE TRIGGER root_model_claim_fail BEFORE UPDATE OF model_observer_token ON agent_runtimes WHEN NEW.model_observer_token IS NOT NULL BEGIN SELECT RAISE(ABORT,'root claim cause'); END;").await.unwrap();
    let error = f.state.ensure_live_row(f.row().await).await.unwrap_err();
    assert!(error.message.contains("root claim cause"));
    assert_eq!(f.native.calls.load(Ordering::SeqCst), 0);
    assert!(f.runtime().await.model_observer_token.is_none());
    let error = f
        .state
        .drain_model_reporting_for_shutdown(Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("root claim cause"));
}

#[tokio::test]
async fn root_model_captured_foreign_session_binding_rejects_before_context_or_claim() {
    let f = RootModelFixture::new(true).await;
    let mut row = f.row().await;
    row.agent_id = Some("a_foreign".into());
    let cwd = f._dir.path().join("foreign-context");
    row.cwd = Some(cwd.to_string_lossy().into_owned());
    let before = f.runtime().await;
    let error = f.state.ensure_live_row(row).await.unwrap_err();
    assert!(error.message.contains("bindings disagree"));
    assert!(!cwd.exists());
    assert_eq!(f.native.calls.load(Ordering::SeqCst), 0);
    let after = f.runtime().await;
    assert_eq!(before.model_report_revision, after.model_report_revision);
    assert_eq!(before.model_observer_token, after.model_observer_token);
    f.drain().await;
}
