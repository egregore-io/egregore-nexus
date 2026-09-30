//! Actual adapters and daemon ownership/projector over a hermetic ACP peer replaying captured
//! metadata. This is not a live provider invocation or the Gateway's TypeScript consumer.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexus::daemon::{
    gateway_stream_socket::{GatewayStreamFrame, GatewayStreamPublisher},
    AppState,
};
use nexus_agent::adapter::{
    AdapterModelReportingProfile, HarnessCommand, HermesAdapter, OpenCodeAdapter,
};
use nexus_agent::{Adapter, AdapterRegistry, LaunchCtx};
use nexus_contracts::{GatewayProjectionKind, HarnessId, ModelEvidenceSlot, SessionId};
use nexus_store::{repos::AgentRuntimes, DaemonStore};
use serde_json::{json, Value};

mod model_projection_artifacts {
    include!("support/model_projection_artifacts.rs");
}

fn profile(harness: &str) -> AdapterModelReportingProfile {
    let mut registry = AdapterRegistry::with_builtins();
    nexus_harness_claude::register(&mut registry);
    nexus_harness_codex::register(&mut registry);
    registry
        .select(&HarnessId::new(harness).unwrap())
        .unwrap()
        .profile()
        .expect("actual builtin registration must supply its verified ACP profile")
        .clone()
}

fn adapter(harness: &str, command: HarnessCommand, ctx: LaunchCtx) -> Arc<dyn Adapter> {
    match harness {
        "claude" => {
            Arc::new(nexus_harness_claude::ClaudeAdapter::with_command_and_context(command, ctx))
        }
        "codex" => Arc::new(nexus_harness_codex::CodexAdapter::with_command_and_context(
            command, ctx,
        )),
        "opencode" => Arc::new(OpenCodeAdapter::with_command_and_context(command, ctx)),
        "hermes" => Arc::new(HermesAdapter::with_command_and_context(command, ctx)),
        _ => unreachable!(),
    }
}

fn command(harness: &str, cwd: String) -> HarnessCommand {
    let fixtures: Value =
        serde_json::from_str(include_str!("fixtures/model_reporting/native.json")).unwrap();
    let events = fixtures["rows"][format!("{harness}.acp")]["events"]
        .as_array()
        .unwrap();
    let payload = |method| {
        events.iter().find(|event| event["kind"] == method).unwrap()["payload"].to_string()
    };
    HarnessCommand {
        program: env!("CARGO_BIN_EXE_nexus_model_fixture_acp").into(),
        cwd: Some(cwd),
        env: vec![
            ("FAKE_ACP_RAW_NEW".into(), payload("session/new")),
            ("FAKE_ACP_RAW_LOAD".into(), payload("session/load")),
        ],
        ..Default::default()
    }
}

async fn next_report(
    rx: &mut tokio::sync::broadcast::Receiver<GatewayStreamFrame>,
    sid: &SessionId,
    active: bool,
    expected: &str,
    after_revision: u64,
) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let GatewayStreamFrame::Projection { event } = rx.recv().await.unwrap() {
                if event.kind == GatewayProjectionKind::RuntimeUpserted
                    && event.payload["runtimeId"] == sid.0
                    && event.payload["modelReport"]["observerActive"] == active
                    && event.payload["modelReport"]["configured"]["observation"]["modelId"]
                        == expected
                    && event.payload["modelReport"]["reportRevision"]
                        .as_u64()
                        .is_some_and(|r| r > after_revision)
                {
                    return event.payload;
                }
            }
        }
    })
    .await
    .expect("actual adapter metadata reaches the canonical WsSink publisher")
}

// Normal Rust tests still exercise the full adapter path without artifacts. The explicit
// cross-language gate supplies a fresh directory and run id and requires all four outputs.
fn export_projection_run(harness: &str, fresh: &Value, resumed: &Value, stopped: &Value) {
    model_projection_artifacts::export(harness, "headless", fresh, resumed, stopped);
}

async fn run(harness: &'static str) {
    let dir = tempfile::tempdir().unwrap();
    let daemon = DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    let native = Arc::new(Mutex::new(Vec::<Arc<dyn Adapter>>::new()));
    let captured = native.clone();
    let mut registry = AdapterRegistry::new();
    registry.register_observed(
        &HarnessId::new(harness).unwrap(),
        Arc::new(move |ctx| {
            let command = command(harness, ctx.cwd.clone().unwrap());
            let adapter = adapter(harness, command, ctx);
            captured.lock().unwrap().push(adapter.clone());
            adapter
        }),
        profile(harness),
    );
    let publisher = GatewayStreamPublisher::new(256);
    let mut rx = publisher.subscribe();
    let state = AppState::wire_with_registry_and_gateway_stream(
        store.clone(),
        &nexus_common::Config::default(),
        registry,
        Some(publisher),
    );
    state.wait_for_runtime_identity_ready().await.unwrap();
    // This root has no PTY supervisor. Exercise actual headed-request -> ACP degradation for
    // Claude, alongside explicit headless requests for the other adapters. Evidence follows
    // the backend actually constructed, never the requested presentation mode.
    let requested_headless = harness != "claude";
    let request = |name: &str| {
        serde_json::from_value(json!({
            "kind":harness, "name":name, "headless":requested_headless,
            "cwd":dir.path().to_str().unwrap(), "project":"default"
        }))
        .unwrap()
    };
    let name = format!("model-{harness}");
    let launched = state.launch_agent(request(&name), "default", None).await;
    // Cleanup even when the deliberately disconnected constructor scaffold rejects activation.
    if let Err(error) = &launched {
        let adapters = native.lock().unwrap().clone();
        for adapter in adapters {
            adapter.kill().await;
        }
        panic!("actual {harness} launch must carry its reporting context: {error:?}");
    }
    let sid = launched.unwrap().session_id;
    let expected = match harness {
        "claude" | "codex" => "gpt-astra",
        "opencode" => "fixture-provider/gpt-astra",
        "hermes" => "fixture-provider:gpt-astra",
        _ => unreachable!(),
    };
    let row = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = AgentRuntimes::new(&store)
                .find_by_runtime_id(&sid.0)
                .await
                .unwrap()
                .unwrap();
            if row.model_report.as_ref().is_some_and(|r| r.observer_active) {
                break row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("daemon activation persists");
    let report = row.model_report.as_ref().unwrap();
    assert!(
        matches!(&report.configured, ModelEvidenceSlot::Observed { observation, .. }
        if observation.model_id == expected && observation.native_session_id.as_deref() == Some("fixture-root") && observation.provider_id.is_none()),
        "{harness}: actual adapter must retain captured reporting before initialize: {report:?}"
    );
    let frame = next_report(&mut rx, &sid, true, expected, 0).await;
    assert_eq!(frame["modelReport"]["backend"], format!("{harness}.acp"));
    assert_eq!(frame["agentId"], row.agent_id);
    assert_eq!(frame["presence"], "online");
    assert!(row.active);
    assert!(row.stopped_at.is_none());
    assert!(report.observer_active);
    assert_eq!(frame["modelReport"], serde_json::to_value(report).unwrap());
    assert!(frame.get("modelObserverToken").is_none());
    assert!(!frame
        .to_string()
        .contains(row.model_observer_token.as_deref().unwrap()));
    // Kill only this actual child, leaving the active durable identity to exercise the public
    // launch-by-id cold resume path (session/load), not a manually constructed collector call.
    state.teardown_harness(&name, "default").await;
    let resumed = state
        .launch_agent(request(&row.agent_id), "default", None)
        .await
        .unwrap();
    assert_eq!(resumed.session_id, sid);
    assert_eq!(
        native.lock().unwrap().len(),
        2,
        "cold resume constructs the actual adapter again"
    );
    let expected_load = if harness == "opencode" {
        "fixture-provider/opaque-resumed-model"
    } else {
        expected
    };
    let resumed_frame =
        next_report(&mut rx, &sid, true, expected_load, report.report_revision).await;
    assert_eq!(resumed_frame["agentId"], row.agent_id);
    assert!(
        resumed_frame["modelReport"]["reportRevision"]
            .as_u64()
            .unwrap()
            > report.report_revision
    );
    let resumed_row = AgentRuntimes::new(&store)
        .find_by_runtime_id(&sid.0)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(resumed_row.model_observer_token, row.model_observer_token);
    assert_eq!(
        resumed_frame["modelReport"],
        serde_json::to_value(resumed_row.model_report.as_ref().unwrap()).unwrap()
    );
    state.teardown_owned_transports_for_shutdown().await;
    state
        .drain_model_reporting_for_shutdown(Duration::from_secs(5))
        .await
        .unwrap();
    let stopped = AgentRuntimes::new(&store)
        .find_by_runtime_id(&sid.0)
        .await
        .unwrap()
        .unwrap();
    assert!(!stopped.active);
    assert!(stopped.stopped_at.is_some());
    assert!(!stopped.model_report.as_ref().unwrap().observer_active);
    assert!(stopped.model_observer_token.is_none());
    // All shutdown-owned publication has settled. Revocation may have emitted an intermediate
    // observer-inactive/online frame before durable offline; inspect LAST, not first/any match.
    let mut final_frame = None;
    while let Ok(frame) = rx.try_recv() {
        if let GatewayStreamFrame::Projection { event } = frame {
            if event.kind == GatewayProjectionKind::RuntimeUpserted
                && event.payload["runtimeId"] == sid.0
            {
                final_frame = Some(event.payload);
            }
        }
    }
    let final_frame = final_frame.expect("settled shutdown publishes a final runtime snapshot");
    assert_eq!(final_frame["agentId"], row.agent_id);
    assert_eq!(final_frame["presence"], "offline");
    assert_eq!(final_frame["active"], false);
    assert_eq!(
        final_frame["stoppedAt"],
        serde_json::to_value(stopped.stopped_at).unwrap()
    );
    assert_eq!(
        final_frame["modelReport"],
        serde_json::to_value(stopped.model_report.unwrap()).unwrap()
    );
    export_projection_run(harness, &frame, &resumed_frame, &final_frame);
}

#[tokio::test]
async fn actual_claude_adapter_model_reaches_daemon_and_gateway_publisher() {
    run("claude").await;
}
#[tokio::test]
async fn actual_codex_adapter_model_reaches_daemon_and_gateway_publisher() {
    run("codex").await;
}
#[tokio::test]
async fn actual_opencode_adapter_model_reaches_daemon_and_gateway_publisher() {
    run("opencode").await;
}
#[tokio::test]
async fn actual_hermes_adapter_model_reaches_daemon_and_gateway_publisher() {
    run("hermes").await;
}
