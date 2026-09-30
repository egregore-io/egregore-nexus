// Shared test-only export of actual WsSink bodies through the production publisher. Ordinary
// Rust tests do not write artifacts; the explicit composed gate requires every fresh mode file.
use super::{GatewayStreamFrame, GatewayStreamPublisher};
use nexus_contracts::GatewayProjectionKind;
use serde_json::{json, Value};
use std::io::Write;

#[allow(dead_code)] // Some callers already capture their own stronger native-specific frame.
pub(super) async fn next(
    rx: &mut tokio::sync::broadcast::Receiver<GatewayStreamFrame>,
    runtime: &str,
    after: u64,
    active: bool,
    evidence: &str,
) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let GatewayStreamFrame::Projection { event } = rx.recv().await.unwrap() {
                let body = event.payload;
                if body["runtimeId"] == runtime
                    && body["active"] == active
                    && body["modelReport"]["observerActive"] == active
                    && body["modelReport"]["reportRevision"]
                        .as_u64()
                        .is_some_and(|r| r > after)
                    && body["modelReport"][evidence]["observation"]["modelId"].is_string()
                    && (active || body["presence"] == "offline")
                {
                    return body;
                }
            }
        }
    })
    .await
    .expect("newer native model/status frame from actual WsSink")
}

pub(super) fn export(harness: &str, mode: &str, fresh: &Value, newer: &Value, stopped: &Value) {
    let Some(directory) = std::env::var_os("NEXUS_MODEL_PROJECTION_FIXTURE_DIR") else {
        assert!(std::env::var_os("NEXUS_MODEL_PROJECTION_RUN_ID").is_none());
        return;
    };
    let run_id = std::env::var("NEXUS_MODEL_PROJECTION_RUN_ID").unwrap();
    assert!(!run_id.is_empty());
    assert!(matches!(mode, "headless" | "headed"));
    for body in [fresh, newer, stopped] {
        assert_eq!(body["harness"], harness);
        assert_eq!(body["runtimeId"], fresh["runtimeId"]);
        assert_eq!(body["agentId"], fresh["agentId"]);
        assert!(body.get("modelObserverToken").is_none());
    }
    assert_eq!(fresh["modelReport"]["observerActive"], true);
    assert_eq!(newer["modelReport"]["observerActive"], true);
    assert_eq!(stopped["modelReport"]["observerActive"], false);
    assert_eq!(stopped["active"], false);
    assert_eq!(stopped["presence"], "offline");
    assert!(stopped["stoppedAt"].as_i64().is_some());
    let revision = |body: &Value| body["modelReport"]["reportRevision"].as_u64().unwrap();
    assert!(revision(newer) > revision(fresh));
    assert!(revision(stopped) > revision(newer));
    let publisher = GatewayStreamPublisher::new(16);
    let mut rx = publisher.subscribe();
    let mut frames = Vec::new();
    // Distinct envelope times make a rejected OLD timestamp advance observable. Payload
    // timestamps remain exactly as captured; only the replay envelope is controlled.
    let envelope_time = nexus_common::now();
    for (index, payload) in [newer, fresh, stopped, fresh].into_iter().enumerate() {
        publisher.publish_projection(
            format!("{run_id}:{harness}:{mode}:{index}"),
            GatewayProjectionKind::RuntimeUpserted,
            envelope_time.checked_add(index as i64).unwrap(),
            payload.clone(),
        );
        let frame = rx.try_recv().expect("real publisher emitted captured body");
        let GatewayStreamFrame::Projection { event } = &frame else {
            panic!("expected projection frame")
        };
        assert_eq!(&event.payload, payload);
        frames.push(frame);
    }
    let artifact = json!({"runId":run_id,"harness":harness,"mode":mode,"frames":frames,
        "expected":{"fresh":fresh,"newer":newer,"stopped":stopped}});
    let path = std::path::PathBuf::from(directory).join(format!("{harness}.{mode}.json"));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.write_all(&serde_json::to_vec(&artifact).unwrap())
        .unwrap();
    file.sync_all().unwrap();
}
