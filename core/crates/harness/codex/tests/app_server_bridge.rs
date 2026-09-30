//! `CodexBridge` integration tests for the fresh-headed codex launch path.
//!
//! These tests use the hermetic fake app-server binary and create the rollout file that a real
//! codex TUI writes after its first turn. The bridge must discover that rollout, resume the
//! thread on its own JSON-RPC connection, bind injection, and forward notifications.

use std::sync::Arc;

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::AgentUpdateKind;
use nexus_harness_codex::{BridgeLaunchOptions, CodexAppServerClient, CodexBridge, SupervisorOpts};

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");

struct ModelSink {
    profile: nexus_contracts::model_report::ModelProfileIdentity,
    root: std::sync::Mutex<Option<String>>,
    updates: std::sync::Mutex<Vec<nexus_contracts::model_report::NativeModelUpdate>>,
    closed: std::sync::atomic::AtomicBool,
}
impl nexus_contracts::model_report::ModelObservationSink for ModelSink {
    fn accepts_profile(
        &self,
        identity: &nexus_contracts::model_report::ModelProfileIdentity,
    ) -> bool {
        self.profile.matches(identity) && !self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn bind_native_root(&self, root: &str) -> bool {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return false;
        }
        let mut captured = self.root.lock().unwrap();
        match captured.as_deref() {
            Some(old) => old == root,
            None => {
                *captured = Some(root.into());
                true
            }
        }
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

fn captured_model() -> (Arc<ModelSink>, nexus_agent::adapter::NativeModelReporting) {
    let profile = nexus_harness_codex::app_server::model_reporting::profile();
    let sink = Arc::new(ModelSink {
        profile: profile.identity().clone(),
        root: Default::default(),
        updates: Default::default(),
        closed: Default::default(),
    });
    (sink.clone(), profile.capture(sink).unwrap())
}
fn model_payload(resume: bool) -> serde_json::Value {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../nexus/tests/fixtures/model_reporting/native.json"
    ))
    .unwrap();
    fixture["rows"]["codex.headed"]["events"][if resume { 1 } else { 0 }]["payload"].clone()
}

async fn check_native_model_binding(resume: bool) {
    use nexus_contracts::model_report::{ModelEvidenceField, ModelEvidenceValue};
    let dir = tempdir(if resume { "mr" } else { "ms" });
    let payload = model_payload(resume);
    let root = payload["thread"]["id"].as_str().unwrap().to_owned();
    if resume {
        write_rollout(&dir, &root);
    }
    let (sink, reporting) = captured_model();
    let bridge = CodexBridge::new();
    let session = SessionId("model-session".into());
    bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.into(),
                session_dir: dir.clone(),
                codex_home: Some(dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![(
                    if resume {
                        "FAKE_CODEX_RESUME_RESPONSE"
                    } else {
                        "FAKE_CODEX_START_RESPONSE"
                    }
                    .into(),
                    payload.to_string(),
                )],
            },
            Arc::new(RecSink::default()),
            BridgeLaunchOptions {
                model_reporting: Some(reporting.clone()),
                known_thread_id: resume.then(|| root.clone()),
                create_thread_if_missing: !resume,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let updates = sink.updates.lock().unwrap().clone();
    let current =
        bridge
            .transport()
            .with_current_model_binding(&session, &reporting, str::to_owned);
    let (_, foreign) = captured_model();
    let foreign_callback =
        bridge
            .transport()
            .with_current_model_binding(&session, &foreign, |_| true);
    bridge.kill(&session);
    let after_kill = bridge
        .transport()
        .with_current_model_binding(&session, &reporting, |_| true);
    let closed = sink.closed.load(std::sync::atomic::Ordering::SeqCst);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        updates.len(),
        1,
        "actual client response must reach captured model observer"
    );
    assert_eq!(updates[0].field, ModelEvidenceField::Configured);
    assert_eq!(updates[0].native_session_id, root);
    assert!(
        matches!(&updates[0].value,ModelEvidenceValue::Observed(value) if value.model_id=="gpt-astra" && value.provider_id.as_deref()==Some("fixture"))
    );
    assert_eq!(current, Some(root));
    assert_eq!(foreign_callback, None);
    assert_eq!(after_kill, None);
    assert!(closed, "native kill closes captured model admission");
}
#[tokio::test]
async fn native_model_start_uses_actual_client_and_exact_published_owner() {
    check_native_model_binding(false).await;
}
#[tokio::test]
async fn native_model_resume_uses_actual_client_and_exact_published_owner() {
    check_native_model_binding(true).await;
}

#[tokio::test]
async fn native_model_cancelled_setup_closes_before_any_late_response() {
    let dir = tempdir("mc");
    let payload = model_payload(true);
    let root = payload["thread"]["id"].as_str().unwrap().to_owned();
    write_rollout(&dir, &root);
    let entered = dir.join("entered");
    let release = dir.join("release");
    let (sink, reporting) = captured_model();
    let bridge = CodexBridge::new();
    let worker = bridge.clone();
    let session = SessionId("model-cancel".into());
    let worker_session = session.clone();
    let opts = SupervisorOpts {
        codex_exe: FAKE_BIN.into(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![
            ("FAKE_CODEX_RESUME_RESPONSE".into(), payload.to_string()),
            ("FAKE_CODEX_RESUME_GATE_THREAD".into(), root.clone()),
            (
                "FAKE_CODEX_RESUME_ENTERED".into(),
                entered.to_string_lossy().into(),
            ),
            (
                "FAKE_CODEX_RESUME_RELEASE".into(),
                release.to_string_lossy().into(),
            ),
        ],
    };
    let task = tokio::spawn(async move {
        worker
            .launch_with_options(
                worker_session,
                opts,
                Arc::new(RecSink::default()),
                BridgeLaunchOptions {
                    model_reporting: Some(reporting),
                    known_thread_id: Some(root),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !entered.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let closed_before_kill = sink.closed.load(std::sync::atomic::Ordering::SeqCst);
    std::fs::write(&release, b"release").unwrap();
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        closed_before_kill,
        "abandoned setup must close OLD observer before late native reply"
    );
    assert!(sink.updates.lock().unwrap().is_empty());
}

#[derive(Default)]
struct RecSink(tokio::sync::Mutex<Vec<WsEvent>>);

#[tokio::test]
async fn native_model_old_setup_reply_cannot_borrow_new_same_process_binding() {
    let dir = tempdir("mn");
    let old_payload = model_payload(true);
    let new_payload = model_payload(false);
    let old_root = old_payload["thread"]["id"].as_str().unwrap().to_owned();
    let new_root = new_payload["thread"]["id"].as_str().unwrap().to_owned();
    write_rollout(&dir, &old_root);
    let rollout = dir.join("codex-home/sessions/2026/06/rollout-test.jsonl");
    std::fs::rename(&rollout, rollout.with_file_name("rollout-old.jsonl")).unwrap();
    write_rollout(&dir, &new_root);
    let entered = dir.join("entered");
    let release = dir.join("release");
    let replied = dir.join("replied");
    let responses = serde_json::json!({old_root.clone():old_payload,new_root.clone():new_payload});
    let opts = SupervisorOpts {
        codex_exe: FAKE_BIN.into(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![
            ("FAKE_CODEX_RESUME_RESPONSES".into(), responses.to_string()),
            ("FAKE_CODEX_RESUME_GATE_THREAD".into(), old_root.clone()),
            (
                "FAKE_CODEX_RESUME_ENTERED".into(),
                entered.to_string_lossy().into(),
            ),
            (
                "FAKE_CODEX_RESUME_RELEASE".into(),
                release.to_string_lossy().into(),
            ),
            (
                "FAKE_CODEX_RESUME_REPLIED".into(),
                replied.to_string_lossy().into(),
            ),
        ],
    };
    let session = SessionId("model-new-wins".into());
    let bridge = CodexBridge::new();
    let (old_sink, old_reporting) = captured_model();
    let worker = bridge.clone();
    let worker_session = session.clone();
    let old_options = opts.clone();
    let captured = old_reporting.clone();
    let old = tokio::spawn(async move {
        worker
            .launch_with_options(
                worker_session,
                old_options,
                Arc::new(RecSink::default()),
                BridgeLaunchOptions {
                    model_reporting: Some(captured),
                    known_thread_id: Some(old_root),
                    ..Default::default()
                },
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !entered.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let original_pid = bridge.process_ledger(&session).unwrap().os_pid;
    let (new_sink, new_reporting) = captured_model();
    let new = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        bridge.launch_with_options(
            session.clone(),
            opts,
            Arc::new(RecSink::default()),
            BridgeLaunchOptions {
                model_reporting: Some(new_reporting.clone()),
                known_thread_id: Some(new_root.clone()),
                ..Default::default()
            },
        ),
    )
    .await;
    let new_before_old_release = matches!(&new, Ok(Ok(_)));
    let replacement_pid = bridge.process_ledger(&session).map(|ids| ids.os_pid);
    std::fs::write(&release, b"release").unwrap();
    let old_reply_attempted = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !replied.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let old = tokio::time::timeout(std::time::Duration::from_secs(5), old).await;
    let old_callback =
        bridge
            .transport()
            .with_current_model_binding(&session, &old_reporting, |_| true);
    let new_callback =
        bridge
            .transport()
            .with_current_model_binding(&session, &new_reporting, str::to_owned);
    let old_closed = old_sink.closed.load(std::sync::atomic::Ordering::SeqCst);
    let new_open = !new_sink.closed.load(std::sync::atomic::Ordering::SeqCst);
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(dir);
    assert!(
        new_before_old_release,
        "NEW binding must complete while OLD native response is parked"
    );
    assert_eq!(
        replacement_pid,
        Some(original_pid),
        "binding replacement reuses the same native process"
    );
    assert!(
        old_reply_attempted,
        "server attempted OLD response only after NEW completed"
    );
    assert!(matches!(old, Ok(Ok(Err(_)))));
    assert!(old_closed);
    assert!(new_open);
    assert!(old_sink.updates.lock().unwrap().is_empty());
    assert_eq!(new_sink.updates.lock().unwrap().len(), 1);
    assert_eq!(old_callback, None);
    assert_eq!(new_callback, Some(new_root));
}

#[async_trait]
impl EventSink for RecSink {
    async fn emit(&self, event: WsEvent) {
        self.0.lock().await.push(event);
    }
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-codex-bridge-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

fn write_rollout(session_dir: &std::path::Path, thread_id: &str) {
    let rollout_dir = session_dir
        .join("codex-home")
        .join("sessions")
        .join("2026")
        .join("06");
    std::fs::create_dir_all(&rollout_dir).expect("create rollout dir");
    let rollout = rollout_dir.join("rollout-test.jsonl");
    let first_line = serde_json::json!({
        "type": "session_meta",
        "payload": { "id": thread_id }
    });
    std::fs::write(&rollout, format!("{first_line}\n")).expect("write rollout");
}

#[tokio::test]
async fn same_directory_prelisten_cleanup_cannot_unlink_replacement() {
    assert_same_directory_retirement(false).await;
}

#[tokio::test]
async fn same_directory_listening_candidate_cannot_be_adopted_then_killed_by_old_owner() {
    assert_same_directory_retirement(true).await;
}

#[cfg(unix)]
#[tokio::test]
async fn replacement_waits_for_registered_child_retirement_before_same_path_adoption() {
    let dir = tempdir("registered-retirement");
    let entered = dir.join("retiring");
    let release = dir.join("release");
    let bridge = CodexBridge::new();
    let session = SessionId("registered-retirement".into());
    let mut opts = SupervisorOpts {
        codex_exe: FAKE_BIN.into(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![
            (
                "FAKE_CODEX_TERM_ENTERED".into(),
                entered.to_string_lossy().into(),
            ),
            (
                "FAKE_CODEX_TERM_RELEASE".into(),
                release.to_string_lossy().into(),
            ),
        ],
    };
    bridge
        .launch_with_options(
            session.clone(),
            opts.clone(),
            Arc::new(RecSink::default()),
            BridgeLaunchOptions {
                create_thread_if_missing: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let old_pid = bridge.process_ledger(&session).unwrap().os_pid;
    assert!(bridge.kill(&session));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !entered.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    opts.env.clear();
    let ready = Arc::new(tokio::sync::Notify::new());
    let mut new = tokio::spawn({
        let bridge = bridge.clone();
        let session = session.clone();
        let ready = ready.clone();
        async move {
            ready.notify_one();
            bridge
                .launch_with_options(
                    session,
                    opts,
                    Arc::new(RecSink::default()),
                    BridgeLaunchOptions {
                        create_thread_if_missing: true,
                        ..Default::default()
                    },
                )
                .await
        }
    });
    ready.notified().await;
    let early = tokio::time::timeout(std::time::Duration::from_millis(100), &mut new).await;
    let overtook = early.is_ok();
    std::fs::write(release, b"release").unwrap();
    let socket = if let Ok(result) = early {
        result.unwrap().unwrap()
    } else {
        new.await.unwrap().unwrap()
    };
    let replacement_pid = bridge.process_ledger(&session).map(|ids| ids.os_pid);
    let usable = CodexAppServerClient::connect(&socket, "control").await;
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(dir);
    assert!(
        !overtook,
        "replacement adopted a child whose registered shutdown had not finished"
    );
    assert!(replacement_pid.is_some_and(|pid| pid != old_pid));
    assert!(usable.is_ok());
}

async fn assert_same_directory_retirement(listening: bool) {
    let dir = tempdir("same-directory-retirement");
    let entered = dir.join("entered");
    let release = dir.join("release");
    let bridge = CodexBridge::new();
    let session = SessionId("same-directory-retirement".into());
    let old = tokio::spawn({
        let bridge = bridge.clone();
        let session = session.clone();
        let dir = dir.clone();
        let entered = entered.clone();
        let release = release.clone();
        async move {
            bridge
                .launch_with_options(
                    session,
                    SupervisorOpts {
                        codex_exe: FAKE_BIN.into(),
                        session_dir: dir.clone(),
                        codex_home: Some(dir.join("home")),
                        model: None,
                        bus_mcp: None,
                        cwd: None,
                        env: vec![
                            (
                                if listening {
                                    "FAKE_CODEX_PROBE_ENTERED"
                                } else {
                                    "FAKE_CODEX_START_ENTERED"
                                }
                                .into(),
                                entered.to_string_lossy().into(),
                            ),
                            (
                                if listening {
                                    "FAKE_CODEX_PROBE_RELEASE"
                                } else {
                                    "FAKE_CODEX_START_RELEASE"
                                }
                                .into(),
                                release.to_string_lossy().into(),
                            ),
                        ],
                    },
                    Arc::new(RecSink::default()),
                    BridgeLaunchOptions {
                        create_thread_if_missing: true,
                        ..Default::default()
                    },
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !entered.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!bridge.has(&session));
    bridge.kill(&session);
    let socket = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        bridge.launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.into(),
                session_dir: dir.clone(),
                codex_home: Some(dir.join("home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            Arc::new(RecSink::default()),
            BridgeLaunchOptions {
                create_thread_if_missing: true,
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    std::fs::write(release, b"release").unwrap();
    assert!(tokio::time::timeout(std::time::Duration::from_secs(3), old)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    let usable = CodexAppServerClient::connect(&socket, "replacement-control").await;
    let owns_replacement = bridge.process_ledger(&session).is_some();
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(dir);
    assert!(
        usable.is_ok(),
        "old cleanup removed the current physical endpoint"
    );
    assert!(
        owns_replacement,
        "replacement adopted an unregistered retiring child"
    );
}

#[tokio::test]
async fn kill_before_handle_publication_revokes_old_setup_without_harming_replacement() {
    let old_dir = tempdir("prehandle-old");
    let new_dir = tempdir("prehandle-new");
    let entered = old_dir.join("entered");
    let release = old_dir.join("release");
    let bridge = CodexBridge::new();
    let session = SessionId("prehandle-same-session".into());
    let old = tokio::spawn({
        let bridge = bridge.clone();
        let session = session.clone();
        let old_dir = old_dir.clone();
        let entered = entered.clone();
        let release = release.clone();
        async move {
            bridge
                .launch_with_options(
                    session,
                    SupervisorOpts {
                        codex_exe: FAKE_BIN.into(),
                        session_dir: old_dir.clone(),
                        codex_home: Some(old_dir.join("home")),
                        model: None,
                        bus_mcp: None,
                        cwd: None,
                        env: vec![
                            (
                                "FAKE_CODEX_START_ENTERED".into(),
                                entered.to_string_lossy().into(),
                            ),
                            (
                                "FAKE_CODEX_START_RELEASE".into(),
                                release.to_string_lossy().into(),
                            ),
                        ],
                    },
                    Arc::new(RecSink::default()),
                    BridgeLaunchOptions {
                        create_thread_if_missing: true,
                        ..Default::default()
                    },
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !entered.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!bridge.has(&session));
    bridge.kill(&session);
    bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.into(),
                session_dir: new_dir.clone(),
                codex_home: Some(new_dir.join("home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            Arc::new(RecSink::default()),
            BridgeLaunchOptions {
                create_thread_if_missing: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let replacement_pid = bridge.process_ledger(&session).unwrap().os_pid;
    std::fs::write(&release, b"release").unwrap();
    let old_result = tokio::time::timeout(std::time::Duration::from_secs(3), old)
        .await
        .unwrap()
        .unwrap();
    let actual_pid = bridge.process_ledger(&session).unwrap().os_pid;
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(old_dir);
    let _ = std::fs::remove_dir_all(new_dir);
    assert!(
        old_result.is_err(),
        "revoked pre-handle setup published after replacement"
    );
    assert_eq!(
        actual_pid, replacement_pid,
        "stale setup replaced or cleaned up the new handle"
    );
}

#[tokio::test]
async fn launch_discovers_rollout_binds_transport_and_forwards_notifications() {
    let session_dir = tempdir("launch");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-bridge".into());

    let sock_path = bridge
        .launch(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
        )
        .await
        .expect("bridge launch should start fake app-server");

    assert!(
        bridge.has(&session),
        "bridge should track the launched session immediately"
    );

    write_rollout(&session_dir, "fake-thread");

    let transport = bridge.transport();
    let bind_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !transport.is_bound(&session) {
        assert!(
            tokio::time::Instant::now() < bind_deadline,
            "bridge did not bind transport after rollout discovery"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let driver = CodexAppServerClient::connect(&sock_path, "driver")
        .await
        .expect("driver client should connect");
    driver
        .turn_start("fake-thread", "go")
        .await
        .expect("turn_start should succeed");

    let event_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let got = sink.0.lock().await.clone();
        if got.iter().any(|e| {
            matches!(
                e,
                WsEvent::AgentUpdate {
                    kind: AgentUpdateKind::Text,
                    ..
                }
            )
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < event_deadline,
            "timed out waiting for Text AgentUpdate; got events: {got:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(
        bridge.kill(&session),
        "kill should remove the launched session"
    );
    assert!(
        !bridge.has(&session),
        "killed session should no longer be tracked"
    );
    let _ = std::fs::remove_dir_all(&session_dir);
}

#[tokio::test]
async fn fresh_launch_can_create_and_bind_an_injectable_thread_immediately() {
    let session_dir = tempdir("eager-thread");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-eager-thread".into());

    bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: Some(session_dir.clone()),
                env: vec![],
            },
            sink as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                create_thread_if_missing: true,
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("fresh bridge launch should create its initial thread");

    assert_eq!(
        bridge.transport().bound_thread_id(&session).as_deref(),
        Some("fake-thread"),
        "a fresh headed session must accept Nexus delivery before a human types in the TUI"
    );

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(session_dir);
}

#[tokio::test]
async fn launch_reports_discovered_thread_id_for_persistence() {
    let session_dir = tempdir("thread-callback");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-thread-callback".into());
    let seen = Arc::new(tokio::sync::Mutex::new(Vec::<(SessionId, String)>::new()));
    let seen_cb = seen.clone();

    let _sock_path = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: None,
                on_thread_discovered: Some(Arc::new(move |session, thread_id| {
                    let seen = seen_cb.clone();
                    Box::pin(async move {
                        seen.lock().await.push((session, thread_id));
                    })
                })),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("bridge launch should start fake app-server");

    write_rollout(&session_dir, "persist-me-thread");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let got = seen.lock().await.clone();
        if got == vec![(session.clone(), "persist-me-thread".to_string())] {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for thread persistence callback; got {got:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
}

#[tokio::test]
async fn launch_with_known_thread_rebinds_existing_bound_session_when_thread_differs() {
    let session_dir = tempdir("known-rebind");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-known-rebind".into());

    let sock_path = bridge
        .launch(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
        )
        .await
        .expect("bridge launch should start fake app-server");

    write_rollout(&session_dir, "thread-one");

    let transport = bridge.transport();
    let first_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while transport.bound_thread_id(&session).as_deref() != Some("thread-one") {
        assert!(
            tokio::time::Instant::now() < first_deadline,
            "bridge did not bind initial thread"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    write_rollout(&session_dir, "thread-two");
    let resumed_sock_path = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: Some("thread-two".to_string()),
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("known-thread launch should reuse the live app-server");

    assert_eq!(
        resumed_sock_path, sock_path,
        "same app-server socket is reused"
    );

    assert_eq!(
        transport.bound_thread_id(&session).as_deref(),
        Some("thread-two"),
        "known-thread launch must not return before rebinding an existing live bridge"
    );

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
}

#[tokio::test]
async fn launch_with_known_thread_id_binds_from_existing_codex_home() {
    let session_dir = tempdir("known-thread");
    let external_session_dir = tempdir("known-thread-source");
    let external_codex_home = external_session_dir.join("codex-home");
    write_rollout(&external_session_dir, "known-thread");
    let sink = Arc::new(RecSink::default());
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-known-thread".into());

    let _sock_path = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![],
            },
            sink.clone() as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: Some("known-thread".to_string()),
                resume_codex_homes: vec![external_codex_home],
                on_thread_discovered: None,
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("bridge launch should start fake app-server");

    let transport = bridge.transport();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !transport.is_bound(&session) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "bridge did not bind known thread without rollout"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
    let _ = std::fs::remove_dir_all(&external_session_dir);
}

#[tokio::test]
async fn launch_with_known_thread_returns_only_after_transport_binding() {
    let session_dir = tempdir("known-thread-ready");
    let external_session_dir = tempdir("known-thread-ready-source");
    let external_codex_home = external_session_dir.join("codex-home");
    write_rollout(&external_session_dir, "known-thread-ready");
    let bridge = CodexBridge::new();
    let session = SessionId("test-codex-known-thread-ready".into());

    let _sock_path = bridge
        .launch_with_options(
            session.clone(),
            SupervisorOpts {
                codex_exe: FAKE_BIN.to_string(),
                session_dir: session_dir.clone(),
                codex_home: Some(session_dir.join("codex-home")),
                model: None,
                bus_mcp: None,
                cwd: None,
                env: vec![("FAKE_CODEX_RESUME_DELAY_MS".into(), "250".into())],
            },
            Arc::new(RecSink::default()) as Arc<dyn EventSink>,
            BridgeLaunchOptions {
                known_thread_id: Some("known-thread-ready".to_string()),
                resume_codex_homes: vec![external_codex_home],
                ..BridgeLaunchOptions::default()
            },
        )
        .await
        .expect("known-thread launch should bind before returning");

    assert_eq!(
        bridge.transport().bound_thread_id(&session).as_deref(),
        Some("known-thread-ready"),
        "known-thread launch returned before publishing the exact transport binding"
    );

    assert!(bridge.kill(&session));
    let _ = std::fs::remove_dir_all(&session_dir);
    let _ = std::fs::remove_dir_all(&external_session_dir);
}
