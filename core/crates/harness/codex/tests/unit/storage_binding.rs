use super::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Default)]
struct Pause {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

fn pauses() -> &'static Mutex<HashMap<(String, String), Arc<Pause>>> {
    static PAUSES: OnceLock<Mutex<HashMap<(String, String), Arc<Pause>>>> = OnceLock::new();
    PAUSES.get_or_init(Mutex::default)
}

pub(super) async fn after_thread_claim(session: &SessionId, thread: &str) {
    let pause = pauses()
        .lock()
        .unwrap()
        .remove(&(session.0.clone(), thread.to_string()));
    if let Some(pause) = pause {
        pause.entered.notify_one();
        pause.release.notified().await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn deferred_callback_write_finishes_before_replacement_publication() {
    use crate::{BridgeLaunchOptions, CodexBridge, SupervisorOpts, ThreadDiscovered};
    use nexus_contracts::{EventSink, WsEvent};
    use nexus_store::repos::{NewSession, Sessions};
    struct Sink;
    #[async_trait::async_trait]
    impl EventSink for Sink {
        async fn emit(&self, _: WsEvent) {}
    }
    let (socket, _) =
        crate::app_server::transport::test_support::spawn_fake_server("callback", "inside-write")
            .await;
    let dir = socket.with_extension("dir");
    let home = dir.join("home");
    let rollouts = home.join("sessions");
    std::fs::create_dir_all(&rollouts).unwrap();
    std::os::unix::fs::symlink(&socket, dir.join("codex.sock")).unwrap();
    for thread in ["old-thread", "new-thread"] {
        std::fs::write(
            rollouts.join(format!("rollout-{thread}.jsonl")),
            format!("{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{thread}\"}}}}\n"),
        )
        .unwrap();
    }
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("callback-inside-write".into());
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: None,
            agent: Some("codex".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: Some(session.0.clone()),
            cwd: None,
            project: "default".into(),
            transport: Some("pty".into()),
        })
        .await
        .unwrap();
    let bridge = CodexBridge::new();
    let pause = Arc::new(Pause::default());
    let new_written = Arc::new(tokio::sync::Notify::new());
    let callback: ThreadDiscovered = Arc::new({
        let store = store.clone();
        let pause = pause.clone();
        let new_written = new_written.clone();
        move |session, thread| {
            let store = store.clone();
            let pause = pause.clone();
            let new_written = new_written.clone();
            Box::pin(async move {
                if thread == "old-thread" {
                    pause.entered.notify_one();
                    pause.release.notified().await;
                }
                Sessions::new(&store)
                    .set_harness_session_id(&session, &thread)
                    .await
                    .unwrap();
                if thread == "new-thread" {
                    new_written.notify_one();
                }
            })
        }
    });
    let opts = SupervisorOpts {
        codex_exe: "unused-adopted".into(),
        session_dir: dir.clone(),
        codex_home: Some(home),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![],
    };
    bridge
        .launch_with_options(
            session.clone(),
            opts.clone(),
            Arc::new(Sink),
            BridgeLaunchOptions {
                known_thread_id: Some("old-thread".into()),
                runtime_store: Some(store.clone()),
                on_thread_discovered: Some(callback.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    pause.entered.notified().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let mut new = tokio::spawn({
        let bridge = bridge.clone();
        let session = session.clone();
        let store = store.clone();
        let entered = entered.clone();
        async move {
            // No await separates this entry signal from the actual setup-exclusion attempt.
            entered.notify_one();
            bridge
                .launch_with_options(
                    session,
                    opts,
                    Arc::new(Sink),
                    BridgeLaunchOptions {
                        known_thread_id: Some("new-thread".into()),
                        runtime_store: Some(store),
                        on_thread_discovered: Some(callback),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    entered.notified().await;
    let early = tokio::time::timeout(std::time::Duration::from_millis(100), &mut new).await;
    let overtook = early.is_ok();
    pause.release.notify_one();
    if let Ok(result) = early {
        result.unwrap().unwrap();
    } else {
        new.await.unwrap().unwrap();
    }
    new_written.notified().await;
    let row = Sessions::new(&store)
        .find_by_session_id(&session)
        .await
        .unwrap()
        .unwrap();
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(dir);
    assert!(
        !overtook,
        "NEW published while OLD's deferred persistence callback was writing"
    );
    assert_eq!(row.harness_session_id.as_deref(), Some("new-thread"));
}

#[cfg(unix)]
#[tokio::test]
async fn sidecar_write_already_inside_helper_finishes_before_replacement_publication() {
    use crate::{BridgeLaunchOptions, CodexBridge, SupervisorOpts};
    use nexus_contracts::{EventSink, WsEvent};
    struct Sink;
    #[async_trait::async_trait]
    impl EventSink for Sink {
        async fn emit(&self, _: WsEvent) {}
    }
    let (socket, _) =
        crate::app_server::transport::test_support::spawn_fake_server("sidecar", "inside-write")
            .await;
    let dir = socket.with_extension("dir");
    let home = dir.join("home");
    let rollouts = home.join("sessions");
    std::fs::create_dir_all(&rollouts).unwrap();
    std::os::unix::fs::symlink(&socket, dir.join("codex.sock")).unwrap();
    for thread in ["old-thread", "new-thread"] {
        std::fs::write(
            rollouts.join(format!("rollout-{thread}.jsonl")),
            format!("{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{thread}\"}}}}\n"),
        )
        .unwrap();
    }
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let bridge = CodexBridge::new();
    let session = SessionId("sidecar-inside-helper".into());
    let pause = Arc::new(Pause::default());
    pauses()
        .lock()
        .unwrap()
        .insert((session.0.clone(), "old-thread".into()), pause.clone());
    let opts = SupervisorOpts {
        codex_exe: "unused-adopted".into(),
        session_dir: dir.clone(),
        codex_home: Some(home),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: vec![],
    };
    let old = tokio::spawn({
        let bridge = bridge.clone();
        let session = session.clone();
        let opts = opts.clone();
        let store = store.clone();
        async move {
            bridge
                .launch_with_options(
                    session,
                    opts,
                    Arc::new(Sink),
                    BridgeLaunchOptions {
                        known_thread_id: Some("old-thread".into()),
                        runtime_store: Some(store),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), pause.entered.notified())
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let mut new = tokio::spawn({
        let bridge = bridge.clone();
        let session = session.clone();
        let store = store.clone();
        let entered = entered.clone();
        async move {
            entered.notify_one();
            bridge
                .launch_with_options(
                    session,
                    opts,
                    Arc::new(Sink),
                    BridgeLaunchOptions {
                        known_thread_id: Some("new-thread".into()),
                        runtime_store: Some(store),
                        ..Default::default()
                    },
                )
                .await
        }
    });
    entered.notified().await;
    let early = tokio::time::timeout(std::time::Duration::from_millis(100), &mut new).await;
    let overtook = early.is_ok();
    pause.release.notify_one();
    let _ = old.await.unwrap();
    if let Ok(result) = early {
        result.unwrap().unwrap();
    } else {
        new.await.unwrap().unwrap();
    }
    let row = CodexRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bridge.transport().bound_thread_id(&session).as_deref(),
        Some("new-thread")
    );
    bridge.kill(&session);
    let _ = std::fs::remove_dir_all(dir);
    assert!(
        !overtook,
        "NEW published while OLD was still inside the actual sidecar helper"
    );
    assert_eq!(row.codex_thread_id.as_deref(), Some("new-thread"));
}
