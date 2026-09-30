use std::io::Write;
use std::sync::Arc;

use nexus::daemon::transcript_archive::{
    archive_codex_once, archive_file_once, ArchiveFileRequest, ArchiveOutcome,
};
use nexus::daemon::WsSink;
use nexus_contracts::{AgentUpdateKind, EventSink, SessionId, WsEvent};
use nexus_harness_codex::storage::{CodexRuntimeLaunch, CodexRuntimeStateRepo};
use nexus_store::repos::transcript_archive::TranscriptArchive;
use nexus_store::repos::{
    AgentRuntimes, NewAgentRuntime, NewSession, Sessions, TranscriptArchiveRow,
};
use nexus_store::Store;
use serde_json::json;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-transcript-archive-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn seed_codex_session(store: &Store, session: &SessionId, agent_id: &str) {
    Sessions::new(store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("codex-archive".to_string()),
            agent: Some("codex".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: Some("codex-thread".to_string()),
            client_key: Some(session.0.clone()),
            cwd: None,
            project: "default".to_string(),
            transport: Some("codex-appserver".to_string()),
        })
        .await
        .unwrap();
    AgentRuntimes::new(store)
        .create(NewAgentRuntime {
            runtime_id: session.0.clone(),
            agent_id: agent_id.to_string(),
            harness: "codex".to_string(),
            cwd: None,
            transport: Some("codex-appserver".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
}

async fn wait_for_archive_progress(
    store: &Store,
    runtime_id: &str,
    expected_bytes: i64,
    expected_last_event: &str,
) -> TranscriptArchiveRow {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut last_seen = None;
    loop {
        if let Some(row) = TranscriptArchive::new(store)
            .find_by_runtime_id(runtime_id)
            .await
            .unwrap()
        {
            if row.bytes_archived == expected_bytes
                && row.last_event.as_deref() == Some(expected_last_event)
            {
                return row;
            }
            last_seen = Some(row);
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for transcript archive progress for {runtime_id}; last_seen={last_seen:?}"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn cleanup_archive(path: &str) {
    let path = std::path::PathBuf::from(path);
    let _ = std::fs::remove_file(&path);
    if let Some(parent) = path.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn archive_codex_once_skips_non_codex_runtime_sidecar() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_non_codex_sidecar".to_string());
    let dir = temp_dir("non-codex");
    let rollout = dir.join("rollout.jsonl");
    std::fs::write(&rollout, r#"{"msg":"must not archive"}"#).unwrap();
    Sessions::new(&store)
        .create(NewSession {
            session_id: session.clone(),
            name: Some("hugo".to_string()),
            agent: Some("claude".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: Some("claude-native".to_string()),
            client_key: Some("s_non_codex_sidecar".to_string()),
            cwd: None,
            project: "default".to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: session.0.clone(),
            agent_id: "a_hugo".to_string(),
            harness: "claude".to_string(),
            cwd: None,
            transport: Some("pty".to_string()),
            presence: Some("online".to_string()),
            active: true,
        })
        .await
        .unwrap();
    let codex = CodexRuntimeStateRepo::new(&store);
    codex
        .upsert_launch(CodexRuntimeLaunch {
            runtime_id: session.clone(),
            codex_thread_id: None,
            codex_home: dir.join("codex-home"),
            app_server_sock: dir.join("codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: true,
        })
        .await
        .unwrap();
    codex
        .set_thread(&session, "codex-thread-stale", Some(rollout))
        .await
        .unwrap();

    let outcome = archive_codex_once(store.clone(), &session, Some("release".to_string()))
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ArchiveOutcome::Skipped {
            reason: "non_codex_runtime".to_string()
        }
    );
    assert!(TranscriptArchive::new(&store)
        .find_by_runtime_id(&session.0)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn ws_sink_turn_end_archives_codex_rollout_tail() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let session = SessionId("s_codex_turn_end_archive".to_string());
    let dir = temp_dir("codex-turn-end");
    let rollout = dir.join("rollout.jsonl");
    let body = r#"{"type":"response_item","text":"codex turn end archive"}"#;
    std::fs::write(&rollout, body).unwrap();
    seed_codex_session(&store, &session, "a_codex_turn_end").await;
    let codex = CodexRuntimeStateRepo::new(&store);
    codex
        .upsert_launch(CodexRuntimeLaunch {
            runtime_id: session.clone(),
            codex_thread_id: None,
            codex_home: dir.join("codex-home"),
            app_server_sock: dir.join("codex.sock"),
            app_server_pid: None,
            mcp_sidecar_pids_json: None,
            app_server_adopted: true,
        })
        .await
        .unwrap();
    codex
        .set_thread(&session, "codex-thread-turn-end", Some(rollout.clone()))
        .await
        .unwrap();

    WsSink::new(16, Some(store.clone()))
        .emit(WsEvent::AgentUpdate {
            session_id: session.clone(),
            kind: AgentUpdateKind::TurnEnd,
            data: json!({}),
        })
        .await;

    let archived = wait_for_archive_progress(&store, &session.0, body.len() as i64, "Stop").await;
    assert_eq!(archived.harness, "codex");
    assert_eq!(archived.source_kind, "file");
    assert_eq!(archived.source_path, rollout.to_string_lossy());
    assert_eq!(archived.bytes_archived, body.len() as i64);
    assert_eq!(archived.last_event.as_deref(), Some("Stop"));
    assert_eq!(
        std::fs::read_to_string(&archived.archive_path).unwrap(),
        body
    );
    cleanup_archive(&archived.archive_path);
}

#[tokio::test]
async fn archive_file_once_appends_tail_without_duplication() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let dir = temp_dir("append");
    let source = dir.join("source.jsonl");
    let archive = dir.join("archive.jsonl");
    std::fs::write(&source, "one\n").unwrap();

    let first = archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_append".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source.clone(),
        archive_path: archive.clone(),
        last_event: Some("SessionStart".to_string()),
    })
    .await
    .unwrap();
    assert_eq!(first, ArchiveOutcome::Advanced { bytes_archived: 4 });
    assert_eq!(std::fs::read_to_string(&archive).unwrap(), "one\n");

    std::fs::write(&source, "one\ntwo\n").unwrap();
    let second = archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_append".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source.clone(),
        archive_path: archive.clone(),
        last_event: Some("Stop".to_string()),
    })
    .await
    .unwrap();
    assert_eq!(second, ArchiveOutcome::Advanced { bytes_archived: 8 });

    let third = archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_append".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source.clone(),
        archive_path: archive.clone(),
        last_event: Some("Stop".to_string()),
    })
    .await
    .unwrap();
    assert_eq!(third, ArchiveOutcome::Unchanged { bytes_archived: 8 });
    assert_eq!(std::fs::read_to_string(&archive).unwrap(), "one\ntwo\n");
}

#[tokio::test]
async fn archive_file_once_resets_cursor_when_source_path_switches() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let dir = temp_dir("source-switch");
    let placeholder = dir.join("bridge-transcript.jsonl");
    let real = dir.join("real-transcript.jsonl");
    let archive = dir.join("archive.jsonl");
    std::fs::write(&placeholder, "placeholder\n").unwrap();
    std::fs::write(&real, "real-one\nreal-two\n").unwrap();

    archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_source_switch".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: placeholder,
        archive_path: archive.clone(),
        last_event: Some("SessionStart".to_string()),
    })
    .await
    .unwrap();

    let switched = archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_source_switch".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: real.clone(),
        archive_path: archive.clone(),
        last_event: Some("Stop".to_string()),
    })
    .await
    .unwrap();

    assert_eq!(
        switched,
        ArchiveOutcome::Advanced {
            bytes_archived: "real-one\nreal-two\n".len() as i64
        }
    );
    assert_eq!(
        std::fs::read_to_string(&archive).unwrap(),
        "placeholder\nreal-one\nreal-two\n"
    );
    let row = TranscriptArchive::new(&store)
        .find_by_runtime_id("s_archive_source_switch")
        .await
        .unwrap()
        .expect("source switch archive row");
    assert_eq!(row.source_path, real.to_string_lossy());
    assert_eq!(row.archive_offset, "placeholder\n".len() as i64);
    assert_eq!(row.seal_reason, None);
}

#[tokio::test]
async fn archive_file_once_seals_when_archive_file_no_longer_matches_manifest() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let dir = temp_dir("archive-tamper");
    let source = dir.join("source.jsonl");
    let archive = dir.join("archive.jsonl");
    std::fs::write(&source, "one\n").unwrap();

    archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_file_tamper".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source.clone(),
        archive_path: archive.clone(),
        last_event: Some("SessionStart".to_string()),
    })
    .await
    .unwrap();

    std::fs::remove_file(&archive).unwrap();
    let outcome = archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_file_tamper".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source,
        archive_path: archive,
        last_event: Some("Stop".to_string()),
    })
    .await
    .unwrap();

    assert_eq!(
        outcome,
        ArchiveOutcome::Sealed {
            reason: "fork_or_tamper".to_string()
        }
    );
}

#[tokio::test]
async fn archive_file_once_recovers_if_append_landed_before_progress_update() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let dir = temp_dir("crash-recover");
    let source = dir.join("source.jsonl");
    let archive = dir.join("archive.jsonl");
    std::fs::write(&source, "one\n").unwrap();

    archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_crash_recover".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source.clone(),
        archive_path: archive.clone(),
        last_event: Some("SessionStart".to_string()),
    })
    .await
    .unwrap();

    std::fs::write(&source, "one\ntwo\n").unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&archive)
        .unwrap()
        .write_all(b"two\n")
        .unwrap();

    let outcome = archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_crash_recover".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source,
        archive_path: archive.clone(),
        last_event: Some("Stop".to_string()),
    })
    .await
    .unwrap();

    assert_eq!(outcome, ArchiveOutcome::Unchanged { bytes_archived: 8 });
    assert_eq!(std::fs::read_to_string(&archive).unwrap(), "one\ntwo\n");
    let row = TranscriptArchive::new(&store)
        .find_by_runtime_id("s_archive_crash_recover")
        .await
        .unwrap()
        .expect("crash recovery archive row");
    assert_eq!(row.bytes_archived, 8);
    assert_eq!(row.last_event.as_deref(), Some("Stop"));
}

#[tokio::test]
async fn archive_file_once_serializes_concurrent_passes_for_runtime() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let dir = temp_dir("concurrent");
    let source = dir.join("source.jsonl");
    let archive = dir.join("archive.jsonl");
    std::fs::write(&source, "one\n").unwrap();
    let req = ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_concurrent".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source.clone(),
        archive_path: archive.clone(),
        last_event: Some("SessionStart".to_string()),
    };

    let (first, second) = tokio::join!(archive_file_once(req.clone()), archive_file_once(req));

    assert!(first.is_ok(), "first archive pass failed: {first:?}");
    assert!(second.is_ok(), "second archive pass failed: {second:?}");
    assert_eq!(std::fs::read_to_string(&archive).unwrap(), "one\n");
    let row = TranscriptArchive::new(&store)
        .find_by_runtime_id("s_archive_concurrent")
        .await
        .unwrap()
        .expect("concurrent archive row");
    assert_eq!(row.bytes_archived, 4);
}

#[tokio::test]
async fn archive_file_once_seals_on_prefix_mismatch_or_truncation() {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let dir = temp_dir("seal");
    let source = dir.join("source.jsonl");
    let archive = dir.join("archive.jsonl");
    std::fs::write(&source, "one\ntwo\n").unwrap();

    archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_tamper".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source.clone(),
        archive_path: archive.clone(),
        last_event: Some("SessionStart".to_string()),
    })
    .await
    .unwrap();

    std::fs::write(&source, "ONE\ntwo\nthree\n").unwrap();
    let tampered = archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_tamper".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: source.clone(),
        archive_path: archive.clone(),
        last_event: Some("PostCompact".to_string()),
    })
    .await
    .unwrap();
    assert_eq!(
        tampered,
        ArchiveOutcome::Sealed {
            reason: "fork_or_tamper".to_string()
        }
    );
    assert_eq!(std::fs::read_to_string(&archive).unwrap(), "one\ntwo\n");
    let row = TranscriptArchive::new(&store)
        .find_by_runtime_id("s_archive_tamper")
        .await
        .unwrap()
        .expect("tamper archive row");
    assert_eq!(row.seal_reason.as_deref(), Some("fork_or_tamper"));

    let truncated_source = dir.join("truncated-source.jsonl");
    let truncated_archive = dir.join("truncated-archive.jsonl");
    std::fs::write(&truncated_source, "alpha\nbeta\n").unwrap();
    archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_truncate".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: truncated_source.clone(),
        archive_path: truncated_archive.clone(),
        last_event: Some("SessionStart".to_string()),
    })
    .await
    .unwrap();
    std::fs::write(&truncated_source, "alpha\n").unwrap();
    let truncated = archive_file_once(ArchiveFileRequest {
        store: store.clone(),
        runtime_id: "s_archive_truncate".to_string(),
        agent_id: Some("a_robin".to_string()),
        agent_name: "robin".to_string(),
        project: "default".to_string(),
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path: truncated_source,
        archive_path: truncated_archive,
        last_event: Some("Stop".to_string()),
    })
    .await
    .unwrap();
    assert_eq!(
        truncated,
        ArchiveOutcome::Sealed {
            reason: "fork_or_tamper".to_string()
        }
    );
}
