//! Claude subagent isolation through the real forward pass: sidechain rows never enter the
//! parent lane, `subagents/agent-*.jsonl` files are tailed into the child lane behind durable
//! epoch-scoped cursors, and a cursor from another daemon epoch declares unknown coverage and
//! halts instead of skipping or replaying.
//!
//! Also pinned: least-recently-served rotation past the per-pass file budget, child failures
//! isolated from the parent's pass, generation/continuity checks (replacement, truncation,
//! truncate-and-regrow, absent or oversized first uuid), and halted cursors that keep their real
//! resolution and re-declare coverage under each real store reopen epoch.
//!
//! Run: `cargo test -p nexus-harness-claude --test native_child_streams`

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus_common::now;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ports::EventSink;
use nexus_contracts::{AgentUpdateKind, ChildResolution, SessionId};
use nexus_harness_claude::native::bridge::{write_launch_settings, ClaudeNativeBridgePaths};
use nexus_harness_claude::native::forwarder::forward_once;
use nexus_harness_claude::storage::{
    ClaudeChildStreamsRepo, ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo,
};
use nexus_store::repos::{
    ChildStreamBounds, ChildStreamEvents, DaemonState, LaneFilter, StreamEvents,
};
use nexus_store::Store;
use serde_json::json;

const ROOT: &str = "native-root";

fn temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-claude-children-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        label,
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

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

impl CaptureSink {
    fn parent(&self) -> Vec<(AgentUpdateKind, serde_json::Value)> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                WsEvent::AgentUpdate { kind, data, .. } => Some((*kind, data.clone())),
                _ => None,
            })
            .collect()
    }

    fn children(
        &self,
    ) -> Vec<(
        nexus_contracts::ChildStream,
        AgentUpdateKind,
        String,
        serde_json::Value,
    )> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                WsEvent::ChildAgentUpdate {
                    child,
                    kind,
                    source_ref,
                    data,
                    ..
                } => Some((child.clone(), *kind, source_ref.clone(), data.clone())),
                _ => None,
            })
            .collect()
    }
}

struct Fixture {
    store: Arc<Store>,
    session: SessionId,
    paths: ClaudeNativeBridgePaths,
    sink: CaptureSink,
    project: std::path::PathBuf,
    transcript: std::path::PathBuf,
    /// The durable store file when the fixture is file-backed (reopen tests), else `None`.
    store_file: Option<std::path::PathBuf>,
}

/// A runtime whose hook log has named the real transcript under `project`, discovered by one
/// forward pass so the parent tail starts at that file's current end.
async fn fixture(label: &str) -> Fixture {
    fixture_with(label, None, ChildStreamBounds::default()).await
}

/// A file-backed fixture whose store can be reopened like a daemon restart.
async fn file_fixture(label: &str) -> Fixture {
    let file = temp_dir(&format!("{label}-store")).join("state.db");
    fixture_with(label, Some(file), ChildStreamBounds::default()).await
}

async fn open_store(file: Option<&std::path::Path>, bounds: ChildStreamBounds) -> Arc<Store> {
    let location = file
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_else(|| ":memory:".into());
    let store = Arc::new(Store::open(&location).await.unwrap());
    store.migrate().await.unwrap();
    store.configure_child_stream_bounds(bounds);
    store
}

impl Fixture {
    /// A daemon restart: a fresh `Store` over the same durable file (its volatile `mem` lane is
    /// new and empty) booted under `epoch`.
    async fn reopen(&mut self, epoch: &str) {
        let file = self.store_file.clone().expect("file-backed fixture");
        self.store = open_store(Some(&file), ChildStreamBounds::default()).await;
        DaemonState::new(&self.store)
            .set_boot_epoch(epoch, now())
            .await
            .unwrap();
    }
}

async fn fixture_with(
    label: &str,
    store_file: Option<std::path::PathBuf>,
    bounds: ChildStreamBounds,
) -> Fixture {
    let store = open_store(store_file.as_deref(), bounds).await;
    DaemonState::new(&store)
        .set_boot_epoch("boot_a", now())
        .await
        .unwrap();
    let session = SessionId(format!("s_{label}"));
    let paths = ClaudeNativeBridgePaths::new(&temp_dir(label), &session);
    write_launch_settings(&paths, "blake", "nexus", "/usr/bin/nexus").unwrap();
    ClaudeRuntimeStateRepo::new(&store)
        .upsert_launch(ClaudeRuntimeLaunch {
            runtime_id: session.clone(),
            bridge_dir: paths.bridge_dir.clone(),
            claude_session_id: None,
            launch_cwd: temp_dir(&format!("{label}-cwd")),
            transcript_path: Some(paths.bridge_dir.join("transcript.jsonl")),
            bridge_pid: None,
            hook_pids_json: None,
        })
        .await
        .unwrap();
    let project = temp_dir(&format!("{label}-project"));
    let transcript = project.join(format!("{ROOT}.jsonl"));
    std::fs::write(&transcript, "").unwrap();
    std::fs::write(
        &paths.hook_log_path,
        json!({"event": "SessionStart", "session_id": ROOT, "transcript_path": transcript})
            .to_string(),
    )
    .unwrap();
    let sink = CaptureSink::default();
    forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();
    assert!(sink.parent().is_empty());
    assert!(sink.children().is_empty());
    Fixture {
        store,
        session,
        paths,
        sink,
        project,
        transcript,
        store_file,
    }
}

fn root_row(uuid: &str, text: &str) -> String {
    format!(
        "{}\n",
        json!({"type": "assistant", "isSidechain": false, "sessionId": ROOT, "uuid": uuid,
               "message": {"id": format!("m-{uuid}"), "type": "message", "role": "assistant",
                           "content": [{"type": "text", "text": text}]}})
    )
}

async fn cursor_of(fx: &Fixture, agent: &str) -> nexus_harness_claude::storage::ClaudeChildCursor {
    ClaudeChildStreamsRepo::new(&fx.store)
        .find(&fx.session, ROOT, agent)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("cursor for {agent}"))
}

async fn lane_of(fx: &Fixture, agent: &str) -> Option<nexus_store::repos::LaneSummary> {
    ChildStreamEvents::new(&fx.store)
        .lanes(&fx.session, None, 100)
        .await
        .unwrap()
        .lanes
        .into_iter()
        .find(|l| l.child_key == format!("n:{agent}"))
}

fn child_row(agent: &str, uuid: &str, parent: Option<&str>, body: serde_json::Value) -> String {
    let mut row = json!({
        "type": body["role"].as_str().unwrap_or("assistant"),
        "isSidechain": true,
        "agentId": agent,
        "sessionId": ROOT,
        "uuid": uuid,
        "parentUuid": parent,
        "message": body,
    });
    if body.get("role").and_then(|r| r.as_str()) == Some("assistant") {
        row["message"]["type"] = json!("message");
    }
    format!("{row}\n")
}

fn child_file(fx: &Fixture, agent: &str) -> std::path::PathBuf {
    let dir = fx.project.join(ROOT).join("subagents");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("agent-{agent}.jsonl"))
}

fn append(path: &std::path::Path, text: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .unwrap();
    file.write_all(text.as_bytes()).unwrap();
}

async fn pass(fx: &Fixture) -> nexus_harness_claude::native::forwarder::ClaudeForwarderStats {
    forward_once(
        fx.store.clone(),
        fx.session.clone(),
        fx.paths.clone(),
        Arc::new(fx.sink.clone()),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn subagent_files_go_to_the_child_lane_and_sidechain_rows_never_reach_the_parent() {
    let fx = fixture("children").await;
    let abc = child_file(&fx, "abc");
    append(
        &abc,
        &child_row(
            "abc",
            "u1",
            None,
            json!({"role": "user", "content": "do the thing"}),
        ),
    );
    append(
        &abc,
        &child_row(
            "abc",
            "u2",
            Some("u1"),
            json!({"role": "assistant", "id": "m1", "content": [
                {"type": "text", "text": "working"},
                {"type": "tool_use", "id": "t1", "name": "Read", "input": {"f": "x"}}
            ]}),
        ),
    );
    append(
        &abc,
        &child_row(
            "abc",
            "u3",
            Some("u2"),
            json!({"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": "ok"}
            ]}),
        ),
    );
    append(
        &abc,
        &child_row(
            "abc",
            "u4",
            Some("u3"),
            json!({"role": "assistant", "id": "m2", "content": [{"type": "text", "text": "done"}], "stop_reason": "end_turn"}),
        ),
    );
    // The root transcript gains its own row and a stray sidechain row Claude wrote into it.
    append(
        &fx.transcript,
        &format!(
            "{}\n{}\n",
            json!({"type": "assistant", "isSidechain": false, "sessionId": ROOT, "uuid": "r1",
                   "message": {"id": "rm1", "type": "message", "role": "assistant",
                               "content": [{"type": "text", "text": "root says"}]}}),
            json!({"type": "assistant", "isSidechain": true, "agentId": "zzz", "sessionId": ROOT, "uuid": "r2",
                   "message": {"id": "zm1", "type": "message", "role": "assistant",
                               "content": [{"type": "text", "text": "stray child"}]}}),
        ),
    );

    let stats = pass(&fx).await;
    // Parent lane: exactly the root's own text.
    let parent = fx.sink.parent();
    assert_eq!(parent.len(), 1, "{parent:?}");
    assert_eq!(parent[0].0, AgentUpdateKind::Text);
    assert_eq!(parent[0].1["text"], "root says");
    assert_eq!(stats.text_events, 1);

    // Child lane: the subagent file's six events in order, all root-verified for agent abc.
    let children = fx.sink.children();
    let abc_events: Vec<_> = children
        .iter()
        .filter(|(c, ..)| c.id.as_deref() == Some("abc"))
        .collect();
    assert_eq!(
        abc_events.iter().map(|(_, k, ..)| *k).collect::<Vec<_>>(),
        vec![
            AgentUpdateKind::UserInput,
            AgentUpdateKind::Text,
            AgentUpdateKind::ToolCall,
            AgentUpdateKind::ToolCall,
            AgentUpdateKind::Text,
            AgentUpdateKind::TurnEnd,
        ]
    );
    for (child, _, source_ref, _) in &abc_events {
        assert_eq!(child.harness, "claude");
        assert_eq!(child.root, ROOT);
        assert_eq!(child.parent, None, "no immediate parent is claimed");
        assert_eq!(child.depth, None, "no depth is claimed");
        assert_eq!(child.resolution, ChildResolution::RootVerified);
        assert_eq!(
            child.evidence.as_deref(),
            Some("subagents_dir+sessionId+agentId")
        );
        assert_eq!(child.locator, "claude:subagents/agent-abc.jsonl@u1");
        assert!(source_ref.starts_with("claude:abc@u1#"), "{source_ref}");
    }
    assert_eq!(abc_events[0].3["text"], "do the thing");
    assert_eq!(abc_events[1].3["text"], "working");
    assert_eq!(abc_events[2].3["id"], "t1");
    assert_eq!(abc_events[2].3["status"], "in_progress");
    assert_eq!(abc_events[3].3["status"], "completed");
    assert_eq!(abc_events[4].3["text"], "done");
    // The stray sidechain row of the root transcript is a child event, not parent text.
    let stray: Vec<_> = children
        .iter()
        .filter(|(c, ..)| c.id.as_deref() == Some("zzz"))
        .collect();
    assert_eq!(stray.len(), 1);
    assert_eq!(stray[0].1, AgentUpdateKind::Text);
    assert_eq!(stray[0].3["text"], "stray child");
    assert_eq!(stray[0].0.resolution, ChildResolution::RootVerified);
    assert_eq!(
        stray[0].0.evidence.as_deref(),
        Some("main_transcript_sidechain+sessionId+agentId")
    );
    assert_eq!(stats.child_events, 7);

    // A second pass forwards nothing new: the child cursor is durable.
    let before = fx.sink.children().len();
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    assert_eq!(fx.sink.children().len(), before);
    assert_eq!(fx.sink.parent().len(), 1);
    let cursors = ClaudeChildStreamsRepo::new(&fx.store)
        .list(&fx.session)
        .await
        .unwrap();
    assert_eq!(cursors.len(), 1);
    assert_eq!(cursors[0].agent_id, "abc");
    assert_eq!(cursors[0].native_session_id, ROOT);
    assert_eq!(cursors[0].epoch, "boot_a");
    assert_eq!(cursors[0].generation, "u1");
    assert!(cursors[0].cursor > 0);
    assert!(!cursors[0].halted);
    assert_eq!(cursors[0].resolution, "root_verified");
    assert_eq!(
        cursors[0].anchor_uuid, "u4",
        "anchor is the last forwarded record"
    );
    assert!(cursors[0].anchor_offset < cursors[0].cursor);
    assert!(cursors[0].served_at > 0);
    assert_eq!(stats.child_errors, 0);

    // Nothing of the child reached the parent's stream lane through the sink either: the sink
    // here is a capture, so the parent lane check is the parent event list above.
    assert!(StreamEvents::new(&fx.store)
        .since(&fx.session, 0)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_cursor_from_another_epoch_declares_unknown_coverage_and_halts() {
    let fx = fixture("epoch").await;
    let abc = child_file(&fx, "abc");
    append(
        &abc,
        &child_row(
            "abc",
            "u1",
            None,
            json!({"role": "user", "content": "first"}),
        ),
    );
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 1);
    let cursor_after_first = ClaudeChildStreamsRepo::new(&fx.store)
        .list(&fx.session)
        .await
        .unwrap()[0]
        .cursor;

    // The daemon restarts under a new epoch; the file grows meanwhile.
    DaemonState::new(&fx.store)
        .set_boot_epoch("boot_b", now())
        .await
        .unwrap();
    append(
        &abc,
        &child_row(
            "abc",
            "u2",
            Some("u1"),
            json!({"role": "user", "content": "second"}),
        ),
    );
    let stats = pass(&fx).await;
    assert_eq!(
        stats.child_events, 0,
        "nothing is skipped or replayed without a restart recovery policy"
    );
    let cursors = ClaudeChildStreamsRepo::new(&fx.store)
        .list(&fx.session)
        .await
        .unwrap();
    assert!(cursors[0].halted);
    assert_eq!(cursors[0].halt_reason, "daemon_restart_policy_undecided");
    assert_eq!(cursors[0].epoch, "boot_b");
    assert_eq!(
        cursors[0].cursor, cursor_after_first,
        "the cursor did not advance"
    );
    let lanes = ChildStreamEvents::new(&fx.store)
        .lanes(&fx.session, None, 10)
        .await
        .unwrap()
        .lanes;
    let lane = lanes
        .iter()
        .find(|l| l.child_key == "n:abc")
        .expect("coverage declared on the child's lane");
    let coverage = lane.coverage.as_ref().unwrap();
    assert_eq!(coverage["unknown_before"], true);
    assert_eq!(coverage["generation"], "u1");
    assert_eq!(coverage["from"], cursor_after_first);
    assert_eq!(coverage["reason"], "daemon_restart_policy_undecided");
    assert_eq!(lane.child.resolution, ChildResolution::RootVerified);
    assert_eq!(lane.epoch, "boot_b");
    assert_eq!(lane.live_rows, 0);
    // A later pass stays halted: still nothing emitted, still the same cursor.
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    assert!(ChildStreamEvents::new(&fx.store)
        .page(&fx.session, &LaneFilter::default(), 0, 10)
        .await
        .unwrap()
        .rows
        .is_empty());
}

#[tokio::test]
async fn a_file_whose_records_disagree_with_its_name_is_unresolved() {
    let fx = fixture("mismatch").await;
    let bad = child_file(&fx, "bad");
    append(
        &bad,
        &child_row(
            "other",
            "u1",
            None,
            json!({"role": "assistant", "id": "m1", "content": [{"type": "text", "text": "who am i"}]}),
        ),
    );
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 1);
    let children = fx.sink.children();
    assert_eq!(children.len(), 1);
    let (child, kind, _, data) = &children[0];
    assert_eq!(*kind, AgentUpdateKind::Text);
    assert_eq!(data["text"], "who am i");
    assert_eq!(
        child.id.as_deref(),
        Some("bad"),
        "the lane is the file's native id"
    );
    assert_eq!(child.resolution, ChildResolution::Unresolved);
    assert_eq!(child.evidence, None);
    assert!(fx.sink.parent().is_empty());
    assert_eq!(cursor_of(&fx, "bad").await.resolution, "unresolved");
}

fn user_row(agent: &str, uuid: &str, text: &str) -> String {
    child_row(agent, uuid, None, json!({"role": "user", "content": text}))
}

#[tokio::test]
async fn rotation_serves_every_file_across_passes_while_the_parent_keeps_progressing() {
    let fx = fixture("rotation").await;
    let agents: Vec<String> = (0..40).map(|i| format!("a{i:02}")).collect();
    for agent in &agents {
        append(&child_file(&fx, agent), &user_row(agent, "u1", "hello"));
    }
    append(&fx.transcript, &root_row("r1", "root one"));

    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 32, "one pass serves the file budget");
    assert_eq!(stats.text_events, 1);
    assert_eq!(fx.sink.parent().len(), 1);
    let served_first: std::collections::BTreeSet<String> = fx
        .sink
        .children()
        .iter()
        .map(|(c, ..)| c.id.clone().unwrap())
        .collect();
    assert_eq!(served_first.len(), 32);
    let expected_first: std::collections::BTreeSet<String> = agents[..32].iter().cloned().collect();
    assert_eq!(served_first, expected_first, "name order breaks the tie");

    // Already-served files grow; the parent grows too.
    for agent in &agents[..8] {
        append(&child_file(&fx, agent), &user_row(agent, "u2", "again"));
    }
    append(&fx.transcript, &root_row("r2", "root two"));

    let stats = pass(&fx).await;
    // Never-served files first (8), then the least recently served by name (a00..a23), which
    // includes the 8 that grew: 8 + 8 events.
    assert_eq!(stats.child_events, 16, "{stats:?}");
    assert_eq!(stats.text_events, 1);
    assert_eq!(fx.sink.parent().len(), 2);
    let all: Vec<(String, String)> = fx
        .sink
        .children()
        .iter()
        .map(|(c, _, _, d)| {
            (
                c.id.clone().unwrap(),
                d["text"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let distinct: std::collections::BTreeSet<_> = all.iter().cloned().collect();
    assert_eq!(all.len(), distinct.len(), "no record is forwarded twice");
    let seen_ids: std::collections::BTreeSet<String> =
        all.iter().map(|(id, _)| id.clone()).collect();
    assert_eq!(seen_ids.len(), 40, "every file was served after two passes");
    assert_eq!(all.iter().filter(|(_, t)| t == "again").count(), 8);

    // Every file has a durable cursor and nothing is left to forward.
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    assert_eq!(stats.child_errors, 0);
    let cursors = ClaudeChildStreamsRepo::new(&fx.store)
        .list(&fx.session)
        .await
        .unwrap();
    assert_eq!(cursors.len(), 40);
    assert!(cursors.iter().all(|c| !c.halted && c.served_at > 0));
}

#[cfg(unix)]
#[tokio::test]
async fn a_child_file_failure_never_blocks_the_parent_or_the_other_children() {
    use std::os::unix::fs::PermissionsExt;
    let fx = fixture("isolation").await;
    let aaa = child_file(&fx, "aaa");
    append(&aaa, &user_row("aaa", "u1", "from aaa"));
    append(&child_file(&fx, "bbb"), &user_row("bbb", "u1", "from bbb"));
    append(&fx.transcript, &root_row("r1", "root once"));
    std::fs::set_permissions(&aaa, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&aaa).is_ok() {
        eprintln!("skipping: this user can read a mode-000 file");
        return;
    }

    let stats = pass(&fx).await;
    assert_eq!(stats.child_errors, 1, "{stats:?}");
    assert_eq!(stats.text_events, 1, "the parent's pass completed");
    assert_eq!(stats.child_events, 1, "the readable sibling was served");
    assert_eq!(fx.sink.parent().len(), 1);
    assert_eq!(fx.sink.children()[0].0.id.as_deref(), Some("bbb"));
    let cursors = ClaudeChildStreamsRepo::new(&fx.store)
        .list(&fx.session)
        .await
        .unwrap();
    assert_eq!(cursors.len(), 2, "the failed file is registered, untouched");
    let failed = cursors.iter().find(|c| c.agent_id == "aaa").unwrap();
    assert_eq!(failed.cursor, 0);
    assert!(!failed.halted);
    assert!(
        failed.served_at > 0,
        "the attempt is marked even though the read failed"
    );

    // The next pass: the parent is not re-emitted, the failed child recovers.
    std::fs::set_permissions(&aaa, std::fs::Permissions::from_mode(0o644)).unwrap();
    let stats = pass(&fx).await;
    assert_eq!(stats.child_errors, 0);
    assert_eq!(stats.text_events, 0);
    assert_eq!(stats.child_events, 1);
    assert_eq!(fx.sink.parent().len(), 1, "exactly once");
    let children = fx.sink.children();
    assert_eq!(children.len(), 2);
    assert_eq!(children[1].0.id.as_deref(), Some("aaa"));
    assert_eq!(children[1].3["text"], "from aaa");
    assert!(!cursor_of(&fx, "aaa").await.halted);
}

#[tokio::test]
async fn a_replaced_file_halts_with_source_replaced() {
    let fx = fixture("replaced").await;
    let abc = child_file(&fx, "abc");
    append(&abc, &user_row("abc", "u1", "first life"));
    assert_eq!(pass(&fx).await.child_events, 1);

    // The file is rewritten from scratch (same name, new first record), longer than before.
    std::fs::write(
        &abc,
        format!(
            "{}{}",
            user_row("abc", "v1", "second life"),
            user_row("abc", "v2", "second life continued")
        ),
    )
    .unwrap();
    let stats = pass(&fx).await;
    assert_eq!(
        stats.child_events, 0,
        "nothing of the new file is replayed or skipped"
    );
    let cursor = cursor_of(&fx, "abc").await;
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "source_replaced");
    assert_eq!(
        cursor.generation, "u1",
        "the cursor keeps its own generation"
    );
    let lane = lane_of(&fx, "abc").await.expect("coverage declared");
    let coverage = lane.coverage.unwrap();
    assert_eq!(coverage["reason"], "source_replaced");
    assert_eq!(coverage["generation"], "u1");
    assert_eq!(coverage["previous_generation"], "u1");
    assert_eq!(coverage["current_generation"], "v1");
    assert_eq!(coverage["unknown_before"], true);
    assert_eq!(coverage["from"], cursor.cursor);
    assert_eq!(pass(&fx).await.child_events, 0, "stays halted");
}

#[tokio::test]
async fn a_truncated_file_halts_and_a_regrown_one_stays_halted() {
    let fx = fixture("truncated").await;
    let abc = child_file(&fx, "abc");
    let first = user_row("abc", "u1", "one");
    append(&abc, &first);
    append(&abc, &user_row("abc", "u2", "two"));
    assert_eq!(pass(&fx).await.child_events, 2);
    let before = cursor_of(&fx, "abc").await;

    // Truncated back to its first record: shorter than the cursor.
    std::fs::write(&abc, &first).unwrap();
    assert_eq!(pass(&fx).await.child_events, 0);
    let cursor = cursor_of(&fx, "abc").await;
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "source_truncated");
    assert_eq!(cursor.cursor, before.cursor, "the cursor did not move");
    let coverage = lane_of(&fx, "abc").await.unwrap().coverage.unwrap();
    assert_eq!(coverage["reason"], "source_truncated");
    assert_eq!(coverage["from"], before.cursor);

    // Regrown past the old cursor: still halted, nothing replayed or skipped.
    append(&abc, &user_row("abc", "u3", "three"));
    append(&abc, &user_row("abc", "u4", "four"));
    assert_eq!(pass(&fx).await.child_events, 0);
    let cursor = cursor_of(&fx, "abc").await;
    assert!(cursor.halted);
    assert_eq!(cursor.cursor, before.cursor);
}

#[tokio::test]
async fn a_file_truncated_and_regrown_between_passes_halts_on_its_anchor() {
    let fx = fixture("regrown").await;
    let abc = child_file(&fx, "abc");
    let first = user_row("abc", "u1", "one");
    append(&abc, &first);
    append(&abc, &user_row("abc", "u2", "two"));
    assert_eq!(pass(&fx).await.child_events, 2);
    let before = cursor_of(&fx, "abc").await;
    assert_eq!(before.anchor_uuid, "u2");

    // Between passes the file is cut to its first record and regrown longer than the cursor,
    // with the same first record: length and generation look intact, the anchor does not.
    std::fs::write(
        &abc,
        format!(
            "{}{}{}",
            first,
            user_row("abc", "x2", "a different second record that is longer"),
            user_row("abc", "x3", "and a third one")
        ),
    )
    .unwrap();
    assert!(std::fs::metadata(&abc).unwrap().len() as i64 > before.cursor);
    assert_eq!(
        pass(&fx).await.child_events,
        0,
        "no mid-record or replayed forwarding"
    );
    let cursor = cursor_of(&fx, "abc").await;
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "source_rewritten");
    assert_eq!(cursor.cursor, before.cursor);
    let coverage = lane_of(&fx, "abc").await.unwrap().coverage.unwrap();
    assert_eq!(coverage["reason"], "source_rewritten");
    assert_eq!(coverage["anchor_uuid"], "u2");
}

#[tokio::test]
async fn an_absent_or_oversized_first_uuid_halts_as_generation_unverifiable() {
    let fx = fixture("generation").await;
    // No uuid on the first record.
    let nouuid = child_file(&fx, "nouuid");
    append(
        &nouuid,
        &format!(
            "{}\n",
            json!({"type": "summary", "isSidechain": true, "agentId": "nouuid", "sessionId": ROOT,
                   "summary": "no uuid here"})
        ),
    );
    append(&nouuid, &user_row("nouuid", "u2", "later"));
    // A first line longer than the meta-line budget.
    let big = child_file(&fx, "big");
    let padding = "x".repeat(70 * 1024);
    append(
        &big,
        &child_row(
            "big",
            "u1",
            None,
            json!({"role": "user", "content": padding}),
        ),
    );
    append(&big, &user_row("big", "u2", "after the big one"));

    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0, "{stats:?}");
    assert_eq!(stats.child_errors, 0);
    for agent in ["nouuid", "big"] {
        let cursor = cursor_of(&fx, agent).await;
        assert!(cursor.halted, "{agent}");
        assert_eq!(cursor.halt_reason, "generation_unverifiable", "{agent}");
        assert_eq!(cursor.cursor, 0);
        let lane = lane_of(&fx, agent)
            .await
            .unwrap_or_else(|| panic!("lane {agent}"));
        assert_eq!(
            lane.coverage.as_ref().unwrap()["reason"],
            "generation_unverifiable"
        );
        assert_eq!(lane.coverage.as_ref().unwrap()["from"], 0);
        assert_eq!(
            lane.child.resolution,
            ChildResolution::Unresolved,
            "nothing was ever verified for {agent}"
        );
    }
    assert_eq!(pass(&fx).await.child_events, 0);
}

#[tokio::test]
async fn a_halted_cursor_keeps_its_real_resolution_and_re_declares_under_each_reopen_epoch() {
    let mut fx = file_fixture("reopen").await;
    let bad = child_file(&fx, "bad");
    append(
        &bad,
        &child_row(
            "other",
            "u1",
            None,
            json!({"role": "user", "content": "who am i"}),
        ),
    );
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 1);
    assert_eq!(
        fx.sink.children()[0].0.resolution,
        ChildResolution::Unresolved
    );
    let first = cursor_of(&fx, "bad").await;
    assert_eq!(first.resolution, "unresolved");
    assert_eq!(first.epoch, "boot_a");

    // Restart one: a real store reopen under a new epoch.
    fx.reopen("boot_b").await;
    assert!(
        lane_of(&fx, "bad").await.is_none(),
        "the volatile lane is fresh"
    );
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    let cursor = cursor_of(&fx, "bad").await;
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "daemon_restart_policy_undecided");
    assert_eq!(cursor.epoch, "boot_b");
    assert_eq!(cursor.cursor, first.cursor);
    let lane = lane_of(&fx, "bad").await.expect("declared under boot_b");
    assert_eq!(lane.epoch, "boot_b");
    assert_eq!(
        lane.child.resolution,
        ChildResolution::Unresolved,
        "the declaration carries the cursor's resolution, not one inferred from the file name"
    );
    assert_eq!(lane.child.evidence, None);
    assert_eq!(
        lane.coverage.as_ref().unwrap()["reason"],
        "daemon_restart_policy_undecided"
    );

    // Restart two: the halted cursor re-declares under boot_c so the fresh lane shows it.
    fx.reopen("boot_c").await;
    assert!(lane_of(&fx, "bad").await.is_none());
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    let cursor = cursor_of(&fx, "bad").await;
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "daemon_restart_policy_undecided");
    assert_eq!(cursor.epoch, "boot_c");
    assert_eq!(cursor.cursor, first.cursor);
    let lane = lane_of(&fx, "bad").await.expect("re-declared under boot_c");
    assert_eq!(lane.epoch, "boot_c");
    assert_eq!(lane.child.resolution, ChildResolution::Unresolved);
    assert_eq!(
        lane.coverage.as_ref().unwrap()["reason"],
        "daemon_restart_policy_undecided"
    );
    assert_eq!(lane.coverage.as_ref().unwrap()["from"], first.cursor);
    assert!(ChildStreamEvents::new(&fx.store)
        .page(&fx.session, &LaneFilter::default(), 0, 10)
        .await
        .unwrap()
        .rows
        .is_empty());
}

#[tokio::test]
async fn a_refused_coverage_declaration_leaves_the_cursor_retrying() {
    let bounds = ChildStreamBounds {
        max_unresolved_lanes_per_session: 0,
        ..ChildStreamBounds::default()
    };
    let fx = fixture_with("refused", None, bounds).await;
    let bad = child_file(&fx, "bad");
    append(
        &bad,
        &child_row(
            "other",
            "u1",
            None,
            json!({"role": "user", "content": "who am i"}),
        ),
    );
    assert_eq!(pass(&fx).await.child_events, 1);
    let first = cursor_of(&fx, "bad").await;
    assert_eq!(first.resolution, "unresolved");

    DaemonState::new(&fx.store)
        .set_boot_epoch("boot_b", now())
        .await
        .unwrap();
    for _ in 0..2 {
        let stats = pass(&fx).await;
        assert_eq!(stats.child_events, 0);
        assert_eq!(stats.child_errors, 0);
        let cursor = cursor_of(&fx, "bad").await;
        assert!(
            !cursor.halted,
            "a refused declaration is not recorded as halted"
        );
        assert_eq!(
            cursor.epoch, "boot_a",
            "the epoch is unchanged so the pass retries"
        );
        assert_eq!(cursor.cursor, first.cursor);
        assert!(
            lane_of(&fx, "bad").await.is_none(),
            "the lane refused the unresolved declaration"
        );
    }
    // The refusal is observable on the session's refusal sentinel.
    let lanes = ChildStreamEvents::new(&fx.store)
        .lanes(&fx.session, None, 100)
        .await
        .unwrap()
        .lanes;
    let refused = lanes
        .iter()
        .find(|l| l.child_key == "s:refused")
        .expect("refusal sentinel");
    assert!(refused.refused_rows >= 2, "{}", refused.refused_rows);
}

#[tokio::test]
async fn an_empty_or_incomplete_first_line_waits_instead_of_halting() {
    let fx = fixture("pending").await;
    let empty = child_file(&fx, "empty");
    std::fs::write(&empty, "").unwrap();
    let partial = child_file(&fx, "partial");
    let row = user_row("partial", "u1", "half written");
    std::fs::write(&partial, &row[..row.len() / 2]).unwrap();

    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    assert_eq!(stats.child_errors, 0);
    for agent in ["empty", "partial"] {
        let cursor = cursor_of(&fx, agent).await;
        assert!(!cursor.halted, "{agent} is pending, not halted");
        assert_eq!(cursor.cursor, 0);
        assert!(
            lane_of(&fx, agent).await.is_none(),
            "no coverage declared for {agent}"
        );
    }

    // A restart while nothing was consumed is not a gap: the cursors follow the new epoch.
    DaemonState::new(&fx.store)
        .set_boot_epoch("boot_b", now())
        .await
        .unwrap();
    append(&empty, &user_row("empty", "e1", "now it starts"));
    std::fs::write(&partial, &row).unwrap();
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 2, "{stats:?}");
    for agent in ["empty", "partial"] {
        let cursor = cursor_of(&fx, agent).await;
        assert!(!cursor.halted, "{agent}");
        assert_eq!(cursor.epoch, "boot_b");
        assert!(cursor.cursor > 0);
    }
}

#[tokio::test]
async fn a_malformed_record_halts_after_the_records_before_it_are_forwarded() {
    let fx = fixture("malformed").await;
    let abc = child_file(&fx, "abc");
    append(&abc, &user_row("abc", "u1", "fine"));
    append(&abc, "{this is not json}\n");
    append(&abc, &user_row("abc", "u3", "never reached"));

    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 1, "{stats:?}");
    assert_eq!(stats.child_errors, 0);
    assert_eq!(fx.sink.children()[0].3["text"], "fine");
    let cursor = cursor_of(&fx, "abc").await;
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "source_malformed");
    assert_eq!(cursor.anchor_uuid, "u1");
    let coverage = lane_of(&fx, "abc").await.unwrap().coverage.unwrap();
    assert_eq!(coverage["reason"], "source_malformed");
    assert_eq!(coverage["malformed_at"], cursor.cursor);
    assert_eq!(coverage["from"], cursor.cursor);
    assert_eq!(
        pass(&fx).await.child_events,
        0,
        "nothing past the malformed record"
    );
}

#[tokio::test]
async fn discovery_rotates_past_the_window_and_every_file_is_eventually_served() {
    let fx = fixture("window").await;
    let total = 1100usize;
    for i in 0..total {
        let agent = format!("w{i:04}");
        append(&child_file(&fx, &agent), &user_row(&agent, "u1", "hello"));
    }
    let repo = ClaudeChildStreamsRepo::new(&fx.store);

    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 32);
    assert_eq!(
        repo.list(&fx.session).await.unwrap().len(),
        1024,
        "the first window registers exactly its entries"
    );
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 32);
    assert_eq!(
        repo.list(&fx.session).await.unwrap().len(),
        total,
        "the second window reaches the entries past the first"
    );

    let mut passes = 2;
    while fx.sink.children().len() < total && passes < 60 {
        pass(&fx).await;
        passes += 1;
    }
    let seen: std::collections::BTreeSet<String> = fx
        .sink
        .children()
        .iter()
        .map(|(c, ..)| c.id.clone().unwrap())
        .collect();
    assert_eq!(seen.len(), total, "every file served after {passes} passes");
    assert_eq!(fx.sink.children().len(), total, "no record forwarded twice");
    assert_eq!(passes, total.div_ceil(32));
}

#[cfg(unix)]
#[tokio::test]
async fn failing_files_lose_their_place_and_a_healthy_successor_is_served() {
    use std::os::unix::fs::PermissionsExt;
    let fx = fixture("starvation").await;
    let mut failing = Vec::new();
    for i in 0..32 {
        let agent = format!("a{i:02}");
        let path = child_file(&fx, &agent);
        append(&path, &user_row(&agent, "u1", "unreadable"));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        failing.push(path);
    }
    if std::fs::File::open(&failing[0]).is_ok() {
        eprintln!("skipping: this user can read a mode-000 file");
        return;
    }
    append(&child_file(&fx, "zzz"), &user_row("zzz", "u1", "healthy"));

    let stats = pass(&fx).await;
    assert_eq!(stats.child_errors, 32, "{stats:?}");
    assert_eq!(stats.child_events, 0, "the 33rd file waits one pass");

    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 1, "{stats:?}");
    assert_eq!(fx.sink.children()[0].0.id.as_deref(), Some("zzz"));
    assert_eq!(fx.sink.children()[0].3["text"], "healthy");
    assert_eq!(
        stats.child_errors, 31,
        "31 failing files were retried alongside it"
    );
    let zzz = cursor_of(&fx, "zzz").await;
    assert!(zzz.cursor > 0);
    for path in &failing {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
}

#[tokio::test]
async fn a_consumed_file_that_shrank_below_its_first_line_declares_source_truncated() {
    let fx = fixture("shrunk").await;
    let gone = child_file(&fx, "gone");
    let row = user_row("gone", "u1", "was here");
    append(&gone, &row);
    let half = child_file(&fx, "half");
    append(&half, &user_row("half", "u1", "was here too"));
    assert_eq!(pass(&fx).await.child_events, 2);
    let gone_before = cursor_of(&fx, "gone").await.cursor;
    let half_before = cursor_of(&fx, "half").await.cursor;

    std::fs::write(&gone, "").unwrap();
    std::fs::write(&half, &row[..row.len() / 2]).unwrap();
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    assert_eq!(stats.child_errors, 0);
    for (agent, before) in [("gone", gone_before), ("half", half_before)] {
        let cursor = cursor_of(&fx, agent).await;
        assert!(cursor.halted, "{agent} is judged, not left pending");
        assert_eq!(cursor.halt_reason, "source_truncated", "{agent}");
        assert_eq!(cursor.cursor, before, "{agent} did not move");
        let coverage = lane_of(&fx, agent).await.unwrap().coverage.unwrap();
        assert_eq!(coverage["reason"], "source_truncated");
        assert_eq!(coverage["from"], before);
    }
}

#[tokio::test]
async fn a_reboot_declares_coverage_even_when_the_consumed_file_is_now_empty() {
    let fx = fixture("reboot-empty").await;
    let abc = child_file(&fx, "abc");
    append(&abc, &user_row("abc", "u1", "consumed"));
    assert_eq!(pass(&fx).await.child_events, 1);
    let before = cursor_of(&fx, "abc").await.cursor;

    std::fs::write(&abc, "").unwrap();
    DaemonState::new(&fx.store)
        .set_boot_epoch("boot_b", now())
        .await
        .unwrap();
    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    let cursor = cursor_of(&fx, "abc").await;
    assert!(
        cursor.halted,
        "the pending first line does not suppress the restart declaration"
    );
    assert_eq!(cursor.halt_reason, "daemon_restart_policy_undecided");
    assert_eq!(cursor.epoch, "boot_b");
    assert_eq!(cursor.cursor, before);
    let lane = lane_of(&fx, "abc")
        .await
        .expect("coverage declared under boot_b");
    assert_eq!(lane.epoch, "boot_b");
    assert_eq!(
        lane.coverage.as_ref().unwrap()["reason"],
        "daemon_restart_policy_undecided"
    );
    assert_eq!(lane.coverage.as_ref().unwrap()["from"], before);
}

#[cfg(unix)]
#[tokio::test]
async fn a_registered_file_replaced_by_a_symlink_is_rejected_at_the_open() {
    let fx = fixture("symlink").await;
    let abc = child_file(&fx, "abc");
    append(&abc, &user_row("abc", "u1", "real"));
    assert_eq!(pass(&fx).await.child_events, 1);
    let before = cursor_of(&fx, "abc").await.cursor;

    // The registered path becomes a symlink to a longer file with the same first record.
    let target = fx.project.join("elsewhere.jsonl");
    std::fs::write(
        &target,
        format!(
            "{}{}",
            user_row("abc", "u1", "real"),
            user_row("abc", "u2", "from the symlink target")
        ),
    )
    .unwrap();
    std::fs::remove_file(&abc).unwrap();
    std::os::unix::fs::symlink(&target, &abc).unwrap();

    let stats = pass(&fx).await;
    assert_eq!(
        stats.child_events, 0,
        "nothing behind the symlink is followed"
    );
    assert_eq!(stats.child_errors, 0);
    let cursor = cursor_of(&fx, "abc").await;
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "source_not_regular");
    assert_eq!(cursor.cursor, before);
    let coverage = lane_of(&fx, "abc").await.unwrap().coverage.unwrap();
    assert_eq!(coverage["reason"], "source_not_regular");
    assert_eq!(coverage["kind"], "symlink");
    assert_eq!(coverage["from"], before);
}

/// Run one `#[ignore]`d test of this binary in a child process under a hard deadline. `None`
/// means the deadline passed and the child was killed.
#[cfg(unix)]
fn run_ignored_under_watchdog(
    name: &str,
    timeout: std::time::Duration,
) -> Option<std::process::ExitStatus> {
    let exe = std::env::current_exe().expect("test binary path");
    let mut child = std::process::Command::new(exe)
        .args([name, "--exact", "--ignored", "--nocapture"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn inner test");
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll inner test") {
            return Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Inner half of the FIFO pin: a pass over a registered file that became a FIFO must return.
#[cfg(unix)]
#[tokio::test]
#[ignore]
async fn fifo_replacement_inner() {
    let fx = fixture("fifo").await;
    let abc = child_file(&fx, "abc");
    append(&abc, &user_row("abc", "u1", "real"));
    assert_eq!(pass(&fx).await.child_events, 1);
    let before = cursor_of(&fx, "abc").await.cursor;

    std::fs::remove_file(&abc).unwrap();
    let c_path = std::ffi::CString::new(abc.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) }, 0, "mkfifo");

    let stats = pass(&fx).await;
    assert_eq!(stats.child_events, 0);
    assert_eq!(stats.child_errors, 0);
    let cursor = cursor_of(&fx, "abc").await;
    assert!(cursor.halted);
    assert_eq!(cursor.halt_reason, "source_not_regular");
    assert_eq!(cursor.cursor, before);
    let coverage = lane_of(&fx, "abc").await.unwrap().coverage.unwrap();
    assert_eq!(coverage["kind"], "fifo");
    // The parent's own pass keeps working afterwards.
    append(&fx.transcript, &root_row("r1", "still here"));
    assert_eq!(pass(&fx).await.text_events, 1);
}

#[cfg(unix)]
#[test]
fn a_registered_file_replaced_by_a_fifo_never_blocks_the_pass() {
    match run_ignored_under_watchdog("fifo_replacement_inner", std::time::Duration::from_secs(40)) {
        Some(status) => assert!(status.success(), "inner FIFO pin failed: {status}"),
        None => panic!("the pass blocked on a FIFO at a registered path"),
    }
}
