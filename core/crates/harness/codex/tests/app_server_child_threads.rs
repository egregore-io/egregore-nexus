//! Positive thread attribution through the real forward loop against the fake app-server.
//!
//! The forwarder's connection attaches a main thread and other threads that share the process.
//! Only the main thread's notifications reach the parent's `agent.update` lane, its tool
//! observations and its turn tracker; other threads go to child lanes with lineage read from
//! their rollout metas; notifications without protocol-owned identity go to the unresolved lane.
//! A child's text, tool item and completion interleaved with a held parent turn change nothing
//! on the parent (lane, tool observations, turn tracker), and the parent's own tool item and
//! completion still land afterwards.
//!
//! Run: `cargo test -p nexus-harness-codex --test app_server_child_threads`

use std::sync::Arc;

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_contracts::ChildStream;
use nexus_contracts::{AgentUpdateKind, ChildResolution};
use nexus_harness_codex::app_server::{
    notification_thread_identity, route_notification, spawn_codex_forwarder_scoped,
    CodexThreadScope, CodexToolObservationSink, LineageResolver, NotificationRoute, ThreadIdentity,
    ThreadSpawnMeta,
};
use nexus_harness_codex::CodexTurnTracker;
use nexus_harness_codex::{AutoApprove, CodexAppServer, CodexAppServerClient, SupervisorOpts};
use nexus_transcript::ToolCallObservation;
use serde_json::json;

const FAKE_BIN: &str = env!("CARGO_BIN_EXE_fake_codex_app_server");
const MAIN: &str = "fake-thread";
const CHILD: &str = "child-thread-1";

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-child-threads-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create test tempdir");
    dir
}

/// Write a sub-agent rollout meta line under `root` for `thread`, declaring its parent and depth.
fn write_subagent_rollout(root: &std::path::Path, thread: &str, parent: &str, depth: u32) {
    let day = root.join("2026").join("09").join("18");
    std::fs::create_dir_all(&day).unwrap();
    let meta = json!({
        "timestamp": "2026-09-18T00:00:00.000Z",
        "type": "session_meta",
        "payload": {
            "id": thread,
            "originator": "nexus-harness",
            "source": { "subagent": { "thread_spawn": {
                "parent_thread_id": parent, "depth": depth, "agent_path": "/x", "agent_nickname": "n", "agent_role": null
            } } }
        }
    });
    std::fs::write(
        day.join(format!("rollout-2026-09-18T00-00-00-{thread}.jsonl")),
        format!("{meta}\n"),
    )
    .unwrap();
}

#[derive(Default)]
struct RecSink(tokio::sync::Mutex<Vec<WsEvent>>);

#[async_trait]
impl EventSink for RecSink {
    async fn emit(&self, e: WsEvent) {
        self.0.lock().await.push(e);
    }
}

#[derive(Default)]
struct ToolObsSink(std::sync::Mutex<Vec<(SessionId, ToolCallObservation)>>);

impl ToolObsSink {
    fn observations(&self) -> Vec<(SessionId, ToolCallObservation)> {
        self.0.lock().unwrap().clone()
    }
}

impl CodexToolObservationSink for ToolObsSink {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation) {
        self.0.lock().unwrap().push((session.clone(), observation));
    }
}

struct HarnessOpts {
    tag: &'static str,
    child_threads: &'static [&'static str],
    rollout_root: Option<std::path::PathBuf>,
    env: Vec<(String, String)>,
    lineage: Option<LineageResolver>,
}

struct Harness {
    _srv: CodexAppServer,
    driver: CodexAppServerClient,
    sink: Arc<RecSink>,
    tools: Arc<ToolObsSink>,
    tracker: CodexTurnTracker,
    _forwarder: tokio::task::JoinHandle<()>,
}

/// Start the fake, attach the main thread plus the child threads on the forwarder's own
/// connection, and spawn the scoped forwarder with a tool observation sink and a forwarder-owned
/// turn tracker (the bridge-owned ingress tracker is constructible only through the transport's
/// private binding; its per-thread keying and owner guard are covered by its own unit tests).
async fn harness(opts: HarnessOpts) -> Harness {
    let dir = tempdir(opts.tag);
    let srv = CodexAppServer::start(SupervisorOpts {
        codex_exe: FAKE_BIN.to_string(),
        session_dir: dir.clone(),
        codex_home: Some(dir.join("codex-home")),
        model: None,
        bus_mcp: None,
        cwd: None,
        env: opts.env,
    })
    .await
    .expect("fake app-server should start");
    let tracker = CodexTurnTracker::default();
    let cli = CodexAppServerClient::connect(srv.socket(), "nexus")
        .await
        .expect("nexus client should connect");
    let main = cli.thread_start().await.expect("thread_start");
    assert_eq!(main, MAIN);
    for child in opts.child_threads {
        cli.thread_resume(child).await.expect("attach child thread");
    }
    assert_eq!(cli.attached_threads()[0], MAIN);
    let sink = Arc::new(RecSink::default());
    let tools = Arc::new(ToolObsSink::default());
    let forwarder = spawn_codex_forwarder_scoped(
        SessionId("s_codex".into()),
        Arc::new(cli),
        sink.clone(),
        Arc::new(AutoApprove),
        tracker.clone(),
        Some(tools.clone()),
        match opts.lineage {
            Some(resolver) => {
                CodexThreadScope::bound(main, opts.rollout_root).with_lineage_resolver(resolver)
            }
            None => CodexThreadScope::bound(main, opts.rollout_root),
        },
    );
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let driver = CodexAppServerClient::connect(srv.socket(), "driver")
        .await
        .expect("driver client should connect");
    Harness {
        _srv: srv,
        driver,
        sink,
        tools,
        tracker,
        _forwarder: forwarder,
    }
}

async fn wait_for(sink: &RecSink, pred: impl Fn(&[WsEvent]) -> bool) -> Vec<WsEvent> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let got = sink.0.lock().await.clone();
        if pred(&got) {
            return got;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for events; have {got:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn parent_kinds(events: &[WsEvent]) -> Vec<AgentUpdateKind> {
    events
        .iter()
        .filter_map(|e| match e {
            WsEvent::AgentUpdate { kind, .. } => Some(*kind),
            _ => None,
        })
        .collect()
}

fn parent_texts(events: &[WsEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::Text,
                data,
                ..
            } => Some(data["text"].as_str().unwrap_or("").to_string()),
            _ => None,
        })
        .collect()
}

fn child_events(events: &[WsEvent]) -> Vec<&WsEvent> {
    events
        .iter()
        .filter(|e| matches!(e, WsEvent::ChildAgentUpdate { .. }))
        .collect()
}

fn child_turn_ends(events: &[WsEvent], id: &str) -> usize {
    child_events(events)
        .iter()
        .filter(|e| {
            matches!(e, WsEvent::ChildAgentUpdate { child, kind: AgentUpdateKind::TurnEnd, .. }
                if child.id.as_deref() == Some(id))
        })
        .count()
}

fn last_child(events: &[WsEvent], id: &str) -> ChildStream {
    child_events(events)
        .into_iter()
        .rev()
        .find_map(|e| match e {
            WsEvent::ChildAgentUpdate { child, .. } if child.id.as_deref() == Some(id) => {
                Some(child.clone())
            }
            _ => None,
        })
        .expect("a child event for the thread")
}

/// Run further turns on `thread` (after `done` already completed ones) until the latest child
/// identity satisfies `pred`. Lineage lookups are spaced by the forwarder's interval, so the
/// attempts are spaced too.
async fn child_turns_until(
    h: &Harness,
    thread: &str,
    done: usize,
    max_turns: usize,
    pred: impl Fn(&ChildStream) -> bool,
) -> ChildStream {
    for turn in 1..=max_turns {
        h.driver
            .turn_start(thread, "again")
            .await
            .expect("child turn");
        let got = wait_for(&h.sink, |e| child_turn_ends(e, thread) >= done + turn).await;
        let last = last_child(&got, thread);
        if pred(&last) {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    panic!("lineage of {thread} never reached the expected state");
}

fn thread_scripts() -> String {
    json!({
        MAIN: [
            [
                { "method": "item/agentMessage/delta", "params": { "threadId": "THREAD_ID", "turnId": "tp", "itemId": "ip", "delta": "par" } }
            ],
            [
                { "method": "item/completed", "params": { "threadId": "THREAD_ID", "turnId": "tp", "item": { "id": "cp", "type": "commandExecution", "command": "ls", "status": "completed", "exitCode": 0 } } },
                { "method": "turn/completed", "params": { "threadId": "THREAD_ID", "turnId": "tp" } }
            ]
        ],
        CHILD: [
            [
                { "method": "item/agentMessage/delta", "params": { "threadId": "THREAD_ID", "turnId": "tc", "itemId": "ic", "delta": "kid" } },
                { "method": "item/completed", "params": { "threadId": "THREAD_ID", "turnId": "tc", "item": { "id": "cc", "type": "commandExecution", "command": "rm -rf x", "status": "completed", "exitCode": 0 } } },
                { "method": "item/agentMessage/delta", "params": { "threadId": 123, "thread": { "id": MAIN }, "turnId": "tc", "delta": "malformed" } },
                { "method": "item/agentMessage/delta", "params": { "thread": { "id": MAIN }, "turnId": "tc", "delta": "nested-only" } },
                { "method": "turn/completed", "params": { "threadId": "THREAD_ID", "turnId": "tc" } }
            ],
            [
                { "method": "item/agentMessage/delta", "params": { "threadId": "THREAD_ID", "turnId": "tc2", "itemId": "ic2", "delta": "kid2" } },
                { "method": "turn/completed", "params": { "threadId": "THREAD_ID", "turnId": "tc2" } }
            ]
        ]
    })
    .to_string()
}

#[tokio::test]
async fn main_thread_stays_parent_and_a_spawned_thread_goes_to_its_child_lane_with_lineage() {
    let rollout_root = tempdir("rollouts-verified");
    write_subagent_rollout(&rollout_root, CHILD, MAIN, 1);
    let h = harness(HarnessOpts {
        tag: "verified",
        child_threads: &[CHILD],
        rollout_root: Some(rollout_root),
        env: vec![],
        lineage: None,
    })
    .await;

    // Positive control: the main thread's turn reaches the parent lane, text then completion.
    h.driver.turn_start(MAIN, "go").await.expect("main turn");
    let got = wait_for(&h.sink, |e| {
        parent_kinds(e).contains(&AgentUpdateKind::TurnEnd)
    })
    .await;
    assert_eq!(
        parent_kinds(&got),
        vec![AgentUpdateKind::Text, AgentUpdateKind::TurnEnd]
    );
    assert!(child_events(&got).is_empty());

    // The spawned thread's turn: nothing more on the parent, everything on the child lane. The
    // lineage lookup runs off the forward path, so the first events carry the native id with no
    // lineage yet; once the lookup has folded in, later events are lineage-verified.
    h.driver
        .turn_start(CHILD, "child go")
        .await
        .expect("child turn");
    let got = wait_for(&h.sink, |e| child_turn_ends(e, CHILD) >= 1).await;
    assert_eq!(
        parent_kinds(&got),
        vec![AgentUpdateKind::Text, AgentUpdateKind::TurnEnd],
        "a child turn must add nothing to the parent lane, not even its completion"
    );
    let children = child_events(&got);
    assert_eq!(children.len(), 2);
    for (i, ev) in children.iter().enumerate() {
        let WsEvent::ChildAgentUpdate {
            session_id,
            child,
            kind,
            source_ref,
            data,
        } = ev
        else {
            unreachable!()
        };
        assert_eq!(session_id.0, "s_codex");
        assert_eq!(child.harness, "codex");
        assert_eq!(child.root, MAIN);
        assert_eq!(child.id.as_deref(), Some(CHILD));
        assert!(
            matches!(
                child.resolution,
                ChildResolution::Unresolved | ChildResolution::LineageVerified
            ),
            "never parent, never guessed: {:?}",
            child.resolution
        );
        assert!(
            source_ref.starts_with("codex:child-thread-1@"),
            "{source_ref}"
        );
        assert!(source_ref.ends_with(&format!("#{}", i + 1)), "{source_ref}");
        if i == 0 {
            assert_eq!(*kind, AgentUpdateKind::Text);
            assert_eq!(data["text"], "hello");
        } else {
            assert_eq!(*kind, AgentUpdateKind::TurnEnd);
        }
    }
    let verified = child_turns_until(&h, CHILD, 1, 8, |c| {
        c.resolution == ChildResolution::LineageVerified
    })
    .await;
    assert_eq!(verified.parent.as_deref(), Some(MAIN));
    assert_eq!(verified.depth, Some(1));
    assert_eq!(
        verified.evidence.as_deref(),
        Some("rollout_meta.thread_spawn")
    );
    let got = h.sink.0.lock().await.clone();
    assert_eq!(
        parent_kinds(&got),
        vec![AgentUpdateKind::Text, AgentUpdateKind::TurnEnd],
        "every child turn left the parent lane alone"
    );
}

#[tokio::test]
async fn a_spawned_thread_without_rollout_meta_is_unresolved_with_its_native_id() {
    let h = harness(HarnessOpts {
        tag: "no-meta",
        child_threads: &["child-thread-2"],
        rollout_root: Some(tempdir("rollouts-empty")),
        env: vec![],
        lineage: None,
    })
    .await;
    h.driver
        .turn_start("child-thread-2", "child go")
        .await
        .expect("child turn");
    let got = wait_for(&h.sink, |e| child_events(e).len() >= 2).await;
    assert!(
        parent_kinds(&got).is_empty(),
        "nothing reaches the parent lane"
    );
    let WsEvent::ChildAgentUpdate { child, .. } = child_events(&got)[0] else {
        unreachable!()
    };
    assert_eq!(child.id.as_deref(), Some("child-thread-2"));
    assert_eq!(child.root, MAIN);
    assert_eq!(child.parent, None);
    assert_eq!(child.depth, None);
    assert_eq!(child.resolution, ChildResolution::Unresolved);
    assert_eq!(child.evidence, None);
}

#[tokio::test]
async fn nested_lineage_walks_to_the_main_thread_and_a_dangling_parent_stays_unresolved() {
    let rollout_root = tempdir("rollouts-nested");
    write_subagent_rollout(&rollout_root, CHILD, MAIN, 1);
    write_subagent_rollout(&rollout_root, "child-thread-1-1", CHILD, 2);
    write_subagent_rollout(&rollout_root, "child-thread-orphan", "never-seen-thread", 1);
    let h = harness(HarnessOpts {
        tag: "nested",
        child_threads: &["child-thread-1-1", "child-thread-orphan"],
        rollout_root: Some(rollout_root),
        env: vec![],
        lineage: None,
    })
    .await;

    // Depth 2 needs two cached metas (the thread's and its parent's), each one bounded lookup.
    h.driver
        .turn_start("child-thread-1-1", "nested go")
        .await
        .expect("nested turn");
    let got = wait_for(&h.sink, |e| child_turn_ends(e, "child-thread-1-1") >= 1).await;
    assert_eq!(
        last_child(&got, "child-thread-1-1").id.as_deref(),
        Some("child-thread-1-1")
    );
    let nested = child_turns_until(&h, "child-thread-1-1", 1, 10, |c| {
        c.resolution == ChildResolution::LineageVerified
    })
    .await;
    assert_eq!(nested.parent.as_deref(), Some(CHILD));
    assert_eq!(nested.depth, Some(2));

    // A declared parent that never walks to the main thread stays unresolved, parent kept.
    h.driver
        .turn_start("child-thread-orphan", "orphan go")
        .await
        .expect("orphan turn");
    wait_for(&h.sink, |e| child_turn_ends(e, "child-thread-orphan") >= 1).await;
    let orphan = child_turns_until(&h, "child-thread-orphan", 1, 8, |c| c.parent.is_some()).await;
    assert_eq!(orphan.parent.as_deref(), Some("never-seen-thread"));
    assert_eq!(orphan.depth, Some(1));
    assert_eq!(orphan.resolution, ChildResolution::Unresolved);
    let got = h.sink.0.lock().await.clone();
    assert!(parent_kinds(&got).is_empty());
}

/// A lineage resolver held behind a condition variable. `entered` proves the resolver is
/// actually blocked inside the lookup; dropping the handle releases it unconditionally, so a
/// failing assertion or timeout can never leave the blocking task waiting through runtime
/// teardown.
struct HeldResolver {
    state: Arc<(std::sync::Mutex<(bool, bool)>, std::sync::Condvar)>,
}

impl HeldResolver {
    fn new() -> Self {
        HeldResolver {
            state: Arc::new((
                std::sync::Mutex::new((false, false)),
                std::sync::Condvar::new(),
            )),
        }
    }

    fn resolver(&self) -> LineageResolver {
        let state = self.state.clone();
        Arc::new(move |_root, _thread| {
            let (lock, cv) = &*state;
            let mut flags = lock.lock().unwrap();
            flags.0 = true;
            cv.notify_all();
            while !flags.1 {
                flags = cv.wait(flags).unwrap();
            }
            Some(ThreadSpawnMeta {
                parent_thread_id: MAIN.into(),
                depth: 1,
            })
        })
    }

    /// Wait until the resolver has entered its wait; `false` on timeout.
    fn wait_entered(&self, timeout: std::time::Duration) -> bool {
        let (lock, cv) = &*self.state;
        let deadline = std::time::Instant::now() + timeout;
        let mut flags = lock.lock().unwrap();
        while !flags.0 {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            flags = cv.wait_timeout(flags, deadline - now).unwrap().0;
        }
        true
    }

    fn released(&self) -> bool {
        self.state.0.lock().unwrap().1
    }

    fn release(&self) {
        let (lock, cv) = &*self.state;
        lock.lock().unwrap().1 = true;
        cv.notify_all();
    }
}

impl Drop for HeldResolver {
    fn drop(&mut self) {
        self.release();
    }
}

/// Run one `#[ignore]`d test of this binary in a child process under a hard deadline. `None`
/// means it did not finish in time and was killed.
fn run_inner_with_watchdog(
    name: &str,
    timeout: std::time::Duration,
) -> Option<std::process::ExitStatus> {
    let exe = std::env::current_exe().expect("test binary path");
    let mut child = std::process::Command::new(exe)
        .args([name, "--exact", "--ignored", "--nocapture"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
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

#[tokio::test]
async fn a_held_lineage_lookup_never_delays_the_parent() {
    let held = HeldResolver::new();
    let h = harness(HarnessOpts {
        tag: "held-lookup",
        child_threads: &[CHILD],
        rollout_root: Some(tempdir("rollouts-held-lookup")),
        env: vec![],
        lineage: Some(held.resolver()),
    })
    .await;

    // The child's first turn starts the one outstanding lookup; wait until it is inside it.
    h.driver.turn_start(CHILD, "c1").await.expect("child turn");
    let got = wait_for(&h.sink, |e| child_turn_ends(e, CHILD) >= 1).await;
    assert_eq!(
        last_child(&got, CHILD).resolution,
        ChildResolution::Unresolved
    );
    assert!(
        held.wait_entered(std::time::Duration::from_secs(3)),
        "the lookup never entered the resolver"
    );
    assert!(!held.released());

    // The parent's text and completion flow while the lookup is held inside the resolver.
    h.driver.turn_start(MAIN, "p1").await.expect("parent turn");
    let got = wait_for(&h.sink, |e| {
        parent_kinds(e).contains(&AgentUpdateKind::TurnEnd)
    })
    .await;
    assert_eq!(
        parent_kinds(&got),
        vec![AgentUpdateKind::Text, AgentUpdateKind::TurnEnd]
    );
    assert!(!held.released(), "the lookup is still held");
    assert_eq!(
        last_child(&got, CHILD).resolution,
        ChildResolution::Unresolved
    );

    // Released: the result folds in on a later notification and the lane becomes verified.
    held.release();
    let verified = child_turns_until(&h, CHILD, 1, 8, |c| {
        c.resolution == ChildResolution::LineageVerified
    })
    .await;
    assert_eq!(verified.parent.as_deref(), Some(MAIN));
    assert_eq!(verified.depth, Some(1));
}

/// The regression the held-lookup pin guards against, reproduced deliberately: a lookup awaited
/// on the forward path before the parent's turn. Run only by the watchdog below; it never
/// finishes on its own.
#[tokio::test]
#[ignore]
async fn stalled_awaited_lookup_regression_inner() {
    let held = HeldResolver::new();
    let resolver = held.resolver();
    let root = tempdir("rollouts-stalled");
    // The old shape: the forward path awaits the lookup. Held forever, so this never returns.
    let _ = tokio::task::spawn_blocking(move || resolver(&root, CHILD)).await;
    std::mem::forget(held);
}

#[test]
fn a_stalled_awaited_lookup_is_caught_and_killed_by_the_watchdog() {
    let outcome = run_inner_with_watchdog(
        "stalled_awaited_lookup_regression_inner",
        std::time::Duration::from_secs(6),
    );
    assert!(
        outcome.is_none(),
        "the deliberately stalled lookup must not finish on its own: {outcome:?}"
    );
}

#[tokio::test]
async fn a_child_turn_interleaved_with_a_held_parent_turn_changes_nothing_on_the_parent() {
    let rollout_root = tempdir("rollouts-held");
    write_subagent_rollout(&rollout_root, CHILD, MAIN, 1);
    let h = harness(HarnessOpts {
        tag: "held",
        child_threads: &[CHILD],
        rollout_root: Some(rollout_root),
        env: vec![("FAKE_CODEX_THREAD_SCRIPTS".into(), thread_scripts())],
        lineage: None,
    })
    .await;

    // Parent turn opens with one text delta and no completion: the parent's text is buffered
    // (coalescing window) and its turn stays active. The child turn follows immediately; it
    // carries text, a command item, a malformed identity, a nested-only identity and a
    // completion. Whether its notifications land on the buffered or the unbuffered seat is
    // timing-dependent; both seats run the same divert_foreign call before any parent path,
    // so the assertions here hold for either, and the seat itself is not claimed.
    h.driver
        .turn_start(MAIN, "p1")
        .await
        .expect("parent turn 1");
    h.driver
        .turn_start(CHILD, "c1")
        .await
        .expect("child turn 1");
    let got = wait_for(&h.sink, |e| {
        child_turn_ends(e, CHILD) >= 1 && !parent_texts(e).is_empty()
    })
    .await;

    // Parent lane: exactly the parent's own text, uncontaminated, and no completion.
    assert_eq!(parent_texts(&got), vec!["par".to_string()]);
    assert_eq!(parent_kinds(&got), vec![AgentUpdateKind::Text]);
    // Parent tool observations: none, the child's command item is not the parent's.
    assert!(h.tools.observations().is_empty());
    // Child lane: text, tool call and completion for the child; the two shapeless
    // notifications are unresolved, with no thread id and their own locators.
    let mut child_kinds = Vec::new();
    let mut unresolved = Vec::new();
    for ev in child_events(&got) {
        let WsEvent::ChildAgentUpdate {
            child, kind, data, ..
        } = ev
        else {
            unreachable!()
        };
        if child.id.as_deref() == Some(CHILD) {
            child_kinds.push((*kind, data.clone()));
        } else {
            assert_eq!(child.id, None);
            assert_eq!(child.resolution, ChildResolution::Unresolved);
            assert_eq!(child.root, MAIN);
            assert!(
                child.locator.starts_with("codex:item/agentMessage/delta@"),
                "{}",
                child.locator
            );
            unresolved.push(data["text"].as_str().unwrap_or("").to_string());
        }
    }
    assert_eq!(
        child_kinds.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
        vec![
            AgentUpdateKind::Text,
            AgentUpdateKind::ToolCall,
            AgentUpdateKind::TurnEnd
        ]
    );
    assert_eq!(child_kinds[0].1["text"], "kid");
    assert_eq!(child_kinds[1].1["id"], "cc");
    unresolved.sort();
    assert_eq!(
        unresolved,
        vec!["malformed".to_string(), "nested-only".to_string()]
    );

    // Tracker boundary: the forwarder observes turn authority only for the main thread. The
    // parent's held turn is active; the child's turns, completions included, never reach the
    // tracker, so the child thread has no turn there and the parent's stays as it was.
    assert_eq!(h.tracker.active_turn_id(MAIN).as_deref(), Some("tp"));
    assert_eq!(h.tracker.active_turn_id(CHILD), None);
    h.driver
        .turn_start(CHILD, "c2")
        .await
        .expect("child turn 2");
    let got = wait_for(&h.sink, |e| child_turn_ends(e, CHILD) >= 2).await;
    assert_eq!(h.tracker.active_turn_id(MAIN).as_deref(), Some("tp"));
    assert_eq!(h.tracker.active_turn_id(CHILD), None);
    assert_eq!(parent_kinds(&got), vec![AgentUpdateKind::Text]);
    assert!(h.tools.observations().is_empty());

    // Positive control: the parent's own command item and completion land on the parent lane,
    // its tool observation is published, and the child lane is untouched.
    let child_count = child_events(&got).len();
    h.driver
        .turn_start(MAIN, "p2")
        .await
        .expect("parent turn 2");
    let got = wait_for(&h.sink, |e| {
        parent_kinds(e).contains(&AgentUpdateKind::TurnEnd)
    })
    .await;
    assert_eq!(
        parent_kinds(&got),
        vec![
            AgentUpdateKind::Text,
            AgentUpdateKind::ToolCall,
            AgentUpdateKind::TurnEnd
        ]
    );
    let parent_tool = got
        .iter()
        .find_map(|e| match e {
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::ToolCall,
                data,
                ..
            } => Some(data.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(parent_tool["id"], "cp");
    let observations = h.tools.observations();
    assert!(
        observations
            .iter()
            .any(|(s, o)| s.0 == "s_codex" && o.tool_call_id.as_deref() == Some("cp")),
        "{observations:?}"
    );
    assert_eq!(child_events(&got).len(), child_count);
    assert!(h.tracker.active_turn_id(MAIN).is_none());
}

#[test]
fn identity_is_read_only_in_the_shape_the_method_owns() {
    let delta = "item/agentMessage/delta";
    assert_eq!(
        notification_thread_identity(delta, &json!({ "threadId": "t1", "delta": "x" })),
        ThreadIdentity::Valid("t1".into())
    );
    assert_eq!(
        notification_thread_identity(delta, &json!({ "delta": "x" })),
        ThreadIdentity::Missing
    );
    assert_eq!(
        notification_thread_identity(delta, &json!({ "thread": { "id": "t1" }, "delta": "x" })),
        ThreadIdentity::Missing,
        "the nested shape belongs to thread/started only"
    );
    assert_eq!(
        notification_thread_identity(delta, &json!({ "threadId": 123, "delta": "x" })),
        ThreadIdentity::Malformed
    );
    assert_eq!(
        notification_thread_identity(delta, &json!({ "threadId": "", "delta": "x" })),
        ThreadIdentity::Malformed
    );
    assert_eq!(
        notification_thread_identity(
            delta,
            &json!({ "threadId": 123, "thread": { "id": "t1" }, "delta": "x" })
        ),
        ThreadIdentity::Malformed,
        "a malformed flat id never falls back to the nested shape"
    );
    assert_eq!(
        notification_thread_identity(
            delta,
            &json!({ "threadId": "t1", "thread": { "id": "t2" }, "delta": "x" })
        ),
        ThreadIdentity::Malformed,
        "conflicting shapes are not identity"
    );
    assert_eq!(
        notification_thread_identity(
            delta,
            &json!({ "threadId": "t1", "thread": { "id": "t1" }, "delta": "x" })
        ),
        ThreadIdentity::Valid("t1".into())
    );
    assert_eq!(
        notification_thread_identity("thread/started", &json!({ "thread": { "id": "t1" } })),
        ThreadIdentity::Valid("t1".into())
    );
    assert_eq!(
        notification_thread_identity("thread/started", &json!({ "threadId": "t1" })),
        ThreadIdentity::Missing,
        "thread/started owns the nested shape only"
    );
    assert_eq!(
        notification_thread_identity(
            "thread/started",
            &json!({ "thread": { "id": "t1" }, "threadId": "t2" })
        ),
        ThreadIdentity::Malformed
    );
}

#[test]
fn routing_is_positive_only() {
    let bound = CodexThreadScope::bound(MAIN, None);
    let delta = "item/agentMessage/delta";
    assert_eq!(
        route_notification(&bound, delta, &json!({ "threadId": MAIN, "delta": "x" })),
        NotificationRoute::Main
    );
    assert_eq!(
        route_notification(&bound, delta, &json!({ "threadId": "other", "delta": "x" })),
        NotificationRoute::Child("other".into())
    );
    assert_eq!(
        route_notification(&bound, delta, &json!({ "delta": "x" })),
        NotificationRoute::Unresolved { thread: None },
        "a missing thread id is never parent evidence"
    );
    assert_eq!(
        route_notification(
            &bound,
            delta,
            &json!({ "threadId": 123, "thread": { "id": MAIN }, "delta": "x" })
        ),
        NotificationRoute::Unresolved { thread: None },
        "a malformed flat id with the main thread in the nested shape is not the parent"
    );
    assert_eq!(
        route_notification(
            &bound,
            delta,
            &json!({ "thread": { "id": MAIN }, "delta": "x" })
        ),
        NotificationRoute::Unresolved { thread: None },
        "the nested shape on a runtime notification is not the parent"
    );
    assert_eq!(
        route_notification(
            &bound,
            "thread/started",
            &json!({ "thread": { "id": MAIN } })
        ),
        NotificationRoute::Main,
        "thread/started keeps its nested positive control"
    );
    let unbound = CodexThreadScope::default();
    assert_eq!(
        route_notification(&unbound, delta, &json!({ "threadId": MAIN, "delta": "x" })),
        NotificationRoute::Unresolved {
            thread: Some(MAIN.into())
        }
    );
}
