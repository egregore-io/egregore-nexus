use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use nexus_contracts::batch::{BatchCounts, NexusBatch};
use nexus_contracts::ports::AgentTurnExecutionPort;
use nexus_harness_claude::native::{
    forwarder::ClaudeHookObservationSink,
    transcript::{parse_hook_record, ClaudeHookRecord},
};
use nexus_harness_core::{native_harness_program, NativeProcessPlatform};

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
        completion,
    };
    tokio::time::timeout(
        Duration::from_secs(2),
        input.send_turn("queued during a tool loop"),
    )
    .await
    .expect("structured hook acceptance should settle before the screen preview disappears")
    .expect("Claude raw PTY submission should succeed");

    assert!(writes.lock().unwrap().iter().any(|bytes| bytes == b"\r"));
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

struct NativeWriteBarrier {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
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
    completion.observe_hooks(
        &[native_hook("UserPromptSubmit", "manual", 1)],
        Some(1),
        true,
    );
    assert_eq!(transport.active_turn_sessions(), vec![session.clone()]);
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
    assert!(wiring.claim_native_forwarder_for_owner("claude", &session, old.owner_id()));
    // OLD is parked after the real reservation and before attachment. NEW must replace it.
    let new = supervisor
        .capture_claude_binding(&session, HeadedRuntimeKind::ClaudeNative, &args)
        .unwrap();
    assert!(
        wiring.claim_native_forwarder_for_owner("claude", &session, new.owner_id()),
        "a stale reserved slot must not strand NEW without an observer"
    );
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
    let home = PathBuf::from("/home/operator");
    let profile = home.join(".hermes/profiles/nexus-s_agent");
    assert_eq!(
        resolve_machine_hermes_home(None, Some(&profile), Some(&home)).unwrap(),
        home.join(".hermes")
    );

    let custom_root = PathBuf::from("/srv/hermes-data");
    let custom_profile = custom_root.join("profiles/nexus-s_agent");
    assert_eq!(
        resolve_machine_hermes_home(None, Some(&custom_profile), Some(&home)).unwrap(),
        custom_root
    );
}

#[test]
fn hermes_machine_home_preserves_explicit_root_and_home_default() {
    let home = PathBuf::from("/home/operator");
    let explicit = PathBuf::from("/srv/hermes-data");
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
