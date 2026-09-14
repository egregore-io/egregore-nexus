use super::*;

use nexus_contracts::batch::BatchCounts;
use nexus_contracts::{BatchMessage, MessageId, Scope};

fn human_batch(bodies: &[&str]) -> NexusBatch {
    batch_of(bodies, Kind::Human)
}

fn batch_of(bodies: &[&str], kind: Kind) -> NexusBatch {
    let mut batch = NexusBatch {
        counts: BatchCounts {
            dms: 0,
            thread: 0,
            total: 0,
        },
        dms: Vec::new(),
        threads: Vec::new(),
        dm_message_ids: Vec::new(),
        thread_message_ids: Vec::new(),
        message_ids: Vec::new(),
        auto_reply_note: None,
    };
    for (index, body) in bodies.iter().enumerate() {
        let id = MessageId(format!("m_{index}"));
        batch.dms.push(BatchMessage {
            id: id.clone(),
            from: "pcuser".to_string(),
            kind,
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: (*body).to_string(),
            truncated: false,
        });
        batch.dm_message_ids.push(id.clone());
        batch.message_ids.push(id);
    }
    batch.counts.total = bodies.len() as u32;
    batch
}

#[test]
fn ack_text_uses_current_work_when_present() {
    let text = ack_text(Some("the login refactor"));
    assert!(text.contains("the login refactor"), "{text}");
    assert!(text.contains("next"), "{text}");
}

#[test]
fn ack_text_is_generic_when_current_work_is_absent() {
    // Publishing under the agent's name means a fabricated task name would be a lie, so an
    // absent or blank current_work must fall back rather than interpolate.
    for absent in [None, Some(""), Some("   ")] {
        let text = ack_text(absent);
        assert!(text.contains("mid-task"), "{text}");
        assert!(!text.contains("None"), "{text}");
    }
}

#[test]
fn only_a_single_human_message_ending_in_question_mark_is_treated_as_a_question() {
    assert_eq!(
        batch_question(&human_batch(&["are you online?"])),
        Some("are you online?")
    );
    // Trailing whitespace after the question mark still qualifies.
    assert_eq!(
        batch_question(&human_batch(&["are you online?  \n"])),
        Some("are you online?  \n")
    );
    // A task statement does not.
    assert_eq!(
        batch_question(&human_batch(&["refactor the login page"])),
        None
    );
    // Neither does a multi-message batch, even when the last message ends in '?'.
    assert_eq!(batch_question(&human_batch(&["do this", "ok?"])), None);
    // Nor an agent-authored question; those never reach this path anyway.
    assert_eq!(batch_question(&batch_of(&["ready?"], Kind::Agent)), None);
}

#[test]
fn auto_reply_note_is_available_once_then_consumed() {
    let notes = AutoReplyNotes::default();
    let session = SessionId("s_note".to_string());
    notes.put(&session, "Got it — I'll start this next.".to_string());

    assert!(notes.has_pending(&session));
    let taken = notes.take(&session).expect("the note is available once");
    assert!(taken.contains("Got it"));
    assert_eq!(notes.take(&session), None, "the note is consumed");
    assert!(!notes.has_pending(&session));
}

#[test]
fn a_pending_note_suppresses_a_second_acknowledgment() {
    let notes = AutoReplyNotes::default();
    let session = SessionId("s_dedup".to_string());

    assert!(
        should_acknowledge(&notes, &session),
        "the first message of a busy period acknowledges"
    );
    notes.put(&session, ack_text(None));
    assert!(
        !should_acknowledge(&notes, &session),
        "a second message during the same busy turn must not acknowledge again"
    );

    // The real delivery consumes the note, which re-arms acknowledgment for the next busy period.
    let _ = notes.take(&session);
    assert!(should_acknowledge(&notes, &session));
}

#[test]
fn harnesses_without_a_oneshot_mode_fall_back_to_acknowledgment() {
    // `pi` and `other` inherit the trait default, so the answer path is simply skipped for them
    // and they keep the acknowledgment every harness gets.
    assert!(harness_oneshot("pi", "hello").is_none());
    assert!(harness_oneshot("not-a-registered-harness", "hello").is_none());
}

#[test]
fn claude_opencode_and_codex_declare_oneshot_commands() {
    let claude = harness_oneshot("claude", "say hi").expect("claude declares a one-shot mode");
    assert!(claude.args.contains(&"-p".to_string()), "{:?}", claude.args);
    assert!(claude.args.contains(&"say hi".to_string()));

    let opencode =
        harness_oneshot("opencode", "say hi").expect("opencode declares a one-shot mode");
    assert!(
        opencode.args.contains(&"run".to_string()),
        "{:?}",
        opencode.args
    );
    assert!(opencode.args.contains(&"say hi".to_string()));

    let codex = harness_oneshot("codex", "say hi").expect("codex declares a one-shot mode");
    assert!(codex.args.contains(&"exec".to_string()), "{:?}", codex.args);
    assert!(codex.args.contains(&"say hi".to_string()));
}

#[cfg(unix)]
#[tokio::test]
async fn oneshot_timeout_kills_the_process_and_falls_back_to_ack() {
    let command = nexus_harness_core::HeadedCommand {
        program: "sh".to_string(),
        args: vec!["-c".to_string(), "sleep 60".to_string()],
    };
    let started = std::time::Instant::now();
    let answer = run_oneshot(&command, Duration::from_millis(200)).await;
    assert_eq!(answer, None, "a timed-out one-shot yields no answer");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the one-shot must be bounded, took {:?}",
        started.elapsed()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn oneshot_failure_modes_all_fall_back_to_ack() {
    for script in ["exit 3", "printf ''", "printf '   \\n'"] {
        let command = nexus_harness_core::HeadedCommand {
            program: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
        };
        assert_eq!(
            run_oneshot(&command, Duration::from_secs(5)).await,
            None,
            "script {script:?} must not produce an answer"
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn oneshot_success_returns_trimmed_output() {
    let command = nexus_harness_core::HeadedCommand {
        program: "sh".to_string(),
        args: vec!["-c".to_string(), "printf '  4  \\n'".to_string()],
    };
    assert_eq!(
        run_oneshot(&command, Duration::from_secs(5)).await,
        Some("4".to_string())
    );
}
