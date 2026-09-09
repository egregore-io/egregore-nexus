use super::*;
use std::future::Future;

#[tokio::test]
async fn closed_turn_without_input_receipt_releases_waiter_without_claiming_delivery() {
    for close_before_wait in [false, true] {
        let tracker = CodexTurnTracker::default();
        tracker.observe_active_turn("thread", "old");
        if close_before_wait {
            tracker.complete("thread", "old");
        }
        let mut receipt = std::pin::pin!(tracker.wait_for_accepted_user_input_echo(
            "thread",
            "old",
            "not recorded",
            Duration::from_secs(600),
        ));
        if !close_before_wait {
            std::future::poll_fn(|cx| {
                assert!(receipt.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            tracker.complete("thread", "old");
        }
        tracker.observe_active_turn("thread", "new");
        let result = tokio::time::timeout(Duration::from_millis(100), receipt)
            .await
            .expect("a terminal turn cannot consume new input; do not retain the waiter for600s");
        assert!(result.is_err(), "closing a turn is not an input receipt");
        assert_eq!(tracker.active_turn_id("thread").as_deref(), Some("new"));
    }
}

#[tokio::test]
async fn input_receipt_before_terminal_remains_successful() {
    let tracker = CodexTurnTracker::default();
    tracker.observe_accepted_user_input_echo("thread", "turn", "recorded");
    tracker.complete("thread", "turn");
    assert!(
        tracker
            .wait_for_accepted_user_input_echo(
                "thread",
                "turn",
                "recorded",
                Duration::from_millis(100),
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn early_reader_terminal_does_not_discard_a_still_unprojected_input_receipt() {
    let tracker = CodexTurnTracker::default();
    let mut receipt = Box::pin(tracker.wait_for_accepted_user_input_echo(
        "thread",
        "turn",
        "recorded",
        Duration::from_secs(1),
    ));
    assert!(futures::poll!(&mut receipt).is_pending());
    tracker.ingest_native(&note("thread", "turn", "turn/completed"));
    assert!(
        futures::poll!(&mut receipt).is_pending(),
        "reader activity is not projected receipt ordering"
    );
    tracker.observe_accepted_user_input_echo("thread", "turn", "recorded");
    tracker.settle_completion("thread", "turn");
    receipt.await.unwrap();
}

impl CodexTurnTracker {
    pub(crate) fn live_completion_waiter_count(&self, thread_id: &str, turn_id: &str) -> usize {
        self.lock(thread_id)
            .waiters
            .get(&(thread_id.to_owned(), turn_id.to_owned()))
            .map_or(0, |waiters| {
                waiters.iter().filter(|waiter| !waiter.is_closed()).count()
            })
    }
}

#[test]
fn observation_requires_positive_idle_evidence_and_stable_owner_facts() {
    use nexus_contracts::TurnState;
    let root = CodexTurnTracker::default();
    let owner = root.new_owner(Some("thread".into()));
    assert!(owner.publish_owner("thread"));
    let unknown = owner.observe_turn("thread");
    assert_eq!(unknown.state, TurnState::Unknown);
    assert_eq!(unknown, owner.observe_turn("thread"));
    owner.ingest_native(&note("thread", "turn", "turn/started"));
    let open = owner.observe_turn("thread");
    assert_eq!(open.state, TurnState::NativeOpen);
    assert_ne!(unknown.stamp, open.stamp);
    assert_eq!(open, owner.observe_turn("thread"));
    owner.ingest_native(&note("thread", "turn", "turn/completed"));
    assert_eq!(owner.observe_turn("thread").state, TurnState::VerifiedIdle);
    owner.clear_active_turn("thread");
    assert_eq!(owner.observe_turn("thread").state, TurnState::Unknown);
    owner.revoke_owner();
    assert_eq!(owner.observe_turn("thread").state, TurnState::Unavailable);
    let replacement = root.new_owner(Some("thread".into()));
    assert!(replacement.publish_owner("thread"));
    assert_ne!(
        unknown.stamp.unwrap().owner,
        replacement.observe_turn("thread").stamp.unwrap().owner
    );
}

#[test]
fn idle_setup_seed_cannot_overwrite_newer_native_evidence() {
    use nexus_contracts::TurnState;
    for newer_open in [false, true] {
        let owner = CodexTurnTracker::default().new_owner(Some("thread".into()));
        let before = owner.native_revision();
        if newer_open {
            owner.ingest_native(&note("thread", "new", "turn/started"));
        }
        owner.seed_idle("thread", before);
        assert!(owner.publish_owner("thread"));
        assert_eq!(
            owner.observe_turn("thread").state,
            if newer_open {
                TurnState::NativeOpen
            } else {
                TurnState::VerifiedIdle
            }
        );
        let snapshot = owner.observe_turn("thread");
        owner.record_turn_start_acceptance("thread", "new");
        assert_eq!(owner.observe_turn("thread").state, TurnState::NativeOpen);
        if newer_open {
            assert_eq!(snapshot, owner.observe_turn("thread"));
        }
        owner.ingest_native(&note("thread", "old", "turn/completed"));
        assert_eq!(owner.observe_turn("thread").state, TurnState::NativeOpen);
    }
}

#[test]
fn provisional_idle_summaries_remain_bounded_and_dropped_evidence_is_unknown() {
    let owner = CodexTurnTracker::default().new_owner(None);
    for index in 0..RECENT_COMPLETIONS_LIMIT * 2 {
        owner.ingest_native(&note(&format!("thread-{index}"), "done", "turn/completed"));
    }
    assert!(owner.lock("").idle_threads.len() <= RECENT_COMPLETIONS_LIMIT);
    assert!(owner.publish_owner("thread-0"));
    assert_eq!(
        owner.observe_turn("thread-0").state,
        nexus_contracts::TurnState::Unknown
    );
}

fn note(thread: &str, turn: &str, method: &str) -> Notification {
    Notification {
        id: None,
        method: method.into(),
        params: serde_json::json!({"threadId": thread, "turnId": turn}),
    }
}

#[test]
fn provisional_summary_overflow_cannot_publish_an_unobserved_idle_binding() {
    let owner = CodexTurnTracker::default().new_owner(None);
    for n in 0..=RECENT_COMPLETIONS_LIMIT {
        owner.ingest_native(&note(&format!("thread-{n}"), "active", "turn/started"));
    }
    assert!(!owner.publish_owner(&format!("thread-{RECENT_COMPLETIONS_LIMIT}")));
}

#[tokio::test]
async fn revoked_owner_settles_its_receipt_and_echo_without_touching_replacement() {
    let root = CodexTurnTracker::default();
    let old = root.new_owner(None);
    assert!(old.publish_owner("thread"));
    let old_echo = old.queue_accepted_user_input_echo("thread", "same text".into());
    old.record_accepted_user_input_echo_for_queued(&old_echo, "same-turn");
    let mut old_wait =
        Box::pin(old.wait_for_completion("thread", "same-turn", Duration::from_secs(3)));
    assert!(futures::poll!(&mut old_wait).is_pending());
    old.revoke_owner();
    let new = root.new_owner(None);
    assert!(new.publish_owner("thread"));
    let new_echo = new.queue_accepted_user_input_echo("thread", "same text".into());
    new.record_accepted_user_input_echo_for_queued(&new_echo, "same-turn");
    let mut new_wait =
        Box::pin(new.wait_for_completion("thread", "same-turn", Duration::from_secs(3)));
    assert!(futures::poll!(&mut new_wait).is_pending());
    assert!(old.take_accepted_user_input_echo("thread", "same-turn", "same text"));
    old.settle_completion("thread", "same-turn");
    old_wait.await.unwrap();
    assert!(futures::poll!(&mut new_wait).is_pending());
    assert!(new.take_accepted_user_input_echo("thread", "same-turn", "same text"));
    new.settle_completion("thread", "same-turn");
    new_wait.await.unwrap();
}

#[test]
fn provisional_owner_selects_actual_thread_and_never_publishes_other_notes() {
    let root = CodexTurnTracker::default();
    let owner = root.new_owner(None);
    owner.ingest_native(&note("unrelated", "foreign", "turn/started"));
    owner.ingest_native(&note("chosen", "native", "turn/started"));
    assert_eq!(root.active_turn_id("chosen"), None);
    assert!(owner.publish_owner("chosen"));
    assert_eq!(root.active_turn_id("chosen").as_deref(), Some("native"));
    assert_eq!(owner.active_turn_id("unrelated"), None);
    owner.ingest_native(&note("unrelated", "foreign-again", "turn/started"));
    assert_eq!(owner.active_turn_id("unrelated"), None);
}

#[test]
fn same_thread_owner_views_are_isolated_and_root_ambiguity_fails_closed() {
    let root = CodexTurnTracker::default();
    let old = root.new_owner(Some("thread".into()));
    assert!(old.publish_owner("thread"));
    old.ingest_native(&note("thread", "old", "turn/started"));
    let new = root.new_owner(Some("thread".into()));
    assert!(new.publish_owner("thread"));
    new.ingest_native(&note("thread", "new", "turn/started"));
    assert_eq!(root.active_turn_id("thread"), None);
    assert_eq!(old.active_turn_id("thread").as_deref(), Some("old"));
    assert_eq!(new.active_turn_id("thread").as_deref(), Some("new"));
    old.revoke_owner();
    old.ingest_native(&note("thread", "new", "turn/completed"));
    old.settle_completion("thread", "new");
    assert_eq!(root.active_turn_id("thread").as_deref(), Some("new"));
    assert!(!new
        .lock("thread")
        .completed
        .contains_key(&("thread".into(), "new".into())));
}

#[test]
fn revoked_namespaces_live_only_as_long_as_captured_views() {
    let root = CodexTurnTracker::default();
    for _ in 0..256 {
        let owner = root.new_owner(None);
        let retained = owner.clone();
        owner.revoke_owner();
        drop(owner);
        assert_eq!(root.inner.lock().unwrap().owners.len(), 1);
        drop(retained);
        assert!(root.inner.lock().unwrap().owners.is_empty());
    }
}

#[test]
fn late_start_responses_cannot_revive_either_of_two_native_terminals() {
    let tracker = CodexTurnTracker::default();
    tracker.observe_active_turn("thread", "t1");
    tracker.observe_terminal_turn("thread", "t1");
    tracker.observe_active_turn("thread", "t2");
    tracker.observe_terminal_turn("thread", "t2");
    tracker.record_turn_start_acceptance("thread", "t1");
    assert_eq!(tracker.active_turn_id("thread"), None);
    tracker.record_turn_start_acceptance("thread", "t2");
    assert_eq!(tracker.active_turn_id("thread"), None);
}

#[test]
fn old_projection_settlement_cannot_replace_newer_native_terminal() {
    let tracker = CodexTurnTracker::default();
    tracker.observe_terminal_turn("thread", "t1");
    tracker.observe_active_turn("thread", "t2");
    tracker.observe_terminal_turn("thread", "t2");
    tracker.complete("thread", "t1");
    tracker.record_turn_start_acceptance("thread", "t2");
    assert_eq!(tracker.active_turn_id("thread"), None);
}
