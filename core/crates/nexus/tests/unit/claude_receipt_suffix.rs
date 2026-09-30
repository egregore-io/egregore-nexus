use super::*;
use std::sync::atomic::AtomicU64;

struct SuffixedNativeInput {
    completion: Arc<ClaudeTurnCompletion>,
    sequence: AtomicU64,
}

#[async_trait]
impl HarnessInput for SuffixedNativeInput {
    async fn send_turn(&self, text: &str) -> Result<(), String> {
        let offset = self.sequence.fetch_add(2, Ordering::SeqCst) + 1;
        let prompt = format!("{text}\n");
        let submit = native_hook("UserPromptSubmit", &prompt, offset);
        self.completion.observe_hooks(
            &[submit.clone(), native_hook("Stop", &prompt, offset + 1)],
            Some(offset + 1),
            true,
        );
        self.completion.accept_native_user_input(&submit).await;
        // A repeated receipt must not publish a second acceptance.
        assert!(!self.completion.accept_native_user_input(&submit).await);
        Ok(())
    }
}

#[tokio::test]
async fn submit_newline_completes_observed_turn_and_unblocks_next_equal_input() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let input = ClaudeNativeHarness {
        input: Arc::new(SuffixedNativeInput {
            completion: completion.clone(),
            sequence: AtomicU64::new(0),
        }),
        completion: completion.clone(),
    };
    let observer = Arc::new(CountAcceptance::default());
    let text = "<nexus-batch>one delivery</nexus-batch>";
    for ordinal in 1..=2 {
        tokio::time::timeout(
            Duration::from_secs(1),
            input.send_turn_observed(text, observer.clone()),
        )
        .await
        .expect("a receipt with the transport newline must not wait for the 600s fallback")
        .expect("matching submit and Stop must complete the observed turn");
        assert_eq!(observer.count.load(Ordering::SeqCst), ordinal);
        assert!(!completion.has_open_turn());
    }
}

#[tokio::test]
async fn submit_newline_still_requires_current_owner_session_offset_and_single_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("hooks.jsonl");
    std::fs::write(&log, vec![b' '; 100]).unwrap();
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), Some(log)));
    let observer = Arc::new(CountAcceptance::default());
    let registration = completion.register_accepted_input("same", observer.clone());
    let old = native_hook("UserPromptSubmit", "same\n", 50);
    let mut foreign = native_hook("UserPromptSubmit", "same\n", 110);
    foreign.session_id = Some("foreign".into());
    let current = native_hook("UserPromptSubmit", "same\n", 120);
    completion.observe_hooks(
        &[old.clone(), foreign.clone(), current.clone()],
        Some(120),
        true,
    );
    assert!(!completion.accept_native_user_input(&old).await);
    assert!(!completion.accept_native_user_input(&foreign).await);
    assert!(!registration.was_accepted());
    assert!(completion.accept_native_user_input(&current).await);
    assert!(registration.was_accepted());
    assert!(!completion.accept_native_user_input(&current).await);
    assert_eq!(observer.count.load(Ordering::SeqCst), 1);

    let retired = completion.register_accepted_input("late", observer.clone());
    completion.invalidate();
    let late = native_hook("UserPromptSubmit", "late\n", 130);
    completion.observe_hooks(&[late.clone()], Some(130), true);
    assert!(!completion.accept_native_user_input(&late).await);
    assert!(!retired.was_accepted());
    assert_eq!(observer.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn submit_newline_acceptance_waits_for_its_matching_stop() {
    let completion = Arc::new(ClaudeTurnCompletion::new(Some("native".into()), None));
    let observer = Arc::new(CountAcceptance::default());
    let registration = completion.register_accepted_input("prompt\n", observer.clone());
    let submit = native_hook("UserPromptSubmit", "prompt\n\n", 1);
    completion.observe_hooks(&[submit.clone()], Some(1), true);
    assert!(completion.accept_native_user_input(&submit).await);
    let mut wrong_stop = native_hook("Stop", "", 2);
    wrong_stop.prompt_id = Some("other-turn".into());
    completion.observe_hooks(&[wrong_stop], Some(2), true);
    assert!(registration.wait(Duration::from_millis(25)).await.is_err());
    assert!(completion.has_open_turn());
    completion.observe_hooks(&[native_hook("Stop", "", 3)], Some(3), true);
    registration.wait(Duration::from_secs(1)).await.unwrap();
    assert_eq!(observer.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn raw_writer_payload_roundtrips_through_observed_wrapper_receipt() {
    tokio::time::timeout(Duration::from_secs(5), async {
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
        let terminal = ScreenModelBackend::wrap(backend as Arc<dyn TerminalBackend>);
        let text = "<nexus-batch>raw receipt</nexus-batch>";
        output
            .send(format!("❯ {text}\r\n────────────────────\r\n").into_bytes())
            .unwrap();
        while !terminal.contents().contains(text) {
            tokio::task::yield_now().await;
        }
        let input = ClaudeNativeHarness {
            input: Arc::new(ClaudeRawPtyInput {
                input: Arc::new(LivenessProbeInput { alive: true }),
                terminal,
                completion: completion.clone(),
            }),
            completion: completion.clone(),
        };
        let observer = Arc::new(CountAcceptance::default());
        for ordinal in 1..=2 {
            writes.lock().unwrap().clear();
            let receipt = async {
                loop {
                    if writes.lock().unwrap().iter().any(|bytes| bytes == b"\r") {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                let payload = writes
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|bytes| bytes.starts_with(b"\x1b[200~"))
                    .expect("real raw writer emitted bracketed paste")
                    .clone();
                let prompt = std::str::from_utf8(&payload)
                    .unwrap()
                    .strip_prefix("\x1b[200~")
                    .unwrap()
                    .strip_suffix("\x1b[201~")
                    .unwrap();
                assert_eq!(prompt, format!("{text}\n"));
                let offset = ordinal as u64 * 2 - 1;
                let submit = native_hook("UserPromptSubmit", prompt, offset);
                completion.observe_hooks(&[submit.clone()], Some(offset), true);
                assert!(completion.accept_native_user_input(&submit).await);
                assert!(!completion.accept_native_user_input(&submit).await);
                assert_eq!(observer.count.load(Ordering::SeqCst), ordinal);
                completion.observe_hooks(
                    &[native_hook("Stop", "", offset + 1)],
                    Some(offset + 1),
                    true,
                );
            };
            let (sent, ()) =
                tokio::join!(input.send_turn_observed(text, observer.clone()), receipt);
            sent.expect("actual raw writer receipt must release the observed wrapper");
            assert_eq!(observer.count.load(Ordering::SeqCst), ordinal);
            assert!(!completion.has_open_turn());
        }
    })
    .await
    .expect("raw writer receipt roundtrip watchdog");
}
