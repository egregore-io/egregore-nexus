// Opt-in, real-provider acceptance. Never uses an existing Nexus or Claude identity.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit operator permission, native Claude credentials, and a disposable directory"]
async fn claude_operator_live_disposable_pty() {
    use super::*;
    fn sleep_child(pid: u32, depth: usize) -> Option<u32> {
        if depth == 0 {
            return None;
        }
        let children = std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/children")).ok()?;
        for child in children
            .split_whitespace()
            .filter_map(|p| p.parse::<u32>().ok())
        {
            if std::fs::read_to_string(format!("/proc/{child}/comm"))
                .ok()
                .is_some_and(|v| v.trim() == "sleep")
            {
                return Some(child);
            }
            if let Some(found) = sleep_child(child, depth - 1) {
                return Some(found);
            }
        }
        None
    }
    fn process_executing(pid: u32) -> bool {
        std::fs::read(format!("/proc/{pid}/cmdline"))
            .ok()
            .is_some_and(|v| !v.is_empty())
    }
    fn process_age_seconds(pid: u32) -> f64 {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        let ticks: f64 = stat
            .rsplit_once(')')
            .unwrap()
            .1
            .split_whitespace()
            .nth(19)
            .unwrap()
            .parse()
            .unwrap();
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        assert!(hz > 0);
        let uptime: f64 = std::fs::read_to_string("/proc/uptime")
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap();
        uptime - ticks / hz as f64
    }
    let program = std::env::var("NEXUS_CLAUDE_LIVE_PROGRAM").expect("explicit native executable");
    let dir = PathBuf::from(
        std::env::var("NEXUS_CLAUDE_LIVE_DIR").expect("disposable artifact directory"),
    );
    assert!(dir.starts_with("/tmp/") && dir.read_dir().unwrap().next().is_none());
    let root = uuid::Uuid::new_v4().to_string();
    let hook_log = dir.join("hooks.jsonl");
    let hook_script = dir.join("hook.py");
    std::fs::write(
        &hook_script,
        r#"import json,sys
from pathlib import Path
d=json.load(sys.stdin)
d['event']=d.get('hook_event_name')
with Path(sys.argv[1]).open('a') as f: f.write(json.dumps(d)+'\n')
"#,
    )
    .unwrap();
    let command = format!(
        "/usr/bin/python3 {} {}",
        hook_script.display(),
        hook_log.display()
    );
    let mut hooks = serde_json::Map::new();
    for event in [
        "SessionStart",
        "UserPromptSubmit",
        "PreToolUse",
        "PostToolUse",
        "PostToolUseFailure",
        "Stop",
    ] {
        hooks.insert(
            event.into(),
            serde_json::json!([{"hooks":[{"type":"command","command":command}]}]),
        );
    }
    let settings = dir.join("settings.json");
    std::fs::write(
        &settings,
        serde_json::to_vec(&serde_json::json!({"hooks":hooks})).unwrap(),
    )
    .unwrap();
    let mut cmd = portable_pty::CommandBuilder::new(program);
    scrub_inherited_nexus_identity_env(&mut cmd);
    cmd.cwd(&dir);
    for arg in ["--session-id", &root, "--name", "NexusInputProbe", "--setting-sources", "",
        "--settings", settings.to_str().unwrap(), "--strict-mcp-config", "--mcp-config", "{\"mcpServers\":{}}",
        "--tools", "Bash", "--allowedTools", "Bash(sleep 12)", "--permission-mode", "dontAsk",
        "--system-prompt", "You are a disposable PTY input test. Only run the exact sleep command requested, and otherwise reply briefly. Never access files or other sessions."] {
        cmd.arg(arg);
    }
    apply_headed_terminal_environment(&mut cmd);
    let pty = Arc::new(
        PtySession::spawn(
            cmd,
            PtySize {
                rows: 40,
                cols: 120,
                pixel_width: 0,
                pixel_height: 0,
            },
        )
        .unwrap(),
    );
    struct Cleanup(Arc<PtySession>);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.kill();
        }
    }
    let _cleanup = Cleanup(pty.clone());
    let terminal = ScreenModelBackend::wrap(pty.clone());
    terminal.spawn_query_responder();
    let completion = Arc::new(ClaudeTurnCompletion::new(
        Some(root.clone()),
        Some(hook_log.clone()),
    ));
    let captured = completion.clone();
    let seen = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let observed = seen.clone();
    let poller = tokio::spawn(async move {
        let mut offset = 0usize;
        loop {
            if let Ok(bytes) = std::fs::read(&hook_log) {
                for line in bytes[offset..].split_inclusive(|b| *b == b'\n') {
                    if line.last() != Some(&b'\n') {
                        break;
                    }
                    offset += line.len();
                    let value: serde_json::Value = serde_json::from_slice(line).unwrap();
                    let record = parse_hook_record(&value, offset as u64);
                    observed.lock().unwrap().push(value);
                    captured.observe_hooks(&[record.clone()], Some(offset as u64), true);
                    captured.accept_native_user_input(&record).await;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });
    struct PollCleanup(tokio::task::JoinHandle<()>);
    impl Drop for PollCleanup {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _poll_cleanup = PollCleanup(poller);
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut trusted = false;
    loop {
        let screen = terminal.contents();
        std::fs::write(dir.join("screen.txt"), &screen).unwrap();
        if screen.contains("Yes, I trust this folder") {
            if !trusted {
                // Consent is limited to this empty disposable directory, never an operator cwd.
                std::fs::write(dir.join("trust-screen.txt"), &screen).unwrap();
                // The first render can precede the native keyboard listener becoming ready.
                tokio::time::sleep(Duration::from_secs(1)).await;
                if screen.contains("❯ No, exit") {
                    pty.write(b"\x1b[B").unwrap();
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                assert!(
                    terminal.contents().contains("❯ Yes, I trust"),
                    "must select disposable trust explicitly before Enter"
                );
                pty.write(b"\r").unwrap();
                trusted = true;
            }
            assert!(
                Instant::now() < deadline,
                "disposable workspace trust did not settle"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if claude_raw_prompt_rendered(&screen) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "native startup unavailable; inspect disposable screen.txt"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let transport = PtyTransport::default();
    let session = SessionId(format!("s_input_probe_{}", uuid::Uuid::new_v4()));
    transport.bind(
        session.clone(),
        Arc::new(ClaudeNativeHarness {
            input: Arc::new(ClaudeRawPtyInput {
                input: pty.clone(),
                terminal: terminal.clone(),
                completion: completion.clone(),
            }),
            completion: completion.clone(),
        }),
    );
    for redirect in [false, true] {
        let before = seen.lock().unwrap().len();
        let first = transport
            .prompt(
                &session,
                "Run exactly sleep 12 once, then reply SLEEP_DONE. No other tools or actions."
                    .into(),
            )
            .await;
        std::fs::write(dir.join("screen.txt"), terminal.contents()).unwrap();
        first.unwrap();
        let sleep_pid = tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                if seen.lock().unwrap()[before..].iter().any(|v| {
                    v["event"] == "PreToolUse"
                        && v["tool_input"]["command"]
                            .as_str()
                            .is_some_and(|c| c.trim() == "sleep 12")
                }) {
                    if let Some(pid) = sleep_child(pty.child_pid().unwrap(), 12) {
                        break pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("must witness real native tool work before operator input");
        assert!(completion.has_open_turn());
        let event = nexus_contracts::WsEvent::AgentUpdate {
            session_id: session.clone(),
            kind: nexus_contracts::AgentUpdateKind::UserInput,
            data: serde_json::json!({"text":"Reply INPUT_PROBE_OK only."}),
        };
        let started = Instant::now();
        let input_before = seen.lock().unwrap().len();
        assert!(
            process_age_seconds(sleep_pid) < 1.0,
            "sleep must have more than 11s remaining before input"
        );
        let interrupt_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        let omit_interrupt = std::env::var_os("NEXUS_CLAUDE_LIVE_OMIT_INTERRUPT").is_some();
        let submission = async {
            if redirect && !omit_interrupt {
                transport
                    .steer_observed(
                        &session,
                        "Reply INPUT_PROBE_OK only.".into(),
                        Arc::new(DiscardClaudeDisplay),
                        event,
                    )
                    .await
                    .map(|_| ())
            } else {
                transport
                    .prompt_observed(
                        &session,
                        "Reply INPUT_PROBE_OK only.".into(),
                        Arc::new(DiscardClaudeDisplay),
                        event,
                    )
                    .await
            }
        };
        let result = if redirect {
            tokio::time::timeout_at(interrupt_deadline, submission)
                .await
                .expect("interrupt deadline includes native receipt wait")
        } else {
            submission.await
        };
        std::fs::write(dir.join("screen.txt"), terminal.contents()).unwrap();
        eprintln!(
            "native input redirect={redirect}, receipt_ms={}, result={result:?}",
            started.elapsed().as_millis()
        );
        result.unwrap();
        assert_eq!(
            seen.lock().unwrap()[input_before..]
                .iter()
                .filter(|v| v["event"] == "UserPromptSubmit"
                    && v["session_id"] == root
                    && v["prompt"] == "Reply INPUT_PROBE_OK only."
                    && v["prompt_id"].as_str().is_some_and(|id| !id.is_empty()))
                .count(),
            1,
            "exact new input must have one independent native receipt on the fresh root"
        );
        if redirect {
            assert!(
                tokio::time::Instant::now() < interrupt_deadline,
                "receipt cannot consume the interruption deadline"
            );
            tokio::time::timeout_at(interrupt_deadline, async {
                loop {
                    if !process_executing(sleep_pid) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("explicit redirect must stop captured native sleep before its 12s natural completion");
            assert!(
                tokio::time::Instant::now() < interrupt_deadline,
                "a late natural exit is not interruption proof"
            );
        } else {
            assert!(
                process_executing(sleep_pid),
                "normal input leaves actual native work running"
            );
            assert!(
                !seen.lock().unwrap()[before..]
                    .iter()
                    .any(|v| v["is_interrupt"] == true),
                "normal input must not interrupt"
            );
        }
        tokio::time::timeout(Duration::from_secs(45), async {
            while completion.has_open_turn() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("native completion must arrive without synthetic Stop");
        assert!(
            !completion.is_unknown(),
            "matching native Stop restores known closed state"
        );
    }
    completion.invalidate();
    eprintln!("disposable Claude root={root}; artifacts={}", dir.display());
}
