use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use nexus_contracts::events::WsEvent;
use nexus_contracts::ports::EventSink;
use nexus_contracts::{AgentUpdateKind, SessionId};
use nexus_harness_claude::native::bridge::{write_launch_settings, ClaudeNativeBridgePaths};
use nexus_harness_claude::native::forwarder::{
    forward_once, forward_once_with_tool_observations, ClaudeToolObservationSink,
};
use nexus_harness_claude::storage::{ClaudeRuntimeLaunch, ClaudeRuntimeStateRepo};
use nexus_store::{
    repos::{Agents, NativeThreadBindings, NewAgent, NewNativeThreadBinding, NewSession, Sessions},
    Store,
};
use nexus_transcript::{ToolCallObservation, ToolCallPhase};
use serde_json::json;

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

#[derive(Clone, Default)]
struct CaptureToolSink {
    observations: Arc<Mutex<Vec<(SessionId, ToolCallObservation)>>>,
}

impl ClaudeToolObservationSink for CaptureToolSink {
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation) {
        self.observations
            .lock()
            .unwrap()
            .push((session.clone(), observation));
    }
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("nexus-claude-forwarder-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn setup(label: &str) -> (Arc<Store>, SessionId, ClaudeNativeBridgePaths, CaptureSink) {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
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
    (store, session, paths, CaptureSink::default())
}

async fn seed_identity_session(store: &Store, runtime: &SessionId, name: &str, agent_id: &str) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: agent_id.to_string(),
            project: "default".to_string(),
            name: Some(name.to_string()),
            default_harness: Some("claude".to_string()),
            role: None,
            tier: Some("agent".to_string()),
            owner: None,
        })
        .await
        .unwrap();
    Sessions::new(store)
        .create(NewSession {
            session_id: runtime.clone(),
            name: Some(name.to_string()),
            agent: Some("claude".to_string()),
            kind: "agent".to_string(),
            role: None,
            tier: "agent".to_string(),
            harness_session_id: None,
            client_key: Some(format!("ck_{name}")),
            cwd: Some("/work/project".to_string()),
            project: "default".to_string(),
            transport: Some("pty".to_string()),
        })
        .await
        .unwrap();
    Sessions::new(store)
        .set_agent_id(runtime, agent_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn forwarder_tolerates_missing_files() {
    let (store, session, paths, sink) = setup("missing").await;

    let stats = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.text_events, 0);
    assert!(sink.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn forwarder_emits_message_deltas_and_turn_end() {
    let (store, session, paths, sink) = setup("deltas").await;
    std::fs::write(
        &paths.message_delta_log_path,
        [
            r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"Hel"}}"#,
            r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"lo"}}"#,
        ]
        .join("\n"),
    )
    .unwrap();
    std::fs::write(
        &paths.hook_log_path,
        r#"{"event":"Stop","payload":{"hook_event_name":"Stop","reason":"end_turn"}}"#,
    )
    .unwrap();

    let stats = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();

    assert_eq!(stats.text_events, 1);
    assert_eq!(stats.turn_end_events, 1);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "Hello");
    assert!(matches!(
        &events[1],
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::TurnEnd,
            ..
        }
    ));

    let state = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert!(state.message_delta_cursor > 0);
    assert!(state.hook_cursor > 0);
}

#[tokio::test]
async fn forwarder_emits_user_prompt_submit_as_user_input() {
    let (store, session, paths, sink) = setup("user-input").await;
    std::fs::write(
        &paths.hook_log_path,
        r#"{"event":"UserPromptSubmit","payload":{"hook_event_name":"UserPromptSubmit","session_id":"claude-real","prompt_id":"p_123","prompt":"hello from web"}}"#,
    )
    .unwrap();
    std::fs::write(
        &paths.message_delta_log_path,
        r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"reply"}}"#,
    )
    .unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.hook_log_path)
        .unwrap()
        .write_all(
            br#"
{"event":"Stop","payload":{"hook_event_name":"Stop","reason":"end_turn"}}"#,
        )
        .unwrap();

    let stats = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.user_input_events, 1);
    assert_eq!(stats.text_events, 1);
    assert_eq!(stats.turn_end_events, 1);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 3);
    assert_agent_update(&events[0], AgentUpdateKind::UserInput, "hello from web");
    assert_agent_update(&events[1], AgentUpdateKind::Text, "reply");
    assert!(matches!(
        &events[2],
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::TurnEnd,
            ..
        }
    ));
}

#[tokio::test]
async fn forwarder_counts_session_start_hook_for_delivery_rearm() {
    let (store, session, paths, sink) = setup("session-start").await;
    std::fs::write(
        &paths.hook_log_path,
        r#"{"event":"SessionStart","payload":{"hook_event_name":"SessionStart","session_id":"claude-real"}}"#,
    )
    .unwrap();

    let stats = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.session_start_events, 1);
    assert!(
        sink.events.lock().unwrap().is_empty(),
        "SessionStart re-arms delivery but does not render as an agent update"
    );
}

#[tokio::test]
async fn forwarder_suppresses_duplicate_final_transcript_text() {
    let (store, session, paths, sink) = setup("dedupe").await;
    std::fs::write(
        &paths.message_delta_log_path,
        r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"done"}}"#,
    )
    .unwrap();
    std::fs::write(
        paths.bridge_dir.join("transcript.jsonl"),
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}"#,
    )
    .unwrap();

    let stats = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.text_events, 1);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "done");
}

#[tokio::test]
async fn forwarder_correlates_claude_2_1_headed_completion_across_id_namespaces() {
    let (store, session, paths, sink) = setup("claude-2-1-headed-dedupe").await;
    let reply = "Hello! What can I help you with today?";

    // Claude 2.1's MessageDisplay hook uses a hook UUID that does not appear in its transcript.
    std::fs::write(
        &paths.message_delta_log_path,
        format!(
            r#"{{"event":"MessageDisplay","payload":{{"hook_event_name":"MessageDisplay","prompt_id":"00000000-0000-4000-8000-000000000106","turn_id":"00000000-0000-4000-8000-000000000108","message_id":"00000000-0000-4000-8000-000000000109","index":0,"delta":{reply:?},"final":true}}}}"#
        ),
    )
    .unwrap();
    let streamed = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();
    assert_eq!(streamed.text_events, 1);
    assert_agent_update(
        &sink.events.lock().unwrap()[0],
        AgentUpdateKind::Text,
        reply,
    );
    sink.events.lock().unwrap().clear();

    // The durable transcript then writes thinking + text rows under a different model msg_* id.
    // Both rows carry stop_reason=end_turn, while the native Stop hook is the sole real boundary.
    std::fs::write(
        paths.bridge_dir.join("transcript.jsonl"),
        [
            json!({
                "type": "assistant",
                "uuid": "00000000-0000-4000-8000-000000000107",
                "message": {
                    "id": "msg_synthetic_completion_001",
                    "role": "assistant",
                    "content": [{ "type": "thinking", "thinking": "hidden" }],
                    "stop_reason": "end_turn",
                },
            })
            .to_string(),
            json!({
                "type": "assistant",
                "uuid": "00000000-0000-4000-8000-000000000110",
                "message": {
                    "id": "msg_synthetic_completion_001",
                    "role": "assistant",
                    "content": [{ "type": "text", "text": reply }],
                    "stop_reason": "end_turn",
                },
            })
            .to_string(),
        ]
        .join("\n"),
    )
    .unwrap();
    std::fs::write(
        &paths.hook_log_path,
        format!(
            r#"{{"event":"Stop","payload":{{"hook_event_name":"Stop","prompt_id":"00000000-0000-4000-8000-000000000106","last_assistant_message":{reply:?}}}}}"#
        ),
    )
    .unwrap();

    let finalized = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();

    assert_eq!(finalized.text_events, 0);
    assert_eq!(finalized.turn_end_events, 1);
    {
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1, "only the native Stop boundary remains");
        assert!(matches!(
            &events[0],
            WsEvent::AgentUpdate {
                kind: AgentUpdateKind::TurnEnd,
                ..
            }
        ));
    }
    assert!(
        nexus_store::repos::ProducerIdentities::new(&store)
            .list_for_runtime(&session.0)
            .await
            .unwrap()
            .is_empty(),
        "the displayed occurrence is consumed exactly once"
    );
    sink.events.lock().unwrap().clear();
    let replay = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(replay, Default::default());
    assert!(sink.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn forwarder_groups_transcript_only_thinking_and_text_into_one_completion() {
    let (store, session, paths, sink) = setup("transcript-group").await;
    std::fs::write(
        paths.bridge_dir.join("transcript.jsonl"),
        [
            r#"{"type":"assistant","message":{"id":"msg_fallback","role":"assistant","content":[{"type":"thinking","thinking":"hidden"}],"stop_reason":"end_turn"}}"#,
            r#"{"type":"assistant","message":{"id":"msg_fallback","role":"assistant","content":[{"type":"text","text":"fallback"}],"stop_reason":"end_turn"}}"#,
        ]
        .join("\n"),
    )
    .unwrap();
    let stats = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.text_events, 1);
    assert_eq!(stats.turn_end_events, 1);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "fallback");
    assert!(matches!(
        &events[1],
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::TurnEnd,
            ..
        }
    ));
}

#[tokio::test]
async fn forwarder_preserves_two_identical_displayed_completions_as_two_occurrences() {
    let (store, session, paths, sink) = setup("identical-occurrences").await;
    let transcript_path = paths.bridge_dir.join("transcript.jsonl");

    for turn in 1..=2 {
        let display = format!(
            r#"{{"event":"MessageDisplay","payload":{{"hook_event_name":"MessageDisplay","prompt_id":"prompt-{turn}","message_id":"hook-{turn}","index":0,"delta":"same reply","final":true}}}}"#
        );
        if turn == 1 {
            std::fs::write(&paths.message_delta_log_path, display).unwrap();
        } else {
            std::fs::OpenOptions::new()
                .append(true)
                .open(&paths.message_delta_log_path)
                .unwrap()
                .write_all(format!("\n{display}").as_bytes())
                .unwrap();
        }
        forward_once(
            store.clone(),
            session.clone(),
            paths.clone(),
            Arc::new(sink.clone()),
        )
        .await
        .unwrap();

        let model_message_id = format!("model-{turn}");
        let transcript = [
            json!({
                "type": "assistant",
                "message": {
                    "id": model_message_id.clone(),
                    "role": "assistant",
                    "content": [{ "type": "thinking", "thinking": "hidden" }],
                    "stop_reason": "end_turn",
                },
            })
            .to_string(),
            json!({
                "type": "assistant",
                "message": {
                    "id": model_message_id,
                    "role": "assistant",
                    "content": [{ "type": "text", "text": "same reply" }],
                    "stop_reason": "end_turn",
                },
            })
            .to_string(),
        ]
        .join("\n");
        let stop = format!(
            r#"{{"event":"Stop","payload":{{"hook_event_name":"Stop","prompt_id":"prompt-{turn}","last_assistant_message":"same reply"}}}}"#
        );
        for (path, body) in [(&transcript_path, transcript), (&paths.hook_log_path, stop)] {
            let mut options = std::fs::OpenOptions::new();
            options.create(true).append(true);
            let mut file = options.open(path).unwrap();
            if turn > 1 {
                file.write_all(b"\n").unwrap();
            }
            file.write_all(body.as_bytes()).unwrap();
        }
        forward_once(
            store.clone(),
            session.clone(),
            paths.clone(),
            Arc::new(sink.clone()),
        )
        .await
        .unwrap();
    }

    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 4);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "same reply");
    assert!(matches!(
        &events[1],
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::TurnEnd,
            ..
        }
    ));
    assert_agent_update(&events[2], AgentUpdateKind::Text, "same reply");
    assert!(matches!(
        &events[3],
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::TurnEnd,
            ..
        }
    ));
}

#[tokio::test]
async fn forwarder_emits_transcript_tool_use_and_result_updates() {
    let (store, session, paths, sink) = setup("tool-calls").await;
    std::fs::write(
        paths.bridge_dir.join("transcript.jsonl"),
        [
            r#"{"type":"assistant","sessionId":"claude-real","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_read","name":"Read","input":{"file_path":"README.md"}}]}}"#,
            r#"{"type":"user","sessionId":"claude-real","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_read","content":"README contents"}]}}"#,
        ]
        .join("\n"),
    )
    .unwrap();

    let stats = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.tool_call_events, 2);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_tool_call_update(
        &events[0],
        json!({
            "id": "toolu_read",
            "tool": "Read",
            "title": "Read",
            "status": "in_progress",
            "input": { "file_path": "README.md" },
        }),
    );
    assert_tool_call_update(
        &events[1],
        json!({
            "id": "toolu_read",
            "status": "completed",
            "content": "README contents",
        }),
    );
}

#[tokio::test]
async fn forwarder_publishes_transcript_tool_call_observations() {
    let (store, session, paths, sink) = setup("tool-call-observations").await;
    let tool_sink = CaptureToolSink::default();
    std::fs::write(
        paths.bridge_dir.join("transcript.jsonl"),
        [
            r#"{"type":"assistant","sessionId":"claude-real","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_read","name":"Read","input":{"file_path":"README.md"}}]}}"#,
            r#"{"type":"user","sessionId":"claude-real","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_read","content":"README contents","is_error":false}]}}"#,
        ]
        .join("\n"),
    )
    .unwrap();
    let tool_events: Arc<dyn ClaudeToolObservationSink> = Arc::new(tool_sink.clone());

    let stats = forward_once_with_tool_observations(
        store,
        session.clone(),
        paths,
        Arc::new(sink.clone()),
        Some(tool_events),
    )
    .await
    .unwrap();

    assert_eq!(stats.tool_call_events, 2);
    assert_eq!(
        sink.events.lock().unwrap().len(),
        2,
        "transcript fallback still renders the visible tool-call row"
    );
    let observations = tool_sink.observations.lock().unwrap();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].0, session);
    assert_tool_observation(
        &observations[0].1,
        Some("toolu_read"),
        "Read",
        ToolCallPhase::Pre,
        true,
    );
    assert_tool_observation(
        &observations[1].1,
        Some("toolu_read"),
        "",
        ToolCallPhase::Post,
        true,
    );
}

#[tokio::test]
async fn forwarder_publishes_hook_tool_call_observations_without_visible_rows() {
    let (store, session, paths, sink) = setup("hook-tool-call-observations").await;
    let tool_sink = CaptureToolSink::default();
    std::fs::write(
        &paths.hook_log_path,
        [
            r#"{"event":"PreToolUse","payload":{"hook_event_name":"PreToolUse","session_id":"claude-real","tool_use_id":"toolu_edit","tool_name":"Edit","tool_input":{"file_path":"README.md"}}}"#,
            r#"{"event":"PostToolUse","payload":{"hook_event_name":"PostToolUse","session_id":"claude-real","tool_use_id":"toolu_edit","status":"failed","error":"permission denied"}}"#,
        ]
        .join("\n"),
    )
    .unwrap();
    let tool_events: Arc<dyn ClaudeToolObservationSink> = Arc::new(tool_sink.clone());

    let stats = forward_once_with_tool_observations(
        store,
        session,
        paths,
        Arc::new(sink.clone()),
        Some(tool_events),
    )
    .await
    .unwrap();

    assert_eq!(
        stats.tool_call_events, 0,
        "hook observations are developer events only; they do not render AG-UI tool rows"
    );
    assert!(
        sink.events.lock().unwrap().is_empty(),
        "hook tool observations must not emit WsEvent agent updates"
    );
    let observations = tool_sink.observations.lock().unwrap();
    assert_eq!(observations.len(), 2);
    assert_tool_observation(
        &observations[0].1,
        Some("toolu_edit"),
        "Edit",
        ToolCallPhase::Pre,
        true,
    );
    assert_tool_observation(
        &observations[1].1,
        Some("toolu_edit"),
        "",
        ToolCallPhase::Post,
        false,
    );
}

#[tokio::test]
async fn forwarder_resumes_from_persisted_cursors() {
    let (store, session, paths, sink) = setup("resume").await;
    std::fs::write(
        &paths.message_delta_log_path,
        r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"one"}}"#,
    )
    .unwrap();
    forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();
    sink.events.lock().unwrap().clear();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.message_delta_log_path)
        .unwrap()
        .write_all(
            br#"
{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"two"}}"#,
        )
        .unwrap();

    forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "two");
}

#[tokio::test]
async fn forwarder_retries_partial_trailing_hook_object() {
    let (store, session, paths, sink) = setup("partial").await;
    std::fs::write(
        &paths.message_delta_log_path,
        r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"one"}}{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"two"#,
    )
    .unwrap();

    forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();

    {
        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_agent_update(&events[0], AgentUpdateKind::Text, "one");
    }
    let partial_len = std::fs::metadata(&paths.message_delta_log_path)
        .unwrap()
        .len() as i64;
    let state = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert!(state.message_delta_cursor < partial_len);

    sink.events.lock().unwrap().clear();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&paths.message_delta_log_path)
        .unwrap()
        .write_all(br#""}}"#)
        .unwrap();

    forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "two");
}

#[tokio::test]
async fn forwarder_discovers_real_transcript_path_without_replaying_old_history() {
    let (store, session, paths, sink) = setup("transcript-path").await;
    let real_transcript = paths.bridge_dir.join("real-claude-transcript.jsonl");
    std::fs::write(
        &real_transcript,
        r#"{"type":"assistant","sessionId":"claude-real","message":{"role":"assistant","content":[{"type":"text","text":"old history"}],"stop_reason":"end_turn"}}"#,
    )
    .unwrap();
    let old_len = std::fs::metadata(&real_transcript).unwrap().len() as i64;
    std::fs::write(
        &paths.hook_log_path,
        format!(
            r#"{{"event":"UserPromptSubmit","payload":{{"hook_event_name":"UserPromptSubmit","session_id":"claude-real","transcript_path":"{}"}}}}"#,
            real_transcript.display()
        ),
    )
    .unwrap();

    let stats = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();

    assert_eq!(stats.text_events, 0);
    assert!(sink.events.lock().unwrap().is_empty());
    let state = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.claude_session_id.as_deref(), Some("claude-real"));
    assert_eq!(
        state.transcript_path.as_deref(),
        Some(real_transcript.as_path())
    );
    assert_eq!(state.transcript_cursor, old_len);

    sink.events.lock().unwrap().clear();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&real_transcript)
        .unwrap()
        .write_all(
            br#"
{"type":"assistant","sessionId":"claude-real","message":{"role":"assistant","content":[{"type":"text","text":"fresh reply"}],"stop_reason":"end_turn"}}"#,
        )
        .unwrap();

    let stats = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();

    assert_eq!(stats.text_events, 1);
    assert_eq!(stats.turn_end_events, 1);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "fresh reply");
    assert!(matches!(
        &events[1],
        WsEvent::AgentUpdate {
            kind: AgentUpdateKind::TurnEnd,
            ..
        }
    ));
}

#[tokio::test]
async fn forwarder_repairs_consumed_hook_binding_when_stored_transcript_is_missing() {
    let (store, session, paths, sink) = setup("stale-static-binding").await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let stale_transcript = paths.bridge_dir.join("never-created-transcript.jsonl");
    repo.set_claude_session(&session, "claude-static-wrong", Some(stale_transcript))
        .await
        .unwrap();

    let real_transcript = paths.bridge_dir.join("actual-claude-transcript.jsonl");
    std::fs::write(
        &real_transcript,
        r#"{"type":"assistant","sessionId":"claude-hook-truth","message":{"role":"assistant","content":[{"type":"text","text":"old transcript history"}],"stop_reason":"end_turn"}}"#,
    )
    .unwrap();
    let old_len = std::fs::metadata(&real_transcript).unwrap().len() as i64;
    std::fs::write(
        &paths.hook_log_path,
        format!(
            r#"{{"event":"SessionStart","payload":{{"hook_event_name":"SessionStart","session_id":"claude-hook-truth","transcript_path":"{}"}}}}"#,
            real_transcript.display()
        ),
    )
    .unwrap();
    let consumed_hook_cursor = std::fs::metadata(&paths.hook_log_path).unwrap().len() as i64;
    repo.set_hook_cursor(&session, consumed_hook_cursor)
        .await
        .unwrap();

    let stats = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();

    assert_eq!(stats.text_events, 0);
    assert!(
        sink.events.lock().unwrap().is_empty(),
        "repair must not replay old transcript history"
    );
    let state = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(
        state.claude_session_id.as_deref(),
        Some("claude-hook-truth")
    );
    assert_eq!(
        state.transcript_path.as_deref(),
        Some(real_transcript.as_path())
    );
    assert_eq!(state.transcript_cursor, old_len);

    std::fs::OpenOptions::new()
        .append(true)
        .open(&real_transcript)
        .unwrap()
        .write_all(
            br#"
{"type":"assistant","sessionId":"claude-hook-truth","message":{"role":"assistant","content":[{"type":"text","text":"fresh visible reply"}],"stop_reason":"end_turn"}}"#,
        )
        .unwrap();

    let stats = forward_once(store, session, paths, Arc::new(sink.clone()))
        .await
        .unwrap();
    assert_eq!(stats.text_events, 1);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_agent_update(&events[0], AgentUpdateKind::Text, "fresh visible reply");
}

#[tokio::test]
async fn forwarder_does_not_rebind_full_hook_path_without_native_session_id() {
    let (store, session, paths, sink) = setup("path-without-native-id").await;
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let stale_transcript = paths.bridge_dir.join("missing-static-transcript.jsonl");
    repo.set_transcript_path(&session, stale_transcript.clone())
        .await
        .unwrap();

    let unowned_transcript = paths.bridge_dir.join("unowned-transcript.jsonl");
    std::fs::write(
        &paths.hook_log_path,
        format!(
            r#"{{"event":"SessionStart","payload":{{"hook_event_name":"SessionStart","transcript_path":"{}"}}}}"#,
            unowned_transcript.display()
        ),
    )
    .unwrap();
    let consumed_hook_cursor = std::fs::metadata(&paths.hook_log_path).unwrap().len() as i64;
    repo.set_hook_cursor(&session, consumed_hook_cursor)
        .await
        .unwrap();

    let stats = forward_once(
        store.clone(),
        session.clone(),
        paths,
        Arc::new(sink.clone()),
    )
    .await
    .unwrap();

    assert_eq!(stats.text_events, 0);
    assert!(sink.events.lock().unwrap().is_empty());
    let state = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert!(state.claude_session_id.is_none());
    assert_eq!(state.transcript_path, Some(stale_transcript));
}

#[tokio::test]
async fn forwarder_ignores_legacy_global_binding_and_keeps_claude_hint_runtime_scoped() {
    let (store, session, paths, sink) = setup("capstone-rebind-reject").await;
    seed_identity_session(&store, &session, "attempted", "a_attempted").await;
    let owner = SessionId("s_true_owner".into());
    seed_identity_session(&store, &owner, "owner", "a_owner").await;
    NativeThreadBindings::new(&store)
        .claim(NewNativeThreadBinding {
            harness: "claude".into(),
            native_thread_id: "claude-owned-by-other-agent".into(),
            agent_id: "a_owner".into(),
            project: "default".into(),
            runtime_id: Some(owner.0.clone()),
        })
        .await
        .unwrap();

    let real_transcript = paths.bridge_dir.join("owned-by-other-agent.jsonl");
    std::fs::write(
        &paths.hook_log_path,
        format!(
            r#"{{"event":"SessionStart","payload":{{"hook_event_name":"SessionStart","session_id":"claude-owned-by-other-agent","transcript_path":"{}"}}}}"#,
            real_transcript.display()
        ),
    )
    .unwrap();
    let consumed_hook_cursor = std::fs::metadata(&paths.hook_log_path).unwrap().len() as i64;
    ClaudeRuntimeStateRepo::new(&store)
        .set_hook_cursor(&session, consumed_hook_cursor)
        .await
        .unwrap();

    let stats = forward_once(
        store.clone(),
        session.clone(),
        paths,
        Arc::new(sink.clone()),
    )
    .await
    .expect("a legacy global Claude binding must not decide Nexus runtime identity");

    assert_eq!(stats, Default::default());
    assert!(
        sink.events.lock().unwrap().is_empty(),
        "repairing a consumed hook must not replay visible events"
    );
    let state = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("runtime state");
    assert_eq!(
        state.claude_session_id.as_deref(),
        Some("claude-owned-by-other-agent"),
        "Claude's id is retained as this runtime's opaque provider correlation key"
    );
}

#[tokio::test]
async fn forwarder_keeps_duplicate_claude_hint_scoped_to_attempting_runtime() {
    let (store, session, paths, sink) = setup("duplicate-native-owner").await;
    let owner = SessionId("s_existing_owner".into());
    let owner_transcript = paths.bridge_dir.join("owner-transcript.jsonl");
    let repo = ClaudeRuntimeStateRepo::new(&store);
    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: owner.clone(),
        bridge_dir: paths.bridge_dir.join("owner-bridge"),
        claude_session_id: None,
        launch_cwd: temp_dir("duplicate-native-owner-cwd"),
        transcript_path: Some(owner_transcript),
        bridge_pid: None,
        hook_pids_json: None,
    })
    .await
    .unwrap();
    repo.set_session(
        &owner,
        "claude-real",
        Some(paths.bridge_dir.join("owner-real.jsonl")),
    )
    .await
    .unwrap();
    let attempted_transcript = paths.bridge_dir.join("attempted-real.jsonl");
    std::fs::write(
        &paths.hook_log_path,
        format!(
            r#"{{"event":"UserPromptSubmit","payload":{{"hook_event_name":"UserPromptSubmit","session_id":"claude-real","transcript_path":"{}","prompt":"wrong bridge"}}}}"#,
            attempted_transcript.display()
        ),
    )
    .unwrap();

    let stats = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .expect("duplicate Claude hints must not merge or reject Nexus runtimes");

    assert_eq!(stats.user_input_events, 1);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        WsEvent::AgentUpdate {
            session_id,
            kind,
            data,
        } => {
            assert_eq!(session_id, &session);
            assert_eq!(*kind, AgentUpdateKind::UserInput);
            assert_eq!(data["text"], "wrong bridge");
        }
        other => panic!("expected runtime-scoped agent.update, got {other:?}"),
    }
    drop(events);
    let attempted = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("attempting runtime state");
    assert_eq!(
        attempted.claude_session_id.as_deref(),
        Some("claude-real"),
        "the provider key is scoped by Nexus runtime rather than globally claimed"
    );
}

#[tokio::test]
async fn forwarder_scopes_duplicate_claude_hint_without_transcript_path() {
    let (store, session, paths, sink) = setup("duplicate-native-owner-no-path").await;
    let owner = SessionId("s_existing_owner_no_path".into());
    let repo = ClaudeRuntimeStateRepo::new(&store);
    repo.upsert_launch(ClaudeRuntimeLaunch {
        runtime_id: owner.clone(),
        bridge_dir: paths.bridge_dir.join("owner-bridge"),
        claude_session_id: None,
        launch_cwd: temp_dir("duplicate-native-owner-no-path-cwd"),
        transcript_path: None,
        bridge_pid: None,
        hook_pids_json: None,
    })
    .await
    .unwrap();
    repo.set_session(&owner, "claude-real-no-path", None)
        .await
        .unwrap();
    std::fs::write(
        &paths.hook_log_path,
        r#"{"event":"UserPromptSubmit","payload":{"hook_event_name":"UserPromptSubmit","session_id":"claude-real-no-path","prompt":"wrong bridge without path"}}"#,
    )
    .unwrap();

    let stats = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .expect("a duplicate opaque provider hint must remain runtime-scoped");

    assert_eq!(stats.user_input_events, 1);
    let events = sink.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        WsEvent::AgentUpdate {
            session_id,
            kind,
            data,
        } => {
            assert_eq!(session_id, &session);
            assert_eq!(*kind, AgentUpdateKind::UserInput);
            assert_eq!(data["text"], "wrong bridge without path");
        }
        other => panic!("expected runtime-scoped agent.update, got {other:?}"),
    }
    drop(events);
    let attempted = repo
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("attempting runtime state");
    assert_eq!(
        attempted.claude_session_id.as_deref(),
        Some("claude-real-no-path"),
        "the provider key remains an opaque correlation hint even without a transcript path"
    );
}

#[tokio::test]
async fn forwarder_rejects_mixed_hook_native_session_ids_before_emitting() {
    let (store, session, paths, sink) = setup("mixed-hook-session-ids").await;
    std::fs::write(
        &paths.hook_log_path,
        [
            r#"{"event":"UserPromptSubmit","payload":{"hook_event_name":"UserPromptSubmit","session_id":"foreign-claude-session","prompt":"foreign prompt"}}"#,
            r#"{"event":"UserPromptSubmit","payload":{"hook_event_name":"UserPromptSubmit","session_id":"current-claude-session","prompt":"current prompt"}}"#,
        ]
        .join("\n"),
    )
    .unwrap();

    let err = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .expect_err("mixed native Claude ids in one hook batch must fail closed");

    assert!(
        err.to_string().contains("foreign-claude-session")
            && err.to_string().contains("current-claude-session"),
        "unexpected error: {err}"
    );
    assert!(
        sink.events.lock().unwrap().is_empty(),
        "mixed native ids must be rejected before any hook user_input emits"
    );
    let attempted = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("attempting runtime state");
    assert!(
        attempted.claude_session_id.is_none(),
        "mixed-id rejection must not persist the latest id"
    );
}

#[tokio::test]
async fn forwarder_rejects_mixed_message_delta_native_session_ids_before_emitting() {
    let (store, session, paths, sink) = setup("mixed-message-session-ids").await;
    std::fs::write(
        &paths.message_delta_log_path,
        [
            r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","session_id":"foreign-claude-session","delta":"foreign"}}"#,
            r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","session_id":"current-claude-session","delta":"current"}}"#,
        ]
        .join("\n"),
    )
    .unwrap();

    let err = forward_once(
        store.clone(),
        session.clone(),
        paths.clone(),
        Arc::new(sink.clone()),
    )
    .await
    .expect_err("mixed native Claude ids in one message-delta batch must fail closed");

    assert!(
        err.to_string().contains("foreign-claude-session")
            && err.to_string().contains("current-claude-session"),
        "unexpected error: {err}"
    );
    assert!(
        sink.events.lock().unwrap().is_empty(),
        "mixed native ids must be rejected before message deltas are concatenated"
    );
    let attempted = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(&session)
        .await
        .unwrap()
        .expect("attempting runtime state");
    assert!(
        attempted.claude_session_id.is_none(),
        "mixed-id rejection must not persist the latest id"
    );
}

fn assert_agent_update(event: &WsEvent, kind: AgentUpdateKind, text: &str) {
    match event {
        WsEvent::AgentUpdate { kind: k, data, .. } => {
            assert_eq!(*k, kind);
            assert_eq!(data["text"], text);
        }
        other => panic!("expected agent.update, got {other:?}"),
    }
}

fn assert_tool_call_update(event: &WsEvent, expected: serde_json::Value) {
    match event {
        WsEvent::AgentUpdate { kind, data, .. } => {
            assert_eq!(*kind, AgentUpdateKind::ToolCall);
            assert_eq!(*data, expected);
        }
        other => panic!("expected tool_call agent.update, got {other:?}"),
    }
}

fn assert_tool_observation(
    observation: &ToolCallObservation,
    id: Option<&str>,
    tool: &str,
    phase: ToolCallPhase,
    ok: bool,
) {
    assert_eq!(observation.tool_call_id.as_deref(), id);
    assert_eq!(observation.tool, tool);
    assert_eq!(observation.phase, phase);
    assert_eq!(observation.ok, ok);
}

use std::io::Write;
