use nexus_harness_claude::native::message_delta::parse_message_display_jsonl;
use nexus_harness_claude::native::transcript::{
    parse_transcript_jsonl, parse_transcript_line, ClaudeToolUpdate, CompactionPhase, TurnBoundary,
};
use serde_json::json;

#[test]
fn message_display_records_produce_text_deltas_in_order() {
    let jsonl = [
        r#"{"event":"MessageDisplay","payload":{"session_id":"claude-s","transcript_path":"/tmp/t.jsonl","hook_event_name":"MessageDisplay","prompt_id":"prompt-1","turn_id":"turn-1","message_id":"msg-1","index":0,"delta":"Hel","final":false}}"#,
        r#"{"hook_event_name":"MessageDisplay","session_id":"claude-s","prompt_id":"prompt-1","turn_id":"turn-1","message_id":"msg-1","index":1,"delta":"lo","final":true}"#,
    ]
    .join("\n");

    let deltas = parse_message_display_jsonl(&jsonl);

    assert_eq!(deltas.len(), 2);
    assert_eq!(deltas[0].session_id.as_deref(), Some("claude-s"));
    assert_eq!(deltas[0].transcript_path.as_deref(), Some("/tmp/t.jsonl"));
    assert_eq!(deltas[0].turn_id.as_deref(), Some("turn-1"));
    assert_eq!(deltas[0].prompt_id.as_deref(), Some("prompt-1"));
    assert_eq!(deltas[0].message_id.as_deref(), Some("msg-1"));
    assert_eq!(deltas[0].index, Some(0));
    assert_eq!(deltas[0].delta, "Hel");
    assert!(!deltas[0].final_chunk);
    assert_eq!(deltas[1].index, Some(1));
    assert_eq!(deltas[1].delta, "lo");
    assert!(deltas[1].final_chunk);
}

#[test]
fn message_display_parser_accepts_pretty_wrapped_object_stream() {
    let jsonl = r#"
{
  "event": "MessageDisplay",
  "payload": {
    "session_id": "claude-s",
    "hook_event_name": "MessageDisplay",
    "turn_id": "turn-1",
    "message_id": "msg-1",
    "index": 0,
    "final": false,
    "delta": "Hel"
  }
}
{
  "event": "MessageDisplay",
  "payload": {
    "session_id": "claude-s",
    "hook_event_name": "MessageDisplay",
    "turn_id": "turn-1",
    "message_id": "msg-1",
    "index": 1,
    "final": true,
    "delta": "lo"
  }
}
"#;

    let deltas = parse_message_display_jsonl(jsonl);

    assert_eq!(deltas.len(), 2);
    assert_eq!(deltas[0].delta, "Hel");
    assert_eq!(deltas[1].delta, "lo");
    assert!(deltas[1].final_chunk);
}

#[test]
fn message_display_parser_stops_before_partial_trailing_object() {
    let jsonl = r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"done"}}{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","delta":"partial"#;

    let deltas = parse_message_display_jsonl(jsonl);

    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].delta, "done");
}

#[test]
fn message_display_parser_skips_non_delta_records() {
    let jsonl = [
        r#"{"event":"Stop","payload":{"hook_event_name":"Stop","delta":"ignored"}}"#,
        "not-json",
        r#"{"event":"MessageDisplay","payload":{"hook_event_name":"MessageDisplay","index":0}}"#,
    ]
    .join("\n");

    let deltas = parse_message_display_jsonl(&jsonl);

    assert!(deltas.is_empty());
}

#[test]
fn transcript_parser_accepts_pretty_wrapped_hook_object_stream() {
    let jsonl = r#"
{
  "event": "Stop",
  "payload": {
    "session_id": "claude-s",
    "transcript_path": "/tmp/claude-s.jsonl",
    "hook_event_name": "Stop",
    "reason": "end_turn"
  }
}
"#;

    let records = parse_transcript_jsonl(jsonl);

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].session_id.as_deref(), Some("claude-s"));
    assert_eq!(
        records[0].transcript_path.as_deref(),
        Some("/tmp/claude-s.jsonl")
    );
    assert_eq!(
        records[0].boundary,
        Some(TurnBoundary::Stop {
            reason: Some("end_turn".into()),
            last_assistant_message: None,
        })
    );
}

#[test]
fn user_prompt_submit_records_recover_prompt_text() {
    let record = parse_transcript_line(
        r#"{"event":"UserPromptSubmit","payload":{"session_id":"claude-s","transcript_path":"/tmp/claude-s.jsonl","hook_event_name":"UserPromptSubmit","prompt_id":"p_123","prompt":"hello from web"}}"#,
    )
    .unwrap();

    assert_eq!(record.session_id.as_deref(), Some("claude-s"));
    assert_eq!(
        record.transcript_path.as_deref(),
        Some("/tmp/claude-s.jsonl")
    );
    let prompt = record.user_prompt.unwrap();
    assert_eq!(prompt.prompt_id.as_deref(), Some("p_123"));
    assert_eq!(prompt.text, "hello from web");
}

#[test]
fn transcript_records_recover_session_id_and_assistant_text() {
    let mode = r#"{"type":"mode","mode":"normal","sessionId":"claude-session"}"#;
    let assistant = r#"{"type":"assistant","sessionId":"claude-session","message":{"id":"msg-a","role":"assistant","content":[{"type":"thinking","thinking":"hidden"},{"type":"text","text":"first"},{"type":"text","text":" second"}],"stop_reason":"end_turn"}}"#;

    let records = parse_transcript_jsonl(&format!("{mode}\n{assistant}"));

    assert_eq!(records[0].session_id.as_deref(), Some("claude-session"));
    assert!(records[0].assistant_text.is_empty());
    assert_eq!(records[1].session_id.as_deref(), Some("claude-session"));
    assert_eq!(records[1].assistant_message_id.as_deref(), Some("msg-a"));
    assert_eq!(records[1].assistant_text.len(), 2);
    assert_eq!(
        records[1].assistant_text[0].message_id.as_deref(),
        Some("msg-a")
    );
    assert_eq!(records[1].assistant_text[0].text, "first");
    assert_eq!(records[1].assistant_text[1].text, " second");
    assert_eq!(
        records[1].boundary,
        Some(TurnBoundary::Stop {
            reason: Some("end_turn".into()),
            last_assistant_message: None,
        })
    );
}

#[test]
fn transcript_thinking_only_record_preserves_model_message_identity() {
    let record = parse_transcript_line(
        r#"{"type":"assistant","uuid":"row-thinking","message":{"id":"msg-model","role":"assistant","content":[{"type":"thinking","thinking":"hidden"}],"stop_reason":"end_turn"}}"#,
    )
    .unwrap();

    assert_eq!(record.assistant_message_id.as_deref(), Some("msg-model"));
    assert!(record.assistant_text.is_empty());
    assert!(record.boundary.is_some());
}

#[test]
fn transcript_records_recover_tool_use_and_result_updates() {
    let assistant = r#"{"type":"assistant","sessionId":"claude-session","message":{"id":"msg-a","role":"assistant","content":[{"type":"tool_use","id":"toolu_123","name":"Read","input":{"file_path":"README.md"}}]}}"#;
    let user = r#"{"type":"user","sessionId":"claude-session","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_123","content":"README contents"}]}}"#;

    let records = parse_transcript_jsonl(&format!("{assistant}\n{user}"));

    assert_eq!(
        records[0].tool_updates,
        vec![ClaudeToolUpdate::Start {
            id: "toolu_123".into(),
            name: "Read".into(),
            input: Some(json!({ "file_path": "README.md" })),
        }]
    );
    assert_eq!(
        records[1].tool_updates,
        vec![ClaudeToolUpdate::Result {
            id: "toolu_123".into(),
            content: Some("README contents".into()),
            is_error: None,
        }]
    );
}

#[test]
fn hook_records_recover_pre_and_post_tool_use_updates() {
    let pre = parse_transcript_line(
        r#"{"event":"PreToolUse","payload":{"hook_event_name":"PreToolUse","session_id":"claude-s","tool_use_id":"toolu_edit","tool_name":"Edit","tool_input":{"file_path":"README.md"}}}"#,
    )
    .unwrap();
    let post = parse_transcript_line(
        r#"{"hook_event_name":"PostToolUse","session_id":"claude-s","toolUseId":"toolu_edit","status":"failed","error":"permission denied"}"#,
    )
    .unwrap();

    assert_eq!(
        pre.tool_updates,
        vec![ClaudeToolUpdate::Start {
            id: "toolu_edit".into(),
            name: "Edit".into(),
            input: Some(json!({ "file_path": "README.md" })),
        }]
    );
    assert_eq!(
        post.tool_updates,
        vec![ClaudeToolUpdate::Result {
            id: "toolu_edit".into(),
            content: None,
            is_error: Some(true),
        }]
    );
}

#[test]
fn stop_hook_records_recover_turn_boundary() {
    let record = parse_transcript_line(
        r#"{"event":"Stop","payload":{"session_id":"claude-s","hook_event_name":"Stop","last_assistant_message":"done"}}"#,
    )
    .unwrap();

    assert_eq!(record.session_id.as_deref(), Some("claude-s"));
    assert_eq!(
        record.boundary,
        Some(TurnBoundary::Stop {
            reason: None,
            last_assistant_message: Some("done".into()),
        })
    );
}

#[test]
fn stop_failure_hook_records_recover_failed_boundary() {
    let record = parse_transcript_line(
        r#"{"hook_event_name":"StopFailure","session_id":"claude-s","error":"rate_limit","error_details":{"retry_after_ms":1000},"last_assistant_message":"partial"}"#,
    )
    .unwrap();

    assert_eq!(record.session_id.as_deref(), Some("claude-s"));
    assert_eq!(
        record.boundary,
        Some(TurnBoundary::StopFailure {
            error: Some("rate_limit".into()),
            error_details: Some(r#"{"retry_after_ms":1000}"#.into()),
            last_assistant_message: Some("partial".into()),
        })
    );
}

#[test]
fn compaction_hook_records_recover_markers() {
    let pre = parse_transcript_line(
        r#"{"event":"PreCompact","payload":{"session_id":"claude-s","hook_event_name":"PreCompact","trigger":"manual","custom_instructions":"keep nexus notes"}}"#,
    )
    .unwrap();
    let post = parse_transcript_line(
        r#"{"hook_event_name":"PostCompact","session_id":"claude-s","trigger":"auto","compact_summary":"summary text"}"#,
    )
    .unwrap();

    let pre_marker = pre.compaction.unwrap();
    assert_eq!(pre_marker.phase, CompactionPhase::Pre);
    assert_eq!(pre_marker.trigger.as_deref(), Some("manual"));
    assert_eq!(
        pre_marker.custom_instructions.as_deref(),
        Some("keep nexus notes")
    );
    assert_eq!(pre_marker.summary, None);

    let post_marker = post.compaction.unwrap();
    assert_eq!(post_marker.phase, CompactionPhase::Post);
    assert_eq!(post_marker.trigger.as_deref(), Some("auto"));
    assert_eq!(post_marker.summary.as_deref(), Some("summary text"));
}

#[test]
fn transcript_summary_record_recovers_compaction_marker() {
    let record = parse_transcript_line(
        r#"{"type":"summary","sessionId":"claude-s","summary":"compacted conversation"}"#,
    )
    .unwrap();

    assert_eq!(record.session_id.as_deref(), Some("claude-s"));
    let marker = record.compaction.unwrap();
    assert_eq!(marker.phase, CompactionPhase::Transcript);
    assert_eq!(marker.summary.as_deref(), Some("compacted conversation"));
}
#[test]
fn response_model_retains_native_identity_time_and_thinking_only_evidence() {
    use nexus_contracts::model_report::{ModelEvidenceField, ModelEvidenceValue};
    use nexus_harness_claude::native::model_reporting::{parse_response_model, profile};
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../nexus/tests/fixtures/model_reporting/native.json"
    ))
    .unwrap();
    let mut raw = fixture["rows"]["claude.headed"]["events"][0]["payload"].clone();
    for content in [
        serde_json::json!([]),
        serde_json::json!([{"type":"thinking","thinking":"private"}]),
    ] {
        raw["message"]["content"] = content;
        let record = parse_response_model(&raw).expect("native root assistant model");
        let update = record.update(1);
        assert_eq!(update.field, ModelEvidenceField::ResponseReported);
        assert_eq!(update.native_session_id, "native-root");
        let ModelEvidenceValue::Observed(observation) = update.value else {
            panic!("observed native response")
        };
        assert_eq!(observation.model_id, "fixture-response-model");
        assert_eq!(
            observation.native_message_id.as_deref(),
            Some("fixture-response")
        );
        assert_eq!(observation.native_reported_at, Some(1788912000000));
        assert_eq!(observation.observed_at, 1);
        assert!(observation.provider_id.is_none());
        assert!(observation.native_turn_id.is_none());
    }
    assert_eq!(
        profile().response_reported(),
        nexus_contracts::ModelEvidenceCapability::Supported
    );
    assert_eq!(
        profile().configured(),
        nexus_contracts::ModelEvidenceCapability::Unverified
    );
}

#[test]
fn response_model_excludes_child_synthetic_and_conflicting_native_identity() {
    use nexus_harness_claude::native::model_reporting::parse_response_model;
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../nexus/tests/fixtures/model_reporting/native.json"
    ))
    .unwrap();
    let root = fixture["rows"]["claude.headed"]["events"][0]["payload"].clone();
    assert!(parse_response_model(&root).is_some());
    assert!(
        parse_response_model(&fixture["rows"]["claude.headed"]["events"][1]["payload"]).is_none()
    );
    for (path, value) in [
        ("/isSidechain", serde_json::json!(true)),
        ("/isSidechain", serde_json::Value::Null),
        ("/message/model", serde_json::json!("<synthetic>")),
        ("/message/id", serde_json::json!(" ")),
        ("/message/role", serde_json::json!("user")),
    ] {
        let mut raw = root.clone();
        *raw.pointer_mut(path).unwrap() = value;
        assert!(parse_response_model(&raw).is_none(), "reject {path}");
    }
    let mut raw = root;
    raw["session_id"] = serde_json::json!("foreign");
    assert!(parse_response_model(&raw).is_none());
}

#[test]
fn response_model_distinguishes_missing_malformed_and_opaque_model() {
    use nexus_contracts::model_report::ModelEvidenceValue;
    use nexus_harness_claude::native::model_reporting::parse_response_model;
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../nexus/tests/fixtures/model_reporting/native.json"
    ))
    .unwrap();
    let mut raw = fixture["rows"]["claude.headed"]["events"][0]["payload"].clone();
    raw["message"].as_object_mut().unwrap().remove("model");
    assert!(matches!(
        parse_response_model(&raw).unwrap().update(1).value,
        ModelEvidenceValue::Unknown(_)
    ));
    for invalid in [
        serde_json::Value::Null,
        serde_json::json!(" "),
        serde_json::json!(3),
    ] {
        raw["message"]["model"] = invalid;
        assert!(matches!(
            parse_response_model(&raw).unwrap().update(1).value,
            ModelEvidenceValue::Invalid(_)
        ));
    }
    raw["message"]["model"] = serde_json::json!("opaque/unlisted model");
    raw.as_object_mut().unwrap().remove("timestamp");
    let ModelEvidenceValue::Observed(observation) =
        parse_response_model(&raw).unwrap().update(1).value
    else {
        panic!("opaque")
    };
    assert_eq!(observation.model_id, "opaque/unlisted model");
    assert!(observation.native_reported_at.is_none());
    raw["timestamp"] = serde_json::json!("not a native timestamp");
    assert!(matches!(
        parse_response_model(&raw).unwrap().update(1).value,
        ModelEvidenceValue::Invalid(_)
    ));
}
