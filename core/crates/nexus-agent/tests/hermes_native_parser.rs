use nexus_agent::adapter::hermes::native::{
    translate_message_row, HermesForwardState, HermesMessageRow, HermesNativeAdapter,
};
use nexus_contracts::AgentUpdateKind;
use nexus_transcript::ToolCallPhase;
use serde_json::json;

#[test]
fn native_session_model_is_configured_only_and_preserves_exact_native_identity() {
    use nexus_agent::adapter::hermes::native::{configured_model, model_profile};
    use nexus_contracts::model_report::{ModelEvidenceField, ModelEvidenceValue};
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../nexus/tests/fixtures/model_reporting/native.json"
    ))
    .unwrap();
    let row = &native["rows"]["hermes.headed"]["events"][0]["payload"];
    let update = configured_model(row, "fixture-root", 17).expect("captured native row");
    assert_eq!(update.native_session_id, "fixture-root");
    assert_eq!(update.field, ModelEvidenceField::Configured);
    let ModelEvidenceValue::Observed(observation) = update.value else {
        panic!("native row supplies configured evidence");
    };
    assert_eq!(observation.model_id, row["model"].as_str().unwrap());
    assert_eq!(
        observation.native_session_id.as_deref(),
        Some("fixture-root")
    );
    assert_eq!(observation.source.as_str(), "hermes.gateway.session");
    assert_eq!(observation.observed_at, 17);
    assert!(observation.provider_id.is_none());
    assert!(observation.native_turn_id.is_none());
    assert!(observation.native_message_id.is_none());
    assert!(observation.native_reported_at.is_none());
    let profile = model_profile();
    let telemetry = profile.telemetry().unwrap();
    assert_eq!(
        telemetry.usage().capability(),
        nexus_contracts::ModelEvidenceCapability::Supported
    );
    assert_eq!(
        telemetry.context().capability(),
        nexus_contracts::ModelEvidenceCapability::Unsupported
    );
    assert_eq!(
        telemetry.quota().capability(),
        nexus_contracts::ModelEvidenceCapability::Unsupported
    );
}

#[test]
fn native_session_model_rejects_foreign_child_and_ambiguous_lineage() {
    use nexus_agent::adapter::hermes::native::configured_model;
    let base = json!({"id":"root", "model":"opaque/model", "parent_session_id":null,
        "model_config":"{}"});
    for changed in [
        json!({"id":"foreign"}),
        json!({"parent_session_id":"root"}),
        json!({"parent_session_id":""}),
        json!({"model_config":"{\"_delegate_from\":\"root\"}"}),
        json!({"model_config":"{\"_branched_from\":\"root\"}"}),
        json!({"model_config":"not-json"}),
        json!({"model_config":"[]"}),
    ] {
        let mut row = base.clone();
        row.as_object_mut()
            .unwrap()
            .extend(changed.as_object().unwrap().clone());
        assert!(configured_model(&row, "root", 1).is_none(), "{row}");
    }
    assert!(configured_model(&base, "", 1).is_none());
}

#[test]
fn native_session_usage_validates_every_counter_and_preserves_unknown_without_identity_guessing() {
    use nexus_agent::adapter::hermes::native::session_usage;
    use nexus_contracts::telemetry::{NativeTelemetryUpdate, NativeTelemetryValue};
    let base = json!({"id":"root","parent_session_id":null,"model_config":"{}",
        "usage":{"api_call_count":1,"input_tokens":120,"output_tokens":30,
            "cache_read_tokens":80,"cache_write_tokens":20,"reasoning_tokens":12}});
    for key in [
        "api_call_count",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "reasoning_tokens",
    ] {
        for malformed in [
            json!(-1),
            json!(1.5),
            json!("1"),
            json!(true),
            json!(null),
            json!(9_007_199_254_740_992_u64),
        ] {
            let mut row = base.clone();
            row["usage"][key] = malformed;
            assert!(
                matches!(
                    session_usage(&row, "root", 17).unwrap(),
                    NativeTelemetryUpdate::Usage {
                        value: NativeTelemetryValue::Invalid,
                        ..
                    }
                ),
                "malformed {key} cannot partially publish counters"
            );
        }
    }
    for changes in [
        json!({"id":"foreign"}),
        json!({"ended_at":17}),
        json!({"parent_session_id":"parent"}),
        json!({"model_config":"{\"_delegate_from\":\"parent\"}"}),
    ] {
        let mut row = base.clone();
        row.as_object_mut()
            .unwrap()
            .extend(changes.as_object().unwrap().clone());
        assert!(session_usage(&row, "root", 17).is_none());
    }
    let NativeTelemetryUpdate::Usage {
        value: NativeTelemetryValue::Observed(value),
        ..
    } = session_usage(&base, "root", 17).unwrap()
    else {
        panic!("usage is independent of missing configured model")
    };
    assert_eq!(value.metadata.observed_at.get(), 17);
    assert!(value.metadata.native_reported_at.is_none());
    assert!(value.model.is_none());
}

#[test]
fn native_session_model_does_not_guess_from_config_and_keeps_invalid_distinct() {
    use nexus_agent::adapter::hermes::native::configured_model;
    use nexus_contracts::model_report::ModelEvidenceValue;
    let base = json!({"id":"root", "parent_session_id":null,
        "model_config":"{\"model\":\"desired-only\",\"provider\":\"not-authority\"}"});
    assert!(matches!(
        configured_model(&base, "root", 1).unwrap().value,
        ModelEvidenceValue::Unknown(_)
    ));
    for model in [json!(null), json!(false), json!(""), json!("  ")] {
        let mut row = base.clone();
        row["model"] = model;
        assert!(matches!(
            configured_model(&row, "root", 1).unwrap().value,
            ModelEvidenceValue::Invalid(_)
        ));
    }
    let mut row = base;
    row["model"] = json!("provider/native/opaque");
    assert!(matches!(
        configured_model(&row, "root", 9_007_199_254_740_992)
            .unwrap()
            .value,
        ModelEvidenceValue::Invalid(_)
    ));
}

fn row(
    id: i64,
    role: &str,
    content: Option<&str>,
    tool_calls: Option<serde_json::Value>,
    tool_call_id: Option<&str>,
    tool_name: Option<&str>,
    finish_reason: Option<&str>,
) -> HermesMessageRow {
    HermesMessageRow {
        id,
        session_id: "ses_native".to_string(),
        role: role.to_string(),
        content: content.map(str::to_string),
        tool_call_id: tool_call_id.map(str::to_string),
        tool_calls,
        tool_name: tool_name.map(str::to_string),
        timestamp: id as f64,
        finish_reason: finish_reason.map(str::to_string),
        active: true,
    }
}

#[test]
fn hermes_message_rows_emit_session_updates_without_terminal_chrome() {
    let mut state = HermesForwardState::default();
    let rows = vec![
        row(1, "user", Some("hello hermes"), None, None, None, None),
        row(
            2,
            "assistant",
            Some(""),
            Some(json!([{
                "id": "call_1",
                "type": "function",
                "function": {
                    "name": "terminal",
                    "arguments": "{\"command\":\"pwd\"}"
                }
            }])),
            None,
            None,
            Some("tool_calls"),
        ),
        row(
            3,
            "tool",
            Some("{\"output\":\"/tmp\\n\",\"exit_code\":0}"),
            None,
            Some("call_1"),
            Some("terminal"),
            None,
        ),
        row(4, "assistant", Some("done"), None, None, None, Some("stop")),
    ];

    let updates: Vec<_> = rows
        .iter()
        .flat_map(|row| translate_message_row(row, &mut state))
        .collect();

    assert_eq!(updates.len(), 5);
    assert_eq!(updates[0].kind, AgentUpdateKind::UserInput);
    assert_eq!(updates[0].data["text"], "hello hermes");
    assert_eq!(updates[1].kind, AgentUpdateKind::ToolCall);
    assert_eq!(updates[1].data["id"], "call_1");
    assert_eq!(updates[1].data["title"], "terminal");
    assert_eq!(
        updates[1].data["tool"], "terminal",
        "C-TOOL: the OpenAI-style function name IS the machine name"
    );
    assert_eq!(updates[1].data["status"], "in_progress");
    assert_eq!(updates[1].data["input"]["command"], "pwd");
    assert_eq!(updates[2].kind, AgentUpdateKind::ToolCall);
    assert_eq!(updates[2].data["id"], "call_1");
    assert_eq!(updates[2].data["status"], "completed");
    assert_eq!(updates[2].data["output"]["output"], "/tmp\n");
    assert!(updates[2].data.get("rawOutput").is_none());
    assert_eq!(updates[3].kind, AgentUpdateKind::Text);
    assert_eq!(updates[3].data["text"], "done");
    assert_eq!(updates[4].kind, AgentUpdateKind::TurnEnd);
    assert_eq!(updates[4].data["reason"], "stop");

    assert_eq!(state.hermes_session_id.as_deref(), Some("ses_native"));
    assert_eq!(state.last_message_id, 4);
}

#[test]
fn hermes_tool_rows_emit_tool_call_observations() {
    let adapter = HermesNativeAdapter;
    let assistant = row(
        1,
        "assistant",
        Some(""),
        Some(json!([{
            "id": "call_1",
            "type": "function",
            "function": {
                "name": "terminal",
                "arguments": "{\"command\":\"pwd\"}"
            }
        }])),
        None,
        None,
        Some("tool_calls"),
    );
    let ok_result = row(
        2,
        "tool",
        Some("{\"output\":\"/tmp\\n\",\"exit_code\":0}"),
        None,
        Some("call_1"),
        Some("terminal"),
        None,
    );
    let failed_result = row(
        3,
        "tool",
        Some("{\"error\":\"boom\",\"exit_code\":1}"),
        None,
        Some("call_2"),
        Some("terminal"),
        None,
    );
    let inactive = HermesMessageRow {
        active: false,
        ..failed_result.clone()
    };

    let assistant = adapter.tool_call_observations(&assistant);
    assert_eq!(assistant.len(), 1);
    assert_eq!(assistant[0].tool_call_id.as_deref(), Some("call_1"));
    assert_eq!(assistant[0].tool, "terminal");
    assert_eq!(assistant[0].phase, ToolCallPhase::Pre);
    assert!(assistant[0].ok);

    let ok_result = adapter.tool_call_observations(&ok_result);
    assert_eq!(ok_result.len(), 1);
    assert_eq!(ok_result[0].tool_call_id.as_deref(), Some("call_1"));
    assert_eq!(ok_result[0].tool, "terminal");
    assert_eq!(ok_result[0].phase, ToolCallPhase::Post);
    assert!(ok_result[0].ok);

    let failed_result = adapter.tool_call_observations(&failed_result);
    assert_eq!(failed_result.len(), 1);
    assert_eq!(failed_result[0].tool_call_id.as_deref(), Some("call_2"));
    assert_eq!(failed_result[0].phase, ToolCallPhase::Post);
    assert!(!failed_result[0].ok);

    assert!(adapter.tool_call_observations(&inactive).is_empty());
}

#[test]
fn hermes_native_visible_rate_limit_text_remains_plain_text() {
    let mut state = HermesForwardState::default();
    let updates = translate_message_row(
        &row(
            1,
            "assistant",
            Some("provider says rate_limit in visible text"),
            None,
            None,
            None,
            Some("stop"),
        ),
        &mut state,
    );

    assert_eq!(
        updates.iter().map(|event| event.kind).collect::<Vec<_>>(),
        vec![AgentUpdateKind::Text, AgentUpdateKind::TurnEnd],
        "Hermes native text has no provider-error field and must not become breaker telemetry"
    );
    assert_eq!(
        updates[0].data["text"],
        "provider says rate_limit in visible text"
    );
}

mod c_tool_sanitization {
    use super::*;
    use serde_json::Value;

    const MAX_FIELD_BYTES: usize = 8 * 1024;

    fn nested_payload(text: &str) -> Value {
        json!({
            "command": "fixture",
            "nested": [{ "payload": text, "small": "keep", "number": 7,
                "flag": true, "none": null }],
            "array": [1, false, null, "keep"]
        })
    }

    fn assert_structure_and_small_values(value: &Value) {
        assert!(value.is_object(), "tool values must stay structured");
        assert_eq!(value["command"], "fixture");
        assert!(value["nested"].is_array());
        assert!(value["nested"][0].is_object());
        assert_eq!(value["nested"][0]["small"], "keep");
        assert_eq!(value["nested"][0]["number"], 7);
        assert_eq!(value["nested"][0]["flag"], true);
        assert_eq!(value["nested"][0].get("none"), Some(&Value::Null));
        assert_eq!(value["array"], json!([1, false, null, "keep"]));
    }

    fn emitted_input(arguments: Value) -> Value {
        let updates = translate_message_row(
            &row(
                1,
                "assistant",
                None,
                Some(json!([{
                    "id": "call_sanitize",
                    "type": "function",
                    "function": { "name": "terminal", "arguments": arguments }
                }])),
                None,
                None,
                Some("tool_calls"),
            ),
            &mut HermesForwardState::default(),
        );
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].kind, AgentUpdateKind::ToolCall);
        assert_eq!(updates[0].data["id"], "call_sanitize");
        assert_eq!(updates[0].data["tool"], "terminal");
        assert_eq!(updates[0].data["status"], "in_progress");
        updates[0].data.get("input").expect("emitted input").clone()
    }

    fn emitted_result(content: &str) -> Value {
        let updates = translate_message_row(
            &row(
                2,
                "tool",
                Some(content),
                None,
                Some("call_sanitize"),
                Some("terminal"),
                None,
            ),
            &mut HermesForwardState::default(),
        );
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].kind, AgentUpdateKind::ToolCall);
        assert_eq!(updates[0].data["id"], "call_sanitize");
        assert_eq!(updates[0].data["status"], "completed");
        // This tests emitted bytes, independently of the separate rawOutput key defect.
        updates[0]
            .data
            .get("output")
            .or_else(|| updates[0].data.get("rawOutput"))
            .expect("emitted result under its actual key")
            .clone()
    }

    fn assert_input_sanitized(text: &str) {
        let payload = nested_payload(text);
        let emitted = [
            emitted_input(payload.clone()),
            emitted_input(json!(payload.to_string())),
        ];
        let sanitized = emitted.map(|value| {
            assert_structure_and_small_values(&value);
            value["nested"][0]["payload"].as_str() == Some("[omitted]")
        });
        assert_eq!(
            sanitized,
            [true; 2],
            "structured and JSON-string arguments must sanitize before emission ({} input bytes)",
            text.len()
        );
    }

    fn assert_result_sanitized(text: &str) {
        let nested = emitted_result(&nested_payload(text).to_string());
        let plain = emitted_result(text);
        assert_structure_and_small_values(&nested);
        assert_eq!(
            [
                nested["nested"][0]["payload"].as_str() == Some("[omitted]"),
                plain.as_str() == Some("[omitted]")
            ],
            [true; 2],
            "parsed JSON and raw-string results must sanitize before emission ({} input bytes)",
            text.len()
        );
    }

    #[test]
    fn hermes_input_omits_oversized_ascii_before_emission() {
        assert_input_sanitized(&"a".repeat(MAX_FIELD_BYTES + 1));
    }

    #[test]
    fn hermes_input_uses_utf8_bytes_not_character_count() {
        let text = format!("{}a", "é".repeat(MAX_FIELD_BYTES / 2));
        assert_eq!(text.len(), MAX_FIELD_BYTES + 1);
        assert!(text.chars().count() < MAX_FIELD_BYTES);
        assert_input_sanitized(&text);
    }

    #[test]
    fn hermes_input_omits_small_inline_base64_before_emission() {
        assert_input_sanitized("data:image/png;base64,AAAA");
    }

    #[test]
    fn hermes_result_omits_oversized_ascii_before_emission() {
        assert_result_sanitized(&"a".repeat(MAX_FIELD_BYTES + 1));
    }

    #[test]
    fn hermes_result_uses_utf8_bytes_not_character_count() {
        let text = format!("{}a", "é".repeat(MAX_FIELD_BYTES / 2));
        assert_eq!(text.len(), MAX_FIELD_BYTES + 1);
        assert!(text.chars().count() < MAX_FIELD_BYTES);
        assert_result_sanitized(&text);
    }

    #[test]
    fn hermes_result_omits_small_inline_base64_before_emission() {
        assert_result_sanitized("data:image/png;base64,AAAA");
    }

    #[test]
    fn hermes_preserves_exact_byte_boundary_and_small_structured_controls() {
        for text in [
            "small text".to_string(),
            "a".repeat(MAX_FIELD_BYTES),
            "é".repeat(MAX_FIELD_BYTES / 2),
            "aGVsbG8=".to_string(),
            "data:text/plain,hello".to_string(),
            "literal data:image/png;base64,AAAA".to_string(),
        ] {
            let expected = nested_payload(&text);
            for emitted in [
                emitted_input(expected.clone()),
                emitted_input(json!(expected.to_string())),
                emitted_result(&expected.to_string()),
            ] {
                assert_structure_and_small_values(&emitted);
                assert!(
                    emitted == expected,
                    "safe structured fixture must survive unchanged ({} text bytes)",
                    text.len()
                );
            }
            assert!(
                emitted_result(&text).as_str() == Some(text.as_str()),
                "safe raw result must survive unchanged ({} text bytes)",
                text.len()
            );
        }
    }
}
