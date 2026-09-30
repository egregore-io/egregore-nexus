use nexus_agent::adapter::opencode::native::{
    translate_event_row, OpenCodeEventRow, OpenCodeForwardState, OpenCodeNativeAdapter,
};
use nexus_contracts::AgentUpdateKind;
use nexus_transcript::ToolCallPhase;
use serde_json::json;

#[test]
fn native_telemetry_uses_response_replacements_and_the_live_tui_context_formula() {
    use nexus_agent::adapter::opencode::telemetry::OpenCodeTelemetry;
    use nexus_contracts::telemetry::{
        NativeTelemetryUpdate, NativeTelemetryValue, TokenUsageScope,
    };
    let mut collector = OpenCodeTelemetry::default();
    let base = json!({"id":"msg_b","role":"assistant","sessionID":"root","modelID":"native-model","providerID":"native-provider",
        "finish":"tool-calls","tokens":{"input":120,"output":30,"reasoning":12,"cache":{"read":80,"write":20}}});
    for input in [120, 120, 12, 0] {
        let mut info = base.clone();
        info["tokens"]["input"] = json!(input);
        let updates = collector.observe(&info, &json!(1000), "root", 10);
        assert_eq!(updates.len(), 2);
        let NativeTelemetryUpdate::Usage {
            value: NativeTelemetryValue::Observed(usage),
            ..
        } = &updates[0]
        else {
            panic!("usage")
        };
        assert_eq!(usage.scope, TokenUsageScope::LastResponse);
        assert_eq!(usage.input_tokens.unwrap().get(), input);
        assert_eq!(usage.cache_write_tokens.unwrap().get(), 20);
        assert!(
            usage.total_tokens.is_none()
                && usage.native_turn_id.is_none()
                && usage.reset_id.is_none()
        );
        let NativeTelemetryUpdate::Context {
            value: NativeTelemetryValue::Observed(context),
            ..
        } = &updates[1]
        else {
            panic!("context")
        };
        assert_eq!(
            context.used_tokens.as_ref().unwrap().value.get(),
            input + 142
        );
        assert_eq!(
            context.used_percent.as_ref().unwrap().value.get(),
            (((input + 142) as f64 / 1000.0) * 100.0).round()
        );
        assert_eq!(
            context.remaining_tokens.as_ref().unwrap().value.get(),
            1000 - input - 142
        );
        assert_eq!(
            context
                .effective_capacity_tokens
                .as_ref()
                .unwrap()
                .provenance,
            nexus_contracts::telemetry::TelemetryProvenance::Native
        );
    }
    let mut older = base.clone();
    older["id"] = json!("msg_a");
    assert!(
        collector
            .observe(&older, &json!(1000), "root", 20)
            .is_empty(),
        "late older native live-sort ID cannot regress either category"
    );
    let mut child = base.clone();
    child["sessionID"] = json!("child");
    assert!(collector
        .observe(&child, &json!(1000), "root", 20)
        .is_empty());
}

#[test]
fn native_telemetry_does_not_measure_constructor_zeros_or_infer_capacity() {
    use nexus_agent::adapter::opencode::telemetry::OpenCodeTelemetry;
    use nexus_contracts::telemetry::{NativeTelemetryUpdate, NativeTelemetryValue};
    let mut collector = OpenCodeTelemetry::default();
    let mut info = json!({"id":"msg_a","role":"assistant","sessionID":"root",
        "tokens":{"input":0,"output":0,"reasoning":0,"cache":{"read":0,"write":0}}});
    assert!(collector.observe(&info, &json!(1000), "root", 1).is_empty());
    info["finish"] = json!("stop");
    assert_eq!(
        collector.observe(&info, &json!(1000), "root", 2).len(),
        1,
        "native completed zero usage, but TUI selects output-positive context only"
    );
    info["tokens"]["output"] = json!(1200);
    let values = collector.observe(&info, &json!(1000), "root", 3);
    let NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(value),
        ..
    } = &values[1]
    else {
        panic!("context")
    };
    assert_eq!(value.used_percent.as_ref().unwrap().value.get(), 120.0);
    assert!(value.remaining_percent.is_none() && value.remaining_tokens.is_none());
    let values = collector.observe(&info, &serde_json::Value::Null, "root", 4);
    let NativeTelemetryUpdate::Context {
        value: NativeTelemetryValue::Observed(value),
        ..
    } = &values[1]
    else {
        panic!("context")
    };
    assert!(value.effective_capacity_tokens.is_none() && value.used_percent.is_none());
}

#[test]
fn native_telemetry_removal_reselects_retained_messages_without_relabeling_root() {
    use nexus_agent::adapter::opencode::telemetry::OpenCodeTelemetry;
    use nexus_contracts::telemetry::{NativeTelemetryUpdate, NativeTelemetryValue};
    let mut collector = OpenCodeTelemetry::default();
    let mut info = json!({"id":"msg_a","role":"assistant","sessionID":"root","finish":"stop",
        "tokens":{"input":100,"output":20,"reasoning":0,"cache":{"read":0,"write":0}}});
    collector.observe(&info, &json!(1000), "root", 10);
    info["id"] = json!("msg_b");
    info["tokens"]["input"] = json!(200);
    collector.observe(&info, &json!(1000), "root", 20);
    assert!(
        collector.remove("msg_b", "foreign").is_empty(),
        "foreign removal cannot relabel or erase retained samples"
    );
    let values = collector.remove("msg_b", "root");
    let NativeTelemetryUpdate::Usage {
        value: NativeTelemetryValue::Observed(value),
        ..
    } = &values[0]
    else {
        panic!("fallback retained native row")
    };
    assert_eq!(value.input_tokens.unwrap().get(), 100);
    assert_eq!(value.metadata.observed_at.get(), 10);
    let values = collector.remove("msg_a", "root");
    assert!(matches!(
        &values[0],
        NativeTelemetryUpdate::Usage {
            value: NativeTelemetryValue::Unknown,
            ..
        }
    ));
    assert!(matches!(
        &values[1],
        NativeTelemetryUpdate::Context {
            value: NativeTelemetryValue::Unknown,
            ..
        }
    ));
}

fn row(seq: i64, event_type: &str, data: serde_json::Value) -> OpenCodeEventRow {
    OpenCodeEventRow {
        seq,
        event_type: event_type.to_string(),
        data,
    }
}

#[test]
fn native_selected_model_preserves_exact_assistant_metadata_without_response_promotion() {
    use nexus_agent::adapter::opencode::native::{model_profile, selected_model};
    use nexus_contracts::model_report::{ModelEvidenceField, ModelEvidenceValue};
    use nexus_contracts::ModelEvidenceCapability;
    let profile = model_profile();
    assert_eq!(profile.turn_selected(), ModelEvidenceCapability::Supported);
    assert_eq!(profile.configured(), ModelEvidenceCapability::Unverified);
    assert_eq!(
        profile.response_reported(),
        ModelEvidenceCapability::Unverified
    );
    let telemetry = profile.telemetry().unwrap();
    assert_eq!(
        telemetry.usage().capability(),
        ModelEvidenceCapability::Supported
    );
    assert_eq!(
        telemetry.context().capability(),
        ModelEvidenceCapability::Supported
    );
    assert_eq!(
        telemetry.quota().capability(),
        ModelEvidenceCapability::Unsupported
    );
    let update = selected_model(
        &json!({
            "sessionID": "ses_root", "id": "msg_native", "role": "assistant",
            "modelID": " opaque/not-in-catalog ", "providerID": "native-provider",
            "time": {"created": 1730000000123i64}, "parentID": "msg_user"
        }),
        "ses_root",
        1730000000456,
    )
    .expect("selected native metadata");
    assert_eq!(update.field, ModelEvidenceField::TurnSelected);
    assert_eq!(update.native_session_id, "ses_root");
    let ModelEvidenceValue::Observed(model) = update.value else {
        panic!("observed")
    };
    assert_eq!(model.model_id, " opaque/not-in-catalog ");
    assert_eq!(model.provider_id.as_deref(), Some("native-provider"));
    assert_eq!(model.native_session_id.as_deref(), Some("ses_root"));
    assert_eq!(model.native_message_id.as_deref(), Some("msg_native"));
    assert_eq!(
        model.native_turn_id, None,
        "parent message is not a turn ID"
    );
    assert_eq!(model.native_reported_at, Some(1730000000123));
    assert_eq!(model.observed_at, 1730000000456);
    assert_eq!(model.source.as_str(), "opencode.plugin.assistant");
}

#[test]
fn native_selected_model_does_not_adopt_foreign_or_incomplete_identity() {
    use nexus_agent::adapter::opencode::native::selected_model;
    for value in [
        json!({"sessionID":"ses_child","id":"m","role":"assistant","modelID":"child"}),
        json!({"sessionID":"ses_root","id":"m","role":"user","model":{"modelID":"request"}}),
        json!({"id":"m","role":"assistant","modelID":"missing-root"}),
        json!({"sessionID":"ses_root","role":"assistant","modelID":"missing-message"}),
        json!({"sessionID":"ses_root","id":" ","role":"assistant","modelID":"blank-message"}),
    ] {
        assert!(selected_model(&value, "ses_root", 1).is_none(), "{value}");
    }
}

#[test]
fn native_selected_model_keeps_missing_invalid_and_observed_separate() {
    use nexus_agent::adapter::opencode::native::selected_model;
    use nexus_contracts::model_report::ModelEvidenceValue;
    let base = json!({"sessionID":"ses_root","id":"m","role":"assistant"});
    assert!(matches!(
        selected_model(&base, "ses_root", 1).unwrap().value,
        ModelEvidenceValue::Unknown(_)
    ));
    for invalid in [json!(null), json!(" "), json!(7), json!({})] {
        let mut value = base.clone();
        value["modelID"] = invalid;
        assert!(matches!(
            selected_model(&value, "ses_root", 1).unwrap().value,
            ModelEvidenceValue::Invalid(_)
        ));
    }
    let mut valid = base.clone();
    valid["modelID"] = json!("native");
    assert!(matches!(
        selected_model(&valid, "ses_root", 1).unwrap().value,
        ModelEvidenceValue::Observed(_)
    ));
    for extra in [
        json!({"providerID":false}),
        json!({"providerID":" "}),
        json!({"time":{"created":9007199254740992i64}}),
        json!({"time":{"created":1.5}}),
        json!({"time":null}),
    ] {
        let mut value = valid.clone();
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(
            matches!(
                selected_model(&value, "ses_root", 1).unwrap().value,
                ModelEvidenceValue::Invalid(_)
            ),
            "{value}"
        );
    }
}

#[test]
fn opencode_event_rows_emit_session_updates_without_terminal_chrome() {
    let mut state = OpenCodeForwardState::default();
    let rows = vec![
        row(
            1,
            "message.updated.1",
            json!({
                "sessionID": "ses_native",
                "info": { "id": "msg_user", "sessionID": "ses_native", "role": "user" }
            }),
        ),
        row(
            2,
            "message.part.updated.1",
            json!({
                "sessionID": "ses_native",
                "part": {
                    "id": "prt_user_text",
                    "sessionID": "ses_native",
                    "messageID": "msg_user",
                    "type": "text",
                    "text": "hello opencode"
                }
            }),
        ),
        row(
            3,
            "message.updated.1",
            json!({
                "sessionID": "ses_native",
                "info": { "id": "msg_assistant", "sessionID": "ses_native", "role": "assistant" }
            }),
        ),
        row(
            4,
            "message.part.updated.1",
            json!({
                "sessionID": "ses_native",
                "part": {
                    "id": "prt_assistant_text",
                    "sessionID": "ses_native",
                    "messageID": "msg_assistant",
                    "type": "text",
                    "text": "hel"
                }
            }),
        ),
        row(
            5,
            "message.part.updated.1",
            json!({
                "sessionID": "ses_native",
                "part": {
                    "id": "prt_assistant_text",
                    "sessionID": "ses_native",
                    "messageID": "msg_assistant",
                    "type": "text",
                    "text": "hello"
                }
            }),
        ),
        row(
            6,
            "message.part.updated.1",
            json!({
                "sessionID": "ses_native",
                "part": {
                    "id": "prt_tool",
                    "sessionID": "ses_native",
                    "messageID": "msg_assistant",
                    "type": "tool",
                    "callID": "call_1",
                    "tool": "bash",
                    "state": {
                        "status": "running",
                        "input": { "command": "pwd" },
                        "metadata": { "output": "" }
                    }
                }
            }),
        ),
        row(
            7,
            "message.part.updated.1",
            json!({
                "sessionID": "ses_native",
                "part": {
                    "id": "prt_tool",
                    "sessionID": "ses_native",
                    "messageID": "msg_assistant",
                    "type": "tool",
                    "callID": "call_1",
                    "tool": "bash",
                    "state": {
                        "status": "completed",
                        "input": { "command": "pwd" },
                        "output": "/tmp\n"
                    }
                }
            }),
        ),
        row(
            8,
            "message.part.updated.1",
            json!({
                "sessionID": "ses_native",
                "part": {
                    "id": "prt_finish",
                    "sessionID": "ses_native",
                    "messageID": "msg_assistant",
                    "type": "step-finish",
                    "reason": "stop"
                }
            }),
        ),
    ];

    let updates: Vec<_> = rows
        .iter()
        .flat_map(|row| translate_event_row(row, &mut state))
        .collect();

    assert_eq!(updates.len(), 6);
    assert_eq!(updates[0].kind, AgentUpdateKind::UserInput);
    assert_eq!(updates[0].data["text"], "hello opencode");
    assert_eq!(updates[1].kind, AgentUpdateKind::Text);
    assert_eq!(updates[1].data["text"], "hel");
    assert_eq!(updates[2].kind, AgentUpdateKind::Text);
    assert_eq!(updates[2].data["text"], "lo");
    assert_eq!(updates[3].kind, AgentUpdateKind::ToolCall);
    assert_eq!(updates[3].data["id"], "call_1");
    assert_eq!(updates[3].data["title"], "bash");
    assert_eq!(updates[3].data["status"], "in_progress");
    assert_eq!(updates[3].data["input"]["command"], "pwd");
    assert_eq!(
        updates[3].data["tool"], "bash",
        "C-TOOL: registered name is the machine name"
    );
    assert_eq!(updates[4].kind, AgentUpdateKind::ToolCall);
    assert_eq!(updates[4].data["status"], "completed");
    assert_eq!(updates[4].data["output"], "/tmp\n");
    assert_eq!(updates[5].kind, AgentUpdateKind::TurnEnd);
    assert_eq!(updates[5].data["reason"], "stop");

    assert_eq!(state.opencode_session_id.as_deref(), Some("ses_native"));
    assert_eq!(state.last_seq, 8);
}

#[test]
fn opencode_tool_rows_emit_tool_call_observations() {
    let adapter = OpenCodeNativeAdapter;
    let running = row(
        1,
        "message.part.updated.1",
        json!({
            "sessionID": "ses_native",
            "part": {
                "id": "prt_tool",
                "sessionID": "ses_native",
                "messageID": "msg_assistant",
                "type": "tool",
                "callID": "call_1",
                "tool": "bash",
                "state": {
                    "status": "running",
                    "input": { "command": "pwd" }
                }
            }
        }),
    );
    let completed = row(
        2,
        "message.part.updated.1",
        json!({
            "sessionID": "ses_native",
            "part": {
                "id": "prt_tool",
                "sessionID": "ses_native",
                "messageID": "msg_assistant",
                "type": "tool",
                "callID": "call_1",
                "tool": "bash",
                "state": {
                    "status": "completed",
                    "output": "/tmp\n"
                }
            }
        }),
    );
    let failed = row(
        3,
        "message.part.updated.1",
        json!({
            "sessionID": "ses_native",
            "part": {
                "id": "prt_tool_failed",
                "sessionID": "ses_native",
                "messageID": "msg_assistant",
                "type": "tool",
                "callID": "call_2",
                "tool": "bash",
                "state": {
                    "status": "failed",
                    "output": "boom"
                }
            }
        }),
    );
    let non_tool = row(
        4,
        "message.part.updated.1",
        json!({
            "sessionID": "ses_native",
            "part": {
                "id": "prt_text",
                "messageID": "msg_assistant",
                "type": "text",
                "text": "not a tool"
            }
        }),
    );

    let running = adapter.tool_call_observations(&running);
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].tool_call_id.as_deref(), Some("call_1"));
    assert_eq!(running[0].tool, "bash");
    assert_eq!(running[0].phase, ToolCallPhase::Pre);
    assert!(running[0].ok);

    let completed = adapter.tool_call_observations(&completed);
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].tool_call_id.as_deref(), Some("call_1"));
    assert_eq!(completed[0].phase, ToolCallPhase::Post);
    assert!(completed[0].ok);

    let failed = adapter.tool_call_observations(&failed);
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].tool_call_id.as_deref(), Some("call_2"));
    assert_eq!(failed[0].phase, ToolCallPhase::Post);
    assert!(!failed[0].ok);

    assert!(adapter.tool_call_observations(&non_tool).is_empty());
}

// These fixtures cover only the legacy native DB observer, not the plugin or ACP lanes.
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

    fn emitted_tool(state: Value) -> Value {
        let updates = translate_event_row(
            &row(
                1,
                "message.part.updated.1",
                json!({
                    "sessionID": "ses_native",
                    "part": {
                        "id": "prt_sanitize",
                        "sessionID": "ses_native",
                        "messageID": "msg_assistant",
                        "type": "tool",
                        "callID": "call_sanitize",
                        "tool": "bash",
                        "state": state
                    }
                }),
            ),
            &mut OpenCodeForwardState::default(),
        );
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].kind, AgentUpdateKind::ToolCall);
        assert_eq!(updates[0].data["id"], "call_sanitize");
        assert_eq!(updates[0].data["tool"], "bash");
        updates[0].data.clone()
    }

    fn emitted_input(input: Value) -> Value {
        let data = emitted_tool(json!({ "status": "running", "input": input }));
        assert_eq!(data["status"], "in_progress");
        data.get("input").expect("emitted input").clone()
    }

    fn emitted_result(output: Value, from_metadata: bool) -> Value {
        let state = if from_metadata {
            json!({ "status": "completed", "metadata": { "output": output } })
        } else {
            json!({ "status": "completed", "output": output })
        };
        let data = emitted_tool(state);
        assert_eq!(data["status"], "completed");
        data.get("output").expect("emitted output").clone()
    }

    fn assert_input_sanitized(text: &str) {
        let emitted = emitted_input(nested_payload(text));
        assert_structure_and_small_values(&emitted);
        assert!(
            emitted["nested"][0]["payload"].as_str() == Some("[omitted]"),
            "legacy DB input must sanitize before emission ({} input bytes)",
            text.len()
        );
    }

    fn assert_result_sanitized(text: &str) {
        let sanitized = [false, true].map(|from_metadata| {
            let nested = emitted_result(nested_payload(text), from_metadata);
            let plain = emitted_result(json!(text), from_metadata);
            assert_structure_and_small_values(&nested);
            [
                nested["nested"][0]["payload"].as_str() == Some("[omitted]"),
                plain.as_str() == Some("[omitted]"),
            ]
        });
        assert_eq!(
            sanitized,
            [[true; 2]; 2],
            "legacy DB direct/metadata nested/plain output must sanitize ({} input bytes)",
            text.len()
        );
    }

    #[test]
    fn legacy_db_input_omits_oversized_ascii_before_emission() {
        assert_input_sanitized(&"a".repeat(MAX_FIELD_BYTES + 1));
    }

    #[test]
    fn legacy_db_input_uses_utf8_bytes_not_character_count() {
        let text = format!("{}a", "é".repeat(MAX_FIELD_BYTES / 2));
        assert_eq!(text.len(), MAX_FIELD_BYTES + 1);
        assert!(text.chars().count() < MAX_FIELD_BYTES);
        assert_input_sanitized(&text);
    }

    #[test]
    fn legacy_db_input_omits_small_inline_base64_before_emission() {
        assert_input_sanitized("data:image/png;base64,AAAA");
    }

    #[test]
    fn legacy_db_result_omits_oversized_ascii_before_emission() {
        assert_result_sanitized(&"a".repeat(MAX_FIELD_BYTES + 1));
    }

    #[test]
    fn legacy_db_result_uses_utf8_bytes_not_character_count() {
        let text = format!("{}a", "é".repeat(MAX_FIELD_BYTES / 2));
        assert_eq!(text.len(), MAX_FIELD_BYTES + 1);
        assert!(text.chars().count() < MAX_FIELD_BYTES);
        assert_result_sanitized(&text);
    }

    #[test]
    fn legacy_db_result_omits_small_inline_base64_before_emission() {
        assert_result_sanitized("data:image/png;base64,AAAA");
    }

    #[test]
    fn legacy_db_preserves_exact_byte_boundary_and_small_structured_controls() {
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
                emitted_result(expected.clone(), false),
                emitted_result(expected.clone(), true),
            ] {
                assert_structure_and_small_values(&emitted);
                assert!(
                    emitted == expected,
                    "safe structured fixture must survive unchanged ({} text bytes)",
                    text.len()
                );
            }
            for from_metadata in [false, true] {
                assert!(
                    emitted_result(json!(text), from_metadata).as_str() == Some(text.as_str()),
                    "safe raw result must survive unchanged ({} text bytes)",
                    text.len()
                );
            }
        }
    }
}
