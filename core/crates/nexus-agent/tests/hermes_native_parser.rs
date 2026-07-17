use nexus_agent::adapter::hermes::native::{
    translate_message_row, HermesForwardState, HermesMessageRow, HermesNativeAdapter,
};
use nexus_contracts::AgentUpdateKind;
use nexus_transcript::ToolCallPhase;
use serde_json::json;

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
    assert_eq!(updates[2].data["rawOutput"]["output"], "/tmp\n");
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
