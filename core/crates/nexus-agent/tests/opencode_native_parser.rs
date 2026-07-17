use nexus_agent::adapter::opencode::native::{
    translate_event_row, OpenCodeEventRow, OpenCodeForwardState, OpenCodeNativeAdapter,
};
use nexus_contracts::AgentUpdateKind;
use nexus_transcript::ToolCallPhase;
use serde_json::json;

fn row(seq: i64, event_type: &str, data: serde_json::Value) -> OpenCodeEventRow {
    OpenCodeEventRow {
        seq,
        event_type: event_type.to_string(),
        data,
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
