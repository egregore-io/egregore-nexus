use nexus_contracts::AgentUpdateKind;
use nexus_harness_codex::app_server::{method, tool_call_observations, translate_codex};
use nexus_transcript::ToolCallPhase;

const MAX_FIELD_BYTES: usize = 8 * 1024;
const OMITTED: &str = "[omitted]";

fn ev(m: &str, p: serde_json::Value) -> Option<nexus_agent::StreamEvent> {
    translate_codex(m, &p)
}

fn obs(m: &str, p: serde_json::Value) -> Vec<nexus_transcript::ToolCallObservation> {
    tool_call_observations(m, &p)
}

#[test]
fn agent_message_delta_is_text() {
    let event = ev(
        method::AGENT_MESSAGE_DELTA,
        serde_json::json!({"delta":"hi", "itemId":"am1"}),
    )
    .unwrap();

    assert_eq!(event.kind, AgentUpdateKind::Text);
    assert_eq!(event.data["text"], "hi");
    assert_eq!(event.data["itemId"], "am1");
}

#[test]
fn reasoning_delta_is_thinking() {
    let event = ev(
        method::REASONING_TEXT_DELTA,
        serde_json::json!({"delta":"mm"}),
    )
    .unwrap();

    assert_eq!(event.kind, AgentUpdateKind::Thinking);
    assert_eq!(event.data["text"], "mm");
}

#[test]
fn plan_delta_is_plan() {
    let event = ev(method::PLAN_DELTA, serde_json::json!({"delta":"step 1"})).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::Plan);
    assert_eq!(event.data["text"], "step 1");
}

#[test]
fn command_output_delta_is_tool_call_in_progress() {
    let params = serde_json::json!({"itemId":"i42","delta":"stdout line\n"});
    let event = ev(method::COMMAND_OUTPUT_DELTA, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::ToolCall);
    assert_eq!(event.data["id"], "i42");
    assert_eq!(event.data["status"], "in_progress");
    assert_eq!(event.data["output"], "stdout line\n");
}

#[test]
fn command_output_delta_is_tool_call_pre_observation() {
    let observations = obs(
        method::COMMAND_OUTPUT_DELTA,
        serde_json::json!({"itemId":"i42","delta":"stdout line\n"}),
    );

    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].tool_call_id.as_deref(), Some("i42"));
    assert_eq!(observations[0].tool, "commandExecution");
    assert_eq!(observations[0].phase, ToolCallPhase::Pre);
    assert!(observations[0].ok);
}

#[test]
fn completed_command_item_is_tool_call() {
    let params = serde_json::json!({"item":{"type":"commandExecution","id":"i1","command":"ls","aggregatedOutput":"a\nb","commandActions":[],"cwd":"/","status":"completed"}});
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::ToolCall);
    assert_eq!(event.data["status"], "completed");
    assert_eq!(event.data["id"], "i1");
    assert_eq!(event.data["title"], "ls");
    assert_eq!(
        event.data["tool"], "shell",
        "C-TOOL: canonical machine name, never the command line"
    );
    assert_eq!(event.data["kind"], "commandExecution");
    assert_eq!(event.data["output"], "a\nb");
    // Replay convergence: args must survive materialization, not just the title —
    // the command and cwd ARE the tool call's arguments.
    assert_eq!(
        event.data["input"],
        serde_json::json!({"command": "ls", "cwd": "/"})
    );
}

#[test]
fn completed_web_search_item_carries_query_as_raw_input() {
    let params = serde_json::json!({
        "item": {"type":"webSearch","id":"ws1","query":"rust lazy txn","status":"completed"}
    });
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::ToolCall);
    assert_eq!(event.data["title"], "rust lazy txn");
    assert_eq!(event.data["tool"], "search");
    assert_eq!(
        event.data["input"],
        serde_json::json!({"query": "rust lazy txn"})
    );
}

#[test]
fn completed_file_change_item_is_tool_call() {
    let params = serde_json::json!({
        "item": {
            "type": "fileChange",
            "id": "fc1",
            "status": "completed",
            "changes": [{"path": "foo.rs", "diff": "...", "kind": {"type": "update"}}]
        }
    });
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::ToolCall);
    assert_eq!(event.data["id"], "fc1");
    assert_eq!(event.data["status"], "completed");
    assert_eq!(event.data["kind"], "fileChange");
    assert_eq!(event.data["tool"], "edit");
    assert!(event.data.get("input").is_some());
}

#[test]
fn completed_web_search_item_is_tool_call() {
    let params = serde_json::json!({
        "item": {"type": "webSearch", "id": "ws1", "query": "rust async"}
    });
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::ToolCall);
    assert_eq!(event.data["id"], "ws1");
    assert_eq!(event.data["title"], "rust async");
    assert_eq!(event.data["kind"], "webSearch");
}

#[test]
fn completed_mcp_tool_call_item_is_tool_call() {
    let params = serde_json::json!({
        "item": {
            "type": "mcpToolCall",
            "id": "mcp1",
            "server": "files",
            "tool": "read_file",
            "status": "completed",
            "arguments": {"path": "/tmp/x"},
            "result": {"content": []}
        }
    });
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::ToolCall);
    assert_eq!(event.data["id"], "mcp1");
    assert_eq!(event.data["title"], "files/read_file");
    assert_eq!(
        event.data["tool"], "files/read_file",
        "MCP registered name IS the machine name"
    );
    assert_eq!(event.data["kind"], "mcpToolCall");
    assert_eq!(event.data["input"]["path"], "/tmp/x");
}

#[test]
fn completed_tool_items_are_tool_call_post_observations() {
    let cases = [
        (
            serde_json::json!({
                "item": {"type":"commandExecution","id":"i1","command":"ls","status":"completed"}
            }),
            "i1",
            "ls",
        ),
        (
            serde_json::json!({
                "item": {"type":"fileChange","id":"fc1","status":"completed","changes":[]}
            }),
            "fc1",
            "fileChange",
        ),
        (
            serde_json::json!({
                "item": {"type":"webSearch","id":"ws1","query":"rust async"}
            }),
            "ws1",
            "rust async",
        ),
        (
            serde_json::json!({
                "item": {
                    "type":"mcpToolCall",
                    "id":"mcp1",
                    "server":"files",
                    "tool":"read_file",
                    "status":"completed",
                    "arguments":{},
                    "result":null
                }
            }),
            "mcp1",
            "files/read_file",
        ),
    ];

    for (params, id, tool) in cases {
        let observations = obs(method::ITEM_COMPLETED, params);
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].tool_call_id.as_deref(), Some(id));
        assert_eq!(observations[0].tool, tool);
        assert_eq!(observations[0].phase, ToolCallPhase::Post);
        assert!(observations[0].ok);
    }
}

#[test]
fn completed_failed_tool_items_are_post_observations_with_ok_false() {
    let observations = obs(
        method::ITEM_COMPLETED,
        serde_json::json!({
            "item": {
                "type":"commandExecution",
                "id":"cmd1",
                "command":"cargo test",
                "status":"failed",
                "exitCode":101
            }
        }),
    );

    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].tool_call_id.as_deref(), Some("cmd1"));
    assert_eq!(observations[0].tool, "cargo test");
    assert_eq!(observations[0].phase, ToolCallPhase::Post);
    assert!(!observations[0].ok);
}

#[test]
fn completed_agent_message_is_text() {
    let params = serde_json::json!({
        "item": {"type": "agentMessage", "id": "am1", "text": "final answer"}
    });
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::Text);
    assert_eq!(event.data["text"], "final answer");
    assert_eq!(event.data["itemId"], "am1");
}

#[test]
fn non_tool_items_do_not_emit_tool_call_observations() {
    assert!(obs(
        method::ITEM_COMPLETED,
        serde_json::json!({"item": {"type": "agentMessage", "id": "am1", "text": "done"}}),
    )
    .is_empty());
    assert!(obs(
        method::ITEM_COMPLETED,
        serde_json::json!({
            "item": {"type": "userMessage", "id": "um1", "content": [{"type":"text","text":"hi"}]}
        }),
    )
    .is_empty());
}

#[test]
fn completed_user_message_is_user_input() {
    let params = serde_json::json!({
        "item": {
            "type": "userMessage",
            "id": "um1",
            "content": [
                {"type": "text", "text": "typed in the Codex TUI"}
            ]
        }
    });
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::UserInput);
    assert_eq!(event.data["text"], "typed in the Codex TUI");
}

#[test]
fn completed_user_message_keeps_text_and_marks_non_text_parts() {
    let params = serde_json::json!({
        "item": {
            "type": "userMessage",
            "id": "um1",
            "content": [
                {"type": "text", "text": "look at this"},
                {"type": "image", "source": "opaque"},
                {"type": "mention", "text": "@workspace"}
            ]
        }
    });
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::UserInput);
    assert_eq!(
        event.data["text"],
        "look at this\n[image input]\n[mention input]"
    );
}

#[test]
fn completed_user_message_with_only_non_text_parts_is_still_visible() {
    let params = serde_json::json!({
        "item": {
            "type": "userMessage",
            "id": "um1",
            "content": [
                {"type": "localImage", "path": "/tmp/screenshot.png"}
            ]
        }
    });
    let event = ev(method::ITEM_COMPLETED, params).unwrap();

    assert_eq!(event.kind, AgentUpdateKind::UserInput);
    assert_eq!(event.data["text"], "[localImage input]");
}

#[test]
fn turn_completed_is_turn_end() {
    let event = ev(
        method::TURN_COMPLETED,
        serde_json::json!({"threadId":"t","turnId":"u"}),
    )
    .unwrap();

    assert_eq!(event.kind, AgentUpdateKind::TurnEnd);
    assert_eq!(event.data, serde_json::json!({}));
}

#[test]
fn error_notification_is_turn_end() {
    let event = ev(
        method::TURN_FAILED,
        serde_json::json!({"error":"contextWindowExceeded","threadId":"t","turnId":"u","willRetry":false}),
    )
    .unwrap();

    assert_eq!(event.kind, AgentUpdateKind::TurnEnd);
    assert_eq!(event.data, serde_json::json!({}));
}

#[test]
fn retrying_error_notification_is_not_turn_end() {
    assert!(ev(
        method::TURN_FAILED,
        serde_json::json!({
            "error": {
                "message": "usage limit",
                "codexErrorInfo": "usageLimitExceeded"
            },
            "threadId": "t",
            "turnId": "u",
            "willRetry": true
        }),
    )
    .is_none());
}

#[test]
fn thread_started_not_rendered() {
    assert!(ev(method::THREAD_STARTED, serde_json::json!({"threadId":"t"})).is_none());
}

#[test]
fn turn_started_not_rendered() {
    assert!(ev(
        method::TURN_STARTED,
        serde_json::json!({"threadId":"t","turnId":"u"})
    )
    .is_none());
}

#[test]
fn item_started_not_rendered() {
    assert!(ev(
        method::ITEM_STARTED,
        serde_json::json!({"threadId":"t","turnId":"u","item":{"type":"agentMessage","id":"x","text":""}})
    )
    .is_none());
}

#[test]
fn token_usage_not_rendered() {
    assert!(ev(
        method::TOKEN_USAGE_UPDATED,
        serde_json::json!({"threadId":"t","tokenCount":100})
    )
    .is_none());
}

#[test]
fn item_completed_unknown_type_not_rendered() {
    let params = serde_json::json!({"item": {"type": "contextCompaction", "id": "cc1"}});

    assert!(ev(method::ITEM_COMPLETED, params).is_none());
}

#[test]
fn oversized_delta_is_omitted() {
    let big = "x".repeat(MAX_FIELD_BYTES + 1);
    let event = ev(
        method::AGENT_MESSAGE_DELTA,
        serde_json::json!({"delta": big}),
    )
    .unwrap();

    assert_eq!(event.data["text"], OMITTED);
}

#[test]
fn base64_blob_in_command_output_is_omitted() {
    let blob = format!("data:image/png;base64,{}", "A".repeat(100));
    let params = serde_json::json!({"itemId":"i1","delta": blob});
    let event = ev(method::COMMAND_OUTPUT_DELTA, params).unwrap();

    assert_eq!(event.data["output"], OMITTED);
}

#[test]
fn small_delta_passes_through() {
    let event = ev(
        method::AGENT_MESSAGE_DELTA,
        serde_json::json!({"delta": "hello"}),
    )
    .unwrap();

    assert_eq!(event.data["text"], "hello");
}
