use agent_client_protocol::schema::v1::{
    AvailableCommand, AvailableCommandsUpdate, ContentBlock, ContentChunk, Plan, PlanEntry,
    PlanEntryPriority, PlanEntryStatus, SessionInfoUpdate, SessionUpdate, TextContent, ToolCall,
    ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, ToolKind,
};
use nexus_acp_stream::translate;
use nexus_contracts::AgentUpdateKind;

const MAX_FIELD_BYTES: usize = 8 * 1024;
const OMITTED: &str = "[omitted]";

fn text_block(s: &str) -> ContentBlock {
    ContentBlock::Text(TextContent::new(s))
}

#[test]
fn thought_chunk_maps_to_thinking() {
    let update = SessionUpdate::AgentThoughtChunk(ContentChunk::new(text_block("why")));
    let event = translate(&update).expect("thought is renderable");

    assert_eq!(event.kind, AgentUpdateKind::Thinking);
    assert_eq!(event.data["text"], "why");
}

#[test]
fn message_chunk_maps_to_text() {
    let update = SessionUpdate::AgentMessageChunk(ContentChunk::new(text_block("hi")));
    let event = translate(&update).expect("message is renderable");

    assert_eq!(event.kind, AgentUpdateKind::Text);
    assert_eq!(event.data["text"], "hi");
}

#[test]
fn tool_call_maps_to_tool_call_with_id() {
    let tool_call = ToolCall::new("tc_1", "Read file").status(ToolCallStatus::InProgress);
    let event = translate(&SessionUpdate::ToolCall(tool_call)).expect("tool call is renderable");

    assert_eq!(event.kind, AgentUpdateKind::ToolCall);
    assert_eq!(event.data["id"], "tc_1");
    assert_eq!(event.data["title"], "Read file");
    assert_eq!(event.data["status"], "in_progress");
}

#[test]
fn tool_call_update_keeps_merge_id_and_status() {
    let tool_call_update = ToolCallUpdate::new(
        "tc_1",
        ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
    );
    let event = translate(&SessionUpdate::ToolCallUpdate(tool_call_update))
        .expect("tool update is renderable");

    assert_eq!(event.kind, AgentUpdateKind::ToolCall);
    assert_eq!(event.data["id"], "tc_1");
    assert_eq!(event.data["status"], "completed");
}

#[test]
fn plan_maps_to_plan_entries() {
    let plan = Plan::new(vec![PlanEntry::new(
        "do the thing",
        PlanEntryPriority::High,
        PlanEntryStatus::Pending,
    )]);
    let event = translate(&SessionUpdate::Plan(plan)).expect("plan is renderable");

    assert_eq!(event.kind, AgentUpdateKind::Plan);
    assert_eq!(event.data["entries"][0]["content"], "do the thing");
}

#[test]
fn available_commands_map_to_commands_with_both_names() {
    let update = SessionUpdate::AvailableCommandsUpdate(AvailableCommandsUpdate::new(vec![
        AvailableCommand::new("compact", "Compact the context"),
        AvailableCommand::new("clear", "Clear the conversation"),
    ]));
    let event = translate(&update).expect("available commands are renderable");

    assert_eq!(event.kind, AgentUpdateKind::Commands);
    let names: Vec<&str> = event.data["commands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|command| command["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["compact", "clear"]);
    assert_eq!(
        event.data["commands"][0]["description"],
        "Compact the context"
    );
}

#[test]
fn unhandled_variant_is_none() {
    let update = SessionUpdate::SessionInfoUpdate(SessionInfoUpdate::new());

    assert!(translate(&update).is_none());
}

#[test]
fn oversized_tool_field_is_omitted() {
    let big = "x".repeat(MAX_FIELD_BYTES + 1);
    let tool_call = ToolCall::new("tc_big", "Run").raw_output(serde_json::json!({ "result": big }));
    let event = translate(&SessionUpdate::ToolCall(tool_call)).expect("renderable");

    assert_eq!(event.data["output"]["result"], OMITTED);
}

#[test]
fn base64_image_blob_is_omitted() {
    let blob = format!("data:image/png;base64,{}", "A".repeat(100));
    let tool_call =
        ToolCall::new("tc_img", "Gen").raw_output(serde_json::json!({ "result": blob }));
    let event = translate(&SessionUpdate::ToolCall(tool_call)).expect("renderable");

    assert_eq!(event.data["output"]["result"], OMITTED);
}

#[test]
fn small_fields_pass_through() {
    let tool_call = ToolCall::new("tc_ok", "Run").raw_input(serde_json::json!({ "cmd": "ls" }));
    let event = translate(&SessionUpdate::ToolCall(tool_call)).expect("renderable");

    assert_eq!(event.data["input"]["cmd"], "ls");
}

// --- C-TOOL v1 (docs/tool-call-contract.md) ---------------------------------

#[test]
fn tool_call_carries_canonical_tool_and_structured_input() {
    let tool_call = ToolCall::new("tc_x", "/usr/bin/zsh -lc 'ls'")
        .kind(ToolKind::Execute)
        .status(ToolCallStatus::InProgress)
        .raw_input(serde_json::json!({ "command": "ls" }));
    let event = translate(&SessionUpdate::ToolCall(tool_call)).expect("renderable");

    assert_eq!(
        event.data["tool"], "shell",
        "execute kind → canonical `shell`"
    );
    assert_eq!(event.data["title"], "/usr/bin/zsh -lc 'ls'");
    assert!(
        event.data["input"].is_object(),
        "input must be the structured object"
    );
    assert_eq!(event.data["input"]["command"], "ls");
    assert!(
        event.data.get("rawInput").is_none(),
        "rawInput is replaced by the contract's `input`"
    );
}

#[test]
fn tool_call_kind_maps_to_canonical_names() {
    for (kind, expected) in [
        (ToolKind::Read, "read"),
        (ToolKind::Edit, "edit"),
        (ToolKind::Search, "search"),
        (ToolKind::Fetch, "fetch"),
        (ToolKind::Execute, "shell"),
    ] {
        let tool_call = ToolCall::new("tc_k", "t").kind(kind);
        let event = translate(&SessionUpdate::ToolCall(tool_call)).expect("renderable");
        assert_eq!(event.data["tool"], expected);
    }
}

#[test]
fn tool_call_update_uses_contract_keys_and_derives_tool_when_kind_present() {
    let update = ToolCallUpdate::new(
        "tc_x",
        ToolCallUpdateFields::new()
            .kind(ToolKind::Execute)
            .raw_input(serde_json::json!({ "command": "pwd" }))
            .raw_output(serde_json::json!({ "stdout": "/tmp" })),
    );
    let event = translate(&SessionUpdate::ToolCallUpdate(update)).expect("renderable");

    assert_eq!(event.data["tool"], "shell");
    assert_eq!(event.data["input"]["command"], "pwd");
    assert_eq!(event.data["output"]["stdout"], "/tmp");
    assert!(event.data.get("rawInput").is_none());
    assert!(event.data.get("rawOutput").is_none());
}

#[test]
fn tool_call_update_without_kind_stays_a_partial_patch() {
    let update = ToolCallUpdate::new(
        "tc_x",
        ToolCallUpdateFields::new().status(ToolCallStatus::Completed),
    );
    let event = translate(&SessionUpdate::ToolCallUpdate(update)).expect("renderable");

    assert_eq!(event.data["id"], "tc_x");
    assert_eq!(event.data["status"], "completed");
    assert!(
        event.data.get("tool").is_none(),
        "no kind → no derivable tool on a patch"
    );
}
