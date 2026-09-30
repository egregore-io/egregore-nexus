use nexus_harness_codex::app_server::protocol::{
    initialize_params, method, thread_compact_params, thread_id_of, thread_resume_params,
    thread_start_params, turn_interrupt_params, turn_start_params, turn_steer_params,
};

#[test]
fn thread_resume_params_can_include_cwd_override() {
    let params = thread_resume_params("abc-123", Some("/repo"));

    assert_eq!(params["threadId"], "abc-123");
    assert_eq!(params["cwd"], "/repo");
    assert_eq!(params["approvalPolicy"], "never");
    assert_eq!(params["sandbox"], "danger-full-access");
}

#[test]
fn turn_start_params_matches_schema_shape() {
    let params = turn_start_params("t1", "hello");

    assert_eq!(params["threadId"], "t1");
    assert_eq!(params["input"][0]["type"], "text");
    assert_eq!(params["input"][0]["text"], "hello");
}

#[test]
fn turn_steer_params_matches_schema_shape() {
    let params = turn_steer_params("t1", "focus tests", "turn-42");

    assert_eq!(params["threadId"], "t1");
    assert_eq!(params["input"][0]["type"], "text");
    assert_eq!(params["input"][0]["text"], "focus tests");
    assert_eq!(params["expectedTurnId"], "turn-42");
}

#[test]
fn thread_id_extracted_from_delta_notification() {
    let notification = serde_json::json!({"threadId":"t9","turnId":"u1","itemId":"i1","delta":"x"});

    assert_eq!(
        thread_id_of(method::AGENT_MESSAGE_DELTA, &notification).as_deref(),
        Some("t9")
    );
}

#[test]
fn thread_start_params_sets_autonomous_approval_policy() {
    let params = thread_start_params(None);

    assert!(params.is_object());
    assert_eq!(params["approvalPolicy"], "never");
    assert_eq!(params["sandbox"], "danger-full-access");
}

#[test]
fn thread_start_params_can_include_cwd() {
    let params = thread_start_params(Some("/repo"));

    assert_eq!(params["cwd"], "/repo");
    assert_eq!(params["approvalPolicy"], "never");
    assert_eq!(params["sandbox"], "danger-full-access");
}

#[test]
fn thread_resume_params_has_thread_id() {
    let params = thread_resume_params("abc-123", None);

    assert_eq!(params["threadId"], "abc-123");
}

#[test]
fn thread_compact_params_has_thread_id() {
    let params = thread_compact_params("abc-compact");

    assert_eq!(params["threadId"], "abc-compact");
}

#[test]
fn turn_interrupt_params_has_both_ids() {
    let params = turn_interrupt_params("t1", "turn-42");

    assert_eq!(params["threadId"], "t1");
    assert_eq!(params["turnId"], "turn-42");
}

#[test]
fn initialize_params_has_client_info() {
    let params = initialize_params("nexus-bridge");

    assert_eq!(params["clientInfo"]["name"], "nexus-bridge");
}

#[test]
fn thread_id_of_returns_none_for_missing_field() {
    let notification = serde_json::json!({"delta": "x"});

    assert_eq!(
        thread_id_of(method::AGENT_MESSAGE_DELTA, &notification),
        None
    );
}

#[test]
fn turn_failed_maps_to_error_notification() {
    assert_eq!(method::TURN_FAILED, "error");
}

#[test]
fn method_constants_match_schema() {
    assert_eq!(method::INITIALIZE, "initialize");
    assert_eq!(method::THREAD_START, "thread/start");
    assert_eq!(method::THREAD_RESUME, "thread/resume");
    assert_eq!(method::THREAD_COMPACT_START, "thread/compact/start");
    assert_eq!(method::TURN_START, "turn/start");
    assert_eq!(method::TURN_STEER, "turn/steer");
    assert_eq!(method::TURN_INTERRUPT, "turn/interrupt");
    assert_eq!(method::INITIALIZED, "initialized");
    assert_eq!(method::THREAD_STARTED, "thread/started");
    assert_eq!(method::TURN_STARTED, "turn/started");
    assert_eq!(method::TURN_COMPLETED, "turn/completed");
    assert_eq!(method::ITEM_STARTED, "item/started");
    assert_eq!(method::ITEM_COMPLETED, "item/completed");
    assert_eq!(method::AGENT_MESSAGE_DELTA, "item/agentMessage/delta");
    assert_eq!(method::REASONING_TEXT_DELTA, "item/reasoning/textDelta");
    assert_eq!(method::PLAN_DELTA, "item/plan/delta");
    assert_eq!(
        method::COMMAND_OUTPUT_DELTA,
        "item/commandExecution/outputDelta"
    );
    assert_eq!(method::TOKEN_USAGE_UPDATED, "thread/tokenUsage/updated");
    assert_eq!(
        method::CMD_REQUEST_APPROVAL,
        "item/commandExecution/requestApproval"
    );
    assert_eq!(
        method::FILE_REQUEST_APPROVAL,
        "item/fileChange/requestApproval"
    );
    assert_eq!(
        method::PERMISSIONS_REQUEST_APPROVAL,
        "item/permissions/requestApproval"
    );
    assert_eq!(
        method::TOOL_REQUEST_USER_INPUT,
        "item/tool/requestUserInput"
    );
}
