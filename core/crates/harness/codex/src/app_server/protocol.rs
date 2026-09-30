// Codex app-server protocol constants and typed param builders.
//
// SCHEMA SOURCE OF TRUTH
// ─────────────────────
// All method names and param shapes are pinned from the top-level (non-versioned)
// schema in the codex-rs repository:
//
//   codex-rs/app-server-protocol/schema/json/
//     ClientRequest.json          — client → server requests & client notifications
//     ServerNotification.json     — server → client notifications
//     ServerRequest.json          — server → client requests (approvals)
//     ClientNotification.json     — "initialized" client notification
//
// VERSION DECISION
// ────────────────
// The top-level (non-versioned) schema is the one in use. The reference client
// (codex-rs/app-server-client/src/remote.rs) imports directly from
// `codex_app_server_protocol::{ClientRequest, ServerNotification, …}` which maps
// to the top-level schema. The v1/ and v2/ subdirectories contain older snapshots
// and are not consumed by the reference client. Evidence: remote.rs line 27:
//   use codex_app_server_protocol::ClientRequest;
//
// TURN ERROR NOTIFICATIONS
// ────────────────────────
// • TURN_FAILED: The schema has no "turn/failed" notification.
//   The turn-error path is the top-level "error" notification
//   (ServerNotification.json line 4860, params: ErrorNotification { error, threadId,
//   turnId, willRetry }). TURN_FAILED is kept as an alias for "error" so call-sites
//   that want to match turn errors can use the named constant.
//
// TurnStartParams (ClientRequest.json line 4074):
//   required: ["input", "threadId"]
//   input: array of UserInput where UserInput.type = "text" (enum), UserInput.text = String
//   (ClientRequest.json line 4205)
//
// TurnInterruptParams (ClientRequest.json line 4034):
//   required: ["threadId", "turnId"]
//
// ThreadStartParams (ClientRequest.json line 3868):
//   no required fields; all optional (cwd, model, approvalPolicy, etc.)
//
// ThreadResumeParams (ClientRequest.json line 3692):
//   required: ["threadId"], optional overrides include `cwd`, `approvalPolicy`, and `sandbox`.
//
// ThreadCompactStartParams:
//   required: ["threadId"].
//
// InitializeParams (ClientRequest.json line 1271):
//   required: ["clientInfo"] where clientInfo: { name, version }

use serde_json::{json, Value};

/// Method-name constants pinned from the codex app-server schema.
pub mod method {
    // client → server requests (ClientRequest.json)
    pub const INITIALIZE: &str = "initialize";
    pub const THREAD_START: &str = "thread/start";
    pub const THREAD_RESUME: &str = "thread/resume";
    pub const TURN_START: &str = "turn/start";
    /// Append user input to the currently active regular turn.
    pub const TURN_STEER: &str = "turn/steer";
    pub const TURN_INTERRUPT: &str = "turn/interrupt";
    /// Native context compaction — the same operation the TUI's `/compact` runs.
    /// Verified against the shipped app-server binary's method table (alongside
    /// `thread/rollback`, `thread/inject`); completion is signalled by the
    /// `thread/compacted` notification.
    pub const THREAD_COMPACT_START: &str = "thread/compact/start";

    // client → server notification (ClientNotification.json)
    pub const INITIALIZED: &str = "initialized";

    // server → client notifications (ServerNotification.json, subset we render)
    pub const THREAD_STARTED: &str = "thread/started";
    pub const TURN_STARTED: &str = "turn/started";
    pub const TURN_COMPLETED: &str = "turn/completed";
    /// The schema has no "turn/failed". Turn errors arrive as the top-level
    /// "error" notification (ErrorNotification: { error, threadId, turnId, willRetry }).
    /// This constant maps to that real method string so matchers work correctly.
    pub const TURN_FAILED: &str = "error";
    pub const ITEM_STARTED: &str = "item/started";
    pub const ITEM_COMPLETED: &str = "item/completed";
    pub const AGENT_MESSAGE_DELTA: &str = "item/agentMessage/delta";
    pub const REASONING_TEXT_DELTA: &str = "item/reasoning/textDelta";
    pub const PLAN_DELTA: &str = "item/plan/delta";
    pub const COMMAND_OUTPUT_DELTA: &str = "item/commandExecution/outputDelta";
    pub const TOKEN_USAGE_UPDATED: &str = "thread/tokenUsage/updated";
    pub const THREAD_COMPACTED: &str = "thread/compacted";

    // server → client requests (ServerRequest.json, approval methods)
    pub const CMD_REQUEST_APPROVAL: &str = "item/commandExecution/requestApproval";
    pub const FILE_REQUEST_APPROVAL: &str = "item/fileChange/requestApproval";
    pub const PERMISSIONS_REQUEST_APPROVAL: &str = "item/permissions/requestApproval";
    pub const TOOL_REQUEST_USER_INPUT: &str = "item/tool/requestUserInput";
}

/// Build params for the `initialize` request.
///
/// Schema (ClientRequest.json, InitializeParams line 1271):
///   required: clientInfo: { name: String, version: String }
pub fn initialize_params(client_name: &str) -> Value {
    json!({
        "clientInfo": {
            "name": client_name,
            "version": "0.1.0"
        }
    })
}

/// Build params for `thread/start`.
///
/// Schema (ClientRequest.json, ThreadStartParams line 3868):
///   No required fields. Optional fields include `cwd`, `approvalPolicy`, and `sandbox`.
pub fn thread_start_params(cwd: Option<&str>) -> Value {
    // The bridge thread is an autonomous bus agent: it must run tool calls (incl. the nexus-bus
    // MCP `reply`/`dm`) WITHOUT a human clicking "approve" in the TUI. Without this, codex defaults
    // to asking for approval and the call is rejected in a headless/detached turn ("user rejected
    // MCP tool call"). `approvalPolicy: never` + `sandbox: danger-full-access` mirrors the prior
    // headed-codex `--dangerously-bypass-approvals-and-sandbox`. The human TUI resumes this same
    // thread and inherits the policy.
    let mut params = json!({
        "approvalPolicy": "never",
        "sandbox": "danger-full-access"
    });
    if let Some(cwd) = cwd {
        params["cwd"] = Value::String(cwd.to_string());
    }
    params
}

/// Build params for `thread/resume`.
///
/// Schema (ClientRequest.json, ThreadResumeParams line 3692):
///   required: ["threadId"]; optional fields include `cwd`, `approvalPolicy`, and `sandbox`.
pub fn thread_resume_params(thread_id: &str, cwd: Option<&str>) -> Value {
    let mut params = json!({
        "threadId": thread_id,
        "approvalPolicy": "never",
        "sandbox": "danger-full-access"
    });
    if let Some(cwd) = cwd {
        params["cwd"] = Value::String(cwd.to_string());
    }
    params
}

/// Build params for `turn/start`.
///
/// Schema (ClientRequest.json, TurnStartParams line 4074 + UserInput line 4205):
///   required: ["input", "threadId"]
///   input: array of UserInput
///   UserInput (text variant): { "type": "text", "text": String }
pub fn turn_start_params(thread_id: &str, text: &str) -> Value {
    json!({
        "threadId": thread_id,
        "input": [{ "type": "text", "text": text }]
    })
}

/// Build params for `turn/steer`.
///
/// Schema (`TurnSteerParams`): `threadId`, `input`, and `expectedTurnId` are required. Codex uses
/// the expected id as an optimistic concurrency guard against steering the wrong native turn.
pub fn turn_steer_params(thread_id: &str, text: &str, expected_turn_id: &str) -> Value {
    json!({
        "threadId": thread_id,
        "input": [{ "type": "text", "text": text }],
        "expectedTurnId": expected_turn_id
    })
}

/// Build params for `thread/compact/start`.
pub fn thread_compact_params(thread_id: &str) -> Value {
    json!({ "threadId": thread_id })
}

/// Build params for `turn/interrupt`.
///
/// Schema (ClientRequest.json, TurnInterruptParams line 4034):
///   required: ["threadId", "turnId"]
pub fn turn_interrupt_params(thread_id: &str, turn_id: &str) -> Value {
    json!({ "threadId": thread_id, "turnId": turn_id })
}

/// Extract the `threadId` field from a notification params object.
///
/// Most server notifications that carry thread context include a top-level
/// `"threadId"` string field (confirmed in AgentMessageDeltaNotification,
/// CommandExecutionOutputDeltaNotification, ItemStartedNotification,
/// TurnStartedNotification, etc. in ServerNotification.json).
///
/// Returns `None` when the field is absent or not a string.
pub fn thread_id_of(_method: &str, params: &Value) -> Option<String> {
    // Most runtime notifications carry a flat `threadId`; the lifecycle
    // `thread/started` notification nests it at `thread.id` (Thread struct).
    params
        .get("threadId")
        .and_then(Value::as_str)
        .or_else(|| {
            params
                .get("thread")
                .and_then(|t| t.get("id"))
                .and_then(Value::as_str)
        })
        .map(String::from)
}
