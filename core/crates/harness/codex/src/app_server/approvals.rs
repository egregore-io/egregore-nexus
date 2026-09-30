//! `ApprovalHandler` trait and the `AutoApprove` default implementation.
//!
//! The trait supplies responses to server approval requests. The default
//! `AutoApprove` accepts command and file-change requests without operator interaction.

use nexus_contracts::ids::SessionId;
use serde_json::Value;

/// Decides the JSON-RPC `result` to respond with when codex sends a
/// server→client approval request.
#[async_trait::async_trait]
pub trait ApprovalHandler: Send + Sync {
    /// Called for each server→client request whose `method` is one of the
    /// approval methods. Return the JSON value to place in `result`.
    async fn ask(&self, session: &SessionId, method: &str, params: &Value) -> Value;
}

/// Always approves — returns the schema-correct response shape for each
/// approval method type.
///
/// # Pinned response shapes (schema source of truth)
///
/// - `item/commandExecution/requestApproval`
///   → `CommandExecutionRequestApprovalResponse { decision: CommandExecutionApprovalDecision }`
///   Schema: `codex-rs/app-server-protocol/schema/json/CommandExecutionRequestApprovalResponse.json`
///   `required: ["decision"]`; AutoApprove → `{"decision": "accept"}`
///
/// - `item/fileChange/requestApproval`
///   → `FileChangeRequestApprovalResponse { decision: FileChangeApprovalDecision }`
///   Schema: `…/FileChangeRequestApprovalResponse.json`
///   `required: ["decision"]`; AutoApprove → `{"decision": "accept"}`
///
/// - `item/permissions/requestApproval`
///   → `PermissionsRequestApprovalResponse { permissions: GrantedPermissionProfile, scope?, strictAutoReview? }`
///   Schema: `…/PermissionsRequestApprovalResponse.json`
///   `required: ["permissions"]`; AutoApprove → `{"permissions": {}}` (empty grant profile —
///   both `fileSystem` and `network` are nullable, so `{}` is a valid minimal grant)
///
/// - `item/tool/requestUserInput`
///   → `ToolRequestUserInputResponse { answers: { <id>: ToolRequestUserInputAnswer } }`
///   Schema: `…/ToolRequestUserInputResponse.json`
///   `required: ["answers"]`; AutoApprove → `{"answers": {}}` (empty map — no input to give;
///   safe because `answers` is `additionalProperties: ToolRequestUserInputAnswer`, so an empty
///   object is schema-valid)
///
/// This is the default used in `pty_supervisor`; it does not ask an operator.
pub struct AutoApprove;

#[async_trait::async_trait]
impl ApprovalHandler for AutoApprove {
    async fn ask(&self, _session: &SessionId, method: &str, _params: &Value) -> Value {
        match method {
            // item/commandExecution/requestApproval
            // Schema: CommandExecutionRequestApprovalResponse.json — required: ["decision"]
            // CommandExecutionApprovalDecision enum: "accept" | "acceptForSession" | object variants | "decline" | "cancel"
            super::protocol::method::CMD_REQUEST_APPROVAL => {
                serde_json::json!({ "decision": "accept" })
            }
            // item/fileChange/requestApproval
            // Schema: FileChangeRequestApprovalResponse.json — required: ["decision"]
            // FileChangeApprovalDecision enum: "accept" | "acceptForSession" | "decline" | "cancel"
            super::protocol::method::FILE_REQUEST_APPROVAL => {
                serde_json::json!({ "decision": "accept" })
            }
            // item/permissions/requestApproval
            // Schema: PermissionsRequestApprovalResponse.json — required: ["permissions"]
            // GrantedPermissionProfile: { fileSystem?: AdditionalFileSystemPermissions|null, network?: AdditionalNetworkPermissions|null }
            // Both fields are nullable so {} is a valid minimal grant profile.
            super::protocol::method::PERMISSIONS_REQUEST_APPROVAL => {
                serde_json::json!({ "permissions": {} })
            }
            // item/tool/requestUserInput
            // Schema: ToolRequestUserInputResponse.json — required: ["answers"]
            // answers: { [id: string]: ToolRequestUserInputAnswer } — empty map is valid.
            super::protocol::method::TOOL_REQUEST_USER_INPUT => {
                serde_json::json!({ "answers": {} })
            }
            // Unknown approval method — return empty result to avoid hanging codex.
            _ => serde_json::json!({}),
        }
    }
}
