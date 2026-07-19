//! Language-neutral message-hook wire contracts.
//!
//! Gateway evaluates these contracts only at canonical Message Post boundaries. Agent-session
//! updates and token streams deliberately do not use them.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::ids::AgentId;
use crate::send::{Ack, SendTarget};

/// Public delivery timing selected by a `before_send` hook pipeline.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryTiming {
    Interrupt,
    YieldTurn,
    AfterToolLoop,
}

impl Default for DeliveryTiming {
    fn default() -> Self {
        Self::Interrupt
    }
}

impl DeliveryTiming {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Interrupt => "interrupt",
            Self::YieldTurn => "yield_turn",
            Self::AfterToolLoop => "after_tool_loop",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "interrupt" => Some(Self::Interrupt),
            "yield_turn" => Some(Self::YieldTurn),
            "after_tool_loop" => Some(Self::AfterToolLoop),
            _ => None,
        }
    }
}

/// Terminal pipeline decision. Omitted hook output defaults to continuing the send.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HookAction {
    #[default]
    Continue,
    Reject,
}

/// Authenticated sender identity visible to a hook but immutable in hook output.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HookSender {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub name: String,
}

/// Canonical message document passed through the hook pipeline.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HookMessage {
    pub sender: HookSender,
    pub target: SendTarget,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default)]
    pub mention: Vec<String>,
    #[typeshare(serialized_as = "Record<string, unknown>")]
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

/// Compact, signed execution provenance attached by Gateway after validating hook output.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HookExecutedBy {
    pub hook_id: String,
    pub entrypoint: String,
    pub runtime: String,
    pub artifact_digest: String,
    pub invocation_id: String,
    pub outcome: String,
    pub attestation: serde_json::Value,
}

/// Correlated request from the daemon for a new logical send.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HookBeforeSendRequest {
    pub evaluation_id: String,
    pub message: HookMessage,
}

/// Final Gateway result for one `before_send` evaluation.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HookBeforeSendResult {
    pub evaluation_id: String,
    #[serde(default)]
    pub action: HookAction,
    pub message: HookMessage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing: Option<DeliveryTiming>,
    #[serde(default)]
    pub executed_by: Vec<HookExecutedBy>,
}

/// Gateway-local invocation after the canonical send receipt exists.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HookAfterReceiptRequest {
    pub invocation_id: String,
    pub message: HookMessage,
    pub receipt: Ack,
    #[serde(default)]
    pub executed_by: Vec<HookExecutedBy>,
}

/// Allowed `after_receipt` output. Message content and timing are immutable at this boundary.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HookAfterReceiptResult {
    pub invocation_id: String,
    #[typeshare(serialized_as = "Option<Record<string, unknown>>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    pub executed_by: Vec<HookExecutedBy>,
}

/// Hook capability advertised by the local Gateway during stream negotiation.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayHookCapabilities {
    pub protocol_version: u32,
    pub generation: String,
    pub events: Vec<String>,
}

/// Event-specific payload carried by one correlated daemon-to-Gateway evaluation.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "event", content = "request", rename_all = "snake_case")]
pub enum HookEvaluationRequest {
    BeforeSend(HookBeforeSendRequest),
    AfterReceipt(HookAfterReceiptRequest),
}

impl HookEvaluationRequest {
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::BeforeSend(_) => "before_send",
            Self::AfterReceipt(_) => "after_receipt",
        }
    }
}

/// Event-specific terminal result returned by Gateway.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "event", content = "result", rename_all = "snake_case")]
pub enum HookEvaluationResponse {
    BeforeSend(HookBeforeSendResult),
    AfterReceipt(HookAfterReceiptResult),
}

/// One daemon request on the private correlated hook lane.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayHookEvaluation {
    pub correlation_id: String,
    pub request: HookEvaluationRequest,
}

/// Structured Gateway-side failure for one correlation ID.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HookEvaluationFailure {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
}
