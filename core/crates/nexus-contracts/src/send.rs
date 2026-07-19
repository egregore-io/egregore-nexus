//! Send — the one `to` contract (CLI §4, backend §3). A single `SendRequest` envelope whose
//! `to` is a tagged verb: dm a name, post to a thread, publish to a topic, or reply-in-context.
//! Retry-prone clients may also carry a producer idempotency key; the daemon returns the original
//! message id for repeats instead of inserting another Message Post row.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::ids::{AgentId, MessageId};
use crate::ports::ContractError;
use crate::rpc::codes;

/// The send target/verb. `dm`/`post`/`publish` carry their name; `reply` targets the
/// conversation the current injected turn arrived from (no target needed — CLI §4).
///
/// This is an internally-tagged (`verb`) serde union. typeshare 1.13 cannot emit
/// internally-tagged algebraic enums (it only supports adjacently-tagged), so we map it via
/// `serialized_as` to the hand-authored TS union `SendTargetWire` (appended to the generated
/// mirror by the gen step). TS-only annotation — the serde wire shape is unchanged and the inline
/// round-trip tests assert the internally-tagged JSON.
#[typeshare(serialized_as = "SendTargetWire")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "verb", rename_all = "camelCase")]
pub enum SendTarget {
    /// Private 2-party DM. Stable `agent_id` is authoritative when present; `name` remains
    /// optional display metadata and the legacy addressing fallback.
    Dm {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, rename = "agentId", skip_serializing_if = "Option::is_none")]
        agent_id: Option<AgentId>,
    },
    /// Post to a named thread → fan-out to members.
    Post { thread: String },
    /// Publish to a topic (pub/sub).
    Publish { topic: String },
    /// Context-aware reply into the current turn's conversation.
    Reply,
}

impl SendTarget {
    /// Build the legacy/default named-DM shape. The wire keeps `name` and omits `agentId` until a
    /// caller has resolved stable identity.
    pub fn dm_name(name: impl Into<String>) -> Self {
        Self::Dm {
            name: Some(name.into()),
            agent_id: None,
        }
    }

    /// Build a canonical identity-addressed DM while retaining optional display metadata.
    pub fn dm_agent(agent_id: AgentId, name: Option<String>) -> Self {
        Self::Dm {
            name,
            agent_id: Some(agent_id),
        }
    }
}

/// A send request. `summary` is the optional short index line; `mention` is a soft highlight
/// inside a thread (not a routing change). `idempotency_key` is scoped by the daemon to the
/// sender session so a retry returns the original [`Ack`] without creating another message.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SendRequest {
    pub to: SendTarget,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub body: String,
    #[serde(default)]
    pub mention: Vec<String>,
    /// Developer-owned metadata that is committed atomically with the message body.
    #[typeshare(serialized_as = "Option<Record<string, unknown>>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// Public send acknowledgement. Routing and recipient counts stay internal; callers receive only
/// the durable message identity they can use for reads, receipts, and diagnostics.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Ack {
    pub message_id: MessageId,
    /// Internal recipient-row count used by the bus and source delivery service. This is
    /// intentionally absent from every public/model-facing envelope.
    #[serde(default, skip_serializing)]
    #[typeshare(skip)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fanout: Option<u32>,
}

pub const EMPTY_SEND_BODY_MESSAGE: &str =
    "message body must not be empty — pass -m <text> or --stdin";

pub fn validate_send_body(body: &str) -> Result<(), ContractError> {
    if body.trim().is_empty() {
        return Err(ContractError {
            code: codes::INVALID_PARAMS,
            message: EMPTY_SEND_BODY_MESSAGE.into(),
        });
    }
    Ok(())
}

pub fn validate_send_request(req: &SendRequest) -> Result<(), ContractError> {
    validate_send_body(&req.body)?;
    if let SendTarget::Dm { name, agent_id } = &req.to {
        let has_name = name
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
        let has_agent_id = agent_id
            .as_ref()
            .is_some_and(|value| !value.0.trim().is_empty());
        if !has_name && !has_agent_id {
            return Err(ContractError {
                code: codes::INVALID_PARAMS,
                message: "dm target requires name or agentId".into(),
            });
        }
    }
    Ok(())
}
