//! Versioned local IPC envelopes between Nexus producers and the daemon.
//!
//! Public CLI, REST, MCP, and WebSocket payloads keep their existing contracts. These frames are
//! the process boundary that lets the daemon own the embedded libSQL store exclusively. A command
//! frame carries the durable `command_intents` identity; a query frame is routed directly by the
//! daemon and cannot smuggle command-ledger fields.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::{Kind, RpcError, Tier};

/// Initial daemon IPC protocol version.
pub const DAEMON_IPC_PROTOCOL_VERSION: u32 = 1;

/// Caller evidence forwarded by a trusted local producer surface.
///
/// The daemon canonicalizes registered callers from `client_key`; it never treats the optional
/// display name or claimed tier as proof of a registered identity. An env-less local CLI uses the
/// daemon's explicit local-operator sentinel.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonIpcCaller {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Descriptive routing scope retained for wire compatibility. It is metadata, not an IPC or
    /// store-partition key.
    pub project: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key: Option<String>,
    pub kind: Kind,
    pub tier: Tier,
}

/// One operation carried over daemon IPC.
#[typeshare(serialized_as = "DaemonIpcCallWire")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(
    tag = "mode",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DaemonIpcCall {
    /// Insert (or resume) one durable command-intent row and hold the IPC response until the daemon
    /// worker records a terminal result.
    Command {
        command_id: String,
        kind: String,
        params: serde_json::Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// Durably insert (or resume) one command and return its queue receipt immediately. This is
    /// the daemon-owned acceptance boundary used by UI queue surfaces; execution and terminal
    /// settlement remain visible through command events rather than a producer-side DB read.
    Enqueue {
        command_id: String,
        kind: String,
        params: serde_json::Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        idempotency_key: Option<String>,
    },
    /// Execute a read-shaped method through the daemon's typed request router. Query calls never
    /// insert command-intent rows.
    Query {
        method: String,
        params: serde_json::Value,
    },
}

/// One bounded request frame sent to the daemon-owned local endpoint.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonIpcRequest {
    pub version: u32,
    /// Boot-scoped bearer from the mode-0600 endpoint manifest. This rejects stale endpoints and
    /// accidental cross-instance connections; surface authentication still belongs to CLI/gateway.
    pub token: String,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller: Option<DaemonIpcCaller>,
    pub call: DaemonIpcCall,
}

/// One response frame. Exactly one of `result` and `error` is present.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonIpcResponse {
    pub version: u32,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl DaemonIpcResponse {
    pub fn success(request_id: impl Into<String>, result: serde_json::Value) -> Self {
        Self {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            request_id: request_id.into(),
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(request_id: impl Into<String>, error: RpcError) -> Self {
        Self {
            version: DAEMON_IPC_PROTOCOL_VERSION,
            request_id: request_id.into(),
            result: None,
            error: Some(error),
        }
    }
}
