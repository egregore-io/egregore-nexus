//! # Internal RPC-shaped command envelope.
//!
//! Nexus still defines a JSON-RPC-shaped envelope (same family as ACP/LSP) for command-intent
//! request/response rows and internal notifications. This module owns the reusable method-agnostic
//! envelope — `Request`, `Response`, `Notification`, `RpcError` — plus the error `codes`.
//!
//! CLI/MCP/gateway writes use store-backed command intents. The command rows store the same DTO
//! params/results as JSON values, then the daemon command worker routes them through the same method
//! registry.
//!
//! Per the JSON-RPC standard the envelope's `params`/`result` are arbitrary JSON
//! (`serde_json::Value`). The concrete type of each method's `params`/`result` is pinned by the
//! **method registry**: typed dispatch
//! reads `method`, then deserializes `params` into that method's request type (e.g. `"send"` →
//! [`crate::send::SendRequest`]) and serializes the handler's result type back into `result`.
//!
//! Internal event-shaped rows may use **notifications** ([`Notification`]: a request shape with no
//! `id`) whose `params` carry a [`crate::events::WsEvent`]; the notification `method` is the dotted
//! event name (e.g. `"message.created"`).

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// The JSON-RPC-compatible protocol version string carried in every envelope.
pub const JSONRPC_VERSION: &str = "2.0";

/// A command-envelope request id — a number or a string (the two id forms JSON-RPC allows).
/// `#[serde(untagged)]` so it serializes as a bare `7` or `"abc"` on the wire.
///
/// `serialized_as` maps the untagged enum to a TS union for the generated mirror — typeshare 1.13
/// cannot infer untagged-enum unions on its own and rejects the bare `i64`. This is a TS-only
/// annotation; the serde wire shape (bare number or bare string) is unchanged.
#[typeshare(serialized_as = "RequestIdWire")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum RequestId {
    Num(i64),
    Str(String),
}

/// A command-envelope request. `params` is method-specific (see the registry); deserialize it into the
/// registry's concrete type after reading `method`. An absent `id` means **notification
/// semantics on the request side** (a fire-and-forget call expecting no response) — for the
/// internal push/event shape use [`Notification`].
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Request {
    /// Always `"2.0"` ([`JSONRPC_VERSION`]).
    pub jsonrpc: String,
    /// Correlates the response. Absent = notification semantics (no response expected).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    /// The method name (a key in the Nexus method registry).
    pub method: String,
    /// Method params — deserialize into the registry type for `method`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

/// A command-envelope notification: the internal event shape (no `id`). `params` carries a
/// [`crate::events::WsEvent`]; `method` is the dotted event name (e.g. `"message.created"`).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Notification {
    /// Always `"2.0"` ([`JSONRPC_VERSION`]).
    pub jsonrpc: String,
    /// The dotted event name (the `WsEvent` `type`).
    pub method: String,
    /// The event body — deserialize into the matching `WsEvent` variant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

/// A command-envelope response. **Exactly one** of `result` / `error` is set; the unused one is
/// omitted from the wire via `skip_serializing_if`. `result` is the registry's result type for the
/// request's `method`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Response {
    /// Always `"2.0"` ([`JSONRPC_VERSION`]).
    pub jsonrpc: String,
    /// Echoes the request id (`None` only for a response to an id-less request — rare).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    /// The success payload — the registry result type for the method. Mutually exclusive with `error`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// The failure object. Mutually exclusive with `result`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

/// A command-envelope error object. `code` is one of [`codes`]; `data` is optional structured detail.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// JSON-RPC-compatible error codes. The `-32700..-32600` range follows the JSON-RPC standard;
/// `-32001..-32099` is the server-reserved "implementation-defined" range we use for Nexus
/// application errors.
pub mod codes {
    // --- Standard JSON-RPC 2.0 codes ---
    /// Invalid JSON was received (parse error).
    pub const PARSE_ERROR: i32 = -32700;
    /// The JSON is not a valid Request object.
    pub const INVALID_REQUEST: i32 = -32600;
    /// The method does not exist in the registry.
    pub const METHOD_NOT_FOUND: i32 = -32601;
    /// `params` did not deserialize into the registry type for the method.
    pub const INVALID_PARAMS: i32 = -32602;
    /// Internal server error.
    pub const INTERNAL_ERROR: i32 = -32603;

    // --- Nexus application codes (server-reserved -32001..-32099) ---
    /// Caller is not authorized for this method (e.g. non-admin calling an admin method).
    pub const UNAUTHORIZED: i32 = -32001;
    /// A name (agent/thread/topic/project) is already bound.
    pub const DUPLICATE_NAME: i32 = -32002;
    /// The named entity (agent/thread/topic/message) was not found.
    pub const NOT_FOUND: i32 = -32003;
    /// The request reached outside the caller's project scope.
    pub const PROJECT_SCOPE_VIOLATION: i32 = -32004;
    /// The target session is paused (self-hold) — the message path is closed.
    pub const PAUSED: i32 = -32005;
    /// The requested active-turn operation raced with completion or no turn is active.
    pub const ACTIVE_TURN_REQUIRED: i32 = -32006;
}
