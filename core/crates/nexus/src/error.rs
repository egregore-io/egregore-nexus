//! Error helpers for the `nexus` binary.
//!
//! The daemon never invents its own error taxonomy: every fallible path returns either a
//! [`nexus_common::NexusError`] (from the services) or a [`nexus_contracts::ContractError`] (from
//! the ports). Both map onto a single JSON-RPC wire shape - a [`nexus_contracts::RpcError`] for the
//! gateway/agent-session edge - via [`contract_to_rpc`].

use nexus_contracts::{codes, ContractError, RpcError};

/// Map a [`ContractError`] onto the JSON-RPC [`RpcError`] (code copied straight through).
pub fn contract_to_rpc(e: &ContractError) -> RpcError {
    RpcError {
        code: e.code,
        message: e.message.clone(),
        data: None,
    }
}

/// Build an `RpcError` from a raw code + message (used for dispatch-level failures such as
/// unknown-method / bad-params that never reach a port).
pub fn rpc_error(code: i32, message: impl Into<String>) -> RpcError {
    RpcError {
        code,
        message: message.into(),
        data: None,
    }
}

/// Convenience: the standard "method not found" error for an unknown registry key.
pub fn method_not_found(method: &str) -> RpcError {
    rpc_error(
        codes::METHOD_NOT_FOUND,
        format!("method not found: {method}"),
    )
}

/// Convenience: the standard "invalid params" error when `params` fails to deserialize.
pub fn invalid_params(detail: impl std::fmt::Display) -> RpcError {
    rpc_error(codes::INVALID_PARAMS, format!("invalid params: {detail}"))
}

/// Convenience: the "unauthorized" application error (e.g. an unauthenticated authenticated-only
/// method, or an agent calling an admin method).
pub fn unauthorized(detail: impl Into<String>) -> RpcError {
    rpc_error(codes::UNAUTHORIZED, detail.into())
}
