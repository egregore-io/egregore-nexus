//! The workspace-wide error type and its bidirectional mapping to the wire-facing
//! [`nexus_contracts::ContractError`] (which carries a [`nexus_contracts::codes`] value).

use nexus_contracts::{codes, ContractError};

/// The workspace-wide error. Every crate's local error converts into this; the JSON-RPC edge
/// converts it into an `RpcError` via `to_contract_error`, and the CLI maps it back to a message.
#[derive(Debug, thiserror::Error)]
pub enum NexusError {
    #[error("name already bound: {0}")]
    DuplicateName(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("project scope violation")]
    ProjectScope,
    #[error("target is paused")]
    Paused,
    #[error("message policy denied: {0}")]
    PolicyDenied(String),
    #[error("ambiguous name: {0}")]
    Ambiguous(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("store error: {0}")]
    Store(String),
    #[error("adapter error: {0}")]
    Adapter(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl NexusError {
    /// Map to the wire-facing error (code from `nexus_contracts::codes`).
    pub fn to_contract_error(&self) -> ContractError {
        let code = match self {
            NexusError::DuplicateName(_) => codes::DUPLICATE_NAME,
            NexusError::NotFound(_) => codes::NOT_FOUND,
            NexusError::Unauthorized => codes::UNAUTHORIZED,
            NexusError::ProjectScope => codes::PROJECT_SCOPE_VIOLATION,
            NexusError::Paused => codes::PAUSED,
            NexusError::PolicyDenied(_) => codes::UNAUTHORIZED,
            NexusError::Invalid(_) | NexusError::Ambiguous(_) => codes::INVALID_PARAMS,
            _ => codes::INTERNAL_ERROR,
        };
        ContractError {
            code,
            message: self.to_string(),
        }
    }
}

impl From<ContractError> for NexusError {
    fn from(e: ContractError) -> Self {
        match e.code {
            codes::DUPLICATE_NAME => NexusError::DuplicateName(e.message),
            codes::NOT_FOUND => NexusError::NotFound(e.message),
            codes::UNAUTHORIZED => NexusError::Unauthorized,
            codes::PROJECT_SCOPE_VIOLATION => NexusError::ProjectScope,
            codes::PAUSED => NexusError::Paused,
            _ => NexusError::Internal(e.message),
        }
    }
}
impl From<NexusError> for ContractError {
    fn from(e: NexusError) -> Self {
        e.to_contract_error()
    }
}
