//! `nexus-agent`'s local error and its conversion into the workspace [`NexusError`].

use nexus_common::NexusError;

/// Errors raised inside the agent-transport crate. Converts into [`NexusError`] for the port seam.
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    /// No adapter is registered for the requested harness.
    #[error("no adapter registered for harness: {0}")]
    NoAdapter(String),

    /// The named session is not registered (launch/attach first).
    #[error("session not registered: {0}")]
    NoSession(String),

    /// An adapter operation (open/resume/inject/stream) failed.
    #[error("adapter error: {0}")]
    Adapter(String),
}

impl From<AgentError> for NexusError {
    fn from(e: AgentError) -> Self {
        match e {
            AgentError::NoAdapter(m) | AgentError::NoSession(m) => NexusError::NotFound(m),
            AgentError::Adapter(m) => NexusError::Adapter(m),
        }
    }
}
