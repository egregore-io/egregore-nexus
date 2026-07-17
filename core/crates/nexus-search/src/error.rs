//! Crate-local error for `nexus-search`. Search is thin over `nexus-store`'s search-index helpers,
//! so most failures are already `NexusError`s bubbling up from the store; this enum exists for the
//! crate's own validation surface and converts both ways so the daemon's edge can map it to an
//! `RpcError` (via [`nexus_common::NexusError`]).

use nexus_common::NexusError;

/// Errors raised by the search service itself (store errors arrive pre-wrapped as [`NexusError`]).
#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    /// A request was malformed (e.g. an empty FTS query).
    #[error("invalid search request: {0}")]
    Invalid(String),
    /// Underlying store / query failure.
    #[error(transparent)]
    Store(#[from] NexusError),
}

impl From<SearchError> for NexusError {
    fn from(e: SearchError) -> Self {
        match e {
            SearchError::Invalid(m) => NexusError::Invalid(m),
            SearchError::Store(e) => e,
        }
    }
}
