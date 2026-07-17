//! Store error mapping. libSQL errors are opaque strings to the rest of the workspace; every
//! one funnels into [`nexus_common::NexusError::Store`] so the edge can map it to an `RpcError`.

use nexus_common::NexusError;

/// Map a `libsql::Error` into the workspace error as a [`NexusError::Store`].
pub(crate) fn store_err(e: libsql::Error) -> NexusError {
    NexusError::Store(e.to_string())
}

/// Map any error displayable as a string into a [`NexusError::Store`].
pub(crate) fn store_msg(msg: impl std::fmt::Display) -> NexusError {
    NexusError::Store(msg.to_string())
}
