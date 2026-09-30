//! Error helpers for the dispatch crate. Everything funnels through the workspace-wide
//! [`nexus_common::NexusError`]; the daemon's edge maps that onto a wire `ContractError`.

use nexus_common::NexusError;

/// Convenience alias for delivery-dispatch operations.
pub type DispatchResult<T> = Result<T, NexusError>;
