//! Identity-crate error helpers. The crate works in terms of the workspace-wide
//! [`nexus_common::NexusError`] internally and maps it to the wire-facing
//! [`nexus_contracts::ContractError`] only at the `IdentityPort` boundary (every port method
//! returns [`nexus_contracts::PortResult`]).

use nexus_common::NexusError;
use nexus_contracts::ports::PortResult;

/// Lift an internal `Result<T, NexusError>` into a port-facing [`PortResult`], converting the
/// error to its `ContractError` wire form (code from `nexus_contracts::codes`).
pub(crate) fn to_port<T>(r: Result<T, NexusError>) -> PortResult<T> {
    r.map_err(|e| e.to_contract_error())
}
