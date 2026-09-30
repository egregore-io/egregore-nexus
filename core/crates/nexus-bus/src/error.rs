//! Bus error helpers. The bus has no error type of its own — it works in terms of
//! [`nexus_common::NexusError`] internally and maps to [`ContractError`] at the [`BusPort`]
//! boundary (backend §11: resolution errors are explicit, never a silent drop or a guess).
//!
//! [`BusPort`]: nexus_contracts::ports::BusPort
//! [`ContractError`]: nexus_contracts::ports::ContractError

use nexus_common::NexusError;
use nexus_contracts::ports::PortResult;

/// Map an internal [`NexusError`] result into a wire-facing [`PortResult`].
pub(crate) fn to_port<T>(r: Result<T, NexusError>) -> PortResult<T> {
    r.map_err(|e| e.to_contract_error())
}
