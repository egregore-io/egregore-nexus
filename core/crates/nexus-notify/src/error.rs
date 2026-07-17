//! Notify error helpers. Like the bus, this crate has no error type of its own — it works in
//! terms of [`nexus_common::NexusError`] internally and maps to [`ContractError`] at the
//! [`NotifyPort`] boundary (backend §11: explicit errors, never a silent drop or a guess).
//!
//! Note: a *bad signature* is **not** an error — it is recorded (`hmac_ok=false`) and dropped,
//! returning a successful [`NotifyResponse`] with an empty `routed_to`. Errors here are real
//! failures (store, scope, the admin-tier gate on `forward`).
//!
//! [`NotifyPort`]: nexus_contracts::ports::NotifyPort
//! [`ContractError`]: nexus_contracts::ports::ContractError
//! [`NotifyResponse`]: nexus_contracts::notify::NotifyResponse

use nexus_common::NexusError;
use nexus_contracts::ports::PortResult;

/// Map an internal [`NexusError`] result into a wire-facing [`PortResult`].
pub(crate) fn to_port<T>(r: Result<T, NexusError>) -> PortResult<T> {
    r.map_err(|e| e.to_contract_error())
}
