//! Admin crate error surface. Admin adds no error semantics of its own — every fallible op either
//! fails the tier guard ([`NexusError::Unauthorized`]) or surfaces the delegated port's
//! [`ContractError`]. This module documents that mapping; the crate returns
//! `Result<T, ContractError>` (the port-trait contract) directly.

pub use nexus_common::NexusError;
pub use nexus_contracts::ports::ContractError;
