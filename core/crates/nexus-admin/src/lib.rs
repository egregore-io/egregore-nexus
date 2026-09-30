//! # nexus-admin
//!
//! The guardrailed admin command set (backend spec §8): `impl AdminPort` where **every op is
//! `tier_guard(caller, Tier::Admin)` then delegate** to the relevant capability/domain port. Admin
//! adds **extra commands only — never insertion into the message path** (the non-negotiable §8
//! guardrail; anchor §5: this is what stops the daemon regressing into a passive orchestrator).
//!
//! - [`Admin`] (in [`service`]) — the `Arc<dyn AdminPort>` service, wired from port seams by the
//!   `nexus` binary's `AppState`.
//! - Per-op modules ([`spawn`], [`remove`], [`roles`], [`channel`], [`route`], [`monitor`]) — each a
//!   `guard + delegate` function.
//!
//! **The crate exposes no `send`/`enqueue`/message-write surface** — that is enforced structurally:
//! [`AdminPort`](nexus_contracts::ports::AdminPort) has no such method, and [`Admin`] holds no store
//! or realtime/bus handle. The only path-adjacent op, [`route`], goes through
//! [`NotifyPort::forward`](nexus_contracts::ports::NotifyPort::forward) (a one-shot forward of an
//! already-ingested notification), never a direct message write.

use nexus_contracts::enums::Tier;
use nexus_contracts::ports::{Caller, ContractError};

pub mod channel;
pub mod error;
pub mod monitor;
pub mod project;
pub mod remove;
pub mod roles;
pub mod route;
pub mod service;
pub mod spawn;

pub use service::Admin;

/// The single authorization primitive every admin op wraps. Delegates to
/// [`nexus_identity::tier_guard`] and maps its [`NexusError`](nexus_common::NexusError) into the
/// port-facing [`ContractError`] (so a non-admin caller surfaces `Unauthorized` on the wire).
pub(crate) fn guard(caller: &Caller, required: Tier) -> Result<(), ContractError> {
    nexus_identity::tier_guard(caller, required).map_err(|e| e.to_contract_error())
}
