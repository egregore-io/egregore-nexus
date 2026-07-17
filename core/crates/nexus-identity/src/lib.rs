//! # nexus-identity
//!
//! Register-once identity for Nexus (backend spec §5.1, §8, §2.4). Implements the
//! [`nexus_contracts::ports::IdentityPort`] over the `nexus-store` `Sessions` repo:
//!
//! - **Register-once** ([`registry`]): idempotent on `client_key` (known key → *resume* the same
//!   session, rebinding a possibly-new `harness_session_id`; unknown key with a *live* name →
//!   [`nexus_common::NexusError::DuplicateName`]; else *create* + bind `name ↔ harness_session_id`).
//!   First create emits `agent.spawned` via the [`nexus_contracts::ports::EventSink`].
//! - **Binding** ([`binding`]): enum↔store-string mapping and `Caller`/`Whoami` assembly.
//! - **Presence** ([`presence`]): `StatusState`→`Presence`, heartbeat staleness → `offline`.
//! - **Tiers** ([`tier`]): [`tier::tier_guard`] — `Unauthorized` when `caller.tier < required`.
//! - **Service** ([`service`]): the [`service::Identity`] wiring all of the above into the port.

mod binding;
mod error;
pub mod presence;
pub mod registry;
pub mod service;
pub mod tier;

pub use service::Identity;
pub use tier::tier_guard;
