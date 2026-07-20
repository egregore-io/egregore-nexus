//! # nexus-store
//!
//! The durable libSQL store for Nexus (foundation layer, backend spec §4). The daemon is the
//! **sole writer**; the gateway holds a read-only Drizzle view. This crate owns the schema +
//! migrations and exposes the per-table repositories every higher crate consumes through them.
//!
//! - [`Store`] — the libSQL connection handle ([`Store::open`] + idempotent [`Store::migrate`]).
//! - [`repos`] — the project-scoped repositories: sessions, messages, the `in_flight` delivery
//!   state machine, threads, topics, notifications, and materialized agent-session history.
//! - [`search_index`] — FTS5 helpers with a caller-supplied `scope_sql` `WHERE` fragment so
//!   `nexus-search` can inject the per-agent scope. Semantic vectors are plugin-owned.

pub mod command_kinds;
pub mod daemon_store;
mod error;
pub mod events;
pub mod migrate;
pub mod repos;
pub mod search_index;
pub mod state;
pub mod types;

pub use daemon_store::DaemonStore;
#[doc(hidden)]
pub use migrate::{migrate_identity_with_fault, MigrationFault};
pub use search_index::SearchHit;
pub use state::{Store, StoreLocation, WriteTxn};
