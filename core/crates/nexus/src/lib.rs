//! # nexus — the daemon side of the one `nexus` binary
//!
//! This crate is the **composition layer**: the only crate allowed to depend on every other
//! `nexus-*` crate. It owns [`daemon::AppState`] (the single wiring point - every service behind its
//! `*Port` trait, one shared [`nexus_store::Store`], and the internal
//! [`nexus_contracts::EventSink`]), the command-intent worker, and the transport-agnostic method
//! router [`daemon::routing`].
//!
//! - store-backed command intents for local CLI/MCP/gateway writes and control operations;
//! - store-backed read views for local CLI/MCP/gateway reads;
//!
//! The command worker reuses [`daemon::routing::route_request`], so a method and its auth/error mapping are
//! defined exactly once while ingress stays store-backed.

pub mod cli;
pub mod daemon;
pub mod error;
pub mod first_run;
pub mod gateway_lifecycle;
pub mod gateway_service;
pub mod harness_registry;
pub mod initial_prompt;
pub(crate) mod lifecycle_process;
pub(crate) mod local_operator;
pub mod names;
pub mod spawn_spec;
pub mod update;
pub mod webconsole_lifecycle;
pub mod webconsole_service;

pub use daemon::{dispatch, AppState, WsSink};
