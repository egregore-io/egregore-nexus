//! The daemon side of the `nexus` binary: [`AppState`] wiring, the store-backed command worker, the
//! shared request router [`routing`], and local harness process management.
//!
//! [`routing`]: routing::route_request
//! [`AppState`]: app::AppState

pub mod agent_session_materializer;
pub mod app;
pub mod claude_native_forwarder;
pub mod claude_resume_harvest;
pub mod command_worker;
pub mod daemon_ipc;
pub mod gateway_hook_bridge;
pub mod gateway_projection_backlog;
pub mod gateway_stream_socket;
pub mod harness_launch;
pub mod hermes_gateway;
pub mod hermes_native_forwarder;
pub mod lifecycle;
pub mod native_forwarder;
pub mod opencode_native_forwarder;
pub mod opencode_plugin_bridge;
pub mod process_guard;
pub mod process_ledger;
pub mod pty_reply_reader;
pub mod pty_supervisor;
pub mod pty_transport;
pub mod retention_policy;
pub mod routing;
pub mod routing_turn_exec;
pub(crate) mod runtime_revive_gate;
pub mod services;
pub mod slash_commands;
mod source_wake;
pub mod stream_raw_writer;
pub mod terminal_socket;
pub mod transcript_archive;

pub use app::{AppState, WsSink};
// Keep the pre-v0.1 module/function paths source-compatible while routing internals use the
// unambiguous `routing::route_request` names.
pub use routing as dispatch;
pub use routing::{dispatch, route_request};
