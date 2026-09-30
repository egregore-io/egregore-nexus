//! Hermes harness adapter — self-contained module (mirrors the `claude/`, `codex/` and `opencode/`
//! templates).
//!
//! Public surface is identical to the old flat `adapter/hermes.rs`:
//! - `HermesAdapter` — the adapter struct
//! - `hermes_command` — the command resolver

pub mod harness;
pub mod native;
pub mod skill;

pub use harness::hermes_command;
pub use harness::HermesAdapter;
