//! One module per cli-spec command group. Each exposes its clap `Args`/`Cmd` types and async
//! handlers `(client, args, json) -> ExitCode`.
//!
//! The contract enums (`StatusState`, `ChannelOp`, …) are serde enums in `nexus-contracts`
//! and deliberately do **not** derive clap's `ValueEnum` (the contracts crate has no clap
//! dependency). The small [`parse`] helpers here bridge a CLI token to the contract type, matching
//! each type's serde lowercase wire token so the CLI and wire never drift. Harness tokens are the
//! open-set [`nexus_contracts::HarnessId`] — any lowercase identifier a registered harness answers
//! to, not a closed list.

pub mod admin;
pub mod agents;
pub mod attach;
pub mod discover;
pub mod gateway;
pub mod lifecycle;
pub mod listen;
pub mod mcp;
pub mod memory;
pub mod notify;
pub mod presence;
pub mod pty_attach;
pub mod resume;
pub mod send;
pub mod source;
pub mod terminal_client;
pub mod threads;
pub mod update;
pub mod webconsole;

/// clap value-parsers for the contract enums (kept out of `nexus-contracts`, which has no clap dep).
pub mod parse {
    use nexus_contracts::{AgentAccessRole, ChannelOp, HarnessId, Kind, StatusState, Tier};

    /// Parse a harness/runtime token (open set — e.g. `claude`, `codex`, `opencode`, `hermes`).
    pub fn harness(s: &str) -> Result<HarnessId, String> {
        HarnessId::new(s.to_ascii_lowercase()).map_err(|e| e.to_string())
    }

    /// Parse a session kind token (`agent`|`app`).
    pub fn kind(s: &str) -> Result<Kind, String> {
        match s.to_ascii_lowercase().as_str() {
            "agent" => Ok(Kind::Agent),
            "app" => Ok(Kind::App),
            "human" => Ok(Kind::Human),
            "notification" => Ok(Kind::Notification),
            other => Err(format!("unknown kind '{other}' (agent|app)")),
        }
    }

    /// Parse a self-status token (`active`|`busy`|`paused`).
    pub fn status_state(s: &str) -> Result<StatusState, String> {
        match s.to_ascii_lowercase().as_str() {
            "active" => Ok(StatusState::Active),
            "busy" => Ok(StatusState::Busy),
            "paused" => Ok(StatusState::Paused),
            other => Err(format!("unknown status '{other}' (active|busy|paused)")),
        }
    }

    /// Parse a privilege tier token (`agent`|`admin`).
    pub fn tier(s: &str) -> Result<Tier, String> {
        match s.to_ascii_lowercase().as_str() {
            "agent" => Ok(Tier::Agent),
            "admin" => Ok(Tier::Admin),
            other => Err(format!("unknown tier '{other}' (agent|admin)")),
        }
    }

    /// Parse an agent access role token (`viewer`|`co_owner`).
    pub fn agent_access_role(s: &str) -> Result<AgentAccessRole, String> {
        match s.to_ascii_lowercase().replace('-', "_").as_str() {
            "viewer" => Ok(AgentAccessRole::Viewer),
            "co_owner" | "coowner" => Ok(AgentAccessRole::CoOwner),
            other => Err(format!("unknown access role '{other}' (viewer|co_owner)")),
        }
    }

    /// Parse a channel op token (`create`|`delete`|`set-route`/`setRoute`).
    pub fn channel_op(s: &str) -> Result<ChannelOp, String> {
        match s.to_ascii_lowercase().as_str() {
            "create" => Ok(ChannelOp::Create),
            "delete" => Ok(ChannelOp::Delete),
            "set-route" | "setroute" => Ok(ChannelOp::SetRoute),
            other => Err(format!(
                "unknown channel op '{other}' (create|delete|set-route)"
            )),
        }
    }
}
