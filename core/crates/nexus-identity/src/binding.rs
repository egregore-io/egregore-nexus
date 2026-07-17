//! Name↔session binding helpers (backend spec §5.1 req #9): translate between the contract DTOs
//! (`Harness`/`Tier`/`Kind`, `Caller`, `Whoami`) and the store's stringly-typed [`SessionRow`].
//! `register` binds `name ↔ harness_session_id` by persisting both on the same row; these helpers
//! own the enum↔string mapping so the service layer never hand-stringifies.

use nexus_common::NexusError;
use nexus_contracts::enums::{Harness, Kind, Tier};
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::Caller;
use nexus_contracts::register::Whoami;
use nexus_store::types::SessionRow;

use crate::presence::presence_from_str;

/// The harness/runtime label as it is stored on `sessions.agent` (display/addressing only).
pub(crate) fn harness_str(h: Harness) -> &'static str {
    match h {
        Harness::Claude => "claude",
        Harness::Codex => "codex",
        Harness::OpenCode => "opencode",
        Harness::Hermes => "hermes",
        Harness::Pi => "pi",
        Harness::Other => "other",
    }
}

/// The tier token stored on `sessions.tier`.
pub(crate) fn tier_str(t: Tier) -> &'static str {
    match t {
        Tier::Agent => "agent",
        Tier::Admin => "admin",
    }
}

/// Parse a stored tier string back into [`Tier`]. Unknown → `Agent` (least privilege).
pub(crate) fn tier_from_str(s: &str) -> Tier {
    match s {
        "admin" => Tier::Admin,
        _ => Tier::Agent,
    }
}

/// The kind token stored on `sessions.kind`. Defaults to `agent` when the request omits it
/// (matching the contract: `kind` defaults to agent server-side).
pub(crate) fn kind_str(k: Option<Kind>) -> &'static str {
    match k.unwrap_or(Kind::Agent) {
        Kind::Agent => "agent",
        Kind::Human => "human",
        Kind::Notification => "notification",
        Kind::App => "app",
    }
}

/// Resolve a persisted [`SessionRow`] into the caller's authenticated [`Caller`] (the identity the
/// daemon threads into every later port call — never a client-supplied `from`).
pub(crate) fn caller_from_row(row: &SessionRow) -> Caller {
    Caller {
        agent_id: None,
        session: SessionId(row.session_id.0.clone()),
        name: row.display_name(),
        project: row.project.clone(),
        tier: tier_from_str(&row.tier),
    }
}

/// Assemble the `whoami` DTO from a persisted row.
pub(crate) fn whoami_from_row(row: &SessionRow) -> Whoami {
    Whoami {
        agent_id: None,
        name: row.name.clone(),
        session_id: SessionId(row.session_id.0.clone()),
        role: row.role.clone(),
        tier: tier_from_str(&row.tier),
        project: row.project.clone(),
        presence: presence_from_str(row.presence.as_deref()),
    }
}

/// A live session is one that is not offline — i.e. eligible to *hold* a name against a
/// different-`client_key` re-register (backend §5.1: a different ck with the same live name is a
/// `DuplicateName`). Offline rows are stale identities and do not block.
pub(crate) fn is_live(row: &SessionRow) -> bool {
    !matches!(row.presence.as_deref(), Some("offline"))
}

/// Guard: a found row must belong to the requested project (defense-in-depth; the store reads are
/// already project-scoped, but the service crosses no project boundary).
pub(crate) fn assert_project(row: &SessionRow, project: &str) -> Result<(), NexusError> {
    if row.project == project {
        Ok(())
    } else {
        Err(NexusError::ProjectScope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> SessionRow {
        SessionRow {
            session_id: SessionId("s_1".into()),
            name: Some("ben".into()),
            agent: Some("claude".into()),
            kind: "agent".into(),
            role: Some("backend".into()),
            tier: "admin".into(),
            harness_session_id: Some("h_1".into()),
            client_key: Some("ck".into()),
            cwd: None,
            project: "p".into(),
            current_work: None,
            presence: Some("busy".into()),
            paused: false,
            paused_by: None,
            callback_url: None,
            last_heartbeat: Some(1),
            created_at: 0,
            transport: None,
            metadata_json: None,
            agent_id: None,
        }
    }

    #[test]
    fn caller_and_whoami_carry_tier_and_presence() {
        let r = row();
        let c = caller_from_row(&r);
        assert_eq!(c.tier, Tier::Admin);
        assert_eq!(c.name, "ben");
        let w = whoami_from_row(&r);
        assert_eq!(w.tier, Tier::Admin);
        assert_eq!(w.presence, nexus_contracts::enums::Presence::Busy);
    }

    #[test]
    fn live_unless_offline() {
        let mut r = row();
        assert!(is_live(&r));
        r.presence = Some("offline".into());
        assert!(!is_live(&r));
    }
}
