//! Pure harness routing, durable native-identity helpers, and launch path utilities.

use std::sync::Arc;

use nexus_common::NexusError;
use nexus_contracts::ids::SessionId;
use nexus_contracts::{ContractError, HarnessId, Kind, SpawnIdentityPolicy, SpawnRequest, Tier};
use nexus_harness_core::HeadedRuntimeKind;
use nexus_store::repos::{
    AgentRef, IdentitySessions, NativeThreadBindings, NewNativeThreadBinding, Sessions,
};
use nexus_store::types::{AgentRow, SessionRow};
use nexus_store::Store;

use crate::daemon::pty_supervisor::{harness_agent_token, harness_program, headed_runtime_kind};

// ── Launch routing ──────────────────────────────────────────────────────────────────────────────

/// The outcome of [`launch_route`]: which code path `launch_agent` should take.
#[doc(hidden)]
#[derive(Debug, PartialEq, Eq)]
pub enum LaunchRoute {
    /// Spawn the harness in a daemon-owned PTY using the given binary name.
    Headed(&'static str),
    /// Use the ACP adapter path (headless, or pty-absent degradation for headed requests).
    Headless,
    /// The requested harness kind has no TUI binary; return an INVALID_PARAMS error.
    NoTuiBinary,
}

/// Pure routing decision for `AppState::launch_agent`.
///
/// Encodes the headed/headless decision table:
/// - **Headless** (`headless == true`): always ACP, regardless of whether a PTY is present.
/// - **Headed, pty present, program exists** → [`LaunchRoute::Headed`] with the binary name.
/// - **Headed, pty present, no program** (Pi/Other) → [`LaunchRoute::NoTuiBinary`].
/// - **Headed, pty absent** → [`LaunchRoute::Headless`] (degrade; preserves mock-port behaviour).
#[doc(hidden)]
pub fn launch_route(headless: bool, pty_present: bool, kind: &HarnessId) -> LaunchRoute {
    if headless {
        return LaunchRoute::Headless;
    }
    // headed
    if pty_present {
        match harness_program(kind) {
            Some(program) => LaunchRoute::Headed(program),
            None => LaunchRoute::NoTuiBinary,
        }
    } else {
        // No PTY backend — degrade to ACP rather than error (keeps mock-port tests passing).
        LaunchRoute::Headless
    }
}

// ── Revive / teardown routing ────────────────────────────────────────────────────────────────────

/// Which revive backend to use for a session, based on its durable `transport` column.
///
/// `transport="pty"` is the ONLY positive signal that a daemon-owned tmux harness exists for this
/// session — it is written exclusively by the headed launch path (`launch_agent_with_program`).
/// Every other row has no tmux harness the daemon could respawn:
/// - a **headless** launch (`"acp"`),
/// - a **self-registered** agent (`register` mints the member row but spawns nothing → `transport`
///   is `NULL`; it is driven over the ACP/turn-exec path), and
/// - a **legacy** pre-migration row (`NULL`).
///
/// So only `Some("pty")` respawns tmux; EVERYTHING ELSE (`"acp"`, `None`, any unknown token) revives
/// over ACP — the daemon can always synthesize a fresh ACP session, but it cannot meaningfully
/// "respawn tmux" for an agent it never launched into tmux. Defaulting `NULL`→`Pty` here was the bug
/// that left a registered agent's boot-pending DM stuck (`respawn_pending_agents` no-op'd it through
/// the tmux path on a daemon with no live harness for that session).
///
/// NOTE the deliberate asymmetry with [`teardown_route`], which keeps `NULL`→`Pty`: there
/// `kill_harness` is a harmless no-op for an unbound/ACP session yet still terminates a legacy tmux
/// harness, so the conservative default loses nothing. Revive must pick the ONE backend that can
/// actually come up; teardown is best-effort cleanup.
#[doc(hidden)]
#[derive(Debug, PartialEq, Eq)]
pub enum ReviveRoute {
    Acp,
    Pty,
    /// Headed app-server sessions restart the server binary, resume the stored thread id, and
    /// rebind structured turn delivery under the same Nexus session id.
    CodexAppServer,
    /// Headed plugin-bridge sessions restart the native-plugin bridge, resume the stored
    /// `ses_*` id, and rebind structured delivery under the same Nexus session id.
    OpenCodePlugin,
}

#[doc(hidden)]
pub fn revive_route(transport: Option<&str>) -> ReviveRoute {
    match transport {
        Some("pty") => ReviveRoute::Pty,
        Some("codex-appserver") => ReviveRoute::CodexAppServer,
        Some("opencode-plugin") => ReviveRoute::OpenCodePlugin,
        _ => ReviveRoute::Acp,
    }
}

/// Which teardown backend to use. `Some("acp")` closes the ACP child; everything else (`"pty"`,
/// `None` legacy, unknown) goes through `kill_harness` — a no-op when nothing is pty-bound, but it
/// still terminates a legacy tmux harness, so it is the safe best-effort default. (See the asymmetry
/// note on [`revive_route`]: revive must bring up the right backend; teardown only needs to not leak.)
#[doc(hidden)]
#[derive(Debug, PartialEq, Eq)]
pub enum TeardownRoute {
    Acp,
    Pty,
}

#[doc(hidden)]
pub fn teardown_route(transport: Option<&str>) -> TeardownRoute {
    match transport {
        Some("acp") => TeardownRoute::Acp,
        _ => TeardownRoute::Pty,
    }
}

/// The harness/runtime label stored on `sessions.agent` (mirrors `nexus-identity`'s private map).
pub(crate) fn harness_token(h: &HarnessId) -> &'static str {
    harness_agent_token(h)
}

/// Inverse of [`harness_token`]: a stored `sessions.agent` label back to a [`HarnessId`]. Used by
/// the revive path to re-resolve which adapter to (re-)spawn. Tokens ARE registry ids now, so this
/// is a validating parse. Unknown / legacy / NULL / invalid -> `claude` (the historical default
/// for pre-label rows); unknown-but-valid ids flow through and hit the registry's lookup-miss
/// policy (generic non-headed contract) downstream.
#[doc(hidden)]
pub fn harness_from_token(token: Option<&str>) -> HarnessId {
    token
        .and_then(|tok| HarnessId::new(tok).ok())
        .unwrap_or_else(|| HarnessId::new("claude").expect("claude is a valid harness id"))
}

/// Headed-runtime resolution keeps its OWN fallback, distinct from [`harness_from_token`]:
/// revive falls back to `claude`, but a NULL / legacy / invalid `sessions.agent` label must keep
/// attaching via the generic `Screen` runtime (pre-label rows were screen-attached; respawning
/// them as ClaudeNative would change attach behavior for rows that never ran claude).
#[doc(hidden)]
pub fn headed_runtime_from_agent_token(token: Option<&str>) -> HeadedRuntimeKind {
    token
        .and_then(|tok| HarnessId::new(tok).ok())
        .map(|h| headed_runtime_kind(&h))
        .unwrap_or(HeadedRuntimeKind::Screen)
}

pub(crate) fn default_agent_cwd(agent_key: &str) -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    format!("{home}/.nexus/agents/{agent_key}")
}

pub(crate) fn default_agent_cwd_for(agent_id: Option<&str>, name: &str) -> String {
    default_agent_cwd(agent_id.unwrap_or(name))
}

pub(crate) fn resolve_agent_launch_cwd(
    agent_id: &str,
    _name: Option<&str>,
    explicit: Option<String>,
) -> (String, bool) {
    match explicit {
        Some(cwd) => (cwd, false),
        None => (default_agent_cwd(agent_id), true),
    }
}

pub(crate) fn ensure_agent_launch_cwd(path: &str, private: bool) {
    let _ = std::fs::create_dir_all(path);
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
}

/// The kind token stored on `sessions.kind`.
pub(crate) fn kind_token(k: Kind) -> &'static str {
    match k {
        Kind::Agent => "agent",
        Kind::Human => "human",
        Kind::Notification => "notification",
        Kind::App => "app",
    }
}

/// The tier token stored on `sessions.tier`.
pub(crate) fn tier_token(t: Tier) -> &'static str {
    match t {
        Tier::Agent => "agent",
        Tier::Admin => "admin",
    }
}

pub(crate) fn claude_resume_session_id(args: &[String]) -> Option<&str> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--resume" {
            return iter.next().map(String::as_str).filter(|id| !id.is_empty());
        }
        if let Some(id) = arg.strip_prefix("--resume=") {
            if !id.is_empty() {
                return Some(id);
            }
        }
    }
    None
}

const CODEX_THREAD_BINDING_CLAIM_ATTEMPTS: usize = 40;
const CODEX_THREAD_BINDING_CLAIM_RETRY_MS: u64 = 25;

pub(crate) async fn persist_codex_thread_binding_once(
    store: &Arc<Store>,
    session: &SessionId,
    thread_id: &str,
) -> Result<bool, NexusError> {
    let sessions = Sessions::new(store.as_ref());
    let Some(row) = sessions.find_by_session_id(session).await? else {
        return Ok(false);
    };
    sessions.set_harness_session_id(session, thread_id).await?;
    if store.has_split_authority() {
        IdentitySessions::new(store.as_ref())
            .set_native_resume_key(&session.0, thread_id)
            .await?;
    }
    let Some(agent_id) = row.agent_id else {
        return Ok(false);
    };
    NativeThreadBindings::new(store.as_ref())
        .claim(NewNativeThreadBinding {
            provider: "codex".into(),
            kind: "harness".into(),
            native_thread_id: thread_id.to_string(),
            agent_id,
            project: row.project,
            runtime_id: Some(session.0.clone()),
        })
        .await?;
    Ok(true)
}

pub(crate) async fn persist_codex_thread_binding_with_retry(
    store: Arc<Store>,
    session: SessionId,
    thread_id: String,
) {
    for attempt in 0..CODEX_THREAD_BINDING_CLAIM_ATTEMPTS {
        match persist_codex_thread_binding_once(&store, &session, &thread_id).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    target: "nexus::codex",
                    session = %session,
                    thread_id = %thread_id,
                    error = %e,
                    "failed to claim durable codex thread binding"
                );
                return;
            }
        }
        if attempt + 1 < CODEX_THREAD_BINDING_CLAIM_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(
                CODEX_THREAD_BINDING_CLAIM_RETRY_MS,
            ))
            .await;
        }
    }
    tracing::warn!(
        target: "nexus::codex",
        session = %session,
        thread_id = %thread_id,
        "skipping durable codex thread binding claim because session registration did not become visible"
    );
}

pub(crate) fn native_session_taken_error(
    label: &str,
    native_id: &str,
    owner: &SessionRow,
) -> ContractError {
    ContractError {
        code: nexus_contracts::codes::INVALID_PARAMS,
        message: format!(
            "{label} {native_id} is already bound to {}:{}; use that identity or remove it before rebinding",
            owner.display_name(), owner.session_id
        ),
    }
}

pub(crate) fn codex_thread_taken_error(
    thread_id: &str,
    owner_name: &str,
    owner_agent_id: &str,
) -> ContractError {
    ContractError {
        code: nexus_contracts::codes::INVALID_PARAMS,
        message: format!(
            "codex thread {thread_id} is already bound to {owner_name}:{owner_agent_id}; use that identity or delete/transfer ownership before rebinding"
        ),
    }
}

pub(crate) fn spawn_identity_policy(req: &SpawnRequest) -> SpawnIdentityPolicy {
    req.identity_policy.unwrap_or_else(|| {
        req.name
            .as_deref()
            .map(AgentRef::parse)
            .map(|agent_ref| match agent_ref {
                AgentRef::Id(_) => SpawnIdentityPolicy::ExplicitAgentId,
                AgentRef::Name(_) => SpawnIdentityPolicy::ExplicitName,
            })
            .unwrap_or(SpawnIdentityPolicy::Implicit)
    })
}

pub(crate) fn codex_resume_agent_owner_matches(
    req: &SpawnRequest,
    project: &str,
    owner: &AgentRow,
) -> bool {
    match spawn_identity_policy(req) {
        SpawnIdentityPolicy::Implicit => true,
        SpawnIdentityPolicy::ExplicitName => {
            owner.project == project && req.name.as_deref() == owner.name.as_deref()
        }
        SpawnIdentityPolicy::ExplicitAgentId => {
            req.name.as_deref() == Some(owner.agent_id.as_str())
        }
    }
}

pub(crate) fn codex_resume_session_owner_matches(
    req: &SpawnRequest,
    project: &str,
    owner: &SessionRow,
) -> bool {
    match spawn_identity_policy(req) {
        SpawnIdentityPolicy::Implicit => true,
        SpawnIdentityPolicy::ExplicitName => {
            owner.project == project && req.name.as_deref() == owner.name.as_deref()
        }
        SpawnIdentityPolicy::ExplicitAgentId => req.name.as_deref() == owner.agent_id.as_deref(),
    }
}

pub(crate) fn protected_remove_target(row: &SessionRow) -> bool {
    row.is_human()
        || row.tier == tier_token(Tier::Admin)
        || row
            .role
            .as_deref()
            .is_some_and(|role| role.eq_ignore_ascii_case("lead"))
}
