//! Ambient CLI identity parsing shared by store-backed CLI paths.
//!
//! Nexus commands never accept a `--from`; they derive caller identity from the process
//! environment (`NEXUS_NAME`, `NEXUS_CLIENT_KEY`, `NEXUS_PROJECT`, and related fields). Keeping the
//! parser separate lets the store-backed command-intent and read-view paths use the same identity
//! without depending on a daemon transport.

use nexus_contracts::{codes, AgentId, ContractError, HarnessId, Kind, RegisterRequest, Tier};

#[cfg(test)]
const IDENTITY_ENV_KEYS: &[&str] = &[
    "NEXUS_NAME",
    "NEXUS_CLIENT_KEY",
    "NEXUS_PROJECT",
    "NEXUS_AGENT",
    "NEXUS_AGENT_ID",
    "NEXUS_TIER",
    "NEXUS_KIND",
    "NEXUS_SESSION_ID",
    "CLAUDE_CODE_SESSION_ID",
];

/// Read an ambient identity from the environment, if present.
///
/// `NEXUS_NAME` enables the legacy nameful identity. A staged daemon launch omits `NEXUS_NAME` and
/// instead exports `NEXUS_AGENT_ID` + `NEXUS_SESSION_ID` + `NEXUS_CLIENT_KEY`; that still resolves
/// as an agent identity and must never fall through to the local operator.
///
/// When Nexus exports its stable runtime id, prefer it over the rotating client key. Provider
/// session ids such as `CLAUDE_CODE_SESSION_ID` never participate in Nexus identity.
pub fn identity_from_env() -> Option<RegisterRequest> {
    identity_from_env_result().ok().flatten()
}

/// Read an ambient identity and fail loudly on partial agent identity markers.
pub fn identity_from_env_result() -> Result<Option<RegisterRequest>, ContractError> {
    let name = std::env::var("NEXUS_NAME").ok().filter(|s| !s.is_empty());
    let agent_id = std::env::var("NEXUS_AGENT_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .map(AgentId);
    if name.is_none() && agent_id.is_none() {
        return Ok(None);
    }
    let client_key = std::env::var("NEXUS_CLIENT_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ContractError {
            code: codes::UNAUTHORIZED,
            message: "NEXUS_CLIENT_KEY is required when NEXUS_NAME or NEXUS_AGENT_ID is set".into(),
        })?;
    let project = std::env::var("NEXUS_PROJECT")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "default".into());
    let harness = std::env::var("NEXUS_AGENT")
        .ok()
        .and_then(|s| HarnessId::new(s).ok())
        .unwrap_or_else(|| HarnessId::new("claude").expect("builtin harness id is valid"));
    let tier = match std::env::var("NEXUS_TIER").ok().as_deref() {
        Some("admin") => Tier::Admin,
        _ => Tier::Agent,
    };
    let kind = match std::env::var("NEXUS_KIND").ok().as_deref() {
        Some("agent") => Some(Kind::Agent),
        Some("human") => Some(Kind::Human),
        Some("app") => Some(Kind::App),
        Some("notification") => Some(Kind::Notification),
        _ if agent_id.is_some() => Some(Kind::Agent),
        _ => None,
    };
    Ok(Some(RegisterRequest {
        agent_id,
        name,
        harness,
        harness_session_id: stable_harness_session_id_from_env()
            .unwrap_or_else(|| format!("hs_{client_key}")),
        project,
        client_key,
        runtime_credential: None,
        tier,
        kind,
        role: None,
        cwd: None,
    }))
}

/// Return the Nexus-owned stable runtime id when the runtime exports one.
///
/// `CLAUDE_CODE_SESSION_ID` is intentionally ignored: it is an opaque provider resume hint, not a
/// Nexus authentication or identity key.
pub fn stable_harness_session_id_from_env() -> Option<String> {
    std::env::var("NEXUS_SESSION_ID")
        .ok()
        .filter(|s| !s.is_empty())
}

/// Serializes tests that mutate Nexus environment variables.
///
/// Process environment is global inside one test binary, so per-module locks do not protect other
/// parallel tests from mid-test restores. Unit and integration tests that change Nexus env vars
/// should use [`TestEnvGuard`] or [`with_test_env_vars`] so the whole crate shares one lock.
#[doc(hidden)]
pub static ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[doc(hidden)]
pub use ENV_TEST_LOCK as STABLE_ENV_TEST_LOCK;

/// RAII guard for temporary Nexus env mutations in tests.
///
/// The guard holds [`ENV_TEST_LOCK`], snapshots every key named in `vars`, applies the requested
/// values (`None` removes a var), and restores the original values on drop. It is intentionally
/// available to integration tests too; production code should not need it.
#[doc(hidden)]
pub struct TestEnvGuard {
    saved: Vec<(String, Option<String>)>,
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl TestEnvGuard {
    /// Lock the shared env-test gate and apply `vars` until the guard is dropped.
    pub fn new(vars: &[(&str, Option<&str>)]) -> Self {
        let guard = ENV_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut saved = Vec::new();
        for (key, _) in vars {
            if saved.iter().any(|(saved_key, _)| saved_key == key) {
                continue;
            }
            saved.push(((*key).to_string(), std::env::var(key).ok()));
        }
        for (key, value) in vars {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        Self {
            saved,
            _guard: guard,
        }
    }

    /// Remove each key in `keys` until the guard is dropped.
    pub fn cleared(keys: &[&str]) -> Self {
        let vars = keys.iter().map(|key| (*key, None)).collect::<Vec<_>>();
        Self::new(&vars)
    }
}

impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

/// Test helper: run `f` with temporary Nexus env variables under the shared env lock.
#[doc(hidden)]
pub fn with_test_env_vars<T>(vars: &[(&str, Option<&str>)], f: impl FnOnce() -> T) -> T {
    let _guard = TestEnvGuard::new(vars);
    f()
}

/// Test-only: save the identity/operator env family, run `f` against a scrubbed env, restore.
/// Holds [`ENV_TEST_LOCK`] for the duration so parallel tests never observe the scrub.
#[cfg(test)]
#[doc(hidden)]
pub(crate) fn with_scrubbed_identity_env<T>(setup: &[(&str, &str)], f: impl FnOnce() -> T) -> T {
    let mut vars = Vec::new();
    for key in IDENTITY_ENV_KEYS {
        if !setup.iter().any(|(setup_key, _)| setup_key == key) {
            vars.push((*key, None));
        }
    }
    vars.extend(setup.iter().map(|(key, value)| (*key, Some(*value))));
    with_test_env_vars(&vars, f)
}
