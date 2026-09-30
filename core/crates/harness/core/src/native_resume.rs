//! Harness-neutral inputs and outputs for exact native runtime resurrection.
//!
//! Concrete harnesses interpret native keys and decide how to resume them. The daemon supplies
//! evidence for one selected runtime, verifies Nexus ownership, and executes the returned plan.

/// Native identity evidence scoped to the existing runtime selected by the caller.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeResumeEvidence<'a> {
    /// Explicit requested native key; a constraint, never independent ownership evidence.
    pub requested_key: Option<&'a str>,
    /// Durable native binding belonging to this exact runtime.
    pub binding_key: Option<&'a str>,
    /// Native key in the runtime's persistent resurrection capsule.
    pub capsule_key: Option<&'a str>,
    /// Native key observed in this runtime's current transport sidecar or ready result.
    pub observed_key: Option<&'a str>,
    /// Compatibility native key on the runtime's legacy session row.
    pub legacy_key: Option<&'a str>,
}

/// Native storage scope required to find the selected conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeResumeStore {
    /// Reuse the selected runtime's original launch-local native store.
    OriginalRuntime,
    /// Use the harness's default native store.
    Default,
}

/// Validated native resume operation, independent of process or database implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeResumePlan {
    /// Exact opaque native identity selected by the harness policy.
    pub native_key: String,
    /// Native argument tail required to resume that identity.
    pub argv: Vec<String>,
    /// Storage scope the caller must preserve when executing this plan.
    pub store: NativeResumeStore,
}
