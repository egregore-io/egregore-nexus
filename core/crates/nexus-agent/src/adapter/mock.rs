//! The mock adapter (backend spec §11 testing approach). Records every injected prompt and emits
//! scripted `session/update` chunks, so a turn's `<nexus-batch …>` shape and the reply stream are
//! assertable without a live model. It can also be set to fail [`Adapter::open_session`] to drive
//! the init-failure path (`agent.status=errored`, session retained).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use nexus_common::NexusError;

use super::{Adapter, AdapterInjectError, AdapterProviderLimit, StreamEvent};

/// Shared, cheaply-cloneable test adapter. All clones see the same recorded prompts and script
/// (the state is behind an `Arc<Mutex<…>>`), so a test can hold a handle, register a clone in the
/// [`crate::AdapterRegistry`], drive a turn, then assert against the recorded prompt.
#[derive(Clone, Default)]
pub struct MockAdapter {
    inner: Arc<Mutex<MockState>>,
}

#[derive(Default)]
struct MockState {
    /// Every prompt handed to [`Adapter::inject`], in order.
    injected: Vec<String>,
    /// The scripted reply events the next [`Adapter::stream_updates`] returns.
    script: Vec<StreamEvent>,
    /// When set, [`Adapter::open_session`] fails with this message (init-failure path).
    fail_open: Option<String>,
    /// When set, [`Adapter::resume`] fails with this message — but `open_session` /
    /// `new_session_only` still succeed. Models the live stale-resume boot: after a daemon restart
    /// the stored ACP resume key is invalid, so `session/load` fails and the daemon must fall back
    /// to a fresh `session/new` and STILL end with a live, injectable adapter.
    fail_resume: Option<String>,
    /// Number of times [`Adapter::kill`] was called — asserted by kill-path tests.
    kill_count: u32,
    /// Optional structured provider-limit failure returned by inject. Test-only hook for proving
    /// the agent service preserves typed adapter errors through the observed injection seam.
    provider_limit: Option<AdapterProviderLimit>,
}

impl MockAdapter {
    /// A fresh mock with no script and a succeeding `open_session`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Script the next `stream_updates` to yield these strings as ordered [`AgentUpdateKind::Text`]
    /// reply events (the common case — a streamed text reply). For richer streams (thinking, tool
    /// calls, …) use [`MockAdapter::script_events`].
    ///
    /// [`AgentUpdateKind::Text`]: nexus_contracts::AgentUpdateKind::Text
    pub fn script_updates(&self, chunks: Vec<&str>) {
        let mut s = self.inner.lock().unwrap();
        s.script = chunks.into_iter().map(StreamEvent::text).collect();
    }

    /// Script the exact [`StreamEvent`]s the next `stream_updates` will yield, in order — for tests
    /// that need the full pass-through stream (mixed kinds: thinking, tool calls, plans, commands).
    pub fn script_events(&self, events: Vec<StreamEvent>) {
        self.inner.lock().unwrap().script = events;
    }

    /// Make `open_session` fail with `msg` (drives the init-failure / errored-status path).
    pub fn fail_open_session(&self, msg: &str) {
        self.inner.lock().unwrap().fail_open = Some(msg.to_string());
    }

    /// Make `resume` (session/load) fail with `msg` while `open_session` / `new_session_only`
    /// (session/new) still succeed — the live stale-resume boot path. The daemon's `open_session_for`
    /// must fall back to `new_session_only` and bind a LIVE adapter.
    pub fn fail_resume(&self, msg: &str) {
        self.inner.lock().unwrap().fail_resume = Some(msg.to_string());
    }

    /// All prompts injected so far, in order.
    pub fn injected_prompts(&self) -> Vec<String> {
        self.inner.lock().unwrap().injected.clone()
    }

    /// The single most-recently-injected prompt, if any.
    pub fn last_prompt(&self) -> Option<String> {
        self.inner.lock().unwrap().injected.last().cloned()
    }

    /// How many times [`Adapter::kill`] was called on this adapter.
    pub fn kill_count(&self) -> u32 {
        self.inner.lock().unwrap().kill_count
    }

    /// Make injection return a structured provider-limit failure.
    pub fn fail_inject_provider_limit(&self, limit: AdapterProviderLimit) {
        self.inner.lock().unwrap().provider_limit = Some(limit);
    }
}

#[async_trait]
impl Adapter for MockAdapter {
    async fn open_session(&self) -> Result<(), NexusError> {
        if let Some(msg) = self.inner.lock().unwrap().fail_open.clone() {
            return Err(NexusError::Adapter(msg));
        }
        Ok(())
    }

    async fn resume(&self, _resume_key: &str) -> Result<(), NexusError> {
        // A dedicated resume-failure switch models the live stale-resume boot (session/load fails,
        // session/new still works). Falls back to the shared open_session switch otherwise.
        if let Some(msg) = self.inner.lock().unwrap().fail_resume.clone() {
            return Err(NexusError::Adapter(msg));
        }
        self.open_session().await
    }

    async fn new_session_only(&self) -> Result<(), NexusError> {
        // The resume-fail fallback: a fresh session/new on the (mock's) connection. Succeeds unless
        // the open switch is set — so `fail_resume` alone yields a LIVE adapter after the fallback.
        self.open_session().await
    }

    async fn inject(&self, prompt: String) -> Result<(), AdapterInjectError> {
        let mut state = self.inner.lock().unwrap();
        state.injected.push(prompt);
        if let Some(limit) = state.provider_limit.clone() {
            return Err(AdapterInjectError::ProviderLimit(limit));
        }
        Ok(())
    }

    async fn inject_with_accepted_event(
        &self,
        prompt: String,
        accepted_event: Option<StreamEvent>,
    ) -> Result<(), AdapterInjectError> {
        let mut state = self.inner.lock().unwrap();
        state.injected.push(prompt);
        if let Some(limit) = state.provider_limit.clone() {
            return Err(AdapterInjectError::ProviderLimit(limit));
        }
        if let Some(event) = accepted_event {
            state.script.insert(0, event);
        }
        Ok(())
    }

    async fn inject_completion_observed(
        &self,
        prompt: String,
    ) -> Result<Vec<StreamEvent>, AdapterInjectError> {
        let mut state = self.inner.lock().unwrap();
        state.injected.push(prompt);
        if let Some(limit) = state.provider_limit.clone() {
            return Err(AdapterInjectError::ProviderLimit(limit));
        }
        Ok(std::mem::take(&mut state.script))
    }

    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError> {
        Ok(self.inner.lock().unwrap().script.clone())
    }

    async fn kill(&self) {
        self.inner.lock().unwrap().kill_count += 1;
    }
}
