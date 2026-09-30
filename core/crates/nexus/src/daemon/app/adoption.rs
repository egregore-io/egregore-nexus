//! Boot adoption and native transcript-forwarder ownership.

use super::*;

impl AppState {
    /// Reattach daemon-owned loop/stream observers for active headed PTY runtimes after a daemon
    /// restart. This is adoption only: it does not relaunch a harness or mutate tmux state.
    pub async fn adopt_active_pty_runtimes(&self) {
        use nexus_store::repos::Sessions;

        self.revive_active_opencode_plugin_runtimes().await;
        let runtimes = match AgentRuntimes::new(&self.store)
            .list_active_by_transport("pty")
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::revive",
                    error = %error,
                    "failed to list active PTY runtimes for daemon adoption"
                );
                return;
            }
        };
        for runtime in runtimes {
            let session = SessionId(runtime.runtime_id);
            let row = match Sessions::new(&self.store)
                .find_by_session_id(&session)
                .await
            {
                Ok(Some(row)) if row.is_agent() => row,
                Ok(_) => continue,
                Err(error) => {
                    tracing::warn!(
                        target: "nexus::revive",
                        session = %session,
                        error = %error,
                        "failed to load session row for active PTY adoption"
                    );
                    continue;
                }
            };
            let headed_runtime = headed_runtime_from_agent_token(Some(runtime.harness.as_str()));
            if let Some(supervisor) = &self.pty {
                let cwd = row.cwd.clone().or(runtime.cwd.clone()).unwrap_or_else(|| {
                    default_agent_cwd_for(row.agent_id.as_deref(), &row.display_name())
                });
                if let Err(error) = supervisor.adopt_pty_backend(&session, &cwd, headed_runtime) {
                    tracing::warn!(
                        target: "nexus::revive",
                        session = %session,
                        name = %row.display_name(),
                        error = %error,
                        "failed to adopt active tmux runtime; drain loop not started"
                    );
                    continue;
                }
            }
            match headed_runtime {
                HeadedRuntimeKind::ClaudeNative => {
                    self.spawn_claude_native_forwarder_if_needed(&session).await;
                }
                HeadedRuntimeKind::OpenCodePlugin => {
                    self.spawn_opencode_native_forwarder_if_needed(&session)
                        .await;
                }
                HeadedRuntimeKind::HermesGateway => {
                    self.spawn_hermes_native_forwarder_if_needed(&session).await;
                }
                HeadedRuntimeKind::CodexAppServer => {}
                HeadedRuntimeKind::Screen => {
                    self.spawn_generic_pty_reader_if_needed(&session, runtime.harness.as_str())
                        .await;
                }
            }
            if let Err(error) = self
                .presence
                .mark_transport_present(&session, TransportHandle::EventLoop)
                .await
            {
                tracing::warn!(
                    target: "nexus::presence",
                    session = %session,
                    error = %error,
                    "failed to materialize adopted PTY runtime presence"
                );
                // Earlier adoption/attachment work remains; only the later wakeable tail stops.
                continue;
            }
            if let Some(w) = &self.loop_wiring {
                w.spawn_loop(&session, &row.project).await;
                w.ring(&session);
            }
        }
    }

    pub(crate) async fn revive_active_opencode_plugin_runtimes(&self) {
        use nexus_store::repos::Sessions;
        let runtimes = match AgentRuntimes::new(&self.store)
            .list_active_by_transport("opencode-plugin")
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::revive",
                    error = %error,
                    "failed to list active OpenCode plugin runtimes for daemon revive"
                );
                return;
            }
        };
        for runtime in runtimes {
            let session = SessionId(runtime.runtime_id);
            let row = match Sessions::new(&self.store)
                .find_by_session_id(&session)
                .await
            {
                Ok(Some(row)) if row.is_agent() => row,
                Ok(_) => continue,
                Err(error) => {
                    tracing::warn!(
                        target: "nexus::revive",
                        session = %session,
                        error = %error,
                        "failed to load session row for active OpenCode plugin revive"
                    );
                    continue;
                }
            };
            if let Some(name) = row.name.as_deref() {
                if let Err(error) = self.ensure_opencode_plugin_live(name, &row.project).await {
                    tracing::warn!(
                        target: "nexus::revive",
                        session = %row.session_id,
                        name = %row.display_name(),
                        error = ?error,
                        "failed to revive active OpenCode plugin runtime during daemon boot"
                    );
                }
            }
        }
    }

    /// Reattach daemon-owned stream observers for active headed Claude runtimes after a daemon
    /// restart. This is adoption only: it does not relaunch Claude or mutate tmux state.
    pub async fn adopt_active_claude_forwarders(&self) {
        let runtimes = match AgentRuntimes::new(&self.store)
            .list_active_by_harness_transport("claude", "pty")
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::claude_native_forwarder",
                    error = %error,
                    "failed to list active Claude runtimes for forwarder adoption"
                );
                return;
            }
        };
        for runtime in runtimes {
            let session = SessionId(runtime.runtime_id);
            self.spawn_claude_native_forwarder_if_needed(&session).await;
        }
    }

    /// Reattach daemon-owned stream observers for active headed OpenCode runtimes after a daemon
    /// restart. This is adoption only: it does not relaunch OpenCode or mutate tmux state.
    pub async fn adopt_active_opencode_forwarders(&self) {
        let runtimes = match AgentRuntimes::new(&self.store)
            .list_active_by_harness_transport("opencode", "pty")
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::opencode_native_forwarder",
                    error = %error,
                    "failed to list active OpenCode runtimes for forwarder adoption"
                );
                return;
            }
        };
        for runtime in runtimes {
            let session = SessionId(runtime.runtime_id);
            self.spawn_opencode_native_forwarder_if_needed(&session)
                .await;
        }
    }

    /// Reattach daemon-owned stream observers for active headed Hermes runtimes after a daemon
    /// restart. This is adoption only: it does not relaunch Hermes or mutate tmux state.
    pub async fn adopt_active_hermes_forwarders(&self) {
        let runtimes = match AgentRuntimes::new(&self.store)
            .list_active_by_harness_transport("hermes", "pty")
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::hermes_native_forwarder",
                    error = %error,
                    "failed to list active Hermes runtimes for forwarder adoption"
                );
                return;
            }
        };
        for runtime in runtimes {
            let session = SessionId(runtime.runtime_id);
            self.spawn_hermes_native_forwarder_if_needed(&session).await;
        }
    }

    pub(crate) async fn agent_name_for_runtime(
        &self,
        session: &SessionId,
    ) -> Result<Option<String>, NexusError> {
        let Some(runtime) = AgentRuntimes::new(&self.store)
            .find_by_runtime_id(&session.0)
            .await?
        else {
            return Ok(None);
        };
        Ok(Agents::new(&self.store)
            .find_by_id(&runtime.agent_id)
            .await?
            .and_then(|agent| agent.name))
    }

    /// Start the headed Claude native JSONL forwarder once for a session. This is called from
    /// launch, revive, and boot adoption so already-running Claude TUIs continue feeding
    /// `stream_events` without requiring a relaunch.
    pub(crate) async fn spawn_claude_native_forwarder_if_needed(&self, session: &SessionId) {
        const HARNESS: &str = "claude";
        let Some(w) = &self.loop_wiring else { return };
        // Capture before path/identity reads. A late attachment must never borrow NEW's owner.
        let completion = self
            .pty
            .as_ref()
            .map(|supervisor| supervisor.claude_turn_completion(session));
        let claimed = match self.pty.as_ref().zip(completion.as_ref()) {
            Some((supervisor, owner)) => {
                // Owner capture above must precede this await. The synchronous owner gate then
                // rejects OLD if a replacement appeared while waiting for the same Store guard.
                let transition = w.store.lock_presence_transition().await;
                supervisor
                    .with_claude_owner(session, owner, || {
                        w.claim_native_forwarder_for_owner(
                            HARNESS,
                            session,
                            owner.owner_id(),
                            &transition,
                        )
                    })
                    .unwrap_or(false)
            }
            None => w.claim_native_forwarder(HARNESS, session).await,
        };
        if !claimed {
            tracing::debug!(
                target: "nexus::claude_native_forwarder",
                session = %session,
                "Claude native forwarder already running"
            );
            return;
        }
        let release = || match &completion {
            Some(owner) => w.release_native_forwarder_for_owner(HARNESS, session, owner.owner_id()),
            None => w.release_native_forwarder(HARNESS, session),
        };

        let paths = match claude_native_paths_for_runtime(&self.store, session).await {
            Ok(paths) => paths,
            Err(error) => {
                release();
                tracing::warn!(
                    target: "nexus::claude_native_forwarder",
                    session = %session,
                    error = %error,
                    "failed to resolve Claude native bridge paths"
                );
                return;
            }
        };
        let tool_events = if let Some(publisher) = w.gateway_stream.clone() {
            match self.agent_name_for_runtime(session).await {
                Ok(Some(agent_name)) => Some(Arc::new(GatewayClaudeToolObservationSink::new(
                    publisher, agent_name,
                ))
                    as Arc<dyn nexus_harness_claude::native::forwarder::ClaudeToolObservationSink>),
                Ok(None) => {
                    tracing::debug!(
                        target: "nexus::claude_native_forwarder",
                        session = %session,
                        "skipping Claude tool-call developer events; runtime has no agent name"
                    );
                    None
                }
                Err(error) => {
                    tracing::warn!(
                        target: "nexus::claude_native_forwarder",
                        session = %session,
                        error = %error,
                        "skipping Claude tool-call developer events; failed to resolve agent name"
                    );
                    None
                }
            }
        } else {
            None
        };
        if let Some(owner) = &completion {
            if !owner.is_current() {
                release();
                return;
            }
            owner.attach_hook_source(paths.hook_log_path.clone());
            if let Ok(Some(state)) =
                nexus_harness_claude::storage::ClaudeRuntimeStateRepo::new(&self.store)
                    .find_by_runtime_id(session)
                    .await
            {
                owner.attach_resume_identity(state.claude_session_id);
            }
            if !owner.is_current() {
                release();
                return;
            }
        }
        if let Some(owner) = &completion {
            if let Err(error) = self
                .attach_claude_model_reporting(session, owner, &paths)
                .await
            {
                // Display/archive are independent from model admission. The failed captured
                // setup already revoked its claim and left modeled input unavailable; keep
                // forwarding native output without fabricating a reporting fallback.
                tracing::warn!(session=%session,error=%error,"failed to attach captured Claude model reporting");
            }
        }
        let attach = || {
            let handle = spawn_claude_native_forwarder_with_tool_events(
                self.store.clone(),
                session.clone(),
                paths,
                w.events.clone(),
                w.bell.clone(),
                DEFAULT_CLAUDE_NATIVE_FORWARDER_POLL_MS,
                tool_events,
                completion.clone(),
            );
            match &completion {
                Some(owner) => {
                    w.track_native_forwarder_for_owner(HARNESS, session, owner.owner_id(), handle)
                }
                None => w.track_native_forwarder(HARNESS, session, handle),
            }
        };
        if let Some((supervisor, owner)) = self.pty.as_ref().zip(completion.as_ref()) {
            if supervisor
                .with_claude_owner(session, owner, attach)
                .is_none()
            {
                release();
            }
        } else {
            attach();
        }
    }

    /// Start the headed OpenCode native SQLite event forwarder once for a session.
    pub(crate) async fn spawn_opencode_native_forwarder_if_needed(&self, session: &SessionId) {
        let Some(w) = &self.loop_wiring else { return };
        if !w.claim_native_forwarder("opencode", session).await {
            tracing::debug!(
                target: "nexus::opencode_native_forwarder",
                session = %session,
                "OpenCode native forwarder already running"
            );
            return;
        }

        match OpenCodeRuntimeStateRepo::new(&self.store)
            .find_by_runtime_id(session)
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                w.release_native_forwarder("opencode", session);
                tracing::debug!(
                    target: "nexus::opencode_native_forwarder",
                    session = %session,
                    "no OpenCode runtime sidecar state; not starting forwarder"
                );
                return;
            }
            Err(error) => {
                w.release_native_forwarder("opencode", session);
                tracing::warn!(
                    target: "nexus::opencode_native_forwarder",
                    session = %session,
                    error = %error,
                    "failed to resolve OpenCode native state"
                );
                return;
            }
        }

        let tool_events = if let Some(publisher) = w.gateway_stream.clone() {
            match self.agent_name_for_runtime(session).await {
                Ok(Some(agent_name)) => Some(Arc::new(GatewayOpenCodeToolObservationSink::new(
                    publisher, agent_name,
                ))
                    as Arc<dyn OpenCodeToolObservationSink>),
                Ok(None) => {
                    tracing::debug!(
                        target: "nexus::opencode_native_forwarder",
                        session = %session,
                        "skipping OpenCode tool-call developer events; runtime has no agent name"
                    );
                    None
                }
                Err(error) => {
                    tracing::warn!(
                        target: "nexus::opencode_native_forwarder",
                        session = %session,
                        error = %error,
                        "skipping OpenCode tool-call developer events; failed to resolve agent name"
                    );
                    None
                }
            }
        } else {
            None
        };

        let handle = spawn_opencode_native_forwarder_with_tool_events(
            self.store.clone(),
            session.clone(),
            w.events.clone(),
            w.bell.clone(),
            DEFAULT_OPENCODE_NATIVE_FORWARDER_POLL_MS,
            tool_events,
        );
        w.track_native_forwarder("opencode", session, handle);
    }

    /// Start the headed Hermes native SQLite message forwarder once for a session.
    pub(crate) async fn spawn_hermes_native_forwarder_if_needed(&self, session: &SessionId) {
        let Some(w) = &self.loop_wiring else { return };
        if !w.claim_native_forwarder("hermes", session).await {
            tracing::debug!(
                target: "nexus::hermes_native_forwarder",
                session = %session,
                "Hermes native forwarder already running"
            );
            return;
        }

        match HermesRuntimeStateRepo::new(&self.store)
            .find_by_runtime_id(session)
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                w.release_native_forwarder("hermes", session);
                tracing::debug!(
                    target: "nexus::hermes_native_forwarder",
                    session = %session,
                    "no Hermes runtime sidecar state; not starting forwarder"
                );
                return;
            }
            Err(error) => {
                w.release_native_forwarder("hermes", session);
                tracing::warn!(
                    target: "nexus::hermes_native_forwarder",
                    session = %session,
                    error = %error,
                    "failed to resolve Hermes native state"
                );
                return;
            }
        }

        let tool_events = if let Some(publisher) = w.gateway_stream.clone() {
            match self.agent_name_for_runtime(session).await {
                Ok(Some(agent_name)) => Some(Arc::new(GatewayHermesToolObservationSink::new(
                    publisher, agent_name,
                ))
                    as Arc<dyn HermesToolObservationSink>),
                Ok(None) => {
                    tracing::debug!(
                        target: "nexus::hermes_native_forwarder",
                        session = %session,
                        "skipping Hermes tool-call developer events; runtime has no agent name"
                    );
                    None
                }
                Err(error) => {
                    tracing::warn!(
                        target: "nexus::hermes_native_forwarder",
                        session = %session,
                        error = %error,
                        "skipping Hermes tool-call developer events; failed to resolve agent name"
                    );
                    None
                }
            }
        } else {
            None
        };

        let handle = spawn_hermes_native_forwarder_with_tool_events(
            self.store.clone(),
            session.clone(),
            w.events.clone(),
            w.bell.clone(),
            DEFAULT_HERMES_NATIVE_FORWARDER_POLL_MS,
            tool_events,
        );
        w.track_native_forwarder("hermes", session, handle);
    }

    /// Reattach the generic PTY scraper for headed runtimes that do not have a structured bridge.
    /// Harnesses whose contract reports a non-`Screen` runtime (`uses_structured_output`) deliver
    /// rows through their own bridge/forwarder, so they must not also receive PTY screen-scraped
    /// stream rows during daemon boot adoption. Unknown tokens resolve to `Screen` and keep the
    /// scraper.
    pub(crate) async fn spawn_generic_pty_reader_if_needed(
        &self,
        session: &SessionId,
        harness: &str,
    ) {
        if !matches!(
            headed_runtime_from_agent_token(Some(harness)),
            HeadedRuntimeKind::Screen
        ) {
            return;
        }
        let (Some(supervisor), Some(w)) = (&self.pty, &self.loop_wiring) else {
            return;
        };
        let pty_output: broadcast::Receiver<Vec<u8>> =
            supervisor.pty_output(session).unwrap_or_else(|| {
                let (_, rx) = broadcast::channel::<Vec<u8>>(1);
                rx
            });
        w.spawn_raw_stream_writer(session, pty_output.resubscribe())
            .await;
        spawn_pty_reply_reader(
            session.clone(),
            pty_output,
            w.events.clone(),
            w.bell.clone(),
            1500,
        );
    }

    pub(crate) async fn spawn_raw_stream_writer_for_terminal(&self, session: &SessionId) {
        let (Some(supervisor), Some(w)) = (&self.pty, &self.loop_wiring) else {
            return;
        };
        let Some(output) = supervisor.pty_output(session) else {
            return;
        };
        w.spawn_raw_stream_writer(session, output).await;
    }

    /// The daemon-owned [`PtySupervisor`], if this `AppState` was built by [`wire_pty`].
    /// Tests and local terminal surfaces use its backend-neutral output and terminal endpoint APIs.
    pub fn pty_supervisor(&self) -> Option<&Arc<PtySupervisor>> {
        self.pty.as_ref()
    }

    /// Whether this wiring includes the concrete ACP agent used for headless launches.
    #[doc(hidden)]
    pub fn has_acp_agent(&self) -> bool {
        self.agent_concrete.is_some()
    }
}
