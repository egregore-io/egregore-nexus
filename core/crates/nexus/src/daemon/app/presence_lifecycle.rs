//! Boot recovery and authoritative presence reconciliation.

use super::*;

impl AppState {
    /// Spawn boot-time pending-inbox recovery after final runtime wiring is assembled.
    /// The PTY supervisor must already be mounted so headed runtimes rebind correctly.
    pub(crate) fn spawn_boot_respawn(&self) {
        // Capture the recovery boundary before returning the wired state. The async recovery task
        // must never adopt messages accepted after startup as if they were pre-boot backlog.
        let boot_pending_cutoff = now();
        let st = self.clone();
        let reporting = self.model_reporting.clone();
        let guard = self
            .boot_readiness
            .guard(move || reporting.close_admission());
        tokio::spawn(async move {
            let initial_presence = async {
                if let Err(error) = st.restore_minimal_thread_routing_once().await {
                    tracing::error!(
                        target: "nexus::boot",
                        error = %error,
                        "failed to restore minimal thread routing; thread delivery remains unavailable"
                    );
                }
                if let Err(error) = st.restore_unsettled_delivery_obligations_once().await {
                    tracing::error!(
                        target: "nexus::delivery",
                        error = %error,
                        "failed to rebuild unsettled delivery state during daemon boot"
                    );
                    return Err(error);
                }
                // An `injecting` row crossed (or may have crossed) the external harness boundary in a
                // previous daemon process. Its outcome is unknowable after restart, so fail closed:
                // terminal DLQ evidence, never an automatic duplicate prompt.
                match Inbox::new(&st.store).recover_stale_injecting().await {
                    Ok(recovered) if recovered.count > 0 => tracing::warn!(
                        target: "nexus::delivery",
                        count = recovered.count,
                        "settled stale delivery attempts as delivery_outcome_unknown"
                    ),
                    Ok(_) => {}
                    Err(error) => tracing::error!(
                        target: "nexus::delivery",
                        %error,
                        "failed to recover stale delivery attempts; refusing to infer delivery"
                    ),
                }
                if let Err(error) = st.reconcile_claude_resume_ids_once().await {
                    tracing::warn!(
                        target: "nexus::revive",
                        error = %error,
                        "failed to backfill Claude resume ids during daemon boot"
                    );
                }
                st.adopt_active_pty_runtimes().await;
                // Presume-dead AFTER adoption: adoption re-registers the transports that
                // actually survived; every other session still claiming liveness is a
                // pre-restart fossil (including CLI/MCP peers, which the transport-scoped
                // reconcile used to skip — the phantom-online class). Runs after adoption so
                // it cannot stop the runtime rows adoption uses as its worklist.
                let presume_dead = st.reconcile_boot_presume_dead_once().await;
                if let Err(error) = &presume_dead {
                    tracing::warn!(
                        target: "nexus::presence",
                        error = %error,
                        "failed boot presume-dead presence reconcile"
                    );
                }
                let stale = st.reconcile_stale_presence_once().await;
                if let Err(error) = &stale {
                    tracing::warn!(
                        target: "nexus::presence",
                        error = %error,
                        "failed to reconcile stale presence during daemon boot"
                    );
                }
                Ok(InitialPresenceOutcome {
                    presume_dead: presume_dead.map_err(|error| error.to_string()),
                    stale: stale.map_err(|error| error.to_string()),
                })
            };
            if let Err(error) = run_boot(
                guard,
                st.model_reporting.initialize(),
                async {
                    st.restore_runtime_identity_sessions_once()
                        .await
                        .map(|_| ())
                },
                initial_presence,
                st.respawn_pending_agents_before(boot_pending_cutoff),
            )
            .await
            {
                tracing::error!(target: "nexus::boot", %error, "daemon boot recovery failed");
            }
        });
    }

    /// Wait for model-owner invalidation and successful persistent identity directory restoration.
    /// Production calls this before command/IPC ingress, not before adoption or backlog recovery.
    /// Errors and interrupted boot outcomes remain terminal for current and later waiters.
    #[doc(hidden)]
    pub async fn wait_for_runtime_identity_ready(&self) -> Result<(), NexusError> {
        self.boot_readiness.wait_ingress().await
    }

    /// Run one automatic Claude resume-id harvest pass.
    ///
    /// This fills older headed Claude sidecar rows that have an exact native `session_id` in their
    /// bridge hook log but no persisted `claude_session_id`. It is telemetry-only for boot: filled
    /// rows and skipped rows are warning logged, while ambiguous logs remain unmodified so revive
    /// still fails closed instead of using unsafe generic continue behavior.
    pub async fn reconcile_claude_resume_ids_once(
        &self,
    ) -> Result<ClaudeResumeHarvestReport, NexusError> {
        let report = harvest_missing_claude_resume_ids(&self.store, &default_home()).await?;
        log_claude_resume_harvest_report(&report);
        Ok(report)
    }

    /// Boot-time presume-dead pass, run AFTER PTY adoption. Every presence claim in
    /// the store predates the restart, and the shutdown-side offline sweep is best-effort
    /// (it can lose the store-write versus teardown race, leaving phantom-online rows).
    /// Adoption re-registers the transports that
    /// actually survived; any other agent session still claiming liveness — including
    /// CLI/MCP peers with no transport label, which the registry-scoped reconcile skips —
    /// is a fossil and flips offline now instead of waiting out the heartbeat TTL. Peers
    /// that are actually alive come back on their next authenticated command.
    pub async fn reconcile_boot_presume_dead_once(&self) -> Result<(), NexusError> {
        let rows = Sessions::new(&self.store).list_all().await?;
        let registry = self.presence.registry();
        for row in rows {
            if !row.is_agent() {
                continue;
            }
            if !matches!(row.presence.as_deref(), Some("online") | Some("busy")) {
                continue;
            }
            if registry.is_present(&row.session_id) {
                continue;
            }
            self.presence
                .mark_transport_offline(&row.session_id)
                .await?;
        }
        Ok(())
    }

    /// Rebuild daemon-owned presence after a boot from the in-memory transport registry.
    ///
    /// The registry starts empty on daemon restart; rows with a durable transport label are only
    /// present again after a successful reattach/adoption registers a live handle. CLI/MCP peers
    /// have no daemon transport handle, so they stay governed by their heartbeat path.
    pub async fn reconcile_transport_presence_from_registry_once(&self) -> Result<(), NexusError> {
        let rows = Sessions::new(&self.store).list_all().await?;
        let registry = self.presence.registry();
        for row in rows {
            if !row.is_agent() || row.transport.is_none() {
                continue;
            }
            if registry.is_present(&row.session_id) {
                continue;
            }
            self.presence
                .mark_transport_offline(&row.session_id)
                .await?;
        }
        Ok(())
    }

    /// Run one stale-presence reconcile pass using the configured heartbeat TTL.
    ///
    /// This is the daemon-side twin of the read-model's derived presence rule: a missing or expired
    /// heartbeat makes a raw `online`/`busy` row effectively offline. The durable pass also stops
    /// active runtime rows whose corresponding session has gone stale so rosters and runtime admin
    /// views converge after daemon restarts. Each stale row is applied through [`PresenceWriter`]
    /// so the durable transition and its ordered `sys.fleet.status` fact cannot diverge.
    pub async fn reconcile_stale_presence_once(&self) -> Result<(), NexusError> {
        let ts = now();
        self.archive_stale_claude_runtimes(ts).await;
        self.archive_stale_codex_runtimes(ts).await;
        self.presence
            .reconcile_stale_presence(ts, self.heartbeat_ttl_ms)
            .await?;
        if !self.store.has_split_authority() {
            crate::daemon::agent_session_materializer::abort_open_turns_for_offline_sessions(
                &self.store,
                ts,
            )
            .await?;
        }
        Ok(())
    }

    /// Make a live agent session wakeable in the in-memory realtime registry, then ensure its
    /// drain loop exists and is rung.
    ///
    /// Durable liveness alone is not enough after a prior offline transition: `mark_session_offline`
    /// leaves the realtime registry at `Offline`, and `DispatchService::enqueue` deliberately holds offline
    /// sessions without ringing. Every live restore/re-register path that wants pending bus rows to
    /// drain must clear that stale hold before it spawns/rings the loop. Paused sessions remain held:
    /// we stand up the loop for future resume, but keep the registry at `Paused` and do not issue an
    /// explicit ring.
    pub(crate) async fn make_live_agent_wakeable(
        &self,
        session: &SessionId,
        project: &str,
        paused: bool,
    ) {
        let Some(w) = &self.loop_wiring else { return };
        let transition = w.store.lock_presence_transition().await;
        w.registry.set(session, session_wake_state(paused));
        w.spawn_loop_under_transition(session, project, &transition);
        if !paused {
            w.ring(session);
        }
    }

    /// Mark one session offline through the complete daemon-owned liveness path.
    ///
    /// This is stronger than `IdentityPort::set_offline`: it updates the compatibility session row,
    /// stops the active runtime row, aborts any materialized open turn, updates the in-memory wake
    /// registry, removes the heartbeat-keeper spawn guard, releases native forwarder guards, and emits
    /// `agent.status` so member projections do not wait for polling.
    pub async fn mark_session_offline(&self, session: &SessionId) -> Result<(), NexusError> {
        if let Some(w) = &self.loop_wiring {
            w.teardown_session_transports(session);
        }

        self.presence.mark_transport_offline(session).await?;
        Ok(())
    }

    /// Apply the same offline transition used by the heartbeat keeper when a harness probe reports
    /// a definitely dead transport.
    pub async fn handle_dead_harness_probe(&self, session: &SessionId) -> Result<(), NexusError> {
        if let Some(w) = &self.loop_wiring {
            w.teardown_session_transports(session);
        }
        self.presence.mark_dead_harness_offline(session).await
    }

    async fn archive_stale_claude_runtimes(&self, ts: i64) {
        if self.store.has_split_authority() {
            return;
        }
        let runtimes = match AgentRuntimes::new(&self.store)
            .list_active_by_harness_transport("claude", "pty")
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::transcript_archive",
                    error = %error,
                    "failed to list active Claude runtimes before stale reconcile archive"
                );
                return;
            }
        };
        let sessions = Sessions::new(&self.store);
        for runtime in runtimes {
            let session_id = SessionId(runtime.runtime_id.clone());
            match sessions.find_by_session_id(&session_id).await {
                Ok(Some(session)) if self.is_stale_runtime_row(&runtime, &session, ts) => {
                    if let Err(error) = archive_claude_once(
                        self.store.clone(),
                        &session_id,
                        Some("stale_reconcile".to_string()),
                    )
                    .await
                    {
                        tracing::warn!(
                            target: "nexus::transcript_archive",
                            session = %session_id,
                            error = %error,
                            "failed to final-flush Claude transcript before stale reconcile"
                        );
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        target: "nexus::transcript_archive",
                        session = %session_id,
                        error = %error,
                        "failed to load session before stale reconcile archive"
                    );
                }
            }
        }
    }

    async fn archive_stale_codex_runtimes(&self, ts: i64) {
        if self.store.has_split_authority() {
            return;
        }
        let runtimes = match AgentRuntimes::new(&self.store)
            .list_active_by_harness_transport("codex", "codex-appserver")
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::transcript_archive",
                    error = %error,
                    "failed to list active Codex runtimes before stale reconcile archive"
                );
                return;
            }
        };
        let sessions = Sessions::new(&self.store);
        for runtime in runtimes {
            let session_id = SessionId(runtime.runtime_id.clone());
            match sessions.find_by_session_id(&session_id).await {
                Ok(Some(session)) if self.is_stale_runtime_row(&runtime, &session, ts) => {
                    if let Err(error) = archive_codex_once(
                        self.store.clone(),
                        &session_id,
                        Some("stale_reconcile".to_string()),
                    )
                    .await
                    {
                        tracing::warn!(
                            target: "nexus::transcript_archive",
                            session = %session_id,
                            error = %error,
                            "failed to final-flush Codex rollout before stale reconcile"
                        );
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        target: "nexus::transcript_archive",
                        session = %session_id,
                        error = %error,
                        "failed to load session before stale reconcile archive"
                    );
                }
            }
        }
    }

    fn is_stale_runtime_row(
        &self,
        runtime: &AgentRuntimeRow,
        session: &SessionRow,
        ts: i64,
    ) -> bool {
        if session.presence.as_deref() == Some("offline") {
            return true;
        }
        // Birth counts as the first heartbeat: a codex-appserver launch has NO heartbeat until
        // the agent's own MCP registers. Treating NULL as stale would incorrectly stop a
        // fresh runtime at its first reconcile tick.
        let heartbeat = runtime
            .last_heartbeat
            .or(session.last_heartbeat)
            .or(Some(runtime.started_at.max(session.created_at)));
        nexus_common::presence::is_stale(heartbeat, ts, self.heartbeat_ttl_ms)
    }

    /// Periodically collapse stale raw presence rows to durable offline state.
    pub(crate) fn spawn_presence_reconciler(&self) {
        let st = self.clone();
        let interval_ms = (self.heartbeat_ttl_ms.max(2) / 2) as u64;
        tokio::spawn(async move {
            if let Err(error) = st.boot_readiness.wait_initial_presence().await {
                tracing::error!(target: "nexus::presence", %error,
                    "boot did not reach initial presence reconciliation; periodic reconcile not started");
                return;
            }
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            // Start the cadence only after adoption and both best-effort initial attempts settle.
            tick.tick().await;
            loop {
                tick.tick().await;
                if let Err(error) = st.reconcile_stale_presence_once().await {
                    tracing::warn!(
                        target: "nexus::presence",
                        error = %error,
                        "failed to reconcile stale presence"
                    );
                }
                // The same cadence re-drives durable pre-injection deliveries after their revive
                // backoff expires. This keeps bootstrap recovery independent of new bus traffic.
                st.respawn_pending_agents().await;
            }
        });
    }
}
