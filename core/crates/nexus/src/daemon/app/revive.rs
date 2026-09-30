//! Harness-neutral revive routing and headed transport reattachment.

use super::*;

impl AppState {
    /// Close model admission and await this state's tracked model settlement after transport
    /// teardown. The budget bounds only this waiter: cancellation/timeout does not abort owned
    /// work, certify rollback, or guarantee settlement after the runtime/process exits.
    pub async fn drain_model_reporting_for_shutdown(
        &self,
        budget: std::time::Duration,
    ) -> Result<(), NexusError> {
        self.model_reporting.shutdown(budget).await
    }

    /// UNIFIED REVIVE — edge wrapper that resolves `name-or-id` once, then dispatches by row.
    ///
    /// - `transport = "acp"` → [`AppState::ensure_live`] (re-open/resume ACP session).
    /// - `transport = "pty"` → [`AppState::ensure_harness_live`] (respawn tmux).
    /// - `transport = "codex-appserver"` → [`AppState::ensure_codex_appserver_live`].
    /// - `transport = "opencode-plugin"` → [`AppState::ensure_opencode_plugin_live`].
    /// - `NULL` / unknown legacy rows → ACP.
    ///
    /// Name fallback remains for fossil compatibility rows with no durable identity, but every
    /// populated `agent_id` path selects the active runtime by id before touching transport state.
    pub async fn ensure_alive(
        &self,
        name_or_id: &str,
        project: &str,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let agents = Agents::new(&self.store);
        let parsed = AgentRef::parse(name_or_id);
        if let AgentRef::Id(agent_id) = &parsed {
            if agents
                .find_by_id(agent_id)
                .await
                .map_err(|error| error.to_contract_error())?
                .is_some()
            {
                return self.ensure_alive_agent(agent_id).await;
            }
        }
        match agents
            .resolve_ref("", &AgentRef::Name(name_or_id.to_string()), true)
            .await
        {
            Ok(agent) => match self.ensure_alive_agent(&agent.agent_id).await {
                Ok(session) => Ok(session),
                Err(error) if error.code == nexus_contracts::codes::NOT_FOUND => {
                    let row = self.resolve_session_by_name(name_or_id, project).await?;
                    self.ensure_alive_row(row).await
                }
                Err(error) => Err(error),
            },
            Err(NexusError::NotFound(_)) => {
                let row = self.resolve_session_by_name(name_or_id, project).await?;
                self.ensure_alive_row(row).await
            }
            Err(error) => Err(error.to_contract_error()),
        }
    }

    /// Revive the active runtime for a stable durable identity without re-resolving its name.
    ///
    /// If the runtime was deliberately stopped by daemon liveness convergence, fall back to the
    /// newest compatibility session row so thread wake and boot-pending replay can respawn the same
    /// durable agent instead of stranding its queued rows behind an inactive runtime.
    pub async fn ensure_alive_agent(
        &self,
        agent_id: &str,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let sessions = Sessions::new(&self.store);
        let row = sessions
            .active_runtime_session_for_agent(agent_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        let row = match row {
            Some(row) => row,
            None => sessions
                .find_by_agent_id(agent_id)
                .await
                .map_err(|e| e.to_contract_error())?
                .ok_or_else(|| nexus_contracts::ContractError {
                    code: nexus_contracts::codes::NOT_FOUND,
                    message: format!("agent id {agent_id} has no runtime session"),
                })?,
        };
        self.ensure_alive_row(row).await
    }

    pub(crate) async fn ensure_codex_resume_row_live(
        &self,
        row: SessionRow,
        thread_id: &str,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        if row.agent.as_deref() == Some("codex")
            || row.transport.as_deref() == Some("codex-appserver")
        {
            self.ensure_codex_appserver_live_row_with_thread(row, Some(thread_id))
                .await
        } else {
            self.ensure_alive_row(row).await
        }
    }

    pub(crate) async fn ensure_alive_row(
        &self,
        row: SessionRow,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        self.ensure_alive_row_with_native_request(row, None).await
    }

    /// Forward a captured native request through the selected runtime host, including ID reuse.
    /// Hosts without this request interface must not silently discard an explicit constraint.
    pub(crate) async fn ensure_alive_row_with_native_request(
        &self,
        row: SessionRow,
        requested_key: Option<&str>,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let route = revive_route(row.transport.as_deref());
        if requested_key.is_some() && route != ReviveRoute::OpenCodePlugin {
            return Err(NexusError::Invalid(
                "selected runtime host does not accept this native resume request".into(),
            )
            .to_contract_error());
        }
        match route {
            ReviveRoute::Acp => self.ensure_live_row(row).await,
            ReviveRoute::Pty => self.ensure_harness_live_row(row).await,
            ReviveRoute::CodexAppServer => self.ensure_codex_appserver_live_row(row).await,
            ReviveRoute::OpenCodePlugin => {
                self.ensure_opencode_plugin_live_row_with_key(row, requested_key)
                    .await
            }
        }
    }

    /// REVIVE-ON-INTERACTION for headed Codex app-server sessions. Reuses the durable Nexus session
    /// id, adopts a live `codex app-server` or starts one when absent, resumes the stored thread id,
    /// launches the human TUI with `codex resume --remote`, rebinds the structured transport, and
    /// rings the drain loop.
    pub async fn ensure_codex_appserver_live(
        &self,
        name: &str,
        project: &str,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let row = self.resolve_session_by_name(name, project).await?;
        self.ensure_codex_appserver_live_row(row).await
    }

    pub(crate) async fn ensure_codex_appserver_live_row(
        &self,
        row: SessionRow,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        self.ensure_codex_appserver_live_row_with_thread(row, None)
            .await
    }

    pub(crate) async fn ensure_codex_appserver_live_row_with_thread(
        &self,
        mut row: SessionRow,
        requested_thread_id: Option<&str>,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let name = row.name.clone().ok_or_else(|| {
            NexusError::Invalid("cannot revive unnamed Codex runtime".into()).to_contract_error()
        })?;
        let session = row.session_id.clone();
        let project = row.project.clone();
        let client_key = row.client_key.clone().unwrap_or_else(|| session.0.clone());

        if !row.is_agent() {
            return Ok(session);
        }
        let requested_thread_id = requested_thread_id.map(str::to_string);
        if let Some(thread_id) = requested_thread_id.as_deref() {
            let claimed = persist_codex_thread_binding_once(&self.store, &session, thread_id)
                .await
                .map_err(|e| e.to_contract_error())?;
            if !claimed {
                tracing::warn!(
                    target: "nexus::codex",
                    session = %session,
                    thread_id = %thread_id,
                    "codex resume restored thread id but could not claim durable binding because the session has no agent_id"
                );
            }
            row.harness_session_id = Some(thread_id.to_string());
        }
        let Some(supervisor) = self.pty.clone() else {
            return Ok(session);
        };
        // Boot adoption and durable prompt recovery can both discover the same dead structured
        // runtime. Collapse them into one revive, then re-check the final transport binding while
        // still holding the shared gate.
        let _revive = self.runtime_revive_gate.acquire(&session).await;
        let transport = supervisor.codex_transport();
        if transport.is_bound(&session) {
            let bound_thread_id = transport.bound_thread_id(&session);
            let bound_to_requested_thread = match requested_thread_id.as_deref() {
                Some(thread_id) => bound_thread_id.as_deref() == Some(thread_id),
                None => bound_thread_id.is_some(),
            };
            if bound_to_requested_thread {
                self.make_live_agent_wakeable(&session, &project, row.paused)
                    .await;
                return Ok(session);
            }
        }
        let reporting_agent = self.capture_model_reporting_agent(&row).await?;
        let mut model_observation = self.reserve_native_observation(
            &reporting_agent,
            &session,
            nexus_harness_codex::app_server::model_reporting::profile(),
        )?;
        model_observation.commit(self).await?;
        let events = self
            .loop_wiring
            .as_ref()
            .map(|w| w.events.clone())
            .ok_or_else(|| nexus_contracts::ContractError {
                code: -32004,
                message: "codex app-server revive requires loop wiring (events sink)".into(),
            })?;

        let cwd = row
            .cwd
            .clone()
            .unwrap_or_else(|| default_agent_cwd_for(row.agent_id.as_deref(), &name));
        ensure_agent_launch_cwd(&cwd, row.cwd.is_none());
        let nexus_exe = std::env::current_exe()
            .ok()
            .and_then(|p| p.to_str().map(String::from))
            .unwrap_or_else(|| "nexus".to_string());
        let codex_state = nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&self.store)
            .find_by_runtime_id(&session)
            .await
            .map_err(|e| e.to_contract_error())?;
        let capsule = if self.store.has_split_authority() {
            IdentitySessions::new(&self.store)
                .find(&session.0)
                .await
                .map_err(|e| e.to_contract_error())?
        } else {
            None
        };
        let viewer_backend = if codex_state.is_some() {
            nexus_harness_codex::storage::codex_runtime_viewer_backend(codex_state.as_ref())
        } else {
            capsule
                .as_ref()
                .and_then(|capsule| capsule.backend.as_deref())
                .unwrap_or("pty")
        };
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let revive_seed = nexus_harness_codex::storage::codex_runtime_revive_seed(
            &std::path::PathBuf::from(&home),
            &session,
            codex_state.as_ref(),
            row.harness_session_id.as_deref(),
        );
        let resume_codex_homes: Vec<std::path::PathBuf> = codex_state
            .as_ref()
            .and_then(|state| state.codex_home.clone())
            .into_iter()
            .collect();
        let state_dir = revive_seed.state_dir.to_string_lossy().into_owned();
        let rollout_root = revive_seed.rollout_root;
        let thread_id = requested_thread_id
            .or(revive_seed.thread_id)
            .or_else(|| nexus_harness_codex::latest_rollout_thread_id(&rollout_root))
            .ok_or_else(|| nexus_contracts::ContractError {
                code: -32004,
                message: format!(
                    "cannot revive headed codex session {session}: no stored codex thread id and no rollout under {}",
                    rollout_root.display()
                ),
            })?;

        let size = portable_pty::PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        };
        supervisor
            .respawn_codex_appserver(
                &session,
                &reporting_agent,
                &name,
                &project,
                &client_key,
                &nexus_exe,
                &cwd,
                size,
                events,
                "codex",
                None,
                &state_dir,
                Some(thread_id.clone()),
                resume_codex_homes,
                Some(self.codex_thread_persistence_callback()),
                viewer_backend,
                Some(model_observation.reporting()),
            )
            .await
            .map_err(|e| nexus_contracts::ContractError {
                code: -32004,
                message: format!("codex app-server respawn failed: {e}"),
            })?;

        if transport.bound_thread_id(&session).as_deref() != Some(thread_id.as_str()) {
            return Err(nexus_contracts::ContractError {
                code: -32004,
                message: format!(
                    "codex app-server respawn returned without binding session {session} to thread {thread_id}"
                ),
            });
        }

        self.finish_codex_appserver_resume(&name, &session, &project, row.paused)
            .await?;
        model_observation.activate_codex(self, &transport).await?;
        Ok(session)
    }

    /// Restamp a revived Codex app-server row as live and publish best-effort resume telemetry.
    ///
    /// The liveness transition is load-bearing for prompt delivery; the lifecycle developer event is
    /// metadata-only and must never fail the revive path.
    #[doc(hidden)]
    pub async fn finish_codex_appserver_resume(
        &self,
        name: &str,
        session: &SessionId,
        project: &str,
        paused: bool,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        // The drainer intentionally ignores agent-owned pending rows while the durable session is
        // offline. Materialize liveness before spawning/ringing the loop; doing this afterward is
        // a race where the first (and only) wake drains zero rows and mail waits for another post.
        self.presence.materialize_online_for_resume(session).await?;
        self.make_live_agent_wakeable(session, project, paused)
            .await;
        self.append_agent_lifecycle_best_effort(name, session, "resume", "codex app-server resume")
            .await;
        Ok(session.clone())
    }

    /// REVIVE-ON-INTERACTION for headed OpenCode native-plugin sessions. Reuses the durable Nexus
    /// session id, restarts the loopback plugin bridge, resumes the stored native OpenCode
    /// `ses_*` id in that runtime's launch-local DB, rebinds structured turn delivery, and rings
    /// the drain loop. A plain tmux adoption is intentionally not enough for this transport: after
    /// a daemon restart the old plugin still points at the dead daemon's loopback URL/token, so
    /// adopting the pane would route turns to the wrong input and lose completion callbacks.
    pub async fn ensure_opencode_plugin_live(
        &self,
        name: &str,
        project: &str,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let row = self.resolve_session_by_name(name, project).await?;
        self.ensure_opencode_plugin_live_row(row).await
    }

    pub(crate) async fn ensure_opencode_plugin_live_row(
        &self,
        row: SessionRow,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        self.ensure_opencode_plugin_live_row_with_key(row, None)
            .await
    }

    pub(crate) async fn ensure_opencode_plugin_live_row_with_key(
        &self,
        row: SessionRow,
        requested_key: Option<&str>,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let name = row.name.clone().ok_or_else(|| {
            NexusError::Invalid("cannot revive unnamed OpenCode runtime".into()).to_contract_error()
        })?;
        let session = row.session_id.clone();
        let project = row.project.clone();
        let client_key = row.client_key.clone().unwrap_or_else(|| session.0.clone());

        if !row.is_agent() {
            return Ok(session);
        }
        let Some(supervisor) = self.pty.clone() else {
            return Ok(session);
        };
        // Boot adoption and delivery recovery run independently. Collapse their resurrection of
        // this structured runtime into one launch, then re-check liveness under the gate so the
        // waiter reuses the bridge/viewer pair installed by the owner.
        let _revive = self.runtime_revive_gate.acquire(&session).await;
        let kind = harness_from_token(row.agent.as_deref());
        if headed_runtime_kind(&kind) != HeadedRuntimeKind::OpenCodePlugin {
            return Err(NexusError::Invalid(format!(
                "cannot revive runtime {session}: harness metadata does not match plugin transport"
            ))
            .to_contract_error());
        }
        let harness = harness_registry_by_id(&kind);
        let reporting_agent = self.capture_model_reporting_agent(&row).await?;
        let opencode_state = OpenCodeRuntimeStateRepo::new(&self.store)
            .find_by_runtime_id(&session)
            .await
            .map_err(|e| e.to_contract_error())?;
        let capsule = if self.store.has_split_authority() {
            IdentitySessions::new(&self.store)
                .find(&session.0)
                .await
                .map_err(|e| e.to_contract_error())?
        } else {
            None
        };
        let resume = self
            .native_resume_plan_for_row(
                &row,
                harness,
                &reporting_agent,
                capsule.as_ref(),
                opencode_state
                    .as_ref()
                    .and_then(|state| state.opencode_session_id.as_deref()),
                requested_key,
            )
            .await?;
        if supervisor.has_opencode_plugin(&session)
            && supervisor.transport().is_harness_alive(&session) == Some(true)
        {
            // Validate the requested identity before reuse; retain the existing model owner.
            let _ = self
                .attach_opencode_plugin_session_machinery(&session, &project)
                .await;
            return Ok(session);
        }
        let events = self
            .loop_wiring
            .as_ref()
            .map(|w| w.events.clone())
            .ok_or_else(|| nexus_contracts::ContractError {
                code: -32004,
                message: "plugin revive requires loop wiring (events sink)".into(),
            })?;
        let cwd = row
            .cwd
            .clone()
            .unwrap_or_else(|| default_agent_cwd_for(row.agent_id.as_deref(), &name));
        ensure_agent_launch_cwd(&cwd, row.cwd.is_none());
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        let state_dir = format!("{home}/.nexus");
        let viewer_backend = opencode_state
            .as_ref()
            .map(|state| state.viewer_backend.as_str())
            .or_else(|| {
                capsule
                    .as_ref()
                    .and_then(|capsule| capsule.backend.as_deref())
            })
            .unwrap_or("tmux");
        let size = portable_pty::PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        };
        let mut model_observation = self.reserve_native_observation(
            &reporting_agent,
            &session,
            nexus_agent::adapter::opencode::native::model_profile(),
        )?;
        model_observation.commit(self).await?;
        supervisor
            .launch_opencode_plugin(
                &session,
                row.agent_id.as_deref().unwrap_or(&name),
                Some(&name),
                &project,
                &client_key,
                &cwd,
                size,
                events,
                &state_dir,
                &resume.argv,
                Some(&resume.native_key),
                resume.store == nexus_harness_core::NativeResumeStore::OriginalRuntime,
                viewer_backend,
                Some(model_observation.reporting()),
            )
            .await
            .map_err(|e| nexus_contracts::ContractError {
                code: -32004,
                message: format!("opencode native-plugin revive failed: {e}"),
            })?;

        if !supervisor.has_opencode_plugin(&session)
            || supervisor.transport().is_harness_alive(&session) != Some(true)
        {
            return Err(nexus_contracts::ContractError {
                code: -32004,
                message: format!("timed out rebinding opencode plugin session {session}"),
            });
        }

        self.attach_opencode_plugin_session_machinery(&session, &project)
            .await?;
        model_observation.activate_plugin(self, &supervisor).await?;
        Ok(session)
    }

    /// REVIVE-ON-INTERACTION: ensure the named PTY-backed agent's HARNESS is alive, respawning it
    /// with an exact native resume id when required. Called on the
    /// delivery paths — a DM `prompt` or a thread post to the agent — so messaging a dead agent
    /// transparently brings it back BEFORE the message is injected. Reuses the session id (the
    /// `/agent/<name>:<sid>` view + history carry over), restarts the appropriate output forwarder
    /// (Claude native JSONL or generic PTY scraper) + drain loop, and stamps it online.
    ///
    /// Headed Codex is explicitly excluded from this generic tmux adoption path. A daemon restart
    /// can leave both the remote TUI pane and `codex.sock` alive, but adopting either as a plain PTY
    /// leaves the new daemon without a trustworthy Codex `turn/completed` stream. Codex rows must
    /// revive through [`Self::ensure_codex_appserver_live`], which adopts or relaunches the
    /// structured app-server bridge and binds a fresh completion forwarder.
    pub async fn ensure_harness_live(
        &self,
        name: &str,
        project: &str,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let row = self.resolve_session_by_name(name, project).await?;
        self.ensure_harness_live_row(row).await
    }

    pub(crate) async fn ensure_harness_live_row(
        &self,
        row: SessionRow,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let name = row.name.clone().ok_or_else(|| {
            NexusError::Invalid("cannot revive unnamed harness runtime".into()).to_contract_error()
        })?;
        let session = row.session_id.clone();
        let project = row.project.clone();
        let client_key = row.client_key.clone().unwrap_or_else(|| session.0.clone());

        // Only AGENTS have a harness to revive — a DM/whatever can target an operator/app member.
        if !row.is_agent() {
            return Ok(session);
        }
        if row.agent.as_deref() == Some("codex")
            || row.transport.as_deref() == Some("codex-appserver")
        {
            return self.ensure_codex_appserver_live_row(row).await;
        }
        // No supervisor (ACP/mock) → nothing to respawn; just hand back the id.
        let Some(supervisor) = self.pty.clone() else {
            return Ok(session);
        };
        // Already alive → nothing to do. `Some(true)` means a live bound harness; `None` (no probe)
        // is treated as "leave it" — the ACP path doesn't run here.
        if self.agent.is_harness_alive(&session) != Some(false) {
            return Ok(session);
        }

        let kind = harness_from_token(row.agent.as_deref());
        let harness = harness_registry_by_id(&kind);
        let program = harness.program();
        if program.is_empty() {
            return Err(nexus_contracts::ContractError {
                code: nexus_contracts::codes::INVALID_PARAMS,
                message: format!("harness {kind:?} has no headed TUI program"),
            });
        }
        let cwd = row
            .cwd
            .clone()
            .unwrap_or_else(|| default_agent_cwd_for(row.agent_id.as_deref(), &name));
        ensure_agent_launch_cwd(&cwd, row.cwd.is_none());

        let capsule = if self.store.has_split_authority() {
            IdentitySessions::new(&self.store)
                .find(&session.0)
                .await
                .map_err(|e| e.to_contract_error())?
        } else {
            None
        };
        let viewer_backend = match kind.as_str() {
            "hermes" => HermesRuntimeStateRepo::new(&self.store)
                .find_by_runtime_id(&session)
                .await
                .map_err(|e| e.to_contract_error())?
                .map(|state| state.viewer_backend)
                .or_else(|| capsule.as_ref().and_then(|capsule| capsule.backend.clone()))
                .unwrap_or_else(|| "tmux".to_string()),
            "claude" => {
                let state = nexus_harness_claude::storage::ClaudeRuntimeStateRepo::new(&self.store)
                    .find_by_runtime_id(&session)
                    .await
                    .map_err(|e| e.to_contract_error())?;
                if state.as_ref().is_some_and(|state| {
                    state.tmux_socket.is_some() || state.tmux_session.is_some()
                }) {
                    "tmux".to_string()
                } else if state.is_some() {
                    "pty".to_string()
                } else {
                    capsule
                        .as_ref()
                        .and_then(|capsule| capsule.backend.clone())
                        // Pre-split stores did not have a runtime capsule. Their headed Claude
                        // launcher used deterministic tmux, so retain that legacy adoption shape
                        // when neither the sidecar nor the capsule can identify a backend.
                        .unwrap_or_else(|| "tmux".to_string())
                }
            }
            _ => capsule
                .as_ref()
                .and_then(|capsule| capsule.backend.clone())
                .unwrap_or_else(|| "tmux".to_string()),
        };

        // If the daemon restarted while the tmux session survived, the in-memory PTY binding is gone
        // but the deterministic tmux session is still the correct runtime. Adopt it before respawn so
        // warm/revive remains a no-op for live headed harnesses.
        if viewer_backend == "tmux"
            && supervisor
                .adopt_pty_backend(&session, &cwd, headed_runtime_kind(&kind))
                .is_ok()
        {
            self.attach_pty_session_machinery(
                &supervisor,
                &session,
                &project,
                row.agent.as_deref(),
                row.paused,
            )
            .await;
            return Ok(session);
        }

        // Dead → respawn the harness for THIS session. Native-session harnesses must resume by
        // exact durable id; generic cwd-based continuation can attach the wrong native session.
        let nexus_exe = std::env::current_exe()
            .ok()
            .and_then(|p| p.to_str().map(String::from))
            .unwrap_or_else(|| "nexus".to_string());
        let mut gateway_observation =
            if headed_runtime_kind(&kind) == HeadedRuntimeKind::HermesGateway {
                Some(
                    self.prepare_gateway_resume(
                        &row,
                        nexus_agent::adapter::hermes::native::model_profile(),
                    )
                    .await?,
                )
            } else {
                None
            };
        // The native gateway restores its launch-local session-key store, not a CLI --session
        // argument. The captured observer rejects any different root selected by that framework.
        let revive_tail = if gateway_observation.is_some() {
            Vec::new()
        } else {
            self.headed_revive_tail_for_row(&row, &kind).await?
        };
        let viewer_backend = match kind.as_str() {
            "hermes" => HermesRuntimeStateRepo::new(&self.store)
                .find_by_runtime_id(&session)
                .await
                .map_err(|e| e.to_contract_error())?
                .map(|state| state.viewer_backend)
                .unwrap_or_else(|| "tmux".to_string()),
            "claude" => {
                let state = nexus_harness_claude::storage::ClaudeRuntimeStateRepo::new(&self.store)
                    .find_by_runtime_id(&session)
                    .await
                    .map_err(|e| e.to_contract_error())?;
                if state.as_ref().is_some_and(|state| {
                    state.tmux_socket.is_some() || state.tmux_session.is_some()
                }) {
                    "tmux".to_string()
                } else {
                    "pty".to_string()
                }
            }
            _ => "tmux".to_string(),
        };
        if kind.as_str() == "claude" {
            supervisor.arm_claude_startup_wait(&session);
        }
        supervisor.kill(&session);
        let respawn = self
            .respawn_stored_terminal(
                &supervisor,
                &row,
                &name,
                &client_key,
                &kind,
                &viewer_backend,
                &nexus_exe,
                &cwd,
                &revive_tail,
                gateway_observation
                    .as_ref()
                    .map(|observation| observation.reporting()),
            )
            .await;
        if let Err(error) = respawn {
            supervisor.clear_claude_startup_wait(&session);
            return Err(nexus_contracts::ContractError {
                code: -32004,
                message: format!("harness {viewer_backend} respawn failed: {error}"),
            });
        }
        if kind.as_str() == "claude" {
            supervisor
                .wait_for_claude_startup(&session)
                .map_err(|message| nexus_contracts::ContractError {
                    code: -32004,
                    message,
                })?;
        }

        if let Some(observation) = gateway_observation.as_mut() {
            self.attach_pty_session_machinery_checked(
                &supervisor,
                &session,
                &project,
                row.agent.as_deref(),
                row.paused,
            )
            .await?;
            observation.finish_gateway(self, &supervisor).await?;
        } else {
            self.attach_pty_session_machinery(
                &supervisor,
                &session,
                &project,
                row.agent.as_deref(),
                row.paused,
            )
            .await;
        }
        Ok(session)
    }

    #[doc(hidden)]
    pub async fn headed_revive_tail_for_row(
        &self,
        row: &SessionRow,
        kind: &HarnessId,
    ) -> Result<Vec<String>, nexus_contracts::ContractError> {
        let harness = harness_registry_by_id(kind);
        if let nexus_harness_core::ResumeStyle::Flag(prefix) = harness.resume_style() {
            // The native session-id store for flag-resume harnesses is currently the
            // legacy native runtime repo; a future flag-style harness must route its own
            // durable key through the registry (P3).
            let state = nexus_harness_claude::storage::ClaudeRuntimeStateRepo::new(&self.store)
                .find_by_runtime_id(&row.session_id)
                .await
                .map_err(|e| e.to_contract_error())?;
            let capsule = if self.store.has_split_authority() {
                IdentitySessions::new(&self.store)
                    .find(&row.session_id.0)
                    .await
                    .map_err(|e| e.to_contract_error())?
            } else {
                None
            };
            let Some(native_session_id) = capsule
                .and_then(|capsule| capsule.native_resume_key)
                .or_else(|| state.and_then(|state| state.claude_session_id))
                .or_else(|| row.harness_session_id.clone())
                .filter(|id| !id.is_empty())
            else {
                return Err(nexus_contracts::ContractError {
                    code: nexus_contracts::codes::INVALID_PARAMS,
                    message: format!(
                        "cannot revive headed {} {}:{} without a stored {} session id; refusing unsafe --continue",
                        harness.program(), row.display_name(), row.session_id.0, prefix.join(" ")
                    ),
                });
            };
            let mut tail: Vec<String> = prefix.iter().map(|s| s.to_string()).collect();
            tail.push(native_session_id);
            return Ok(tail);
        }

        harness
            .revive_tail()
            .map(|tail| tail.argv)
            .map_err(|e| nexus_contracts::ContractError {
                code: nexus_contracts::codes::INVALID_PARAMS,
                message: format!("harness revive tail failed: {e}"),
            })
    }

    pub(super) async fn attach_pty_session_machinery_checked(
        &self,
        supervisor: &Arc<PtySupervisor>,
        session: &SessionId,
        project: &str,
        harness: Option<&str>,
        paused: bool,
    ) -> Result<(), nexus_contracts::ContractError> {
        // Pending agent-owned rows are intentionally invisible while the durable runtime is
        // offline. Restamp liveness BEFORE spawning/ringing the event loop; `spawn_loop` rings on
        // attach, and spending that wake against an offline row drains nothing and strands the
        // original notification until an unrelated later message arrives.
        if let Err(error) = self.mark_rebound_agent_live(session).await {
            tracing::warn!(
                target: "nexus::revive",
                session = %session,
                error = ?error,
                "failed to restamp headed PTY runtime live before wake; refusing to ring"
            );
            return Err(error);
        }

        // Spawn a fresh PtyReplyReader for the newly-bound tmux harness: the old one exited when the
        // previous daemon lost its PTY channel. On revive/adoption we hook `pipe_output()` on the
        // bound TmuxHarness and start a reader task. The drain loop ring pushes any queued mail.
        //
        // NOTE: app-server sessions (transport = "codex-appserver") are
        // routed to `ensure_codex_appserver_live`, never here. Guard the reply-reader call anyway so
        // any legacy codex row (transport = "pty" from before this dispatch flip) does NOT accidentally
        // get the scrape reader re-attached on revive.
        if let Some(w) = &self.loop_wiring {
            self.spawn_raw_stream_writer_for_terminal(session).await;
            match headed_runtime_from_agent_token(harness) {
                HeadedRuntimeKind::ClaudeNative => {
                    self.spawn_claude_native_forwarder_if_needed(session).await;
                }
                HeadedRuntimeKind::OpenCodePlugin => {
                    self.spawn_opencode_native_forwarder_if_needed(session)
                        .await;
                }
                HeadedRuntimeKind::HermesGateway => {
                    self.spawn_hermes_native_forwarder_if_needed(session).await;
                }
                HeadedRuntimeKind::CodexAppServer => {}
                HeadedRuntimeKind::Screen => {
                    // Get the revived/adopted TmuxHarness's output stream via pipe-pane.
                    let pty_output: broadcast::Receiver<Vec<u8>> =
                        supervisor.pty_output(session).unwrap_or_else(|| {
                            let (_, rx) = broadcast::channel::<Vec<u8>>(1);
                            rx
                        });
                    // Structured harnesses are handled by native forwarders above; apply the screen
                    // scraper to the remaining PTY harnesses.
                    spawn_pty_reply_reader(
                        session.to_owned(),
                        pty_output,
                        w.events.clone(),
                        w.bell.clone(),
                        1500,
                    );
                }
            }
            // Publish wake state and the loop together, after native setup/I/O has finished.
            let transition = w.store.lock_presence_transition().await;
            w.registry.set(
                session,
                if paused {
                    nexus_dispatch::AgentState::Paused
                } else {
                    nexus_dispatch::AgentState::Idle
                },
            );
            w.spawn_loop_under_transition(session, project, &transition);
            w.ring(session);
        }
        if let Err(error) = self
            .append_agent_lifecycle_for_session(session, "resume")
            .await
        {
            tracing::warn!(
                target: "nexus::revive",
                session = %session,
                error = ?error,
                "failed to append headed PTY resume lifecycle event after adoption"
            );
        }
        Ok(())
    }

    pub(crate) async fn attach_opencode_plugin_session_machinery(
        &self,
        session: &SessionId,
        project: &str,
    ) -> Result<(), nexus_contracts::ContractError> {
        self.spawn_raw_stream_writer_for_terminal(session).await;
        self.spawn_opencode_native_forwarder_if_needed(session)
            .await;
        if let Some(w) = &self.loop_wiring {
            w.spawn_loop(session, project).await;
            w.ring(session);
        }
        if let Err(error) = self.mark_rebound_agent_live(session).await {
            tracing::warn!(
                target: "nexus::revive",
                session = %session,
                error = ?error,
                "failed to restamp OpenCode plugin runtime live after revive"
            );
            return Err(error);
        }
        if let Err(error) = self
            .append_agent_lifecycle_for_session(session, "resume")
            .await
        {
            tracing::warn!(
                target: "nexus::revive",
                session = %session,
                error = ?error,
                "failed to append native plugin resume lifecycle event after revive"
            );
        }
        Ok(())
    }
}
