//! Runtime registration, member binding, and lifecycle-event emission.

use super::*;

impl AppState {
    /// True when a session is an external pull client rather than a daemon-owned harness.
    ///
    /// `kind=agent` remains valid for SDK/CLI agents, but `harness=other` with no bound transport
    /// has nothing Nexus can spawn or inject. Its inbox must stay pending until `nexus listen` (or
    /// another pull consumer) claims it; auto-revive would only manufacture terminal failures.
    pub(crate) fn is_externally_drained_session(row: &nexus_store::types::SessionRow) -> bool {
        row.agent.as_deref() == Some("other") && row.transport.is_none()
    }

    /// Recreate the boot-scoped addressable runtime directory from Nexus's persistent continuity
    /// capsule. This restores Nexus agent/runtime identity only; it does not claim that a harness
    /// process is live and it does not inspect or validate provider-owned identities.
    pub async fn restore_runtime_identity_sessions_once(&self) -> Result<usize, NexusError> {
        // Single-store embeddings predate the identity authority and already retain their session
        // rows directly. They have no capsule table to rebuild from.
        if !self.store.has_split_authority() {
            return Ok(0);
        }
        let descriptors = IdentitySessions::new(&self.store).list().await?;
        let sessions = Sessions::new(&self.store);
        let agents = Agents::new(&self.store);
        // `identity_sessions` is continuity history keyed by runtime, while the boot-scoped
        // address directory projects one current runtime per durable agent. The repository lists
        // capsules newest first, so retain the first capsule for each immutable agent id and leave
        // older launches as history. Selecting by mutable display name would reintroduce rename
        // races and trying to materialize every historical runtime can violate the live name
        // uniqueness constraint, aborting restoration for unrelated agents.
        let mut projected_agent_ids = HashSet::new();
        let mut restored = 0;

        for descriptor in descriptors {
            if !projected_agent_ids.insert(descriptor.agent_id.clone()) {
                tracing::debug!(
                    target: "nexus::revive",
                    runtime_id = %descriptor.runtime_id,
                    agent_id = %descriptor.agent_id,
                    "skipping superseded resurrection capsule"
                );
                continue;
            }
            let session = SessionId(descriptor.runtime_id.clone());
            if sessions.find_by_session_id(&session).await?.is_some() {
                continue;
            }
            let Some(agent) = agents.find_by_id(&descriptor.agent_id).await? else {
                tracing::warn!(
                    target: "nexus::revive",
                    runtime_id = %descriptor.runtime_id,
                    agent_id = %descriptor.agent_id,
                    "skipping resurrection capsule whose stable agent identity is missing"
                );
                continue;
            };
            if agent.disabled_at.is_some() {
                continue;
            }
            let transport = match (descriptor.mode.as_str(), descriptor.harness.as_str()) {
                ("headless", _) => "acp",
                ("headed", "codex") => "codex-appserver",
                ("headed", "opencode") => "opencode-plugin",
                ("headed", _) => "pty",
                (mode, _) => {
                    tracing::warn!(
                        target: "nexus::revive",
                        runtime_id = %descriptor.runtime_id,
                        mode,
                        "skipping resurrection capsule with unsupported launch mode"
                    );
                    continue;
                }
            };
            let client_key = match descriptor.client_key.clone() {
                Some(client_key) => client_key,
                None => {
                    let client_key = Self::new_client_key();
                    IdentitySessions::new(&self.store)
                        .set_client_key(&descriptor.runtime_id, &client_key)
                        .await?;
                    client_key
                }
            };
            sessions
                .create(NewSession {
                    session_id: session.clone(),
                    name: agent.name.clone(),
                    agent: Some(descriptor.harness.clone()),
                    kind: kind_token(Kind::Agent).to_string(),
                    role: agent.role.clone(),
                    tier: agent.tier.clone(),
                    harness_session_id: descriptor.native_resume_key.clone(),
                    // The client key is part of the runtime continuity capsule. Native headed
                    // bridges can outlive the daemon, so rotating this credential at boot would
                    // strand a live process even though its Nexus runtime identity was restored.
                    client_key: Some(client_key),
                    cwd: descriptor.cwd.clone(),
                    project: descriptor.project.clone(),
                    transport: Some(transport.to_string()),
                })
                .await?;
            sessions
                .set_agent_id(&session, &descriptor.agent_id)
                .await?;
            // A restored runtime is newly visible in this daemon boot just like a freshly bound
            // runtime. Replay the existing lifecycle fact through the buffered Gateway projection
            // lane so a Gateway that reconnects after daemon startup can rebuild its roster from
            // the current boot without polling Core or retaining stale pre-boot state.
            self.ws
                .emit(WsEvent::AgentSpawned {
                    session_id: session,
                    name: agent.name,
                    agent_id: Some(descriptor.agent_id),
                })
                .await;
            restored += 1;
        }
        Ok(restored)
    }

    /// Build the launch context for an agent: the resolved working dir plus the identity env
    /// (`NEXUS_NAME`/`NEXUS_CLIENT_KEY`/`NEXUS_PROJECT`/`NEXUS_AGENT` + `NEXUS_CLI` and a `PATH`
    /// including the `nexus` binary). With that env the agent's own shell Nexus CLI authenticates
    /// as itself —
    /// its OUTBOUND. The client key is the stored per-runtime secret from [`AppState::bind_member`],
    /// not the raw session id and not ambient shell state.
    pub(crate) fn agent_launch_ctx(
        &self,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        kind: HarnessId,
        client_key: &str,
        cwd: Option<String>,
    ) -> (Option<String>, Vec<(String, String)>) {
        // The daemon binary IS `nexus`, so its own path is the CLI the agent should run.
        let nexus_bin = std::env::current_exe()
            .ok()
            .and_then(|p| p.to_str().map(String::from))
            .unwrap_or_else(|| "nexus".to_string());
        let exe_dir = std::path::Path::new(&nexus_bin)
            .parent()
            .and_then(|p| p.to_str())
            .unwrap_or("")
            .to_string();

        // Normal CLI launches send the operator's project cwd explicitly. If a non-CLI caller omits
        // cwd entirely, fall back to a stable agent-id keyed sidecar workspace rather than guessing
        // the daemon's own cwd. The bus how-to is NOT written here — each adapter installs the
        // `nexus-bus` skill into its own harness skills dir.
        let (dir, private_cwd) = resolve_agent_launch_cwd(agent_id, name, cwd);
        ensure_agent_launch_cwd(&dir, private_cwd);

        let path = match std::env::var("PATH") {
            Ok(p) if !exe_dir.is_empty() => format!("{exe_dir}:{p}"),
            Ok(p) => p,
            Err(_) => exe_dir.clone(),
        };
        let mut env = vec![
            ("NEXUS_AGENT_ID".to_string(), agent_id.to_string()),
            ("NEXUS_CLIENT_KEY".to_string(), client_key.to_string()),
            ("NEXUS_PROJECT".to_string(), project.to_string()),
            ("NEXUS_AGENT".to_string(), harness_token(&kind).to_string()),
            ("NEXUS_HOME".to_string(), self.nexus_home.clone()),
            ("NEXUS_CLI".to_string(), nexus_bin),
            ("PATH".to_string(), path),
            // Agents are agents, never the operator: blank any inherited tier override so a
            // daemon environment can never elevate a spawned agent (B21 — admin is assigned via
            // `admin grant-tier`, never inherited or defaulted).
            ("NEXUS_TIER".to_string(), String::new()),
        ];
        if let Some(name) = name {
            env.push(("NEXUS_NAME".to_string(), name.to_string()));
        }
        // Quota-constrained operators may cap Tokio on the daemon. Preserve that cap through the
        // launch context because ACP clients and their sanitized MCP subprocesses otherwise see
        // the host CPU count and each create a full-size runtime.
        if let Some(workers) = std::env::var_os("TOKIO_WORKER_THREADS") {
            env.push((
                "TOKIO_WORKER_THREADS".to_string(),
                workers.to_string_lossy().into_owned(),
            ));
        }
        // The ACP engine scrubs inherited harness-home variables. Reapply only the canonical
        // machine provider authority for the selected harness; Nexus runtime homes never become
        // credential homes.
        if kind.as_str() == "codex" {
            let session_dir = std::path::PathBuf::from(&self.nexus_home)
                .join("codex-sessions")
                .join(agent_id);
            if let Ok(home) =
                nexus_harness_codex::app_server::supervisor::resolve_machine_codex_home_from_env(
                    &session_dir,
                )
            {
                env.push((
                    "CODEX_HOME".to_string(),
                    home.to_string_lossy().into_owned(),
                ));
            }
        }
        if kind.as_str() == "hermes" {
            let source_override =
                std::env::var_os("NEXUS_HERMES_SOURCE_HOME").map(std::path::PathBuf::from);
            let selected = std::env::var_os("HERMES_HOME").map(std::path::PathBuf::from);
            let machine_home = std::env::var_os("HOME").map(std::path::PathBuf::from);
            if let Ok(home) = crate::daemon::pty_supervisor::resolve_machine_hermes_home(
                source_override.as_deref(),
                selected.as_deref(),
                machine_home.as_deref(),
            ) {
                env.push((
                    "HERMES_HOME".to_string(),
                    home.to_string_lossy().into_owned(),
                ));
            }
        }
        if kind.as_str() == "claude" {
            if let Some(config_dir) =
                std::env::var_os("CLAUDE_CONFIG_DIR").filter(|value| !value.is_empty())
            {
                env.push((
                    "CLAUDE_CONFIG_DIR".to_string(),
                    config_dir.to_string_lossy().into_owned(),
                ));
            }
        }
        // OpenCode auth is machine-owned under HOME/XDG_DATA_HOME (and provider API-key env).
        // Those variables remain inherited; only Nexus/OpenCode runtime DB/config overrides are
        // scrubbed and rebuilt by the adapter.
        (Some(dir), env)
    }

    /// Register the identity/member row for an already-launched `session_id` and spawn its per-agent
    /// [`EventLoop`] — the shared tail of both spawn routes (`launch` and `admin.spawn`). This is what
    /// turns a freshly-opened adapter session into an **addressable** (`members`/`resolve`) and
    /// **wakeable** (`dm` → bell → loop → `inject_turn`) bus member. The caller must pass the
    /// resolved durable `agent_id`; the mutable name is stored only as the current display/address
    /// label.
    pub(crate) async fn register_runtime(
        &self,
        req: RegisterRuntimeRequest<'_>,
    ) -> Result<RegisteredRuntime, NexusError> {
        let registered = Self::runtime_descriptor(
            req.session,
            req.agent_id,
            req.name,
            req.project,
            req.kind,
            req.role,
            req.cwd,
        );
        self.register_prepared_runtime(&registered, req.client_key, req.transport, req.owner)
            .await?;
        Ok(registered)
    }

    pub(crate) async fn register_prepared_runtime(
        &self,
        registered: &RegisteredRuntime,
        client_key: &str,
        transport: &str,
        owner: Option<&Caller>,
    ) -> Result<(), NexusError> {
        self.bind_member(
            &registered.session,
            &registered.agent_id,
            registered.name.as_deref(),
            &registered.project,
            registered.kind.clone(),
            registered.role.clone(),
            client_key,
            registered.cwd.clone(),
            transport,
            owner,
        )
        .await
    }

    /// Persist a launcher-captured OS process ledger without making this telemetry load-bearing.
    pub(crate) async fn set_runtime_process_ledger_best_effort(
        &self,
        session: &SessionId,
        entry: RuntimeProcessIds,
        context: &'static str,
    ) {
        if let Err(error) = AgentRuntimes::new(&self.store)
            .set_process_ids(&session.0, entry)
            .await
        {
            tracing::warn!(
                session = %session,
                pid = entry.os_pid,
                pgid = entry.os_pgid,
                context,
                error = %error,
                "failed to persist runtime process ledger"
            );
        }
    }

    pub(crate) async fn wake_registered_runtime(
        &self,
        registered: &RegisteredRuntime,
    ) -> Result<(), NexusError> {
        let paused = Sessions::new(&self.store)
            .find_by_session_id(&registered.session)
            .await?
            .map(|row| row.paused)
            .unwrap_or(false);
        self.make_live_agent_wakeable(&registered.session, &registered.project, paused)
            .await;
        Ok(())
    }

    /// Persist the minimum Nexus-owned capsule needed to recreate one daemon-owned runtime after
    /// a daemon restart. Harness-native session identity remains owned by the harness; Nexus stores
    /// only the opaque resume key it was given and never infers or validates a provider identity.
    pub(crate) async fn persist_runtime_resurrection_descriptor(
        &self,
        registered: &RegisteredRuntime,
        mode: &str,
        backend: &str,
        native_resume_key: Option<String>,
    ) -> Result<(), NexusError> {
        // Legacy single-store embeddings do not mount the identity authority. Preserve their
        // existing launch behavior; production split-authority daemons persist the capsule below.
        if !self.store.has_split_authority() {
            return Ok(());
        }
        let client_key = Sessions::new(&self.store)
            .find_by_session_id(&registered.session)
            .await?
            .and_then(|row| row.client_key);
        IdentitySessions::new(&self.store)
            .upsert(NewIdentitySession {
                runtime_id: registered.session.0.clone(),
                agent_id: registered.agent_id.clone(),
                project: registered.project.clone(),
                harness: Self::harness_to_store(registered.kind.clone()),
                mode: mode.to_string(),
                backend: Some(backend.to_string()),
                cwd: registered.cwd.clone(),
                native_resume_key,
                client_key,
            })
            .await
    }

    pub async fn register_and_wake(
        &self,
        session: &SessionId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        kind: HarnessId,
        role: Option<String>,
        client_key: &str,
        cwd: Option<String>,
        transport: &str,
        owner: Option<&Caller>,
    ) -> Result<(), NexusError> {
        let registered = self
            .register_runtime(RegisterRuntimeRequest {
                session,
                agent_id,
                name,
                project,
                kind,
                role,
                client_key,
                cwd,
                transport,
                owner,
            })
            .await?;
        self.wake_registered_runtime(&registered).await?;
        Ok(())
    }

    /// Bridge a self-/admin-pause into the realtime wake state so the wake policy actually holds a
    /// paused agent (spec §2.4). `IdentityPort::status` persists the `paused` flag, but the bus's
    /// `enqueue` consults the in-memory [`AgentRegistry`] — so without this bridge a "paused" agent
    /// would still be rung. On pause we set [`nexus_dispatch::AgentState::Paused`] (enqueue →
    /// `Hold`, no ring; the loop re-parks); on resume we set [`nexus_dispatch::AgentState::Idle`]
    /// **and ring** so anything that queued while held drains immediately. No-op without loop wiring
    /// (mock-port tests).
    pub fn apply_pause_state(&self, session: &SessionId, paused: bool) {
        let Some(w) = &self.loop_wiring else { return };
        if paused {
            w.registry.set(session, nexus_dispatch::AgentState::Paused);
        } else {
            w.registry.set(session, nexus_dispatch::AgentState::Idle);
            w.bell.ring(session);
        }
    }

    /// Ensure a per-agent [`EventLoop`] is running for an already-registered `kind=agent` session
    /// (the plain-`register` path: "idle loop stood up"). Idempotent — a resume/re-register reuses
    /// the running loop. Resolves the freshly-registered session by name+project. A no-op when there
    /// is no loop wiring (mock-port tests) or the session is not an agent.
    pub async fn ensure_agent_loop(&self, project: &str, name: &str) {
        if self.loop_wiring.is_none() {
            return;
        }
        let repo = Sessions::new(&self.store);
        let row = match repo.find_by_name(project, name).await {
            Ok(Some(r)) => r,
            _ => return,
        };
        // The `other` harness with no daemon transport is an externally drained transport client
        // (`nexus listen`), not an injectable harness. Starting an EventLoop for it creates a
        // second consumer that can claim and terminalize its inbox before the subscriber sees it.
        let externally_drained = Self::is_externally_drained_session(&row);
        // Daemon-owned harnesses register their Nexus MCP while ACP/native startup is still in
        // progress. That identity registration must not override the cold/offline hold and spawn a
        // loop before the turn executor is actually bound; pending mail would otherwise be claimed
        // and terminalized as `session not registered` milliseconds before startup completes.
        let harness_still_opening = self.agent.is_harness_alive(&row.session_id) == Some(false);
        if row.is_agent() && !externally_drained {
            // The same stable session may first register as an external pull client and later
            // acquire a daemon-managed transport. Retire that old ownership before making the
            // harness loop wakeable; otherwise the active subscription suppresses the loop and
            // strands every later delivery. Failure is fail-closed: never create two consumers.
            if let Err(error) = nexus_store::repos::InboxSubscriptions::new(&self.store)
                .mark_inactive_for_session(&row.session_id.0, now())
                .await
            {
                tracing::error!(
                    target: "nexus::delivery",
                    session = %row.session_id,
                    %error,
                    "failed to retire external inbox ownership during managed transport rebind"
                );
                return;
            }
            if harness_still_opening {
                if let Some(wiring) = &self.loop_wiring {
                    wiring
                        .registry
                        .set(&row.session_id, nexus_dispatch::AgentState::Offline);
                }
                return;
            }
            self.make_live_agent_wakeable(
                &SessionId(row.session_id.0.clone()),
                &row.project,
                row.paused,
            )
            .await;
        }
    }

    /// Whether a durable pull subscription currently owns this session's inbox delivery.
    pub(crate) async fn inbox_subscription_owns_delivery(&self, session: &SessionId) -> bool {
        match nexus_store::repos::InboxSubscriptions::new(&self.store)
            .has_active_for_session(&session.0)
            .await
        {
            Ok(active) => active,
            Err(error) => {
                tracing::warn!(
                    target: "nexus::delivery",
                    session = %session,
                    error = %error,
                    "failed to resolve inbox delivery owner; preserving daemon push ownership"
                );
                false
            }
        }
    }

    /// Make a newly-active durable subscription the sole consumer of this session's inbox.
    pub(crate) fn claim_inbox_subscription_delivery(&self, session: &SessionId) {
        if let Some(wiring) = &self.loop_wiring {
            wiring.claim_pull_delivery(session);
        }
    }

    /// Record a metadata-only attach lifecycle event for an agent session.
    ///
    /// Attach is observational: it publishes on `sys.agent.lifecycle` for tooling, but never writes
    /// a message, inserts a command for the target session, or wakes an agent loop. Only the local
    /// operator (admin-tier non-agent caller) or the session itself may claim an attach.
    pub(crate) async fn record_attach_lifecycle(
        &self,
        caller: &Caller,
        session: &SessionId,
    ) -> Result<(), ContractError> {
        let row = Sessions::new(&self.store)
            .find_by_session_id(session)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| {
                NexusError::NotFound(format!("session:{}", session.0)).to_contract_error()
            })?;
        if !row.is_agent() {
            return Err(NexusError::Invalid(format!(
                "{}:{} is not an agent session",
                row.display_name(),
                row.session_id.0
            ))
            .to_contract_error());
        }
        let operator = caller.tier == Tier::Admin && caller.agent_id.is_none();
        let own_session = caller.session == row.session_id;
        if !operator && !own_session {
            return Err(NexusError::Unauthorized.to_contract_error());
        }
        if let Some(name) = row.name.as_deref() {
            self.append_agent_lifecycle(name, &row.session_id, "attach")
                .await
                .map_err(|e| e.to_contract_error())?;
        }
        Ok(())
    }

    /// Append an agent lifecycle event through the shared developer-event surface. This is
    /// telemetry-only and mirrors the existing `started`/`stopped` lifecycle rows.
    pub(crate) async fn append_agent_lifecycle(
        &self,
        name: &str,
        session: &SessionId,
        lifecycle: &str,
    ) -> Result<(), NexusError> {
        DeveloperEvents::new(&self.store)
            .append_agent_lifecycle(name, session, lifecycle, None, now())
            .await?;
        self.store.events().session_lifecycle_changed().signal();
        Ok(())
    }

    /// Best-effort wrapper for lifecycle developer events. Register/resume state changes are
    /// load-bearing daemon paths; lifecycle rows are telemetry and may not block them.
    pub(crate) async fn append_agent_lifecycle_best_effort(
        &self,
        name: &str,
        session: &SessionId,
        lifecycle: &str,
        context: &'static str,
    ) {
        if let Err(error) = self.append_agent_lifecycle(name, session, lifecycle).await {
            tracing::warn!(
                target: "nexus::lifecycle",
                name,
                session = %session,
                lifecycle,
                context,
                error = ?error,
                "failed to append agent lifecycle telemetry"
            );
        }
    }

    pub(crate) async fn append_agent_lifecycle_for_session(
        &self,
        session: &SessionId,
        lifecycle: &str,
    ) -> Result<(), NexusError> {
        let Some(row) = Sessions::new(&self.store)
            .find_by_session_id(session)
            .await?
        else {
            return Ok(());
        };
        if !row.is_agent() {
            return Ok(());
        }
        if let Some(name) = row.name.as_deref() {
            self.append_agent_lifecycle(name, &row.session_id, lifecycle)
                .await?;
        }
        Ok(())
    }

    /// Write the identity/member row for a launched session under a **specific** session id (so the
    /// member row and the agent's adapter binding share one id). `agent_id` is resolved before this
    /// boundary; `name` is only the current display/address label. If an offline fossil row already
    /// holds the requested name, reclaim it for the newly launched session; live rows still hold the
    /// name.
    /// The durable `agents`/`agent_runtimes` graph is bound in the same path so daemon-owned launches
    /// are immediately addressable by stable identity, even before a harness self-register hook runs.
    /// Presence admission covers selection, binding, and online materialization, not native launch
    /// or wakeup. Cancellation before admission has no effects; after binding commits it remains a
    /// partial operation, not an atomic launch transaction.
    pub(crate) async fn bind_member(
        &self,
        session: &SessionId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        kind: HarnessId,
        role: Option<String>,
        client_key: &str,
        cwd: Option<String>,
        transport: &str,
        owner: Option<&Caller>,
    ) -> Result<(), NexusError> {
        let transition = self.presence.binding_transition().await;
        let repo = Sessions::new(&self.store);
        let new = NewSession {
            session_id: SessionId(session.0.clone()),
            name: name.map(str::to_string),
            agent: Some(harness_token(&kind).to_string()),
            kind: kind_token(Kind::Agent).to_string(),
            role: role.clone(),
            tier: tier_token(Tier::Agent).to_string(),
            harness_session_id: None,
            // Per-runtime client key used by the agent's own `nexus` CLI/MCP calls. This is a
            // daemon-minted secret stored on the session row, not the raw session id.
            client_key: Some(client_key.to_string()),
            // Persist the resolved launch cwd so a later REVIVE respawns the harness in the same
            // folder. Normal CLI launches already send the operator's project cwd; the daemon
            // private ~/.nexus/agents/<agent_id> path is only a fallback for callers that omit cwd.
            cwd: cwd.clone(),
            project: project.to_string(),
            transport: Some(transport.to_string()),
        };

        if let Some(name) = name {
            if let Some(existing) = repo.find_by_name_any_project(name).await? {
                if existing.session_id == *session {
                    if let Some(bound_id) = existing.agent_id.as_deref() {
                        if bound_id != agent_id {
                            return Err(NexusError::Invalid(format!(
                                "session {} is bound to {}, not {}",
                                session, bound_id, agent_id
                            )));
                        }
                    }
                    self.bind_stable_agent_runtime(
                        session,
                        agent_id,
                        Some(name),
                        project,
                        kind,
                        role,
                        cwd,
                        transport,
                        owner,
                    )
                    .await?;
                    repo.set_transport(session, transport).await?;
                    repo.set_agent_id(session, agent_id).await?;
                    transition.materialize_online(session).await?;
                    drop(transition);
                    self.ws
                        .project_runtime_binding(session, &AgentId(agent_id.to_string()))
                        .await;
                    self.append_agent_lifecycle_best_effort(
                        name,
                        session,
                        "resume",
                        "same-session bind_member",
                    )
                    .await;
                    return Ok(());
                }
                let existing_is_dead = existing.presence.as_deref() == Some("offline")
                    || self.agent.is_harness_alive(&existing.session_id) == Some(false);
                let existing_belongs_to_other_identity = existing
                    .agent_id
                    .as_deref()
                    .is_some_and(|existing_agent_id| existing_agent_id != agent_id);
                if existing.project != project
                    || !existing_is_dead
                    || existing_belongs_to_other_identity
                {
                    return Err(NexusError::DuplicateName(name.to_string()));
                }
                repo.rebind_name_to_session(new).await?;
                repo.set_agent_id(session, agent_id).await?;
                let agent_id = self
                    .bind_stable_agent_runtime(
                        session,
                        agent_id,
                        Some(name),
                        project,
                        kind,
                        role,
                        cwd,
                        transport,
                        owner,
                    )
                    .await?;
                self.append_agent_lifecycle_best_effort(
                    name,
                    session,
                    "resume",
                    "dead-name bind_member",
                )
                .await;
                transition.materialize_online(session).await?;
                drop(transition);
                self.ws
                    .emit(WsEvent::AgentSpawned {
                        session_id: SessionId(session.0.clone()),
                        name: Some(name.to_string()),
                        agent_id: Some(agent_id),
                    })
                    .await;
                return Ok(());
            }
        }

        repo.create(new).await?;
        repo.set_agent_id(session, agent_id).await?;
        let agent_id = self
            .bind_stable_agent_runtime(
                session, agent_id, name, project, kind, role, cwd, transport, owner,
            )
            .await?;
        if let Some(name) = name {
            self.append_agent_lifecycle_best_effort(name, session, "register", "new bind_member")
                .await;
        }
        // The daemon just launched this agent, so it is live now: stamp a heartbeat so it shows in
        // `members` immediately (a never-beaten row is treated as stale → offline → hidden).
        transition.materialize_online(session).await?;
        drop(transition);
        // Announce the launched member so local observers and directory projections see it immediately.
        self.ws
            .emit(WsEvent::AgentSpawned {
                session_id: SessionId(session.0.clone()),
                name: name.map(str::to_string),
                agent_id: Some(agent_id),
            })
            .await;
        Ok(())
    }

    /// Bind the durable agent identity for a daemon-owned launch to the launched session runtime.
    /// The daemon is trusted to mint this runtime, so runtime credentials are only required on the
    /// public `register` path where an external process claims an existing `agent_id`.
    pub(crate) async fn bind_stable_agent_runtime(
        &self,
        session: &SessionId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        kind: HarnessId,
        role: Option<String>,
        cwd: Option<String>,
        transport: &str,
        owner: Option<&Caller>,
    ) -> Result<String, NexusError> {
        let agents = Agents::new(&self.store);
        let owner = owner.map(Self::managed_agent_owner);
        let agent_id = if let Some(agent) = agents.find_by_id(agent_id).await? {
            if agent.disabled_at.is_some() {
                return Err(NexusError::Unauthorized);
            }
            if agent.project != project || agent.name.as_deref() != name {
                return Err(NexusError::DuplicateName(
                    name.unwrap_or(agent_id).to_string(),
                ));
            }
            if let Some(owner) = &owner {
                agents.set_owner_if_missing(&agent.agent_id, owner).await?;
            }
            agent.agent_id
        } else {
            if let Some(name) = name {
                if agents.find_by_name(name).await?.is_some() {
                    return Err(NexusError::DuplicateName(name.to_string()));
                }
            }
            agents
                .create(NewAgent {
                    agent_id: agent_id.to_string(),
                    project: project.to_string(),
                    name: name.map(str::to_string),
                    default_harness: Some(Self::harness_to_store(kind.clone())),
                    role,
                    tier: Some(tier_token(Tier::Agent).to_string()),
                    owner,
                })
                .await?
        };

        let runtimes = AgentRuntimes::new(&self.store);
        if let Some(existing_runtime) = runtimes.find_by_runtime_id(&session.0).await? {
            if existing_runtime.agent_id != agent_id {
                return Err(NexusError::Invalid(format!(
                    "runtime {} is bound to {}, not {}",
                    session.0, existing_runtime.agent_id, agent_id
                )));
            }
        } else {
            runtimes
                .create(NewAgentRuntime {
                    runtime_id: session.0.clone(),
                    agent_id: agent_id.clone(),
                    harness: Self::harness_to_store(kind),
                    cwd,
                    transport: Some(transport.to_string()),
                    // Binding is prepared first; bind_member stamps session identity and
                    // materializes liveness only after this succeeds.
                    presence: Some("offline".to_string()),
                    active: false,
                })
                .await?;
        }

        Ok(agent_id)
    }
}
