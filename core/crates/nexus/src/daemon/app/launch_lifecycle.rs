//! Launch identity, initial-prompt, recovery, and thread-wake orchestration.

use super::*;

impl AppState {
    pub(crate) async fn resolve_existing_launch_id(
        &self,
        req: &SpawnRequest,
    ) -> Result<Option<LaunchIdentity>, nexus_contracts::ContractError> {
        let Some(AgentRef::Id(agent_id)) = req.name.as_deref().map(AgentRef::parse) else {
            return Ok(None);
        };
        let agent = Agents::new(&self.store)
            .find_by_id(&agent_id)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| nexus_contracts::ContractError {
                code: nexus_contracts::codes::NOT_FOUND,
                message: format!("agent id {agent_id} does not exist"),
            })?;
        if agent.disabled_at.is_some() {
            return Err(NexusError::Unauthorized.to_contract_error());
        }
        Ok(Some(LaunchIdentity {
            agent_id: agent.agent_id,
            name: agent.name,
            project: agent.project,
        }))
    }

    #[doc(hidden)]
    pub async fn resolve_launch_identity_for_session(
        &self,
        req: &SpawnRequest,
        default_project: &str,
        session: &SessionId,
    ) -> Result<LaunchIdentity, NexusError> {
        let agents = Agents::new(&self.store);
        let identity_policy = spawn_identity_policy(req);
        match req.name.as_deref().map(AgentRef::parse) {
            Some(AgentRef::Id(agent_id)) => {
                let agent = agents
                    .find_by_id(&agent_id)
                    .await?
                    .ok_or_else(|| NexusError::NotFound(format!("agent id {agent_id}")))?;
                if agent.disabled_at.is_some() {
                    return Err(NexusError::Unauthorized);
                }
                Ok(LaunchIdentity {
                    agent_id: agent.agent_id,
                    name: agent.name,
                    project: agent.project,
                })
            }
            Some(AgentRef::Name(name)) => {
                let _ = identity_policy;
                if let Some(agent) = agents.find_by_project_name(default_project, &name).await? {
                    if agent.disabled_at.is_some() {
                        return Err(NexusError::Unauthorized);
                    }
                    return Ok(LaunchIdentity {
                        agent_id: agent.agent_id,
                        name: agent.name,
                        project: agent.project,
                    });
                }
                if let Some(agent) = agents.find_by_name(&name).await? {
                    if agent.project != default_project {
                        return Err(NexusError::DuplicateName(name));
                    }
                    if agent.disabled_at.is_some() {
                        return Err(NexusError::Unauthorized);
                    }
                    return Ok(LaunchIdentity {
                        agent_id: agent.agent_id,
                        name: agent.name,
                        project: agent.project,
                    });
                }
                Ok(LaunchIdentity {
                    agent_id: format!("a_{}", session.0),
                    name: Some(name),
                    project: default_project.to_string(),
                })
            }
            None => Ok(LaunchIdentity {
                agent_id: format!("a_{}", session.0),
                name: None,
                project: default_project.to_string(),
            }),
        }
    }

    /// The `launch` registry method — make a launched agent an **addressable, wakeable bus member**.
    ///
    /// This is the orchestration the bare `AgentTurnExecutionPort::launch` cannot do alone (it lives
    /// below identity + realtime). It:
    ///
    /// 1. **opens + binds the adapter** (`agent.launch`) → the canonical `session_id`,
    /// 2. **registers the identity row** under that same `session_id` (so the agent appears in
    ///    `members` and `IdentityPort::resolve(name)` works → `dm <name>` resolves a recipient), and
    /// 3. **spawns the per-agent [`EventLoop`]** bound to that session (so a `dm`'s ring wakes the
    ///    loop → `drain_once` → `inject_turn`).
    ///
    /// Without the loop wiring (mock-port [`AppState::new`]) step 3 is a no-op and step 2 still runs
    /// against the shared store, so the agent is at least addressable. The same
    /// [`AppState::register_and_wake`] tail also backs the admin-tier `admin.spawn` path, so both
    /// spawn routes produce an addressable + wakeable member.
    ///
    /// `caller_project` is the resolved caller's project — the launched agent is registered in **that**
    /// project (so the caller's project-scoped `members`/`resolve` see it), unless the request itself
    /// names a project. It must NOT fall back to `self.project` (the daemon default is empty on the
    /// production [`AppState::wire`] path): a `nexus launch <kind>` with no `--project` carries no
    /// project on the request, and defaulting to the empty project would register the agent invisibly
    /// — unfindable + unresolvable from the caller's own project.
    pub(crate) fn initial_prompt_events(&self) -> Arc<dyn EventSink> {
        self.loop_wiring
            .as_ref()
            .map(|w| w.events.clone())
            .unwrap_or_else(|| Arc::new(self.ws.clone()))
    }

    pub(crate) fn initial_prompt_not_fresh_error() -> nexus_contracts::ContractError {
        NexusError::Invalid(
            "--initial-prompt is only valid for a fresh launch; use a normal prompt for resume, revive, attach, or existing agents"
                .into(),
        )
        .to_contract_error()
    }

    pub(crate) fn invalid_initial_prompt_error(
        message: impl Into<String>,
    ) -> nexus_contracts::ContractError {
        NexusError::Invalid(format!("invalid initial prompt: {}", message.into()))
            .to_contract_error()
    }

    pub(crate) async fn prepare_initial_prompt(
        &self,
        registered: &RegisteredRuntime,
        template: &str,
    ) -> Result<Option<PreparedInitialPrompt>, nexus_contracts::ContractError> {
        let vars = crate::initial_prompt::InitialPromptVars {
            name: registered.name.clone(),
            role: registered.role.clone(),
            harness: harness_token(&registered.kind).to_string(),
            agent_id: registered.agent_id.clone(),
            session_id: registered.session.0.clone(),
            runtime_id: registered.session.0.clone(),
            cwd: registered.cwd.clone(),
        };
        let rendered_prompt = crate::initial_prompt::render_initial_prompt(template, &vars)
            .map_err(|e| Self::invalid_initial_prompt_error(e.to_string()))?;
        let client_message_id = format!("initial-prompt:{}", registered.session.0);
        let outcome = InitialPromptDeliveries::new(&self.store)
            .insert_pending_once(InitialPromptInsert {
                runtime_id: registered.session.0.clone(),
                agent_id: registered.agent_id.clone(),
                session_id: registered.session.0.clone(),
                harness: harness_token(&registered.kind).to_string(),
                template: template.to_string(),
                rendered_prompt: rendered_prompt.clone(),
                client_message_id: client_message_id.clone(),
                created_at_ms: now(),
            })
            .await
            .map_err(|e| e.to_contract_error())?;
        match outcome {
            InitialPromptInsertOutcome::Pending | InitialPromptInsertOutcome::AlreadyPending => {
                Ok(Some(PreparedInitialPrompt {
                    rendered_prompt,
                    client_message_id,
                }))
            }
            InitialPromptInsertOutcome::AlreadyAccepted => Ok(None),
            InitialPromptInsertOutcome::AlreadyFailed => Err(NexusError::Invalid(format!(
                "initial prompt previously failed for runtime {}",
                registered.session.0
            ))
            .to_contract_error()),
        }
    }

    pub(crate) fn runtime_descriptor(
        session: &SessionId,
        agent_id: &str,
        name: Option<&str>,
        project: &str,
        kind: HarnessId,
        role: Option<String>,
        cwd: Option<String>,
    ) -> RegisteredRuntime {
        RegisteredRuntime {
            session: session.clone(),
            agent_id: agent_id.to_string(),
            name: name.map(str::to_string),
            project: project.to_string(),
            kind,
            role,
            cwd,
        }
    }

    pub(crate) fn initial_prompt_event(
        registered: &RegisteredRuntime,
        prepared: &PreparedInitialPrompt,
    ) -> WsEvent {
        WsEvent::AgentUpdate {
            session_id: registered.session.clone(),
            kind: nexus_contracts::AgentUpdateKind::UserInput,
            data: serde_json::json!({
                "text": prepared.rendered_prompt,
                "clientMessageId": prepared.client_message_id,
                "source": "initial_prompt",
                "name": registered.name,
                "harness": harness_token(&registered.kind),
                "runtimeId": registered.session.0,
            }),
        }
    }

    pub(crate) async fn deliver_prepared_initial_prompt(
        &self,
        registered: &RegisteredRuntime,
        prepared: PreparedInitialPrompt,
    ) -> Result<(), nexus_contracts::ContractError> {
        let accepted_event = Self::initial_prompt_event(registered, &prepared);
        let delivery = InitialPromptDeliveries::new(&self.store);
        match self
            .agent
            .prompt_observed(
                &registered.session,
                prepared.rendered_prompt,
                self.initial_prompt_events(),
                accepted_event,
            )
            .await
        {
            Ok(()) => {
                delivery
                    .mark_accepted(&registered.session.0, now())
                    .await
                    .map_err(|e| e.to_contract_error())?;
                Ok(())
            }
            Err(error) => {
                let _ = delivery
                    .mark_failed(&registered.session.0, &error.message, now())
                    .await;
                Err(error)
            }
        }
    }

    pub(crate) async fn fail_initial_prompt_runtime(&self, registered: &RegisteredRuntime) {
        if let Ok(Some(row)) = Sessions::new(&self.store)
            .find_by_session_id(&registered.session)
            .await
        {
            self.teardown_harness_row(&row).await;
        }
        if let Some(w) = &self.loop_wiring {
            w.teardown_session_transports(&registered.session);
        }
        let _ = self
            .presence
            .mark_transport_offline(&registered.session)
            .await;
        let _ = self
            .release_native_session_bindings(&registered.session)
            .await;
    }

    pub(crate) async fn mark_initial_prompt_failed(
        &self,
        registered: &RegisteredRuntime,
        error: &str,
    ) {
        self.mark_initial_prompt_failed_for_session(&registered.session, error)
            .await;
    }

    pub(crate) async fn mark_initial_prompt_failed_for_session(
        &self,
        session: &SessionId,
        error: &str,
    ) {
        let _ = InitialPromptDeliveries::new(&self.store)
            .mark_failed(&session.0, error, now())
            .await;
    }

    pub(crate) async fn register_prepared_initial_prompt_runtime(
        &self,
        registered: &RegisteredRuntime,
        client_key: &str,
        transport: &str,
        owner: Option<&Caller>,
    ) -> Result<(), nexus_contracts::ContractError> {
        if let Err(error) = self
            .register_prepared_runtime(registered, client_key, transport, owner)
            .await
        {
            self.mark_initial_prompt_failed(registered, &error.to_string())
                .await;
            self.fail_initial_prompt_runtime(registered).await;
            return Err(error.to_contract_error());
        }
        Ok(())
    }

    /// Ensure the named agent has a LIVE ACP session, re-spawning + RESUMING it if its adapter died
    /// (a not-fresh session — daemon/process restart). Reads the durable session row (harness +
    /// persisted ACP resume key + cwd) and re-opens via `session/load` (falling back to a fresh
    /// `session/new` inside `open_session_for` if resume is unsupported). Returns the (stable) nexus
    /// session id. Used by the direct-DM `prompt` path so a not-fresh DM continues instead of erroring.
    /// Guarantee the named member's session lives in `project` (the thread's workspace) so thread
    /// fan-out — which resolves members within the project — can reach it. If the session is in another
    /// workspace, MOVE it (never silently drop a thread member; spec §12.4). No-op if already there or
    /// the name doesn't resolve.
    pub async fn ensure_in_workspace(
        &self,
        name: &str,
        project: &str,
    ) -> Result<(), nexus_contracts::ContractError> {
        use nexus_store::repos::Sessions;
        let sessions = Sessions::new(&self.store);
        let row = sessions
            .find_by_name_any_project(name)
            .await
            .map_err(|e| e.to_contract_error())?;
        match row {
            Some(r) if r.project != project => {
                self.identity.assign_project(name, project).await?;
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn ensure_live(
        &self,
        name: &str,
        project: &str,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        let row = self.resolve_session_by_name(name, project).await?;
        self.ensure_live_row(row).await
    }

    pub(crate) async fn ensure_live_row(
        &self,
        row: SessionRow,
    ) -> Result<SessionId, nexus_contracts::ContractError> {
        // Spawn/resume under the agent's OWN project (from the row), not the caller's — the launch
        // context (identity env, bus scope) must match where the agent actually lives.
        let project = row.project.as_str();
        let session = row.session_id.clone();
        let name = row.name.as_deref().ok_or_else(|| {
            NexusError::Invalid("cannot ensure unnamed session live".into()).to_contract_error()
        })?;

        // No concrete agent (mock-port wiring) → nothing to (re)bind; just return the id.
        let Some(agent) = &self.agent_concrete else {
            return Ok(session);
        };
        // Thread fan-out, boot recovery, and the periodic pending-delivery sweep can all discover
        // the same cold runtime concurrently. Serialize the liveness check with the resume itself:
        // once the winner binds its adapter, every waiter observes it as live and reuses it instead
        // of spawning another ACP process against the same harness-native session.
        let _revive = self.runtime_revive_gate.acquire(&session).await;
        // (Re-)spawn + resume the bound harness unless it's already live. NOTE: `is_live`, not
        // `has_session` — a bound-but-errored session (seed registration / failed open) reports
        // has_session=true but can't be injected, so we must (re-)spawn it.
        let was_live = agent.is_live(&session);
        let durable_was_offline = row.presence.as_deref() == Some("offline");
        if !was_live {
            // The harness label lives on the `agent` column.
            let kind = harness_from_token(row.agent.as_deref());
            let client_key = row.client_key.as_deref().unwrap_or(session.0.as_str());
            let agent_key = row.agent_id.as_deref().unwrap_or(name);
            let (cwd, env) = self.agent_launch_ctx(
                agent_key,
                Some(name),
                project,
                kind.clone(),
                client_key,
                row.cwd.clone(),
            );
            let process_ledger = agent
                .open_session_for(
                    session.clone(),
                    name,
                    project,
                    kind,
                    cwd,
                    env,
                    row.harness_session_id.as_deref(), // the persisted ACP resume key (session/load)
                )
                .await?;
            if let Some(entry) = process_ledger {
                self.set_runtime_process_ledger_best_effort(&session, entry, "acp ensure-live")
                    .await;
            }
        }
        // Ensure the per-agent DRAIN LOOP is running too — not just the adapter. Without it, messages
        // delivered to this agent's inbox via the bus (THREAD posts, fan-out) sit `pending` forever:
        // only direct DM `prompt` forces a turn. The loop is spawn-once and re-rings on attach, so it
        // immediately drains anything already queued (e.g. the thread post that triggered this wake)
        // and injects it as a turn → the agent replies.
        //
        // ORDERING INVARIANT (the boot-respawn liveness bug): spawn the loop ONLY once the adapter is
        // actually LIVE (injectable). `spawn_loop` re-rings the bell on attach and drains immediately,
        // so spawning it against a NON-live adapter makes that first drain's `inject_turn` fail ("no
        // usable adapter for session"), the loop logs "rows stay pending" and re-parks, and the
        // pending message stays stuck until some later post happens to wake it (or never). This is the
        // exact stale-resume boot path: a not-fresh agent's `session/load` always fails after a daemon
        // restart, and even after the `session/new` fallback we must confirm the rebind took before
        // standing up the drainer. If it did NOT (errored bind / dead connection), DON'T spawn the
        // loop — the member row remains and a future wake re-drives `ensure_live`, keeping the message
        // re-driveable instead of burning the one attach re-ring on a dead adapter.
        if row.kind == "agent" {
            if !agent.is_live(&session) {
                tracing::warn!(
                    target: "nexus::launch",
                    session = %session, name = %name,
                    "ensure_live: adapter not live after (re)spawn; NOT spawning drain loop (message stays re-driveable)"
                );
                return Ok(session);
            }
            // A thread/DM wake runs for every recipient after fan-out. If the adapter and durable
            // row were already live, refreshing presence here is both redundant and dangerous:
            // simultaneous harness replies can hold the SQLite writer while this best-effort wake
            // attempts touch_heartbeat, turning an otherwise healthy recipient into a terminal
            // `target_unreachable` delivery. A real rebind OR an explicitly offline durable row
            // still needs offline->online materialization before its drain loop is rung. The latter
            // covers externally managed/mock adapters that can remain injectable while the daemon
            // has correctly converged their durable presence to offline.
            if !was_live || durable_was_offline {
                self.mark_rebound_agent_live(&session).await?;
            }
            if let Some(w) = &self.loop_wiring {
                if !row.paused {
                    w.registry.set(&session, nexus_dispatch::AgentState::Idle);
                }
                // Spawn the loop (no-op if already running) AND ring: `spawn_loop` re-rings only on a
                // FRESH spawn, so after a rebind whose loop already existed we must ring here or the
                // newly-live adapter's pending mail never drains.
                w.spawn_loop(&session, project);
                w.ring(&session);
            }
        }
        Ok(session)
    }

    /// Mark a successfully rebound agent runtime live in the durable read/drain model before
    /// ringing its event loop.
    ///
    /// The heartbeat keeper can truthfully flip registered-but-unbound ACP sessions to
    /// `offline` while they have no usable adapter. Once [`ensure_live`](Self::ensure_live) has
    /// rebound an injectable adapter, the compatibility `sessions` row and the generic
    /// `agent_runtimes` row must be made live immediately; otherwise the inbox drain query keeps
    /// hiding that agent's pending rows as offline until a later heartbeat tick happens to restamp
    /// them.
    pub(crate) async fn mark_rebound_agent_live(
        &self,
        session: &SessionId,
    ) -> Result<(), nexus_contracts::ContractError> {
        self.presence
            .materialize_online(session)
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(())
    }

    /// On daemon boot, bring back exactly the agents that have leftover `in_flight` mail: re-spawn each
    /// one's adapter + drain loop (via `ensure_live`), whose attach re-ring drains the pending batch and
    /// injects it as a turn. Without this, leftovers sit pending until some NEW post happens to wake the
    /// agent (spec §6.5, §12.6). Best-effort per agent (a failure is logged, others proceed).
    pub async fn respawn_pending_agents(&self) {
        self.respawn_pending_agents_with_cutoff(None).await;
    }

    pub(crate) async fn respawn_pending_agents_before(&self, created_before: i64) {
        self.respawn_pending_agents_with_cutoff(Some(created_before))
            .await;
    }

    pub(crate) async fn respawn_pending_agents_with_cutoff(&self, created_before: Option<i64>) {
        use nexus_store::repos::Sessions;
        let inbox = nexus_store::repos::Inbox::new(&self.store);
        let dead_letter_result = match created_before {
            Some(cutoff) => {
                inbox
                    .dead_letter_undeliverable_for_dead_recipients_before(cutoff)
                    .await
            }
            None => inbox.dead_letter_undeliverable_for_dead_recipients().await,
        };
        match dead_letter_result {
            Ok(0) => {}
            Ok(failed) => tracing::warn!(
                target: "nexus::revive",
                failed,
                "respawn_pending_agents: preserved dead-recipient backlog as target_dead"
            ),
            Err(e) => tracing::warn!(
                target: "nexus::revive",
                error = %e,
                "respawn_pending_agents: dead-recipient settlement failed before revive"
            ),
        }
        let recipient_result = match created_before {
            Some(cutoff) => inbox.recipients_with_pending_before(cutoff).await,
            None => inbox.recipients_with_pending().await,
        };
        let recipients = match recipient_result {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "respawn_pending_agents: query failed");
                return;
            }
        };
        for sid in recipients {
            let this = self.clone();
            tokio::spawn(async move {
                if this.inbox_subscription_owns_delivery(&sid).await {
                    this.clear_pending_respawn_backoff(&sid);
                    if let Some(wiring) = &this.loop_wiring {
                        wiring.ring(&sid);
                    }
                    return;
                }
                if let Some(backoff) = this.pending_respawn_backoff_for(&sid, now()) {
                    tracing::debug!(
                        target: "nexus::revive",
                        session = %sid,
                        failures = backoff.failures,
                        next_retry_at = backoff.next_retry_at,
                        "respawn_pending_agents: pending recipient is in revive backoff"
                    );
                    return;
                }
                let sessions = Sessions::new(&this.store);
                if let Ok(Some(row)) = sessions.find_by_session_id(&sid).await {
                    if row.kind == "agent" && !Self::is_externally_drained_session(&row) {
                        tracing::info!(
                            target: "nexus::revive",
                            name = %row.display_name(),
                            agent_id = ?row.agent_id,
                            session = %row.session_id,
                            "respawn_pending_agents: ensuring live recipient with pending inbox"
                        );
                        let revive = match row.agent_id.as_deref() {
                            Some(agent_id) => this.ensure_alive_agent(agent_id).await,
                            None => match row.name.as_deref() {
                                Some(name) => this.ensure_alive(name, &row.project).await,
                                None => Err(ContractError {
                                    code: nexus_contracts::codes::INVALID_PARAMS,
                                    message: format!(
                                        "{} is unnamed and cannot be revived by name",
                                        row.session_id.0
                                    ),
                                }),
                            },
                        };
                        match revive {
                            Ok(_) => this.clear_pending_respawn_backoff(&row.session_id),
                            Err(e) => {
                                let backoff = this
                                    .record_revive_failure_or_terminalize(
                                        None,
                                        &row.session_id,
                                        &format!("target could not be revived: {}", e.message),
                                        "pending_recipient_revive",
                                    )
                                    .await;
                                if backoff.failures >= PENDING_RESPAWN_TOMBSTONE_FAILURES {
                                    if let Err(mark_error) =
                                        this.presence.mark_transport_offline(&row.session_id).await
                                    {
                                        tracing::warn!(
                                            target: "nexus::revive",
                                            name = %row.display_name(),
                                            session = %row.session_id,
                                            error = %mark_error,
                                            "respawn_pending_agents: failed to materialize tombstoned recipient offline"
                                        );
                                    }
                                    // Revive backoff hit the
                                    // tombstone threshold — durable lifecycle_state='dead'
                                    // (revive_exhausted), best-effort, never blocks the sweep.
                                    if let Some(agent_id) = row.agent_id.as_deref() {
                                        match nexus_store::repos::Agents::new(&this.store)
                                            .mark_dead(agent_id, "revive_exhausted")
                                            .await
                                        {
                                            Ok(true) => {
                                                this.append_agent_lifecycle_best_effort(
                                                    &row.display_name(),
                                                    &row.session_id,
                                                    "dead",
                                                    "respawn_pending_agents dead-marking",
                                                )
                                                .await;
                                                tracing::warn!(
                                                    target: "nexus::revive",
                                                    name = %row.display_name(),
                                                    session = %row.session_id,
                                                    agent_id,
                                                    failures = backoff.failures,
                                                    "respawn_pending_agents: agent marked DEAD (revive_exhausted)"
                                                );
                                            }
                                            Ok(false) => {}
                                            Err(error) => tracing::warn!(
                                                target: "nexus::revive",
                                                session = %row.session_id,
                                                error = %error,
                                                "respawn_pending_agents: dead-marking failed (best-effort)"
                                            ),
                                        }
                                    }
                                }
                                tracing::warn!(
                                    target: "nexus::revive",
                                    name = %row.display_name(),
                                    session = %row.session_id,
                                    failures = backoff.failures,
                                    next_retry_at = backoff.next_retry_at,
                                    tombstoned = backoff.failures >= PENDING_RESPAWN_TOMBSTONE_FAILURES,
                                    error = ?e,
                                    "respawn_pending_agents: ensure_alive failed; backing off pending recipient"
                                );
                            }
                        }
                    }
                }
            });
        }
    }

    /// Drive pending rows after an explicit operator DLQ requeue. This is the only path that
    /// clears a provider-limit hold for failed mail; reset hints and daemon timers never do so.
    pub async fn drive_explicit_delivery_requeue(&self) {
        let inbox = Inbox::new(&self.store);
        let recipients = match inbox.recipients_with_pending().await {
            Ok(recipients) => recipients,
            Err(error) => {
                tracing::error!(
                    target: "nexus::delivery",
                    %error,
                    "explicit requeue could not enumerate recipients"
                );
                return;
            }
        };
        for session in recipients {
            self.clear_pending_respawn_backoff(&session);
            if let Some(wiring) = &self.loop_wiring {
                wiring
                    .registry
                    .set(&session, nexus_dispatch::AgentState::Idle);
            }

            let Some(row) = Sessions::new(&self.store)
                .find_by_session_id(&session)
                .await
                .ok()
                .flatten()
            else {
                continue;
            };
            if row.kind != "agent" {
                continue;
            }
            if Self::is_externally_drained_session(&row) {
                continue;
            }
            if self.inbox_subscription_owns_delivery(&session).await {
                if let Some(wiring) = &self.loop_wiring {
                    wiring.ring(&session);
                }
                continue;
            }
            let result = match row.agent_id.as_deref() {
                Some(agent_id) => self.ensure_alive_agent(agent_id).await,
                None => match row.name.as_deref() {
                    Some(name) => self.ensure_alive(name, &row.project).await,
                    None => Err(ContractError {
                        code: nexus_contracts::codes::INVALID_PARAMS,
                        message: "unnamed target cannot be revived".into(),
                    }),
                },
            };
            match result {
                Ok(live_session) => {
                    if let Some(wiring) = &self.loop_wiring {
                        wiring.ring(&live_session);
                    }
                }
                Err(error) => {
                    let _ = inbox
                        .mark_recipient_pending_error(
                            &session,
                            TARGET_UNREACHABLE_ERROR_CODE,
                            &format!("explicit retry could not revive target: {}", error.message),
                            Some("{\"source\":\"explicit_dlq_requeue\"}"),
                        )
                        .await;
                }
            }
        }
    }

    /// Wake one durable DM recipient after the canonical message and delivery row have committed.
    /// Harness startup can take seconds, so it must never hold the sender's command receipt open.
    /// The background task owns the first of three bounded pre-injection revive attempts. A
    /// durably dead target is terminal immediately; any other failure leaves the row pending for
    /// the periodic recovery pass until the third failure.
    pub fn wake_dm_agent(
        &self,
        name: Option<String>,
        agent_id: Option<nexus_contracts::AgentId>,
        project: String,
        message_id: nexus_contracts::MessageId,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            let sessions = Sessions::new(&this.store);
            let target = match &agent_id {
                Some(agent_id) => sessions.find_by_agent_id(&agent_id.0).await,
                None => match name.as_deref() {
                    Some(name) => sessions.find_by_name_any_project(name).await,
                    None => return,
                },
            };
            let Ok(Some(target)) = target else {
                return;
            };
            if target.kind != "agent" {
                return;
            }

            if this
                .settle_delivery_if_target_dead(
                    &message_id,
                    &target,
                    "target is durably dead",
                    "bus_dm_dead_target",
                )
                .await
            {
                return;
            }

            if Self::is_externally_drained_session(&target)
                || this
                    .inbox_subscription_owns_delivery(&target.session_id)
                    .await
            {
                return;
            }

            let revive = match target.agent_id.as_deref() {
                Some(agent_id) => this.ensure_alive_agent(agent_id).await,
                None => match target.name.as_deref() {
                    Some(name) => this.ensure_alive(name, &project).await,
                    None => return,
                },
            };
            if let Err(error) = revive {
                let reason = format!("target could not be revived: {}", error.message);
                if !this
                    .settle_delivery_if_target_dead(&message_id, &target, &reason, "bus_dm_revive")
                    .await
                {
                    this.record_revive_failure_or_terminalize(
                        Some(&message_id),
                        &target.session_id,
                        &reason,
                        "bus_dm_revive",
                    )
                    .await;
                }
            }
        });
    }

    /// Wake every unresolved agent recipient for an explicitly targeted notification. Resolution
    /// and fan-out already committed through the canonical bus; reading the resulting delivery
    /// rows avoids a second group/thread/name resolver and keeps project out of transport keys.
    pub async fn wake_notification_recipients(&self, message_id: &nexus_contracts::MessageId) {
        let targets = match Inbox::new(&self.store)
            .delivery_targets_for_message(message_id)
            .await
        {
            Ok(targets) => targets,
            Err(error) => {
                tracing::error!(
                    target: "nexus::delivery",
                    error = %error,
                    message = %message_id,
                    "failed to resolve notification delivery targets after commit"
                );
                return;
            }
        };
        let sessions = Sessions::new(&self.store);
        for target in targets {
            let row = match target.recipient_agent_id.as_deref() {
                Some(agent_id) => sessions.find_by_agent_id(agent_id).await,
                None => match target.recipient_session.as_ref() {
                    Some(session) => sessions.find_by_session_id(session).await,
                    None => continue,
                },
            };
            let Ok(Some(row)) = row else {
                continue;
            };
            if row.kind != "agent" {
                continue;
            }
            self.wake_dm_agent(
                row.name,
                target.recipient_agent_id.or(row.agent_id).map(AgentId),
                row.project,
                message_id.clone(),
            );
        }
    }

    /// Put a cold durable DM target behind the realtime `Offline` hold before fan-out rings it.
    /// Removal normally installs this hold, but an aborted/stale loop can otherwise fall back to
    /// the registry's default `Idle` state and consume the new row before adapter rebind finishes.
    pub async fn hold_cold_dm_target(
        &self,
        name: Option<&str>,
        agent_id: Option<&nexus_contracts::AgentId>,
    ) {
        let Some(wiring) = &self.loop_wiring else {
            return;
        };
        let sessions = Sessions::new(&self.store);
        let target = match agent_id {
            Some(agent_id) => sessions.find_by_agent_id(&agent_id.0).await,
            None => match name {
                Some(name) => sessions.find_by_name_any_project(name).await,
                None => return,
            },
        };
        let Ok(Some(target)) = target else {
            return;
        };
        if target.kind != "agent"
            || Self::is_externally_drained_session(&target)
            || self
                .inbox_subscription_owns_delivery(&target.session_id)
                .await
        {
            return;
        }
        let durably_offline = target.presence.as_deref() != Some("online");
        let transport_dead = self.agent.is_harness_alive(&target.session_id) == Some(false);
        if durably_offline || transport_dead {
            wiring
                .registry
                .set(&target.session_id, nexus_dispatch::AgentState::Offline);
        }
    }

    /// Wake every AGENT member of a thread so a post actually reaches them. A thread `send` writes
    /// each member a `pending` inbox row + rings their bell, but a member whose drain loop / adapter
    /// is down (cold, or after a daemon restart) never consumes it. We [`ensure_live`] each agent
    /// member (spawns its adapter AND its drain loop, which re-rings on attach and drains the just-
    /// posted message → injects a turn → reply). Fire-and-forget per member so the send returns fast;
    /// the sender and non-agent members (operator/app) are skipped.
    pub async fn wake_thread_agents(
        &self,
        thread: &str,
        _project: &str,
        exclude: &str,
        message_id: &nexus_contracts::ids::MessageId,
    ) {
        use nexus_store::repos::{Sessions, Threads};
        let threads = Threads::new(&self.store);
        let Ok(Some(row)) = threads.find_active_any_by_name(thread).await else {
            return;
        };
        let Ok(members) = threads.members(&row.thread_id).await else {
            return;
        };
        let sessions = Sessions::new(&self.store);
        for name in members {
            if name == exclude {
                continue;
            }
            // Only agents have an ACP session to wake; skip operator/app members.
            match sessions.find_by_name_any_project(&name).await {
                Ok(Some(m)) if m.kind == "agent" && !Self::is_externally_drained_session(&m) => {
                    if self.inbox_subscription_owns_delivery(&m.session_id).await {
                        continue;
                    }
                    let this = self.clone();
                    let member_project = m.project.clone();
                    let member_agent_id = m.agent_id.clone();
                    let member_session = m.session_id.clone();
                    let message_id = message_id.clone();
                    tokio::spawn(async move {
                        // Revive the dead agent via the correct backend for its transport.
                        let revive = match member_agent_id.as_deref() {
                            Some(agent_id) => this.ensure_alive_agent(agent_id).await,
                            None => this.ensure_alive(&name, &member_project).await,
                        };
                        if let Err(e) = revive {
                            tracing::warn!(target: "nexus::thread", member = %name, error = ?e, "wake_thread_agents: ensure_alive failed");
                            let is_dead = match member_agent_id.as_deref() {
                                Some(agent_id) => {
                                    Agents::new(&this.store)
                                        .lifecycle_for_id(agent_id)
                                        .await
                                        .ok()
                                        .and_then(|(state, _)| state)
                                        .as_deref()
                                        == Some("dead")
                                }
                                None => false,
                            };
                            let reason =
                                format!("thread target could not be revived: {}", e.message);
                            if is_dead {
                                if let Err(settle_error) = Inbox::new(&this.store)
                                    .mark_delivery_error(
                                        &message_id,
                                        &member_session,
                                        nexus_store::repos::inbox::TARGET_DEAD_ERROR_CODE,
                                        &reason,
                                        Some("{\"source\":\"thread_member_revive\"}"),
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        target: "nexus::delivery",
                                        error = %settle_error,
                                        message = %message_id,
                                        session = %member_session,
                                        "failed to persist dead thread-target delivery error"
                                    );
                                }
                            } else {
                                this.record_revive_failure_or_terminalize(
                                    Some(&message_id),
                                    &member_session,
                                    &reason,
                                    "thread_member_revive",
                                )
                                .await;
                            }
                        }
                    });
                }
                _ => {}
            }
        }
    }
}
