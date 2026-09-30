//! Agent eviction, removal, process teardown, and native-owner resolution.

use super::*;

impl AppState {
    /// EVICT — remove the agent from EVERY thread it's a member of. The session itself (and its
    /// process/record) is untouched; it just leaves all rooms. `thread_members` stores stable
    /// `agent_id` when known and keeps the name as the compatibility fallback.
    pub async fn evict_agent(
        &self,
        name: &str,
        _project: &str,
    ) -> Result<(), nexus_contracts::ContractError> {
        nexus_store::repos::Threads::new(&self.store)
            .remove_member_all(name)
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(())
    }

    /// Resolve the exact session row targeted by an admin request carrying optional stable identity.
    ///
    /// Id-capable callers select the active runtime row for that `agent_id`, falling back to the
    /// newest compatibility row. The resolved row must still belong to the caller's project, matching
    /// the legacy project-scoped name lookup used when no id is supplied.
    pub(crate) async fn resolve_session_for_agent_request(
        &self,
        caller_project: &str,
        agent_id: Option<&AgentId>,
        name: &str,
    ) -> Result<Option<SessionRow>, ContractError> {
        let sessions = Sessions::new(&self.store);
        let Some(agent_id) = agent_id else {
            return sessions
                .find_by_name(caller_project, name)
                .await
                .map_err(|e| e.to_contract_error());
        };

        let row = match sessions
            .active_runtime_session_for_agent(&agent_id.0)
            .await
            .map_err(|e| e.to_contract_error())?
        {
            Some(row) => Some(row),
            None => sessions
                .find_by_agent_id(&agent_id.0)
                .await
                .map_err(|e| e.to_contract_error())?,
        };
        Ok(row.filter(|row| row.project == caller_project))
    }

    /// Persist the explicit request id onto a selected session row after authorization succeeds.
    pub(crate) async fn stamp_selected_session_agent_id(
        &self,
        row: &mut SessionRow,
        agent_id: Option<&AgentId>,
    ) -> Result<(), ContractError> {
        let Some(agent_id) = agent_id else {
            return Ok(());
        };
        if row.agent_id.as_deref() != Some(agent_id.0.as_str()) {
            let sessions = Sessions::new(&self.store);
            sessions
                .set_agent_id(&row.session_id, &agent_id.0)
                .await
                .map_err(|e| e.to_contract_error())?;
            row.agent_id = Some(agent_id.0.clone());
        }
        Ok(())
    }

    /// Return the stable identity tied to an already-selected session row.
    ///
    /// Remove/delete cascades must prefer `sessions.agent_id`; legacy rows can still recover the id
    /// from their exact runtime row, then finally from a project-scoped durable identity with the same
    /// current display name.
    pub(crate) async fn agent_id_for_selected_session(
        &self,
        row: &SessionRow,
    ) -> Result<Option<String>, ContractError> {
        if let Some(agent_id) = row.agent_id.as_ref() {
            return Ok(Some(agent_id.clone()));
        }
        if let Some(runtime) = AgentRuntimes::new(&self.store)
            .find_by_runtime_id(&row.session_id.0)
            .await
            .map_err(|e| e.to_contract_error())?
        {
            return Ok(Some(runtime.agent_id));
        }
        let Some(name) = row.name.as_deref() else {
            return Ok(None);
        };
        Ok(Agents::new(&self.store)
            .find_by_project_name(&row.project, name)
            .await
            .map_err(|e| e.to_contract_error())?
            .map(|agent| agent.agent_id))
    }

    /// REMOVE — detach a session from live delivery while retaining its store rows and inbox.
    ///
    /// This is the app-level implementation for `admin.remove`. It must not route through
    /// [`AgentTurnExecutionPort::remove`], because the live daemon's routing executor deliberately
    /// stubs `remove` as an app-level operation. Id-capable callers select the target by stable
    /// identity before any teardown, so stale names and name reuse cannot retarget the operation.
    /// `kill=true` additionally terminates the harness via the transport-specific teardown path.
    pub async fn remove_agent(
        &self,
        caller: &Caller,
        agent_id: Option<&AgentId>,
        name: &str,
        kill: bool,
    ) -> Result<nexus_contracts::RemoveResponse, nexus_contracts::ContractError> {
        let project = &caller.project;

        let row = self
            .resolve_session_for_agent_request(project, agent_id, name)
            .await?;
        let Some(mut row) = row else {
            return Ok(nexus_contracts::RemoveResponse {
                name: Some(name.to_string()),
                status: "not_found".to_string(),
            });
        };
        if protected_remove_target(&row) && !self.caller_is_human_admin(caller).await? {
            return Err(NexusError::Unauthorized.to_contract_error());
        }
        self.stamp_selected_session_agent_id(&mut row, agent_id)
            .await?;

        if kill {
            self.teardown_harness_row(&row).await;
        } else if let Some(agent) = &self.agent_concrete {
            agent.detach_session(&row.session_id, false).await;
        }
        if let Some(w) = &self.loop_wiring {
            w.teardown_session_transports(&row.session_id);
        }
        self.presence
            .mark_transport_offline(&row.session_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        let purge_agent_id = self.agent_id_for_selected_session(&row).await?;
        AgentAccessGrants::new(&self.store)
            .purge_actor(&row.project, row.name.as_deref(), purge_agent_id.as_deref())
            .await
            .map_err(|e| e.to_contract_error())?;
        self.archive_native_session_for_stop(&row.session_id)
            .await?;
        if let Some(name) = row.name.as_deref() {
            self.append_agent_lifecycle_best_effort(
                name,
                &row.session_id,
                "removed",
                "admin.remove",
            )
            .await;
        }
        self.ws
            .emit(WsEvent::AgentRemoved {
                session_id: row.session_id.clone(),
                name: row.name.clone(),
            })
            .await;

        Ok(nexus_contracts::RemoveResponse {
            name: row.name,
            status: "removed".to_string(),
        })
    }

    /// DELETE — purge the agent entirely: kill its process (if live), then erase its session row,
    /// thread memberships, in_flight rows, and messages from the daemon store. Id-capable callers
    /// purge the exact resolved row/session instead of re-resolving by name. (The gateway separately
    /// purges the web console's own Turso conversation/messages/logs.)
    pub async fn delete_agent(
        &self,
        caller: &Caller,
        agent_id: Option<&AgentId>,
        name: &str,
    ) -> Result<nexus_contracts::RemoveResponse, nexus_contracts::ContractError> {
        let project = &caller.project;
        let row = self
            .resolve_session_for_agent_request(project, agent_id, name)
            .await?;

        if matches!(row.as_ref(), Some(r) if protected_remove_target(r))
            && !self.caller_is_human_admin(caller).await?
        {
            return Err(NexusError::Unauthorized.to_contract_error());
        }

        // Kill the live harness FIRST (best-effort) so nothing keeps running after delete.
        // Route by the row's transport: pty → tmux kill; acp → close ACP child.
        if let Some(mut r) = row {
            self.stamp_selected_session_agent_id(&mut r, agent_id)
                .await?;
            let target_agent_id = self.agent_id_for_selected_session(&r).await?;
            self.teardown_harness_row(&r).await;
            self.release_native_session_bindings(&r.session_id).await?;
            if let Some(target_agent_id) = target_agent_id {
                NativeThreadBindings::new(&self.store)
                    .delete_for_agent(&target_agent_id)
                    .await
                    .map_err(|e| e.to_contract_error())?;
            }
            if let Some(name) = r.name.as_deref() {
                self.append_agent_lifecycle_best_effort(
                    name,
                    &r.session_id,
                    "deleted",
                    "admin.delete",
                )
                .await;
            }
            // Erase every trace from the daemon store (session row + in_flight + thread_members + msgs).
            nexus_store::repos::Sessions::new(&self.store)
                .purge_selected(&r)
                .await
                .map_err(|e| e.to_contract_error())?;
            return Ok(nexus_contracts::RemoveResponse {
                name: r.name,
                status: "deleted".into(),
            });
        }
        Ok(nexus_contracts::RemoveResponse {
            name: Some(name.to_string()),
            status: "deleted".into(),
        })
    }

    /// Kill the named agent's live harness (tmux/PTY) via the [`PtySupervisor`], best-effort. Resolves
    /// `name`→session (the caller's `project` first, then any project — a DM/admin target is a direct
    /// address). No-op (returns `false`) when there's no supervisor or no live harness. This is the
    /// `--kill` half of `admin.remove`: the Admin port tears down the loop, but the harness PROCESS is
    /// the supervisor's to terminate.
    pub async fn kill_harness(&self, name: &str, project: &str) -> bool {
        use nexus_store::repos::Sessions;
        let Some(pty) = &self.pty else {
            return false;
        };
        let sessions = Sessions::new(&self.store);
        let row = match sessions.find_by_name(project, name).await {
            Ok(Some(r)) => Some(r),
            _ => sessions.find_by_name_any_project(name).await.ok().flatten(),
        };
        match row {
            Some(r) => pty.kill(&r.session_id),
            None => false,
        }
    }

    /// Kill a specific PTY/tmux runtime by its already-resolved session row.
    ///
    /// `admin.remove` has already resolved the target name in the caller's project. Teardown must
    /// keep using that exact row; re-resolving by name can drift when stale or cross-project rows
    /// exist and kill the wrong harness.
    pub(crate) async fn kill_harness_row(&self, row: &SessionRow) -> bool {
        let Some(pty) = &self.pty else {
            return false;
        };
        pty.kill(&row.session_id)
    }

    #[doc(hidden)]
    pub fn codex_thread_persistence_callback(&self) -> nexus_harness_codex::ThreadDiscovered {
        let store = self.store.clone();
        Arc::new(move |session, thread_id| {
            let store = store.clone();
            tokio::spawn(async move {
                persist_codex_thread_binding_with_retry(store, session, thread_id).await;
            });
        })
    }

    pub(crate) async fn codex_resume_owner(
        &self,
        thread_id: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        let sessions = Sessions::new(&self.store);
        let mut owners = sessions
            .find_by_harness_session_id_any_project(thread_id)
            .await?;
        let sidecar_rows = nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&self.store)
            .find_by_thread_id(thread_id)
            .await?;
        for state in sidecar_rows {
            if owners
                .iter()
                .any(|owner| owner.session_id == state.runtime_id)
            {
                continue;
            }
            if let Some(owner) = sessions.find_by_session_id(&state.runtime_id).await? {
                owners.push(owner);
            }
        }
        owners.sort_by_key(|owner| owner.created_at);
        Ok(owners.into_iter().next())
    }

    pub(crate) async fn resolve_codex_resume_identity(
        &self,
        thread_id: &str,
        req: &SpawnRequest,
        default_project: &str,
    ) -> Result<Option<CodexResumeIdentity>, ContractError> {
        let Some(binding) = NativeThreadBindings::new(&self.store)
            .find("codex", thread_id)
            .await
            .map_err(|e| e.to_contract_error())?
        else {
            return Ok(None);
        };
        let agent = Agents::new(&self.store)
            .find_by_id(&binding.agent_id)
            .await
            .map_err(|e| e.to_contract_error())?
            .ok_or_else(|| {
                NexusError::NotFound(format!(
                    "native codex thread owner agent {}",
                    binding.agent_id
                ))
                .to_contract_error()
            })?;
        if agent.disabled_at.is_some() {
            return Err(NexusError::Unauthorized.to_contract_error());
        }
        if !codex_resume_agent_owner_matches(req, default_project, &agent) {
            return Err(codex_thread_taken_error(
                thread_id,
                agent
                    .require_name("codex resume owner")
                    .map_err(|e| e.to_contract_error())?,
                &agent.agent_id,
            ));
        }
        let name = agent
            .require_name("codex resume identity")
            .map_err(|e| e.to_contract_error())?
            .to_string();
        Ok(Some(CodexResumeIdentity {
            identity: LaunchIdentity {
                agent_id: agent.agent_id,
                name: Some(name),
                project: agent.project,
            },
            binding,
        }))
    }

    pub(crate) async fn codex_resume_session_row(
        &self,
        resume: &CodexResumeIdentity,
    ) -> Result<Option<SessionRow>, ContractError> {
        let sessions = Sessions::new(&self.store);
        if let Some(runtime_id) = resume.binding.last_runtime_id.as_deref() {
            let session_id = SessionId(runtime_id.to_string());
            if let Some(row) = sessions
                .find_by_session_id(&session_id)
                .await
                .map_err(|e| e.to_contract_error())?
            {
                if row.agent_id.as_deref() == Some(resume.identity.agent_id.as_str()) {
                    return Ok(Some(row));
                }
            }
        }
        sessions
            .find_by_agent_id(&resume.identity.agent_id)
            .await
            .map_err(|e| e.to_contract_error())
    }

    pub(crate) async fn release_native_session_bindings(
        &self,
        session_id: &SessionId,
    ) -> Result<(), ContractError> {
        self.archive_native_session_state(session_id, "release")
            .await;
        Sessions::new(&self.store)
            .clear_harness_session_id(session_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        let native_bindings = NativeThreadBindings::new(&self.store);
        for harness in ["claude", "codex", "opencode", "hermes"] {
            native_bindings
                .mark_released_for_runtime(harness, &session_id.0)
                .await
                .map_err(|e| e.to_contract_error())?;
        }
        nexus_harness_claude::storage::ClaudeRuntimeStateRepo::new(&self.store)
            .delete(session_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        nexus_harness_codex::storage::CodexRuntimeStateRepo::new(&self.store)
            .delete(session_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        OpenCodeRuntimeStateRepo::new(&self.store)
            .delete(session_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        HermesRuntimeStateRepo::new(&self.store)
            .delete(session_id)
            .await
            .map_err(|e| e.to_contract_error())?;
        Ok(())
    }

    /// Final-flush native transcripts when a runtime is stopped while retaining the native
    /// session identifiers and sidecar records required to revive it under the same identity.
    pub(crate) async fn archive_native_session_for_stop(
        &self,
        session_id: &SessionId,
    ) -> Result<(), ContractError> {
        self.archive_native_session_state(session_id, "stop").await;
        Ok(())
    }

    async fn archive_native_session_state(&self, session_id: &SessionId, event: &str) {
        if self.store.has_split_authority() {
            return;
        }
        if let Err(error) =
            archive_claude_once(self.store.clone(), session_id, Some(event.to_string())).await
        {
            tracing::warn!(
                target: "nexus::transcript_archive",
                session = %session_id,
                error = %error,
                event,
                "failed to final-flush Claude transcript during native lifecycle transition"
            );
        }
        if let Err(error) =
            archive_codex_once(self.store.clone(), session_id, Some(event.to_string())).await
        {
            tracing::warn!(
                target: "nexus::transcript_archive",
                session = %session_id,
                error = %error,
                event,
                "failed to final-flush Codex rollout during native lifecycle transition"
            );
        }
    }

    pub(crate) async fn opencode_native_owner(
        &self,
        opencode_session_id: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        if let Some(owner) = self
            .native_thread_binding_owner_row("opencode", opencode_session_id)
            .await?
        {
            return Ok(Some(owner));
        }
        let Some(state) = OpenCodeRuntimeStateRepo::new(&self.store)
            .find_by_opencode_session_id(opencode_session_id)
            .await?
        else {
            return Ok(None);
        };
        Sessions::new(&self.store)
            .find_by_session_id(&state.runtime_id)
            .await
    }

    pub(crate) async fn native_thread_binding_owner_row(
        &self,
        harness: &str,
        native_thread_id: &str,
    ) -> Result<Option<SessionRow>, NexusError> {
        let Some(binding) = NativeThreadBindings::new(&self.store)
            .find(harness, native_thread_id)
            .await?
        else {
            return Ok(None);
        };
        let sessions = Sessions::new(&self.store);
        if let Some(runtime_id) = binding.last_runtime_id.as_deref() {
            if let Some(row) = sessions
                .find_by_session_id(&SessionId(runtime_id.to_string()))
                .await?
            {
                if row.agent_id.as_deref() == Some(binding.agent_id.as_str()) {
                    return Ok(Some(row));
                }
            }
        }
        sessions.find_by_agent_id(&binding.agent_id).await
    }

    pub(crate) async fn resolve_session_by_name(
        &self,
        name: &str,
        project: &str,
    ) -> Result<SessionRow, nexus_contracts::ContractError> {
        let sessions = Sessions::new(&self.store);
        match sessions
            .find_by_name(project, name)
            .await
            .map_err(|e| e.to_contract_error())?
        {
            Some(r) => Ok(r),
            None => sessions
                .find_by_name_any_project(name)
                .await
                .map_err(|e| e.to_contract_error())?
                .ok_or_else(|| nexus_contracts::ContractError {
                    code: nexus_contracts::codes::NOT_FOUND,
                    message: format!("no agent named {name}"),
                }),
        }
    }
}
