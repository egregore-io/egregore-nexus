//! Fresh headless and headed harness launch orchestration.

use super::*;
use crate::daemon::harness_launch::harness_launch_spec;
use crate::daemon::pty_supervisor::harness_agent_token;

fn is_passive_test_program(program: &str) -> bool {
    program == "cat" || cfg!(windows) && program.eq_ignore_ascii_case("cmd.exe")
}

impl AppState {
    pub async fn launch_agent(
        &self,
        mut req: SpawnRequest,
        caller_project: &str,
        owner: Option<&Caller>,
    ) -> Result<SpawnResponse, nexus_contracts::ContractError> {
        if req.headless && !req.harness_args.is_empty() {
            return Err(nexus_contracts::ContractError {
                code: nexus_contracts::codes::INVALID_PARAMS,
                message: "harness launch arguments require a headed launch".into(),
            });
        }
        let requested_native_key = harness_registry_by_id(&req.kind)
            .requested_native_resume_key(&req.harness_args)
            .map_err(|error| NexusError::Invalid(error.to_string()).to_contract_error())?;
        if req.initial_prompt.is_some() && req.resume.is_some() {
            return Err(Self::initial_prompt_not_fresh_error());
        }
        let mut project = req
            .project
            .clone()
            .unwrap_or_else(|| caller_project.to_string());
        let route = launch_route(req.headless, self.pty.is_some(), &req.kind);
        if req.kind.as_str() == "codex" {
            if let Some(thread_id) = req.resume.as_deref() {
                if let Some(resume) = self
                    .resolve_codex_resume_identity(thread_id, &req, &project)
                    .await?
                {
                    if let Some(row) = self.codex_resume_session_row(&resume).await? {
                        if req.initial_prompt.is_some() {
                            return Err(Self::initial_prompt_not_fresh_error());
                        }
                        let session = self.ensure_codex_resume_row_live(row, thread_id).await?;
                        return Ok(SpawnResponse {
                            session_id: session,
                        });
                    }
                } else if let Some(owner) = self
                    .codex_resume_owner(thread_id)
                    .await
                    .map_err(|e| e.to_contract_error())?
                {
                    if req.initial_prompt.is_some() {
                        return Err(Self::initial_prompt_not_fresh_error());
                    }
                    if !codex_resume_session_owner_matches(&req, &project, &owner) {
                        return Err(native_session_taken_error(
                            "codex thread",
                            thread_id,
                            &owner,
                        ));
                    }
                    let session = self
                        .ensure_codex_appserver_live_row_with_thread(owner, Some(thread_id))
                        .await?;
                    return Ok(SpawnResponse {
                        session_id: session,
                    });
                }
            }
        }
        let defer_existing_id_resolution = req.kind.as_str() == "codex"
            && req.resume.is_some()
            && matches!(route, LaunchRoute::Headed(_));
        if !defer_existing_id_resolution {
            if let Some(identity) = self.resolve_existing_launch_id(&req).await? {
                if req.initial_prompt.is_some() {
                    return Err(Self::initial_prompt_not_fresh_error());
                }
                if let Some(row) = Sessions::new(&self.store)
                    .active_runtime_session_for_agent(&identity.agent_id)
                    .await
                    .map_err(|e| e.to_contract_error())?
                {
                    let session = self
                        .ensure_alive_row_with_native_request(row, requested_native_key)
                        .await?;
                    return Ok(SpawnResponse {
                        session_id: session,
                    });
                }
                project = identity.project.clone();
                req.name = identity.name.clone();
                req.project = Some(project.clone());
            }
        }

        // Route the launch: headed vs headless vs no-TUI-binary error. The pure `launch_route`
        // function encodes the decision table (see its unit tests); `launch_agent` only acts on it.
        match route {
            LaunchRoute::Headed(program) => {
                return self
                    .launch_agent_with_program(req, &project, program, owner)
                    .await;
            }
            LaunchRoute::NoTuiBinary => {
                return Err(nexus_contracts::ContractError {
                    code: nexus_contracts::codes::INVALID_PARAMS,
                    message: format!("{:?} has no TUI binary; launch with --headless", req.kind),
                });
            }
            // Headless (or pty-absent degradation): fall through to the ACP path below.
            LaunchRoute::Headless => {}
        }

        // Mock-port fallback (Self::new): no concrete agent → just delegate to the bare port and
        // register the member so it is at least addressable (no loop wiring either way).
        let Some(agent) = &self.agent_concrete else {
            return self.launch_agent_via_port(req, &project, owner).await;
        };

        // (1) Mint the canonical session id daemon-side so the member row, the adapter binding, and
        // the event loop all key on the SAME id.
        let session = nexus_common::new_session_id();
        let client_key = Self::new_client_key();
        let identity = self
            .resolve_launch_identity_for_session(&req, &project, &session)
            .await
            .map_err(|e| e.to_contract_error())?;
        req.name = identity.name.clone();
        req.project = Some(identity.project.clone());
        let launch_label = identity.launch_label().to_string();

        let prepared_adapter = agent.prepare_adapter(&req.kind)?;
        let mut observation = self.reserve_adapter_observation(
            &identity.agent_id,
            &session,
            prepared_adapter.profile(),
        )?;

        // Build the per-agent launch context before registration so the persisted runtime cwd
        // matches the actual adapter cwd, including the daemon default when `req.cwd` is omitted.
        let (cwd, env) = self.agent_launch_ctx(
            &identity.agent_id,
            identity.name.as_deref(),
            &identity.project,
            req.kind.clone(),
            &client_key,
            req.cwd.clone(),
        );

        // (2) Register the member first. With an initial prompt, hold the wake until the boot turn is
        // accepted so regular inbox/command traffic cannot overtake it.
        let registered = Self::runtime_descriptor(
            &session,
            &identity.agent_id,
            identity.name.as_deref(),
            &identity.project,
            req.kind.clone(),
            req.role.clone(),
            cwd.clone(),
        );
        let prepared_initial_prompt = if let Some(template) = req.initial_prompt.as_deref() {
            match self.prepare_initial_prompt(&registered, template).await {
                Ok(prepared) => prepared,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        if prepared_initial_prompt.is_some() {
            self.register_prepared_initial_prompt_runtime(&registered, &client_key, "acp", owner)
                .await?;
        } else {
            self.register_prepared_runtime(&registered, &client_key, "acp", owner)
                .await
                .map_err(|e| e.to_contract_error())?;
        }

        // Establish the resurrection capsule before opening the adapter. A successful ACP
        // session/new reports its native session id from `open_session_for`, whose identity port
        // updates this row in place. Persisting the descriptor afterward with `None` would erase
        // (or, on a new row, miss) the only key that can resume the harness across daemon restart.
        self.persist_runtime_resurrection_descriptor(&registered, "headless", "acp", None)
            .await
            .map_err(|e| e.to_contract_error())?;

        // (5) Open + bind the adapter under that id + project. An init failure retains the (errored)
        // session and surfaces `agent.status=errored`, then propagates — the member row + loop remain.
        let opened = if let Some(observation) = observation.as_mut() {
            match observation.commit(self).await {
                Ok(()) => agent
                    .open_session_for_observed(
                        session.clone(),
                        &launch_label,
                        &identity.project,
                        prepared_adapter,
                        cwd,
                        env,
                        None,
                        observation.sink(),
                    )
                    .await
                    .map(|opened| observation.opened(opened)),
                Err(error) => Err(error),
            }
        } else {
            agent
                .open_session_for(
                    session.clone(),
                    &launch_label,
                    &identity.project,
                    req.kind.clone(),
                    cwd,
                    env,
                    None, // fresh launch — a new session/new (resume is the not-fresh `ensure_live` path)
                )
                .await
        };
        let process_ledger = match opened {
            Ok(process_ledger) => process_ledger,
            Err(error) => {
                if req.initial_prompt.is_some() {
                    self.mark_initial_prompt_failed(&registered, &error.message)
                        .await;
                    self.fail_initial_prompt_runtime(&registered).await;
                }
                return Err(error);
            }
        };
        if let Some(entry) = process_ledger {
            self.set_runtime_process_ledger_best_effort(&session, entry, "acp launch")
                .await;
        }

        if let Some(observation) = observation.as_mut() {
            // Registration already materialized this fresh runtime. Revalidate its exact active
            // binding under presence before accepting native observations or delivering a prompt.
            if let Err(error) = observation.activate(self, agent).await {
                if req.initial_prompt.is_some() {
                    self.mark_initial_prompt_failed(&registered, &error.message)
                        .await;
                    self.fail_initial_prompt_runtime(&registered).await;
                }
                return Err(error);
            }
        }

        if let Some(prepared) = prepared_initial_prompt {
            if let Err(error) = self
                .deliver_prepared_initial_prompt(&registered, prepared)
                .await
            {
                self.fail_initial_prompt_runtime(&registered).await;
                return Err(error);
            }
        }
        self.wake_registered_runtime(&registered)
            .await
            .map_err(|e| e.to_contract_error())?;

        Ok(SpawnResponse {
            session_id: session,
        })
    }

    /// PTY-native launch: spawn `program` in a daemon-owned PTY via the [`PtySupervisor`],
    /// make the launched session an addressable + wakeable bus member (`register_and_wake`, exactly
    /// as the ACP path), and tail its harness transcript so the conversation streams to the web
    /// console + a `turn_end` rings the bell (lets the drainer push pending mail into the PTY). The
    /// supervisor's [`PtyTransport`] is already the daemon's turn-exec (wired by [`wire_pty`]) and is
    /// bound to this session inside `launch`, so a later `send` to the agent lands in its PTY with no
    /// manual `bind`. Production callers reach this via [`launch_agent`] with `harness_program(kind)`;
    /// tests call it directly with the platform's passive shell for a deterministic, offline
    /// harness stand-in.
    pub async fn launch_agent_with_program(
        &self,
        req: SpawnRequest,
        project: &str,
        program: &str,
        owner: Option<&Caller>,
    ) -> Result<SpawnResponse, nexus_contracts::ContractError> {
        use portable_pty::PtySize;

        let harness_contract = harness_registry_by_id(&req.kind);
        let requested_native_key = harness_contract
            .requested_native_resume_key(&req.harness_args)
            .map_err(|error| NexusError::Invalid(error.to_string()).to_contract_error())?;
        let nexus_exe = std::env::current_exe()
            .ok()
            .and_then(|p| p.to_str().map(String::from))
            .unwrap_or_else(|| "nexus".to_string());

        let session = nexus_common::new_session_id();
        let client_key = Self::new_client_key();
        let mut resume_identity = None;
        if req.initial_prompt.is_some() && req.resume.is_some() {
            return Err(Self::initial_prompt_not_fresh_error());
        }
        if req.kind.as_str() == "codex" {
            if let Some(thread_id) = req.resume.as_deref() {
                if let Some(identity) = self
                    .resolve_codex_resume_identity(thread_id, &req, project)
                    .await?
                {
                    if let Some(row) = self.codex_resume_session_row(&identity).await? {
                        let session = self
                            .ensure_codex_appserver_live_row_with_thread(row, Some(thread_id))
                            .await?;
                        return Ok(SpawnResponse {
                            session_id: session,
                        });
                    }
                    resume_identity = Some(identity.identity);
                } else if let Some(owner) = self
                    .codex_resume_owner(thread_id)
                    .await
                    .map_err(|e| e.to_contract_error())?
                {
                    if !codex_resume_session_owner_matches(&req, project, &owner) {
                        return Err(native_session_taken_error(
                            "codex thread",
                            thread_id,
                            &owner,
                        ));
                    }
                    let session = self
                        .ensure_codex_appserver_live_row_with_thread(owner, Some(thread_id))
                        .await?;
                    return Ok(SpawnResponse {
                        session_id: session,
                    });
                }
            }
        }
        let identity = match resume_identity {
            Some(identity) => identity,
            None => self
                .resolve_launch_identity_for_session(&req, project, &session)
                .await
                .map_err(|e| e.to_contract_error())?,
        };
        let name = identity.name.clone();
        let project = identity.project.clone();
        // Per-harness launch policy (daemon::harness_launch): claude fresh
        // launches spawn in a private per-SESSION folder and receive the
        // requested folder via `--add-dir` (transcript-slug isolation); other
        // harnesses keep the requested cwd / per-agent default.
        let launch_spec = harness_launch_spec(
            &req.kind,
            &identity.agent_id,
            &session.0,
            req.cwd.clone(),
            claude_resume_session_id(&req.harness_args).is_some(),
        );
        let cwd = launch_spec.cwd.clone();
        ensure_agent_launch_cwd(&cwd, launch_spec.private_cwd);
        let effective_harness_args: Vec<String> = req
            .harness_args
            .iter()
            .cloned()
            .chain(launch_spec.extra_args.iter().cloned())
            .collect();

        if req.initial_prompt.is_some()
            && (claude_resume_session_id(&req.harness_args).is_some()
                || requested_native_key.is_some())
        {
            return Err(Self::initial_prompt_not_fresh_error());
        }

        // Claude Code owns its native identity/session namespace. Nexus forwards an explicit
        // `--resume` key as an opaque best-effort hint and never reuses, replaces, or rejects a
        // Nexus runtime based on another row carrying the same Claude value.
        if harness_contract.headed_runtime_kind() == HeadedRuntimeKind::OpenCodePlugin {
            if let Some(native_key) = requested_native_key {
                if let Some(owner) = self
                    .opencode_native_owner(native_key)
                    .await
                    .map_err(|e| e.to_contract_error())?
                {
                    if owner.project != project || req.name.as_deref() != owner.name.as_deref() {
                        return Err(native_session_taken_error(
                            harness_contract.resume_key_description(),
                            native_key,
                            &owner,
                        ));
                    }
                    // Durable ownership survives a stopped process or daemon restart. Reuse
                    // the owner identity only after its actual transport has been restored.
                    let session = self
                        .ensure_alive_row_with_native_request(owner, Some(native_key))
                        .await?;
                    return Ok(SpawnResponse {
                        session_id: session,
                    });
                }
            }
        }

        let supervisor = self
            .pty
            .as_ref()
            .expect("launch_agent_with_program requires a PtySupervisor");
        let passive_test_program = is_passive_test_program(program);
        let headed_runtime = if passive_test_program {
            HeadedRuntimeKind::Screen
        } else {
            headed_runtime_kind(&req.kind)
        };

        // Headed OpenCode runs through a native in-process plugin loaded by `opencode serve`; keep
        // the project-local MCP config in the launch cwd as a compatible outbound tool surface for
        // the attached TUI, but do NOT start the old SQLite event tailer for new launches. Streaming
        // and delivery now ride the plugin bridge.
        if headed_runtime == HeadedRuntimeKind::OpenCodePlugin {
            if let Some(bus_name) = name.clone() {
                let ctx = nexus_agent::LaunchCtx {
                    cwd: Some(cwd.clone()),
                    bus_name: Some(bus_name),
                    bus_project: Some(project.clone()),
                    bus_client_key: Some(client_key.clone()),
                    bus_agent: Some(harness_agent_token(&req.kind).to_string()),
                    ..Default::default()
                };
                nexus_agent::write_opencode_mcp_config(&cwd, &ctx);
            }
        }

        // Hermes headed launch is gateway-native: `PtySupervisor::launch_headed_pty` builds an isolated
        // HERMES_HOME with the Nexus platform plugin and a Unix-socket bridge. Do not touch
        // ~/.hermes and do not configure MCP for headed Hermes; gateway delivery/streaming replaces
        // both tmux typing and the old state.db polling forwarder.

        // (0) Headed codex uses the app-server bridge (structured streaming), NOT PTY scraping.
        // This branch returns early — it never reaches `spawn_pty_reply_reader`.
        if req.kind.as_str() == "codex" {
            let registered = Self::runtime_descriptor(
                &session,
                &identity.agent_id,
                name.as_deref(),
                &project,
                req.kind.clone(),
                req.role.clone(),
                Some(cwd.clone()),
            );
            let prepared = match req.initial_prompt.as_deref() {
                Some(template) => self.prepare_initial_prompt(&registered, template).await?,
                None => None,
            };
            if req.initial_prompt.is_some() {
                self.register_prepared_initial_prompt_runtime(
                    &registered,
                    &client_key,
                    "codex-appserver",
                    owner,
                )
                .await?;
            } else {
                // The app-server eagerly starts the fresh Codex thread below. Register the durable
                // runtime first so the MCP process spawned by that thread can authenticate on its
                // first attempt instead of permanently caching an unavailable store client.
                self.register_prepared_runtime(&registered, &client_key, "codex-appserver", owner)
                    .await
                    .map_err(|e| e.to_contract_error())?;
            }
            // Create the resurrection capsule before the app-server eagerly creates its thread.
            // Thread discovery updates this row in place; inserting it afterward with a fresh
            // launch's `None` resume key either misses or erases the only native correlation.
            let mut model_observation = self.reserve_native_observation(
                &identity.agent_id,
                &session,
                nexus_harness_codex::app_server::model_reporting::profile(),
            )?;
            model_observation.commit(self).await?;
            self.persist_runtime_resurrection_descriptor(
                &registered,
                "headed",
                req.resolved_backend(self.launch_backend_default()),
                req.resume.clone(),
            )
            .await
            .map_err(|e| e.to_contract_error())?;
            let events = self
                .loop_wiring
                .as_ref()
                .map(|w| w.events())
                .ok_or_else(|| nexus_contracts::ContractError {
                    code: -32004,
                    message: "codex headed launch requires loop wiring (events sink)".into(),
                })?;
            let size = PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            };
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
            let state_dir = format!("{home}/.nexus");
            let launch_result = supervisor
                .launch_codex_appserver(
                    &session,
                    &identity.agent_id,
                    name.as_deref(),
                    &project,
                    &client_key,
                    &nexus_exe,
                    &cwd,
                    size,
                    events,
                    "codex",
                    None,
                    &state_dir,
                    req.resume.clone(),
                    Vec::new(),
                    &effective_harness_args,
                    Some(self.codex_thread_persistence_callback()),
                    req.resolved_backend(self.launch_backend_default()),
                    Some(model_observation.reporting()),
                )
                .await
                .map_err(|e| nexus_contracts::ContractError {
                    code: -32004,
                    message: format!("codex app-server launch failed: {e}"),
                });
            if let Err(error) = launch_result {
                if req.initial_prompt.is_some() {
                    self.mark_initial_prompt_failed(&registered, &error.message)
                        .await;
                }
                self.fail_initial_prompt_runtime(&registered).await;
                return Err(error);
            }
            if let Some(prepared) = prepared {
                if let Err(error) = self
                    .deliver_prepared_initial_prompt(&registered, prepared)
                    .await
                {
                    self.fail_initial_prompt_runtime(&registered).await;
                    return Err(error);
                }
            }
            self.wake_registered_runtime(&registered)
                .await
                .map_err(|e| e.to_contract_error())?;
            supervisor
                .stamp_codex_appserver_process_ledger(&session)
                .await
                .map_err(|e| nexus_contracts::ContractError {
                    code: -32004,
                    message: format!("codex process-ledger persistence failed: {e}"),
                })?;
            self.spawn_raw_stream_writer_for_terminal(&session).await;
            model_observation
                .activate_codex(self, &supervisor.codex_transport())
                .await?;
            return Ok(SpawnResponse {
                session_id: session,
            });
        }

        // (0b) Headed OpenCode uses a native plugin loaded inside `opencode serve`, plus a
        // foreground `opencode attach` TUI in the requested raw PTY or tmux backend. The transport
        // binding is the plugin bridge, not terminal keystrokes.
        if req.kind.as_str() == "opencode" {
            let registered = Self::runtime_descriptor(
                &session,
                &identity.agent_id,
                name.as_deref(),
                &project,
                req.kind.clone(),
                req.role.clone(),
                Some(cwd.clone()),
            );
            let prepared = match req.initial_prompt.as_deref() {
                Some(template) => self.prepare_initial_prompt(&registered, template).await?,
                None => None,
            };
            if req.initial_prompt.is_some() {
                self.register_prepared_initial_prompt_runtime(
                    &registered,
                    &client_key,
                    "opencode-plugin",
                    owner,
                )
                .await?;
            } else {
                // The captured model claim needs a durable runtime before native setup. Like the
                // app-server path, registration may survive a later failed/canceled launch.
                self.register_prepared_runtime(&registered, &client_key, "opencode-plugin", owner)
                    .await
                    .map_err(|e| e.to_contract_error())?;
            }
            let mut model_observation = self.reserve_native_observation(
                &identity.agent_id,
                &session,
                nexus_agent::adapter::opencode::native::model_profile(),
            )?;
            model_observation.commit(self).await?;
            let events = self
                .loop_wiring
                .as_ref()
                .map(|w| w.events())
                .ok_or_else(|| nexus_contracts::ContractError {
                    code: -32004,
                    message: "opencode headed launch requires loop wiring (events sink)".into(),
                })?;
            let size = PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            };
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
            let state_dir = format!("{home}/.nexus");
            let viewer_backend = req.resolved_backend(self.launch_backend_default());
            let launch_result = supervisor
                .launch_opencode_plugin(
                    &session,
                    &identity.agent_id,
                    name.as_deref(),
                    &project,
                    &client_key,
                    &cwd,
                    size,
                    events,
                    &state_dir,
                    &effective_harness_args,
                    requested_native_key,
                    false,
                    viewer_backend,
                    Some(model_observation.reporting()),
                )
                .await
                .map_err(|e| nexus_contracts::ContractError {
                    code: -32004,
                    message: format!("opencode native-plugin launch failed: {e}"),
                });
            if let Err(error) = launch_result {
                if req.initial_prompt.is_some() {
                    self.mark_initial_prompt_failed(&registered, &error.message)
                        .await;
                }
                self.fail_initial_prompt_runtime(&registered).await;
                return Err(error);
            }
            if let Some(prepared) = prepared {
                if let Err(error) = self
                    .deliver_prepared_initial_prompt(&registered, prepared)
                    .await
                {
                    self.fail_initial_prompt_runtime(&registered).await;
                    return Err(error);
                }
            }
            self.wake_registered_runtime(&registered)
                .await
                .map_err(|e| e.to_contract_error())?;
            let descriptor = Self::runtime_descriptor(
                &session,
                &identity.agent_id,
                name.as_deref(),
                &project,
                req.kind.clone(),
                req.role.clone(),
                Some(cwd.clone()),
            );
            // The harness decides which accepted native identity belongs in its capsule.
            let reported_key = OpenCodeRuntimeStateRepo::new(&self.store)
                .find_by_runtime_id(&session)
                .await
                .map_err(|e| e.to_contract_error())?
                .and_then(|state| state.opencode_session_id);
            let native_resume_key = harness_contract
                .capture_resurrection_key(requested_native_key, reported_key.as_deref())
                .map_err(|error| NexusError::Invalid(error.to_string()).to_contract_error())?;
            self.persist_runtime_resurrection_descriptor(
                &descriptor,
                "headed",
                viewer_backend,
                native_resume_key,
            )
            .await
            .map_err(|e| e.to_contract_error())?;
            supervisor
                .stamp_bound_process_ledger(&session)
                .await
                .map_err(|e| nexus_contracts::ContractError {
                    code: -32004,
                    message: format!("opencode process-ledger persistence failed: {e}"),
                })?;
            self.spawn_raw_stream_writer_for_terminal(&session).await;
            model_observation.activate_plugin(self, &supervisor).await?;
            return Ok(SpawnResponse {
                session_id: session,
            });
        }

        // (1) Bind the minted runtime identity to its native transport. Raw PTY is the default;
        // tmux requires an explicit backend choice. The passive fixture always uses raw PTY.
        // Capture output for the later reader: raw subscribe() or tmux pipe_output()/pipe-pane.
        let descriptor = Self::runtime_descriptor(
            &session,
            &identity.agent_id,
            name.as_deref(),
            &project,
            req.kind.clone(),
            req.role.clone(),
            Some(cwd.clone()),
        );
        let profile = (headed_runtime == HeadedRuntimeKind::HermesGateway)
            .then(nexus_agent::adapter::hermes::native::model_profile);
        let (registered, mut gateway_observation) = self
            .prepare_terminal_reporting(
                descriptor,
                req.initial_prompt.as_deref(),
                &client_key,
                owner,
                profile,
            )
            .await?;
        let size = PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        };
        let pty_output_result: Result<
            Option<broadcast::Receiver<Vec<u8>>>,
            nexus_contracts::ContractError,
        > = if passive_test_program {
            let launch_result = supervisor
                .launch(
                    &session,
                    program,
                    &identity.agent_id,
                    name.as_deref(),
                    &project,
                    &client_key,
                    &nexus_exe,
                    &cwd,
                    size,
                )
                .await
                .map_err(|e| nexus_contracts::ContractError {
                    code: -32004,
                    message: format!("pty launch failed: {e}"),
                });
            launch_result.map(|_| supervisor.pty_output(&session))
        } else if req.resolved_backend(self.launch_backend_default()) == "pty" {
            // The raw daemon-owned PTY
            // backend is the DEFAULT for headed launches — `backend: None` no longer means
            // tmux. tmux runs only when explicitly requested (`--backend tmux`). Hermes
            // launches here too because its gateway bridge is backend-agnostic.
            let launch_result = supervisor
                .launch_headed_raw_pty_observed(
                    &session,
                    &req.kind,
                    &identity.agent_id,
                    name.as_deref(),
                    &project,
                    &client_key,
                    &nexus_exe,
                    &cwd,
                    size,
                    &effective_harness_args,
                    self.loop_wiring.as_ref().map(|w| w.events()),
                    self.loop_wiring.as_ref().map(|w| w.bell()),
                    gateway_observation
                        .as_ref()
                        .map(|observation| observation.reporting()),
                )
                .await
                .map_err(|e| nexus_contracts::ContractError {
                    code: -32004,
                    message: format!("raw pty harness launch failed: {e}"),
                });
            launch_result.map(|()| {
                if headed_runtime != HeadedRuntimeKind::Screen {
                    None
                } else {
                    supervisor.pty_output(&session).or_else(|| {
                        let (_, rx) = broadcast::channel::<Vec<u8>>(1);
                        Some(rx)
                    })
                }
            })
        } else {
            let launch_result = supervisor
                .launch_headed_pty_observed(
                    &session,
                    &req.kind,
                    &identity.agent_id,
                    name.as_deref(),
                    &project,
                    &client_key,
                    &nexus_exe,
                    &cwd,
                    size,
                    &effective_harness_args,
                    self.loop_wiring.as_ref().map(|w| w.events()),
                    self.loop_wiring.as_ref().map(|w| w.bell()),
                    gateway_observation
                        .as_ref()
                        .map(|observation| observation.reporting()),
                )
                .await
                .map_err(|e| nexus_contracts::ContractError {
                    code: -32004,
                    message: format!("tmux harness launch failed: {e}"),
                });
            // Structured native harnesses stream from their stores; do not scrape their
            // full-screen TUIs. Other tmux harnesses still use pipe-pane + ScreenText.
            launch_result.map(|()| {
                if headed_runtime != HeadedRuntimeKind::Screen {
                    None
                } else {
                    supervisor.pty_output(&session).or_else(|| {
                        // Harness tracking lost between launch and here (race is impossible in
                        // practice, but return a dummy closed channel so nothing panics).
                        let (_, rx) = broadcast::channel::<Vec<u8>>(1);
                        Some(rx)
                    })
                }
            })
        };
        let pty_output = match pty_output_result {
            Ok(pty_output) => pty_output,
            Err(error) => {
                if let Some((registered, _)) = &registered {
                    self.mark_initial_prompt_failed(registered, &error.message)
                        .await;
                    self.fail_initial_prompt_runtime(registered).await;
                }
                return Err(error);
            }
        };

        // Claude's prompt transport settles only after the native hook forwarder observes a later
        // Stop/StopFailure record. Initial-prompt launches deliberately withhold the ordinary bus
        // wake until the boot turn is accepted, so start just this completion observer now—after
        // the PTY and sidecar paths exist, but before prompt_observed waits on its generation.
        let initial_prompt_needs_claude_forwarder =
            registered.is_some() && headed_runtime == HeadedRuntimeKind::ClaudeNative;
        if initial_prompt_needs_claude_forwarder {
            self.spawn_claude_native_forwarder_if_needed(&session).await;
        }

        // (2) Register the member + stand up its drain loop — addressable + wakeable, exactly as ACP.
        // Persist the resolved cwd so a later revive respawns the harness in the SAME folder.
        if let Some((registered, prepared)) = registered {
            if let Some(prepared) = prepared {
                if let Err(error) = self
                    .deliver_prepared_initial_prompt(&registered, prepared)
                    .await
                {
                    self.fail_initial_prompt_runtime(&registered).await;
                    return Err(error);
                }
            }
            self.wake_registered_runtime(&registered)
                .await
                .map_err(|e| e.to_contract_error())?;
        } else {
            self.register_and_wake(
                &session,
                &identity.agent_id,
                name.as_deref(),
                &project,
                req.kind.clone(),
                req.role.clone(),
                &client_key,
                Some(cwd.clone()),
                "pty",
                owner,
            )
            .await
            .map_err(|e| e.to_contract_error())?;
        }
        let descriptor = Self::runtime_descriptor(
            &session,
            &identity.agent_id,
            name.as_deref(),
            &project,
            req.kind.clone(),
            req.role.clone(),
            Some(cwd.clone()),
        );
        let viewer_backend = if passive_test_program {
            "pty"
        } else {
            req.resolved_backend(self.launch_backend_default())
        };
        self.persist_runtime_resurrection_descriptor(
            &descriptor,
            "headed",
            viewer_backend,
            claude_resume_session_id(&effective_harness_args).map(str::to_string),
        )
        .await
        .map_err(|e| e.to_contract_error())?;
        supervisor
            .stamp_bound_process_ledger(&session)
            .await
            .map_err(|e| nexus_contracts::ContractError {
                code: -32004,
                message: format!("pty process-ledger persistence failed: {e}"),
            })?;

        if let Some(observation) = gateway_observation.as_mut() {
            observation.finish_gateway(self, &supervisor).await?;
        }

        // (3) Wire the output stream. Headed structured harnesses use native forwarders; remaining
        // PTY harnesses still use the generic screen scraper.
        self.spawn_raw_stream_writer_for_terminal(&session).await;
        if let Some(w) = &self.loop_wiring {
            match headed_runtime {
                HeadedRuntimeKind::ClaudeNative => {
                    self.spawn_claude_native_forwarder_if_needed(&session).await;
                }
                HeadedRuntimeKind::OpenCodePlugin => {
                    self.spawn_opencode_native_forwarder_if_needed(&session)
                        .await;
                }
                HeadedRuntimeKind::Screen => {
                    if let Some(pty_output) = pty_output {
                        spawn_pty_reply_reader(
                            session.clone(),
                            pty_output,
                            w.events(),
                            w.bell(),
                            1500,
                        );
                    }
                }
                HeadedRuntimeKind::CodexAppServer | HeadedRuntimeKind::HermesGateway => {}
            }
        }

        Ok(SpawnResponse {
            session_id: session,
        })
    }
}
