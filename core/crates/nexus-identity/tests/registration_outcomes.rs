//! Actual IdentityPort registration controls. Included service has its own trait identity;
//! these tests do not establish daemon wiring or same-Arc coordinator correspondence.
#![allow(dead_code)]

#[path = "../src/binding.rs"]
mod binding;
#[path = "../src/error.rs"]
mod error;
#[path = "../src/presence.rs"]
mod presence;
#[path = "../src/registry.rs"]
mod registry;
#[path = "../src/service.rs"]
mod service;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use nexus_common::{hash_runtime_credential, Config, NexusError};
use nexus_contracts::{
    events::WsEvent,
    ports::{EventSink, IdentityPort},
    register::RegisterRequest,
    HarnessId, Tier,
};
use nexus_store::{
    repos::{
        agent_runtimes::{RuntimeActivationCommitState, SelectedRuntimeActivation},
        sessions::{CapturedStagedSession, SelectedStagedSessionCleanup},
        AgentCredentials, AgentRuntimes, Agents, NewAgent, NewAgentCredential, NewAgentRuntime,
        Sessions,
    },
    Store,
};
use service::{Identity, RuntimeActivation, RuntimeActivationError, RuntimeActivationRequest};

#[derive(Clone, Copy, Default)]
enum Mode {
    #[default]
    Normal,
    Secondary,
    AfterStore,
    Unavailable,
    Reject,
    Replace,
    ReplaceAfterStore,
    CleanupReplace,
    CleanupError,
    CleanupAbsent,
    ChangeSession,
}

struct SelectedStore {
    store: Arc<Store>,
    mode: Mutex<Mode>,
    cleanup_calls: AtomicUsize,
    calls: Mutex<Vec<bool>>,
    outcomes: Mutex<Vec<String>>,
    guard: Mutex<Option<usize>>,
}

impl SelectedStore {
    fn observe_guard(&self, guard: &Arc<tokio::sync::OwnedMutexGuard<()>>) {
        let pointer = Arc::as_ptr(guard) as usize;
        let mut previous = self.guard.lock().unwrap();
        if let Some(previous) = *previous {
            assert_eq!(
                pointer, previous,
                "activation and cleanup use the same held Arc"
            );
        }
        *previous = Some(pointer);
    }

    fn next_registration(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
        *self.guard.lock().unwrap() = None;
    }
}

#[async_trait]
impl RuntimeActivation for SelectedStore {
    async fn cleanup_non_agent_residue(
        &self,
        _: service::NonAgentResumeOperation,
        _: Arc<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<nexus_store::repos::agent_runtimes::SelectedRuntimeResidueCleanup, NexusError> {
        Err(NexusError::Internal(
            "unexpected non-agent residue cleanup in registration fixture".into(),
        ))
    }

    async fn set_identity_offline(
        &self,
        _: service::IdentityOfflineOperation,
        _: Arc<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<(), NexusError> {
        Err(NexusError::Internal(
            "unexpected Identity offline in registration fixture".into(),
        ))
    }

    async fn cleanup_unbound_registration(
        &self,
        staged: CapturedStagedSession,
        transition: Arc<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<SelectedStagedSessionCleanup, NexusError> {
        self.observe_guard(&transition);
        self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        assert!(AgentRuntimes::new(&self.store)
            .find_by_runtime_id(&staged.row().session_id.0)
            .await?
            .is_none());
        let mode = *self.mode.lock().unwrap();
        match mode {
            Mode::CleanupReplace => {
                self.store
                    .conn
                    .execute(
                        "UPDATE sessions SET current_work='replacement' WHERE session_id=?1",
                        [staged.row().session_id.0.as_str()],
                    )
                    .await
                    .unwrap();
            }
            Mode::CleanupError => {
                return Err(NexusError::Internal("cleanup secondary cause".into()))
            }
            Mode::CleanupAbsent => {
                self.store
                    .conn
                    .execute(
                        "DELETE FROM sessions WHERE session_id=?1",
                        [staged.row().session_id.0.as_str()],
                    )
                    .await
                    .unwrap();
            }
            _ => {}
        }
        Sessions::new(&self.store)
            .remove_staged_registration_selected(&staged)
            .await
    }

    async fn activate_runtime(
        &self,
        request: RuntimeActivationRequest,
        transition: Arc<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<SelectedRuntimeActivation, RuntimeActivationError> {
        self.observe_guard(&transition);
        let mode = *self.mode.lock().unwrap();
        self.calls
            .lock()
            .unwrap()
            .push(matches!(request, RuntimeActivationRequest::CreateActive(_)));
        if matches!(mode, Mode::Reject) {
            return Err(RuntimeActivationError::RejectedBeforeStore {
                cause: NexusError::Invalid("admission rejected".into()),
            });
        }
        let repo = AgentRuntimes::new(&self.store);
        let (id, agent) = match &request {
            RuntimeActivationRequest::CreateActive(runtime) => {
                (&runtime.runtime_id, &runtime.agent_id)
            }
            RuntimeActivationRequest::ActivateExisting {
                runtime_id,
                agent_id,
            } => (&runtime_id.0, &agent_id.0),
        };
        let siblings = repo
            .active_sibling_runtime_pairs(agent, id)
            .await
            .map_err(|cause| RuntimeActivationError::RejectedBeforeStore { cause })?;
        if matches!(mode, Mode::Replace | Mode::ReplaceAfterStore) {
            repo.create(NewAgentRuntime {
                runtime_id: id.clone(),
                agent_id: "a_other".into(),
                harness: "claude".into(),
                cwd: None,
                transport: None,
                presence: Some("offline".into()),
                active: false,
            })
            .await
            .unwrap();
        }
        if matches!(mode, Mode::ChangeSession) {
            self.store
                .conn
                .execute(
                    "UPDATE sessions SET current_work='replacement' WHERE session_id=?1",
                    [id.as_str()],
                )
                .await
                .unwrap();
        }
        let result = match request {
            RuntimeActivationRequest::CreateActive(runtime) => {
                repo.create_active_selected(runtime, &siblings).await
            }
            RuntimeActivationRequest::ActivateExisting {
                runtime_id,
                agent_id,
            } => {
                repo.activate_selected(&runtime_id.0, &agent_id.0, &siblings)
                    .await
            }
        };
        self.outcomes.lock().unwrap().push(match &result {
            Ok(SelectedRuntimeActivation::Applied(_)) => "Applied".into(),
            Ok(SelectedRuntimeActivation::SelectionChanged) => "SelectionChanged".into(),
            Err(failure) => format!("{:?}", failure.commit_state()),
        });
        match result {
            Ok(receipt) if matches!(mode, Mode::AfterStore | Mode::ReplaceAfterStore) => {
                Err(RuntimeActivationError::AfterStore {
                    receipt,
                    cause: NexusError::Internal("after-store secondary cause".into()),
                })
            }
            Ok(_) if matches!(mode, Mode::Unavailable) => {
                Err(RuntimeActivationError::OutcomeUnavailable {
                    cause: NexusError::Internal("outcome channel lost".into()),
                })
            }
            Ok(receipt) => Ok(receipt),
            Err(failure) => Err(RuntimeActivationError::Store {
                failure,
                settlement_error: matches!(mode, Mode::Secondary)
                    .then(|| NexusError::Internal("settlement secondary cause".into())),
            }),
        }
    }
}

#[derive(Default)]
struct Sink(Mutex<Vec<WsEvent>>);

#[async_trait]
impl EventSink for Sink {
    async fn emit(&self, event: WsEvent) {
        self.0.lock().unwrap().push(event);
    }
}

async fn fixture() -> (Identity, Arc<Store>, Arc<Sink>, Arc<SelectedStore>) {
    let store = Arc::new(Store::open(":memory:").await.unwrap());
    store.migrate().await.unwrap();
    let sink = Arc::new(Sink::default());
    let delegate = Arc::new(SelectedStore {
        store: store.clone(),
        mode: Mutex::new(Mode::Normal),
        cleanup_calls: AtomicUsize::new(0),
        calls: Mutex::new(Vec::new()),
        outcomes: Mutex::new(Vec::new()),
        guard: Mutex::new(None),
    });
    let identity = Identity::new_with_runtime_activation(
        store.clone(),
        sink.clone(),
        &Config::default(),
        delegate.clone(),
    );
    (identity, store, sink, delegate)
}

fn request() -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some("caller".into()),
        harness: HarnessId::new("claude").unwrap(),
        harness_session_id: "native-key".into(),
        project: "project".into(),
        client_key: "key".into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: None,
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

#[tokio::test]
async fn applied_then_required_session_stamp_failure_retains_coupled_identity() {
    let (identity, store, sink, delegate) = fixture().await;
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER fail_stamp BEFORE UPDATE OF agent_id ON sessions
        BEGIN SELECT RAISE(ABORT, 'required stamp failed'); END;",
        )
        .await
        .unwrap();
    let failure = identity.register(request()).await.unwrap_err();
    assert!(failure.message.contains("required stamp failed"));
    let row = Sessions::new(&store)
        .find_by_client_key("project", "key")
        .await
        .unwrap();
    assert!(
        row.is_some(),
        "a runtime effect followed by a required write failure must retain the Session/client key"
    );
    let row = row.unwrap();
    assert!(Agents::new(&store)
        .find_by_id(&format!("a_{}", row.session_id.0))
        .await
        .unwrap()
        .is_some());
    assert!(sink.0.lock().unwrap().is_empty());
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
    assert_eq!(*delegate.outcomes.lock().unwrap(), ["Applied"]);
    assert!(failure.message.contains("creation attempted"));
    store
        .conn
        .execute_batch("DROP TRIGGER fail_stamp;")
        .await
        .unwrap();
    delegate.next_registration(Mode::Normal);
    let retry = identity.register(request()).await.unwrap();
    assert_eq!(retry.session_id, row.session_id);
    assert_eq!(retry.agent_id.unwrap().0, format!("a_{}", row.session_id.0));
    assert_eq!(count(&store, "agents").await, 1);
}

async fn count(store: &Store, table: &str) -> i64 {
    store
        .conn
        .query(&format!("SELECT COUNT(*) FROM {table}"), ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap()
}

async fn agent(store: &Store, id: &str, name: &str) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: id.into(),
            project: "project".into(),
            name: Some(name.into()),
            default_harness: Some("claude".into()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
}

async fn explicit_request(store: &Store) -> RegisterRequest {
    agent(store, "a_caller", "caller").await;
    AgentCredentials::new(store)
        .create_hash(NewAgentCredential {
            credential_id: "credential".into(),
            agent_id: "a_caller".into(),
            secret_hash: hash_runtime_credential("secret"),
            purpose: None,
            label: None,
            scopes_json: r#"["runtime:register"]"#.into(),
            metadata_json: None,
        })
        .await
        .unwrap();
    let mut req = request();
    req.agent_id = Some(nexus_contracts::AgentId("a_caller".into()));
    req.runtime_credential = Some("secret".into());
    req
}

async fn retained(store: &Store) -> nexus_store::types::SessionRow {
    Sessions::new(store)
        .find_by_client_key_any_project("key")
        .await
        .unwrap()
        .expect("retained Session/client key")
}

async fn assert_visible_partial(
    identity: &Identity,
    row: &nexus_store::types::SessionRow,
    expected_agent: Option<&str>,
) {
    let caller = binding::caller_from_row(row);
    let members = identity
        .members(
            &caller,
            nexus_contracts::register::MemberListRequest {
                project: None,
                include_offline: Some(true),
                include_dead: Some(true),
            },
        )
        .await
        .unwrap()
        .members;
    let member = members
        .iter()
        .find(|member| member.session_id == row.session_id)
        .expect("retained rows are visible, not hidden staging");
    assert_eq!(
        member.agent_id.as_ref().map(|id| id.0.as_str()),
        expected_agent
    );
    assert_eq!(
        member.presence,
        presence::presence_from_str(row.presence.as_deref())
    );
}

#[tokio::test]
async fn applied_inside_bind_survives_required_presence_failure_without_creation_veto() {
    let (identity, store, sink, delegate) = fixture().await;
    let req = explicit_request(&store).await;
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER seed_runtime AFTER INSERT ON sessions BEGIN
        INSERT INTO agent_runtimes(runtime_id,agent_id,harness,presence,active,started_at)
        VALUES(NEW.session_id,'a_caller','claude','offline',0,1); END;
        CREATE TRIGGER fail_presence BEFORE UPDATE OF presence ON agent_runtimes
        BEGIN SELECT RAISE(ABORT,'required bind presence failed'); END;",
        )
        .await
        .unwrap();
    let failure = identity.register(req.clone()).await.unwrap_err();
    assert!(failure.message.contains("required bind presence failed"));
    assert!(failure.message.contains("runtime activation applied"));
    assert!(!failure.message.contains("creation attempted"));
    let row = retained(&store).await;
    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id(&row.session_id.0)
        .await
        .unwrap()
        .unwrap();
    assert!(runtime.active);
    assert_eq!(
        runtime.presence.as_deref(),
        Some("offline"),
        "no online repair after required failure"
    );
    assert_eq!(
        *delegate.calls.lock().unwrap(),
        [false],
        "nonresume existing runtime delegates ActivateExisting"
    );
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
    assert!(sink.0.lock().unwrap().is_empty());
    assert_eq!(count(&store, "developer_events").await, 0);
    store
        .conn
        .execute_batch("DROP TRIGGER fail_presence;")
        .await
        .unwrap();
    delegate.next_registration(Mode::Normal);
    let retry = identity.register(req).await.unwrap();
    assert_eq!(retry.session_id, row.session_id);
    assert_eq!(count(&store, "agents").await, 1);
}

#[tokio::test]
async fn explicit_preexisting_agent_confirmed_runtime_no_effect_cleans_advanced_stamp() {
    let (identity, store, sink, delegate) = fixture().await;
    let req = explicit_request(&store).await;
    store.conn.execute_batch("CREATE TRIGGER fail_runtime BEFORE INSERT ON agent_runtimes BEGIN SELECT RAISE(ABORT,'runtime insert failed'); END;").await.unwrap();
    let failure = identity.register(req).await.unwrap_err();
    assert!(failure.message.contains("runtime insert failed"));
    assert!(!failure.message.contains("retained"));
    assert_eq!(*delegate.outcomes.lock().unwrap(), ["NotCommitted"]);
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        count(&store, "sessions").await,
        0,
        "advanced captured stamp, not insertion image, authorized cleanup"
    );
    assert_eq!(count(&store, "agent_runtimes").await, 0);
    assert_eq!(count(&store, "agents").await, 1);
    assert_eq!(count(&store, "agent_credentials").await, 1);
    assert!(sink.0.lock().unwrap().is_empty());
    assert_eq!(count(&store, "developer_events").await, 0);
}

#[tokio::test]
async fn uncertain_runtime_store_failure_keeps_original_and_secondary_causes() {
    let (identity, store, _, delegate) = fixture().await;
    let req = explicit_request(&store).await;
    *delegate.mode.lock().unwrap() = Mode::Secondary;
    store.conn.execute_batch("CREATE TRIGGER fail_runtime BEFORE INSERT ON agent_runtimes BEGIN SELECT RAISE(ROLLBACK,'runtime transaction lost'); END;").await.unwrap();
    let failure = identity.register(req.clone()).await.unwrap_err();
    assert!(failure.message.contains("runtime transaction lost"));
    assert!(failure.message.contains("settlement secondary cause"));
    assert!(failure.message.contains("Unknown"));
    assert_eq!(*delegate.outcomes.lock().unwrap(), ["Unknown"]);
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
    let row = retained(&store).await;
    assert_eq!(count(&store, "agent_runtimes").await, 0);
    store
        .conn
        .execute_batch("DROP TRIGGER fail_runtime;")
        .await
        .unwrap();
    delegate.next_registration(Mode::Normal);
    assert_eq!(
        identity.register(req).await.unwrap().session_id,
        row.session_id
    );
    assert_eq!(
        *delegate.calls.lock().unwrap(),
        [true, false],
        "missing resume runtime is created inactive; only later activation delegated"
    );
}

#[tokio::test]
async fn committed_runtime_store_failure_keeps_original_and_secondary_causes() {
    let (identity, store, sink, delegate) = fixture().await;
    let req = explicit_request(&store).await;
    AgentRuntimes::new(&store)
        .create(NewAgentRuntime {
            runtime_id: "sibling".into(),
            agent_id: "a_caller".into(),
            harness: "claude".into(),
            cwd: None,
            transport: None,
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
    store.conn.execute_batch("CREATE TRIGGER fail_event BEFORE INSERT ON developer_events WHEN NEW.session_id='sibling' BEGIN SELECT RAISE(ABORT,'stopped event failed'); END;").await.unwrap();
    *delegate.mode.lock().unwrap() = Mode::Secondary;
    let failure = identity.register(req).await.unwrap_err();
    assert!(
        failure.message.contains("stopped event failed"),
        "{}",
        failure.message
    );
    assert!(failure.message.contains("settlement secondary cause"));
    assert!(failure.message.contains("Committed"));
    assert_eq!(*delegate.outcomes.lock().unwrap(), ["Committed"]);
    let row = retained(&store).await;
    assert!(
        AgentRuntimes::new(&store)
            .find_by_runtime_id(&row.session_id.0)
            .await
            .unwrap()
            .unwrap()
            .active
    );
    assert!(
        !AgentRuntimes::new(&store)
            .find_by_runtime_id("sibling")
            .await
            .unwrap()
            .unwrap()
            .active
    );
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
    assert!(sink.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn actual_selection_changed_and_after_store_selection_changed_preserve_replacement() {
    for mode in [Mode::Replace, Mode::ReplaceAfterStore] {
        let (identity, store, _, delegate) = fixture().await;
        let req = explicit_request(&store).await;
        agent(&store, "a_other", "other").await;
        *delegate.mode.lock().unwrap() = mode;
        let failure = identity.register(req.clone()).await.unwrap_err();
        assert!(failure.message.contains("cleanup authority lost"));
        if matches!(mode, Mode::ReplaceAfterStore) {
            assert!(failure.message.contains("after-store secondary cause"));
        }
        let row = retained(&store).await;
        assert_eq!(
            AgentRuntimes::new(&store)
                .find_by_runtime_id(&row.session_id.0)
                .await
                .unwrap()
                .unwrap()
                .agent_id,
            "a_other"
        );
        assert_eq!(*delegate.outcomes.lock().unwrap(), ["SelectionChanged"]);
        assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
        delegate.next_registration(Mode::Normal);
        let retry = identity.register(req).await.unwrap_err();
        assert!(
            retry.message.contains("bound to"),
            "runtime owner conflict is not bypassed by retained key"
        );
        assert_eq!(delegate.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn applied_after_store_error_and_unavailable_outcome_are_retained() {
    for (mode, cause) in [
        (Mode::AfterStore, "after-store secondary cause"),
        (Mode::Unavailable, "outcome channel lost"),
    ] {
        let (identity, store, sink, delegate) = fixture().await;
        let req = explicit_request(&store).await;
        *delegate.mode.lock().unwrap() = mode;
        let failure = identity.register(req.clone()).await.unwrap_err();
        assert!(failure.message.contains(cause));
        assert!(failure.message.contains("retained/partial"));
        assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
        let row = retained(&store).await;
        assert!(
            AgentRuntimes::new(&store)
                .find_by_runtime_id(&row.session_id.0)
                .await
                .unwrap()
                .unwrap()
                .active
        );
        assert!(sink.0.lock().unwrap().is_empty());
        delegate.next_registration(Mode::Normal);
        let mut bad = req.clone();
        bad.runtime_credential = Some("wrong".into());
        assert_eq!(
            identity.register(bad).await.unwrap_err().code,
            nexus_contracts::codes::UNAUTHORIZED
        );
        assert_eq!(
            identity.register(req).await.unwrap().session_id,
            row.session_id
        );
    }
}

#[tokio::test]
async fn agent_creation_attempt_is_recorded_before_await_even_on_create_error() {
    for raise in ["ABORT", "ROLLBACK"] {
        let (identity, store, sink, delegate) = fixture().await;
        store.conn.execute_batch(&format!("CREATE TRIGGER fail_agent BEFORE INSERT ON agents BEGIN SELECT RAISE({raise},'agent create failed'); END;")).await.unwrap();
        let failure = identity.register(request()).await.unwrap_err();
        assert!(failure.message.contains("agent create failed"));
        assert!(failure.message.contains("creation attempted"));
        let row = retained(&store).await;
        assert!(
            row.agent_id.is_none(),
            "retention does not synthesize a completed binding"
        );
        assert_eq!(count(&store, "agents").await, 0);
        assert_eq!(count(&store, "agent_runtimes").await, 0);
        assert_visible_partial(&identity, &row, None).await;
        assert_eq!(
            identity.resolve("project", "caller").await.unwrap().session,
            row.session_id
        );
        assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
        assert!(delegate.calls.lock().unwrap().is_empty());
        assert!(sink.0.lock().unwrap().is_empty());
        store
            .conn
            .execute_batch("DROP TRIGGER fail_agent;")
            .await
            .unwrap();
        delegate.next_registration(Mode::Normal);
        let retry = identity.register(request()).await.unwrap();
        assert_eq!(retry.session_id, row.session_id);
        assert_eq!(retry.agent_id.unwrap().0, format!("a_{}", row.session_id.0));
        assert_eq!(count(&store, "agents").await, 1);
    }
}

#[tokio::test]
async fn created_agent_is_retained_despite_later_confirmed_runtime_no_effect() {
    let (identity, store, _, delegate) = fixture().await;
    store.conn.execute_batch("CREATE TRIGGER fail_runtime BEFORE INSERT ON agent_runtimes BEGIN SELECT RAISE(ABORT,'runtime insert failed'); END;").await.unwrap();
    let failure = identity.register(request()).await.unwrap_err();
    assert!(failure.message.contains("NotCommitted"));
    assert!(failure.message.contains("creation attempted"));
    let row = retained(&store).await;
    assert_eq!(count(&store, "agents").await, 1);
    assert!(row.agent_id.is_none());
    assert_visible_partial(&identity, &row, Some(&format!("a_{}", row.session_id.0))).await;
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
    store
        .conn
        .execute_batch("DROP TRIGGER fail_runtime;")
        .await
        .unwrap();
    delegate.next_registration(Mode::Normal);
    assert_eq!(
        identity.register(request()).await.unwrap().session_id,
        row.session_id
    );
    assert_eq!(count(&store, "agents").await, 1);
}

#[tokio::test]
async fn selected_stamp_error_never_uses_late_reread_cleanup_authority() {
    for commit_failure in [false, true] {
        let (identity, store, _, delegate) = fixture().await;
        let req = explicit_request(&store).await;
        if commit_failure {
            store.conn.execute_batch("PRAGMA foreign_keys=ON;
                CREATE TABLE parent(id INTEGER PRIMARY KEY);
                CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
                CREATE TRIGGER fail_stamp AFTER UPDATE OF agent_id ON sessions BEGIN INSERT INTO child VALUES(1); END;").await.unwrap();
        } else {
            store.conn.execute_batch("CREATE TRIGGER fail_stamp AFTER UPDATE OF agent_id ON sessions BEGIN UPDATE sessions SET current_work='corrupt' WHERE session_id=NEW.session_id; END;").await.unwrap();
        }
        let failure = identity.register(req).await.unwrap_err();
        assert!(failure.message.contains(if commit_failure {
            "FOREIGN KEY"
        } else {
            "unexpected fields"
        }));
        assert!(failure.message.contains("stamp failed; outcome uncertain"));
        let row = retained(&store).await;
        assert!(commit_failure || row.agent_id.is_none(), "precommit rollback preserves insertion image; commit error grants no assertion of rollback");
        assert!(delegate.calls.lock().unwrap().is_empty());
        assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn selected_stamp_mismatch_preserves_changed_session_without_restamping() {
    let (identity, store, _, delegate) = fixture().await;
    *delegate.mode.lock().unwrap() = Mode::ChangeSession;
    let failure = identity.register(request()).await.unwrap_err();
    assert!(failure.message.contains("Session selection changed"));
    let row = retained(&store).await;
    assert_eq!(row.current_work.as_deref(), Some("replacement"));
    assert!(row.agent_id.is_none());
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cleanup_selection_change_or_error_preserves_initiating_failure() {
    for mode in [
        Mode::CleanupReplace,
        Mode::CleanupError,
        Mode::CleanupAbsent,
    ] {
        let (identity, store, _, delegate) = fixture().await;
        let req = explicit_request(&store).await;
        *delegate.mode.lock().unwrap() = mode;
        store.conn.execute_batch("CREATE TRIGGER fail_runtime BEFORE INSERT ON agent_runtimes BEGIN SELECT RAISE(ABORT,'runtime initiating cause'); END;").await.unwrap();
        let failure = identity.register(req).await.unwrap_err();
        assert!(failure.message.contains("runtime initiating cause"));
        assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 1);
        match mode {
            Mode::CleanupReplace => {
                assert_eq!(
                    retained(&store).await.current_work.as_deref(),
                    Some("replacement")
                );
                assert!(failure.message.contains("cleanup authority lost"));
            }
            Mode::CleanupError => {
                retained(&store).await;
                assert!(failure.message.contains("cleanup secondary cause"));
            }
            Mode::CleanupAbsent => {
                assert_eq!(count(&store, "sessions").await, 0);
            }
            _ => unreachable!(),
        }
        assert_eq!(count(&store, "agents").await, 1);
    }
}

#[tokio::test]
async fn rejected_before_store_can_cleanup_fresh_but_never_resumed_registration() {
    let (identity, store, _, delegate) = fixture().await;
    let req = explicit_request(&store).await;
    *delegate.mode.lock().unwrap() = Mode::Reject;
    assert!(identity
        .register(req.clone())
        .await
        .unwrap_err()
        .message
        .contains("admission rejected"));
    assert_eq!(count(&store, "sessions").await, 0);
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 1);
    delegate.next_registration(Mode::Normal);
    let first = identity.register(req.clone()).await.unwrap();
    delegate.next_registration(Mode::Reject);
    assert!(identity.register(req).await.is_err());
    assert_eq!(retained(&store).await.session_id, first.session_id);
    assert_eq!(
        delegate.cleanup_calls.load(Ordering::SeqCst),
        1,
        "resumed result never grants insertion cleanup authority"
    );
}

#[tokio::test]
async fn missing_resumed_runtime_is_inactive_before_later_required_write_failure() {
    let (identity, store, _, delegate) = fixture().await;
    let req = explicit_request(&store).await;
    store.conn.execute_batch("CREATE TRIGGER fail_runtime BEFORE INSERT ON agent_runtimes BEGIN SELECT RAISE(ROLLBACK,'runtime transaction lost'); END;").await.unwrap();
    identity.register(req.clone()).await.unwrap_err();
    let row = retained(&store).await;
    store
        .conn
        .execute_batch(
            "DROP TRIGGER fail_runtime;
        CREATE TRIGGER fail_heartbeat BEFORE UPDATE OF last_heartbeat ON sessions
        BEGIN SELECT RAISE(ABORT,'required resume heartbeat failed'); END;",
        )
        .await
        .unwrap();
    delegate.next_registration(Mode::Normal);
    let failure = identity.register(req).await.unwrap_err();
    assert!(failure.message.contains("required resume heartbeat failed"));
    let runtime = AgentRuntimes::new(&store)
        .find_by_runtime_id(&row.session_id.0)
        .await
        .unwrap()
        .unwrap();
    assert!(!runtime.active);
    assert_eq!(runtime.presence.as_deref(), Some("offline"));
    assert_eq!(
        *delegate.calls.lock().unwrap(),
        [true],
        "inactive resume create is not delegated as active; later activation was never reached"
    );
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
    assert_eq!(retained(&store).await.session_id, row.session_id);
}

#[tokio::test]
async fn managed_register_and_resume_telemetry_failures_remain_success() {
    let (identity, store, sink, delegate) = fixture().await;
    store.conn.execute_batch("CREATE TRIGGER fail_events BEFORE INSERT ON developer_events BEGIN SELECT RAISE(ABORT,'best effort telemetry failed'); END;").await.unwrap();
    let first = identity.register(request()).await.unwrap();
    delegate.next_registration(Mode::Normal);
    assert_eq!(
        identity.register(request()).await.unwrap().session_id,
        first.session_id
    );
    assert_eq!(count(&store, "developer_events").await, 0);
    assert_eq!(
        sink.0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, WsEvent::AgentSpawned { .. }))
            .count(),
        1
    );
    assert_eq!(delegate.cleanup_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn accumulator_proof_applied_then_not_committed_or_selection_changed_is_monotonic() {
    // Algebraic sequence, not a claim that one current register branch activates twice.
    let (_, store, _, _) = fixture().await;
    agent(&store, "a_caller", "caller").await;
    let runtime = NewAgentRuntime {
        runtime_id: "actual".into(),
        agent_id: "a_caller".into(),
        harness: "claude".into(),
        cwd: None,
        transport: None,
        presence: None,
        active: true,
    };
    let applied = AgentRuntimes::new(&store)
        .create_active_selected(runtime.clone(), &[])
        .await
        .unwrap();
    let mut disposition = service::RegistrationDisposition::default();
    disposition.record(&Ok(applied));
    assert!(disposition.preserve_required);
    let mut invalid = runtime;
    invalid.active = false;
    let failure = AgentRuntimes::new(&store)
        .create_active_selected(invalid, &[])
        .await
        .unwrap_err();
    assert_eq!(
        failure.commit_state(),
        RuntimeActivationCommitState::NotCommitted
    );
    disposition.record(&Err(RuntimeActivationError::Store {
        failure,
        settlement_error: None,
    }));
    assert!(disposition.preserve_required);
    let changed = AgentRuntimes::new(&store)
        .activate_selected("actual", "wrong", &[])
        .await
        .unwrap();
    disposition.record(&Ok(changed));
    assert!(disposition.preserve_required);
    assert!(disposition.cleanup_authority_lost);
    disposition.record(&Err(RuntimeActivationError::RejectedBeforeStore {
        cause: NexusError::Invalid("later rejection".into()),
    }));
    assert!(disposition.preserve_required && disposition.cleanup_authority_lost);
}
