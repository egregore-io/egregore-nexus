//! Inert registry entries only: no service credential preflight or runtime enablement.
#![allow(dead_code)]

#[path = "../src/binding.rs"]
mod binding;
mod presence {
    pub(crate) use nexus_common::presence::presence_from_token as presence_from_str;
}
#[path = "../src/registry.rs"]
mod registry;

use nexus_common::NexusError;
use nexus_contracts::{register::RegisterRequest, AgentId, HarnessId, Kind, SessionId, Tier};
use nexus_store::{
    repos::{
        sessions::{SelectedStagedSessionCleanup, SelectedStagedSessionStamp},
        AgentRuntimes, Agents, NativeThreadBindings, NewAgent, NewAgentRuntime,
        NewNativeThreadBinding, NewSession, Sessions,
    },
    types::SessionRow,
    Store,
};

use registry::resolve_register_captured;

async fn fixture() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn request(name: &str, key: &str) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(name.into()),
        harness: HarnessId::new("claude").unwrap(),
        harness_session_id: format!("native-{key}"),
        project: "project".into(),
        client_key: key.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: None,
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

fn new_session(id: &str, name: &str, key: &str) -> NewSession {
    NewSession {
        session_id: SessionId(id.into()),
        name: Some(name.into()),
        agent: Some("claude".into()),
        kind: "local.agent".into(),
        role: None,
        tier: "agent".into(),
        harness_session_id: Some(format!("native-{key}")),
        client_key: Some(key.into()),
        cwd: None,
        project: "project".into(),
        transport: None,
    }
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

async fn corrupt_after_insert(store: &Store) {
    store
        .conn
        .execute_batch(
            "CREATE TABLE insert_audit (session_id TEXT);
         CREATE TABLE delete_audit (session_id TEXT);
         CREATE TRIGGER corrupt_projection AFTER INSERT ON sessions BEGIN
           INSERT INTO insert_audit VALUES (NEW.session_id);
           UPDATE sessions SET kind='invalid-kind' WHERE session_id=NEW.session_id;
         END;
         CREATE TRIGGER audit_delete AFTER DELETE ON sessions BEGIN
           INSERT INTO delete_audit VALUES (OLD.session_id);
         END;",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn malformed_projection_control_insert_succeeds_then_read_fails() {
    let store = fixture().await;
    corrupt_after_insert(&store).await;
    let repo = Sessions::new(&store);
    let id = repo
        .create_staged_registration_with_metadata(new_session("control", "control", "key"), None)
        .await
        .expect("AFTER INSERT corruption must not reject the insert itself");
    assert_eq!(count(&store, "sessions").await, 1);
    assert_eq!(count(&store, "insert_audit").await, 1);
    assert!(
        repo.find_by_session_id(&id).await.is_err(),
        "only projection decoding fails"
    );
    assert_eq!(count(&store, "delete_audit").await, 0);
}

#[tokio::test]
async fn legacy_projection_failure_uses_id_only_delete_fallback() {
    let store = fixture().await;
    corrupt_after_insert(&store).await;
    assert!(
        registry::resolve_register(&store, &request("legacy", "key"))
            .await
            .is_err()
    );
    assert_eq!(
        count(&store, "insert_audit").await,
        1,
        "legacy insertion committed"
    );
    assert_eq!(
        count(&store, "delete_audit").await,
        1,
        "late read failure invoked DELETE"
    );
    assert_eq!(count(&store, "sessions").await, 0);
}

#[tokio::test]
async fn captured_projection_failure_rolls_back_without_delete_fallback() {
    let store = fixture().await;
    corrupt_after_insert(&store).await;
    assert!(
        resolve_register_captured(&store, &request("captured", "key"))
            .await
            .is_err()
    );
    assert_eq!(
        count(&store, "delete_audit").await,
        0,
        "capture failure must not invoke legacy DELETE fallback"
    );
    assert_eq!(
        count(&store, "insert_audit").await,
        0,
        "capture transaction rolled back the trigger write"
    );
    assert_eq!(count(&store, "sessions").await, 0);
}

#[derive(Clone, Copy, Debug)]
enum Entry {
    Legacy,
    Captured,
}
const ENTRIES: [Entry; 2] = [Entry::Legacy, Entry::Captured];

impl Entry {
    // Only compare policy/projections here; this adapter never constructs a receipt.
    async fn resolve(
        self,
        store: &Store,
        req: &RegisterRequest,
    ) -> Result<(bool, SessionRow), NexusError> {
        match self {
            Self::Legacy => match registry::resolve_register(store, req).await? {
                registry::RegisterOutcome::Resumed(row) => Ok((false, row)),
                registry::RegisterOutcome::Created(row) => Ok((true, row)),
            },
            Self::Captured => match resolve_register_captured(store, req).await? {
                registry::CapturedRegisterOutcome::Resumed(row) => Ok((false, row)),
                registry::CapturedRegisterOutcome::Created(receipt) => {
                    Ok((true, receipt.row().clone()))
                }
            },
        }
    }
}

async fn agent(store: &Store, id: &str, name: &str) {
    Agents::new(store)
        .create(NewAgent {
            agent_id: id.into(),
            name: Some(name.into()),
            project: "project".into(),
            default_harness: Some("claude".into()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
}

async fn seed(store: &Store, id: &str, name: &str, key: &str) -> SessionRow {
    let repo = Sessions::new(store);
    let id = repo
        .create_staged_registration(new_session(id, name, key))
        .await
        .unwrap();
    repo.find_by_session_id(&id).await.unwrap().unwrap()
}

async fn bind_native(store: &Store, runtime_id: Option<&str>) {
    NativeThreadBindings::new(store)
        .claim(NewNativeThreadBinding {
            provider: "claude".into(),
            kind: "harness".into(),
            native_thread_id: "bound-native".into(),
            agent_id: "a_owner".into(),
            project: "project".into(),
            runtime_id: runtime_id.map(str::to_string),
        })
        .await
        .unwrap();
}

async fn inventory(store: &Store) -> (i64, i64, i64, i64) {
    (
        count(store, "sessions").await,
        count(store, "agents").await,
        count(store, "agent_runtimes").await,
        count(store, "developer_events").await,
    )
}

async fn resume_unchanged(
    entry: Entry,
    store: &Store,
    req: &RegisterRequest,
    expected: &SessionRow,
) {
    let before = inventory(store).await;
    let epoch = store.events().session_lifecycle_changed().epoch();
    let (created, row) = entry.resolve(store, req).await.unwrap();
    assert!(
        !created,
        "{entry:?} must resume, not grant creation authority"
    );
    assert_eq!(&row, expected, "{entry:?}");
    assert_eq!(
        inventory(store).await,
        before,
        "{entry:?} inserted inventory"
    );
    assert_eq!(store.events().session_lifecycle_changed().epoch(), epoch);
    assert_eq!(
        Sessions::new(store)
            .find_by_session_id(&expected.session_id)
            .await
            .unwrap()
            .as_ref(),
        Some(expected)
    );
}

async fn reject_unchanged(entry: Entry, store: &Store, req: &RegisterRequest, expected: &str) {
    let before = inventory(store).await;
    let epoch = store.events().session_lifecycle_changed().epoch();
    let error = entry.resolve(store, req).await.unwrap_err();
    let actual = match error {
        NexusError::Invalid(_) => "invalid",
        NexusError::Unauthorized => "unauthorized",
        NexusError::DuplicateName(_) => "duplicate",
        NexusError::Ambiguous(_) => "ambiguous",
        NexusError::NotFound(_) => "not_found",
        other => panic!("unexpected {entry:?} rejection: {other}"),
    };
    assert_eq!(actual, expected, "{entry:?}");
    assert_eq!(
        inventory(store).await,
        before,
        "{entry:?} inserted inventory"
    );
    assert_eq!(store.events().session_lifecycle_changed().epoch(), epoch);
}

#[tokio::test]
async fn native_owner_and_key_checks_precede_global_key_resume() {
    for entry in ENTRIES {
        let store = fixture().await;
        agent(&store, "a_owner", "owner").await;
        seed(&store, "owner-runtime", "owner", "owner-key").await;
        seed(&store, "other-runtime", "other", "other-key").await;
        bind_native(&store, Some("owner-runtime")).await;
        let mut req = request("other", "other-key");
        req.harness_session_id = "bound-native".into();
        reject_unchanged(entry, &store, &req, "invalid").await;
        req.name = Some("owner".into());
        req.agent_id = Some(AgentId("a_wrong".into()));
        reject_unchanged(entry, &store, &req, "invalid").await;
        req.agent_id = None;
        reject_unchanged(entry, &store, &req, "unauthorized").await;
        req.client_key = "owner-key".into();
        let row = Sessions::new(&store)
            .find_by_session_id(&SessionId("owner-runtime".into()))
            .await
            .unwrap()
            .unwrap();
        resume_unchanged(entry, &store, &req, &row).await;
        req.agent_id = Some(AgentId("a_owner".into()));
        req.name = Some("stale-name".into());
        resume_unchanged(entry, &store, &req, &row).await;
    }
}

#[tokio::test]
async fn native_runtime_ownership_checks_precede_key_resume() {
    for entry in ENTRIES {
        for compatibility_mismatch in [true, false] {
            let store = fixture().await;
            agent(&store, "a_owner", "owner").await;
            agent(&store, "a_other", "other").await;
            seed(&store, "runtime", "owner", "key").await;
            bind_native(&store, Some("runtime")).await;
            if compatibility_mismatch {
                Sessions::new(&store)
                    .set_agent_id(&SessionId("runtime".into()), "a_other")
                    .await
                    .unwrap();
            } else {
                AgentRuntimes::new(&store)
                    .create(NewAgentRuntime {
                        runtime_id: "runtime".into(),
                        agent_id: "a_other".into(),
                        harness: "claude".into(),
                        cwd: None,
                        transport: None,
                        presence: None,
                        active: true,
                    })
                    .await
                    .unwrap();
            }
            let mut req = request("owner", "key");
            req.harness_session_id = "bound-native".into();
            reject_unchanged(entry, &store, &req, "invalid").await;
        }
    }
}

#[tokio::test]
async fn native_owner_fallback_requires_key_and_missing_runtime_does_not_fall_through() {
    for entry in ENTRIES {
        let store = fixture().await;
        agent(&store, "a_owner", "owner").await;
        seed(&store, "global", "global", "global-key").await;
        bind_native(&store, Some("missing-runtime")).await;
        let mut req = request("owner", "global-key");
        req.harness_session_id = "bound-native".into();
        reject_unchanged(entry, &store, &req, "not_found").await;
        seed(&store, "owner-runtime", "owner", "owner-key").await;
        Sessions::new(&store)
            .set_agent_id(&SessionId("owner-runtime".into()), "a_owner")
            .await
            .unwrap();
        reject_unchanged(entry, &store, &req, "unauthorized").await;
        req.client_key = "owner-key".into();
        let row = Sessions::new(&store)
            .find_by_session_id(&SessionId("owner-runtime".into()))
            .await
            .unwrap()
            .unwrap();
        resume_unchanged(entry, &store, &req, &row).await;
    }
}

#[tokio::test]
async fn legacy_harness_name_requires_unique_match_and_matching_key_before_global_resume() {
    for entry in ENTRIES {
        let store = fixture().await;
        let row = seed(&store, "legacy", "owner", "owner-key").await;
        seed(&store, "global", "global", "global-key").await;
        let mut req = request("owner", "owner-key");
        req.project = "different-project".into();
        resume_unchanged(entry, &store, &req, &row).await;
        req.client_key = "global-key".into();
        reject_unchanged(entry, &store, &req, "unauthorized").await;
        store
            .conn
            .execute_batch("DROP INDEX idx_sessions_name;")
            .await
            .unwrap();
        let mut duplicate = new_session("duplicate", "owner", "duplicate-key");
        duplicate.harness_session_id = row.harness_session_id.clone();
        duplicate.project = "elsewhere".into();
        Sessions::new(&store)
            .create_staged_registration(duplicate)
            .await
            .unwrap();
        req.client_key = "owner-key".into();
        reject_unchanged(entry, &store, &req, "ambiguous").await;
    }
}

#[tokio::test]
async fn harness_name_mismatch_creates_separate_session_but_global_key_precedes_alias_guards() {
    for entry in ENTRIES {
        let store = fixture().await;
        let old = seed(&store, "old", "old", "old-key").await;
        let mut req = request("new", "new-key");
        req.harness_session_id = old.harness_session_id.clone().unwrap();
        let (created, new) = entry.resolve(&store, &req).await.unwrap();
        assert!(created);
        assert_ne!(new.session_id, old.session_id);
        assert_eq!(count(&store, "sessions").await, 2);
        assert_eq!(count(&store, "agents").await, 0);
        agent(&store, "a_durable", "durable").await;
        req.name = Some("durable".into());
        req.client_key = "old-key".into();
        req.harness_session_id = "unmatched".into();
        req.project = "different-project".into();
        resume_unchanged(entry, &store, &req, &old).await;
        req.name = new.name.clone();
        resume_unchanged(entry, &store, &req, &old).await;
    }
}

#[tokio::test]
async fn compatibility_name_live_offline_and_ambiguity_policy_is_shared() {
    for entry in ENTRIES {
        let store = fixture().await;
        seed(&store, "owner", "owner", "old-key").await;
        let mut req = request("owner", "new-key");
        reject_unchanged(entry, &store, &req, "duplicate").await;
        store
            .conn
            .execute(
                "UPDATE sessions SET presence='offline' WHERE session_id='owner'",
                (),
            )
            .await
            .unwrap();
        reject_unchanged(entry, &store, &req, "duplicate").await;
        // This is selector policy, NOT authentication: production service preflight verifies it.
        req.agent_id = Some(AgentId("a_preflight_required".into()));
        agent(&store, "a_different_alias_owner", "owner").await;
        let row = Sessions::new(&store)
            .find_by_session_id(&SessionId("owner".into()))
            .await
            .unwrap()
            .unwrap();
        resume_unchanged(entry, &store, &req, &row).await;
        store
            .conn
            .execute_batch("DROP INDEX idx_sessions_name;")
            .await
            .unwrap();
        let mut duplicate = new_session("duplicate", "owner", "duplicate-key");
        duplicate.project = "elsewhere".into();
        Sessions::new(&store)
            .create_staged_registration(duplicate)
            .await
            .unwrap();
        reject_unchanged(entry, &store, &req, "ambiguous").await;
    }
}

#[tokio::test]
async fn durable_agent_alias_guard_preserves_human_and_app_namespaces() {
    for entry in ENTRIES {
        for kind in [None, Some(Kind::Agent), Some(Kind::Human), Some(Kind::App)] {
            let store = fixture().await;
            agent(&store, "a_owner", "alias").await;
            let mut req = request("alias", "key");
            req.kind = kind;
            if kind.is_none() || kind == Some(Kind::Agent) {
                reject_unchanged(entry, &store, &req, "duplicate").await;
                req.agent_id = Some(AgentId("a_other".into()));
                reject_unchanged(entry, &store, &req, "duplicate").await;
                req.agent_id = Some(AgentId("a_owner".into()));
            }
            let (created, row) = entry.resolve(&store, &req).await.unwrap();
            assert!(created);
            assert_eq!(
                row.agent_id, None,
                "registry must not stamp or manufacture identities"
            );
            assert_eq!(
                row.kind,
                format!(
                    "local.{}",
                    match kind {
                        Some(Kind::Human) => "human",
                        Some(Kind::App) => "app",
                        _ => "agent",
                    }
                )
            );
            assert_eq!(count(&store, "agents").await, 1);
            assert_eq!(count(&store, "sessions").await, 1);
            assert_eq!(count(&store, "developer_events").await, 0);
        }
    }
}

#[tokio::test]
async fn durable_alias_ambiguity_rejects_agent_but_not_human_or_app() {
    for entry in ENTRIES {
        for kind in [Kind::Agent, Kind::Human, Kind::App] {
            let store = fixture().await;
            store
                .conn
                .execute_batch("DROP INDEX idx_agents_name_unique;")
                .await
                .unwrap();
            agent(&store, "a_one", "alias").await;
            agent(&store, "a_two", "alias").await;
            let mut req = request("alias", "key");
            req.kind = Some(kind);
            req.agent_id = Some(AgentId("a_one".into()));
            if kind == Kind::Agent {
                reject_unchanged(entry, &store, &req, "ambiguous").await;
            } else {
                assert!(entry.resolve(&store, &req).await.unwrap().0);
                assert_eq!(count(&store, "agents").await, 2);
                assert_eq!(count(&store, "sessions").await, 1);
            }
        }
    }
}

#[tokio::test]
async fn fresh_projection_preserves_request_metadata_without_started_fact() {
    for entry in ENTRIES {
        for access in [None, Some("provider policy: \"parallel\"".to_string())] {
            let store = fixture().await;
            let mut req = request("fresh", "key");
            req.access = access.clone();
            req.role = Some("reviewer".into());
            req.cwd = Some("/disposable/project".into());
            req.tier = Tier::Admin;
            let epoch = store.events().session_lifecycle_changed().epoch();
            let (created, row) = entry.resolve(&store, &req).await.unwrap();
            assert!(created);
            assert_eq!(row.name, req.name);
            assert_eq!(row.agent.as_deref(), Some("claude"));
            assert_eq!(row.client_key.as_deref(), Some(req.client_key.as_str()));
            assert_eq!(
                row.harness_session_id.as_deref(),
                Some(req.harness_session_id.as_str())
            );
            assert_eq!(row.project, req.project);
            assert_eq!(row.role, req.role);
            assert_eq!(row.cwd, req.cwd);
            assert_eq!(row.tier, "admin");
            assert_eq!(row.transport, None);
            assert_eq!(row.access().unwrap(), access);
            assert_eq!(
                row.metadata_json,
                req.access
                    .map(|access| serde_json::json!({"access": access}).to_string())
            );
            assert_eq!(inventory(&store).await, (1, 0, 0, 0));
            assert_eq!(store.events().session_lifecycle_changed().epoch(), epoch);
        }
    }
}

#[tokio::test]
async fn captured_created_carries_actual_insert_image_and_usable_selected_authority() {
    let store = fixture().await;
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER persisted_image AFTER INSERT ON sessions BEGIN
        UPDATE sessions SET current_work='insert-trigger', paused=7 WHERE session_id=NEW.session_id;
        END;",
        )
        .await
        .unwrap();
    let req = request("captured", "key");
    let receipt = match resolve_register_captured(&store, &req).await.unwrap() {
        registry::CapturedRegisterOutcome::Created(receipt) => receipt,
        registry::CapturedRegisterOutcome::Resumed(_) => panic!("fresh insertion required"),
    };
    assert_eq!(
        receipt.row().current_work.as_deref(),
        Some("insert-trigger")
    );
    assert!(receipt.row().paused);
    assert_eq!(receipt.row().agent_id, None);
    match resolve_register_captured(&store, &req).await.unwrap() {
        registry::CapturedRegisterOutcome::Resumed(row) => assert_eq!(&row, receipt.row()),
        registry::CapturedRegisterOutcome::Created(_) => panic!("resume must not carry a receipt"),
    }
    let repo = Sessions::new(&store);
    let advanced = match repo
        .set_agent_id_selected(&receipt, "a_selected")
        .await
        .unwrap()
    {
        SelectedStagedSessionStamp::Updated(receipt) => receipt,
        SelectedStagedSessionStamp::SelectionChanged => {
            panic!("actual raw insert image must match")
        }
    };
    assert_eq!(advanced.row().agent_id.as_deref(), Some("a_selected"));
    assert!(matches!(
        repo.remove_staged_registration_selected(&receipt)
            .await
            .unwrap(),
        SelectedStagedSessionCleanup::SelectionChanged
    ));
    assert!(matches!(
        repo.remove_staged_registration_selected(&advanced)
            .await
            .unwrap(),
        SelectedStagedSessionCleanup::Removed
    ));
    assert_eq!(inventory(&store).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn captured_insert_ignore_returns_error_without_cleanup_fallback() {
    let store = fixture().await;
    store.conn.execute_batch("CREATE TRIGGER ignore_insert BEFORE INSERT ON sessions BEGIN SELECT RAISE(IGNORE); END;").await.unwrap();
    let epoch = store.events().session_lifecycle_changed().epoch();
    let error = resolve_register_captured(&store, &request("ignored", "key"))
        .await
        .err()
        .expect("ignored insert cannot grant receipt");
    assert!(error.to_string().contains("exactly one row"));
    assert_eq!(
        store.events().session_lifecycle_changed().epoch(),
        epoch,
        "even a no-op ID cleanup would signal"
    );
    assert_eq!(inventory(&store).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn captured_commit_failure_returns_no_authority_or_cleanup_fallback() {
    let store = fixture().await;
    store.conn.execute_batch("PRAGMA foreign_keys=ON;
        CREATE TABLE receipt_parent (id INTEGER PRIMARY KEY);
        CREATE TABLE receipt_child (id INTEGER REFERENCES receipt_parent(id) DEFERRABLE INITIALLY DEFERRED);").await.unwrap();
    let probe = store
        .begin_write_txn("registry_deferred_probe")
        .await
        .unwrap();
    assert_eq!(
        probe
            .execute("INSERT INTO receipt_child VALUES (1)", ())
            .await
            .unwrap(),
        1
    );
    let error = probe
        .commit()
        .await
        .expect_err("probe must fail at COMMIT, not INSERT");
    assert!(error.to_string().to_lowercase().contains("foreign key"));
    // Drop schedules rollback while retaining the writer guard. Wait for that rollback before
    // creating a fixture trigger, or the trigger itself would be rolled back with the probe.
    store
        .begin_write_txn("after_deferred_probe")
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    store
        .conn
        .execute_batch(
            "CREATE TRIGGER deferred_failure AFTER INSERT ON sessions BEGIN
        INSERT INTO receipt_child VALUES (1); END;",
        )
        .await
        .unwrap();
    // Also demonstrate that the actual session INSERT and its projection succeed before COMMIT.
    let probe = store
        .begin_write_txn("registry_insert_projection_probe")
        .await
        .unwrap();
    assert_eq!(
        probe
            .execute(
                "INSERT INTO sessions (session_id, kind) VALUES ('probe', 'local.agent')",
                ()
            )
            .await
            .unwrap(),
        1
    );
    assert!(Sessions::new(&store)
        .find_by_session_id(&SessionId("probe".into()))
        .await
        .unwrap()
        .is_some());
    assert!(probe
        .commit()
        .await
        .expect_err("session trigger must defer failure until COMMIT")
        .to_string()
        .to_lowercase()
        .contains("foreign key"));
    store
        .begin_write_txn("after_projection_probe")
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let epoch = store.events().session_lifecycle_changed().epoch();
    let error = resolve_register_captured(&store, &request("uncommitted", "key"))
        .await
        .err()
        .expect("commit failure cannot grant receipt");
    assert!(error.to_string().to_lowercase().contains("foreign key"));
    assert_eq!(
        store.events().session_lifecycle_changed().epoch(),
        epoch,
        "ID-only cleanup signals even when no row remains"
    );
    // This deterministic fixture rolls back on commit-error drop; not a general commit guarantee.
    store
        .begin_write_txn("after_registry_commit_failure")
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    assert_eq!(inventory(&store).await, (0, 0, 0, 0));
}
