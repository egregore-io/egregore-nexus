use nexus_contracts::ids::SessionId;
use nexus_contracts::model_report::*;
use nexus_store::repos::agent_runtimes::{
    ExactRuntimeStop, ExpectedRuntimeBinding, RuntimeActivationCommitState,
    RuntimeActivationFailure, SelectedRuntimeActivation, SelectedRuntimeResidueCleanup,
};
use nexus_store::repos::{AgentRuntimes, NewAgentRuntime, NewSession, Sessions};
use nexus_store::{migrate_identity_with_fault, DaemonStore, MigrationFault, Store};
use std::path::PathBuf;

fn report() -> RuntimeModelReport {
    let unknown = ModelEvidenceSlot::Unknown {
        capability: ModelEvidenceCapability::Unverified,
        reason: None,
    };
    RuntimeModelReport {
        backend: ModelReportBackend::new("unfamiliar.backend/β").unwrap(),
        observer_active: true,
        report_revision: 900,
        configured: ModelEvidenceSlot::Observed {
            capability: ModelEvidenceCapability::Supported,
            observation: ModelObservation {
                model_id: " opaque:vendor/model[mode] ".into(),
                provider_id: None,
                source: ModelObservationSource::new("unfamiliar.source/β").unwrap(),
                observed_at: 42,
                native_session_id: None,
                native_turn_id: None,
                native_message_id: None,
                native_reported_at: None,
            },
        },
        turn_selected: unknown.clone(),
        response_reported: unknown,
        telemetry: None,
    }
}

#[tokio::test]
async fn selected_retention_preserves_predicate_and_never_expands_capture() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    store.identity_conn().execute_batch("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at,model_report_revision,model_report_json) VALUES
        ('a','agent-a','opaque',0,1,5,0,NULL),('b','agent-b','opaque',0,1,10,0,NULL),
        ('recent','agent-c','opaque',0,1,11,0,NULL),('unstopped','agent-d','opaque',0,1,NULL,0,NULL),
        ('active','agent-e','opaque',1,1,5,0,NULL),('versioned','agent-f','opaque',0,1,5,1,'{broken'),
        ('raw-zero','agent-g','opaque',0,1,5,0,'{broken');").await.unwrap();
    let repo = AgentRuntimes::new(&store);
    assert_eq!(
        repo.retention_candidate_pairs(10).await.unwrap(),
        vec![
            ("a".into(), "agent-a".into()),
            ("b".into(), "agent-b".into()),
            ("raw-zero".into(), "agent-g".into())
        ]
    );
    store.identity_conn().execute_batch("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES('new','agent-new','opaque',0,1,2);").await.unwrap();
    let epochs = selected_epochs(&store);
    let changed = repo
        .reap_retention_selected(
            10,
            &[
                ("b".into(), "agent-b".into()),
                ("b".into(), "agent-b".into()),
                ("a".into(), "foreign".into()),
                ("missing".into(), "agent".into()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(changed, vec![("b".into(), "agent-b".into())]);
    assert_eq!(
        identity_values(
            &store,
            "SELECT runtime_id FROM agent_runtimes ORDER BY runtime_id"
        )
        .await,
        [
            "a",
            "active",
            "new",
            "raw-zero",
            "recent",
            "unstopped",
            "versioned"
        ]
        .map(|s| vec![libsql::Value::Text(s.into())])
        .to_vec()
    );
    assert_eq!(
        selected_epochs(&store),
        epochs,
        "legacy runtime retention emits no lifecycle/signal"
    );
    assert_eq!(
        repo.reap_retention_selected(
            10,
            &[
                ("a".into(), "agent-a".into()),
                ("raw-zero".into(), "agent-g".into())
            ]
        )
        .await
        .unwrap()
        .len(),
        2
    );
    assert_eq!(authority(&store, "versioned").await.2, 1);
    assert_eq!(selected_epochs(&store), epochs);
}

#[tokio::test]
async fn selected_retention_revalidates_at_identity_gate() {
    use std::{future::Future, task::Poll};
    for update in [
        "agent_id='new-owner'",
        "active=1",
        "stopped_at=NULL",
        "stopped_at=11",
        "model_report_revision=1,model_report_json='{broken'",
    ] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        store.identity_conn().execute_batch("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES('runtime','agent','opaque',0,1,5);").await.unwrap();
        let repo = AgentRuntimes::new(&store);
        let captured = vec![("runtime".into(), "agent".into())];
        let gate = store
            .begin_identity_write_txn("selected_retention_gate")
            .await
            .unwrap();
        let mut reap = Box::pin(repo.reap_retention_selected(10, &captured));
        let mut follower = Box::pin(store.begin_identity_write_txn("selected_retention_follower"));
        std::future::poll_fn(|cx| {
            assert!(
                reap.as_mut().poll(cx).is_pending(),
                "selected deletion must wait for identity transaction"
            );
            assert!(follower.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        gate.execute(
            &format!("UPDATE agent_runtimes SET {update} WHERE runtime_id='runtime'"),
            (),
        )
        .await
        .unwrap();
        gate.commit().await.unwrap();
        std::future::poll_fn(|cx| {
            assert!(
                follower.as_mut().poll(cx).is_pending(),
                "selected operation must precede follower"
            );
            Poll::Ready(())
        })
        .await;
        let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
        let epochs = selected_epochs(&store);
        assert!(reap.await.unwrap().is_empty(), "{update}");
        follower.await.unwrap().commit().await.unwrap();
        assert_eq!(
            identity_values(&store, "SELECT * FROM agent_runtimes").await,
            before,
            "{update}"
        );
        assert_eq!(selected_epochs(&store), epochs);
    }
}

#[tokio::test]
async fn selected_retention_checks_delete_postabsence_and_commit() {
    for mode in ["ignore", "reinsert-earlier", "abort", "commit"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        store.identity_conn().execute_batch("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES('a','agent-a','opaque',0,1,5),('b','agent-b','opaque',0,1,5);").await.unwrap();
        store.identity_conn().execute_batch(match mode {
            "ignore" => "CREATE TRIGGER retention_fail BEFORE DELETE ON agent_runtimes WHEN OLD.runtime_id='b' BEGIN SELECT RAISE(IGNORE); END;",
            "reinsert-earlier" => "CREATE TRIGGER retention_fail AFTER DELETE ON agent_runtimes WHEN OLD.runtime_id='b' BEGIN INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES('a','replacement','opaque',0,1,5); END;",
            "abort" => "CREATE TRIGGER retention_fail BEFORE DELETE ON agent_runtimes WHEN OLD.runtime_id='b' BEGIN SELECT RAISE(ABORT,'retention original abort'); END;",
            _ => "PRAGMA foreign_keys=ON; CREATE TABLE retention_parent(id INTEGER PRIMARY KEY); CREATE TABLE retention_child(id INTEGER REFERENCES retention_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER retention_fail AFTER DELETE ON agent_runtimes WHEN OLD.runtime_id='b' BEGIN INSERT INTO retention_child VALUES(1); END;",
        }).await.unwrap();
        let before =
            identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
        let epochs = selected_epochs(&store);
        let error = AgentRuntimes::new(&store)
            .reap_retention_selected(
                10,
                &[
                    ("b".into(), "agent-b".into()),
                    ("a".into(), "agent-a".into()),
                ],
            )
            .await
            .expect_err("failed retention cannot return changed receipt");
        assert!(
            error.to_string().contains(match mode {
                "ignore" => "exactly one",
                "reinsert-earlier" => "remains",
                "abort" => "retention original abort",
                _ => "FOREIGN KEY",
            }),
            "{mode}: {error}"
        );
        assert_selected_writer_admitted(&store).await;
        assert_eq!(
            identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
            before,
            "{mode}"
        );
        assert_eq!(selected_epochs(&store), epochs);
    }
}
async fn create(store: &Store, id: &str, active: bool) {
    AgentRuntimes::new(store)
        .create(NewAgentRuntime {
            runtime_id: id.into(),
            agent_id: "agent".into(),
            harness: "opaque".into(),
            cwd: None,
            transport: None,
            presence: Some("online".into()),
            active,
        })
        .await
        .unwrap();
}
async fn authority(
    store: &Store,
    id: &str,
) -> (Option<String>, i64, i64, Option<RuntimeModelReport>) {
    let row = store.identity_conn().query("SELECT model_observer_token,model_observer_sequence,model_report_revision,model_report_json FROM agent_runtimes WHERE runtime_id=?1", libsql::params![id]).await.unwrap().next().await.unwrap().unwrap();
    let token = match row.get_value(0).unwrap() {
        libsql::Value::Null => None,
        libsql::Value::Text(s) => Some(s),
        _ => panic!("token"),
    };
    let report = match row.get_value(3).unwrap() {
        libsql::Value::Null => None,
        libsql::Value::Text(s) => serde_json::from_str(&s).ok(),
        _ => panic!("json"),
    };
    (token, row.get(1).unwrap(), row.get(2).unwrap(), report)
}

fn staged_report() -> RuntimeModelReport {
    let mut initial = report();
    initial.observer_active = false;
    initial.configured = initial.turn_selected.clone();
    initial
}

async fn create_unavailable_runtime(store: &Store, stopped: bool) {
    create(store, "runtime", stopped).await;
    let repo = AgentRuntimes::new(store);
    if stopped {
        repo.stop("runtime").await.unwrap();
    }
    // Non-default persisted sentinels make accidental liveness/process writes visible.
    repo.set_presence("runtime", nexus_contracts::enums::Presence::Offline)
        .await
        .unwrap();
    repo.set_process_ids(
        "runtime",
        nexus_common::RuntimeProcessIds {
            os_pid: 12345,
            os_pgid: 12346,
        },
    )
    .await
    .unwrap();
}

async fn runtime_lifecycle(store: &Store) -> Vec<Vec<libsql::Value>> {
    identity_values(
        store,
        "SELECT runtime_id,agent_id,harness,cwd,transport,presence,active,started_at,\
         stopped_at,last_heartbeat,os_pid,os_pgid FROM agent_runtimes WHERE runtime_id='runtime'",
    )
    .await
}

async fn assert_staged_claim_preserves_runtime(stopped: bool) {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    create_unavailable_runtime(&store, stopped).await;
    let repo = AgentRuntimes::new(&store);
    let before = runtime_lifecycle(&store).await;
    let mut initial = staged_report();
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "staged", &initial)
        .await
        .unwrap());
    initial.report_revision = 1;
    let claimed = (Some("staged".into()), 0, 1, Some(initial.clone()));
    assert_eq!(authority(&store, "runtime").await, claimed);
    assert_eq!(runtime_lifecycle(&store).await, before);
    assert!(!repo
        .apply_model_report("runtime", "agent", "staged", 1, &report())
        .await
        .unwrap());
    assert_eq!(authority(&store, "runtime").await, claimed);
    assert_eq!(runtime_lifecycle(&store).await, before);

    // Non-Observed includes Invalid, not just Unknown; neither makes the observer current.
    initial.turn_selected = ModelEvidenceSlot::Invalid {
        capability: ModelEvidenceCapability::Supported,
        reason: ModelInvalidReason::MalformedNativeMetadata,
    };
    assert!(repo
        .claim_model_observer("runtime", "agent", Some("staged"), "replacement", &initial)
        .await
        .unwrap());
    initial.report_revision = 2;
    assert_eq!(
        authority(&store, "runtime").await,
        (Some("replacement".into()), 0, 2, Some(initial))
    );
    assert_eq!(runtime_lifecycle(&store).await, before);
}

#[tokio::test]
async fn staged_claim_on_inactive_runtime_preserves_liveness_and_rejects_early_apply() {
    assert_staged_claim_preserves_runtime(false).await;
}

fn telemetry_report() -> RuntimeModelReport {
    let runtime: nexus_contracts::AgentRuntimeSummary = serde_json::from_str(include_str!(
        "../../nexus-contracts/fixtures/runtime.telemetry.json"
    ))
    .unwrap();
    runtime.model_report.unwrap()
}

#[tokio::test]
async fn telemetry_staged_claim_rejects_each_observed_category_without_lifecycle_or_authority_writes(
) {
    for stopped in [false, true] {
        for category in ["usage", "context", "quota"] {
            let path = TempStore::new();
            let daemon = path.open().await;
            let store = daemon.compatibility_store();
            create_unavailable_runtime(&store, stopped).await;
            let before = authority(&store, "runtime").await;
            let lifecycle = runtime_lifecycle(&store).await;
            let epochs = selected_epochs(&store);
            let mut value = serde_json::to_value(telemetry_report()).unwrap();
            value["observerActive"] = serde_json::json!(false);
            for other in ["usage", "context", "quota"] {
                if other != category {
                    value["telemetry"][other] =
                        serde_json::json!({"status":"unknown","capability":"supported"});
                }
            }
            let initial: RuntimeModelReport = serde_json::from_value(value).unwrap();
            assert!(
                !AgentRuntimes::new(&store)
                    .claim_model_observer("runtime", "agent", None, "staged-telemetry", &initial)
                    .await
                    .unwrap(),
                "{stopped}/{category}"
            );
            assert_eq!(authority(&store, "runtime").await, before);
            assert_eq!(runtime_lifecycle(&store).await, lifecycle);
            assert_eq!(selected_epochs(&store), epochs);
        }
    }
}

#[tokio::test]
async fn telemetry_unobserved_staging_preserves_liveness_and_rejects_early_apply() {
    for stopped in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create_unavailable_runtime(&store, stopped).await;
        let lifecycle = runtime_lifecycle(&store).await;
        let repo = AgentRuntimes::new(&store);
        let mut initial = staged_report();
        initial.telemetry = Some(
            serde_json::from_value(serde_json::json!({
                "usage": {"status":"unknown","capability":"supported"},
                "context": {"status":"invalid","capability":"supported"},
                "quota": {"status":"unknown","capability":"unsupported"}
            }))
            .unwrap(),
        );
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "staged-telemetry", &initial)
            .await
            .unwrap());
        let claimed = authority(&store, "runtime").await;
        assert_eq!(claimed.3.as_ref().unwrap().telemetry, initial.telemetry);
        assert!(!claimed.3.as_ref().unwrap().observer_active);
        assert_eq!(runtime_lifecycle(&store).await, lifecycle);
        let mut observed = telemetry_report();
        observed.backend = initial.backend;
        assert!(!repo
            .apply_model_report("runtime", "agent", "staged-telemetry", 1, &observed)
            .await
            .unwrap());
        assert_eq!(authority(&store, "runtime").await, claimed);
        assert_eq!(runtime_lifecycle(&store).await, lifecycle);
    }
}

#[tokio::test]
async fn telemetry_apply_and_revoke_preserve_snapshot_under_existing_revision_authority() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    create(&store, "runtime", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer(
            "runtime",
            "agent",
            None,
            "telemetry-owner",
            &staged_report()
        )
        .await
        .unwrap());
    let mut initial = telemetry_report();
    initial.backend = staged_report().backend;
    assert!(repo
        .apply_model_report("runtime", "agent", "telemetry-owner", 1, &initial)
        .await
        .unwrap());
    let persisted = authority(&store, "runtime").await;
    assert_eq!(persisted.3.as_ref().unwrap().telemetry, initial.telemetry);
    assert!(persisted.3.as_ref().unwrap().observer_active);
    assert!(!repo
        .apply_model_report("runtime", "agent", "foreign", 2, &initial)
        .await
        .unwrap());
    assert_eq!(authority(&store, "runtime").await, persisted);
    assert!(repo
        .revoke_model_observer("runtime", "agent", "telemetry-owner")
        .await
        .unwrap());
    let revoked = authority(&store, "runtime").await;
    assert!(revoked.2 > persisted.2);
    assert!(!revoked.3.as_ref().unwrap().observer_active);
    assert_eq!(
        revoked.3.as_ref().unwrap().telemetry,
        initial.telemetry,
        "inactive evidence is historical, not zero"
    );
}

#[tokio::test]
async fn staged_claim_on_stopped_runtime_preserves_liveness_and_rejects_early_apply() {
    assert_staged_claim_preserves_runtime(true).await;
}

#[tokio::test]
async fn staged_claim_rejects_active_flag_or_any_observed_slot_without_writes() {
    for stopped in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create_unavailable_runtime(&store, stopped).await;
        let repo = AgentRuntimes::new(&store);
        let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
        for field in [
            "observer_active",
            "configured",
            "turn_selected",
            "response_reported",
        ] {
            let mut initial = staged_report();
            match field {
                "observer_active" => initial.observer_active = true,
                "configured" => initial.configured = report().configured,
                "turn_selected" => initial.turn_selected = report().configured,
                _ => initial.response_reported = report().configured,
            }
            assert!(
                !repo
                    .claim_model_observer("runtime", "agent", None, "rejected", &initial)
                    .await
                    .unwrap(),
                "stopped={stopped}, field={field}"
            );
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_runtimes").await,
                before
            );
        }
    }
}

#[tokio::test]
async fn staged_claim_retains_exact_identity_and_predecessor_checks() {
    for stopped in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create_unavailable_runtime(&store, stopped).await;
        let repo = AgentRuntimes::new(&store);
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "staged", &staged_report())
            .await
            .unwrap());
        let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
        for (runtime, agent, predecessor, token) in [
            ("missing", "agent", Some("staged"), "new"),
            ("runtime", "wrong", Some("staged"), "new"),
            ("runtime", "agent", None, "new"),
            ("runtime", "agent", Some("wrong"), "new"),
            ("runtime", "agent", Some("staged"), "staged"),
        ] {
            assert!(!repo
                .claim_model_observer(runtime, agent, predecessor, token, &staged_report())
                .await
                .unwrap());
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_runtimes").await,
                before
            );
        }
    }
}

#[tokio::test]
async fn staged_claim_rejects_untrusted_state_exhaustion_and_reserved_backend() {
    for stopped in [false, true] {
        for corruption in ["json", "authority", "exhausted", "unknown"] {
            let path = TempStore::new();
            let daemon = path.open().await;
            let store = daemon.compatibility_store();
            create_unavailable_runtime(&store, stopped).await;
            let repo = AgentRuntimes::new(&store);
            let mut initial = staged_report();
            match corruption {
                "json" => {
                    store.identity_conn().execute(
                        "UPDATE agent_runtimes SET model_report_revision=1,model_report_json='{bad'",
                        (),
                    ).await.unwrap();
                }
                "authority" => {
                    store
                        .identity_conn()
                        .execute("UPDATE agent_runtimes SET model_observer_sequence=-1", ())
                        .await
                        .unwrap();
                }
                "exhausted" => {
                    let mut previous = staged_report();
                    previous.report_revision = MAX_MODEL_REPORT_REVISION;
                    store.identity_conn().execute(
                        "UPDATE agent_runtimes SET model_report_revision=?1,model_report_json=?2",
                        libsql::params![MAX_MODEL_REPORT_REVISION as i64, serde_json::to_string(&previous).unwrap()],
                    ).await.unwrap();
                }
                _ => {
                    initial.backend = ModelReportBackend::new("unknown").unwrap();
                    let invalid = ModelEvidenceSlot::Invalid {
                        capability: ModelEvidenceCapability::Unverified,
                        reason: ModelInvalidReason::CorruptStoredMetadata,
                    };
                    initial.configured = invalid.clone();
                    initial.turn_selected = invalid.clone();
                    initial.response_reported = invalid;
                    initial.validate().unwrap();
                }
            }
            let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
            assert!(
                repo.claim_model_observer("runtime", "agent", None, "rejected", &initial)
                    .await
                    .is_err(),
                "stopped={stopped}, corruption={corruption}"
            );
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_runtimes").await,
                before
            );
        }
    }
}

#[tokio::test]
async fn staged_cleanup_and_store_restore_preserve_exact_owner_fencing() {
    // Store lifecycle simulation only: no native open, AppState activation, or restore-failure gate.
    for intervening in ["stop", "replacement"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create_unavailable_runtime(&store, true).await;
        let repo = AgentRuntimes::new(&store);
        let stopped = runtime_lifecycle(&store).await;
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "abandoned", &staged_report())
            .await
            .unwrap());
        assert!(repo
            .revoke_model_observer("runtime", "agent", "abandoned")
            .await
            .unwrap());
        assert_eq!(runtime_lifecycle(&store).await, stopped);
        let mut cleared = staged_report();
        cleared.report_revision = 2;
        assert_eq!(
            authority(&store, "runtime").await,
            (None, 0, 2, Some(cleared))
        );

        assert!(repo
            .claim_model_observer("runtime", "agent", None, "old", &staged_report())
            .await
            .unwrap());
        let staged = authority(&store, "runtime").await;
        repo.mark_live("runtime").await.unwrap();
        let live = repo.find_by_runtime_id("runtime").await.unwrap().unwrap();
        assert!(live.active);
        assert!(live.stopped_at.is_none());
        assert_eq!(live.presence.as_deref(), Some("online"));
        assert_eq!(authority(&store, "runtime").await, staged);
        assert!(repo
            .apply_model_report("runtime", "agent", "old", 1, &report())
            .await
            .unwrap());
        assert_eq!(authority(&store, "runtime").await.2, 4);

        if intervening == "stop" {
            repo.stop("runtime").await.unwrap();
            repo.mark_live("runtime").await.unwrap();
        } else {
            assert!(repo
                .claim_model_observer("runtime", "agent", Some("old"), "new", &staged_report())
                .await
                .unwrap());
        }
        let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
        assert_eq!(authority(&store, "runtime").await.2, 5);
        assert!(!repo
            .apply_model_report("runtime", "agent", "old", 2, &report())
            .await
            .unwrap());
        assert!(!repo
            .revoke_model_observer("runtime", "agent", "old")
            .await
            .unwrap());
        assert_eq!(
            identity_values(&store, "SELECT * FROM agent_runtimes").await,
            before
        );
    }
}

#[tokio::test]
async fn captured_owner_cas_sequence_and_reopen_preserve_public_revision() {
    let path = TempStore::new();
    {
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create(&store, "runtime", true).await;
        let repo = AgentRuntimes::new(&store);
        assert!(!repo
            .claim_model_observer("missing", "agent", None, "old", &report())
            .await
            .unwrap());
        assert!(!repo
            .claim_model_observer("runtime", "wrong", None, "old", &report())
            .await
            .unwrap());
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "old", &report())
            .await
            .unwrap());
        assert_eq!(authority(&store, "runtime").await.2, 1);
        assert!(repo
            .apply_model_report("runtime", "agent", "old", 2, &report())
            .await
            .unwrap());
        assert!(!repo
            .apply_model_report("runtime", "agent", "old", 1, &report())
            .await
            .unwrap());
        assert!(!repo
            .apply_model_report("runtime", "agent", "old", 2, &report())
            .await
            .unwrap());
        assert!(repo
            .claim_model_observer("runtime", "agent", Some("old"), "new", &report())
            .await
            .unwrap());
        let before = authority(&store, "runtime").await;
        assert_eq!((before.1, before.2), (0, 3));
        assert!(!repo
            .claim_model_observer("runtime", "agent", None, "late", &report())
            .await
            .unwrap());
        assert!(!repo
            .claim_model_observer("runtime", "agent", Some("old"), "late", &report())
            .await
            .unwrap());
        assert!(!repo
            .apply_model_report("runtime", "agent", "old", 3, &report())
            .await
            .unwrap());
        assert!(!repo
            .revoke_model_observer("runtime", "agent", "old")
            .await
            .unwrap());
        assert_eq!(authority(&store, "runtime").await, before);
    }
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    let repo = AgentRuntimes::new(&store);
    assert!(!repo
        .apply_model_report("runtime", "agent", "old", 100, &report())
        .await
        .unwrap());
    assert!(repo
        .apply_model_report("runtime", "agent", "new", 1, &report())
        .await
        .unwrap());
    let state = authority(&store, "runtime").await;
    assert_eq!(state.2, 4);
    assert_eq!(state.3.unwrap().report_revision, 4);
}

#[tokio::test]
async fn direct_invalid_snapshots_and_reserved_unknown_are_rejected_without_change() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    create(&store, "runtime", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    let before = authority(&store, "runtime").await;
    let invalid = ModelEvidenceSlot::Invalid {
        capability: ModelEvidenceCapability::Unverified,
        reason: ModelInvalidReason::CorruptStoredMetadata,
    };
    let mut tombstone = report();
    tombstone.backend = ModelReportBackend::new("unknown").unwrap();
    tombstone.observer_active = false;
    tombstone.configured = invalid.clone();
    tombstone.turn_selected = invalid.clone();
    tombstone.response_reported = invalid;
    let mut zero = report();
    zero.report_revision = 0;
    let mut unsafe_revision = report();
    unsafe_revision.report_revision = MAX_MODEL_REPORT_REVISION + 1;
    let mut blank = report();
    if let ModelEvidenceSlot::Observed { observation, .. } = &mut blank.configured {
        observation.model_id = " ".into();
    }
    let mut capability = report();
    if let ModelEvidenceSlot::Observed { capability, .. } = &mut capability.configured {
        *capability = ModelEvidenceCapability::Unsupported;
    }
    for snapshot in [tombstone, zero, unsafe_revision, blank, capability] {
        assert!(repo
            .claim_model_observer("runtime", "agent", Some("owner"), "new", &snapshot)
            .await
            .is_err());
        assert!(repo
            .apply_model_report("runtime", "agent", "owner", 1, &snapshot)
            .await
            .is_err());
    }
    for token in ["", " ", "bad\ntoken"] {
        assert!(repo
            .claim_model_observer("runtime", "agent", Some("owner"), token, &report())
            .await
            .is_err());
    }
    assert_eq!(authority(&store, "runtime").await, before);
}

#[tokio::test]
async fn every_stop_path_invalidates_owner_and_mark_live_does_not_restore_it() {
    for operation in ["stop", "stale", "inactive", "sibling", "activate", "revoke"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create(&store, "runtime", true).await;
        let repo = AgentRuntimes::new(&store);
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "owner", &report())
            .await
            .unwrap());
        match operation {
            "stop" => repo.stop("runtime").await.unwrap(),
            "stale" => repo.stop_stale(i64::MAX, 1).await.unwrap(),
            "inactive" => repo.set_active("runtime", false).await.unwrap(),
            "sibling" => create(&store, "sibling", true).await,
            "activate" => {
                create(&store, "sibling", false).await;
                repo.set_active("sibling", true).await.unwrap();
            }
            _ => assert!(repo
                .revoke_model_observer("runtime", "agent", "owner")
                .await
                .unwrap()),
        }
        let state = authority(&store, "runtime").await;
        assert_eq!((state.0, state.1, state.2), (None, 0, 2), "{operation}");
        let history = state.3.unwrap();
        assert!(!history.observer_active);
        assert_eq!(history.configured, report().configured);
        assert!(!repo
            .apply_model_report("runtime", "agent", "owner", 99, &report())
            .await
            .unwrap());
        repo.mark_live("runtime").await.unwrap();
        assert_eq!(authority(&store, "runtime").await.3.unwrap(), history);
    }
}

#[tokio::test]
async fn corrupt_json_remains_readable_but_only_explicit_invalidation_recovers() {
    for operation in ["stop", "revoke"] {
        for raw in ["{bad", "null", "{}"] {
            let path = TempStore::new();
            let daemon = path.open().await;
            let store = daemon.compatibility_store();
            create(&store, "runtime", true).await;
            let repo = AgentRuntimes::new(&store);
            assert!(repo
                .claim_model_observer("runtime", "agent", None, "owner", &report())
                .await
                .unwrap());
            store
                .identity_conn()
                .execute(
                    "UPDATE agent_runtimes SET model_report_json=?1",
                    libsql::params![raw],
                )
                .await
                .unwrap();
            let row = repo.find_by_runtime_id("runtime").await.unwrap().unwrap();
            assert!(row.active);
            assert!(row.model_report.is_none());
            assert_eq!(row.model_report_revision, 1);
            assert!(repo
                .apply_model_report("runtime", "agent", "owner", 1, &report())
                .await
                .is_err());
            assert!(repo
                .claim_model_observer("runtime", "agent", Some("owner"), "new", &report())
                .await
                .is_err());
            if operation == "stop" {
                repo.stop("runtime").await.unwrap();
            } else {
                assert!(repo
                    .revoke_model_observer("runtime", "agent", "owner")
                    .await
                    .unwrap());
            }
            let recovered = authority(&store, "runtime").await.3.unwrap();
            assert!(recovered.backend.is_unknown());
            assert!(!recovered.observer_active);
            assert_eq!(recovered.report_revision, 2);
            recovered.validate().unwrap();
        }
    }
}

#[tokio::test]
async fn cleanup_cannot_erase_versioned_history_but_accepts_never_claimed_rows() {
    for staged in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create(&store, "runtime", true).await;
        let repo = AgentRuntimes::new(&store);
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "owner", &report())
            .await
            .unwrap());
        let before = authority(&store, "runtime").await;
        if staged {
            assert!(repo
                .remove_staged_registration("runtime", Some("agent"))
                .await
                .is_err());
        } else {
            assert!(repo.remove_non_agent_residue("runtime").await.is_err());
        }
        assert_eq!(authority(&store, "runtime").await, before);
        create(&store, "fresh", false).await;
        if staged {
            repo.remove_staged_registration("fresh", None)
                .await
                .unwrap();
        } else {
            assert!(repo.remove_non_agent_residue("fresh").await.unwrap());
        }
        assert!(repo.find_by_runtime_id("fresh").await.unwrap().is_none());
    }
}

#[tokio::test]
async fn retired_runtime_ids_survive_split_pruning_and_cannot_reset_revision_history() {
    let path = TempStore::new();
    {
        let daemon = path.open().await;
        let exists = daemon
            .identity()
            .conn
            .query(
                "SELECT name FROM sqlite_master WHERE name='retired_model_runtime_ids'",
                (),
            )
            .await
            .unwrap()
            .next()
            .await
            .unwrap();
        assert!(exists.is_some(), "missing retired-ID authority");
        let mut rows = daemon
            .identity()
            .conn
            .query("PRAGMA table_info(retired_model_runtime_ids)", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(1).unwrap(), "runtime_id");
        assert_eq!(row.get::<String>(2).unwrap(), "TEXT");
        assert_eq!(row.get::<i64>(3).unwrap(), 1);
        assert_eq!(row.get::<i64>(5).unwrap(), 1);
        assert!(rows.next().await.unwrap().is_none());
        drop(rows);
        daemon
            .identity()
            .conn
            .execute(
                "INSERT INTO retired_model_runtime_ids(runtime_id) VALUES ('retired')",
                (),
            )
            .await
            .unwrap();
    }
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    create(&store, "sibling", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("sibling", "agent", None, "owner", &report())
        .await
        .unwrap());
    let result = repo
        .create(NewAgentRuntime {
            runtime_id: "retired".into(),
            agent_id: "agent".into(),
            harness: "opaque".into(),
            cwd: None,
            transport: None,
            presence: None,
            active: true,
        })
        .await;
    assert!(result.is_err());
    assert!(repo.find_by_runtime_id("retired").await.unwrap().is_none());
    assert!(
        repo.find_by_runtime_id("sibling")
            .await
            .unwrap()
            .unwrap()
            .active
    );
    assert_eq!(authority(&store, "sibling").await.2, 1);
}

#[tokio::test]
async fn corruption_and_safe_integer_exhaustion_fail_closed() {
    for sql in [
        "model_observer_token = x'FF'",
        "model_observer_token = ''",
        "model_observer_sequence = -1",
        "model_observer_sequence = 'broken'",
        "model_observer_sequence = 1.5",
        "model_report_revision = -1",
        "model_report_revision = 'broken'",
        "model_report_revision = 1.5",
        "model_report_revision = 9007199254740992",
    ] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create(&store, "runtime", true).await;
        let repo = AgentRuntimes::new(&store);
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "owner", &report())
            .await
            .unwrap());
        store
            .identity_conn()
            .execute(&format!("UPDATE agent_runtimes SET {sql}"), ())
            .await
            .unwrap();
        assert!(
            repo.apply_model_report("runtime", "agent", "owner", 1, &report())
                .await
                .is_err(),
            "{sql}"
        );
        assert!(
            repo.claim_model_observer("runtime", "agent", Some("owner"), "new", &report())
                .await
                .is_err(),
            "{sql}"
        );
        assert!(
            repo.revoke_model_observer("runtime", "agent", "owner")
                .await
                .is_err(),
            "{sql}"
        );
        assert!(repo.stop("runtime").await.is_err(), "{sql}");
        let row = store
            .identity_conn()
            .query("SELECT active,stopped_at FROM agent_runtimes", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 1);
        assert_eq!(row.get_value(1).unwrap(), libsql::Value::Null);
    }
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    create(&store, "runtime", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    let mut max = report();
    max.report_revision = MAX_MODEL_REPORT_REVISION;
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET model_report_revision=?1,model_report_json=?2",
            libsql::params![
                MAX_MODEL_REPORT_REVISION as i64,
                serde_json::to_string(&max).unwrap()
            ],
        )
        .await
        .unwrap();
    let before = authority(&store, "runtime").await;
    assert!(repo
        .apply_model_report("runtime", "agent", "owner", 1, &report())
        .await
        .is_err());
    assert!(repo
        .claim_model_observer("runtime", "agent", Some("owner"), "new", &report())
        .await
        .is_err());
    assert!(repo
        .revoke_model_observer("runtime", "agent", "owner")
        .await
        .is_err());
    assert!(repo.stop("runtime").await.is_err());
    assert_eq!(authority(&store, "runtime").await, before);
}

#[tokio::test]
async fn revision_mismatch_and_missing_json_are_untrusted_not_reset() {
    for json in [None, Some(serde_json::to_string(&report()).unwrap())] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create(&store, "runtime", true).await;
        let repo = AgentRuntimes::new(&store);
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "owner", &report())
            .await
            .unwrap());
        store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET model_report_json=?1",
                libsql::params![json],
            )
            .await
            .unwrap();
        let row = repo.active_for_agent("agent").await.unwrap().unwrap();
        assert!(row.model_report.is_none());
        assert_eq!(row.model_observer_token.as_deref(), Some("owner"));
        assert_eq!(row.model_report_revision, 1);
        assert!(repo
            .apply_model_report("runtime", "agent", "owner", 1, &report())
            .await
            .is_err());
        repo.stop("runtime").await.unwrap();
        assert_eq!(authority(&store, "runtime").await.2, 2);
    }
}

#[tokio::test]
async fn liveness_failure_rolls_back_model_owner_and_all_revision_changes() {
    for operation in ["stop", "stale", "inactive", "activate", "sibling"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        create(&store, "runtime", true).await;
        let repo = AgentRuntimes::new(&store);
        assert!(repo
            .claim_model_observer("runtime", "agent", None, "owner", &report())
            .await
            .unwrap());
        create(&store, "sibling", false).await;
        store.identity_conn().execute_batch("CREATE TRIGGER fail_runtime_stop BEFORE UPDATE OF stopped_at ON agent_runtimes WHEN NEW.stopped_at IS NOT NULL BEGIN SELECT RAISE(ABORT,'injected liveness failure'); END;").await.unwrap();
        let result = match operation {
            "stop" => repo.stop("runtime").await,
            "stale" => repo.stop_stale(i64::MAX, 1).await,
            "inactive" => repo.set_active("runtime", false).await,
            "activate" => repo.set_active("sibling", true).await,
            _ => repo
                .create(NewAgentRuntime {
                    runtime_id: "new".into(),
                    agent_id: "agent".into(),
                    harness: "opaque".into(),
                    cwd: None,
                    transport: None,
                    presence: None,
                    active: true,
                })
                .await
                .map(|_| ()),
        };
        assert!(result.is_err(), "{operation}");
        let row = repo.find_by_runtime_id("runtime").await.unwrap().unwrap();
        assert!(row.active);
        assert!(row.stopped_at.is_none());
        assert_eq!(row.model_observer_token.as_deref(), Some("owner"));
        assert_eq!(row.model_report_revision, 1);
        assert!(row.model_report.unwrap().observer_active);
        assert!(repo
            .apply_model_report("runtime", "agent", "owner", 1, &report())
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn parked_apply_and_invalidation_respect_both_durable_orderings() {
    use std::{future::Future, task::Poll};
    for stop in [false, true] {
        for apply_first in [false, true] {
            let path = TempStore::new();
            let daemon = path.open().await;
            let store = std::sync::Arc::new(daemon.compatibility_store());
            create(&store, "runtime", true).await;
            let repo = AgentRuntimes::new(&store);
            assert!(repo
                .claim_model_observer("runtime", "agent", None, "owner", &report())
                .await
                .unwrap());
            // Park actual repository calls at the identity writer gate. Polling establishes FIFO
            // admission deterministically; no sleeps, mock store, or deferred method invocation.
            let gate = store
                .begin_identity_write_txn("test_park_model_writes")
                .await
                .unwrap();
            let snapshot = report();
            let mut apply =
                std::pin::pin!(repo.apply_model_report("runtime", "agent", "owner", 1, &snapshot));
            let mut invalidate = std::pin::pin!(async {
                if stop {
                    repo.stop("runtime").await.map(|_| true)
                } else {
                    repo.revoke_model_observer("runtime", "agent", "owner")
                        .await
                }
            });
            std::future::poll_fn(|cx| {
                if apply_first {
                    assert!(apply.as_mut().poll(cx).is_pending());
                    assert!(invalidate.as_mut().poll(cx).is_pending());
                } else {
                    assert!(invalidate.as_mut().poll(cx).is_pending());
                    assert!(apply.as_mut().poll(cx).is_pending());
                }
                Poll::Ready(())
            })
            .await;
            gate.commit().await.unwrap();
            let (applied, invalidated) = tokio::join!(apply, invalidate);
            assert_eq!(applied.unwrap(), apply_first);
            assert!(invalidated.unwrap());
            let state = authority(&store, "runtime").await;
            assert_eq!(state.0, None);
            assert_eq!(state.2, if apply_first { 3 } else { 2 });
            assert!(!state.3.unwrap().observer_active);
        }
    }
}

async fn stale_fixture(store: &Store) {
    assert!(store.has_split_authority());
    store
        .identity_conn()
        .execute_batch(
            "INSERT INTO agents(agent_id,project,name,created_at) VALUES
         ('agent','p','selected',1), ('replacement','p','replacement',1),
         ('sibling-agent','p','sibling',1), ('orphan-agent','p','orphan',1);",
        )
        .await
        .unwrap();
    create(store, "runtime", true).await;
    let repo = AgentRuntimes::new(store);
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    assert!(repo
        .apply_model_report("runtime", "agent", "owner", 7, &report())
        .await
        .unwrap());
    store
        .identity_conn()
        .execute_batch(
            "UPDATE agent_runtimes SET started_at=10,last_heartbeat=20,
         presence='busy',os_pid=12345,os_pgid=12346 WHERE runtime_id='runtime';",
        )
        .await
        .unwrap();
}

async fn stale_session(store: &Store) {
    Sessions::new(store)
        .create(NewSession {
            session_id: SessionId("runtime".into()),
            name: Some("selected".into()),
            agent: Some("opaque".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: None,
            cwd: None,
            project: "p".into(),
            transport: None,
        })
        .await
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE sessions SET presence='online',last_heartbeat=20 WHERE session_id='runtime'",
            (),
        )
        .await
        .unwrap();
}

async fn stopped_runtime_ids(store: &Store) -> Vec<String> {
    let mut rows = store
        .conn
        .query(
            "SELECT session_id FROM developer_events WHERE lifecycle='stopped' ORDER BY session_id",
            (),
        )
        .await
        .unwrap();
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        ids.push(row.get(0).unwrap());
    }
    ids
}

async fn create_runtime_for_agent(
    store: &Store,
    runtime_id: &str,
    agent_id: &str,
    started_at: i64,
    last_heartbeat: i64,
) {
    let repo = AgentRuntimes::new(store);
    repo.create(NewAgentRuntime {
        runtime_id: runtime_id.into(),
        agent_id: agent_id.into(),
        harness: "opaque".into(),
        cwd: Some(format!("/{runtime_id}/cwd")),
        transport: Some("acp".into()),
        presence: Some("busy".into()),
        active: true,
    })
    .await
    .unwrap();
    assert!(repo
        .claim_model_observer(runtime_id, agent_id, None, "owner", &report())
        .await
        .unwrap());
    assert!(repo
        .apply_model_report(runtime_id, agent_id, "owner", 7, &report())
        .await
        .unwrap());
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET started_at=?2,last_heartbeat=?3,
             os_pid=12345,os_pgid=12346 WHERE runtime_id=?1",
            libsql::params![runtime_id, started_at, last_heartbeat],
        )
        .await
        .unwrap();
}

async fn exact_stop_fixture(store: &Store, runtime_id: &str, agent_id: &str, active: bool) {
    store
        .identity_conn()
        .execute(
            "INSERT INTO agents(agent_id,project,name,created_at) VALUES (?1,'p',?1,1)",
            libsql::params![agent_id],
        )
        .await
        .unwrap();
    AgentRuntimes::new(store)
        .create(NewAgentRuntime {
            runtime_id: runtime_id.into(),
            agent_id: agent_id.into(),
            harness: "opaque".into(),
            cwd: Some("/exact/cwd".into()),
            transport: Some("acp".into()),
            presence: Some("busy".into()),
            active,
        })
        .await
        .unwrap();
    AgentRuntimes::new(store)
        .set_process_ids(
            runtime_id,
            nexus_common::RuntimeProcessIds {
                os_pid: 12345,
                os_pgid: 12346,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn exact_missing_revalidates_after_its_call_is_parked_at_the_identity_gate() {
    use std::{future::Future, task::Poll};
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    store
        .identity_conn()
        .execute(
            "INSERT INTO agents(agent_id,project,name,created_at) VALUES ('new-agent','p','new',1)",
            (),
        )
        .await
        .unwrap();
    let gate = store
        .begin_identity_write_txn("test_exact_missing_insert_gate")
        .await
        .unwrap();
    let repo = AgentRuntimes::new(&store);
    let mut stop =
        std::pin::pin!(repo.stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Missing));
    let mut follower =
        std::pin::pin!(store.begin_identity_write_txn("test_exact_missing_fifo_witness"));
    std::future::poll_fn(|cx| {
        assert!(stop.as_mut().poll(cx).is_pending());
        assert!(follower.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;

    let mut inserted = report();
    inserted.report_revision = 7;
    gate.execute(
        "INSERT INTO agent_runtimes (
             runtime_id,agent_id,harness,cwd,transport,presence,active,started_at,stopped_at,
             last_heartbeat,os_pid,os_pgid,model_observer_token,model_observer_sequence,
             model_report_revision,model_report_json
         ) VALUES ('runtime','new-agent','opaque','/new/cwd','acp','busy',1,11,NULL,12,
             12345,12346,'new-owner',6,7,?1)",
        libsql::params![serde_json::to_string(&inserted).unwrap()],
    )
    .await
    .unwrap();
    let before = identity_tx_values(&gate, "SELECT * FROM agent_runtimes").await;
    gate.commit().await.unwrap();
    std::future::poll_fn(|cx| {
        assert!(
            follower.as_mut().poll(cx).is_pending(),
            "exact stop did not reach the identity writer gate"
        );
        Poll::Ready(())
    })
    .await;
    assert_eq!(stop.await.unwrap(), ExactRuntimeStop::BindingChanged);
    follower.await.unwrap().commit().await.unwrap();
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

#[tokio::test]
async fn exact_agent_revalidates_rebinding_after_its_call_is_parked_at_the_identity_gate() {
    use std::{future::Future, task::Poll};
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    assert!(repo
        .apply_model_report("runtime", "agent", "owner", 7, &report())
        .await
        .unwrap());
    store
        .identity_conn()
        .execute(
            "INSERT INTO agents(agent_id,project,name,created_at) VALUES ('replacement','p','replacement',1)",
            (),
        )
        .await
        .unwrap();
    let gate = store
        .begin_identity_write_txn("test_exact_rebinding_gate")
        .await
        .unwrap();
    let mut stop = std::pin::pin!(
        repo.stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Agent("agent"),)
    );
    let mut follower =
        std::pin::pin!(store.begin_identity_write_txn("test_exact_rebinding_fifo_witness"));
    std::future::poll_fn(|cx| {
        assert!(stop.as_mut().poll(cx).is_pending());
        assert!(follower.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    gate.execute(
        "UPDATE agent_runtimes SET agent_id='replacement' WHERE runtime_id='runtime'",
        (),
    )
    .await
    .unwrap();
    let before = identity_tx_values(&gate, "SELECT * FROM agent_runtimes").await;
    gate.commit().await.unwrap();
    std::future::poll_fn(|cx| {
        assert!(
            follower.as_mut().poll(cx).is_pending(),
            "exact stop did not reach the identity writer gate"
        );
        Poll::Ready(())
    })
    .await;
    assert_eq!(stop.await.unwrap(), ExactRuntimeStop::BindingChanged);
    follower.await.unwrap().commit().await.unwrap();
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

#[tokio::test]
async fn exact_stop_distinguishes_confirmed_absence_from_a_disappeared_agent_binding() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    let repo = AgentRuntimes::new(&store);
    assert_eq!(
        repo.stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Missing)
            .await
            .unwrap(),
        ExactRuntimeStop::Missing
    );
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    store
        .identity_conn()
        .execute("DELETE FROM agent_runtimes WHERE runtime_id='runtime'", ())
        .await
        .unwrap();
    assert_eq!(
        repo.stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Agent("agent"))
            .await
            .unwrap(),
        ExactRuntimeStop::BindingChanged
    );
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

#[tokio::test]
async fn exact_stop_invalidates_a_matching_active_owner_and_preserves_an_unrelated_sibling() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    exact_stop_fixture(&store, "sibling", "sibling-agent", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    assert!(repo
        .apply_model_report("runtime", "agent", "owner", 7, &report())
        .await
        .unwrap());
    let sibling_before = identity_values(
        &store,
        "SELECT * FROM agent_runtimes WHERE runtime_id='sibling'",
    )
    .await;
    assert_eq!(
        repo.stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Agent("agent"))
            .await
            .unwrap(),
        ExactRuntimeStop::Stopped
    );
    let stopped = repo.find_by_runtime_id("runtime").await.unwrap().unwrap();
    assert!(!stopped.active);
    assert_eq!(stopped.presence.as_deref(), Some("offline"));
    assert!(stopped.stopped_at.is_some());
    assert_eq!((stopped.os_pid, stopped.os_pgid), (None, None));
    let model = authority(&store, "runtime").await;
    assert_eq!((model.0, model.1, model.2), (None, 0, 3));
    assert!(!model.3.unwrap().observer_active);
    assert_eq!(
        identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='sibling'"
        )
        .await,
        sibling_before
    );
    assert_eq!(stopped_runtime_ids(&store).await, vec!["runtime"]);
}

#[tokio::test]
async fn exact_stop_matches_an_already_stopped_row_without_a_false_lifecycle() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    let repo = AgentRuntimes::new(&store);
    repo.stop("runtime").await.unwrap();
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "staged", &staged_report())
        .await
        .unwrap());
    let stopped_at = repo
        .find_by_runtime_id("runtime")
        .await
        .unwrap()
        .unwrap()
        .stopped_at;
    assert_eq!(
        repo.stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Agent("agent"))
            .await
            .unwrap(),
        ExactRuntimeStop::Stopped
    );
    let row = repo.find_by_runtime_id("runtime").await.unwrap().unwrap();
    assert_eq!(row.stopped_at, stopped_at);
    assert_eq!((row.os_pid, row.os_pgid), (None, None));
    assert_eq!(authority(&store, "runtime").await.0, None);
    assert_eq!(stopped_runtime_ids(&store).await, vec!["runtime"]);
}

#[tokio::test]
async fn exact_stop_recovers_corrupt_report_json_when_authority_is_sound() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET model_report_json='{' WHERE runtime_id='runtime'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        repo.stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Agent("agent"))
            .await
            .unwrap(),
        ExactRuntimeStop::Stopped
    );
    let recovered = authority(&store, "runtime").await;
    assert_eq!((recovered.0, recovered.1, recovered.2), (None, 0, 2));
    let recovered = recovered.3.unwrap();
    assert!(!recovered.observer_active);
    assert!(recovered.backend.is_unknown());
}

#[tokio::test]
async fn exact_stop_rejects_corrupt_authority_only_after_matching_the_binding() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    let repo = AgentRuntimes::new(&store);
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET model_observer_token=NULL,model_observer_sequence=7,
             model_report_revision=2,model_report_json='{}' WHERE runtime_id='runtime'",
            (),
        )
        .await
        .unwrap();
    let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
    assert_eq!(
        repo.stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Agent("foreign"))
            .await
            .unwrap(),
        ExactRuntimeStop::BindingChanged
    );
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    let error = repo
        .stop_if_binding_matches("runtime", ExpectedRuntimeBinding::Agent("agent"))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("corrupt stored model report authority"));
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

#[tokio::test]
async fn selected_stale_snapshot_does_not_expand_to_a_newly_stale_runtime() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    stale_fixture(&store).await;
    let repo = AgentRuntimes::new(&store);
    let selected = repo.stale_active_runtime_pairs(1000, 100).await.unwrap();
    assert_eq!(selected, vec![("runtime".into(), "agent".into())]);

    create_runtime_for_agent(&store, "later", "sibling-agent", 10, 20).await;
    let later_before = identity_values(
        &store,
        "SELECT * FROM agent_runtimes WHERE runtime_id='later'",
    )
    .await;

    assert_eq!(
        repo.stop_stale_selected(&selected, 1000, 100)
            .await
            .unwrap(),
        vec![("runtime".into(), "agent".into())]
    );
    assert_eq!(
        identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='later'"
        )
        .await,
        later_before
    );
    assert_eq!(stopped_runtime_ids(&store).await, vec!["runtime"]);
}

async fn assert_selected_stale_change_preserved(change: &str) {
    use std::{future::Future, task::Poll};
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    stale_fixture(&store).await;
    if change == "transport heartbeat" {
        stale_session(&store).await;
    }
    let repo = AgentRuntimes::new(&store);
    let selected = repo.stale_active_runtime_pairs(1000, 100).await.unwrap();
    assert_eq!(selected, vec![("runtime".into(), "agent".into())]);
    let gate = store
        .begin_identity_write_txn("test_selected_stale_gate")
        .await
        .unwrap();
    let mut stop = std::pin::pin!(repo.stop_stale_selected(&selected, 1000, 100));
    let mut follower =
        std::pin::pin!(store.begin_identity_write_txn("test_selected_stale_fifo_witness"));
    std::future::poll_fn(|cx| {
        assert!(stop.as_mut().poll(cx).is_pending());
        assert!(follower.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;

    match change {
        "transport heartbeat" => {
            store
                .conn
                .execute(
                    "UPDATE sessions SET last_heartbeat=999 WHERE session_id='runtime'",
                    (),
                )
                .await
                .unwrap();
        }
        "runtime heartbeat" => {
            gate.execute(
                "UPDATE agent_runtimes SET last_heartbeat=999 WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        }
        "agent" => {
            gate.execute(
                "UPDATE agent_runtimes SET agent_id='replacement' WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        }
        "inactive" => {
            gate.execute(
                "UPDATE agent_runtimes SET active=0 WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        }
        "stopped" => {
            gate.execute(
                "UPDATE agent_runtimes SET stopped_at=998 WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        }
        "removed" => {
            gate.execute("DELETE FROM agent_runtimes WHERE runtime_id='runtime'", ())
                .await
                .unwrap();
        }
        _ => panic!("unknown change"),
    }
    let before = identity_tx_values(
        &gate,
        "SELECT * FROM agent_runtimes WHERE runtime_id='runtime'",
    )
    .await;
    gate.commit().await.unwrap();
    std::future::poll_fn(|cx| {
        assert!(
            follower.as_mut().poll(cx).is_pending(),
            "selected stale stop did not reach the identity writer gate"
        );
        Poll::Ready(())
    })
    .await;
    assert!(stop.await.unwrap().is_empty());
    follower.await.unwrap().commit().await.unwrap();
    assert_eq!(
        identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='runtime'"
        )
        .await,
        before,
        "{change}"
    );
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

#[tokio::test]
async fn selected_stale_revalidates_transport_heartbeat_at_the_identity_gate() {
    assert_selected_stale_change_preserved("transport heartbeat").await;
}

#[tokio::test]
async fn selected_stale_revalidates_runtime_heartbeat_at_the_identity_gate() {
    assert_selected_stale_change_preserved("runtime heartbeat").await;
}

#[tokio::test]
async fn selected_stale_revalidates_binding_and_presence_at_the_identity_gate() {
    for change in ["agent", "inactive", "stopped", "removed"] {
        assert_selected_stale_change_preserved(change).await;
    }
}

#[tokio::test]
async fn selected_stale_stops_only_unique_matching_stale_pairs() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    stale_fixture(&store).await;
    create_runtime_for_agent(&store, "fresh", "replacement", 999, 999).await;
    create_runtime_for_agent(&store, "unselected", "sibling-agent", 10, 20).await;
    let fresh_before = identity_values(
        &store,
        "SELECT * FROM agent_runtimes WHERE runtime_id='fresh'",
    )
    .await;
    let unselected_before = identity_values(
        &store,
        "SELECT * FROM agent_runtimes WHERE runtime_id='unselected'",
    )
    .await;
    let selected = vec![
        ("runtime".into(), "foreign".into()),
        ("runtime".into(), "agent".into()),
        ("runtime".into(), "agent".into()),
        ("fresh".into(), "replacement".into()),
        ("unselected".into(), "foreign".into()),
        ("missing".into(), "agent".into()),
    ];

    assert_eq!(
        AgentRuntimes::new(&store)
            .stop_stale_selected(&selected, 1000, 100)
            .await
            .unwrap(),
        vec![("runtime".into(), "agent".into())]
    );
    assert_eq!(
        identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='fresh'"
        )
        .await,
        fresh_before
    );
    assert_eq!(
        identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='unselected'"
        )
        .await,
        unselected_before
    );
    assert_eq!(stopped_runtime_ids(&store).await, vec!["runtime"]);
}

#[tokio::test]
async fn selected_stale_invalid_authority_rolls_back_earlier_changes() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    stale_fixture(&store).await;
    create_runtime_for_agent(&store, "corrupt", "sibling-agent", 10, 20).await;
    store
        .identity_conn()
        .execute(
            "UPDATE agent_runtimes SET model_observer_token=NULL,model_observer_sequence=7,
             model_report_revision=2,model_report_json='{}' WHERE runtime_id='corrupt'",
            (),
        )
        .await
        .unwrap();
    let before = identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
    let selected = vec![
        ("runtime".into(), "agent".into()),
        ("corrupt".into(), "sibling-agent".into()),
    ];

    let error = AgentRuntimes::new(&store)
        .stop_stale_selected(&selected, 1000, 100)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("corrupt stored model report authority"),
        "{error}"
    );
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
        before
    );
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

async fn assert_stale_candidate_change_preserved(change: &str, mixed: bool) {
    use std::{future::Future, task::Poll};
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    stale_fixture(&store).await;
    if change == "session heartbeat" {
        stale_session(&store).await;
    }
    // Preserve another fresh runtime, including when it becomes the candidate's successor below.
    create(&store, "sibling", false).await;
    store.identity_conn().execute(
        "UPDATE agent_runtimes SET agent_id='sibling-agent',active=1,started_at=999,last_heartbeat=999 WHERE runtime_id='sibling'", (),
    ).await.unwrap();
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("sibling", "sibling-agent", None, "sibling-owner", &report())
        .await
        .unwrap());
    if mixed {
        repo.create(NewAgentRuntime {
            runtime_id: "orphan".into(),
            agent_id: "orphan-agent".into(),
            harness: "opaque".into(),
            cwd: None,
            transport: None,
            presence: Some("online".into()),
            active: true,
        })
        .await
        .unwrap();
        store.identity_conn().execute(
            "UPDATE agent_runtimes SET started_at=10,last_heartbeat=20 WHERE runtime_id='orphan'", (),
        ).await.unwrap();
    }
    let gate = store
        .begin_identity_write_txn("test_stale_candidate_gate")
        .await
        .unwrap();
    let mut stop = std::pin::pin!(repo.stop_stale(1000, 100));
    let mut follower = std::pin::pin!(store.begin_identity_write_txn("test_stale_fifo_witness"));
    std::future::poll_fn(|cx| {
        assert!(stop.as_mut().poll(cx).is_pending());
        assert!(follower.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    match change {
        "session heartbeat" => {
            store
                .conn
                .execute(
                    "UPDATE sessions SET last_heartbeat=999 WHERE session_id='runtime'",
                    (),
                )
                .await
                .unwrap();
        }
        "runtime heartbeat" => {
            gate.execute(
                "UPDATE agent_runtimes SET last_heartbeat=999 WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        }
        "agent" => {
            gate.execute(
                "UPDATE agent_runtimes SET agent_id='replacement' WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        }
        "inactive" => {
            gate.execute(
                "UPDATE agent_runtimes SET active=0 WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        }
        "stopped" => {
            gate.execute(
                "UPDATE agent_runtimes SET stopped_at=998 WHERE runtime_id='runtime'",
                (),
            )
            .await
            .unwrap();
        }
        "removed" => {
            gate.execute("DELETE FROM agent_runtimes WHERE runtime_id='runtime'", ())
                .await
                .unwrap();
        }
        _ => panic!("unknown change"),
    }
    if matches!(change, "agent" | "inactive" | "removed") {
        gate.execute(
            "UPDATE agent_runtimes SET agent_id='agent' WHERE runtime_id='sibling'",
            (),
        )
        .await
        .unwrap();
    }
    let before = identity_values(&store,
        "SELECT * FROM agent_runtimes WHERE runtime_id IN ('runtime','sibling') ORDER BY runtime_id",
    ).await;
    assert!(stopped_runtime_ids(&store).await.is_empty());
    gate.commit().await.unwrap();
    // No repoll of stop since the mutation: FIFO admission proves its candidate snapshot
    // completed before the mutation, rather than merely parking at an earlier query await.
    std::future::poll_fn(|cx| {
        assert!(
            follower.as_mut().poll(cx).is_pending(),
            "stop_stale did not reach the identity writer gate"
        );
        Poll::Ready(())
    })
    .await;
    stop.await.unwrap();
    follower.await.unwrap().commit().await.unwrap();
    assert_eq!(identity_values(&store,
        "SELECT * FROM agent_runtimes WHERE runtime_id IN ('runtime','sibling') ORDER BY runtime_id",
    ).await, before, "{change}");
    assert_eq!(
        stopped_runtime_ids(&store).await,
        if mixed {
            vec!["orphan".to_string()]
        } else {
            vec![]
        }
    );
}

#[tokio::test]
async fn stale_revalidates_transport_heartbeat_after_candidate_snapshot() {
    assert_stale_candidate_change_preserved("session heartbeat", false).await;
}

#[tokio::test]
async fn stale_revalidates_runtime_heartbeat_after_candidate_snapshot() {
    assert_stale_candidate_change_preserved("runtime heartbeat", false).await;
}

#[tokio::test]
async fn stale_revalidates_agent_identity_after_candidate_snapshot() {
    assert_stale_candidate_change_preserved("agent", false).await;
}

#[tokio::test]
async fn stale_emits_stopped_only_for_candidates_actually_changed() {
    for change in [
        "session heartbeat",
        "runtime heartbeat",
        "agent",
        "inactive",
        "stopped",
        "removed",
    ] {
        assert_stale_candidate_change_preserved(change, true).await;
    }
}

#[tokio::test]
async fn stale_orphan_invalidates_owner_and_preserves_fresh_sibling() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    stale_fixture(&store).await;
    create(&store, "sibling", false).await;
    store.identity_conn().execute(
        "UPDATE agent_runtimes SET agent_id='sibling-agent',active=1,started_at=999,last_heartbeat=999 WHERE runtime_id='sibling'", (),
    ).await.unwrap();
    let sibling_before = identity_values(
        &store,
        "SELECT * FROM agent_runtimes WHERE runtime_id='sibling'",
    )
    .await;
    let repo = AgentRuntimes::new(&store);
    repo.stop_stale(1000, 100).await.unwrap();
    let row = repo.find_by_runtime_id("runtime").await.unwrap().unwrap();
    assert!(!row.active);
    assert_eq!(row.presence.as_deref(), Some("offline"));
    assert_eq!(row.stopped_at, Some(1000));
    assert_eq!((row.os_pid, row.os_pgid), (None, None));
    let mut inactive = report();
    inactive.observer_active = false;
    inactive.report_revision = 3;
    assert_eq!(
        authority(&store, "runtime").await,
        (None, 0, 3, Some(inactive))
    );
    assert_eq!(
        identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='sibling'",
        )
        .await,
        sibling_before
    );
    assert_eq!(stopped_runtime_ids(&store).await, vec!["runtime"]);
}

#[tokio::test]
async fn stale_explicit_offline_session_overrides_fresh_timestamps() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    stale_fixture(&store).await;
    stale_session(&store).await;
    store
        .conn
        .execute(
            "UPDATE sessions SET presence='offline',last_heartbeat=999 WHERE session_id='runtime'",
            (),
        )
        .await
        .unwrap();
    store.identity_conn().execute(
        "UPDATE agent_runtimes SET started_at=999,last_heartbeat=999 WHERE runtime_id='runtime'", (),
    ).await.unwrap();
    AgentRuntimes::new(&store)
        .stop_stale(1000, 100)
        .await
        .unwrap();
    assert_eq!(authority(&store, "runtime").await.0, None);
    assert_eq!(stopped_runtime_ids(&store).await, vec!["runtime"]);
}

#[tokio::test]
async fn stale_model_invalidation_failure_rolls_back_all_candidate_changes() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    stale_fixture(&store).await;
    create(&store, "sibling", false).await;
    store.identity_conn().execute(
        "UPDATE agent_runtimes SET agent_id='sibling-agent',active=1,started_at=11,last_heartbeat=20 WHERE runtime_id='sibling'", (),
    ).await.unwrap();
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("sibling", "sibling-agent", None, "sibling-owner", &report())
        .await
        .unwrap());
    store.identity_conn().execute_batch(
        "CREATE TRIGGER fail_stale_model BEFORE UPDATE OF model_report_revision ON agent_runtimes
         WHEN OLD.runtime_id='sibling' BEGIN SELECT RAISE(ABORT,'injected stale model failure'); END;",
    ).await.unwrap();
    let before = identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
    let error = repo.stop_stale(1000, 100).await.unwrap_err();
    assert!(
        error.to_string().contains("injected stale model failure"),
        "{error}"
    );
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
        before
    );
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

#[tokio::test]
async fn same_owner_cannot_change_backend_but_new_claim_can() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    create(&store, "runtime", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    let mut other = report();
    other.backend = ModelReportBackend::new("another.opaque-backend").unwrap();
    assert!(repo
        .apply_model_report("runtime", "agent", "owner", 1, &other)
        .await
        .is_err());
    assert_eq!(authority(&store, "runtime").await.2, 1);
    assert!(repo
        .claim_model_observer("runtime", "agent", Some("owner"), "replacement", &other)
        .await
        .unwrap());
    assert_eq!(
        authority(&store, "runtime").await.3.unwrap().backend,
        other.backend
    );
}

#[tokio::test]
async fn new_null_predecessor_claim_after_revoke_keeps_monotone_history() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    create(&store, "runtime", true).await;
    let repo = AgentRuntimes::new(&store);
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "old", &report())
        .await
        .unwrap());
    assert!(repo
        .revoke_model_observer("runtime", "agent", "old")
        .await
        .unwrap());
    // This models a NEW valid reservation; the store does not prove reservation-origin freshness.
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "new", &report())
        .await
        .unwrap());
    assert_eq!(authority(&store, "runtime").await.2, 3);
    assert!(!repo
        .claim_model_observer("runtime", "agent", Some("old"), "late", &report())
        .await
        .unwrap());
    assert!(!repo
        .claim_model_observer("runtime", "agent", Some("new"), "new", &report())
        .await
        .unwrap());
    assert!(!repo
        .apply_model_report("runtime", "wrong", "new", 1, &report())
        .await
        .unwrap());
    assert!(!repo
        .revoke_model_observer("runtime", "wrong", "new")
        .await
        .unwrap());
    repo.stop("runtime").await.unwrap();
    assert!(!repo
        .claim_model_observer("runtime", "agent", None, "stopped", &report())
        .await
        .unwrap());
    create(&store, "never", false).await;
    repo.stop("never").await.unwrap();
    assert_eq!(authority(&store, "never").await, (None, 0, 0, None));
}

#[tokio::test]
async fn ignored_cas_writes_never_report_success_or_increment() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    create(&store, "runtime", true).await;
    let repo = AgentRuntimes::new(&store);
    store.identity_conn().execute_batch("CREATE TRIGGER ignore_model_write BEFORE UPDATE OF model_report_revision ON agent_runtimes BEGIN SELECT RAISE(IGNORE); END;").await.unwrap();
    assert!(!repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    assert_eq!(authority(&store, "runtime").await.2, 0);
    store
        .identity_conn()
        .execute("DROP TRIGGER ignore_model_write", ())
        .await
        .unwrap();
    assert!(repo
        .claim_model_observer("runtime", "agent", None, "owner", &report())
        .await
        .unwrap());
    store.identity_conn().execute_batch("CREATE TRIGGER ignore_model_write BEFORE UPDATE OF model_report_revision ON agent_runtimes BEGIN SELECT RAISE(IGNORE); END;").await.unwrap();
    assert!(!repo
        .apply_model_report("runtime", "agent", "owner", 1, &report())
        .await
        .unwrap());
    assert!(!repo
        .revoke_model_observer("runtime", "agent", "owner")
        .await
        .unwrap());
    assert!(repo.stop("runtime").await.is_err());
    assert_eq!(authority(&store, "runtime").await.2, 1);
    assert!(
        repo.find_by_runtime_id("runtime")
            .await
            .unwrap()
            .unwrap()
            .active
    );
}

#[tokio::test]
async fn reopen_and_compatibility_migration_reject_partial_model_schema() {
    for split in [false, true] {
        for missing in ["model_observer_sequence", "model_report_json"] {
            let path = TempStore::new();
            if split {
                let daemon = path.open().await;
                daemon
                    .identity()
                    .conn
                    .execute(
                        &format!("ALTER TABLE agent_runtimes DROP COLUMN {missing}"),
                        (),
                    )
                    .await
                    .unwrap();
            } else {
                let store = Store::open(path.0.to_str().unwrap()).await.unwrap();
                store.migrate().await.unwrap();
                store
                    .conn
                    .execute(
                        &format!("ALTER TABLE agent_runtimes DROP COLUMN {missing}"),
                        (),
                    )
                    .await
                    .unwrap();
            }
            let store = Store::open(path.0.to_str().unwrap()).await.unwrap();
            assert!(store.migrate().await.is_err());
        }
    }
}

async fn purge_fixture(store: &Store) -> nexus_store::types::SessionRow {
    store
        .identity_conn()
        .execute_batch(
            "INSERT INTO agents(agent_id,project,name,created_at,metadata_json,owner_name)
         VALUES ('agent','p','selected',1,'private agent metadata','private owner');
         INSERT INTO agent_credentials(credential_id,agent_id,secret_hash,scopes_json,created_at)
         VALUES ('credential','agent','private credential','[]',1);
         INSERT INTO agent_acl_grants(agent_id,principal_project,principal_name,role,
             granted_by_name,granted_by_project,created_at,updated_at)
         VALUES ('agent','p','viewer','viewer','selected','p',1,1);",
        )
        .await
        .unwrap();
    let sessions = Sessions::new(store);
    let id = sessions
        .create(NewSession {
            session_id: SessionId("runtime".into()),
            name: Some("selected".into()),
            agent: Some("opaque".into()),
            kind: "agent".into(),
            role: None,
            tier: "agent".into(),
            harness_session_id: None,
            client_key: None,
            cwd: None,
            project: "p".into(),
            transport: None,
        })
        .await
        .unwrap();
    sessions.set_agent_id(&id, "agent").await.unwrap();
    create(store, "runtime", true).await;
    assert!(AgentRuntimes::new(store)
        .claim_model_observer("runtime", "agent", None, "private owner token", &report())
        .await
        .unwrap());
    sessions.find_by_session_id(&id).await.unwrap().unwrap()
}

#[tokio::test]
async fn selected_identity_purge_returns_exact_receipt_without_transport_tail() {
    use nexus_store::repos::sessions::SelectedIdentityPurge;
    let path = TempStore::new();
    {
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let selected = purge_fixture(&store).await;
        create(&store, "stopped-sibling", false).await;
        store.identity_conn().execute(
            "UPDATE agent_runtimes SET agent_id='foreign-owner',model_report_json='{broken' WHERE runtime_id='runtime'", (),
        ).await.unwrap();
        let sessions = Sessions::new(&store);
        let pairs = sessions
            .runtime_pairs_for_purge(&selected.session_id, Some("agent"))
            .await
            .unwrap();
        assert_eq!(
            pairs,
            vec![
                ("runtime".into(), "foreign-owner".into()),
                ("stopped-sibling".into(), "agent".into())
            ]
        );
        let epochs = selected_epochs(&store);
        let mut unordered = pairs.iter().rev().cloned().collect::<Vec<_>>();
        unordered.push(pairs[0].clone());
        let outcome = sessions
            .purge_identity_selected(&selected, Some("agent"), &unordered)
            .await
            .unwrap();
        let SelectedIdentityPurge::Purged(receipt) = outcome else {
            panic!("selected identity was not purged")
        };
        assert_eq!(receipt.session_id(), &selected.session_id);
        assert_eq!(receipt.agent_id(), Some("agent"));
        assert_eq!(receipt.runtime_pairs(), pairs);
        assert_eq!(
            sessions
                .find_by_session_id(&selected.session_id)
                .await
                .unwrap(),
            Some(selected.clone())
        );
        assert_eq!(
            selected_epochs(&store),
            epochs,
            "identity-only purge published a transport tail"
        );
        assert!(identity_values(&store, "SELECT * FROM agent_runtimes")
            .await
            .is_empty());
        assert!(
            identity_values(&store, "SELECT * FROM agents WHERE agent_id='agent'")
                .await
                .is_empty()
        );
        let SelectedIdentityPurge::Purged(empty) = sessions
            .purge_identity_selected(&selected, Some("agent"), &[])
            .await
            .unwrap()
        else {
            panic!("empty selection must remain exact")
        };
        assert_eq!(empty.session_id(), &selected.session_id);
        assert_eq!(empty.agent_id(), Some("agent"));
        assert!(empty.runtime_pairs().is_empty());
        assert_eq!(selected_epochs(&store), epochs);
    }
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    assert_eq!(
        retired_ids(&store).await,
        vec![vec![libsql::Value::Text("runtime".into())]]
    );
}

#[tokio::test]
async fn selected_identity_purge_rejects_whole_set_change_before_corrupt_metadata() {
    use nexus_store::repos::sessions::SelectedIdentityPurge;
    for empty in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let selected = purge_fixture(&store).await;
        let sessions = Sessions::new(&store);
        let pairs = if empty {
            vec![]
        } else {
            sessions
                .runtime_pairs_for_purge(&selected.session_id, Some("agent"))
                .await
                .unwrap()
        };
        if !empty {
            create(&store, "new-sibling", false).await;
            store.identity_conn().execute("UPDATE agent_runtimes SET model_report_revision='bad' WHERE runtime_id='new-sibling'", ()).await.unwrap();
        }
        let before =
            identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
        let epochs = selected_epochs(&store);
        assert!(matches!(
            sessions
                .purge_identity_selected(&selected, Some("agent"), &pairs)
                .await
                .unwrap(),
            SelectedIdentityPurge::SelectionChanged
        ));
        assert_eq!(
            identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
            before
        );
        assert_eq!(
            sessions
                .find_by_session_id(&selected.session_id)
                .await
                .unwrap(),
            Some(selected)
        );
        assert_eq!(selected_epochs(&store), epochs);
        assert!(retired_ids(&store).await.is_empty());
    }
}

#[tokio::test]
async fn selected_identity_purge_without_agent_never_uses_name_fallback() {
    use nexus_store::repos::sessions::SelectedIdentityPurge;
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    let mut selected = purge_fixture(&store).await;
    selected.agent_id = None;
    let before = identity_values(&store, "SELECT * FROM agents ORDER BY agent_id").await;
    let credentials = identity_values(&store, "SELECT * FROM agent_credentials").await;
    let sessions = Sessions::new(&store);
    let pairs = sessions
        .runtime_pairs_for_purge(&selected.session_id, None)
        .await
        .unwrap();
    let SelectedIdentityPurge::Purged(receipt) = sessions
        .purge_identity_selected(&selected, None, &pairs)
        .await
        .unwrap()
    else {
        panic!("expected exact runtime purge")
    };
    assert_eq!(receipt.agent_id(), None);
    assert_eq!(
        identity_values(&store, "SELECT * FROM agents ORDER BY agent_id").await,
        before
    );
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_credentials").await,
        credentials
    );
    // Name-based ACL actor/grantor cleanup remains deliberate even without an agent ID.
    assert!(identity_values(&store, "SELECT * FROM agent_acl_grants")
        .await
        .is_empty());
}

#[tokio::test]
async fn selected_identity_purge_rechecks_empty_new_sibling_and_rebinding_after_writer_wait() {
    use nexus_store::repos::sessions::SelectedIdentityPurge;
    use std::{future::Future, task::Poll};
    for mode in ["empty", "sibling", "rebind"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let selected = purge_fixture(&store).await;
        if mode == "empty" {
            store
                .identity_conn()
                .execute("DELETE FROM agent_runtimes", ())
                .await
                .unwrap();
        }
        let sessions = Sessions::new(&store);
        let pairs = sessions
            .runtime_pairs_for_purge(&selected.session_id, Some("agent"))
            .await
            .unwrap();
        let gate = store
            .begin_identity_write_txn("selected_purge_gate")
            .await
            .unwrap();
        let mut purge =
            std::pin::pin!(sessions.purge_identity_selected(&selected, Some("agent"), &pairs));
        let mut follower =
            std::pin::pin!(store.begin_identity_write_txn("selected_purge_fifo_witness"));
        std::future::poll_fn(|cx| {
            assert!(purge.as_mut().poll(cx).is_pending());
            assert!(follower.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        gate.execute(match mode {
            "empty" => "INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES ('runtime','agent','opaque',1,1)",
            "sibling" => "INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES ('late-sibling','agent','opaque',0,1)",
            _ => "UPDATE agent_runtimes SET agent_id='replacement' WHERE runtime_id='runtime'",
        }, ()).await.unwrap();
        let before =
            identity_tx_values(&gate, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
        let epochs = selected_epochs(&store);
        gate.commit().await.unwrap();
        std::future::poll_fn(|cx| {
            assert!(
                follower.as_mut().poll(cx).is_pending(),
                "selected purge had not queued before mutation"
            );
            Poll::Ready(())
        })
        .await;
        assert!(matches!(
            purge.await.unwrap(),
            SelectedIdentityPurge::SelectionChanged
        ));
        follower.await.unwrap().commit().await.unwrap();
        assert_eq!(
            identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
            before
        );
        assert_eq!(
            sessions
                .find_by_session_id(&selected.session_id)
                .await
                .unwrap(),
            Some(selected.clone())
        );
        assert!(retired_ids(&store).await.is_empty());
        assert_eq!(selected_epochs(&store), epochs);
    }
}

#[tokio::test]
async fn selected_identity_purge_checked_deletion_and_commit_failures_preserve_graph() {
    use nexus_store::repos::sessions::IdentityPurgeCommitState;
    for mode in [
        "ignore",
        "reinsert",
        "foreign-reinsert",
        "agent-trigger",
        "ignore-agent",
        "ignore-credential",
        "ignore-acl",
        "retirement",
        "ended",
        "commit",
    ] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let selected = purge_fixture(&store).await;
        create(&store, "stopped-sibling", false).await;
        let sessions = Sessions::new(&store);
        let pairs = sessions
            .runtime_pairs_for_purge(&selected.session_id, Some("agent"))
            .await
            .unwrap();
        store.identity_conn().execute_batch(match mode {
            "ignore" => "CREATE TRIGGER fail_selected_purge BEFORE DELETE ON agent_runtimes BEGIN SELECT RAISE(IGNORE); END;",
            "reinsert" => "CREATE TRIGGER fail_selected_purge AFTER DELETE ON agent_runtimes BEGIN INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES (OLD.runtime_id,OLD.agent_id,OLD.harness,0,1); END;",
            "foreign-reinsert" => "CREATE TRIGGER fail_selected_purge AFTER DELETE ON agent_runtimes WHEN OLD.runtime_id='stopped-sibling' BEGIN INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES (OLD.runtime_id,'foreign',OLD.harness,0,1); END;",
            "agent-trigger" => "CREATE TRIGGER fail_selected_purge AFTER DELETE ON agents BEGIN INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES ('stopped-sibling','foreign','opaque',0,1); END;",
            "ignore-agent" => "CREATE TRIGGER fail_selected_purge BEFORE DELETE ON agents BEGIN SELECT RAISE(IGNORE); END;",
            "ignore-credential" => "CREATE TRIGGER fail_selected_purge BEFORE DELETE ON agent_credentials BEGIN SELECT RAISE(IGNORE); END;",
            "ignore-acl" => "CREATE TRIGGER fail_selected_purge BEFORE DELETE ON agent_acl_grants BEGIN SELECT RAISE(IGNORE); END;",
            "retirement" => "CREATE TRIGGER fail_selected_purge BEFORE INSERT ON retired_model_runtime_ids BEGIN SELECT RAISE(ABORT,'initiating retirement failure'); END;",
            "ended" => "CREATE TRIGGER fail_selected_purge BEFORE DELETE ON agent_runtimes BEGIN SELECT RAISE(ROLLBACK,'transaction ended unexpectedly'); END;",
            _ => "PRAGMA foreign_keys=ON; CREATE TABLE purge_parent(id INTEGER PRIMARY KEY); CREATE TABLE purge_child(id INTEGER REFERENCES purge_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fail_selected_purge AFTER DELETE ON agent_runtimes BEGIN INSERT INTO purge_child VALUES(1); END;",
        }).await.unwrap();
        let mut before = Vec::new();
        for table in [
            "agent_runtimes",
            "agents",
            "agent_credentials",
            "agent_acl_grants",
            "retired_model_runtime_ids",
        ] {
            before
                .push(identity_values(&store, &format!("SELECT * FROM {table} ORDER BY 1")).await);
        }
        let epochs = selected_epochs(&store);
        let error = sessions
            .purge_identity_selected(&selected, Some("agent"), &pairs)
            .await
            .expect_err(mode);
        assert_eq!(
            error.commit_state(),
            if matches!(mode, "commit" | "ended") {
                IdentityPurgeCommitState::Unknown
            } else {
                IdentityPurgeCommitState::NotCommitted
            }
        );
        assert!(
            error.cause().to_string().contains(match mode {
                "ignore" => "selected purge runtime deletion count",
                "reinsert" | "foreign-reinsert" | "agent-trigger" =>
                    "selected purge runtime deletion left rows",
                "ignore-agent" | "ignore-credential" | "ignore-acl" =>
                    "selected purge identity deletion left rows",
                "retirement" => "initiating retirement failure",
                "ended" => "transaction ended unexpectedly",
                _ => "FOREIGN KEY",
            }),
            "{mode}: {error}"
        );
        assert_selected_writer_admitted(&store).await;
        for (index, table) in [
            "agent_runtimes",
            "agents",
            "agent_credentials",
            "agent_acl_grants",
            "retired_model_runtime_ids",
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(
                identity_values(&store, &format!("SELECT * FROM {table} ORDER BY 1")).await,
                before[index],
                "{mode}: {table}"
            );
        }
        assert_eq!(
            sessions
                .find_by_session_id(&selected.session_id)
                .await
                .unwrap(),
            Some(selected)
        );
        assert_eq!(selected_epochs(&store), epochs);
    }
}

async fn transport_values(store: &Store, sql: &str) -> Vec<Vec<libsql::Value>> {
    let mut rows = store.conn.query(sql, ()).await.unwrap();
    let mut result = vec![];
    while let Some(row) = rows.next().await.unwrap() {
        result.push(
            (0..row.column_count())
                .map(|i| row.get_value(i).unwrap())
                .collect(),
        );
    }
    result
}

async fn transport_purge_fixture(
    store: &Store,
) -> (
    nexus_store::types::SessionRow,
    nexus_store::repos::sessions::IdentityPurgeReceipt,
) {
    let selected = purge_fixture(store).await;
    store.conn.execute_batch("INSERT INTO messages(message_id,from_name,to_name) VALUES('owned-name','selected','peer'),('unrelated','other','peer');
        INSERT INTO messages(message_id,from_agent_id) VALUES('owned-id','agent');
        INSERT INTO thread_members(thread_id,session_name) VALUES('thread','selected'),('thread','other');
        INSERT INTO thread_members(thread_id,session_name,agent_id) VALUES('ids','old-label','agent');
        INSERT INTO in_flight(in_flight_id,recipient_session) VALUES('owned-session','runtime'),('unrelated','other');
        INSERT INTO in_flight(in_flight_id,recipient_agent_id) VALUES('owned-id','agent');").await.unwrap();
    let sessions = Sessions::new(store);
    let pairs = sessions
        .runtime_pairs_for_purge(&selected.session_id, Some("agent"))
        .await
        .unwrap();
    let nexus_store::repos::sessions::SelectedIdentityPurge::Purged(receipt) = sessions
        .purge_identity_selected(&selected, Some("agent"), &pairs)
        .await
        .unwrap()
    else {
        panic!("fixture identity purge skipped");
    };
    (selected, receipt)
}

#[tokio::test]
async fn selected_transport_purge_uses_actual_receipt_without_repeating_identity_effects() {
    use nexus_store::repos::sessions::SelectedTransportPurge;
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    let (selected, receipt) = transport_purge_fixture(&store).await;
    assert_eq!(receipt.project(), "p");
    assert_eq!(receipt.name(), Some("selected"));
    // A deliberately inserted sentinel makes repeating the identity phase causally visible.
    // This is direct fixture state, not a claim authorizing post-purge identity-ID reuse.
    store.identity_conn().execute("INSERT INTO agents(agent_id,project,name,created_at) VALUES('agent','p','replacement',2)", ()).await.unwrap();
    let before = identity_values(&store, "SELECT * FROM agents").await;
    let retired = retired_ids(&store).await;
    let epochs = selected_epochs(&store);
    assert_eq!(
        Sessions::new(&store)
            .purge_transport_selected(&selected, &receipt)
            .await
            .unwrap(),
        SelectedTransportPurge::Purged
    );
    assert!(Sessions::new(&store)
        .find_by_session_id(&selected.session_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        transport_values(
            &store,
            "SELECT message_id FROM messages ORDER BY message_id"
        )
        .await,
        vec![vec![libsql::Value::Text("unrelated".into())]]
    );
    assert_eq!(
        transport_values(
            &store,
            "SELECT session_name FROM thread_members ORDER BY session_name"
        )
        .await,
        vec![vec![libsql::Value::Text("other".into())]]
    );
    assert_eq!(
        transport_values(
            &store,
            "SELECT in_flight_id FROM in_flight ORDER BY in_flight_id"
        )
        .await,
        vec![vec![libsql::Value::Text("unrelated".into())]]
    );
    assert_eq!(
        identity_values(&store, "SELECT * FROM agents").await,
        before
    );
    assert_eq!(retired_ids(&store).await, retired);
    assert_eq!(selected_epochs(&store), (epochs.0 + 1, epochs.1 + 1));
}

#[tokio::test]
async fn selected_transport_purge_rejects_changed_receipt_labels_and_current_session() {
    use nexus_store::repos::sessions::SelectedTransportPurge;
    for change in ["name", "project", "binding", "missing"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let (mut selected, receipt) = transport_purge_fixture(&store).await;
        match change {
            "name" => {
                selected.name = Some("other".into());
                store
                    .conn
                    .execute(
                        "UPDATE sessions SET name='other' WHERE session_id='runtime'",
                        (),
                    )
                    .await
                    .unwrap();
            }
            "project" => {
                selected.project = "other".into();
                store
                    .conn
                    .execute(
                        "UPDATE sessions SET project='other' WHERE session_id='runtime'",
                        (),
                    )
                    .await
                    .unwrap();
            }
            "binding" => {
                store
                    .conn
                    .execute(
                        "UPDATE sessions SET agent_id='foreign' WHERE session_id='runtime'",
                        (),
                    )
                    .await
                    .unwrap();
            }
            _ => {
                store
                    .conn
                    .execute("DELETE FROM sessions WHERE session_id='runtime'", ())
                    .await
                    .unwrap();
            }
        }
        let before = transport_values(&store, "SELECT * FROM sessions").await;
        let messages = transport_values(&store, "SELECT * FROM messages ORDER BY message_id").await;
        let epochs = selected_epochs(&store);
        assert_eq!(
            Sessions::new(&store)
                .purge_transport_selected(&selected, &receipt)
                .await
                .unwrap(),
            SelectedTransportPurge::SelectionChanged,
            "{change}"
        );
        assert_eq!(
            transport_values(&store, "SELECT * FROM sessions").await,
            before
        );
        assert_eq!(
            transport_values(&store, "SELECT * FROM messages ORDER BY message_id").await,
            messages
        );
        assert_eq!(selected_epochs(&store), epochs);
    }
}

#[tokio::test]
async fn selected_transport_purge_accepts_authorized_agent_with_null_session_stamp() {
    use nexus_store::repos::sessions::{SelectedIdentityPurge, SelectedTransportPurge};
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    let mut selected = purge_fixture(&store).await;
    store
        .conn
        .execute(
            "UPDATE sessions SET agent_id=NULL WHERE session_id='runtime'",
            (),
        )
        .await
        .unwrap();
    selected.agent_id = None;
    let sessions = Sessions::new(&store);
    let pairs = sessions
        .runtime_pairs_for_purge(&selected.session_id, Some("agent"))
        .await
        .unwrap();
    let SelectedIdentityPurge::Purged(receipt) = sessions
        .purge_identity_selected(&selected, Some("agent"), &pairs)
        .await
        .unwrap()
    else {
        panic!("identity purge skipped");
    };
    assert_eq!(receipt.agent_id(), Some("agent"));
    assert_eq!(
        sessions
            .purge_transport_selected(&selected, &receipt)
            .await
            .unwrap(),
        SelectedTransportPurge::Purged
    );
    assert!(sessions
        .find_by_session_id(&selected.session_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn selected_transport_purge_rechecks_replacement_at_actual_transport_gate() {
    use nexus_store::repos::sessions::SelectedTransportPurge;
    use std::{future::Future, task::Poll};
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    let (selected, receipt) = transport_purge_fixture(&store).await;
    let sessions = Sessions::new(&store);
    let gate = store
        .begin_write_txn("selected_transport_gate")
        .await
        .unwrap();
    let mut purge = std::pin::pin!(sessions.purge_transport_selected(&selected, &receipt));
    let mut follower = std::pin::pin!(store.begin_write_txn("selected_transport_fifo"));
    std::future::poll_fn(|cx| {
        assert!(purge.as_mut().poll(cx).is_pending());
        assert!(follower.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    gate.execute(
        "UPDATE sessions SET client_key='new-owner' WHERE session_id='runtime'",
        (),
    )
    .await
    .unwrap();
    let before = identity_tx_values(&gate, "SELECT * FROM sessions").await;
    let messages = identity_tx_values(&gate, "SELECT * FROM messages ORDER BY message_id").await;
    let epochs = selected_epochs(&store);
    gate.commit().await.unwrap();
    std::future::poll_fn(|cx| {
        assert!(
            follower.as_mut().poll(cx).is_pending(),
            "purge did not queue before mutation"
        );
        Poll::Ready(())
    })
    .await;
    assert_eq!(
        purge.await.unwrap(),
        SelectedTransportPurge::SelectionChanged
    );
    follower.await.unwrap().commit().await.unwrap();
    assert_eq!(
        transport_values(&store, "SELECT * FROM sessions").await,
        before
    );
    assert_eq!(
        transport_values(&store, "SELECT * FROM messages ORDER BY message_id").await,
        messages
    );
    assert_eq!(selected_epochs(&store), epochs);
}

#[tokio::test]
async fn selected_transport_purge_checks_required_delete_and_postimage() {
    for mode in ["ignore", "reinsert", "child-mutation"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let (selected, receipt) = transport_purge_fixture(&store).await;
        store.conn.execute_batch(match mode {
            "ignore" => "CREATE TRIGGER tail_fail BEFORE DELETE ON sessions BEGIN SELECT RAISE(IGNORE); END;",
            "reinsert" => "CREATE TRIGGER tail_fail AFTER DELETE ON sessions BEGIN INSERT INTO sessions(session_id,name,kind,tier,project,created_at) VALUES(OLD.session_id,OLD.name,OLD.kind,OLD.tier,OLD.project,OLD.created_at); END;",
            _ => "CREATE TRIGGER tail_fail AFTER DELETE ON in_flight BEGIN UPDATE sessions SET agent_id='foreign' WHERE session_id='runtime'; END;",
        }).await.unwrap();
        let before = transport_values(&store, "SELECT * FROM sessions").await;
        let messages = transport_values(&store, "SELECT * FROM messages ORDER BY message_id").await;
        let epochs = selected_epochs(&store);
        let error = Sessions::new(&store)
            .purge_transport_selected(&selected, &receipt)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(match mode {
                "ignore" => "did not delete exactly one Session",
                "reinsert" => "left a Session row",
                _ => "Session changed during child cleanup",
            }),
            "{mode}: {error}"
        );
        store
            .begin_write_txn("transport_following_error")
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        assert_eq!(
            transport_values(&store, "SELECT * FROM sessions").await,
            before
        );
        assert_eq!(
            transport_values(&store, "SELECT * FROM messages ORDER BY message_id").await,
            messages
        );
        assert_eq!(selected_epochs(&store), epochs);
        assert!(
            identity_values(&store, "SELECT * FROM agents")
                .await
                .is_empty(),
            "identity deletion is not rolled back by transport error"
        );
    }
}

#[tokio::test]
async fn selected_transport_purge_preserves_failure_phase_and_initiating_cause() {
    for mode in ["required", "ended", "commit", "append"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let (selected, receipt) = transport_purge_fixture(&store).await;
        store.conn.execute_batch(match mode {
            "required" => "CREATE TRIGGER tail_fail BEFORE DELETE ON sessions BEGIN SELECT RAISE(ABORT,'original required Session failure'); END;",
            "ended" => "CREATE TRIGGER tail_fail BEFORE DELETE ON in_flight BEGIN SELECT RAISE(ROLLBACK,'original child ended transaction'); END;",
            "commit" => "PRAGMA foreign_keys=ON; CREATE TABLE tail_parent(id INTEGER PRIMARY KEY); CREATE TABLE tail_child(id INTEGER REFERENCES tail_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER tail_fail AFTER DELETE ON sessions BEGIN INSERT INTO tail_child VALUES(1); END;",
            _ => "CREATE TRIGGER tail_fail BEFORE INSERT ON developer_events WHEN NEW.session_id='runtime' AND NEW.lifecycle='stopped' BEGIN SELECT RAISE(ABORT,'original postcommit append failure'); END;",
        }).await.unwrap();
        let before = transport_values(&store, "SELECT * FROM sessions").await;
        let messages = transport_values(&store, "SELECT * FROM messages ORDER BY message_id").await;
        let epochs = selected_epochs(&store);
        let error = Sessions::new(&store)
            .purge_transport_selected(&selected, &receipt)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(match mode {
                "required" => "original required Session failure",
                "ended" => "original child ended transaction",
                "commit" => "FOREIGN KEY",
                _ => "original postcommit append failure",
            }),
            "{mode}: {error}"
        );
        store
            .begin_write_txn("transport_following_phase")
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
        if mode == "append" {
            assert!(transport_values(&store, "SELECT * FROM sessions")
                .await
                .is_empty());
            assert_eq!(
                transport_values(
                    &store,
                    "SELECT message_id FROM messages ORDER BY message_id"
                )
                .await,
                vec![vec![libsql::Value::Text("unrelated".into())]]
            );
        } else {
            assert_eq!(
                transport_values(&store, "SELECT * FROM sessions").await,
                before
            );
            assert_eq!(
                transport_values(&store, "SELECT * FROM messages ORDER BY message_id").await,
                messages
            );
        }
        assert_eq!(selected_epochs(&store), epochs);
        assert!(identity_values(&store, "SELECT * FROM agents")
            .await
            .is_empty());
        assert_eq!(receipt.agent_id(), Some("agent"));
        assert_eq!(receipt.name(), Some("selected"));
    }
}

#[tokio::test]
async fn selected_transport_purge_keeps_ordinary_child_cleanup_best_effort() {
    use nexus_store::repos::sessions::SelectedTransportPurge;
    for table in ["in_flight", "thread_members", "messages"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let (selected, receipt) = transport_purge_fixture(&store).await;
        store.conn.execute_batch(&format!("CREATE TRIGGER ordinary_child_failure BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'ordinary child cleanup failure'); END;")).await.unwrap();
        let before = transport_values(&store, &format!("SELECT * FROM {table} ORDER BY 1")).await;
        let epochs = selected_epochs(&store);
        assert_eq!(
            Sessions::new(&store)
                .purge_transport_selected(&selected, &receipt)
                .await
                .unwrap(),
            SelectedTransportPurge::Purged
        );
        assert!(Sessions::new(&store)
            .find_by_session_id(&selected.session_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            transport_values(&store, &format!("SELECT * FROM {table} ORDER BY 1")).await,
            before
        );
        assert_eq!(selected_epochs(&store), (epochs.0 + 1, epochs.1 + 1));
    }
}

async fn identity_values(store: &Store, sql: &str) -> Vec<Vec<libsql::Value>> {
    let mut rows = store.identity_conn().query(sql, ()).await.unwrap();
    let mut result = vec![];
    while let Some(row) = rows.next().await.unwrap() {
        result.push(
            (0..row.column_count())
                .map(|i| row.get_value(i).unwrap())
                .collect(),
        );
    }
    result
}

async fn identity_tx_values(tx: &nexus_store::WriteTxn, sql: &str) -> Vec<Vec<libsql::Value>> {
    let mut rows = tx.query(sql, ()).await.unwrap();
    let mut result = vec![];
    while let Some(row) = rows.next().await.unwrap() {
        result.push(
            (0..row.column_count())
                .map(|i| row.get_value(i).unwrap())
                .collect(),
        );
    }
    result
}

async fn retired_ids(store: &Store) -> Vec<Vec<libsql::Value>> {
    identity_values(
        store,
        "SELECT * FROM retired_model_runtime_ids ORDER BY runtime_id",
    )
    .await
}

#[tokio::test]
async fn purge_retires_the_deleted_identity_set_and_survives_split_reopen() {
    let path = TempStore::new();
    {
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let selected = purge_fixture(&store).await;
        // Creating a successor stops, but must not exclude, the observed root from purge.
        create(&store, "sibling", true).await;
        assert!(AgentRuntimes::new(&store)
            .claim_model_observer("sibling", "agent", None, "sibling owner", &report())
            .await
            .unwrap());
        AgentRuntimes::new(&store).stop("sibling").await.unwrap();
        AgentRuntimes::new(&store)
            .set_active("runtime", true)
            .await
            .unwrap();
        create(&store, "legacy", false).await;
        store
            .identity_conn()
            .execute(
                "INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at)
             VALUES ('foreign','other-agent','opaque',1,1)",
                (),
            )
            .await
            .unwrap();
        assert!(AgentRuntimes::new(&store)
            .claim_model_observer("foreign", "other-agent", None, "foreign owner", &report())
            .await
            .unwrap());
        let foreign = authority(&store, "foreign").await;
        Sessions::new(&store)
            .purge_selected(&selected)
            .await
            .unwrap();
        assert_eq!(
            retired_ids(&store).await,
            vec![
                vec![libsql::Value::Text("runtime".into())],
                vec![libsql::Value::Text("sibling".into())],
            ]
        );
        for table in [
            "agents",
            "agent_credentials",
            "agent_acl_grants",
            "agent_runtimes",
        ] {
            assert!(
                identity_values(
                    &store,
                    &format!("SELECT * FROM {table} WHERE agent_id='agent'")
                )
                .await
                .is_empty(),
                "{table}"
            );
        }
        assert!(Sessions::new(&store)
            .find_by_session_id(&selected.session_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(authority(&store, "foreign").await, foreign);
        let columns = identity_values(&store, "PRAGMA table_info(retired_model_runtime_ids)").await;
        assert_eq!(
            columns.len(),
            1,
            "no model, identity, token, or source payload retained"
        );
        assert_eq!(columns[0][1], libsql::Value::Text("runtime_id".into()));
    }
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    assert_eq!(retired_ids(&store).await.len(), 2);
    store.identity_conn().execute(
        "INSERT INTO agents(agent_id,project,name,created_at) VALUES ('agent','p','fresh-agent',1)", ()
    ).await.unwrap();
    for id in ["runtime", "sibling"] {
        assert!(AgentRuntimes::new(&store)
            .create(NewAgentRuntime {
                runtime_id: id.into(),
                agent_id: "agent".into(),
                harness: "opaque".into(),
                cwd: None,
                transport: None,
                presence: None,
                active: true,
            })
            .await
            .is_err());
    }
    create(&store, "distinct", true).await;
    create(&store, "legacy", false).await;
}

#[tokio::test]
async fn purge_exact_runtime_fallback_retires_only_that_runtime() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    let mut selected = purge_fixture(&store).await;
    selected.agent_id = None;
    selected.name = None;
    create(&store, "sibling", false).await;
    Sessions::new(&store)
        .purge_selected(&selected)
        .await
        .unwrap();
    assert_eq!(
        retired_ids(&store).await,
        vec![vec![libsql::Value::Text("runtime".into())]]
    );
    assert!(AgentRuntimes::new(&store)
        .find_by_runtime_id("runtime")
        .await
        .unwrap()
        .is_none());
    assert!(AgentRuntimes::new(&store)
        .find_by_runtime_id("sibling")
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        identity_values(&store, "SELECT * FROM agents").await.len(),
        1
    );
}

#[tokio::test]
async fn purge_retirement_failure_preserves_the_entire_identity_graph() {
    for failure in ["ABORT", "IGNORE"] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let selected = purge_fixture(&store).await;
        create(&store, "sibling", true).await;
        assert!(AgentRuntimes::new(&store)
            .claim_model_observer("sibling", "agent", None, "sibling owner", &report())
            .await
            .unwrap());
        let mut before = vec![];
        for table in [
            "agents",
            "agent_credentials",
            "agent_acl_grants",
            "agent_runtimes",
        ] {
            before.push(identity_values(&store, &format!("SELECT * FROM {table}")).await);
        }
        let action = if failure == "ABORT" {
            "RAISE(ABORT,'injected retirement failure')"
        } else {
            "RAISE(IGNORE)"
        };
        store
            .identity_conn()
            .execute_batch(&format!(
                "CREATE TRIGGER fail_retirement BEFORE INSERT ON retired_model_runtime_ids
             WHEN EXISTS (SELECT 1 FROM retired_model_runtime_ids)
             BEGIN SELECT {action}; END;"
            ))
            .await
            .unwrap();
        assert!(
            Sessions::new(&store)
                .purge_selected(&selected)
                .await
                .is_err(),
            "{failure}"
        );
        for (index, table) in [
            "agents",
            "agent_credentials",
            "agent_acl_grants",
            "agent_runtimes",
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(
                identity_values(&store, &format!("SELECT * FROM {table}")).await,
                before[index],
                "{failure}: {table}"
            );
        }
        assert!(retired_ids(&store).await.is_empty());
    }
}

#[tokio::test]
async fn purge_accepts_corrupt_json_but_rejects_corrupt_authority_in_any_affected_row() {
    for sql in [
        "model_report_revision=-1",
        "model_report_revision='broken'",
        "model_report_revision=1.5",
        "model_report_revision=9007199254740992",
        "model_report_revision=0",
        "model_observer_token=x'FF'",
        "model_observer_token=''",
        "model_observer_sequence=-1",
        "model_observer_sequence='broken'",
        "model_observer_sequence=1.5",
        "model_observer_token=NULL,model_observer_sequence=1",
        "model_report_json='{broken'",
    ] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let selected = purge_fixture(&store).await;
        // Poison a sibling: the selected root's pre-transaction lookup cannot catch this.
        create(&store, "sibling", true).await;
        assert!(AgentRuntimes::new(&store)
            .claim_model_observer("sibling", "agent", None, "sibling owner", &report())
            .await
            .unwrap());
        store
            .identity_conn()
            .execute(
                &format!("UPDATE agent_runtimes SET {sql} WHERE runtime_id='sibling'"),
                (),
            )
            .await
            .unwrap();
        let before =
            identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
        let result = Sessions::new(&store)
            .purge(&selected.session_id, "selected")
            .await;
        if sql.starts_with("model_report_json") {
            result.unwrap();
            assert_eq!(retired_ids(&store).await.len(), 2);
        } else {
            assert!(result.is_err(), "{sql}");
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
                before,
                "{sql}"
            );
            assert_eq!(
                identity_values(&store, "SELECT * FROM agents").await.len(),
                1
            );
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_credentials")
                    .await
                    .len(),
                1
            );
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_acl_grants")
                    .await
                    .len(),
                1
            );
            assert!(retired_ids(&store).await.is_empty());
        }
    }
}

#[tokio::test]
async fn parked_apply_and_purge_respect_both_durable_orderings() {
    use std::{future::Future, task::Poll};
    for apply_first in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        let selected = purge_fixture(&store).await;
        let gate = store
            .begin_identity_write_txn("test_park_purge")
            .await
            .unwrap();
        let repo = AgentRuntimes::new(&store);
        let sessions = Sessions::new(&store);
        let snapshot = report();
        let mut apply = std::pin::pin!(repo.apply_model_report(
            "runtime",
            "agent",
            "private owner token",
            1,
            &snapshot
        ));
        let mut purge = std::pin::pin!(sessions.purge_selected(&selected));
        std::future::poll_fn(|cx| {
            if apply_first {
                assert!(apply.as_mut().poll(cx).is_pending());
                assert!(purge.as_mut().poll(cx).is_pending());
            } else {
                assert!(purge.as_mut().poll(cx).is_pending());
                assert!(apply.as_mut().poll(cx).is_pending());
            }
            Poll::Ready(())
        })
        .await;
        gate.commit().await.unwrap();
        let (applied, purged) = tokio::join!(apply, purge);
        assert_eq!(applied.unwrap(), apply_first);
        purged.unwrap();
        assert!(repo.find_by_runtime_id("runtime").await.unwrap().is_none());
        assert_eq!(retired_ids(&store).await.len(), 1);
        assert!(!repo
            .apply_model_report("runtime", "agent", "private owner token", 2, &snapshot)
            .await
            .unwrap());
        assert!(!repo
            .revoke_model_observer("runtime", "agent", "private owner token")
            .await
            .unwrap());
    }
}

fn selected_new_runtime() -> NewAgentRuntime {
    NewAgentRuntime {
        runtime_id: "target".into(),
        agent_id: "agent".into(),
        harness: "opaque-selected".into(),
        cwd: Some("/selected/cwd".into()),
        transport: Some("acp".into()),
        presence: Some("busy".into()),
        active: true,
    }
}

async fn selected_activation_fixture(store: &Store, creating: bool) {
    store
        .identity_conn()
        .execute_batch(
            "INSERT INTO agents(agent_id,project,name,created_at) VALUES
         ('agent','p','selected-agent',1),('foreign','p','foreign-agent',1);",
        )
        .await
        .unwrap();
    let repo = AgentRuntimes::new(store);
    for id in ["sibling-a", "sibling-b", "foreign-runtime"] {
        create(store, id, false).await;
        let agent = if id == "foreign-runtime" {
            "foreign"
        } else {
            "agent"
        };
        // The partial unique index permits one active, unstopped row per agent. Claim each
        // report while live, then stage the legacy active-but-stopped sibling explicitly.
        if id == "sibling-b" {
            store
                .identity_conn()
                .execute(
                    "UPDATE agent_runtimes SET stopped_at=9 WHERE runtime_id='sibling-a'",
                    (),
                )
                .await
                .unwrap();
        }
        store
            .identity_conn()
            .execute(
                "UPDATE agent_runtimes SET agent_id=?2,active=1,started_at=11,last_heartbeat=12,
             os_pid=12345,os_pgid=12346,cwd='/sibling/cwd',transport='acp',presence='busy'
             WHERE runtime_id=?1",
                libsql::params![id, agent],
            )
            .await
            .unwrap();
        assert!(repo
            .claim_model_observer(id, agent, None, "sibling-owner", &report())
            .await
            .unwrap());
        assert!(repo
            .apply_model_report(id, agent, "sibling-owner", 7, &report())
            .await
            .unwrap());
        if id == "sibling-b" {
            store.identity_conn().execute_batch("UPDATE agent_runtimes SET stopped_at=9 WHERE runtime_id='sibling-b'; UPDATE agent_runtimes SET stopped_at=NULL WHERE runtime_id='sibling-a';").await.unwrap();
        }
    }
    // Malformed foreign metadata must never be decoded by the selected mutation.
    store
        .identity_conn()
        .execute_batch(
            "UPDATE agent_runtimes SET agent_id='foreign',model_observer_token=X'80'
         WHERE runtime_id='foreign-runtime';
         UPDATE agent_runtimes SET stopped_at=9 WHERE runtime_id='sibling-b';",
        )
        .await
        .unwrap();
    if !creating {
        let mut target = selected_new_runtime();
        target.active = false;
        repo.create(target).await.unwrap();
        assert!(repo
            .claim_model_observer("target", "agent", None, "target-owner", &staged_report())
            .await
            .unwrap());
        store.identity_conn().execute_batch(
            "UPDATE agent_runtimes SET stopped_at=10,last_heartbeat=13,os_pid=54321,os_pgid=54322
             WHERE runtime_id='target';",
        ).await.unwrap();
    }
}

async fn selected_activation_call(
    store: &Store,
    creating: bool,
    siblings: &[(String, String)],
) -> Result<SelectedRuntimeActivation, RuntimeActivationFailure> {
    let repo = AgentRuntimes::new(store);
    if creating {
        repo.create_active_selected(selected_new_runtime(), siblings)
            .await
    } else {
        repo.activate_selected("target", "agent", siblings).await
    }
}

fn selected_siblings() -> Vec<(String, String)> {
    vec![
        ("sibling-a".into(), "agent".into()),
        ("sibling-b".into(), "agent".into()),
    ]
}

async fn selected_event_rows(store: &Store) -> Vec<Vec<libsql::Value>> {
    let mut rows = store
        .conn
        .query("SELECT * FROM developer_events ORDER BY topic,seq", ())
        .await
        .unwrap();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        out.push(
            (0..row.column_count())
                .map(|i| row.get_value(i).unwrap())
                .collect(),
        );
    }
    out
}

fn selected_epochs(store: &Store) -> (u64, u64) {
    (
        store.events().session_lifecycle_changed().epoch(),
        store.events().developer_event_appended().epoch(),
    )
}

async fn assert_selected_writer_admitted(store: &Store) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        store
            .begin_identity_write_txn("selected_activation_following_writer")
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    })
    .await
    .expect("selected activation stranded the identity writer");
}

async fn assert_selected_changed_at_gate(creating: bool, mutation: &str) {
    use std::{future::Future, task::Poll};
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    selected_activation_fixture(&store, creating).await;
    let repo = AgentRuntimes::new(&store);
    let selected = repo
        .active_sibling_runtime_pairs("agent", "target")
        .await
        .unwrap();
    assert_eq!(selected, selected_siblings());
    let gate = store
        .begin_identity_write_txn("selected_activation_race_gate")
        .await
        .unwrap();
    let mut activation = std::pin::pin!(selected_activation_call(&store, creating, &selected));
    let mut follower =
        std::pin::pin!(store.begin_identity_write_txn("selected_activation_fifo_witness"));
    std::future::poll_fn(|cx| {
        assert!(activation.as_mut().poll(cx).is_pending());
        assert!(follower.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    gate.execute_batch(match mutation {
        "target-present" => "INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at,model_observer_token)
                             VALUES('target','foreign','inserted',1,30,31,X'80');",
        "target-rebound" => "UPDATE agent_runtimes SET agent_id='foreign',model_observer_token=X'80' WHERE runtime_id='target';",
        "target-removed" => "DELETE FROM agent_runtimes WHERE runtime_id='target';",
        "sibling-added" => "INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at,model_observer_token)
                            VALUES('added','agent','inserted',1,30,31,X'80');",
        "sibling-removed" => "UPDATE agent_runtimes SET active=0 WHERE runtime_id='sibling-a';",
        "sibling-rebound" => "UPDATE agent_runtimes SET agent_id='foreign',stopped_at=31,model_observer_token=X'80' WHERE runtime_id='sibling-a';",
        _ => panic!("unknown mutation"),
    }).await.unwrap();
    let before =
        identity_tx_values(&gate, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
    let events = selected_event_rows(&store).await;
    let epochs = selected_epochs(&store);
    gate.commit().await.unwrap();
    std::future::poll_fn(|cx| {
        assert!(
            follower.as_mut().poll(cx).is_pending(),
            "selected activation missed FIFO writer admission"
        );
        Poll::Ready(())
    })
    .await;
    assert_eq!(
        activation.await.unwrap(),
        SelectedRuntimeActivation::SelectionChanged,
        "{creating} {mutation}"
    );
    follower.await.unwrap().commit().await.unwrap();
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
        before
    );
    assert_eq!(selected_event_rows(&store).await, events);
    assert_eq!(selected_epochs(&store), epochs);
}

#[tokio::test]
async fn selected_activation_revalidates_target_at_actual_identity_gate() {
    assert_selected_changed_at_gate(true, "target-present").await;
    assert_selected_changed_at_gate(false, "target-rebound").await;
    assert_selected_changed_at_gate(false, "target-removed").await;
}

#[tokio::test]
async fn selected_activation_revalidates_entire_sibling_set_at_actual_identity_gate() {
    for creating in [false, true] {
        for mutation in ["sibling-added", "sibling-removed", "sibling-rebound"] {
            assert_selected_changed_at_gate(creating, mutation).await;
        }
    }
}

#[tokio::test]
async fn selected_activation_preserves_exact_rows_and_invalidates_all_selected_siblings() {
    for creating in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        selected_activation_fixture(&store, creating).await;
        let foreign = identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='foreign-runtime'",
        )
        .await;
        let target_before = identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='target'",
        )
        .await;
        let epochs = selected_epochs(&store);
        let mut selected = selected_siblings();
        selected.reverse();
        selected.push(selected[0].clone());
        let outcome = selected_activation_call(&store, creating, &selected)
            .await
            .unwrap();
        let SelectedRuntimeActivation::Applied(changes) = outcome else {
            panic!("selection unexpectedly changed")
        };
        assert_eq!(changes.target_pair(), &("target".into(), "agent".into()));
        assert_eq!(changes.stopped_sibling_pairs(), selected_siblings());
        let repo = AgentRuntimes::new(&store);
        assert!(repo
            .active_sibling_runtime_pairs("agent", "target")
            .await
            .unwrap()
            .is_empty());
        for id in ["sibling-a", "sibling-b"] {
            let row = repo.find_by_runtime_id(id).await.unwrap().unwrap();
            assert!(!row.active);
            assert!(row.stopped_at.is_some());
            if id == "sibling-b" {
                assert_eq!(row.stopped_at, Some(9));
            }
            assert_eq!(row.os_pid, None);
            assert_eq!(row.os_pgid, None);
            assert_eq!(row.presence.as_deref(), Some("busy"));
            assert_eq!(row.last_heartbeat, Some(12));
            let (token, sequence, revision, stored_report) = authority(&store, id).await;
            assert_eq!((token, sequence, revision), (None, 0, 3));
            assert!(!stored_report.unwrap().observer_active);
            assert!(!repo
                .apply_model_report(id, "agent", "sibling-owner", 8, &report())
                .await
                .unwrap());
        }
        let target = repo.find_by_runtime_id("target").await.unwrap().unwrap();
        assert!(target.active);
        assert_eq!(target.stopped_at, None);
        if creating {
            assert_eq!(target.harness, "opaque-selected");
            assert_eq!(target.cwd.as_deref(), Some("/selected/cwd"));
            assert_eq!(target.transport.as_deref(), Some("acp"));
            assert_eq!(target.presence.as_deref(), Some("busy"));
            assert_eq!(
                (target.last_heartbeat, target.os_pid, target.os_pgid),
                (None, None, None)
            );
            assert_eq!(authority(&store, "target").await, (None, 0, 0, None));
        } else {
            // Compare every target column except the two legacy activation fields.
            let mut expected = target_before;
            let names: Vec<String> = identity_values(&store, "PRAGMA table_info(agent_runtimes)")
                .await
                .into_iter()
                .map(|row| match &row[1] {
                    libsql::Value::Text(name) => name.clone(),
                    _ => panic!("column name"),
                })
                .collect();
            expected[0][names.iter().position(|n| n == "active").unwrap()] =
                libsql::Value::Integer(1);
            expected[0][names.iter().position(|n| n == "stopped_at").unwrap()] =
                libsql::Value::Null;
            assert_eq!(
                identity_values(
                    &store,
                    "SELECT * FROM agent_runtimes WHERE runtime_id='target'"
                )
                .await,
                expected
            );
        }
        assert_eq!(
            identity_values(
                &store,
                "SELECT * FROM agent_runtimes WHERE runtime_id='foreign-runtime'"
            )
            .await,
            foreign
        );
        assert_eq!(
            stopped_runtime_ids(&store).await,
            vec!["sibling-a", "sibling-b"]
        );
        assert_eq!(selected_epochs(&store), (epochs.0 + 1, epochs.1 + 2));
    }
}

#[tokio::test]
async fn selected_activation_recovers_corrupt_json_and_preserves_unobserved_absence() {
    for creating in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        selected_activation_fixture(&store, creating).await;
        store.identity_conn().execute_batch(
            "UPDATE agent_runtimes SET model_report_json='{broken' WHERE runtime_id='sibling-a';
             UPDATE agent_runtimes SET model_observer_token=NULL,model_observer_sequence=0,
             model_report_revision=0,model_report_json=NULL WHERE runtime_id='sibling-b';",
        ).await.unwrap();
        assert!(matches!(
            selected_activation_call(&store, creating, &selected_siblings())
                .await
                .unwrap(),
            SelectedRuntimeActivation::Applied(_)
        ));
        let (token, sequence, revision, report) = authority(&store, "sibling-a").await;
        assert_eq!((token, sequence, revision), (None, 0, 3));
        let report = report.unwrap();
        assert!(!report.observer_active);
        assert!(report.backend.is_unknown());
        assert!(matches!(
            report.configured,
            ModelEvidenceSlot::Invalid { .. }
        ));
        assert_eq!(authority(&store, "sibling-b").await, (None, 0, 0, None));
    }
}

#[tokio::test]
async fn selected_activation_rolls_back_siblings_on_bad_authority_or_ignored_target_write() {
    for creating in [false, true] {
        for failure_kind in [
            "bad-authority",
            "exhausted-revision",
            "ignored-target",
            "ignored-sibling",
            "ignored-invalidation",
        ] {
            let path = TempStore::new();
            let daemon = path.open().await;
            let store = daemon.compatibility_store();
            selected_activation_fixture(&store, creating).await;
            match failure_kind {
                "bad-authority" => {
                    store.identity_conn().execute("UPDATE agent_runtimes SET model_observer_token=X'80' WHERE runtime_id='sibling-b'", ()).await.unwrap();
                }
                "exhausted-revision" => {
                    store.identity_conn().execute("UPDATE agent_runtimes SET model_report_revision=?1 WHERE runtime_id='sibling-b'", libsql::params![MAX_MODEL_REPORT_REVISION as i64]).await.unwrap();
                }
                "ignored-sibling" => {
                    store.identity_conn().execute_batch("CREATE TRIGGER ignore_selected_sibling BEFORE UPDATE OF active ON agent_runtimes WHEN OLD.runtime_id='sibling-b' BEGIN SELECT RAISE(IGNORE); END;").await.unwrap();
                }
                "ignored-invalidation" => {
                    store.identity_conn().execute_batch("CREATE TRIGGER ignore_selected_invalidation BEFORE UPDATE OF model_report_revision ON agent_runtimes WHEN OLD.runtime_id='sibling-b' BEGIN SELECT RAISE(IGNORE); END;").await.unwrap();
                }
                _ => {
                    let operation = if creating {
                        "INSERT"
                    } else {
                        "UPDATE OF active"
                    };
                    store.identity_conn().execute_batch(&format!("CREATE TRIGGER ignore_selected_target BEFORE {operation} ON agent_runtimes WHEN NEW.runtime_id='target' BEGIN SELECT RAISE(IGNORE); END;")).await.unwrap();
                }
            }
            let before =
                identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
            let epochs = selected_epochs(&store);
            let failure = selected_activation_call(&store, creating, &selected_siblings())
                .await
                .unwrap_err();
            assert_eq!(
                failure.commit_state(),
                RuntimeActivationCommitState::NotCommitted
            );
            assert!(failure.confirmed_changes().is_none());
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
                before
            );
            assert!(selected_event_rows(&store).await.is_empty());
            assert_eq!(selected_epochs(&store), epochs);
            assert_selected_writer_admitted(&store).await;
        }
    }
}

#[tokio::test]
async fn selected_activation_empty_siblings_preserves_even_malformed_existing_target_metadata() {
    for creating in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        selected_activation_fixture(&store, creating).await;
        store
            .identity_conn()
            .execute_batch(
                "DELETE FROM agent_runtimes WHERE runtime_id IN ('sibling-a','sibling-b');",
            )
            .await
            .unwrap();
        if !creating {
            store.identity_conn().execute_batch("UPDATE agent_runtimes SET active=1,stopped_at=NULL,model_observer_token=X'80' WHERE runtime_id='target';").await.unwrap();
        }
        let before = identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='target'",
        )
        .await;
        let epochs = selected_epochs(&store);
        assert!(matches!(
            selected_activation_call(&store, creating, &[])
                .await
                .unwrap(),
            SelectedRuntimeActivation::Applied(_)
        ));
        if !creating {
            assert_eq!(
                identity_values(
                    &store,
                    "SELECT * FROM agent_runtimes WHERE runtime_id='target'"
                )
                .await,
                before
            );
        }
        assert!(selected_event_rows(&store).await.is_empty());
        assert_eq!(selected_epochs(&store), (epochs.0 + 1, epochs.1));
    }
}

#[tokio::test]
async fn selected_activation_rejects_invalid_selectors_and_retired_create_without_effects() {
    for creating in [false, true] {
        for invalid in ["foreign", "target", "retired", "inactive"] {
            if !creating && matches!(invalid, "retired" | "inactive") {
                continue;
            }
            let path = TempStore::new();
            let daemon = path.open().await;
            let store = daemon.compatibility_store();
            selected_activation_fixture(&store, creating).await;
            let mut selected = selected_siblings();
            if invalid == "foreign" {
                selected.push(("foreign-runtime".into(), "foreign".into()));
            }
            if invalid == "target" {
                selected.push(("target".into(), "agent".into()));
            }
            if invalid == "retired" {
                store
                    .identity_conn()
                    .execute(
                        "INSERT INTO retired_model_runtime_ids(runtime_id) VALUES('target')",
                        (),
                    )
                    .await
                    .unwrap();
            }
            let before =
                identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
            let retired = retired_ids(&store).await;
            let epochs = selected_epochs(&store);
            let failure = if invalid == "inactive" {
                let mut target = selected_new_runtime();
                target.active = false;
                AgentRuntimes::new(&store)
                    .create_active_selected(target, &selected)
                    .await
                    .unwrap_err()
            } else {
                selected_activation_call(&store, creating, &selected)
                    .await
                    .unwrap_err()
            };
            assert_eq!(
                failure.commit_state(),
                RuntimeActivationCommitState::NotCommitted
            );
            assert!(matches!(
                failure.cause(),
                nexus_common::NexusError::Invalid(_)
            ));
            assert!(failure.confirmed_changes().is_none());
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
                before
            );
            assert_eq!(retired_ids(&store).await, retired);
            assert!(selected_event_rows(&store).await.is_empty());
            assert_eq!(selected_epochs(&store), epochs);
            assert_selected_writer_admitted(&store).await;
        }
    }
}

#[tokio::test]
async fn selected_activation_confirms_abort_rollback_but_not_already_ended_transaction() {
    for creating in [false, true] {
        for (raise, expected) in [
            ("ABORT", RuntimeActivationCommitState::NotCommitted),
            ("ROLLBACK", RuntimeActivationCommitState::Unknown),
        ] {
            let path = TempStore::new();
            let daemon = path.open().await;
            let store = daemon.compatibility_store();
            selected_activation_fixture(&store, creating).await;
            store.identity_conn().execute_batch(&format!(
                "CREATE TRIGGER fail_selected_liveness BEFORE UPDATE OF stopped_at ON agent_runtimes
                 WHEN OLD.runtime_id='sibling-b' BEGIN SELECT RAISE({raise},'selected liveness failure'); END;"
            )).await.unwrap();
            let before =
                identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
            let epochs = selected_epochs(&store);
            let failure = selected_activation_call(&store, creating, &selected_siblings())
                .await
                .unwrap_err();
            assert_eq!(failure.commit_state(), expected, "{creating} {raise}");
            assert!(failure
                .cause()
                .to_string()
                .contains("selected liveness failure"));
            assert!(failure.confirmed_changes().is_none());
            assert!(store.identity_conn().raw().is_autocommit());
            assert_selected_writer_admitted(&store).await;
            assert_eq!(
                identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
                before
            );
            assert!(selected_event_rows(&store).await.is_empty());
            assert_eq!(selected_epochs(&store), epochs);
        }
    }
}

#[tokio::test]
async fn selected_activation_lifecycle_failure_retains_committed_changes_and_original_cause() {
    for creating in [false, true] {
        for failing_id in ["sibling-a", "sibling-b"] {
            let path = TempStore::new();
            let daemon = path.open().await;
            let store = daemon.compatibility_store();
            selected_activation_fixture(&store, creating).await;
            store
            .conn
            .execute_batch(&format!(
                "CREATE TRIGGER fail_selected_event BEFORE INSERT ON developer_events
                 WHEN NEW.session_id='{failing_id}' BEGIN SELECT RAISE(ABORT,'selected event failure'); END;"
            ))
            .await
            .unwrap();
            let failure = selected_activation_call(&store, creating, &selected_siblings())
                .await
                .unwrap_err();
            assert_eq!(
                failure.commit_state(),
                RuntimeActivationCommitState::Committed
            );
            assert!(failure
                .cause()
                .to_string()
                .contains("selected event failure"));
            let changes = failure.confirmed_changes().expect("committed pair receipt");
            assert_eq!(changes.target_pair(), &("target".into(), "agent".into()));
            assert_eq!(changes.stopped_sibling_pairs(), selected_siblings());
            assert!(
                AgentRuntimes::new(&store)
                    .find_by_runtime_id("target")
                    .await
                    .unwrap()
                    .unwrap()
                    .active
            );
            for id in ["sibling-a", "sibling-b"] {
                assert_eq!(authority(&store, id).await.0, None);
                assert!(
                    !AgentRuntimes::new(&store)
                        .find_by_runtime_id(id)
                        .await
                        .unwrap()
                        .unwrap()
                        .active
                );
            }
            assert_eq!(
                stopped_runtime_ids(&store).await,
                if failing_id == "sibling-a" {
                    vec![]
                } else {
                    vec!["sibling-a"]
                }
            );
            assert_selected_writer_admitted(&store).await;
        }
    }
}

#[tokio::test]
async fn selected_activation_commit_failure_is_unknown_and_drop_releases_writer() {
    for creating in [false, true] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        selected_activation_fixture(&store, creating).await;
        store.identity_conn().execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE activation_commit_parent(id INTEGER PRIMARY KEY);
             CREATE TABLE activation_commit_child(parent_id INTEGER REFERENCES activation_commit_parent(id) DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER fail_selected_commit AFTER UPDATE OF stopped_at ON agent_runtimes
             WHEN OLD.runtime_id='sibling-b' BEGIN INSERT INTO activation_commit_child VALUES(1); END;",
        ).await.unwrap();
        let before =
            identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await;
        let epochs = selected_epochs(&store);
        let failure = selected_activation_call(&store, creating, &selected_siblings())
            .await
            .unwrap_err();
        assert_eq!(
            failure.commit_state(),
            RuntimeActivationCommitState::Unknown
        );
        assert!(failure.cause().to_string().contains("FOREIGN KEY"));
        assert!(failure.confirmed_changes().is_none());
        assert_selected_writer_admitted(&store).await;
        assert!(store.identity_conn().raw().is_autocommit());
        assert_eq!(
            identity_values(&store, "SELECT * FROM agent_runtimes ORDER BY runtime_id").await,
            before
        );
        assert!(
            identity_values(&store, "SELECT * FROM activation_commit_child")
                .await
                .is_empty()
        );
        assert!(selected_event_rows(&store).await.is_empty());
        assert_eq!(selected_epochs(&store), epochs);
    }
}

#[tokio::test]
async fn selected_residue_absence_is_not_a_wildcard_or_a_disappeared_binding() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    let repo = AgentRuntimes::new(&store);
    let epochs = selected_epochs(&store);
    assert_eq!(
        repo.remove_non_agent_residue_selected("runtime", ExpectedRuntimeBinding::Missing)
            .await
            .unwrap(),
        SelectedRuntimeResidueCleanup::AlreadyAbsent
    );
    assert_eq!(
        repo.remove_non_agent_residue_selected("runtime", ExpectedRuntimeBinding::Agent("agent"))
            .await
            .unwrap(),
        SelectedRuntimeResidueCleanup::BindingChanged
    );
    assert_eq!(selected_epochs(&store), epochs);
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
    let epochs = selected_epochs(&store);
    assert_eq!(
        repo.remove_non_agent_residue_selected("runtime", ExpectedRuntimeBinding::Missing)
            .await
            .unwrap(),
        SelectedRuntimeResidueCleanup::BindingChanged
    );
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    assert_eq!(selected_epochs(&store), epochs);
    assert_selected_writer_admitted(&store).await;
}

async fn selected_residue_replacement_at_gate(initially_present: bool) {
    use std::{future::Future, task::Poll};
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    if initially_present {
        exact_stop_fixture(&store, "runtime", "agent", true).await;
    }
    store.identity_conn().execute("INSERT INTO agents(agent_id,project,name,created_at) VALUES ('replacement','p','replacement',1)", ()).await.unwrap();
    let gate = store
        .begin_identity_write_txn("selected_residue_replacement_gate")
        .await
        .unwrap();
    let repo = AgentRuntimes::new(&store);
    let expected = if initially_present {
        ExpectedRuntimeBinding::Agent("agent")
    } else {
        ExpectedRuntimeBinding::Missing
    };
    let mut cleanup = std::pin::pin!(repo.remove_non_agent_residue_selected("runtime", expected));
    let mut follower =
        std::pin::pin!(store.begin_identity_write_txn("selected_residue_fifo_witness"));
    std::future::poll_fn(|cx| {
        assert!(cleanup.as_mut().poll(cx).is_pending());
        assert!(follower.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    if initially_present {
        gate.execute(
            "UPDATE agent_runtimes SET agent_id='replacement' WHERE runtime_id='runtime'",
            (),
        )
        .await
        .unwrap();
    } else {
        gate.execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES ('runtime','replacement','opaque',1,1)", ()).await.unwrap();
    }
    let before = identity_tx_values(&gate, "SELECT * FROM agent_runtimes").await;
    let epochs = selected_epochs(&store);
    gate.commit().await.unwrap();
    std::future::poll_fn(|cx| {
        assert!(
            follower.as_mut().poll(cx).is_pending(),
            "selected residue did not queue at identity writer before replacement"
        );
        Poll::Ready(())
    })
    .await;
    assert_eq!(
        cleanup.await.unwrap(),
        SelectedRuntimeResidueCleanup::BindingChanged
    );
    follower.await.unwrap().commit().await.unwrap();
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    assert_eq!(selected_epochs(&store), epochs);
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

#[tokio::test]
async fn selected_residue_missing_is_rechecked_after_writer_wait() {
    selected_residue_replacement_at_gate(false).await;
}

#[tokio::test]
async fn selected_residue_agent_is_rechecked_after_writer_wait() {
    selected_residue_replacement_at_gate(true).await;
}

#[tokio::test]
async fn selected_residue_foreign_corruption_is_not_decoded() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "foreign", true).await;
    store.identity_conn().execute("UPDATE agent_runtimes SET model_observer_token=X'80',model_report_revision='invalid',model_report_json='{' WHERE runtime_id='runtime'", ()).await.unwrap();
    let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
    let epochs = selected_epochs(&store);
    for expected in [
        ExpectedRuntimeBinding::Missing,
        ExpectedRuntimeBinding::Agent("agent"),
    ] {
        assert_eq!(
            AgentRuntimes::new(&store)
                .remove_non_agent_residue_selected("runtime", expected)
                .await
                .unwrap(),
            SelectedRuntimeResidueCleanup::BindingChanged
        );
    }
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    assert_eq!(selected_epochs(&store), epochs);
}

#[tokio::test]
async fn selected_residue_preserves_every_model_authority_guard() {
    for mutation in [
        "model_report_revision=1",
        "model_observer_token='owner'",
        "model_report_json='{'",
        "model_report_revision='invalid'",
    ] {
        let path = TempStore::new();
        let daemon = path.open().await;
        let store = daemon.compatibility_store();
        exact_stop_fixture(&store, "runtime", "agent", true).await;
        store
            .identity_conn()
            .execute_batch(&format!(
                "UPDATE agent_runtimes SET {mutation} WHERE runtime_id='runtime';"
            ))
            .await
            .unwrap();
        let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
        let epochs = selected_epochs(&store);
        assert!(
            AgentRuntimes::new(&store)
                .remove_non_agent_residue_selected(
                    "runtime",
                    ExpectedRuntimeBinding::Agent("agent")
                )
                .await
                .is_err(),
            "{mutation}"
        );
        assert_selected_writer_admitted(&store).await;
        assert_eq!(
            identity_values(&store, "SELECT * FROM agent_runtimes").await,
            before
        );
        assert_eq!(selected_epochs(&store), epochs);
    }
}

#[tokio::test]
async fn selected_residue_removes_only_unversioned_target_without_stop_or_identity_deletion() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    create(&store, "sibling", false).await;
    let sibling = identity_values(
        &store,
        "SELECT * FROM agent_runtimes WHERE runtime_id='sibling'",
    )
    .await;
    let agents = identity_values(&store, "SELECT * FROM agents").await;
    let events = selected_event_rows(&store).await;
    let epochs = selected_epochs(&store);
    assert_eq!(
        AgentRuntimes::new(&store)
            .remove_non_agent_residue_selected("runtime", ExpectedRuntimeBinding::Agent("agent"))
            .await
            .unwrap(),
        SelectedRuntimeResidueCleanup::Removed
    );
    assert!(identity_values(
        &store,
        "SELECT * FROM agent_runtimes WHERE runtime_id='runtime'"
    )
    .await
    .is_empty());
    assert_eq!(
        identity_values(
            &store,
            "SELECT * FROM agent_runtimes WHERE runtime_id='sibling'"
        )
        .await,
        sibling
    );
    assert_eq!(
        identity_values(&store, "SELECT * FROM agents").await,
        agents
    );
    assert_eq!(selected_event_rows(&store).await, events);
    assert_eq!(selected_epochs(&store), (epochs.0 + 1, epochs.1));
    assert!(retired_ids(&store).await.is_empty());
    assert_selected_writer_admitted(&store).await;
}

async fn selected_residue_rejects_delete_trigger(body: &str) {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    store.identity_conn().execute_batch(body).await.unwrap();
    let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
    let epochs = selected_epochs(&store);
    assert!(
        AgentRuntimes::new(&store)
            .remove_non_agent_residue_selected("runtime", ExpectedRuntimeBinding::Agent("agent"))
            .await
            .is_err(),
        "{body}"
    );
    assert_selected_writer_admitted(&store).await;
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    assert_eq!(selected_epochs(&store), epochs);
    assert!(stopped_runtime_ids(&store).await.is_empty());
}

#[tokio::test]
async fn selected_residue_rejects_ignored_delete() {
    selected_residue_rejects_delete_trigger("CREATE TRIGGER residue_delete BEFORE DELETE ON agent_runtimes BEGIN SELECT RAISE(IGNORE); END;").await;
}

#[tokio::test]
async fn selected_residue_rejects_trigger_reinsertion() {
    selected_residue_rejects_delete_trigger("CREATE TRIGGER residue_delete AFTER DELETE ON agent_runtimes BEGIN INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES (OLD.runtime_id,OLD.agent_id,'reinserted',0,99); END;").await;
}

#[tokio::test]
async fn selected_residue_commit_failure_returns_no_receipt_or_signal() {
    let path = TempStore::new();
    let daemon = path.open().await;
    let store = daemon.compatibility_store();
    exact_stop_fixture(&store, "runtime", "agent", true).await;
    store.identity_conn().execute_batch("PRAGMA foreign_keys=ON;
        CREATE TABLE residue_parent(id INTEGER PRIMARY KEY);
        CREATE TABLE residue_child(parent_id INTEGER REFERENCES residue_parent(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER residue_commit AFTER DELETE ON agent_runtimes BEGIN INSERT INTO residue_child VALUES(1); END;").await.unwrap();
    let before = identity_values(&store, "SELECT * FROM agent_runtimes").await;
    let epochs = selected_epochs(&store);
    let error = AgentRuntimes::new(&store)
        .remove_non_agent_residue_selected("runtime", ExpectedRuntimeBinding::Agent("agent"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("FOREIGN KEY"), "{error}");
    assert_selected_writer_admitted(&store).await;
    assert_eq!(
        identity_values(&store, "SELECT * FROM agent_runtimes").await,
        before
    );
    assert!(identity_values(&store, "SELECT * FROM residue_child")
        .await
        .is_empty());
    assert_eq!(selected_epochs(&store), epochs);
}

#[test]
fn concurrent_temp_store_owners_have_distinct_paths() {
    let barrier = std::sync::Barrier::new(8);
    let owners = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    (0..1024).map(|_| TempStore::new()).collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    let unique: std::collections::HashSet<_> = owners.iter().map(|owner| &owner.0).collect();
    assert_eq!(
        unique.len(),
        owners.len(),
        "live TempStore owners share paths"
    );
}

struct TempStore(PathBuf);
impl TempStore {
    fn new() -> Self {
        // Clock values can repeat between concurrent tests. Keep a distinct
        // process-local identity even when the timestamp is identical.
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |next| next.checked_add(1),
            )
            .expect("TempStore identity exhausted");
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(std::env::temp_dir().join(format!(
            "nexus-model-report-{}-{nonce}-{id}.db",
            std::process::id()
        )))
    }
    async fn open(&self) -> DaemonStore {
        DaemonStore::open(self.0.to_str().unwrap()).await.unwrap()
    }
}
impl Drop for TempStore {
    fn drop(&mut self) {
        for path in [
            self.0.clone(),
            self.0.with_extension("db-wal"),
            self.0.with_extension("db-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}

async fn columns(store: &Store) -> Vec<String> {
    let mut rows = store
        .conn
        .query("PRAGMA table_info(agent_runtimes)", ())
        .await
        .unwrap();
    let mut result = vec![];
    while let Some(row) = rows.next().await.unwrap() {
        result.push(row.get::<String>(1).unwrap());
    }
    result
}
async fn marker(store: &Store) -> String {
    store
        .conn
        .query("SELECT name FROM schema_migrations", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap()
        .get(0)
        .unwrap()
}

#[tokio::test]
async fn fresh_split_store_has_model_authority_columns_and_reopens() {
    let path = TempStore::new();
    for _ in 0..2 {
        let daemon = path.open().await;
        let names = columns(daemon.identity()).await;
        for name in [
            "model_observer_token",
            "model_observer_sequence",
            "model_report_revision",
            "model_report_json",
        ] {
            assert!(names.iter().any(|n| n == name), "missing {name}");
        }
        assert_eq!(
            marker(daemon.identity()).await,
            "v0.1.6_identity_model_report"
        );
        daemon.identity().migrate().await.unwrap();
    }
}

#[tokio::test]
async fn current_marker_upgrade_rolls_back_first_statement_and_retains_data() {
    let path = TempStore::new();
    {
        let daemon = path.open().await;
        // Restore the published pre-model schema without changing its seed SQL semantics.
        for name in [
            "model_observer_token",
            "model_observer_sequence",
            "model_report_revision",
            "model_report_json",
        ] {
            if columns(daemon.identity()).await.iter().any(|n| n == name) {
                daemon
                    .identity()
                    .conn
                    .execute(
                        &format!("ALTER TABLE agent_runtimes DROP COLUMN {name}"),
                        (),
                    )
                    .await
                    .unwrap();
            }
        }
        daemon
            .identity()
            .conn
            .execute(
                "UPDATE schema_migrations SET name = 'v0.1.6_identity_caller_principal'",
                (),
            )
            .await
            .unwrap();
        daemon.identity().conn.execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES ('retained','agent','opaque',1,42)", ()).await.unwrap();
    }
    let error = migrate_identity_with_fault(&path.0, MigrationFault::AfterFirstStatement)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected identity migration fault"),
        "{error}"
    );
    {
        let store = Store::open(path.0.to_str().unwrap()).await.unwrap();
        assert_eq!(marker(&store).await, "v0.1.6_identity_caller_principal");
        assert!(!columns(&store)
            .await
            .iter()
            .any(|n| n == "model_observer_token"));
    }
    let daemon = path.open().await;
    let row = daemon.identity().conn.query("SELECT started_at,model_report_revision,model_observer_sequence,model_report_json FROM agent_runtimes WHERE runtime_id='retained'", ()).await.unwrap().next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 42);
    assert_eq!(row.get::<i64>(1).unwrap(), 0);
    assert_eq!(row.get::<i64>(2).unwrap(), 0);
    assert_eq!(row.get_value(3).unwrap(), libsql::Value::Null);
}

#[tokio::test]
async fn malformed_existing_retired_guard_rolls_back_columns_marker_and_preserves_data() {
    let path = TempStore::new();
    {
        let daemon = path.open().await;
        for column in [
            "model_observer_token",
            "model_observer_sequence",
            "model_report_revision",
            "model_report_json",
        ] {
            daemon
                .identity()
                .conn
                .execute(
                    &format!("ALTER TABLE agent_runtimes DROP COLUMN {column}"),
                    (),
                )
                .await
                .unwrap();
        }
        daemon
            .identity()
            .conn
            .execute_batch(
                "UPDATE schema_migrations SET name = 'v0.1.6_identity_caller_principal';
             ALTER TABLE retired_model_runtime_ids ADD COLUMN metadata TEXT;
             INSERT INTO retired_model_runtime_ids(runtime_id, metadata)
               VALUES ('retired', 'retained guard data');
             INSERT INTO agent_runtimes(runtime_id, agent_id, harness, active, started_at)
               VALUES ('retained', 'agent', 'opaque', 1, 42);",
            )
            .await
            .unwrap();
    }
    let store = Store::open(path.0.to_str().unwrap()).await.unwrap();
    let before_columns = columns(&store).await;
    let error = store.migrate().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("invalid retired model runtime identity authority"));
    drop(store);

    // Inspect a newly opened handle, not just the failing migration's connection.
    let store = Store::open(path.0.to_str().unwrap()).await.unwrap();
    assert_eq!(marker(&store).await, "v0.1.6_identity_caller_principal");
    assert_eq!(columns(&store).await, before_columns);
    let row = store
        .conn
        .query(
            "SELECT active, started_at FROM agent_runtimes WHERE runtime_id = 'retained'",
            (),
        )
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<i64>(1).unwrap(), 42);
    let row = store
        .conn
        .query(
            "SELECT metadata FROM retired_model_runtime_ids WHERE runtime_id = 'retired'",
            (),
        )
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "retained guard data");
}
