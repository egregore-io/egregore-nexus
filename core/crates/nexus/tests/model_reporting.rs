// Compile the actual private coordinator source, not a copied implementation or public test API.
// Native adapter integration remains a separate later checkpoint.
mod boot_readiness {
    include!("../src/daemon/boot_readiness.rs");
}

#[path = "../src/daemon/agent_session_materializer.rs"]
mod agent_session_materializer;
#[path = "../src/daemon/services/presence.rs"]
mod presence;

mod transport_capture_tests {
    use super::presence::{TransportHandle, TransportRegistry};
    use nexus_contracts::SessionId;

    #[test]
    fn captured_old_does_not_remove_same_value_reattachment() {
        for handle in [
            TransportHandle::EventLoop,
            TransportHandle::NativeForwarder("opaque/native".into()),
            TransportHandle::RawStream,
        ] {
            let registry = TransportRegistry::new();
            let session = SessionId("s_reattach".into());
            registry.attach(&session, handle.clone());
            let old = registry.snapshot();

            registry.attach(&session, handle.clone());

            assert!(!registry.detach_captured(&session, &old));
            assert!(registry.is_present(&session), "NEW {handle:?} must survive");
            registry.detach(&session, &handle);
            assert!(!registry.is_present(&session));
        }
    }

    #[test]
    fn captured_unchanged_attachment_is_removed_once() {
        let registry = TransportRegistry::new();
        let session = SessionId("s_unchanged".into());
        registry.attach(&session, TransportHandle::EventLoop);
        let old = registry.snapshot();

        assert!(registry.detach_captured(&session, &old));
        assert!(!registry.is_present(&session));
        assert!(!registry.detach_captured(&session, &old));
    }

    #[test]
    fn captured_mixed_handles_remove_only_unchanged_instances() {
        let registry = TransportRegistry::new();
        let session = SessionId("s_mixed".into());
        let replaced = TransportHandle::NativeForwarder("opaque/native".into());
        registry.attach(&session, TransportHandle::EventLoop);
        registry.attach(&session, replaced.clone());
        let old = registry.snapshot();
        registry.attach(&session, replaced.clone());
        registry.attach(&session, TransportHandle::RawStream);

        assert!(registry.detach_captured(&session, &old));
        assert!(!registry.detach_captured(&session, &old));
        assert!(registry.is_present(&session));
        registry.detach(&session, &replaced);
        assert!(
            registry.is_present(&session),
            "uncaptured RawStream survives"
        );
        registry.detach(&session, &TransportHandle::RawStream);
        assert!(
            !registry.is_present(&session),
            "captured EventLoop was removed"
        );
    }

    #[test]
    fn empty_capture_preserves_later_attachment() {
        let registry = TransportRegistry::new();
        let session = SessionId("s_after_empty".into());
        let empty = registry.snapshot();
        assert!(!registry.detach_captured(&session, &empty));
        registry.attach(&session, TransportHandle::EventLoop);

        assert!(!registry.detach_captured(&session, &empty));
        assert!(registry.is_present(&session));
    }

    #[test]
    fn capture_with_absent_session_preserves_its_later_attachment() {
        let registry = TransportRegistry::new();
        let present = SessionId("s_present".into());
        let absent = SessionId("s_absent".into());
        registry.attach(&present, TransportHandle::EventLoop);
        let old = registry.snapshot();
        registry.attach(&absent, TransportHandle::EventLoop);

        assert!(!registry.detach_captured(&absent, &old));
        assert!(registry.is_present(&absent));
        assert!(registry.detach_captured(&present, &old));
        assert!(!registry.is_present(&present));
        assert!(registry.is_present(&absent));
    }

    #[test]
    fn captured_equal_handles_are_isolated_by_session() {
        let registry = TransportRegistry::new();
        let first = SessionId("s_first".into());
        let second = SessionId("s_second".into());
        let handle = TransportHandle::NativeForwarder("shared/value".into());
        registry.attach(&first, handle.clone());
        registry.attach(&second, handle.clone());
        let old = registry.snapshot();
        registry.attach(&first, handle);

        assert!(!registry.detach_captured(&first, &old));
        assert!(registry.is_present(&second));
        assert!(registry.detach_captured(&second, &old));
        assert!(!registry.is_present(&second));
        assert!(registry.is_present(&first));
    }

    #[test]
    fn cloned_capture_stays_immutable_across_detach_all_and_reattach() {
        let registry = TransportRegistry::new();
        let session = SessionId("s_clone".into());
        registry.attach(&session, TransportHandle::EventLoop);
        let old = registry.snapshot();
        let cloned = old.clone();
        registry.detach_all(&session);
        registry.attach(&session, TransportHandle::EventLoop);
        let fresh = registry.snapshot();
        drop(old);

        assert!(!registry.detach_captured(&session, &cloned));
        assert!(registry.is_present(&session));
        assert!(registry.detach_captured(&session, &fresh));
        assert!(!registry.is_present(&session));
    }

    #[test]
    fn ordinary_detach_operations_keep_value_and_session_semantics() {
        let registry = TransportRegistry::new();
        let session = SessionId("s_ordinary".into());
        let other = SessionId("s_other".into());
        let shared = registry.clone();
        registry.detach(&session, &TransportHandle::EventLoop);
        registry.detach_all(&session);
        registry.attach(&session, TransportHandle::EventLoop);
        registry.attach(&session, TransportHandle::EventLoop);
        registry.attach(&session, TransportHandle::RawStream);
        registry.attach(&other, TransportHandle::EventLoop);

        shared.detach(&session, &TransportHandle::EventLoop);
        assert!(registry.is_present(&session));
        registry.detach(&session, &TransportHandle::RawStream);
        assert!(
            !registry.is_present(&session),
            "equal attaches are not refcounts"
        );
        assert!(registry.is_present(&other));
        registry.attach(&session, TransportHandle::EventLoop);
        registry.attach(&session, TransportHandle::RawStream);
        shared.detach_all(&session);
        assert!(!registry.is_present(&session));
        assert!(registry.is_present(&other));
        shared.detach_all(&other);
        assert!(!registry.is_present(&other));
    }
}

mod daemon {
    pub(crate) use crate::agent_session_materializer;
    pub(crate) use crate::model_reporting;
    pub(crate) mod services {
        pub(crate) use crate::presence;
    }
}

mod model_reporting {
    include!("../src/daemon/model_reporting.rs");

    mod tests {
        use super::*;
        use async_trait::async_trait;
        use nexus_common::NexusError;
        use nexus_contracts::WsEvent;
        use nexus_contracts::{model_report::*, ports::EventSink};
        use nexus_store::{
            repos::{AgentRuntimes, Agents, NewAgent, NewAgentRuntime},
            DaemonStore, Store,
        };
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::{sync::Arc, time::Duration};
        use tokio::sync::Notify;

        struct Events;

        #[tokio::test]
        async fn telemetry_handoff_invalid_clears_only_its_slot_and_exhaustion_closes_origin() {
            use nexus_contracts::telemetry::*;
            let (_dir, _store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = telemetry_reservation(&reporting, true);
            assert!(h.bind_native_root("native/root"));
            let usage = NativeTelemetryUpdate::Usage {
                native_session_id: "native/root".into(),
                value: NativeTelemetryValue::Observed(
                    telemetry_fixture().usage.observation.unwrap(),
                ),
            };
            assert!(h.observe_telemetry(usage.clone()));
            assert!(h.observe_telemetry(NativeTelemetryUpdate::Usage {
                native_session_id: "native/root".into(),
                value: NativeTelemetryValue::Invalid
            }));
            let before = {
                let mut state = h.cell.state.lock().unwrap();
                let report = state.report.telemetry.as_ref().unwrap();
                assert_eq!(report.usage.status, TelemetryAvailability::Invalid);
                assert!(report.usage.observation.is_none());
                assert_eq!(report.context.status, TelemetryAvailability::Unknown);
                assert_eq!(report.quota.status, TelemetryAvailability::Unknown);
                state.sequence = i64::MAX;
                state.report.clone()
            };
            assert!(!h.observe_telemetry(usage.clone()));
            assert!(h.cell.state.lock().unwrap().closed);
            assert_eq!(h.cell.state.lock().unwrap().sequence, i64::MAX);
            assert_eq!(h.cell.state.lock().unwrap().report, before);
            assert!(!h.observe_telemetry(usage));
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        fn telemetry_fixture() -> nexus_contracts::telemetry::RuntimeTelemetryReport {
            let runtime: nexus_contracts::AgentRuntimeSummary = serde_json::from_str(include_str!(
                "../../nexus-contracts/fixtures/runtime.telemetry.json"
            ))
            .unwrap();
            *runtime.model_report.unwrap().telemetry.unwrap()
        }

        #[tokio::test]
        async fn captured_adapter_profile_is_exact_and_closed_owner_cannot_instantiate() {
            use nexus_agent::adapter::{AcpModelMetadataDialect, AdapterModelReportingProfile};
            use nexus_agent::{AdapterRegistry, LaunchCtx, MockAdapter};
            let (_dir, _store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let profile = || {
                AdapterModelReportingProfile::new(
                    ModelReportBackend::new("opaque/adapter").unwrap(),
                    ModelEvidenceCapability::Supported,
                    ModelEvidenceCapability::Unsupported,
                    ModelEvidenceCapability::Unsupported,
                    AcpModelMetadataDialect::ConfigOptions {
                        source: ModelObservationSource::new("fixture/config").unwrap(),
                    },
                )
                .unwrap()
            };
            let selected = profile();
            let h = Arc::new(
                reporting
                    .reserve_adapter("a_observer".into(), "s_observer".into(), &selected)
                    .unwrap(),
            );
            assert!(h.accepts_profile(selected.identity()));
            assert!(!h.accepts_profile(profile().identity()));
            let legacy = reporting
                .reserve(
                    "a_observer".into(),
                    "s_other".into(),
                    selected.backend().clone(),
                    ModelCapabilityProfile {
                        configured: selected.configured(),
                        turn_selected: selected.turn_selected(),
                        response_reported: selected.response_reported(),
                    },
                )
                .unwrap();
            assert!(!legacy.accepts_profile(selected.identity()));
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut registry = AdapterRegistry::new();
            let count = calls.clone();
            let harness = nexus_contracts::HarnessId::new("test").unwrap();
            registry.register_observed(
                &harness,
                Arc::new(move |ctx| {
                    count.fetch_add(1, Ordering::SeqCst);
                    assert!(ctx.model_reporting.is_some());
                    Arc::new(MockAdapter::default())
                }),
                selected.clone(),
            );
            registry
                .select(&harness)
                .unwrap()
                .instantiate_observed(LaunchCtx::default(), h.clone())
                .unwrap();
            let newer = reporting
                .reserve_adapter("a_observer".into(), "s_observer".into(), &selected)
                .unwrap();
            assert!(!h.accepts_profile(selected.identity()));
            assert!(newer.accepts_profile(selected.identity()));
            assert!(registry
                .select(&harness)
                .unwrap()
                .instantiate_observed(LaunchCtx::default(), h)
                .is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            newer.revoke();
            legacy.revoke();
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        fn telemetry_reservation(
            reporting: &ModelReporting,
            quota_supported: bool,
        ) -> ModelObserverHandle {
            use nexus_agent::adapter::{
                AdapterTelemetryCapability, AdapterTelemetryReportingProfile,
            };
            let supported = |source| {
                AdapterTelemetryCapability::new(
                    ModelEvidenceCapability::Supported,
                    Some(ModelObservationSource::new(source).unwrap()),
                )
                .unwrap()
            };
            let profile = AdapterTelemetryReportingProfile::new(
                supported("fixture/usage"),
                supported("fixture/context"),
                if quota_supported {
                    supported("fixture/quota")
                } else {
                    AdapterTelemetryCapability::new(ModelEvidenceCapability::Unsupported, None)
                        .unwrap()
                },
            );
            reporting
                .reserve_with_telemetry(
                    "a_observer".into(),
                    "s_observer".into(),
                    ModelReportBackend::new("fixture/opaque").unwrap(),
                    ModelCapabilityProfile {
                        configured: ModelEvidenceCapability::Supported,
                        turn_selected: ModelEvidenceCapability::Unsupported,
                        response_reported: ModelEvidenceCapability::Supported,
                    },
                    Some(profile),
                )
                .unwrap()
        }

        #[tokio::test]
        async fn telemetry_handoff_stages_privately_merges_and_replaces_cumulative_snapshots() {
            use nexus_contracts::telemetry::*;
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = telemetry_reservation(&reporting, true);
            assert!(h.bind_native_root("native/root"));
            let mut expected = telemetry_fixture();
            let usage = |value| NativeTelemetryUpdate::Usage {
                native_session_id: "native/root".into(),
                value: NativeTelemetryValue::Observed(value),
            };
            assert!(h.observe_telemetry(usage(expected.usage.observation.clone().unwrap())));
            assert!(h.observe_telemetry(NativeTelemetryUpdate::Context {
                native_session_id: "native/root".into(),
                value: NativeTelemetryValue::Observed(
                    expected.context.observation.clone().unwrap()
                )
            }));
            assert!(h.observe_telemetry(NativeTelemetryUpdate::Quota {
                native_session_id: "native/root".into(),
                value: NativeTelemetryValue::Observed(expected.quota.observation.clone().unwrap())
            }));
            assert!(reporting.commit_claim(&h).await.unwrap());
            let staged = row(&store).await.model_report.unwrap();
            assert!(!staged.observer_active);
            assert!(!staged.telemetry.as_ref().unwrap().has_observations());
            assert!(reporting.activate(&h, "native/root"));
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let report = row(&store).await.model_report.unwrap();
                    if report.observer_active && report.telemetry.as_deref() == Some(&expected) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let value = expected.usage.observation.as_mut().unwrap();
            value.input_tokens = Some(TelemetryCounter::new(7).unwrap());
            value.reset_id = Some(TelemetryId::new("usage/epoch2").unwrap());
            assert!(h.observe_telemetry(usage(value.clone())));
            assert!(h.observe_telemetry(usage(value.clone())));
            tokio::time::timeout(Duration::from_secs(2), async {
                while row(&store).await.model_report.unwrap().telemetry.as_deref()
                    != Some(&expected)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(h.observe_telemetry(NativeTelemetryUpdate::Usage {
                native_session_id: "native/root".into(),
                value: NativeTelemetryValue::Unknown
            }));
            let state = h.cell.state.lock().unwrap();
            let current = state.report.telemetry.as_ref().unwrap();
            assert!(current.usage.observation.is_none());
            assert_eq!(current.context, expected.context);
            assert_eq!(current.quota, expected.quota);
            drop(state);
            h.revoke();
            assert!(!h.observe_telemetry(usage(expected.usage.observation.unwrap())));
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn telemetry_handoff_rejects_foreign_source_root_payload_and_unadvertised_capability()
        {
            use nexus_contracts::telemetry::*;
            let (_dir, _store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = telemetry_reservation(&reporting, false);
            assert!(h.bind_native_root("native/root"));
            let fixture = telemetry_fixture();
            let before = h.cell.state.lock().unwrap().snapshot();
            let mut wrong_source = fixture.usage.observation.clone().unwrap();
            wrong_source.metadata.source = ModelObservationSource::new("fixture/context").unwrap();
            let mut wrong_embedded = fixture.usage.observation.clone().unwrap();
            wrong_embedded.metadata.native_session_id = TelemetryId::new("child").unwrap();
            let mut malformed = fixture.context.observation.clone().unwrap();
            malformed.used_tokens.as_mut().unwrap().basis = None;
            for update in [
                NativeTelemetryUpdate::Usage {
                    native_session_id: "child".into(),
                    value: NativeTelemetryValue::Observed(
                        fixture.usage.observation.clone().unwrap(),
                    ),
                },
                NativeTelemetryUpdate::Usage {
                    native_session_id: "native/root".into(),
                    value: NativeTelemetryValue::Observed(wrong_source),
                },
                NativeTelemetryUpdate::Usage {
                    native_session_id: "native/root".into(),
                    value: NativeTelemetryValue::Observed(wrong_embedded),
                },
                NativeTelemetryUpdate::Context {
                    native_session_id: "native/root".into(),
                    value: NativeTelemetryValue::Observed(malformed),
                },
                NativeTelemetryUpdate::Quota {
                    native_session_id: "native/root".into(),
                    value: NativeTelemetryValue::Observed(fixture.quota.observation.unwrap()),
                },
            ] {
                assert!(!h.observe_telemetry(update));
            }
            assert_eq!(h.cell.state.lock().unwrap().sequence, before.sequence);
            assert_eq!(h.cell.state.lock().unwrap().report, before.report);
            let new = reserve(&reporting).unwrap();
            assert!(new.bind_native_root("native/root"));
            let usage = NativeTelemetryUpdate::Usage {
                native_session_id: "native/root".into(),
                value: NativeTelemetryValue::Observed(fixture.usage.observation.unwrap()),
            };
            assert!(!h.observe_telemetry(usage.clone()), "superseded origin");
            assert!(!new.observe_telemetry(usage), "legacy None profile");
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test(flavor = "current_thread")]
        async fn managed_full_purge_tail_veto_append_and_prior_failure_keep_receipt() {
            for mode in ["replacement", "append", "prior"] {
                let (_dir, store, reporting) = fixture().await;
                create_online_session(&store).await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                let selected = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .unwrap();
                let guard = Arc::new(store.lock_presence_transition().await);
                let gate = store
                    .begin_write_txn("full_purge_tail_outcome_gate")
                    .await
                    .unwrap();
                let task = tokio::spawn({
                    let reporting = reporting.clone();
                    let selected = selected.clone();
                    async move {
                        reporting
                            .purge(selected, Some("a_observer".into()), guard)
                            .await
                    }
                });
                tokio::time::timeout(Duration::from_secs(2), async {
                    while !old.cell.state.lock().unwrap().closed {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(!task.is_finished());
                match mode {
                    "replacement" => {
                        gate.execute("UPDATE sessions SET client_key='new-owner' WHERE session_id='s_observer'", ()).await.unwrap();
                    }
                    "append" => {
                        gate.execute("CREATE TRIGGER full_tail_append BEFORE INSERT ON developer_events WHEN NEW.session_id='s_observer' AND NEW.lifecycle='stopped' BEGIN SELECT RAISE(ABORT,'full tail append cause'); END", ()).await.unwrap();
                    }
                    _ => {
                        gate.execute("CREATE TRIGGER full_tail_abort BEFORE DELETE ON sessions BEGIN SELECT RAISE(ABORT,'secondary tail cause'); END", ()).await.unwrap();
                        let mut lane = old.cell.slot.lane.lock().await;
                        lane.uncertain = true;
                        old.cell
                            .slot
                            .retain_captured_failure(Some(&old.cell), "first retained cause");
                    }
                }
                gate.commit().await.unwrap();
                let error = task
                    .await
                    .unwrap()
                    .expect_err("tail outcome must retain identity receipt");
                let ModelPurgeError::AfterStore {
                    receipt: SelectedIdentityPurge::Purged(receipt),
                    cause,
                } = error
                else {
                    panic!("lost identity receipt: {error:?}")
                };
                assert_eq!(receipt.session_id(), &selected.session_id);
                let expected = match mode {
                    "replacement" => "selection changed",
                    "append" => "full tail append cause",
                    _ => "secondary tail cause",
                };
                assert!(cause.to_string().contains(expected), "{mode}: {cause}");
                assert!(AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_observer")
                    .await
                    .unwrap()
                    .is_none());
                let current = Sessions::new(&store)
                    .find_by_session_id(&selected.session_id)
                    .await
                    .unwrap();
                if mode == "append" {
                    assert!(current.is_none(), "postcommit append error is not rollback");
                } else if mode == "replacement" {
                    assert_eq!(current.unwrap().client_key.as_deref(), Some("new-owner"));
                } else {
                    assert_eq!(current, Some(selected));
                }
                let expected_first = if mode == "prior" {
                    "first retained cause"
                } else {
                    expected
                };
                let retained = reporting
                    .inner
                    .registry
                    .lock()
                    .unwrap()
                    .purging_agents
                    .get(&"a_observer".into())
                    .unwrap()
                    .clone();
                let fence_error = retained.error.lock().unwrap().clone().unwrap();
                let slot_error = old.cell.slot.error.lock().unwrap().clone().unwrap();
                assert!(fence_error.contains(expected_first), "{fence_error}");
                assert!(slot_error.contains(expected_first), "{slot_error}");
                assert!(old.cell.slot.lane.lock().await.uncertain);
                assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
                assert!(reserve(&reporting).is_err());
                assert!(reporting
                    .shutdown(Duration::from_secs(2))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains(expected_first));
            }
        }

        #[tokio::test]
        async fn managed_full_purge_identity_veto_never_runs_transport_tail() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            store.conn.execute_batch("UPDATE sessions SET client_key='new-selection' WHERE session_id='s_observer'; CREATE TRIGGER forbid_tail BEFORE DELETE ON sessions BEGIN SELECT RAISE(ABORT,'unexpected tail'); END;").await.unwrap();
            let before = row(&store).await;
            let result = reporting
                .purge(
                    selected,
                    Some("a_observer".into()),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .unwrap();
            assert_eq!(result, SelectedIdentityPurge::SelectionChanged);
            assert_eq!(row(&store).await, before);
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(reporting.commit_claim(&old).await.unwrap());
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .unwrap()
                    .client_key
                    .as_deref(),
                Some("new-selection")
            );
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_full_purge_removes_transport_with_actual_identity_receipt() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let result = reporting
                .purge(
                    selected.clone(),
                    Some("a_observer".into()),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .unwrap();
            let SelectedIdentityPurge::Purged(receipt) = result else {
                panic!("expected receipt")
            };
            assert_eq!(receipt.session_id(), &selected.session_id);
            assert_eq!(receipt.agent_id(), Some("a_observer"));
            assert!(
                Sessions::new(&store)
                    .find_by_session_id(&selected.session_id)
                    .await
                    .unwrap()
                    .is_none(),
                "full purge returned with transport Session intact"
            );
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(!reporting.commit_claim(&old).await.unwrap());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_full_purge_transport_error_retains_committed_identity_receipt() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            store.conn.execute_batch("CREATE TRIGGER full_purge_fail BEFORE DELETE ON sessions BEGIN SELECT RAISE(ABORT,'full purge transport trigger'); END;").await.unwrap();
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let error = reporting
                .purge(
                    selected.clone(),
                    Some("a_observer".into()),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .expect_err("tail failure must propagate");
            let ModelPurgeError::AfterStore {
                receipt: SelectedIdentityPurge::Purged(receipt),
                cause,
            } = error
            else {
                panic!("identity receipt lost: {error:?}")
            };
            assert_eq!(receipt.agent_id(), Some("a_observer"));
            assert!(cause.to_string().contains("full purge transport trigger"));
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("s_observer")
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&selected.session_id)
                    .await
                    .unwrap(),
                Some(selected)
            );
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(reserve(&reporting).is_err());
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("full purge transport trigger"));
        }

        #[tokio::test(flavor = "current_thread")]
        async fn managed_full_purge_cancelled_waiter_retains_guard_and_fence_through_transport() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let guard = Arc::new(store.lock_presence_transition().await);
            let transport = store
                .begin_write_txn("full_purge_transport_gate")
                .await
                .unwrap();
            let task = tokio::spawn({
                let reporting = reporting.clone();
                async move {
                    reporting
                        .purge(selected, Some("a_observer".into()), guard)
                        .await
                }
            });
            tokio::time::timeout(Duration::from_secs(2), async {
                while !old.cell.state.lock().unwrap().closed {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // Closure follows the actual identity receipt; on this current-thread executor the
            // task cannot be observed mid-synchronous settlement. Transport is still gated.
            assert!(
                !task.is_finished(),
                "full purge settled before gated transport phase"
            );
            assert!(
                reserve(&reporting).is_err(),
                "agent fence released before transport"
            );
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            let mut contender = Box::pin(store.lock_presence_transition());
            assert!(futures::poll!(&mut contender).is_pending());
            transport.commit().await.unwrap();
            let guard = tokio::time::timeout(Duration::from_secs(2), contender)
                .await
                .unwrap();
            assert!(Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .is_none());
            drop(guard);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        fn non_agent_resume_request() -> nexus_contracts::register::RegisterRequest {
            nexus_contracts::register::RegisterRequest {
                agent_id: None,
                name: Some("observer".into()),
                harness: nexus_contracts::HarnessId::new("other").unwrap(),
                harness_session_id: "human-native".into(),
                project: "default".into(),
                client_key: "ck_observer".into(),
                runtime_credential: None,
                tier: nexus_contracts::Tier::Agent,
                kind: Some(nexus_contracts::Kind::Human),
                locality: Default::default(),
                access: None,
                role: None,
                cwd: None,
            }
        }

        #[tokio::test]
        async fn managed_identity_purge_closes_reserved_origin_without_transport_deletion() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let guard = Arc::new(store.lock_presence_transition().await);
            let result = reporting
                .purge_identity(selected.clone(), Some("a_observer".into()), guard)
                .await
                .unwrap();
            assert!(matches!(
                result,
                nexus_store::repos::sessions::SelectedIdentityPurge::Purged(_)
            ));
            assert!(
                old.cell.state.lock().unwrap().closed,
                "purge left captured reservation eligible"
            );
            assert!(!reporting.commit_claim(&old).await.unwrap());
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("s_observer")
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&selected.session_id)
                    .await
                    .unwrap(),
                Some(selected)
            );
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_identity_purge_rejects_closed_admission_before_store_effects() {
            for stopped in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                create_online_session(&store).await;
                if stopped {
                    reporting.initialize().await.unwrap();
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                }
                let before = row(&store).await;
                let selected = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .unwrap();
                let guard = Arc::new(store.lock_presence_transition().await);
                assert!(
                    reporting
                        .purge_identity(selected.clone(), Some("a_observer".into()), guard)
                        .await
                        .is_err(),
                    "purge bypassed closed admission"
                );
                assert_eq!(row(&store).await, before);
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&selected.session_id)
                        .await
                        .unwrap(),
                    Some(selected)
                );
            }
        }

        #[tokio::test]
        async fn managed_identity_purge_fences_late_reservations_and_owns_cancelled_settlement() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let absent = reserve_pair(&reporting, "s_absent", "a_observer");
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let guard = Arc::new(store.lock_presence_transition().await);
            let gate = store
                .begin_identity_write_txn("purge_cancel_gate")
                .await
                .unwrap();
            let task = tokio::spawn({
                let reporting = reporting.clone();
                async move {
                    reporting
                        .purge_identity(selected, Some("a_observer".into()), guard)
                        .await
                }
            });
            // Actual coordinator lane ownership while its identity write is held out.
            parked_claim(&old).await;
            let before = reporting.inner.registry.lock().unwrap().slots.len();
            assert!(
                reporting
                    .reserve(
                        "a_observer".into(),
                        "s_late".into(),
                        ModelReportBackend::new("fixture/opaque").unwrap(),
                        ModelCapabilityProfile {
                            configured: ModelEvidenceCapability::Supported,
                            turn_selected: ModelEvidenceCapability::Unsupported,
                            response_reported: ModelEvidenceCapability::Supported,
                        }
                    )
                    .is_err(),
                "purge admitted a new runtime of its whole-agent target"
            );
            assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), before);
            let unrelated = reserve_pair(&reporting, "s_other", "a_other");
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            let mut contender = Box::pin(store.lock_presence_transition());
            assert!(
                futures::poll!(&mut contender).is_pending(),
                "cancel released owned purge guard"
            );
            gate.commit().await.unwrap();
            let guard = tokio::time::timeout(Duration::from_secs(2), contender)
                .await
                .unwrap();
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(absent.cell.state.lock().unwrap().closed);
            assert!(!unrelated.cell.state.lock().unwrap().closed);
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("s_observer")
                .await
                .unwrap()
                .is_none());
            let fresh = reserve_pair(&reporting, "s_after", "a_observer");
            assert!(!fresh.cell.state.lock().unwrap().closed);
            drop(guard);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_identity_purge_foreign_batch_rejection_is_side_effect_free() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            reporting.initialize().await.unwrap();
            let foreign = reserve_pair(&reporting, "s_observer", "a_foreign");
            let absent = reserve_pair(&reporting, "s_absent", "a_observer");
            store.identity_conn().execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES('s_aaa_empty','a_observer','other',0,1)", ()).await.unwrap();
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let before = row(&store).await;
            let slots = reporting.inner.registry.lock().unwrap().slots.len();
            let result = reporting
                .purge_identity(
                    selected,
                    Some("a_observer".into()),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await;
            assert!(
                matches!(result, Err(ModelPurgeError::BeforeStore { .. })),
                "foreign local participant did not veto purge"
            );
            assert_eq!(row(&store).await, before);
            assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), slots);
            for cell in [&foreign.cell, &absent.cell] {
                assert!(!cell.state.lock().unwrap().closed);
                assert!(!*cell.slot.offline_pending.lock().unwrap());
                assert!(cell.slot.error.lock().unwrap().is_none());
            }
            let fresh = reserve_pair(&reporting, "s_after", "a_observer");
            assert!(!fresh.cell.state.lock().unwrap().closed);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_identity_purge_preserves_actual_rollback_and_unknown_outcomes() {
            use nexus_store::repos::sessions::IdentityPurgeCommitState;
            for unknown in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                create_online_session(&store).await;
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                assert!(reporting.commit_claim(&old).await.unwrap());
                let before = row(&store).await;
                let selected = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .unwrap();
                store.identity_conn().execute_batch(if unknown {
                    "PRAGMA foreign_keys=ON; CREATE TABLE purge_parent(id INTEGER PRIMARY KEY); CREATE TABLE purge_child(id INTEGER REFERENCES purge_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER purge_failure AFTER DELETE ON agent_runtimes BEGIN INSERT INTO purge_child VALUES(1); END;"
                } else {
                    "CREATE TRIGGER purge_failure BEFORE DELETE ON agent_runtimes BEGIN SELECT RAISE(ABORT,'original purge abort'); END;"
                }).await.unwrap();
                let error = reporting
                    .purge_identity(
                        selected.clone(),
                        Some("a_observer".into()),
                        Arc::new(store.lock_presence_transition().await),
                    )
                    .await
                    .unwrap_err();
                let ModelPurgeError::Store {
                    failure,
                    settlement_error,
                } = error
                else {
                    panic!("lost store failure: {error:?}");
                };
                assert_eq!(
                    failure.commit_state(),
                    if unknown {
                        IdentityPurgeCommitState::Unknown
                    } else {
                        IdentityPurgeCommitState::NotCommitted
                    }
                );
                let cause = failure.cause().to_string();
                assert!(cause.contains(if unknown {
                    "FOREIGN KEY"
                } else {
                    "original purge abort"
                }));
                assert!(settlement_error.is_none());
                assert_eq!(row(&store).await, before);
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&selected.session_id)
                        .await
                        .unwrap(),
                    Some(selected)
                );
                let closed = old.cell.state.lock().unwrap().closed;
                assert_eq!(closed, unknown);
                let lane = old.cell.slot.lane.lock().await;
                assert_eq!(lane.confirmed.as_deref(), Some(old.cell.key.token.as_str()));
                assert_eq!(lane.uncertain, unknown);
                drop(lane);
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                let registry = reporting.inner.registry.lock().unwrap();
                assert_eq!(
                    registry.purging_agents.contains_key(&"a_observer".into()),
                    unknown
                );
                if unknown {
                    assert_eq!(
                        registry.purging_agents[&"a_observer".into()]
                            .error
                            .lock()
                            .unwrap()
                            .as_deref(),
                        Some(cause.as_str())
                    );
                    assert_eq!(
                        old.cell.slot.error.lock().unwrap().as_deref(),
                        Some(cause.as_str())
                    );
                }
                drop(registry);
                store
                    .identity_conn()
                    .execute_batch("DROP TRIGGER purge_failure;")
                    .await
                    .unwrap();
                if unknown {
                    assert!(reserve(&reporting).is_err());
                    assert!(reporting
                        .shutdown(Duration::from_secs(2))
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains(&cause));
                    // Fence failure must independently reach shutdown even without a slot entry.
                    reporting.inner.registry.lock().unwrap().slots.clear();
                    assert!(reporting
                        .shutdown(Duration::from_secs(2))
                        .await
                        .unwrap_err()
                        .to_string()
                        .contains(&cause));
                } else {
                    assert!(old.cell.slot.error.lock().unwrap().is_none());
                    let fresh = reserve_pair(&reporting, "s_fresh", "a_observer");
                    assert!(!fresh.cell.state.lock().unwrap().closed);
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                }
            }
        }

        #[tokio::test]
        async fn managed_identity_purge_revalidates_session_and_full_runtime_set_after_wait() {
            for change in ["session", "runtime"] {
                let (_dir, store, reporting) = fixture().await;
                create_online_session(&store).await;
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                assert!(reporting.commit_claim(&old).await.unwrap());
                let selected = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .unwrap();
                let lane = old.cell.slot.lane.lock().await;
                let mut call = Box::pin(reporting.purge_identity(
                    selected,
                    Some("a_observer".into()),
                    Arc::new(store.lock_presence_transition().await),
                ));
                activation_admitted(&mut call, &old.cell.slot).await;
                // Admission captured the original durable pairs; the operation is now queued
                // at its runtime lane, so the following mutation is not a pre-selection change.
                if change == "session" {
                    store.conn.execute("UPDATE sessions SET client_key='replacement' WHERE session_id='s_observer'", ()).await.unwrap();
                } else {
                    store.identity_conn().execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES('s_new_sibling','a_observer','other',0,1)", ()).await.unwrap();
                }
                let before = row(&store).await;
                drop(lane);
                assert!(matches!(
                    call.await.unwrap(),
                    SelectedIdentityPurge::SelectionChanged
                ));
                assert_eq!(row(&store).await, before);
                assert!(!old.cell.state.lock().unwrap().closed);
                assert!(old.cell.slot.error.lock().unwrap().is_none());
                let lane = old.cell.slot.lane.lock().await;
                assert!(!lane.uncertain);
                assert_eq!(lane.confirmed.as_deref(), Some(old.cell.key.token.as_str()));
                drop(lane);
                assert!(reporting
                    .inner
                    .registry
                    .lock()
                    .unwrap()
                    .purging_agents
                    .is_empty());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn managed_identity_purge_claim_and_apply_respect_both_actual_lane_orders() {
            for apply in [false, true] {
                for owner_first in [false, true] {
                    let (_dir, store, reporting) = fixture().await;
                    create_online_session(&store).await;
                    reporting.initialize().await.unwrap();
                    let old = reserve(&reporting).unwrap();
                    if apply {
                        assert!(reporting.commit_claim(&old).await.unwrap());
                    }
                    let mut snapshot = old.cell.state.lock().unwrap().snapshot();
                    snapshot.sequence += 1;
                    let selected = Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap()
                        .unwrap();
                    let gate = store
                        .begin_identity_write_txn("purge_owner_order")
                        .await
                        .unwrap();
                    let mut owner = Box::pin(async {
                        if apply {
                            reporting
                                .inner
                                .apply_snapshot(&old.cell, &snapshot)
                                .await
                                .map(|value| value == Some(true))
                        } else {
                            reporting.commit_claim(&old).await
                        }
                    });
                    if owner_first {
                        assert!(futures::poll!(&mut owner).is_pending());
                        parked_claim(&old).await;
                    }
                    let mut purge = Box::pin(reporting.purge_identity(
                        selected,
                        Some("a_observer".into()),
                        Arc::new(store.lock_presence_transition().await),
                    ));
                    activation_admitted(&mut purge, &old.cell.slot).await;
                    if !owner_first {
                        parked_claim(&old).await;
                        assert!(futures::poll!(&mut owner).is_pending());
                    }
                    gate.commit().await.unwrap();
                    assert_eq!(owner.await.unwrap(), owner_first);
                    assert!(matches!(
                        purge.await.unwrap(),
                        SelectedIdentityPurge::Purged(_)
                    ));
                    assert!(old.cell.state.lock().unwrap().closed);
                    assert!(AgentRuntimes::new(&store)
                        .find_by_runtime_id("s_observer")
                        .await
                        .unwrap()
                        .is_none());
                    let lane = old.cell.slot.lane.lock().await;
                    assert!(lane.confirmed.is_none());
                    assert!(!lane.uncertain);
                    drop(lane);
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                }
            }
        }

        #[tokio::test]
        async fn managed_identity_purge_captures_stopped_siblings_without_fencing_foreign_root_agent(
        ) {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            store.identity_conn().execute_batch("INSERT INTO agents(agent_id,project,name,created_at) VALUES('a_other','default','other',1);
                UPDATE agent_runtimes SET agent_id='a_other' WHERE runtime_id='s_observer';
                INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES('s_stopped','a_observer','other',0,1,2);").await.unwrap();
            reporting.initialize().await.unwrap();
            let root = reserve_pair(&reporting, "s_observer", "a_other");
            let stopped = reserve_pair(&reporting, "s_stopped", "a_observer");
            for owner in [&root, &stopped] {
                assert!(reporting.commit_claim(owner).await.unwrap());
            }
            let absent = reserve_pair(&reporting, "s_absent", "a_observer");
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let guard = Arc::new(store.lock_presence_transition().await);
            let gate = store
                .begin_identity_write_txn("purge_overlap_gate")
                .await
                .unwrap();
            let mut call = Box::pin(reporting.purge_identity(
                selected.clone(),
                Some("a_observer".into()),
                guard.clone(),
            ));
            activation_admitted(&mut call, &root.cell.slot).await;
            parked_claim(&root).await;
            let other = reserve_pair(&reporting, "s_other", "a_other");
            let slot_count = reporting.inner.registry.lock().unwrap().slots.len();
            let error = reporting
                .purge_identity(selected.clone(), Some("a_observer".into()), guard.clone())
                .await
                .unwrap_err();
            assert!(matches!(error, ModelPurgeError::BeforeStore { .. }));
            assert_eq!(
                reporting.inner.registry.lock().unwrap().slots.len(),
                slot_count
            );
            assert!(*root.cell.slot.offline_pending.lock().unwrap());
            assert!(reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .purging_agents
                .contains_key(&"a_observer".into()));
            // Same exact root also rejects an overlapping purge with no whole-agent target.
            assert!(matches!(
                reporting
                    .purge_identity(selected.clone(), None, guard.clone())
                    .await,
                Err(ModelPurgeError::BeforeStore { .. })
            ));
            gate.commit().await.unwrap();
            let SelectedIdentityPurge::Purged(receipt) = call.await.unwrap() else {
                panic!("purge skipped");
            };
            assert_eq!(
                receipt.runtime_pairs(),
                &[
                    ("s_observer".into(), "a_other".into()),
                    ("s_stopped".into(), "a_observer".into())
                ]
            );
            for owner in [&root, &stopped, &absent] {
                assert!(owner.cell.state.lock().unwrap().closed);
                let lane = owner.cell.slot.lane.lock().await;
                assert!(lane.confirmed.is_none());
                assert!(!lane.uncertain);
            }
            assert!(!other.cell.state.lock().unwrap().closed);
            assert!(Agents::new(&store)
                .find_by_id("a_other")
                .await
                .unwrap()
                .is_some());
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&selected.session_id)
                    .await
                    .unwrap(),
                Some(selected)
            );
            assert!(reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .purging_agents
                .is_empty());
            drop(guard);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_identity_purge_missing_root_requires_positive_local_authority() {
            for authority in [None, Some("a_other"), Some("a_observer")] {
                for local in [false, true] {
                    let (_dir, store, reporting) = fixture().await;
                    create_online_session(&store).await;
                    store
                        .identity_conn()
                        .execute("DELETE FROM agent_runtimes", ())
                        .await
                        .unwrap();
                    reporting.initialize().await.unwrap();
                    let old = local.then(|| reserve_pair(&reporting, "s_observer", "a_observer"));
                    let selected = Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap()
                        .unwrap();
                    let result = reporting
                        .purge_identity(
                            selected.clone(),
                            authority.map(str::to_owned),
                            Arc::new(store.lock_presence_transition().await),
                        )
                        .await;
                    if local && authority != Some("a_observer") {
                        assert!(matches!(result, Err(ModelPurgeError::BeforeStore { .. })));
                        assert!(!old.as_ref().unwrap().cell.state.lock().unwrap().closed);
                    } else {
                        let SelectedIdentityPurge::Purged(receipt) = result.unwrap() else {
                            panic!("missing root unexpectedly skipped");
                        };
                        assert!(receipt.runtime_pairs().is_empty());
                        assert_eq!(receipt.agent_id(), authority);
                        if let Some(old) = &old {
                            assert!(old.cell.state.lock().unwrap().closed);
                        }
                    }
                    assert_eq!(
                        Sessions::new(&store)
                            .find_by_session_id(&selected.session_id)
                            .await
                            .unwrap(),
                        Some(selected)
                    );
                    assert!(reporting
                        .inner
                        .registry
                        .lock()
                        .unwrap()
                        .purging_agents
                        .is_empty());
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                }
            }
        }

        #[tokio::test]
        async fn managed_identity_purge_rechecks_failure_after_lane_wait_and_retains_fence() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let selected = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let before = row(&store).await;
            let mut lane = old.cell.slot.lane.lock().await;
            let mut call = Box::pin(reporting.purge_identity(
                selected,
                Some("a_observer".into()),
                Arc::new(store.lock_presence_transition().await),
            ));
            activation_admitted(&mut call, &old.cell.slot).await;
            lane.uncertain = true;
            old.cell
                .slot
                .retain_captured_failure(Some(&old.cell), "original prior lane failure");
            drop(lane);
            let error = call.await.unwrap_err();
            let ModelPurgeError::BeforeStore { cause } = error else {
                panic!("unexpected phase");
            };
            assert!(cause.to_string().contains("original prior lane failure"));
            assert_eq!(row(&store).await, before);
            assert!(old.cell.slot.lane.lock().await.uncertain);
            assert!(reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .purging_agents
                .contains_key(&"a_observer".into()));
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("original prior lane failure"));
        }

        async fn non_agent_session(store: &Store, stamp: Option<&str>) {
            create_online_session(store).await;
            store.conn.execute(
                "UPDATE sessions SET kind='local.human',agent_id=?1 WHERE session_id='s_observer'",
                libsql::params![stamp],
            ).await.unwrap();
        }

        #[tokio::test]
        async fn managed_residue_resume_closes_only_captured_unclaimed_origin() {
            use nexus_contracts::ports::IdentityPort;
            let (_dir, store, reporting) = fixture().await;
            non_agent_session(&store, Some("a_observer")).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let events = Arc::new(RecordingEvents::default());
            let identity = managed_identity(store.clone(), reporting.clone(), events.clone());
            let response = identity.register(non_agent_resume_request()).await.unwrap();
            assert_eq!(response.session_id.0, "s_observer");
            assert_eq!(response.agent_id, None);
            assert!(
                old.cell.state.lock().unwrap().closed,
                "resume deleted runtime without closing its captured unclaimed origin"
            );
            assert!(!reporting.commit_claim(&old).await.unwrap());
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("s_observer")
                .await
                .unwrap()
                .is_none());
            assert!(Agents::new(&store)
                .find_by_id("a_observer")
                .await
                .unwrap()
                .is_some());
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .unwrap()
                    .agent_id,
                None
            );
            assert!(events.0.lock().unwrap().is_empty());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_residue_closed_admission_leaves_session_and_runtime_unchanged() {
            use nexus_contracts::ports::IdentityPort;
            for shutdown in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                non_agent_session(&store, Some("a_observer")).await;
                let reporting = Arc::new(reporting);
                if shutdown {
                    reporting.initialize().await.unwrap();
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                }
                let before = row(&store).await;
                let session = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
                assert!(
                    identity.register(non_agent_resume_request()).await.is_err(),
                    "non-agent resume bypassed closed model admission"
                );
                assert_eq!(row(&store).await, before);
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    session
                );
                assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            }
        }

        #[tokio::test]
        async fn managed_residue_binding_authority_uses_runtime_or_positive_missing_stamp() {
            use nexus_contracts::ports::IdentityPort;
            for (present, stamp, with_cell, allowed) in [
                (true, None, true, true),
                (true, Some("stale-agent"), true, true),
                (false, Some("a_observer"), true, true),
                (false, None, true, false),
                (false, Some("foreign"), true, false),
                (false, None, false, true),
            ] {
                let (_dir, store, reporting) = fixture().await;
                non_agent_session(&store, stamp).await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                if !present {
                    AgentRuntimes::new(&store)
                        .remove_non_agent_residue("s_observer")
                        .await
                        .unwrap();
                }
                let old = with_cell.then(|| reserve(&reporting).unwrap());
                let before = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
                let result = identity.register(non_agent_resume_request()).await;
                assert_eq!(
                    result.is_ok(),
                    allowed,
                    "present={present} stamp={stamp:?} cell={with_cell}: {result:?}"
                );
                if let Some(old) = &old {
                    assert_eq!(old.cell.state.lock().unwrap().closed, allowed);
                    assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                    assert!(old.cell.slot.error.lock().unwrap().is_none());
                }
                if !allowed {
                    assert_eq!(
                        Sessions::new(&store)
                            .find_by_session_id(&"s_observer".into())
                            .await
                            .unwrap(),
                        before
                    );
                }
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn managed_residue_rejects_foreign_committed_activated_and_failed_cells() {
            use nexus_contracts::ports::IdentityPort;
            for mode in [
                "foreign",
                "committed",
                "cleared",
                "activated",
                "failed",
                "uncertain",
            ] {
                let (_dir, store, reporting) = fixture().await;
                non_agent_session(&store, Some("a_observer")).await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                let old = if mode == "foreign" {
                    reporting
                        .reserve(
                            "a_foreign".into(),
                            "s_observer".into(),
                            ModelReportBackend::new("fixture/opaque").unwrap(),
                            ModelCapabilityProfile {
                                configured: ModelEvidenceCapability::Supported,
                                turn_selected: ModelEvidenceCapability::Unsupported,
                                response_reported: ModelEvidenceCapability::Supported,
                            },
                        )
                        .unwrap()
                } else {
                    reserve(&reporting).unwrap()
                };
                match mode {
                    "committed" | "cleared" => {
                        assert!(reporting.commit_claim(&old).await.unwrap());
                        if mode == "cleared" {
                            assert!(reporting.revoke_committed(&old).await.unwrap());
                            assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
                            // Disposable fixture removes durable history to isolate the independent
                            // committed-cell guard, not to model a permitted production deletion.
                            store
                                .identity_conn()
                                .execute(
                                    "DELETE FROM agent_runtimes WHERE runtime_id='s_observer'",
                                    (),
                                )
                                .await
                                .unwrap();
                        }
                    }
                    "activated" => old.cell.state.lock().unwrap().activated = true,
                    "failed" => {
                        *old.cell.slot.error.lock().unwrap() =
                            Some("retained residue failure".into())
                    }
                    "uncertain" => old.cell.slot.lane.lock().await.uncertain = true,
                    _ => {}
                }
                let session = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                let runtime = AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_observer")
                    .await
                    .unwrap();
                let closed = old.cell.state.lock().unwrap().closed;
                let error = old.cell.slot.error.lock().unwrap().clone();
                let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
                assert!(
                    identity.register(non_agent_resume_request()).await.is_err(),
                    "{mode}"
                );
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    session,
                    "{mode}"
                );
                assert_eq!(
                    AgentRuntimes::new(&store)
                        .find_by_runtime_id("s_observer")
                        .await
                        .unwrap(),
                    runtime,
                    "{mode}"
                );
                assert_eq!(old.cell.state.lock().unwrap().closed, closed, "{mode}");
                assert_eq!(*old.cell.slot.error.lock().unwrap(), error, "{mode}");
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                if mode == "failed" {
                    *old.cell.slot.error.lock().unwrap() = None;
                }
                if mode == "uncertain" {
                    old.cell.slot.lane.lock().await.uncertain = false;
                }
                if mode == "activated" {
                    old.cell.state.lock().unwrap().activated = false;
                }
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        async fn residue_pending(old: &ModelObserverHandle) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if *old.cell.slot.offline_pending.lock().unwrap() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("actual residue operation did not reach tracked admission");
        }

        #[tokio::test]
        async fn managed_residue_rechecks_session_proof_after_lane_wait_and_skips_rebind() {
            use nexus_contracts::ports::IdentityPort;
            for mutation in [
                "UPDATE sessions SET client_key='replacement-key' WHERE session_id='s_observer'",
                "UPDATE sessions SET kind='local.app' WHERE session_id='s_observer'",
                "UPDATE sessions SET kind='external.human' WHERE session_id='s_observer'",
                "UPDATE sessions SET agent_id='replacement-agent' WHERE session_id='s_observer'",
                "DELETE FROM sessions WHERE session_id='s_observer'",
            ] {
                let (_dir, store, reporting) = fixture().await;
                non_agent_session(&store, Some("a_observer")).await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                let lane = old.cell.slot.lane.lock().await;
                let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
                let resume =
                    tokio::spawn(
                        async move { identity.register(non_agent_resume_request()).await },
                    );
                residue_pending(&old).await;
                store.conn.execute_batch(mutation).await.unwrap();
                let session = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                let runtime = row(&store).await;
                drop(lane);
                let error = resume.await.unwrap().unwrap_err();
                assert!(
                    format!("{error:?}").contains("selection changed"),
                    "{error:?}"
                );
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    session
                );
                assert_eq!(row(&store).await, runtime);
                assert!(!old.cell.state.lock().unwrap().closed);
                assert!(!old.cell.slot.lane.lock().await.uncertain);
                assert!(old.cell.slot.error.lock().unwrap().is_none());
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                assert!(old.bind_native_root(" root/opaque "));
                assert!(old.observe(update(ModelEvidenceField::Configured, "still usable")));
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn managed_residue_runtime_replacement_preserves_new_row_and_resume_stamp() {
            use nexus_contracts::ports::IdentityPort;
            for initially_present in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                non_agent_session(&store, Some("a_observer")).await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                if !initially_present {
                    AgentRuntimes::new(&store)
                        .remove_non_agent_residue("s_observer")
                        .await
                        .unwrap();
                }
                let old = reserve(&reporting).unwrap();
                let lane = old.cell.slot.lane.lock().await;
                let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
                let resume =
                    tokio::spawn(
                        async move { identity.register(non_agent_resume_request()).await },
                    );
                residue_pending(&old).await;
                store.identity_conn().execute("INSERT INTO agents(agent_id,project,created_at) VALUES ('replacement','default',1)", ()).await.unwrap();
                if initially_present {
                    store.identity_conn().execute("UPDATE agent_runtimes SET agent_id='replacement' WHERE runtime_id='s_observer'", ()).await.unwrap();
                } else {
                    store.identity_conn().execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES ('s_observer','replacement','other',1,1)", ()).await.unwrap();
                }
                let before = row(&store).await;
                let session = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                drop(lane);
                assert!(resume.await.unwrap().is_err());
                assert_eq!(row(&store).await, before);
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    session
                );
                assert!(!old.cell.state.lock().unwrap().closed);
                assert!(old.cell.slot.error.lock().unwrap().is_none());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn managed_residue_and_actual_claim_respect_both_lane_orderings() {
            use nexus_contracts::ports::IdentityPort;
            for claim_first in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                non_agent_session(&store, Some("a_observer")).await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                let gate = store
                    .begin_identity_write_txn("residue_claim_order_gate")
                    .await
                    .unwrap();
                let mut claim = Box::pin(reporting.commit_claim(&old));
                if claim_first {
                    assert!(futures::poll!(&mut claim).is_pending());
                    parked_claim(&old).await;
                }
                let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
                let resume =
                    tokio::spawn(
                        async move { identity.register(non_agent_resume_request()).await },
                    );
                residue_pending(&old).await;
                if !claim_first {
                    // Only residue has requested the lane so far. Witness its ownership before
                    // polling the actual claim; no claim/cleanup body is copied into the test.
                    parked_claim(&old).await;
                    assert!(futures::poll!(&mut claim).is_pending());
                }
                gate.commit().await.unwrap();
                assert_eq!(claim.await.unwrap(), claim_first);
                assert_eq!(resume.await.unwrap().is_err(), claim_first);
                let session = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .unwrap();
                if claim_first {
                    assert_eq!(session.agent_id.as_deref(), Some("a_observer"));
                    assert!(!old.cell.state.lock().unwrap().closed);
                    assert_eq!(
                        row(&store).await.model_observer_token.as_deref(),
                        Some(old.cell.key.token.as_str())
                    );
                    assert!(old.cell.slot.error.lock().unwrap().is_none());
                } else {
                    assert!(old.cell.state.lock().unwrap().closed);
                    assert!(session.agent_id.is_none());
                    assert!(AgentRuntimes::new(&store)
                        .find_by_runtime_id("s_observer")
                        .await
                        .unwrap()
                        .is_none());
                }
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn managed_residue_cancellation_before_admission_and_after_submission() {
            use nexus_contracts::ports::IdentityPort;
            for admitted in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                non_agent_session(&store, Some("a_observer")).await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                let before = row(&store).await;
                let session = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
                if !admitted {
                    let presence = store.lock_presence_transition().await;
                    let mut resume = Box::pin(identity.register(non_agent_resume_request()));
                    assert!(futures::poll!(&mut resume).is_pending());
                    drop(resume);
                    drop(presence);
                    assert_eq!(row(&store).await, before);
                    assert!(!old.cell.state.lock().unwrap().closed);
                } else {
                    let gate = store
                        .begin_identity_write_txn("residue_cancel_submitted_gate")
                        .await
                        .unwrap();
                    let resume =
                        tokio::spawn(
                            async move { identity.register(non_agent_resume_request()).await },
                        );
                    residue_pending(&old).await;
                    parked_claim(&old).await;
                    resume.abort();
                    assert!(resume.await.unwrap_err().is_cancelled());
                    let mut presence = Box::pin(store.lock_presence_transition());
                    assert!(
                        futures::poll!(&mut presence).is_pending(),
                        "canceled receiver released owned residue guard"
                    );
                    assert!(*old.cell.slot.offline_pending.lock().unwrap());
                    gate.commit().await.unwrap();
                    let guard = tokio::time::timeout(Duration::from_secs(2), presence)
                        .await
                        .unwrap();
                    assert!(old.cell.state.lock().unwrap().closed);
                    assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                    assert!(AgentRuntimes::new(&store)
                        .find_by_runtime_id("s_observer")
                        .await
                        .unwrap()
                        .is_none());
                    drop(guard);
                }
                // Cancellation after tracked cleanup may leave the Session's old stamp: this
                // is settled runtime deletion, not whole-register rollback or completion.
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    session
                );
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            }
        }

        #[tokio::test]
        async fn managed_residue_store_error_retains_captured_failure_and_suppresses_rebind() {
            use nexus_contracts::ports::IdentityPort;
            for commit_error in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                non_agent_session(&store, Some("a_observer")).await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                store.identity_conn().execute_batch(if commit_error {
                    "PRAGMA foreign_keys=ON; CREATE TABLE residue_p(id INTEGER PRIMARY KEY); CREATE TABLE residue_c(id INTEGER REFERENCES residue_p(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER residue_fail AFTER DELETE ON agent_runtimes BEGIN INSERT INTO residue_c VALUES(1); END;"
                } else {
                    "CREATE TRIGGER residue_fail BEFORE DELETE ON agent_runtimes BEGIN SELECT RAISE(ABORT,'initiating residue failure'); END;"
                }).await.unwrap();
                let before = row(&store).await;
                let session = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
                let error = identity
                    .register(non_agent_resume_request())
                    .await
                    .unwrap_err();
                let cause = if commit_error {
                    "FOREIGN KEY"
                } else {
                    "initiating residue failure"
                };
                assert!(format!("{error:?}").contains(cause), "{error:?}");
                let following = store
                    .begin_identity_write_txn("residue_failed_following_writer")
                    .await
                    .unwrap();
                following.commit().await.unwrap();
                assert_eq!(row(&store).await, before);
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    session
                );
                assert!(old.cell.state.lock().unwrap().closed);
                assert!(old.cell.slot.lane.lock().await.uncertain);
                assert!(old
                    .cell
                    .slot
                    .error
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .contains(cause));
                assert!(old
                    .cell
                    .slot
                    .captured_failure_closure
                    .load(Ordering::SeqCst));
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                assert!(reporting
                    .shutdown(Duration::from_secs(2))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains(cause));
            }
        }

        #[tokio::test]
        async fn managed_residue_runtime_lookup_error_is_not_missing() {
            use nexus_contracts::ports::IdentityPort;
            let (_dir, store, reporting) = fixture().await;
            non_agent_session(&store, Some("a_observer")).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            store.identity_conn().execute("UPDATE agent_runtimes SET model_report_revision='invalid' WHERE runtime_id='s_observer'", ()).await.unwrap();
            let session = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap();
            let identity = managed_identity(store.clone(), reporting.clone(), Arc::new(Events));
            assert!(identity.register(non_agent_resume_request()).await.is_err());
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap(),
                session
            );
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            assert!(old.cell.slot.error.lock().unwrap().is_none());
            store.identity_conn().execute("UPDATE agent_runtimes SET model_report_revision=0 WHERE runtime_id='s_observer'", ()).await.unwrap();
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        fn managed_identity(
            store: Arc<Store>,
            reporting: Arc<ModelReporting>,
            events: Arc<dyn EventSink>,
        ) -> nexus_identity::Identity {
            nexus_identity::Identity::new_with_runtime_activation(
                store,
                events,
                &nexus_common::Config::default(),
                reporting,
            )
        }

        #[tokio::test]
        async fn identity_offline_admission_precedes_prepare() {
            use nexus_contracts::ports::IdentityPort;
            for closed in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                create_online_session(&store).await;
                let reporting = Arc::new(reporting);
                if closed {
                    reporting.initialize().await.unwrap();
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                }
                let before = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                let runtime = row(&store).await;
                let identity = managed_identity(
                    store.clone(),
                    reporting.clone(),
                    Arc::new(RecordingEvents::default()),
                );
                assert!(
                    identity.set_offline(&"s_observer".into()).await.is_err(),
                    "Identity offline must reject before preparation when admission is closed"
                );
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    before
                );
                assert_eq!(row(&store).await, runtime);
                assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            }
        }

        #[derive(Default)]
        struct RecordingEvents(Mutex<Vec<WsEvent>>);
        #[async_trait]
        impl EventSink for RecordingEvents {
            async fn emit(&self, event: WsEvent) {
                self.0.lock().unwrap().push(event);
            }
        }

        #[tokio::test]
        async fn identity_offline_closes_reserved_and_parked_claim_or_apply() {
            use nexus_contracts::ports::IdentityPort;
            for phase in ["reserved", "claim", "apply"] {
                let (_dir, store, reporting) = fixture().await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                assert!(old.bind_native_root(" root/opaque "));
                assert!(old.observe(update(ModelEvidenceField::Configured, "OLD")));
                if phase == "apply" {
                    assert!(reporting.commit_claim(&old).await.unwrap());
                }
                let gate = store
                    .begin_identity_write_txn("identity_offline_old_gate")
                    .await
                    .unwrap();
                let mut claim = Box::pin(reporting.commit_claim(&old));
                if phase == "claim" {
                    assert!(futures::poll!(&mut claim).is_pending());
                    parked_claim(&old).await;
                } else if phase == "apply" {
                    assert!(reporting.activate(&old, " root/opaque "));
                    parked_claim(&old).await;
                }
                let identity = managed_identity(
                    store.clone(),
                    reporting.clone(),
                    Arc::new(RecordingEvents::default()),
                );
                let session = "s_observer".into();
                let mut offline = Box::pin(identity.set_offline(&session));
                assert!(futures::poll!(&mut offline).is_pending());
                // Synchronous admission witness, not a claim based only on Poll::Pending.
                assert!(
                    old.cell.state.lock().unwrap().closed,
                    "Identity must close OLD before any preparation or stop completes: {phase}"
                );
                assert!(*old.cell.slot.offline_pending.lock().unwrap());
                assert!(!reporting.activate(&old, " root/opaque "));
                assert!(!old.observe(update(ModelEvidenceField::Configured, "late OLD")));
                gate.commit().await.unwrap();
                offline.await.unwrap();
                if phase != "apply" {
                    assert!(!claim.await.unwrap());
                }
                wait_completed(&old).await.unwrap();
                assert!(!row(&store).await.active);
                assert!(row(&store).await.model_observer_token.is_none());
                assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
                let fresh = reserve(&reporting).unwrap();
                assert!(reporting.commit_claim(&fresh).await.unwrap());
                let confirmed = fresh.cell.slot.lane.lock().await.confirmed.clone();
                assert!(!reporting.revoke_committed(&old).await.unwrap());
                assert_eq!(fresh.cell.slot.lane.lock().await.confirmed, confirmed);
                assert_eq!(
                    row(&store).await.model_observer_token.as_deref(),
                    Some(fresh.cell.key.token.as_str())
                );
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn identity_offline_cancelled_before_admission_has_zero_effects() {
            use nexus_contracts::ports::IdentityPort;
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let before = row(&store).await;
            let session_before = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap();
            let events = Arc::new(RecordingEvents::default());
            let identity = managed_identity(store.clone(), reporting.clone(), events.clone());
            let guard = store.lock_presence_transition().await;
            let session = "s_observer".into();
            let mut offline = Box::pin(identity.set_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            drop(offline);
            drop(guard);
            // Acquiring the same mutex is a deterministic barrier after cancelling its waiter.
            drop(store.lock_presence_transition().await);
            assert!(!old.cell.state.lock().unwrap().closed);
            assert_eq!(row(&store).await, before);
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&session)
                    .await
                    .unwrap(),
                session_before
            );
            assert!(events.0.lock().unwrap().is_empty());
            assert!(reporting.commit_claim(&old).await.unwrap());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn identity_offline_cancelled_stop_and_status_retain_same_guard_and_before_pause() {
            use nexus_contracts::ports::IdentityPort;
            for paused in [false, true] {
                let (_dir, store, _) = fixture().await;
                create_online_session(&store).await;
                store
                    .conn
                    .execute(
                        "UPDATE sessions SET paused=?1 WHERE session_id='s_observer'",
                        [i64::from(paused)],
                    )
                    .await
                    .unwrap();
                let owner_events = Arc::new(GatedEvents::default());
                let reporting = Arc::new(ModelReporting::new(store.clone(), owner_events.clone()));
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                owner_events.armed.store(true, Ordering::SeqCst);
                let mut claim = Box::pin(reporting.commit_claim(&old));
                assert!(futures::poll!(&mut claim).is_pending());
                owner_events.entered.notified().await;
                let confirmed = old.cell.slot.lane.lock().await.confirmed.clone();
                assert!(confirmed.is_some());
                let gate = store
                    .begin_identity_write_txn("identity_offline_stop_gate")
                    .await
                    .unwrap();
                let status = Arc::new(OfflineStatusGate::default());
                let identity = managed_identity(store.clone(), reporting.clone(), status.clone());
                let session = "s_observer".into();
                let mut offline = Box::pin(identity.set_offline(&session));
                assert!(futures::poll!(&mut offline).is_pending());
                parked_claim(&old).await;
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&session)
                        .await
                        .unwrap()
                        .unwrap()
                        .presence
                        .as_deref(),
                    Some("offline")
                );
                assert!(
                    row(&store).await.active,
                    "actual stop is parked at the identity transaction gate"
                );
                drop(offline);
                let mut contender = Box::pin(store.lock_presence_transition());
                assert!(
                    futures::poll!(&mut contender).is_pending(),
                    "cancelled Identity caller must retain SAME-Store guard through stop"
                );
                // Mutate only compatibility pause after prepare, while actual stop is gated.
                store
                    .conn
                    .execute(
                        "UPDATE sessions SET paused=?1 WHERE session_id='s_observer'",
                        [i64::from(!paused)],
                    )
                    .await
                    .unwrap();
                gate.commit().await.unwrap();
                tokio::time::timeout(Duration::from_secs(2), status.entered.notified())
                    .await
                    .unwrap();
                assert!(!row(&store).await.active);
                assert!(old.cell.slot.lane.try_lock().unwrap().confirmed.is_none());
                assert!(
                    futures::poll!(&mut contender).is_pending(),
                    "same guard must also survive the separately gated status phase"
                );
                assert!(reserve(&reporting).is_err());
                assert_eq!(
                    status.recorded.lock().unwrap().as_slice(),
                    &[WsEvent::AgentStatus {
                        session_id: session.clone(),
                        presence: nexus_contracts::Presence::Offline,
                        paused,
                    }],
                    "Identity status must use BEFORE paused, including eligible Some(false)"
                );
                status.release.notify_one();
                drop(
                    tokio::time::timeout(Duration::from_secs(2), contender)
                        .await
                        .unwrap(),
                );
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                owner_events.release.notify_one();
                assert!(!claim.await.unwrap());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            }
        }

        #[tokio::test]
        async fn identity_offline_no_event_controls_still_stop() {
            use nexus_contracts::ports::IdentityPort;
            for mode in ["missing", "nonagent", "already_offline"] {
                for managed in [false, true] {
                    let (_dir, store, reporting) = fixture().await;
                    if mode != "missing" {
                        create_online_session(&store).await;
                        let sql = if mode == "nonagent" {
                            "UPDATE sessions SET kind='human' WHERE session_id='s_observer'"
                        } else {
                            "UPDATE sessions SET presence='offline' WHERE session_id='s_observer'"
                        };
                        store.conn.execute(sql, ()).await.unwrap();
                    }
                    let reporting = Arc::new(reporting);
                    reporting.initialize().await.unwrap();
                    let events = Arc::new(RecordingEvents::default());
                    let identity = if managed {
                        managed_identity(store.clone(), reporting.clone(), events.clone())
                    } else {
                        nexus_identity::Identity::new(
                            store.clone(),
                            events.clone(),
                            &nexus_common::Config::default(),
                        )
                    };
                    identity.set_offline(&"s_observer".into()).await.unwrap();
                    assert!(!row(&store).await.active, "{mode}, managed={managed}");
                    assert!(
                        events.0.lock().unwrap().is_empty(),
                        "{mode}, managed={managed}"
                    );
                    if let Some(row) = Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap()
                    {
                        assert_eq!(row.presence.as_deref(), Some("offline"));
                    }
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                }
            }
        }

        #[tokio::test]
        async fn identity_offline_prepare_and_stop_errors_preserve_cause_and_confirmed_cache() {
            use nexus_contracts::ports::IdentityPort;
            for phase in ["prepare", "append", "stop"] {
                let (_dir, store, _) = fixture().await;
                create_online_session(&store).await;
                let owner_events = Arc::new(GatedEvents::default());
                let reporting = Arc::new(ModelReporting::new(store.clone(), owner_events.clone()));
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                owner_events.armed.store(true, Ordering::SeqCst);
                let mut claim = Box::pin(reporting.commit_claim(&old));
                assert!(futures::poll!(&mut claim).is_pending());
                owner_events.entered.notified().await;
                let confirmed = old.cell.slot.lane.lock().await.confirmed.clone();
                assert!(confirmed.is_some());
                match phase {
                    "prepare" => store.conn.execute_batch("CREATE TRIGGER identity_fail BEFORE UPDATE OF presence ON sessions BEGIN SELECT RAISE(ABORT, 'original Identity prepare cause'); END;").await.unwrap(),
                    "append" => store.conn.execute_batch("CREATE TRIGGER identity_fail BEFORE INSERT ON developer_events WHEN NEW.lifecycle='offline' BEGIN SELECT RAISE(ABORT, 'original Identity append cause'); END;").await.unwrap(),
                    _ => store.identity_conn().execute_batch("CREATE TRIGGER identity_fail BEFORE UPDATE OF active ON agent_runtimes WHEN NEW.active=0 BEGIN SELECT RAISE(ABORT, 'original Identity stop cause'); END;").await.unwrap(),
                }
                let events = Arc::new(RecordingEvents::default());
                let identity = managed_identity(store.clone(), reporting.clone(), events.clone());
                let error = identity
                    .set_offline(&"s_observer".into())
                    .await
                    .unwrap_err();
                let cause = format!("original Identity {phase} cause");
                assert!(error.message.contains(&cause), "{error:?}");
                assert!(old.cell.state.lock().unwrap().closed);
                assert_eq!(
                    old.cell.slot.lane.lock().await.confirmed,
                    confirmed,
                    "unconfirmed stop must not clear cache"
                );
                assert!(row(&store).await.active);
                assert!(events.0.lock().unwrap().is_empty());
                assert!(reserve(&reporting).is_err());
                assert!(identity.set_offline(&"s_observer".into()).await.is_err());
                owner_events.release.notify_one();
                assert!(claim.await.is_err());
                let error = reporting
                    .shutdown(Duration::from_secs(2))
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains(&cause), "{error}");
                assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            }
        }

        #[tokio::test]
        async fn identity_offline_status_panic_is_tracked_after_caller_cancellation() {
            use nexus_contracts::ports::IdentityPort;
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let events = Arc::new(OfflineStatusGate::default());
            events.panic.store(true, Ordering::SeqCst);
            let identity = managed_identity(store.clone(), reporting.clone(), events.clone());
            let session = "s_observer".into();
            let mut offline = Box::pin(identity.set_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            tokio::time::timeout(Duration::from_secs(2), events.entered.notified())
                .await
                .unwrap();
            drop(offline);
            events.release.notify_one();
            // Same-Store guard acquisition witnesses completion, including caught status unwind.
            drop(
                tokio::time::timeout(Duration::from_secs(2), store.lock_presence_transition())
                    .await
                    .unwrap(),
            );
            assert!(!row(&store).await.active);
            assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
            assert!(reserve(&reporting).is_err());
            assert!(*old.cell.slot.offline_pending.lock().unwrap());
            assert!(identity.set_offline(&session).await.is_err());
            let error = reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected offline publication panic"),
                "{error}"
            );
            assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
        }

        #[tokio::test]
        async fn identity_offline_preserves_open_turn_while_presence_aborts_it() {
            use nexus_contracts::ports::IdentityPort;
            use nexus_store::repos::AgentSessionMessages;
            for managed in [false, true] {
                let store = Arc::new(Store::open(":memory:").await.unwrap());
                store.migrate().await.unwrap();
                assert!(!store.has_split_authority());
                create_online_session(&store).await;
                let session = "s_observer".into();
                let turn = AgentSessionMessages::new(&store)
                    .begin_or_get_open_turn(&session, 1, 10)
                    .await
                    .unwrap();
                let events = Arc::new(RecordingEvents::default());
                let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
                reporting.initialize().await.unwrap();
                let identity = if managed {
                    managed_identity(store.clone(), reporting.clone(), events.clone())
                } else {
                    nexus_identity::Identity::new(
                        store.clone(),
                        events.clone(),
                        &nexus_common::Config::default(),
                    )
                };
                identity.set_offline(&session).await.unwrap();
                let read_status = || async {
                    let mut rows = store
                        .conn
                        .query(
                            "SELECT status FROM agent_session_turns WHERE id=?1",
                            [turn.id.clone()],
                        )
                        .await
                        .unwrap();
                    rows.next()
                        .await
                        .unwrap()
                        .unwrap()
                        .get::<String>(0)
                        .unwrap()
                };
                assert_eq!(
                    read_status().await,
                    "streaming",
                    "Identity does not inherit Presence's open-turn abort policy"
                );
                offline_writer(store.clone(), reporting.clone())
                    .materialize_offline(&session)
                    .await
                    .unwrap();
                assert_eq!(read_status().await, "aborted", "actual Presence remains a distinct positive control even for already-offline rows");
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn identity_offline_confirmed_stop_clears_only_captured_runtime_cache() {
            use nexus_contracts::ports::IdentityPort;
            let (_dir, store, reporting) = fixture().await;
            AgentRuntimes::new(&store)
                .create(NewAgentRuntime {
                    runtime_id: "s_unrelated".into(),
                    agent_id: "a_observer".into(),
                    harness: "other".into(),
                    cwd: None,
                    transport: None,
                    presence: Some("offline".into()),
                    active: false,
                })
                .await
                .unwrap();
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let other = reporting
                .reserve(
                    "a_observer".into(),
                    "s_unrelated".into(),
                    ModelReportBackend::new("fixture/opaque").unwrap(),
                    ModelCapabilityProfile {
                        configured: ModelEvidenceCapability::Supported,
                        turn_selected: ModelEvidenceCapability::Unsupported,
                        response_reported: ModelEvidenceCapability::Supported,
                    },
                )
                .unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            assert!(reporting.commit_claim(&other).await.unwrap());
            assert!(old.cell.slot.lane.lock().await.confirmed.is_some());
            let other_cache = other.cell.slot.lane.lock().await.confirmed.clone();
            assert!(other_cache.is_some());
            let other_row = AgentRuntimes::new(&store)
                .find_by_runtime_id("s_unrelated")
                .await
                .unwrap();
            let identity = managed_identity(
                store.clone(),
                reporting.clone(),
                Arc::new(RecordingEvents::default()),
            );
            identity.set_offline(&"s_observer".into()).await.unwrap();
            assert!(!row(&store).await.active);
            assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
            assert_eq!(other.cell.slot.lane.lock().await.confirmed, other_cache);
            assert_eq!(
                AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_unrelated")
                    .await
                    .unwrap(),
                other_row
            );
            assert!(!other.cell.state.lock().unwrap().closed);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn identity_offline_retains_stop_cause_before_unlocking_lane() {
            use nexus_contracts::ports::IdentityPort;
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            events.armed.store(true, Ordering::SeqCst);
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            store.identity_conn().execute_batch("CREATE TRIGGER identity_fail BEFORE UPDATE OF active ON agent_runtimes WHEN NEW.active=0 BEGIN SELECT RAISE(ABORT, 'lane-owned Identity stop cause'); END;").await.unwrap();
            let gate = store
                .begin_identity_write_txn("identity_stop_failure_gate")
                .await
                .unwrap();
            let identity = managed_identity(
                store.clone(),
                reporting.clone(),
                Arc::new(RecordingEvents::default()),
            );
            let session = "s_observer".into();
            let mut offline = Box::pin(identity.set_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            parked_claim(&old).await;
            let inner = reporting.inner.clone();
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = std::thread::spawn(move || {
                let _registry = inner.registry.lock().unwrap();
                entered_tx.send(()).unwrap();
                let _ = release_rx.recv_timeout(Duration::from_secs(5));
            });
            entered_rx.await.unwrap();
            let mut lane_barrier = Box::pin(old.cell.slot.lane.lock());
            assert!(futures::poll!(&mut lane_barrier).is_pending());
            gate.commit().await.unwrap();
            let lane = tokio::time::timeout(Duration::from_secs(2), lane_barrier)
                .await
                .unwrap();
            let retained = old.cell.slot.error.lock().unwrap().clone();
            let uncertain = lane.uncertain;
            drop(lane);
            release_tx.send(()).unwrap();
            blocker.join().unwrap();
            events.release.notify_one();
            assert!(uncertain);
            assert!(retained.as_deref().is_some_and(|cause| cause.contains("lane-owned Identity stop cause")), "originating stop error must exist before lane release, independently of task completion: {retained:?}");
            assert!(offline.await.is_err());
            assert!(claim.await.is_err());
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("lane-owned Identity stop cause"));
        }
        #[async_trait]
        impl EventSink for Events {
            async fn emit(&self, _: WsEvent) {
                panic!("model reporting must not emit compatibility events")
            }
        }

        async fn retention_row(store: &Store, id: &str, agent: &str) {
            store.identity_conn().execute(
                "INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES(?1,?2,'other',0,1,2)",
                libsql::params![id, agent],
            ).await.unwrap();
        }

        async fn retention_pending(reporting: &ModelReporting, id: &str) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if reporting
                        .inner
                        .registry
                        .lock()
                        .unwrap()
                        .slots
                        .get(&SessionId(id.into()))
                        .is_some_and(|slot| *slot.offline_pending.lock().unwrap())
                    {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("retention installed captured pending slot");
        }

        #[tokio::test]
        async fn managed_retention_preserves_every_existing_slot_and_agent_fence() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            store
                .identity_conn()
                .execute(
                    "UPDATE agent_runtimes SET active=0,stopped_at=2 WHERE runtime_id='s_observer'",
                    (),
                )
                .await
                .unwrap();
            let old = reserve(&reporting).unwrap();
            let mut retained = Vec::new();
            for kind in [
                "empty",
                "closed",
                "cached",
                "uncertain",
                "failed",
                "pending",
            ] {
                let id = format!("retention_{kind}");
                retention_row(&store, &id, "a_observer").await;
                let slot = Arc::new(RuntimeSlot::default());
                match kind {
                    "cached" => slot.lane.lock().await.confirmed = Some("captured-token".into()),
                    "uncertain" => slot.lane.lock().await.uncertain = true,
                    "failed" => *slot.error.lock().unwrap() = Some("prior failure".into()),
                    "pending" => *slot.offline_pending.lock().unwrap() = true,
                    _ => (),
                }
                reporting
                    .inner
                    .registry
                    .lock()
                    .unwrap()
                    .slots
                    .insert(id.into(), slot.clone());
                retained.push(slot);
            }
            let closed = reserve_pair(&reporting, "retention_closed", "a_observer");
            closed.cell.close();
            retention_row(&store, "retention_fenced", "a_fenced").await;
            reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .purging_agents
                .insert("a_fenced".into(), Arc::new(PurgeFence::default()));
            retention_row(&store, "retention_free", "a_observer").await;
            let before = reporting.inner.registry.lock().unwrap().slots.len();
            assert_eq!(reporting.reap_retention(10).await.unwrap(), 1);
            assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), before);
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("retention_fenced")
                .await
                .unwrap()
                .is_some());
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(closed.cell.state.lock().unwrap().closed);
            assert!(
                reporting.commit_claim(&old).await.unwrap(),
                "unclaimed OLD remains usable"
            );
            for kind in [
                "empty",
                "closed",
                "cached",
                "uncertain",
                "failed",
                "pending",
            ] {
                assert!(AgentRuntimes::new(&store)
                    .find_by_runtime_id(&format!("retention_{kind}"))
                    .await
                    .unwrap()
                    .is_some());
            }
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("prior failure"));
        }

        #[tokio::test]
        async fn managed_retention_rejects_unready_and_shutdown_without_slots_or_delete() {
            let (_dir, store, reporting) = fixture().await;
            retention_row(&store, "retention_free", "a_observer").await;
            assert!(reporting.reap_retention(10).await.is_err());
            reporting.initialize().await.unwrap();
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            assert!(reporting.reap_retention(10).await.is_err());
            assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("retention_free")
                .await
                .unwrap()
                .is_some());
        }

        #[tokio::test(flavor = "current_thread")]
        async fn managed_retention_cancelled_waiter_keeps_guard_and_exact_selection() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            retention_row(&store, "retention_free", "a_observer").await;
            let held = store
                .begin_identity_write_txn("retention_test_gate")
                .await
                .unwrap();
            let reporting = Arc::new(reporting);
            let task = tokio::spawn({
                let reporting = reporting.clone();
                async move { reporting.reap_retention(10).await }
            });
            retention_pending(&reporting, "retention_free").await;
            assert!(reporting
                .reserve(
                    "a_observer".into(),
                    "retention_free".into(),
                    ModelReportBackend::new("fixture/opaque").unwrap(),
                    ModelCapabilityProfile {
                        configured: ModelEvidenceCapability::Supported,
                        turn_selected: ModelEvidenceCapability::Unsupported,
                        response_reported: ModelEvidenceCapability::Supported,
                    }
                )
                .is_err());
            let unrelated = reserve_pair(&reporting, "retention_unrelated", "a_observer");
            assert!(!unrelated.cell.state.lock().unwrap().closed);
            retention_row(&store, "retention_later", "a_observer").await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            let mut contender = Box::pin(store.lock_presence_transition());
            assert!(
                futures::poll!(&mut contender).is_pending(),
                "owned settlement retains presence"
            );
            held.commit().await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), contender)
                .await
                .unwrap();
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("retention_free")
                .await
                .unwrap()
                .is_none());
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("retention_later")
                .await
                .unwrap()
                .is_some());
            assert!(!reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .slots
                .contains_key(&SessionId("retention_free".into())));
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_retention_store_error_keeps_failed_empty_lane_and_shutdown_cause() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            retention_row(&store, "retention_free", "a_observer").await;
            store.identity_conn().execute_batch("CREATE TRIGGER retention_fail BEFORE DELETE ON agent_runtimes BEGIN SELECT RAISE(ABORT,'original retention failure'); END;").await.unwrap();
            assert!(reporting
                .reap_retention(10)
                .await
                .unwrap_err()
                .to_string()
                .contains("original retention failure"));
            let slot = reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .slots
                .get(&SessionId("retention_free".into()))
                .expect("failed slot retained")
                .clone();
            assert!(slot.lane.lock().await.uncertain);
            assert!(slot.lane.lock().await.confirmed.is_none());
            assert!(*slot.offline_pending.lock().unwrap());
            assert!(slot
                .error
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .contains("original retention failure"));
            assert_eq!(
                reporting.reap_retention(10).await.unwrap(),
                0,
                "failed slot skipped, never retried"
            );
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("retention_free")
                .await
                .unwrap()
                .is_some());
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("original retention failure"));
        }

        #[tokio::test]
        async fn managed_retention_reclaims_unowned_slots_without_later_reserve() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            for batch in 0..3 {
                for id in 0..16 {
                    retention_row(&store, &format!("retention_{batch}_{id}"), "a_observer").await;
                }
                assert_eq!(reporting.reap_retention(10).await.unwrap(), 16);
                assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
                assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            }
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn managed_retention_presence_dispatch_never_falls_back_after_skip_or_rejection() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            let legacy = PresenceWriter::new(
                store.clone(),
                Arc::new(Events),
                crate::presence::TransportRegistry::new(),
            );
            let managed = legacy.clone().with_model_reporting(reporting.clone());
            retention_row(&store, "retention_free", "a_observer").await;
            assert!(managed.reap_runtime_retention(10).await.is_err());
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("retention_free")
                .await
                .unwrap()
                .is_some());
            reporting.initialize().await.unwrap();
            store
                .identity_conn()
                .execute(
                    "UPDATE agent_runtimes SET active=0,stopped_at=2 WHERE runtime_id='s_observer'",
                    (),
                )
                .await
                .unwrap();
            let old = reserve(&reporting).unwrap();
            assert_eq!(managed.reap_runtime_retention(10).await.unwrap(), 1);
            assert_eq!(managed.reap_runtime_retention(10).await.unwrap(), 0);
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("s_observer")
                .await
                .unwrap()
                .is_some());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            assert!(managed.reap_runtime_retention(10).await.is_err());
            assert_eq!(
                legacy.reap_runtime_retention(10).await.unwrap(),
                1,
                "None retains standalone SQL policy"
            );
        }

        #[tokio::test(flavor = "current_thread")]
        async fn managed_retention_revalidated_skip_releases_empty_slot_without_closing_later_owner(
        ) {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            retention_row(&store, "retention_free", "a_observer").await;
            let held = store
                .begin_identity_write_txn("retention_rebind_gate")
                .await
                .unwrap();
            let reporting = Arc::new(reporting);
            let task = tokio::spawn({
                let reporting = reporting.clone();
                async move { reporting.reap_retention(10).await }
            });
            retention_pending(&reporting, "retention_free").await;
            let slot = reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .slots
                .get(&SessionId("retention_free".into()))
                .unwrap()
                .clone();
            assert!(
                slot.lane.try_lock().is_err(),
                "selected task owns lane before identity wait"
            );
            store.identity_conn().execute("UPDATE agent_runtimes SET agent_id='replacement',active=1,stopped_at=NULL WHERE runtime_id='retention_free'", ()).await.unwrap();
            held.commit().await.unwrap();
            assert_eq!(task.await.unwrap().unwrap(), 0);
            assert!(!slot.lane.lock().await.uncertain);
            assert!(!*slot.offline_pending.lock().unwrap());
            assert!(slot.error.lock().unwrap().is_none());
            drop(slot);
            let next = reserve_pair(&reporting, "retention_free", "replacement");
            assert!(!next.cell.state.lock().unwrap().closed);
            let row = AgentRuntimes::new(&store)
                .find_by_runtime_id("retention_free")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.agent_id, "replacement");
            assert!(row.active);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test(flavor = "current_thread")]
        async fn managed_retention_cancel_before_presence_has_no_admission() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            retention_row(&store, "retention_free", "a_observer").await;
            let held = store.lock_presence_transition().await;
            let mut operation = Box::pin(reporting.reap_retention(10));
            assert!(futures::poll!(&mut operation).is_pending());
            drop(operation);
            assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
            assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            drop(held);
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("retention_free")
                .await
                .unwrap()
                .is_some());
            assert_eq!(reporting.reap_retention(10).await.unwrap(), 1);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        async fn fixture() -> (tempfile::TempDir, Arc<Store>, ModelReporting) {
            let dir = tempfile::tempdir().unwrap();
            let daemon = DaemonStore::open(dir.path().join("identity.db").to_str().unwrap())
                .await
                .unwrap();
            let store = Arc::new(daemon.compatibility_store());
            Agents::new(&store)
                .create(NewAgent {
                    agent_id: "a_observer".into(),
                    project: "default".into(),
                    name: None,
                    default_harness: None,
                    role: None,
                    tier: None,
                    owner: None,
                })
                .await
                .unwrap();
            AgentRuntimes::new(&store)
                .create(NewAgentRuntime {
                    runtime_id: "s_observer".into(),
                    agent_id: "a_observer".into(),
                    harness: "other".into(),
                    cwd: None,
                    transport: None,
                    presence: Some("online".into()),
                    active: true,
                })
                .await
                .unwrap();
            let reporting = ModelReporting::new(store.clone(), Arc::new(Events));
            (dir, store, reporting)
        }

        fn reserve(reporting: &ModelReporting) -> Result<ModelObserverHandle, NexusError> {
            reporting.reserve(
                "a_observer".into(),
                "s_observer".into(),
                ModelReportBackend::new("fixture/opaque").unwrap(),
                ModelCapabilityProfile {
                    configured: ModelEvidenceCapability::Supported,
                    turn_selected: ModelEvidenceCapability::Unsupported,
                    response_reported: ModelEvidenceCapability::Supported,
                },
            )
        }

        async fn staged_registration(
            store: &Store,
            id: &str,
            agent: Option<&str>,
        ) -> CapturedStagedSession {
            use nexus_store::repos::{sessions::SelectedStagedSessionStamp, NewSession};
            let receipt = Sessions::new(store)
                .create_staged_registration_captured(
                    NewSession {
                        session_id: id.into(),
                        name: None,
                        agent: Some("other".into()),
                        kind: "agent".into(),
                        role: None,
                        tier: "agent".into(),
                        harness_session_id: None,
                        client_key: Some(format!("ck_{id}")),
                        cwd: None,
                        project: "default".into(),
                        transport: None,
                    },
                    None,
                )
                .await
                .unwrap();
            match agent {
                None => receipt,
                Some(agent) => match Sessions::new(store)
                    .set_agent_id_selected(&receipt, agent)
                    .await
                    .unwrap()
                {
                    SelectedStagedSessionStamp::Updated(receipt) => receipt,
                    _ => panic!("fixture stamp changed selection"),
                },
            }
        }

        async fn unbound_fixture() -> (tempfile::TempDir, Arc<Store>, ModelReporting) {
            let (dir, store, reporting) = fixture().await;
            store
                .identity_conn()
                .execute(
                    "DELETE FROM agent_runtimes WHERE runtime_id='s_observer'",
                    (),
                )
                .await
                .unwrap();
            reporting.initialize().await.unwrap();
            (dir, store, reporting)
        }

        #[tokio::test]
        async fn unbound_missing_removes_exact_session_and_closes_only_never_committed_capture() {
            for closed in [false, true] {
                let (_dir, store, reporting) = unbound_fixture().await;
                let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
                let old = reserve(&reporting).unwrap();
                if closed {
                    old.cell.close();
                    wait_completed(&old).await.unwrap();
                }
                let slot = old.cell.slot.clone();
                let outcome = reporting
                    .cleanup_unbound_registration(
                        staged,
                        Arc::new(store.lock_presence_transition().await),
                    )
                    .await;
                assert!(
                    matches!(outcome, Ok(SelectedStagedSessionCleanup::Removed)),
                    "expected actual removed receipt"
                );
                assert!(Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .is_none());
                assert!(old.cell.state.lock().unwrap().closed);
                assert!(!old.cell.state.lock().unwrap().committed);
                assert!(Arc::ptr_eq(
                    &slot,
                    reporting
                        .inner
                        .registry
                        .lock()
                        .unwrap()
                        .slots
                        .get(&"s_observer".into())
                        .unwrap()
                ));
                assert!(!*slot.offline_pending.lock().unwrap());
                assert!(slot.error.lock().unwrap().is_none());
                assert!(!slot.lane.lock().await.uncertain);
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn unbound_runtime_binding_veto_preserves_usable_handle() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
            let old = reserve(&reporting).unwrap();
            let before = row(&store).await;
            let outcome = reporting
                .cleanup_unbound_registration(
                    staged,
                    Arc::new(store.lock_presence_transition().await),
                )
                .await;
            assert!(
                matches!(outcome, Ok(SelectedStagedSessionCleanup::SelectionChanged)),
                "runtime veto must be clean"
            );
            assert_eq!(row(&store).await, before);
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(reporting.commit_claim(&old).await.unwrap());
            assert!(Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .is_some());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn unbound_absent_and_exact_replacement_are_distinct_success_receipts() {
            for absent in [false, true] {
                let (_dir, store, reporting) = unbound_fixture().await;
                let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
                let old = reserve(&reporting).unwrap();
                store
                    .conn
                    .execute("DELETE FROM sessions WHERE session_id='s_observer'", ())
                    .await
                    .unwrap();
                if !absent {
                    let _replacement = staged_registration(&store, "s_observer", None).await;
                }
                let outcome = reporting
                    .cleanup_unbound_registration(
                        staged,
                        Arc::new(store.lock_presence_transition().await),
                    )
                    .await;
                assert!(if absent {
                    matches!(outcome, Ok(SelectedStagedSessionCleanup::AlreadyAbsent))
                } else {
                    matches!(outcome, Ok(SelectedStagedSessionCleanup::SelectionChanged))
                });
                assert_eq!(old.cell.state.lock().unwrap().closed, absent);
                assert!(old.cell.slot.error.lock().unwrap().is_none());
                assert!(!old.cell.slot.lane.lock().await.uncertain);
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn unbound_missing_becomes_bound_at_actual_identity_gate_vetoes_without_session_write(
        ) {
            let (_dir, store, reporting) = unbound_fixture().await;
            let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
            let old = reserve(&reporting).unwrap();
            let gate = store
                .begin_identity_write_txn("unbound_missing_to_bound")
                .await
                .unwrap();
            let mut call = Box::pin(reporting.cleanup_unbound_registration(
                staged,
                Arc::new(store.lock_presence_transition().await),
            ));
            activation_admitted(&mut call, &old.cell.slot).await;
            // No other lane user exists: this witnesses the owned cleanup holding the actual
            // lane at its first datastore await, behind the explicitly held identity gate.
            parked_claim(&old).await;
            gate.execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,presence,active,started_at) VALUES('s_observer','a_observer','other','online',1,1)", ()).await.unwrap();
            gate.commit().await.unwrap();
            assert!(matches!(
                call.await,
                Ok(SelectedStagedSessionCleanup::SelectionChanged)
            ));
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(old.cell.slot.error.lock().unwrap().is_none());
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            assert!(Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .is_some());
            assert!(
                reporting.commit_claim(&old).await.unwrap(),
                "clean veto leaves the same handle usable"
            );
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn unbound_old_claim_and_cleanup_both_actual_lane_orders() {
            for claim_first in [false, true] {
                let (_dir, store, reporting) = if claim_first {
                    fixture().await
                } else {
                    unbound_fixture().await
                };
                reporting.initialize().await.unwrap();
                let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
                let old = reserve(&reporting).unwrap();
                let gate = store
                    .begin_identity_write_txn("unbound_claim_order")
                    .await
                    .unwrap();
                let mut claim = Box::pin(reporting.commit_claim(&old));
                let mut call = Box::pin(reporting.cleanup_unbound_registration(
                    staged,
                    Arc::new(store.lock_presence_transition().await),
                ));
                if claim_first {
                    assert!(futures::poll!(&mut claim).is_pending());
                    parked_claim(&old).await;
                    activation_admitted(&mut call, &old.cell.slot).await;
                } else {
                    activation_admitted(&mut call, &old.cell.slot).await;
                    parked_claim(&old).await;
                    assert!(futures::poll!(&mut claim).is_pending());
                }
                assert!(old.cell.state.lock().unwrap().claim_requested);
                assert!(!old.cell.state.lock().unwrap().committed);
                gate.commit().await.unwrap();
                if claim_first {
                    assert!(claim.await.unwrap());
                    let authority = row(&store).await;
                    assert!(
                        call.await.is_err(),
                        "actual winning OLD claim bars Session removal"
                    );
                    assert!(!old.cell.state.lock().unwrap().closed);
                    assert_eq!(row(&store).await, authority);
                    assert_eq!(
                        old.cell.slot.lane.lock().await.confirmed.as_deref(),
                        Some(old.cell.key.token.as_str())
                    );
                    assert!(!old.cell.slot.lane.lock().await.uncertain);
                    assert!(old.bind_native_root(" root/opaque "));
                    assert!(
                        reporting.activate(&old, " root/opaque "),
                        "winning OLD remains usable after cleanup veto"
                    );
                    assert!(Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap()
                        .is_some());
                } else {
                    assert!(matches!(
                        call.await,
                        Ok(SelectedStagedSessionCleanup::Removed)
                    ));
                    assert!(!claim.await.unwrap());
                    assert!(old.cell.state.lock().unwrap().closed);
                    assert!(Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap()
                        .is_none());
                }
                assert!(old.cell.slot.error.lock().unwrap().is_none());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn unbound_admission_rejects_ineligible_cells_without_partial_effects() {
            for reason in [
                "pending",
                "error",
                "uncertain",
                "confirmed",
                "foreign",
                "no_agent",
                "activated",
                "committed_cleared",
            ] {
                let (_dir, store, reporting) = fixture().await;
                reporting.initialize().await.unwrap();
                let staged = staged_registration(
                    &store,
                    "s_observer",
                    if reason == "no_agent" {
                        None
                    } else {
                        Some("a_observer")
                    },
                )
                .await;
                let old = reserve(&reporting).unwrap();
                match reason {
                    "pending" => *old.cell.slot.offline_pending.lock().unwrap() = true,
                    "error" => old
                        .cell
                        .slot
                        .retain_captured_failure(None, "original rejected cleanup"),
                    "uncertain" => old.cell.slot.lane.lock().await.uncertain = true,
                    "confirmed" => {
                        old.cell.slot.lane.lock().await.confirmed =
                            Some("retained-confirmed".into())
                    }
                    "foreign" => {
                        // Use the real reservation path, then keep this replacement alive below.
                        let foreign = reserve_pair(&reporting, "s_observer", "a_foreign");
                        assert!(reporting
                            .cleanup_unbound_registration(
                                staged,
                                Arc::new(store.lock_presence_transition().await)
                            )
                            .await
                            .is_err());
                        assert!(!foreign.cell.state.lock().unwrap().closed);
                        assert!(!*foreign.cell.slot.offline_pending.lock().unwrap());
                        assert!(Sessions::new(&store)
                            .find_by_session_id(&"s_observer".into())
                            .await
                            .unwrap()
                            .is_some());
                        reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                        continue;
                    }
                    "activated" => old.cell.state.lock().unwrap().activated = true,
                    "committed_cleared" => {
                        assert!(reporting.commit_claim(&old).await.unwrap());
                        assert!(reporting.revoke_committed(&old).await.unwrap());
                        assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
                        assert!(old.cell.state.lock().unwrap().committed);
                    }
                    _ => {}
                }
                store
                    .identity_conn()
                    .execute(
                        "DELETE FROM agent_runtimes WHERE runtime_id='s_observer'",
                        (),
                    )
                    .await
                    .unwrap();
                let before = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                let tasks = reporting.inner.registry.lock().unwrap().tasks;
                let closed = old.cell.state.lock().unwrap().closed;
                assert!(
                    reporting
                        .cleanup_unbound_registration(
                            staged,
                            Arc::new(store.lock_presence_transition().await)
                        )
                        .await
                        .is_err(),
                    "{reason}"
                );
                assert_eq!(old.cell.state.lock().unwrap().closed, closed, "{reason}");
                assert_eq!(
                    *old.cell.slot.offline_pending.lock().unwrap(),
                    reason == "pending",
                    "{reason}"
                );
                assert_eq!(
                    reporting.inner.registry.lock().unwrap().tasks,
                    tasks,
                    "{reason}"
                );
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    before,
                    "{reason}"
                );
                // Fixture-only invalid states are restored so unrelated worker shutdown is clean.
                *old.cell.slot.offline_pending.lock().unwrap() = false;
                *old.cell.slot.error.lock().unwrap() = None;
                old.cell.slot.lane.lock().await.uncertain = false;
                old.cell.slot.lane.lock().await.confirmed = None;
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn unbound_notready_and_shutdown_admission_create_no_slots() {
            for shutdown in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                if shutdown {
                    reporting.initialize().await.unwrap();
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                }
                let staged = staged_registration(&store, "s_unbound", None).await;
                assert!(reporting
                    .cleanup_unbound_registration(
                        staged,
                        Arc::new(store.lock_presence_transition().await)
                    )
                    .await
                    .is_err());
                assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
                assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
                assert!(Sessions::new(&store)
                    .find_by_session_id(&"s_unbound".into())
                    .await
                    .unwrap()
                    .is_some());
            }
        }

        #[tokio::test]
        async fn unbound_rechecks_errors_uncertainty_and_committed_state_after_actual_lane_wait() {
            for reason in ["error", "uncertain", "confirmed", "committed", "activated"] {
                let (_dir, store, reporting) = unbound_fixture().await;
                let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
                let old = reserve(&reporting).unwrap();
                let mut lane = old.cell.slot.lane.lock().await;
                let mut call = Box::pin(reporting.cleanup_unbound_registration(
                    staged,
                    Arc::new(store.lock_presence_transition().await),
                ));
                activation_admitted(&mut call, &old.cell.slot).await;
                match reason {
                    "error" => old
                        .cell
                        .slot
                        .retain_captured_failure(None, "original while queued"),
                    "uncertain" => lane.uncertain = true,
                    "confirmed" => lane.confirmed = Some("queued-confirmed".into()),
                    "committed" => old.cell.state.lock().unwrap().committed = true,
                    "activated" => old.cell.state.lock().unwrap().activated = true,
                    _ => unreachable!(),
                }
                drop(lane);
                assert!(call.await.is_err(), "{reason}");
                assert!(!old.cell.state.lock().unwrap().closed);
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                assert!(Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .is_some());
                *old.cell.slot.error.lock().unwrap() = None;
                old.cell.slot.lane.lock().await.uncertain = false;
                old.cell.slot.lane.lock().await.confirmed = None;
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn unbound_cancelled_receiver_retains_presence_task_and_settlement() {
            for at_session in [false, true] {
                let (_dir, store, reporting) = unbound_fixture().await;
                let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
                let old = reserve(&reporting).unwrap();
                let gate = if at_session {
                    store.begin_write_txn("unbound_session_cancel").await
                } else {
                    store
                        .begin_identity_write_txn("unbound_identity_cancel")
                        .await
                }
                .unwrap();
                let transport_lock = store.write_lock();
                let baseline = Arc::strong_count(&transport_lock);
                let mut call = Box::pin(reporting.cleanup_unbound_registration(
                    staged,
                    Arc::new(store.lock_presence_transition().await),
                ));
                activation_admitted(&mut call, &old.cell.slot).await;
                parked_claim(&old).await;
                if at_session {
                    unbound_transport_waiter(&transport_lock, baseline).await;
                }
                drop(call);
                let mut presence = Box::pin(store.lock_presence_transition());
                assert!(futures::poll!(&mut presence).is_pending());
                assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 2);
                assert!(*old.cell.slot.offline_pending.lock().unwrap());
                assert!(reserve(&reporting).is_err());
                gate.commit().await.unwrap();
                drop(
                    tokio::time::timeout(Duration::from_secs(2), presence)
                        .await
                        .unwrap(),
                );
                assert!(old.cell.state.lock().unwrap().closed);
                assert!(old.cell.slot.error.lock().unwrap().is_none());
                assert!(!old.cell.slot.lane.lock().await.uncertain);
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                assert!(Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .is_none());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn unbound_never_polled_receiver_has_zero_effects() {
            let (_dir, store, reporting) = unbound_fixture().await;
            let staged = staged_registration(&store, "s_observer", None).await;
            let call = reporting.cleanup_unbound_registration(
                staged,
                Arc::new(store.lock_presence_transition().await),
            );
            drop(call);
            assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
            assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            let _presence = store.lock_presence_transition().await;
            assert!(Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .is_some());
        }

        #[tokio::test]
        async fn unbound_many_unobserved_successes_reclaim_unowned_slots_but_keep_handles_and_failures(
        ) {
            let (_dir, store, reporting) = unbound_fixture().await;
            let old = reserve(&reporting).unwrap();
            let held_slot = old.cell.slot.clone();
            let failed = staged_registration(&store, "s_failed_cleanup", None).await;
            store.conn.execute_batch("CREATE TRIGGER unbound_gc_failure BEFORE DELETE ON sessions WHEN OLD.session_id='s_failed_cleanup' BEGIN SELECT RAISE(ABORT, 'original unbound gc failure'); END;").await.unwrap();
            let error = match reporting
                .cleanup_unbound_registration(
                    failed,
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
            {
                Err(error) => error,
                Ok(_) => panic!("expected actual Session cleanup failure"),
            };
            assert!(error.to_string().contains("original unbound gc failure"));
            for index in 0..48 {
                let id = format!("s_cleanup_{index}");
                let staged = staged_registration(&store, &id, None).await;
                let gate = store
                    .begin_identity_write_txn("unobserved_unbound_gc")
                    .await
                    .unwrap();
                let mut call = Box::pin(reporting.cleanup_unbound_registration(
                    staged,
                    Arc::new(store.lock_presence_transition().await),
                ));
                assert!(futures::poll!(&mut call).is_pending());
                assert!(reporting
                    .inner
                    .registry
                    .lock()
                    .unwrap()
                    .slots
                    .contains_key(&id.clone().into()));
                drop(call);
                gate.commit().await.unwrap();
                drop(store.lock_presence_transition().await);
                assert!(!reporting
                    .inner
                    .registry
                    .lock()
                    .unwrap()
                    .slots
                    .contains_key(&id.into()));
            }
            assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), 2);
            assert!(Arc::ptr_eq(
                &held_slot,
                reporting
                    .inner
                    .registry
                    .lock()
                    .unwrap()
                    .slots
                    .get(&"s_observer".into())
                    .unwrap()
            ));
            old.cell.close();
            wait_completed(&old).await.unwrap();
            drop(old);
            drop(held_slot);
            reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .reclaim_settled_slots();
            assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), 1);
            assert!(reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .slots
                .contains_key(&"s_failed_cleanup".into()));
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
        }

        #[tokio::test]
        async fn unbound_actual_session_commit_failure_retains_original_before_secondary_old_and_foreign_current(
        ) {
            let (_dir, store, reporting) = unbound_fixture().await;
            let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
            let old = reserve(&reporting).unwrap();
            store.conn.execute_batch("PRAGMA foreign_keys=ON;
                CREATE TABLE unbound_parent(id INTEGER PRIMARY KEY);
                CREATE TABLE unbound_child(parent_id INTEGER REFERENCES unbound_parent(id) DEFERRABLE INITIALLY DEFERRED);
                CREATE TRIGGER unbound_commit_fail AFTER DELETE ON sessions BEGIN INSERT INTO unbound_child VALUES(1); END;").await.unwrap();
            let gate = store
                .begin_write_txn("unbound_failed_commit_gate")
                .await
                .unwrap();
            let mut call = Box::pin(reporting.cleanup_unbound_registration(
                staged,
                Arc::new(store.lock_presence_transition().await),
            ));
            activation_admitted(&mut call, &old.cell.slot).await;
            parked_claim(&old).await;
            let (_foreign_dir, _foreign_store, foreign_reporting) = unbound_fixture().await;
            let foreign = reserve(&foreign_reporting).unwrap();
            // Retained foreign out-of-band current is a closure-scope fixture, NOT a new owner
            // admitted onto the failed lane or evidence of NEW usability on that failed lane.
            *old.cell.slot.current.lock().unwrap() = Arc::downgrade(&foreign.cell);
            let entered = AtomicBool::new(false);
            let mut secondary = Box::pin(async {
                let mut lane = old.cell.slot.lane.lock().await;
                entered.store(true, Ordering::SeqCst);
                assert!(old.cell.state.lock().unwrap().closed);
                assert!(lane.uncertain);
                let original = old.cell.slot.error.lock().unwrap().clone().unwrap();
                assert!(
                    original.contains("FOREIGN KEY constraint failed"),
                    "{original}"
                );
                assert!(old
                    .cell
                    .slot
                    .captured_failure_closure
                    .load(Ordering::Relaxed));
                let result = match require_lane(&old.cell, &lane) {
                    Ok(()) => reporting.inner.cleanup(&old.cell, &mut lane).await,
                    Err(error) => Err(error),
                };
                drop(lane);
                reporting
                    .inner
                    .fail_owner(&old.cell, "secondary OLD cleanup failure");
                assert_eq!(
                    old.cell.slot.error.lock().unwrap().as_deref(),
                    Some(original.as_str())
                );
                result
            });
            assert!(futures::poll!(&mut secondary).is_pending());
            assert!(!entered.load(Ordering::SeqCst));
            gate.commit().await.unwrap();
            let error = match call.await {
                Err(error) => error,
                Ok(_) => panic!("commit failure must not certify absence"),
            };
            assert!(
                error.to_string().contains("FOREIGN KEY constraint failed"),
                "{error}"
            );
            assert!(secondary.await.is_err());
            assert_eq!(
                old.cell.slot.error.lock().unwrap().as_deref(),
                Some(error.to_string().as_str())
            );
            assert!(!foreign.cell.state.lock().unwrap().closed);
            assert!(reserve(&reporting).is_err());
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
            foreign_reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn unbound_identity_select_failure_is_not_clean_veto() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
            let old = reserve(&reporting).unwrap();
            store
                .identity_conn()
                .execute(
                    "ALTER TABLE agent_runtimes RENAME TO unbound_unavailable_runtimes",
                    (),
                )
                .await
                .unwrap();
            let error = match reporting
                .cleanup_unbound_registration(
                    staged,
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
            {
                Err(error) => error,
                Ok(_) => panic!("failed identity SELECT cannot certify clean veto"),
            };
            assert!(error.to_string().contains("no such table"), "{error}");
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(old.cell.slot.lane.lock().await.uncertain);
            assert_eq!(
                old.cell.slot.error.lock().unwrap().as_deref(),
                Some(error.to_string().as_str())
            );
            assert!(Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .is_some());
            store
                .identity_conn()
                .execute(
                    "ALTER TABLE unbound_unavailable_runtimes RENAME TO agent_runtimes",
                    (),
                )
                .await
                .unwrap();
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
        }

        #[tokio::test]
        async fn unbound_tolerant_existing_identity_fields_still_return_clean_binding_veto() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
            let old = reserve(&reporting).unwrap();
            store.identity_conn().execute("UPDATE agent_runtimes SET active='not-an-integer',model_observer_token='' WHERE runtime_id='s_observer'", ()).await.unwrap();
            assert!(matches!(
                reporting
                    .cleanup_unbound_registration(
                        staged,
                        Arc::new(store.lock_presence_transition().await)
                    )
                    .await,
                Ok(SelectedStagedSessionCleanup::SelectionChanged)
            ));
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(old.cell.slot.error.lock().unwrap().is_none());
            assert!(!old.cell.slot.lane.lock().await.uncertain);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        async fn unbound_transport_waiter(lock: &Arc<tokio::sync::Mutex<()>>, baseline: usize) {
            // Isolated fixture: its only additional transport-lock owner is the actual selected
            // Session operation. The idle unclaimed observer never acquires this store gate.
            tokio::time::timeout(Duration::from_secs(2), async {
                while Arc::strong_count(lock) == baseline {
                    tokio::task::yield_now().await;
                }
                assert_eq!(Arc::strong_count(lock), baseline + 1);
            })
            .await
            .expect("cleanup did not reach its actual Session transaction gate");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn unbound_cancellation_after_session_commit_before_captured_local_close() {
            let (_dir, store, reporting) = unbound_fixture().await;
            let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
            let old = reserve(&reporting).unwrap();
            let transport_lock = store.write_lock();
            let gate = transport_lock.clone().lock_owned().await;
            let baseline = Arc::strong_count(&transport_lock);
            let mut call = Box::pin(reporting.cleanup_unbound_registration(
                staged,
                Arc::new(store.lock_presence_transition().await),
            ));
            activation_admitted(&mut call, &old.cell.slot).await;
            unbound_transport_waiter(&transport_lock, baseline).await;
            // Dedicated blocking thread owns the std mutex; no std mutex is held across an
            // await on this executor. Dropping release on any assertion failure unblocks it.
            let cell = old.cell.clone();
            let (ready, entered) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel::<()>();
            let closer = tokio::task::spawn_blocking(move || {
                let _closure_gate = cell.state.lock().unwrap();
                let _ = ready.send(());
                let _ = wait.recv();
            });
            entered.await.unwrap();
            drop(gate);
            // FIFO transport gate successor can enter only after Session commit has released
            // its transaction guard. Query inside this successor transaction is the commit
            // witness; merely reading the shared connection's uncommitted DELETE is not.
            let committed = store
                .begin_write_txn("unbound_postcommit_witness")
                .await
                .unwrap();
            let mut rows = committed
                .query(
                    "SELECT count(*) FROM sessions WHERE session_id='s_observer'",
                    (),
                )
                .await
                .unwrap();
            assert_eq!(
                rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
                0
            );
            drop(rows);
            committed.commit().await.unwrap();
            assert!(futures::poll!(&mut call).is_pending());
            drop(call);
            let mut presence = Box::pin(store.lock_presence_transition());
            assert!(futures::poll!(&mut presence).is_pending());
            assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 2);
            assert!(*old.cell.slot.offline_pending.lock().unwrap());
            assert!(old.cell.slot.lane.try_lock().is_err());
            drop(release);
            closer.await.unwrap();
            drop(
                tokio::time::timeout(Duration::from_secs(2), presence)
                    .await
                    .unwrap(),
            );
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            assert!(old.cell.slot.error.lock().unwrap().is_none());
            assert!(!old.cell.slot.lane.lock().await.uncertain);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn unbound_session_replacement_at_actual_transport_gate_preserves_handle_and_only_cleanup_signals(
        ) {
            for replace in [false, true] {
                let (_dir, store, _) = unbound_fixture().await;
                let events = Arc::new(CombinedEvents::default());
                let reporting = ModelReporting::new(store.clone(), events.clone());
                reporting.initialize().await.unwrap();
                let staged = staged_registration(&store, "s_observer", Some("a_observer")).await;
                let old = reserve(&reporting).unwrap();
                let gate = store
                    .begin_write_txn("unbound_exact_transport_replacement")
                    .await
                    .unwrap();
                let transport_lock = store.write_lock();
                let baseline = Arc::strong_count(&transport_lock);
                let epoch = store.events().session_lifecycle_changed().epoch();
                let facts = store.events().developer_event_appended().epoch();
                let mut call = Box::pin(reporting.cleanup_unbound_registration(
                    staged,
                    Arc::new(store.lock_presence_transition().await),
                ));
                activation_admitted(&mut call, &old.cell.slot).await;
                unbound_transport_waiter(&transport_lock, baseline).await;
                if replace {
                    // Mutate the captured raw project image while cleanup waits at the gate.
                    // The exact-row veto must preserve this captured handle's usability.
                    gate.execute(
                        "UPDATE sessions SET project=NULL WHERE session_id='s_observer'",
                        (),
                    )
                    .await
                    .unwrap();
                }
                gate.commit().await.unwrap();
                let result = call.await;
                assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
                assert_eq!(events.projections.load(Ordering::SeqCst), 0);
                assert_eq!(
                    store.events().session_lifecycle_changed().epoch(),
                    epoch + u64::from(!replace)
                );
                assert_eq!(store.events().developer_event_appended().epoch(), facts);
                if replace {
                    assert!(matches!(
                        result,
                        Ok(SelectedStagedSessionCleanup::SelectionChanged)
                    ));
                    assert!(!old.cell.state.lock().unwrap().closed);
                    // A veto after the transport CAS also leaves this very handle usable.
                    AgentRuntimes::new(&store)
                        .create(NewAgentRuntime {
                            runtime_id: "s_observer".into(),
                            agent_id: "a_observer".into(),
                            harness: "other".into(),
                            cwd: None,
                            transport: None,
                            presence: Some("online".into()),
                            active: true,
                        })
                        .await
                        .unwrap();
                    // Check callbacks before the independent claim below deliberately publishes.
                    assert_eq!(events.projections.load(Ordering::SeqCst), 0);
                    assert!(reporting.commit_claim(&old).await.unwrap());
                } else {
                    assert!(matches!(result, Ok(SelectedStagedSessionCleanup::Removed)));
                    assert!(old.cell.state.lock().unwrap().closed);
                    assert_eq!(events.projections.load(Ordering::SeqCst), 0);
                }
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        async fn presence_activation_fixture() -> (
            tempfile::TempDir,
            Arc<Store>,
            Arc<ModelReporting>,
            Vec<ModelObserverHandle>,
            PresenceWriter,
            Arc<CombinedEvents>,
        ) {
            let (dir, store, reporting, owners) = activation_fixture(false).await;
            create_online_session(&store).await;
            // A distinct compatibility row lets lifecycle failures on the sibling coexist with
            // assertions about the target's earlier, non-transactional Session writes.
            Sessions::new(&store)
                .create(nexus_store::repos::NewSession {
                    session_id: "s_target".into(),
                    name: Some("target".into()),
                    agent: Some("other".into()),
                    kind: "agent".into(),
                    role: None,
                    tier: "agent".into(),
                    harness_session_id: None,
                    client_key: Some("target-key".into()),
                    cwd: None,
                    project: "default".into(),
                    transport: None,
                })
                .await
                .unwrap();
            store.conn.execute("UPDATE sessions SET presence='offline',last_heartbeat=1 WHERE session_id='s_target'", ()).await.unwrap();
            store.identity_conn().execute("UPDATE agent_runtimes SET presence='offline',last_heartbeat=1 WHERE runtime_id='s_target'", ()).await.unwrap();
            let reporting = Arc::new(reporting);
            let events = Arc::new(CombinedEvents::default());
            let writer =
                PresenceWriter::new(store.clone(), events.clone(), TransportRegistry::new())
                    .with_model_reporting(reporting.clone());
            (dir, store, reporting, owners, writer, events)
        }

        async fn presence_activate(
            writer: &PresenceWriter,
            activity: bool,
        ) -> Result<(), NexusError> {
            if activity {
                writer.restore_online_on_activity(&"s_target".into()).await
            } else {
                writer
                    .mark_transport_present(
                        &"s_target".into(),
                        crate::presence::TransportHandle::EventLoop,
                    )
                    .await
            }
        }

        async fn assert_presence_activation_veto(
            store: &Store,
            events: &CombinedEvents,
            error: NexusError,
            cause: &str,
        ) {
            let error = error.to_string();
            assert!(error.contains(cause), "original cause missing: {error}");
            assert!(
                error.contains("Session") && error.contains("already"),
                "partial Session write missing: {error}"
            );
            let session = Sessions::new(store)
                .find_by_session_id(&"s_target".into())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(session.presence.as_deref(), Some("online"));
            assert!(session.last_heartbeat.unwrap() > 1);
            let runtime = activation_row(store, "s_target").await;
            assert_eq!(runtime.presence.as_deref(), Some("offline"));
            assert_eq!(runtime.last_heartbeat, Some(1));
            assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn presence_activation_closes_actual_observed_siblings_preserving_target() {
            for activity in [false, true] {
                let (_dir, store, reporting, owners, writer, events) =
                    presence_activation_fixture().await;
                let before = activation_row(&store, "s_target").await;
                presence_activate(&writer, activity).await.unwrap();
                for owner in &owners[..2] {
                    assert!(
                        owner.cell.state.lock().unwrap().closed,
                        "actual sibling observer must close"
                    );
                    let row = activation_row(&store, &owner.cell.key.runtime_id.0).await;
                    assert!(!row.active);
                    assert!(row.model_observer_token.is_none());
                }
                let target = activation_row(&store, "s_target").await;
                assert_eq!(target.model_report, before.model_report);
                assert_eq!(target.model_observer_token, before.model_observer_token);
                assert_eq!(target.model_report_revision, before.model_report_revision);
                assert!(!owners[2].cell.state.lock().unwrap().closed);
                assert_eq!(target.presence.as_deref(), Some("online"));
                assert_eq!(events.statuses.load(Ordering::SeqCst), 1);
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn presence_activation_closed_admission_retains_session_partial_without_runtime_tail()
        {
            for activity in [false, true] {
                let (_dir, store, reporting, _owners, writer, events) =
                    presence_activation_fixture().await;
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                let before = activation_row(&store, "s_target").await;
                let error = presence_activate(&writer, activity)
                    .await
                    .expect_err("managed closed admission must veto Presence activation");
                assert_presence_activation_veto(&store, &events, error, "not admitting activation")
                    .await;
                assert_eq!(activation_row(&store, "s_target").await.stopped_at, Some(9));
                assert_eq!(activation_row(&store, "s_target").await, before);
                assert_eq!(writer.registry().is_present(&"s_target".into()), !activity);
            }
        }

        #[tokio::test]
        async fn presence_activation_actual_store_failures_stop_tail_and_preserve_cause() {
            for activity in [false, true] {
                for committed in [false, true] {
                    let (_dir, store, reporting, owners, writer, events) =
                        presence_activation_fixture().await;
                    if committed {
                        store.conn.execute_batch("CREATE TRIGGER presence_activation_fail BEFORE INSERT ON developer_events WHEN NEW.lifecycle='stopped' BEGIN SELECT RAISE(ABORT,'original presence lifecycle failure'); END;").await.unwrap();
                    } else {
                        store.identity_conn().execute_batch("CREATE TRIGGER presence_activation_fail BEFORE UPDATE OF stopped_at ON agent_runtimes WHEN OLD.runtime_id='s_sibling' BEGIN SELECT RAISE(ABORT,'original presence activation abort'); END;").await.unwrap();
                    }
                    let error = presence_activate(&writer, activity).await.unwrap_err();
                    let message = error.to_string();
                    assert!(
                        message.contains(if committed {
                            "Committed"
                        } else {
                            "NotCommitted"
                        }),
                        "{message}"
                    );
                    assert_presence_activation_veto(
                        &store,
                        &events,
                        error,
                        if committed {
                            "original presence lifecycle failure"
                        } else {
                            "original presence activation abort"
                        },
                    )
                    .await;
                    // Applied activation itself clears stopped_at; that is not mark_live tail.
                    assert_eq!(
                        activation_row(&store, "s_target").await.stopped_at,
                        if committed { None } else { Some(9) }
                    );
                    for owner in &owners[..2] {
                        assert_eq!(owner.cell.state.lock().unwrap().closed, committed);
                    }
                    if committed {
                        assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
                    } else {
                        store
                            .identity_conn()
                            .execute_batch("DROP TRIGGER presence_activation_fail;")
                            .await
                            .unwrap();
                        reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                    }
                }
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn presence_activation_selected_binding_veto_and_cancel_retain_delegate_guard() {
            for cancel in [false, true] {
                for activity in [false, true] {
                    let (_dir, store, reporting, owners, writer, events) =
                        presence_activation_fixture().await;
                    let held = owners[0].cell.slot.lane.lock().await;
                    let mut call = Box::pin(presence_activate(&writer, activity));
                    // Actual admission is after the Presence runtime read and selected capture.
                    activation_admitted(&mut call, &owners[0].cell.slot).await;
                    if cancel {
                        drop(call);
                        let mut presence = Box::pin(store.lock_presence_transition());
                        assert!(futures::poll!(&mut presence).is_pending());
                        let mut shutdown = Box::pin(reporting.shutdown(Duration::from_secs(2)));
                        assert!(futures::poll!(&mut shutdown).is_pending());
                        drop(held);
                        shutdown.await.unwrap();
                        drop(presence.await);
                        assert!(!activation_row(&store, "s_observer").await.active);
                        assert_eq!(
                            activation_row(&store, "s_target").await.presence.as_deref(),
                            Some("offline")
                        );
                        assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
                    } else {
                        Agents::new(&store)
                            .create(NewAgent {
                                agent_id: "a_replacement".into(),
                                project: "default".into(),
                                name: None,
                                default_harness: None,
                                role: None,
                                tier: None,
                                owner: None,
                            })
                            .await
                            .unwrap();
                        store.identity_conn().execute("UPDATE agent_runtimes SET agent_id='a_replacement' WHERE runtime_id='s_target'", ()).await.unwrap();
                        drop(held);
                        assert_presence_activation_veto(
                            &store,
                            &events,
                            call.await.unwrap_err(),
                            "selection changed",
                        )
                        .await;
                        for owner in &owners {
                            assert!(!owner.cell.state.lock().unwrap().closed);
                        }
                        reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                    }
                }
            }
        }

        #[tokio::test]
        async fn presence_activation_paused_activity_busy_transport_and_missing_runtime_controls() {
            let (_dir, store, reporting, owners, writer, events) =
                presence_activation_fixture().await;
            Sessions::new(&store)
                .set_paused(&"s_target".into(), true, Some("self"))
                .await
                .unwrap();
            let before = activation_row(&store, "s_target").await;
            writer
                .restore_online_on_activity(&"s_target".into())
                .await
                .unwrap();
            assert_eq!(activation_row(&store, "s_target").await, before);
            assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
            Sessions::new(&store)
                .set_presence(&"s_target".into(), nexus_contracts::Presence::Busy)
                .await
                .unwrap();
            // Transport does not share activity's pause veto, but preserves explicit busy.
            let lease = writer.binding_transition().await;
            lease.materialize_online(&"s_target".into()).await.unwrap();
            drop(lease);
            assert!(owners[0].cell.state.lock().unwrap().closed);
            assert_eq!(
                activation_row(&store, "s_target").await.presence.as_deref(),
                Some("busy")
            );
            assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            store
                .identity_conn()
                .execute("DELETE FROM agent_runtimes WHERE runtime_id='s_target'", ())
                .await
                .unwrap();
            Sessions::new(&store)
                .set_paused(&"s_target".into(), false, None)
                .await
                .unwrap();
            for activity in [false, true] {
                store.conn.execute("UPDATE sessions SET presence='offline',last_heartbeat=1 WHERE session_id='s_target'", ()).await.unwrap();
                presence_activate(&writer, activity).await.unwrap();
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_target".into())
                        .await
                        .unwrap()
                        .unwrap()
                        .presence
                        .as_deref(),
                    Some("online")
                );
            }
            assert_eq!(events.statuses.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn presence_activation_failed_lane_has_no_runtime_tail() {
            for activity in [false, true] {
                let (_dir, store, reporting, owners, writer, events) =
                    presence_activation_fixture().await;
                *owners[2].cell.slot.error.lock().unwrap() =
                    Some("prior actual lane failure".into());
                let before = activation_row(&store, "s_observer").await;
                let target_before = activation_row(&store, "s_target").await;
                let error = presence_activate(&writer, activity).await.unwrap_err();
                assert_presence_activation_veto(
                    &store,
                    &events,
                    error,
                    "prior actual lane failure",
                )
                .await;
                assert_eq!(activation_row(&store, "s_observer").await, before);
                assert_eq!(activation_row(&store, "s_target").await, target_before);
                for owner in &owners {
                    assert!(!owner.cell.state.lock().unwrap().closed);
                }
                *owners[2].cell.slot.error.lock().unwrap() = None;
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn presence_activation_unknown_commit_failure_stops_tail_without_rollback_claim() {
            for activity in [false, true] {
                let (_dir, store, reporting, owners, writer, events) =
                    presence_activation_fixture().await;
                store.identity_conn().execute_batch("PRAGMA foreign_keys=ON;
                    CREATE TABLE presence_parent(id INTEGER PRIMARY KEY);
                    CREATE TABLE presence_child(parent_id INTEGER REFERENCES presence_parent(id) DEFERRABLE INITIALLY DEFERRED);
                    CREATE TRIGGER presence_commit_fail AFTER UPDATE OF stopped_at ON agent_runtimes
                    WHEN OLD.runtime_id='s_sibling' BEGIN INSERT INTO presence_child VALUES(1); END;").await.unwrap();
                let error = presence_activate(&writer, activity).await.unwrap_err();
                assert!(error.to_string().contains("Unknown"), "{error}");
                assert_presence_activation_veto(
                    &store,
                    &events,
                    error,
                    "FOREIGN KEY constraint failed",
                )
                .await;
                for owner in &owners {
                    assert!(owner.cell.state.lock().unwrap().closed);
                    assert!(owner.cell.slot.lane.lock().await.uncertain);
                }
                assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn presence_activation_actual_identity_then_transport_gate_cancel_retains_settlement()
        {
            for activity in [false, true] {
                let (_dir, store, reporting, owners, writer, events) =
                    presence_activation_fixture().await;
                let before = activation_row(&store, "s_target").await;
                let identity = store
                    .begin_identity_write_txn("presence_activation_identity_gate")
                    .await
                    .unwrap();
                let mut call = Box::pin(presence_activate(&writer, activity));
                activation_admitted(&mut call, &owners[0].cell.slot).await;
                parked_claim(&owners[0]).await;
                assert_eq!(activation_row(&store, "s_target").await, before);
                assert!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_target".into())
                        .await
                        .unwrap()
                        .unwrap()
                        .last_heartbeat
                        .unwrap()
                        > 1
                );
                let transport_lock = store.write_lock();
                let transport = transport_lock.clone().lock_owned().await;
                let baseline = Arc::strong_count(&transport_lock);
                identity.commit().await.unwrap();
                // Only the admitted actual activation task can clone this isolated gate now.
                // This witnesses committed identity followed by the lifecycle append boundary.
                unbound_transport_waiter(&transport_lock, baseline).await;
                assert_eq!(activation_row(&store, "s_target").await.stopped_at, None);
                assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
                drop(call);
                let mut presence = Box::pin(store.lock_presence_transition());
                assert!(futures::poll!(&mut presence).is_pending());
                let mut shutdown = Box::pin(reporting.shutdown(Duration::from_secs(2)));
                assert!(futures::poll!(&mut shutdown).is_pending());
                drop(transport);
                shutdown.await.unwrap();
                drop(presence.await);
                assert!(owners[0].cell.state.lock().unwrap().closed);
                let target = activation_row(&store, "s_target").await;
                assert_eq!(target.presence.as_deref(), Some("offline"));
                assert_eq!(target.last_heartbeat, Some(1));
                assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
            }
        }

        fn activation_request(creating: bool) -> RuntimeActivationRequest {
            if creating {
                RuntimeActivationRequest::CreateActive(NewAgentRuntime {
                    runtime_id: "s_target".into(),
                    agent_id: "a_observer".into(),
                    harness: "other".into(),
                    cwd: None,
                    transport: None,
                    presence: Some("online".into()),
                    active: true,
                })
            } else {
                RuntimeActivationRequest::ActivateExisting {
                    runtime_id: "s_target".into(),
                    agent_id: "a_observer".into(),
                }
            }
        }

        async fn activation_fixture(
            creating: bool,
        ) -> (
            tempfile::TempDir,
            Arc<Store>,
            ModelReporting,
            Vec<ModelObserverHandle>,
        ) {
            let (dir, store, reporting) = fixture().await;
            for id in ["s_sibling", "s_target"] {
                if creating && id == "s_target" {
                    continue;
                }
                AgentRuntimes::new(&store)
                    .create(NewAgentRuntime {
                        runtime_id: id.into(),
                        agent_id: "a_observer".into(),
                        harness: "other".into(),
                        cwd: None,
                        transport: None,
                        presence: Some("online".into()),
                        active: false,
                    })
                    .await
                    .unwrap();
            }
            // Active-but-stopped rows remain siblings under the production predicate, and do not
            // violate the partial unique index for the one active, unstopped runtime.
            store
                .identity_conn()
                .execute("UPDATE agent_runtimes SET active=1,stopped_at=9", ())
                .await
                .unwrap();
            reporting.initialize().await.unwrap();
            let mut owners = Vec::new();
            for id in ["s_observer", "s_sibling", "s_target"] {
                if creating && id == "s_target" {
                    continue;
                }
                let owner = reserve_pair(&reporting, id, "a_observer");
                assert!(reporting.commit_claim(&owner).await.unwrap());
                store
                    .identity_conn()
                    .execute(
                        "UPDATE agent_runtimes SET stopped_at=NULL WHERE runtime_id=?1",
                        libsql::params![id],
                    )
                    .await
                    .unwrap();
                assert!(owner.bind_native_root(" root/opaque "));
                assert!(owner.observe(update(ModelEvidenceField::Configured, id)));
                assert!(reporting.activate(&owner, " root/opaque "));
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        if activation_row(&store, id)
                            .await
                            .model_report
                            .is_some_and(|report| {
                                matches!(report.configured, ModelEvidenceSlot::Observed { .. })
                            })
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                store
                    .identity_conn()
                    .execute(
                        "UPDATE agent_runtimes SET stopped_at=9 WHERE runtime_id=?1",
                        libsql::params![id],
                    )
                    .await
                    .unwrap();
                owners.push(owner);
            }
            (dir, store, reporting, owners)
        }

        async fn activation_row(store: &Store, id: &str) -> nexus_store::types::AgentRuntimeRow {
            AgentRuntimes::new(store)
                .find_by_runtime_id(id)
                .await
                .unwrap()
                .unwrap()
        }

        #[tokio::test]
        async fn activation_exact_multiple_siblings_preserves_target_authority() {
            for creating in [false, true] {
                let (_dir, store, reporting, owners) = activation_fixture(creating).await;
                let before = if creating {
                    None
                } else {
                    Some(activation_row(&store, "s_target").await)
                };
                let revisions = [
                    activation_row(&store, "s_observer")
                        .await
                        .model_report_revision,
                    activation_row(&store, "s_sibling")
                        .await
                        .model_report_revision,
                ];
                let receipt = reporting
                    .activate_runtime(
                        activation_request(creating),
                        Arc::new(store.lock_presence_transition().await),
                    )
                    .await
                    .unwrap();
                let SelectedRuntimeActivation::Applied(changes) = receipt else {
                    panic!("expected committed activation")
                };
                assert_eq!(
                    changes.target_pair(),
                    &("s_target".into(), "a_observer".into())
                );
                assert_eq!(
                    changes.stopped_sibling_pairs(),
                    &[
                        ("s_observer".into(), "a_observer".into()),
                        ("s_sibling".into(), "a_observer".into())
                    ]
                );
                for (index, owner) in owners.iter().take(2).enumerate() {
                    let row = activation_row(&store, &owner.cell.key.runtime_id.0).await;
                    assert!(!row.active);
                    assert!(row.model_observer_token.is_none());
                    assert!(row.model_report_revision > revisions[index]);
                    assert!(owner.cell.state.lock().unwrap().closed);
                    let lane = owner.cell.slot.lane.lock().await;
                    assert!(lane.confirmed.is_none());
                    assert!(!lane.uncertain);
                    assert!(owner.cell.slot.error.lock().unwrap().is_none());
                    assert!(!*owner.cell.slot.offline_pending.lock().unwrap());
                }
                if let Some(before) = before {
                    let row = activation_row(&store, "s_target").await;
                    assert_eq!(row.model_observer_token, before.model_observer_token);
                    assert_eq!(row.model_report_revision, before.model_report_revision);
                    assert_eq!(row.model_report, before.model_report);
                    assert!(!owners[2].cell.state.lock().unwrap().closed);
                }
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn activation_abort_preserves_cells_and_returns_actual_not_committed() {
            use nexus_store::repos::agent_runtimes::RuntimeActivationCommitState;
            let (_dir, store, reporting, owners) = activation_fixture(false).await;
            let before = activation_row(&store, "s_observer").await;
            store.identity_conn().execute_batch("CREATE TRIGGER activation_abort BEFORE UPDATE OF stopped_at ON agent_runtimes WHEN OLD.runtime_id='s_sibling' BEGIN SELECT RAISE(ABORT,'original activation abort'); END;").await.unwrap();
            let error = reporting
                .activate_runtime(
                    activation_request(false),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .unwrap_err();
            let RuntimeActivationError::Store {
                failure,
                settlement_error,
            } = error
            else {
                panic!("missing store provenance: {error:?}")
            };
            assert_eq!(
                failure.commit_state(),
                RuntimeActivationCommitState::NotCommitted
            );
            assert!(failure
                .cause()
                .to_string()
                .contains("original activation abort"));
            assert!(settlement_error.is_none());
            assert_eq!(
                activation_row(&store, "s_observer")
                    .await
                    .model_observer_token,
                before.model_observer_token
            );
            for owner in &owners {
                assert!(!owner.cell.state.lock().unwrap().closed);
                assert!(owner.cell.slot.error.lock().unwrap().is_none());
                assert!(!owner.cell.slot.lane.lock().await.uncertain);
                assert!(!*owner.cell.slot.offline_pending.lock().unwrap());
            }
            store
                .identity_conn()
                .execute_batch("DROP TRIGGER activation_abort;")
                .await
                .unwrap();
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn activation_foreign_last_rejection_has_no_partial_admission() {
            let (_dir, store, reporting, owners) = activation_fixture(true).await;
            store.identity_conn().execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES('s_alpha','a_observer','other',1,1,9)",()).await.unwrap();
            let foreign = reserve_pair(&reporting, "s_target", "a_foreign");
            let slots = reporting.inner.registry.lock().unwrap().slots.len();
            let error = reporting
                .activate_runtime(
                    activation_request(true),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                RuntimeActivationError::RejectedBeforeStore { .. }
            ));
            assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), slots);
            for owner in owners.iter().chain([&foreign]) {
                assert!(!*owner.cell.slot.offline_pending.lock().unwrap());
                assert!(!owner.cell.state.lock().unwrap().closed);
            }
            assert!(AgentRuntimes::new(&store)
                .find_by_runtime_id("s_target")
                .await
                .unwrap()
                .is_none());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        async fn activation_admitted<F: Future>(
            call: &mut std::pin::Pin<Box<F>>,
            slot: &RuntimeSlot,
        ) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    assert!(
                        futures::poll!(&mut *call).is_pending(),
                        "activation settled before gate"
                    );
                    if *slot.offline_pending.lock().unwrap() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn activation_selection_races_skip_without_cell_cache_or_publication_changes() {
            for mutation in ["target", "added", "removed", "rebound"] {
                let (_dir, store, previous, _owners) = activation_fixture(false).await;
                previous.shutdown(Duration::from_secs(2)).await.unwrap();
                let events = Arc::new(CombinedEvents::default());
                let reporting = ModelReporting::new(store.clone(), events.clone());
                reporting.initialize().await.unwrap();
                let old = reserve_pair(&reporting, "s_observer", "a_observer");
                assert!(reporting.commit_claim(&old).await.unwrap());
                let before = activation_row(&store, "s_observer").await;
                let lane = old.cell.slot.lane.lock().await;
                let mut activation = Box::pin(reporting.activate_runtime(
                    activation_request(false),
                    Arc::new(store.lock_presence_transition().await),
                ));
                activation_admitted(&mut activation, &old.cell.slot).await;
                let projections = events.projections.load(Ordering::SeqCst);
                // Direct durable writer deliberately bypasses presence; store TX must revalidate.
                store.identity_conn().execute("INSERT INTO agents(agent_id,project,created_at) VALUES('a_foreign','default',1)",()).await.unwrap();
                let sql = match mutation {
                    "target" => "UPDATE agent_runtimes SET agent_id='a_foreign' WHERE runtime_id='s_target'",
                    "added" => "INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES('s_added','a_observer','other',1,1,9)",
                    "removed" => "UPDATE agent_runtimes SET active=0 WHERE runtime_id='s_sibling'",
                    _ => "UPDATE agent_runtimes SET agent_id='a_foreign' WHERE runtime_id='s_sibling'",
                };
                store.identity_conn().execute(sql, ()).await.unwrap();
                drop(lane);
                assert_eq!(
                    activation.await.unwrap(),
                    SelectedRuntimeActivation::SelectionChanged,
                    "{mutation}"
                );
                assert_eq!(events.projections.load(Ordering::SeqCst), projections);
                assert!(!old.cell.state.lock().unwrap().closed);
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                assert_eq!(
                    old.cell.slot.lane.lock().await.confirmed,
                    before.model_observer_token
                );
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn activation_cancelled_receiver_keeps_presence_and_settles_without_poison() {
            let (_dir, store, reporting, owners) = activation_fixture(true).await;
            let lane = owners[0].cell.slot.lane.lock().await;
            let mut activation = Box::pin(reporting.activate_runtime(
                activation_request(true),
                Arc::new(store.lock_presence_transition().await),
            ));
            activation_admitted(&mut activation, &owners[0].cell.slot).await;
            drop(activation);
            let mut presence = Box::pin(store.lock_presence_transition());
            assert!(futures::poll!(&mut presence).is_pending());
            let mut shutdown = Box::pin(reporting.shutdown(Duration::from_secs(2)));
            assert!(futures::poll!(&mut shutdown).is_pending());
            drop(lane);
            shutdown.await.unwrap();
            drop(presence.await);
            assert!(matches!(
                reporting
                    .activate_runtime(
                        activation_request(true),
                        Arc::new(store.lock_presence_transition().await)
                    )
                    .await,
                Err(RuntimeActivationError::RejectedBeforeStore { .. })
            ));
            assert!(activation_row(&store, "s_target").await.active);
            for owner in &owners {
                assert!(owner.cell.slot.error.lock().unwrap().is_none());
                assert!(!*owner.cell.slot.offline_pending.lock().unwrap());
                assert!(!owner.cell.slot.lane.lock().await.uncertain);
            }
        }

        #[tokio::test]
        async fn activation_unknown_actual_commit_failure_retains_original_captured_uncertainty() {
            use nexus_store::repos::agent_runtimes::RuntimeActivationCommitState;
            let (_dir, store, reporting, owners) = activation_fixture(false).await;
            store.identity_conn().execute_batch("PRAGMA foreign_keys=ON;
                CREATE TABLE activation_parent(id INTEGER PRIMARY KEY);
                CREATE TABLE activation_child(parent_id INTEGER REFERENCES activation_parent(id) DEFERRABLE INITIALLY DEFERRED);
                CREATE TRIGGER activation_commit_fail AFTER UPDATE OF stopped_at ON agent_runtimes
                WHEN OLD.runtime_id='s_sibling' BEGIN INSERT INTO activation_child VALUES(1); END;").await.unwrap();
            let error = reporting
                .activate_runtime(
                    activation_request(false),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .unwrap_err();
            let RuntimeActivationError::Store {
                failure,
                settlement_error,
            } = error
            else {
                panic!("expected actual unknown: {error:?}")
            };
            assert_eq!(
                failure.commit_state(),
                RuntimeActivationCommitState::Unknown
            );
            assert!(failure.confirmed_changes().is_none());
            assert!(settlement_error.is_none());
            for owner in &owners {
                assert!(owner.cell.state.lock().unwrap().closed);
                assert_eq!(
                    owner.cell.slot.error.lock().unwrap().as_deref(),
                    Some(failure.cause().to_string().as_str())
                );
                let lane = owner.cell.slot.lane.lock().await;
                assert!(lane.uncertain);
                assert_eq!(
                    lane.confirmed.as_deref(),
                    Some(owner.cell.key.token.as_str())
                );
            }
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
        }

        #[tokio::test]
        async fn activation_actual_committed_lifecycle_failure_retains_full_receipt() {
            use nexus_store::repos::agent_runtimes::RuntimeActivationCommitState;
            let (_dir, store, reporting, owners) = activation_fixture(false).await;
            // Runtime lifecycle append only exists for a compatibility session with a name.
            create_online_session(&store).await;
            store.conn.execute_batch("CREATE TRIGGER activation_lifecycle_fail BEFORE INSERT ON developer_events WHEN NEW.lifecycle='stopped' BEGIN SELECT RAISE(ABORT,'original activation lifecycle failure'); END;").await.unwrap();
            let error = reporting
                .activate_runtime(
                    activation_request(false),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .unwrap_err();
            let RuntimeActivationError::Store {
                failure,
                settlement_error,
            } = error
            else {
                panic!("expected actual committed: {error:?}")
            };
            assert_eq!(
                failure.commit_state(),
                RuntimeActivationCommitState::Committed
            );
            assert!(failure
                .cause()
                .to_string()
                .contains("original activation lifecycle failure"));
            assert_eq!(
                failure
                    .confirmed_changes()
                    .unwrap()
                    .stopped_sibling_pairs()
                    .len(),
                2
            );
            assert!(settlement_error.is_none());
            assert!(activation_row(&store, "s_target").await.active);
            for owner in owners.iter().take(2) {
                assert!(owner.cell.state.lock().unwrap().closed);
                assert!(owner.cell.slot.lane.lock().await.confirmed.is_none());
                assert!(activation_row(&store, &owner.cell.key.runtime_id.0)
                    .await
                    .model_observer_token
                    .is_none());
            }
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
        }

        #[tokio::test]
        async fn activation_publication_outside_lanes_retains_receipt_on_failure_or_cancel() {
            for cancel in [false, true] {
                let (_dir, store, previous, _owners) = activation_fixture(false).await;
                previous.shutdown(Duration::from_secs(2)).await.unwrap();
                let events = Arc::new(StaleProjection {
                    store: store.clone(),
                    armed: AtomicBool::new(false),
                    panic: AtomicBool::new(!cancel),
                    entered: Notify::new(),
                    release: Notify::new(),
                    rows: Mutex::new(Vec::new()),
                });
                let reporting = ModelReporting::new(store.clone(), events.clone());
                reporting.initialize().await.unwrap();
                let old = reserve_pair(&reporting, "s_observer", "a_observer");
                assert!(reporting.commit_claim(&old).await.unwrap());
                events.armed.store(true, Ordering::SeqCst);
                let mut activation = Box::pin(reporting.activate_runtime(
                    activation_request(false),
                    Arc::new(store.lock_presence_transition().await),
                ));
                tokio::select! { _ = events.entered.notified() => {}, result = &mut activation => panic!("settled before publication gate: {result:?}") }
                assert!(
                    old.cell.slot.lane.try_lock().is_ok(),
                    "publication must release lanes"
                );
                assert!(*old.cell.slot.offline_pending.lock().unwrap());
                let mut presence = Box::pin(store.lock_presence_transition());
                assert!(futures::poll!(&mut presence).is_pending());
                if cancel {
                    drop(activation);
                    events.release.notify_one();
                    drop(presence.await);
                    assert!(old.cell.slot.error.lock().unwrap().is_none());
                    assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                    reporting.shutdown(Duration::from_secs(2)).await.unwrap();
                } else {
                    events.release.notify_one();
                    let error = activation.await.unwrap_err();
                    let RuntimeActivationError::AfterStore { receipt, cause } = error else {
                        panic!("lost applied receipt: {error:?}")
                    };
                    let SelectedRuntimeActivation::Applied(changes) = receipt else {
                        panic!("not applied")
                    };
                    assert_eq!(changes.stopped_sibling_pairs().len(), 2);
                    assert!(cause.to_string().contains("postcommit projection panic"));
                    assert_eq!(
                        old.cell.slot.error.lock().unwrap().as_deref(),
                        Some(cause.to_string().as_str())
                    );
                    assert!(old
                        .cell
                        .slot
                        .captured_failure_closure
                        .load(Ordering::Relaxed));
                    drop(presence.await);
                    assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
                }
            }
        }

        #[tokio::test]
        async fn activation_rechecks_failure_and_uncertainty_after_lane_wait() {
            for retained_error in [false, true] {
                let (_dir, store, reporting, owners) = activation_fixture(false).await;
                let before = activation_row(&store, "s_observer").await;
                let mut lane = owners[0].cell.slot.lane.lock().await;
                let mut call = Box::pin(reporting.activate_runtime(
                    activation_request(false),
                    Arc::new(store.lock_presence_transition().await),
                ));
                activation_admitted(&mut call, &owners[0].cell.slot).await;
                lane.uncertain = true;
                if retained_error {
                    owners[0]
                        .cell
                        .slot
                        .retain_captured_failure(None, "earlier original lane failure");
                }
                drop(lane);
                let error = call.await.unwrap_err();
                let RuntimeActivationError::RejectedBeforeStore { cause } = error else {
                    panic!("unexpected submission {error:?}")
                };
                assert!(cause.to_string().contains(if retained_error {
                    "earlier original lane failure"
                } else {
                    "uncertain settlement"
                }));
                assert_eq!(activation_row(&store, "s_observer").await, before);
                for owner in &owners {
                    assert!(!*owner.cell.slot.offline_pending.lock().unwrap());
                }
                // Restore only test-injected inert state; no production settlement was submitted.
                owners[0].cell.slot.lane.lock().await.uncertain = false;
                *owners[0].cell.slot.error.lock().unwrap() = None;
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn activation_normal_closed_predecessor_both_cleanup_orders_remains_eligible() {
            for cleanup_first in [false, true] {
                let (_dir, store, reporting, owners) = activation_fixture(false).await;
                let old = &owners[0];
                let before = activation_row(&store, "s_observer").await;
                let sibling_before = activation_row(&store, "s_sibling").await;
                let target_before = activation_row(&store, "s_target").await;
                if cleanup_first {
                    assert!(reporting.revoke_committed(old).await.unwrap());
                }
                let gate = store
                    .begin_identity_write_txn("activation_closed_predecessor_order_gate")
                    .await
                    .unwrap();
                let mut activation = Box::pin(reporting.activate_runtime(
                    activation_request(false),
                    Arc::new(store.lock_presence_transition().await),
                ));
                activation_admitted(&mut activation, &old.cell.slot).await;
                // No test lane guard or OLD cleanup is running here. The activation task must
                // actually own this lane while its selected store write waits at the identity gate.
                parked_claim(old).await;
                let cleanup_entered = AtomicBool::new(false);
                let mut cleanup = Box::pin(async {
                    let mut lane = old.cell.slot.lane.lock().await;
                    cleanup_entered.store(true, Ordering::SeqCst);
                    reporting.inner.cleanup(&old.cell, &mut lane).await
                });
                if !cleanup_first {
                    old.cell.close();
                    // This is the real captured cleanup, not only its completion-channel waiter.
                    // Pending must mean lane exclusion, not a cleanup already at the identity gate.
                    assert!(futures::poll!(&mut cleanup).is_pending());
                    assert!(
                        !cleanup_entered.load(Ordering::SeqCst),
                        "cleanup acquired the lane before activation owned it"
                    );
                    assert_eq!(activation_row(&store, "s_observer").await, before);
                }
                gate.commit().await.unwrap();
                let SelectedRuntimeActivation::Applied(changes) = activation.await.unwrap() else {
                    panic!("expected exact applied activation receipt")
                };
                assert_eq!(
                    changes.target_pair(),
                    &("s_target".into(), "a_observer".into())
                );
                assert_eq!(
                    changes.stopped_sibling_pairs(),
                    &[
                        ("s_observer".into(), "a_observer".into()),
                        ("s_sibling".into(), "a_observer".into()),
                    ]
                );
                if !cleanup_first {
                    assert!(
                        !cleanup.await.unwrap(),
                        "activation already revoked this exact token"
                    );
                    assert!(cleanup_entered.load(Ordering::SeqCst));
                    assert!(!wait_completed(old).await.unwrap());
                }
                let after = activation_row(&store, "s_observer").await;
                assert!(!after.active);
                assert!(after.model_observer_token.is_none());
                assert_eq!(
                    after.model_report_revision,
                    before.model_report_revision + 1
                );
                assert_eq!(
                    activation_row(&store, "s_sibling")
                        .await
                        .model_report_revision,
                    sibling_before.model_report_revision + 1
                );
                let target_after = activation_row(&store, "s_target").await;
                assert!(target_after.active);
                assert_eq!(
                    target_after.model_observer_token,
                    target_before.model_observer_token
                );
                assert_eq!(
                    target_after.model_report_revision,
                    target_before.model_report_revision
                );
                assert_eq!(target_after.model_report, target_before.model_report);
                assert!(old.cell.state.lock().unwrap().closed);
                assert!(old.cell.slot.error.lock().unwrap().is_none());
                let lane = old.cell.slot.lane.lock().await;
                assert!(lane.confirmed.is_none());
                assert!(!lane.uncertain);
                drop(lane);
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn activation_apply_both_actual_lane_orders_preserve_revision_order() {
            for apply_first in [false, true] {
                let (_dir, store, reporting, owners) = activation_fixture(false).await;
                let old = &owners[0];
                store
                    .identity_conn()
                    .execute(
                        "UPDATE agent_runtimes SET stopped_at=NULL WHERE runtime_id='s_observer'",
                        (),
                    )
                    .await
                    .unwrap();
                let before = activation_row(&store, "s_observer")
                    .await
                    .model_report_revision;
                let mut snapshot = old.cell.state.lock().unwrap().snapshot();
                snapshot.sequence += 1;
                let gate = store
                    .begin_identity_write_txn("activation_apply_order_gate")
                    .await
                    .unwrap();
                let mut apply = Box::pin(reporting.inner.apply_snapshot(&old.cell, &snapshot));
                let mut activation = Box::pin(reporting.activate_runtime(
                    activation_request(false),
                    Arc::new(store.lock_presence_transition().await),
                ));
                if apply_first {
                    assert!(futures::poll!(&mut apply).is_pending());
                    assert!(
                        old.cell.slot.lane.try_lock().is_err(),
                        "apply entered actual lane before store gate"
                    );
                    activation_admitted(&mut activation, &old.cell.slot).await;
                } else {
                    activation_admitted(&mut activation, &old.cell.slot).await;
                    parked_claim(old).await; // Actual activation task holds lane at identity gate.
                    assert!(futures::poll!(&mut apply).is_pending());
                }
                gate.commit().await.unwrap();
                assert_eq!(
                    apply.await.unwrap(),
                    if apply_first { Some(true) } else { None }
                );
                assert!(matches!(
                    activation.await.unwrap(),
                    SelectedRuntimeActivation::Applied(_)
                ));
                let after = activation_row(&store, "s_observer").await;
                assert_eq!(
                    after.model_report_revision,
                    before + if apply_first { 2 } else { 1 }
                );
                assert!(after.model_observer_token.is_none());
                assert!(!old.observe(update(ModelEvidenceField::Configured, "late")));
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn activation_claim_both_actual_lane_orders_do_not_restore_stopped_sibling() {
            for claim_first in [false, true] {
                let (_dir, store, reporting) = fixture().await;
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                let gate = store
                    .begin_identity_write_txn("activation_claim_order_gate")
                    .await
                    .unwrap();
                let mut claim = Box::pin(reporting.commit_claim(&old));
                let mut activation = Box::pin(reporting.activate_runtime(
                    activation_request(true),
                    Arc::new(store.lock_presence_transition().await),
                ));
                if claim_first {
                    assert!(futures::poll!(&mut claim).is_pending());
                    parked_claim(&old).await;
                    activation_admitted(&mut activation, &old.cell.slot).await;
                } else {
                    activation_admitted(&mut activation, &old.cell.slot).await;
                    parked_claim(&old).await;
                    assert!(futures::poll!(&mut claim).is_pending());
                }
                gate.commit().await.unwrap();
                assert!(matches!(
                    activation.await.unwrap(),
                    SelectedRuntimeActivation::Applied(_)
                ));
                let claimed = claim.await.unwrap();
                if !claim_first {
                    assert!(!claimed);
                }
                let row = activation_row(&store, "s_observer").await;
                assert!(row.model_observer_token.is_none());
                assert!(!row.active);
                assert_eq!(row.model_report_revision, if claim_first { 2 } else { 0 });
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn activation_successful_unowned_slots_are_reclaimed() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            reporting
                .activate_runtime(
                    activation_request(true),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .unwrap();
            let registry = reporting.inner.registry.lock().unwrap();
            assert!(registry.slots.is_empty());
            assert_eq!(registry.tasks, 0);
            drop(registry);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[derive(Default)]
        struct ActivationPublicationOrder {
            calls: AtomicUsize,
            old_entered: Notify,
            old_release: Notify,
            activation_entered: Notify,
            activation_release: Notify,
            panicking: Notify,
        }
        #[async_trait]
        impl EventSink for ActivationPublicationOrder {
            async fn emit(&self, _: WsEvent) {
                panic!("unexpected compatibility event")
            }
            async fn project_runtime_binding(&self, runtime: &SessionId, _: &AgentId) {
                if runtime.0 != "s_observer" {
                    return;
                }
                match self.calls.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        self.old_entered.notify_one();
                        self.old_release.notified().await;
                    }
                    1 => {
                        self.activation_entered.notify_one();
                        self.activation_release.notified().await;
                        self.panicking.notify_one();
                        panic!("activation original parked publication failure");
                    }
                    _ => {}
                }
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn activation_parked_publication_retains_actual_receipt_first_cause_before_old_secondary(
        ) {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(ActivationPublicationOrder::default());
            let reporting = ModelReporting::new(store.clone(), events.clone());
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            events.old_entered.notified().await;
            let mut activation = Box::pin(reporting.activate_runtime(
                activation_request(true),
                Arc::new(store.lock_presence_transition().await),
            ));
            tokio::select! {_=events.activation_entered.notified()=>{},result=&mut activation=>panic!("early activation: {result:?}")}
            let (_foreign_dir, _foreign_store, foreign_reporting) = fixture().await;
            foreign_reporting.initialize().await.unwrap();
            let foreign = reserve(&foreign_reporting).unwrap();
            // Out-of-band foreign current fixture tests captured-only closure. It is NOT an
            // admitted successor on this failed lane, nor a same-pair incarnation guarantee.
            *old.cell.slot.current.lock().unwrap() = Arc::downgrade(&foreign.cell);
            let held = old.cell.slot.lane.lock().await;
            events.activation_release.notify_one();
            events.panicking.notified().await;
            // Current-thread event task runs through catch to the held lane before yielding.
            let mut barrier = Box::pin(old.cell.slot.lane.lock());
            assert!(futures::poll!(&mut barrier).is_pending());
            events.old_release.notify_one();
            drop(held);
            let lane = barrier.await;
            assert!(lane.uncertain);
            assert!(old
                .cell
                .slot
                .error
                .lock()
                .unwrap()
                .as_deref()
                .unwrap()
                .contains("activation original parked publication failure"));
            assert!(old
                .cell
                .slot
                .captured_failure_closure
                .load(Ordering::Relaxed));
            assert!(!foreign.cell.state.lock().unwrap().closed);
            drop(lane);
            let error = activation.await.unwrap_err();
            let RuntimeActivationError::AfterStore { receipt, cause } = error else {
                panic!("lost receipt: {error:?}")
            };
            let SelectedRuntimeActivation::Applied(changes) = receipt else {
                panic!("missing applied receipt")
            };
            assert_eq!(
                changes.target_pair(),
                &("s_target".into(), "a_observer".into())
            );
            assert_eq!(
                changes.stopped_sibling_pairs(),
                &[("s_observer".into(), "a_observer".into())]
            );
            assert!(claim.await.is_err());
            assert_eq!(
                old.cell.slot.error.lock().unwrap().as_deref(),
                Some(cause.to_string().as_str())
            );
            assert!(!foreign.cell.state.lock().unwrap().closed);
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
            foreign_reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn activation_real_receipt_validation_requires_full_target_and_sibling_selection() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let receipt = reporting
                .activate_runtime(
                    activation_request(true),
                    Arc::new(store.lock_presence_transition().await),
                )
                .await
                .unwrap();
            let SelectedRuntimeActivation::Applied(changes) = receipt else {
                panic!("expected store-produced changes")
            };
            let siblings = vec![("s_observer".into(), "a_observer".into())];
            assert!(activation_receipt_matches(
                &changes,
                "s_target",
                "a_observer",
                &siblings
            ));
            assert!(!activation_receipt_matches(
                &changes,
                "s_other",
                "a_observer",
                &siblings
            ));
            assert!(!activation_receipt_matches(
                &changes, "s_target", "a_other", &siblings
            ));
            assert!(!activation_receipt_matches(
                &changes,
                "s_target",
                "a_observer",
                &[]
            ));
            assert!(!activation_receipt_matches(
                &changes,
                "s_target",
                "a_observer",
                &[("s_other".into(), "a_observer".into())]
            ));
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn activation_captured_missing_target_replaced_before_store_skips_exactly() {
            let (_dir, store, reporting, owners) = activation_fixture(true).await;
            let held = owners[0].cell.slot.lane.lock().await;
            let mut activation = Box::pin(reporting.activate_runtime(
                activation_request(true),
                Arc::new(store.lock_presence_transition().await),
            ));
            activation_admitted(&mut activation, &owners[0].cell.slot).await;
            store.identity_conn().execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES('s_target','a_observer','other',0,1)",()).await.unwrap();
            let before = activation_row(&store, "s_target").await;
            drop(held);
            assert_eq!(
                activation.await.unwrap(),
                SelectedRuntimeActivation::SelectionChanged
            );
            assert_eq!(activation_row(&store, "s_target").await, before);
            for owner in &owners {
                assert!(!owner.cell.state.lock().unwrap().closed);
            }
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn activation_committed_store_failure_keeps_secondary_publication_cause_separate() {
            use nexus_store::repos::agent_runtimes::RuntimeActivationCommitState;
            let (_dir, store, previous, _owners) = activation_fixture(false).await;
            previous.shutdown(Duration::from_secs(2)).await.unwrap();
            create_online_session(&store).await;
            let events = Arc::new(StaleProjection {
                store: store.clone(),
                armed: AtomicBool::new(false),
                panic: AtomicBool::new(true),
                entered: Notify::new(),
                release: Notify::new(),
                rows: Mutex::new(Vec::new()),
            });
            let reporting = ModelReporting::new(store.clone(), events.clone());
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            store.conn.execute_batch("CREATE TRIGGER activation_lifecycle_secondary BEFORE INSERT ON developer_events WHEN NEW.lifecycle='stopped' BEGIN SELECT RAISE(ABORT,'original lifecycle before secondary'); END;").await.unwrap();
            events.armed.store(true, Ordering::SeqCst);
            let mut activation = Box::pin(reporting.activate_runtime(
                activation_request(false),
                Arc::new(store.lock_presence_transition().await),
            ));
            tokio::select! {_=events.entered.notified()=>{},result=&mut activation=>panic!("early result: {result:?}")}
            assert!(old
                .cell
                .slot
                .error
                .lock()
                .unwrap()
                .as_deref()
                .unwrap()
                .contains("original lifecycle before secondary"));
            events.release.notify_one();
            let error = activation.await.unwrap_err();
            let RuntimeActivationError::Store {
                failure,
                settlement_error,
            } = error
            else {
                panic!("lost actual store failure: {error:?}")
            };
            assert_eq!(
                failure.commit_state(),
                RuntimeActivationCommitState::Committed
            );
            assert!(failure
                .cause()
                .to_string()
                .contains("original lifecycle before secondary"));
            assert_eq!(
                failure
                    .confirmed_changes()
                    .unwrap()
                    .stopped_sibling_pairs()
                    .len(),
                2
            );
            assert!(settlement_error
                .unwrap()
                .to_string()
                .contains("postcommit projection panic"));
            assert_eq!(
                old.cell.slot.error.lock().unwrap().as_deref(),
                Some(failure.cause().to_string().as_str())
            );
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
        }

        #[tokio::test]
        async fn activation_initial_target_selection_cannot_be_authorized_by_late_creation() {
            let (_dir, store, reporting, owners) = activation_fixture(true).await;
            let before = activation_row(&store, "s_observer").await;
            let slots = reporting.inner.registry.lock().unwrap().slots.len();
            let mut held = Some(owners[0].cell.slot.lane.lock().await);
            let mut activation = Box::pin(reporting.activate_runtime(
                activation_request(false),
                Arc::new(store.lock_presence_transition().await),
            ));
            let result = tokio::time::timeout(Duration::from_secs(2),async {
                loop {
                    if let std::task::Poll::Ready(result) = futures::poll!(&mut activation) {break result;}
                    if *owners[0].cell.slot.offline_pending.lock().unwrap() {
                        // If an initially missing existing-target request is admitted, a direct
                        // writer can create that pair while the actual lane is still excluded.
                        store.identity_conn().execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at) VALUES('s_target','a_observer','other',0,1)",()).await.unwrap();
                        drop(held.take());
                        break activation.await;
                    }
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            drop(held);
            assert!(
                matches!(
                    result,
                    Err(RuntimeActivationError::RejectedBeforeStore { .. })
                ),
                "initial target absence must reject before admission, got {result:?}"
            );
            assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), slots);
            assert_eq!(activation_row(&store, "s_observer").await, before);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn activation_failed_pending_uncertain_last_participant_rejects_whole_batch() {
            for mode in ["failed", "pending", "uncertain"] {
                let (_dir, store, reporting, owners) = activation_fixture(false).await;
                store.identity_conn().execute("INSERT INTO agent_runtimes(runtime_id,agent_id,harness,active,started_at,stopped_at) VALUES('s_alpha','a_observer','other',1,1,9)",()).await.unwrap();
                let target = &owners[2];
                let before = activation_row(&store, "s_observer").await;
                let slots = reporting.inner.registry.lock().unwrap().slots.len();
                match mode {
                    "failed" => {
                        *target.cell.slot.error.lock().unwrap() =
                            Some("prior activation failure".into())
                    }
                    "pending" => *target.cell.slot.offline_pending.lock().unwrap() = true,
                    _ => target.cell.slot.lane.lock().await.uncertain = true,
                }
                assert!(matches!(
                    reporting
                        .activate_runtime(
                            activation_request(false),
                            Arc::new(store.lock_presence_transition().await)
                        )
                        .await,
                    Err(RuntimeActivationError::RejectedBeforeStore { .. })
                ));
                assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), slots);
                assert_eq!(activation_row(&store, "s_observer").await, before);
                for owner in &owners {
                    assert!(!owner.cell.state.lock().unwrap().closed);
                }
                assert!(!*owners[0].cell.slot.offline_pending.lock().unwrap());
                assert!(!*owners[1].cell.slot.offline_pending.lock().unwrap());
                *target.cell.slot.error.lock().unwrap() = None;
                *target.cell.slot.offline_pending.lock().unwrap() = false;
                target.cell.slot.lane.lock().await.uncertain = false;
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        fn offline_writer(
            store: Arc<Store>,
            reporting: Arc<ModelReporting>,
        ) -> crate::daemon::services::presence::PresenceWriter {
            use crate::daemon::services::presence::{PresenceWriter, TransportRegistry};
            PresenceWriter::new(store, Arc::new(Events), TransportRegistry::new())
                .with_model_reporting(reporting)
        }

        async fn age_runtime(store: &Store, runtime: &str, age: i64) {
            store
                .identity_conn()
                .execute(
                    "UPDATE agent_runtimes SET started_at=?2,last_heartbeat=?2 WHERE runtime_id=?1",
                    libsql::params![runtime, age],
                )
                .await
                .unwrap();
        }

        async fn extra_runtime(store: &Store, runtime: &str, age: i64) {
            let agent = format!("a_{runtime}");
            Agents::new(store)
                .create(NewAgent {
                    agent_id: agent.clone(),
                    project: "default".into(),
                    name: None,
                    default_harness: None,
                    role: None,
                    tier: None,
                    owner: None,
                })
                .await
                .unwrap();
            AgentRuntimes::new(store)
                .create(NewAgentRuntime {
                    runtime_id: runtime.into(),
                    agent_id: agent,
                    harness: "other".into(),
                    cwd: None,
                    transport: None,
                    presence: Some("online".into()),
                    active: true,
                })
                .await
                .unwrap();
            age_runtime(store, runtime, age).await;
        }

        fn reserve_pair(
            reporting: &ModelReporting,
            runtime: &str,
            agent: &str,
        ) -> ModelObserverHandle {
            reporting
                .reserve(
                    agent.into(),
                    runtime.into(),
                    ModelReportBackend::new("fixture/opaque").unwrap(),
                    ModelCapabilityProfile {
                        configured: ModelEvidenceCapability::Supported,
                        turn_selected: ModelEvidenceCapability::Unsupported,
                        response_reported: ModelEvidenceCapability::Supported,
                    },
                )
                .unwrap()
        }

        async fn sweep_admitted(
            sweep: &mut std::pin::Pin<Box<impl Future<Output = Result<(), NexusError>>>>,
            slot: &RuntimeSlot,
        ) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    assert!(
                        futures::poll!(&mut *sweep).is_pending(),
                        "sweep settled before its held gate"
                    );
                    if *slot.offline_pending.lock().unwrap() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("sweep did not admit its captured slot");
        }

        #[derive(Default)]
        struct CombinedEvents {
            statuses: AtomicUsize,
            projections: AtomicUsize,
            panic_status: AtomicBool,
        }
        #[async_trait]
        impl EventSink for CombinedEvents {
            async fn emit(&self, _: WsEvent) {
                self.statuses.fetch_add(1, Ordering::SeqCst);
                assert!(
                    !self.panic_status.load(Ordering::SeqCst),
                    "combined publication panic"
                );
            }
            async fn project_runtime_binding(&self, _: &SessionId, _: &AgentId) {
                self.projections.fetch_add(1, Ordering::SeqCst);
            }
        }

        async fn combined_fixture() -> (
            tempfile::TempDir,
            Arc<Store>,
            Arc<ModelReporting>,
            PresenceWriter,
            Arc<CombinedEvents>,
        ) {
            let (dir, store, _) = fixture().await;
            create_online_session(&store).await;
            Sessions::new(&store)
                .set_agent_id(&"s_observer".into(), "a_observer")
                .await
                .unwrap();
            store
                .conn
                .execute("UPDATE sessions SET created_at=1,last_heartbeat=1", ())
                .await
                .unwrap();
            age_runtime(&store, "s_observer", 1).await;
            let events = Arc::new(CombinedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let writer =
                PresenceWriter::new(store.clone(), events.clone(), TransportRegistry::new())
                    .with_model_reporting(reporting.clone());
            (dir, store, reporting, writer, events)
        }

        // Store::lock_presence_transition clones the underlying mutex Arc only when entered.
        // Compare counts within ONE synchronous target poll on a current-thread executor: other
        // tasks may run between polls, but cannot contribute a clone during this interval. Repos
        // borrow Store, and cloning Arc<Store> does not clone its private presence mutex Arc.
        async fn witness_presence_wait(
            held: &tokio::sync::OwnedMutexGuard<()>,
            mut target: std::pin::Pin<&mut impl Future>,
        ) {
            assert!(matches!(
                tokio::runtime::Handle::current().runtime_flavor(),
                tokio::runtime::RuntimeFlavor::CurrentThread
            ));
            let mutex = tokio::sync::OwnedMutexGuard::mutex(held);
            tokio::time::timeout(
                Duration::from_secs(2),
                futures::future::poll_fn(|cx| {
                    let before = Arc::strong_count(mutex);
                    // Prevent Tokio's semaphore budget check from yielding before FIFO enqueue.
                    let polled = std::future::Future::poll(
                        std::pin::pin!(tokio::task::unconstrained(target.as_mut())),
                        cx,
                    );
                    assert!(
                        polled.is_pending(),
                        "target settled before entering held presence gate"
                    );
                    let after = Arc::strong_count(mutex);
                    if after == before {
                        return std::task::Poll::Pending;
                    }
                    assert_eq!(
                        after,
                        before + 1,
                        "target must retain exactly one presence-wait Arc"
                    );
                    std::task::Poll::Ready(())
                }),
            )
            .await
            .expect("target never entered the actual presence gate after selecting OLD");
        }

        #[tokio::test(flavor = "current_thread")]
        async fn combined_stale_same_snapshot_protects_both_phases_and_captured_absence() {
            use crate::presence::TransportHandle;
            for attached in [false, true] {
                let (_dir, store, reporting, writer, events) = combined_fixture().await;
                let old = reserve(&reporting).unwrap();
                reporting.commit_claim(&old).await.unwrap();
                extra_runtime(&store, "s_orphan", 1).await;
                let orphan = reserve_pair(&reporting, "s_orphan", "a_s_orphan");
                reporting.commit_claim(&orphan).await.unwrap();
                let registry = writer.registry();
                let session = SessionId("s_observer".into());
                let orphan_session = SessionId("s_orphan".into());
                if attached {
                    registry.attach(&session, TransportHandle::RawStream);
                }
                if attached {
                    registry.attach(&orphan_session, TransportHandle::RawStream);
                }
                let before = row(&store).await;
                let orphan_before = AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_orphan")
                    .await
                    .unwrap();
                let guard = store.lock_presence_transition().await;
                let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
                witness_presence_wait(&guard, sweep.as_mut()).await;
                registry.attach(&session, TransportHandle::RawStream);
                registry.attach(&orphan_session, TransportHandle::RawStream);
                drop(guard);
                sweep.await.unwrap();
                assert_eq!(row(&store).await, before);
                assert!(registry.is_present(&session));
                assert!(registry.is_present(&orphan_session));
                assert_eq!(
                    AgentRuntimes::new(&store)
                        .find_by_runtime_id("s_orphan")
                        .await
                        .unwrap(),
                    orphan_before,
                    "runtime-only phase uses original capture too"
                );
                assert!(!orphan.cell.state.lock().unwrap().closed);
                assert!(!old.cell.state.lock().unwrap().closed);
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
                assert_eq!(events.projections.load(Ordering::SeqCst), 2);
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn combined_stale_selection_refresh_rebinding_and_missing_are_zero_effect() {
            for change in [
                "heartbeat",
                "session_agent",
                "null_to_bound",
                "runtime_agent",
                "missing_to_runtime",
            ] {
                let (_dir, store, reporting, writer, events) = combined_fixture().await;
                let session = SessionId("s_observer".into());
                if change == "null_to_bound" {
                    store
                        .conn
                        .execute("UPDATE sessions SET agent_id=NULL", ())
                        .await
                        .unwrap();
                }
                if change == "missing_to_runtime" {
                    store
                        .identity_conn()
                        .execute("DELETE FROM agent_runtimes", ())
                        .await
                        .unwrap();
                }
                let guard = store.lock_presence_transition().await;
                let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
                witness_presence_wait(&guard, sweep.as_mut()).await;
                match change {
                    "heartbeat" => {
                        store
                            .conn
                            .execute("UPDATE sessions SET last_heartbeat=1000", ())
                            .await
                            .unwrap();
                    }
                    "session_agent" => {
                        store
                            .conn
                            .execute("UPDATE sessions SET agent_id='a_other'", ())
                            .await
                            .unwrap();
                    }
                    "null_to_bound" => {
                        Sessions::new(&store)
                            .set_agent_id(&session, "a_observer")
                            .await
                            .unwrap();
                    }
                    "runtime_agent" => {
                        extra_runtime(&store, "s_other", 1000).await;
                        store
                            .identity_conn()
                            .execute(
                                "UPDATE agent_runtimes SET active=0 WHERE runtime_id='s_other'",
                                (),
                            )
                            .await
                            .unwrap();
                        store.identity_conn().execute("UPDATE agent_runtimes SET agent_id='a_s_other' WHERE runtime_id='s_observer'", ()).await.unwrap();
                    }
                    "missing_to_runtime" => {
                        AgentRuntimes::new(&store)
                            .create(NewAgentRuntime {
                                runtime_id: session.0.clone(),
                                agent_id: "a_observer".into(),
                                harness: "other".into(),
                                cwd: None,
                                transport: None,
                                presence: Some("online".into()),
                                active: true,
                            })
                            .await
                            .unwrap();
                        age_runtime(&store, &session.0, 1).await;
                    }
                    _ => unreachable!(),
                }
                let before = AgentRuntimes::new(&store)
                    .find_by_runtime_id(&session.0)
                    .await
                    .unwrap();
                let before_session = Sessions::new(&store)
                    .find_by_session_id(&session)
                    .await
                    .unwrap();
                drop(guard);
                sweep.await.unwrap();
                assert_eq!(
                    AgentRuntimes::new(&store)
                        .find_by_runtime_id(&session.0)
                        .await
                        .unwrap(),
                    before,
                    "{change}: both phases preserve new selection"
                );
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&session)
                        .await
                        .unwrap(),
                    before_session,
                    "{change}"
                );
                assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
                assert_eq!(events.projections.load(Ordering::SeqCst), 0);
                assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn combined_stale_confirmed_false_session_cas_preserves_cell_cache_and_no_hooks() {
            let (_dir, store, reporting, writer, events) = combined_fixture().await;
            let old = reserve(&reporting).unwrap();
            reporting.commit_claim(&old).await.unwrap();
            let before = row(&store).await;
            let gate = store
                .begin_write_txn("combined_session_cas_gate")
                .await
                .unwrap();
            let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            parked_claim(&old).await;
            gate.execute(
                "UPDATE sessions SET last_heartbeat=1000 WHERE session_id='s_observer'",
                (),
            )
            .await
            .unwrap();
            gate.commit().await.unwrap();
            sweep.await.unwrap();
            assert_eq!(row(&store).await, before);
            assert!(!old.cell.state.lock().unwrap().closed);
            let lane = old.cell.slot.lane.lock().await;
            assert!(!lane.uncertain);
            assert_eq!(lane.confirmed.as_deref(), Some(old.cell.key.token.as_str()));
            drop(lane);
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
            assert_eq!(events.projections.load(Ordering::SeqCst), 1);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn combined_stale_success_missing_runtime_and_settled_unowned_slots_are_bounded() {
            for missing in [false, true] {
                let (_dir, store, reporting, writer, events) = combined_fixture().await;
                if missing {
                    store
                        .identity_conn()
                        .execute("DELETE FROM agent_runtimes", ())
                        .await
                        .unwrap();
                }
                for _ in 0..24 {
                    store
                        .conn
                        .execute("UPDATE sessions SET presence='online',last_heartbeat=1", ())
                        .await
                        .unwrap();
                    writer.reconcile_stale_presence(1000, 100).await.unwrap();
                    assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
                }
                assert_eq!(events.statuses.load(Ordering::SeqCst), 24);
                let calls = events.projections.load(Ordering::SeqCst);
                assert_eq!(calls, if missing { 0 } else { 24 });
                writer.reconcile_stale_presence(1000, 100).await.unwrap();
                assert_eq!(events.statuses.load(Ordering::SeqCst), 24);
                assert_eq!(events.projections.load(Ordering::SeqCst), calls);
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn combined_stale_closed_predecessor_cleanup_can_win_either_order() {
            for cleanup_first in [false, true] {
                let (_dir, store, reporting, writer, _) = combined_fixture().await;
                let old = reserve(&reporting).unwrap();
                reporting.commit_claim(&old).await.unwrap();
                if cleanup_first {
                    reporting.revoke_committed(&old).await.unwrap();
                } else {
                    let lane = old.cell.slot.lane.lock().await;
                    let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
                    sweep_admitted(&mut sweep, &old.cell.slot).await;
                    old.cell.close();
                    drop(lane);
                    sweep.await.unwrap();
                }
                writer.reconcile_stale_presence(1000, 100).await.unwrap();
                assert!(!row(&store).await.active);
                assert!(old.cell.state.lock().unwrap().closed);
                assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn combined_stale_direct_writer_after_session_commit_is_explicit_partial_and_preserves_new(
        ) {
            use crate::presence::TransportHandle;
            let (_dir, store, reporting, writer, events) = combined_fixture().await;
            let old = reserve(&reporting).unwrap();
            reporting.commit_claim(&old).await.unwrap();
            extra_runtime(&store, "s_new", 1000).await;
            let new = reserve_pair(&reporting, "s_new", "a_s_new");
            reporting.commit_claim(&new).await.unwrap();
            let session = SessionId("s_observer".into());
            writer
                .registry()
                .attach(&session, TransportHandle::RawStream);
            let gate = store
                .begin_identity_write_txn("combined_partial_runtime_gate")
                .await
                .unwrap();
            let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            parked_claim(&old).await;
            // Durable session observation is the explicit entered handshake for the second store.
            tokio::time::timeout(Duration::from_secs(2), async {
                while Sessions::new(&store)
                    .find_by_session_id(&session)
                    .await
                    .unwrap()
                    .unwrap()
                    .presence
                    .as_deref()
                    != Some("offline")
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            gate.execute(
                "UPDATE agent_runtimes SET active=0 WHERE runtime_id='s_new'",
                (),
            )
            .await
            .unwrap();
            gate.execute("UPDATE agent_runtimes SET agent_id='a_s_new',model_observer_token='direct-new' WHERE runtime_id='s_observer'", ()).await.unwrap();
            // Deliberate out-of-band local replacement pins the no-late-current-lookup boundary.
            *old.cell.slot.current.lock().unwrap() = Arc::downgrade(&new.cell);
            writer
                .registry()
                .attach(&session, TransportHandle::RawStream);
            gate.commit().await.unwrap();
            let before = row(&store).await;
            let error = sweep.await.unwrap_err().to_string();
            let _ = wait_completed(&old).await;
            assert!(
                error.contains("partial stale transition")
                    && error.contains("runtime binding changed"),
                "{error}"
            );
            assert_eq!(row(&store).await, before);
            assert!(writer.registry().is_present(&session));
            assert!(!new.cell.state.lock().unwrap().closed);
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(old.cell.slot.lane.lock().await.uncertain);
            assert_eq!(
                old.cell.slot.lane.lock().await.confirmed.as_deref(),
                Some(old.cell.key.token.as_str())
            );
            assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("runtime binding changed"));
        }

        #[tokio::test]
        async fn combined_stale_postcommit_failures_retain_original_cause_and_uncertainty() {
            for failure in ["session_append", "runtime_append", "publication_panic"] {
                let (_dir, store, reporting, writer, events) = combined_fixture().await;
                let old = reserve(&reporting).unwrap();
                reporting.commit_claim(&old).await.unwrap();
                let before = row(&store).await;
                let cause = match failure {
                    "session_append" => {
                        store.conn.execute_batch("CREATE TRIGGER combined_fail BEFORE INSERT ON developer_events WHEN NEW.lifecycle='offline' BEGIN SELECT RAISE(ABORT, 'original session append failure'); END;").await.unwrap();
                        "original session append failure"
                    }
                    "runtime_append" => {
                        store.conn.execute_batch("CREATE TRIGGER combined_fail BEFORE INSERT ON developer_events WHEN NEW.lifecycle='stopped' BEGIN SELECT RAISE(ABORT, 'original runtime append failure'); END;").await.unwrap();
                        "original runtime append failure"
                    }
                    _ => {
                        events.panic_status.store(true, Ordering::SeqCst);
                        "combined publication panic"
                    }
                };
                let error = writer
                    .reconcile_stale_presence(1000, 100)
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains("partial stale transition") && error.contains(cause),
                    "{failure}: {error}"
                );
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap()
                        .unwrap()
                        .presence
                        .as_deref(),
                    Some("offline")
                );
                assert!(old.cell.state.lock().unwrap().closed);
                let _ = wait_completed(&old).await;
                assert!(old.cell.slot.lane.lock().await.uncertain);
                assert!(old
                    .cell
                    .slot
                    .error
                    .lock()
                    .unwrap()
                    .as_deref()
                    .unwrap()
                    .contains(cause));
                let after = row(&store).await;
                if failure == "session_append" {
                    assert_eq!(after, before);
                } else {
                    assert!(!after.active);
                    assert!(after.model_observer_token.is_none());
                }
                if failure != "publication_panic" {
                    assert_eq!(
                        old.cell.slot.lane.lock().await.confirmed.as_deref(),
                        Some(old.cell.key.token.as_str()),
                        "postcommit error must not infer NULL"
                    );
                }
                assert!(reporting
                    .shutdown(Duration::from_secs(2))
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains(cause));
            }
        }

        #[tokio::test]
        async fn combined_stale_foreign_missing_and_uncertain_local_admission_have_zero_effect() {
            for mode in ["foreign", "missing", "uncertain"] {
                let (_dir, store, reporting, writer, events) = combined_fixture().await;
                let old = reserve(&reporting).unwrap();
                if mode == "foreign" {
                    store
                        .identity_conn()
                        .execute("UPDATE agent_runtimes SET agent_id='foreign'", ())
                        .await
                        .unwrap();
                }
                if mode == "missing" {
                    store
                        .identity_conn()
                        .execute("DELETE FROM agent_runtimes", ())
                        .await
                        .unwrap();
                }
                if mode == "uncertain" {
                    old.cell.slot.lane.lock().await.uncertain = true;
                }
                let before = AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_observer")
                    .await
                    .unwrap();
                let before_session = Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap();
                assert!(writer.reconcile_stale_presence(1000, 100).await.is_err());
                assert_eq!(
                    AgentRuntimes::new(&store)
                        .find_by_runtime_id("s_observer")
                        .await
                        .unwrap(),
                    before
                );
                assert_eq!(
                    Sessions::new(&store)
                        .find_by_session_id(&"s_observer".into())
                        .await
                        .unwrap(),
                    before_session
                );
                assert!(
                    !old.cell.state.lock().unwrap().closed,
                    "{mode}: rejection precedes closure"
                );
                assert!(
                    !*old.cell.slot.offline_pending.lock().unwrap(),
                    "{mode}: rejection precedes pending"
                );
                assert_eq!(events.statuses.load(Ordering::SeqCst), 0);
                if mode == "uncertain" {
                    old.cell.slot.lane.lock().await.uncertain = false;
                }
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn combined_stale_preexisting_generic_failure_keeps_original_closure_policy() {
            let (_dir, store, reporting, writer, _) = combined_fixture().await;
            let old = reserve(&reporting).unwrap();
            reporting.commit_claim(&old).await.unwrap();
            extra_runtime(&store, "s_foreign", 1000).await;
            let foreign = reserve_pair(&reporting, "s_foreign", "a_s_foreign");
            let lane = old.cell.slot.lane.lock().await;
            let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            *old.cell.slot.error.lock().unwrap() = Some("original generic failure".into());
            *old.cell.slot.current.lock().unwrap() = Arc::downgrade(&foreign.cell);
            drop(lane);
            assert!(sweep
                .await
                .unwrap_err()
                .to_string()
                .contains("original generic failure"));
            let _ = wait_completed(&old).await;
            assert!(!old
                .cell
                .slot
                .captured_failure_closure
                .load(Ordering::Relaxed));
            assert!(
                foreign.cell.state.lock().unwrap().closed,
                "captured secondary failure must not narrow a pre-existing generic closure policy"
            );
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("original generic failure"));
        }

        #[tokio::test]
        async fn actual_stale_secondary_old_failure_preserves_foreign_current_after_captured_closure(
        ) {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            reporting.commit_claim(&old).await.unwrap();
            age_runtime(&store, "s_observer", 1).await;
            extra_runtime(&store, "s_foreign", 1000).await;
            let foreign = reserve_pair(&reporting, "s_foreign", "a_s_foreign");
            store.identity_conn().execute_batch("CREATE TRIGGER captured_stop_failure BEFORE UPDATE OF active ON agent_runtimes WHEN NEW.active=0 BEGIN SELECT RAISE(ABORT, 'captured runtime failure'); END;").await.unwrap();
            let gate = store
                .begin_identity_write_txn("captured_runtime_failure_gate")
                .await
                .unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            parked_claim(&old).await;
            *old.cell.slot.current.lock().unwrap() = Arc::downgrade(&foreign.cell);
            gate.commit().await.unwrap();
            assert!(sweep
                .await
                .unwrap_err()
                .to_string()
                .contains("captured runtime failure"));
            let _ = wait_completed(&old).await;
            assert!(
                !foreign.cell.state.lock().unwrap().closed,
                "secondary worker must preserve the captured-only runtime sweep failure scope"
            );
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("captured runtime failure"));
        }

        #[tokio::test]
        async fn combined_stale_uncertainty_arriving_behind_held_lane_rejects_before_session_write()
        {
            let (_dir, store, reporting, writer, _) = combined_fixture().await;
            let old = reserve(&reporting).unwrap();
            let before = row(&store).await;
            let session_before = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap();
            let mut lane = old.cell.slot.lane.lock().await;
            let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            lane.uncertain = true;
            drop(lane);
            let error = sweep.await.unwrap_err().to_string();
            assert!(
                error.contains("uncertain") && !error.contains("partial stale transition"),
                "{error}"
            );
            assert_eq!(row(&store).await, before);
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap(),
                session_before
            );
            let original = "internal: model observer lane is closed after uncertain settlement";
            assert!(old.cell.slot.lane.lock().await.uncertain);
            assert!(*old.cell.slot.offline_pending.lock().unwrap());
            assert_eq!(
                old.cell.slot.error.lock().unwrap().as_deref(),
                Some(original)
            );
            assert!(error.contains(original));
            wait_completed(&old).await.unwrap();
            assert!(old.cell.slot.lane.lock().await.uncertain);
            assert!(*old.cell.slot.offline_pending.lock().unwrap());
            assert_eq!(
                old.cell.slot.error.lock().unwrap().as_deref(),
                Some(original)
            );
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
        }

        #[tokio::test]
        async fn combined_stale_publication_releases_lane_retains_presence_and_rereads_canonical() {
            let (_dir, store, _, _, _) = combined_fixture().await;
            let events = Arc::new(StaleProjection {
                store: store.clone(),
                armed: AtomicBool::new(false),
                panic: AtomicBool::new(false),
                entered: Notify::new(),
                release: Notify::new(),
                rows: Mutex::new(Vec::new()),
            });
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            reporting.commit_claim(&old).await.unwrap();
            events.rows.lock().unwrap().clear();
            let writer = PresenceWriter::new(
                store.clone(),
                Arc::new(CombinedEvents::default()),
                TransportRegistry::new(),
            )
            .with_model_reporting(reporting.clone());
            events.armed.store(true, Ordering::SeqCst);
            let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result = &mut sweep => panic!("did not enter publication: {result:?}"),
                    _ = events.entered.notified() => {}
                }
            })
            .await
            .unwrap();
            assert!(old.cell.slot.lane.try_lock().unwrap().confirmed.is_none());
            assert!(!row(&store).await.active);
            drop(sweep);
            let mut presence = Box::pin(store.lock_presence_transition());
            assert!(futures::poll!(&mut presence).is_pending());
            let mut newer = old.cell.initial.clone();
            newer.backend = ModelReportBackend::new("fixture/newer").unwrap();
            assert!(AgentRuntimes::new(&store)
                .claim_model_observer("s_observer", "a_observer", None, "new-durable", &newer)
                .await
                .unwrap());
            let canonical = row(&store).await;
            events.release.notify_one();
            drop(
                tokio::time::timeout(Duration::from_secs(2), presence)
                    .await
                    .unwrap(),
            );
            assert_eq!(*events.rows.lock().unwrap(), vec![canonical]);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn combined_stale_publication_failure_retains_lane_through_captured_closure() {
            let (_dir, store, _, _, _) = combined_fixture().await;
            let owner_events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), owner_events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            owner_events.armed.store(true, Ordering::SeqCst);
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            tokio::time::timeout(Duration::from_secs(2), owner_events.entered.notified())
                .await
                .unwrap();
            extra_runtime(&store, "s_foreign", 1000).await;
            let foreign = reserve_pair(&reporting, "s_foreign", "a_s_foreign");
            let status = Arc::new(OfflineStatusGate::default());
            status.panic.store(true, Ordering::SeqCst);
            let writer =
                PresenceWriter::new(store.clone(), status.clone(), TransportRegistry::new())
                    .with_model_reporting(reporting.clone());
            let mut sweep = Box::pin(writer.reconcile_stale_presence(1000, 100));
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result = &mut sweep => panic!("sweep missed status gate: {result:?}"),
                    _ = status.entered.notified() => {}
                }
            })
            .await
            .unwrap();
            assert!(!row(&store).await.active);
            assert!(old.cell.slot.lane.try_lock().unwrap().confirmed.is_none());
            // Out-of-band, separate-slot foreign fixture, not a same-slot NEW usability claim.
            *old.cell.slot.current.lock().unwrap() = Arc::downgrade(&foreign.cell);
            let captured = old.cell.clone();
            let blocker = tokio::task::spawn_blocking(move || {
                let closure_gate = captured.state.lock().unwrap();
                status.release.notify_one();
                let deadline = std::time::Instant::now() + Duration::from_secs(2);
                // OLD's worker is still in its independent claim-publication gate. Only the
                // combined task can hold error while waiting to close the captured state here.
                let entered = loop {
                    if captured.slot.error.try_lock().is_err() {
                        break true;
                    }
                    if std::time::Instant::now() >= deadline {
                        break false;
                    }
                    std::thread::yield_now();
                };
                let retained_lane = captured.slot.lane.try_lock().is_err();
                // Release every blocking fixture guard before reporting any assertion failure.
                drop(closure_gate);
                (entered, retained_lane)
            });
            let witnessed = tokio::time::timeout(Duration::from_secs(5), blocker).await;
            // Always release OLD, including an unexpected witness failure, before assertions.
            owner_events.release.notify_one();
            let (entered, retained_lane) = witnessed.unwrap().unwrap();
            let error = sweep.await.unwrap_err().to_string();
            assert!(claim.await.is_err());
            assert!(wait_completed(&old).await.is_err());
            let shutdown = reporting.shutdown(Duration::from_secs(2));
            // Check foreign before shutdown deliberately closes every current admission.
            let foreign_open = !foreign.cell.state.lock().unwrap().closed;
            let shutdown_error = shutdown.await.unwrap_err().to_string();
            assert!(
                entered,
                "publication failure never entered captured closure gate"
            );
            assert!(retained_lane, "publication failure released lane before captured cause/closure retention completed");
            let cause = "injected offline publication panic";
            assert!(error.contains(cause), "{error}");
            assert!(shutdown_error.contains(cause), "{shutdown_error}");
            assert!(old
                .cell
                .slot
                .error
                .lock()
                .unwrap()
                .as_deref()
                .unwrap()
                .contains(cause));
            assert!(old
                .cell
                .slot
                .captured_failure_closure
                .load(Ordering::Relaxed));
            assert!(old.cell.slot.lane.lock().await.uncertain);
            assert!(*old.cell.slot.offline_pending.lock().unwrap());
            assert!(
                foreign_open,
                "secondary OLD failure must not close foreign current"
            );
        }

        #[tokio::test]
        async fn actual_stale_changed_orphan_closes_captured_owner_and_preserves_sibling() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            age_runtime(&store, "s_observer", 1).await;
            extra_runtime(&store, "s_sibling", 1000).await;
            let sibling = AgentRuntimes::new(&store)
                .find_by_runtime_id("s_sibling")
                .await
                .unwrap()
                .unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            writer.stop_stale_runtimes(1000, 100).await.unwrap();
            assert!(
                old.cell.state.lock().unwrap().closed,
                "confirmed stop must close captured OLD"
            );
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
            assert!(!row(&store).await.active);
            assert_eq!(
                AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_sibling")
                    .await
                    .unwrap()
                    .unwrap(),
                sibling
            );
            let fresh = reserve(&reporting).unwrap();
            assert!(
                reporting.commit_claim(&fresh).await.unwrap(),
                "changed stop establishes NULL predecessor"
            );
            assert!(!reporting.revoke_committed(&old).await.unwrap());
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(fresh.cell.key.token.as_str())
            );
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_stale_refresh_at_identity_gate_skips_without_closing_or_clearing_cache() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            assert!(old.bind_native_root(" root/opaque "));
            age_runtime(&store, "s_observer", 1).await;
            let gate = store
                .begin_identity_write_txn("stale_refresh_gate")
                .await
                .unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            parked_claim(&old).await;
            assert!(
                !old.cell.state.lock().unwrap().closed,
                "selection is not confirmed staleness"
            );
            gate.execute(
                "UPDATE agent_runtimes SET last_heartbeat=1000 WHERE runtime_id='s_observer'",
                (),
            )
            .await
            .unwrap();
            let calls = events.calls.load(Ordering::SeqCst);
            gate.commit().await.unwrap();
            let refreshed = row(&store).await;
            sweep.await.unwrap();
            assert_eq!(row(&store).await, refreshed);
            assert_eq!(
                events.calls.load(Ordering::SeqCst),
                calls,
                "skips publish zero hooks"
            );
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            let lane = old.cell.slot.lane.lock().await;
            assert_eq!(lane.confirmed.as_deref(), Some(old.cell.key.token.as_str()));
            assert!(!lane.uncertain);
            drop(lane);
            assert!(old.observe(update(ModelEvidenceField::Configured, "still usable")));
            let fresh = reserve(&reporting).unwrap();
            assert!(
                reporting.commit_claim(&fresh).await.unwrap(),
                "skip preserves exact predecessor CAS"
            );
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_stale_captures_once_before_newly_stale_sibling() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            age_runtime(&store, "s_observer", 1).await;
            extra_runtime(&store, "s_later", 1000).await;
            let lane = old.cell.slot.lane.lock().await;
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            age_runtime(&store, "s_later", 1).await;
            let before = AgentRuntimes::new(&store)
                .find_by_runtime_id("s_later")
                .await
                .unwrap()
                .unwrap();
            drop(lane);
            sweep.await.unwrap();
            assert_eq!(
                AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_later")
                    .await
                    .unwrap()
                    .unwrap(),
                before
            );
            assert!(old.cell.state.lock().unwrap().closed);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_stale_cancelled_waiter_retains_guard_pending_and_blocks_attachment() {
            use crate::presence::TransportHandle;
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            age_runtime(&store, "s_observer", 1).await;
            let lane = old.cell.slot.lane.lock().await;
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(reserve(&reporting).is_err());
            drop(sweep);
            let session = "s_observer".into();
            let mut attach =
                Box::pin(writer.mark_transport_present(&session, TransportHandle::EventLoop));
            assert!(futures::poll!(&mut attach).is_pending());
            assert!(!writer.registry().is_present(&session));
            assert!(*old.cell.slot.offline_pending.lock().unwrap());
            assert!(reporting.inner.registry.lock().unwrap().tasks >= 2);
            drop(lane);
            tokio::time::timeout(Duration::from_secs(2), attach)
                .await
                .unwrap()
                .unwrap();
            assert!(writer.registry().is_present(&session));
            assert!(
                row(&store).await.active,
                "attachment wins only after tracked stop"
            );
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        async fn stale_rejected_batch(reverse: bool, failed: bool) {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let valid = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&valid).await.unwrap());
            age_runtime(&store, "s_observer", if reverse { 3 } else { 1 }).await;
            extra_runtime(&store, "s_would_create", 2).await;
            extra_runtime(&store, "s_foreign", if reverse { 1 } else { 3 }).await;
            let foreign = reserve_pair(
                &reporting,
                "s_foreign",
                if failed { "a_s_foreign" } else { "a_foreign" },
            );
            if failed {
                *foreign.cell.slot.error.lock().unwrap() = Some("earlier original failure".into());
            }
            let before = row(&store).await;
            let would_create_before = AgentRuntimes::new(&store)
                .find_by_runtime_id("s_would_create")
                .await
                .unwrap();
            let foreign_before = AgentRuntimes::new(&store)
                .find_by_runtime_id("s_foreign")
                .await
                .unwrap();
            let slots = reporting.inner.registry.lock().unwrap().slots.len();
            let writer = offline_writer(store.clone(), reporting.clone());
            assert!(
                writer.stop_stale_runtimes(1000, 100).await.is_err(),
                "whole batch must reject"
            );
            assert_eq!(
                reporting.inner.registry.lock().unwrap().slots.len(),
                slots,
                "rejection cannot install would-create slots"
            );
            assert!(!*valid.cell.slot.offline_pending.lock().unwrap());
            assert!(!*foreign.cell.slot.offline_pending.lock().unwrap());
            assert!(!valid.cell.state.lock().unwrap().closed);
            assert!(!foreign.cell.state.lock().unwrap().closed);
            assert_eq!(
                valid.cell.slot.lane.lock().await.confirmed.as_deref(),
                Some(valid.cell.key.token.as_str())
            );
            assert_eq!(row(&store).await, before);
            assert_eq!(
                AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_would_create")
                    .await
                    .unwrap(),
                would_create_before
            );
            assert_eq!(
                AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_foreign")
                    .await
                    .unwrap(),
                foreign_before
            );
            assert!(foreign.cell.slot.lane.lock().await.confirmed.is_none());
            assert!(valid.bind_native_root(" root/opaque "));
            assert!(valid.observe(update(ModelEvidenceField::Configured, "usable")));
            let result = reporting.shutdown(Duration::from_secs(2)).await;
            assert_eq!(result.is_err(), failed);
        }

        #[tokio::test]
        async fn actual_stale_whole_batch_foreign_owner_rejection_forward_order() {
            stale_rejected_batch(false, false).await;
        }

        #[tokio::test]
        async fn actual_stale_whole_batch_foreign_owner_rejection_reverse_order() {
            stale_rejected_batch(true, false).await;
        }

        #[tokio::test]
        async fn actual_stale_whole_batch_failed_slot_rejection_forward_order() {
            stale_rejected_batch(false, true).await;
        }

        #[tokio::test]
        async fn actual_stale_whole_batch_failed_slot_rejection_reverse_order() {
            stale_rejected_batch(true, true).await;
        }

        #[tokio::test]
        async fn actual_stale_pre_ready_and_shutdown_reject_without_slots_or_writes() {
            let (_dir, store, reporting) = fixture().await;
            age_runtime(&store, "s_observer", 1).await;
            let reporting = Arc::new(reporting);
            let writer = offline_writer(store.clone(), reporting.clone());
            assert!(writer.stop_stale_runtimes(1000, 100).await.is_err());
            reporting.initialize().await.unwrap();
            reporting.close_admission();
            assert!(writer.stop_stale_runtimes(1000, 100).await.is_err());
            assert!(row(&store).await.active);
            assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
        }

        #[tokio::test]
        async fn actual_stale_unowned_successful_slots_are_reclaimed() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            age_runtime(&store, "s_observer", 1).await;
            for index in 0..16 {
                extra_runtime(&store, &format!("s_orphan_{index}"), 1).await;
            }
            offline_writer(store, reporting.clone())
                .stop_stale_runtimes(1000, 100)
                .await
                .unwrap();
            let registry = reporting.inner.registry.lock().unwrap();
            assert_eq!(registry.tasks, 0);
            assert!(registry.slots.is_empty());
        }

        async fn stale_store_failure(postcommit: bool) {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            age_runtime(&store, "s_observer", 1).await;
            extra_runtime(&store, "s_ownerless_failure", 2).await;
            let cause = if postcommit {
                store
                    .identity_conn()
                    .execute(
                        "UPDATE agents SET name='observer' WHERE agent_id='a_observer'",
                        (),
                    )
                    .await
                    .unwrap();
                store.conn.execute_batch("CREATE TRIGGER fail_stale_lifecycle BEFORE INSERT ON developer_events WHEN NEW.lifecycle='stopped' BEGIN SELECT RAISE(ABORT, 'original postcommit lifecycle failure'); END;").await.unwrap();
                "original postcommit lifecycle failure"
            } else {
                "authority"
            };
            let before = row(&store).await;
            let gate = store
                .begin_identity_write_txn("stale_store_failure_gate")
                .await
                .unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            parked_claim(&old).await;
            if !postcommit {
                // Corrupt only after candidate capture, so this pins transaction rollback and
                // tracked local settlement rather than a harmless pre-admission scan error.
                gate.execute("UPDATE agent_runtimes SET model_observer_token='' WHERE runtime_id='s_ownerless_failure'", ()).await.unwrap();
            }
            gate.commit().await.unwrap();
            let error = sweep.await.unwrap_err().to_string();
            assert!(error.contains(cause), "{error}");
            assert!(
                old.cell.state.lock().unwrap().closed,
                "unknown outcome must fence captured local authority"
            );
            assert!(!old.bind_native_root(" root/opaque "));
            assert!(reserve(&reporting).is_err());
            let slots: Vec<_> = reporting
                .inner
                .registry
                .lock()
                .unwrap()
                .slots
                .values()
                .cloned()
                .collect();
            assert_eq!(
                slots.len(),
                2,
                "ownerless failed participant must remain retained"
            );
            for slot in &slots {
                assert!(slot
                    .error
                    .lock()
                    .unwrap()
                    .as_deref()
                    .is_some_and(|value| value.contains(cause)));
                assert!(*slot.offline_pending.lock().unwrap());
                assert!(slot.lane.lock().await.uncertain);
            }
            assert_eq!(
                old.cell.slot.lane.lock().await.confirmed.as_deref(),
                Some(old.cell.key.token.as_str()),
                "error cannot infer a NULL predecessor"
            );
            let after = row(&store).await;
            if postcommit {
                assert!(!after.active);
                assert!(after.model_observer_token.is_none());
                assert_eq!(
                    after.model_report_revision,
                    before.model_report_revision + 1
                );
            } else {
                assert_eq!(
                    after, before,
                    "invalid authority rolls back earlier selected rows"
                );
            }
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains(cause));
        }

        #[tokio::test]
        async fn actual_stale_invalid_authority_closes_captured_cells_and_retains_rollback_error() {
            stale_store_failure(false).await;
        }

        #[tokio::test]
        async fn actual_stale_postcommit_lifecycle_failure_retains_uncertainty_and_original_cause()
        {
            stale_store_failure(true).await;
        }

        struct StaleProjection {
            store: Arc<Store>,
            armed: AtomicBool,
            panic: AtomicBool,
            entered: Notify,
            release: Notify,
            rows: Mutex<Vec<nexus_store::types::AgentRuntimeRow>>,
        }

        #[async_trait]
        impl EventSink for StaleProjection {
            async fn emit(&self, _: WsEvent) {
                panic!("runtime-only sweep emitted compatibility status");
            }
            async fn project_runtime_binding(&self, runtime: &SessionId, agent: &AgentId) {
                if self.armed.swap(false, Ordering::SeqCst) {
                    self.entered.notify_one();
                    self.release.notified().await;
                    assert!(
                        !self.panic.load(Ordering::SeqCst),
                        "postcommit projection panic"
                    );
                }
                let row = AgentRuntimes::new(&self.store)
                    .find_by_runtime_id(&runtime.0)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(row.agent_id, agent.0);
                self.rows.lock().unwrap().push(row);
            }
        }

        async fn stale_projection(panic: bool) {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(StaleProjection {
                store: store.clone(),
                armed: AtomicBool::new(false),
                panic: AtomicBool::new(panic),
                entered: Notify::new(),
                release: Notify::new(),
                rows: Mutex::new(Vec::new()),
            });
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            events.rows.lock().unwrap().clear();
            age_runtime(&store, "s_observer", 1).await;
            extra_runtime(&store, "s_fresh_projection", 1000).await;
            events.armed.store(true, Ordering::SeqCst);
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result = &mut sweep => panic!("sweep settled without entering projection: {result:?}"),
                    _ = events.entered.notified() => {}
                }
            }).await.unwrap();
            assert!(!row(&store).await.active);
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(
                old.cell.slot.lane.try_lock().unwrap().confirmed.is_none(),
                "projection must release model lane"
            );
            assert!(*old.cell.slot.offline_pending.lock().unwrap());
            drop(sweep);
            let mut presence = Box::pin(store.lock_presence_transition());
            assert!(futures::poll!(&mut presence).is_pending());
            // Hook spy, not a WsSink/Gateway claim: this deliberately bypasses presence to pin
            // its canonical read after a newer durable report has won.
            let mut newer = old.cell.initial.clone();
            newer.backend = ModelReportBackend::new("fixture/newer").unwrap();
            assert!(AgentRuntimes::new(&store)
                .claim_model_observer("s_observer", "a_observer", None, "new-durable", &newer)
                .await
                .unwrap());
            let canonical = row(&store).await;
            events.release.notify_one();
            drop(
                tokio::time::timeout(Duration::from_secs(2), presence)
                    .await
                    .unwrap(),
            );
            let result = reporting.shutdown(Duration::from_secs(2)).await;
            if panic {
                assert!(result.unwrap_err().to_string().contains("panicked"));
                assert!(*old.cell.slot.offline_pending.lock().unwrap());
            } else {
                result.unwrap();
                assert_eq!(events.rows.lock().unwrap().as_slice(), &[canonical]);
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            }
        }

        #[tokio::test]
        async fn actual_stale_changed_only_projection_rereads_newer_durable_report() {
            stale_projection(false).await;
        }

        #[tokio::test]
        async fn actual_stale_projection_panic_is_postcommit_and_retained() {
            stale_projection(true).await;
        }

        #[tokio::test]
        async fn actual_stale_reversed_scan_order_acquires_sorted_lanes_before_identity_write() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            age_runtime(&store, "s_observer", 2).await;
            extra_runtime(&store, "s_z_first_in_scan", 1).await;
            let first_lane = reserve(&reporting).unwrap();
            let last_lane = reserve_pair(&reporting, "s_z_first_in_scan", "a_s_z_first_in_scan");
            let selected = AgentRuntimes::new(&store)
                .stale_active_runtime_pairs(1000, 100)
                .await
                .unwrap();
            assert_eq!(selected[0].0, "s_z_first_in_scan");
            let held_last = last_lane.cell.slot.lane.lock().await;
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            sweep_admitted(&mut sweep, &first_lane.cell.slot).await;
            // A wrong scan-order locker parks at Z without ever acquiring O. The held Z gate
            // makes O's actual lock ownership a deterministic witness, bounded without a hang.
            parked_claim(&first_lane).await;
            assert!(
                row(&store).await.active,
                "all lanes precede the transaction"
            );
            drop(held_last);
            sweep.await.unwrap();
            assert!(first_lane.cell.state.lock().unwrap().closed);
            assert!(last_lane.cell.state.lock().unwrap().closed);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_stale_late_uncertain_lane_rejects_before_any_store_write() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            age_runtime(&store, "s_observer", 1).await;
            extra_runtime(&store, "s_z_uncertain", 2).await;
            let last = reserve_pair(&reporting, "s_z_uncertain", "a_s_z_uncertain");
            last.cell.slot.lane.lock().await.uncertain = true;
            let before = row(&store).await;
            let writer = offline_writer(store.clone(), reporting.clone());
            assert!(writer.stop_stale_runtimes(1000, 100).await.is_err());
            assert_eq!(row(&store).await, before);
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(last.cell.state.lock().unwrap().closed);
            assert!(old.cell.slot.error.lock().unwrap().is_some());
            assert!(last.cell.slot.error.lock().unwrap().is_some());
            assert!(reporting.shutdown(Duration::from_secs(2)).await.is_err());
        }

        #[tokio::test]
        async fn actual_stale_matching_closed_owner_allows_cleanup_or_stop_to_win() {
            for cleanup_first in [true, false] {
                let (_dir, store, _) = fixture().await;
                let events = Arc::new(GatedEvents::default());
                let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                events.armed.store(!cleanup_first, Ordering::SeqCst);
                let mut claim = Box::pin(reporting.commit_claim(&old));
                if cleanup_first {
                    assert!(claim.await.unwrap());
                    old.revoke();
                    assert!(wait_completed(&old).await.unwrap());
                } else {
                    assert!(futures::poll!(&mut claim).is_pending());
                    events.entered.notified().await;
                    old.revoke();
                    drop(claim);
                }
                age_runtime(&store, "s_observer", 1).await;
                let writer = offline_writer(store.clone(), reporting.clone());
                writer.stop_stale_runtimes(1000, 100).await.unwrap();
                assert!(!row(&store).await.active);
                assert!(old.cell.state.lock().unwrap().closed);
                assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
                events.release.notify_one();
                wait_completed(&old).await.unwrap();
                let fresh = reserve(&reporting).unwrap();
                assert!(reporting.commit_claim(&fresh).await.unwrap());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn actual_stale_store_error_closes_and_records_original_cause_before_lane_release() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            events.armed.store(true, Ordering::SeqCst);
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            age_runtime(&store, "s_observer", 1).await;
            store.identity_conn().execute_batch("CREATE TRIGGER fail_stale_stop BEFORE UPDATE OF active ON agent_runtimes WHEN NEW.active=0 BEGIN SELECT RAISE(ABORT, 'lane-owned original stale failure'); END;").await.unwrap();
            let gate = store
                .begin_identity_write_txn("stale_original_error_gate")
                .await
                .unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            sweep_admitted(&mut sweep, &old.cell.slot).await;
            parked_claim(&old).await;
            let inner = reporting.inner.clone();
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = std::thread::spawn(move || {
                let _registry = inner.registry.lock().unwrap();
                entered_tx.send(()).unwrap();
                let _ = release_rx.recv_timeout(Duration::from_secs(5));
            });
            entered_rx.await.unwrap();
            let mut barrier = Box::pin(old.cell.slot.lane.lock());
            assert!(futures::poll!(&mut barrier).is_pending());
            gate.commit().await.unwrap();
            let lane = tokio::time::timeout(Duration::from_secs(2), barrier)
                .await
                .unwrap();
            let error_at_release = old.cell.slot.error.lock().unwrap().clone();
            let closed_at_release = old.cell.state.lock().unwrap().closed;
            let uncertain_at_release = lane.uncertain;
            let cache_at_release = lane.confirmed.clone();
            drop(lane);
            release_tx.send(()).unwrap();
            blocker.join().unwrap();
            events.release.notify_one();
            assert!(
                error_at_release
                    .as_deref()
                    .is_some_and(|error| error.contains("lane-owned original stale failure")),
                "original cause absent before outer registry settlement: {error_at_release:?}"
            );
            assert!(
                closed_at_release,
                "local closure must precede lane release, not outer completion"
            );
            assert!(uncertain_at_release);
            assert_eq!(
                cache_at_release.as_deref(),
                Some(old.cell.key.token.as_str())
            );
            assert!(sweep
                .await
                .unwrap_err()
                .to_string()
                .contains("lane-owned original stale failure"));
            assert!(claim.await.is_err());
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("lane-owned original stale failure"));
        }

        #[tokio::test]
        async fn actual_stale_does_not_mutate_compatibility_presence_or_transport() {
            use crate::presence::TransportHandle;
            use nexus_store::repos::Sessions;
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            Sessions::new(&store)
                .set_presence(&"s_observer".into(), nexus_contracts::Presence::Offline)
                .await
                .unwrap();
            let before = Sessions::new(&store)
                .find_by_session_id(&"s_observer".into())
                .await
                .unwrap()
                .unwrap();
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            writer
                .registry()
                .attach(&"s_observer".into(), TransportHandle::RawStream);
            writer.stop_stale_runtimes(1000, 100).await.unwrap();
            assert!(
                !row(&store).await.active,
                "explicit offline remains stale even with recent start/heartbeat"
            );
            assert!(old.cell.state.lock().unwrap().closed);
            assert_eq!(
                Sessions::new(&store)
                    .find_by_session_id(&"s_observer".into())
                    .await
                    .unwrap()
                    .unwrap(),
                before
            );
            assert!(writer.registry().is_present(&"s_observer".into()));
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_stale_invalid_candidate_ids_reject_whole_batch_without_effects() {
            for bad_runtime in [true, false] {
                let (_dir, store, reporting) = fixture().await;
                let reporting = Arc::new(reporting);
                reporting.initialize().await.unwrap();
                let old = reserve(&reporting).unwrap();
                age_runtime(&store, "s_observer", 1).await;
                extra_runtime(&store, "s_invalid_candidate", 2).await;
                if bad_runtime {
                    store.identity_conn().execute("UPDATE agent_runtimes SET runtime_id=char(10) WHERE runtime_id='s_invalid_candidate'", ()).await.unwrap();
                } else {
                    Agents::new(&store)
                        .create(NewAgent {
                            agent_id: "\n".into(),
                            project: "default".into(),
                            name: None,
                            default_harness: None,
                            role: None,
                            tier: None,
                            owner: None,
                        })
                        .await
                        .unwrap();
                    store.identity_conn().execute("UPDATE agent_runtimes SET agent_id=char(10) WHERE runtime_id='s_invalid_candidate'", ()).await.unwrap();
                }
                let before = row(&store).await;
                let writer = offline_writer(store.clone(), reporting.clone());
                assert!(
                    writer.stop_stale_runtimes(1000, 100).await.is_err(),
                    "invalid selected identity must reject"
                );
                assert_eq!(row(&store).await, before);
                assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), 1);
                assert!(!old.cell.state.lock().unwrap().closed);
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
                reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            }
        }

        #[tokio::test]
        async fn actual_stale_pending_participant_rejects_without_partial_admission() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            age_runtime(&store, "s_observer", 1).await;
            extra_runtime(&store, "s_would_create", 2).await;
            extra_runtime(&store, "s_pending", 3).await;
            let pending = reserve_pair(&reporting, "s_pending", "a_s_pending");
            *pending.cell.slot.offline_pending.lock().unwrap() = true;
            let before = row(&store).await;
            let writer = offline_writer(store.clone(), reporting.clone());
            assert!(writer.stop_stale_runtimes(1000, 100).await.is_err());
            assert_eq!(row(&store).await, before);
            assert_eq!(reporting.inner.registry.lock().unwrap().slots.len(), 2);
            assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            assert!(*pending.cell.slot.offline_pending.lock().unwrap());
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(!pending.cell.state.lock().unwrap().closed);
            *pending.cell.slot.offline_pending.lock().unwrap() = false;
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_stale_mixed_batch_closes_and_projects_only_confirmed_changed_pair() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(StaleProjection {
                store: store.clone(),
                armed: AtomicBool::new(false),
                panic: AtomicBool::new(false),
                entered: Notify::new(),
                release: Notify::new(),
                rows: Mutex::new(Vec::new()),
            });
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let changed = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&changed).await.unwrap());
            age_runtime(&store, "s_observer", 1).await;
            extra_runtime(&store, "s_skip", 2).await;
            let skipped = reserve_pair(&reporting, "s_skip", "a_s_skip");
            assert!(reporting.commit_claim(&skipped).await.unwrap());
            events.rows.lock().unwrap().clear();
            let gate = store
                .begin_identity_write_txn("stale_mixed_batch_gate")
                .await
                .unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let mut sweep = Box::pin(writer.stop_stale_runtimes(1000, 100));
            sweep_admitted(&mut sweep, &changed.cell.slot).await;
            parked_claim(&skipped).await;
            gate.execute(
                "UPDATE agent_runtimes SET last_heartbeat=1000 WHERE runtime_id='s_skip'",
                (),
            )
            .await
            .unwrap();
            gate.commit().await.unwrap();
            let refreshed = AgentRuntimes::new(&store)
                .find_by_runtime_id("s_skip")
                .await
                .unwrap()
                .unwrap();
            sweep.await.unwrap();
            assert!(changed.cell.state.lock().unwrap().closed);
            assert!(!skipped.cell.state.lock().unwrap().closed);
            assert_eq!(
                skipped.cell.slot.lane.lock().await.confirmed.as_deref(),
                Some(skipped.cell.key.token.as_str())
            );
            assert!(!*skipped.cell.slot.offline_pending.lock().unwrap());
            assert_eq!(
                AgentRuntimes::new(&store)
                    .find_by_runtime_id("s_skip")
                    .await
                    .unwrap()
                    .unwrap(),
                refreshed
            );
            let projected = events.rows.lock().unwrap().clone();
            assert_eq!(projected.len(), 1);
            assert_eq!(projected[0].runtime_id, "s_observer");
            assert!(!projected[0].active);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_offline_closes_unclaimed_old_before_fresh_staged_claim() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(old.bind_native_root(" root/opaque "));
            assert!(old.observe(update(ModelEvidenceField::Configured, "OLD")));
            let writer = offline_writer(store.clone(), reporting.clone());
            writer
                .materialize_offline(&"s_observer".into())
                .await
                .unwrap();
            assert!(
                !reporting.commit_claim(&old).await.unwrap(),
                "offline must close the valid unclaimed OLD reservation"
            );
            assert!(!old.bind_native_root(" root/opaque "));
            assert!(!reporting.activate(&old, " root/opaque "));
            assert!(!old.observe(update(ModelEvidenceField::Configured, "late OLD")));
            let stopped = row(&store).await;
            assert!(!stopped.active);
            assert!(stopped.model_observer_token.is_none());
            assert!(stopped.model_report.is_none());
            let fresh = reserve(&reporting).unwrap();
            assert!(
                Arc::ptr_eq(&old.cell.slot, &fresh.cell.slot),
                "settlement GC must retain a slot captured by OLD"
            );
            assert!(reporting.commit_claim(&fresh).await.unwrap());
            let before = row(&store).await;
            assert!(!before.model_report.as_ref().unwrap().observer_active);
            assert!(!reporting.revoke_committed(&old).await.unwrap());
            assert_eq!(
                row(&store).await.model_report_revision,
                before.model_report_revision
            );
            assert_eq!(
                row(&store).await.model_observer_token,
                before.model_observer_token
            );
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_offline_only_sessions_reclaim_settled_unowned_slots() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let writer = offline_writer(store, reporting.clone());
            for index in 0..32 {
                writer
                    .materialize_offline(&format!("s_unobserved_{index}").into())
                    .await
                    .unwrap();
            }
            let registry = reporting.inner.registry.lock().unwrap();
            assert_eq!(registry.tasks, 0);
            assert!(registry.slots.is_empty(), "offline-only traffic must not require a future observer reserve to reclaim {} slots", registry.slots.len());
        }

        #[tokio::test]
        async fn actual_offline_gc_preserves_slot_owned_only_by_parked_worker() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            events.armed.store(true, Ordering::SeqCst);
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            let completed = old.cell.completed.subscribe();
            let captured = Arc::downgrade(&old.cell.slot);
            let writer = offline_writer(store, reporting.clone());
            writer
                .materialize_offline(&"s_observer".into())
                .await
                .unwrap();
            drop(claim);
            drop(old);
            writer
                .materialize_offline(&"s_gc_control".into())
                .await
                .unwrap();
            {
                let registry = reporting.inner.registry.lock().unwrap();
                assert_eq!(
                    registry.tasks, 1,
                    "only the publication-gated OLD worker remains"
                );
                assert_eq!(registry.slots.len(), 1);
                let slot = registry.slots.get(&SessionId("s_observer".into())).unwrap();
                assert!(captured.ptr_eq(&Arc::downgrade(slot)));
            }
            events.release.notify_one();
            assert!(!wait_outcome(completed).await.unwrap());
            writer
                .materialize_offline(&"s_gc_after_worker".into())
                .await
                .unwrap();
            assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
            assert!(captured.upgrade().is_none());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn actual_offline_records_stop_failure_before_lane_release() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            events.armed.store(true, Ordering::SeqCst);
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            store.identity_conn().execute_batch("CREATE TRIGGER fail_stop BEFORE UPDATE OF active ON agent_runtimes WHEN NEW.active = 0 BEGIN SELECT RAISE(ABORT, 'lane-owned original stop failure'); END;").await.unwrap();
            let gate = store
                .begin_identity_write_txn("park_offline_stop_failure")
                .await
                .unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let session = "s_observer".into();
            let mut offline = Box::pin(writer.materialize_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            parked_claim(&old).await;

            // Block only outer registry settlement. The store stop already owns its runtime lane
            // and is parked at the explicit identity gate; neither OLD nor stop can publish an
            // error through their registry-locked completion path until this test releases it.
            let inner = reporting.inner.clone();
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = std::thread::spawn(move || {
                let _registry = inner.registry.lock().unwrap();
                entered_tx.send(()).unwrap();
                let _ = release_rx.recv_timeout(Duration::from_secs(5));
            });
            entered_rx.await.unwrap();
            // Register the root-task barrier before releasing the transaction. Keep OLD's
            // publication parked: assigning the lane to a worker deliberately blocked by the
            // registry gate would test executor starvation rather than error publication order.
            let mut lane_barrier = Box::pin(old.cell.slot.lane.lock());
            assert!(futures::poll!(&mut lane_barrier).is_pending());
            gate.commit().await.unwrap();
            let lane = tokio::time::timeout(Duration::from_secs(2), lane_barrier)
                .await
                .unwrap();
            let retained_at_release = old.cell.slot.error.lock().unwrap().clone();
            assert!(lane.uncertain);
            drop(lane);
            release_tx.send(()).unwrap();
            blocker.join().unwrap();
            events.release.notify_one();
            assert!(retained_at_release.as_deref().is_some_and(|error| error.contains("lane-owned original stop failure")), "stop must retain its cause before unlocking its lane, independently of outer completion: {retained_at_release:?}");
            assert!(offline.await.is_err());
            assert!(claim.await.is_err());
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("lane-owned original stop failure"));
        }

        #[tokio::test]
        async fn actual_offline_cancelled_waiter_retains_presence_guard_until_settled() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let events = Arc::new(Events);
            let writer =
                PresenceWriter::new(store.clone(), events.clone(), TransportRegistry::new())
                    .with_model_reporting(reporting.clone());
            let session = "s_observer".into();
            // Explicit release gate: stop cannot own the lane until this guard is dropped.
            let lane = old.cell.slot.lane.lock().await;
            let mut offline = Box::pin(writer.materialize_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            assert!(
                old.cell.state.lock().unwrap().closed,
                "admission must synchronously close OLD before any store await"
            );
            assert!(
                reserve(&reporting).is_err(),
                "pending stop must reject new reservations"
            );
            drop(offline);
            // This historical control proves the offline delegate retains SAME-Store presence,
            // even against a legacy writer. Managed activation after shutdown is separately
            // required to reject; the offline operation itself remains managed here.
            let online_writer = PresenceWriter::new(store.clone(), events, writer.registry());
            let mut online = Box::pin(online_writer.materialize_online(&session));
            assert!(
                futures::poll!(&mut online).is_pending(),
                "cancelled waiter must not release the owned presence guard"
            );
            assert!(reporting.shutdown(Duration::ZERO).await.is_err());
            drop(lane);
            tokio::time::timeout(Duration::from_secs(2), online)
                .await
                .unwrap()
                .unwrap();
            let restored = row(&store).await;
            assert!(
                restored.active,
                "online must happen after the cancelled offline settlement"
            );
            assert_eq!(restored.presence.as_deref(), Some("online"));
            assert!(restored.model_observer_token.is_none());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_offline_failure_retains_closed_lane_and_shutdown_error() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            store.identity_conn().execute_batch("CREATE TRIGGER fail_stop BEFORE UPDATE OF active ON agent_runtimes WHEN NEW.active = 0 BEGIN SELECT RAISE(ABORT, 'injected offline stop failure'); END;").await.unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let error = writer
                .materialize_offline(&"s_observer".into())
                .await
                .unwrap_err();
            assert!(error.to_string().contains("injected offline stop failure"));
            assert!(
                old.cell.state.lock().unwrap().closed,
                "failed stop must close local OLD"
            );
            assert!(
                reserve(&reporting).is_err(),
                "failed stop cannot reopen its lane"
            );
            wait_completed(&old).await.unwrap();
            drop(old);
            for index in 0..4 {
                writer
                    .materialize_offline(&format!("s_other_{index}").into())
                    .await
                    .unwrap();
            }
            {
                let registry = reporting.inner.registry.lock().unwrap();
                assert_eq!(registry.slots.len(), 1, "GC must keep unowned failed/uncertain authority but collect successful unrelated slots");
                let failed = registry.slots.get(&SessionId("s_observer".into())).unwrap();
                assert!(failed.error.lock().unwrap().is_some());
                assert!(failed.lane.try_lock().unwrap().uncertain);
            }
            let error = reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("injected offline stop failure"));
            assert!(row(&store).await.active);
        }

        #[tokio::test]
        async fn actual_offline_pre_ready_rejects_without_mutation() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            let writer = offline_writer(store.clone(), reporting.clone());
            assert!(writer
                .materialize_offline(&"s_observer".into())
                .await
                .is_err());
            assert!(row(&store).await.active);
            assert_eq!(row(&store).await.model_report_revision, 0);
            assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
        }

        #[tokio::test]
        async fn actual_offline_after_parked_claim_wins_before_fresh_owner() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let gate = store
                .begin_identity_write_txn("offline_park_claim")
                .await
                .unwrap();
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            parked_claim(&old).await;
            let writer = offline_writer(store.clone(), reporting.clone());
            let session = "s_observer".into();
            let mut offline = Box::pin(writer.materialize_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            assert!(old.cell.state.lock().unwrap().closed);
            gate.commit().await.unwrap();
            offline.await.unwrap();
            assert!(!claim.await.unwrap());
            let stopped = row(&store).await;
            assert!(stopped.model_report_revision >= 2);
            assert!(!stopped.active);
            assert!(stopped.model_observer_token.is_none());
            let fresh = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&fresh).await.unwrap());
            assert!(!reporting.revoke_committed(&old).await.unwrap());
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(fresh.cell.key.token.as_str())
            );
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_offline_serializes_after_entered_apply() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            assert!(old.bind_native_root(" root/opaque "));
            assert!(old.observe(update(ModelEvidenceField::Configured, "OLD")));
            let gate = store
                .begin_identity_write_txn("offline_park_apply")
                .await
                .unwrap();
            assert!(reporting.activate(&old, " root/opaque "));
            parked_claim(&old).await;
            let writer = offline_writer(store.clone(), reporting.clone());
            let session = "s_observer".into();
            let mut offline = Box::pin(writer.materialize_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            assert!(old.cell.state.lock().unwrap().closed);
            gate.commit().await.unwrap();
            offline.await.unwrap();
            let stopped = row(&store).await;
            let revision = stopped.model_report_revision;
            assert_eq!(revision, 3);
            let report = stopped.model_report.unwrap();
            assert!(!report.observer_active);
            assert!(
                matches!(report.configured, ModelEvidenceSlot::Observed { observation, .. } if observation.model_id == "OLD")
            );
            assert!(stopped.model_observer_token.is_none());
            assert!(!old.observe(update(ModelEvidenceField::Configured, "late")));
            wait_completed(&old).await.unwrap();
            assert_eq!(
                row(&store).await.model_report_revision,
                revision,
                "no late OLD apply or cleanup may change stopped report"
            );
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_offline_fences_production_apply_path_parked_before_lane_admission() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            events.armed.store(true, Ordering::SeqCst);
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            assert!(old.bind_native_root(" root/opaque "));
            assert!(old.observe(update(ModelEvidenceField::Configured, "OLD")));
            assert!(reporting.activate(&old, " root/opaque "));
            let snapshot = old.cell.state.lock().unwrap().snapshot();
            let lane = old.cell.slot.lane.lock().await;
            // Drive the exact private apply path called by run_owner. The spawned owner remains
            // explicitly parked in publication: this pins apply-path admission, not worker snapshot
            // consumption. Polling Pending proves this snapshot is waiting at the runtime lane.
            let mut apply = Box::pin(reporting.inner.apply_snapshot(&old.cell, &snapshot));
            assert!(futures::poll!(&mut apply).is_pending());
            let writer = offline_writer(store.clone(), reporting.clone());
            let session = "s_observer".into();
            let mut offline = Box::pin(writer.materialize_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            assert!(old.cell.state.lock().unwrap().closed);
            drop(lane);
            assert_eq!(
                apply.await.unwrap(),
                None,
                "closed local authority must reject before any durable CAS"
            );
            offline.await.unwrap();
            let stopped = row(&store).await;
            assert_eq!(stopped.model_report_revision, 2);
            assert!(stopped.model_observer_token.is_none());
            let report = stopped.model_report.unwrap();
            assert!(!report.observer_active);
            assert!(matches!(
                report.configured,
                ModelEvidenceSlot::Unknown { .. }
            ));
            events.release.notify_one();
            assert!(!claim.await.unwrap());
            assert!(!wait_completed(&old).await.unwrap());
            assert_eq!(row(&store).await.model_report_revision, 2);
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_offline_cancelled_before_admission_has_no_effect() {
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let session = "s_observer".into();
            let presence = store.lock_presence_transition().await;
            let mut offline = Box::pin(writer.materialize_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            drop(offline);
            drop(presence);
            assert!(!old.cell.state.lock().unwrap().closed);
            assert!(row(&store).await.active);
            assert!(reporting.commit_claim(&old).await.unwrap());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn actual_offline_failed_stop_keeps_confirmed_token_and_original_error() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            events.armed.store(true, Ordering::SeqCst);
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            store.identity_conn().execute_batch("CREATE TRIGGER fail_stop BEFORE UPDATE OF active ON agent_runtimes WHEN NEW.active = 0 BEGIN SELECT RAISE(ABORT, 'original offline stop failure'); END;").await.unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            assert!(writer
                .materialize_offline(&"s_observer".into())
                .await
                .unwrap_err()
                .to_string()
                .contains("original offline stop failure"));
            assert_eq!(
                old.cell.slot.lane.lock().await.confirmed.as_deref(),
                Some(old.cell.key.token.as_str())
            );
            assert!(old.cell.slot.lane.lock().await.uncertain);
            events.release.notify_one();
            assert!(claim.await.is_err());
            assert!(wait_completed(&old).await.is_err());
            let error = reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("original offline stop failure"),
                "shutdown must retain the initiating stop error: {error}"
            );
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(old.cell.key.token.as_str())
            );
        }

        #[tokio::test]
        async fn actual_offline_late_old_callback_cannot_mutate_fresh_confirmed_owner() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            events.armed.store(true, Ordering::SeqCst);
            let mut claim = Box::pin(reporting.commit_claim(&old));
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            let writer = offline_writer(store.clone(), reporting.clone());
            writer
                .materialize_offline(&"s_observer".into())
                .await
                .unwrap();
            let stopped = row(&store).await;
            assert_eq!(stopped.model_report_revision, 2);
            assert!(old.cell.slot.lane.lock().await.confirmed.is_none());
            let fresh = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&fresh).await.unwrap());
            let revision = row(&store).await.model_report_revision;
            events.release.notify_one();
            assert!(!claim.await.unwrap());
            assert!(!wait_completed(&old).await.unwrap());
            assert_eq!(
                fresh.cell.slot.lane.lock().await.confirmed.as_deref(),
                Some(fresh.cell.key.token.as_str())
            );
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(fresh.cell.key.token.as_str())
            );
            assert_eq!(row(&store).await.model_report_revision, revision);
            assert!(fresh.bind_native_root(" root/opaque "));
            assert!(reporting.activate(&fresh, " root/opaque "));
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[derive(Default)]
        struct OfflineStatusGate {
            entered: Notify,
            release: Notify,
            panic: AtomicBool,
            recorded: Mutex<Vec<WsEvent>>,
        }

        #[async_trait]
        impl EventSink for OfflineStatusGate {
            async fn emit(&self, event: WsEvent) {
                let offline = matches!(
                    event,
                    WsEvent::AgentStatus {
                        presence: nexus_contracts::Presence::Offline,
                        ..
                    }
                );
                self.recorded.lock().unwrap().push(event);
                if offline {
                    self.entered.notify_one();
                    self.release.notified().await;
                    assert!(
                        !self.panic.load(Ordering::SeqCst),
                        "injected offline publication panic"
                    );
                }
            }
        }

        async fn create_online_session(store: &Store) {
            use nexus_store::repos::{NewSession, Sessions};
            Sessions::new(store)
                .create(NewSession {
                    session_id: "s_observer".into(),
                    name: Some("observer".into()),
                    agent: Some("other".into()),
                    kind: "agent".into(),
                    role: None,
                    tier: "agent".into(),
                    harness_session_id: None,
                    client_key: Some("ck_observer".into()),
                    cwd: None,
                    project: "default".into(),
                    transport: None,
                })
                .await
                .unwrap();
            Sessions::new(store)
                .set_presence(&"s_observer".into(), nexus_contracts::Presence::Online)
                .await
                .unwrap();
        }

        async fn offline_publication_settlement(panic: bool) {
            use crate::daemon::services::presence::{PresenceWriter, TransportRegistry};
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            let events = Arc::new(OfflineStatusGate::default());
            events.panic.store(panic, Ordering::SeqCst);
            let writer =
                PresenceWriter::new(store.clone(), events.clone(), TransportRegistry::new())
                    .with_model_reporting(reporting.clone());
            let session = "s_observer".into();
            let mut offline = Box::pin(writer.materialize_offline(&session));
            assert!(futures::poll!(&mut offline).is_pending());
            tokio::time::timeout(Duration::from_secs(2), events.entered.notified())
                .await
                .unwrap();
            assert!(!row(&store).await.active);
            assert!(
                old.cell.slot.lane.try_lock().unwrap().confirmed.is_none(),
                "post-commit status must not hold model lane"
            );
            assert!(
                reserve(&reporting).is_err(),
                "status publication remains part of offline settlement"
            );
            drop(offline);
            // A poisoned managed lane must reject activation. Only the panic branch uses an
            // unmanaged contender to preserve this historical presence-guard ordering control;
            // the successful settlement still exercises actual managed online activation.
            let online_writer = if panic {
                PresenceWriter::new(store.clone(), events.clone(), writer.registry())
            } else {
                writer.clone()
            };
            let mut online = Box::pin(online_writer.materialize_online(&session));
            assert!(
                futures::poll!(&mut online).is_pending(),
                "presence guard must outlive the cancelled waiter through publication"
            );
            events.release.notify_one();
            tokio::time::timeout(Duration::from_secs(2), online)
                .await
                .unwrap()
                .unwrap();
            let settled = reporting.shutdown(Duration::from_secs(2)).await;
            if panic {
                assert!(settled.unwrap_err().to_string().contains("panicked"));
                assert!(*old.cell.slot.offline_pending.lock().unwrap());
            } else {
                settled.unwrap();
                assert!(!*old.cell.slot.offline_pending.lock().unwrap());
            }
            assert!(row(&store).await.active);
            assert_eq!(
                events.recorded.lock().unwrap().as_slice(),
                &[
                    WsEvent::AgentStatus {
                        session_id: session.clone(),
                        presence: nexus_contracts::Presence::Offline,
                        paused: false
                    },
                    WsEvent::AgentStatus {
                        session_id: session,
                        presence: nexus_contracts::Presence::Online,
                        paused: false
                    },
                ]
            );
        }

        #[tokio::test]
        async fn actual_offline_cancelled_during_status_retains_guard_without_lane() {
            offline_publication_settlement(false).await;
        }

        #[tokio::test]
        async fn actual_offline_prepare_failure_does_not_claim_stop_success() {
            let (_dir, store, reporting) = fixture().await;
            create_online_session(&store).await;
            let reporting = Arc::new(reporting);
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            store.conn.execute_batch("CREATE TRIGGER fail_offline_presence BEFORE UPDATE OF presence ON sessions WHEN NEW.presence = 'offline' BEGIN SELECT RAISE(ABORT, 'injected offline prepare failure'); END;").await.unwrap();
            let writer = offline_writer(store.clone(), reporting.clone());
            let error = writer
                .materialize_offline(&"s_observer".into())
                .await
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("injected offline prepare failure"));
            assert!(old.cell.state.lock().unwrap().closed);
            assert!(reserve(&reporting).is_err());
            assert!(*old.cell.slot.offline_pending.lock().unwrap());
            let runtime = row(&store).await;
            assert!(runtime.active);
            assert_eq!(runtime.model_report_revision, 0);
            assert!(runtime.stopped_at.is_none());
            assert!(reporting
                .shutdown(Duration::from_secs(2))
                .await
                .unwrap_err()
                .to_string()
                .contains("injected offline prepare failure"));
        }

        #[tokio::test]
        async fn actual_offline_publication_panic_keeps_lane_closed_and_shutdown_error() {
            offline_publication_settlement(true).await;
        }

        #[tokio::test]
        async fn pre_ready_reserve_has_zero_ticket_or_store_effects() {
            let (_dir, store, reporting) = fixture().await;
            assert!(reserve(&reporting).is_err());
            assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
            let row = AgentRuntimes::new(&store)
                .find_by_runtime_id("s_observer")
                .await
                .unwrap()
                .unwrap();
            assert!(row.model_observer_token.is_none());
            assert_eq!(row.model_report_revision, 0);
            reporting.initialize().await.unwrap();
            let handle = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&handle).await.unwrap());
            assert!(reporting.revoke_committed(&handle).await.unwrap());
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        async fn row(store: &Store) -> nexus_store::types::AgentRuntimeRow {
            AgentRuntimes::new(store)
                .find_by_runtime_id("s_observer")
                .await
                .unwrap()
                .unwrap()
        }

        fn update(field: ModelEvidenceField, model: &str) -> NativeModelUpdate {
            NativeModelUpdate {
                native_session_id: " root/opaque ".into(),
                field,
                value: ModelEvidenceValue::Observed(ModelObservation {
                    model_id: model.into(),
                    provider_id: None,
                    source: ModelObservationSource::new("fixture/source").unwrap(),
                    observed_at: 42,
                    native_session_id: Some(" root/opaque ".into()),
                    native_turn_id: None,
                    native_message_id: None,
                    native_reported_at: None,
                }),
            }
        }

        async fn parked_claim(handle: &ModelObserverHandle) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if handle.cell.slot.lane.try_lock().is_err() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("claim never entered the actual store write gate");
        }

        #[tokio::test]
        async fn old_reservation_cannot_claim_after_new_claim_revoke_null_cycle() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let new = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&new).await.unwrap());
            assert!(reporting.revoke_committed(&new).await.unwrap());
            let before = row(&store).await.model_report_revision;
            assert!(!reporting.commit_claim(&old).await.unwrap());
            assert!(!reporting.activate(&old, " root/opaque "));
            assert_eq!(row(&store).await.model_report_revision, before);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn superseded_during_real_claim_cleans_exact_token_before_new_claim() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let gate = store.begin_identity_write_txn("park_claim").await.unwrap();
            let claim = reporting.commit_claim(&old);
            tokio::pin!(claim);
            assert!(futures::poll!(&mut claim).is_pending());
            parked_claim(&old).await;
            let new = reserve(&reporting).unwrap();
            gate.commit().await.unwrap();
            assert!(!claim.await.unwrap());
            let settled = row(&store).await;
            assert!(settled.model_observer_token.is_none());
            assert_eq!(settled.model_report_revision, 2);
            assert!(reporting.commit_claim(&new).await.unwrap());
            assert!(!reporting.revoke_committed(&old).await.unwrap());
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(new.cell.key.token.as_str())
            );
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn staged_metadata_is_private_and_slots_merge_independently() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(h.bind_native_root(" root/opaque "));
            assert!(h.observe(update(ModelEvidenceField::Configured, "A")));
            assert!(h.observe(update(ModelEvidenceField::ResponseReported, "B")));
            assert!(h.observe(update(ModelEvidenceField::Configured, "C")));
            assert!(reporting.commit_claim(&h).await.unwrap());
            let initial = row(&store).await.model_report.unwrap();
            assert!(!initial.observer_active);
            assert!(matches!(
                initial.configured,
                ModelEvidenceSlot::Unknown { .. }
            ));
            assert!(matches!(
                initial.response_reported,
                ModelEvidenceSlot::Unknown { .. }
            ));
            assert!(reporting.activate(&h, " root/opaque "));
            tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let report = row(&store).await.model_report.unwrap();
            if report.observer_active {
                assert!(matches!(report.configured, ModelEvidenceSlot::Observed { observation, .. } if observation.model_id == "C"));
                assert!(matches!(report.response_reported, ModelEvidenceSlot::Observed { observation, .. } if observation.model_id == "B"));
                assert!(matches!(report.turn_selected, ModelEvidenceSlot::Unknown { capability: ModelEvidenceCapability::Unsupported, reason: Some(ModelUnknownReason::AwaitingNativeMetadata) }));
                break;
            }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn exact_root_validation_and_closed_handoff_have_no_notifications() {
            let (_dir, _store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(!h.bind_native_root(""));
            assert!(h.bind_native_root(" root/opaque "));
            assert!(h.bind_native_root(" root/opaque "));
            assert!(!h.bind_native_root("root/opaque"));
            assert!(!h.observe(update(ModelEvidenceField::TurnSelected, "unsupported")));
            let mut foreign = update(ModelEvidenceField::Configured, "A");
            foreign.native_session_id = "foreign".into();
            assert!(!h.observe(foreign));
            let mut invalid = update(ModelEvidenceField::Configured, " ");
            assert!(!h.observe(invalid.clone()));
            invalid.value = ModelEvidenceValue::Observed(ModelObservation {
                native_session_id: Some("foreign".into()),
                ..match update(ModelEvidenceField::Configured, "A").value {
                    ModelEvidenceValue::Observed(o) => o,
                    _ => unreachable!(),
                }
            });
            assert!(!h.observe(invalid));
            h.revoke();
            let seq = h.cell.state.lock().unwrap().sequence;
            assert!(!h.bind_native_root(" root/opaque "));
            assert!(!h.observe(update(ModelEvidenceField::Configured, "A")));
            assert_eq!(h.cell.state.lock().unwrap().sequence, seq);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn cancelled_unactivated_claim_settles_and_shutdown_timeout_is_honest() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            let gate = store.begin_identity_write_txn("park_claim").await.unwrap();
            {
                let claim = reporting.commit_claim(&h);
                tokio::pin!(claim);
                assert!(futures::poll!(&mut claim).is_pending());
                parked_claim(&h).await;
            }
            assert!(!reporting.activate(&h, " root/opaque "));
            assert!(reporting.shutdown(Duration::ZERO).await.is_err());
            assert!(reserve(&reporting).is_err());
            gate.commit().await.unwrap();
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            assert!(row(&store).await.model_observer_token.is_none());
        }

        #[tokio::test]
        async fn initialize_invalidates_survivor_once_and_repeated_calls_keep_new_owner() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            let initial = h.cell.initial.clone();
            drop(h);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
            // Seed the previous process's durable owner without leaving a second live coordinator.
            assert!(AgentRuntimes::new(&store)
                .claim_model_observer(
                    "s_observer",
                    "a_observer",
                    None,
                    "surviving-owner",
                    &initial,
                )
                .await
                .unwrap());
            let revision = row(&store).await.model_report_revision;
            let next_boot = ModelReporting::new(store.clone(), Arc::new(Events));
            let (a, b) = tokio::join!(next_boot.initialize(), next_boot.initialize());
            a.unwrap();
            b.unwrap();
            assert!(row(&store).await.model_observer_token.is_none());
            assert_eq!(row(&store).await.model_report_revision, revision + 1);
            let next = reserve(&next_boot).unwrap();
            assert!(next_boot.commit_claim(&next).await.unwrap());
            next_boot.initialize().await.unwrap();
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(next.cell.key.token.as_str())
            );
            next_boot.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[derive(Default)]
        struct GatedEvents {
            armed: AtomicBool,
            entered: Notify,
            release: Notify,
            calls: AtomicUsize,
        }

        #[async_trait]
        impl EventSink for GatedEvents {
            async fn emit(&self, _: WsEvent) {
                panic!("unexpected compatibility event")
            }
            async fn project_runtime_binding(&self, session: &SessionId, agent: &AgentId) {
                assert_eq!(session.0, "s_observer");
                assert_eq!(agent.0, "a_observer");
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.armed.swap(false, Ordering::SeqCst) {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
            }
        }

        async fn wait_completed(h: &ModelObserverHandle) -> Result<bool, NexusError> {
            tokio::time::timeout(
                Duration::from_secs(2),
                wait_outcome(h.cell.completed.subscribe()),
            )
            .await
            .unwrap()
        }

        #[tokio::test]
        async fn cancellation_before_claim_enters_lane_performs_zero_writes() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            let lane = h.cell.slot.lane.lock().await;
            {
                let claim = reporting.commit_claim(&h);
                tokio::pin!(claim);
                assert!(futures::poll!(&mut claim).is_pending());
            }
            assert!(
                h.cell.state.lock().unwrap().closed,
                "cancelled unactivated claimant must close synchronously"
            );
            drop(lane);
            assert!(!wait_completed(&h).await.unwrap());
            assert_eq!(row(&store).await.model_report_revision, 0);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn cancellation_after_commit_during_publication_cleans_only_old_token() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            events.armed.store(true, Ordering::SeqCst);
            let reporting = ModelReporting::new(store.clone(), events.clone());
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            {
                let claim = reporting.commit_claim(&old);
                tokio::pin!(claim);
                assert!(futures::poll!(&mut claim).is_pending());
                events.entered.notified().await;
                assert_eq!(
                    row(&store).await.model_observer_token.as_deref(),
                    Some(old.cell.key.token.as_str())
                );
            }
            let new = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&new).await.unwrap());
            events.release.notify_one();
            assert!(!wait_completed(&old).await.unwrap());
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(new.cell.key.token.as_str())
            );
            assert!(!reporting.activate(&old, " root/opaque "));
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn abandoned_committed_handle_is_closed_and_exactly_cleaned() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&h).await.unwrap());
            let completed = h.cell.completed.subscribe();
            drop(h);
            assert!(wait_outcome(completed).await.unwrap());
            assert!(row(&store).await.model_observer_token.is_none());
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn cleanup_failure_closes_lane_and_is_retained_without_caller() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&old).await.unwrap());
            store.identity_conn().execute_batch("CREATE TRIGGER fail_revoke BEFORE UPDATE OF model_observer_token ON agent_runtimes WHEN NEW.model_observer_token IS NULL BEGIN SELECT RAISE(ABORT, 'injected exact cleanup failure'); END;").await.unwrap();
            let new = reserve(&reporting).unwrap();
            assert!(wait_completed(&old).await.is_err());
            assert!(reporting.commit_claim(&new).await.is_err());
            assert!(reserve(&reporting).is_err());
            assert!(reporting.shutdown(Duration::from_secs(1)).await.is_err());
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(old.cell.key.token.as_str())
            );
        }

        #[tokio::test]
        async fn blocked_store_keeps_latest_snapshot_and_sync_handoff_immediate() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(h.bind_native_root(" root/opaque "));
            assert!(reporting.commit_claim(&h).await.unwrap());
            let gate = store.begin_identity_write_txn("park_apply").await.unwrap();
            assert!(reporting.activate(&h, " root/opaque "));
            parked_claim(&h).await;
            for value in 0..10_000 {
                assert!(h.observe(update(ModelEvidenceField::Configured, &value.to_string())));
            }
            let latest = h.cell.latest.borrow().clone();
            assert_eq!(latest.sequence, 10_001);
            assert!(
                matches!(latest.report.configured, ModelEvidenceSlot::Observed { observation, .. } if observation.model_id == "9999")
            );
            assert_eq!(h.cell.latest.receiver_count(), 1);
            h.revoke();
            assert!(!h.observe(update(ModelEvidenceField::Configured, "late")));
            assert!(reporting.shutdown(Duration::ZERO).await.is_err());
            gate.commit().await.unwrap();
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            let final_row = row(&store).await;
            assert!(final_row.model_observer_token.is_none());
            assert!(!final_row.model_report.unwrap().observer_active);
            assert_eq!(final_row.model_report_revision, 3); // claim, admitted apply, exact revoke
        }

        #[tokio::test]
        async fn blocked_event_sink_releases_lane_and_does_not_block_sync_revoke() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = ModelReporting::new(store.clone(), events.clone());
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(h.bind_native_root(" root/opaque "));
            assert!(reporting.commit_claim(&h).await.unwrap());
            events.armed.store(true, Ordering::SeqCst);
            assert!(reporting.activate(&h, " root/opaque "));
            events.entered.notified().await;
            assert!(h.cell.slot.lane.try_lock().is_ok());
            for value in 0..1000 {
                assert!(h.observe(update(ModelEvidenceField::Configured, &value.to_string())));
            }
            h.revoke();
            assert!(!h.bind_native_root(" root/opaque "));
            assert!(reporting.shutdown(Duration::ZERO).await.is_err());
            events.release.notify_one();
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
            assert!(row(&store).await.model_observer_token.is_none());
        }

        #[tokio::test]
        async fn revoke_before_lane_parked_apply_prevents_the_apply_write() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(h.bind_native_root(" root/opaque "));
            assert!(reporting.commit_claim(&h).await.unwrap());
            let lane = h.cell.slot.lane.lock().await;
            assert!(reporting.activate(&h, " root/opaque "));
            h.revoke();
            drop(lane);
            assert!(reporting.revoke_committed(&h).await.unwrap());
            assert_eq!(row(&store).await.model_report_revision, 2);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn initialization_partial_failure_retry_and_orphan_owner_inventory() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            let initial = h.cell.initial.clone();
            drop(h);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
            let repo = AgentRuntimes::new(&store);
            assert!(repo
                .claim_model_observer("s_observer", "a_observer", None, "surviving", &initial)
                .await
                .unwrap());
            // Explicit corrupt/orphan authority fixture; boot must not rely on Agents::list.
            store.identity_conn().execute_batch("INSERT INTO agent_runtimes (runtime_id, agent_id, harness, active, presence, started_at, model_observer_token) VALUES ('z_orphan', 'a_missing', 'other', 1, 'online', 0, '');").await.unwrap();
            let boot = ModelReporting::new(store.clone(), Arc::new(Events));
            assert!(boot.initialize().await.is_err());
            assert!(reserve(&boot).is_err());
            assert!(row(&store).await.model_observer_token.is_none());
            let revision = row(&store).await.model_report_revision;
            store.identity_conn().execute("UPDATE agent_runtimes SET model_observer_token='orphan-owner', model_report_revision=1 WHERE runtime_id='z_orphan'", ()).await.unwrap();
            boot.initialize().await.unwrap();
            assert_eq!(row(&store).await.model_report_revision, revision);
            assert!(repo
                .find_by_runtime_id("z_orphan")
                .await
                .unwrap()
                .unwrap()
                .model_observer_token
                .is_none());
            boot.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn shutdown_during_initialization_publication_never_reopens_admission() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            let initial = h.cell.initial.clone();
            drop(h);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
            AgentRuntimes::new(&store)
                .claim_model_observer("s_observer", "a_observer", None, "survivor", &initial)
                .await
                .unwrap();
            let events = Arc::new(GatedEvents::default());
            events.armed.store(true, Ordering::SeqCst);
            let boot = ModelReporting::new(store.clone(), events.clone());
            {
                let initialize = boot.initialize();
                tokio::pin!(initialize);
                assert!(futures::poll!(&mut initialize).is_pending());
                events.entered.notified().await;
                assert!(boot.shutdown(Duration::ZERO).await.is_err());
            }
            events.release.notify_one();
            boot.shutdown(Duration::from_secs(2)).await.unwrap();
            assert!(reserve(&boot).is_err());
            assert!(boot.initialize().await.is_err());
            assert!(!boot.inner.registry.lock().unwrap().ready);
        }

        #[tokio::test]
        async fn boot_guard_abort_during_real_initialization_never_reopens_admission() {
            use crate::boot_readiness::{run_boot, BootReadiness, InitialPresenceOutcome};
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            let initial = h.cell.initial.clone();
            drop(h);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
            AgentRuntimes::new(&store)
                .claim_model_observer("s_observer", "a_observer", None, "survivor", &initial)
                .await
                .unwrap();
            let events = Arc::new(GatedEvents::default());
            events.armed.store(true, Ordering::SeqCst);
            let boot = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            let readiness = BootReadiness::pending();
            let captured = boot.clone();
            let guard = readiness.guard(move || captured.close_admission());
            let running = boot.clone();
            let task = tokio::spawn(async move {
                run_boot(
                    guard,
                    running.initialize(),
                    async { Ok(()) },
                    async { Ok(InitialPresenceOutcome::default()) },
                    async {},
                )
                .await
            });
            events.entered.notified().await;
            let mut same_initialization = Box::pin(boot.initialize());
            assert!(futures::poll!(&mut same_initialization).is_pending());
            assert_eq!(boot.inner.registry.lock().unwrap().tasks, 1);
            assert_eq!(events.calls.load(Ordering::SeqCst), 1);
            let mut ingress = Box::pin(readiness.wait_ingress());
            let mut presence = Box::pin(readiness.wait_initial_presence());
            assert!(futures::poll!(&mut ingress).is_pending());
            assert!(futures::poll!(&mut presence).is_pending());
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(ingress.await.is_err());
            assert!(presence.await.is_err());
            assert!(readiness.wait_ingress().await.is_err());
            assert!(readiness.wait_initial_presence().await.is_err());
            assert!(boot.inner.registry.lock().unwrap().shutdown);
            assert!(reserve(&boot).is_err());
            assert!(
                boot.shutdown(Duration::ZERO).await.is_err(),
                "guard does not claim async settlement finished"
            );
            events.release.notify_one();
            boot.shutdown(Duration::from_secs(2)).await.unwrap();
            same_initialization.await.unwrap();
            assert!(!boot.inner.registry.lock().unwrap().ready);
            assert!(boot.initialize().await.is_err());
            assert!(reserve(&boot).is_err());
        }

        #[tokio::test]
        async fn successful_boot_guard_keeps_exact_coordinator_open_and_initialization_single_flight(
        ) {
            use crate::boot_readiness::{run_boot, BootReadiness, InitialPresenceOutcome};
            let (_dir, _store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            let readiness = BootReadiness::pending();
            let captured = reporting.clone();
            let guard = readiness.guard(move || captured.close_admission());
            run_boot(
                guard,
                reporting.initialize(),
                async { Ok(()) },
                async { Ok(InitialPresenceOutcome::default()) },
                async {},
            )
            .await
            .unwrap();
            readiness.wait_ingress().await.unwrap();
            let handle = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&handle).await.unwrap());
            // A repeated initialize on this same coordinator must not sweep its new owner.
            let (first, second) = tokio::join!(reporting.initialize(), reporting.initialize());
            first.unwrap();
            second.unwrap();
            assert!(handle.cell.current());
            assert!(!reporting.inner.registry.lock().unwrap().shutdown);
            reporting.close_admission();
            assert!(
                handle.cell.state.lock().unwrap().closed,
                "synchronous close closes existing exact cells"
            );
            assert!(reserve(&reporting).is_err());
            reporting.shutdown(Duration::from_secs(2)).await.unwrap();
        }

        #[tokio::test]
        async fn never_polled_boot_guard_closes_its_captured_coordinator_only() {
            use crate::boot_readiness::{run_boot, BootReadiness, InitialPresenceOutcome};
            let (_dir, store, reporting) = fixture().await;
            let reporting = Arc::new(reporting);
            let unrelated = ModelReporting::new(store, Arc::new(Events));
            let readiness = BootReadiness::pending();
            let captured = reporting.clone();
            let guard = readiness.guard(move || captured.close_admission());
            let running = reporting.clone();
            let task = tokio::spawn(async move {
                run_boot(
                    guard,
                    running.initialize(),
                    async { Ok(()) },
                    async { Ok(InitialPresenceOutcome::default()) },
                    async {},
                )
                .await
            });
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(readiness.wait_ingress().await.is_err());
            assert!(readiness.wait_initial_presence().await.is_err());
            assert!(reporting.initialize().await.is_err());
            assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            assert!(!unrelated.inner.registry.lock().unwrap().shutdown);
        }

        #[tokio::test]
        async fn boot_captured_revoke_cannot_touch_replaced_token() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            let initial = h.cell.initial.clone();
            drop(h);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
            AgentRuntimes::new(&store)
                .claim_model_observer("s_observer", "a_observer", None, "captured", &initial)
                .await
                .unwrap();
            let boot = ModelReporting::new(store.clone(), Arc::new(Events));
            let gate = store
                .begin_identity_write_txn("park_boot_exact_revoke")
                .await
                .unwrap();
            // Poll the real boot worker directly to its write-gate await: every preceding local libsql
            // read completes synchronously, so the captured key is fixed before this future yields.
            let sweep = boot.inner.initialize_boot();
            tokio::pin!(sweep);
            assert!(futures::poll!(&mut sweep).is_pending());
            gate.execute("UPDATE agent_runtimes SET model_observer_token='replacement' WHERE runtime_id='s_observer'", ()).await.unwrap();
            gate.commit().await.unwrap();
            assert!(sweep.await.is_err());
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some("replacement")
            );
            assert!(!boot.inner.registry.lock().unwrap().ready);
            // Retry is the sole explicit boot retry, not a normal-operation current-owner query.
            boot.initialize().await.unwrap();
            boot.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn invalid_ready_reservations_create_no_tickets_or_writes() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            for (agent, runtime, backend) in [
                ("", "s_observer", "fixture"),
                ("a_observer", "\n", "fixture"),
                ("a_observer", "s_observer", "unknown"),
            ] {
                assert!(reporting
                    .reserve(
                        agent.into(),
                        runtime.into(),
                        ModelReportBackend::new(backend).unwrap(),
                        ModelCapabilityProfile {
                            configured: ModelEvidenceCapability::Supported,
                            turn_selected: ModelEvidenceCapability::Unverified,
                            response_reported: ModelEvidenceCapability::Unsupported,
                        }
                    )
                    .is_err());
            }
            assert!(reporting.inner.registry.lock().unwrap().slots.is_empty());
            assert_eq!(reporting.inner.registry.lock().unwrap().tasks, 0);
            assert_eq!(row(&store).await.model_report_revision, 0);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        struct PanickingEvents;
        #[async_trait]
        impl EventSink for PanickingEvents {
            async fn emit(&self, _: WsEvent) {
                unreachable!()
            }
            async fn project_runtime_binding(&self, _: &SessionId, _: &AgentId) {
                panic!("injected publication panic")
            }
        }

        #[derive(Default)]
        struct GatedOldPanic {
            first: AtomicBool,
            panicking: AtomicBool,
            entered: Notify,
            release: Notify,
            successor_published: Notify,
        }

        #[async_trait]
        impl EventSink for GatedOldPanic {
            async fn emit(&self, _: WsEvent) {
                unreachable!()
            }

            async fn project_runtime_binding(&self, _: &SessionId, _: &AgentId) {
                if !self.first.swap(true, Ordering::SeqCst) {
                    self.entered.notify_one();
                    self.release.notified().await;
                    self.panicking.store(true, Ordering::SeqCst);
                    panic!("injected OLD publication failure after successor claim");
                }
                self.successor_published.notify_one();
            }
        }

        async fn old_publication_failure_closes_successor(activate_successor: bool) {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedOldPanic::default());
            let reporting = ModelReporting::new(store.clone(), events.clone());
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let claim = reporting.commit_claim(&old);
            tokio::pin!(claim);
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            let new = reserve(&reporting).unwrap();
            assert!(new.bind_native_root(" root/opaque "));
            assert!(reporting.commit_claim(&new).await.unwrap());
            events.successor_published.notified().await;
            if activate_successor {
                assert!(reporting.activate(&new, " root/opaque "));
                events.successor_published.notified().await;
            }
            let successor_row = row(&store).await;
            assert_eq!(
                successor_row.model_observer_token.as_deref(),
                Some(new.cell.key.token.as_str())
            );
            assert!(!new.cell.state.lock().unwrap().closed);
            events.release.notify_one();
            assert!(claim.await.is_err());
            assert!(new.cell.slot.error.lock().unwrap().is_some());
            assert!(
                new.cell.state.lock().unwrap().closed,
                "recorded OLD failure must synchronously close the current successor"
            );
            assert!(!reporting.activate(&new, " root/opaque "));
            assert!(!new.bind_native_root(" root/opaque "));
            assert!(!new.observe(update(ModelEvidenceField::Configured, "must-reject")));
            assert!(reserve(&reporting).is_err());
            // Closure wakes an idle successor; error settlement must not wait for new metadata.
            assert!(wait_completed(&new).await.is_err());
            assert!(reporting.shutdown(Duration::from_secs(1)).await.is_err());
            let failed = row(&store).await;
            assert_eq!(
                failed.model_observer_token,
                successor_row.model_observer_token
            );
            assert_eq!(
                failed.model_report_revision,
                successor_row.model_report_revision
            );
        }

        #[tokio::test]
        async fn old_publication_panic_closes_committed_unactivated_successor() {
            old_publication_failure_closes_successor(false).await;
        }

        #[tokio::test]
        async fn old_publication_panic_wakes_and_closes_idle_active_successor() {
            old_publication_failure_closes_successor(true).await;
        }

        fn wait_until_gate_entered(mut entered: impl FnMut() -> bool) {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !entered() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "concurrent operation did not enter its actual lock gate"
                );
                std::thread::yield_now();
            }
        }

        async fn reservation_racing_terminal_failure(failure_first: bool) {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedOldPanic::default());
            let reporting = Arc::new(ModelReporting::new(store.clone(), events.clone()));
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let claim = reporting.commit_claim(&old);
            tokio::pin!(claim);
            assert!(futures::poll!(&mut claim).is_pending());
            events.entered.notified().await;
            let before = row(&store).await;
            let slot = old.cell.slot.clone();
            let runtime = tokio::runtime::Handle::current();
            let reserve_reporting = reporting.clone();
            let attempted = tokio::task::spawn_blocking(move || {
                // Gate actual production mutex acquisition, not a timing guess or a store fake.
                // Every blocking guard is scoped entirely inside this synchronous closure.
                std::thread::scope(|scope| {
                    let current_gate = slot.current.lock().unwrap();
                    if failure_first {
                        events.release.notify_one();
                        // No reservation exists yet: only fail_owner can own this error lock.
                        // It therefore already owns registry and is blocked on current_gate.
                        wait_until_gate_entered(|| slot.error.try_lock().is_err());
                        if reserve_reporting.inner.registry.try_lock().is_ok() {
                            // Release the test gate before failing, without poisoning production locks.
                            drop(current_gate);
                            panic!("terminal failure must hold the reservation registry until current closure completes");
                        }
                    }
                    let (entered, receipt) = std::sync::mpsc::sync_channel(1);
                    let worker_reporting = reserve_reporting.clone();
                    let reserving = scope.spawn(move || {
                        let _runtime = runtime.enter();
                        entered.send(()).unwrap();
                        reserve(&worker_reporting)
                    });
                    receipt.recv_timeout(Duration::from_secs(2)).unwrap();
                    if !failure_first {
                        // OLD is still parked in the sink: this lock is held by actual reserve,
                        // after its failure check but before publishing the new current ticket.
                        wait_until_gate_entered(|| {
                            reserve_reporting.inner.registry.try_lock().is_err()
                        });
                        events.release.notify_one();
                        wait_until_gate_entered(|| events.panicking.load(Ordering::SeqCst));
                    }
                    drop(current_gate);
                    reserving.join().unwrap()
                })
            });
            let attempted = tokio::time::timeout(Duration::from_secs(5), attempted)
                .await
                .unwrap()
                .unwrap();
            assert!(claim.await.is_err());
            if failure_first {
                assert!(
                    attempted.is_err(),
                    "reservation after terminal failure must create no ticket"
                );
                assert_eq!(
                    old.cell
                        .slot
                        .current
                        .lock()
                        .unwrap()
                        .upgrade()
                        .unwrap()
                        .key
                        .token,
                    old.cell.key.token
                );
            } else {
                let successor =
                    attempted.expect("reservation already held registry before OLD failed");
                assert!(
                    successor.cell.state.lock().unwrap().closed,
                    "an admitted racing reservation must not escape terminal closure"
                );
                assert!(reporting.commit_claim(&successor).await.is_err());
                assert!(!reporting.activate(&successor, " root/opaque "));
                assert!(!successor.bind_native_root(" root/opaque "));
                assert!(!successor.observe(update(ModelEvidenceField::Configured, "must-reject")));
            }
            assert!(reserve(&reporting).is_err());
            assert!(reporting.shutdown(Duration::from_secs(1)).await.is_err());
            let after = row(&store).await;
            assert_eq!(after.model_observer_token, before.model_observer_token);
            assert_eq!(after.model_report_revision, before.model_report_revision);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn reserve_holding_registry_before_failure_cannot_leave_open_ticket() {
            reservation_racing_terminal_failure(false).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn failure_holding_registry_before_reserve_admits_no_ticket() {
            reservation_racing_terminal_failure(true).await;
        }

        #[tokio::test]
        async fn panic_after_claim_retains_failure_and_cannot_reset_lane() {
            let (_dir, store, _) = fixture().await;
            let reporting = ModelReporting::new(store.clone(), Arc::new(PanickingEvents));
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&h).await.is_err());
            assert!(!reporting.activate(&h, " root/opaque "));
            let token = h.cell.key.token.clone();
            drop(h);
            assert!(reserve(&reporting).is_err());
            assert!(reporting.shutdown(Duration::from_secs(1)).await.is_err());
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(token.as_str())
            );
        }

        #[tokio::test]
        async fn settled_slots_are_reclaimed_without_discarding_captured_handles() {
            let (_dir, _store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let retained = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&retained).await.unwrap());
            assert!(reporting.revoke_committed(&retained).await.unwrap());
            let slot = retained.cell.slot.clone();
            let next = reserve(&reporting).unwrap();
            assert!(Arc::ptr_eq(&slot, &next.cell.slot));
            drop(next);
            for index in 0..100 {
                let h = reporting
                    .reserve(
                        "a_observer".into(),
                        format!("s_missing_{index}").into(),
                        ModelReportBackend::new("fixture").unwrap(),
                        ModelCapabilityProfile {
                            configured: ModelEvidenceCapability::Unverified,
                            turn_selected: ModelEvidenceCapability::Unverified,
                            response_reported: ModelEvidenceCapability::Unverified,
                        },
                    )
                    .unwrap();
                assert!(!reporting.commit_claim(&h).await.unwrap());
                assert!(!wait_completed(&h).await.unwrap());
            }
            assert!(reporting.inner.registry.lock().unwrap().slots.len() <= 2);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn superseded_claim_cleanup_is_durable_before_publication_blocks() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            events.armed.store(true, Ordering::SeqCst);
            let reporting = ModelReporting::new(store.clone(), events.clone());
            reporting.initialize().await.unwrap();
            let old = reserve(&reporting).unwrap();
            let gate = store
                .begin_identity_write_txn("park_old_claim")
                .await
                .unwrap();
            let claim = reporting.commit_claim(&old);
            tokio::pin!(claim);
            assert!(futures::poll!(&mut claim).is_pending());
            parked_claim(&old).await;
            let new = reserve(&reporting).unwrap();
            gate.commit().await.unwrap();
            events.entered.notified().await;
            let settled = row(&store).await;
            assert!(
                settled.model_observer_token.is_none(),
                "superseded DB claim must be cleaned before releasing its lane for publication"
            );
            assert_eq!(settled.model_report_revision, 2);
            assert!(old.cell.slot.lane.try_lock().is_ok());
            assert!(reporting.commit_claim(&new).await.unwrap());
            events.release.notify_one();
            assert!(!claim.await.unwrap());
            assert_eq!(
                row(&store).await.model_observer_token.as_deref(),
                Some(new.cell.key.token.as_str())
            );
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn apply_commits_before_revoke_and_revoke_has_strictly_greater_revision() {
            let (_dir, store, _) = fixture().await;
            let events = Arc::new(GatedEvents::default());
            let reporting = ModelReporting::new(store.clone(), events.clone());
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(h.bind_native_root(" root/opaque "));
            assert!(h.observe(update(
                ModelEvidenceField::Configured,
                "observed-before-stop"
            )));
            assert!(reporting.commit_claim(&h).await.unwrap());
            events.armed.store(true, Ordering::SeqCst);
            assert!(reporting.activate(&h, " root/opaque "));
            events.entered.notified().await;
            let active = row(&store).await.model_report.unwrap();
            assert!(active.observer_active);
            let revoke = reporting.revoke_committed(&h);
            tokio::pin!(revoke);
            assert!(futures::poll!(&mut revoke).is_pending());
            assert!(!h.observe(update(ModelEvidenceField::Configured, "late")));
            events.release.notify_one();
            assert!(revoke.await.unwrap());
            let inactive = row(&store).await.model_report.unwrap();
            assert!(!inactive.observer_active);
            assert!(inactive.report_revision > active.report_revision);
            assert_eq!(inactive.configured, active.configured);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }

        #[tokio::test]
        async fn durable_revoke_before_parked_exact_apply_changes_zero_rows() {
            let (_dir, store, reporting) = fixture().await;
            reporting.initialize().await.unwrap();
            let h = reserve(&reporting).unwrap();
            assert!(reporting.commit_claim(&h).await.unwrap());
            assert!(reporting.revoke_committed(&h).await.unwrap());
            let revoked_revision = row(&store).await.model_report_revision;
            let gate = store
                .begin_identity_write_txn("park_previously_captured_apply")
                .await
                .unwrap();
            // A previously captured exact apply can arrive after the durable invalidation transaction.
            // Exercise the real store CAS, without a current-owner lookup or fake store predicate.
            let repo = AgentRuntimes::new(&store);
            let mut snapshot = h.cell.initial.clone();
            snapshot.observer_active = true;
            let apply = repo.apply_model_report(
                "s_observer",
                "a_observer",
                &h.cell.key.token,
                1,
                &snapshot,
            );
            tokio::pin!(apply);
            assert!(futures::poll!(&mut apply).is_pending());
            gate.commit().await.unwrap();
            assert!(!apply.await.unwrap());
            assert_eq!(row(&store).await.model_report_revision, revoked_revision);
            reporting.shutdown(Duration::from_secs(1)).await.unwrap();
        }
    }
}
