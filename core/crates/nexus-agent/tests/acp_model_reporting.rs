//! Registry/profile foundation only: inert factories, no native adapter construction or sessions.

use std::sync::{Arc, Mutex};

use nexus_agent::adapter::{AcpModelMetadataDialect, AdapterModelReportingProfile};
use nexus_agent::{Adapter, AdapterFactory, AdapterRegistry, AgentError, LaunchCtx, MockAdapter};
use nexus_common::NexusError;
use nexus_contracts::{
    HarnessId, ModelEvidenceCapability, ModelObservationSource, ModelReportBackend,
};

fn harness() -> HarnessId {
    HarnessId::new("registry-test").unwrap()
}

fn profile(backend: &str, dialect: AcpModelMetadataDialect) -> AdapterModelReportingProfile {
    AdapterModelReportingProfile::new(
        ModelReportBackend::new(backend).unwrap(),
        ModelEvidenceCapability::Supported,
        ModelEvidenceCapability::Unsupported,
        ModelEvidenceCapability::Unverified,
        dialect,
    )
    .unwrap()
}

fn config_profile() -> AdapterModelReportingProfile {
    profile(
        "test-backend-b",
        AcpModelMetadataDialect::ConfigOptions {
            source: ModelObservationSource::new("test-config-b").unwrap(),
        },
    )
}

struct CapturedSink(nexus_contracts::model_report::ModelProfileIdentity);
impl nexus_contracts::model_report::ModelObservationSink for CapturedSink {
    fn accepts_profile(
        &self,
        profile: &nexus_contracts::model_report::ModelProfileIdentity,
    ) -> bool {
        self.0.matches(profile)
    }
    fn bind_native_root(&self, _: &str) -> bool {
        true
    }
    fn observe(&self, _: nexus_contracts::model_report::NativeModelUpdate) -> bool {
        false
    }
    fn revoke(&self) {}
}

#[test]
fn native_carrier_requires_exact_profile_and_sink_allocation_without_acp_dialect() {
    use nexus_agent::adapter::NativeModelReportingProfile;
    let profile = || {
        NativeModelReportingProfile::new(
            ModelReportBackend::new("fixture.native").unwrap(),
            ModelEvidenceCapability::Supported,
            ModelEvidenceCapability::Unverified,
            ModelEvidenceCapability::Unsupported,
        )
        .unwrap()
    };
    let selected = profile();
    let foreign = profile();
    let sink = Arc::new(CapturedSink(selected.identity().clone()));
    assert!(
        foreign.capture(sink.clone()).is_err(),
        "equal metadata is not captured profile authority"
    );
    let captured = selected.clone().capture(sink).unwrap();
    assert!(captured.same_owner(&captured.clone()));
    let other = selected
        .clone()
        .capture(Arc::new(CapturedSink(selected.identity().clone())))
        .unwrap();
    assert!(
        !captured.same_owner(&other),
        "same profile does not make two sinks one owner"
    );
    assert_eq!(captured.profile().backend().as_str(), "fixture.native");
    assert_eq!(
        captured.profile().configured(),
        ModelEvidenceCapability::Supported
    );
    assert!(captured.profile().telemetry().is_none());
    assert_eq!(format!("{captured:?}"), "NativeModelReporting { .. }");
}

#[test]
fn native_profile_rejects_tombstone_and_rotates_identity_on_telemetry_change() {
    use nexus_agent::adapter::{
        AdapterTelemetryCapability, AdapterTelemetryReportingProfile, NativeModelReportingProfile,
    };
    let make = |backend| {
        NativeModelReportingProfile::new(
            ModelReportBackend::new(backend).unwrap(),
            ModelEvidenceCapability::Supported,
            ModelEvidenceCapability::Unverified,
            ModelEvidenceCapability::Unverified,
        )
    };
    assert!(
        make("unknown").is_err(),
        "corrupt-store tombstone is not a native capability"
    );
    let profile = make("fixture.native").unwrap();
    let sink = Arc::new(CapturedSink(profile.identity().clone()));
    let absent =
        AdapterTelemetryCapability::new(ModelEvidenceCapability::Unverified, None).unwrap();
    let changed = profile
        .clone()
        .with_telemetry(AdapterTelemetryReportingProfile::new(
            absent.clone(),
            absent.clone(),
            absent,
        ));
    assert!(!profile.identity().matches(changed.identity()));
    assert!(changed.capture(sink).is_err());
}

#[test]
fn captured_carrier_pairs_selected_profile_before_factory_and_survives_replacement() {
    let spy = FactorySpy::new();
    let replacement = FactorySpy::new();
    let profile = config_profile();
    let sink = Arc::new(CapturedSink(profile.identity().clone()));
    let mut registry = AdapterRegistry::new();
    registry.register_observed(&harness(), spy.factory(), profile.clone());
    let selected = registry.select(&harness()).unwrap();
    registry.register_observed(&harness(), replacement.factory(), config_profile());
    selected
        .instantiate_observed(LaunchCtx::default(), sink)
        .unwrap();
    assert_eq!(spy.call_count(), 1);
    assert_eq!(replacement.call_count(), 0);
    let calls = spy.calls.lock().unwrap();
    let reporting = calls[0]
        .model_reporting
        .as_ref()
        .expect("captured carrier reaches factory");
    assert!(reporting.profile().identity().matches(profile.identity()));
    assert_eq!(reporting.profile(), &profile);
    assert_eq!(format!("{reporting:?}"), "AdapterModelReporting { .. }");
    let engine = nexus_agent::adapter::AcpEngine::new().with_reporting(Some(reporting.clone()));
    assert!(engine
        .reporting()
        .unwrap()
        .profile()
        .identity()
        .matches(profile.identity()));
}

#[test]
fn captured_carrier_rejects_foreign_or_absent_profile_without_factory_effects() {
    let spy = FactorySpy::new();
    let mut registry = AdapterRegistry::new();
    let profile = config_profile();
    registry.register_observed(&harness(), spy.factory(), profile.clone());
    // Equal metadata does not make another independently captured profile the same allocation.
    let foreign = config_profile();
    assert_eq!(profile, foreign);
    assert!(!profile.identity().matches(foreign.identity()));
    assert!(registry
        .select(&harness())
        .unwrap()
        .instantiate_observed(
            LaunchCtx::default(),
            Arc::new(CapturedSink(foreign.identity().clone()))
        )
        .is_err());
    registry.register(&harness(), spy.factory());
    assert!(registry
        .select(&harness())
        .unwrap()
        .instantiate_observed(
            LaunchCtx::default(),
            Arc::new(CapturedSink(profile.identity().clone()))
        )
        .is_err());
    assert_eq!(spy.call_count(), 0);
}

#[test]
fn captured_carrier_never_forwards_injected_context_or_reuses_changed_profile_identity() {
    use nexus_agent::adapter::{AdapterTelemetryCapability, AdapterTelemetryReportingProfile};
    let spy = FactorySpy::new();
    let profile = config_profile();
    let mut registry = AdapterRegistry::new();
    registry.register_observed(&harness(), spy.factory(), profile.clone());
    registry
        .select(&harness())
        .unwrap()
        .instantiate_observed(
            LaunchCtx::default(),
            Arc::new(CapturedSink(profile.identity().clone())),
        )
        .unwrap();
    let injected = spy.calls.lock().unwrap()[0].clone();
    registry
        .select(&harness())
        .unwrap()
        .instantiate(injected.clone());
    assert!(spy.calls.lock().unwrap()[1].model_reporting.is_none());
    assert!(registry
        .select(&harness())
        .unwrap()
        .instantiate_observed(injected, Arc::new(CapturedSink(profile.identity().clone())))
        .is_err());
    assert_eq!(spy.call_count(), 2);
    let absent =
        AdapterTelemetryCapability::new(ModelEvidenceCapability::Unsupported, None).unwrap();
    let changed = profile
        .clone()
        .with_telemetry(AdapterTelemetryReportingProfile::new(
            absent.clone(),
            absent.clone(),
            absent,
        ));
    assert!(profile.identity().matches(profile.clone().identity()));
    assert!(!profile.identity().matches(changed.identity()));
}

#[test]
fn telemetry_profile_rejects_mismatched_capability_source() {
    use nexus_agent::adapter::AdapterTelemetryCapability;
    let source = ModelObservationSource::new("native/usage").unwrap();
    assert!(AdapterTelemetryCapability::new(ModelEvidenceCapability::Supported, None).is_err());
    assert!(
        AdapterTelemetryCapability::new(ModelEvidenceCapability::Unsupported, Some(source))
            .is_err()
    );
}

#[test]
fn telemetry_profile_capture_survives_replacement() {
    use nexus_agent::adapter::{AdapterTelemetryCapability, AdapterTelemetryReportingProfile};
    let source = ModelObservationSource::new("native/usage").unwrap();
    let telemetry = AdapterTelemetryReportingProfile::new(
        AdapterTelemetryCapability::new(ModelEvidenceCapability::Supported, Some(source.clone()))
            .unwrap(),
        AdapterTelemetryCapability::new(ModelEvidenceCapability::Unsupported, None).unwrap(),
        AdapterTelemetryCapability::new(ModelEvidenceCapability::Unverified, None).unwrap(),
    );
    let spy = FactorySpy::new();
    let mut registry = AdapterRegistry::new();
    assert!(config_profile().telemetry().is_none());
    registry.register_observed(
        &harness(),
        spy.factory(),
        config_profile().with_telemetry(telemetry.clone()),
    );
    let captured = registry.select(&harness()).unwrap();
    registry.register_observed(&harness(), spy.factory(), config_profile());
    assert_eq!(captured.profile().unwrap().telemetry(), Some(&telemetry));
    assert_eq!(
        captured
            .profile()
            .unwrap()
            .telemetry()
            .unwrap()
            .usage()
            .source(),
        Some(&source)
    );
    assert!(registry
        .select(&harness())
        .unwrap()
        .profile()
        .unwrap()
        .telemetry()
        .is_none());
    assert_eq!(spy.call_count(), 0);
    captured.instantiate(LaunchCtx::default());
    assert_eq!(spy.call_count(), 1);
}

struct FactorySpy {
    calls: Arc<Mutex<Vec<LaunchCtx>>>,
    adapter: Arc<dyn Adapter>,
}

impl FactorySpy {
    fn new() -> Self {
        Self {
            calls: Arc::default(),
            adapter: Arc::new(MockAdapter::new()),
        }
    }

    fn factory(&self) -> AdapterFactory {
        let calls = self.calls.clone();
        let adapter = self.adapter.clone();
        Arc::new(move |ctx| {
            calls.lock().unwrap().push(ctx);
            adapter.clone()
        })
    }

    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

#[test]
fn selection_is_inert_and_consuming_instantiation_calls_captured_factory_once() {
    let spy = FactorySpy::new();
    let mut registry = AdapterRegistry::new();
    registry.register_observed(&harness(), spy.factory(), config_profile());

    let selected = registry.select(&harness()).unwrap();
    assert_eq!(spy.call_count(), 0);
    assert_eq!(selected.profile(), Some(&config_profile()));
    let adapter = selected.instantiate(LaunchCtx::default());
    assert_eq!(spy.call_count(), 1);
    assert!(Arc::ptr_eq(&adapter, &spy.adapter));
}

#[test]
fn selected_factory_and_distinct_native_sources_survive_registration_replacement() {
    let a = FactorySpy::new();
    let b = FactorySpy::new();
    let original_profile = profile(
        "Unfamiliar Backend / v7",
        AcpModelMetadataDialect::ConfigOptionsAndLegacyModels {
            config_options_source: ModelObservationSource::new("Native.ConfigOptions/v7").unwrap(),
            legacy_models_source: ModelObservationSource::new("Native.LegacyModels/v2").unwrap(),
        },
    );
    let mut registry = AdapterRegistry::new();
    registry.register_observed(&harness(), a.factory(), original_profile.clone());
    let captured_a = registry.select(&harness()).unwrap();
    registry.register_observed(&harness(), b.factory(), config_profile());
    let captured_b = registry.select(&harness()).unwrap();

    assert_eq!(captured_a.profile(), Some(&original_profile));
    assert_eq!(captured_b.profile(), Some(&config_profile()));
    assert_eq!(a.call_count(), 0);
    assert_eq!(b.call_count(), 0);
    assert!(Arc::ptr_eq(
        &captured_a.instantiate(LaunchCtx::default()),
        &a.adapter
    ));
    assert_eq!(a.call_count(), 1);
    assert_eq!(b.call_count(), 0);
    assert!(Arc::ptr_eq(
        &captured_b.instantiate(LaunchCtx::default()),
        &b.adapter
    ));
    assert_eq!(a.call_count(), 1);
    assert_eq!(b.call_count(), 1);
}

#[test]
fn unobserved_capture_stays_unobserved_after_observed_replacement() {
    let a = FactorySpy::new();
    let b = FactorySpy::new();
    let mut registry = AdapterRegistry::new();
    registry.register(&harness(), a.factory());
    let captured = registry.select(&harness()).unwrap();
    registry.register_observed(&harness(), b.factory(), config_profile());

    assert!(captured.profile().is_none());
    assert_eq!(
        registry.select(&harness()).unwrap().profile(),
        Some(&config_profile())
    );
    assert!(Arc::ptr_eq(
        &captured.instantiate(LaunchCtx::default()),
        &a.adapter
    ));
    assert_eq!(a.call_count(), 1);
    assert_eq!(b.call_count(), 0);
}

#[test]
fn ordinary_registration_clears_previously_observed_profile() {
    let a = FactorySpy::new();
    let b = FactorySpy::new();
    let mut registry = AdapterRegistry::new();
    registry.register_observed(&harness(), a.factory(), config_profile());
    registry.register(&harness(), b.factory());

    let captured = registry.select(&harness()).unwrap();
    assert!(captured.profile().is_none());
    assert!(Arc::ptr_eq(
        &captured.instantiate(LaunchCtx::default()),
        &b.adapter
    ));
    assert_eq!(a.call_count(), 0);
    assert_eq!(b.call_count(), 1);
}

#[test]
fn get_preserves_single_construction_and_every_launch_context_field() {
    let spy = FactorySpy::new();
    let mut registry = AdapterRegistry::new();
    registry.register(&harness(), spy.factory());
    let ctx = LaunchCtx {
        cwd: Some("/inert/not-opened".into()),
        env: vec![("TEST_KEY".into(), "test value".into())],
        bus_name: Some("test-name".into()),
        bus_project: Some("test-project".into()),
        bus_client_key: Some("test-client-key".into()),
        bus_agent: Some("test-agent".into()),
        suppress_acp_mcp: true,
        model_reporting: None,
    };

    let adapter = registry.get(&harness(), ctx.clone()).unwrap();
    assert!(Arc::ptr_eq(&adapter, &spy.adapter));
    let calls = spy.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let actual = &calls[0];
    assert_eq!(actual.cwd, ctx.cwd);
    assert_eq!(actual.env, ctx.env);
    assert_eq!(actual.bus_name, ctx.bus_name);
    assert_eq!(actual.bus_project, ctx.bus_project);
    assert_eq!(actual.bus_client_key, ctx.bus_client_key);
    assert_eq!(actual.bus_agent, ctx.bus_agent);
    assert_eq!(actual.suppress_acp_mcp, ctx.suppress_acp_mcp);
}

#[test]
fn empty_registry_and_missing_adapter_errors_remain_compatible() {
    for registry in [AdapterRegistry::new(), AdapterRegistry::default()] {
        assert!(!registry.has(&harness()));
        let selected_error = match registry.select(&harness()) {
            Err(error) => error,
            Ok(_) => panic!("missing factory must not be selected"),
        };
        let get_error = match registry.get(&harness(), LaunchCtx::default()) {
            Err(error) => error,
            Ok(_) => panic!("missing factory must not be constructed"),
        };
        let expected =
            "no headless adapter is registered for harness registry-test on this daemon — \
                        this harness is not supported for headless launch yet";
        assert!(matches!(&selected_error, AgentError::NoAdapter(message) if message == expected));
        assert_eq!(selected_error.to_string(), get_error.to_string());
    }
}

#[test]
fn builtin_registration_keeps_model_and_explicit_native_telemetry_capabilities_separate() {
    let registry = AdapterRegistry::with_builtins();
    for id in ["hermes", "opencode"] {
        let id = HarnessId::new(id).unwrap();
        assert!(registry.has(&id));
        let selected = registry.select(&id).unwrap();
        let profile = selected
            .profile()
            .expect("builtin ACP metadata collector enabled");
        assert_eq!(profile.backend().as_str(), format!("{id}.acp"));
        assert_eq!(profile.configured(), ModelEvidenceCapability::Supported);
        assert_eq!(profile.turn_selected(), ModelEvidenceCapability::Unverified);
        assert_eq!(
            profile.response_reported(),
            ModelEvidenceCapability::Unverified
        );
        let telemetry = profile
            .telemetry()
            .expect("independently pinned native usage/context source");
        assert_eq!(
            telemetry.usage().capability(),
            ModelEvidenceCapability::Supported
        );
        assert_eq!(
            telemetry.context().capability(),
            ModelEvidenceCapability::Supported
        );
        assert_eq!(
            telemetry.quota().capability(),
            ModelEvidenceCapability::Unsupported
        );
        assert_eq!(
            telemetry.usage().source().unwrap().as_str(),
            format!("{id}.acp.prompt.usage")
        );
        assert_eq!(
            telemetry.prompt_usage_scope().unwrap().wire(),
            if id.as_str() == "hermes" {
                nexus_contracts::telemetry::TokenUsageScope::SessionCumulative
            } else {
                nexus_contracts::telemetry::TokenUsageScope::LastResponse
            }
        );
        assert!(telemetry.context_usage_basis().is_some());
    }
}

#[test]
fn acp_usage_semantics_require_the_corresponding_supported_source() {
    use nexus_agent::adapter::{
        AcpContextUsageBasis, AcpPromptUsageScope, AdapterTelemetryCapability,
        AdapterTelemetryReportingProfile,
    };
    let unavailable =
        AdapterTelemetryCapability::new(ModelEvidenceCapability::Unverified, None).unwrap();
    let profile = AdapterTelemetryReportingProfile::new(
        unavailable.clone(),
        unavailable.clone(),
        unavailable,
    );
    assert!(profile
        .clone()
        .with_prompt_usage(AcpPromptUsageScope::LastResponse)
        .is_err());
    assert!(profile
        .with_context_usage(AcpContextUsageBasis::HermesRequestEstimate)
        .is_err());
}

#[test]
fn profile_getters_preserve_opaque_identifiers_and_independent_capabilities() {
    let profile = profile(
        "  Unfamiliar Backend / 模型-v7  ",
        AcpModelMetadataDialect::ConfigOptions {
            source: ModelObservationSource::new("  Vendor.Native / Config-v7  ").unwrap(),
        },
    );
    assert_eq!(
        profile.backend().as_str(),
        "  Unfamiliar Backend / 模型-v7  "
    );
    assert_eq!(profile.configured(), ModelEvidenceCapability::Supported);
    assert_eq!(
        profile.turn_selected(),
        ModelEvidenceCapability::Unsupported
    );
    assert_eq!(
        profile.response_reported(),
        ModelEvidenceCapability::Unverified
    );
    match profile.dialect() {
        AcpModelMetadataDialect::ConfigOptions { source } => {
            assert_eq!(source.as_str(), "  Vendor.Native / Config-v7  ");
        }
        other => panic!("unexpected dialect: {other:?}"),
    }
}

#[test]
fn reserved_tombstone_backend_cannot_be_registered_as_a_reporting_profile() {
    let result = AdapterModelReportingProfile::new(
        ModelReportBackend::new("unknown").unwrap(),
        ModelEvidenceCapability::Unverified,
        ModelEvidenceCapability::Unverified,
        ModelEvidenceCapability::Unverified,
        AcpModelMetadataDialect::ConfigOptions {
            source: ModelObservationSource::new("native-config").unwrap(),
        },
    );
    assert!(matches!(result, Err(NexusError::Adapter(_))));
}

#[test]
fn invalid_opaque_ids_are_rejected_before_profile_construction() {
    for invalid in [
        "".to_owned(),
        "  ".to_owned(),
        "bad\nsource".to_owned(),
        "x".repeat(129),
    ] {
        assert!(ModelReportBackend::new(invalid.clone()).is_err());
        assert!(ModelObservationSource::new(invalid).is_err());
    }
}
