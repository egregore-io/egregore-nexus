//! The runtime-agnostic adapter seam (backend spec §5). Each agent `kind`/[`nexus_contracts::HarnessId`] maps to an
//! [`Adapter`] implementing a common inject/stream contract so the bus stays runtime-agnostic:
//! harnesses over the same uniform ACP transport, and [`MockAdapter`] for
//! tests/acceptance. ACP `session/prompt` is the injection transport; `session/update` is the
//! reply stream. There is no per-harness "app-server" route — nexus is the orchestrator.
//! Claude and Codex are wired by the composition root via their harness crates.

use async_trait::async_trait;

use nexus_common::{NexusError, RuntimeProcessIds};
use nexus_contracts::{
    ContractError, HarnessId, InjectError, ModelEvidenceCapability, ModelObservationSource,
    ModelReportBackend, OperatorAction, ProviderError, ProviderLimit, ProviderLimitReason,
    ResetHint, SessionId, SteerCapability,
};

pub mod acp;
mod acp_quota;
pub mod bootstrap;
pub mod engine;
pub mod hermes;
pub mod mock;
mod model_metadata;
mod native_reporting;
pub mod opencode;
pub mod provider_limit;
pub mod skill;
pub mod spawn_spec;

pub use engine::{AcpEngine, HarnessCommand};
pub use hermes::HermesAdapter;
pub use mock::MockAdapter;
pub use native_reporting::{NativeModelReporting, NativeModelReportingProfile};
pub use opencode::OpenCodeAdapter;
pub use spawn_spec::SpawnSpecAdapter;

/// Native ACP metadata paths and their independently supplied, validated provenance identifiers.
/// This describes decoding support only; it neither enables a collector nor selects a model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcpModelMetadataDialect {
    ConfigOptions {
        source: ModelObservationSource,
    },
    ConfigOptionsAndLegacyModels {
        config_options_source: ModelObservationSource,
        legacy_models_source: ModelObservationSource,
    },
}

/// Immutable model-reporting support captured with an adapter factory before construction.
/// The backend and each native path's source are opaque adapter-owned identifiers, not a shared
/// harness catalog. A profile is not evidence that a collector is enabled or a launch is current.
#[derive(Debug, Clone)]
pub struct AdapterModelReportingProfile {
    backend: ModelReportBackend,
    configured: ModelEvidenceCapability,
    turn_selected: ModelEvidenceCapability,
    response_reported: ModelEvidenceCapability,
    dialect: AcpModelMetadataDialect,
    telemetry: Option<AdapterTelemetryReportingProfile>,
    identity: nexus_contracts::model_report::ModelProfileIdentity,
}

impl PartialEq for AdapterModelReportingProfile {
    fn eq(&self, other: &Self) -> bool {
        self.backend == other.backend
            && self.configured == other.configured
            && self.turn_selected == other.turn_selected
            && self.response_reported == other.response_reported
            && self.dialect == other.dialect
            && self.telemetry == other.telemetry
    }
}
impl Eq for AdapterModelReportingProfile {}

/// Constructed only by the consumed prepared factory from its captured profile and observer.
#[derive(Clone)]
pub struct AdapterModelReporting {
    profile: AdapterModelReportingProfile,
    sink: std::sync::Arc<dyn nexus_contracts::model_report::ModelObservationSink>,
}
impl std::fmt::Debug for AdapterModelReporting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AdapterModelReporting { .. }")
    }
}
impl AdapterModelReporting {
    pub(crate) fn captured(
        profile: AdapterModelReportingProfile,
        sink: std::sync::Arc<dyn nexus_contracts::model_report::ModelObservationSink>,
    ) -> Self {
        Self { profile, sink }
    }
    pub fn profile(&self) -> &AdapterModelReportingProfile {
        &self.profile
    }
    pub fn sink(&self) -> &dyn nexus_contracts::model_report::ModelObservationSink {
        self.sink.as_ref()
    }
}

/// Immutable category support and exact adapter-owned source. This does not enable a collector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterTelemetryCapability {
    capability: ModelEvidenceCapability,
    source: Option<ModelObservationSource>,
}

impl AdapterTelemetryCapability {
    pub fn new(
        capability: ModelEvidenceCapability,
        source: Option<ModelObservationSource>,
    ) -> Result<Self, NexusError> {
        if (capability == ModelEvidenceCapability::Supported) != source.is_some() {
            return Err(NexusError::Adapter("supported telemetry requires its exact adapter source; unavailable telemetry cannot advertise one".into()));
        }
        if let Some(source) = &source {
            source.validate().map_err(|error| {
                NexusError::Adapter(format!("invalid telemetry source: {error}"))
            })?;
        }
        Ok(Self { capability, source })
    }
    pub fn capability(&self) -> ModelEvidenceCapability {
        self.capability
    }
    pub fn source(&self) -> Option<&ModelObservationSource> {
        self.source.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterTelemetryReportingProfile {
    usage: AdapterTelemetryCapability,
    context: AdapterTelemetryCapability,
    quota: AdapterTelemetryCapability,
    prompt_usage_scope: Option<AcpPromptUsageScope>,
    context_usage_basis: Option<AcpContextUsageBasis>,
    quota_dialect: Option<AcpQuotaDialect>,
}

pub use nexus_harness_telemetry::{AcpContextUsageBasis, AcpQuotaDialect};

/// Adapter-pinned semantics of session/prompt.usage, never inferred from the common ACP shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcpPromptUsageScope {
    LastResponse,
    LastPrompt,
    SessionCumulative,
}
impl AcpPromptUsageScope {
    pub fn wire(self) -> nexus_contracts::telemetry::TokenUsageScope {
        use nexus_contracts::telemetry::TokenUsageScope;
        match self {
            Self::LastResponse => TokenUsageScope::LastResponse,
            Self::LastPrompt => TokenUsageScope::LastPrompt,
            Self::SessionCumulative => TokenUsageScope::SessionCumulative,
        }
    }
}

impl AdapterTelemetryReportingProfile {
    pub fn new(
        usage: AdapterTelemetryCapability,
        context: AdapterTelemetryCapability,
        quota: AdapterTelemetryCapability,
    ) -> Self {
        Self {
            usage,
            context,
            quota,
            prompt_usage_scope: None,
            context_usage_basis: None,
            quota_dialect: None,
        }
    }
    pub fn with_prompt_usage(mut self, scope: AcpPromptUsageScope) -> Result<Self, NexusError> {
        if self.usage.capability() != ModelEvidenceCapability::Supported {
            return Err(NexusError::Adapter(
                "ACP prompt usage needs a supported captured usage source".into(),
            ));
        }
        self.prompt_usage_scope = Some(scope);
        Ok(self)
    }
    pub fn prompt_usage_scope(&self) -> Option<AcpPromptUsageScope> {
        self.prompt_usage_scope
    }
    pub fn with_context_usage(mut self, basis: AcpContextUsageBasis) -> Result<Self, NexusError> {
        if self.context.capability() != ModelEvidenceCapability::Supported {
            return Err(NexusError::Adapter(
                "ACP context needs a supported captured context source".into(),
            ));
        }
        self.context_usage_basis = Some(basis);
        Ok(self)
    }
    pub fn context_usage_basis(&self) -> Option<AcpContextUsageBasis> {
        self.context_usage_basis
    }
    pub fn with_quota_dialect(mut self, dialect: AcpQuotaDialect) -> Result<Self, NexusError> {
        if self.quota.capability() != ModelEvidenceCapability::Supported {
            return Err(NexusError::Adapter(
                "ACP quota dialect requires a supported source".into(),
            ));
        }
        self.quota_dialect = Some(dialect);
        Ok(self)
    }
    pub fn quota_dialect(&self) -> Option<AcpQuotaDialect> {
        self.quota_dialect
    }
    pub fn usage(&self) -> &AdapterTelemetryCapability {
        &self.usage
    }
    pub fn context(&self) -> &AdapterTelemetryCapability {
        &self.context
    }
    pub fn quota(&self) -> &AdapterTelemetryCapability {
        &self.quota
    }
}

impl AdapterModelReportingProfile {
    /// Validate a profile. The corrupt-storage tombstone backend `unknown` cannot describe a
    /// registered adapter; unfamiliar valid backend/source identifiers are preserved exactly.
    pub fn new(
        backend: ModelReportBackend,
        configured: ModelEvidenceCapability,
        turn_selected: ModelEvidenceCapability,
        response_reported: ModelEvidenceCapability,
        dialect: AcpModelMetadataDialect,
    ) -> Result<Self, NexusError> {
        let invalid =
            |error| NexusError::Adapter(format!("invalid model reporting profile: {error}"));
        backend.validate().map_err(invalid)?;
        if backend.is_unknown() {
            return Err(NexusError::Adapter(
                "model reporting profile cannot use reserved backend unknown".into(),
            ));
        }
        match &dialect {
            AcpModelMetadataDialect::ConfigOptions { source } => {
                source.validate().map_err(invalid)?;
            }
            AcpModelMetadataDialect::ConfigOptionsAndLegacyModels {
                config_options_source,
                legacy_models_source,
            } => {
                config_options_source.validate().map_err(invalid)?;
                legacy_models_source.validate().map_err(invalid)?;
            }
        }
        Ok(Self {
            backend,
            configured,
            turn_selected,
            response_reported,
            dialect,
            telemetry: None,
            identity: Default::default(),
        })
    }

    /// Consume the profile before factory selection; existing constructors remain telemetry-absent.
    pub fn with_telemetry(mut self, telemetry: AdapterTelemetryReportingProfile) -> Self {
        self.telemetry = Some(telemetry);
        self.identity = Default::default();
        self
    }
    pub fn identity(&self) -> &nexus_contracts::model_report::ModelProfileIdentity {
        &self.identity
    }
    pub fn telemetry(&self) -> Option<&AdapterTelemetryReportingProfile> {
        self.telemetry.as_ref()
    }

    pub fn backend(&self) -> &ModelReportBackend {
        &self.backend
    }

    pub fn configured(&self) -> ModelEvidenceCapability {
        self.configured
    }

    pub fn turn_selected(&self) -> ModelEvidenceCapability {
        self.turn_selected
    }

    pub fn response_reported(&self) -> ModelEvidenceCapability {
        self.response_reported
    }

    pub fn dialect(&self) -> &AcpModelMetadataDialect {
        &self.dialect
    }
}

/// Adapter-owned structured provider-limit metadata before the daemon has attached the Nexus
/// session id. The agent service wraps this into [`InjectError::ProviderLimit`] at the observed
/// injection seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterProviderLimit {
    pub harness: HarnessId,
    pub reason: ProviderLimitReason,
    pub reset_hint: Option<ResetHint>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub source: String,
}

/// Adapter-owned structured provider failure before the daemon attaches the Nexus session id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterProviderError {
    pub harness: HarnessId,
    pub reason: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub retryable: bool,
    pub source: String,
}

/// Adapter-owned structured operator-action metadata before the daemon has attached the Nexus
/// session id. These are account/auth/billing stops, not rapid retry candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterOperatorAction {
    pub harness: HarnessId,
    pub reason: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub source: String,
}

/// Injection failure returned by concrete adapters. Plain `Contract` preserves legacy behavior;
/// typed variants are consumed by the realtime breaker on the observed bus-delivery path.
#[derive(Debug, thiserror::Error)]
pub enum AdapterInjectError {
    #[error("strict completion-observed injection is not supported by this adapter")]
    Unsupported,
    #[error("{:?} provider limit ({:?})", .0.harness, .0.reason)]
    ProviderLimit(AdapterProviderLimit),
    #[error("{:?} provider error ({})", .0.harness, .0.reason)]
    ProviderError(AdapterProviderError),
    #[error("{:?} operator action required ({})", .0.harness, .0.reason)]
    OperatorAction(AdapterOperatorAction),
    #[error("harness turn completion timed out ({origin})")]
    CompletionTimeout { origin: String },
    #[error(transparent)]
    Contract(#[from] NexusError),
}

impl AdapterInjectError {
    /// Attach the Nexus session id and convert to the shared observed-injection error shape.
    pub fn into_inject_error(self, session: &SessionId) -> InjectError {
        match self {
            AdapterInjectError::Unsupported => InjectError::Contract(
                NexusError::Adapter(
                    "strict completion-observed injection is not supported by this adapter".into(),
                )
                .to_contract_error(),
            ),
            AdapterInjectError::ProviderLimit(limit) => InjectError::ProviderLimit(ProviderLimit {
                harness: limit.harness,
                session: session.clone(),
                reason: limit.reason,
                reset_hint: limit.reset_hint,
                provider: limit.provider,
                model: limit.model,
                source: limit.source,
            }),
            AdapterInjectError::ProviderError(error) => InjectError::ProviderError(ProviderError {
                harness: error.harness,
                session: session.clone(),
                reason: error.reason,
                provider: error.provider,
                model: error.model,
                retryable: error.retryable,
                source: error.source,
            }),
            AdapterInjectError::OperatorAction(action) => {
                InjectError::OperatorAction(OperatorAction {
                    harness: action.harness,
                    session: session.clone(),
                    reason: action.reason,
                    provider: action.provider,
                    model: action.model,
                    source: action.source,
                })
            }
            AdapterInjectError::CompletionTimeout { origin } => InjectError::CompletionTimeout {
                session: session.clone(),
                source: origin,
            },
            AdapterInjectError::Contract(error) => InjectError::Contract(error.to_contract_error()),
        }
    }

    /// Collapse to the legacy contract error for non-observed callers.
    pub fn into_contract_error(self, session: &SessionId) -> ContractError {
        ContractError::from(self.into_inject_error(session))
    }
}

/// One translated ACP `session/update` event from an agent turn — re-exported from
/// [`nexus_acp_stream`]. The daemon relays each as a tagged `WsEvent::AgentUpdate { kind, data }`.
/// (Replaces the former text-only `UpdateChunk`: the full stream is forwarded now, not just the
/// reply text.)
pub use nexus_acp_stream::StreamEvent;

/// The injection/stream contract every harness adapter implements (backend spec §2.3, §5).
///
/// A turn is two steps: [`Adapter::inject`] hands the rendered prompt to the harness over ACP
/// `session/prompt`, then [`Adapter::stream_updates`] yields the reply as ordered `session/update`
/// chunks. [`Adapter::open_session`] stands up (or, via [`Adapter::resume`], re-attaches) the ACP
/// session for a harness; a failure here surfaces as `agent.status=errored` and the session is
/// retained for retry (spec §11), never silently dropped.
#[async_trait]
pub trait Adapter: Send + Sync {
    /// Open a fresh ACP session for this harness. Fallible — a failure is surfaced as an errored
    /// status by the caller and the registered session is kept for retry (spec §11).
    async fn open_session(&self) -> Result<(), NexusError>;

    /// Re-attach an existing harness session by its resume key (`client_key` / ACP thread), reusing
    /// the same session so the name↔session binding is preserved (spec §5).
    async fn resume(&self, resume_key: &str) -> Result<(), NexusError>;

    /// Inject one already-rendered prompt as a single ACP `session/prompt` turn.
    async fn inject(&self, prompt: String) -> Result<(), AdapterInjectError>;

    /// Strict direct-observed completion. Return this turn's owned buffer only after its
    /// correlated native prompt response, before releasing serialization. No live relay or
    /// quiescence fallback may supply this receipt. Legacy adapters fail closed by default.
    async fn inject_completion_observed(
        &self,
        _prompt: String,
    ) -> Result<Vec<StreamEvent>, AdapterInjectError> {
        Err(AdapterInjectError::Unsupported)
    }

    /// Active-turn redirect capability exposed by this adapter.
    fn steer_capability(&self) -> SteerCapability {
        SteerCapability::None
    }

    /// Native turn evidence only; legacy adapters do not manufacture an idle stamp.
    fn observe_turn(&self) -> nexus_contracts::TurnObservation {
        nexus_contracts::TurnObservation {
            steer_capability: self.steer_capability(),
            ..Default::default()
        }
    }

    /// Interrupt the adapter's active turn. Real ACP adapters map this to `session/cancel`.
    async fn interrupt_active_turn(&self) -> Result<(), NexusError> {
        Err(NexusError::Adapter(
            "this adapter does not support active-turn redirect".into(),
        ))
    }

    /// Trigger transport-native context compaction for the current session. Adapters must
    /// override this only when their protocol exposes a real compact operation; the default
    /// fails closed so `/compact` is never injected as ordinary model text by accident.
    async fn compact(&self) -> Result<(), AdapterInjectError> {
        Err(AdapterInjectError::Contract(NexusError::Adapter(
            "compact is not supported by this adapter".into(),
        )))
    }

    /// Inject one rendered prompt and, when the adapter can identify the prompt-accepted boundary,
    /// place `accepted_event` at the front of the turn's stream before assistant output.
    ///
    /// Default implementations ignore the event and preserve the legacy inject behavior. Real ACP
    /// adapters override through [`AcpEngine`] so bus/steer input appears before streamed replies.
    async fn inject_with_accepted_event(
        &self,
        prompt: String,
        _accepted_event: Option<StreamEvent>,
    ) -> Result<(), AdapterInjectError> {
        self.inject(prompt).await
    }

    /// Drain the most recent injected turn's reply as the ordered, translated ACP `session/update`
    /// stream — the full pass-through ([`StreamEvent`]s: text, thinking, tool calls, plans,
    /// available commands), in wire order.
    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError>;

    /// Terminate the spawned harness OS process (SIGKILL). Called by the `kill=true` path in
    /// `admin.remove`. The default implementation is a no-op — [`MockAdapter`] inherits it (tests
    /// that need to verify the call override via [`MockAdapter`]'s explicit impl). The real
    /// adapters override to call `self.engine.kill()`.
    async fn kill(&self) {}

    /// Install the REALTIME relay channel for the turn about to be injected: each renderable
    /// `session/update` is forwarded as it arrives, so the caller emits bounded `agent.update`
    /// deltas (true streaming). An oversized provider text update may normalize into multiple
    /// exact slices. Returns `None` when the adapter has no live engine ([`MockAdapter`] → the
    /// caller falls back to draining [`Adapter::stream_updates`] at turn-end). The real ACP
    /// adapters override to return `Some` from their engine. Pair with [`Adapter::clear_live`].
    fn install_live(&self) -> Option<tokio::sync::mpsc::UnboundedReceiver<StreamEvent>> {
        None
    }

    /// Close the realtime relay channel at turn-end. Default no-op (no live engine).
    fn clear_live(&self) {}

    /// The harness's ACP session id (the `session/load` resume key). The daemon persists this so a
    /// not-fresh agent can be re-spawned and resumed. Default `None` (no live engine); the real ACP
    /// adapters override from their engine.
    async fn acp_session_id(&self) -> Option<String> {
        None
    }

    /// Exact OS process tuple for a live ACP harness child, when this adapter owns one.
    fn runtime_process_ids(&self) -> Option<RuntimeProcessIds> {
        None
    }

    /// Open a FRESH `session/new` on the ALREADY-connected engine (no re-spawn). Used as the
    /// fallback when `resume` fails after the engine is already connected — re-calling
    /// `open_session` there would error with "acp engine already connected". Default delegates to
    /// `open_session` (for the mock, which has no separate connect step).
    async fn new_session_only(&self) -> Result<(), NexusError> {
        self.open_session().await
    }
}
