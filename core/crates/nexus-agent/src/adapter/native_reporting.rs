//! Captured model reporting for native headed owners, without an ACP decoder/factory fiction.
use std::sync::Arc;

use nexus_common::NexusError;
use nexus_contracts::model_report::{ModelObservationSink, ModelProfileIdentity};
use nexus_contracts::{ModelEvidenceCapability, ModelReportBackend};

use super::AdapterTelemetryReportingProfile;

#[derive(Clone, Debug)]
pub struct NativeModelReportingProfile {
    backend: ModelReportBackend,
    configured: ModelEvidenceCapability,
    turn_selected: ModelEvidenceCapability,
    response_reported: ModelEvidenceCapability,
    telemetry: Option<AdapterTelemetryReportingProfile>,
    identity: ModelProfileIdentity,
}

impl NativeModelReportingProfile {
    pub fn new(
        backend: ModelReportBackend,
        configured: ModelEvidenceCapability,
        turn_selected: ModelEvidenceCapability,
        response_reported: ModelEvidenceCapability,
    ) -> Result<Self, NexusError> {
        if backend.is_unknown() || backend.validate().is_err() {
            return Err(NexusError::Adapter(
                "invalid native model reporting backend".into(),
            ));
        }
        Ok(Self {
            backend,
            configured,
            turn_selected,
            response_reported,
            telemetry: None,
            identity: Default::default(),
        })
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
    pub fn telemetry(&self) -> Option<&AdapterTelemetryReportingProfile> {
        self.telemetry.as_ref()
    }
    pub fn identity(&self) -> &ModelProfileIdentity {
        &self.identity
    }
    pub fn with_telemetry(mut self, telemetry: AdapterTelemetryReportingProfile) -> Self {
        self.telemetry = Some(telemetry);
        self.identity = Default::default();
        self
    }
    /// Correspondence is checked before native setup; this is not a lease on native liveness.
    pub fn capture(
        self,
        sink: Arc<dyn ModelObservationSink>,
    ) -> Result<NativeModelReporting, NexusError> {
        if !sink.accepts_profile(&self.identity) {
            return Err(NexusError::Adapter(
                "native observer does not match captured profile or is closed".into(),
            ));
        }
        Ok(NativeModelReporting {
            profile: self,
            sink,
        })
    }
}

#[derive(Clone)]
pub struct NativeModelReporting {
    profile: NativeModelReportingProfile,
    sink: Arc<dyn ModelObservationSink>,
}

impl NativeModelReporting {
    pub fn profile(&self) -> &NativeModelReportingProfile {
        &self.profile
    }
    pub fn sink(&self) -> &dyn ModelObservationSink {
        self.sink.as_ref()
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        self.profile.identity.matches(&other.profile.identity)
            && Arc::ptr_eq(&self.sink, &other.sink)
    }
}

impl std::fmt::Debug for NativeModelReporting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeModelReporting { .. }")
    }
}
