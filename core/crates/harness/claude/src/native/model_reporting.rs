//! Response-model facts from root native assistant records; not configured/requested models.
use nexus_agent::adapter::NativeModelReportingProfile;
use nexus_contracts::model_report::{ModelEvidenceField, ModelEvidenceValue, NativeModelUpdate};
use nexus_contracts::{
    ModelEvidenceCapability, ModelInvalidReason, ModelObservation, ModelObservationSource,
    ModelReportBackend, ModelUnknownReason,
};
use serde_json::Value;

/// One opened transcript and its byte image. The handle and bytes have the same origin;
/// consumers cannot construct a source witness from a later path-only metadata lookup.
pub struct ClaudeTranscriptRead {
    path: std::path::PathBuf,
    handle: std::sync::Arc<same_file::Handle>,
    bytes: Vec<u8>,
}

/// Bounded append-only continuity evidence. Retaining the handle keeps the original file
/// resource alive; the digest also rejects observed truncation/regrowth with changed bytes.
pub struct ClaudeTranscriptCheckpoint {
    path: std::path::PathBuf,
    handle: Option<std::sync::Arc<same_file::Handle>>,
    len: usize,
    digest: [u8; 32],
}

impl ClaudeTranscriptRead {
    /// Model ownership has its own admission floor, independent of the display cursor.
    pub fn response_models_after(&self, floor: u64) -> Vec<(ClaudeResponseModel, u64)> {
        let mut stream = serde_json::Deserializer::from_slice(&self.bytes).into_iter::<Value>();
        let mut models = Vec::new();
        loop {
            let mut start = stream.byte_offset();
            while self.bytes.get(start).is_some_and(u8::is_ascii_whitespace) {
                start += 1;
            }
            let Some(Ok(value)) = stream.next() else {
                break;
            };
            if start as u64 >= floor {
                if let Some(model) = parse_response_model(&value) {
                    models.push((model, stream.byte_offset() as u64));
                }
            }
        }
        models
    }
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(Self {
            path: path.to_owned(),
            handle: std::sync::Arc::new(same_file::Handle::from_file(file)?),
            bytes,
        })
    }
    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn checkpoint(&self) -> ClaudeTranscriptCheckpoint {
        use sha2::Digest;
        ClaudeTranscriptCheckpoint {
            path: self.path.clone(),
            handle: Some(self.handle.clone()),
            len: self.bytes.len(),
            digest: sha2::Sha256::digest(&self.bytes).into(),
        }
    }
    pub fn continues(&self, previous: &ClaudeTranscriptCheckpoint) -> bool {
        use sha2::Digest;
        self.path == previous.path
            && previous
                .handle
                .as_ref()
                .is_none_or(|handle| &self.handle == handle)
            && self.bytes.get(..previous.len).is_some_and(|prefix| {
                <[u8; 32]>::from(sha2::Sha256::digest(prefix)) == previous.digest
            })
    }
}

impl ClaudeTranscriptCheckpoint {
    pub fn capture(path: &std::path::Path) -> std::io::Result<Self> {
        use sha2::Digest;
        match ClaudeTranscriptRead::open(path) {
            Ok(read) => Ok(read.checkpoint()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                path: path.to_owned(),
                handle: None,
                len: 0,
                digest: sha2::Sha256::digest([]).into(),
            }),
            Err(error) => Err(error),
        }
    }
    pub fn matches_path(&self, path: &std::path::Path) -> bool {
        self.path == path
    }
    pub fn is_missing(&self) -> bool {
        self.handle.is_none()
    }
    pub fn byte_floor(&self) -> u64 {
        self.len as u64
    }
}

/// Native source selected before prompt submission. Callers cannot synthesize its root or
/// replace the selected path with a later display/runtime lookup.
pub struct CapturedClaudeResponseSource {
    root: String,
    checkpoint: ClaudeTranscriptCheckpoint,
}

impl CapturedClaudeResponseSource {
    pub fn root(&self) -> &str {
        &self.root
    }
    pub fn into_checkpoint(self) -> ClaudeTranscriptCheckpoint {
        self.checkpoint
    }
}

pub fn capture_response_source(
    hook_path: &std::path::Path,
    after_hook_offset: u64,
    retained_root: Option<&str>,
    stored_source: Option<(&str, &std::path::Path)>,
) -> std::io::Result<Option<CapturedClaudeResponseSource>> {
    let bytes = match std::fs::read(hook_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error),
    };
    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "ambiguous Claude model source admission",
        )
    };
    let mut stream = serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>();
    let mut selected: Option<(String, std::path::PathBuf)> = None;
    while let Some(value) = stream.next() {
        let value = match value {
            Ok(value) => value,
            // The native hook appends one record in pieces. A trailing unfinished record
            // grants no source authority, including over an earlier complete candidate.
            Err(error) if error.is_eof() => return Ok(None),
            Err(_) => return Err(invalid()),
        };
        let offset = stream.byte_offset() as u64;
        let record = crate::native::transcript::parse_hook_record(&value, offset);
        if !record.valid || record.kind.as_deref() != Some("SessionStart") {
            continue;
        }
        let Some(root) = record.session_id else {
            continue;
        };
        if let Some(retained) = retained_root {
            if root != retained {
                if offset > after_hook_offset {
                    return Err(invalid());
                }
                continue;
            }
        } else if offset <= after_hook_offset {
            continue;
        }
        let mut path: Option<&str> = None;
        for object in [&value, value.get("payload").unwrap_or(&value)] {
            for field in ["transcript_path", "transcriptPath"] {
                if let Some(raw) = object.get(field) {
                    let next = raw
                        .as_str()
                        .filter(|s| !s.trim().is_empty())
                        .ok_or_else(invalid)?;
                    if path.is_some_and(|prior| prior != next) {
                        return Err(invalid());
                    }
                    path = Some(next);
                }
            }
        }
        if let Some(path) = path {
            if selected.as_ref().is_some_and(|(prior, _)| prior != &root) {
                return Err(invalid());
            }
            selected = Some((root, path.into()));
        }
    }
    if selected.is_none() {
        if let Some((stored_root, path)) =
            stored_source.filter(|(root, _)| Some(*root) == retained_root)
        {
            selected = Some((stored_root.into(), path.to_owned()));
        }
    }
    selected
        .map(|(root, path)| {
            Ok(CapturedClaudeResponseSource {
                root,
                checkpoint: ClaudeTranscriptCheckpoint::capture(&path)?,
            })
        })
        .transpose()
}

pub fn profile() -> NativeModelReportingProfile {
    NativeModelReportingProfile::new(
        ModelReportBackend::new("claude.transcript").unwrap(),
        ModelEvidenceCapability::Unverified,
        ModelEvidenceCapability::Unverified,
        ModelEvidenceCapability::Supported,
    )
    .unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeResponseModel {
    pub native_session_id: String,
    pub native_message_id: String,
    pub model: Result<Option<String>, ModelInvalidReason>,
    pub native_reported_at: Option<i64>,
}

impl ClaudeResponseModel {
    pub fn update(&self, observed_at: i64) -> NativeModelUpdate {
        let value = match &self.model {
            Ok(Some(model)) => {
                let observation = ModelObservation {
                    model_id: model.clone(),
                    provider_id: None,
                    source: ModelObservationSource::new("claude.transcript.assistant").unwrap(),
                    observed_at,
                    native_session_id: Some(self.native_session_id.clone()),
                    native_turn_id: None,
                    native_message_id: Some(self.native_message_id.clone()),
                    native_reported_at: self.native_reported_at,
                };
                if observation.validate().is_ok() {
                    ModelEvidenceValue::Observed(observation)
                } else {
                    ModelEvidenceValue::Invalid(ModelInvalidReason::MalformedNativeMetadata)
                }
            }
            Ok(None) => ModelEvidenceValue::Unknown(ModelUnknownReason::AwaitingNativeMetadata),
            Err(reason) => ModelEvidenceValue::Invalid(*reason),
        };
        NativeModelUpdate {
            native_session_id: self.native_session_id.clone(),
            field: ModelEvidenceField::ResponseReported,
            value,
        }
    }
}

pub fn parse_response_model(value: &Value) -> Option<ClaudeResponseModel> {
    if value.get("type")?.as_str()? != "assistant"
        || value.get("isSidechain")?.as_bool()? != false
        || value.get("agentId").is_some_and(|agent| !agent.is_null())
    {
        return None;
    }
    let session = value
        .get("sessionId")?
        .as_str()
        .filter(|id| !id.trim().is_empty())?;
    if value
        .get("session_id")
        .is_some_and(|id| id.as_str() != Some(session))
    {
        return None;
    }
    let message = value.get("message")?;
    if message.get("type")?.as_str()? != "message" || message.get("role")?.as_str()? != "assistant"
    {
        return None;
    }
    let id = message
        .get("id")?
        .as_str()
        .filter(|id| !id.trim().is_empty())?;
    if message.get("model").and_then(Value::as_str) == Some("<synthetic>") {
        return None;
    }
    let mut model = match message.get("model") {
        None => Ok(None),
        Some(Value::String(model)) if !model.trim().is_empty() => Ok(Some(model.clone())),
        Some(_) => Err(ModelInvalidReason::MalformedNativeMetadata),
    };
    let native_reported_at = match value.get("timestamp") {
        None => None,
        Some(Value::String(raw)) => {
            match time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339) {
                Ok(time) => {
                    match i64::try_from(time.unix_timestamp_nanos().div_euclid(1_000_000)) {
                        Ok(millis) => Some(millis),
                        Err(_) => {
                            model = Err(ModelInvalidReason::MalformedNativeMetadata);
                            None
                        }
                    }
                }
                Err(_) => {
                    model = Err(ModelInvalidReason::MalformedNativeMetadata);
                    None
                }
            }
        }
        Some(_) => {
            model = Err(ModelInvalidReason::MalformedNativeMetadata);
            None
        }
    };
    Some(ClaudeResponseModel {
        native_session_id: session.into(),
        native_message_id: id.into(),
        model,
        native_reported_at,
    })
}
