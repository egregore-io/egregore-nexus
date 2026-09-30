//! Native hook-log resume identity policy.
use super::transcript::parse_transcript_value;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeHookHarvest {
    pub session_id: String,
    pub transcript_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarvestError {
    MissingHookLog,
    NoNativeSessionId,
    AmbiguousNativeSessionId(BTreeSet<String>),
    Io(String),
}

/// One native session id named in the bridge hook log, in append order.
struct HookSessionCandidate {
    session_id: String,
    transcript_path: Option<PathBuf>,
    session_start: bool,
}

fn hook_session_candidates(content: &str) -> Vec<HookSessionCandidate> {
    let mut values: Vec<Value> = Vec::new();
    // Object-stream records first (multi-line objects), stopping at the first malformed record;
    // then single-line records, so a partial or foreign line never hides the rest of the log.
    let mut stream = serde_json::Deserializer::from_str(content).into_iter::<Value>();
    while let Some(Ok(value)) = stream.next() {
        values.push(value);
    }
    values.extend(
        content
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok()),
    );
    values
        .iter()
        .filter_map(|value| {
            let record = parse_transcript_value(value)?;
            let session_id = record.session_id?;
            let payload = value.get("payload").unwrap_or(value);
            let event = payload
                .get("hook_event_name")
                .or_else(|| payload.get("hookEventName"))
                .or_else(|| value.get("event"))
                .and_then(Value::as_str);
            Some(HookSessionCandidate {
                session_id,
                transcript_path: record.transcript_path.map(PathBuf::from),
                session_start: event == Some("SessionStart"),
            })
        })
        .collect()
}

/// The Claude session id this runtime's bridge currently belongs to. Ownership is the
/// runtime's: when the log names several Claude sessions, the latest `SessionStart` (a resume,
/// fork or `/clear`) is the one to resume. Several ids with no `SessionStart` at all cannot be
/// ordered and stay ambiguous.
pub fn harvest_claude_hook_resume_id(path: &Path) -> Result<ClaudeHookHarvest, HarvestError> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(HarvestError::MissingHookLog)
        }
        Err(e) => return Err(HarvestError::Io(e.to_string())),
    };

    let candidates = hook_session_candidates(&content);
    let ids = candidates
        .iter()
        .map(|candidate| candidate.session_id.clone())
        .collect::<BTreeSet<_>>();
    let session_id = match ids.len() {
        0 => return Err(HarvestError::NoNativeSessionId),
        1 => ids.into_iter().next().expect("one id"),
        _ => match candidates
            .iter()
            .rev()
            .find(|candidate| candidate.session_start)
        {
            Some(started) => started.session_id.clone(),
            None => return Err(HarvestError::AmbiguousNativeSessionId(ids)),
        },
    };
    let transcript_path = candidates
        .into_iter()
        .rev()
        .filter(|candidate| candidate.session_id == session_id)
        .find_map(|candidate| candidate.transcript_path);
    Ok(ClaudeHookHarvest {
        session_id,
        transcript_path,
    })
}
