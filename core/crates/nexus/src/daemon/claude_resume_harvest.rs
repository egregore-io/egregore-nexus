//! Automatic Claude resume-id repair from headed bridge hook logs.
//!
//! `nexus attach`/`nexus resume` must never fall back to generic Claude "continue latest"
//! behavior. Older headed Claude rows can still have an exact native `session_id` in their
//! per-session bridge `hooks.jsonl` while the sidecar's `claude_session_id` column is empty. This
//! module harvests that evidence automatically at daemon boot and on the revive path, then writes
//! through [`ClaudeRuntimeStateRepo::set_claude_session`]. The harvested value is an opaque
//! best-effort resume hint; Nexus does not validate Claude Code identity or uniqueness.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use nexus_common::NexusError;
use nexus_contracts::SessionId;
use nexus_harness_claude::native::transcript::{parse_transcript_jsonl, parse_transcript_line};
use nexus_harness_claude::storage::{claude_runtime_revive_seed, ClaudeRuntimeStateRepo};
use nexus_store::Store;

/// Summary of one automatic Claude resume-id harvest pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeResumeHarvestReport {
    pub scanned: usize,
    pub updated: usize,
    pub skipped: usize,
    pub items: Vec<ClaudeResumeHarvestItem>,
}

/// One Claude runtime row considered by the automatic harvest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeResumeHarvestItem {
    pub runtime_id: SessionId,
    pub hook_log: PathBuf,
    pub status: ClaudeResumeHarvestStatus,
    pub native_session_id: Option<String>,
    pub transcript_path: Option<PathBuf>,
    pub reason: Option<String>,
}

/// Per-runtime harvest result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeResumeHarvestStatus {
    AlreadyPresent,
    Updated,
    MissingRuntimeState,
    MissingHookLog,
    NoNativeSessionId,
    AmbiguousNativeSessionId,
    UpdateFailed,
}

/// Fill every missing Claude native resume id that can be proven from bridge hook logs.
pub async fn harvest_missing_claude_resume_ids(
    store: &Store,
    home: &Path,
) -> Result<ClaudeResumeHarvestReport, NexusError> {
    let repo = ClaudeRuntimeStateRepo::new(store);
    let missing = repo.list_missing_claude_session_id().await?;
    let mut items = Vec::new();
    for state in missing {
        items.push(harvest_claude_resume_id_for_state(store, home, state).await?);
    }
    Ok(report_from_items(items))
}

/// Try to fill one runtime's missing Claude native resume id from its bridge hook log.
pub async fn harvest_claude_resume_id_for_runtime(
    store: &Store,
    home: &Path,
    runtime_id: &SessionId,
) -> Result<ClaudeResumeHarvestItem, NexusError> {
    let repo = ClaudeRuntimeStateRepo::new(store);
    let Some(state) = repo.find_by_runtime_id(runtime_id).await? else {
        let hook_log = claude_runtime_revive_seed(home, runtime_id, None)
            .bridge_dir
            .join("hooks.jsonl");
        let (status, native_session_id, transcript_path, reason) =
            no_sidecar_harvest_status(&hook_log);
        return Ok(ClaudeResumeHarvestItem {
            runtime_id: runtime_id.clone(),
            hook_log,
            status,
            native_session_id,
            transcript_path,
            reason,
        });
    };
    harvest_claude_resume_id_for_state(store, home, state).await
}

/// Default user home passed to Claude's revive seed when a sidecar row lacks `bridge_dir`.
pub fn default_home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Human one-line reason for an unfilled runtime.
pub fn revive_failure_truth(item: &ClaudeResumeHarvestItem) -> String {
    let detail = item.reason.as_deref().unwrap_or(match item.status {
        ClaudeResumeHarvestStatus::MissingRuntimeState => "no Claude runtime sidecar row exists",
        ClaudeResumeHarvestStatus::MissingHookLog => "bridge hooks.jsonl is missing",
        ClaudeResumeHarvestStatus::NoNativeSessionId => {
            "no Claude session_id found in bridge hook log"
        }
        ClaudeResumeHarvestStatus::AmbiguousNativeSessionId => {
            "bridge hook log contains multiple Claude session ids"
        }
        ClaudeResumeHarvestStatus::UpdateFailed => "failed to persist harvested Claude session id",
        ClaudeResumeHarvestStatus::AlreadyPresent | ClaudeResumeHarvestStatus::Updated => {
            "Claude resume id is available"
        }
    });
    format!("cannot revive headed Claude runtime without a stored --resume session id; {detail}")
}

async fn harvest_claude_resume_id_for_state(
    store: &Store,
    home: &Path,
    state: nexus_harness_claude::storage::ClaudeRuntimeState,
) -> Result<ClaudeResumeHarvestItem, NexusError> {
    let seed = claude_runtime_revive_seed(home, &state.runtime_id, Some(&state));
    let hook_log = seed.bridge_dir.join("hooks.jsonl");
    let mut item = ClaudeResumeHarvestItem {
        runtime_id: state.runtime_id.clone(),
        hook_log: hook_log.clone(),
        status: ClaudeResumeHarvestStatus::NoNativeSessionId,
        native_session_id: state.claude_session_id.filter(|id| !id.is_empty()),
        transcript_path: state.transcript_path,
        reason: None,
    };

    if item.native_session_id.is_some() {
        item.status = ClaudeResumeHarvestStatus::AlreadyPresent;
        return Ok(item);
    }

    let harvest = match harvest_claude_hook_resume_id(&hook_log) {
        Ok(harvest) => harvest,
        Err(HarvestError::MissingHookLog) => {
            item.status = ClaudeResumeHarvestStatus::MissingHookLog;
            item.reason = Some("bridge hooks.jsonl is missing".into());
            return Ok(item);
        }
        Err(HarvestError::NoNativeSessionId) => {
            item.status = ClaudeResumeHarvestStatus::NoNativeSessionId;
            item.reason = Some("no Claude session_id found in bridge hook log".into());
            return Ok(item);
        }
        Err(HarvestError::AmbiguousNativeSessionId(ids)) => {
            item.status = ClaudeResumeHarvestStatus::AmbiguousNativeSessionId;
            item.reason = Some(format!(
                "bridge hook log contains multiple Claude session ids: {}",
                ids.into_iter().collect::<Vec<_>>().join(", ")
            ));
            return Ok(item);
        }
        Err(HarvestError::Io(message)) => return Err(NexusError::Store(message)),
    };

    item.native_session_id = Some(harvest.session_id.clone());
    item.transcript_path = harvest.transcript_path.clone();
    match ClaudeRuntimeStateRepo::new(store)
        .set_claude_session(
            &state.runtime_id,
            &harvest.session_id,
            harvest.transcript_path,
        )
        .await
    {
        Ok(()) => {
            item.status = ClaudeResumeHarvestStatus::Updated;
        }
        Err(error) => {
            item.status = ClaudeResumeHarvestStatus::UpdateFailed;
            item.reason = Some(error.to_string());
        }
    }
    Ok(item)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClaudeHookHarvest {
    session_id: String,
    transcript_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HarvestError {
    MissingHookLog,
    NoNativeSessionId,
    AmbiguousNativeSessionId(BTreeSet<String>),
    Io(String),
}

fn harvest_claude_hook_resume_id(path: &Path) -> Result<ClaudeHookHarvest, HarvestError> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(HarvestError::MissingHookLog)
        }
        Err(e) => return Err(HarvestError::Io(e.to_string())),
    };

    let mut candidates = Vec::new();
    for record in parse_transcript_jsonl(&content) {
        if let Some(session_id) = record.session_id {
            candidates.push((session_id, record.transcript_path.map(PathBuf::from)));
        }
    }
    for line in content.lines() {
        let Some(record) = parse_transcript_line(line) else {
            continue;
        };
        if let Some(session_id) = record.session_id {
            candidates.push((session_id, record.transcript_path.map(PathBuf::from)));
        }
    }

    let ids = candidates
        .iter()
        .map(|(session_id, _)| session_id.clone())
        .collect::<BTreeSet<_>>();
    match ids.len() {
        0 => Err(HarvestError::NoNativeSessionId),
        1 => {
            let session_id = ids.into_iter().next().expect("one id");
            let transcript_path = candidates
                .into_iter()
                .rev()
                .find_map(|(id, transcript_path)| (id == session_id).then_some(transcript_path))
                .flatten();
            Ok(ClaudeHookHarvest {
                session_id,
                transcript_path,
            })
        }
        _ => Err(HarvestError::AmbiguousNativeSessionId(ids)),
    }
}

fn no_sidecar_harvest_status(
    hook_log: &Path,
) -> (
    ClaudeResumeHarvestStatus,
    Option<String>,
    Option<PathBuf>,
    Option<String>,
) {
    match harvest_claude_hook_resume_id(hook_log) {
        Ok(harvest) => (
            ClaudeResumeHarvestStatus::MissingRuntimeState,
            Some(harvest.session_id),
            harvest.transcript_path,
            Some("no Claude runtime sidecar row exists; bridge hook log has native session evidence but there is no row to update".into()),
        ),
        Err(HarvestError::MissingHookLog) => (
            ClaudeResumeHarvestStatus::MissingRuntimeState,
            None,
            None,
            Some("no Claude runtime sidecar row exists and bridge hooks.jsonl is missing".into()),
        ),
        Err(HarvestError::NoNativeSessionId) => (
            ClaudeResumeHarvestStatus::MissingRuntimeState,
            None,
            None,
            Some("no Claude runtime sidecar row exists and bridge hook log has no Claude session_id".into()),
        ),
        Err(HarvestError::AmbiguousNativeSessionId(ids)) => (
            ClaudeResumeHarvestStatus::AmbiguousNativeSessionId,
            None,
            None,
            Some(format!(
                "no Claude runtime sidecar row exists and bridge hook log contains multiple Claude session ids: {}",
                ids.into_iter().collect::<Vec<_>>().join(", ")
            )),
        ),
        Err(HarvestError::Io(message)) => (
            ClaudeResumeHarvestStatus::MissingRuntimeState,
            None,
            None,
            Some(message),
        ),
    }
}

fn report_from_items(items: Vec<ClaudeResumeHarvestItem>) -> ClaudeResumeHarvestReport {
    let updated = items
        .iter()
        .filter(|item| item.status == ClaudeResumeHarvestStatus::Updated)
        .count();
    let skipped = items
        .iter()
        .filter(|item| {
            !matches!(
                item.status,
                ClaudeResumeHarvestStatus::AlreadyPresent | ClaudeResumeHarvestStatus::Updated
            )
        })
        .count();
    ClaudeResumeHarvestReport {
        scanned: items.len(),
        updated,
        skipped,
        items,
    }
}
