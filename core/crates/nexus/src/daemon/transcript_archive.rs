//! Daemon-owned canonical transcript archive writer.
//!
//! Harness sidecars expose mutable working-copy transcript/session artifacts. This module owns
//! the durable append-only archive manifest and byte-tail copy policy.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use nexus_common::NexusError;
use nexus_contracts::ids::SessionId;
use nexus_harness_claude::storage::ClaudeRuntimeStateRepo;
use nexus_harness_codex::storage::CodexRuntimeStateRepo;
use nexus_store::repos::{
    AgentRuntimes, NewTranscriptArchive, Sessions, TranscriptArchive, TranscriptArchiveProgress,
};
use nexus_store::Store;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone)]
pub struct ArchiveFileRequest {
    pub store: Arc<Store>,
    pub runtime_id: String,
    pub agent_id: Option<String>,
    pub agent_name: String,
    pub project: String,
    pub harness: String,
    pub source_kind: String,
    pub source_path: PathBuf,
    pub archive_path: PathBuf,
    pub last_event: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveOutcome {
    Advanced { bytes_archived: i64 },
    Unchanged { bytes_archived: i64 },
    Sealed { reason: String },
    Skipped { reason: String },
}

pub async fn archive_claude_once(
    store: Arc<Store>,
    session: &SessionId,
    last_event: Option<String>,
) -> Result<ArchiveOutcome, NexusError> {
    let Some(claude) = ClaudeRuntimeStateRepo::new(&store)
        .find_by_runtime_id(session)
        .await?
    else {
        return Ok(ArchiveOutcome::Skipped {
            reason: "missing_claude_state".to_string(),
        });
    };
    let Some(source_path) = claude.transcript_path else {
        return Ok(ArchiveOutcome::Skipped {
            reason: "missing_transcript_path".to_string(),
        });
    };
    let session_row = Sessions::new(&store).find_by_session_id(session).await?;
    let runtime_row = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await?;
    let agent_id = runtime_row.as_ref().map(|runtime| runtime.agent_id.clone());
    let agent_name = session_row
        .as_ref()
        .map(|row| row.display_name())
        .unwrap_or_else(|| session.0.clone());
    let project = session_row
        .as_ref()
        .map(|row| row.project.clone())
        .unwrap_or_else(|| "default".to_string());
    let archive_path = default_archive_path(agent_id.as_deref(), &agent_name, session);

    archive_file_once(ArchiveFileRequest {
        store,
        runtime_id: session.0.clone(),
        agent_id,
        agent_name,
        project,
        harness: "claude".to_string(),
        source_kind: "file".to_string(),
        source_path,
        archive_path,
        last_event,
    })
    .await
}

pub async fn archive_codex_once(
    store: Arc<Store>,
    session: &SessionId,
    last_event: Option<String>,
) -> Result<ArchiveOutcome, NexusError> {
    let Some(codex) = CodexRuntimeStateRepo::new(&store)
        .find_by_runtime_id(session)
        .await?
    else {
        return Ok(ArchiveOutcome::Skipped {
            reason: "missing_codex_state".to_string(),
        });
    };
    let Some(source_path) = codex.rollout_path else {
        return Ok(ArchiveOutcome::Skipped {
            reason: "missing_rollout_path".to_string(),
        });
    };
    let session_row = Sessions::new(&store).find_by_session_id(session).await?;
    let runtime_row = AgentRuntimes::new(&store)
        .find_by_runtime_id(&session.0)
        .await?;
    if runtime_row
        .as_ref()
        .map(|runtime| runtime.harness.as_str() != "codex")
        .unwrap_or(false)
        || session_row
            .as_ref()
            .and_then(|row| row.agent.as_deref())
            .map(|agent| agent != "codex")
            .unwrap_or(false)
    {
        return Ok(ArchiveOutcome::Skipped {
            reason: "non_codex_runtime".to_string(),
        });
    }
    let agent_id = runtime_row.as_ref().map(|runtime| runtime.agent_id.clone());
    let agent_name = session_row
        .as_ref()
        .map(|row| row.display_name())
        .unwrap_or_else(|| session.0.clone());
    let project = session_row
        .as_ref()
        .map(|row| row.project.clone())
        .unwrap_or_else(|| "default".to_string());
    let archive_path = default_archive_path(agent_id.as_deref(), &agent_name, session);

    archive_file_once(ArchiveFileRequest {
        store,
        runtime_id: session.0.clone(),
        agent_id,
        agent_name,
        project,
        harness: "codex".to_string(),
        source_kind: "file".to_string(),
        source_path,
        archive_path,
        last_event,
    })
    .await
}

pub async fn archive_file_once(req: ArchiveFileRequest) -> Result<ArchiveOutcome, NexusError> {
    let lock = archive_lock(&req.runtime_id);
    let _guard = lock.lock().await;
    archive_file_once_locked(req).await
}

async fn archive_file_once_locked(req: ArchiveFileRequest) -> Result<ArchiveOutcome, NexusError> {
    let repo = TranscriptArchive::new(&req.store);
    repo.create(NewTranscriptArchive {
        runtime_id: req.runtime_id.clone(),
        agent_id: req.agent_id.clone(),
        agent_name: req.agent_name.clone(),
        project: req.project.clone(),
        harness: req.harness.clone(),
        source_kind: req.source_kind.clone(),
        source_path: req.source_path.to_string_lossy().into_owned(),
        archive_path: req.archive_path.to_string_lossy().into_owned(),
    })
    .await?;

    let Some(mut row) = repo.find_by_runtime_id(&req.runtime_id).await? else {
        return Err(NexusError::Internal(format!(
            "transcript archive row missing after create: {}",
            req.runtime_id
        )));
    };
    if row.sealed_at.is_some() {
        return Ok(ArchiveOutcome::Sealed {
            reason: row.seal_reason.unwrap_or_else(|| "sealed".to_string()),
        });
    }

    let requested_source_path = req.source_path.to_string_lossy().into_owned();
    if row.source_path != requested_source_path {
        let archive_path = PathBuf::from(row.archive_path.clone());
        let archive_offset = tokio::task::spawn_blocking(move || file_len_if_exists(&archive_path))
            .await
            .map_err(|e| NexusError::Internal(format!("join archive stat task: {e}")))??;
        let archive_offset = i64::try_from(archive_offset)
            .map_err(|_| NexusError::Invalid("archive file too large".to_string()))?;
        repo.reset_source(
            &req.runtime_id,
            &requested_source_path,
            &row.archive_path,
            archive_offset,
            req.last_event.clone(),
        )
        .await
        .map_err(|e| NexusError::Internal(format!("reset archive source: {e}")))?;
        let Some(refreshed) = repo.find_by_runtime_id(&req.runtime_id).await? else {
            return Err(NexusError::Internal(format!(
                "transcript archive row missing after source reset: {}",
                req.runtime_id
            )));
        };
        row = refreshed;
    }

    let source_path = PathBuf::from(row.source_path.clone());
    let archive_path = PathBuf::from(row.archive_path.clone());
    let source_len = tokio::task::spawn_blocking({
        let source_path = source_path.clone();
        move || file_len(&source_path)
    })
    .await
    .map_err(|e| NexusError::Internal(format!("join transcript stat task: {e}")))??;
    let mut archived_len = u64::try_from(row.bytes_archived.max(0))
        .map_err(|_| NexusError::Invalid("negative archive cursor".to_string()))?;
    let archive_offset = u64::try_from(row.archive_offset.max(0))
        .map_err(|_| NexusError::Invalid("negative archive offset".to_string()))?;

    if source_len < archived_len {
        repo.seal(&req.runtime_id, "fork_or_tamper").await?;
        return Ok(ArchiveOutcome::Sealed {
            reason: "fork_or_tamper".to_string(),
        });
    }
    if let Some(expected_hash) = row.prefix_sha256.as_deref() {
        let actual_hash = tokio::task::spawn_blocking({
            let source_path = source_path.clone();
            move || hash_file_range(&source_path, 0, archived_len)
        })
        .await
        .map_err(|e| NexusError::Internal(format!("join transcript hash task: {e}")))??;
        if actual_hash != expected_hash {
            repo.seal(&req.runtime_id, "fork_or_tamper").await?;
            return Ok(ArchiveOutcome::Sealed {
                reason: "fork_or_tamper".to_string(),
            });
        }
        let archive_hash = match tokio::task::spawn_blocking({
            let archive_path = archive_path.clone();
            move || hash_file_range(&archive_path, archive_offset, archived_len)
        })
        .await
        .map_err(|e| NexusError::Internal(format!("join archive hash task: {e}")))?
        {
            Ok(hash) => hash,
            Err(_) => {
                repo.seal(&req.runtime_id, "fork_or_tamper").await?;
                return Ok(ArchiveOutcome::Sealed {
                    reason: "fork_or_tamper".to_string(),
                });
            }
        };
        if archive_hash != expected_hash {
            repo.seal(&req.runtime_id, "fork_or_tamper").await?;
            return Ok(ArchiveOutcome::Sealed {
                reason: "fork_or_tamper".to_string(),
            });
        }
    }

    let archive_len = tokio::task::spawn_blocking({
        let archive_path = archive_path.clone();
        move || file_len_if_exists(&archive_path)
    })
    .await
    .map_err(|e| NexusError::Internal(format!("join archive stat task: {e}")))??;
    let expected_archive_len = archive_offset
        .checked_add(archived_len)
        .ok_or_else(|| NexusError::Invalid("archive cursor overflow".to_string()))?;
    if archive_len < expected_archive_len {
        repo.seal(&req.runtime_id, "fork_or_tamper").await?;
        return Ok(ArchiveOutcome::Sealed {
            reason: "fork_or_tamper".to_string(),
        });
    }
    if archive_len > expected_archive_len {
        let extra_len = archive_len - expected_archive_len;
        if extra_len > source_len.saturating_sub(archived_len) {
            repo.seal(&req.runtime_id, "fork_or_tamper").await?;
            return Ok(ArchiveOutcome::Sealed {
                reason: "fork_or_tamper".to_string(),
            });
        }
        let matches_source_tail = tokio::task::spawn_blocking({
            let archive_path = archive_path.clone();
            let source_path = source_path.clone();
            move || {
                ranges_equal(
                    &archive_path,
                    expected_archive_len,
                    &source_path,
                    archived_len,
                    extra_len,
                )
            }
        })
        .await
        .map_err(|e| NexusError::Internal(format!("join archive recovery task: {e}")))??;
        if !matches_source_tail {
            repo.seal(&req.runtime_id, "fork_or_tamper").await?;
            return Ok(ArchiveOutcome::Sealed {
                reason: "fork_or_tamper".to_string(),
            });
        }
        archived_len = archived_len
            .checked_add(extra_len)
            .ok_or_else(|| NexusError::Invalid("archive recovery overflow".to_string()))?;
    }
    if source_len == archived_len {
        if req.last_event.is_some() {
            repo.update_progress(TranscriptArchiveProgress {
                runtime_id: req.runtime_id.clone(),
                bytes_archived: i64::try_from(archived_len)
                    .map_err(|_| NexusError::Invalid("source transcript too large".to_string()))?,
                prefix_sha256: if archived_len == 0 {
                    None
                } else {
                    Some(
                        tokio::task::spawn_blocking({
                            let source_path = source_path.clone();
                            move || hash_file_range(&source_path, 0, archived_len)
                        })
                        .await
                        .map_err(|e| {
                            NexusError::Internal(format!("join transcript hash task: {e}"))
                        })??,
                    )
                },
                last_event: req.last_event,
            })
            .await?;
        }
        return Ok(ArchiveOutcome::Unchanged {
            bytes_archived: i64::try_from(archived_len)
                .map_err(|_| NexusError::Invalid("source transcript too large".to_string()))?,
        });
    }

    let tail_len = source_len - archived_len;
    let tail = tokio::task::spawn_blocking({
        let source_path = source_path.clone();
        move || read_file_range(&source_path, archived_len, tail_len)
    })
    .await
    .map_err(|e| NexusError::Internal(format!("join transcript tail read task: {e}")))??;
    tokio::task::spawn_blocking({
        let archive_path = archive_path.clone();
        move || append_tail(&archive_path, &tail)
    })
    .await
    .map_err(|e| NexusError::Internal(format!("join transcript archive task: {e}")))??;
    let bytes_archived = i64::try_from(source_len)
        .map_err(|_| NexusError::Invalid("source transcript too large".to_string()))?;
    let prefix_sha256 = tokio::task::spawn_blocking({
        let source_path = source_path.clone();
        move || hash_file_range(&source_path, 0, source_len)
    })
    .await
    .map_err(|e| NexusError::Internal(format!("join transcript hash task: {e}")))??;
    repo.update_progress(TranscriptArchiveProgress {
        runtime_id: req.runtime_id.clone(),
        bytes_archived,
        prefix_sha256: Some(prefix_sha256),
        last_event: req.last_event,
    })
    .await?;
    Ok(ArchiveOutcome::Advanced { bytes_archived })
}

fn archive_lock(runtime_id: &str) -> Arc<AsyncMutex<()>> {
    static LOCKS: OnceLock<Mutex<std::collections::HashMap<String, Arc<AsyncMutex<()>>>>> =
        OnceLock::new();
    let locks = LOCKS.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let mut locks = locks.lock().expect("transcript archive lock map poisoned");
    locks
        .entry(runtime_id.to_string())
        .or_insert_with(|| Arc::new(AsyncMutex::new(())))
        .clone()
}

fn file_len(path: &Path) -> Result<u64, NexusError> {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|e| NexusError::Adapter(format!("stat transcript {}: {e}", path.display())))
}

fn file_len_if_exists(path: &Path) -> Result<u64, NexusError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(NexusError::Adapter(format!(
            "stat archive {}: {e}",
            path.display()
        ))),
    }
}

fn read_file_range(path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, NexusError> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| NexusError::Adapter(format!("read transcript {}: {e}", path.display())))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| NexusError::Adapter(format!("seek {}: {e}", path.display())))?;
    let len = usize::try_from(len)
        .map_err(|_| NexusError::Invalid("file range too large".to_string()))?;
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)
        .map_err(|e| NexusError::Adapter(format!("read transcript {}: {e}", path.display())))?;
    Ok(bytes)
}

fn hash_file_range(path: &Path, offset: u64, len: u64) -> Result<String, NexusError> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| NexusError::Adapter(format!("read {}: {e}", path.display())))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| NexusError::Adapter(format!("seek {}: {e}", path.display())))?;
    let mut remaining = len;
    let mut buf = [0_u8; 8192];
    let mut hasher = Sha256::new();
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        file.read_exact(&mut buf[..want])
            .map_err(|e| NexusError::Adapter(format!("read {}: {e}", path.display())))?;
        hasher.update(&buf[..want]);
        remaining -= want as u64;
    }
    Ok(hex_digest(hasher.finalize()))
}

fn ranges_equal(
    left_path: &Path,
    left_offset: u64,
    right_path: &Path,
    right_offset: u64,
    len: u64,
) -> Result<bool, NexusError> {
    let mut left = std::fs::File::open(left_path)
        .map_err(|e| NexusError::Adapter(format!("read {}: {e}", left_path.display())))?;
    let mut right = std::fs::File::open(right_path)
        .map_err(|e| NexusError::Adapter(format!("read {}: {e}", right_path.display())))?;
    left.seek(SeekFrom::Start(left_offset))
        .map_err(|e| NexusError::Adapter(format!("seek {}: {e}", left_path.display())))?;
    right
        .seek(SeekFrom::Start(right_offset))
        .map_err(|e| NexusError::Adapter(format!("seek {}: {e}", right_path.display())))?;
    let mut remaining = len;
    let mut left_buf = [0_u8; 8192];
    let mut right_buf = [0_u8; 8192];
    while remaining > 0 {
        let want = remaining.min(left_buf.len() as u64) as usize;
        left.read_exact(&mut left_buf[..want])
            .map_err(|e| NexusError::Adapter(format!("read {}: {e}", left_path.display())))?;
        right
            .read_exact(&mut right_buf[..want])
            .map_err(|e| NexusError::Adapter(format!("read {}: {e}", right_path.display())))?;
        if left_buf[..want] != right_buf[..want] {
            return Ok(false);
        }
        remaining -= want as u64;
    }
    Ok(true)
}

fn append_tail(path: &Path, tail: &[u8]) -> Result<(), NexusError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            NexusError::Adapter(format!("create archive dir {}: {e}", parent.display()))
        })?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| NexusError::Adapter(format!("open archive {}: {e}", path.display())))?;
    file.write_all(tail)
        .map_err(|e| NexusError::Adapter(format!("append archive {}: {e}", path.display())))?;
    file.sync_data()
        .map_err(|e| NexusError::Adapter(format!("sync archive {}: {e}", path.display())))?;
    Ok(())
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn default_archive_path(agent_id: Option<&str>, agent_name: &str, session: &SessionId) -> PathBuf {
    let root = default_nexus_state_dir().join("archives");
    match agent_id {
        Some(agent_id) if !agent_id.is_empty() => root
            .join(safe_segment(agent_id))
            .join(safe_segment(&session.0))
            .join("transcript.jsonl"),
        _ => root
            .join("legacy")
            .join(safe_segment(agent_name))
            .join(safe_segment(&session.0))
            .join("transcript.jsonl"),
    }
}

fn safe_segment(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "_".to_string()
    } else {
        cleaned
    }
}

fn default_nexus_state_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".nexus")
}
