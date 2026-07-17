//! Canonical transcript archive manifest rows.
//!
//! Harness-owned transcript/session artifacts are mutable working copies. The daemon-owned
//! archive is append-only and tracked here by disposable runtime id.

use libsql::params;

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::state::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTranscriptArchive {
    pub runtime_id: String,
    pub agent_id: Option<String>,
    pub agent_name: String,
    pub project: String,
    pub harness: String,
    pub source_kind: String,
    pub source_path: String,
    pub archive_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptArchiveProgress {
    pub runtime_id: String,
    pub bytes_archived: i64,
    pub prefix_sha256: Option<String>,
    pub last_event: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptArchiveRow {
    pub runtime_id: String,
    pub agent_id: Option<String>,
    pub agent_name: String,
    pub project: String,
    pub harness: String,
    pub source_kind: String,
    pub source_path: String,
    pub archive_path: String,
    pub archive_offset: i64,
    pub bytes_archived: i64,
    pub prefix_sha256: Option<String>,
    pub last_event: Option<String>,
    pub sealed_at: Option<i64>,
    pub seal_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

pub struct TranscriptArchive<'a> {
    store: &'a Store,
}

impl<'a> TranscriptArchive<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub async fn create(&self, row: NewTranscriptArchive) -> Result<(), NexusError> {
        let ts = now();
        self.store
            .conn
            .execute(
                "INSERT INTO transcript_archive (
                    runtime_id, agent_id, agent_name, project, harness, source_kind,
                    source_path, archive_path, archive_offset, bytes_archived, prefix_sha256,
                    last_event, sealed_at, seal_reason, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, 0, NULL, NULL, NULL, NULL, ?9, ?9)
                ON CONFLICT(runtime_id) DO UPDATE SET
                    agent_id = COALESCE(excluded.agent_id, transcript_archive.agent_id),
                    agent_name = excluded.agent_name,
                    project = excluded.project,
                    harness = excluded.harness,
                    source_kind = excluded.source_kind,
                    updated_at = excluded.updated_at",
                params![
                    row.runtime_id,
                    row.agent_id,
                    row.agent_name,
                    row.project,
                    row.harness,
                    row.source_kind,
                    row.source_path,
                    row.archive_path,
                    ts,
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn reset_source(
        &self,
        runtime_id: &str,
        source_path: &str,
        archive_path: &str,
        archive_offset: i64,
        last_event: Option<String>,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE transcript_archive
                 SET source_path = ?2,
                     archive_path = ?3,
                     archive_offset = ?4,
                     bytes_archived = 0,
                     prefix_sha256 = NULL,
                     last_event = ?5,
                     updated_at = ?6
                 WHERE runtime_id = ?1 AND sealed_at IS NULL",
                params![
                    runtime_id,
                    source_path,
                    archive_path,
                    archive_offset.max(0),
                    last_event,
                    now(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn find_by_runtime_id(
        &self,
        runtime_id: &str,
    ) -> Result<Option<TranscriptArchiveRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                &format!("{SELECT} WHERE runtime_id = ?1"),
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_archive(&row)?)),
            None => Ok(None),
        }
    }

    pub async fn update_progress(
        &self,
        progress: TranscriptArchiveProgress,
    ) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "UPDATE transcript_archive
                 SET bytes_archived = ?2,
                     prefix_sha256 = ?3,
                     last_event = ?4,
                     updated_at = ?5
                 WHERE runtime_id = ?1 AND sealed_at IS NULL",
                params![
                    progress.runtime_id,
                    progress.bytes_archived.max(0),
                    progress.prefix_sha256,
                    progress.last_event,
                    now(),
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn seal(&self, runtime_id: &str, reason: &str) -> Result<(), NexusError> {
        let ts = now();
        self.store
            .conn
            .execute(
                "UPDATE transcript_archive
                 SET sealed_at = COALESCE(sealed_at, ?2),
                     seal_reason = COALESCE(seal_reason, ?3),
                     updated_at = ?2
                 WHERE runtime_id = ?1",
                params![runtime_id, ts, reason],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

const SELECT: &str = "SELECT runtime_id, agent_id, agent_name, project, harness, source_kind,
    source_path, archive_path, archive_offset, bytes_archived, prefix_sha256, last_event,
    sealed_at, seal_reason, created_at, updated_at FROM transcript_archive";

fn row_to_archive(row: &libsql::Row) -> Result<TranscriptArchiveRow, NexusError> {
    Ok(TranscriptArchiveRow {
        runtime_id: get_text(row, 0)?,
        agent_id: get_opt_text(row, 1)?,
        agent_name: get_text(row, 2)?,
        project: get_text(row, 3)?,
        harness: get_text(row, 4)?,
        source_kind: get_text(row, 5)?,
        source_path: get_text(row, 6)?,
        archive_path: get_text(row, 7)?,
        archive_offset: get_opt_int(row, 8)?.unwrap_or(0),
        bytes_archived: get_opt_int(row, 9)?.unwrap_or(0),
        prefix_sha256: get_opt_text(row, 10)?,
        last_event: get_opt_text(row, 11)?,
        sealed_at: get_opt_int(row, 12)?,
        seal_reason: get_opt_text(row, 13)?,
        created_at: get_opt_int(row, 14)?.unwrap_or(0),
        updated_at: get_opt_int(row, 15)?.unwrap_or(0),
    })
}
