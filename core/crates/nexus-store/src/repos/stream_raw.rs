//! Raw terminal-output stream lane stored in the attached tmpfs stream DB.
//!
//! `stream_raw` is view-only ephemera for terminal-style consumers. Rows are never materialized
//! into the main Nexus database; after each append the repo trims old rows per session to a small
//! row-and-byte ring so a noisy PTY cannot grow the tmpfs store without bound.

use std::borrow::Cow;

use libsql::params;

use nexus_common::{now, NexusError};
use nexus_contracts::ids::SessionId;

use crate::error::store_err;
use crate::state::Store;

const DEFAULT_RAW_ROW_CAP: i64 = 2000;
const DEFAULT_RAW_BYTE_CAP: i64 = 64 * 1024;

/// One raw terminal-output chunk read from `mem.stream_raw`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRawRow {
    pub id: i64,
    pub session_id: String,
    pub chunk: Vec<u8>,
    pub created_at: i64,
}

/// Accessor for the tmpfs-backed `mem.stream_raw` lane.
pub struct StreamRaw<'a> {
    store: &'a Store,
}

impl<'a> StreamRaw<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        StreamRaw { store }
    }

    /// Append a raw output chunk and trim the session's ring to the configured row and byte caps.
    ///
    /// `NEXUS_STREAM_RAW_CAP` caps rows per session (default 2000) and
    /// `NEXUS_STREAM_RAW_BYTE_CAP` caps retained bytes per session (default 64 KiB). A single chunk
    /// larger than the byte cap is stored as its newest suffix so one write cannot blow the ring
    /// before trimming runs.
    pub async fn append(&self, session_id: &SessionId, chunk: &[u8]) -> Result<i64, NexusError> {
        let caps = raw_caps();
        let chunk = cap_chunk(chunk, caps.byte_cap);
        self.store
            .stream_conn()
            .execute(
                "INSERT INTO mem.stream_raw (session_id, chunk, created_at) VALUES (?1, ?2, ?3)",
                params![session_id.0.clone(), chunk.as_ref().to_vec(), now()],
            )
            .await
            .map_err(store_err)?;
        let id = self.store.stream_conn().last_insert_rowid();
        self.trim_session(session_id, caps).await?;
        Ok(id)
    }

    /// Read a session's raw chunks with `id` greater than `after_id`, in order.
    pub async fn since(
        &self,
        session_id: &SessionId,
        after_id: i64,
    ) -> Result<Vec<StreamRawRow>, NexusError> {
        let mut rows = self
            .store
            .stream_conn()
            .query(
                "SELECT id, session_id, chunk, created_at FROM mem.stream_raw \
                 WHERE session_id = ?1 AND id > ?2 ORDER BY id ASC",
                params![session_id.0.clone(), after_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(StreamRawRow {
                id: row.get(0).map_err(store_err)?,
                session_id: row.get(1).map_err(store_err)?,
                chunk: row.get(2).map_err(store_err)?,
                created_at: row.get(3).map_err(store_err)?,
            });
        }
        Ok(out)
    }

    /// Trim a session to the newest rows that fit within `caps`.
    ///
    /// `row_cap <= 0` or `byte_cap <= 0` keeps no rows. Byte trimming is row-granular: it preserves
    /// the newest suffix of rows whose cumulative `length(chunk)` is within the byte cap.
    pub async fn trim_session(
        &self,
        session_id: &SessionId,
        caps: StreamRawCaps,
    ) -> Result<u64, NexusError> {
        let by_rows = self
            .store
            .stream_conn()
            .execute(
                "DELETE FROM mem.stream_raw \
                 WHERE session_id = ?1 \
                   AND id NOT IN (\
                     SELECT id FROM mem.stream_raw \
                     WHERE session_id = ?1 ORDER BY id DESC LIMIT ?2\
                   )",
                params![session_id.0.clone(), caps.row_cap.max(0)],
            )
            .await
            .map_err(store_err)?;
        let by_bytes = self
            .store
            .stream_conn()
            .execute(
                "DELETE FROM mem.stream_raw \
                 WHERE session_id = ?1 \
                   AND (\
                     ?2 <= 0 \
                     OR id IN (\
                       SELECT id FROM (\
                         SELECT id, \
                                SUM(length(chunk)) OVER (\
                                  ORDER BY id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                                ) AS retained_bytes \
                         FROM mem.stream_raw \
                         WHERE session_id = ?1\
                       ) \
                       WHERE retained_bytes > ?2\
                     )\
                   )",
                params![session_id.0.clone(), caps.byte_cap.max(0)],
            )
            .await
            .map_err(store_err)?;
        Ok(by_rows + by_bytes)
    }
}

/// Per-session retention caps for the raw terminal lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRawCaps {
    /// Maximum retained raw rows for one session.
    pub row_cap: i64,
    /// Maximum retained raw bytes for one session.
    pub byte_cap: i64,
}

fn raw_caps() -> StreamRawCaps {
    StreamRawCaps {
        row_cap: env_i64("NEXUS_STREAM_RAW_CAP", DEFAULT_RAW_ROW_CAP),
        byte_cap: env_i64("NEXUS_STREAM_RAW_BYTE_CAP", DEFAULT_RAW_BYTE_CAP),
    }
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|cap| *cap >= 0)
        .unwrap_or(default)
}

fn cap_chunk(chunk: &[u8], byte_cap: i64) -> Cow<'_, [u8]> {
    if byte_cap <= 0 {
        return Cow::Borrowed(&[]);
    }
    let byte_cap = byte_cap as usize;
    if chunk.len() <= byte_cap {
        Cow::Borrowed(chunk)
    } else {
        Cow::Owned(chunk[chunk.len() - byte_cap..].to_vec())
    }
}
