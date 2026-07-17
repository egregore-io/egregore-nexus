//! Durable cross-pass producer identity suppression for native forwarders.
//!
//! Native headed harnesses can see one semantic assistant message twice: first as streamed deltas,
//! then as a final transcript snapshot. This repo stores bounded pending streamed occurrences by
//! runtime so a later final snapshot can be suppressed after a forwarder restart. When both
//! surfaces share ids it mirrors the `nexus-transcript` in-memory `ProducerIdentityStore`; when
//! they do not, a serialized native forwarder can consume occurrences in append order.

use libsql::params;
use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::state::Store;

/// The default maximum pending streamed occurrences retained per runtime.
pub const DEFAULT_PENDING_PRODUCER_IDENTITIES_PER_RUNTIME: usize = 64;

/// One pending streamed producer occurrence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerIdentityRow {
    pub runtime_id: String,
    pub producer_id: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Async durable store for pending streamed producer occurrences.
pub struct ProducerIdentities<'a> {
    store: &'a Store,
    cap: usize,
}

impl<'a> ProducerIdentities<'a> {
    /// Bind a repo to the shared store connection with the production per-runtime cap.
    pub fn new(store: &'a Store) -> Self {
        Self::with_cap(store, DEFAULT_PENDING_PRODUCER_IDENTITIES_PER_RUNTIME)
    }

    /// Bind a repo with a test/custom per-runtime cap.
    pub fn with_cap(store: &'a Store, cap: usize) -> Self {
        Self {
            store,
            cap: cap.max(1),
        }
    }

    /// Record `producer_id` as a pending streamed record for `runtime_id`.
    ///
    /// Duplicate inserts are ignored without reordering, matching `InMemoryProducerIdentityStore`.
    /// After each successful insert the oldest ids for the runtime are evicted to enforce the cap.
    pub async fn insert(&self, runtime_id: &str, producer_id: &str) -> Result<(), NexusError> {
        if runtime_id.is_empty() || producer_id.is_empty() {
            return Ok(());
        }
        let ts = now();
        self.store
            .identity_conn()
            .execute(
                "INSERT OR IGNORE INTO producer_identities (
                    runtime_id, producer_id, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?3)",
                params![runtime_id, producer_id, ts],
            )
            .await
            .map_err(store_err)?;
        self.evict_overflow(runtime_id).await
    }

    /// Remove a pending id. Returns `true` when it existed, meaning a final snapshot should be
    /// suppressed.
    pub async fn remove(&self, runtime_id: &str, producer_id: &str) -> Result<bool, NexusError> {
        let affected = self
            .store
            .identity_conn()
            .execute(
                "DELETE FROM producer_identities
                 WHERE runtime_id = ?1 AND producer_id = ?2",
                params![runtime_id, producer_id],
            )
            .await
            .map_err(store_err)?;
        Ok(affected > 0)
    }

    /// Admit a streamed record: always emit, and remember the id as pending.
    pub async fn admit_streamed(
        &self,
        runtime_id: &str,
        producer_id: &str,
    ) -> Result<bool, NexusError> {
        self.insert(runtime_id, producer_id).await?;
        Ok(true)
    }

    /// Admit a final snapshot: emit only if there was no pending streamed id to consume.
    pub async fn admit_final(
        &self,
        runtime_id: &str,
        producer_id: &str,
    ) -> Result<bool, NexusError> {
        Ok(!self.remove(runtime_id, producer_id).await?)
    }

    /// List pending ids for one runtime in FIFO order.
    pub async fn list_for_runtime(
        &self,
        runtime_id: &str,
    ) -> Result<Vec<ProducerIdentityRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT runtime_id, producer_id, created_at, updated_at
                 FROM producer_identities
                 WHERE runtime_id = ?1
                 ORDER BY created_at, rowid",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(ProducerIdentityRow {
                runtime_id: row.get::<String>(0).map_err(store_err)?,
                producer_id: row.get::<String>(1).map_err(store_err)?,
                created_at: row.get::<i64>(2).map_err(store_err)?,
                updated_at: row.get::<i64>(3).map_err(store_err)?,
            });
        }
        Ok(out)
    }

    /// Consume the oldest pending streamed occurrence for one serialized runtime forwarder.
    ///
    /// Native surfaces do not always share an id namespace with their final transcript. Their
    /// append order is still stable, so a harness can pair the next final record with this FIFO
    /// occurrence without content deduplication. The caller must serialize consumption per
    /// runtime, as the native forwarders do.
    pub async fn consume_oldest(
        &self,
        runtime_id: &str,
    ) -> Result<Option<ProducerIdentityRow>, NexusError> {
        let Some(row) = self.list_for_runtime(runtime_id).await?.into_iter().next() else {
            return Ok(None);
        };
        if self.remove(runtime_id, &row.producer_id).await? {
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    /// Clear pending producer occurrences when a runtime binds a different native session.
    pub async fn clear_runtime(&self, runtime_id: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM producer_identities WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    async fn evict_overflow(&self, runtime_id: &str) -> Result<(), NexusError> {
        let rows = self.list_for_runtime(runtime_id).await?;
        let overflow = rows.len().saturating_sub(self.cap);
        for row in rows.into_iter().take(overflow) {
            self.remove(&row.runtime_id, &row.producer_id).await?;
        }
        Ok(())
    }
}
