//! The `sources` repo — named event producers that push onto the notification bus.
//! Each source has a **plaintext** token (the daemon is the trusted local core; the gateway does all
//! HMAC/crypto — it reads this token to verify a remote producer's signature), a topic, and enabled.

use libsql::params;

use nexus_common::NexusError;

use crate::error::store_err;
use crate::state::Store;

/// A row from the `sources` table.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceRow {
    pub name: String,
    pub token: String,
    pub topic: String,
    pub enabled: bool,
    pub created_at: i64,
    pub last_fired_at: Option<i64>,
}

/// Persistence for the `sources` table.
pub struct Sources<'a> {
    store: &'a Store,
}

impl<'a> Sources<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Sources { store }
    }

    /// Insert a new source row. Errors with [`NexusError::DuplicateName`] if the name already
    /// exists (the `name` column is the PRIMARY KEY — re-registration must be explicit).
    pub async fn create(
        &self,
        name: &str,
        token: &str,
        topic: &str,
        now: i64,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO sources (name, token, topic, enabled, created_at) \
                 VALUES (?1, ?2, ?3, 1, ?4)",
                params![name.to_string(), token.to_string(), topic.to_string(), now],
            )
            .await
            .map_err(|e| {
                // PRIMARY KEY violation → UNIQUE constraint failed: sources.name
                let msg = e.to_string();
                if msg.contains("UNIQUE constraint failed") || msg.contains("SQLITE_CONSTRAINT") {
                    NexusError::DuplicateName(name.to_string())
                } else {
                    store_err(e)
                }
            })?;
        Ok(())
    }

    /// Find a source by its name.
    pub async fn find(&self, name: &str) -> Result<Option<SourceRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT name, token, topic, enabled, created_at, last_fired_at \
                 FROM sources WHERE name = ?1",
                params![name.to_string()],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_source(&row)?)),
            None => Ok(None),
        }
    }

    /// List every source in insertion order.
    pub async fn list(&self) -> Result<Vec<SourceRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT name, token, topic, enabled, created_at, last_fired_at \
                 FROM sources ORDER BY created_at",
                (),
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_source(&row)?);
        }
        Ok(out)
    }

    /// Enable or disable a source (stored as INTEGER 1/0).
    pub async fn set_enabled(&self, name: &str, enabled: bool) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE sources SET enabled = ?2 WHERE name = ?1",
                params![name.to_string(), enabled as i64],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Rotate the (plaintext) token for a source.
    pub async fn set_token(&self, name: &str, token: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE sources SET token = ?2 WHERE name = ?1",
                params![name.to_string(), token.to_string()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Record the latest fire timestamp for a source.
    pub async fn touch_fired(&self, name: &str, now: i64) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE sources SET last_fired_at = ?2 WHERE name = ?1",
                params![name.to_string(), now],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Delete a source row entirely.
    pub async fn delete(&self, name: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "DELETE FROM sources WHERE name = ?1",
                params![name.to_string()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

fn row_to_source(row: &libsql::Row) -> Result<SourceRow, NexusError> {
    use crate::repos::sessions::{get_opt_int, get_text};
    Ok(SourceRow {
        name: get_text(row, 0)?,
        token: get_text(row, 1)?,
        topic: get_text(row, 2)?,
        enabled: get_opt_int(row, 3)?.unwrap_or(1) != 0,
        created_at: get_opt_int(row, 4)?.unwrap_or(0),
        last_fired_at: get_opt_int(row, 5)?,
    })
}
