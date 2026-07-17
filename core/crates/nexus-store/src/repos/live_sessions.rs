use libsql::params;

use nexus_common::NexusError;

use crate::error::store_err;
use crate::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewLiveSession {
    pub runtime_id: String,
    pub presence: String,
    pub connection_id: Option<String>,
    pub boot_epoch: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSessionRow {
    pub runtime_id: String,
    pub presence: String,
    pub connection_id: Option<String>,
    pub boot_epoch: String,
    pub updated_at: i64,
}

pub struct LiveSessions<'a> {
    store: &'a Store,
}

impl<'a> LiveSessions<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub async fn upsert(&self, session: NewLiveSession) -> Result<(), NexusError> {
        self.store
            .conn
            .execute(
                "INSERT INTO live_sessions
                 (runtime_id, presence, connection_id, boot_epoch, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(runtime_id) DO UPDATE SET
                   presence = excluded.presence,
                   connection_id = excluded.connection_id,
                   boot_epoch = excluded.boot_epoch,
                   updated_at = excluded.updated_at",
                params![
                    session.runtime_id,
                    session.presence,
                    session.connection_id,
                    session.boot_epoch,
                    session.updated_at
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn find(&self, runtime_id: &str) -> Result<Option<LiveSessionRow>, NexusError> {
        let mut rows = self
            .store
            .conn
            .query(
                "SELECT runtime_id, presence, connection_id, boot_epoch, updated_at
                 FROM live_sessions WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        Ok(Some(LiveSessionRow {
            runtime_id: row.get(0).map_err(store_err)?,
            presence: row.get(1).map_err(store_err)?,
            connection_id: row.get(2).map_err(store_err)?,
            boot_epoch: row.get(3).map_err(store_err)?,
            updated_at: row.get(4).map_err(store_err)?,
        }))
    }
}
