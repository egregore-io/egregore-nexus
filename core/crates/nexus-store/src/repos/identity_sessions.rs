use libsql::params;

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewIdentitySession {
    pub runtime_id: String,
    pub agent_id: String,
    pub project: String,
    pub harness: String,
    pub mode: String,
    pub backend: Option<String>,
    pub cwd: Option<String>,
    pub native_resume_key: Option<String>,
    pub client_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentitySessionRow {
    pub runtime_id: String,
    pub agent_id: String,
    pub project: String,
    pub harness: String,
    pub mode: String,
    pub backend: Option<String>,
    pub cwd: Option<String>,
    pub native_resume_key: Option<String>,
    pub client_key: Option<String>,
    pub updated_at: i64,
}

pub struct IdentitySessions<'a> {
    store: &'a Store,
}

impl<'a> IdentitySessions<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub async fn upsert(&self, session: NewIdentitySession) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO identity_sessions
                 (runtime_id, agent_id, project, harness, mode, backend, cwd,
                  native_resume_key, client_key, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                 ON CONFLICT(runtime_id) DO UPDATE SET
                   agent_id = excluded.agent_id,
                   project = excluded.project,
                   harness = excluded.harness,
                   mode = excluded.mode,
                   backend = excluded.backend,
                   cwd = excluded.cwd,
                   native_resume_key = excluded.native_resume_key,
                   client_key = COALESCE(excluded.client_key, identity_sessions.client_key),
                   updated_at = excluded.updated_at",
                params![
                    session.runtime_id,
                    session.agent_id,
                    session.project,
                    session.harness,
                    session.mode,
                    session.backend,
                    session.cwd,
                    session.native_resume_key,
                    session.client_key,
                    now()
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn find(&self, runtime_id: &str) -> Result<Option<IdentitySessionRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT runtime_id, agent_id, project, harness, mode, backend, cwd,
                        native_resume_key, client_key, updated_at
                 FROM identity_sessions WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        Ok(Some(IdentitySessionRow {
            runtime_id: row.get(0).map_err(store_err)?,
            agent_id: row.get(1).map_err(store_err)?,
            project: row.get(2).map_err(store_err)?,
            harness: row.get(3).map_err(store_err)?,
            mode: row.get(4).map_err(store_err)?,
            backend: row.get(5).map_err(store_err)?,
            cwd: row.get(6).map_err(store_err)?,
            native_resume_key: row.get(7).map_err(store_err)?,
            client_key: row.get(8).map_err(store_err)?,
            updated_at: row.get(9).map_err(store_err)?,
        }))
    }

    /// List every daemon-owned resurrection capsule, newest first.
    ///
    /// These rows are not live presence. Callers rebuild a boot-scoped session directory from
    /// them and only mark a runtime online after the owning harness transport is reattached.
    pub async fn list(&self) -> Result<Vec<IdentitySessionRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT runtime_id, agent_id, project, harness, mode, backend, cwd,
                        native_resume_key, client_key, updated_at
                 FROM identity_sessions ORDER BY updated_at DESC, runtime_id",
                (),
            )
            .await
            .map_err(store_err)?;
        let mut sessions = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            sessions.push(IdentitySessionRow {
                runtime_id: row.get(0).map_err(store_err)?,
                agent_id: row.get(1).map_err(store_err)?,
                project: row.get(2).map_err(store_err)?,
                harness: row.get(3).map_err(store_err)?,
                mode: row.get(4).map_err(store_err)?,
                backend: row.get(5).map_err(store_err)?,
                cwd: row.get(6).map_err(store_err)?,
                native_resume_key: row.get(7).map_err(store_err)?,
                client_key: row.get(8).map_err(store_err)?,
                updated_at: row.get(9).map_err(store_err)?,
            });
        }
        Ok(sessions)
    }

    /// Store the opaque native resume key reported by the harness for this Nexus runtime.
    /// Nexus does not interpret the key or claim ownership of the harness's identity space.
    pub async fn set_native_resume_key(
        &self,
        runtime_id: &str,
        native_resume_key: &str,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .identity_conn()
            .execute(
                "UPDATE identity_sessions
                 SET native_resume_key = ?2, updated_at = ?3
                 WHERE runtime_id = ?1",
                params![runtime_id, native_resume_key, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(changed > 0)
    }

    /// Persist the per-runtime launch credential so a surviving native bridge and a respawned
    /// harness keep the same Nexus transport identity across daemon restarts.
    pub async fn set_client_key(
        &self,
        runtime_id: &str,
        client_key: &str,
    ) -> Result<bool, NexusError> {
        let changed = self
            .store
            .identity_conn()
            .execute(
                "UPDATE identity_sessions
                 SET client_key = ?2, updated_at = ?3
                 WHERE runtime_id = ?1",
                params![runtime_id, client_key, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(changed > 0)
    }
}
