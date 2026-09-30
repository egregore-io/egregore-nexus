//! Runtime credential repo.
//!
//! Credentials are generic authorization records for stable agents. The store persists only a
//! caller-supplied hash; plaintext secrets are never stored here.

use libsql::params;

use nexus_common::{now, NexusError};

use crate::error::store_err;
use crate::repos::sessions::{get_opt_int, get_opt_text, get_text};
use crate::state::Store;
use crate::types::AgentCredentialRow;

/// Fields needed to create a hashed credential row.
#[derive(Debug, Clone)]
pub struct NewAgentCredential {
    pub credential_id: String,
    pub agent_id: String,
    pub secret_hash: String,
    pub purpose: Option<String>,
    pub label: Option<String>,
    pub scopes_json: String,
    pub metadata_json: Option<String>,
}

/// Persistence for the `agent_credentials` table.
pub struct AgentCredentials<'a> {
    store: &'a Store,
}

impl<'a> AgentCredentials<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        AgentCredentials { store }
    }

    /// Insert a credential hash for an agent.
    pub async fn create_hash(&self, credential: NewAgentCredential) -> Result<String, NexusError> {
        let credential_id = credential.credential_id.clone();
        self.store
            .identity_conn()
            .execute(
                "INSERT INTO agent_credentials (credential_id, agent_id, secret_hash, purpose, \
                 label, scopes_json, metadata_json, revoked_at, last_used_at, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8)",
                params![
                    credential.credential_id,
                    credential.agent_id,
                    credential.secret_hash,
                    credential.purpose,
                    credential.label,
                    credential.scopes_json,
                    credential.metadata_json,
                    now()
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(credential_id)
    }

    /// List active, non-revoked credentials for an agent.
    pub async fn find_active(&self, agent_id: &str) -> Result<Vec<AgentCredentialRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!("{SELECT} WHERE agent_id = ?1 AND revoked_at IS NULL ORDER BY created_at"),
                params![agent_id],
            )
            .await
            .map_err(store_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(store_err)? {
            out.push(row_to_credential(&row)?);
        }
        Ok(out)
    }

    /// Find one credential by id, including already-revoked rows.
    pub async fn find_by_id(
        &self,
        credential_id: &str,
    ) -> Result<Option<AgentCredentialRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                &format!("{SELECT} WHERE credential_id = ?1 LIMIT 1"),
                params![credential_id],
            )
            .await
            .map_err(store_err)?;
        match rows.next().await.map_err(store_err)? {
            Some(row) => Ok(Some(row_to_credential(&row)?)),
            None => Ok(None),
        }
    }

    /// Stamp a credential as used now.
    pub async fn touch_last_used(&self, credential_id: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_credentials SET last_used_at = ?2 WHERE credential_id = ?1",
                params![credential_id, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// Revoke a credential.
    pub async fn revoke(&self, credential_id: &str) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE agent_credentials SET revoked_at = ?2 WHERE credential_id = ?1",
                params![credential_id, now()],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

const SELECT: &str = "SELECT credential_id, agent_id, secret_hash, purpose, label, scopes_json, \
     metadata_json, revoked_at, last_used_at, created_at FROM agent_credentials";

fn row_to_credential(row: &libsql::Row) -> Result<AgentCredentialRow, NexusError> {
    Ok(AgentCredentialRow {
        credential_id: get_text(row, 0)?,
        agent_id: get_text(row, 1)?,
        secret_hash: get_text(row, 2)?,
        purpose: get_opt_text(row, 3)?,
        label: get_opt_text(row, 4)?,
        scopes_json: get_text(row, 5)?,
        metadata_json: get_opt_text(row, 6)?,
        revoked_at: get_opt_int(row, 7)?,
        last_used_at: get_opt_int(row, 8)?,
        created_at: get_opt_int(row, 9)?.unwrap_or(0),
    })
}
