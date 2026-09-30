use libsql::params;

use nexus_common::NexusError;

use crate::error::store_err;
use crate::state::Store;

#[derive(Debug, Clone)]
pub struct InitialPromptInsert {
    pub runtime_id: String,
    pub agent_id: String,
    pub session_id: String,
    pub harness: String,
    pub template: String,
    pub rendered_prompt: String,
    pub client_message_id: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialPromptInsertOutcome {
    Pending,
    AlreadyPending,
    AlreadyAccepted,
    AlreadyFailed,
}

pub struct InitialPromptDeliveries<'a> {
    store: &'a Store,
}

impl<'a> InitialPromptDeliveries<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub async fn insert_pending_once(
        &self,
        insert: InitialPromptInsert,
    ) -> Result<InitialPromptInsertOutcome, NexusError> {
        if let Some(existing) = self.get(&insert.runtime_id).await? {
            if existing.template != insert.template
                || existing.rendered_prompt != insert.rendered_prompt
                || existing.client_message_id != insert.client_message_id
            {
                return Err(NexusError::Invalid(format!(
                    "conflicting initial prompt for runtime {}",
                    insert.runtime_id
                )));
            }
            return Ok(match existing.status.as_str() {
                "pending" => InitialPromptInsertOutcome::AlreadyPending,
                "accepted" => InitialPromptInsertOutcome::AlreadyAccepted,
                "failed" => InitialPromptInsertOutcome::AlreadyFailed,
                other => {
                    return Err(NexusError::Invalid(format!(
                        "invalid initial prompt delivery status {other}"
                    )))
                }
            });
        }

        self.store
            .identity_conn()
            .execute(
                "INSERT INTO initial_prompt_deliveries (runtime_id, agent_id, session_id, \
                 harness, template, rendered_prompt, client_message_id, status, error, \
                 created_at_ms, accepted_at_ms, failed_at_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', NULL, ?8, NULL, NULL)",
                params![
                    insert.runtime_id,
                    insert.agent_id,
                    insert.session_id,
                    insert.harness,
                    insert.template,
                    insert.rendered_prompt,
                    insert.client_message_id,
                    insert.created_at_ms,
                ],
            )
            .await
            .map_err(store_err)?;
        Ok(InitialPromptInsertOutcome::Pending)
    }

    pub async fn mark_accepted(
        &self,
        runtime_id: &str,
        accepted_at_ms: i64,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE initial_prompt_deliveries SET status = 'accepted', error = NULL, \
                 accepted_at_ms = ?2 WHERE runtime_id = ?1",
                params![runtime_id, accepted_at_ms],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn mark_failed(
        &self,
        runtime_id: &str,
        error: &str,
        failed_at_ms: i64,
    ) -> Result<(), NexusError> {
        self.store
            .identity_conn()
            .execute(
                "UPDATE initial_prompt_deliveries SET status = 'failed', error = ?2, \
                 failed_at_ms = ?3 WHERE runtime_id = ?1",
                params![runtime_id, error, failed_at_ms],
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }

    pub async fn get(&self, runtime_id: &str) -> Result<Option<InitialPromptRow>, NexusError> {
        let mut rows = self
            .store
            .identity_conn()
            .query(
                "SELECT runtime_id, agent_id, session_id, harness, template, rendered_prompt, \
                 client_message_id, status, error, created_at_ms, accepted_at_ms, failed_at_ms \
                 FROM initial_prompt_deliveries WHERE runtime_id = ?1",
                params![runtime_id],
            )
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        Ok(Some(InitialPromptRow {
            runtime_id: row.get(0).map_err(store_err)?,
            agent_id: row.get(1).map_err(store_err)?,
            session_id: row.get(2).map_err(store_err)?,
            harness: row.get(3).map_err(store_err)?,
            template: row.get(4).map_err(store_err)?,
            rendered_prompt: row.get(5).map_err(store_err)?,
            client_message_id: row.get(6).map_err(store_err)?,
            status: row.get(7).map_err(store_err)?,
            error: row.get(8).map_err(store_err)?,
            created_at_ms: row.get(9).map_err(store_err)?,
            accepted_at_ms: row.get(10).map_err(store_err)?,
            failed_at_ms: row.get(11).map_err(store_err)?,
        }))
    }

    pub async fn pending_sessions(
        &self,
        session_ids: &[String],
    ) -> Result<Vec<String>, NexusError> {
        let mut pending = Vec::new();
        for session_id in session_ids {
            let mut rows = self
                .store
                .identity_conn()
                .query(
                    "SELECT 1 FROM initial_prompt_deliveries \
                     WHERE session_id = ?1 AND status = 'pending' LIMIT 1",
                    params![session_id.as_str()],
                )
                .await
                .map_err(store_err)?;
            if rows.next().await.map_err(store_err)?.is_some() {
                pending.push(session_id.clone());
            }
        }
        Ok(pending)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialPromptRow {
    pub runtime_id: String,
    pub agent_id: String,
    pub session_id: String,
    pub harness: String,
    pub template: String,
    pub rendered_prompt: String,
    pub client_message_id: String,
    pub status: String,
    pub error: Option<String>,
    pub created_at_ms: i64,
    pub accepted_at_ms: Option<i64>,
    pub failed_at_ms: Option<i64>,
}
