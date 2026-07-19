//! Freeform entity metadata bags.
//!
//! Metadata is deliberately opaque to Nexus: authenticated callers may attach any JSON value to
//! core entities for integrations and launch tooling. Rows remain project-scoped, and callers own
//! interpretation of the JSON shape.

use libsql::params;

use nexus_common::NexusError;

use crate::error::{store_err, store_msg};
use crate::repos::sessions::{get_opt_text, get_text};
use crate::state::Store;

/// Core entities that carry a generic metadata JSON bag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataEntity {
    Message,
    Session,
    Thread,
    Agent,
}

impl MetadataEntity {
    pub fn from_token(token: &str) -> Option<Self> {
        match token {
            "message" | "messages" => Some(Self::Message),
            "session" | "sessions" => Some(Self::Session),
            "thread" | "threads" => Some(Self::Thread),
            "agent" | "agents" => Some(Self::Agent),
            _ => None,
        }
    }

    pub fn token(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Session => "session",
            Self::Thread => "thread",
            Self::Agent => "agent",
        }
    }
}

/// Metadata response returned by the store and daemon command.
#[derive(Debug, Clone, PartialEq)]
pub struct EntityMetadataRow {
    pub entity: MetadataEntity,
    pub id: String,
    pub metadata: serde_json::Value,
}

/// Repository for reading and replacing core entity metadata.
pub struct Metadata<'a> {
    store: &'a Store,
}

impl<'a> Metadata<'a> {
    /// Bind a repo to the shared store connection.
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Fetch the current metadata bag. Missing/NULL metadata reads as `{}`.
    pub async fn get(
        &self,
        project: &str,
        entity: MetadataEntity,
        id: &str,
    ) -> Result<EntityMetadataRow, NexusError> {
        let Some(raw) = self.select_metadata(project, entity, id).await? else {
            return Err(NexusError::NotFound(format!("{}:{id}", entity.token())));
        };
        let metadata = match raw {
            Some(raw) => serde_json::from_str(&raw).map_err(store_msg)?,
            None => serde_json::json!({}),
        };
        Ok(EntityMetadataRow {
            entity,
            id: id.to_string(),
            metadata,
        })
    }

    /// Replace the metadata JSON bag for one entity in `project`.
    pub async fn set(
        &self,
        project: &str,
        entity: MetadataEntity,
        id: &str,
        metadata: &serde_json::Value,
    ) -> Result<EntityMetadataRow, NexusError> {
        let raw = serde_json::to_string(metadata).map_err(store_msg)?;
        let changed = match entity {
            MetadataEntity::Message => {
                self.store
                    .conn
                    .execute(
                        "UPDATE messages SET metadata_json = ?3 \
                         WHERE project = ?1 AND message_id = ?2",
                        params![project, id, raw],
                    )
                    .await
            }
            MetadataEntity::Session => {
                self.store
                    .conn
                    .execute(
                        "UPDATE sessions SET metadata_json = ?3 \
                         WHERE project = ?1 AND session_id = ?2",
                        params![project, id, raw],
                    )
                    .await
            }
            MetadataEntity::Thread => {
                self.store
                    .conn
                    .execute(
                        "UPDATE threads SET metadata_json = ?3 \
                         WHERE project = ?1 AND name = ?2",
                        params![project, id, raw],
                    )
                    .await
            }
            MetadataEntity::Agent => {
                self.store
                    .identity_conn()
                    .execute(
                        "UPDATE agents SET metadata_json = ?3 \
                         WHERE project = ?1 AND (agent_id = ?2 OR name = ?2)",
                        params![project, id, raw],
                    )
                    .await
            }
        }
        .map_err(store_err)?;
        if changed == 0 {
            return Err(NexusError::NotFound(format!("{}:{id}", entity.token())));
        }
        self.get(project, entity, id).await
    }

    /// Recursively merge one Gateway-owned hook result into a canonical message metadata bag.
    ///
    /// Message ids are globally unique transport identities, so this internal Gateway path does
    /// not use `project` as an authorization partition. Replaying the same invocation is safe:
    /// object/scalar patches are naturally idempotent and signed `executedBy` entries are keyed by
    /// their stable invocation id.
    pub async fn merge_message_hook_metadata(
        &self,
        id: &str,
        invocation_id: &str,
        patch: &serde_json::Value,
    ) -> Result<serde_json::Value, NexusError> {
        if invocation_id.trim().is_empty() {
            return Err(NexusError::Invalid(
                "hook metadata merge requires invocationId".into(),
            ));
        }
        if !patch.is_object() {
            return Err(NexusError::Invalid(
                "hook metadata merge requires an object metadata patch".into(),
            ));
        }

        let txn = self.store.begin_write_txn("hook.metadata.merge").await?;
        let result = async {
            let mut rows = txn
                .query(
                    "SELECT metadata_json FROM messages WHERE message_id = ?1 LIMIT 1",
                    params![id],
                )
                .await?;
            let Some(row) = rows.next().await.map_err(store_err)? else {
                return Err(NexusError::NotFound(format!("message:{id}")));
            };
            let mut metadata = get_opt_text(&row, 0)?
                .map(|raw| serde_json::from_str::<serde_json::Value>(&raw))
                .transpose()
                .map_err(store_msg)?
                .unwrap_or_else(|| serde_json::json!({}));
            merge_hook_value(&mut metadata, patch, &mut Vec::new());
            let raw = serde_json::to_string(&metadata).map_err(store_msg)?;
            txn.execute(
                "UPDATE messages SET metadata_json = ?2 WHERE message_id = ?1",
                params![id, raw],
            )
            .await?;
            Ok(metadata)
        }
        .await;
        match result {
            Ok(metadata) => {
                txn.commit().await?;
                Ok(metadata)
            }
            Err(error) => {
                txn.rollback(&error).await?;
                Err(error)
            }
        }
    }

    async fn select_metadata(
        &self,
        project: &str,
        entity: MetadataEntity,
        id: &str,
    ) -> Result<Option<Option<String>>, NexusError> {
        let sql = match entity {
            MetadataEntity::Message => {
                "SELECT message_id, metadata_json FROM messages WHERE project = ?1 AND message_id = ?2"
            }
            MetadataEntity::Session => {
                "SELECT session_id, metadata_json FROM sessions WHERE project = ?1 AND session_id = ?2"
            }
            MetadataEntity::Thread => {
                "SELECT name, metadata_json FROM threads WHERE project = ?1 AND name = ?2"
            }
            MetadataEntity::Agent => {
                "SELECT agent_id, metadata_json FROM agents \
                 WHERE project = ?1 AND (agent_id = ?2 OR name = ?2)"
            }
        };
        let conn = match entity {
            MetadataEntity::Agent => self.store.identity_conn(),
            _ => self.store.conn.clone(),
        };
        let mut rows = conn
            .query(sql, params![project, id])
            .await
            .map_err(store_err)?;
        let Some(row) = rows.next().await.map_err(store_err)? else {
            return Ok(None);
        };
        let _ = get_text(&row, 0)?;
        Ok(Some(get_opt_text(&row, 1)?))
    }
}

fn merge_hook_value(
    base: &mut serde_json::Value,
    patch: &serde_json::Value,
    path: &mut Vec<String>,
) {
    if path == &["_nexus", "hooks", "executedBy"] {
        if let (Some(current), Some(incoming)) = (base.as_array_mut(), patch.as_array()) {
            for entry in incoming {
                let invocation_id = entry
                    .get("invocationId")
                    .and_then(serde_json::Value::as_str);
                let duplicate = invocation_id.is_some_and(|id| {
                    current.iter().any(|existing| {
                        existing
                            .get("invocationId")
                            .and_then(serde_json::Value::as_str)
                            == Some(id)
                    })
                });
                if !duplicate {
                    current.push(entry.clone());
                }
            }
            return;
        }
    }
    if let (Some(base), Some(patch)) = (base.as_object_mut(), patch.as_object()) {
        for (key, value) in patch {
            path.push(key.clone());
            match base.get_mut(key) {
                Some(current) => merge_hook_value(current, value, path),
                None => {
                    base.insert(key.clone(), value.clone());
                }
            }
            path.pop();
        }
        return;
    }
    *base = patch.clone();
}
