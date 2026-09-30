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
