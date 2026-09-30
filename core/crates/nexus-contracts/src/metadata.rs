//! Freeform metadata bags for core Nexus entities.
//!
//! Metadata is intentionally opaque to the bus. Authenticated callers may attach arbitrary JSON
//! to core entities for launch tooling, UI hints, and integration state without changing the
//! entity's routing or authorization semantics.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// Core entity families that support an opaque metadata JSON bag.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MetadataEntityKind {
    Message,
    Session,
    Thread,
    Agent,
}

/// Set/replace the metadata bag for one project-scoped entity.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetadataSetRequest {
    pub entity: MetadataEntityKind,
    /// Entity id. For threads and agents this may be the public name; messages/sessions use ids.
    pub id: String,
    /// Opaque JSON payload. Nexus stores and returns it without interpreting its shape.
    pub metadata: serde_json::Value,
}

/// Read request for one entity metadata bag.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetadataGetRequest {
    pub entity: MetadataEntityKind,
    pub id: String,
}

/// Metadata response for REST and command-intent callers.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetadataResponse {
    pub entity: MetadataEntityKind,
    pub id: String,
    pub metadata: serde_json::Value,
}
