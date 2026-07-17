//! Project scope labels. Project is a plain string namespace carried on sessions,
//! threads, topics, notifications, and search rows; Nexus no longer persists a
//! first-class `projects` table.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

use crate::ids::ProjectId;

/// A project scope label exposed for compatibility with older clients.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub project_id: ProjectId,
    /// Unique human-readable scope label.
    pub name: String,
    /// Compatibility metadata when a caller can provide a creator; not backed by
    /// a durable project primitive.
    pub created_by: String,
    /// Optional cwd/repo hint for legacy clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_path: Option<String>,
    /// Unix epoch millis.
    #[typeshare(serialized_as = "number")]
    pub created_at: i64,
}

/// Compatibility request for older clients that still present a project label
/// creation flow.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RegisterProjectRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_path: Option<String>,
}

/// Response: a compatibility project label record.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RegisterProjectResponse {
    pub project: Project,
}
