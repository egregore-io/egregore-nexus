//! Topic / pub-sub ops (CLI §6, backend §10).

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// `nexus subscribe <topic>` — `group` = competing-consumer group.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeRequest {
    pub topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

/// `nexus unsubscribe <topic>`.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnsubscribeRequest {
    pub topic: String,
}

/// Subscribe result: the topic + the cursor the subscriber resumes from.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeResponse {
    pub topic: String,
    #[typeshare(serialized_as = "number")]
    pub cursor: i64,
}

/// One topic in a list (`nexus topics`).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicSummary {
    pub topic: String,
    pub subscribers: u32,
}

/// `GET /topics` / `nexus topics` response.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicListResponse {
    pub topics: Vec<TopicSummary>,
}
