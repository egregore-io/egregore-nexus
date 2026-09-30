//! Canonical Gateway runtime snapshots on the existing `/api/agui/ws` socket.
//! A fresh, socket-unique subscription always hydrates; sequence orders that subscription only,
//! not durable replay. Runtime reportRevision remains the evidence authority. Unavailable is
//! not an empty snapshot: consumers must clear live availability and retain only labeled history.
use crate::{AgentId, AgentRuntimeSummary};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use typeshare::typeshare;

#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeSubscribeTag {
    #[serde(rename = "runtime.subscribe")]
    Subscribe,
}
#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeUnsubscribeTag {
    #[serde(rename = "runtime.unsubscribe")]
    Unsubscribe,
}
#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeSnapshotTag {
    #[serde(rename = "runtime.snapshot")]
    Snapshot,
}
#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeUnavailableTag {
    #[serde(rename = "runtime.unavailable")]
    Unavailable,
}

#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RuntimeUnavailableReason {
    Unauthorized,
    NotFound,
    Unavailable,
    InvalidSnapshot,
}

#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeSubscribeFrame {
    pub t: RuntimeSubscribeTag,
    #[serde(with = "subscription_id")]
    pub subscription_id: String,
    pub agent_id: AgentId,
}
#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeUnsubscribeFrame {
    pub t: RuntimeUnsubscribeTag,
    #[serde(with = "subscription_id")]
    pub subscription_id: String,
}
#[typeshare]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeSnapshotFrame {
    pub t: RuntimeSnapshotTag,
    #[serde(with = "subscription_id")]
    pub subscription_id: String,
    pub agent_id: AgentId,
    /// Positive safe integer, starting at one for a new subscription. Not an afterSeq cursor.
    #[typeshare(serialized_as = "number")]
    #[serde(with = "sequence")]
    pub sequence: u64,
    /// Complete agent-scoped canonical list, including stopped history. Empty means absence.
    pub runtimes: Vec<AgentRuntimeSummary>,
}
#[typeshare]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeUnavailableFrame {
    pub t: RuntimeUnavailableTag,
    #[serde(with = "subscription_id")]
    pub subscription_id: String,
    pub agent_id: AgentId,
    #[typeshare(serialized_as = "number")]
    #[serde(with = "sequence")]
    pub sequence: u64,
    /// Bounded public category; no internal database/authentication detail is exposed.
    pub reason: RuntimeUnavailableReason,
}

mod subscription_id {
    use super::*;
    fn valid(value: &str) -> bool {
        !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
    }
    pub fn serialize<S: Serializer>(value: &str, s: S) -> Result<S::Ok, S::Error> {
        if !valid(value) {
            return Err(serde::ser::Error::custom("invalid runtime subscription id"));
        }
        value.serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
        let value = String::deserialize(d)?;
        if !valid(&value) {
            return Err(serde::de::Error::custom("invalid runtime subscription id"));
        }
        Ok(value)
    }
}
mod sequence {
    use super::*;
    fn valid(value: u64) -> bool {
        (1..=crate::MAX_MODEL_REPORT_REVISION).contains(&value)
    }
    pub fn serialize<S: Serializer>(value: &u64, s: S) -> Result<S::Ok, S::Error> {
        if !valid(*value) {
            return Err(serde::ser::Error::custom(
                "invalid runtime snapshot sequence",
            ));
        }
        value.serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        let value = u64::deserialize(d)?;
        if !valid(value) {
            return Err(serde::de::Error::custom(
                "invalid runtime snapshot sequence",
            ));
        }
        Ok(value)
    }
}
