//! Shared closed enums used across the contract surface. All serialize as their
//! lowercase token (matching the spec wire values) and map to TS string-literal unions.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// Sender/recipient kind. `app` = the human/web client session (backend §4 `sessions.kind`).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Agent,
    Human,
    Notification,
    App,
}

/// Where an entity originates. This is deliberately parallel to [`Kind`]: the nature vocabulary
/// remains closed while locality can be carried independently across wire and storage seams.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Locality {
    #[default]
    Local,
    External,
    Trusted,
}

/// Message scope (the one `to` contract resolves into one of these).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Dm,
    Thread,
    Topic,
}

/// Privilege tier (overview §5). The human user sits above tiers and is not represented here.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Agent,
    Admin,
}

/// Agent-session access grant role. `viewer` may observe; `coOwner` may observe and delegate.
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum AgentAccessRole {
    Viewer,
    CoOwner,
}

/// Presence state (backend §8).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Presence {
    Online,
    Busy,
    Offline,
}

/// Per-recipient delivery state machine (backend §2.1 / §4 `in_flight.state`).
#[typeshare]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryState {
    Pending,
    Notified,
    Delivered,
    Acked,
}
