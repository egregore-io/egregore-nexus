//! # nexus-common
//!
//! Foundation primitives shared by every Nexus crate: the workspace-wide [`NexusError`] and its
//! mapping to/from [`nexus_contracts::ContractError`], prefixed id generators, the wire clock
//! [`now`], in-band provenance rendering ([`render_nexus`]/[`render_batch`]/[`is_plain_user_dm`]),
//! the figment-loaded [`Config`], and tracing init [`init_tracing`].

pub mod config;
pub mod credential;
pub mod error;
pub mod ids;
pub mod log;
pub mod presence;
pub mod process_ids;
pub mod provenance;
pub mod time;

pub use config::{
    gateway_projection_delivery_mode_path, persist_gateway_projection_delivery_mode,
    read_gateway_projection_delivery_mode, Config, GatewayProjectionBacklogConfig,
    GatewayProjectionDeliveryMode,
};
pub use credential::hash_runtime_credential;
pub use error::NexusError;
pub use ids::{new_message_id, new_project_id, new_session_id, new_source_token, new_thread_id};
pub use log::init_tracing;
pub use process_ids::RuntimeProcessIds;
pub use provenance::{
    is_plain_user_dm, parse_outbound, render_batch, render_batch_for, render_injected_turn_for,
    render_nexus, OutboundMsg, OutboundTarget,
};
pub use time::now;
