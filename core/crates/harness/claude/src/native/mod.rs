//! Claude native headed bridge support.
//!
//! This module owns Claude-specific headed sidecar files: a launch-local bridge directory, hook
//! settings, and later the transcript/message-delta forwarder. It is intentionally harness-local;
//! it does not add daemon RPC, WebSocket, Unix-socket, or gateway control surfaces.

pub mod bridge;
pub mod codec;
pub mod forwarder;
pub mod hooks;
pub mod message_delta;
pub mod model_reporting;
pub mod response_usage;
pub mod resume;
pub mod statusline;
pub mod statusline_capture;
pub mod transcript;
