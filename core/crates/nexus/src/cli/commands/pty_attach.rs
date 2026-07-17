//! `nexus pty-attach <session>` — attach to a local headed harness terminal session by session id.
//!
//! The attach path resolves a backend-neutral local attach descriptor from the store and execs it
//! locally.

use std::process::ExitCode;

use nexus_contracts::SessionId;

use crate::cli::commands::attach::record_then_exec_pty_attach;
use crate::cli::read_client::ReadClient;
use crate::cli::store_client::StoreClient;

/// `nexus pty-attach <session>` — attach the operator's terminal to a headed harness terminal session.
#[derive(clap::Args, Debug)]
pub struct PtyAttachArgs {
    /// The Nexus session id printed by `nexus launch`.
    pub session: String,
}

/// Resolve a headed runtime by session id and attach to its local terminal session.
pub async fn pty_attach(store: &StoreClient, read: &ReadClient, a: PtyAttachArgs) -> ExitCode {
    attach_session(store, read, SessionId(a.session)).await
}

/// Attach to a specific Nexus session id. Used by `nexus launch` auto-attach.
pub async fn attach_session(
    store: &StoreClient,
    read: &ReadClient,
    session: SessionId,
) -> ExitCode {
    match read.pty_attach_descriptor_for_session(&session).await {
        Ok(descriptor) => record_then_exec_pty_attach(store, &descriptor).await,
        Err(e) => {
            eprintln!("error: {}", e.message);
            ExitCode::from(1)
        }
    }
}
