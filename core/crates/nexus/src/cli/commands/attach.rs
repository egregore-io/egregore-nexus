//! `nexus attach <name>` — attach to a local headed harness terminal session.
//!
//! Session output is rendered by store-backed `/agent` views, while terminal attach is a purely
//! local operation for headed harnesses.

use std::process::ExitCode;

use clap::Args;
use nexus_contracts::{codes, ContractError, SessionId, SpawnResponse};
use nexus_store::command_kinds;
use serde_json::json;

use crate::cli::read_client::{PtyAttachDescriptor, ReadClient};
use crate::cli::store_client::StoreClient;

/// `nexus attach <name>` — attach the operator's terminal to a headed harness terminal session.
#[derive(Args, Debug)]
pub struct AttachArgs {
    /// Agent name or Nexus session id for the headed runtime.
    pub name: String,
}

/// Resolve a headed runtime from the store and attach to its local terminal session.
///
/// If the stored runtime points at a dead local target, revive it through the same daemon-owned
/// launch command used by `nexus launch --detach`, then attach to the newly materialized session.
pub async fn attach(
    store: &StoreClient,
    read: &ReadClient,
    a: AttachArgs,
    _json: bool,
) -> ExitCode {
    match read.pty_attach_descriptor(&a.name).await {
        Ok(descriptor) => match local_attach_target_is_live(&descriptor) {
            Some(false) => revive_and_attach(store, read, &a.name, None).await,
            Some(true) | None => record_then_exec_pty_attach(store, &descriptor).await,
        },
        Err(e) => revive_and_attach(store, read, &a.name, Some(e)).await,
    }
}

async fn revive_and_attach(
    store: &StoreClient,
    read: &ReadClient,
    target: &str,
    original_error: Option<ContractError>,
) -> ExitCode {
    let plan = match read.attach_revive_plan(target).await {
        Ok(plan) => plan,
        Err(plan_error) => {
            eprintln!(
                "error: {}",
                attach_repair_error_message(original_error.as_ref(), &plan_error)
            );
            return ExitCode::from(1);
        }
    };

    eprintln!(
        "[attach] no live local terminal target for {}; reviving {:?} runtime ...",
        plan.name, plan.spawn.kind
    );
    let resp: SpawnResponse = match store
        .command(command_kinds::harness::LAUNCH, &plan.spawn)
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            eprintln!("error: revive launch failed: {}", e.message);
            return ExitCode::from(1);
        }
    };
    let descriptor = match read
        .pty_attach_descriptor_for_session(&resp.session_id)
        .await
    {
        Ok(descriptor) => descriptor,
        Err(e) => {
            eprintln!("error: revived runtime has no attach target: {}", e.message);
            return ExitCode::from(1);
        }
    };
    if matches!(local_attach_target_is_live(&descriptor), Some(false)) {
        eprintln!(
            "error: revived runtime {} did not materialize a local attach target",
            resp.session_id.0
        );
        return ExitCode::from(1);
    }
    record_then_exec_pty_attach(store, &descriptor).await
}

/// Record the metadata-only `attach` lifecycle event before yielding the process to the terminal
/// backend. The daemon command enforces that only the operator or the session itself can publish
/// the event; a failed record stops the attach so terminal takeovers are not silent.
pub async fn record_then_exec_pty_attach(
    store: &StoreClient,
    descriptor: &PtyAttachDescriptor,
) -> ExitCode {
    if let Err(error) = record_attach_event(store, &descriptor.session_id).await {
        eprintln!(
            "error: could not record attach lifecycle event: {}",
            attach_lifecycle_error_message(&descriptor.session_id, &error)
        );
        return ExitCode::from(1);
    }
    exec_pty_attach(descriptor)
}

/// Submit the daemon-managed attach lifecycle command for a Nexus session id.
pub async fn record_attach_event(
    store: &StoreClient,
    session: &SessionId,
) -> Result<(), ContractError> {
    let _: serde_json::Value = store
        .command(
            command_kinds::identity::ATTACH,
            &json!({ "sessionId": session.0.clone() }),
        )
        .await?;
    Ok(())
}

/// Take over a local headed harness using its backend-provided attach descriptor.
///
/// On Unix we `exec` so the operator's process becomes the backend attach client. If the operator
/// is already inside tmux and the runtime itself is tmux-backed, opening the harness in a new
/// window avoids nested-tmux attach failures. Raw PTY attaches stay in the current pane.
pub fn exec_pty_attach(descriptor: &PtyAttachDescriptor) -> ExitCode {
    let backend = descriptor.backend.as_str();
    let argv = descriptor.argv.as_slice();
    let Some((bin, rest)) = argv.split_first() else {
        eprintln!("error: empty {backend} attach argv");
        return ExitCode::from(1);
    };

    if should_open_tmux_window_for_attach(backend, std::env::var_os("TMUX").is_some()) {
        eprintln!(
            "[attach] in tmux - opening the harness in a new window of your current tmux ..."
        );
        let status = std::process::Command::new("tmux")
            .arg("new-window")
            .arg("-n")
            .arg("nexus")
            .args(argv)
            .status();
        return match status {
            Ok(s) if s.success() => {
                eprintln!("[attach] opened in a new tmux window.");
                ExitCode::SUCCESS
            }
            Ok(_) => {
                eprintln!("error: `tmux new-window` failed");
                ExitCode::from(1)
            }
            Err(e) => {
                eprintln!("error: could not run `tmux new-window`: {e}");
                ExitCode::from(1)
            }
        };
    }

    eprintln!("[attach] attaching via {backend} (detach: Ctrl-b d; agent keeps running) ...");
    let mut cmd = std::process::Command::new(bin);
    cmd.args(rest);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        let err = cmd.exec();
        eprintln!("error: failed to exec {backend} attach ({bin}): {err}");
        ExitCode::from(1)
    }
    #[cfg(not(unix))]
    {
        match cmd.status() {
            Ok(s) if s.success() => ExitCode::SUCCESS,
            Ok(_) => ExitCode::from(1),
            Err(e) => {
                eprintln!("error: failed to run {backend} attach ({bin}): {e}");
                ExitCode::from(1)
            }
        }
    }
}

fn should_open_tmux_window_for_attach(backend: &str, in_tmux: bool) -> bool {
    in_tmux && backend == "tmux"
}

fn attach_repair_error_message<'a>(
    _original_error: Option<&'a ContractError>,
    plan_error: &'a ContractError,
) -> &'a str {
    &plan_error.message
}

fn attach_lifecycle_error_message(session: &SessionId, error: &ContractError) -> String {
    if error.code == codes::UNAUTHORIZED {
        return format!(
            "{} (attach lifecycle events are operator/self-gated for session {}; use an operator client or attach only your own session)",
            error.message, session.0
        );
    }
    error.message.clone()
}

fn liveness_probe_argv(descriptor: &PtyAttachDescriptor) -> Option<&[String]> {
    descriptor.liveness_argv.as_deref()
}

fn local_attach_target_is_live(descriptor: &PtyAttachDescriptor) -> Option<bool> {
    if descriptor.backend == "raw-pty" {
        return raw_pty_attach_target_is_live(descriptor);
    }
    let argv = liveness_probe_argv(descriptor)?;
    let (bin, rest) = argv.split_first()?;
    std::process::Command::new(bin)
        .args(rest)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()
        .map(|status| status.success())
}

#[cfg(unix)]
fn raw_pty_attach_target_is_live(descriptor: &PtyAttachDescriptor) -> Option<bool> {
    let session = descriptor.argv.get(2)?;
    let endpoint = crate::daemon::terminal_socket::read_terminal_endpoint_manifest(&SessionId(
        session.clone(),
    ))?;
    Some(std::os::unix::net::UnixStream::connect(&endpoint.path).is_ok())
}

/// A named pipe exists only while its server holds an instance open, so `open` succeeding — or
/// failing because every instance is momentarily busy — means the daemon is serving it.
#[cfg(windows)]
fn raw_pty_attach_target_is_live(descriptor: &PtyAttachDescriptor) -> Option<bool> {
    use windows_sys::Win32::Foundation::{ERROR_FILE_NOT_FOUND, ERROR_PIPE_BUSY};

    let session = descriptor.argv.get(2)?;
    let endpoint = crate::daemon::terminal_socket::read_terminal_endpoint_manifest(&SessionId(
        session.clone(),
    ))?;
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&endpoint.path)
    {
        Ok(_) => Some(true),
        Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => Some(true),
        Err(e) if e.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) => Some(false),
        Err(_) => None,
    }
}

#[cfg(not(any(unix, windows)))]
fn raw_pty_attach_target_is_live(_descriptor: &PtyAttachDescriptor) -> Option<bool> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::read_client::PtyAttachDescriptor;
    use nexus_contracts::SessionId;

    #[test]
    fn local_attach_liveness_uses_descriptor_probe_argv() {
        let descriptor = PtyAttachDescriptor {
            session_id: SessionId("s_future".to_string()),
            backend: "future".to_string(),
            argv: vec!["future-attach".to_string(), "opaque-target".to_string()],
            liveness_argv: Some(vec![
                "future-probe".to_string(),
                "opaque-target".to_string(),
            ]),
        };

        assert_eq!(
            liveness_probe_argv(&descriptor),
            Some(["future-probe".to_string(), "opaque-target".to_string()].as_slice())
        );
    }

    #[test]
    fn revive_plan_error_wins_over_stale_descriptor_error() {
        let descriptor_error = ContractError {
            code: codes::NOT_FOUND,
            message: "error connecting to /tmp/stale.sock".to_string(),
        };
        let plan_error = ContractError {
            code: codes::INVALID_PARAMS,
            message: "cannot revive headed Claude runtime without a stored --resume session id"
                .to_string(),
        };

        assert_eq!(
            attach_repair_error_message(Some(&descriptor_error), &plan_error),
            "cannot revive headed Claude runtime without a stored --resume session id"
        );
    }

    #[test]
    fn attach_lifecycle_unauthorized_message_explains_operator_self_gate() {
        let message = attach_lifecycle_error_message(
            &SessionId("s_other_agent".to_string()),
            &ContractError {
                code: codes::UNAUTHORIZED,
                message: "unauthorized".to_string(),
            },
        );

        assert!(message.contains("operator/self-gated"), "{message}");
        assert!(message.contains("s_other_agent"), "{message}");
        assert!(
            message.contains("attach only your own session"),
            "{message}"
        );
    }

    #[test]
    fn dead_tmux_descriptor_is_not_treated_as_live() {
        let descriptor = PtyAttachDescriptor {
            session_id: SessionId("s_dead_tmux_attach".to_string()),
            backend: "tmux".to_string(),
            argv: vec!["tmux".to_string(), "attach".to_string()],
            liveness_argv: Some(vec!["false".to_string()]),
        };

        assert_eq!(local_attach_target_is_live(&descriptor), Some(false));
    }

    #[cfg(unix)]
    #[test]
    fn stale_raw_pty_manifest_is_not_treated_as_live() {
        let dir =
            std::env::temp_dir().join(format!("nexus-attach-dead-raw-{}", std::process::id()));
        let dir_s = dir.to_string_lossy().to_string();
        let _env = crate::cli::ambient::TestEnvGuard::new(&[(
            "NEXUS_TERMINAL_MANIFEST_DIR",
            Some(dir_s.as_str()),
        )]);
        let session = SessionId("s_dead_raw_attach".to_string());
        let manifest_path =
            crate::daemon::terminal_socket::terminal_endpoint_manifest_path(&session);
        std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        std::fs::write(
            &manifest_path,
            format!(
                r#"{{"session_id":"{}","path":"{}/missing.sock","token":"tok"}}"#,
                session.0,
                dir.display()
            ),
        )
        .unwrap();
        let descriptor = PtyAttachDescriptor {
            session_id: session.clone(),
            backend: "raw-pty".to_string(),
            argv: vec![
                "nexus".to_string(),
                "terminal-client".to_string(),
                session.0.clone(),
            ],
            liveness_argv: None,
        };

        assert_eq!(local_attach_target_is_live(&descriptor), Some(false));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tmux_window_wrapper_only_applies_to_tmux_backend() {
        assert!(should_open_tmux_window_for_attach("tmux", true));
        assert!(!should_open_tmux_window_for_attach("raw-pty", true));
        assert!(!should_open_tmux_window_for_attach("tmux", false));
    }
}
