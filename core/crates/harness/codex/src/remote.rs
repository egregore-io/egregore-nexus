//! Command construction for a headed client attached to a Codex app-server.

use portable_pty::CommandBuilder;

fn remote_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("ws://")
        || endpoint.starts_with("wss://")
        || endpoint.starts_with("unix://")
    {
        endpoint.to_string()
    } else {
        format!("unix://{endpoint}")
    }
}

/// Build a fresh headed client using an explicitly resolved native executable.
pub fn remote_command_with_executable(
    executable: &str,
    endpoint: &str,
    cwd: Option<&str>,
    harness_args: &[String],
) -> CommandBuilder {
    let mut command = CommandBuilder::new(executable);
    command.arg("--remote");
    command.arg(remote_endpoint(endpoint));
    command.arg("--dangerously-bypass-approvals-and-sandbox");
    command.arg("--dangerously-bypass-hook-trust");
    command.arg("-c");
    command.arg("check_for_update_on_startup=false");
    if let Some(dir) = cwd {
        command.arg("-C");
        command.arg(dir);
        command.cwd(dir);
    }
    for arg in harness_args {
        command.arg(arg);
    }
    command
}

/// Build a headed client that resumes a stored thread through the app-server.
///
/// Remote resume inherits permissions from the app-server thread. Do not inject CLI permission
/// overrides here: Codex rejects them on this path. Caller-supplied native arguments remain intact.
pub fn remote_resume_command_with_executable(
    executable: &str,
    endpoint: &str,
    thread_id: &str,
    cwd: Option<&str>,
    harness_args: &[String],
) -> CommandBuilder {
    let mut command = CommandBuilder::new(executable);
    command.arg("resume");
    command.arg("--remote");
    command.arg(remote_endpoint(endpoint));
    command.arg("--dangerously-bypass-hook-trust");
    command.arg("-c");
    command.arg("check_for_update_on_startup=false");
    if let Some(dir) = cwd {
        command.arg("-C");
        command.arg(dir);
        command.cwd(dir);
    }
    for arg in harness_args {
        command.arg(arg);
    }
    command.arg(thread_id);
    command
}
