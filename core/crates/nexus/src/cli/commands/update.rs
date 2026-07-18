//! Installation-aware Nexus update command.

use std::process::ExitCode;

use clap::Args;

use crate::update::install_context::{detect_install_context, InstallMethod};
use crate::update::system::execute_system_update;
use crate::update::transaction::{UpdateErrorReport, UpdateReport, UpdateStatus};

#[derive(Args, Debug, Clone, PartialEq, Eq)]
pub struct UpdateArgs {
    /// Check for an update without changing the installation.
    #[arg(long)]
    pub check: bool,
}

pub async fn run(args: UpdateArgs, json: bool) -> ExitCode {
    if let Err(error) = crate::daemon::lifecycle::ensure_operator_supervision() {
        return render_initial_error("OPERATOR_ONLY", &error.to_string(), json);
    }
    let context = match detect_install_context() {
        Ok(context) => context,
        Err(error) => {
            return render_initial_error("INSTALL_CONTEXT_INVALID", &error.to_string(), json)
        }
    };
    if matches!(
        context.method,
        InstallMethod::Manual | InstallMethod::Development
    ) {
        return render_initial_error(
            "UNMANAGED_INSTALL",
            "automatic update requires an npm or Cargo installation; use the package manager that installed Nexus",
            json,
        );
    }

    let result = execute_system_update(&context, args.check).await;
    render_report(&result.report, result.exit_code, json)
}

fn render_report(report: &UpdateReport, exit_code: u8, json: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).unwrap_or_else(|_| "{}".into())
        );
    } else {
        match report.status {
            UpdateStatus::Current => println!("Nexus {} is current", report.current_version),
            UpdateStatus::Available => println!(
                "Nexus update available: {} -> {}",
                report.current_version,
                report.target_version.as_deref().unwrap_or("unknown")
            ),
            UpdateStatus::Updated => println!(
                "Nexus updated: {} -> {}",
                report.current_version,
                report.target_version.as_deref().unwrap_or("unknown")
            ),
            UpdateStatus::RolledBack | UpdateStatus::Failed => {
                if let Some(error) = &report.error {
                    eprintln!("error: {}", error.message);
                    if let Some(command) = &error.recovery_command {
                        eprintln!("recovery: {command}");
                    }
                }
            }
        }
    }
    ExitCode::from(exit_code)
}

fn render_initial_error(code: &str, message: &str, json: bool) -> ExitCode {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "version": 1,
                "status": "failed",
                "error": UpdateErrorReport {
                    code: code.into(),
                    message: message.into(),
                    failed_facet: None,
                    log_path: None,
                    recovery_command: None,
                }
            })
        );
    } else {
        eprintln!("error: {message}");
    }
    ExitCode::from(1)
}
