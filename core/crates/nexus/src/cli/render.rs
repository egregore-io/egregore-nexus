//! CLI output: human-readable by default, `--json` for machines, and a uniform
//! [`ContractError`] → message-to-stderr + non-zero exit. Identity is implicit, so nothing here
//! prints ids the agent must re-type.

use std::process::ExitCode;

use nexus_contracts::ContractError;
use serde::Serialize;

/// Serialize any typed response as JSON (the DTOs already carry `#[serde(rename_all = "camelCase")]`,
/// so keys are camelCase). Pretty-printed for human-friendly `--json` output.
pub fn to_json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
}

/// Print a typed response as JSON to stdout.
pub fn print_json<T: Serialize>(value: &T) {
    println!("{}", to_json_string(value));
}

/// Print a human one-liner (callers pass an already-formatted line/table) to stdout.
pub fn print_human(line: &str) {
    println!("{line}");
}

/// Terminal step for any command: render the result and return the process exit code. `Ok` → print
/// (JSON when `json`, else the lossless JSON fallback) + success; `Err` → message to stderr + a
/// non-zero exit. Use [`finish_with`] to supply a richer human table.
pub fn finish<T: Serialize>(result: Result<T, ContractError>, json: bool) -> ExitCode {
    finish_with(result, json, |v| to_json_string(v))
}

/// Like [`finish`] but lets a command supply its own human rendering while keeping `--json` uniform.
/// `Ok` → `human(&value)` (or JSON when `json`) + success; `Err` → message to stderr + a non-zero
/// exit. The exit code on error is `1` so scripts and the live test can branch on failure.
pub fn finish_with<T: Serialize>(
    result: Result<T, ContractError>,
    json: bool,
    human: impl FnOnce(&T) -> String,
) -> ExitCode {
    match result {
        Ok(value) => {
            if json {
                print_json(&value);
            } else {
                print_human(&human(&value));
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {}", e.message);
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_contracts::{codes, ContractError, Presence, SessionId, Tier, Whoami};

    #[test]
    fn json_render_is_valid_json_camelcase() {
        let who = Whoami {
            agent_id: None,
            name: Some("ben".into()),
            session_id: SessionId("s_1".into()),
            role: None,
            tier: Tier::Agent,
            project: "egregore".into(),
            presence: Presence::Online,
        };
        let s = to_json_string(&who);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["name"], "ben");
        assert_eq!(v["sessionId"], "s_1");
        assert_eq!(v["tier"], "agent");
    }

    #[test]
    fn error_render_exits_nonzero() {
        let err: Result<Whoami, ContractError> = Err(ContractError {
            code: codes::UNAUTHORIZED,
            message: "admin only".into(),
        });
        let code = finish(err, /*json=*/ false);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn ok_render_exits_zero() {
        let ok: Result<i32, ContractError> = Ok(7);
        assert_eq!(finish(ok, true), ExitCode::SUCCESS);
    }
}
