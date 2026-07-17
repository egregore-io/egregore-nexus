//! Shared local-operator identity markers for zero-login local control paths.
//!
//! `LOCAL_OPERATOR_SESSION_ID` is the durable marker used for authorization and
//! routing. The display name is human-facing only and may come from trusted
//! local operator config or the operating-system account.

use std::path::PathBuf;

pub(crate) const LOCAL_OPERATOR_SESSION_ID: &str = "local-operator";
const LEGACY_LOCAL_OPERATOR_NAME: &str = "operator";

pub(crate) fn display_name() -> String {
    operator_config_name()
        .or_else(os_account_name)
        .unwrap_or_else(|| LEGACY_LOCAL_OPERATOR_NAME.to_string())
}

fn operator_config_name() -> Option<String> {
    let raw = std::fs::read_to_string(nexus_home()?.join("operator.json")).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&raw).ok()?;
    clean_name(parsed.get("name")?.as_str())
}

fn nexus_home() -> Option<PathBuf> {
    std::env::var("NEXUS_HOME")
        .ok()
        .and_then(|value| clean_name(Some(&value)))
        .map(PathBuf::from)
        .or_else(|| home_dir().map(|home| home.join(".nexus")))
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        std::env::var("USERPROFILE")
            .ok()
            .and_then(|value| clean_name(Some(&value)))
            .map(PathBuf::from)
    }

    #[cfg(not(windows))]
    {
        std::env::var("HOME")
            .ok()
            .and_then(|value| clean_name(Some(&value)))
            .map(PathBuf::from)
    }
}

fn os_account_name() -> Option<String> {
    #[cfg(windows)]
    {
        let username = std::env::var("USERNAME")
            .ok()
            .and_then(|value| clean_name(Some(&value)));
        let domain = std::env::var("USERDOMAIN")
            .ok()
            .and_then(|value| clean_name(Some(&value)));
        let computer = std::env::var("COMPUTERNAME")
            .ok()
            .and_then(|value| clean_name(Some(&value)));

        if let (Some(domain), Some(username)) = (domain, username.clone()) {
            if computer.as_deref() != Some(domain.as_str()) && !username.contains('\\') {
                return Some(format!("{domain}\\{username}"));
            }
        }
        username
    }

    #[cfg(not(windows))]
    {
        std::env::var("USER")
            .ok()
            .and_then(|value| clean_name(Some(&value)))
            .or_else(|| {
                std::env::var("LOGNAME")
                    .ok()
                    .and_then(|value| clean_name(Some(&value)))
            })
    }
}

fn clean_name(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
