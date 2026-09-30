//! One-time interactive consent for installed background services.

use async_trait::async_trait;
use std::fs::{self, File, OpenOptions};
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

use crate::{
    daemon::lifecycle, gateway_lifecycle, gateway_service, webconsole_lifecycle, webconsole_service,
};

#[derive(Clone, Copy, Debug, Default)]
#[doc(hidden)]
pub struct Components {
    pub gateway: bool,
    pub webconsole: bool,
}

#[async_trait(?Send)]
#[doc(hidden)]
pub trait SetupBackend {
    fn components(&mut self) -> Components;
    fn ask(&mut self, components: Components) -> Result<bool, String>;
    async fn daemon(&mut self) -> Result<(), String>;
    async fn gateway(&mut self) -> Result<(), String>;
    async fn webconsole(&mut self) -> Result<(), String>;
    fn remember(&mut self, accepted: bool) -> Result<(), String>;
}

#[doc(hidden)]
pub async fn run_setup(backend: &mut impl SetupBackend) -> Result<(), String> {
    let components = backend.components();
    let accepted = backend.ask(components)?;
    if accepted {
        if components.webconsole && !components.gateway {
            return Err(
                "Webconsole requires the missing Gateway package; install the complete stack first"
                    .into(),
            );
        }
        backend.daemon().await?;
        if components.gateway {
            backend.gateway().await?;
            if components.webconsole {
                backend.webconsole().await?;
            }
        }
    }
    backend.remember(accepted)
}

#[doc(hidden)]
pub fn should_prompt(args: &[String], terminal: bool, operator: bool, ci: bool) -> bool {
    terminal
        && operator
        && !ci
        && !args.iter().any(|arg| {
            matches!(
                arg.as_str(),
                "--help" | "-h" | "--version" | "-V" | "--json" | "--quiet" | "-q"
            )
        })
        && (args.is_empty()
            || matches!(
                args[0].as_str(),
                "launch" | "resume" | "attach" | "members" | "agents" | "threads" | "topics"
            ))
}

/// Offer setup only for an eligible, interactive operator. No package installation occurs here.
pub async fn maybe_setup(args: &[String]) -> Result<bool, String> {
    let terminal =
        io::stdin().is_terminal() && io::stdout().is_terminal() && io::stderr().is_terminal();
    let ci = ["CI", "NEXUS_NO_SETUP", "NEXUS_NO_AUTOSTART"]
        .iter()
        .any(|key| {
            std::env::var_os(key)
                .is_some_and(|value| !value.is_empty() && value != "0" && value != "false")
        });
    if !should_prompt(
        args,
        terminal,
        lifecycle::ensure_operator_supervision().is_ok(),
        ci,
    ) {
        return Ok(false);
    }
    let home = lifecycle::nexus_home();
    let Some(decision) = Decision::acquire(&home)? else {
        return Ok(false);
    };
    if decision.remembered()? {
        return Ok(false);
    }
    let mut backend = SystemSetup { decision };
    run_setup(&mut backend).await.map_err(|cause| format!(
        "background setup incomplete: {cause}. Earlier services may already be running; no completed choice was saved."
    ))?;
    Ok(true)
}

#[doc(hidden)]
pub struct Decision {
    home: PathBuf,
    _lock: File,
}

impl Decision {
    pub fn acquire(home: &Path) -> Result<Option<Self>, String> {
        fs::create_dir_all(home).map_err(|e| e.to_string())?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options
            .open(home.join("first-run.lock"))
            .map_err(|e| e.to_string())?;
        match lock.try_lock() {
            Ok(()) => Ok(Some(Self {
                home: home.to_owned(),
                _lock: lock,
            })),
            Err(std::fs::TryLockError::WouldBlock) => {
                Err("another Nexus setup is in progress; retry after it finishes".into())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    pub fn remembered(&self) -> Result<bool, String> {
        let file = match File::open(self.home.join("first-run.json")) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.to_string()),
        };
        let mut bytes = Vec::new();
        file.take(1025)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        let value = serde_json::from_slice::<serde_json::Value>(&bytes)
            .map_err(|e| format!("invalid first-run.json; preserving existing file: {e}"))?;
        if bytes.len() > 1024 || value["version"] != 1 || !value["background"].is_boolean() {
            return Err("invalid first-run.json; preserving existing file".into());
        }
        Ok(true)
    }

    pub fn remember(&self, accepted: bool) -> Result<(), String> {
        let path = self
            .home
            .join(format!("first-run.{}.tmp", std::process::id()));
        let mut created = false;
        let result = (|| -> io::Result<()> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&path)?;
            created = true;
            writeln!(
                file,
                "{}",
                serde_json::json!({ "version": 1, "background": accepted })
            )?;
            file.sync_all()?;
            fs::rename(&path, self.home.join("first-run.json"))
        })();
        if created && result.is_err() {
            let _ = fs::remove_file(&path);
        }
        result.map_err(|e| e.to_string())
    }
}

struct SystemSetup {
    decision: Decision,
}

#[async_trait(?Send)]
impl SetupBackend for SystemSetup {
    fn components(&mut self) -> Components {
        Components {
            gateway: gateway_lifecycle::resolve_installed_gateway().is_ok(),
            webconsole: webconsole_lifecycle::resolve_installed_webconsole().is_ok(),
        }
    }

    fn ask(&mut self, components: Components) -> Result<bool, String> {
        eprintln!("Nexus first run — installed components:");
        eprintln!(
            "  Daemon: installed; service registered: {}",
            lifecycle::service_installed()
        );
        eprintln!(
            "  Gateway: {}; service registered: {}",
            if components.gateway {
                "installed"
            } else {
                "not found"
            },
            gateway_service::gateway_service_status()
                .map(|s| s.installed)
                .unwrap_or(false)
        );
        eprintln!(
            "  Webconsole: {}; service registered: {}",
            if components.webconsole {
                "installed"
            } else {
                "not found"
            },
            webconsole_service::installed()
        );
        if !components.gateway || !components.webconsole {
            eprintln!("For the complete stack: npm install -g @egregore/nexus (nothing will be downloaded by setup).");
        }
        eprintln!("This enables per-user services at login. Existing services may restart; no browser tab will open.");
        eprint!("Run installed Nexus components in the background now and at login? [y/N] ");
        io::stderr().flush().map_err(|e| e.to_string())?;
        let mut answer = String::new();
        if io::stdin()
            .read_line(&mut answer)
            .map_err(|e| e.to_string())?
            == 0
        {
            return Err("input closed before a choice was made".into());
        }
        Ok(matches!(
            answer.trim().to_ascii_lowercase().as_str(),
            "y" | "yes"
        ))
    }

    async fn daemon(&mut self) -> Result<(), String> {
        lifecycle::install_service_only()
            .await
            .map_err(|e| e.to_string())
    }
    async fn gateway(&mut self) -> Result<(), String> {
        if !gateway_service::gateway_service_status()
            .map(|s| s.installed)
            .unwrap_or(false)
        {
            gateway_lifecycle::stop_gateway(false).map_err(|e| e.to_string())?;
        }
        gateway_service::install_gateway_service()
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn webconsole(&mut self) -> Result<(), String> {
        webconsole_service::install().await
    }
    fn remember(&mut self, accepted: bool) -> Result<(), String> {
        self.decision.remember(accepted)?;
        eprintln!(
            "{}",
            if accepted {
                "Nexus background services enabled and running."
            } else {
                "Background startup declined. You can enable services later with nexus daemon/gateway/webconsole install."
            }
        );
        Ok(())
    }
}
