//! Daemon configuration (figment: defaults < file < env).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const GATEWAY_DELIVERY_MODE_FILE: &str = "gateway-delivery-mode";

/// Daemon-to-Gateway delivery policy. Absence of Gateway never changes this setting implicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum GatewayProjectionDeliveryMode {
    /// Retain a bounded RAM-only boot-epoch backlog until Gateway acknowledges projection.
    #[default]
    Buffered,
    /// Attempt the connected Gateway once and retain no replay or ACK state.
    BestEffort,
}

/// Whether canonical sends may bypass Gateway-owned message hooks when no provider is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HookGatewayMode {
    /// Preserve the local-first transport path when Gateway is absent or reconnecting.
    #[default]
    Optional,
    /// Fail sends before acceptance unless a hook-capable Gateway evaluates them.
    Required,
}

impl std::str::FromStr for HookGatewayMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "optional" => Ok(Self::Optional),
            "required" => Ok(Self::Required),
            other => Err(format!(
                "invalid hook Gateway mode {other:?}; expected optional or required"
            )),
        }
    }
}

impl std::str::FromStr for GatewayProjectionDeliveryMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "buffered" => Ok(Self::Buffered),
            "best_effort" => Ok(Self::BestEffort),
            other => Err(format!(
                "invalid Gateway projection delivery mode {other:?}; expected buffered or best_effort"
            )),
        }
    }
}

/// Capacity controls for the daemon's volatile Gateway projection backlog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayProjectionBacklogConfig {
    pub delivery_mode: GatewayProjectionDeliveryMode,
    pub max_events: usize,
    pub max_bytes: usize,
    pub batch_events: usize,
}

impl Default for GatewayProjectionBacklogConfig {
    fn default() -> Self {
        Self {
            delivery_mode: GatewayProjectionDeliveryMode::Buffered,
            max_events: 50_000,
            max_bytes: 67_108_864,
            batch_events: 1_000,
        }
    }
}

/// Daemon configuration (figment: defaults < file < env). All paths default under `~/.nexus`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Embedded libSQL file owned exclusively by the daemon.
    pub db_path: String,
    /// Legacy pre-v0.1 shared-server URL. Retained only so existing config files still parse;
    /// daemon-owned IPC means production runtime code must not open it.
    pub db_url: Option<String>,
    /// Legacy pre-v0.1 shared-server token. Retained for config compatibility and ignored by the
    /// daemon-owned embedded-store runtime.
    pub db_auth_token: String,
    pub drain_limit: u32,
    pub msg_preview_chars: u32,
    pub heartbeat_ttl_ms: i64,
    pub command_intent_retention_ms: i64,
    /// Size threshold for immediate operational-table reaping. Set <= 0 to disable.
    pub reap_size_threshold_mb: i64,
    /// Minimum interval between scheduled operational-table reap passes (`daily`, `weekly`, or a
    /// duration such as `12h`, `30m`, `60000ms`). Size threshold crossings can run earlier.
    pub reap_interval: String,
    /// Shared public-notification secret used independently by gateway edge verification and the
    /// daemon command worker's durable-envelope re-verification. Empty disables public `/notify`.
    pub hmac_secret: String,
    /// Default terminal backend for headed launches when the caller passes no `--backend`:
    /// `"pty"` (raw daemon-owned PTY, the built-in default) or `"tmux"` (multiplexer-owned;
    /// survives daemon restarts). Opt-in only — unset keeps `"pty"`. An explicit `--backend`
    /// always wins, and hermes stays tmux regardless. Settable in `nexus.toml`
    /// (`launch_backend = "tmux"`) or via `NEXUS_LAUNCH_BACKEND`.
    pub launch_backend: Option<String>,
    /// Volatile daemon-to-Gateway projection delivery and capacity policy.
    pub gateway_projection: GatewayProjectionBacklogConfig,
    /// Availability policy for the blocking Gateway-owned `before_send` hook boundary.
    pub hook_gateway_mode: HookGatewayMode,
    /// Bounded volatile child lane (native subagent streams): rows retained per owner session.
    pub child_stream_max_rows_per_session: u32,
    /// Bytes retained per owner session, counting data plus child identity, source reference
    /// and key.
    pub child_stream_max_bytes_per_session: u64,
    /// Lane records (verified or not, across root replacements) kept per owner session before
    /// tombstones compact into the session loss summary.
    pub child_stream_max_lanes_per_session: u32,
    /// Distinct unresolved lanes per owner session before new unresolved observations are refused.
    pub child_stream_max_unresolved_lanes_per_session: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            db_path: "~/.nexus/nexus.db".into(),
            db_url: None,
            db_auth_token: String::new(),
            drain_limit: 50,
            msg_preview_chars: 800,
            heartbeat_ttl_ms: 30_000,
            command_intent_retention_ms: 24 * 60 * 60 * 1_000,
            reap_size_threshold_mb: 1_024,
            reap_interval: "daily".into(),
            hmac_secret: String::new(),
            launch_backend: None,
            gateway_projection: GatewayProjectionBacklogConfig::default(),
            hook_gateway_mode: HookGatewayMode::Optional,
            child_stream_max_rows_per_session: 2048,
            child_stream_max_bytes_per_session: 4 * 1024 * 1024,
            child_stream_max_lanes_per_session: 256,
            child_stream_max_unresolved_lanes_per_session: 64,
        }
    }
}

/// Expand a leading `~` using explicit HOME or the OS user home (USERPROFILE on Windows).
pub fn expand_tilde(path: &str) -> String {
    if path == "~" || path.starts_with("~/") {
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(std::env::home_dir);
        if let Some(home) = home {
            return if path == "~" {
                home.to_string_lossy().into_owned()
            } else {
                home.join(&path[2..]).to_string_lossy().into_owned()
            };
        }
    }
    path.to_string()
}

impl Config {
    /// Load defaults, overlaid by the host-global `$NEXUS_HOME/nexus.toml` (default
    /// `~/.nexus/nexus.toml`), then a cwd-local `nexus.toml`, then `NEXUS_*` env vars.
    /// Paths (`db_path`) have any leading `~` expanded.
    ///
    /// The host-global layer keeps daemon-owned paths and lifecycle settings independent of the
    /// caller's working directory. Legacy `db_url` fields remain parseable for config-file
    /// compatibility but do not select the daemon's embedded store.
    pub fn load() -> Self {
        Self::try_load()
            .unwrap_or_else(|err| panic!("failed to load Nexus config; refusing defaults: {err}"))
    }

    /// Fallible form of [`Config::load`]. Malformed config is fatal to callers that use the host
    /// store: defaulting after a parse/type error can point writes at a new local DB.
    pub fn try_load() -> Result<Self, figment::Error> {
        use figment::{
            providers::{Env, Format, Serialized, Toml},
            Figment,
        };
        let nexus_home = std::env::var("NEXUS_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(expand_tilde("~/.nexus")));
        let global_toml = nexus_home.join("nexus.toml");
        let mut cfg: Config = Figment::from(Serialized::defaults(Config::default()))
            .merge(Toml::file(global_toml))
            .merge(Toml::file("nexus.toml"))
            .merge(Env::prefixed("NEXUS_"))
            .extract()?;
        cfg.db_path = expand_tilde(&cfg.db_path);
        cfg.db_url = cfg
            .db_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(expand_file_url_tilde);
        if let Ok(mode) = std::env::var("NEXUS_GATEWAY_DELIVERY_MODE") {
            cfg.gateway_projection.delivery_mode = mode.parse().map_err(figment::Error::from)?;
        }
        if let Some(mode) =
            read_gateway_projection_delivery_mode(&nexus_home).map_err(figment::Error::from)?
        {
            cfg.gateway_projection.delivery_mode = mode;
        }
        Ok(cfg)
    }

    /// Legacy pre-v0.1 location projection retained for callers that deserialize old config.
    /// The daemon does not use this method; it opens [`Self::daemon_store_path`].
    pub fn db_location(&self) -> String {
        self.db_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(expand_file_url_tilde)
            .unwrap_or_else(|| expand_tilde(&self.db_path))
    }

    /// Embedded durable-store path opened by the daemon.
    ///
    /// `db_url` intentionally does not participate: after the daemon-IPC cutover no producer needs
    /// a shared database endpoint, so selecting a remote/server URL here would reintroduce sqld and
    /// Hrana as a correctness dependency.
    pub fn daemon_store_path(&self) -> String {
        expand_tilde(&self.db_path)
    }

    /// Legacy pre-v0.1 auth-token projection. Blank means "no token".
    pub fn db_auth_token(&self) -> Option<&str> {
        let token = self.db_auth_token.trim();
        if token.is_empty() {
            None
        } else {
            Some(token)
        }
    }

    /// Legacy pre-v0.1 client URL projection retained for API compatibility. Production clients
    /// discover daemon IPC instead of opening this location.
    pub fn db_client_url(&self) -> String {
        let location = self.db_location();
        if is_client_url(&location) || location == ":memory:" {
            location
        } else {
            format!("file:{location}")
        }
    }
}

/// Path of the explicit operator delivery-mode choice. This separate atomic scalar means an
/// environment value may seed the first boot without overriding a later CLI decision.
pub fn gateway_projection_delivery_mode_path(nexus_home: &Path) -> PathBuf {
    nexus_home.join(GATEWAY_DELIVERY_MODE_FILE)
}

/// Persist an explicit operator delivery-mode choice atomically.
pub fn persist_gateway_projection_delivery_mode(
    nexus_home: &Path,
    mode: GatewayProjectionDeliveryMode,
) -> std::io::Result<()> {
    std::fs::create_dir_all(nexus_home)?;
    let path = gateway_projection_delivery_mode_path(nexus_home);
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let value = match mode {
        GatewayProjectionDeliveryMode::Buffered => "buffered\n",
        GatewayProjectionDeliveryMode::BestEffort => "best_effort\n",
    };
    std::fs::write(&temporary, value)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
    }
    let _ = std::fs::remove_file(&path);
    std::fs::rename(temporary, path)
}

/// Load the explicit operator choice when one exists.
pub fn read_gateway_projection_delivery_mode(
    nexus_home: &Path,
) -> Result<Option<GatewayProjectionDeliveryMode>, String> {
    let path = gateway_projection_delivery_mode_path(nexus_home);
    let value = match std::fs::read_to_string(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read {}: {error}", path.display())),
    };
    value
        .parse()
        .map(Some)
        .map_err(|error| format!("read {}: {error}", path.display()))
}

fn is_client_url(s: &str) -> bool {
    s.starts_with("file:")
        || s.starts_with("libsql://")
        || s.starts_with("http://")
        || s.starts_with("https://")
}

fn expand_file_url_tilde(s: &str) -> String {
    if let Some(rest) = s.strip_prefix("file:") {
        return format!("file:{}", expand_tilde(rest));
    }
    s.to_string()
}
