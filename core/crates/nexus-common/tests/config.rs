use nexus_common::{
    persist_gateway_projection_delivery_mode, Config, GatewayProjectionDeliveryMode,
    HookGatewayMode,
};
use std::sync::{Mutex, OnceLock};

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

fn temp_config_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-{label}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn gateway_projection_defaults_to_buffered_and_env_can_explicitly_opt_out() {
    let defaults = Config::default();
    assert_eq!(
        defaults.gateway_projection.delivery_mode,
        GatewayProjectionDeliveryMode::Buffered
    );
    assert_eq!(defaults.gateway_projection.max_events, 50_000);
    assert_eq!(defaults.gateway_projection.max_bytes, 67_108_864);
    assert_eq!(defaults.gateway_projection.batch_events, 1_000);

    let _guard = env_lock();
    let dir = temp_config_dir("gateway-projection-mode");
    let previous_home = std::env::var("NEXUS_HOME").ok();
    let previous_mode = std::env::var("NEXUS_GATEWAY_DELIVERY_MODE").ok();
    std::env::set_var("NEXUS_HOME", &dir);
    std::env::set_var("NEXUS_GATEWAY_DELIVERY_MODE", "best_effort");
    let loaded = Config::try_load().unwrap();
    assert_eq!(
        loaded.gateway_projection.delivery_mode,
        GatewayProjectionDeliveryMode::BestEffort
    );

    match previous_home {
        Some(value) => std::env::set_var("NEXUS_HOME", value),
        None => std::env::remove_var("NEXUS_HOME"),
    }
    match previous_mode {
        Some(value) => std::env::set_var("NEXUS_GATEWAY_DELIVERY_MODE", value),
        None => std::env::remove_var("NEXUS_GATEWAY_DELIVERY_MODE"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn hook_gateway_defaults_optional_and_exact_environment_can_require_it() {
    assert_eq!(
        Config::default().hook_gateway_mode,
        HookGatewayMode::Optional
    );

    let _guard = env_lock();
    let dir = temp_config_dir("hook-gateway-mode");
    let previous_home = std::env::var("NEXUS_HOME").ok();
    let previous_mode = std::env::var("NEXUS_HOOK_GATEWAY_MODE").ok();
    std::env::set_var("NEXUS_HOME", &dir);
    std::env::set_var("NEXUS_HOOK_GATEWAY_MODE", "required");
    assert_eq!(
        Config::try_load().unwrap().hook_gateway_mode,
        HookGatewayMode::Required
    );

    match previous_home {
        Some(value) => std::env::set_var("NEXUS_HOME", value),
        None => std::env::remove_var("NEXUS_HOME"),
    }
    match previous_mode {
        Some(value) => std::env::set_var("NEXUS_HOOK_GATEWAY_MODE", value),
        None => std::env::remove_var("NEXUS_HOOK_GATEWAY_MODE"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn explicit_gateway_delivery_mode_survives_and_overrides_later_environment_seeding() {
    let _guard = env_lock();
    let dir = temp_config_dir("gateway-projection-explicit");
    let previous_home = std::env::var("NEXUS_HOME").ok();
    let previous_mode = std::env::var("NEXUS_GATEWAY_DELIVERY_MODE").ok();
    std::env::set_var("NEXUS_HOME", &dir);
    std::env::set_var("NEXUS_GATEWAY_DELIVERY_MODE", "best_effort");

    persist_gateway_projection_delivery_mode(&dir, GatewayProjectionDeliveryMode::Buffered)
        .expect("persist explicit operator mode");
    let loaded = Config::try_load().expect("load explicit operator mode");
    assert_eq!(
        loaded.gateway_projection.delivery_mode,
        GatewayProjectionDeliveryMode::Buffered,
        "an explicit operator choice must win over the bootstrap environment"
    );

    match previous_home {
        Some(value) => std::env::set_var("NEXUS_HOME", value),
        None => std::env::remove_var("NEXUS_HOME"),
    }
    match previous_mode {
        Some(value) => std::env::set_var("NEXUS_GATEWAY_DELIVERY_MODE", value),
        None => std::env::remove_var("NEXUS_GATEWAY_DELIVERY_MODE"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn db_location_prefers_db_url() {
    let cfg = Config {
        db_path: "/tmp/wrong.db".into(),
        db_url: Some("http://127.0.0.1:8080".into()),
        ..Config::default()
    };

    assert_eq!(cfg.db_location(), "http://127.0.0.1:8080");
    assert_eq!(cfg.db_client_url(), "http://127.0.0.1:8080");
}

#[test]
fn db_client_url_derives_file_url_from_legacy_path() {
    let cfg = Config {
        db_path: "/tmp/nexus.db".into(),
        db_url: None,
        ..Config::default()
    };

    assert_eq!(cfg.db_location(), "/tmp/nexus.db");
    assert_eq!(cfg.db_client_url(), "file:/tmp/nexus.db");
}

#[test]
fn daemon_store_path_always_selects_the_embedded_file() {
    let cfg = Config {
        db_path: "/tmp/nexus-embedded.db".into(),
        db_url: Some("http://127.0.0.1:4141".into()),
        db_auth_token: "retired-server-token".into(),
        ..Config::default()
    };

    assert_eq!(cfg.daemon_store_path(), "/tmp/nexus-embedded.db");
}

#[test]
fn blank_db_url_falls_back_to_db_path() {
    let cfg = Config {
        db_path: "/tmp/nexus.db".into(),
        db_url: Some("  ".into()),
        ..Config::default()
    };

    assert_eq!(cfg.db_location(), "/tmp/nexus.db");
}

#[test]
fn blank_db_auth_token_is_none() {
    let cfg = Config {
        db_auth_token: "  ".into(),
        ..Config::default()
    };
    assert_eq!(cfg.db_auth_token(), None);

    let cfg = Config {
        db_auth_token: "tok".into(),
        ..Config::default()
    };
    assert_eq!(cfg.db_auth_token(), Some("tok"));
}

#[test]
fn db_location_reads_host_global_nexus_toml_from_nexus_home() {
    // A server-mode db_url configured once in $NEXUS_HOME/nexus.toml must reach every process
    // without NEXUS_DB_URL in the environment (2026-07-09 empty-file-store incident).
    let _guard = env_lock();
    let dir = temp_config_dir("global-toml");
    std::fs::write(
        dir.join("nexus.toml"),
        "db_url = \"http://127.0.0.1:4141\"\n",
    )
    .unwrap();
    let prev_home = std::env::var("NEXUS_HOME").ok();
    let prev_url = std::env::var("NEXUS_DB_URL").ok();
    std::env::set_var("NEXUS_HOME", &dir);
    std::env::remove_var("NEXUS_DB_URL");

    let cfg = nexus_common::Config::load();
    assert_eq!(cfg.db_location(), "http://127.0.0.1:4141");

    match prev_home {
        Some(v) => std::env::set_var("NEXUS_HOME", v),
        None => std::env::remove_var("NEXUS_HOME"),
    }
    if let Some(v) = prev_url {
        std::env::set_var("NEXUS_DB_URL", v);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn malformed_host_global_nexus_toml_fails_loud() {
    let _guard = env_lock();
    let dir = temp_config_dir("bad-global-toml");
    std::fs::write(
        dir.join("nexus.toml"),
        "db_url = \"http://127.0.0.1:4141\"\ndrain_limit = \"oops\"\n",
    )
    .unwrap();
    let prev_home = std::env::var("NEXUS_HOME").ok();
    let prev_url = std::env::var("NEXUS_DB_URL").ok();
    let prev_drain = std::env::var("NEXUS_DRAIN_LIMIT").ok();
    std::env::set_var("NEXUS_HOME", &dir);
    std::env::remove_var("NEXUS_DB_URL");
    std::env::remove_var("NEXUS_DRAIN_LIMIT");

    let err = Config::try_load().expect_err("bad drain_limit must not default");
    let message = err.to_string();
    assert!(
        message.contains("drain_limit") || message.contains("invalid type"),
        "error should name the malformed config field/type: {message}"
    );

    match prev_home {
        Some(v) => std::env::set_var("NEXUS_HOME", v),
        None => std::env::remove_var("NEXUS_HOME"),
    }
    match prev_url {
        Some(v) => std::env::set_var("NEXUS_DB_URL", v),
        None => std::env::remove_var("NEXUS_DB_URL"),
    }
    match prev_drain {
        Some(v) => std::env::set_var("NEXUS_DRAIN_LIMIT", v),
        None => std::env::remove_var("NEXUS_DRAIN_LIMIT"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
