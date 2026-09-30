use clap::{CommandFactory, Parser};
use nexus::cli::Cli;
use nexus::gateway_lifecycle::{GatewayLifecycleError, GATEWAY_INSTALL_HINT};

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("nexus").chain(args.iter().copied())).unwrap()
}

#[test]
fn gateway_lifecycle_verbs_parse() {
    parse(&["gateway", "start"]);
    parse(&["gateway", "stop", "--force"]);
    parse(&["gateway", "restart", "--force"]);
    parse(&["gateway", "status"]);
    parse(&["gateway", "logs", "--follow", "--lines", "25"]);
    parse(&["gateway", "delivery-mode", "show"]);
    parse(&["gateway", "delivery-mode", "set", "buffered"]);
    parse(&["gateway", "delivery-mode", "set", "best-effort"]);
}

#[test]
fn gateway_delivery_mode_rejects_unknown_values_at_the_cli_boundary() {
    assert!(
        Cli::try_parse_from(["nexus", "gateway", "delivery-mode", "set", "lossy-maybe"]).is_err()
    );
}

#[test]
fn public_cli_reports_the_release_version() {
    let command = Cli::command();
    assert_eq!(command.get_version(), Some(env!("CARGO_PKG_VERSION")));
    let long = command
        .get_long_version()
        .expect("release CLI must expose source revision in its long version");
    assert!(long.starts_with(env!("CARGO_PKG_VERSION")));
    assert!(
        long.contains("revision "),
        "unexpected long version: {long}"
    );
    assert!(
        !long.contains("revision unknown"),
        "unexpected long version: {long}"
    );
}

#[test]
fn gateway_json_autostart_keeps_daemon_announcement_off_stdout() {
    let lifecycle = include_str!("../src/daemon/lifecycle.rs");

    assert!(
        lifecycle.contains("start_with_announcement(args, true)"),
        "operator-invoked daemon start should retain its human announcement"
    );
    assert!(
        lifecycle.contains("start_with_announcement(\n        StartArgs")
            && lifecycle.contains("false,\n    )"),
        "gateway dependency autostart must use the silent start path so --json stays parseable"
    );
}

#[test]
fn missing_gateway_error_has_machine_hint() {
    let error = GatewayLifecycleError::not_installed();
    assert_eq!(error.code(), "GATEWAY_NOT_INSTALLED");
    assert_eq!(error.hint(), Some(GATEWAY_INSTALL_HINT));
    assert_eq!(error.exit_code(), 3);
}

#[test]
fn missing_gateway_error_has_human_hint() {
    let error = GatewayLifecycleError::not_installed();
    assert_eq!(error.message(), "Nexus Gateway is not installed");
    let value = serde_json::to_value(error.envelope()).unwrap();
    assert_eq!(value["error"]["code"], "GATEWAY_NOT_INSTALLED");
    assert!(value["error"]["hint"]
        .as_str()
        .unwrap()
        .contains("@egregore/nexus-gateway"));
}
