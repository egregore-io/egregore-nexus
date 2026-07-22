use clap::{CommandFactory, Parser};
use nexus::cli::{commands::discover::member_list_request, Cli, Command};
use nexus::gateway_lifecycle::{GatewayLifecycleError, GATEWAY_INSTALL_HINT};

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("nexus").chain(args.iter().copied())).unwrap()
}

#[test]
fn gateway_lifecycle_verbs_parse() {
    parse(&["gateway", "install"]);
    parse(&["gateway", "start"]);
    parse(&["gateway", "stop", "--force"]);
    parse(&["gateway", "restart", "--force"]);
    parse(&["gateway", "status"]);
    parse(&["gateway", "logs", "--follow", "--lines", "25"]);
    parse(&["gateway", "delivery-mode", "show"]);
    parse(&["gateway", "delivery-mode", "set", "buffered"]);
    parse(&["gateway", "delivery-mode", "set", "best-effort"]);
    parse(&["gateway", "hooks", "list"]);
    parse(&["--json", "gateway", "hooks", "list"]);
    parse(&["gateway", "hooks", "list", "--json"]);
    parse(&["gateway", "uninstall"]);
}

#[test]
fn gateway_read_client_forwards_the_standard_rest_bearer() {
    let source = include_str!("../src/cli/gateway_read_client.rs");
    assert!(source.contains("NEXUS_REST_TOKEN"));
    assert!(source.contains("AUTHORIZATION"));
}

#[test]
fn lifecycle_affordances_parse() {
    parse(&[
        "members",
        "--project",
        "v015-lab",
        "--include-offline",
        "--presence",
    ]);
    parse(&["webconsole", "launch", "--no-open"]);
    parse(&[
        "webconsole",
        "start",
        "--host",
        "127.0.0.1",
        "--port",
        "4200",
    ]);
    parse(&["webconsole", "stop", "--force"]);
    parse(&["webconsole", "restart"]);
    parse(&["webconsole", "status"]);
    parse(&["webconsole", "logs", "--follow", "--lines", "25"]);
    parse(&["webconsole", "url"]);
    parse(&["update"]);
    parse(&["update", "--check"]);
    parse(&["--json", "update", "--check"]);
}

#[test]
fn members_cli_maps_project_metadata_and_global_omission_into_the_read_request() {
    let filtered = parse(&["members", "--project", "v015-lab", "--include-offline"]);
    let global = parse(&["members"]);
    let Command::Members(filtered) = filtered.command else {
        panic!("expected members command");
    };
    let Command::Members(global) = global.command else {
        panic!("expected members command");
    };

    let filtered = member_list_request(&filtered);
    let global = member_list_request(&global);

    assert_eq!(filtered.project.as_deref(), Some("v015-lab"));
    assert_eq!(filtered.include_offline, Some(true));
    assert_eq!(global.project, None);
    assert_eq!(global.include_offline, Some(false));
}

#[test]
fn gateway_delivery_mode_rejects_unknown_values_at_the_cli_boundary() {
    assert!(
        Cli::try_parse_from(["nexus", "gateway", "delivery-mode", "set", "lossy-maybe"]).is_err()
    );
}

#[test]
fn public_cli_reports_only_the_release_version() {
    let command = Cli::command();
    assert_eq!(command.get_version(), Some(env!("CARGO_PKG_VERSION")));
    let long = command
        .get_long_version()
        .expect("release CLI must expose its release version");
    assert_eq!(long, env!("CARGO_PKG_VERSION"));
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

#[test]
fn only_explicit_follow_keeps_gateway_and_webconsole_logs_attached() {
    let gateway = include_str!("../src/gateway_lifecycle.rs");
    let webconsole = include_str!("../src/webconsole_lifecycle.rs");

    assert!(gateway.contains(
        "if follow {\n        follow_file_tail(&backend.paths.log, lines)?;\n    } else {\n        print_file_tail(&backend.paths.log, lines)?;"
    ));
    assert!(webconsole
        .contains("if follow {\n        follow_file_tail(&path, lines)\n            .map_err"));
    assert!(webconsole
        .contains("} else {\n        print_file_tail(&path, lines)\n            .map_err"));
}

#[test]
fn lifecycle_and_update_subprocesses_use_shared_bounded_or_detached_boundaries() {
    for source in [
        include_str!("../src/gateway_lifecycle.rs"),
        include_str!("../src/gateway_service.rs"),
        include_str!("../src/webconsole_lifecycle.rs"),
        include_str!("../src/update/system.rs"),
        include_str!("../src/update/lock.rs"),
    ] {
        assert!(!source.contains(".output()"));
    }

    assert!(
        include_str!("../src/gateway_lifecycle.rs").contains("lifecycle_process::spawn_detached")
    );
    assert!(include_str!("../src/webconsole_lifecycle.rs")
        .contains("lifecycle_process::spawn_detached"));
    assert!(include_str!("../src/update/system.rs").contains("lifecycle_process::run_bounded"));
}
