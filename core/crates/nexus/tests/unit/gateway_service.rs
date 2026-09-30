use std::path::PathBuf;

use async_trait::async_trait;
use nexus::gateway_service::{
    install_gateway_service_with, launchd_plist, systemd_unit, uninstall_gateway_service_with,
    windows_registration_script, windows_runner_script, GatewayServiceBackend, GatewayServiceSpec,
};

#[test]
fn launchd_restart_after_rewrite_or_explicit_unload_bootstraps_without_second_stop() {
    for previously_loaded in [false, true] {
        let mut calls = Vec::new();
        nexus::gateway_service::launchd_start_with(
            "gui/501",
            "io.egregore.nexus.gateway",
            "/tmp/gateway.plist",
            true,
            |args| {
                calls.push(args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>());
                if args[0] == "print" && !previously_loaded {
                    Err((Some(113), "service absent".into()))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
        let expected = if previously_loaded {
            vec!["kickstart", "-k", "gui/501/io.egregore.nexus.gateway"]
        } else {
            vec!["bootstrap", "gui/501", "/tmp/gateway.plist"]
        };
        assert_eq!(calls[1], expected);
        assert_eq!(calls.len(), 2);
    }
}

#[test]
fn launchd_query_and_action_errors_are_not_classified_as_absence() {
    for code in [None, Some(1), Some(3), Some(5), Some(125)] {
        let mut calls = 0;
        let result = nexus::gateway_service::launchd_start_with(
            "gui/501",
            "label",
            "/tmp/service.plist",
            false,
            |_| {
                calls += 1;
                Err((code, "manager denied".into()))
            },
        );
        assert_eq!(result.unwrap_err(), "manager denied");
        assert_eq!(calls, 1);
    }
    let result = nexus::gateway_service::launchd_start_with(
        "gui/501",
        "label",
        "/tmp/service.plist",
        true,
        |args| {
            if args[0] == "print" {
                Err((Some(113), "absent".into()))
            } else {
                Err((Some(5), "bootstrap denied".into()))
            }
        },
    );
    assert_eq!(result.unwrap_err(), "bootstrap denied");
}

#[test]
fn launchd_stop_is_idempotent_only_for_confirmed_missing_service() {
    for (code, expected_ok) in [(113, true), (1, false), (3, false), (125, false)] {
        let mut calls = 0;
        let result = nexus::gateway_service::launchd_stop_with("gui/501/label", |_| {
            calls += 1;
            Err((Some(code), "query failed".into()))
        });
        assert_eq!(result.is_ok(), expected_ok);
        assert_eq!(calls, 1);
    }
    let mut calls = Vec::new();
    let result = nexus::gateway_service::launchd_stop_with("gui/501/label", |args| {
        calls.push(args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>());
        if args[0] == "print" {
            Ok(())
        } else {
            Err((Some(1), "stop denied".into()))
        }
    });
    assert_eq!(result.unwrap_err(), "stop denied");
    assert_eq!(
        calls,
        [
            vec!["print", "gui/501/label"],
            vec!["bootout", "gui/501/label"]
        ]
    );
}

#[derive(Default)]
struct FakeBackend {
    calls: Vec<&'static str>,
    installed: bool,
}

#[async_trait(?Send)]
impl GatewayServiceBackend for FakeBackend {
    async fn ensure_daemon_service(&mut self) -> Result<(), String> {
        self.calls.push("ensure-daemon");
        Ok(())
    }

    fn resolve_gateway(&mut self) -> Result<GatewayServiceSpec, String> {
        self.calls.push("resolve-gateway");
        Ok(spec())
    }

    fn write_definition(&mut self, _spec: &GatewayServiceSpec) -> Result<(), String> {
        self.calls.push("write-definition");
        self.installed = true;
        Ok(())
    }

    fn enable(&mut self) -> Result<(), String> {
        self.calls.push("enable");
        Ok(())
    }

    fn start(&mut self) -> Result<(), String> {
        self.calls.push("start");
        Ok(())
    }

    async fn wait_healthy(&mut self) -> Result<(), String> {
        self.calls.push("health");
        Ok(())
    }

    fn stop(&mut self) -> Result<(), String> {
        self.calls.push("stop");
        Ok(())
    }

    fn disable(&mut self) -> Result<(), String> {
        self.calls.push("disable");
        Ok(())
    }

    fn remove_definition(&mut self) -> Result<(), String> {
        self.calls.push("remove-definition");
        self.installed = false;
        Ok(())
    }

    fn installed(&self) -> bool {
        self.installed
    }
}

#[tokio::test]
async fn install_ensures_daemon_before_starting_gateway() {
    let mut backend = FakeBackend::default();

    let report = install_gateway_service_with(&mut backend).await.unwrap();

    assert!(report.installed);
    assert!(report.running);
    assert_eq!(
        backend.calls,
        [
            "ensure-daemon",
            "resolve-gateway",
            "write-definition",
            "enable",
            "start",
            "health",
        ]
    );
}

#[test]
fn uninstall_is_gateway_only_and_idempotent() {
    let mut absent = FakeBackend::default();
    let first = uninstall_gateway_service_with(&mut absent).unwrap();
    assert!(!first.installed);
    assert!(absent.calls.is_empty());

    let mut installed = FakeBackend {
        installed: true,
        ..FakeBackend::default()
    };
    let removed = uninstall_gateway_service_with(&mut installed).unwrap();
    assert!(!removed.installed);
    assert_eq!(installed.calls, ["stop", "disable", "remove-definition"]);
}

#[test]
fn systemd_unit_is_dependency_ordered_and_shell_free() {
    let rendered = systemd_unit(&spec());

    assert!(rendered.contains("Requires=nexus-daemon.service"));
    assert!(rendered.contains("After=nexus-daemon.service"));
    assert!(rendered.contains("PartOf=nexus-daemon.service"));
    assert!(rendered.contains("Restart=on-failure"));
    assert!(rendered.contains("ExecStart=\"/opt/egregore gateway/nexus-gateway\""));
    assert!(!rendered.contains("sh -c"));
}

#[test]
fn launchd_uses_program_arguments_and_keepalive() {
    let rendered = launchd_plist(&spec());

    assert!(rendered.contains("<key>ProgramArguments</key>"));
    assert!(rendered.contains("<key>KeepAlive</key><true/>"));
    assert!(rendered.contains("/opt/egregore gateway/nexus-gateway"));
    assert!(!rendered.contains("/bin/sh"));
}

#[test]
fn windows_task_uses_native_action_fields() {
    let rendered = windows_registration_script(&GatewayServiceSpec {
        executable: PathBuf::from(r"C:\Program Files\Egregore\nexus-gateway.cmd"),
        ..spec()
    });

    assert!(rendered.contains("New-ScheduledTaskAction"));
    assert!(rendered.contains("-Execute 'powershell.exe'"));
    assert!(rendered.contains("nexus-gateway.cmd"));
    assert!(rendered.contains("EgregoreNexusGateway"));
}

#[test]
fn windows_runner_invokes_a_spaced_cmd_shim_without_a_cmd_exe_wrapper() {
    // PowerShell passes hand-written quotes through literally, so `cmd.exe /S /C "<path>"` had
    // its quotes stripped straight back off and a spaced installation path was split at the
    // space. PowerShell executes a `.cmd` shim directly, so call it directly.
    let rendered = windows_runner_script(&GatewayServiceSpec {
        executable: PathBuf::from(r"C:\Program Files\Egregore\nexus-gateway.cmd"),
        ..spec()
    });

    assert!(rendered.contains(r"& 'C:\Program Files\Egregore\nexus-gateway.cmd'"));
    assert!(!rendered.contains("cmd.exe"));
    assert!(!rendered.contains(r#"\""#));
    assert!(rendered.contains("$env:NEXUS_GATEWAY_DISCOVERY = 'write'"));
}

#[test]
fn native_service_manager_commands_use_the_shared_bounded_process_boundary() {
    let source = include_str!("../../src/gateway_service.rs");

    assert!(source.contains("lifecycle_process::run_bounded"));
    assert!(!source.contains(".output()"));
    assert!(!source.contains(".status()"));
}

fn spec() -> GatewayServiceSpec {
    GatewayServiceSpec {
        executable: PathBuf::from("/opt/egregore gateway/nexus-gateway"),
        home: PathBuf::from("/home/ada/.nexus"),
        log: PathBuf::from("/home/ada/.nexus/gateway.log"),
    }
}
