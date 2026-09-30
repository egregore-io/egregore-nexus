use nexus::webconsole_service::{
    launchd_plist, systemd_unit, windows_registration, windows_runner, Spec,
};

fn spec() -> Spec {
    Spec {
        executable: "/opt/Nexus & Co/nexus-webui".into(),
        home: "/home/user/nexus space".into(),
        gateway_url: "http://127.0.0.1:4321".into(),
        path: "/opt/node/bin:/usr/bin".into(),
    }
}

#[test]
fn webconsole_systemd_is_background_user_service_with_gateway_dependency() {
    let text = systemd_unit(&spec());
    for expected in [
        "Requires=nexus-gateway.service",
        "After=nexus-gateway.service",
        "PartOf=nexus-gateway.service",
        "WantedBy=default.target",
        "Restart=on-failure",
        "--host",
        "127.0.0.1",
        "4200",
        "http://127.0.0.1:4321",
        "webconsole.json",
        "PATH=/opt/node/bin:/usr/bin",
        "\"/opt/Nexus & Co/nexus-webui\"",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    assert!(!text.contains("webconsole launch"));
    let mut hostile = spec();
    hostile.home = "/home/percent%user".into();
    assert!(systemd_unit(&hostile).contains("percent%%user"));
}

#[test]
fn webconsole_launchd_runs_at_login_without_a_browser_and_escapes_xml() {
    let text = launchd_plist(&spec());
    for expected in [
        "io.egregore.nexus.webconsole",
        "RunAtLoad",
        "KeepAlive",
        "/opt/Nexus &amp; Co/nexus-webui",
        "http://127.0.0.1:4321",
        "webconsole.json",
        "PATH",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    assert!(!text.contains("<string>open</string>"));
}

#[test]
fn webconsole_windows_runs_user_scoped_at_login_with_literal_paths() {
    let mut spec = spec();
    spec.executable = "C:\\Users\\O'Brien\\nexus-webui.cmd".into();
    let runner = windows_runner(&spec);
    assert!(runner.contains("O''Brien"));
    assert!(runner.contains("'--gateway-url' 'http://127.0.0.1:4321'"));
    assert!(runner.contains("--discovery"));
    let registration = windows_registration(&spec);
    for expected in [
        "-AtLogOn",
        "-RunLevel Limited",
        "-RestartCount",
        "EgregoreNexusWebconsole",
    ] {
        assert!(registration.contains(expected), "missing {expected}");
    }
    assert!(!registration.contains("Highest"));
}

#[test]
fn systemd_environment_and_log_paths_do_not_double_literal_dollars() {
    let mut spec = spec();
    spec.path = "/opt/$node/bin:/usr/bin".into();
    spec.home = "/home/$user".into();
    let unit = systemd_unit(&spec);
    assert!(unit.contains("Environment=\"PATH=/opt/$node/bin:/usr/bin\""));
    // The pure formatter is tested on every host; PathBuf::join uses that host's separator.
    let separator = if cfg!(windows) { r"\\" } else { "/" };
    assert!(unit.contains(&format!(
        "StandardOutput=\"append:/home/$user{separator}webconsole.log\""
    )));
    assert!(unit.contains(&format!("\"/home/$$user{separator}webconsole.json\"")));
}
