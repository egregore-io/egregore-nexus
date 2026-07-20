use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("repo root")
        .to_path_buf()
}

fn spawn_headless(home: &Path) -> Child {
    let root = repo_root();
    Command::new(root.join("gateway/node_modules/.bin/tsx"))
        .arg(root.join("gateway/test-fixtures/transport-secret-server.ts"))
        .arg(home)
        .current_dir(root.join("gateway"))
        .env("NEXUS_HOME", home)
        .env("HOME", home)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn live headless Gateway fixture")
}

fn wait_ready(home: &Path, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if home.join("gateway.json").is_file() && home.join("transport-test-token").is_file() {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll headless fixture") {
            panic!("headless Gateway fixture exited early: {status}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for live headless Gateway fixture");
}

fn run_cli(home: &Path, token: Option<&str>, args: &[&str], stdin: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nexus"));
    command
        .args(args)
        .env("NEXUS_HOME", home)
        .env("HOME", home)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(token) = token {
        command.env("NEXUS_REST_TOKEN", token);
    } else {
        command.env_remove("NEXUS_REST_TOKEN");
    }
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
    }
    let mut child = command.spawn().expect("spawn Nexus CLI");
    if let Some(value) = stdin {
        child
            .stdin
            .take()
            .expect("CLI stdin")
            .write_all(value.as_bytes())
            .expect("write CLI stdin");
    }
    child.wait_with_output().expect("collect Nexus CLI")
}

#[test]
fn shipped_cli_mutates_live_headless_gateway_secrets_via_stdin_only() {
    let home = tempfile::tempdir().expect("temp Nexus home");
    let mut server = spawn_headless(home.path());
    wait_ready(home.path(), &mut server);
    let token = std::fs::read_to_string(home.path().join("transport-test-token"))
        .expect("fixture bearer token");
    let marker = "token-marker-never-echo";

    let unauthorized = run_cli(
        home.path(),
        None,
        &["gateway", "transport", "secret", "set", "fake.token"],
        Some(marker),
    );
    assert!(!unauthorized.status.success());
    assert!(!String::from_utf8_lossy(&unauthorized.stdout).contains(marker));
    assert!(!String::from_utf8_lossy(&unauthorized.stderr).contains(marker));

    for operation in ["set", "rotate"] {
        let output = run_cli(
            home.path(),
            Some(&token),
            &["gateway", "transport", "secret", operation, "fake.token"],
            Some(marker),
        );
        assert!(
            output.status.success(),
            "{operation}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains(marker));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(marker));
    }

    let removed = run_cli(
        home.path(),
        Some(&token),
        &["gateway", "transport", "secret", "rm", "fake.token"],
        None,
    );
    assert!(
        removed.status.success(),
        "{}",
        String::from_utf8_lossy(&removed.stderr)
    );

    let argv_refused = run_cli(
        home.path(),
        Some(&token),
        &[
            "gateway",
            "transport",
            "secret",
            "set",
            "fake.token",
            marker,
        ],
        None,
    );
    assert!(!argv_refused.status.success());
    assert!(!String::from_utf8_lossy(&argv_refused.stdout).contains(marker));
    assert!(!String::from_utf8_lossy(&argv_refused.stderr).contains(marker));

    server.kill().ok();
    let _ = server.wait();
}
