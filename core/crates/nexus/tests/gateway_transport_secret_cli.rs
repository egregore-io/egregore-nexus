use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// Fixture startup only, not a Gateway product-latency requirement.
const STARTUP_WATCHDOG: Duration = Duration::from_secs(60);
const CAPTURE_LIMIT: usize = 64 * 1024;

struct CapturedPipe {
    tail: Arc<Mutex<Vec<u8>>>,
    reader: Option<std::thread::JoinHandle<()>>,
}

impl CapturedPipe {
    fn start(mut pipe: impl Read + Send + 'static) -> Self {
        let tail = Arc::new(Mutex::new(Vec::<u8>::new()));
        let captured = tail.clone();
        let reader = std::thread::spawn(move || {
            let mut chunk = [0_u8; 1024];
            loop {
                match pipe.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(count) => {
                        let mut tail = captured.lock().unwrap();
                        tail.extend_from_slice(&chunk[..count]);
                        let excess = tail.len().saturating_sub(CAPTURE_LIMIT);
                        tail.drain(..excess);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
        });
        Self {
            tail,
            reader: Some(reader),
        }
    }

    fn ready_marker(&self) -> bool {
        let tail = self.tail.lock().unwrap();
        tail.windows(6)
            .enumerate()
            .any(|(index, bytes)| bytes == b"ready\n" && (index == 0 || tail[index - 1] == b'\n'))
    }

    fn finish(&mut self) {
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

struct FixtureChild {
    child: Child,
    started: Instant,
    stdout: CapturedPipe,
    stderr: CapturedPipe,
}

impl FixtureChild {
    fn spawn(command: &mut Command) -> Self {
        let started = Instant::now();
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn Gateway fixture process");
        let stdout = CapturedPipe::start(child.stdout.take().unwrap());
        let stderr = CapturedPipe::start(child.stderr.take().unwrap());
        Self {
            child,
            started,
            stdout,
            stderr,
        }
    }

    fn stop(&mut self) {
        // Own only the captured child. Never trust a descriptor PID as kill authority.
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stdout.finish();
        self.stderr.finish();
    }
}

impl Drop for FixtureChild {
    fn drop(&mut self) {
        self.stop();
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("repo root")
        .to_path_buf()
}

fn spawn_headless(home: &Path) -> FixtureChild {
    let root = repo_root();
    FixtureChild::spawn(
        Command::new("node")
            // tsx's CLI spawns a second Node process. Load TS in the captured child
            // itself so RAII cleanup owns the actual server on Windows and Unix.
            .args(["--import", "tsx"])
            .arg(root.join("gateway/test-fixtures/transport-secret-server.ts"))
            .arg(home)
            .current_dir(root.join("gateway"))
            .env("NEXUS_HOME", home)
            .env("HOME", home),
    )
}

fn wait_ready(home: &Path, server: &mut FixtureChild, budget: Duration) -> Result<(), String> {
    let deadline = Instant::now() + budget;
    let result = loop {
        match server.child.try_wait() {
            Ok(Some(status)) => break Err(format!("fixture exited early: {status}")),
            Err(error) => break Err(format!("fixture status unavailable: {error}")),
            Ok(None) => {}
        }
        let descriptor = std::fs::read(home.join("gateway.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok());
        let pid = descriptor.as_ref().and_then(|value| value["pid"].as_u64());
        if let Some(pid) = pid {
            if pid != u64::from(server.child.id()) {
                break Err(format!(
                    "fixture descriptor PID {pid} does not match owned child {}",
                    server.child.id()
                ));
            }
        }
        let has_token = std::fs::read_to_string(home.join("transport-test-token"))
            .is_ok_and(|token| !token.trim().is_empty());
        let marker = server.stdout.ready_marker();
        if pid.is_some() && has_token && marker {
            return Ok(());
        }
        if Instant::now() >= deadline {
            break Err(format!("fixture startup watchdog expired; descriptor_pid_present={}; nonempty_token={has_token}; ready_marker={marker}", pid.is_some()));
        }
        // Poll actual readiness; this delay is not a synchronization assumption.
        std::thread::sleep(Duration::from_millis(25));
    };
    server.stop();
    result.map_err(|reason| format!(
        "{reason}; elapsed={:?}; startup_budget={budget:?}; owned_pid={}; reaped_status={:?}; startup_stderr={}",
        server.started.elapsed(), server.child.id(), server.child.try_wait(),
        String::from_utf8_lossy(&server.stderr.tail.lock().unwrap()),
    ))
}

fn spawn_probe(home: &Path, script: &str) -> FixtureChild {
    FixtureChild::spawn(
        Command::new("node")
            .args(["-e", script])
            .arg(home)
            .env("HOME", home)
            .env("NEXUS_HOME", home),
    )
}

#[test]
fn fixture_early_exit_reports_bounded_stderr_and_reaps_owned_child() {
    let home = tempfile::tempdir().unwrap();
    let mut server = spawn_probe(home.path(),
        "process.stderr.write('x'.repeat(100000)+'startup-probe-early-exit\\n',()=>process.exit(23))");
    let error = wait_ready(home.path(), &mut server, Duration::from_secs(10)).unwrap_err();
    assert!(error.contains("exited early"), "{error}");
    assert!(error.contains("startup-probe-early-exit"), "{error}");
    assert!(error.contains("elapsed="), "{error}");
    assert_eq!(server.child.try_wait().unwrap().unwrap().code(), Some(23));
    assert!(server.stderr.tail.lock().unwrap().len() <= CAPTURE_LIMIT);
    assert!(server.stderr.reader.is_none());
}

#[test]
fn fixture_missing_ready_reports_watchdog_and_reaps_owned_child() {
    let home = tempfile::tempdir().unwrap();
    let mut server = spawn_probe(home.path(),
        "process.stderr.write('startup-probe-missing-ready\\n');process.stdout.write('ready\\n');setInterval(()=>{},1000)");
    // Establish that Node is running before testing the deliberately short
    // readiness budget. A marker alone must not replace descriptor/token checks.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !server.stdout.ready_marker() {
        assert!(server.child.try_wait().unwrap().is_none(), "probe exited");
        assert!(Instant::now() < deadline, "probe did not start");
        std::thread::sleep(Duration::from_millis(5));
    }
    let error = wait_ready(home.path(), &mut server, Duration::from_millis(25)).unwrap_err();
    assert!(error.contains("watchdog expired"), "{error}");
    assert!(error.contains("descriptor_pid_present=false"), "{error}");
    assert!(error.contains("nonempty_token=false"), "{error}");
    assert!(error.contains("startup-probe-missing-ready"), "{error}");
    assert!(
        server.child.try_wait().unwrap().is_some(),
        "owned child was not reaped"
    );
    assert!(server.stdout.reader.is_none() && server.stderr.reader.is_none());
}

#[test]
fn fixture_rejects_descriptor_pid_that_is_not_the_owned_child() {
    let home = tempfile::tempdir().unwrap();
    let mut server = spawn_probe(
        home.path(),
        r#"
const fs=require('node:fs'),path=require('node:path');
fs.writeFileSync(path.join(process.argv[1],'gateway.json'),JSON.stringify({pid:process.pid+1}));
fs.writeFileSync(path.join(process.argv[1],'transport-test-token'),'fixture-token');
process.stdout.write('ready\n');setInterval(()=>{},1000);
"#,
    );
    let error = wait_ready(home.path(), &mut server, Duration::from_secs(10)).unwrap_err();
    assert!(error.contains("does not match owned child"), "{error}");
    assert!(
        server.child.try_wait().unwrap().is_some(),
        "owned child was not reaped"
    );
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
    wait_ready(home.path(), &mut server, STARTUP_WATCHDOG)
        .unwrap_or_else(|error| panic!("{error}"));
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

    server.stop();
}
