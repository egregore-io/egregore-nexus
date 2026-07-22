use std::fs;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use super::*;

#[cfg(unix)]
fn shell(script: &str) -> Command {
    let mut command = Command::new("sh");
    command.arg("-c").arg(script);
    command
}

#[cfg(unix)]
#[test]
fn bounded_command_returns_capped_stdout_and_stderr() {
    let mut command = shell("printf 'abcdef'; printf 'uvwxyz' >&2");

    let output = run_bounded(
        &mut command,
        "bounded output contract",
        Duration::from_secs(2),
        4,
    )
    .unwrap();

    assert!(output.status.success());
    assert_eq!(output.stdout, b"abcd");
    assert_eq!(output.stderr, b"uvwx");
}

#[cfg(unix)]
#[test]
fn bounded_command_reports_nonzero_exit_with_capped_evidence() {
    let mut command = shell("printf 'public-error' >&2; exit 17");

    let error =
        run_bounded(&mut command, "failing contract", Duration::from_secs(2), 6).unwrap_err();

    match error {
        BoundedProcessError::Exit {
            operation,
            status,
            stderr,
            ..
        } => {
            assert_eq!(operation, "failing contract");
            assert_eq!(status.code(), Some(17));
            assert_eq!(stderr, b"public");
        }
        other => panic!("expected exit error, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn bounded_command_times_out_without_holding_the_caller() {
    let mut command = shell("sleep 60");
    let started = Instant::now();

    let error = run_bounded(
        &mut command,
        "direct timeout contract",
        Duration::from_millis(150),
        1024,
    )
    .unwrap_err();

    assert!(matches!(error, BoundedProcessError::Timeout { .. }));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "bounded runner exceeded cleanup budget: {:?}",
        started.elapsed()
    );
}

#[cfg(unix)]
#[test]
fn descendant_retaining_output_pipe_is_killed_and_cannot_wedge_cli() {
    let temp = tempfile::tempdir().unwrap();
    let pid_path = temp.path().join("descendant.pid");
    let script = format!(
        "sleep 60 & child=$!; printf '%s\\n' \"$child\" > '{}'; exit 0",
        pid_path.display()
    );
    let mut command = shell(&script);
    let started = Instant::now();

    let error = run_bounded(
        &mut command,
        "descendant pipe contract",
        Duration::from_millis(250),
        1024,
    )
    .unwrap_err();

    assert!(matches!(error, BoundedProcessError::Timeout { .. }));
    assert!(started.elapsed() < Duration::from_secs(3));
    let pid: u32 = fs::read_to_string(&pid_path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        wait_until_process_dead(pid, Duration::from_secs(2)),
        "descendant pid {pid} survived bounded cleanup"
    );
}

#[cfg(unix)]
#[test]
fn detached_child_survives_cli_return_with_closed_stdin() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("detached.marker");
    let script = format!(
        "if read ignored; then exit 91; fi; sleep 0.1; printf detached > '{}'",
        marker.display()
    );
    let mut command = shell(&script);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = spawn_detached(&mut command).unwrap();
    let pid = child.id();
    drop(child);

    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && !marker.exists() {
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(fs::read_to_string(marker).unwrap(), "detached");
    assert!(pid > 0);
}

#[cfg(unix)]
fn wait_until_process_dead(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !process_is_live(pid) {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    !process_is_live(pid)
}

#[cfg(unix)]
fn process_is_live(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        if let Some((_, tail)) = stat.rsplit_once(") ") {
            return !matches!(tail.chars().next(), Some('Z' | 'X'));
        }
    }
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
#[test]
fn windows_bounded_command_uses_a_killable_tree() {
    let mut command = Command::new("cmd.exe");
    command.args(["/D", "/S", "/C", "ping -n 60 127.0.0.1 >NUL"]);

    let error = run_bounded(
        &mut command,
        "windows tree contract",
        Duration::from_millis(200),
        1024,
    )
    .unwrap_err();

    assert!(matches!(error, BoundedProcessError::Timeout { .. }));
}
