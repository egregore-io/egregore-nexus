use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use super::*;

#[test]
fn updater_uses_the_shared_bounded_runner_without_reader_joins() {
    let source = include_str!("../../src/update/system.rs");

    assert!(source.contains("lifecycle_process::run_bounded"));
    assert!(source.contains("Duration::from_secs(15 * 60)"));
    assert!(!source.contains("stdout_reader.join()"));
    assert!(!source.contains("stderr_reader.join()"));
}

#[cfg(unix)]
#[test]
fn updater_command_cannot_wedge_on_descendant_held_output() {
    let temp = tempfile::tempdir().unwrap();
    let pid_path = temp.path().join("descendant.pid");
    let script = format!(
        "sleep 60 & child=$!; printf '%s\\n' \"$child\" > '{}'; exit 0",
        pid_path.display()
    );
    let plan = CommandPlan {
        program: PathBuf::from("sh"),
        args: vec!["-c".into(), script],
        cwd: temp.path().to_path_buf(),
        env_overrides: BTreeMap::new(),
    };
    let started = Instant::now();

    let error = run_command(&plan, Duration::from_millis(250)).unwrap_err();

    assert!(error.contains("timed out"));
    assert!(started.elapsed() < Duration::from_secs(3));
    let pid: u32 = fs::read_to_string(pid_path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(wait_until_process_dead(pid, Duration::from_secs(2)));
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
