#![cfg(test)]

use std::{
    process::{Command, Stdio},
    time::Duration,
};

#[test]
fn unavailable_ipc_leaves_process_alive_unless_force_is_explicit() {
    let home = tempfile::tempdir().unwrap();
    let mut child = Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "Start-Sleep -Seconds 30",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = child.id();
    let normal = super::stop(home.path(), pid, false, Duration::from_millis(20));
    let survived = child.try_wait().unwrap().is_none();
    let forced = super::stop(home.path(), pid, true, Duration::from_millis(20));
    let exited_before_cleanup = child.try_wait().unwrap().is_some();
    // Cleanup before assertions even if the function regresses; this is a disposable native child.
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        normal.is_err(),
        "unavailable IPC must not invent an accepted stop"
    );
    assert!(survived, "ordinary stop must not silently force-terminate");
    assert!(
        forced.is_ok(),
        "explicit force must terminate the pinned process: {forced:?}"
    );
    assert!(
        exited_before_cleanup,
        "force must observe exit before fixture cleanup"
    );
}

#[test]
fn manager_failure_prevents_even_explicit_force_and_manager_exit_is_observed() {
    let home = tempfile::tempdir().unwrap();
    let mut child = Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "Start-Sleep -Seconds 30",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = child.id();
    let denied = super::stop_after(home.path(), pid, true, Duration::from_millis(20), || {
        Err(std::io::Error::other("manager refused"))
    });
    let survived = child.try_wait().unwrap().is_none();
    let stopped = super::stop_after(home.path(), pid, false, Duration::from_millis(20), || {
        child.kill()?;
        child.wait()?;
        Ok(())
    });
    let mut manager_called_after_exit = false;
    let already_stopped =
        super::stop_after(home.path(), pid, false, Duration::from_millis(20), || {
            manager_called_after_exit = true;
            Ok(())
        });
    let _ = child.kill();
    let _ = child.wait();
    assert!(denied.unwrap_err().to_string().contains("manager refused"));
    assert!(
        survived,
        "manager failure must prevent subsequent child effects"
    );
    assert!(
        stopped.is_ok(),
        "manager-killed pinned process needs no IPC: {stopped:?}"
    );
    assert!(already_stopped.is_ok());
    assert!(
        manager_called_after_exit,
        "an exited daemon does not prove its manager stopped"
    );
}
