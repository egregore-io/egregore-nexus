//! The bounded rollout scan behind Codex lineage: every visited entry spends budget, only
//! regular files are opened, and a FIFO named like a rollout is never waited on.

use std::path::Path;

use nexus_harness_codex::app_server::{thread_spawn_meta_with_budget, RolloutScanBudget};
use serde_json::json;

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-lineage-scan-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create test tempdir");
    dir
}

fn write_rollout(dir: &Path, name: &str, thread: &str, parent: Option<&str>) {
    std::fs::create_dir_all(dir).unwrap();
    let source = match parent {
        Some(parent) => {
            json!({ "subagent": { "thread_spawn": { "parent_thread_id": parent, "depth": 1 } } })
        }
        None => json!("cli"),
    };
    let meta = json!({
        "timestamp": "2026-09-18T00:00:00.000Z",
        "type": "session_meta",
        "payload": { "id": thread, "originator": "nexus-harness", "source": source }
    });
    std::fs::write(
        dir.join(format!("rollout-{name}.jsonl")),
        format!("{meta}\n"),
    )
    .unwrap();
}

#[test]
fn the_file_budget_ends_the_scan_before_the_target_is_reached() {
    let root = tempdir("files");
    // Twenty non-matching rollouts at the root are visited before any subdirectory.
    for n in 0..20 {
        write_rollout(&root, &format!("other-{n:02}"), &format!("other-{n}"), None);
    }
    write_subagent_target(&root.join("deeper"));
    let tight = RolloutScanBudget {
        entries: 4096,
        files: 10,
        queued_dirs: 256,
    };
    assert_eq!(thread_spawn_meta_with_budget(&root, "target", &tight), None);
    let roomy = RolloutScanBudget {
        files: 100,
        ..tight
    };
    let meta = thread_spawn_meta_with_budget(&root, "target", &roomy).expect("found within budget");
    assert_eq!(meta.parent_thread_id, "main");
}

#[test]
fn the_entry_budget_counts_directories_and_non_rollout_files() {
    let root = tempdir("entries");
    // Fifty empty directories and fifty plain files at the root: none is a rollout, every one
    // costs an entry.
    for n in 0..50 {
        std::fs::create_dir_all(root.join(format!("dir-{n:02}"))).unwrap();
        std::fs::write(root.join(format!("note-{n:02}.txt")), "not a rollout").unwrap();
    }
    write_subagent_target(&root.join("zz-target"));
    let tight = RolloutScanBudget {
        entries: 40,
        files: 512,
        queued_dirs: 256,
    };
    assert_eq!(thread_spawn_meta_with_budget(&root, "target", &tight), None);
    let roomy = RolloutScanBudget {
        entries: 1000,
        ..tight
    };
    assert!(thread_spawn_meta_with_budget(&root, "target", &roomy).is_some());
}

/// Run one `#[ignore]`d test of this binary in a child process under a hard deadline. `None`
/// means it did not finish in time and was killed.
fn run_inner_with_watchdog(
    name: &str,
    timeout: std::time::Duration,
) -> Option<std::process::ExitStatus> {
    let exe = std::env::current_exe().expect("test binary path");
    let mut child = std::process::Command::new(exe)
        .args([name, "--exact", "--ignored", "--nocapture"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn inner test");
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll inner test") {
            return Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// The FIFO sits at the root and the target below a subdirectory: every root entry, the FIFO
/// included, is visited before the scan descends, so a scan that opened the FIFO would block
/// here forever. Run only under the watchdog.
#[cfg(unix)]
#[test]
#[ignore]
fn fifo_scan_inner() {
    let root = tempdir("fifo-inner");
    let fifo = root.join("rollout-2026-09-18T00-00-00-fifo.jsonl");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo runs");
    assert!(status.success(), "mkfifo is required for this pin");
    write_subagent_target(&root.join("deeper"));
    let meta = thread_spawn_meta_with_budget(&root, "target", &RolloutScanBudget::default())
        .expect("regular rollout found below the FIFO's directory");
    assert_eq!(meta.parent_thread_id, "main");
}

#[cfg(unix)]
#[test]
fn a_fifo_named_like_a_rollout_is_skipped_and_never_opened() {
    let outcome = run_inner_with_watchdog("fifo_scan_inner", std::time::Duration::from_secs(10));
    match outcome {
        Some(status) => assert!(status.success(), "inner FIFO scan failed: {status:?}"),
        None => panic!("the scan blocked on the FIFO and had to be killed"),
    }
}

#[test]
fn a_meta_line_beyond_the_cap_is_not_meta() {
    let root = tempdir("longline");
    let huge = format!(
        "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"target\",\"pad\":\"{}\",\"source\":{{\"subagent\":{{\"thread_spawn\":{{\"parent_thread_id\":\"main\",\"depth\":1}}}}}}}}}}\n",
        "x".repeat(70 * 1024)
    );
    std::fs::write(root.join("rollout-2026-09-18T00-00-00-long.jsonl"), huge).unwrap();
    assert_eq!(
        thread_spawn_meta_with_budget(&root, "target", &RolloutScanBudget::default()),
        None
    );
}

fn write_subagent_target(dir: &Path) {
    write_rollout(dir, "2026-09-18T00-00-00-target", "target", Some("main"));
}
