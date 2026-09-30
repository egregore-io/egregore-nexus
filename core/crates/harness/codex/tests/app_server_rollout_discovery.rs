//! Rollout discovery must bind MAIN threads only — codex 0.144 multi-agent v2 writes
//! sub-agent rollouts into the same `CODEX_HOME/sessions` tree, and those threads refuse
//! direct app-server input (`-32600`). Choosing a newer sub-agent rollout instead of the
//! main thread prevents bus delivery.
//!
//! Run: `cargo test -p nexus-harness-codex --test app_server_rollout_discovery`

use nexus_harness_codex::app_server::bridge::{newest_rollout, rollout_is_subagent};

fn write_rollout(dir: &std::path::Path, name: &str, meta: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, format!("{meta}\n")).expect("write rollout fixture");
    path
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nexus-rollout-discovery-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
        tag,
    ));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

/// Synthetic metadata preserving the codex 0.144.1 main/sub-agent wire shapes.
const MAIN_META: &str = r#"{"timestamp":"2000-01-01T00:00:00.000Z","type":"session_meta","payload":{"id":"00000000-0000-7000-8000-000000000001","source":"vscode","originator":"nexus-harness"}}"#;
const SUBAGENT_META: &str = r#"{"timestamp":"2000-01-01T00:01:00.000Z","type":"session_meta","payload":{"id":"00000000-0000-7000-8000-000000000002","originator":"nexus-harness","source":{"subagent":{"thread_spawn":{"parent_thread_id":"00000000-0000-7000-8000-000000000001","depth":1,"agent_path":"/root/x","agent_nickname":"Example","agent_role":null}}}}}"#;

#[test]
fn subagent_rollouts_are_classified_by_their_meta_source() {
    let dir = tempdir("classify");
    let main = write_rollout(&dir, "rollout-2000-01-01T00-00-00-main.jsonl", MAIN_META);
    let sub = write_rollout(&dir, "rollout-2000-01-01T00-01-00-sub.jsonl", SUBAGENT_META);
    assert!(!rollout_is_subagent(&main));
    assert!(rollout_is_subagent(&sub));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The regression: a NEWER sub-agent rollout must never win over the older main rollout.
#[test]
fn newest_rollout_skips_newer_subagent_rollouts() {
    let dir = tempdir("skip-sub");
    let main = write_rollout(&dir, "rollout-2000-01-01T00-00-00-main.jsonl", MAIN_META);
    let sub = write_rollout(&dir, "rollout-2000-01-01T00-01-00-sub.jsonl", SUBAGENT_META);
    // Make the sub-agent rollout unambiguously newer on the filesystem clock.
    let newer = std::time::SystemTime::now();
    let f = std::fs::File::options()
        .append(true)
        .open(&sub)
        .expect("open sub rollout");
    f.set_modified(newer).expect("bump sub mtime");
    drop(f);

    assert_eq!(
        newest_rollout(&dir).as_deref(),
        Some(main.as_path()),
        "discovery must bind the MAIN thread, not the newer sub-agent rollout"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A session home with ONLY sub-agent rollouts yields no binding at all (wait for the
/// main rollout) rather than a thread that rejects input.
#[test]
fn all_subagent_home_yields_no_candidate() {
    let dir = tempdir("only-sub");
    write_rollout(&dir, "rollout-2000-01-01T00-01-00-sub.jsonl", SUBAGENT_META);
    assert_eq!(newest_rollout(&dir), None);
    let _ = std::fs::remove_dir_all(&dir);
}
