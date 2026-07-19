//! Registry conformance golden: snapshots the observable surface of every
//! harness contract reachable through `harness_registry()`.
//!
//! This fixture is the "no behavior change" oracle for the harness identity
//! de-hardcoding refactor: every phase must leave this snapshot byte-identical
//! (Phase 4 re-keys the header token only). Regenerate intentionally with:
//!
//!   UPDATE_HARNESS_GOLDEN=1 cargo test -p nexus --test harness_registry_golden
//!
//! and review the diff like a contract change.

use std::fmt::Write as _;

use nexus::harness_registry::harness_registry_by_id;
use nexus_contracts::HarnessId;
use nexus_harness_core::{HarnessIdentity, SlashCommand};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/harness_registry_golden.txt"
);

const KINDS: [&str; 6] = ["claude", "codex", "opencode", "hermes", "pi", "other"];

const TAILS: [&[&str]; 3] = [
    &[],
    &["--model", "opus"],
    // Codex compatibility spelling `resume <thread>` — the one tail today that
    // resolves to sidecar resume metadata instead of verbatim passthrough.
    &["resume", "t-999"],
];

const SLASH_INPUTS: [&str; 4] = ["/compact", "/compress", "/compact now", "/clear"];

/// The cross-platform golden records the stable harness contract. Native executable spelling is
/// covered separately by `nexus-harness-core/tests/native_programs.rs`.
fn stable_program(program: &str) -> &str {
    program.strip_suffix(".exe").unwrap_or(program)
}

fn snapshot() -> String {
    let mut out = String::new();
    for (index, token) in KINDS.into_iter().enumerate() {
        let kind = HarnessId::new(token).expect("golden kinds are valid harness ids");
        let contract = harness_registry_by_id(&kind);

        writeln!(out, "[{token}]").unwrap();
        writeln!(out, "program = {:?}", stable_program(contract.program())).unwrap();
        writeln!(out, "agent_token = {:?}", contract.agent_token()).unwrap();
        writeln!(
            out,
            "headed_runtime_kind = {:?}",
            contract.headed_runtime_kind()
        )
        .unwrap();
        writeln!(
            out,
            "uses_structured_output = {}",
            contract.uses_structured_output()
        )
        .unwrap();
        writeln!(out, "attach_revivable = {}", contract.attach_revivable()).unwrap();

        for tail in TAILS {
            let tail: Vec<String> = tail.iter().map(|s| s.to_string()).collect();
            match contract.resolve_tail(&tail) {
                Ok(resolved) => writeln!(
                    out,
                    "resolve_tail {:?} = resume={:?} argv={:?} requires_tui={}",
                    tail, resolved.resume, resolved.argv, resolved.requires_tui
                )
                .unwrap(),
                Err(err) => writeln!(out, "resolve_tail {tail:?} = err({err})").unwrap(),
            }
        }

        match contract.revive_tail() {
            Ok(resolved) => writeln!(
                out,
                "revive_tail = resume={:?} argv={:?} requires_tui={}",
                resolved.resume, resolved.argv, resolved.requires_tui
            )
            .unwrap(),
            Err(err) => writeln!(out, "revive_tail = err({err})").unwrap(),
        }

        let identity = HarnessIdentity {
            name: "golden",
            project: "proj",
            client_key: "ck-1",
            agent: contract.agent_token(),
        };
        let tail: Vec<String> = vec!["--flag".to_string(), "value".to_string()];
        match contract.headed_cli_command(&identity, "/usr/bin/nexus", &tail) {
            Ok(cmd) => writeln!(
                out,
                "headed_cli_command = program={:?} args={:?}",
                stable_program(&cmd.program),
                cmd.args
            )
            .unwrap(),
            Err(err) => writeln!(out, "headed_cli_command = err({err})").unwrap(),
        }
        match contract.headed_pty_command(&identity, "/usr/bin/nexus", &tail) {
            Ok(cmd) => writeln!(
                out,
                "headed_pty_command = program={:?} args={:?}",
                stable_program(&cmd.program),
                cmd.args
            )
            .unwrap(),
            Err(err) => writeln!(out, "headed_pty_command = err({err})").unwrap(),
        }

        for input in SLASH_INPUTS {
            let command = SlashCommand::parse(input).expect("slash inputs parse");
            match contract.translate_slash_command(&command) {
                Ok(action) => writeln!(out, "slash {input:?} = {action:?}").unwrap(),
                Err(err) => writeln!(out, "slash {input:?} = err({err})").unwrap(),
            }
        }
        if index + 1 < KINDS.len() {
            writeln!(out).unwrap();
        }
    }
    out
}

#[test]
fn harness_registry_matches_golden() {
    let actual = snapshot();
    if std::env::var_os("UPDATE_HARNESS_GOLDEN").is_some() {
        std::fs::create_dir_all(std::path::Path::new(FIXTURE).parent().unwrap()).unwrap();
        std::fs::write(FIXTURE, &actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(FIXTURE).unwrap_or_else(|err| {
        panic!(
            "missing golden fixture {FIXTURE}: {err}\n\
             seed it with: UPDATE_HARNESS_GOLDEN=1 cargo test -p nexus --test harness_registry_golden"
        )
    });
    assert_eq!(
        actual, expected,
        "harness registry observable surface changed; if intentional, regenerate with \
         UPDATE_HARNESS_GOLDEN=1 and review the diff as a contract change"
    );
}
