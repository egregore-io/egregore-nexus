//! Smoke test: run the typeshare CLI over this crate and assert the generated TS exists and
//! contains the key types. Skips (passes) if the `typeshare` binary is not installed, so the
//! crate's `cargo test` does not hard-depend on the external tool; CI installs it explicitly.

use std::path::Path;
use std::process::Command;

#[test]
fn typeshare_generates_contracts_with_key_types() {
    // Skip gracefully if the CLI isn't present.
    if Command::new("typeshare").arg("--version").output().is_err() {
        eprintln!("typeshare CLI not installed; skipping generation smoke test");
        return;
    }

    let out_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../gateway/src/shared/types/contracts.gen.ts");
    std::fs::create_dir_all(out_path.parent().unwrap()).unwrap();

    // Exercise the canonical generator instead of duplicating it here. The script appends the
    // hand-authored wire unions and normalizes generator-owned whitespace, so a passing contract
    // smoke cannot leave the tracked output dirty with a different byte shape.
    let generator = Path::new(env!("CARGO_MANIFEST_DIR")).join("gen/gen-ts.sh");
    let status = Command::new("bash")
        .arg(&generator)
        .status()
        .expect("failed to run canonical contract generator");
    assert!(
        status.success(),
        "canonical contract generator exited non-zero"
    );

    let ts = std::fs::read_to_string(&out_path).expect("contracts.gen.ts not written");
    for ty in [
        "RegisterRequest",
        "SendRequest",
        "SendTarget",
        "NexusBatch",
        "WsEvent",
        "Message",
        "Provenance",
        "SearchRequest",
        "NotifyRequest",
        "NotifySendRequest",
        "SpawnRequest",
        "Request",
        "Response",
        "Notification",
        "RpcError",
        "RequestId",
        "AgentId = string",
        "CredentialId = string",
    ] {
        assert!(ts.contains(ty), "generated TS missing type {ty}");
    }
    assert!(
        ts.contains("export type NotifyTargetWire ="),
        "generated TS missing the explicit notification target union"
    );
    assert!(
        ts.contains("export type DaemonIpcCallWire ="),
        "generated TS missing the daemon IPC operation union"
    );
    // camelCase wire fields landed in TS:
    assert!(
        ts.contains("sessionId"),
        "expected camelCase sessionId in TS"
    );
    assert!(
        ts.contains("messageId"),
        "expected camelCase messageId in TS"
    );
}
