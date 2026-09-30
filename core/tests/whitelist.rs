//! Whitelist / tier-guard acceptance (spec §8, §11): every admin method is blocked for an
//! agent-tier caller (`-32001` UNAUTHORIZED) and allowed for an admin; and admin is **extra commands
//! only** — no admin method injects into the message path (the §8 guardrail: admin never reorders or
//! inserts into routing/realtime).

mod common;

use common::*;

use nexus_contracts::{codes, Harness, SpawnResponse};

const PROJECT: &str = "proj";

/// Every admin method rejects an agent-tier caller with UNAUTHORIZED.
#[tokio::test]
async fn admin_methods_blocked_for_agent_tier() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("agent", "ck_agent", PROJECT))
        .await
        .ok();

    // admin.spawn
    let r = d
        .rpc_as(
            "agent",
            PROJECT,
            "admin.spawn",
            spawn(Harness::Claude, "x", PROJECT),
        )
        .await;
    assert_eq!(
        r.error_code(),
        codes::UNAUTHORIZED,
        "agent cannot admin.spawn"
    );

    // admin.remove
    let r = d
        .rpc_as("agent", PROJECT, "admin.remove", remove("x"))
        .await;
    assert_eq!(
        r.error_code(),
        codes::UNAUTHORIZED,
        "agent cannot admin.remove"
    );

    // admin.assignRole
    let r = d
        .rpc_as(
            "agent",
            PROJECT,
            "admin.assignRole",
            serde_json::json!({ "name": "x", "role": "lead" }),
        )
        .await;
    assert_eq!(
        r.error_code(),
        codes::UNAUTHORIZED,
        "agent cannot admin.assignRole"
    );

    // admin.assign
    let r = d
        .rpc_as(
            "agent",
            PROJECT,
            "admin.assign",
            serde_json::json!({ "id": "a_s_x", "name": "y" }),
        )
        .await;
    assert_eq!(
        r.error_code(),
        codes::UNAUTHORIZED,
        "agent cannot admin.assign"
    );

    // admin.monitor
    let r = d
        .rpc_as(
            "agent",
            PROJECT,
            "admin.monitor",
            serde_json::json!({ "follow": false, "scope": "all" }),
        )
        .await;
    assert_eq!(
        r.error_code(),
        codes::UNAUTHORIZED,
        "agent cannot admin.monitor"
    );
}

/// An admin-tier caller is allowed to run admin methods (here: spawn), and the spawned agent becomes
/// an addressable member (the admin.spawn wiring tail).
#[tokio::test]
async fn admin_tier_allowed_to_spawn() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg_admin("root", "ck_root", PROJECT))
        .await
        .ok();

    let spawned: SpawnResponse = d
        .rpc_as(
            "root",
            PROJECT,
            "admin.spawn",
            spawn(Harness::Claude, "svc", PROJECT),
        )
        .await
        .result();
    assert!(!spawned.session_id.0.is_empty(), "admin may spawn");

    let members: nexus_contracts::MemberListResponse = d
        .rpc_as(
            "root",
            PROJECT,
            "members",
            serde_json::json!({ "includeOffline": true }),
        )
        .await
        .result();
    assert!(
        members
            .members
            .iter()
            .any(|m| m.name.as_deref() == Some("svc")),
        "an admin-spawned agent is an addressable member too"
    );
}

/// Admin is **extra commands only**: an admin command (assign-role) does not create a message or an
/// in_flight row — the message path is untouched (spec §8 guardrail). We assert via the recipient's
/// history staying empty after an admin op.
#[tokio::test]
async fn admin_op_does_not_inject_into_message_path() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg_admin("root", "ck_root", PROJECT))
        .await
        .ok();
    d.rpc_as(
        "root",
        PROJECT,
        "launch",
        spawn(Harness::Claude, "worker", PROJECT),
    )
    .await
    .ok();

    let mock = d.mock().clone();
    let before = mock.injected_prompts().len();

    // An admin role assignment — a pure command, not a message.
    d.rpc_as(
        "root",
        PROJECT,
        "admin.assignRole",
        serde_json::json!({ "name": "worker", "role": "lead" }),
    )
    .await
    .ok();

    // No turn should have been injected by the admin op.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert_eq!(
        mock.injected_prompts().len(),
        before,
        "an admin command must not inject anything into the message path"
    );
}
