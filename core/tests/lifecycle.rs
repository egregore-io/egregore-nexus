//! Lifecycle acceptance (spec §11, AionUi-adapted): **launch → self-register → addressable member →
//! idle loop stood up → dm wakes the loop → turn injected → remove**, plus register resume reuses
//! the session.
//!
//! This is the scenario that forces the critical launch wiring: a launched mock agent must become a
//! registered, addressable bus member with a *running* event loop, so a `dm` to it wakes the loop
//! and `inject_turn` delivers the `<nexus-batch …>` turn to the mock adapter — the empirical gap the
//! plan called out (`launch` previously spawned the adapter but never registered/woke the agent).

mod common;

use common::*;

use nexus_contracts::{
    codes, Harness, MemberListResponse, RegisterResponse, RemoveResponse, SpawnResponse,
};
use nexus_store::repos::{Agents, Sessions};

const PROJECT: &str = "proj";

/// register is idempotent on `client_key`: a second register with the same key resumes the same
/// session (no duplicate), and the startup directive contains the `<nexus>` framing.
#[tokio::test]
async fn register_returns_directive_and_resumes_on_same_client_key() {
    let d = TestDaemon::start().await;

    let r: RegisterResponse = d
        .rpc_anon("register", reg("ben", "ck1", PROJECT))
        .await
        .result();
    assert!(
        r.directive.contains("<nexus"),
        "directive must carry the <nexus> framing"
    );
    let sid = r.session_id.clone();

    let r2: RegisterResponse = d
        .rpc_anon("register", reg("ben", "ck1", PROJECT))
        .await
        .result();
    assert_eq!(
        r2.session_id, sid,
        "same client_key resumes the same session (register-once)"
    );
}

/// Shell-originated launch is a control-plane action: the command intent carries the synthetic
/// local operator identity, so a human does not have to export `NEXUS_NAME` just to spawn an agent.
/// The launched agent still receives its own session-backed identity and can idempotently
/// self-register with that session key.
#[tokio::test]
async fn local_operator_launch_does_not_require_registered_agent_caller() {
    let d = TestDaemon::start().await;

    let spawned = d
        .local_operator_launch(PROJECT, spawn(Harness::Codex, "worker1", PROJECT))
        .await
        .expect("local operator launch should be trusted without agent registration");
    assert!(
        !spawned.session_id.0.is_empty(),
        "launch returns the daemon-minted session id"
    );

    let resolved = d.state().identity.resolve(PROJECT, "worker1").await;
    assert!(
        resolved.is_ok(),
        "launched agent is reserved/addressable immediately"
    );
    let resolved = resolved.unwrap();
    assert_eq!(resolved.session, spawned.session_id);
    assert_eq!(resolved.project, PROJECT);

    d.rpc_anon("register", reg("boss", "ck_boss", PROJECT))
        .await
        .ok();
    d.rpc_as("boss", PROJECT, "send", dm("worker1", "hello"))
        .await
        .ok();

    let mock = d.mock().clone();
    let delivered = wait_until(
        || mock.injected_prompts().iter().any(|p| p.contains("hello")),
        100,
        10,
    )
    .await;
    assert!(delivered, "reserved local-launched agent must be wakeable");
}

/// The daemon no longer has a local HTTP/socket edge that synthesizes callers. Authenticated
/// methods must receive a resolved caller; producers that originate from the local shell carry the
/// synthetic local operator through command-intent metadata instead.
#[tokio::test]
async fn dispatch_launch_without_caller_is_unauthorized() {
    let d = TestDaemon::start().await;

    let launch = d
        .rpc_anon("launch", spawn(Harness::Codex, "remote_worker", PROJECT))
        .await;

    assert_eq!(launch.error_code(), codes::UNAUTHORIZED);
}

#[tokio::test]
async fn launch_without_name_stages_id_only_identity() {
    let d = TestDaemon::start().await;

    let spawned = d
        .local_operator_launch(PROJECT, spawn_no_name(Harness::Codex, PROJECT))
        .await
        .expect("local operator launch without name should succeed");

    let expected_agent_id = format!("a_{}", spawned.session_id.0);
    let session = Sessions::new(&d.state().store)
        .find_by_session_id(&spawned.session_id)
        .await
        .unwrap()
        .expect("staged launch creates a durable session");
    assert_eq!(session.name, None);
    assert_eq!(session.project, PROJECT);

    let agent = Agents::new(&d.state().store)
        .find_by_id(&expected_agent_id)
        .await
        .unwrap()
        .expect("staged launch creates a durable agent identity");
    assert_eq!(agent.name, None);
    assert_eq!(agent.project, PROJECT);
}

/// THE critical end-to-end: launch a mock agent named `worker1` → it appears in `members` → a `dm`
/// to it wakes its event loop → the mock adapter receives an injected `<nexus …>` turn → remove.
#[tokio::test]
async fn launch_makes_agent_addressable_and_wakeable_then_remove() {
    let d = TestDaemon::start().await;

    // An admin caller does the launch + remove; also a registered sender so the DM resolves.
    d.rpc_anon("register", reg_admin("boss", "ck_boss", PROJECT))
        .await
        .ok();

    // (1) Launch a mock agent named "worker1".
    let spawned: SpawnResponse = d
        .rpc_as(
            "boss",
            PROJECT,
            "launch",
            spawn(Harness::Claude, "worker1", PROJECT),
        )
        .await
        .result();
    assert!(
        !spawned.session_id.0.is_empty(),
        "launch returns a session id"
    );

    // (2) It must be an addressable bus member now (the gap: previously it never appeared here).
    let members: MemberListResponse = d
        .rpc_as("boss", PROJECT, "members", serde_json::json!({}))
        .await
        .result();
    assert!(
        members
            .members
            .iter()
            .any(|m| m.name.as_deref() == Some("worker1")),
        "launched agent must appear in members; got {:?}",
        members.members.iter().map(|m| &m.name).collect::<Vec<_>>()
    );

    // (3) DM worker1 → the bus rings its bell → the spawned loop drains → inject_turn on the mock.
    d.rpc_as(
        "boss",
        PROJECT,
        "send",
        dm("worker1", "rebase before you start"),
    )
    .await
    .ok();

    let mock = d.mock().clone();
    let delivered = wait_until(
        || {
            mock.injected_prompts()
                .iter()
                .any(|p| p.contains("rebase before you start"))
        },
        100,
        10,
    )
    .await;
    assert!(
        delivered,
        "the dm must wake the loop and be injected as a turn; injected = {:?}",
        mock.injected_prompts()
    );
    // The injected turn carries bus provenance (wrapped, since the sender is not THE core human DM).
    let last = mock.last_prompt().unwrap();
    assert!(
        last.contains("<nexus"),
        "injected bus turn must carry <nexus> provenance: {last}"
    );
    assert!(
        last.contains("from=\"boss\""),
        "turn names the sender: {last}"
    );

    // (4) Remove tears the loop binding down (store retained).
    let removed: RemoveResponse = d
        .rpc_as("boss", PROJECT, "admin.remove", remove("worker1"))
        .await
        .result();
    assert_eq!(removed.status, "removed");
}

/// Regression: the boot-respawn liveness/ordering bug. After a daemon restart a not-fresh agent's
/// stored ACP resume key is always stale, so `ensure_live`'s `session/load` fails and it MUST fall
/// back to `session/new` and end with a LIVE, injectable adapter — and only THEN spawn/ring the
/// drain loop. The live symptom was: the loop drained + injected before a usable adapter existed, so
/// the pending message failed with "no usable adapter for session" / "session not registered" and
/// stayed stuck.
///
/// We model the cold (post-restart) state with a `fail_resume`-scripted mock (session/load fails,
/// session/new succeeds) and a registered agent that has NO live adapter yet but DOES have a pending
/// DM. Calling `ensure_live` (what `respawn_pending_agents` does on boot) must recover via the
/// fallback and DELIVER the pending message, not leave it pending.
#[tokio::test]
async fn ensure_live_resume_fallback_delivers_pending_message() {
    // Mock whose session/load (resume) fails but session/new (open / new_session_only) succeeds —
    // the live stale-resume boot reality.
    let mock = nexus_agent::MockAdapter::new();
    mock.fail_resume("session/load failed: Resource not found (stale resume key after restart)");
    let d = TestDaemon::start_with(mock.clone()).await;

    // A registered sender + a registered agent. `register` stands up the agent's idle drain loop but
    // does NOT open an ACP adapter — so the agent is addressable + has a loop, but is NOT live yet
    // (exactly the post-restart state: member row + stale resume key, no live harness process).
    d.rpc_anon("register", reg("boss", "ck_boss", PROJECT))
        .await
        .ok();
    d.rpc_anon("register", reg("worker1", "ck_w1", PROJECT))
        .await
        .ok();

    // A DM to the not-live agent: the bus enqueues it pending + rings the bell. The idle loop wakes,
    // drains, and tries to inject — but there is NO live adapter, so the turn is HELD (rows stay
    // pending). This is the stuck state on boot.
    d.rpc_as(
        "boss",
        PROJECT,
        "send",
        dm("worker1", "queued before restart"),
    )
    .await
    .ok();

    // ensure_live = the boot respawn path. It re-spawns the adapter (resume fails → session/new
    // fallback → LIVE), then spawns/rings the loop, which re-drains the pending DM and injects it.
    d.state()
        .ensure_live("worker1", PROJECT)
        .await
        .expect("ensure_live must recover the not-fresh agent via the session/new fallback");

    let delivered = wait_until(
        || {
            mock.injected_prompts()
                .iter()
                .any(|p| p.contains("queued before restart"))
        },
        200,
        10,
    )
    .await;
    assert!(
        delivered,
        "the pending DM must be DELIVERED after the resume-fail→new_session fallback, not left \
         stuck; injected = {:?}",
        mock.injected_prompts()
    );
}

/// Reproduction-then-prevention for the launch→project-scoping bug: a launched agent must inherit the
/// **caller's** project even when the launch request carries no project of its own (the real
/// `nexus launch <kind> --name <n>` CLI invocation sends no `--project`). Before the fix, `launch`
/// discarded the resolved caller and `launch_agent` defaulted the project to the daemon's empty
/// default — so `worker1` was registered under `""`, invisible/unresolvable from the caller's `demo`
/// project. This drives the REAL `dispatch("launch")` arm with a caller in a project
/// distinct from `PROJECT`, and asserts addressability + resolvability + wakeability in that project.
#[tokio::test]
async fn launch_inherits_caller_project_when_request_omits_it() {
    let d = TestDaemon::start().await;

    // A caller `boss` bound in project `demo` (distinct from the other tests' `proj` so a stray
    // default-project registration cannot accidentally satisfy the assertions).
    const DEMO: &str = "demo";
    d.rpc_anon("register", reg_admin("boss", "ck_boss", DEMO))
        .await
        .ok();

    // (1) Launch a mock agent named "worker1" WITHOUT a project on the request — the CLI reproduction.
    let spawned: SpawnResponse = d
        .rpc_as(
            "boss",
            DEMO,
            "launch",
            spawn_no_project(Harness::Codex, "worker1"),
        )
        .await
        .result();
    assert!(
        !spawned.session_id.0.is_empty(),
        "launch returns a session id"
    );

    // (2) It must appear in `members` in the CALLER'S project (this failed before the fix: the agent
    // landed in the empty default project and was invisible from `demo`).
    let members: MemberListResponse = d
        .rpc_as("boss", DEMO, "members", serde_json::json!({}))
        .await
        .result();
    assert!(
        members
            .members
            .iter()
            .any(|m| m.name.as_deref() == Some("worker1")),
        "launched agent must appear in the caller's-project members; got {:?}",
        members.members.iter().map(|m| &m.name).collect::<Vec<_>>()
    );

    // (3) `IdentityPort::resolve(caller.project, name)` must succeed (so `dm worker1` resolves a
    // recipient). White-box against the wired state's identity port, scoped to `demo`.
    let resolved = d.state().identity.resolve(DEMO, "worker1").await;
    assert!(
        resolved.is_ok(),
        "IdentityPort::resolve(\"{DEMO}\", \"worker1\") must succeed: {:?}",
        resolved.err()
    );
    assert_eq!(
        resolved.unwrap().project,
        DEMO,
        "resolved under the caller's project"
    );

    // (4) A DM to worker1 (in `demo`) wakes its loop and the mock receives the injected turn.
    d.rpc_as("boss", DEMO, "send", dm("worker1", "inherit the project"))
        .await
        .ok();

    let mock = d.mock().clone();
    let delivered = wait_until(
        || {
            mock.injected_prompts()
                .iter()
                .any(|p| p.contains("inherit the project"))
        },
        100,
        10,
    )
    .await;
    assert!(
        delivered,
        "the dm must wake the loop and inject a turn; injected = {:?}",
        mock.injected_prompts()
    );
}
