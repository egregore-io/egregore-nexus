//! Init-failure acceptance (spec §11): when the adapter fails to open its session, the launch
//! surfaces an error, but the session is **retained** — the agent is still an addressable bus member
//! (registered first, so it can be retried) and a message sent to it is **not lost** (its in_flight
//! row stays pending / re-driveable; nothing is injected because the adapter is down).
//!
//! This is the daemon's failure contract: a launched agent whose harness handshake fails is kept,
//! not silently dropped, and the message path loses nothing.

mod common;

use common::*;

use nexus_contracts::{codes, Harness, MemberListResponse};

const PROJECT: &str = "proj";

/// A failing `open_session` makes `launch` error, yet the agent is retained as an addressable
/// member and a DM to it stays pending (never injected, never dropped).
#[tokio::test]
async fn launch_with_failing_adapter_retains_member_and_loses_no_message() {
    // A mock whose open_session fails (the init-failure switch).
    let mock = nexus_agent::MockAdapter::new();
    mock.fail_open_session("acp handshake refused");
    let d = TestDaemon::start_with(mock.clone()).await;

    d.rpc_anon("register", reg("driver", "ck_driver", PROJECT))
        .await
        .ok();

    // Launch errors (adapter init failure → INTERNAL_ERROR), but does not panic the daemon.
    let launch = d
        .rpc_as(
            "driver",
            PROJECT,
            "launch",
            spawn(Harness::Claude, "broken", PROJECT),
        )
        .await;
    assert!(
        launch.is_error(),
        "a failing adapter makes launch return an error"
    );
    assert_eq!(
        launch.error_code(),
        codes::INTERNAL_ERROR,
        "init failure → INTERNAL_ERROR"
    );

    // The session is RETAINED: `broken` is still an addressable member (registered before the
    // adapter open was attempted, so it survives the failure for retry).
    let members: MemberListResponse = d
        .rpc_as(
            "driver",
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
            .any(|m| m.name.as_deref() == Some("broken")),
        "errored agent is retained in members; got {:?}",
        members.members.iter().map(|m| &m.name).collect::<Vec<_>>()
    );

    // A DM to the errored agent is NOT lost: the loop wakes, but inject fails (adapter down), so the
    // row stays pending and nothing is injected. Assert no prompt was ever delivered to the mock.
    d.rpc_as("driver", PROJECT, "send", dm("broken", "are you up?"))
        .await
        .ok();
    // Give the loop ample time to (fail to) drain.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(
        mock.injected_prompts().is_empty(),
        "a down adapter injects nothing; the message stays pending (not lost)"
    );
}
