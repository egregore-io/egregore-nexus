//! Wake-policy acceptance (spec §2.4): the one behavioral relaxation vs AionCore.
//!
//! - an **idle** peer is woken with **no human gate** (a DM injects a turn);
//! - **coalescing** never loses messages (several mid-window arrivals all get delivered, batched into
//!   as few turns as the turn-end re-drain allows);
//! - a **paused** agent **holds** (no turn injected) until it resumes, then the held message drains.

mod common;

use common::*;

use nexus_contracts::Harness;

const PROJECT: &str = "proj";

/// The Nexus relaxation: an idle agent is woken by a peer DM with no human in the loop.
#[tokio::test]
async fn idle_peer_wakes_with_no_human_gate() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("peer", "ck_peer", PROJECT))
        .await
        .ok();
    d.rpc_as(
        "peer",
        PROJECT,
        "launch",
        spawn(Harness::Claude, "idle_one", PROJECT),
    )
    .await
    .ok();

    d.rpc_as("peer", PROJECT, "send", dm("idle_one", "wake up"))
        .await
        .ok();

    let mock = d.mock().clone();
    let woke = wait_until(
        || {
            mock.injected_prompts()
                .iter()
                .any(|p| p.contains("wake up"))
        },
        100,
        10,
    )
    .await;
    assert!(woke, "an idle agent wakes on a peer DM with no human gate");
}

/// Coalescing must not lose messages: a burst of DMs to one agent are all delivered (the loop drains
/// the whole pending queue, re-draining at turn-end to absorb mid-turn arrivals).
#[tokio::test]
async fn coalescing_delivers_every_message() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("sender", "ck_sender", PROJECT))
        .await
        .ok();
    d.rpc_as(
        "sender",
        PROJECT,
        "launch",
        spawn(Harness::Claude, "sink", PROJECT),
    )
    .await
    .ok();

    // Fire several DMs in quick succession.
    for i in 0..5 {
        d.rpc_as("sender", PROJECT, "send", dm("sink", &format!("msg-{i}")))
            .await
            .ok();
    }

    let mock = d.mock().clone();
    let all_delivered = wait_until(
        || {
            let prompts = mock.injected_prompts();
            (0..5).all(|i| prompts.iter().any(|p| p.contains(&format!("msg-{i}"))))
        },
        200,
        10,
    )
    .await;
    assert!(
        all_delivered,
        "every coalesced message must be delivered; injected = {:?}",
        mock.injected_prompts()
    );
}

/// A paused agent holds: a DM sent while paused is NOT injected; after resume it drains.
#[tokio::test]
async fn paused_holds_until_resume() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("ctl", "ck_ctl", PROJECT))
        .await
        .ok();
    d.rpc_as(
        "ctl",
        PROJECT,
        "launch",
        spawn(Harness::Claude, "sleeper", PROJECT),
    )
    .await
    .ok();

    // The agent self-pauses (resolved as itself).
    d.rpc_as(
        "sleeper",
        PROJECT,
        "status",
        serde_json::json!({ "state": "paused" }),
    )
    .await
    .ok();

    // A DM arrives while paused → held, not injected.
    d.rpc_as("ctl", PROJECT, "send", dm("sleeper", "held message"))
        .await
        .ok();
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let mock = d.mock().clone();
    assert!(
        !mock
            .injected_prompts()
            .iter()
            .any(|p| p.contains("held message")),
        "a paused agent must hold the message (no turn injected): {:?}",
        mock.injected_prompts()
    );

    // Resume → the held message drains and is injected.
    d.rpc_as(
        "sleeper",
        PROJECT,
        "status",
        serde_json::json!({ "state": "active" }),
    )
    .await
    .ok();
    let drained = wait_until(
        || {
            mock.injected_prompts()
                .iter()
                .any(|p| p.contains("held message"))
        },
        200,
        10,
    )
    .await;
    assert!(drained, "on resume the held message drains and is injected");
}
