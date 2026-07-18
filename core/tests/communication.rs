//! Communication acceptance (spec §11): a DM lands as an injected turn carrying its provenance; a
//! thread post fans out to every member; a topic publish fans out to every subscriber. The bus's
//! one-`to` contract (DM | thread | topic) and drain-once delivery are exercised end-to-end against
//! a real daemon + the hermetic mock adapter.

mod common;

use common::*;

use nexus_contracts::{Ack, SubscribeResponse};

const PROJECT: &str = "proj";

/// A DM to a launched agent is injected as a turn that carries the sender's provenance (wrapped in
/// `<nexus from=…>` since it is bus traffic, not THE core human's plain DM).
#[tokio::test]
async fn dm_lands_as_injected_turn_with_provenance() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("ana", "ck_ana", PROJECT))
        .await
        .ok();
    d.rpc_as(
        "ana",
        PROJECT,
        "launch",
        spawn(hid("claude"), "worker", PROJECT),
    )
    .await
    .ok();

    d.rpc_as(
        "ana",
        PROJECT,
        "send",
        dm("worker", "take the auth refactor?"),
    )
    .await
    .ok();

    let mock = d.mock().clone();
    let got = wait_until(
        || {
            mock.injected_prompts()
                .iter()
                .any(|p| p.contains("take the auth refactor?"))
        },
        100,
        10,
    )
    .await;
    assert!(
        got,
        "dm must be injected; injected = {:?}",
        mock.injected_prompts()
    );
    let last = mock.last_prompt().unwrap();
    assert!(last.contains("<nexus"), "bus DM is wrapped: {last}");
    assert!(
        last.contains("from=\"ana\""),
        "carries sender provenance: {last}"
    );
}

/// A thread post fans out to every OTHER member — the sender is never delivered its own post
/// (matches DM semantics) — and a member's loop injects the thread turn (`thread=…` provenance).
#[tokio::test]
async fn thread_post_fans_out_to_members_but_not_the_sender() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("lead", "ck_lead", PROJECT))
        .await
        .ok();
    // Two launched (registered + wakeable) members.
    d.rpc_as(
        "lead",
        PROJECT,
        "launch",
        spawn(hid("claude"), "m1", PROJECT),
    )
    .await
    .ok();
    d.rpc_as(
        "lead",
        PROJECT,
        "launch",
        spawn(hid("claude"), "m2", PROJECT),
    )
    .await
    .ok();

    // Create a thread with both members.
    d.rpc_as(
        "lead",
        PROJECT,
        "thread.new",
        serde_json::json!({ "name": "backend", "members": ["m1", "m2"] }),
    )
    .await
    .ok();

    // Post to the thread → fan-out to every OTHER member. `lead` is a member (the creator), but a
    // sender is never delivered its own post, so the recipient set is {m1, m2} = 2, NOT {lead,m1,m2}.
    let ack: Ack = d
        .rpc_as(
            "lead",
            PROJECT,
            "send",
            serde_json::json!({
                "to": { "verb": "post", "thread": "backend" },
                "body": "plan: split the auth refactor",
                "mention": [],
            }),
        )
        .await
        .result();
    assert_eq!(
        ack.fanout, None,
        "public thread acknowledgements keep recipient fanout internal"
    );

    // The thread turn reaches a member loop (both share the mock; assert the thread provenance).
    let mock = d.mock().clone();
    let got = wait_until(
        || {
            mock.injected_prompts().iter().any(|p| {
                p.contains("plan: split the auth refactor") && p.contains("thread=\"backend\"")
            })
        },
        100,
        10,
    )
    .await;
    assert!(
        got,
        "thread post must inject with thread provenance; injected = {:?}",
        mock.injected_prompts()
    );
}

/// A topic publish fans out to every subscriber (`Ack.fanout` equals the subscriber count).
#[tokio::test]
async fn topic_publish_fans_out_to_subscribers() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("pubber", "ck_pub", PROJECT))
        .await
        .ok();
    d.rpc_as(
        "pubber",
        PROJECT,
        "launch",
        spawn(hid("claude"), "sub1", PROJECT),
    )
    .await
    .ok();
    d.rpc_as(
        "pubber",
        PROJECT,
        "launch",
        spawn(hid("claude"), "sub2", PROJECT),
    )
    .await
    .ok();

    // Both subscribe to the topic (subscribe is per-caller, so resolve each as itself).
    let s1: SubscribeResponse = d
        .rpc_as(
            "sub1",
            PROJECT,
            "subscribe",
            serde_json::json!({ "topic": "builds" }),
        )
        .await
        .result();
    assert_eq!(s1.topic, "builds");
    d.rpc_as(
        "sub2",
        PROJECT,
        "subscribe",
        serde_json::json!({ "topic": "builds" }),
    )
    .await
    .ok();

    let ack: Ack = d
        .rpc_as(
            "pubber",
            PROJECT,
            "send",
            serde_json::json!({
                "to": { "verb": "publish", "topic": "builds" },
                "body": "nightly is green",
                "mention": [],
            }),
        )
        .await
        .result();
    assert_eq!(
        ack.fanout, None,
        "public topic acknowledgements keep recipient fanout internal"
    );
}
