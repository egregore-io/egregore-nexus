//! Member-ops acceptance (spec §11): thread create/join/leave is reflected in `threads` /
//! `thread.members`, and a non-member is excluded from a thread's fan-out (no in_flight row, never
//! injected). Exercises the directory + membership side of the bus against a real daemon.

mod common;

use common::*;

use nexus_contracts::{
    Ack, MemberListResponse, Presence, RemoveRequest, ThreadListResponse, ThreadMembersResponse,
};

const PROJECT: &str = "proj";

/// create → join → leave is reflected in the thread's member list and the caller's thread list.
#[tokio::test]
async fn thread_create_join_leave_reflected_in_listings() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("owner", "ck_owner", PROJECT))
        .await
        .ok();
    d.rpc_anon("register", reg("joiner", "ck_joiner", PROJECT))
        .await
        .ok();

    // owner creates a thread (owner auto-added as member).
    d.rpc_as(
        "owner",
        PROJECT,
        "thread.new",
        serde_json::json!({ "name": "infra", "members": [] }),
    )
    .await
    .ok();

    // owner sees the thread in its list.
    let listed: ThreadListResponse = d.rpc_as_unit("owner", PROJECT, "threads").await.result();
    assert!(
        listed.threads.iter().any(|t| t.name == "infra"),
        "creator lists its thread"
    );

    // joiner joins → appears in the member list.
    d.rpc_as(
        "joiner",
        PROJECT,
        "thread.join",
        serde_json::json!({ "name": "infra" }),
    )
    .await
    .ok();
    let members: ThreadMembersResponse = d
        .rpc_as(
            "owner",
            PROJECT,
            "thread.members",
            serde_json::json!({ "name": "infra" }),
        )
        .await
        .result();
    assert!(
        members.members.contains(&"owner".to_string()),
        "creator is a member"
    );
    assert!(
        members.members.contains(&"joiner".to_string()),
        "joiner is a member after join"
    );

    // joiner leaves → drops out of the member list.
    d.rpc_as(
        "joiner",
        PROJECT,
        "thread.leave",
        serde_json::json!({ "name": "infra" }),
    )
    .await
    .ok();
    let after: ThreadMembersResponse = d
        .rpc_as(
            "owner",
            PROJECT,
            "thread.members",
            serde_json::json!({ "name": "infra" }),
        )
        .await
        .result();
    assert!(
        !after.members.contains(&"joiner".to_string()),
        "joiner excluded after leave"
    );
}

/// A non-member is excluded from a thread's fan-out: a post reaches only the members, and the
/// outsider's loop is never injected with the thread message.
#[tokio::test]
async fn non_member_excluded_from_thread_fanout() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg("poster", "ck_poster", PROJECT))
        .await
        .ok();
    // `inside` is a launched (wakeable) member; `outside` is launched but NOT in the thread.
    d.rpc_as(
        "poster",
        PROJECT,
        "launch",
        spawn(hid("claude"), "inside", PROJECT),
    )
    .await
    .ok();
    d.rpc_as(
        "poster",
        PROJECT,
        "launch",
        spawn(hid("claude"), "outside", PROJECT),
    )
    .await
    .ok();

    // Thread with only {poster, inside}.
    d.rpc_as(
        "poster",
        PROJECT,
        "thread.new",
        serde_json::json!({ "name": "secret", "members": ["inside"] }),
    )
    .await
    .ok();

    let ack: Ack = d
        .rpc_as(
            "poster",
            PROJECT,
            "send",
            serde_json::json!({
                "to": { "verb": "post", "thread": "secret" },
                "body": "members only",
                "mention": [],
            }),
        )
        .await
        .result();
    // Fan-out is {inside} = 1 — `poster` is the sender (never delivered its own post) and `outside`
    // is a non-member; both are excluded.
    assert_eq!(
        ack.fanout, None,
        "public acknowledgement must not expose the authorized recipient count"
    );

    // Give the loops a moment; the thread message must have been injected to a member, but the body
    // must never carry the outsider as a recipient (it is simply not in the fan-out set).
    let mock = d.mock().clone();
    let injected = wait_until(
        || {
            mock.injected_prompts()
                .iter()
                .any(|p| p.contains("members only"))
        },
        100,
        10,
    )
    .await;
    assert!(injected, "the thread post reaches a member's loop");
}

#[tokio::test]
async fn removed_agent_leaves_default_members_and_remains_offline_for_audit() {
    let d = TestDaemon::start().await;
    d.rpc_anon("register", reg_admin("owner", "ck_owner", PROJECT))
        .await
        .ok();
    d.rpc_as(
        "owner",
        PROJECT,
        "launch",
        spawn(hid("claude"), "doomed-member", PROJECT),
    )
    .await
    .ok();

    d.rpc_as(
        "owner",
        PROJECT,
        "admin.remove",
        RemoveRequest {
            agent_id: None,
            name: "doomed-member".to_string(),
            kill: true,
        },
    )
    .await
    .ok();

    let default_members: MemberListResponse = d
        .rpc_as("owner", PROJECT, "members", serde_json::json!({}))
        .await
        .result();
    assert!(
        default_members
            .members
            .iter()
            .all(|member| member.name.as_deref() != Some("doomed-member")),
        "default members must hide offline removed agents"
    );

    let audit_members: MemberListResponse = d
        .rpc_as(
            "owner",
            PROJECT,
            "members",
            serde_json::json!({ "includeOffline": true }),
        )
        .await
        .result();
    let removed = audit_members
        .members
        .iter()
        .find(|member| member.name.as_deref() == Some("doomed-member"))
        .expect("offline audit row remains visible with includeOffline");
    assert_eq!(removed.presence, Presence::Offline);
}
