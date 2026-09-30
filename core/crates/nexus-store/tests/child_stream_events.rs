//! The bounded volatile child lane: keys, pages, lanes, bounds and accounting.

use nexus_contracts::events::{ChildResolution, ChildStream};
use nexus_contracts::ids::SessionId;
use nexus_store::repos::{
    child_key, AppendOutcome, ChildStreamBounds, ChildStreamEvents, LaneFilter, RefusedBy,
    StreamEvents, REFUSED_SENTINEL_KEY,
};
use nexus_store::Store;
use serde_json::json;

async fn open_store() -> Store {
    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    store
}

fn session() -> SessionId {
    SessionId("s_owner".into())
}

fn child(root: &str, id: Option<&str>, locator: &str, resolution: ChildResolution) -> ChildStream {
    ChildStream {
        harness: "claude".into(),
        root: root.into(),
        id: id.map(str::to_string),
        locator: locator.into(),
        parent: None,
        parent_ref: None,
        depth: None,
        resolution,
        evidence: Some("test".into()),
    }
}

fn verified(root: &str, id: &str) -> ChildStream {
    child(
        root,
        Some(id),
        &format!("claude:{id}#0"),
        ChildResolution::RootVerified,
    )
}

fn unresolved(root: &str, n: usize) -> ChildStream {
    child(
        root,
        None,
        &format!("claude:hooks.jsonl#{n}"),
        ChildResolution::Unresolved,
    )
}

async fn append(
    store: &Store,
    child: &ChildStream,
    n: usize,
    bounds: &ChildStreamBounds,
) -> AppendOutcome {
    ChildStreamEvents::new(store)
        .append(
            &session(),
            "boot_a",
            child,
            "text",
            &format!("{}#{n}", child.locator),
            &json!({ "text": format!("chunk {n}") }).to_string(),
            bounds,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn keys_are_namespaced_by_native_id_or_locator() {
    assert_eq!(child_key(&verified("ses_root", "agent-1")), "n:agent-1");
    assert_eq!(
        child_key(&unresolved("ses_root", 7)),
        "l:claude:hooks.jsonl#7"
    );
}

#[tokio::test]
async fn append_lands_only_in_the_child_lane_with_epoch_and_source_ref() {
    let store = open_store().await;
    let bounds = ChildStreamBounds::default();
    let outcome = append(&store, &verified("ses_root", "agent-1"), 1, &bounds).await;
    let AppendOutcome::Stored(id) = outcome else {
        panic!("expected stored, got {outcome:?}");
    };
    let page = ChildStreamEvents::new(&store)
        .page(&session(), &LaneFilter::default(), 0, 10)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    let row = &page.rows[0];
    assert_eq!(row.id, id);
    assert_eq!(row.child_key, "n:agent-1");
    assert_eq!(row.epoch, "boot_a");
    assert_eq!(row.harness, "claude");
    assert_eq!(row.root, "ses_root");
    assert_eq!(row.source_ref, "claude:agent-1#0#1");
    assert_eq!(row.child.id.as_deref(), Some("agent-1"));
    assert_eq!(row.child.resolution, ChildResolution::RootVerified);
    assert!(
        row.bytes > row.data.len() as i64,
        "bytes count metadata too"
    );
    assert!(page.next_after_id.is_none());
    let parent_lane = StreamEvents::new(&store)
        .since(&session(), 0)
        .await
        .unwrap();
    assert!(
        parent_lane.is_empty(),
        "the parent stream lane is untouched"
    );
}

#[tokio::test]
async fn a_reused_native_id_under_another_root_is_a_separate_lane() {
    let store = open_store().await;
    let bounds = ChildStreamBounds::default();
    append(&store, &verified("ses_root_a", "agent-1"), 1, &bounds).await;
    append(&store, &verified("ses_root_b", "agent-1"), 2, &bounds).await;
    let lanes = ChildStreamEvents::new(&store)
        .lanes(&session(), None, 10)
        .await
        .unwrap();
    assert_eq!(lanes.lanes.len(), 2);
    let roots: Vec<&str> = lanes.lanes.iter().map(|l| l.child.root.as_str()).collect();
    assert_eq!(roots, vec!["ses_root_a", "ses_root_b"]);
    let only_b = ChildStreamEvents::new(&store)
        .page(
            &session(),
            &LaneFilter {
                root: Some("ses_root_b".into()),
                ..LaneFilter::default()
            },
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(only_b.rows.len(), 1);
    assert_eq!(only_b.rows[0].root, "ses_root_b");
}

#[tokio::test]
async fn row_bound_evicts_oldest_and_accounts_on_the_lane() {
    let store = open_store().await;
    let bounds = ChildStreamBounds {
        max_rows_per_session: 3,
        ..ChildStreamBounds::default()
    };
    let a = verified("ses_root", "agent-a");
    let b = verified("ses_root", "agent-b");
    append(&store, &a, 1, &bounds).await;
    append(&store, &a, 2, &bounds).await;
    append(&store, &b, 3, &bounds).await;
    append(&store, &b, 4, &bounds).await;
    append(&store, &b, 5, &bounds).await;
    let repo = ChildStreamEvents::new(&store);
    let page = repo
        .page(&session(), &LaneFilter::default(), 0, 10)
        .await
        .unwrap();
    assert_eq!(
        page.rows
            .iter()
            .map(|r| r.source_ref.as_str())
            .collect::<Vec<_>>(),
        vec![
            "claude:agent-b#0#3",
            "claude:agent-b#0#4",
            "claude:agent-b#0#5"
        ]
    );
    let lanes = repo.lanes(&session(), None, 10).await.unwrap().lanes;
    let lane_a = lanes.iter().find(|l| l.child_key == "n:agent-a").unwrap();
    assert_eq!(lane_a.live_rows, 0, "lane a is a tombstone");
    assert_eq!(lane_a.evicted_rows, 2);
    assert_eq!(lane_a.evicted_through, 2);
    assert!(lane_a.evicted_bytes > 0);
    assert!(lane_a.live_first_id.is_none());
    let lane_b = lanes.iter().find(|l| l.child_key == "n:agent-b").unwrap();
    assert_eq!(lane_b.live_rows, 3);
    assert_eq!(lane_b.evicted_rows, 0);
    assert_eq!(lane_b.live_first_id, Some(3));
    assert_eq!(lane_b.live_last_id, Some(5));
}

#[tokio::test]
async fn byte_bound_evicts_by_metadata_inclusive_size() {
    let store = open_store().await;
    let one_row_bytes = {
        let probe = open_store().await;
        append(
            &probe,
            &verified("ses_root", "agent-a"),
            1,
            &ChildStreamBounds::default(),
        )
        .await;
        ChildStreamEvents::new(&probe)
            .page(&session(), &LaneFilter::default(), 0, 1)
            .await
            .unwrap()
            .rows[0]
            .bytes as u64
    };
    let bounds = ChildStreamBounds {
        max_bytes_per_session: one_row_bytes * 2,
        ..ChildStreamBounds::default()
    };
    let a = verified("ses_root", "agent-a");
    append(&store, &a, 1, &bounds).await;
    append(&store, &a, 2, &bounds).await;
    append(&store, &a, 3, &bounds).await;
    let lanes = ChildStreamEvents::new(&store)
        .lanes(&session(), None, 10)
        .await
        .unwrap()
        .lanes;
    assert_eq!(lanes[0].live_rows, 2);
    assert_eq!(lanes[0].evicted_rows, 1);
    assert_eq!(lanes[0].evicted_bytes as u64, one_row_bytes);
}

#[tokio::test]
async fn unresolved_lane_bound_refuses_and_counts_on_the_sentinel() {
    let store = open_store().await;
    let bounds = ChildStreamBounds {
        max_unresolved_lanes_per_session: 2,
        ..ChildStreamBounds::default()
    };
    assert!(matches!(
        append(&store, &unresolved("ses_root", 1), 1, &bounds).await,
        AppendOutcome::Stored(_)
    ));
    assert!(matches!(
        append(&store, &unresolved("ses_root", 2), 2, &bounds).await,
        AppendOutcome::Stored(_)
    ));
    assert_eq!(
        append(&store, &unresolved("ses_root", 3), 3, &bounds).await,
        AppendOutcome::Refused(RefusedBy::UnresolvedLanes)
    );
    // An existing unresolved lane keeps accepting rows; a verified lane is not limited by it.
    assert!(matches!(
        append(&store, &unresolved("ses_root", 1), 4, &bounds).await,
        AppendOutcome::Stored(_)
    ));
    assert!(matches!(
        append(&store, &verified("ses_root", "agent-a"), 5, &bounds).await,
        AppendOutcome::Stored(_)
    ));
    let lanes = ChildStreamEvents::new(&store)
        .lanes(&session(), None, 10)
        .await
        .unwrap()
        .lanes;
    let sentinel = lanes
        .iter()
        .find(|l| l.child_key == REFUSED_SENTINEL_KEY)
        .expect("sentinel lane");
    assert_eq!(sentinel.refused_rows, 1);
    assert_eq!(sentinel.live_rows, 0);
    let page = ChildStreamEvents::new(&store)
        .page(&session(), &LaneFilter::default(), 0, 10)
        .await
        .unwrap();
    assert_eq!(
        page.rows.len(),
        4,
        "the refused observation was never stored"
    );
}

#[tokio::test]
async fn lane_budget_compacts_tombstones_into_session_loss_then_refuses() {
    let store = open_store().await;
    let bounds = ChildStreamBounds {
        max_rows_per_session: 2,
        max_lanes_per_session: 2,
        ..ChildStreamBounds::default()
    };
    let repo = ChildStreamEvents::new(&store);
    append(&store, &verified("ses_root", "agent-a"), 1, &bounds).await;
    append(&store, &verified("ses_root", "agent-b"), 2, &bounds).await;
    // lane a's only row is evicted by the row bound: a becomes a tombstone.
    append(&store, &verified("ses_root", "agent-b"), 3, &bounds).await;
    assert!(repo.session_loss(&session()).await.unwrap().is_none());
    // A third lane is over budget: the tombstone is compacted, the new lane is stored.
    assert!(matches!(
        append(&store, &verified("ses_root", "agent-c"), 4, &bounds).await,
        AppendOutcome::Stored(_)
    ));
    let loss = repo
        .session_loss(&session())
        .await
        .unwrap()
        .expect("loss row");
    assert_eq!(loss.compacted_lanes, 1);
    assert_eq!(loss.evicted_rows, 1);
    assert!(loss.evicted_bytes > 0);
    assert_eq!(loss.epoch, "boot_a");
    let lanes = repo.lanes(&session(), None, 10).await.unwrap().lanes;
    assert!(lanes.iter().all(|l| l.child_key != "n:agent-a"));
    // The row bound left one live row in each remaining lane (b#3, c#4): nothing is
    // compactable, so a fourth lane is refused and counted on the sentinel.
    let live: Vec<(String, i64)> = lanes
        .iter()
        .map(|l| (l.child_key.clone(), l.live_rows))
        .collect();
    assert_eq!(
        live,
        vec![("n:agent-b".to_string(), 1), ("n:agent-c".to_string(), 1)]
    );
    assert_eq!(
        append(&store, &verified("ses_root", "agent-d"), 6, &bounds).await,
        AppendOutcome::Refused(RefusedBy::Lanes)
    );
    let sentinel = repo
        .lanes(&session(), None, 10)
        .await
        .unwrap()
        .lanes
        .into_iter()
        .find(|l| l.child_key == REFUSED_SENTINEL_KEY)
        .expect("sentinel");
    assert_eq!(sentinel.refused_rows, 1);
}

#[tokio::test]
async fn pages_and_lane_enumeration_are_sql_limited() {
    let store = open_store().await;
    let bounds = ChildStreamBounds::default();
    for n in 0..5 {
        append(
            &store,
            &verified("ses_root", &format!("agent-{n}")),
            n,
            &bounds,
        )
        .await;
    }
    let repo = ChildStreamEvents::new(&store);
    let first = repo
        .page(&session(), &LaneFilter::default(), 0, 2)
        .await
        .unwrap();
    assert_eq!(first.rows.len(), 2);
    assert_eq!(first.next_after_id, Some(2));
    let second = repo
        .page(&session(), &LaneFilter::default(), 2, 2)
        .await
        .unwrap();
    assert_eq!(second.rows.len(), 2);
    assert_eq!(second.next_after_id, Some(4));
    let last = repo
        .page(&session(), &LaneFilter::default(), 4, 2)
        .await
        .unwrap();
    assert_eq!(last.rows.len(), 1);
    assert!(last.next_after_id.is_none());

    let lanes_a = repo.lanes(&session(), None, 3).await.unwrap();
    assert_eq!(lanes_a.lanes.len(), 3);
    assert_eq!(
        lanes_a.next_after.as_ref().map(|c| c.child_key.as_str()),
        Some("n:agent-2")
    );
    let lanes_b = repo
        .lanes(&session(), lanes_a.next_after.as_ref(), 3)
        .await
        .unwrap();
    assert_eq!(lanes_b.lanes.len(), 2);
    assert!(lanes_b.next_after.is_none());
}

#[tokio::test]
async fn coverage_is_declared_per_lane_and_evict_session_clears_everything() {
    let store = open_store().await;
    let repo = ChildStreamEvents::new(&store);
    let c = verified("ses_root", "agent-a");
    repo.set_coverage(
        &session(),
        "boot_a",
        &c,
        &json!({ "generation": "g1", "from": 0, "to": 0, "unknown_before": true }),
        &ChildStreamBounds::default(),
    )
    .await
    .unwrap();
    let lanes = repo.lanes(&session(), None, 10).await.unwrap().lanes;
    assert_eq!(lanes.len(), 1);
    assert_eq!(lanes[0].live_rows, 0);
    assert_eq!(lanes[0].coverage.as_ref().unwrap()["unknown_before"], true);
    append(&store, &c, 1, &ChildStreamBounds::default()).await;
    let lanes = repo.lanes(&session(), None, 10).await.unwrap().lanes;
    assert_eq!(lanes[0].live_rows, 1);
    assert_eq!(lanes[0].coverage.as_ref().unwrap()["generation"], "g1");
    repo.evict_session(&session()).await.unwrap();
    assert!(repo
        .page(&session(), &LaneFilter::default(), 0, 10)
        .await
        .unwrap()
        .rows
        .is_empty());
    assert!(repo
        .lanes(&session(), None, 10)
        .await
        .unwrap()
        .lanes
        .is_empty());
    assert!(repo.session_loss(&session()).await.unwrap().is_none());
}
