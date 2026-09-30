//! Child-lane invariants: composite lane cursor, bounded
//! metadata-only mutations, the verified-to-unresolved admission gate on both mutation paths,
//! serialized concurrent admission with owned inserted ids, and failure behavior that never
//! grows retained state, never orphans an event, and never counts a loss that did not happen.

use std::sync::Arc;

use nexus_contracts::events::{ChildResolution, ChildStream};
use nexus_contracts::ids::SessionId;
use nexus_store::repos::{
    AppendOutcome, ChildStreamBounds, ChildStreamEvents, LaneFilter, LaneMutation, RefusedBy,
    MAX_COVERAGE_BYTES, REFUSED_SENTINEL_KEY,
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

fn stream(root: &str, id: Option<&str>, locator: &str, resolution: ChildResolution) -> ChildStream {
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
    stream(
        root,
        Some(id),
        &format!("claude:{id}#0"),
        ChildResolution::RootVerified,
    )
}

fn unresolved(root: &str, n: usize) -> ChildStream {
    stream(
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
) -> Result<AppendOutcome, nexus_common::NexusError> {
    append_data(
        store,
        child,
        n,
        &json!({ "text": format!("chunk {n}") }).to_string(),
        bounds,
    )
    .await
}

async fn append_data(
    store: &Store,
    child: &ChildStream,
    n: usize,
    data: &str,
    bounds: &ChildStreamBounds,
) -> Result<AppendOutcome, nexus_common::NexusError> {
    ChildStreamEvents::new(store)
        .append(
            &session(),
            "boot_a",
            child,
            "text",
            &format!("{}#{n}", child.locator),
            data,
            bounds,
        )
        .await
}

async fn lane(store: &Store, key: &str) -> Option<nexus_store::repos::LaneSummary> {
    let mut cursor = None;
    loop {
        let page = ChildStreamEvents::new(store)
            .lanes(&session(), cursor.as_ref(), 8)
            .await
            .unwrap();
        if let Some(found) = page.lanes.iter().find(|l| l.child_key == key) {
            return Some(found.clone());
        }
        match page.next_after {
            Some(next) => cursor = Some(next),
            None => return None,
        }
    }
}

async fn sentinel_refusals(store: &Store) -> i64 {
    lane(store, REFUSED_SENTINEL_KEY)
        .await
        .map(|l| l.refused_rows)
        .unwrap_or(0)
}

async fn rows_of(store: &Store, key: &str) -> Vec<String> {
    ChildStreamEvents::new(store)
        .page(
            &session(),
            &LaneFilter {
                child_key: Some(key.into()),
                ..LaneFilter::default()
            },
            0,
            100,
        )
        .await
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r.source_ref)
        .collect()
}

#[tokio::test]
async fn lane_enumeration_with_page_size_one_never_skips_lanes_sharing_a_child_key() {
    let store = open_store().await;
    let bounds = ChildStreamBounds::default();
    append(&store, &verified("ses_root_a", "agent-a"), 1, &bounds)
        .await
        .unwrap();
    append(&store, &verified("ses_root_b", "agent-a"), 2, &bounds)
        .await
        .unwrap();
    append(&store, &verified("ses_root_a", "agent-b"), 3, &bounds)
        .await
        .unwrap();
    let repo = ChildStreamEvents::new(&store);
    let mut seen: Vec<(String, String)> = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let page = repo.lanes(&session(), cursor.as_ref(), 1).await.unwrap();
        pages += 1;
        assert!(page.lanes.len() <= 1);
        for lane in &page.lanes {
            seen.push((lane.child_key.clone(), lane.child.root.clone()));
        }
        match page.next_after {
            Some(next) => cursor = Some(next),
            None => break,
        }
        assert!(pages < 10, "enumeration must terminate");
    }
    assert_eq!(
        seen,
        vec![
            ("n:agent-a".to_string(), "ses_root_a".to_string()),
            ("n:agent-a".to_string(), "ses_root_b".to_string()),
            ("n:agent-b".to_string(), "ses_root_a".to_string()),
        ]
    );
}

#[tokio::test]
async fn coverage_creation_respects_the_lane_budget_and_the_per_record_caps() {
    let store = open_store().await;
    let repo = ChildStreamEvents::new(&store);
    let bounds = ChildStreamBounds {
        max_lanes_per_session: 1,
        ..ChildStreamBounds::default()
    };
    let small = json!({ "generation": "g1", "unknown_before": true });
    assert_eq!(
        repo.set_coverage(
            &session(),
            "boot_a",
            &verified("ses_root", "agent-a"),
            &small,
            &bounds
        )
        .await
        .unwrap(),
        LaneMutation::Applied
    );
    // Budget of one lane, and a coverage-only lane is not a tombstone: the second is refused.
    assert_eq!(
        repo.set_coverage(
            &session(),
            "boot_a",
            &verified("ses_root", "agent-b"),
            &small,
            &bounds
        )
        .await
        .unwrap(),
        LaneMutation::Refused(RefusedBy::Lanes)
    );
    // Oversized coverage on the existing lane is refused before touching it.
    let huge = json!({ "blob": "x".repeat(MAX_COVERAGE_BYTES + 1) });
    assert_eq!(
        repo.set_coverage(
            &session(),
            "boot_a",
            &verified("ses_root", "agent-a"),
            &huge,
            &bounds
        )
        .await
        .unwrap(),
        LaneMutation::Refused(RefusedBy::Oversized)
    );
    let lane_a = lane(&store, "n:agent-a").await.unwrap();
    assert_eq!(lane_a.coverage.as_ref().unwrap()["generation"], "g1");
    assert!(lane(&store, "n:agent-b").await.is_none());
    assert_eq!(sentinel_refusals(&store).await, 2);
}

#[tokio::test]
async fn an_oversized_identity_is_refused_and_leaves_no_tombstone() {
    let store = open_store().await;
    let bounds = ChildStreamBounds::default();
    let oversized = stream(
        "ses_root",
        None,
        &"claude:".repeat(200),
        ChildResolution::Unresolved,
    );
    assert_eq!(
        append(&store, &oversized, 1, &bounds).await.unwrap(),
        AppendOutcome::Refused(RefusedBy::Oversized)
    );
    let repo = ChildStreamEvents::new(&store);
    let lanes = repo.lanes(&session(), None, 10).await.unwrap().lanes;
    assert_eq!(lanes.len(), 1, "only the sentinel exists");
    assert_eq!(lanes[0].child_key, REFUSED_SENTINEL_KEY);
    assert_eq!(lanes[0].refused_rows, 1);
    assert!(repo
        .page(&session(), &LaneFilter::default(), 0, 10)
        .await
        .unwrap()
        .rows
        .is_empty());
}

#[tokio::test]
async fn an_event_larger_than_the_byte_budget_is_refused_not_stored_then_evicted() {
    let store = open_store().await;
    let bounds = ChildStreamBounds {
        max_bytes_per_session: 256,
        ..ChildStreamBounds::default()
    };
    let big = json!({ "text": "y".repeat(300) }).to_string();
    assert_eq!(
        append_data(&store, &verified("ses_root", "agent-a"), 1, &big, &bounds)
            .await
            .unwrap(),
        AppendOutcome::Refused(RefusedBy::Oversized)
    );
    assert!(rows_of(&store, "n:agent-a").await.is_empty());
    assert!(lane(&store, "n:agent-a").await.is_none());
    assert_eq!(sentinel_refusals(&store).await, 1);
}

#[tokio::test]
async fn a_verified_lane_turning_unresolved_passes_the_unresolved_bound_on_append() {
    let store = open_store().await;
    let bounds = ChildStreamBounds {
        max_unresolved_lanes_per_session: 1,
        ..ChildStreamBounds::default()
    };
    assert!(matches!(
        append(&store, &unresolved("ses_root", 1), 1, &bounds)
            .await
            .unwrap(),
        AppendOutcome::Stored(_)
    ));
    assert!(matches!(
        append(&store, &verified("ses_root", "agent-x"), 2, &bounds)
            .await
            .unwrap(),
        AppendOutcome::Stored(_)
    ));
    // Same lane key, now declared unresolved: that is a new unresolved lane for the bound.
    let flipped = stream(
        "ses_root",
        Some("agent-x"),
        "claude:agent-x#0",
        ChildResolution::Unresolved,
    );
    assert_eq!(
        append(&store, &flipped, 3, &bounds).await.unwrap(),
        AppendOutcome::Refused(RefusedBy::UnresolvedLanes)
    );
    let lane_x = lane(&store, "n:agent-x").await.unwrap();
    assert_eq!(lane_x.child.resolution, ChildResolution::RootVerified);
    assert_eq!(lane_x.live_rows, 1);
    // With room in the bound, the flip is admitted and the lane becomes unresolved.
    let roomy = ChildStreamBounds {
        max_unresolved_lanes_per_session: 2,
        ..ChildStreamBounds::default()
    };
    assert!(matches!(
        append(&store, &flipped, 4, &roomy).await.unwrap(),
        AppendOutcome::Stored(_)
    ));
    let lane_x = lane(&store, "n:agent-x").await.unwrap();
    assert_eq!(lane_x.child.resolution, ChildResolution::Unresolved);
    assert_eq!(lane_x.live_rows, 2);
}

#[tokio::test]
async fn a_coverage_update_that_turns_a_lane_unresolved_consumes_the_unresolved_budget() {
    let store = open_store().await;
    let repo = ChildStreamEvents::new(&store);
    let tight = ChildStreamBounds {
        max_unresolved_lanes_per_session: 1,
        ..ChildStreamBounds::default()
    };
    append(&store, &unresolved("ses_root", 1), 1, &tight)
        .await
        .unwrap();
    append(&store, &verified("ses_root", "agent-x"), 2, &tight)
        .await
        .unwrap();
    let flipped = stream(
        "ses_root",
        Some("agent-x"),
        "claude:agent-x#0",
        ChildResolution::Unresolved,
    );
    let cov = json!({ "generation": "g1", "unknown_before": true });
    // Over the bound: refused, and the lane keeps its verified identity and status.
    assert_eq!(
        repo.set_coverage(&session(), "boot_a", &flipped, &cov, &tight)
            .await
            .unwrap(),
        LaneMutation::Refused(RefusedBy::UnresolvedLanes)
    );
    let lane_x = lane(&store, "n:agent-x").await.unwrap();
    assert_eq!(lane_x.child.resolution, ChildResolution::RootVerified);
    assert!(lane_x.coverage.is_none());
    // Within a wider bound: applied, identity and status updated together with the coverage.
    let roomy = ChildStreamBounds {
        max_unresolved_lanes_per_session: 2,
        ..ChildStreamBounds::default()
    };
    assert_eq!(
        repo.set_coverage(&session(), "boot_b", &flipped, &cov, &roomy)
            .await
            .unwrap(),
        LaneMutation::Applied
    );
    let lane_x = lane(&store, "n:agent-x").await.unwrap();
    assert_eq!(lane_x.child.resolution, ChildResolution::Unresolved);
    assert_eq!(lane_x.epoch, "boot_b");
    assert_eq!(lane_x.coverage.as_ref().unwrap()["generation"], "g1");
    // The flipped lane now occupies the budget: a further new unresolved lane is refused.
    assert_eq!(
        append(&store, &unresolved("ses_root", 2), 5, &roomy)
            .await
            .unwrap(),
        AppendOutcome::Refused(RefusedBy::UnresolvedLanes)
    );
}

#[tokio::test]
async fn concurrent_appends_are_admitted_serially_and_return_their_own_ids() {
    let store = Arc::new(open_store().await);
    let bounds = ChildStreamBounds {
        max_unresolved_lanes_per_session: 3,
        ..ChildStreamBounds::default()
    };
    let mut tasks = Vec::new();
    for n in 0..8 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            append(&store, &unresolved("ses_root", n), n, &bounds)
                .await
                .unwrap()
        }));
    }
    let mut stored_ids = Vec::new();
    let mut refused = 0;
    for task in tasks {
        match task.await.unwrap() {
            AppendOutcome::Stored(id) => stored_ids.push(id),
            AppendOutcome::Refused(RefusedBy::UnresolvedLanes) => refused += 1,
            other => panic!("unexpected outcome {other:?}"),
        }
    }
    assert_eq!(stored_ids.len(), 3);
    assert_eq!(refused, 5);
    stored_ids.sort_unstable();
    stored_ids.dedup();
    assert_eq!(
        stored_ids.len(),
        3,
        "returned ids are owned, never another insert's"
    );
    let rows = ChildStreamEvents::new(&store)
        .page(&session(), &LaneFilter::default(), 0, 10)
        .await
        .unwrap()
        .rows;
    let mut row_ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    row_ids.sort_unstable();
    assert_eq!(row_ids, stored_ids);
    assert_eq!(sentinel_refusals(&store).await, 5);
}

#[tokio::test]
async fn a_failing_eviction_never_grows_retained_state_and_is_counted_once_on_recovery() {
    let store = open_store().await;
    let bounds = ChildStreamBounds {
        max_rows_per_session: 1,
        ..ChildStreamBounds::default()
    };
    let a = verified("ses_root", "agent-a");
    let first = match append(&store, &a, 1, &bounds).await.unwrap() {
        AppendOutcome::Stored(id) => id,
        other => panic!("{other:?}"),
    };
    store
        .stream_conn()
        .execute(
            "CREATE TRIGGER mem.block_child_delete BEFORE DELETE ON child_stream_events \
             BEGIN SELECT RAISE(ABORT, 'delete blocked'); END",
            (),
        )
        .await
        .unwrap();
    // Repeated appends each need room; the eviction fails; nothing is inserted, nothing counted.
    assert!(append(&store, &a, 2, &bounds).await.is_err());
    assert!(append(&store, &a, 3, &bounds).await.is_err());
    assert_eq!(
        rows_of(&store, "n:agent-a").await,
        vec!["claude:agent-a#0#1"]
    );
    let lane_a = lane(&store, "n:agent-a").await.unwrap();
    assert_eq!(lane_a.live_rows, 1);
    assert_eq!(
        lane_a.evicted_rows, 0,
        "a loss that did not happen is never counted"
    );
    assert_eq!(lane_a.evicted_through, 0);
    // Recovery: the next append evicts exactly one row and counts it exactly once.
    store
        .stream_conn()
        .execute("DROP TRIGGER mem.block_child_delete", ())
        .await
        .unwrap();
    assert!(matches!(
        append(&store, &a, 4, &bounds).await.unwrap(),
        AppendOutcome::Stored(_)
    ));
    assert_eq!(
        rows_of(&store, "n:agent-a").await,
        vec!["claude:agent-a#0#4"]
    );
    let lane_a = lane(&store, "n:agent-a").await.unwrap();
    assert_eq!(lane_a.live_rows, 1);
    assert_eq!(lane_a.evicted_rows, 1);
    assert_eq!(lane_a.evicted_through, first);
    assert!(lane_a.evicted_bytes > 0);
}

#[tokio::test]
async fn a_failing_lane_record_write_leaves_no_orphan_event() {
    let store = open_store().await;
    let bounds = ChildStreamBounds::default();
    store
        .stream_conn()
        .execute(
            "CREATE TRIGGER mem.block_orphan_lane BEFORE INSERT ON child_stream_lanes \
             WHEN NEW.child_key = 'n:agent-orphan' \
             BEGIN SELECT RAISE(ABORT, 'lane blocked'); END",
            (),
        )
        .await
        .unwrap();
    let orphan = verified("ses_root", "agent-orphan");
    assert!(append(&store, &orphan, 1, &bounds).await.is_err());
    assert!(rows_of(&store, "n:agent-orphan").await.is_empty());
    assert!(lane(&store, "n:agent-orphan").await.is_none());
    store
        .stream_conn()
        .execute("DROP TRIGGER mem.block_orphan_lane", ())
        .await
        .unwrap();
    assert!(matches!(
        append(&store, &orphan, 2, &bounds).await.unwrap(),
        AppendOutcome::Stored(_)
    ));
    assert_eq!(
        rows_of(&store, "n:agent-orphan").await,
        vec!["claude:agent-orphan#0#2"]
    );
    let lane_o = lane(&store, "n:agent-orphan").await.unwrap();
    assert_eq!(lane_o.live_rows, 1);
    assert!(lane_o.first_seen_id > 0);
}
