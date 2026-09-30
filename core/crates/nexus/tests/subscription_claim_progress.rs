//! Disposable diagnostic: real split authority, external agents, command lanes and pull ACKs.
//! No providers, IPC socket, extra heartbeat, client retry loop, or production instrumentation.
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use nexus::cli::{commands::listen, store_client::StoreClient};
use nexus::daemon::{command_worker, gateway_stream_socket::GatewayStreamPublisher, AppState};
use nexus_agent::AdapterRegistry;
use nexus_common::Config;
use nexus_contracts::{
    Ack, CreateThreadRequest, Kind, RegisterRequest, SendRequest, SendTarget, Tier,
};
use nexus_store::{command_kinds, DaemonStore, Store};

#[derive(Default, Debug)]
struct Evidence {
    stages: BTreeMap<String, String>,
    completed: usize,
    snapshot: String,
}
type Shared = Arc<Mutex<Evidence>>;

fn stage(evidence: &Shared, actor: &str, value: &str) {
    evidence
        .lock()
        .unwrap()
        .stages
        .insert(actor.into(), value.into());
}

async fn bounded<T>(evidence: &Shared, label: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(12), future)
        .await
        .unwrap_or_else(|_| panic!("stage timeout {label}: {:?}", evidence.lock().unwrap()))
}

async fn snapshot(store: &Store) -> String {
    // Explicit allowlist: no request/result JSON, bodies, client keys or credentials.
    let mut rows = store.identity_conn().query(
        "SELECT kind,status,COUNT(*) FROM command_intents GROUP BY kind,status ORDER BY kind,status", (),
    ).await.unwrap();
    let mut facts = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        facts.push(format!(
            "{}:{}={}",
            row.get::<String>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<i64>(2).unwrap()
        ));
    }
    drop(rows);
    let mut rows = store.identity_conn().query(
        "SELECT command_id,kind,status,claimed_at,started_at,attempts FROM command_intents WHERE status IN ('pending','claimed') ORDER BY created_at LIMIT 16", (),
    ).await.unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        facts.push(format!(
            "id={} kind={} status={} claimed={:?} started={:?} attempts={}",
            row.get::<String>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<String>(2).unwrap(),
            row.get::<Option<i64>>(3).unwrap(),
            row.get::<Option<i64>>(4).unwrap(),
            row.get::<i64>(5).unwrap()
        ));
    }
    facts.join("; ")
}

fn registration(index: usize) -> RegisterRequest {
    RegisterRequest {
        agent_id: None,
        name: Some(format!("lg{:02}", index + 1)),
        harness: "other".parse().unwrap(),
        harness_session_id: format!("progress-native-{index}"),
        project: "load".into(),
        client_key: format!("ck_lg{:02}", index + 1),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(Kind::Agent),
        locality: Default::default(),
        access: None,
        role: None,
        cwd: None,
    }
}

async fn send(client: &StoreClient, target: SendTarget, body: &str) -> Ack {
    client
        .command(
            command_kinds::message_post::SEND,
            &SendRequest {
                to: target,
                summary: None,
                body: body.into(),
                mention: vec![],
                metadata: None,
                idempotency_key: None,
            },
        )
        .await
        .unwrap()
}

async fn scenario(evidence: Shared, directory: &std::path::Path) {
    stage(&evidence, "setup", "split-store");
    let daemon = DaemonStore::open(directory.join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    let store = Arc::new(daemon.compatibility_store());
    assert!(store.has_split_authority());
    let state = AppState::wire_with_registry_and_gateway_stream(
        store.clone(),
        &Config::default(),
        AdapterRegistry::new(),
        Some(GatewayStreamPublisher::new(4096)),
    );
    bounded(
        &evidence,
        "model-ready",
        state.wait_for_runtime_identity_ready(),
    )
    .await
    .unwrap();
    let monitor = tokio::spawn({
        let store = store.clone();
        let evidence = evidence.clone();
        async move {
            loop {
                let value = snapshot(&store).await;
                evidence.lock().unwrap().snapshot = value;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    });
    let mut clients = Vec::new();
    for index in 0..12 {
        stage(&evidence, "setup", &format!("register-{index}"));
        let request = registration(index);
        bounded(
            &evidence,
            "register",
            state.identity.register(request.clone()),
        )
        .await
        .unwrap();
        clients.push(
            StoreClient::from_store_with_identity_for_tests_resolving(store.clone(), request)
                .await
                .unwrap(),
        );
    }
    let worker = command_worker::spawn(state.clone());
    stage(&evidence, "setup", "thread-create");
    let created: Result<(), _> = bounded(
        &evidence,
        "thread-create",
        clients[0].command(
            command_kinds::thread::CREATE,
            &CreateThreadRequest {
                name: "load-c".into(),
                members: (1..=12).map(|index| format!("lg{index:02}")).collect(),
            },
        ),
    )
    .await;
    created.unwrap();
    stage(&evidence, "setup", "subscribe");
    let mut subscriptions = Vec::new();
    for client in &clients {
        subscriptions.push(
            bounded(
                &evidence,
                "subscribe",
                listen::subscribe(client, None, None),
            )
            .await
            .unwrap()
            .subscription_id,
        );
    }

    // Causal diagnostic control: both independent lanes can claim/start while presence is held,
    // but neither may enter its subscription handler until the same gate is released.
    stage(&evidence, "control", "seed-batch");
    bounded(
        &evidence,
        "control-send",
        send(&clients[1], SendTarget::dm_name("lg01"), "control"),
    )
    .await;
    let durable = bounded(
        &evidence,
        "control-next",
        listen::next_batch(&clients[0], &subscriptions[0], None),
    )
    .await
    .unwrap()
    .batch
    .unwrap();
    let presence = store.lock_presence_transition().await;
    let ack = tokio::spawn({
        let client = clients[0].clone();
        async move { listen::ack_subscription_batch(&client, &durable).await }
    });
    let next = tokio::spawn({
        let client = clients[1].clone();
        let subscription = subscriptions[1].clone();
        async move { listen::next_batch(&client, &subscription, None).await }
    });
    stage(
        &evidence,
        "control",
        "presence-held-waiting-for-two-started-claims",
    );
    bounded(&evidence, "held-presence-claims", async {
        loop {
            let mut rows = store.identity_conn().query(
                "SELECT COUNT(*) FROM command_intents WHERE status='claimed' AND started_at IS NOT NULL AND kind IN ('inbox.subscription_ack','inbox.subscription_next')", (),
            ).await.unwrap();
            if rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap() == 2 { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await;
    assert!(
        !ack.is_finished() && !next.is_finished(),
        "held presence must exclude both handlers"
    );
    evidence.lock().unwrap().snapshot = snapshot(&store).await;
    eprintln!("held-presence control: {:?}", evidence.lock().unwrap());
    drop(presence);
    bounded(&evidence, "released-ack", ack)
        .await
        .unwrap()
        .unwrap();
    bounded(&evidence, "released-next", next)
        .await
        .unwrap()
        .unwrap();
    stage(&evidence, "control", "released-both-complete");

    for round in 0..3 {
        // 25 of 36 sends are thread posts (69.4%), the remaining 11 are cyclic DMs.
        let post_count = if round == 2 { 9 } else { 8 };
        stage(&evidence, "round", &format!("{round}-send"));
        let mut sends = tokio::task::JoinSet::new();
        for index in 0..12 {
            let client = clients[index].clone();
            sends.spawn(async move {
                let target = if index < post_count {
                    SendTarget::Post {
                        thread: "load-c".into(),
                    }
                } else {
                    SendTarget::dm_name(format!("lg{:02}", (index + 1) % 12 + 1))
                };
                (
                    index,
                    send(&client, target, &format!("round-{round}-{index}")).await,
                )
            });
        }
        let admitted = bounded(&evidence, "round-sends", async {
            let mut admitted = BTreeMap::new();
            while let Some(result) = sends.join_next().await {
                let (index, ack) = result.unwrap();
                admitted.insert(index, ack.message_id.0);
            }
            admitted
        })
        .await;
        let mut receivers = tokio::task::JoinSet::new();
        for index in 0..12 {
            let client = clients[index].clone();
            let subscription = subscriptions[index].clone();
            let evidence = evidence.clone();
            let expected = admitted
                .iter()
                .filter(|(sender, _)| {
                    if **sender < post_count {
                        **sender != index
                    } else {
                        (**sender + 1) % 12 == index
                    }
                })
                .map(|(sender, message_id)| (message_id.clone(), format!("round-{round}-{sender}")))
                .collect::<BTreeMap<_, _>>();
            receivers.spawn(async move {
                let actor = format!("receiver-{index:02}");
                stage(&evidence, &actor, "next-submitted");
                let durable = listen::next_batch(&client, &subscription, None)
                    .await
                    .unwrap()
                    .batch
                    .expect("queued mixed batch");
                assert!(
                    !durable.batch.thread_message_ids.is_empty(),
                    "every actor must exercise ACK_THREADS"
                );
                assert_eq!(durable.batch.counts.total as usize, expected.len());
                let actual = durable
                    .batch
                    .dms
                    .iter()
                    .chain(&durable.batch.threads)
                    .map(|message| (message.id.0.clone(), message.body.clone()))
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(
                    actual, expected,
                    "exact admitted IDs/bodies must reach this recipient"
                );
                stage(&evidence, &actor, "next-result-split-ack-submitted");
                listen::split_ack(&client, &durable.batch).await.unwrap();
                stage(
                    &evidence,
                    &actor,
                    "split-ack-result-subscription-ack-submitted",
                );
                listen::ack_subscription_batch(&client, &durable)
                    .await
                    .unwrap();
                stage(&evidence, &actor, "subscription-ack-result");
                evidence.lock().unwrap().completed += 1;
            });
        }
        bounded(&evidence, "round-receivers", async {
            while let Some(result) = receivers.join_next().await {
                result.unwrap();
            }
        })
        .await;
    }
    let mut rows = store
        .conn
        .query("SELECT state,COUNT(*) FROM in_flight GROUP BY state", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "acked");
    assert_eq!(row.get::<i64>(1).unwrap(), 25 * 11 + 11 + 1);
    assert!(rows.next().await.unwrap().is_none());
    assert!(nexus_store::repos::DeliveryObligations::new(&store)
        .pending()
        .await
        .unwrap()
        .is_empty());
    evidence.lock().unwrap().snapshot = snapshot(&store).await;
    assert_eq!(evidence.lock().unwrap().completed, 36);
    let mut rows = store
        .identity_conn()
        .query(
            "SELECT COUNT(*) FROM command_intents WHERE kind='inbox.ack_threads' AND status='done'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        36
    );
    eprintln!("completed: {:?}", evidence.lock().unwrap());
    monitor.abort();
    worker.abort();
    assert!(
        monitor.await.is_err_and(|error| error.is_cancelled()),
        "owned monitor did not cancel cleanly"
    );
    assert!(
        worker.await.is_err_and(|error| error.is_cancelled()),
        "owned worker did not cancel cleanly"
    );
}

fn run_with_workers(workers: usize) {
    let evidence = Arc::new(Mutex::new(Evidence::default()));
    let (sender, receiver) = mpsc::sync_channel(1);
    let child_evidence = evidence.clone();
    std::thread::spawn(move || {
        // The directory outlives scenario unwinding AND runtime-owned AppState tasks.
        let directory = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(async {
                bounded(
                    &child_evidence,
                    "overall",
                    scenario(child_evidence.clone(), directory.path()),
                )
                .await
            });
        }));
        runtime.shutdown_timeout(Duration::from_secs(1));
        let cleanup = directory.close();
        let outcome = match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(_), Ok(())) => {
                Err("scenario panicked; explicit directory cleanup succeeded".to_string())
            }
            (Ok(()), Err(error)) => Err(format!(
                "explicit directory cleanup failed after runtime shutdown: {error}"
            )),
            (Err(_), Err(error)) => Err(format!(
                "scenario panicked; explicit directory cleanup also failed: {error}"
            )),
        };
        let _ = sender.send(outcome);
    });
    // Independent OS-thread watchdog still reports cached evidence if runtime workers block.
    // It does not terminate a blocked OS thread; this is an explicit diagnostic limitation.
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(30)),
        Ok(Ok(())),
        "workers={workers}: {:?}",
        evidence.lock().unwrap()
    );
}

#[test]
fn split_agent_subscription_progress_two_workers() {
    run_with_workers(2);
}

#[test]
fn split_agent_subscription_progress_four_workers() {
    run_with_workers(4);
}

async fn attached_stream_path(store: &Store) -> String {
    let mut rows = store
        .stream_conn()
        .query("PRAGMA database_list", ())
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        if row.get::<String>(1).unwrap() == "mem" {
            return row.get(2).unwrap();
        }
    }
    panic!("stream schema was not attached");
}

async fn stream_authority_control(mode: &str, directory: &std::path::Path) {
    use nexus_store::repos::agent_runtimes::{
        AgentRuntimes, NewAgentRuntime, SelectedRuntimeActivation,
    };
    use nexus_store::repos::agents::{Agents, NewAgent};
    use nexus_store::repos::delivery_obligations::{DeliveryObligations, NewDeliveryObligation};
    use std::task::Poll;

    let expected_path = directory.join("stream.db");
    if mode == "legacy" {
        let store = Store::open(directory.join("legacy.db").to_str().unwrap())
            .await
            .unwrap();
        store.migrate().await.unwrap();
        assert_eq!(
            attached_stream_path(&store).await,
            expected_path.to_str().unwrap()
        );
        eprintln!("stream-authority control=legacy named attachment preserved");
        return;
    }
    let daemon = DaemonStore::open(directory.join("identity.db").to_str().unwrap())
        .await
        .unwrap();
    let identity_attachment = attached_stream_path(daemon.identity()).await;
    let transport_attachment = attached_stream_path(daemon.transport()).await;
    assert_eq!(
        transport_attachment,
        if mode == "named" {
            expected_path.to_str().unwrap()
        } else {
            ""
        }
    );
    if mode == "anonymous" {
        assert!(identity_attachment.is_empty());
    }
    let store = daemon.compatibility_store();
    Agents::new(&store)
        .create(NewAgent {
            agent_id: "a_stream_control".into(),
            project: "load".into(),
            name: None,
            default_harness: Some("other".into()),
            role: None,
            tier: None,
            owner: None,
        })
        .await
        .unwrap();
    let runtimes = AgentRuntimes::new(&store);
    runtimes
        .create(NewAgentRuntime {
            runtime_id: "s_stream_control".into(),
            agent_id: "a_stream_control".into(),
            harness: "other".into(),
            cwd: None,
            transport: None,
            presence: None,
            active: false,
        })
        .await
        .unwrap();
    let obligations = DeliveryObligations::new(&store);
    obligations
        .insert(NewDeliveryObligation {
            message_id: "m_stream_control".into(),
            recipient_agent_id: "a_stream_control".into(),
            recipient_runtime_id: Some("s_stream_control".into()),
            payload_json: "{}".into(),
            dedupe_key: "stream-control".into(),
            attempt: 0,
            state: "pending".into(),
            created_at: 1,
        })
        .await
        .unwrap();
    assert_eq!(obligations.pending().await.unwrap().len(), 1);

    // Test-only: expose the production BEGIN retry's async yield instead of blocking a runtime
    // worker for each 5-second native busy wait. Production timeout/retry policy is unchanged.
    let mut pragma = daemon
        .identity()
        .conn
        .query("PRAGMA busy_timeout=0", ())
        .await
        .unwrap();
    while pragma.next().await.unwrap().is_some() {}
    drop(pragma);
    // This is exactly the transaction retained by acknowledge_pull_batch before it invokes the
    // durable settlement helper below; no fake mutex or synthetic replica stands in for storage.
    let transport_tx = store
        .begin_write_txn("test_pull_ack_transport_stage")
        .await
        .unwrap();
    let mut activation =
        Box::pin(runtimes.activate_selected("s_stream_control", "a_stream_control", &[]));
    let first_activation = futures::poll!(activation.as_mut());
    let activation_pending = first_activation.is_pending();
    let identity_held = daemon.identity().write_lock().try_lock().is_err();
    let messages = [nexus_contracts::MessageId("m_stream_control".into())];
    let mut settlement =
        Box::pin(obligations.settle_pull_batch_for_runtime(&messages, "s_stream_control"));
    let first_settlement = futures::poll!(settlement.as_mut());
    let settlement_pending = first_settlement.is_pending();
    eprintln!("stream-authority control={mode} identity_named={} activation_pending={activation_pending} identity_gate_held={identity_held} settlement_pending={settlement_pending}", !identity_attachment.is_empty());
    let mut work = Box::pin(async {
        let (activated, settled) = futures::join!(
            async {
                match first_activation {
                    Poll::Ready(result) => result,
                    Poll::Pending => activation.await,
                }
            },
            async {
                match first_settlement {
                    Poll::Ready(result) => result,
                    Poll::Pending => settlement.await,
                }
            },
        );
        assert!(matches!(
            activated.unwrap(),
            SelectedRuntimeActivation::Applied(_)
        ));
        assert_eq!(
            settled.unwrap(),
            1,
            "real recipient obligation must be retired"
        );
    });
    let blocked = tokio::time::timeout(Duration::from_millis(250), &mut work)
        .await
        .is_err();
    // Always release the exact owned transaction and drain both futures before the RED assertion.
    transport_tx.commit().await.unwrap();
    if blocked {
        tokio::time::timeout(Duration::from_secs(3), work)
            .await
            .expect("release control failed");
        eprintln!("stream-authority control={mode} releasing_transport_restored_activation_and_settlement=true");
    }
    assert!(obligations.pending().await.unwrap().is_empty());
    assert!(!blocked, "{mode}: transport transaction blocked activation and ACK settlement; activation_pending={activation_pending}, identity_gate_held={identity_held}, settlement_pending={settlement_pending}");
    assert!(
        identity_attachment.is_empty(),
        "split identity must not attach transport's named stream file"
    );
}

/// Borrows only the exact process just spawned by this fixture. Drop reaps on unwind; normal
/// polling errors/timeouts retain their original diagnostic and append both cleanup outcomes.
struct OwnedChildGuard<'a> {
    child: &'a mut std::process::Child,
    reaped: bool,
}

impl<'a> OwnedChildGuard<'a> {
    fn new(child: &'a mut std::process::Child) -> Self {
        Self {
            child,
            reaped: false,
        }
    }

    fn terminate_and_reap(&mut self) -> String {
        let kill = self.child.kill();
        // A child can exit between the final poll and kill. Always attempt wait, even if kill
        // reports an error; retain that error alongside the authoritative reap result.
        let wait = self.child.wait();
        self.reaped = wait.is_ok();
        format!("owned-child cleanup: kill={kill:?}; wait={wait:?}")
    }

    fn wait_until(
        &mut self,
        deadline: std::time::Instant,
    ) -> Result<std::process::ExitStatus, String> {
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.reaped = true;
                    return Ok(status);
                }
                Ok(None) => {}
                Err(error) => {
                    return Err(format!(
                        "child poll failed: {error}; {}",
                        self.terminate_and_reap()
                    ));
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!(
                    "child exceeded watchdog deadline; {}",
                    self.terminate_and_reap()
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for OwnedChildGuard<'_> {
    fn drop(&mut self) {
        if !self.reaped {
            eprintln!("{}", self.terminate_and_reap());
        }
    }
}

#[test]
fn named_stream_identity_transport_independent() {
    const CHILD_MODE: &str = "NEXUS_TEST_STREAM_AUTHORITY_CHILD";
    if let Ok(mode) = std::env::var(CHILD_MODE) {
        if mode == "cleanup-control" {
            loop {
                std::thread::park();
            }
        }
        let directory = std::env::var_os("NEXUS_TEST_STREAM_AUTHORITY_ROOT").unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(stream_authority_control(
                &mode,
                std::path::Path::new(&directory),
            ));
        }));
        runtime.shutdown_timeout(Duration::from_secs(1));
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
        return;
    }
    // Causal unwind control: the guard must terminate/reap its exact child before ownership
    // returns here. Reusing the already-reaped handle also covers the exited-before-kill case.
    let mut cleanup_child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "named_stream_identity_transport_independent"])
        .env(CHILD_MODE, "cleanup-control")
        .spawn()
        .unwrap();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = OwnedChildGuard::new(&mut cleanup_child);
        std::panic::resume_unwind(Box::new("owned-child cleanup control"));
    }));
    let observed = cleanup_child.try_wait();
    // Independently clean up BEFORE asserting the guard oracle, so a broken Drop cannot leak
    // the intentionally parked child. Keep the observation and every cleanup outcome visible.
    let kill = cleanup_child.kill();
    let wait = cleanup_child.wait();
    assert!(
        unwind.is_err() && observed.as_ref().is_ok_and(Option::is_some) && wait.is_ok(),
        "unwind control: observed={observed:?}; independent kill={kill:?}; wait={wait:?}"
    );
    let mut finished = OwnedChildGuard::new(&mut cleanup_child);
    let cleanup = finished.terminate_and_reap();
    assert!(
        finished.reaped,
        "finished child must still be reaped after kill attempt: {cleanup}"
    );
    eprintln!("finished-child control: {cleanup}");
    drop(finished);
    // Child-only environment: this test never changes process-global configuration seen by the
    // ordinary two/four-worker scenarios or by tests running concurrently in this executable.
    for mode in ["anonymous", "legacy", "named"] {
        let directory = tempfile::tempdir().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "named_stream_identity_transport_independent",
                "--nocapture",
            ])
            .env(CHILD_MODE, mode)
            .env("NEXUS_TEST_STREAM_AUTHORITY_ROOT", directory.path())
            .env_remove("NEXUS_STREAM_DB_PATH");
        if mode != "anonymous" {
            command.env("NEXUS_STREAM_DB_PATH", directory.path().join("stream.db"));
        }
        let mut child = command.spawn().unwrap();
        let status = {
            let mut child = OwnedChildGuard::new(&mut child);
            child.wait_until(std::time::Instant::now() + Duration::from_secs(15))
        };
        let cleanup = directory.close();
        assert!(status.as_ref().is_ok_and(|status| status.success()) && cleanup.is_ok(), "{mode}: isolated stream-authority child result={status:?}; directory cleanup={cleanup:?}");
    }
}
