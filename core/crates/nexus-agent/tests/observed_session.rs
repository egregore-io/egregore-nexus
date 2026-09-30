//! Actual Agent service with inert adapters: captured open/entry ownership, not native execution.
use async_trait::async_trait;
use nexus_agent::adapter::{AcpModelMetadataDialect, AdapterModelReportingProfile};
use nexus_agent::service::OpenedSession;
use nexus_agent::{Adapter, AdapterInjectError, AdapterRegistry, Agent, StreamEvent};
use nexus_common::NexusError;
use nexus_contracts::model_report::{
    ModelObservationSink, ModelProfileIdentity, NativeModelUpdate,
};
use nexus_contracts::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore};

struct Identity(bool);
#[async_trait]
impl IdentityPort for Identity {
    async fn register(&self, _: RegisterRequest) -> Result<RegisterResponse, ContractError> {
        unimplemented!()
    }
    async fn whoami(&self, _: &Caller) -> Result<Whoami, ContractError> {
        unimplemented!()
    }
    async fn resolve(&self, _: &str, _: &str) -> Result<Caller, ContractError> {
        if !self.0 {
            return Err(ContractError {
                code: codes::NOT_FOUND,
                message: "absent".into(),
            });
        }
        Ok(Caller {
            agent_id: None,
            session: sid(),
            name: "agent".into(),
            project: "default".into(),
            tier: Tier::Agent,
            locality: Default::default(),
            access: None,
            principal_id: None,
        })
    }
    async fn members(
        &self,
        _: &Caller,
        _: MemberListRequest,
    ) -> Result<MemberListResponse, ContractError> {
        unimplemented!()
    }
    async fn status(&self, _: &Caller, _: StatusRequest) -> Result<StatusResponse, ContractError> {
        unimplemented!()
    }
    async fn heartbeat(&self, _: &Caller) -> Result<HeartbeatResponse, ContractError> {
        unimplemented!()
    }
    async fn assign_project(
        &self,
        _: &str,
        _: &str,
    ) -> Result<AssignProjectResponse, ContractError> {
        unimplemented!()
    }
}

struct Sink {
    identity: ModelProfileIdentity,
    closed: AtomicBool,
}
impl ModelObservationSink for Sink {
    fn accepts_profile(&self, p: &ModelProfileIdentity) -> bool {
        !self.closed.load(Ordering::SeqCst) && self.identity.matches(p)
    }
    fn bind_native_root(&self, _: &str) -> bool {
        !self.closed.load(Ordering::SeqCst)
    }
    fn observe(&self, _: NativeModelUpdate) -> bool {
        !self.closed.load(Ordering::SeqCst)
    }
    fn revoke(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}
impl Sink {
    fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

struct Events {
    entered: Notify,
    release: Semaphore,
    park_spawn: AtomicBool,
    events: Mutex<Vec<WsEvent>>,
}
impl Default for Events {
    fn default() -> Self {
        Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
            park_spawn: AtomicBool::new(false),
            events: Mutex::new(Vec::new()),
        }
    }
}
#[async_trait]
impl EventSink for Events {
    async fn emit(&self, event: WsEvent) {
        if matches!(event, WsEvent::AgentSpawned { .. }) && self.park_spawn.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        self.events.lock().unwrap().push(event);
    }
}

struct Native {
    sink: Arc<Sink>,
    entered: Notify,
    release: Semaphore,
    park: AtomicBool,
    fail: AtomicBool,
    resume_fail: AtomicBool,
    opens: AtomicUsize,
    resumes: AtomicUsize,
    news: AtomicUsize,
    kills: AtomicUsize,
    root: Mutex<Option<String>>,
}
#[async_trait]
impl Adapter for Native {
    async fn open_session(&self) -> Result<(), NexusError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        if self.park.load(Ordering::SeqCst) {
            self.release.acquire().await.unwrap().forget();
        }
        if self.fail.load(Ordering::SeqCst) {
            Err(NexusError::Adapter("native open cause".into()))
        } else {
            Ok(())
        }
    }
    async fn resume(&self, _: &str) -> Result<(), NexusError> {
        self.resumes.fetch_add(1, Ordering::SeqCst);
        if self.resume_fail.load(Ordering::SeqCst) {
            Err(NexusError::Adapter("load cause".into()))
        } else {
            Ok(())
        }
    }
    async fn new_session_only(&self) -> Result<(), NexusError> {
        self.news.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn inject(&self, _: String) -> Result<(), AdapterInjectError> {
        Ok(())
    }
    async fn stream_updates(&self) -> Result<Vec<StreamEvent>, NexusError> {
        Ok(vec![])
    }
    async fn acp_session_id(&self) -> Option<String> {
        self.root.lock().unwrap().clone()
    }
    fn runtime_process_ids(&self) -> Option<nexus_common::RuntimeProcessIds> {
        Some(nexus_common::RuntimeProcessIds {
            os_pid: 123,
            os_pgid: 456,
        })
    }
    async fn kill(&self) {
        assert!(self.sink.closed(), "observer closes before native kill");
        self.kills.fetch_add(1, Ordering::SeqCst);
    }
}

struct Fixture {
    agent: Agent,
    sink: Arc<Sink>,
    native: Arc<Native>,
    events: Arc<Events>,
    calls: Arc<AtomicUsize>,
    injected: Arc<AtomicBool>,
}
fn sid() -> SessionId {
    SessionId("s_observed".into())
}
fn kind() -> HarnessId {
    HarnessId::new("observed-test").unwrap()
}
fn profile() -> AdapterModelReportingProfile {
    AdapterModelReportingProfile::new(
        ModelReportBackend::new("inert-test").unwrap(),
        ModelEvidenceCapability::Supported,
        ModelEvidenceCapability::Unsupported,
        ModelEvidenceCapability::Unsupported,
        AcpModelMetadataDialect::ConfigOptions {
            source: ModelObservationSource::new("inert-config").unwrap(),
        },
    )
    .unwrap()
}
fn fixture(resolves: bool, panic_factory: bool) -> Fixture {
    let profile = profile();
    let sink = Arc::new(Sink {
        identity: profile.identity().clone(),
        closed: AtomicBool::new(false),
    });
    let native = Arc::new(Native {
        sink: sink.clone(),
        entered: Notify::new(),
        release: Semaphore::new(0),
        park: AtomicBool::new(false),
        fail: AtomicBool::new(false),
        resume_fail: AtomicBool::new(false),
        opens: AtomicUsize::new(0),
        resumes: AtomicUsize::new(0),
        news: AtomicUsize::new(0),
        kills: AtomicUsize::new(0),
        root: Mutex::new(Some("actual-native-root".into())),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let injected = Arc::new(AtomicBool::new(false));
    let mut registry = AdapterRegistry::new();
    let (n, c, i) = (native.clone(), calls.clone(), injected.clone());
    registry.register_observed(
        &kind(),
        Arc::new(move |ctx| {
            c.fetch_add(1, Ordering::SeqCst);
            assert!(!panic_factory, "factory cause");
            i.store(ctx.model_reporting.is_some(), Ordering::SeqCst);
            n.clone()
        }),
        profile,
    );
    let events = Arc::new(Events::default());
    Fixture {
        agent: Agent::new(registry, Arc::new(Identity(resolves)), events.clone()),
        sink,
        native,
        events,
        calls,
        injected,
    }
}
async fn open(f: &Fixture, resume: Option<&str>) -> Result<OpenedSession, ContractError> {
    f.agent
        .open_session_for_observed(
            sid(),
            "agent",
            "default",
            f.agent.prepare_adapter(&kind()).unwrap(),
            None,
            vec![],
            resume,
            f.sink.clone(),
        )
        .await
}
fn spawn_open(f: &Fixture) -> tokio::task::JoinHandle<Result<OpenedSession, ContractError>> {
    let agent = f.agent.clone();
    let sink = f.sink.clone();
    tokio::spawn(async move {
        agent
            .open_session_for_observed(
                sid(),
                "agent",
                "default",
                agent.prepare_adapter(&kind()).unwrap(),
                None,
                vec![],
                None,
                sink,
            )
            .await
    })
}
async fn entered(n: &Notify) {
    tokio::time::timeout(std::time::Duration::from_secs(2), n.notified())
        .await
        .expect("actual operation entered gate");
}

#[test]
fn dropping_never_polled_open_revokes_before_factory() {
    let f = fixture(false, false);
    drop(f.agent.open_session_for_observed(
        sid(),
        "agent",
        "default",
        f.agent.prepare_adapter(&kind()).unwrap(),
        None,
        vec![],
        None,
        f.sink.clone(),
    ));
    assert!(f.sink.closed());
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert!(!f.agent.has_session(&sid()));
}

#[tokio::test]
async fn cancellation_during_open_closes_captured_observer_without_binding() {
    let f = fixture(false, false);
    f.native.park.store(true, Ordering::SeqCst);
    let task = spawn_open(&f);
    entered(&f.native.entered).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(f.sink.closed());
    assert!(!f.agent.has_session(&sid()));
    assert_eq!(f.native.kills.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn success_carries_actual_root_and_replacement_invalidates_exact_receipt() {
    let f = fixture(false, false);
    let receipt = open(&f, None).await.unwrap();
    assert!(f.injected.load(Ordering::SeqCst));
    assert_eq!(receipt.native_root(), Some("actual-native-root"));
    assert_eq!(
        receipt.process_ids(),
        Some(nexus_common::RuntimeProcessIds {
            os_pid: 123,
            os_pgid: 456
        })
    );
    assert!(!f.sink.closed());
    assert_eq!(f.agent.with_current_binding(&receipt, || 42), Some(42));
    f.agent
        .bind_session(sid(), "replacement", "default", f.native.clone(), false);
    assert!(f.sink.closed());
    assert_eq!(f.agent.with_current_binding(&receipt, || 42), None);
    assert!(f.agent.is_live(&sid()));
}

#[tokio::test]
async fn failed_open_retains_errored_entry_but_revokes_observer() {
    let f = fixture(false, false);
    f.native.fail.store(true, Ordering::SeqCst);
    let error = match open(&f, None).await {
        Err(e) => e,
        Ok(_) => panic!("expected failure"),
    };
    assert!(error.message.contains("native open cause"));
    assert!(f.sink.closed());
    assert!(f.agent.has_session(&sid()));
    assert!(!f.agent.is_live(&sid()));
    assert!(matches!(
        f.events.events.lock().unwrap().as_slice(),
        [WsEvent::AgentStatus {
            presence: Presence::Offline,
            paused: false,
            ..
        }]
    ));
}

#[tokio::test]
async fn factory_panic_revokes_pending_observer() {
    let f = fixture(false, true);
    let error = match spawn_open(&f).await {
        Err(e) => e,
        Ok(_) => panic!("factory must panic"),
    };
    assert!(error.is_panic());
    assert!(f.sink.closed());
    assert!(!f.agent.has_session(&sid()));
}

#[tokio::test]
async fn foreign_profile_rejects_before_factory_and_revokes_only_supplied_sink() {
    let f = fixture(false, false);
    let foreign = Arc::new(Sink {
        identity: profile().identity().clone(),
        closed: AtomicBool::new(false),
    });
    let result = f
        .agent
        .open_session_for_observed(
            sid(),
            "agent",
            "default",
            f.agent.prepare_adapter(&kind()).unwrap(),
            None,
            vec![],
            None,
            foreign.clone(),
        )
        .await;
    assert!(result.is_err());
    assert!(foreign.closed());
    assert!(!f.sink.closed());
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn resume_fallback_keeps_captured_adapter_and_actual_new_root() {
    let f = fixture(false, false);
    f.native.resume_fail.store(true, Ordering::SeqCst);
    let receipt = open(&f, Some("not-the-actual-root")).await.unwrap();
    assert_eq!(receipt.native_root(), Some("actual-native-root"));
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.native.resumes.load(Ordering::SeqCst), 1);
    assert_eq!(f.native.news.load(Ordering::SeqCst), 1);
    assert_eq!(f.native.opens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn detach_closes_before_optional_kill() {
    for kill in [false, true] {
        let f = fixture(false, false);
        let receipt = open(&f, None).await.unwrap();
        assert!(f.agent.detach_session(&sid(), kill).await);
        assert!(f.sink.closed());
        assert_eq!(f.agent.with_current_binding(&receipt, || ()), None);
        assert_eq!(f.native.kills.load(Ordering::SeqCst), usize::from(kill));
        assert!(!f.agent.detach_session(&sid(), kill).await);
    }
}

#[tokio::test]
async fn both_remove_selectors_close_before_kill() {
    for resolves in [false, true] {
        let f = fixture(resolves, false);
        open(&f, None).await.unwrap();
        let result = f
            .agent
            .remove(RemoveRequest {
                agent_id: None,
                name: "agent".into(),
                kill: true,
            })
            .await
            .unwrap();
        assert_eq!(result.status, "removed");
        assert!(f.sink.closed());
        assert_eq!(f.native.kills.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancellation_at_final_event_closes_observer_but_retains_bound_delivery_effect() {
    let f = fixture(false, false);
    f.events.park_spawn.store(true, Ordering::SeqCst);
    let task = spawn_open(&f);
    entered(&f.events.entered).await;
    assert!(f.agent.is_live(&sid()));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(f.sink.closed());
    assert!(f.agent.is_live(&sid()));
    assert_eq!(f.native.kills.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn old_final_event_completion_cannot_certify_replaced_entry() {
    let f = fixture(false, false);
    f.events.park_spawn.store(true, Ordering::SeqCst);
    let task = spawn_open(&f);
    entered(&f.events.entered).await;
    f.agent
        .bind_session(sid(), "replacement", "default", f.native.clone(), false);
    f.events.release.add_permits(1);
    let receipt = task.await.unwrap().unwrap();
    assert!(f.sink.closed());
    assert_eq!(f.agent.with_current_binding(&receipt, || ()), None);
}

#[tokio::test]
async fn cancel_old_after_observed_replacement_preserves_new_sink_and_binding() {
    let old = fixture(false, false);
    old.events.park_spawn.store(true, Ordering::SeqCst);
    let task = spawn_open(&old);
    entered(&old.events.entered).await;
    // The OLD operation has already bound, and is parked only in its final publication.
    old.events.park_spawn.store(false, Ordering::SeqCst);
    let new = fixture(false, false);
    let receipt = old
        .agent
        .open_session_for_observed(
            sid(),
            "agent",
            "default",
            new.agent.prepare_adapter(&kind()).unwrap(),
            None,
            vec![],
            None,
            new.sink.clone(),
        )
        .await
        .unwrap();
    assert!(old.sink.closed());
    assert!(!new.sink.closed());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!new.sink.closed());
    assert_eq!(old.agent.with_current_binding(&receipt, || 1), Some(1));
    assert!(old.agent.detach_session(&sid(), true).await);
    assert!(new.sink.closed());
    assert_eq!(new.native.kills.load(Ordering::SeqCst), 1);
    assert_eq!(old.native.kills.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn absent_native_root_is_not_synthesized_from_resume_or_name() {
    let f = fixture(false, false);
    *f.native.root.lock().unwrap() = None;
    let receipt = open(&f, Some("suggested-resume-root")).await.unwrap();
    assert_eq!(receipt.native_root(), None);
    assert!(!f.sink.closed());
    // This receipt does not authorize model activation: its later caller requires an actual root.
    assert_eq!(f.agent.with_current_binding(&receipt, || ()), Some(()));
}

#[tokio::test]
async fn legacy_open_does_not_inject_or_close_unrelated_reporting() {
    let f = fixture(false, false);
    let ledger = f
        .agent
        .open_session_for(sid(), "agent", "default", kind(), None, vec![], None)
        .await
        .unwrap();
    assert_eq!(
        ledger,
        Some(nexus_common::RuntimeProcessIds {
            os_pid: 123,
            os_pgid: 456
        })
    );
    assert!(!f.injected.load(Ordering::SeqCst));
    assert!(!f.sink.closed());
    assert!(f.agent.detach_session(&sid(), false).await);
    assert!(!f.sink.closed());
}
