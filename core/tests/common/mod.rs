//! Acceptance-suite harness: boot a **real in-test daemon** with the hermetic [`MockAdapter`]
//! registered, then drive its transport-agnostic dispatch registry directly.
//!
//! The daemon is wired with the *production* [`AppState::wire_with_registry`] path (so the
//! launch/register loop wiring is real), differing from production only in the adapter registry: a
//! [`MockAdapter`] records injected `<nexus-batch …>` turns and emits scripted replies, so a turn's
//! shape and the wake→inject path are assertable without a live model.
//!
//! These end-to-end scenarios anchor on the AionUi acceptance rows (spec §11), re-pointed at Nexus.

#![allow(dead_code)]

use std::sync::Arc;

use nexus::daemon::{dispatch, AppState};
use nexus_agent::{Adapter, AdapterRegistry, MockAdapter};
use nexus_common::Config;
use nexus_contracts::{
    Caller, ContractError, Harness, RegisterRequest, RemoveRequest, Request, RequestId, Response,
    SendRequest, SendTarget, SessionId, SpawnRequest, SpawnResponse, Tier, JSONRPC_VERSION,
};
use nexus_store::Store;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// A booted in-test daemon: the wired [`AppState`] plus the shared mock adapter.
pub struct TestDaemon {
    state: AppState,
    /// The mock adapter every launched agent drives (shared handle).
    mock: MockAdapter,
}

impl TestDaemon {
    /// Boot a daemon over `:memory:` with a `MockAdapter` registered for **both** `claude` and
    /// `codex` (so `launch <kind>` is hermetic for either).
    pub async fn start() -> Self {
        Self::start_with(MockAdapter::new()).await
    }

    /// Boot with a caller-supplied mock (e.g. one scripted to fail `open_session`, or pre-scripted
    /// reply chunks). The same mock instance backs every launched agent.
    pub async fn start_with(mock: MockAdapter) -> Self {
        let store = Arc::new(Store::open(":memory:").await.unwrap());
        store.migrate().await.unwrap();

        let mut registry = AdapterRegistry::new();
        let m1 = mock.clone();
        registry.register(
            Harness::Claude,
            Arc::new(move |_cwd| Arc::new(m1.clone()) as Arc<dyn Adapter>),
        );
        let m2 = mock.clone();
        registry.register(
            Harness::Codex,
            Arc::new(move |_cwd| Arc::new(m2.clone()) as Arc<dyn Adapter>),
        );

        let state = AppState::wire_with_registry(store, &Config::default(), registry);

        TestDaemon { state, mock }
    }

    /// The shared mock adapter (records injected prompts, holds the reply script).
    pub fn mock(&self) -> &MockAdapter {
        &self.mock
    }

    /// The wired state (for white-box assertions, e.g. listing members directly).
    pub fn state(&self) -> &AppState {
        &self.state
    }

    /// Call a registry method as an already-registered caller.
    /// Use [`TestDaemon::rpc_anon`] for `register`/`notify` (unauthenticated).
    pub async fn rpc_as<P: Serialize>(
        &self,
        name: &str,
        project: &str,
        method: &str,
        params: P,
    ) -> Rpc {
        let caller = self
            .state
            .identity
            .resolve(project, name)
            .await
            .unwrap_or_else(|e| panic!("failed to resolve caller {project}/{name}: {e}"));
        self.call_dispatch(Some(caller), method, serde_json::to_value(params).unwrap())
            .await
    }

    /// Call a unit-param authenticated method (`whoami`, `threads`, `topics`, `heartbeat`) as a
    /// resolved caller.
    pub async fn rpc_as_unit(&self, name: &str, project: &str, method: &str) -> Rpc {
        let caller = self
            .state
            .identity
            .resolve(project, name)
            .await
            .unwrap_or_else(|e| panic!("failed to resolve caller {project}/{name}: {e}"));
        self.call_dispatch(Some(caller), method, serde_json::Value::Null)
            .await
    }

    /// Call an **unauthenticated** method (`register`, `notify`).
    pub async fn rpc_anon<P: Serialize>(&self, method: &str, params: P) -> Rpc {
        self.call_dispatch(None, method, serde_json::to_value(params).unwrap())
            .await
    }

    /// Call a registry method as the synthetic local operator used by store-backed command intents.
    pub async fn rpc_local_operator<P: Serialize>(
        &self,
        project: &str,
        method: &str,
        params: P,
    ) -> Rpc {
        self.call_dispatch(
            Some(local_operator(project)),
            method,
            serde_json::to_value(params).unwrap(),
        )
        .await
    }

    /// Launch with the local operator identity that a shell-originated command intent carries.
    pub async fn local_operator_launch(
        &self,
        project: &str,
        req: SpawnRequest,
    ) -> Result<SpawnResponse, ContractError> {
        let rpc = self.rpc_local_operator(project, "launch", req).await;
        if let Some(e) = rpc.resp.error {
            return Err(ContractError {
                code: e.code,
                message: e.message,
            });
        }
        Ok(serde_json::from_value(rpc.resp.result.expect("launch result")).unwrap())
    }

    async fn call_dispatch(
        &self,
        caller: Option<Caller>,
        method: &str,
        params: serde_json::Value,
    ) -> Rpc {
        let req = Request {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id: Some(RequestId::Num(1)),
            method: method.to_string(),
            params: Some(params),
        };
        Rpc {
            resp: dispatch(&self.state, caller, req).await,
        }
    }
}

fn local_operator(project: &str) -> Caller {
    Caller {
        agent_id: None,
        session: SessionId("local-operator".to_string()),
        name: "operator".to_string(),
        project: project.to_string(),
        tier: Tier::Admin,
    }
}

/// A thin wrapper over a JSON-RPC [`Response`] with ergonomic accessors for the acceptance asserts.
pub struct Rpc {
    pub resp: Response,
}

impl Rpc {
    /// Decode the success result into `T`, panicking with the error if the call failed.
    pub fn result<T: DeserializeOwned>(self) -> T {
        if let Some(e) = self.resp.error {
            panic!("rpc error: [{}] {}", e.code, e.message);
        }
        serde_json::from_value(self.resp.result.expect("a result")).unwrap()
    }

    /// Assert the call succeeded (any result), returning self for chaining.
    pub fn ok(self) -> Self {
        assert!(
            self.resp.error.is_none(),
            "expected ok, got error: {:?}",
            self.resp.error
        );
        self
    }

    /// The error code if the call failed (panics if it succeeded).
    pub fn error_code(self) -> i32 {
        self.resp.error.expect("expected an error").code
    }

    /// True if the call returned an error.
    pub fn is_error(&self) -> bool {
        self.resp.error.is_some()
    }
}

// ---- request builders shared across scenarios ----

/// A `RegisterRequest` for an agent named `name` with idempotency key `ck` in `project`.
pub fn reg(name: &str, ck: &str, project: &str) -> RegisterRequest {
    RegisterRequest {
        name: Some(name.into()),
        agent_id: None,
        harness: Harness::Claude,
        harness_session_id: format!("hs_{name}"),
        project: project.into(),
        client_key: ck.into(),
        runtime_credential: None,
        tier: Tier::Agent,
        kind: Some(nexus_contracts::Kind::Agent),
        role: None,
        cwd: None,
    }
}

/// A `RegisterRequest` for an **admin**-tier caller (for the whitelist scenario).
pub fn reg_admin(name: &str, ck: &str, project: &str) -> RegisterRequest {
    RegisterRequest {
        tier: Tier::Admin,
        ..reg(name, ck, project)
    }
}

/// A `SpawnRequest` to launch an agent of `kind` named `name` in `project`.
pub fn spawn(kind: Harness, name: &str, project: &str) -> SpawnRequest {
    SpawnRequest {
        kind,
        name: Some(name.into()),
        identity_policy: None,
        cwd: None,
        project: Some(project.into()),
        role: None,
        initial_prompt: None,
        resume: None,
        headless: false,
        harness_args: Vec::new(),
        backend: None,
    }
}

/// A `SpawnRequest` with **no project** — mirrors the real CLI `nexus launch <kind> --name <n>`
/// invocation (no `--project`). The daemon must register the launched agent in the **caller's**
/// project, not an empty/default one. Used by the reproduction test for the launch→project bug.
pub fn spawn_no_project(kind: Harness, name: &str) -> SpawnRequest {
    SpawnRequest {
        kind,
        name: Some(name.into()),
        identity_policy: None,
        cwd: None,
        project: None,
        role: None,
        initial_prompt: None,
        resume: None,
        headless: false,
        harness_args: Vec::new(),
        backend: None,
    }
}

/// A `SpawnRequest` with no explicit name, matching `nexus launch codex`.
pub fn spawn_no_name(kind: Harness, project: &str) -> SpawnRequest {
    SpawnRequest {
        kind,
        name: None,
        identity_policy: None,
        cwd: None,
        project: Some(project.into()),
        role: None,
        initial_prompt: None,
        resume: None,
        headless: false,
        harness_args: Vec::new(),
        backend: None,
    }
}

/// A `RemoveRequest` for `name`.
pub fn remove(name: &str) -> RemoveRequest {
    RemoveRequest {
        agent_id: None,
        name: name.into(),
        kill: false,
    }
}

/// A DM `SendRequest` to `name` carrying `body`.
pub fn dm(name: &str, body: &str) -> SendRequest {
    SendRequest {
        to: SendTarget::dm_name(name),
        summary: None,
        body: body.into(),
        mention: vec![],
        idempotency_key: None,
    }
}

/// Poll `f` until it returns `true` or `tries * step_ms` elapses. Returns whether it became true.
/// Used to await the asynchronous wake→inject (the loop runs on its own task).
pub async fn wait_until<F: FnMut() -> bool>(mut f: F, tries: u32, step_ms: u64) -> bool {
    for _ in 0..tries {
        if f() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(step_ms)).await;
    }
    f()
}
