# Fast Acknowledgment for Busy Agents Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Messages sent to an agent that is mid-turn are acknowledged immediately in the agent's voice and are delivered when the turn ends, instead of being dead-lettered and silently lost.

**Architecture:** Two changes. First, an unconditional repair: `delivery_action` stops returning an error for the default `Interrupt` timing against a busy uninterruptible backend, and instead waits for the turn boundary — which makes it infallible and removes an error path from the event loop. Second, an advisory fast-acknowledgment path behind a new `FastAckPort`, called fire-and-forget from the event loop and implemented in the `nexus` composition crate, which alone may depend on the harness crates.

**Tech Stack:** Rust 2021, tokio, async-trait, libsql-backed `nexus-store`, existing `nexus-dispatch` event loop and `nexus-contracts` port traits.

## Global Constraints

- All `cargo` commands run from the `core/` directory (the workspace root is `core/Cargo.toml`).
- `nexus-dispatch` MUST NOT depend on any harness crate. `crates/nexus/src/harness_registry.rs` documents that the `nexus` composition crate is the only layer allowed to depend on every harness crate. Anything needing a harness goes behind a port trait.
- The fast-acknowledgment path is advisory: it is spawned and never awaited by the delivery path. A panic, error, or hang inside it MUST NOT delay, block, or fail delivery.
- Acknowledgments are emitted ONLY for batches containing a message from a human sender. This is the structural cascade guard; do not replace it with loop detection.
- The acknowledgment path MUST NOT spawn a process or call a model. Only the optional answer path may spawn a one-shot harness command.
- The feature is always on. Do not add a config flag, launch flag, or environment toggle.
- One-shot harness timeout is exactly `10` seconds. The child MUST be killed and reaped on elapse.
- The answer-classification rule is exactly: the batch qualifies as a question only when it contains exactly one human message whose last non-whitespace character is `?`. Every other shape acknowledges.
- Recent history passed to the answer path is capped at the most recent `10` messages of that conversation.
- Run `cargo fmt --all` before every commit. Commits must leave `cargo clippy -p <crate> --tests` clean for the crates touched.

---

### Task 1: Make delivery timing infallible and stop dead-lettering busy agents

This is the correctness repair. It is independently valuable: on its own it fixes messages vanishing.

**Files:**
- Modify: `core/crates/nexus-dispatch/src/delivery_timing.rs:17-41`
- Modify: `core/crates/nexus-dispatch/src/lib.rs:27`
- Modify: `core/crates/nexus-dispatch/src/event_loop.rs:37` (import), `:189-235` (first call site), `:466-495` (second call site), `:519-533` (delete helper)
- Test: `core/crates/nexus-dispatch/tests/delivery_timing.rs`
- Test: `core/crates/nexus-dispatch/tests/loop.rs:1041` (existing test encodes the bug; it is rewritten)

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `delivery_action(timing: DeliveryTiming, turn_active: bool, capability: SteerCapability) -> DeliveryAction` — infallible. `DeliveryTimingError` no longer exists.

- [ ] **Step 1: Rewrite the timing tests to expect the repaired matrix**

Replace the whole of `core/crates/nexus-dispatch/tests/delivery_timing.rs` with:

```rust
use nexus_contracts::{DeliveryTiming, SteerCapability};
use nexus_dispatch::{delivery_action, DeliveryAction};

#[test]
fn idle_targets_start_normally_for_every_public_timing() {
    for timing in [
        DeliveryTiming::Interrupt,
        DeliveryTiming::YieldTurn,
        DeliveryTiming::AfterToolLoop,
    ] {
        assert_eq!(
            delivery_action(timing, false, SteerCapability::None),
            DeliveryAction::StartTurn,
        );
    }
}

#[test]
fn interrupt_uses_native_steer_then_atomic_interrupt_and_send() {
    assert_eq!(
        delivery_action(
            DeliveryTiming::Interrupt,
            true,
            SteerCapability::NativeSteer,
        ),
        DeliveryAction::NativeSteer,
    );
    assert_eq!(
        delivery_action(
            DeliveryTiming::Interrupt,
            true,
            SteerCapability::InterruptAndSend,
        ),
        DeliveryAction::InterruptAndSend,
    );
}

/// The repair: a busy agent whose backend cannot be interrupted must WAIT for the turn
/// boundary. Returning an error here dead-lettered the message and the operator never
/// received a reply at all.
#[test]
fn interrupt_on_busy_uninterruptible_agent_waits_for_boundary() {
    assert_eq!(
        delivery_action(DeliveryTiming::Interrupt, true, SteerCapability::None),
        DeliveryAction::WaitForTurnBoundary,
    );
}

#[test]
fn yield_and_tool_loop_policies_wait_for_an_authoritative_boundary() {
    for capability in [
        SteerCapability::NativeSteer,
        SteerCapability::InterruptAndSend,
        SteerCapability::None,
    ] {
        assert_eq!(
            delivery_action(DeliveryTiming::YieldTurn, true, capability),
            DeliveryAction::WaitForTurnBoundary,
        );
        assert_eq!(
            delivery_action(DeliveryTiming::AfterToolLoop, true, capability),
            DeliveryAction::WaitForFinalTurnCompletion,
        );
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p nexus-dispatch --test delivery_timing`
Expected: FAIL to compile — `unresolved import nexus_dispatch::DeliveryTimingError` is gone from the test but `delivery_action` still returns `Result`, so `assert_eq!` comparisons against bare `DeliveryAction` mismatch types.

- [ ] **Step 3: Make `delivery_action` infallible**

In `core/crates/nexus-dispatch/src/delivery_timing.rs`, delete the `DeliveryTimingError` enum (lines 17-20) and replace the function with:

```rust
/// Resolve one public timing value against authoritative active-turn state.
///
/// Infallible. A busy target whose backend cannot be interrupted waits for the turn
/// boundary; it is never a delivery failure.
pub fn delivery_action(
    timing: DeliveryTiming,
    turn_active: bool,
    capability: SteerCapability,
) -> DeliveryAction {
    if !turn_active {
        return DeliveryAction::StartTurn;
    }

    match timing {
        DeliveryTiming::Interrupt => match capability {
            SteerCapability::NativeSteer => DeliveryAction::NativeSteer,
            SteerCapability::InterruptAndSend => DeliveryAction::InterruptAndSend,
            SteerCapability::None => DeliveryAction::WaitForTurnBoundary,
        },
        DeliveryTiming::YieldTurn => DeliveryAction::WaitForTurnBoundary,
        DeliveryTiming::AfterToolLoop => DeliveryAction::WaitForFinalTurnCompletion,
    }
}
```

- [ ] **Step 4: Update the crate export**

In `core/crates/nexus-dispatch/src/lib.rs` line 27, change:

```rust
pub use delivery_timing::{delivery_action, DeliveryAction, DeliveryTimingError};
```

to:

```rust
pub use delivery_timing::{delivery_action, DeliveryAction};
```

- [ ] **Step 5: Update the first event-loop call site**

In `core/crates/nexus-dispatch/src/event_loop.rs` line 37, change the import:

```rust
use crate::delivery_timing::{delivery_action, DeliveryAction, DeliveryTimingError};
```

to:

```rust
use crate::delivery_timing::{delivery_action, DeliveryAction};
```

Then replace the `match delivery_action(...)` block beginning at line 189 with the unwrapped form (note every `Ok(...)` pattern loses its wrapper and the whole `Err(error)` arm is deleted):

```rust
            match delivery_action(
                timing,
                turn_active,
                deps.turn_exec.steer_capability(&session),
            ) {
                DeliveryAction::WaitForTurnBoundary
                | DeliveryAction::WaitForFinalTurnCompletion => {
                    if let Err(error) = deps.turn_exec.wait_for_turn_completion(&session).await {
                        if !claim_batch(&session, &deps, &inbox, &batch).await {
                            break;
                        }
                        handle_inject_error(
                            &session,
                            &deps,
                            &inbox,
                            &batch,
                            InjectError::Contract(error),
                        )
                        .await;
                    }
                    continue;
                }
                DeliveryAction::NativeSteer | DeliveryAction::InterruptAndSend => {
                    if !claim_batch(&session, &deps, &inbox, &batch).await {
                        break;
                    }
                    match steer_claimed_batch(&session, &deps, &inbox, &batch).await {
                        SteerAttempt::Delivered | SteerAttempt::TurnEnded => continue,
                        SteerAttempt::Failed => break,
                    }
                }
                DeliveryAction::StartTurn => {}
            }
```

- [ ] **Step 6: Update the second event-loop call site**

In the same file, replace the `match delivery_action(timing, true, ...)` block beginning at line 466 with:

```rust
    match delivery_action(timing, true, deps.turn_exec.steer_capability(session)) {
        DeliveryAction::NativeSteer => {
            if claim_batch(session, deps, inbox, &batch).await {
                let _ = steer_claimed_batch(session, deps, inbox, &batch).await;
            }
            false
        }
        DeliveryAction::InterruptAndSend => {
            interrupt_and_redrive_notified_batch(session, deps, inbox, &batch).await
        }
        DeliveryAction::WaitForTurnBoundary | DeliveryAction::WaitForFinalTurnCompletion => {
            // The current turn future is still being polled by the caller. Leave these rows
            // notified; its terminal branch immediately re-drains them.
            false
        }
        DeliveryAction::StartTurn => unreachable!("the caller owns an active turn"),
    }
}
```

- [ ] **Step 7: Delete the now-dead error helper**

In the same file, delete the entire `delivery_timing_contract_error` function (lines 519-533 before your edits — search for `fn delivery_timing_contract_error`). It has no remaining callers.

If `DeliveryTiming` or `ContractError` become unused imports after this deletion, remove them from the import list. `cargo build` will name them.

- [ ] **Step 8: Rewrite the loop test that encodes the old behavior**

`core/crates/nexus-dispatch/tests/loop.rs` line 1041 contains `active_turn_interrupt_without_adapter_support_is_a_typed_terminal_error`, which asserts the message is settled `error` with `DELIVERY_TIMING_UNSUPPORTED`. That test encodes the bug as a contract. Rename it and invert its assertions:

```rust
#[tokio::test]
async fn active_turn_interrupt_without_adapter_support_waits_and_delivers_at_the_boundary() {
```

Keep the whole setup body unchanged up to and including the `EventLoop::spawn(...)` call. Replace everything after the spawn with:

```rust
    wait_for_delivery_state(&store, "m_interrupt_unsupported", "delivered").await;
    assert_eq!(
        turn_exec.injected.lock().unwrap().len(),
        1,
        "a busy uninterruptible agent must still receive the message at its turn boundary"
    );
    let mut rows = store
        .conn
        .query(
            "SELECT COUNT(*) FROM in_flight WHERE message_id = ?1 AND state = 'error'",
            ["m_interrupt_unsupported"],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        row.get::<i64>(0).unwrap(),
        0,
        "the message must never be dead-lettered"
    );
}
```

Note: the fake turn-exec this test uses must allow the turn to end so the boundary is reached. If it is a never-completing fake, switch its `turn_exec` to `BoundaryWaitTurnExec` (already defined at `loop.rs:283`), matching the pattern in the `yield`/`after_tool_loop` tests above it.

- [ ] **Step 9: Run the full dispatch suite**

Run: `cargo test -p nexus-dispatch`
Expected: PASS, all tests. If `loop.rs:1096`'s `DELIVERY_TIMING_UNSUPPORTED` reference still exists anywhere, it is inside the test you rewrote — delete that assertion.

- [ ] **Step 10: Verify nothing else referenced the removed symbols**

Run: `cargo build --workspace 2>&1 | grep -E "DeliveryTimingError|delivery_timing_contract_error" || echo CLEAN`
Expected: `CLEAN`

Then run: `cargo clippy -p nexus-dispatch --tests`
Expected: no warnings from `delivery_timing.rs` or `event_loop.rs`.

- [ ] **Step 11: Commit**

```bash
cd core && cargo fmt --all && cd ..
git add core/crates/nexus-dispatch/src/delivery_timing.rs \
        core/crates/nexus-dispatch/src/lib.rs \
        core/crates/nexus-dispatch/src/event_loop.rs \
        core/crates/nexus-dispatch/tests/delivery_timing.rs \
        core/crates/nexus-dispatch/tests/loop.rs
git commit -m "fix(dispatch): deliver to busy uninterruptible agents at the turn boundary

A message delivered to an agent mid-turn whose terminal backend cannot be
interrupted was dead-lettered: delivery_action returned
Err(InterruptUnsupported) for the default Interrupt timing and the event loop
settled it as a terminal failure. Operators saw an agent that silently ignored
them.

That arm now waits for the turn boundary, which the event loop already
implements. The error variant had exactly one constructor and one consumer, so
delivery_action becomes infallible and the loop loses an error path entirely.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01MeufKqAQ8z9xK1sQiubPUa"
```

---

### Task 2: Add the FastAckPort and call it from the event loop

**Files:**
- Modify: `core/crates/nexus-contracts/src/ports.rs` (append the trait)
- Modify: `core/crates/nexus-contracts/src/lib.rs` (export it, if `ports` items are re-exported there)
- Modify: `core/crates/nexus-dispatch/src/event_loop.rs` (`LoopDeps` struct + the `WaitForTurnBoundary` arm)
- Modify: all 23 `LoopDeps { .. }` construction sites across `core/crates/nexus-dispatch/src/event_loop.rs`, `core/crates/nexus-dispatch/tests/loop.rs`, `core/crates/nexus/src/daemon/services/loop_wiring.rs`, `core/crates/harness/codex/tests/app_server_turn_completion.rs`
- Test: `core/crates/nexus-dispatch/tests/loop.rs`

**Interfaces:**
- Consumes: `delivery_action` returning `DeliveryAction` (Task 1).
- Produces:
  - `nexus_contracts::FastAckPort` with `async fn fast_ack(&self, session: &SessionId, batch: &NexusBatch)`
  - `LoopDeps::fast_ack: Option<Arc<dyn FastAckPort>>`
  - `nexus_dispatch::batch_has_human_sender(batch: &NexusBatch) -> bool`

- [ ] **Step 1: Write the failing loop tests**

Append to `core/crates/nexus-dispatch/tests/loop.rs`:

```rust
/// Records every fast-ack invocation, and can be told to misbehave so we can prove that
/// misbehaviour cannot affect delivery.
struct RecordingFastAck {
    calls: Arc<Mutex<Vec<String>>>,
    behavior: FastAckBehavior,
}

#[derive(Clone, Copy)]
enum FastAckBehavior {
    Normal,
    Panic,
    Hang,
}

#[async_trait::async_trait]
impl nexus_contracts::FastAckPort for RecordingFastAck {
    async fn fast_ack(&self, session: &SessionId, _batch: &nexus_contracts::NexusBatch) {
        self.calls.lock().unwrap().push(session.0.clone());
        match self.behavior {
            FastAckBehavior::Normal => {}
            FastAckBehavior::Panic => panic!("fast ack exploded"),
            FastAckBehavior::Hang => std::future::pending::<()>().await,
        }
    }
}

#[test]
fn human_sender_guard_selects_only_batches_containing_human_messages() {
    let human = human_dm_batch("m_human", "hello");
    let agent = agent_dm_batch("m_agent", "hello");
    assert!(nexus_dispatch::batch_has_human_sender(&human));
    assert!(!nexus_dispatch::batch_has_human_sender(&agent));
}
```

Add these two batch builders next to the other helpers in the same file:

```rust
fn human_dm_batch(message_id: &str, body: &str) -> nexus_contracts::NexusBatch {
    dm_batch_with_kind(message_id, body, nexus_contracts::Kind::Human)
}

fn agent_dm_batch(message_id: &str, body: &str) -> nexus_contracts::NexusBatch {
    dm_batch_with_kind(message_id, body, nexus_contracts::Kind::Agent)
}

fn dm_batch_with_kind(
    message_id: &str,
    body: &str,
    kind: nexus_contracts::Kind,
) -> nexus_contracts::NexusBatch {
    let mut batch = nexus_contracts::NexusBatch::default();
    batch.message_ids.push(message_id.to_string());
    batch.counts.total = 1;
    batch.dms.push(nexus_contracts::BatchMessage {
        id: message_id.to_string(),
        from: "pcuser".to_string(),
        kind,
        scope: nexus_contracts::Scope::Dm,
        body: body.to_string(),
        ..Default::default()
    });
    batch
}
```

If `BatchMessage` or `NexusBatch` do not implement `Default`, construct them field-by-field instead — run `cargo build -p nexus-dispatch --tests` and let rustc list the required fields, then fill each with the literal shown above plus `None`/empty defaults for the rest.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p nexus-dispatch --test loop human_sender_guard`
Expected: FAIL to compile — `nexus_contracts::FastAckPort` and `nexus_dispatch::batch_has_human_sender` do not exist.

- [ ] **Step 3: Declare the port**

Append to `core/crates/nexus-contracts/src/ports.rs`:

```rust
/// Advisory immediate acknowledgment for a message that arrived while the agent is mid-turn.
///
/// Implementations MUST NOT block the caller and MUST NOT be able to fail delivery: the event
/// loop spawns this and never awaits it. Returning early, erroring, or panicking is safe.
#[async_trait::async_trait]
pub trait FastAckPort: Send + Sync {
    async fn fast_ack(&self, session: &SessionId, batch: &crate::NexusBatch);
}
```

If `core/crates/nexus-contracts/src/lib.rs` re-exports port items individually (check for an existing `pub use ports::{...}` line), add `FastAckPort` to it.

- [ ] **Step 4: Add the human-sender guard**

Append to `core/crates/nexus-dispatch/src/delivery_timing.rs`:

```rust
use nexus_contracts::{Kind, NexusBatch};

/// Whether a batch contains at least one message from a human sender.
///
/// This is the structural cascade guard for fast acknowledgment: an agent never acknowledges
/// another agent's message, so an acknowledgment can never trigger another one.
pub fn batch_has_human_sender(batch: &NexusBatch) -> bool {
    batch
        .dms
        .iter()
        .chain(batch.threads.iter())
        .any(|message| matches!(message.kind, Kind::Human))
}
```

Merge the `use nexus_contracts::...` line with the existing one at the top of the file rather than adding a second import statement.

Export it from `core/crates/nexus-dispatch/src/lib.rs` line 27:

```rust
pub use delivery_timing::{batch_has_human_sender, delivery_action, DeliveryAction};
```

- [ ] **Step 5: Add the field to LoopDeps**

In `core/crates/nexus-dispatch/src/event_loop.rs`, add to the `LoopDeps` struct after `provider_limit_default_cooldown`:

```rust
    /// Advisory fast-acknowledgment adapter. `None` disables the behavior entirely; the
    /// delivery path is identical either way.
    pub fast_ack: Option<Arc<dyn nexus_contracts::FastAckPort>>,
```

- [ ] **Step 6: Fill in every construction site**

Run: `cargo build --workspace --tests 2>&1 | grep "missing field" -A 2`

rustc names all 23 sites. Add `fast_ack: None,` to every one of them. Every site is a test or the daemon wiring; the daemon wiring gets its real value in Task 6.

Re-run until the build is clean.

- [ ] **Step 7: Spawn the fast ack from the event loop**

In `core/crates/nexus-dispatch/src/event_loop.rs`, in the `WaitForTurnBoundary | WaitForFinalTurnCompletion` arm you edited in Task 1, insert the spawn immediately before the `wait_for_turn_completion` call:

```rust
                DeliveryAction::WaitForTurnBoundary
                | DeliveryAction::WaitForFinalTurnCompletion => {
                    // Advisory only: spawned and never awaited, so a slow, failing, or
                    // panicking acknowledgment cannot delay or fail this delivery.
                    if let Some(fast_ack) = deps.fast_ack.clone() {
                        if crate::delivery_timing::batch_has_human_sender(&batch) {
                            let session = session.clone();
                            let batch = batch.clone();
                            tokio::spawn(async move {
                                fast_ack.fast_ack(&session, &batch).await;
                            });
                        }
                    }
                    if let Err(error) = deps.turn_exec.wait_for_turn_completion(&session).await {
```

Leave the rest of the arm exactly as Task 1 left it.

Note: the "one acknowledgment per busy turn" rule is NOT implemented here. It lives in the adapter (Task 4), which owns the pending-note map that makes deduplication natural. The loop's only guard is the human-sender check.

- [ ] **Step 8: Add the delivery-isolation tests**

Append to `core/crates/nexus-dispatch/tests/loop.rs`. Model the setup body on the existing `active_turn_interrupt_without_adapter_support_waits_and_delivers_at_the_boundary` test from Task 1 — same store, same `BoundaryWaitTurnExec`, same message insertion — changing only the `LoopDeps.fast_ack` value and the assertions:

```rust
#[tokio::test]
async fn fast_ack_failure_never_affects_delivery() {
    for behavior in [FastAckBehavior::Panic, FastAckBehavior::Hang] {
        // (setup identical to the boundary-delivery test, with:)
        //     fast_ack: Some(Arc::new(RecordingFastAck {
        //         calls: Arc::new(Mutex::new(Vec::new())),
        //         behavior,
        //     })),
        // then:
        wait_for_delivery_state(&store, "m_interrupt_unsupported", "delivered").await;
    }
}

#[tokio::test]
async fn idle_agent_gets_no_fast_ack() {
    // Setup with MockTurnExec (never busy) and:
    //     fast_ack: Some(Arc::new(RecordingFastAck { calls: calls.clone(), behavior: Normal }))
    wait_for_delivery_state(&store, "m_idle", "delivered").await;
    assert!(
        calls.lock().unwrap().is_empty(),
        "an idle agent is delivered to immediately and must not be acknowledged"
    );
}
```

- [ ] **Step 9: Run the suite**

Run: `cargo test -p nexus-dispatch`
Expected: PASS, including every pre-existing test (they now pass `fast_ack: None`).

- [ ] **Step 10: Commit**

```bash
cd core && cargo fmt --all && cd ..
git add core/crates/nexus-contracts core/crates/nexus-dispatch core/crates/nexus/src/daemon/services/loop_wiring.rs core/crates/harness/codex/tests/app_server_turn_completion.rs
git commit -m "feat(dispatch): add the advisory FastAckPort and call it from the event loop

The port is spawned and never awaited, so a failing or hanging acknowledgment
cannot affect delivery. Acknowledgments are gated on the batch containing a
human sender, which structurally prevents agent-to-agent acknowledgment
cascades. No adapter is wired yet; every construction site passes None and the
delivery path is unchanged.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01MeufKqAQ8z9xK1sQiubPUa"
```

---

### Task 3: Declare one-shot commands on the harness contract

**Files:**
- Modify: `core/crates/harness/core/src/lib.rs` (add the trait method)
- Modify: `core/crates/nexus/src/harness_registry.rs` (`OpenCodeHarness`)
- Modify: `core/crates/harness/claude/src/lib.rs` (`ClaudeHarness`)
- Modify: `core/crates/harness/codex/src/lib.rs` (`CodexHarness`)
- Test: `core/crates/harness/core/tests/oneshot_command.rs` (create)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `Harness::oneshot_command(&self, prompt: &str) -> Option<HeadedCommand>`, defaulting to `None`. `HeadedCommand { program: String, args: Vec<String> }` already exists in `nexus_harness_core`.

- [ ] **Step 1: Write the failing test**

Create `core/crates/harness/core/tests/oneshot_command.rs`:

```rust
//! One-shot invocation is declared per harness. Harnesses without one degrade to
//! acknowledgment-only fast replies; they must never be handed a fabricated command.

use nexus_harness_core::Harness;

#[test]
fn claude_opencode_codex_declare_oneshot_commands() {
    let claude = nexus_harness_claude::ClaudeHarness
        .oneshot_command("say hi")
        .expect("claude declares a one-shot mode");
    assert!(claude.args.contains(&"-p".to_string()));
    assert!(claude.args.contains(&"say hi".to_string()));

    let codex = nexus_harness_codex::CodexHarness
        .oneshot_command("say hi")
        .expect("codex declares a one-shot mode");
    assert!(codex.args.contains(&"exec".to_string()));
    assert!(codex.args.contains(&"say hi".to_string()));
}

#[test]
fn harnesses_without_oneshot_mode_default_to_none() {
    // GenericHarness is the fallback contract used by `pi` and `other`; it must inherit the
    // default so the fast-ack adapter falls back to acknowledgment instead of spawning.
    let generic = nexus_harness_core::GenericHarness::new("pi", "");
    assert!(generic.oneshot_command("say hi").is_none());
}
```

Add `nexus-harness-claude` and `nexus-harness-codex` to `[dev-dependencies]` in `core/crates/harness/core/Cargo.toml` if they are not already there. If that introduces a dependency cycle (harness-core is depended on BY those crates), move both `#[test]` functions into `core/crates/nexus/tests/unit/harness_oneshot.rs` instead, wired in the same way other files under `core/crates/nexus/tests/unit/` are included via `#[path]`, and keep only the `GenericHarness` test in harness-core.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p nexus-harness-core --test oneshot_command`
Expected: FAIL to compile — no method named `oneshot_command`.

- [ ] **Step 3: Add the trait method with a safe default**

In `core/crates/harness/core/src/lib.rs`, inside the `Harness` trait (beside `attach_backend` and `resume_style`):

```rust
    /// One-shot, non-interactive invocation for short auxiliary prompts.
    ///
    /// `None` means this harness has no one-shot mode. Callers MUST degrade to a
    /// deterministic acknowledgment rather than inventing a command line.
    fn oneshot_command(&self, _prompt: &str) -> Option<HeadedCommand> {
        None
    }
```

- [ ] **Step 4: Implement it for the three harnesses that have one**

In `core/crates/harness/claude/src/lib.rs`, inside `impl Harness for ClaudeHarness`:

```rust
    fn oneshot_command(&self, prompt: &str) -> Option<HeadedCommand> {
        Some(HeadedCommand {
            program: self.program().to_string(),
            args: vec![
                "-p".to_string(),
                "--model".to_string(),
                "haiku".to_string(),
                prompt.to_string(),
            ],
        })
    }
```

In `core/crates/nexus/src/harness_registry.rs`, inside `impl HarnessContract for OpenCodeHarness`:

```rust
    fn oneshot_command(&self, prompt: &str) -> Option<HeadedCommand> {
        Some(HeadedCommand {
            program: self.program().to_string(),
            args: vec!["run".to_string(), prompt.to_string()],
        })
    }
```

In `core/crates/harness/codex/src/lib.rs`, inside `impl Harness for CodexHarness`:

```rust
    fn oneshot_command(&self, prompt: &str) -> Option<HeadedCommand> {
        Some(HeadedCommand {
            program: self.program().to_string(),
            args: vec!["exec".to_string(), prompt.to_string()],
        })
    }
```

Import `HeadedCommand` in each file if it is not already imported.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p nexus-harness-core --test oneshot_command && cargo test -p egregore-nexus --lib harness`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd core && cargo fmt --all && cd ..
git add core/crates/harness core/crates/nexus/src/harness_registry.rs
git commit -m "feat(harness): declare one-shot invocation per harness contract

Claude, OpenCode, and Codex expose a non-interactive one-shot mode used for
short auxiliary prompts. The trait method defaults to None so every other
harness degrades to acknowledgment-only rather than a fabricated command line.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01MeufKqAQ8z9xK1sQiubPUa"
```

---

### Task 4: Implement the triage adapter

**Files:**
- Create: `core/crates/nexus/src/daemon/fast_ack.rs`
- Modify: `core/crates/nexus/src/daemon/mod.rs` (add `pub mod fast_ack;`)
- Test: `core/crates/nexus/tests/unit/fast_ack.rs` (create, included via `#[path]` from `fast_ack.rs`)

**Interfaces:**
- Consumes: `nexus_contracts::FastAckPort` (Task 2), `Harness::oneshot_command` (Task 3).
- Produces:
  - `pub struct FastAckAdapter` implementing `FastAckPort`
  - `pub fn ack_text(current_work: Option<&str>) -> String`
  - `pub fn batch_question(batch: &NexusBatch) -> Option<&str>`
  - `pub struct AutoReplyNotes` with `take(&self, session: &SessionId) -> Option<String>` and `has_pending(&self, session: &SessionId) -> bool`

- [ ] **Step 1: Write the failing tests**

Create `core/crates/nexus/tests/unit/fast_ack.rs`:

```rust
use super::*;

#[test]
fn ack_text_uses_current_work_when_present() {
    let text = ack_text(Some("the login refactor"));
    assert!(text.contains("the login refactor"), "{text}");
    assert!(text.contains("next"), "{text}");
}

#[test]
fn ack_text_is_generic_when_current_work_is_absent() {
    for absent in [None, Some(""), Some("   ")] {
        let text = ack_text(absent);
        assert!(text.contains("mid-task"), "{text}");
        // Must never fabricate a task name.
        assert!(!text.contains("None"), "{text}");
    }
}

#[test]
fn only_a_single_human_message_ending_in_question_mark_is_treated_as_a_question() {
    assert_eq!(
        batch_question(&single_human_dm("are you online?")),
        Some("are you online?")
    );
    // Trailing whitespace after the question mark still qualifies.
    assert_eq!(
        batch_question(&single_human_dm("are you online?  \n")),
        Some("are you online?  \n")
    );
    // A task statement does not.
    assert_eq!(batch_question(&single_human_dm("refactor the login page")), None);
    // More than one message does not, even if the last ends in '?'.
    assert_eq!(batch_question(&two_human_dms("do this", "ok?")), None);
}

#[test]
fn auto_reply_note_is_stapled_into_the_next_turn_and_consumed_once() {
    let notes = AutoReplyNotes::default();
    let session = SessionId("s_note".to_string());
    notes.put(&session, "Got it — I'll start this next.".to_string());

    assert!(notes.has_pending(&session));
    let taken = notes.take(&session).expect("the note is available once");
    assert!(taken.contains("Got it"));
    assert_eq!(notes.take(&session), None, "the note is consumed");
    assert!(!notes.has_pending(&session));
}
```

Add the two batch builders at the bottom of the same test file, constructing `NexusBatch` exactly as Task 2's `dm_batch_with_kind` helper does (repeat that construction here rather than importing it — the two test trees are separate):

```rust
fn single_human_dm(body: &str) -> NexusBatch {
    // same construction as Task 2's dm_batch_with_kind, Kind::Human, one dm
    todo_construct_batch(&[body])
}

fn two_human_dms(first: &str, second: &str) -> NexusBatch {
    todo_construct_batch(&[first, second])
}
```

Replace `todo_construct_batch` with a real local helper that pushes one `BatchMessage { kind: Kind::Human, scope: Scope::Dm, body, .. }` per input into `batch.dms` and sets `batch.counts.total` to the number of inputs. Run `cargo build -p egregore-nexus --tests` and let rustc list any required fields you have not set.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p egregore-nexus --lib fast_ack`
Expected: FAIL to compile — module `fast_ack` does not exist.

- [ ] **Step 3: Create the adapter module**

Create `core/crates/nexus/src/daemon/fast_ack.rs`:

```rust
//! Advisory immediate acknowledgment for messages that arrive while an agent is mid-turn.
//!
//! The acknowledgment itself is deterministic: it is formatted from the agent's declared
//! `current_work` and spawns no process. Only the optional answer path runs a harness's
//! one-shot command, and only for a batch that is exactly one human question.
//!
//! Everything here is advisory. The event loop spawns it and never awaits it, so any failure
//! degrades to a plain acknowledgment or to silence — never to a delivery failure.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexus_contracts::ids::SessionId;
use nexus_contracts::{Kind, NexusBatch};

/// Hard bound on a one-shot harness invocation. The child is killed and reaped on elapse.
const ONESHOT_TIMEOUT: Duration = Duration::from_secs(10);

/// Most recent messages of a conversation handed to the answer path.
const HISTORY_LIMIT: usize = 10;

/// Pending auto-reply notes, keyed by session, consumed by the next injected turn.
///
/// This map doubles as the "one acknowledgment per busy turn" guard: while a note is pending
/// for a session, no further acknowledgment is emitted for it. The real delivery consumes the
/// note, which re-arms acknowledgment for the next busy period.
///
/// Deliberately in-memory. A daemon restart drops pending notes, which at worst makes an agent
/// phrase a reply as though it had not already acknowledged. That is redundant, never
/// incorrect, and avoids a store migration for a cosmetic benefit.
#[derive(Default)]
pub struct AutoReplyNotes {
    notes: Mutex<HashMap<String, String>>,
}

impl AutoReplyNotes {
    pub fn put(&self, session: &SessionId, note: String) {
        self.notes.lock().unwrap().insert(session.0.clone(), note);
    }

    pub fn has_pending(&self, session: &SessionId) -> bool {
        self.notes.lock().unwrap().contains_key(&session.0)
    }

    /// Remove and return the pending note, if any.
    pub fn take(&self, session: &SessionId) -> Option<String> {
        self.notes.lock().unwrap().remove(&session.0)
    }
}

/// The acknowledgment sent while the agent is busy. Never fabricates a task name.
pub fn ack_text(current_work: Option<&str>) -> String {
    match current_work.map(str::trim).filter(|work| !work.is_empty()) {
        Some(work) => format!("Got it — I'm on {work}; I'll pick this up next."),
        None => "Got it — I'm mid-task; I'll pick this up next.".to_string(),
    }
}

/// The batch's question text, when the batch is exactly one human message ending in `?`.
///
/// Deliberately crude and conservative: a wrong auto-answer in the agent's voice is worse
/// than a correct late one, so anything ambiguous acknowledges instead.
pub fn batch_question(batch: &NexusBatch) -> Option<&str> {
    let mut human = batch
        .dms
        .iter()
        .chain(batch.threads.iter())
        .filter(|message| matches!(message.kind, Kind::Human));
    let only = human.next()?;
    if human.next().is_some() {
        return None;
    }
    if batch.counts.total != 1 {
        return None;
    }
    only.body
        .trim_end()
        .ends_with('?')
        .then_some(only.body.as_str())
}

#[cfg(test)]
#[path = "../../tests/unit/fast_ack.rs"]
mod tests;
```

Register the module: add `pub mod fast_ack;` to `core/crates/nexus/src/daemon/mod.rs` beside the other `pub mod` lines.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p egregore-nexus --lib fast_ack`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit the pure logic**

```bash
cd core && cargo fmt --all && cd ..
git add core/crates/nexus/src/daemon/fast_ack.rs core/crates/nexus/src/daemon/mod.rs core/crates/nexus/tests/unit/fast_ack.rs
git commit -m "feat(fast-ack): add acknowledgment text, question rule, and note store

The acknowledgment is formatted from the agent's declared current_work and
never fabricates a task name. The question rule is deliberately crude: exactly
one human message ending in '?'. The note map doubles as the one-per-busy-turn
guard, since the real delivery consumes the note.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01MeufKqAQ8z9xK1sQiubPUa"
```

- [ ] **Step 6: Write the failing one-shot timeout test**

Append to `core/crates/nexus/tests/unit/fast_ack.rs`:

```rust
#[tokio::test]
async fn oneshot_timeout_kills_the_process_and_falls_back_to_ack() {
    let command = nexus_harness_core::HeadedCommand {
        program: "sh".to_string(),
        args: vec!["-c".to_string(), "sleep 60".to_string()],
    };
    let started = std::time::Instant::now();
    let answer = run_oneshot(&command, Duration::from_millis(200)).await;
    assert_eq!(answer, None, "a timed-out one-shot yields no answer");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the one-shot must be bounded, took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn oneshot_garbage_output_falls_back_to_ack() {
    let command = nexus_harness_core::HeadedCommand {
        program: "sh".to_string(),
        args: vec!["-c".to_string(), "exit 3".to_string()],
    };
    assert_eq!(run_oneshot(&command, Duration::from_secs(5)).await, None);

    let empty = nexus_harness_core::HeadedCommand {
        program: "sh".to_string(),
        args: vec!["-c".to_string(), "printf ''".to_string()],
    };
    assert_eq!(run_oneshot(&empty, Duration::from_secs(5)).await, None);
}
```

These two tests use `sh`, so gate the module or the tests with `#[cfg(unix)]` to keep the Windows CI job green.

- [ ] **Step 7: Run to verify it fails**

Run: `cargo test -p egregore-nexus --lib fast_ack::tests::oneshot`
Expected: FAIL to compile — `run_oneshot` does not exist.

- [ ] **Step 8: Implement the bounded one-shot runner**

Append to `core/crates/nexus/src/daemon/fast_ack.rs`:

```rust
/// Run a harness one-shot command under a hard timeout, returning its trimmed stdout.
///
/// Returns `None` on timeout, spawn failure, non-zero exit, or empty output. The child is
/// killed and reaped on timeout so no process outlives the call.
pub async fn run_oneshot(
    command: &nexus_harness_core::HeadedCommand,
    timeout: Duration,
) -> Option<String> {
    let mut child = tokio::process::Command::new(&command.program)
        .args(&command.args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;

    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => output,
        // Elapsed or wait failure: `kill_on_drop` reaps the child as `child` is dropped here.
        _ => return None,
    };
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}
```

Note: `wait_with_output` consumes `child`, so on the timeout arm the future is dropped and `kill_on_drop(true)` performs the kill. Do not add a manual `child.kill()` after the move — it will not compile.

- [ ] **Step 9: Run the tests**

Run: `cargo test -p egregore-nexus --lib fast_ack`
Expected: PASS, 6 tests. Confirm the timeout test completes in well under 5 seconds.

- [ ] **Step 10: Commit**

```bash
cd core && cargo fmt --all && cd ..
git add core/crates/nexus/src/daemon/fast_ack.rs core/crates/nexus/tests/unit/fast_ack.rs
git commit -m "feat(fast-ack): add the bounded one-shot harness runner

Spawn failure, non-zero exit, empty output, and timeout all yield None so the
caller degrades to a plain acknowledgment. kill_on_drop reaps the child on
elapse so no one-shot process outlives the call.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01MeufKqAQ8z9xK1sQiubPUa"
```

---

### Task 5: Staple the auto-reply note into the next injected turn

**Files:**
- Modify: `core/crates/nexus-common/src/provenance.rs:57` (`render_injected_turn_for`)
- Modify: callers of `render_injected_turn_for` in `core/crates/nexus-dispatch/src/event_loop.rs`
- Test: `core/crates/nexus-common/tests/` (add to the existing provenance test file, or create `provenance_note.rs`)

**Interfaces:**
- Consumes: `AutoReplyNotes::take` (Task 4).
- Produces: `render_injected_turn_with_note(batch: &NexusBatch, receiver: &str, note: Option<&str>) -> String`. The existing `render_injected_turn_for(batch, receiver)` is retained as a thin wrapper passing `None`, so existing callers and tests are untouched.

- [ ] **Step 1: Write the failing test**

Create `core/crates/nexus-common/tests/provenance_note.rs`:

```rust
use nexus_common::{render_injected_turn_for, render_injected_turn_with_note};

mod support;

#[test]
fn a_note_is_prefixed_to_the_rendered_turn() {
    let batch = support::single_human_dm("refactor the login page");
    let rendered = render_injected_turn_with_note(
        &batch,
        "BE",
        Some("Got it — I'm on the login refactor; I'll pick this up next."),
    );
    assert!(
        rendered.starts_with("[auto-reply already sent as you: \""),
        "{rendered}"
    );
    assert!(rendered.contains("refactor the login page"), "{rendered}");
}

#[test]
fn no_note_renders_exactly_as_before() {
    let batch = support::single_human_dm("refactor the login page");
    assert_eq!(
        render_injected_turn_with_note(&batch, "BE", None),
        render_injected_turn_for(&batch, "BE"),
    );
}
```

Create `core/crates/nexus-common/tests/support/mod.rs` with a `single_human_dm` helper building a `NexusBatch` the same way as in Task 4 (one `BatchMessage`, `Kind::Human`, `Scope::Dm`, `counts.total = 1`).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p egregore-nexus-common --test provenance_note`
Expected: FAIL to compile — `render_injected_turn_with_note` not found.

- [ ] **Step 3: Implement the note-aware renderer**

In `core/crates/nexus-common/src/provenance.rs`, replace the body of `render_injected_turn_for` with a delegation and add the new function:

```rust
/// Render the text injected into a harness turn for one drained batch.
///
/// See [`render_injected_turn_with_note`]; this is the no-note form.
pub fn render_injected_turn_for(batch: &NexusBatch, receiver: &str) -> String {
    render_injected_turn_with_note(batch, receiver, None)
}

/// Render the injected turn, optionally prefixed with an acknowledgment already sent under
/// this agent's name while it was busy.
///
/// The prefix keeps the agent's single voice coherent: it continues from what was already
/// said instead of repeating or contradicting it.
pub fn render_injected_turn_with_note(
    batch: &NexusBatch,
    receiver: &str,
    note: Option<&str>,
) -> String {
    let body = render_injected_turn_body(batch, receiver);
    match note {
        Some(note) => format!("[auto-reply already sent as you: {note:?}]\n{body}"),
        None => body,
    }
}

fn render_injected_turn_body(batch: &NexusBatch, receiver: &str) -> String {
    // ... the ORIGINAL body of render_injected_turn_for, moved here verbatim ...
}
```

Move the original function body (the `single_plain_human_dm` check and the `render_batch_for` fallback) into `render_injected_turn_body` unchanged. Export `render_injected_turn_with_note` from `core/crates/nexus-common/src/lib.rs` alongside the existing `render_injected_turn_for` export.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p egregore-nexus-common`
Expected: PASS, including every existing provenance test (the no-note path is byte-identical).

- [ ] **Step 5: Commit**

```bash
cd core && cargo fmt --all && cd ..
git add core/crates/nexus-common
git commit -m "feat(provenance): allow an auto-reply note to prefix an injected turn

render_injected_turn_for keeps its exact behavior and delegates to a note-aware
form, so an agent that already acknowledged a message while busy sees what was
said under its name and continues coherently.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01MeufKqAQ8z9xK1sQiubPUa"
```

---

### Task 6: Wire the adapter into the daemon

The end-to-end task: the adapter reads `current_work`, posts as the agent, stores the note, and the event loop consumes the note on the next injection.

**Files:**
- Modify: `core/crates/nexus/src/daemon/fast_ack.rs` (add `FastAckAdapter`)
- Modify: `core/crates/nexus/src/daemon/services/loop_wiring.rs` (construct and pass it)
- Modify: `core/crates/nexus-dispatch/src/event_loop.rs` (consume the note at injection)
- Test: `core/crates/nexus/tests/unit/fast_ack.rs`

**Interfaces:**
- Consumes: `FastAckPort` (Task 2), `oneshot_command` (Task 3), `ack_text`/`batch_question`/`run_oneshot`/`AutoReplyNotes` (Task 4), `render_injected_turn_with_note` (Task 5).
- Produces: `FastAckAdapter::new(store, bus, notes, turn_exec) -> FastAckAdapter`.

- [ ] **Step 1: Add the note hand-off to LoopDeps and use it at injection**

In `core/crates/nexus-dispatch/src/event_loop.rs`, add to `LoopDeps` beside `fast_ack`:

```rust
    /// Pending auto-reply note supplier, consumed once per injected turn.
    pub auto_reply_note: Option<Arc<dyn nexus_contracts::AutoReplyNotePort>>,
```

Declare that port in `core/crates/nexus-contracts/src/ports.rs`:

```rust
/// Supplies the acknowledgment already sent under an agent's name while it was busy.
/// `take` MUST consume the note so it is injected exactly once.
pub trait AutoReplyNotePort: Send + Sync {
    fn take(&self, session: &SessionId) -> Option<String>;
}
```

Find the `render_injected_turn_for` call in `event_loop.rs` (it builds the text passed to `inject_turn`) and change it to:

```rust
            let note = deps
                .auto_reply_note
                .as_ref()
                .and_then(|notes| notes.take(&session));
            let rendered =
                nexus_common::render_injected_turn_with_note(&batch, &session.0, note.as_deref());
```

using `rendered` where the previous call's result was used. Add `auto_reply_note: None,` to all 23 `LoopDeps` construction sites — run `cargo build --workspace --tests` and let rustc name them.

- [ ] **Step 2: Implement `AutoReplyNotePort` for `AutoReplyNotes`**

Append to `core/crates/nexus/src/daemon/fast_ack.rs`:

```rust
impl nexus_contracts::AutoReplyNotePort for AutoReplyNotes {
    fn take(&self, session: &SessionId) -> Option<String> {
        AutoReplyNotes::take(self, session)
    }
}
```

- [ ] **Step 3: Write the failing adapter test**

Append to `core/crates/nexus/tests/unit/fast_ack.rs`:

```rust
#[tokio::test]
async fn a_pending_note_suppresses_a_second_acknowledgment() {
    let notes = AutoReplyNotes::default();
    let session = SessionId("s_dedup".to_string());

    assert!(should_acknowledge(&notes, &session), "first message acknowledges");
    notes.put(&session, "Got it — I'm mid-task; I'll pick this up next.".to_string());
    assert!(
        !should_acknowledge(&notes, &session),
        "a second message during the same busy turn must not acknowledge again"
    );

    // The real delivery consumes the note, which re-arms acknowledgment.
    let _ = notes.take(&session);
    assert!(should_acknowledge(&notes, &session));
}
```

- [ ] **Step 4: Run to verify it fails**

Run: `cargo test -p egregore-nexus --lib fast_ack::tests::a_pending_note`
Expected: FAIL to compile — `should_acknowledge` not found.

- [ ] **Step 5: Implement the guard and the adapter**

Append to `core/crates/nexus/src/daemon/fast_ack.rs`:

```rust
/// Whether an acknowledgment should be emitted now.
///
/// One acknowledgment per busy period: while a note is pending for this session the agent has
/// already acknowledged and has not yet been delivered to, so further messages stay silent.
/// They are all delivered together at the turn boundary regardless.
pub fn should_acknowledge(notes: &AutoReplyNotes, session: &SessionId) -> bool {
    !notes.has_pending(session)
}

/// Daemon adapter for [`nexus_contracts::FastAckPort`].
pub struct FastAckAdapter {
    store: Arc<nexus_store::Store>,
    bus: Arc<dyn nexus_contracts::BusPort>,
    notes: Arc<AutoReplyNotes>,
    turn_exec: Arc<dyn nexus_contracts::AgentTurnExecutionPort>,
}

impl FastAckAdapter {
    pub fn new(
        store: Arc<nexus_store::Store>,
        bus: Arc<dyn nexus_contracts::BusPort>,
        notes: Arc<AutoReplyNotes>,
        turn_exec: Arc<dyn nexus_contracts::AgentTurnExecutionPort>,
    ) -> Self {
        Self { store, bus, notes, turn_exec }
    }
}

#[async_trait::async_trait]
impl nexus_contracts::FastAckPort for FastAckAdapter {
    async fn fast_ack(&self, session: &SessionId, batch: &NexusBatch) {
        if !should_acknowledge(&self.notes, session) {
            return;
        }
        // Reserve immediately so a burst of arrivals cannot each pass the guard.
        self.notes.put(session, String::new());

        let Some(agent) = load_agent_row(&self.store, session).await else {
            let _ = self.notes.take(session);
            return;
        };

        let mut reply = ack_text(agent.current_work.as_deref());
        if let Some(question) = batch_question(batch) {
            if let Some(command) = harness_oneshot(&agent.harness, question) {
                if let Some(answer) = run_oneshot(&command, ONESHOT_TIMEOUT).await {
                    reply = answer;
                }
            }
        }

        // The turn may have ended while we were working. The real reply is imminent and
        // strictly better, so discard rather than post a stale acknowledgment.
        if !self
            .turn_exec
            .active_turn_sessions()
            .iter()
            .any(|active| active == session)
        {
            let _ = self.notes.take(session);
            return;
        }

        if post_as_agent(&self.bus, &agent, batch, &reply).await.is_ok() {
            self.notes.put(session, reply);
        } else {
            let _ = self.notes.take(session);
        }
    }
}
```

Implement the three helpers in the same file:

- `load_agent_row(store, session)` — read the agent's `name`, `agent_id`, `project`, `harness`, and `current_work` for this session from the store. Follow the existing query pattern in `core/crates/nexus/src/daemon/services/` for reading an agent row by session id.
- `harness_oneshot(harness_token, prompt) -> Option<HeadedCommand>` — resolve the harness through `crate::harness_registry` and call `oneshot_command(prompt)`. Return `None` for an unknown token.
- `post_as_agent(bus, agent, batch, reply)` — build a `Caller` for the agent (`agent_id`, `session`, `name`, `project`, `tier: Tier::Agent`, defaults for the rest) and a `SendRequest` whose `to` mirrors where the batch came from: `SendTarget::Post { thread }` when the triggering message carried a thread, else `SendTarget::Dm { name: Some(sender_name), agent_id: None }`. Set `body: reply.to_string()` and `metadata` to a map containing `"nexusAutoReply": true`. Call `bus.send(caller, request).await`. See `core/crates/nexus/src/daemon/routing.rs:874` for the exact call shape.

Note: `bus.send` alone does not wake other agents — the route handler does that separately. Do NOT add waking here; an acknowledgment must not drive other agents' loops.

- [ ] **Step 6: Run the unit tests**

Run: `cargo test -p egregore-nexus --lib fast_ack`
Expected: PASS, 7 tests.

- [ ] **Step 7: Construct the adapter in the daemon wiring**

In `core/crates/nexus/src/daemon/services/loop_wiring.rs`, create one shared `Arc<AutoReplyNotes>` for the daemon's lifetime (beside the other shared handles), and in the `LoopDeps { .. }` literal replace the placeholders from Steps 1 and 6 of Task 2:

```rust
            fast_ack: Some(Arc::new(crate::daemon::fast_ack::FastAckAdapter::new(
                store.clone(),
                bus.clone(),
                auto_reply_notes.clone(),
                turn_exec.clone(),
            ))),
            auto_reply_note: Some(auto_reply_notes.clone()),
```

Use the names already in scope at that construction site for `store`, `bus`, and `turn_exec`; if `bus` is not in scope there, thread it in from `AppState` the same way `store` is.

- [ ] **Step 8: Full verification**

```bash
cd core
cargo fmt --all -- --check
cargo clippy --workspace --tests 2>&1 | grep -E "^(error|warning)" -A 4 | grep -E "fast_ack|delivery_timing|event_loop|provenance" || echo CLEAN
cargo test -p nexus-dispatch -p egregore-nexus-common -p nexus-harness-core
cargo test -p egregore-nexus --lib
```

Expected: fmt clean, `CLEAN` from clippy, all suites PASS.

- [ ] **Step 9: Commit**

```bash
cd core && cargo fmt --all && cd ..
git add core/crates
git commit -m "feat(fast-ack): wire the acknowledgment adapter into the daemon

A message arriving for a busy agent is acknowledged immediately in that agent's
voice, formatted from its declared current_work, and answered instead when the
batch is exactly one human question and the harness declares a one-shot mode.
The pending note doubles as the one-per-busy-turn guard and is consumed by the
next injected turn so the agent continues coherently.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01MeufKqAQ8z9xK1sQiubPUa"
```

- [ ] **Step 10: Manual verification on WSL**

```bash
cd core && cargo build -p egregore-nexus && cd ..
nexus daemon stop && nexus daemon start
nexus launch claude --name BE --detach
# Give BE a long task, then immediately send a follow-up:
nexus dm BE -m "refactor the login page"
nexus dm BE -m "also update the README"
```

Expected:
1. An acknowledgment from BE arrives within seconds of the second message.
2. BE's real reply to the follow-up arrives after its current turn ends.
3. `nexus admin dlq list` reports `dead-letters: none` — the failure signature of the old behavior.
4. A third message sent during the same busy turn produces no second acknowledgment.

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: the delivery repair and the `DeliveryTimingError` removal → Task 1; `FastAckPort`, `LoopDeps`, the event-loop hook, and the human-sender cascade guard → Task 2; `oneshot_command` and the per-harness table → Task 3; acknowledgment text, the `?` classification rule, the 10 s bounded one-shot, and the note store → Task 4; note stapling into the next turn → Task 5; `current_work` lookup, posting as the agent, the turn-ended race check, the one-per-busy-turn guard, and daemon wiring → Task 6. Every spec test name appears in a task except the three loop tests `fast_ack_fires_only_for_human_senders`, `fast_ack_is_emitted_once_per_busy_turn`, and `fast_ack_is_discarded_when_the_turn_ends_first`; these are covered at the unit level instead — by `human_sender_guard_selects_only_batches_containing_human_messages` (Task 2), `a_pending_note_suppresses_a_second_acknowledgment` (Task 6), and the turn-ended branch of `fast_ack` (Task 6 Step 5) — because the loop-level fakes cannot observe the adapter's internal decisions.

**Known deviation from the spec.** The spec places the "one acknowledgment per in-flight turn" guard in the event-loop hook. The plan places it in the adapter, which owns the pending-note map that makes deduplication natural and re-arms it on consumption. Observable behavior is identical; the loop keeps only the human-sender guard.

**Type consistency.** `delivery_action` returns bare `DeliveryAction` from Task 1 onward and every later snippet matches on unwrapped variants. `AutoReplyNotes` exposes `put`/`has_pending`/`take` in Task 4 and every later use draws from that set. `HeadedCommand { program, args }` is used identically in Tasks 3, 4, and 6. `FastAckPort::fast_ack(&self, &SessionId, &NexusBatch)` is declared once in Task 2 and implemented with that exact signature in Tasks 2 (fake) and 6 (adapter).

**Residual risk carried from the spec.** The answer path can answer a trivial question incorrectly in the agent's voice. Mitigated by the deliberately crude `?` rule, the ban on performing work, and the note that lets the agent correct itself.
