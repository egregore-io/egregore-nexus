# Fast Acknowledgment for Busy Agents

## Goal

A message sent to an agent that is mid-turn is acknowledged immediately instead
of waiting for the agent's current turn to end, and is never lost.

Two independent defects produce today's behavior:

1. A message delivered to a busy agent whose terminal backend cannot be
   interrupted is **dead-lettered**. `delivery_action` returns
   `Err(DeliveryTimingError::InterruptUnsupported)` for the default
   `DeliveryTiming::Interrupt` while a turn is active, and the event loop routes
   that error to `handle_inject_error`. The message is settled as a terminal
   failure and never reaches the agent. Operators observe this as an agent that
   silently ignores them.
2. Even once delivery is repaired, the sender receives no signal until the
   agent's in-flight turn completes, which can take minutes.

Fixing (1) is a correctness repair and is unconditional. Fixing (2) adds an
advisory fast-acknowledgment path.

## User-visible behavior

- Messages sent to a busy agent are delivered when its current turn ends. They
  are no longer dead-lettered.
- While an agent is busy, a message from a **human** sender receives an
  immediate reply in that agent's own voice, for example
  *"Got it — I'm on the login refactor; I'll pick this up next."*
- If the message is a simple question and the agent's harness declares a
  one-shot mode, the immediate reply answers the question instead of only
  acknowledging it.
- The acknowledgment never performs the requested work. Tasks are always
  executed by the real agent when it reaches the message.
- When the real agent reaches the message, its injected turn is prefixed with
  the acknowledgment that was already sent under its name, so it continues
  coherently rather than repeating or contradicting it.
- The behavior is always on. It requires no launch flag, no configuration, and
  no change to any existing command.

## Non-goals

- The acknowledgment never starts, performs, or commits to work.
- Agent-to-agent traffic is never acknowledged.
- No mechanism is added for an agent to spawn its own subagents.
- Answer quality is not guaranteed. The path is biased toward acknowledging.

## Compatibility

- `nexus launch <kind>`, attach, resume, and every send verb are unchanged.
- An idle agent's delivery path is byte-for-byte unchanged.
- Existing harnesses require no changes. The new harness trait method defaults
  to `None`.
- `LoopDeps` gains an optional port. Existing dispatch tests construct it with
  `None` and are unaffected.

## Architecture

`nexus-dispatch` must not depend on harness crates;
`crates/nexus/src/harness_registry.rs` documents that the `nexus` composition
crate is the only layer permitted to depend on every harness crate. The
fast-acknowledgment path therefore follows the existing
`AgentTurnExecutionPort` pattern: a port trait declared in contracts, called
from dispatch, implemented in the composition root.

```
message for agent arrives
        |
        v
  agent mid-turn?  --no-->  deliver now (unchanged)
        | yes
        v
  batch stays pending, loop awaits the turn boundary
        |
        +--> fast-ack path (spawned, non-blocking):
        |      gather current_work, recent history, the batch
        |      triage: task or complex -> ACK; simple question -> ANSWER
        |      post the reply to the bus as the agent
        |      record the reply as a note for the agent's next turn
        v
  turn boundary reached
        v
  deliver the original message, prefixed with the recorded note
        v
  the real agent performs the task
```

The fast-ack path is spawned and never awaited by the delivery path. A panic,
error, or hang inside it cannot delay, block, or fail delivery.

## Components

### 1. Busy-delivery repair

`crates/nexus-dispatch/src/delivery_timing.rs`

`delivery_action(Interrupt, turn_active = true, SteerCapability::None)` returns
`Ok(DeliveryAction::WaitForTurnBoundary)` instead of
`Err(DeliveryTimingError::InterruptUnsupported)`. The event loop already
implements `WaitForTurnBoundary`: it awaits `wait_for_turn_completion` and
re-drains, delivering the batch on the next pass.

That arm is the only constructor of `DeliveryTimingError::InterruptUnsupported`,
and `delivery_timing_contract_error` in `event_loop.rs` is its only consumer.
Both are therefore removed, `DeliveryTimingError` is deleted, and
`delivery_action` becomes infallible, returning `DeliveryAction` directly. The
event loop's `Err(error)` arm and its `handle_inject_error` call are removed
with it. Timing selection can no longer fail.

### 2. Fast-acknowledgment port

`crates/nexus-contracts/`

```rust
#[async_trait]
pub trait FastAckPort: Send + Sync {
    /// Advisory. Implementations must not block and must never fail delivery.
    async fn fast_ack(&self, session: &SessionId, batch: &NexusBatch);
}
```

`LoopDeps` gains `pub fast_ack: Option<Arc<dyn FastAckPort>>`.

### 3. Event-loop hook

`crates/nexus-dispatch/src/event_loop.rs`

In the `WaitForTurnBoundary` branch, before awaiting turn completion, the loop
spawns `fast_ack` through `tokio::spawn` when the port is present, then
continues to await the boundary exactly as it does today.

Guards applied before spawning:

- the batch contains at least one message from a human sender;
- no acknowledgment has already been emitted for this session's current
  in-flight turn.

### 4. Triage implementation

`crates/nexus/src/daemon/fast_ack.rs` (new)

1. Read `current_work` for the agent from the store.
2. Build the acknowledgment text deterministically from `current_work`. This
   path invokes no model and spawns no process.
3. Classify the message with a deterministic rule: the batch qualifies as a
   question only when it contains exactly one human message whose last
   non-whitespace character is `?`. Every other shape acknowledges. The rule is
   intentionally crude and conservative, and it keeps classification free of
   any model call so the acknowledgment path spawns no process.
   If the batch qualifies **and** the agent's harness declares a one-shot
   command, run that command with a bounded timeout to produce an answer, and
   use the answer in place of the acknowledgment.
4. Re-check that the turn is still active. If it ended, discard and post
   nothing.
5. Post the result to the bus as the agent, tagged in metadata as an automatic
   reply.
6. Record the text as this session's pending auto-reply note.

### 5. Harness one-shot declaration

`crates/harness/core/src/lib.rs`, implemented in
`crates/nexus/src/harness_registry.rs`

```rust
/// One-shot, non-interactive invocation for short auxiliary prompts.
/// `None` means the harness has no one-shot mode; callers fall back to
/// acknowledgment only.
fn oneshot_command(&self, prompt: &str) -> Option<HeadedCommand> { None }
```

| harness | one-shot command | acknowledge | answer |
| --- | --- | --- | --- |
| claude | `claude -p --model haiku` | yes | yes |
| opencode | `opencode run` | yes | yes |
| codex | `codex exec` | yes | yes |
| hermes, pi, other | none | yes | no |

The one-shot process is a fresh process with no memory of the agent's session.
It receives only the prompt it is given. This is why its authority is limited to
acknowledging and answering trivially.

### 6. Auto-reply note

An in-memory `Mutex<HashMap<SessionId, String>>` owned by the composition root.
`render_injected_turn_for` prefixes the pending note to the next injected turn
and clears it:

```
[auto-reply already sent as you: "Got it — you're on the login refactor; I'll start this next."]
<original message>
```

The note is deliberately not persisted. If the daemon restarts before the
agent's next turn, the note is lost and the agent may phrase its reply as though
it had not already acknowledged. That is redundant, never incorrect, and avoids
a store migration for a cosmetic benefit.

### Data sources

| requirement | source |
| --- | --- |
| what the agent is doing | `current_work` on the agent register |
| whether the agent is busy | `has_active_turn` / `TurnObservation` |
| the message | the `NexusBatch` the loop already holds |
| recent history | store message repo, answer path only, most recent 10 messages of that conversation |
| post as the agent | existing routing send path |

## Failure handling

### Cascade prevention

An acknowledgment posted into a thread is received by the other agents in that
thread. Without a guard, a busy agent would acknowledge another agent's
acknowledgment indefinitely.

**Acknowledgments are emitted only for messages from a human sender.** An
auto-reply can therefore never trigger another one. This eliminates the cascade
class structurally rather than by loop detection.

### Acknowledgment storms

At most one acknowledgment per session per in-flight turn. Subsequent messages
arriving during the same turn are queued silently; they are delivered together
at the boundary regardless.

### Triage failures

| failure | behavior |
| --- | --- |
| one-shot process hangs | 10 s hard timeout; child killed and reaped; fall back to acknowledgment |
| harness declares no one-shot mode | acknowledgment only |
| harness missing, auth failure, non-zero exit | discard output; acknowledgment only |
| empty or unusable output | discard; acknowledgment only |
| triage panics | isolated by `tokio::spawn`; delivery unaffected |
| `current_work` absent or stale | generic phrasing; never names an invented task |
| bus post fails | logged and dropped; delivery unaffected |

Every branch degrades to a plain acknowledgment or to silence. No branch
produces a delivery failure.

### Turn-ended race

If the turn completes while triage is running, the acknowledgment is stale and
the real reply is imminent. Triage re-checks turn state immediately before
posting and discards the reply if the turn has ended.

### Resource bounds

- At most one in-flight triage per session.
- One-shot children are spawned with a timeout and explicitly killed; none
  survive daemon shutdown.
- The acknowledgment path spawns no process, so the common case has no process
  cost.

### Accepted residual risk

The answer path can answer a trivial question incorrectly in the agent's voice,
and the agent may contradict it later. This is inherent to presenting a single
agent voice. It is mitigated by biasing triage toward acknowledging, forbidding
commitments and work, and showing the agent the note so it can correct.

## Testing

### `crates/nexus-dispatch/tests/delivery_timing.rs`

- `interrupt_on_busy_uninterruptible_agent_waits_for_boundary` — the repaired
  matrix arm. **Fails on current code**, which returns
  `Err(InterruptUnsupported)`.
- The remaining matrix is unchanged: `NativeSteer` and `InterruptAndSend` still
  win when available; idle still yields `StartTurn`; `YieldTurn` and
  `AfterToolLoop` are unaffected.

### `crates/nexus-dispatch/tests/loop.rs`

Adds a `RecordingFastAck` fake port; reuses the existing `BoundaryWaitTurnExec`
and `HangingTurnExec` fakes.

- `busy_uninterruptible_message_is_delivered_at_turn_boundary_not_dead_lettered`
  — the batch settles `delivered` and no dead-letter row is written.
- `fast_ack_fires_only_for_human_senders` — an agent-sourced batch produces zero
  acknowledgments.
- `fast_ack_is_emitted_once_per_busy_turn` — three messages during one turn
  produce exactly one acknowledgment.
- `fast_ack_is_discarded_when_the_turn_ends_first`.
- `fast_ack_failure_never_affects_delivery` — a port that panics, errors, and
  hangs; delivery still completes.
- `idle_agent_gets_no_fast_ack`.
- `loop_runs_unchanged_with_no_fast_ack_port`.

### `crates/harness/core/tests/`

- `harnesses_without_oneshot_mode_default_to_none` — `pi`, `other`, `hermes`.
- `claude_opencode_codex_declare_oneshot_commands` — argv assertions only, no
  process spawn, so these run on every platform including the Windows job.

### `crates/nexus/tests/unit/fast_ack.rs`

- `ack_text_uses_current_work_when_present`.
- `ack_text_is_generic_when_current_work_is_absent`.
- `only_a_single_human_message_ending_in_question_mark_is_treated_as_a_question`
  — covers a task statement, a multi-message batch whose last message ends in
  `?`, and trailing whitespace after the `?`.
- `oneshot_timeout_kills_the_process_and_falls_back_to_ack`.
- `oneshot_garbage_output_falls_back_to_ack`.
- `auto_reply_note_is_stapled_into_the_next_turn_and_consumed_once`.

### Out of scope for automated tests

Model answer quality is non-deterministic; the one-shot invocation is faked at
the process boundary in all tests so the suite stays hermetic and offline.
Note persistence across daemon restart is explicitly not implemented.

### Manual verification

Message a busy agent and confirm: an acknowledgment arrives within seconds; the
real reply follows at the turn boundary; `nexus admin dlq list` remains empty,
which is the failure signature of the current behavior.
