//! Advisory immediate acknowledgment for messages that arrive while an agent is mid-turn.
//!
//! Without this, a message sent to a busy agent is silent until that agent's current turn ends,
//! which can be minutes. The sender cannot tell the difference between "queued" and "lost".
//!
//! The acknowledgment itself is deterministic: it is formatted from the agent's declared
//! `current_work` and spawns no process, so the common case costs nothing and cannot invent a
//! fact. Only the optional answer path runs a harness's one-shot command, and only when the batch
//! is exactly one human question.
//!
//! Everything here is advisory. The event loop spawns it and never awaits it, so every failure
//! degrades to a plain acknowledgment or to silence — never to a delivery failure. The message
//! itself is delivered by the normal turn-boundary path regardless of what happens here.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexus_contracts::ids::SessionId;
use nexus_contracts::{Kind, NexusBatch};

/// Hard bound on a one-shot harness invocation. The child is killed and reaped on elapse.
const ONESHOT_TIMEOUT: Duration = Duration::from_secs(10);

/// Pending auto-reply notes, keyed by session, consumed by the next injected turn.
///
/// This map doubles as the "one acknowledgment per busy turn" guard: while a note is pending for
/// a session, no further acknowledgment is emitted for it. The real delivery consumes the note,
/// which re-arms acknowledgment for the next busy period.
///
/// Deliberately in-memory. A daemon restart drops pending notes, which at worst makes an agent
/// phrase a reply as though it had not already acknowledged — redundant, never incorrect — and
/// avoids a store migration for a cosmetic benefit.
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

impl nexus_contracts::AutoReplyNotePort for AutoReplyNotes {
    fn take(&self, session: &SessionId) -> Option<String> {
        AutoReplyNotes::take(self, session)
    }
}

/// The acknowledgment sent while the agent is busy.
///
/// Never fabricates a task name: an absent or blank `current_work` yields generic phrasing rather
/// than a confident-sounding invention, because this text is published under the agent's name.
pub fn ack_text(current_work: Option<&str>) -> String {
    match current_work.map(str::trim).filter(|work| !work.is_empty()) {
        Some(work) => format!("Got it — I'm on {work}; I'll pick this up next."),
        None => "Got it — I'm mid-task; I'll pick this up next.".to_string(),
    }
}

/// The batch's question text, when the batch is exactly one human message ending in `?`.
///
/// Deliberately crude and conservative. A wrong auto-answer in the agent's voice is worse than a
/// correct late one, so anything ambiguous acknowledges instead of answering.
pub fn batch_question(batch: &NexusBatch) -> Option<&str> {
    if batch.counts.total != 1 {
        return None;
    }
    let mut human = batch
        .dms
        .iter()
        .chain(batch.threads.iter())
        .filter(|message| matches!(message.kind, Kind::Human));
    let only = human.next()?;
    if human.next().is_some() {
        return None;
    }
    only.body
        .trim_end()
        .ends_with('?')
        .then_some(only.body.as_str())
}

/// Whether an acknowledgment should be emitted now.
///
/// One acknowledgment per busy period: while a note is pending for this session the agent has
/// already acknowledged and has not yet been delivered to, so further arrivals stay silent. They
/// are all delivered together at the turn boundary regardless, so repeating the acknowledgment
/// would add noise, not information.
pub fn should_acknowledge(notes: &AutoReplyNotes, session: &SessionId) -> bool {
    !notes.has_pending(session)
}

/// Run a harness one-shot command under a hard timeout, returning its trimmed stdout.
///
/// Returns `None` on spawn failure, non-zero exit, empty output, or timeout, so every failure
/// mode collapses into "fall back to the deterministic acknowledgment". `kill_on_drop` reaps the
/// child when the timeout drops the future, so no one-shot process outlives this call.
pub async fn run_oneshot(
    command: &nexus_harness_core::HeadedCommand,
    timeout: Duration,
) -> Option<String> {
    let child = tokio::process::Command::new(&command.program)
        .args(&command.args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;

    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(Ok(output)) => output,
        // Elapsed, or the wait itself failed. `kill_on_drop` reaps the child as the future drops.
        _ => return None,
    };
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// Resolve a harness token to its one-shot command, if that harness declares one.
pub fn harness_oneshot(harness: &str, prompt: &str) -> Option<nexus_harness_core::HeadedCommand> {
    let id = nexus_contracts::HarnessId::new(harness).ok()?;
    crate::harness_registry::harness_registry_by_id(&id).oneshot_command(prompt)
}

#[cfg(test)]
#[path = "../../tests/unit/fast_ack.rs"]
mod tests;
