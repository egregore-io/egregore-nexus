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

/// Daemon adapter for [`nexus_contracts::FastAckPort`].
///
/// Reads the agent's own identity and declared work from the store, formats (or, for a lone
/// question, answers) a reply, and publishes it to the bus **as that agent** so the operator sees
/// one continuous voice. The text is then recorded as the session's pending note so the agent's
/// next turn continues from it.
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
        Self {
            store,
            bus,
            notes,
            turn_exec,
        }
    }

    fn turn_is_active(&self, session: &SessionId) -> bool {
        self.turn_exec
            .active_turn_sessions()
            .iter()
            .any(|active| active == session)
    }
}

#[async_trait::async_trait]
impl nexus_contracts::FastAckPort for FastAckAdapter {
    async fn fast_ack(&self, session: &SessionId, batch: &NexusBatch) {
        if !should_acknowledge(&self.notes, session) {
            return;
        }
        // Reserve the slot before any await so a burst of arrivals cannot each pass the guard and
        // produce a flurry of identical acknowledgments.
        self.notes.put(session, String::new());

        let row = match nexus_store::repos::sessions::Sessions::new(&self.store)
            .find_by_session_id(session)
            .await
        {
            Ok(Some(row)) => row,
            _ => {
                let _ = self.notes.take(session);
                return;
            }
        };
        let Some(reply_to) = acknowledgment_target(batch) else {
            let _ = self.notes.take(session);
            return;
        };

        let mut reply = ack_text(row.current_work.as_deref());
        if let Some(question) = batch_question(batch) {
            if let Some(harness) = row.agent.as_deref() {
                if let Some(command) = harness_oneshot(harness, &answer_prompt(&row, question)) {
                    if let Some(answer) = run_oneshot(&command, ONESHOT_TIMEOUT).await {
                        reply = answer;
                    }
                }
            }
        }

        // The turn may have ended while we were resolving an answer. The agent's real reply is
        // imminent and strictly better, so discard rather than post a stale acknowledgment.
        if !self.turn_is_active(session) {
            let _ = self.notes.take(session);
            return;
        }

        let caller = nexus_contracts::Caller {
            agent_id: row.agent_id.clone().map(nexus_contracts::ids::AgentId),
            session: session.clone(),
            name: row.name.clone().unwrap_or_default(),
            project: row.project.clone(),
            tier: nexus_contracts::Tier::Agent,
            locality: Default::default(),
            access: None,
            principal_id: None,
        };
        let mut metadata = serde_json::Map::new();
        metadata.insert("nexusAutoReply".to_string(), serde_json::Value::Bool(true));
        let request = nexus_contracts::SendRequest {
            to: reply_to,
            summary: None,
            body: reply.clone(),
            mention: Vec::new(),
            metadata: Some(metadata),
            idempotency_key: None,
        };

        match self.bus.send(&caller, request).await {
            Ok(_) => self.notes.put(session, reply),
            Err(error) => {
                tracing::debug!(
                    target: "nexus::fast_ack",
                    %session,
                    %error,
                    "fast acknowledgment could not be posted; delivery is unaffected"
                );
                let _ = self.notes.take(session);
            }
        }
    }
}

/// Where an acknowledgment should go: back to the thread it came from, else a DM to the sender.
///
/// Only human-authored messages are considered, matching the cascade guard applied by the loop.
fn acknowledgment_target(batch: &NexusBatch) -> Option<nexus_contracts::SendTarget> {
    let message = batch
        .threads
        .iter()
        .chain(batch.dms.iter())
        .find(|message| matches!(message.kind, Kind::Human))?;
    Some(match message.thread.as_deref() {
        Some(thread) => nexus_contracts::SendTarget::Post {
            thread: thread.to_string(),
        },
        None => nexus_contracts::SendTarget::Dm {
            name: Some(message.from.clone()),
            agent_id: None,
        },
    })
}

/// Prompt handed to the one-shot process. It has no access to the agent's session, so everything
/// it may rely on has to be stated here, including the instruction to stay in the agent's voice
/// and to defer rather than guess.
fn answer_prompt(row: &nexus_store::types::SessionRow, question: &str) -> String {
    let name = row.name.as_deref().unwrap_or("this agent");
    let work = row
        .current_work
        .as_deref()
        .map(str::trim)
        .filter(|work| !work.is_empty())
        .unwrap_or("another task");
    format!(
        "You are {name}, replying in your own voice on a team chat. You are currently busy with \
         {work} and cannot do new work right now. Someone just asked: {question:?}\n\n\
         If you can answer that in one or two short sentences from general knowledge, do so. \
         Otherwise reply only that you are mid-task and will get to it next. Never promise to \
         have done anything. Reply with the message text alone, no preamble."
    )
}
