//! Claude native bridge forwarder.
//!
//! The first bridge pass is an explicit `forward_once`: read append-only hook object streams from
//! their persisted byte cursors, emit Nexus `agent.update` events, and persist the new cursors. A
//! later live loop can call this repeatedly or wrap it with file watching without changing the
//! parser or storage contract. Completed `MessageDisplay` occurrences are paired FIFO with grouped
//! transcript assistant records because current Claude hook UUIDs and transcript model ids occupy
//! different namespaces.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use nexus_common::NexusError;
use nexus_contracts::events::{ChildResolution, ChildStream, WsEvent};
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_store::repos::{ChildStreamEvents, DaemonState, LaneMutation, ProducerIdentities};
use nexus_store::Store;
use nexus_transcript::ToolCallObservation;
use serde_json::{json, Value};

use crate::native::bridge::ClaudeNativeBridgePaths;
use crate::native::codec::{
    compaction_event, text_event, tool_event, tool_observation, turn_boundary_event,
    user_input_event, ClaudeNativeEvent,
};
use crate::native::message_delta::parse_message_display_value;
use crate::native::model_reporting::ClaudeTranscriptRead;
use crate::native::transcript::{
    parse_hook_record, parse_transcript_value, AssistantText, ClaudeHookRecord, ClaudeToolUpdate,
    TranscriptRecord,
};
use crate::storage::{
    ClaudeChildCursor, ClaudeChildStreamsRepo, ClaudeRuntimeState, ClaudeRuntimeStateRepo,
};

/// Subagent transcript files one child pass will serve; the rest are served by later passes,
/// least recently served first, so every file is reached.
const MAX_CHILD_FILES_PER_PASS: usize = 32;
/// Directory entries one pass will list per owned session's `subagents/` directory.
const MAX_CHILD_DIR_ENTRIES: usize = 1024;
/// Bytes one pass will read from one subagent file, starting at its cursor.
const MAX_CHILD_BYTES_PER_FILE_PASS: u64 = 256 * 1024;
/// Records one pass will forward from one subagent file.
const MAX_CHILD_RECORDS_PER_FILE_PASS: usize = 256;
/// Bytes of a subagent file's first line read to establish its source generation.
const MAX_CHILD_META_LINE_BYTES: u64 = 64 * 1024;

/// Counts returned from one forwarder pass.
///
/// Lifecycle counts such as [`ClaudeForwarderStats::session_start_events`] and
/// [`ClaudeForwarderStats::compaction_events`] are intentionally exposed even when they do not
/// render a user-visible event; the daemon uses them to re-ring delivery after Claude lifecycle
/// hooks that can make a headed session ready again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClaudeForwarderStats {
    pub session_start_events: usize,
    pub user_input_events: usize,
    pub text_events: usize,
    pub tool_call_events: usize,
    pub turn_end_events: usize,
    pub compaction_events: usize,
    /// `child_agent.update` events emitted for subagent records (main-transcript sidechain rows
    /// and tailed `subagents/` files). Never counted as parent activity.
    pub child_events: usize,
    /// Child passes that failed (read or cursor errors). The parent's pass, cursors and stats
    /// are unaffected; the child pass retries next time.
    pub child_errors: usize,
}

/// Observation-only sink for Claude native tool-call phases.
///
/// Implementations must keep this lane ephemeral. It is called from the native forwarder in the
/// same pass as normal `agent.update` projection, but it must not write store rows, enqueue command
/// intents, ring realtime bells, or inject agent turns.
pub trait ClaudeToolObservationSink: Send + Sync {
    /// Publish one native tool-call observation for `session`.
    fn publish_tool_call(&self, session: &SessionId, observation: ToolCallObservation);
}

/// Native hook facts are ingested synchronously before any presentation await. Acceptance
/// receives that same record, never a later text-only reconstruction from a public event.
#[async_trait::async_trait]
pub trait ClaudeHookObservationSink: Send + Sync {
    fn observe_hooks(&self, records: &[ClaudeHookRecord], file_len: Option<u64>, complete: bool);
    /// A captured reporting floor may precede a display cursor that intentionally skips history.
    fn model_byte_floor(&self, _path: &Path) -> Option<u64> {
        None
    }
    /// Typed native response facts and absolute source offsets. This is not display-derived
    /// model evidence; the binding-owned consumer must validate its captured root and source.
    fn observe_models(
        &self,
        _path: &Path,
        _records: &[(crate::native::model_reporting::ClaudeResponseModel, u64)],
        _source: Option<&ClaudeTranscriptRead>,
        _complete: bool,
    ) {
    }
    async fn accept_input(&self, _record: &ClaudeHookRecord) -> bool {
        false
    }
}

/// Forward newly appended Claude native bridge records into Nexus events once.
pub async fn forward_once(
    store: Arc<Store>,
    session: SessionId,
    paths: ClaudeNativeBridgePaths,
    events: Arc<dyn EventSink>,
) -> Result<ClaudeForwarderStats, NexusError> {
    forward_once_with_tool_observations(store, session, paths, events, None).await
}

/// Forward one Claude native bridge pass while also publishing observation-only tool events.
pub async fn forward_once_with_tool_observations(
    store: Arc<Store>,
    session: SessionId,
    paths: ClaudeNativeBridgePaths,
    events: Arc<dyn EventSink>,
    tool_observations: Option<Arc<dyn ClaudeToolObservationSink>>,
) -> Result<ClaudeForwarderStats, NexusError> {
    forward_once_with_observations(store, session, paths, events, tool_observations, None).await
}

/// Compatibility-preserving pass with an optional binding-owned hook observer.
pub async fn forward_once_with_observations(
    store: Arc<Store>,
    session: SessionId,
    paths: ClaudeNativeBridgePaths,
    events: Arc<dyn EventSink>,
    tool_observations: Option<Arc<dyn ClaudeToolObservationSink>>,
    hooks: Option<Arc<dyn ClaudeHookObservationSink>>,
) -> Result<ClaudeForwarderStats, NexusError> {
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let state = repo.find_by_runtime_id(&session).await?;
    let state_ref = state.as_ref();
    let mut stats = ClaudeForwarderStats::default();

    let producer_ids = ProducerIdentities::new(&store);

    let (message_values, message_cursor) = read_new_json_values(
        &paths.message_delta_log_path,
        cursor(state_ref, CursorKind::MessageDelta),
    )?;

    let hook_start = cursor(state_ref, CursorKind::Hook);
    let hook_read = match read_json_records(&paths.hook_log_path, hook_start) {
        Ok(read) => read,
        Err(error) => {
            if let Some(hooks) = &hooks {
                hooks.observe_hooks(&[], None, false);
            }
            return Err(error);
        }
    };
    let hook_cursor = hook_read.cursor;
    let hook_records: Vec<_> = hook_read
        .values
        .iter()
        .map(|(value, offset)| parse_hook_record(value, *offset))
        .collect();
    if let Some(hooks) = &hooks {
        hooks.observe_hooks(&hook_records, hook_read.file_len, hook_read.complete);
    }
    let hook_values: Vec<_> = hook_read
        .values
        .into_iter()
        .map(|(value, _)| value)
        .collect();

    let ResolvedTail {
        transcript_path,
        start_cursor: transcript_start_cursor,
        rebound: transcript_rebound,
        owned_sessions,
    } = resolve_transcript_tail(
        &repo,
        &session,
        state_ref,
        &paths,
        &message_values,
        &hook_values,
    )
    .await?;
    if transcript_rebound {
        producer_ids.clear_runtime(&session.0).await?;
    }
    // The bridge belongs to the runtime. Records that name a Claude session this runtime never
    // started are stray and are dropped; they never stall the owner's stream.
    let (hook_values, hook_records) =
        retain_owned_hooks(hook_values, hook_records, &owned_sessions);
    let message_values = retain_owned_values(message_values, &owned_sessions);
    count_hook_session_starts(&hook_values, &mut stats);
    emit_hook_user_inputs(
        &session,
        events.as_ref(),
        &hook_values,
        &hook_records,
        hooks.as_deref(),
        &mut stats,
    )
    .await;
    emit_hook_tool_observations(&session, tool_observations.as_deref(), &hook_values);
    let StreamedMessages {
        text: message_text,
        completed_message_ids,
    } = emit_message_deltas(&session, events.as_ref(), &message_values, &mut stats).await;
    for id in completed_message_ids {
        producer_ids.admit_streamed(&session.0, &id).await?;
    }

    let transcript_read = match read_json_records(&transcript_path, transcript_start_cursor) {
        Ok(read) => read,
        Err(error) => {
            if let Some(hooks) = &hooks {
                hooks.observe_models(&transcript_path, &[], None, false);
            }
            return Err(error);
        }
    };
    if let Some(hooks) = &hooks {
        let models: Vec<_> = if let Some((source, floor)) = transcript_read
            .source
            .as_ref()
            .zip(hooks.model_byte_floor(&transcript_path))
        {
            source.response_models_after(floor)
        } else {
            transcript_read
                .values
                .iter()
                .filter_map(|(value, offset)| {
                    parse_transcript_value(value)?
                        .response_model
                        .map(|model| (model, *offset))
                })
                .collect()
        };
        hooks.observe_models(
            &transcript_path,
            &models,
            transcript_read.source.as_ref(),
            transcript_read.complete,
        );
    }
    let transcript_cursor = transcript_read.cursor;
    // Sidechain rows never enter the parent path: they are the subagent's, even when Claude
    // writes them into the root transcript. They go to the child lane with their native marks.
    let (transcript_values, sidechain_values): (Vec<_>, Vec<_>) =
        transcript_read.values.into_iter().partition(|(value, _)| {
            parse_transcript_value(value).is_none_or(|record| record.sidechain.is_none())
        });
    let transcript_values: Vec<_> = transcript_values
        .into_iter()
        .map(|(value, _)| value)
        .collect();
    let current_root = owned_sessions.last().cloned();
    emit_main_transcript_sidechain(
        &session,
        events.as_ref(),
        &transcript_path,
        current_root.as_deref(),
        &sidechain_values,
        &mut stats,
    )
    .await;
    emit_transcript_records(
        &session,
        events.as_ref(),
        &transcript_values,
        message_text.as_deref(),
        &producer_ids,
        tool_observations.as_deref(),
        &mut stats,
    )
    .await?;
    emit_hook_lifecycle_records(&session, events.as_ref(), &hook_values, &mut stats).await;

    repo.set_cursors(
        &session,
        Some(hook_cursor),
        Some(transcript_cursor),
        Some(message_cursor),
    )
    .await?;

    // The child pass runs after the parent's cursors are committed and never fails the parent's
    // pass: its errors are counted and logged, and it retries on the next pass.
    if let Err(error) = forward_child_streams(
        &store,
        &session,
        events.as_ref(),
        &owned_sessions,
        &transcript_path,
        &mut stats,
    )
    .await
    {
        stats.child_errors += 1;
        tracing::warn!(
            target: "nexus::claude_child_streams",
            session = %session,
            error = %error,
            "Claude child stream pass failed; parent pass unaffected"
        );
    }

    Ok(stats)
}

/// Result of forwarding one pass of `MessageDisplay` deltas: the concatenated streamed text (if
/// any was emitted) and the distinct Claude message ids those deltas carried.
struct StreamedMessages {
    text: Option<String>,
    completed_message_ids: Vec<String>,
}

fn count_hook_session_starts(values: &[Value], stats: &mut ClaudeForwarderStats) {
    stats.session_start_events += values
        .iter()
        .filter(|value| hook_event_name(value).as_deref() == Some("SessionStart"))
        .count();
}

fn hook_event_name(value: &Value) -> Option<String> {
    let payload = value.get("payload").unwrap_or(value);
    string_from(payload, &["hook_event_name", "hookEventName", "event"])
        .or_else(|| string_from(value, &["event", "hook_event_name", "hookEventName"]))
}

fn string_from(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
        .map(str::to_string)
}

/// What one forward pass follows: the transcript to tail, where to start in it, whether the
/// runtime moved to a new Claude session, and which Claude session ids this runtime owns.
struct ResolvedTail {
    transcript_path: PathBuf,
    start_cursor: i64,
    rebound: bool,
    owned_sessions: Vec<String>,
}

/// Ownership of the bridge belongs to the runtime. Claude's own session id is a
/// field on each record, not the owner: a `SessionStart` naming a new id (a resume, fork or
/// `/clear`) moves the runtime forward to that session; records naming any id the runtime never
/// started are stray. Only a batch that names several ids while nothing is known yet cannot be
/// ordered, and that one case stays fail-closed.
async fn resolve_transcript_tail(
    repo: &ClaudeRuntimeStateRepo<'_>,
    session: &SessionId,
    state: Option<&ClaudeRuntimeState>,
    paths: &ClaudeNativeBridgePaths,
    message_values: &[Value],
    hook_values: &[Value],
) -> Result<ResolvedTail, NexusError> {
    let default_path = default_transcript_path(paths);
    let stored_path = state
        .and_then(|state| state.transcript_path.clone())
        .unwrap_or_else(|| default_path.clone());
    let stored_session = state.and_then(|state| state.claude_session_id.clone());

    let mut current_session = match last_session_start_id(hook_values) {
        Some(started) => Some(started),
        None => match stored_session.clone() {
            Some(stored) => Some(stored),
            None => {
                let discovered = distinct_claude_session_ids(message_values, hook_values);
                if discovered.len() > 1 {
                    return Err(NexusError::Ambiguous(format!(
                        "mixed native Claude session ids in one bridge batch: {}",
                        discovered.join(", ")
                    )));
                }
                discovered.into_iter().next()
            }
        },
    };

    // A batch that names only sessions the runtime does not know, without a `SessionStart`,
    // means the start was consumed before its id was persisted. The full hook log is the
    // runtime's own record: its latest `SessionStart` decides.
    let batch_ids = distinct_claude_session_ids(message_values, hook_values);
    let stored_transcript_missing = !stored_path.exists();
    let batch_disowns_current = !batch_ids.is_empty()
        && current_session
            .as_deref()
            .is_some_and(|current| !batch_ids.iter().any(|id| id == current));
    let mut discovered_path = current_session
        .as_deref()
        .and_then(|current| latest_transcript_path_for(message_values, hook_values, current))
        .map(PathBuf::from);
    if stored_transcript_missing || batch_disowns_current {
        let (all_hook_values, _) = read_new_json_values(&paths.hook_log_path, 0)?;
        if let Some(latest_started) = last_session_start_id(&all_hook_values) {
            if current_session.as_deref() != Some(latest_started.as_str()) {
                current_session = Some(latest_started);
                discovered_path = None;
            }
        }
        if discovered_path.is_none() {
            discovered_path = current_session
                .as_deref()
                .and_then(|current| latest_transcript_path_for(&[], &all_hook_values, current))
                .map(PathBuf::from);
        }
    }

    let rebound = current_session
        .as_deref()
        .zip(stored_session.as_deref())
        .is_some_and(|(current, stored)| current != stored);
    let mut owned_sessions = Vec::new();
    if let Some(stored) = stored_session.clone() {
        owned_sessions.push(stored);
    }
    if let Some(current) = current_session.clone() {
        if !owned_sessions.contains(&current) {
            owned_sessions.push(current);
        }
    }

    if let Some(claude_session_id) = current_session.as_deref() {
        repo.set_claude_session(session, claude_session_id, discovered_path.clone())
            .await?;
    }

    let Some(discovered_path) = discovered_path else {
        return Ok(ResolvedTail {
            transcript_path: stored_path,
            start_cursor: cursor(state, CursorKind::Transcript),
            rebound,
            owned_sessions,
        });
    };

    if discovered_path == stored_path {
        return Ok(ResolvedTail {
            transcript_path: discovered_path,
            start_cursor: cursor(state, CursorKind::Transcript),
            rebound,
            owned_sessions,
        });
    }

    // A new transcript file (first discovery, or a fork that copied the history) starts at its
    // current end: what it already holds was either forwarded from the previous file or never
    // this runtime's live output.
    let start_cursor = transcript_end_offset(&discovered_path)?;
    repo.set_transcript_path_and_cursor(session, discovered_path.clone(), start_cursor)
        .await?;
    Ok(ResolvedTail {
        transcript_path: discovered_path,
        start_cursor,
        rebound: true,
        owned_sessions,
    })
}

/// The Claude session id named by the latest `SessionStart` hook in `hook_values`, if any.
fn last_session_start_id(hook_values: &[Value]) -> Option<String> {
    hook_values.iter().rev().find_map(|value| {
        if hook_event_name(value).as_deref() != Some("SessionStart") {
            return None;
        }
        parse_transcript_value(value)
            .and_then(|record| record.session_id)
            .filter(|id| !id.is_empty())
    })
}

fn record_session_id(value: &Value) -> Option<String> {
    parse_message_display_value(value)
        .and_then(|delta| delta.session_id)
        .or_else(|| parse_transcript_value(value).and_then(|record| record.session_id))
        .filter(|session| !session.is_empty())
}

/// The latest transcript path named by a record of `session_id`, searching message deltas
/// first and hook records second, newest first.
fn latest_transcript_path_for(
    message_values: &[Value],
    hook_values: &[Value],
    session_id: &str,
) -> Option<String> {
    let path_of = |value: &Value| {
        (record_session_id(value).as_deref() == Some(session_id))
            .then(|| {
                parse_message_display_value(value)
                    .and_then(|delta| delta.transcript_path)
                    .or_else(|| {
                        parse_transcript_value(value).and_then(|record| record.transcript_path)
                    })
            })
            .flatten()
            .filter(|path| !path.is_empty())
    };
    message_values
        .iter()
        .rev()
        .find_map(path_of)
        .or_else(|| hook_values.iter().rev().find_map(path_of))
}

fn distinct_claude_session_ids(message_values: &[Value], hook_values: &[Value]) -> Vec<String> {
    let mut out = Vec::new();
    for value in message_values.iter().chain(hook_values) {
        let Some(session_id) = record_session_id(value) else {
            continue;
        };
        if !out.contains(&session_id) {
            out.push(session_id);
        }
    }
    out
}

/// Keep hook records (and their parsed provenance, index-aligned) that carry no Claude session
/// id or one this runtime owns.
fn retain_owned_hooks(
    values: Vec<Value>,
    records: Vec<ClaudeHookRecord>,
    owned: &[String],
) -> (Vec<Value>, Vec<ClaudeHookRecord>) {
    values
        .into_iter()
        .zip(records)
        .filter(|(value, _)| is_owned_record(value, owned))
        .unzip()
}

fn retain_owned_values(values: Vec<Value>, owned: &[String]) -> Vec<Value> {
    values
        .into_iter()
        .filter(|value| is_owned_record(value, owned))
        .collect()
}

fn is_owned_record(value: &Value, owned: &[String]) -> bool {
    match record_session_id(value) {
        Some(session_id) => owned.iter().any(|id| *id == session_id),
        None => true,
    }
}

async fn emit_message_deltas(
    session: &SessionId,
    events: &dyn EventSink,
    values: &[Value],
    stats: &mut ClaudeForwarderStats,
) -> StreamedMessages {
    let mut text = String::new();
    let mut completed_message_ids: Vec<String> = Vec::new();
    for delta in values.iter().filter_map(parse_message_display_value) {
        text.push_str(&delta.delta);
        if delta.final_chunk {
            if let Some(id) = delta.message_id {
                if !completed_message_ids.contains(&id) {
                    completed_message_ids.push(id);
                }
            }
        }
    }
    if text.is_empty() {
        return StreamedMessages {
            text: None,
            completed_message_ids: Vec::new(),
        };
    }
    emit_claude_event(session, events, text_event(text.clone())).await;
    stats.text_events += 1;
    StreamedMessages {
        text: Some(text),
        completed_message_ids,
    }
}

async fn emit_transcript_records(
    session: &SessionId,
    events: &dyn EventSink,
    values: &[Value],
    streamed_text: Option<&str>,
    producer_ids: &ProducerIdentities<'_>,
    tool_observations: Option<&dyn ClaudeToolObservationSink>,
    stats: &mut ClaudeForwarderStats,
) -> Result<(), NexusError> {
    let records = values
        .iter()
        .filter_map(parse_transcript_value)
        .collect::<Vec<_>>();
    let mut index = 0;
    while index < records.len() {
        let message_id = records[index].assistant_message_id.as_deref();
        let mut end = index + 1;
        if let Some(message_id) = message_id {
            while end < records.len()
                && records[end].assistant_message_id.as_deref() == Some(message_id)
            {
                end += 1;
            }
        }
        let group = &records[index..end];
        let text = group
            .iter()
            .map(|record| concat_assistant_text(&record.assistant_text))
            .collect::<String>();
        // Claude 2.1 gives MessageDisplay and transcript records unrelated ids. The native
        // streams are append-only and this forwarder is serialized per runtime, so one completed
        // displayed occurrence pairs with the next non-empty assistant transcript group.
        let suppressed = if text.is_empty() {
            false
        } else {
            producer_ids.consume_oldest(&session.0).await?.is_some()
                || Some(text.as_str()) == streamed_text
        };
        if !text.is_empty() && !suppressed {
            emit_claude_event(session, events, text_event(&text)).await;
            stats.text_events += 1;
        }
        for record in group {
            publish_tool_observations(session, tool_observations, &record.tool_updates);
            emit_tool_updates(session, events, &record.tool_updates, stats).await;
            if let Some(compaction) = &record.compaction {
                emit_claude_event(session, events, compaction_event(compaction)).await;
                stats.compaction_events += 1;
            }
        }
        // A thinking-only assistant row is not a turn boundary. Claude writes the visible text
        // record with the same model message id later; Stop remains authoritative for streamed
        // completions, while unmatched transcript-only text retains one fallback boundary.
        if !suppressed && (!text.is_empty() || message_id.is_none()) {
            if let Some(boundary) = group.iter().find_map(|record| record.boundary.as_ref()) {
                emit_claude_event(session, events, turn_boundary_event(boundary)).await;
                stats.turn_end_events += 1;
            }
        }
        index = end;
    }
    Ok(())
}

async fn emit_hook_user_inputs(
    session: &SessionId,
    events: &dyn EventSink,
    values: &[Value],
    records: &[ClaudeHookRecord],
    hooks: Option<&dyn ClaudeHookObservationSink>,
    stats: &mut ClaudeForwarderStats,
) {
    for (value, provenance) in values.iter().zip(records) {
        let Some(record) = parse_transcript_value(value) else {
            continue;
        };
        if let Some(prompt) = &record.user_prompt {
            if let Some(hooks) = hooks {
                if hooks.accept_input(provenance).await {
                    stats.user_input_events += 1;
                    continue;
                }
            }
            emit_claude_event(
                session,
                events,
                user_input_event(&prompt.text, prompt.prompt_id.as_deref()),
            )
            .await;
            stats.user_input_events += 1;
        }
    }
}

async fn emit_hook_lifecycle_records(
    session: &SessionId,
    events: &dyn EventSink,
    values: &[Value],
    stats: &mut ClaudeForwarderStats,
) {
    for record in values.iter().filter_map(parse_transcript_value) {
        emit_record_lifecycle(session, events, &record, stats).await;
    }
}

fn emit_hook_tool_observations(
    session: &SessionId,
    sink: Option<&dyn ClaudeToolObservationSink>,
    values: &[Value],
) {
    for record in values.iter().filter_map(parse_transcript_value) {
        publish_tool_observations(session, sink, &record.tool_updates);
    }
}

async fn emit_record_lifecycle(
    session: &SessionId,
    events: &dyn EventSink,
    record: &TranscriptRecord,
    stats: &mut ClaudeForwarderStats,
) {
    if let Some(boundary) = &record.boundary {
        emit_claude_event(session, events, turn_boundary_event(boundary)).await;
        stats.turn_end_events += 1;
    }
    if let Some(compaction) = &record.compaction {
        emit_claude_event(session, events, compaction_event(compaction)).await;
        stats.compaction_events += 1;
    }
}

fn publish_tool_observations(
    session: &SessionId,
    sink: Option<&dyn ClaudeToolObservationSink>,
    updates: &[ClaudeToolUpdate],
) {
    let Some(sink) = sink else { return };
    for update in updates {
        sink.publish_tool_call(session, tool_observation(update));
    }
}

async fn emit_tool_updates(
    session: &SessionId,
    events: &dyn EventSink,
    updates: &[ClaudeToolUpdate],
    stats: &mut ClaudeForwarderStats,
) {
    for update in updates {
        emit_claude_event(session, events, tool_event(update)).await;
        stats.tool_call_events += 1;
    }
}

async fn emit_claude_event(session: &SessionId, events: &dyn EventSink, event: ClaudeNativeEvent) {
    events
        .emit(WsEvent::AgentUpdate {
            session_id: session.clone(),
            kind: event.kind,
            data: event.data,
        })
        .await;
}

/// Child lane identity of a Claude subagent transcript. Root affiliation is proven by the file
/// living under the owned session's `subagents/` directory and each record carrying that
/// `sessionId` and its own `agentId`; immediate parent and depth are never claimed.
fn claude_child_stream(
    root: &str,
    agent_id: &str,
    generation: &str,
    resolution: ChildResolution,
    evidence: Option<&str>,
) -> ChildStream {
    ChildStream {
        harness: "claude".into(),
        root: root.into(),
        id: Some(agent_id.into()),
        locator: format!("claude:subagents/agent-{agent_id}.jsonl@{generation}"),
        parent: None,
        parent_ref: None,
        depth: None,
        resolution,
        evidence: evidence.map(str::to_owned),
    }
}

/// The renderable events of one subagent record: user text, assistant text, tool updates and
/// the assistant's stop boundary. Compaction markers of a child are not forwarded.
fn child_events_for(record: &TranscriptRecord, value: &Value) -> Vec<ClaudeNativeEvent> {
    let mut out = Vec::new();
    if let Some(text) = user_row_text(value) {
        out.push(user_input_event(text, None));
    }
    let text = concat_assistant_text(&record.assistant_text);
    if !text.is_empty() {
        out.push(text_event(text));
    }
    for update in &record.tool_updates {
        out.push(tool_event(update));
    }
    if let Some(boundary) = &record.boundary {
        out.push(turn_boundary_event(boundary));
    }
    out
}

/// Text of a transcript `user` row: a string content or its `text` blocks. Tool results are
/// tool updates, not user text.
fn user_row_text(value: &Value) -> Option<String> {
    if value.get("type").and_then(Value::as_str) != Some("user") {
        return None;
    }
    let content = value.pointer("/message/content")?;
    if let Some(text) = content.as_str() {
        return (!text.is_empty()).then(|| text.to_owned());
    }
    let text = content
        .as_array()?
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("");
    (!text.is_empty()).then_some(text)
}

/// The `uuid` of a subagent file's first record, read within a bounded first line; empty when
/// unavailable. Names the source generation in every `source_ref` of that file.
/// What opening a registered child path yielded.
enum ChildOpen {
    Regular(std::fs::File),
    /// The path names something other than a regular file; `&str` says what.
    NotRegular(&'static str),
}

/// Open a registered child path for reading and keep the handle only when it is a regular
/// file, so every later check reads that file. On Unix the open carries
/// `O_NONBLOCK | O_NOFOLLOW`: a FIFO opens at once even with no writer and is then rejected
/// by `fstat`, and a symlink fails the open itself. On other platforms the open is a plain
/// `File::open`, which follows a symlink to a regular target; the regular-file check still
/// applies to whatever was opened. Non-blocking and no-follow are Unix guarantees only.
fn open_regular_child_file(path: &Path) -> Result<ChildOpen, NexusError> {
    #[cfg(unix)]
    let opened = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
            .open(path)
    };
    #[cfg(not(unix))]
    let opened = std::fs::File::open(path);
    let file = match opened {
        Ok(file) => file,
        #[cfg(unix)]
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
            return Ok(ChildOpen::NotRegular("symlink"));
        }
        Err(error) => {
            return Err(NexusError::Internal(format!(
                "open claude child file {}: {error}",
                path.display()
            )));
        }
    };
    let file_type = file
        .metadata()
        .map_err(|e| {
            NexusError::Internal(format!("stat claude child file {}: {e}", path.display()))
        })?
        .file_type();
    if file_type.is_file() {
        return Ok(ChildOpen::Regular(file));
    }
    #[cfg(unix)]
    let kind = {
        use std::os::unix::fs::FileTypeExt;
        if file_type.is_fifo() {
            "fifo"
        } else if file_type.is_socket() {
            "socket"
        } else if file_type.is_char_device() || file_type.is_block_device() {
            "device"
        } else if file_type.is_dir() {
            "directory"
        } else {
            "other"
        }
    };
    #[cfg(not(unix))]
    let kind = if file_type.is_dir() {
        "directory"
    } else {
        "other"
    };
    Ok(ChildOpen::NotRegular(kind))
}

/// What a child file's first line says about its source generation.
#[derive(Debug, PartialEq, Eq)]
enum Generation {
    /// The first record's `uuid`.
    Ready(String),
    /// The file is empty or its first line is not complete yet.
    Pending,
    /// The first line is complete but has no `uuid`, is not JSON, or is longer than
    /// [`MAX_CHILD_META_LINE_BYTES`].
    Unverifiable,
}

/// Read the first line of an already opened file within the meta-line budget. The handle is
/// the one the caller keeps for the rest of the pass, so the line comes from the same inode
/// the read will use.
fn first_record_generation(file: &mut std::fs::File) -> Result<Generation, NexusError> {
    use std::io::{BufRead, Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))
        .map_err(|e| NexusError::Internal(format!("seek claude child file: {e}")))?;
    let mut first = String::new();
    std::io::BufReader::new(file.by_ref().take(MAX_CHILD_META_LINE_BYTES))
        .read_line(&mut first)
        .map_err(|e| NexusError::Internal(format!("read claude child file: {e}")))?;
    if !first.ends_with('\n') {
        if first.len() as u64 >= MAX_CHILD_META_LINE_BYTES {
            return Ok(Generation::Unverifiable);
        }
        return Ok(Generation::Pending);
    }
    Ok(serde_json::from_str::<Value>(first.trim_end())
        .ok()
        .and_then(|value| value.get("uuid")?.as_str().map(str::to_owned))
        .filter(|uuid| !uuid.is_empty())
        .map_or(Generation::Unverifiable, Generation::Ready))
}

/// The main transcript's generation for sidechain rows found in it: its first record uuid,
/// empty when unavailable.
fn first_record_uuid(path: &Path) -> String {
    let Ok(mut file) = std::fs::File::open(path) else {
        return String::new();
    };
    match first_record_generation(&mut file) {
        Ok(Generation::Ready(uuid)) => uuid,
        _ => String::new(),
    }
}

/// The record recorded as the cursor's anchor still starts at `anchor_offset`, ends exactly at
/// `cursor`, and carries `anchor_uuid`. This is bounded detection of a changed anchor record
/// (it catches a file truncated and regrown past the cursor), not proof that every earlier
/// byte is unchanged.
fn anchor_intact(file: &mut std::fs::File, cursor: &ClaudeChildCursor) -> Result<bool, NexusError> {
    if cursor.cursor <= 0 {
        return Ok(true);
    }
    let read = read_child_records(
        file,
        cursor.anchor_offset,
        (cursor.cursor - cursor.anchor_offset).max(0) as u64,
        1,
    )?;
    let Some((value, _)) = read.records.first() else {
        return Ok(false);
    };
    let uuid = value
        .get("uuid")
        .and_then(Value::as_str)
        .unwrap_or_default();
    Ok(read.end == cursor.cursor && uuid == cursor.anchor_uuid)
}

/// One bounded read of a subagent file.
struct ChildRead {
    /// Complete records in order with the byte offset each one ends at.
    records: Vec<(Value, u64)>,
    /// Offset after the last complete record (the cursor to store).
    end: i64,
    /// Offset of a record that is not JSON, when the read stopped on one. A record that is
    /// merely incomplete (still being written, or cut by the byte budget) is not malformed.
    malformed_at: Option<i64>,
}

/// A bounded read of an opened subagent file from `cursor`: at most `max_bytes` are read and
/// at most `max_records` complete records returned with their end offsets; a partial record
/// at the end stays for the next pass. Consumed prefixes are never re-read.
fn read_child_records(
    file: &mut std::fs::File,
    cursor: i64,
    max_bytes: u64,
    max_records: usize,
) -> Result<ChildRead, NexusError> {
    use std::io::{Read, Seek, SeekFrom};
    let start = u64::try_from(cursor.max(0)).unwrap_or(0);
    file.seek(SeekFrom::Start(start))
        .map_err(|e| NexusError::Internal(format!("seek claude child file: {e}")))?;
    let mut buf = Vec::new();
    file.by_ref()
        .take(max_bytes)
        .read_to_end(&mut buf)
        .map_err(|e| NexusError::Internal(format!("read claude child file: {e}")))?;
    let mut records = Vec::new();
    let mut consumed = 0usize;
    let mut malformed_at = None;
    let mut stream = serde_json::Deserializer::from_slice(&buf).into_iter::<Value>();
    while records.len() < max_records {
        match stream.next() {
            Some(Ok(value)) => {
                consumed = stream.byte_offset();
                records.push((value, start + consumed as u64));
            }
            Some(Err(error)) => {
                if !error.is_eof() {
                    malformed_at = Some((start + consumed as u64) as i64);
                }
                break;
            }
            None => break,
        }
    }
    Ok(ChildRead {
        records,
        end: (start + consumed as u64) as i64,
        malformed_at,
    })
}

/// Sidechain rows found in the root transcript: emitted on the child lane, never the parent's.
/// Root affiliation is the row's own `sessionId` equal to the owned root plus its `agentId`.
async fn emit_main_transcript_sidechain(
    session: &SessionId,
    events: &dyn EventSink,
    transcript_path: &Path,
    current_root: Option<&str>,
    values: &[(Value, u64)],
    stats: &mut ClaudeForwarderStats,
) {
    if values.is_empty() {
        return;
    }
    let generation = first_record_uuid(transcript_path);
    for (value, end_offset) in values {
        let Some(record) = parse_transcript_value(value) else {
            continue;
        };
        let Some(mark) = record.sidechain.as_ref() else {
            continue;
        };
        let root = current_root.unwrap_or_default();
        let agent_id = mark.agent_id.clone().unwrap_or_default();
        let verified = !agent_id.is_empty()
            && current_root.is_some()
            && record.session_id.as_deref() == current_root;
        let child = if verified {
            claude_child_stream(
                root,
                &agent_id,
                &generation,
                ChildResolution::RootVerified,
                Some("main_transcript_sidechain+sessionId+agentId"),
            )
        } else {
            ChildStream {
                harness: "claude".into(),
                root: root.into(),
                id: (!agent_id.is_empty()).then(|| agent_id.clone()),
                locator: format!("claude:transcript.jsonl@{generation}#{end_offset}"),
                parent: None,
                parent_ref: None,
                depth: None,
                resolution: ChildResolution::Unresolved,
                evidence: None,
            }
        };
        let source_ref = format!("claude:transcript@{generation}#{end_offset}");
        for event in child_events_for(&record, value) {
            events
                .emit(WsEvent::ChildAgentUpdate {
                    session_id: session.clone(),
                    child: child.clone(),
                    kind: event.kind,
                    source_ref: source_ref.clone(),
                    data: event.data,
                })
                .await;
            stats.child_events += 1;
        }
    }
}

/// Declare bounded unknown coverage for a child file under the current epoch, carrying the
/// cursor's own resolution rather than one inferred from the file name. `Applied` means the
/// declaration is visible on the lane; `Refused` means the lane bounds refused it and the caller
/// must not record the cursor as halted under this epoch.
async fn declare_child_coverage(
    store: &Store,
    session: &SessionId,
    epoch: &str,
    cursor: &ClaudeChildCursor,
    reason: &str,
    extra: Value,
) -> Result<LaneMutation, NexusError> {
    let resolution = if cursor.resolution == "root_verified" {
        ChildResolution::RootVerified
    } else {
        ChildResolution::Unresolved
    };
    let evidence =
        (resolution == ChildResolution::RootVerified).then_some("subagents_dir+sessionId+agentId");
    let child = claude_child_stream(
        &cursor.native_session_id,
        &cursor.agent_id,
        &cursor.generation,
        resolution,
        evidence,
    );
    let mut coverage = json!({
        "generation": cursor.generation,
        "from": cursor.cursor,
        "unknown_before": true,
        "reason": reason,
    });
    if let (Some(target), Some(source)) = (coverage.as_object_mut(), extra.as_object()) {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
    ChildStreamEvents::new(store)
        .set_coverage(
            session,
            epoch,
            &child,
            &coverage,
            &store.child_stream_bounds(),
        )
        .await
}

/// Tail every owned session's `subagents/agent-*.jsonl` into the child lane behind durable,
/// epoch-scoped cursors.
///
/// Discovery is a rotating window: each pass inspects at most [`MAX_CHILD_DIR_ENTRIES`]
/// directory entries starting where the previous window ended (durable per owned session),
/// wrapping to the start once the directory is exhausted, and registers each new file with an
/// untouched cursor. Complete coverage relies on the directory's readdir order being stable
/// while it is unchanged; entries the window skips are walked but not inspected. Scheduling is
/// one SQL page: the [`MAX_CHILD_FILES_PER_PASS`] cursors served least recently, each marked
/// as attempted before any file work so a failing file loses its place in the rotation. Per
/// file the pass reads at most [`MAX_CHILD_BYTES_PER_FILE_PASS`] bytes and
/// [`MAX_CHILD_RECORDS_PER_FILE_PASS`] records from one opened handle, never re-reading a
/// consumed prefix. Continuity is judged on that same handle before the cursor is used:
/// epoch, length, first-record generation and the anchor record; any failure declares bounded
/// unknown coverage with the exact reason and halts the cursor, adopting no replay or skip
/// policy. A halted cursor re-declares its coverage under every later epoch so a fresh
/// volatile lane always shows it.
///
/// Scheduling covers every cursor registered under this runtime, including cursors of roots
/// the runtime owned earlier (`owned_sessions` only drives discovery for this pass). Those
/// cursors stay eligible and their events keep the root recorded on the cursor; the pass never
/// relabels them as the current root. This is broader than current-root-only tailing.
async fn forward_child_streams(
    store: &Store,
    session: &SessionId,
    events: &dyn EventSink,
    owned_sessions: &[String],
    transcript_path: &Path,
    stats: &mut ClaudeForwarderStats,
) -> Result<(), NexusError> {
    let Some(project_dir) = transcript_path.parent() else {
        return Ok(());
    };
    let epoch = match DaemonState::new(store).boot_epoch().await? {
        Some(epoch) if !epoch.is_empty() => epoch,
        _ => return Ok(()),
    };
    let repo = ClaudeChildStreamsRepo::new(store);

    // Discovery window per owned session: register new files, never inspect more than the
    // window, remember where the next pass continues.
    for native in owned_sessions {
        let dir = project_dir.join(native).join("subagents");
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let skip = repo.discovery_skip(session, native).await?;
        let mut inspected = 0usize;
        let mut exhausted = true;
        let mut found: Vec<(String, PathBuf)> = Vec::new();
        for entry in entries.skip(skip) {
            if inspected == MAX_CHILD_DIR_ENTRIES {
                exhausted = false;
                break;
            }
            inspected += 1;
            let Ok(entry) = entry else {
                continue;
            };
            if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            let path = entry.path();
            let Some(agent_id) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix("agent-"))
                .and_then(|n| n.strip_suffix(".jsonl"))
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
            else {
                continue;
            };
            found.push((agent_id, path));
        }
        let next_skip = if exhausted { 0 } else { skip + inspected };
        repo.set_discovery_skip(session, native, next_skip).await?;
        let ids: Vec<String> = found.iter().map(|(id, _)| id.clone()).collect();
        let known = repo.known_agent_ids(session, native, &ids).await?;
        for (agent_id, path) in found {
            if known.contains(&agent_id) {
                continue;
            }
            repo.insert_if_absent(&ClaudeChildCursor {
                runtime_id: session.clone(),
                native_session_id: native.clone(),
                agent_id,
                path,
                epoch: epoch.clone(),
                generation: String::new(),
                cursor: 0,
                halted: false,
                halt_reason: String::new(),
                resolution: "unresolved".into(),
                served_at: 0,
                anchor_offset: 0,
                anchor_uuid: String::new(),
                updated_at: 0,
            })
            .await?;
        }
    }

    // Scheduling: one page of the least recently served cursors, each marked as attempted
    // before any file work.
    let served_at = nexus_common::now();
    let batch = repo
        .least_recently_served(session, MAX_CHILD_FILES_PER_PASS)
        .await?;
    for cursor in &batch {
        repo.mark_attempt(
            session,
            &cursor.native_session_id,
            &cursor.agent_id,
            served_at,
        )
        .await?;
    }
    for mut cursor in batch {
        cursor.served_at = served_at;
        let agent_id = cursor.agent_id.clone();
        // One file's failure never blocks the others in this pass; it is counted and retried.
        if let Err(error) =
            serve_child_file(store, &repo, session, events, &epoch, cursor, stats).await
        {
            stats.child_errors += 1;
            tracing::warn!(
                target: "nexus::claude_child_streams",
                session = %session, agent_id = %agent_id, error = %error,
                "Claude child file pass failed; retried next pass"
            );
        }
    }
    Ok(())
}

/// One subagent file's share of a pass: re-declare a halted cursor under a new epoch, judge
/// continuity on one opened handle, then forward a bounded slice of records from that same
/// handle and persist the cursor.
async fn serve_child_file(
    store: &Store,
    repo: &ClaudeChildStreamsRepo<'_>,
    session: &SessionId,
    events: &dyn EventSink,
    epoch: &str,
    mut cursor: ClaudeChildCursor,
    stats: &mut ClaudeForwarderStats,
) -> Result<(), NexusError> {
    let native = cursor.native_session_id.clone();
    let agent_id = cursor.agent_id.clone();
    let path = cursor.path.clone();

    // A halted cursor re-declares its coverage under every later epoch; it never advances.
    if cursor.halted {
        if cursor.epoch != epoch {
            let reason = cursor.halt_reason.clone();
            if declare_child_coverage(store, session, epoch, &cursor, &reason, json!({})).await?
                == LaneMutation::Applied
            {
                cursor.epoch = epoch.to_string();
            }
        }
        repo.upsert(&cursor).await?;
        return Ok(());
    }

    // The handle used for every check and for the read: a replacement of the path after this
    // point is not seen until the next pass opens the new file.
    let mut file = match open_regular_child_file(&path)? {
        ChildOpen::Regular(file) => file,
        ChildOpen::NotRegular(kind) => {
            // The registered path no longer names a regular file (a FIFO, a symlink, a
            // directory): rejected at the open boundary without blocking, and declared.
            let reason = "source_not_regular";
            if declare_child_coverage(
                store,
                session,
                epoch,
                &cursor,
                reason,
                json!({ "kind": kind }),
            )
            .await?
                == LaneMutation::Applied
            {
                cursor.epoch = epoch.to_string();
                cursor.halted = true;
                cursor.halt_reason = reason.to_string();
            }
            repo.upsert(&cursor).await?;
            return Ok(());
        }
    };
    let file_len = file
        .metadata()
        .map_err(|e| {
            NexusError::Internal(format!("stat claude child file {}: {e}", path.display()))
        })?
        .len() as i64;
    let consumed = cursor.cursor > 0 || !cursor.generation.is_empty();

    // A cursor that never consumed anything has nothing behind it to replay or skip: it simply
    // continues under the current epoch.
    if !consumed && cursor.epoch != epoch {
        cursor.epoch = epoch.to_string();
    }

    // Continuity, in this order: a consumed cursor from another epoch, a consumed file that got
    // shorter, then the first record, then the anchor. A pending first line only defers a
    // cursor that never consumed anything.
    let generation = first_record_generation(&mut file)?;
    let halt: Option<(&str, Value)> = if consumed && cursor.epoch != epoch {
        Some(("daemon_restart_policy_undecided", json!({})))
    } else if consumed && file_len < cursor.cursor {
        Some(("source_truncated", json!({ "current_len": file_len })))
    } else {
        match &generation {
            Generation::Pending if !consumed => {
                // Empty, or the first line is still being written: nothing to judge yet.
                repo.upsert(&cursor).await?;
                return Ok(());
            }
            Generation::Pending => Some((
                "source_rewritten",
                json!({ "first_line": "incomplete", "current_len": file_len }),
            )),
            Generation::Unverifiable => Some(("generation_unverifiable", json!({}))),
            Generation::Ready(current) if consumed && &cursor.generation != current => Some((
                "source_replaced",
                json!({ "previous_generation": cursor.generation, "current_generation": current }),
            )),
            Generation::Ready(_) if consumed && !anchor_intact(&mut file, &cursor)? => Some((
                "source_rewritten",
                json!({ "anchor_offset": cursor.anchor_offset, "anchor_uuid": cursor.anchor_uuid }),
            )),
            Generation::Ready(_) => None,
        }
    };
    if let Some((reason, extra)) = halt {
        if declare_child_coverage(store, session, epoch, &cursor, reason, extra).await?
            == LaneMutation::Applied
        {
            cursor.epoch = epoch.to_string();
            cursor.halted = true;
            cursor.halt_reason = reason.to_string();
        } else {
            tracing::warn!(
                target: "nexus::claude_child_streams",
                session = %session, agent_id = %agent_id, reason,
                "child coverage declaration refused by the lane bounds; retried next pass"
            );
        }
        repo.upsert(&cursor).await?;
        return Ok(());
    }
    if let Generation::Ready(current) = &generation {
        if cursor.generation.is_empty() {
            cursor.generation = current.clone();
        }
    }

    let read = read_child_records(
        &mut file,
        cursor.cursor,
        MAX_CHILD_BYTES_PER_FILE_PASS,
        MAX_CHILD_RECORDS_PER_FILE_PASS,
    )?;
    if read.records.is_empty()
        && read.malformed_at.is_none()
        && file_len - cursor.cursor >= MAX_CHILD_BYTES_PER_FILE_PASS as i64
    {
        // A single record larger than one pass's read budget can never be forwarded; say so
        // instead of stalling silently.
        let reason = "record_exceeds_pass_budget";
        if declare_child_coverage(
            store,
            session,
            epoch,
            &cursor,
            reason,
            json!({ "budget": MAX_CHILD_BYTES_PER_FILE_PASS }),
        )
        .await?
            == LaneMutation::Applied
        {
            cursor.halted = true;
            cursor.halt_reason = reason.to_string();
        }
        repo.upsert(&cursor).await?;
        return Ok(());
    }
    // Bounded detection of an in-place change between the checks and the read on the same
    // handle: the generation and the anchor must still agree, and the file must still reach
    // the end of what was read. Otherwise nothing is emitted and the next pass judges it.
    let still_same = first_record_generation(&mut file)? == generation
        && anchor_intact(&mut file, &cursor)?
        && file
            .metadata()
            .map(|m| m.len() as i64 >= read.end)
            .unwrap_or(false);
    if !still_same {
        repo.upsert(&cursor).await?;
        return Ok(());
    }

    let mut record_start = cursor.cursor;
    for (value, end_offset) in &read.records {
        let end = *end_offset as i64;
        if let Some(record) = parse_transcript_value(value) {
            let agrees = record
                .sidechain
                .as_ref()
                .and_then(|mark| mark.agent_id.as_deref())
                == Some(agent_id.as_str())
                && record.session_id.as_deref() == Some(native.as_str());
            let (resolution, evidence) = if agrees {
                (
                    ChildResolution::RootVerified,
                    Some("subagents_dir+sessionId+agentId"),
                )
            } else {
                (ChildResolution::Unresolved, None)
            };
            let child =
                claude_child_stream(&native, &agent_id, &cursor.generation, resolution, evidence);
            cursor.resolution = if agrees {
                "root_verified"
            } else {
                "unresolved"
            }
            .into();
            let source_ref = format!("claude:{agent_id}@{}#{end}", cursor.generation);
            for event in child_events_for(&record, value) {
                events
                    .emit(WsEvent::ChildAgentUpdate {
                        session_id: session.clone(),
                        child: child.clone(),
                        kind: event.kind,
                        source_ref: source_ref.clone(),
                        data: event.data,
                    })
                    .await;
                stats.child_events += 1;
            }
        }
        cursor.anchor_offset = record_start;
        cursor.anchor_uuid = value
            .get("uuid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        record_start = end;
    }
    cursor.cursor = read.end.max(cursor.cursor);
    if let Some(offset) = read.malformed_at {
        // What parsed before the malformed record was forwarded; the file cannot be followed
        // past it, and that is declared rather than left as a silent stall.
        let reason = "source_malformed";
        if declare_child_coverage(
            store,
            session,
            epoch,
            &cursor,
            reason,
            json!({ "malformed_at": offset }),
        )
        .await?
            == LaneMutation::Applied
        {
            cursor.halted = true;
            cursor.halt_reason = reason.to_string();
        }
    }
    repo.upsert(&cursor).await?;
    Ok(())
}

fn concat_assistant_text(items: &[AssistantText]) -> String {
    items
        .iter()
        .map(|item| item.text.as_str())
        .collect::<Vec<_>>()
        .join("")
}

#[derive(Debug, Clone, Copy)]
enum CursorKind {
    Hook,
    Transcript,
    MessageDelta,
}

fn cursor(state: Option<&ClaudeRuntimeState>, kind: CursorKind) -> i64 {
    let Some(state) = state else {
        return 0;
    };
    match kind {
        CursorKind::Hook => state.hook_cursor,
        CursorKind::Transcript => state.transcript_cursor,
        CursorKind::MessageDelta => state.message_delta_cursor,
    }
}

fn read_new_json_values(path: &Path, cursor: i64) -> Result<(Vec<Value>, i64), NexusError> {
    let read = read_json_records(path, cursor)?;
    Ok((
        read.values.into_iter().map(|(value, _)| value).collect(),
        read.cursor,
    ))
}

struct JsonRecords {
    values: Vec<(Value, u64)>,
    cursor: i64,
    file_len: Option<u64>,
    complete: bool,
    source: Option<ClaudeTranscriptRead>,
}

fn read_json_records(path: &Path, cursor: i64) -> Result<JsonRecords, NexusError> {
    let source = match ClaudeTranscriptRead::open(path) {
        Ok(source) => source,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(JsonRecords {
                values: Vec::new(),
                cursor: cursor.max(0),
                file_len: None,
                complete: false,
                source: None,
            });
        }
        Err(e) => {
            return Err(NexusError::Internal(format!(
                "read claude native bridge file {}: {e}",
                path.display()
            )));
        }
    };
    let bytes = source.bytes();
    let start = usize::try_from(cursor.max(0))
        .ok()
        .map(|idx| idx.min(bytes.len()))
        .unwrap_or(0);
    let mut values = Vec::new();
    let mut consumed = 0;
    let mut complete = cursor.max(0) as u64 <= bytes.len() as u64;
    let mut stream = serde_json::Deserializer::from_slice(&bytes[start..]).into_iter::<Value>();
    while let Some(next) = stream.next() {
        match next {
            Ok(value) => {
                consumed = stream.byte_offset();
                values.push((value, (start + consumed) as u64));
            }
            Err(_) => {
                complete = false;
                break;
            }
        }
    }
    Ok(JsonRecords {
        values,
        cursor: start as i64 + consumed as i64,
        file_len: Some(bytes.len() as u64),
        complete,
        source: Some(source),
    })
}

fn transcript_end_offset(path: &Path) -> Result<i64, NexusError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(i64::try_from(metadata.len()).unwrap_or(i64::MAX)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(NexusError::Internal(format!(
            "stat claude transcript file {}: {e}",
            path.display()
        ))),
    }
}

/// The transcript path currently defaults inside the bridge directory.
pub fn default_transcript_path(paths: &ClaudeNativeBridgePaths) -> PathBuf {
    paths.bridge_dir.join("transcript.jsonl")
}
