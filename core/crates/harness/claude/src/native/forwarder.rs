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
use nexus_contracts::events::WsEvent;
use nexus_contracts::ids::SessionId;
use nexus_contracts::ports::EventSink;
use nexus_store::repos::ProducerIdentities;
use nexus_store::Store;
use nexus_transcript::ToolCallObservation;
use serde_json::Value;

use crate::native::bridge::ClaudeNativeBridgePaths;
use crate::native::codec::{
    compaction_event, text_event, tool_event, tool_observation, turn_boundary_event,
    user_input_event, ClaudeNativeEvent,
};
use crate::native::message_delta::parse_message_display_value;
use crate::native::transcript::{
    parse_transcript_value, AssistantText, ClaudeToolUpdate, TranscriptRecord,
};
use crate::storage::{ClaudeRuntimeState, ClaudeRuntimeStateRepo};

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
    let repo = ClaudeRuntimeStateRepo::new(&store);
    let state = repo.find_by_runtime_id(&session).await?;
    let state_ref = state.as_ref();
    let mut stats = ClaudeForwarderStats::default();

    let producer_ids = ProducerIdentities::new(&store);

    let (message_values, message_cursor) = read_new_json_values(
        &paths.message_delta_log_path,
        cursor(state_ref, CursorKind::MessageDelta),
    )?;

    let (hook_values, hook_cursor) =
        read_new_json_values(&paths.hook_log_path, cursor(state_ref, CursorKind::Hook))?;

    let (transcript_path, transcript_start_cursor, transcript_rebound) = resolve_transcript_tail(
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
    count_hook_session_starts(&hook_values, &mut stats);
    emit_hook_user_inputs(&session, events.as_ref(), &hook_values, &mut stats).await;
    emit_hook_tool_observations(&session, tool_observations.as_deref(), &hook_values);
    let StreamedMessages {
        text: message_text,
        completed_message_ids,
    } = emit_message_deltas(&session, events.as_ref(), &message_values, &mut stats).await;
    for id in completed_message_ids {
        producer_ids.admit_streamed(&session.0, &id).await?;
    }

    let (transcript_values, transcript_cursor) =
        read_new_json_values(&transcript_path, transcript_start_cursor)?;
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

async fn resolve_transcript_tail(
    repo: &ClaudeRuntimeStateRepo<'_>,
    session: &SessionId,
    state: Option<&ClaudeRuntimeState>,
    paths: &ClaudeNativeBridgePaths,
    message_values: &[Value],
    hook_values: &[Value],
) -> Result<(PathBuf, i64, bool), NexusError> {
    let default_path = default_transcript_path(paths);
    let stored_path = state
        .and_then(|state| state.transcript_path.clone())
        .unwrap_or_else(|| default_path.clone());
    let mut discovered = latest_transcript_path(message_values)
        .or_else(|| latest_transcript_path(hook_values))
        .map(PathBuf::from);
    let mut discovered_session_ids = distinct_claude_session_ids(message_values, hook_values);
    if discovered_session_ids.len() > 1 {
        return Err(NexusError::Ambiguous(format!(
            "mixed native Claude session ids in one bridge batch: {}",
            discovered_session_ids.join(", ")
        )));
    }
    let batch_discovered_session_id = discovered_session_ids.first().cloned();

    if should_reconcile_from_full_hook_log(
        state,
        &stored_path,
        batch_discovered_session_id.as_deref(),
    ) {
        let (all_hook_values, _) = read_new_json_values(&paths.hook_log_path, 0)?;
        let hook_session_ids = distinct_claude_session_ids(&[], &all_hook_values);
        if hook_session_ids.len() > 1 {
            return Err(NexusError::Ambiguous(format!(
                "mixed native Claude session ids in bridge hook log: {}",
                hook_session_ids.join(", ")
            )));
        }
        if let Some(hook_session_id) = hook_session_ids.first() {
            match discovered_session_ids.first() {
                Some(batch_session_id) if batch_session_id != hook_session_id => {
                    return Err(NexusError::Ambiguous(format!(
                        "native Claude session id changed between bridge batch ({batch_session_id}) and full hook log ({hook_session_id})"
                    )));
                }
                Some(_) => {}
                None => discovered_session_ids.push(hook_session_id.clone()),
            }
        }
        if discovered.is_none() && !discovered_session_ids.is_empty() {
            discovered = latest_transcript_path(&all_hook_values).map(PathBuf::from);
        }
    }
    let discovered_session_id = discovered_session_ids.first().map(String::as_str);
    let session_rebound = discovered_session_id
        .zip(state.and_then(|value| value.claude_session_id.as_deref()))
        .is_some_and(|(discovered, stored)| discovered != stored);

    if let Some(claude_session_id) = discovered_session_id {
        repo.set_claude_session(session, claude_session_id, discovered.clone())
            .await?;
    }

    let Some(discovered_path) = discovered else {
        return Ok((
            stored_path,
            cursor(state, CursorKind::Transcript),
            session_rebound,
        ));
    };

    if discovered_path == stored_path {
        return Ok((
            discovered_path,
            cursor(state, CursorKind::Transcript),
            session_rebound,
        ));
    }

    let start_cursor = transcript_end_offset(&discovered_path)?;
    repo.set_transcript_path_and_cursor(session, discovered_path.clone(), start_cursor)
        .await?;
    Ok((discovered_path, start_cursor, true))
}

fn should_reconcile_from_full_hook_log(
    state: Option<&ClaudeRuntimeState>,
    stored_path: &Path,
    discovered_session_id: Option<&str>,
) -> bool {
    let Some(state) = state else {
        return false;
    };
    let stored_transcript_missing = !stored_path.exists();
    let hook_disagrees = discovered_session_id
        .zip(state.claude_session_id.as_deref())
        .is_some_and(|(discovered, stored)| discovered != stored);
    stored_transcript_missing || hook_disagrees
}

fn latest_transcript_path(values: &[Value]) -> Option<String> {
    values
        .iter()
        .rev()
        .find_map(|value| {
            parse_message_display_value(value)
                .and_then(|delta| delta.transcript_path)
                .or_else(|| parse_transcript_value(value).and_then(|record| record.transcript_path))
        })
        .filter(|path| !path.is_empty())
}

fn distinct_claude_session_ids(message_values: &[Value], hook_values: &[Value]) -> Vec<String> {
    let mut out = Vec::new();
    for value in message_values.iter().chain(hook_values) {
        let session_id = parse_message_display_value(value)
            .and_then(|delta| delta.session_id)
            .or_else(|| parse_transcript_value(value).and_then(|record| record.session_id));
        let Some(session_id) = session_id.filter(|session| !session.is_empty()) else {
            continue;
        };
        if !out.iter().any(|existing| existing == &session_id) {
            out.push(session_id);
        }
    }
    out
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
    stats: &mut ClaudeForwarderStats,
) {
    for record in values.iter().filter_map(parse_transcript_value) {
        if let Some(prompt) = &record.user_prompt {
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
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), cursor.max(0)));
        }
        Err(e) => {
            return Err(NexusError::Internal(format!(
                "read claude native bridge file {}: {e}",
                path.display()
            )));
        }
    };
    let start = usize::try_from(cursor.max(0))
        .ok()
        .map(|idx| idx.min(bytes.len()))
        .unwrap_or(0);
    let mut values = Vec::new();
    let mut consumed = 0;
    let mut stream = serde_json::Deserializer::from_slice(&bytes[start..]).into_iter::<Value>();
    while let Some(next) = stream.next() {
        match next {
            Ok(value) => {
                values.push(value);
                consumed = stream.byte_offset();
            }
            Err(_) => break,
        }
    }
    Ok((values, start as i64 + consumed as i64))
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
