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
use crate::native::model_reporting::ClaudeTranscriptRead;
use crate::native::transcript::{
    parse_hook_record, parse_transcript_value, AssistantText, ClaudeHookRecord, ClaudeToolUpdate,
    TranscriptRecord,
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
    let transcript_values: Vec<_> = transcript_read
        .values
        .into_iter()
        .map(|(value, _)| value)
        .collect();
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
