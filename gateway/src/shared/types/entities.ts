// Contract entities — re-exported from the generated mirror, plus client-only
// VIEW-MODELS that *extend* a contract type. View-models are never sent to the
// daemon and are never DB rows; the Drizzle read models live separately in
// @drizzle and are never imported here.
//
// NEVER redefine a contract wire type — re-export + extend only.
import type {
  Message,
  MemberSummary,
  ThreadSummary,
  TopicSummary,
  Project,
  Whoami,
  Provenance,
  ProvenanceStamp,
  SearchHit,
  HistoryEntry,
  BatchMessage,
  NexusBatch,
} from "./contracts.gen";

export type {
  Message,
  MemberSummary,
  ThreadSummary,
  TopicSummary,
  Project,
  Whoami,
  Provenance,
  ProvenanceStamp,
  SearchHit,
  HistoryEntry,
  BatchMessage,
  NexusBatch,
};

/**
 * Client view-model: a server `Message` plus optimistic-UI flags.
 *
 * - `pending` — appended locally with a temp id, not yet echoed by the daemon.
 * - `failed`  — the send errored; the UI offers retry.
 * - `tempId`  — the client-generated id used to reconcile against the
 *   `message.created` WS echo (which carries the real `messageId`).
 *
 * Never sent to the daemon, never a DB row.
 */
export type MessageVM = Message & {
  pending?: boolean;
  failed?: boolean;
  tempId?: string;
};
