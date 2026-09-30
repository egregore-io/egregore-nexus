//! Drain-once batcher (spec §2.3.1). One wake = one bounded store page = one [`NexusBatch`],
//! partitioned into the message store; the drop carries previews + ids so headed/MCP agents can
//! call the `read` tool and CLI agents can run `nexus read <id>` for a truncated body. A one-row
//! lookahead reports that more mail is waiting without loading the recipient's full backlog. This
//! mirrors AionCore's drain-once + split-ack.

use nexus_common::Config;
use nexus_contracts::ids::{MessageId, SessionId};
use nexus_contracts::message::Message;
use nexus_contracts::{BatchCounts, BatchMessage, NexusBatch, Scope};
use nexus_store::repos::inbox::Inbox;

use crate::error::DispatchResult;

/// Builds drain-once [`NexusBatch`]es from a recipient's pending in-flight queue.
pub struct InboxDrainer;

impl InboxDrainer {
    /// Drain a recipient's pending queue **once** into a single [`NexusBatch`].
    ///
    /// - asks the store for at most `limit + 1` pending rows, oldest first (the configured page
    ///   plus one lookahead); the lookahead stays in-flight and becomes a synthetic tail row that
    ///   identifies the next waiting message without counting or loading the remaining backlog;
    /// - partitions into DMs (`Scope::Dm`) and threads (`Scope::Thread`/`Scope::Topic`);
    /// - caps each body at `preview_chars`, marking it `truncated=true` with a marker that leads
    ///   with the MCP `read` tool and also names the CLI `nexus read <id>` fallback.
    /// - computes [`BatchCounts`] and the split-ack id vectors.
    pub async fn drain_once(
        inbox: &Inbox<'_>,
        session: &SessionId,
        project: &str,
        limit: u32,
        preview_chars: u32,
    ) -> DispatchResult<NexusBatch> {
        // Fetch exactly one page plus one lookahead. The lookahead is enough to tell the recipient
        // that another page exists without materializing its entire durable queue.
        let pending = inbox
            .pending_for(session, project, limit.saturating_add(1))
            .await?;

        Ok(Self::batch_from_rows(&pending, limit, preview_chars))
    }

    /// Drain exactly the rows already advanced through the latest notification boundary.
    /// Rows enqueued concurrently after `mark_notified` remain pending for the next drain.
    pub async fn drain_notified_once(
        inbox: &Inbox<'_>,
        session: &SessionId,
        project: &str,
        limit: u32,
        preview_chars: u32,
    ) -> DispatchResult<NexusBatch> {
        let notified = inbox
            .notified_for(session, project, limit.saturating_add(1))
            .await?;
        Ok(Self::batch_from_rows(&notified, limit, preview_chars))
    }

    fn batch_from_rows(
        pending: &[(String, Message)],
        limit: u32,
        preview_chars: u32,
    ) -> NexusBatch {
        let overflow = pending.len() as u32 > limit;
        let kept_n = if overflow {
            limit as usize
        } else {
            pending.len()
        };
        let kept = &pending[..kept_n];
        let lookahead = pending.get(kept_n);

        let mut dms = Vec::new();
        let mut threads = Vec::new();
        let mut dm_message_ids = Vec::new();
        let mut thread_message_ids = Vec::new();
        let mut message_ids = Vec::new();

        for (_in_flight_id, msg) in kept {
            let bm = to_batch_message(msg, preview_chars);
            message_ids.push(msg.id.clone());
            match msg.scope {
                Scope::Dm => {
                    dm_message_ids.push(msg.id.clone());
                    dms.push(bm);
                }
                Scope::Thread | Scope::Topic => {
                    thread_message_ids.push(msg.id.clone());
                    threads.push(bm);
                }
            }
        }

        // Overflow tail: a single synthetic note names only the next waiting message. It stays
        // in-flight for the next drain and is NOT added to any split-ack vector.
        if let Some((_, next)) = lookahead {
            let note = BatchMessage {
                id: MessageId("".into()),
                from: "nexus".into(),
                kind: nexus_contracts::Kind::Notification,
                scope: Scope::Dm,
                thread: None,
                topic: None,
                body: format!("… more messages waiting (next id: {})", next.id),
                truncated: false,
            };
            dms.push(note);
        }

        let counts = BatchCounts {
            dms: dm_message_ids.len() as u32,
            thread: thread_message_ids.len() as u32,
            total: (dm_message_ids.len() + thread_message_ids.len()) as u32,
        };

        NexusBatch {
            counts,
            dms,
            threads,
            dm_message_ids,
            thread_message_ids,
            message_ids,
        }
    }

    /// Convenience: drain using the [`Config`] caps (`drain_limit`, `msg_preview_chars`).
    pub async fn drain_with_config(
        inbox: &Inbox<'_>,
        session: &SessionId,
        project: &str,
        config: &Config,
    ) -> DispatchResult<NexusBatch> {
        Self::drain_once(
            inbox,
            session,
            project,
            config.drain_limit,
            config.msg_preview_chars,
        )
        .await
    }
}

/// Build one [`BatchMessage`] from a durable [`Message`], applying the preview cap.
fn to_batch_message(msg: &Message, preview_chars: u32) -> BatchMessage {
    let (body, truncated) = preview(&msg.body, preview_chars, &msg.id);
    BatchMessage {
        id: msg.id.clone(),
        from: msg.provenance.from.clone(),
        kind: msg.provenance.kind,
        scope: msg.scope,
        thread: msg.provenance.thread.clone(),
        topic: msg.provenance.topic.clone(),
        body,
        truncated,
    }
}

/// Apply the per-message preview budget. If `body` exceeds `preview_chars` (counted in `char`s,
/// not bytes), truncate to that many chars and append the read marker; else return it unchanged.
fn preview(body: &str, preview_chars: u32, id: &MessageId) -> (String, bool) {
    let cap = preview_chars as usize;
    if body.chars().count() <= cap {
        return (body.to_string(), false);
    }
    let head: String = body.chars().take(cap).collect();
    let marker =
        format!("[truncated — fetch full body: MCP `read` tool id={id} or CLI `nexus read {id}`]");
    (format!("{head} … {marker}"), true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_contracts::enums::{Kind, Scope};
    use nexus_contracts::ids::ProjectId;
    use nexus_contracts::message::Provenance;
    use nexus_store::repos::messages::Messages;
    use nexus_store::Store;

    async fn migrated() -> Store {
        let s = Store::open(":memory:").await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    fn dm(id: &str, from: &str, body: &str) -> Message {
        Message {
            id: MessageId(id.into()),
            project: ProjectId("p_demo".into()),
            from: from.into(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: body.into(),
            summary: None,
            provenance: Provenance {
                from: from.into(),
                kind: Kind::Agent,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: 0,
        }
    }

    fn dm_at(id: &str, created_at: i64) -> Message {
        let mut message = dm(id, "ben", "hi");
        message.created_at = created_at;
        message
    }

    async fn seed_large_backlog(store: &Store, session: &SessionId, count: u32) {
        let sql = format!(
            r#"
            WITH RECURSIVE seq(n) AS (
                VALUES(1)
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < {count}
            )
            INSERT INTO messages (
                message_id, from_name, kind, to_name, body, provenance, project, created_at
            )
            SELECT
                printf('m_%06d', n),
                'ben',
                'dm',
                'ana',
                'hi',
                '{{"from":"ben","kind":"agent","thread":null,"topic":null,"stamp":null}}',
                'p_demo',
                n
            FROM seq;

            WITH RECURSIVE seq(n) AS (
                VALUES(1)
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < {count}
            )
            INSERT INTO in_flight (
                in_flight_id, message_id, recipient_session, state
            )
            SELECT
                printf('if_%06d', n),
                printf('m_%06d', n),
                '{session}',
                'pending'
            FROM seq;
            "#,
            session = session.0,
        );
        store.conn.execute_transactional_batch(&sql).await.unwrap();
    }

    fn thread_msg(id: &str, from: &str, thread: &str, body: &str) -> Message {
        let mut m = dm(id, from, body);
        m.scope = Scope::Thread;
        m.thread = Some(nexus_contracts::ids::ThreadId(format!("t_{thread}")));
        m.provenance.thread = Some(thread.into());
        m
    }

    #[tokio::test]
    async fn drains_two_dms_and_one_long_thread_with_truncation() {
        let store = migrated().await;
        let session = SessionId("s_ana".into());
        let msgs = Messages::new(&store);
        let inbox = Inbox::new(&store);

        msgs.insert(&dm("m_01", "ana", "take the auth refactor today?"))
            .await
            .unwrap();
        msgs.insert(&dm("m_02", "ben", "rebase before you start."))
            .await
            .unwrap();
        let long_body = "X".repeat(2000);
        msgs.insert(&thread_msg("m_03", "dylan", "backend", &long_body))
            .await
            .unwrap();

        for id in ["m_01", "m_02", "m_03"] {
            inbox
                .enqueue(&MessageId(id.into()), &session)
                .await
                .unwrap();
        }

        let batch = InboxDrainer::drain_once(&inbox, &session, "p_demo", 50, 800)
            .await
            .unwrap();

        assert_eq!(batch.counts.dms, 2);
        assert_eq!(batch.counts.thread, 1);
        assert_eq!(batch.counts.total, 3);
        assert_eq!(batch.dms.len(), 2);
        assert_eq!(batch.threads.len(), 1);

        let t = &batch.threads[0];
        assert!(t.truncated, "long thread body must be truncated");
        assert!(t.body.contains(
            "[truncated — fetch full body: MCP `read` tool id=m_03 or CLI `nexus read m_03`]"
        ));
        assert_eq!(t.thread.as_deref(), Some("backend"));

        // split-ack vectors
        assert_eq!(batch.dm_message_ids.len(), 2);
        assert_eq!(batch.thread_message_ids, vec![MessageId("m_03".into())]);
        assert_eq!(batch.message_ids.len(), 3);

        // short DMs untouched
        assert!(!batch.dms[0].truncated);
    }

    #[tokio::test]
    async fn empty_queue_drains_empty_batch() {
        let store = migrated().await;
        let session = SessionId("s_nobody".into());
        let inbox = Inbox::new(&store);
        let batch = InboxDrainer::drain_once(&inbox, &session, "p_demo", 50, 800)
            .await
            .unwrap();
        assert_eq!(batch.counts.total, 0);
        assert!(batch.dms.is_empty() && batch.threads.is_empty());
    }

    #[tokio::test]
    async fn injection_drain_excludes_rows_arriving_after_notification_boundary() {
        let store = migrated().await;
        let session = SessionId("s_notification_boundary".into());
        let inbox = Inbox::new(&store);
        let first = MessageId("m_boundary_first".into());
        let late = MessageId("m_boundary_late".into());

        Messages::new(&store)
            .insert(&dm(&first.0, "ben", "already notified"))
            .await
            .unwrap();
        Messages::new(&store)
            .insert(&dm(&late.0, "ben", "arrived late"))
            .await
            .unwrap();
        inbox.enqueue(&first, &session).await.unwrap();
        inbox.mark_notified(&session).await.unwrap();
        inbox.enqueue(&late, &session).await.unwrap();

        let batch = InboxDrainer::drain_notified_once(&inbox, &session, "p_demo", 50, 800)
            .await
            .unwrap();
        assert_eq!(batch.message_ids, vec![first]);

        inbox.mark_notified(&session).await.unwrap();
        let next = InboxDrainer::drain_notified_once(&inbox, &session, "p_demo", 50, 800)
            .await
            .unwrap();
        assert_eq!(
            next.message_ids,
            vec![MessageId("m_boundary_first".into()), late]
        );
    }

    #[tokio::test]
    async fn per_drop_cap_summarizes_bounded_lookahead() {
        let store = migrated().await;
        let session = SessionId("s_flood".into());
        let msgs = Messages::new(&store);
        let inbox = Inbox::new(&store);
        for i in 0..5 {
            let id = format!("m_{i}");
            msgs.insert(&dm(&id, "ben", "hi")).await.unwrap();
            inbox.enqueue(&MessageId(id), &session).await.unwrap();
        }
        // cap at 3 → 3 kept + a tail note derived only from the one-row lookahead.
        let batch = InboxDrainer::drain_once(&inbox, &session, "p_demo", 3, 800)
            .await
            .unwrap();
        assert_eq!(batch.counts.total, 3, "only capped count is acked-eligible");
        assert_eq!(batch.message_ids.len(), 3);
        // tail note is an extra dms entry beyond the 3 real DMs.
        let tail = batch.dms.last().expect("bounded overflow tail");
        assert_eq!(tail.body, "… more messages waiting (next id: m_3)");
        assert!(
            !tail.body.contains("m_4"),
            "overflow summary must not enumerate rows beyond lookahead"
        );
    }

    #[tokio::test]
    async fn large_backlog_with_oversized_tail_fetches_only_limit_plus_one_rows() {
        let store = migrated().await;
        let session = SessionId("s_large".into());
        seed_large_backlog(&store, &session, 100_000).await;

        // If drain_once asks the repo for more than the configured 10 rows plus one lookahead,
        // decoding reaches this one-megabyte poison row and the drain fails. Keeping it just
        // beyond the lookahead makes both the row and payload fetch ceilings observable without
        // a test-only query spy.
        store
            .conn
            .execute(
                "UPDATE messages SET body = replace(hex(zeroblob(524288)), '00', 'XX'), \
                 provenance = 'not-json' WHERE message_id = 'm_000012'",
                (),
            )
            .await
            .unwrap();

        let batch = InboxDrainer::drain_once(&Inbox::new(&store), &session, "p_demo", 10, 800)
            .await
            .expect("rows beyond the one-row lookahead must not be fetched");

        assert_eq!(
            batch.message_ids,
            (1..=10)
                .map(|n| MessageId(format!("m_{n:06}")))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            batch.dms.last().expect("overflow tail").body,
            "… more messages waiting (next id: m_000011)"
        );
    }

    #[tokio::test]
    async fn consecutive_pages_are_exactly_oldest_first_after_split_ack() {
        let store = migrated().await;
        let session = SessionId("s_pages".into());
        let msgs = Messages::new(&store);
        let inbox = Inbox::new(&store);

        for n in (1..=7).rev() {
            let id = format!("m_{n:03}");
            msgs.insert(&dm_at(&id, n)).await.unwrap();
            inbox.enqueue(&MessageId(id), &session).await.unwrap();
        }
        inbox.mark_notified(&session).await.unwrap();

        for expected in [[1, 2, 3].as_slice(), [4, 5, 6].as_slice(), [7].as_slice()] {
            let batch = InboxDrainer::drain_once(&inbox, &session, "p_demo", 3, 800)
                .await
                .unwrap();
            let expected_ids = expected
                .iter()
                .map(|n| MessageId(format!("m_{n:03}")))
                .collect::<Vec<_>>();
            assert_eq!(batch.message_ids, expected_ids);
            assert_eq!(batch.dm_message_ids, batch.message_ids);
            assert!(batch.thread_message_ids.is_empty());

            for message_id in &batch.dm_message_ids {
                assert_eq!(inbox.mark_injecting(message_id, &session).await.unwrap(), 1);
                assert_eq!(inbox.mark_delivered(message_id, &session).await.unwrap(), 1);
            }
            inbox
                .ack_many(&batch.dm_message_ids, &session)
                .await
                .unwrap();
        }

        assert_eq!(
            InboxDrainer::drain_once(&inbox, &session, "p_demo", 3, 800)
                .await
                .unwrap()
                .counts
                .total,
            0
        );
    }
}
