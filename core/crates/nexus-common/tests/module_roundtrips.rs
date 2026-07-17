//! Public nexus-common behavior coverage moved out of production modules.
//!
//! These tests mirror the old inline `#[cfg(test)]` blocks so shared utility modules stay
//! focused on runtime code while preserving coverage.

mod credential {
    use nexus_common::hash_runtime_credential;

    #[test]
    fn runtime_credential_hash_is_stable_and_prefixed() {
        assert_eq!(
            hash_runtime_credential("secret"),
            "sha256:2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"
        );
        assert_ne!(
            hash_runtime_credential("secret"),
            hash_runtime_credential("different")
        );
    }
}

mod error {
    use nexus_common::NexusError;
    use nexus_contracts::codes;

    #[test]
    fn maps_to_contract_codes() {
        assert_eq!(
            NexusError::DuplicateName("ben".into())
                .to_contract_error()
                .code,
            codes::DUPLICATE_NAME
        );
        assert_eq!(
            NexusError::NotFound("m_1".into()).to_contract_error().code,
            codes::NOT_FOUND
        );
        assert_eq!(
            NexusError::Unauthorized.to_contract_error().code,
            codes::UNAUTHORIZED
        );
        assert_eq!(
            NexusError::ProjectScope.to_contract_error().code,
            codes::PROJECT_SCOPE_VIOLATION
        );
        assert_eq!(NexusError::Paused.to_contract_error().code, codes::PAUSED);
    }
}

mod ids {
    use nexus_common::{new_message_id, new_project_id, new_session_id, new_thread_id};

    #[test]
    fn ids_are_prefixed_and_unique() {
        let a = new_message_id();
        let b = new_message_id();
        assert!(a.0.starts_with("m_"));
        assert_ne!(a, b);
        assert!(new_session_id().0.starts_with("s_"));
        assert!(new_thread_id().0.starts_with("t_"));
        assert!(new_project_id().0.starts_with("p_"));
    }
}

mod provenance {
    use nexus_common::{
        is_plain_user_dm, parse_outbound, render_batch, render_batch_for, render_nexus,
        OutboundMsg, OutboundTarget,
    };
    use nexus_contracts::{
        BatchCounts, BatchMessage, Kind, MessageId, NexusBatch, Provenance, Scope,
    };

    #[test]
    fn agent_dm_is_wrapped() {
        let p = Provenance {
            from: "ben".into(),
            kind: Kind::Agent,
            thread: None,
            topic: None,
            stamp: None,
        };
        let out = render_nexus(&p, "rebase first");
        assert!(out.starts_with("<nexus from=\"ben\" kind=\"agent\""));
        assert!(out.contains(">rebase first</nexus>"));
        assert!(!is_plain_user_dm(&p));
    }

    #[test]
    fn core_user_dm_is_plain() {
        let p = Provenance {
            from: "etan".into(),
            kind: Kind::Human,
            thread: None,
            topic: None,
            stamp: None,
        };
        assert!(
            is_plain_user_dm(&p),
            "core human DM must be delivered plain (spec §6)"
        );
    }

    #[test]
    fn thread_message_carries_thread_attr() {
        let p = Provenance {
            from: "dylan".into(),
            kind: Kind::Agent,
            thread: Some("backend".into()),
            topic: None,
            stamp: None,
        };
        assert!(render_nexus(&p, "plan").contains("thread=\"backend\""));
    }

    #[test]
    fn render_batch_preserves_multi_message_shell_sensitive_bodies() {
        let bodies = [
            "literal `backticks` $vars \"quotes\" 'single quotes' $(no_subshell)",
            "angle text: use a < b and c > d without splitting the batch",
        ];
        let batch = NexusBatch {
            counts: BatchCounts {
                dms: 1,
                thread: 1,
                total: 2,
            },
            dms: vec![BatchMessage {
                id: MessageId("m_dm".into()),
                from: "ada".into(),
                kind: Kind::Agent,
                scope: Scope::Dm,
                thread: None,
                topic: None,
                body: bodies[0].into(),
                truncated: false,
            }],
            threads: vec![BatchMessage {
                id: MessageId("m_thread".into()),
                from: "ben".into(),
                kind: Kind::Human,
                scope: Scope::Thread,
                thread: Some("backend".into()),
                topic: None,
                body: bodies[1].into(),
                truncated: false,
            }],
            dm_message_ids: vec![MessageId("m_dm".into())],
            thread_message_ids: vec![MessageId("m_thread".into())],
            message_ids: vec![MessageId("m_dm".into()), MessageId("m_thread".into())],
        };

        let rendered = render_batch(&batch);

        assert!(rendered.starts_with("<nexus-batch dms=\"1\" thread=\"1\" total=\"2\">\n"));
        assert!(rendered.ends_with("</nexus-batch>"));
        assert_eq!(rendered.matches("<nexus ").count(), 2);
        assert_eq!(rendered.matches("<nexus-batch").count(), 1);
        assert!(rendered.contains(bodies[0]));
        assert!(rendered.contains(bodies[1]));
        assert!(rendered.contains("id=\"m_dm\""));
        assert!(rendered.contains("id=\"m_thread\" thread=\"backend\""));
    }

    #[test]
    fn rendered_batch_names_sender_target_and_receiver() {
        let batch = NexusBatch {
            counts: BatchCounts {
                dms: 1,
                thread: 1,
                total: 2,
            },
            dms: vec![BatchMessage {
                id: MessageId("m_dm".into()),
                from: "roman".into(),
                kind: Kind::Agent,
                scope: Scope::Dm,
                thread: None,
                topic: None,
                body: "direct note".into(),
                truncated: false,
            }],
            threads: vec![BatchMessage {
                id: MessageId("m_thread".into()),
                from: "bianca".into(),
                kind: Kind::Agent,
                scope: Scope::Thread,
                thread: Some("thread-fresh".into()),
                topic: None,
                body: "@ellie please verify this".into(),
                truncated: false,
            }],
            dm_message_ids: vec![MessageId("m_dm".into())],
            thread_message_ids: vec![MessageId("m_thread".into())],
            message_ids: vec![MessageId("m_dm".into()), MessageId("m_thread".into())],
        };

        let rendered = render_batch_for(&batch, "s_ellie");

        assert!(
            rendered
                .contains("<nexus-batch dms=\"1\" thread=\"1\" total=\"2\" receiver=\"s_ellie\""),
            "batch envelope must name the receiver: {rendered}"
        );
        assert!(
            !rendered.contains("<nexus-instructions>"),
            "per-batch instruction prose is gone — addressee guidance ships once, in the \
register/wake directive: {rendered}"
        );
        assert!(
            rendered.contains("from=\"roman\" kind=\"agent\" scope=\"dm\" id=\"m_dm\" target=\"dm:s_ellie\" receiver=\"s_ellie\""),
            "DM item must name sender, DM target, and receiver: {rendered}"
        );
        assert!(
            rendered.contains("from=\"bianca\" kind=\"agent\" scope=\"thread\" id=\"m_thread\" target=\"thread:thread-fresh\" receiver=\"s_ellie\" thread=\"thread-fresh\""),
            "thread item must name sender, thread target, and receiver: {rendered}"
        );
    }

    // ---- parse_outbound (the inverse of render_nexus) ----

    #[test]
    fn parse_outbound_dm() {
        let got = parse_outbound("<nexus to=\"boss\">pong</nexus>");
        assert_eq!(
            got,
            vec![OutboundMsg {
                to: OutboundTarget::Dm("boss".into()),
                body: "pong".into()
            }]
        );
    }

    #[test]
    fn parse_outbound_thread_and_topic() {
        assert_eq!(
            parse_outbound("<nexus thread=\"backend\">plan</nexus>"),
            vec![OutboundMsg {
                to: OutboundTarget::Thread("backend".into()),
                body: "plan".into()
            }]
        );
        assert_eq!(
            parse_outbound("<nexus topic=\"ci\">green</nexus>"),
            vec![OutboundMsg {
                to: OutboundTarget::Topic("ci".into()),
                body: "green".into()
            }]
        );
    }

    #[test]
    fn parse_outbound_no_envelope_is_empty() {
        // A bare reply (no <nexus> block) → empty; the caller applies the default-to-sender rule.
        assert!(parse_outbound("just a plain pong, no envelope").is_empty());
        assert!(parse_outbound("").is_empty());
    }

    #[test]
    fn parse_outbound_ignores_nexus_batch_wrapper() {
        // The inbound `<nexus-batch>` wrapper must never be mistaken for an outbound envelope, but a
        // bare `<nexus …>` inside it (an agent quoting its inbox) still parses.
        let injected = "<nexus-batch dms=\"1\" thread=\"0\" total=\"1\">\n  <nexus from=\"x\" kind=\"agent\" scope=\"dm\" id=\"m1\">hi</nexus>\n</nexus-batch>";
        // `from=`-only inner element has no routable target → skipped.
        assert!(parse_outbound(injected).is_empty());
    }

    #[test]
    fn parse_outbound_multiple_in_order() {
        let text = "before <nexus to=\"a\">one</nexus> mid <nexus topic=\"t\">two</nexus> after";
        assert_eq!(
            parse_outbound(text),
            vec![
                OutboundMsg {
                    to: OutboundTarget::Dm("a".into()),
                    body: "one".into()
                },
                OutboundMsg {
                    to: OutboundTarget::Topic("t".into()),
                    body: "two".into()
                },
            ]
        );
    }

    #[test]
    fn parse_outbound_body_may_contain_angle_brackets() {
        let got = parse_outbound("<nexus to=\"boss\">use a < b and c > d</nexus>");
        assert_eq!(got[0].body, "use a < b and c > d");
    }

    #[test]
    fn parse_outbound_skips_blank_target() {
        // A `from=`-only envelope (no to/thread/topic) is not routable → skipped, not guessed.
        assert!(parse_outbound("<nexus from=\"ben\" kind=\"agent\">hi</nexus>").is_empty());
    }

    /// THE symmetry lock: an outbound `<nexus to="…">` envelope mirrors the attribute format
    /// `render_nexus` emits, so a round-trip recovers the target + body. (Inbound renders `from`,
    /// outbound parses `to`; we additionally assert each channel kind round-trips.)
    #[test]
    fn parse_outbound_roundtrips_render_nexus_shape() {
        // DM: render with `from`, but the outbound form swaps `from`→`to`; assert the parser
        // recovers the body and the DM target from the to-shaped envelope.
        let dm = parse_outbound("<nexus to=\"boss\">pong</nexus>");
        assert_eq!(
            dm,
            vec![OutboundMsg {
                to: OutboundTarget::Dm("boss".into()),
                body: "pong".into()
            }]
        );

        // Thread: render_nexus emits `thread="backend"`; parse_outbound recovers a Thread target.
        let p = Provenance {
            from: "dylan".into(),
            kind: Kind::Agent,
            thread: Some("backend".into()),
            topic: None,
            stamp: None,
        };
        let rendered = render_nexus(&p, "plan");
        assert!(rendered.contains("thread=\"backend\""));
        assert_eq!(
            parse_outbound(&rendered),
            vec![OutboundMsg {
                to: OutboundTarget::Thread("backend".into()),
                body: "plan".into()
            }]
        );

        // Topic: same for a topic-scoped render.
        let pt = Provenance {
            from: "dylan".into(),
            kind: Kind::Agent,
            thread: None,
            topic: Some("ci".into()),
            stamp: None,
        };
        assert_eq!(
            parse_outbound(&render_nexus(&pt, "green")),
            vec![OutboundMsg {
                to: OutboundTarget::Topic("ci".into()),
                body: "green".into()
            }]
        );
    }
}
