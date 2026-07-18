//! Contract round-trip coverage moved out of production modules.
//!
//! These tests intentionally mirror the old inline `#[cfg(test)]` blocks so the
//! `nexus-contracts` source tree stays focused on wire types while preserving coverage.

mod ack {
    use nexus_contracts::ack::*;
    use nexus_contracts::ids::MessageId;

    #[test]
    fn single_ack_roundtrips() {
        let req = AckRequest {
            message_id: MessageId("m_01".into()),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["messageId"], "m_01");
        let back: AckRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn bulk_thread_ack_roundtrips() {
        let req = AckThreadsRequest {
            message_ids: vec![MessageId("m_03".into()), MessageId("m_04".into())],
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["messageIds"][0], "m_03");
        assert_eq!(json["messageIds"][1], "m_04");
        let back: AckThreadsRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn ack_response_roundtrips() {
        let resp = AckResponse { acked: 2 };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["acked"], 2);
        let back: AckResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }
}

mod admin {
    use nexus_contracts::admin::*;
    use nexus_contracts::enums::Tier;
    use nexus_contracts::ids::{AgentId, MessageId, SessionId};
    use nexus_contracts::HarnessId;

    #[test]
    fn route_forward_roundtrips() {
        let req = RouteForwardRequest {
            notif: MessageId("n_01".into()),
            to: "ben".into(),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["notif"], "n_01");
        assert_eq!(json["to"], "ben");
        let back: RouteForwardRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn spawn_roundtrips() {
        let req = SpawnRequest {
            kind: HarnessId::new("codex").unwrap(),
            name: Some("dylan".into()),
            identity_policy: None,
            cwd: None,
            project: Some("egregore".into()),
            role: Some("backend".into()),
            initial_prompt: None,
            resume: None,
            harness_args: Vec::new(),
            headless: false,
            backend: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["kind"], "codex");
        assert_eq!(json["name"], "dylan");
        let back: SpawnRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);

        let resp = SpawnResponse {
            session_id: SessionId("s_02".into()),
        };
        assert_eq!(serde_json::to_value(&resp).unwrap()["sessionId"], "s_02");
    }

    /// `headless` defaults to `false` when the field is absent (backward compat for old callers).
    #[test]
    fn spawn_request_headless_defaults_false_when_absent() {
        let json = serde_json::json!({
            "kind": "claude",
            "name": "ada",
            "cwd": null,
            "project": "egregore",
            "role": null
        });
        let parsed: SpawnRequest = serde_json::from_value(json).unwrap();
        assert!(
            !parsed.headless,
            "headless must default to false when field is absent"
        );
        assert!(
            parsed.harness_args.is_empty(),
            "harness_args must default empty when field is absent"
        );
    }

    /// `headless: true` survives a round-trip.
    #[test]
    fn spawn_request_headless_true_roundtrips() {
        let req = SpawnRequest {
            kind: HarnessId::new("claude").unwrap(),
            name: Some("ada".into()),
            identity_policy: None,
            cwd: None,
            project: Some("egregore".into()),
            role: None,
            initial_prompt: None,
            resume: None,
            harness_args: Vec::new(),
            headless: true,
            backend: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["headless"], true);
        let back: SpawnRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn spawn_request_initial_prompt_roundtrips() {
        let req = SpawnRequest {
            kind: HarnessId::new("codex").unwrap(),
            name: Some("otto".into()),
            identity_policy: None,
            cwd: Some("/tmp/project".into()),
            project: Some("default".into()),
            role: Some("lead".into()),
            initial_prompt: Some("You are <var.name>. Role: <var.role>.".into()),
            resume: None,
            harness_args: Vec::new(),
            headless: false,
            backend: Some("pty".into()),
        };

        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(
            json["initialPrompt"],
            "You are <var.name>. Role: <var.role>."
        );
        let back: SpawnRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn remove_and_assign_role_roundtrip() {
        let rm = RemoveRequest {
            agent_id: None,
            name: "dylan".into(),
            kill: false,
        };
        assert_eq!(serde_json::to_value(&rm).unwrap()["name"], "dylan");

        // kill=true round-trips.
        let rm_kill = RemoveRequest {
            agent_id: None,
            name: "dylan".into(),
            kill: true,
        };
        let j = serde_json::to_value(&rm_kill).unwrap();
        assert_eq!(j["kill"], true);
        let back: RemoveRequest = serde_json::from_value(j).unwrap();
        assert_eq!(back, rm_kill);

        // Old callers that omit the field get kill=false (serde default).
        let old_json = serde_json::json!({"name": "dylan"});
        let parsed: RemoveRequest = serde_json::from_value(old_json).unwrap();
        assert!(
            !parsed.kill,
            "serde(default) must give false when field absent"
        );

        let rmr = RemoveResponse {
            name: Some("dylan".into()),
            status: "removed".into(),
        };
        assert_eq!(serde_json::to_value(&rmr).unwrap()["status"], "removed");

        let ar = AssignRoleRequest {
            agent_id: Some(AgentId("a_ben".into())),
            name: "ben".into(),
            role: "lead".into(),
        };
        let j = serde_json::to_value(&ar).unwrap();
        assert_eq!(j["role"], "lead");
        let back: AssignRoleRequest = serde_json::from_value(j).unwrap();
        assert_eq!(back, ar);
        let arr = AssignRoleResponse {
            name: Some("ben".into()),
            role: "lead".into(),
        };
        assert_eq!(serde_json::to_value(&arr).unwrap()["name"], "ben");

        let ap = AssignProjectRequest {
            agent_id: Some(AgentId("a_ben".into())),
            name: "ben".into(),
            project: "egregore-nexus".into(),
        };
        let pj = serde_json::to_value(&ap).unwrap();
        assert_eq!(pj["project"], "egregore-nexus");
        let back: AssignProjectRequest = serde_json::from_value(pj).unwrap();
        assert_eq!(back, ap);
        let apr = AssignProjectResponse {
            name: Some("ben".into()),
            project: "egregore-nexus".into(),
        };
        assert_eq!(serde_json::to_value(&apr).unwrap()["name"], "ben");
    }

    #[test]
    fn admin_rename_roundtrips() {
        let req = AdminRenameRequest {
            source: "s_staged".into(),
            target: "nora".into(),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["source"], "s_staged");
        assert_eq!(json["target"], "nora");
        let back: AdminRenameRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);

        let resp = AdminRenameResponse {
            agent_id: AgentId("a_staged".into()),
            session_id: Some(SessionId("s_staged".into())),
            name: "nora".into(),
            previous: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["agentId"], "a_staged");
        assert_eq!(json["sessionId"], "s_staged");
        assert_eq!(json["name"], "nora");
        assert!(json.get("previous").is_none());
        let back: AdminRenameResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn grant_tier_roundtrips() {
        let req = GrantTierRequest {
            agent_id: None,
            name: "ben".into(),
            tier: Tier::Admin,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["name"], "ben");
        assert_eq!(json["tier"], "admin");
        let back: GrantTierRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);

        let resp = GrantTierResponse {
            name: Some("ben".into()),
            tier: Tier::Admin,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "ben");
        assert_eq!(json["tier"], "admin");
        let back: GrantTierResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn channel_and_monitor_roundtrip() {
        let ch = ChannelRequest {
            op: ChannelOp::SetRoute,
            topic: "ci".into(),
            source: Some("github".into()),
        };
        let json = serde_json::to_value(&ch).unwrap();
        assert_eq!(json["op"], "setRoute");
        assert_eq!(json["topic"], "ci");
        let back: ChannelRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, ch);

        let mon = MonitorRequest {
            follow: true,
            scope: Some("backend".into()),
        };
        let mj = serde_json::to_value(&mon).unwrap();
        assert_eq!(mj["follow"], true);
        let mback: MonitorRequest = serde_json::from_value(mj).unwrap();
        assert_eq!(mback, mon);
    }

    #[test]
    fn dlq_admin_contracts_roundtrip() {
        let list = DlqListRequest {
            for_target: Some("ana".into()),
            since: Some("1h".into()),
            limit: Some(25),
        };
        let json = serde_json::to_value(&list).unwrap();
        assert_eq!(json["for"], "ana");
        assert_eq!(json["since"], "1h");
        assert_eq!(json["limit"], 25);
        let back: DlqListRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, list);

        let entry = DlqEntry {
            in_flight_id: "if_dead".into(),
            message_id: MessageId("m_dead".into()),
            sender: "ben".into(),
            recipient_name: Some("ana".into()),
            recipient_agent_id: Some(AgentId("a_ana".into())),
            recipient_session: Some(SessionId("s_ana".into())),
            created_at: 1_700_000_000,
            dead_lettered_at: Some(1_700_000_123),
            attempt_count: 2,
            error_code: Some("provider_error".into()),
            error_reason: Some("provider limit timeout".into()),
            error_details: Some(serde_json::json!({
                "retryable": true,
                "source": "claude.acp.prompt_error",
            })),
            body_preview: "hello".into(),
        };
        let list_response = DlqListResponse {
            rows: vec![entry],
            total: 1,
        };
        let json = serde_json::to_value(&list_response).unwrap();
        assert_eq!(json["rows"][0]["inFlightId"], "if_dead");
        assert_eq!(json["rows"][0]["messageId"], "m_dead");
        assert_eq!(json["rows"][0]["recipientAgentId"], "a_ana");
        assert_eq!(json["rows"][0]["attemptCount"], 2);
        assert_eq!(json["rows"][0]["errorCode"], "provider_error");
        assert_eq!(json["rows"][0]["errorDetails"]["retryable"], true);
        assert_eq!(json["total"], 1);
        let back: DlqListResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, list_response);

        let requeue = DlqRequeueRequest {
            in_flight_id: Some("if_dead".into()),
            for_target: None,
            since: None,
        };
        let json = serde_json::to_value(&requeue).unwrap();
        assert_eq!(json["inFlightId"], "if_dead");
        let back: DlqRequeueRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, requeue);

        let purge = DlqPurgeRequest {
            in_flight_id: None,
            for_target: Some("ana".into()),
            since: Some("24h".into()),
            yes: true,
        };
        let json = serde_json::to_value(&purge).unwrap();
        assert_eq!(json["for"], "ana");
        assert_eq!(json["yes"], true);
        let back: DlqPurgeRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, purge);

        let mutation = DlqMutationResponse {
            count: 2,
            in_flight_ids: vec!["if_a".into(), "if_b".into()],
        };
        let json = serde_json::to_value(&mutation).unwrap();
        assert_eq!(json["count"], 2);
        assert_eq!(json["inFlightIds"][1], "if_b");
        let back: DlqMutationResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, mutation);
    }
}

mod agents {
    use nexus_contracts::agents::*;
    use nexus_contracts::enums::{AgentAccessRole, Presence};
    use nexus_contracts::ids::{AgentId, CredentialId, SessionId};
    use nexus_contracts::HarnessId;

    fn runtime() -> AgentRuntimeSummary {
        AgentRuntimeSummary {
            runtime_id: SessionId("s_1".into()),
            agent_id: AgentId("a_1".into()),
            harness: HarnessId::new("codex").unwrap(),
            cwd: Some("/repo".into()),
            transport: Some("codex-appserver".into()),
            presence: Presence::Online,
            active: true,
            started_at: 10,
            stopped_at: None,
            last_heartbeat: Some(11),
        }
    }

    #[test]
    fn agent_create_roundtrips() {
        let req = AgentCreateRequest {
            name: "blake".into(),
            default_harness: Some(HarnessId::new("codex").unwrap()),
            project: Some("default".into()),
            role: Some("backend".into()),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["defaultHarness"], "codex");
        let back: AgentCreateRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn credential_create_response_roundtrips() {
        let resp = AgentCredentialCreateResponse {
            credential_id: CredentialId("cred_1".into()),
            agent_id: AgentId("a_1".into()),
            secret: "nexus_rt_secret".into(),
            label: Some("local".into()),
            purpose: Some("runtime".into()),
            scopes: vec!["runtime:register".into()],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["credentialId"], "cred_1");
        assert_eq!(json["scopes"][0], "runtime:register");
        let back: AgentCredentialCreateResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn access_grant_and_revoke_roundtrip() {
        let grant = AgentAccessGrantRequest {
            agent_id: None,
            name: "target".into(),
            principal_agent_id: None,
            principal: "alice".into(),
            project: Some("default".into()),
            role: AgentAccessRole::CoOwner,
        };
        let json = serde_json::to_value(&grant).unwrap();
        assert_eq!(json["role"], "coOwner");
        let back: AgentAccessGrantRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, grant);

        let revoke = AgentAccessRevokeResponse {
            name: Some("target".into()),
            principal: "alice".into(),
            project: "default".into(),
            revoked: true,
        };
        let json = serde_json::to_value(&revoke).unwrap();
        assert_eq!(json["revoked"], true);
        let back: AgentAccessRevokeResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, revoke);
    }

    #[test]
    fn owner_transfer_roundtrip() {
        let req = AgentOwnerTransferRequest {
            agent_id: None,
            name: "target".into(),
            owner_agent_id: None,
            owner: "alice".into(),
            project: Some("ops".into()),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["owner"], "alice");
        let back: AgentOwnerTransferRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);

        let resp = AgentOwnerTransferResponse {
            name: Some("target".into()),
            previous_owner: Some("owner".into()),
            previous_project: Some("default".into()),
            owner: "alice".into(),
            project: "ops".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["previousOwner"], "owner");
        let back: AgentOwnerTransferResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn summary_and_runtime_list_roundtrip() {
        let summary = AgentSummary {
            agent_id: AgentId("a_1".into()),
            name: Some("blake".into()),
            project: "default".into(),
            default_harness: Some(HarnessId::new("codex").unwrap()),
            role: None,
            tier: Some("agent".into()),
            disabled: false,
            active_runtime: Some(runtime()),
        };
        let list = AgentRuntimeListResponse {
            agent_id: AgentId("a_1".into()),
            runtimes: vec![runtime()],
        };
        assert_eq!(
            serde_json::from_value::<AgentSummary>(serde_json::to_value(&summary).unwrap())
                .unwrap(),
            summary
        );
        assert_eq!(
            serde_json::from_value::<AgentRuntimeListResponse>(
                serde_json::to_value(&list).unwrap()
            )
            .unwrap(),
            list
        );
    }
}

mod batch {
    use nexus_contracts::batch::*;
    use nexus_contracts::enums::{Kind, Scope};
    use nexus_contracts::ids::MessageId;

    fn sample() -> NexusBatch {
        NexusBatch {
            counts: BatchCounts {
                dms: 2,
                thread: 1,
                total: 3,
            },
            dms: vec![
                BatchMessage {
                    id: MessageId("m_01".into()),
                    from: "etan".into(),
                    kind: Kind::Human,
                    scope: Scope::Dm,
                    thread: None,
                    topic: None,
                    body: "take the auth refactor today?".into(),
                    truncated: false,
                },
                BatchMessage {
                    id: MessageId("m_02".into()),
                    from: "ben".into(),
                    kind: Kind::Agent,
                    scope: Scope::Dm,
                    thread: None,
                    topic: None,
                    body: "rebase before you start.".into(),
                    truncated: false,
                },
            ],
            threads: vec![BatchMessage {
                id: MessageId("m_03".into()),
                from: "dylan".into(),
                kind: Kind::Agent,
                scope: Scope::Thread,
                thread: Some("backend".into()),
                topic: None,
                body: "migration plan ... [truncated]".into(),
                truncated: true,
            }],
            dm_message_ids: vec![MessageId("m_01".into()), MessageId("m_02".into())],
            thread_message_ids: vec![MessageId("m_03".into())],
            message_ids: vec![
                MessageId("m_01".into()),
                MessageId("m_02".into()),
                MessageId("m_03".into()),
            ],
        }
    }

    #[test]
    fn batch_roundtrips_camelcase() {
        let batch = sample();
        let json = serde_json::to_value(&batch).unwrap();
        assert_eq!(json["counts"]["dms"], 2);
        assert_eq!(json["counts"]["thread"], 1);
        assert_eq!(json["counts"]["total"], 3);
        assert_eq!(json["dms"][0]["from"], "etan");
        assert_eq!(json["dms"][0]["kind"], "human");
        assert_eq!(json["dms"][0]["scope"], "dm");
        assert_eq!(json["threads"][0]["thread"], "backend");
        assert_eq!(json["threads"][0]["truncated"], true);
        assert_eq!(json["dmMessageIds"][0], "m_01");
        assert_eq!(json["threadMessageIds"][0], "m_03");
        assert_eq!(json["messageIds"][2], "m_03");
        let back: NexusBatch = serde_json::from_value(json).unwrap();
        assert_eq!(back, batch);
    }
}

mod enums {
    use nexus_contracts::enums::*;
    use nexus_contracts::HarnessId;

    #[test]
    fn enums_serialize_lowercase() {
        assert_eq!(serde_json::to_string(&Kind::Agent).unwrap(), r#""agent""#);
        assert_eq!(serde_json::to_string(&Kind::Human).unwrap(), r#""human""#);
        assert_eq!(
            serde_json::to_string(&Kind::Notification).unwrap(),
            r#""notification""#
        );
        assert_eq!(serde_json::to_string(&Kind::App).unwrap(), r#""app""#);

        assert_eq!(serde_json::to_string(&Scope::Dm).unwrap(), r#""dm""#);
        assert_eq!(
            serde_json::to_string(&Scope::Thread).unwrap(),
            r#""thread""#
        );
        assert_eq!(serde_json::to_string(&Scope::Topic).unwrap(), r#""topic""#);

        assert_eq!(serde_json::to_string(&Tier::Agent).unwrap(), r#""agent""#);
        assert_eq!(serde_json::to_string(&Tier::Admin).unwrap(), r#""admin""#);
        assert_eq!(
            serde_json::to_string(&AgentAccessRole::CoOwner).unwrap(),
            r#""coOwner""#
        );

        assert_eq!(
            serde_json::to_string(&Presence::Online).unwrap(),
            r#""online""#
        );
        assert_eq!(serde_json::to_string(&Presence::Busy).unwrap(), r#""busy""#);
        assert_eq!(
            serde_json::to_string(&Presence::Offline).unwrap(),
            r#""offline""#
        );

        assert_eq!(
            serde_json::to_string(&HarnessId::new("claude").unwrap()).unwrap(),
            r#""claude""#
        );
        assert_eq!(
            serde_json::to_string(&HarnessId::new("codex").unwrap()).unwrap(),
            r#""codex""#
        );

        assert_eq!(
            serde_json::to_string(&DeliveryState::Pending).unwrap(),
            r#""pending""#
        );
        assert_eq!(
            serde_json::to_string(&DeliveryState::Acked).unwrap(),
            r#""acked""#
        );
    }

    #[test]
    fn enums_roundtrip() {
        let p: Presence = serde_json::from_str(r#""busy""#).unwrap();
        assert_eq!(p, Presence::Busy);
        let d: DeliveryState = serde_json::from_str(r#""notified""#).unwrap();
        assert_eq!(d, DeliveryState::Notified);
    }

    #[test]
    fn harness_opencode_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&HarnessId::new("opencode").unwrap()).unwrap(),
            r#""opencode""#
        );
        let h: HarnessId = serde_json::from_str(r#""opencode""#).unwrap();
        assert_eq!(h, HarnessId::new("opencode").unwrap());
    }

    #[test]
    fn harness_hermes_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&HarnessId::new("hermes").unwrap()).unwrap(),
            r#""hermes""#
        );
        let h: HarnessId = serde_json::from_str(r#""hermes""#).unwrap();
        assert_eq!(h, HarnessId::new("hermes").unwrap());
    }
}

mod events {
    use nexus_contracts::enums::Presence;
    use nexus_contracts::events::*;
    use nexus_contracts::ids::{MessageId, SessionId};

    #[test]
    fn message_created_event_tagged_by_type() {
        let ev = WsEvent::MessageCreated {
            message_id: MessageId("m_01".into()),
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "message.created");
        assert_eq!(json["messageId"], "m_01");
        let back: WsEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn agent_update_thinking_carries_kind_and_data() {
        let ev = WsEvent::AgentUpdate {
            session_id: SessionId("s_01".into()),
            kind: AgentUpdateKind::Thinking,
            data: serde_json::json!({ "text": "hmm" }),
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "agent.update");
        assert_eq!(json["sessionId"], "s_01");
        assert_eq!(json["kind"], "thinking");
        assert_eq!(json["data"]["text"], "hmm");
        let back: WsEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn agent_update_text_and_tool_call_kinds() {
        let text = WsEvent::AgentUpdate {
            session_id: SessionId("s_01".into()),
            kind: AgentUpdateKind::Text,
            data: serde_json::json!({ "text": "hi" }),
        };
        let json = serde_json::to_value(&text).unwrap();
        assert_eq!(json["kind"], "text");
        assert_eq!(json["data"]["text"], "hi");
        assert_eq!(serde_json::from_value::<WsEvent>(json).unwrap(), text);

        let tool = WsEvent::AgentUpdate {
            session_id: SessionId("s_01".into()),
            kind: AgentUpdateKind::ToolCall,
            data: serde_json::json!({ "id": "tc_1", "title": "Read", "status": "completed" }),
        };
        let json = serde_json::to_value(&tool).unwrap();
        // snake_case tag for the multi-word kind.
        assert_eq!(json["kind"], "tool_call");
        assert_eq!(json["data"]["id"], "tc_1");
        assert_eq!(serde_json::from_value::<WsEvent>(json).unwrap(), tool);
    }

    #[test]
    fn agent_update_kind_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(AgentUpdateKind::Commands).unwrap(),
            "commands"
        );
        assert_eq!(serde_json::to_value(AgentUpdateKind::Plan).unwrap(), "plan");
    }

    #[test]
    fn agent_status_roundtrip() {
        let st = WsEvent::AgentStatus {
            session_id: SessionId("s_01".into()),
            presence: Presence::Busy,
            paused: false,
        };
        let json = serde_json::to_value(&st).unwrap();
        assert_eq!(json["type"], "agent.status");
        assert_eq!(json["presence"], "busy");
        let back: WsEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, st);
    }

    #[test]
    fn thread_member_changed_keeps_spec_name() {
        let ev = WsEvent::ThreadMemberChanged {
            thread: "backend".into(),
            members: vec!["ben".into(), "dylan".into()],
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "thread.member.changed");
        let back: WsEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn notification_received_roundtrips() {
        let ev = WsEvent::NotificationReceived {
            notif_id: MessageId("n_01".into()),
            routed_to: vec!["ben".into()],
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "notification.received");
        assert_eq!(json["routedTo"][0], "ben");
        let back: WsEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn developer_event_roundtrips_as_metadata_only_ws_event() {
        let ev = WsEvent::DeveloperEvent {
            event: DeveloperEventEnvelope {
                kind: DeveloperEventKind::Message,
                topic: "sys.message.thread.nexus-project".into(),
                seq: 7,
                ts: 1_783_600_000_000,
                thread: Some("nexus-project".into()),
                dm: None,
                from: Some("ada".into()),
                message_id: Some(MessageId("m_01".into())),
                agent: None,
                session_id: None,
                lifecycle: None,
                current_work: None,
                data: None,
                tool: None,
                phase: None,
                ok: None,
            },
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "developer.event");
        assert_eq!(json["event"]["kind"], "message");
        assert_eq!(json["event"]["topic"], "sys.message.thread.nexus-project");
        assert_eq!(json["event"]["seq"], 7);
        assert_eq!(json["event"]["thread"], "nexus-project");
        assert!(
            json["event"].get("body").is_none(),
            "developer event envelopes must not expose message bodies"
        );
        let back: WsEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn developer_tool_call_event_roundtrips_as_ephemeral_ws_event() {
        let ev = WsEvent::DeveloperEvent {
            event: DeveloperEventEnvelope {
                kind: DeveloperEventKind::ToolCall,
                topic: "sys.agent.otto.tool_call".into(),
                seq: 2,
                ts: 1_783_600_000_100,
                thread: None,
                dm: None,
                from: None,
                message_id: None,
                agent: Some("otto".into()),
                session_id: Some(SessionId("s_otto".into())),
                lifecycle: None,
                current_work: None,
                data: None,
                tool: Some("Read".into()),
                phase: Some(DeveloperToolCallPhase::Post),
                ok: Some(true),
            },
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "developer.event");
        assert_eq!(json["event"]["kind"], "tool_call");
        assert_eq!(json["event"]["topic"], "sys.agent.otto.tool_call");
        assert_eq!(json["event"]["tool"], "Read");
        assert_eq!(json["event"]["phase"], "post");
        assert_eq!(json["event"]["ok"], true);
        assert!(
            json["event"].get("body").is_none(),
            "ephemeral tool-call envelopes must not expose message bodies"
        );
        let back: WsEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn developer_action_event_roundtrips_as_metadata_only_ws_event() {
        let ev = WsEvent::DeveloperEvent {
            event: DeveloperEventEnvelope {
                kind: DeveloperEventKind::Action,
                topic: "sys.thread.ops".into(),
                seq: 3,
                ts: 1_783_600_000_200,
                thread: Some("ops".into()),
                dm: None,
                from: Some("ada".into()),
                message_id: None,
                agent: None,
                session_id: None,
                lifecycle: None,
                current_work: None,
                data: Some(serde_json::json!({ "action": "thread.join", "member": "ada" })),
                tool: None,
                phase: None,
                ok: None,
            },
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(json["type"], "developer.event");
        assert_eq!(json["event"]["kind"], "action");
        assert_eq!(json["event"]["data"]["action"], "thread.join");
        assert!(
            json["event"].get("body").is_none(),
            "action developer events must not expose message bodies"
        );
        let back: WsEvent = serde_json::from_value(json).unwrap();
        assert_eq!(back, ev);
    }
}

mod ids {
    use nexus_contracts::ids::*;

    #[test]
    fn session_id_is_transparent_string() {
        let id = SessionId("s_01".to_string());
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#""s_01""#);
        let back: SessionId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn all_id_types_are_transparent() {
        assert_eq!(
            serde_json::to_string(&AgentId("a_01".into())).unwrap(),
            r#""a_01""#
        );
        assert_eq!(
            serde_json::to_string(&CredentialId("cred_01".into())).unwrap(),
            r#""cred_01""#
        );
        assert_eq!(
            serde_json::to_string(&MessageId("m_01".into())).unwrap(),
            r#""m_01""#
        );
        assert_eq!(
            serde_json::to_string(&ThreadId("t_01".into())).unwrap(),
            r#""t_01""#
        );
        assert_eq!(
            serde_json::to_string(&TopicId("pub".into())).unwrap(),
            r#""pub""#
        );
        assert_eq!(
            serde_json::to_string(&ProjectId("p_01".into())).unwrap(),
            r#""p_01""#
        );
    }
}

mod message {
    use nexus_contracts::enums::{Kind, Scope};
    use nexus_contracts::ids::{MessageId, ProjectId, ThreadId};
    use nexus_contracts::message::*;

    #[test]
    fn message_roundtrips_camelcase() {
        let msg = Message {
            id: MessageId("m_03".into()),
            project: ProjectId("p_01".into()),
            from: "dylan".into(),
            scope: Scope::Thread,
            thread: Some(ThreadId("t_backend".into())),
            topic: None,
            body: "here's the migration plan".into(),
            summary: Some("migration plan".into()),
            provenance: Provenance {
                from: "dylan".into(),
                kind: Kind::Agent,
                thread: Some("backend".into()),
                topic: None,
                stamp: Some(ProvenanceStamp {
                    algo: "ed25519".into(),
                    signature: "abc123".into(),
                    signed_at: 1_700_000_000,
                }),
            },
            created_at: 1_700_000_001,
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["id"], "m_03");
        assert_eq!(json["scope"], "thread");
        assert_eq!(json["thread"], "t_backend");
        assert!(json.get("topic").is_none());
        assert_eq!(json["provenance"]["kind"], "agent");
        assert_eq!(json["provenance"]["thread"], "backend");
        assert_eq!(json["provenance"]["stamp"]["algo"], "ed25519");
        assert_eq!(json["provenance"]["stamp"]["signedAt"], 1_700_000_000i64);
        assert_eq!(json["createdAt"], 1_700_000_001i64);
        let back: Message = serde_json::from_value(json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn dm_message_omits_thread_and_topic() {
        let msg = Message {
            id: MessageId("m_01".into()),
            project: ProjectId("p_01".into()),
            from: "etan".into(),
            scope: Scope::Dm,
            thread: None,
            topic: None,
            body: "take the auth refactor?".into(),
            summary: None,
            provenance: Provenance {
                from: "etan".into(),
                kind: Kind::Human,
                thread: None,
                topic: None,
                stamp: None,
            },
            created_at: 1_700_000_000,
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert!(json.get("thread").is_none());
        assert!(json.get("summary").is_none());
        assert!(json["provenance"].get("stamp").is_none());
        let back: Message = serde_json::from_value(json).unwrap();
        assert_eq!(back, msg);
    }
}

mod notify {
    use nexus_contracts::notify::*;

    #[test]
    fn notify_request_roundtrips() {
        let req = NotifyRequest {
            source: "github".into(),
            topic: Some("ci".into()),
            payload: serde_json::json!({ "status": "green", "sha": "abc" }),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["source"], "github");
        assert_eq!(json["topic"], "ci");
        assert_eq!(json["payload"]["status"], "green");
        let back: NotifyRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn notify_command_request_preserves_the_exact_signed_envelope() {
        let req = NotifyCommandRequest {
            raw_body: r#"{"source":"github","payload":{"status":"green"}}"#.into(),
            timestamp: "1784000000000".into(),
            signature: format!("sha256={}", "ab".repeat(32)),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["rawBody"], req.raw_body);
        assert_eq!(json["timestamp"], "1784000000000");
        assert_eq!(json["signature"], req.signature);
        let back: NotifyCommandRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn notify_response_roundtrips() {
        let resp = NotifyResponse {
            notif_id: nexus_contracts::ids::MessageId("n_01".into()),
            routed_to: vec!["ben".into()],
            hmac_ok: true,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["notifId"], "n_01");
        assert_eq!(json["routedTo"][0], "ben");
        assert_eq!(json["hmacOk"], true);
        let back: NotifyResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn route_rule_roundtrips() {
        let rule = RouteRule {
            source: Some("github".into()),
            topic: None,
            to: "ci".into(),
        };
        let json = serde_json::to_value(&rule).unwrap();
        assert_eq!(json["source"], "github");
        assert_eq!(json["to"], "ci");
        let back: RouteRule = serde_json::from_value(json).unwrap();
        assert_eq!(back, rule);
    }

    #[test]
    fn signature_header_constant() {
        assert_eq!(NOTIFY_SIGNATURE_HEADER, "X-Nexus-Signature");
    }
}

mod ports {
    use nexus_contracts::ports::*;

    // Object-safety + signature lock: this must compile for the trait to be usable as `Arc<dyn …>`.
    fn _assert_object_safe(
        _i: &dyn IdentityPort,
        _r: &dyn DispatchPort,
        _b: &dyn BusPort,
        _a: &dyn AgentTurnExecutionPort,
        _n: &dyn NotifyPort,
        _s: &dyn SearchPort,
        _ad: &dyn AdminPort,
        _es: &dyn EventSink,
    ) {
    }

    #[test]
    fn contract_error_carries_code_and_message() {
        let e = ContractError {
            code: nexus_contracts::codes::NOT_FOUND,
            message: "nope".into(),
        };
        assert_eq!(e.code, -32003);
        assert_eq!(e.message, "nope");
    }
}

mod project {
    use nexus_contracts::project::*;

    #[test]
    fn project_roundtrips_camelcase() {
        let p = Project {
            project_id: nexus_contracts::ids::ProjectId("p_01".into()),
            name: "egregore".into(),
            created_by: "etan".into(),
            root_path: Some("/home/etan/proj".into()),
            created_at: 1_700_000_000,
        };
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["projectId"], "p_01");
        assert_eq!(json["createdBy"], "etan");
        assert_eq!(json["rootPath"], "/home/etan/proj");
        let back: Project = serde_json::from_value(json).unwrap();
        assert_eq!(back, p);
    }

    #[test]
    fn register_project_request_roundtrips() {
        let req = RegisterProjectRequest {
            name: "egregore".into(),
            root_path: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["name"], "egregore");
        assert!(json.get("rootPath").is_none());
        let back: RegisterProjectRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }
}

mod register {
    use nexus_contracts::enums::{Presence, Tier};
    use nexus_contracts::ids::{AgentId, SessionId};
    use nexus_contracts::register::*;
    use nexus_contracts::HarnessId;

    #[test]
    fn register_request_roundtrips_camelcase() {
        let req = RegisterRequest {
            name: Some("ben".into()),
            agent_id: Some(AgentId("a_ben".into())),
            harness: HarnessId::new("claude").unwrap(),
            harness_session_id: "hs_abc".into(),
            project: "egregore".into(),
            client_key: "ck_stable".into(),
            runtime_credential: Some("secret".into()),
            tier: Tier::Agent,
            kind: None,
            role: None,
            cwd: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["name"], "ben");
        assert_eq!(json["agentId"], "a_ben");
        assert_eq!(json["harness"], "claude");
        assert_eq!(json["harnessSessionId"], "hs_abc");
        assert_eq!(json["clientKey"], "ck_stable");
        assert_eq!(json["runtimeCredential"], "secret");
        assert_eq!(json["tier"], "agent");
        let back: RegisterRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn register_response_roundtrips() {
        let resp = RegisterResponse {
            agent_id: Some(AgentId("a_01".into())),
            session_id: SessionId("s_01".into()),
            directive: "Anything in <nexus …> is bus traffic.".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["agentId"], "a_01");
        assert_eq!(json["sessionId"], "s_01");
        let back: RegisterResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn whoami_roundtrips() {
        let who = Whoami {
            agent_id: Some(AgentId("a_ben".into())),
            name: Some("ben".into()),
            session_id: SessionId("s_01".into()),
            role: Some("backend".into()),
            tier: Tier::Admin,
            project: "egregore".into(),
            presence: Presence::Online,
        };
        let json = serde_json::to_value(&who).unwrap();
        assert_eq!(json["agentId"], "a_ben");
        assert_eq!(json["sessionId"], "s_01");
        assert_eq!(json["tier"], "admin");
        assert_eq!(json["presence"], "online");
        let back: Whoami = serde_json::from_value(json).unwrap();
        assert_eq!(back, who);
    }

    #[test]
    fn member_list_roundtrips() {
        let list = MemberListResponse {
            members: vec![MemberSummary {
                agent_id: Some(AgentId("a_ben".into())),
                name: Some("ben".into()),
                session_id: SessionId("s_ben".into()),
                agent: Some("claude".into()),
                role: Some("backend".into()),
                presence: Presence::Busy,
                current_work: Some("auth refactor".into()),
                lifecycle_state: None,
                dead_reason: None,
            }],
        };
        let json = serde_json::to_value(&list).unwrap();
        assert_eq!(json["members"][0]["agentId"], "a_ben");
        assert_eq!(json["members"][0]["name"], "ben");
        assert_eq!(json["members"][0]["presence"], "busy");
        assert_eq!(json["members"][0]["currentWork"], "auth refactor");
        let back: MemberListResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, list);
    }

    #[test]
    fn status_request_and_response_roundtrip() {
        let req = StatusRequest {
            state: Some(StatusState::Paused),
            work: Some("compacting".into()),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["state"], "paused");
        assert_eq!(json["work"], "compacting");
        let back: StatusRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);

        let resp = StatusResponse {
            presence: Presence::Busy,
            current_work: Some("auth refactor".into()),
            paused: false,
        };
        let rj = serde_json::to_value(&resp).unwrap();
        assert_eq!(rj["presence"], "busy");
        assert_eq!(rj["currentWork"], "auth refactor");
        assert_eq!(rj["paused"], false);
        let rback: StatusResponse = serde_json::from_value(rj).unwrap();
        assert_eq!(rback, resp);
    }

    #[test]
    fn heartbeat_roundtrips() {
        let req = HeartbeatRequest {};
        let _ = serde_json::to_value(&req).unwrap();
        let resp = HeartbeatResponse { ok: true };
        assert_eq!(serde_json::to_value(&resp).unwrap()["ok"], true);
    }
}

mod rpc {
    use nexus_contracts::rpc::*;
    use nexus_contracts::send::{SendRequest, SendTarget};

    #[test]
    fn request_carries_jsonrpc_version_and_typed_params() {
        let params = SendRequest {
            to: SendTarget::Dm {
                name: Some("ben".into()),
                agent_id: None,
            },
            summary: None,
            body: "rebase first".into(),
            mention: vec![],
            idempotency_key: None,
        };
        let req = Request {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id: Some(RequestId::Num(7)),
            method: "send".into(),
            params: Some(serde_json::to_value(&params).unwrap()),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["id"], 7);
        assert_eq!(json["method"], "send");
        assert_eq!(json["params"]["to"]["verb"], "dm");

        let back: Request = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
        // typed dispatch: the registry says method "send" → params is SendRequest
        let typed: SendRequest = serde_json::from_value(back.params.unwrap()).unwrap();
        assert_eq!(typed, params);
    }

    #[test]
    fn ok_response_has_result_and_no_error() {
        let resp = Response {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id: Some(RequestId::Str("abc".into())),
            result: Some(serde_json::json!({ "acked": 2 })),
            error: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["id"], "abc");
        assert_eq!(json["result"]["acked"], 2);
        // exactly one of result/error → error omitted by skip_serializing_if
        assert!(json.get("error").is_none());
        let back: Response = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn error_response_uses_app_code_and_omits_result() {
        let resp = Response {
            jsonrpc: JSONRPC_VERSION.to_string(),
            id: Some(RequestId::Num(9)),
            result: None,
            error: Some(RpcError {
                code: codes::DUPLICATE_NAME,
                message: "name already bound".into(),
                data: None,
            }),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["error"]["code"], codes::DUPLICATE_NAME);
        assert_eq!(json["error"]["code"], -32002);
        assert!(json.get("result").is_none());
        let back: Response = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
        // mutually exclusive: this response carries an error, not a result
        assert!(back.result.is_none() && back.error.is_some());
    }

    #[test]
    fn notification_has_no_id() {
        let notif = Notification {
            jsonrpc: JSONRPC_VERSION.to_string(),
            method: "message.created".into(),
            params: Some(serde_json::json!({ "messageId": "m_01" })),
        };
        let json = serde_json::to_value(&notif).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["method"], "message.created");
        assert!(json.get("id").is_none(), "notifications carry no id");
        let back: Notification = serde_json::from_value(json).unwrap();
        assert_eq!(back, notif);
    }

    #[test]
    fn standard_codes_match_jsonrpc_spec() {
        assert_eq!(codes::PARSE_ERROR, -32700);
        assert_eq!(codes::INVALID_REQUEST, -32600);
        assert_eq!(codes::METHOD_NOT_FOUND, -32601);
        assert_eq!(codes::INVALID_PARAMS, -32602);
        assert_eq!(codes::INTERNAL_ERROR, -32603);
        assert_eq!(codes::ACTIVE_TURN_REQUIRED, -32006);
    }
}

mod search {
    use nexus_contracts::search::*;

    #[test]
    fn search_request_roundtrips() {
        let req = SearchRequest {
            query: "auth refactor".into(),
            mode: SearchMode::Hybrid,
            limit: Some(20),
            thread: Some("backend".into()),
            with: None,
            since: Some(1_700_000_000),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["query"], "auth refactor");
        assert_eq!(json["mode"], "hybrid");
        assert_eq!(json["limit"], 20);
        assert_eq!(json["since"], 1_700_000_000i64);
        let back: SearchRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn search_hit_roundtrips() {
        let resp = SearchResponse {
            hits: vec![SearchHit {
                message_id: nexus_contracts::ids::MessageId("m_05".into()),
                from: "dylan".into(),
                when: 1_700_000_001,
                snippet: "…the migration plan…".into(),
                score: 0.87,
            }],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["hits"][0]["messageId"], "m_05");
        assert!((json["hits"][0]["score"].as_f64().unwrap() - 0.87).abs() < 1e-6);
        let back: SearchResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn history_roundtrips() {
        let req = HistoryRequest {
            thread: None,
            with: Some("ben".into()),
            topic: None,
            limit: Some(50),
            before: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["with"], "ben");
        let back: HistoryRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);

        let resp = HistoryResponse {
            entries: vec![HistoryEntry {
                from: "ben".into(),
                when: 1_700_000_002,
                summary: Some("rebase note".into()),
                body: "rebase before you start".into(),
            }],
        };
        let rj = serde_json::to_value(&resp).unwrap();
        assert_eq!(rj["entries"][0]["from"], "ben");
        let rback: HistoryResponse = serde_json::from_value(rj).unwrap();
        assert_eq!(rback, resp);
    }
}

mod send {
    use nexus_contracts::ids::MessageId;
    use nexus_contracts::send::*;

    #[test]
    fn dm_send_request_roundtrips() {
        let req = SendRequest {
            to: SendTarget::Dm {
                name: Some("ben".into()),
                agent_id: None,
            },
            summary: None,
            body: "rebase before you start".into(),
            mention: vec![],
            idempotency_key: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["to"]["verb"], "dm");
        assert_eq!(json["to"]["name"], "ben");
        assert_eq!(json["body"], "rebase before you start");
        let back: SendRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn dm_target_with_known_identity_keeps_agent_id_and_name() {
        let target: SendTarget = serde_json::from_value(serde_json::json!({
            "verb": "dm",
            "agentId": "a_ben",
            "name": "ben"
        }))
        .unwrap();

        let json = serde_json::to_value(target).unwrap();
        assert_eq!(json["verb"], "dm");
        assert_eq!(json["agentId"], "a_ben");
        assert_eq!(json["name"], "ben");
    }

    #[test]
    fn dm_target_accepts_agent_id_without_name() {
        let target: SendTarget = serde_json::from_value(serde_json::json!({
            "verb": "dm",
            "agentId": "a_unnamed"
        }))
        .unwrap();

        let json = serde_json::to_value(target).unwrap();
        assert_eq!(json["agentId"], "a_unnamed");
        assert!(json.get("name").is_none());
    }

    #[test]
    fn dm_target_without_name_or_agent_id_is_rejected_by_request_validation() {
        let target: SendTarget =
            serde_json::from_value(serde_json::json!({ "verb": "dm" })).unwrap();
        let req = SendRequest {
            to: target,
            summary: None,
            body: "has a body but no recipient".into(),
            mention: vec![],
            idempotency_key: None,
        };

        let error = validate_send_request(&req).unwrap_err();
        assert_eq!(error.code, nexus_contracts::codes::INVALID_PARAMS);
        assert!(error.message.contains("name or agentId"));
    }

    #[test]
    fn post_thread_with_mention_roundtrips() {
        let req = SendRequest {
            to: SendTarget::Post {
                thread: "backend".into(),
            },
            summary: Some("plan".into()),
            body: "step 1 ...".into(),
            mention: vec!["dylan".into()],
            idempotency_key: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["to"]["verb"], "post");
        assert_eq!(json["to"]["thread"], "backend");
        assert_eq!(json["mention"][0], "dylan");
        let back: SendRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn publish_and_reply_roundtrip() {
        let pub_req = SendRequest {
            to: SendTarget::Publish { topic: "ci".into() },
            summary: None,
            body: "build green".into(),
            mention: vec![],
            idempotency_key: None,
        };
        assert_eq!(
            serde_json::to_value(&pub_req).unwrap()["to"]["verb"],
            "publish"
        );

        let reply = SendRequest {
            to: SendTarget::Reply,
            summary: None,
            body: "on it".into(),
            mention: vec![],
            idempotency_key: None,
        };
        let json = serde_json::to_value(&reply).unwrap();
        assert_eq!(json["to"]["verb"], "reply");
        let back: SendRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, reply);
    }

    #[test]
    fn ack_envelope_omits_internal_fanout_count() {
        let ack = Ack {
            message_id: MessageId("m_10".into()),
            fanout: Some(3),
        };
        let json = serde_json::to_value(&ack).unwrap();
        assert_eq!(json["messageId"], "m_10");
        assert!(
            json.get("fanout").is_none(),
            "recipient counts are internal routing telemetry, not envelope data"
        );
        let back: Ack = serde_json::from_value(json).unwrap();
        assert_eq!(back.message_id, ack.message_id);
        assert_eq!(back.fanout, None);
    }
}

mod source {
    use nexus_contracts::source::*;
    use nexus_contracts::MessageId;

    #[test]
    fn source_roundtrips() {
        let s = Source {
            name: "github-ci".into(),
            topic: "ci.events".into(),
            enabled: true,
            created_at: 1719300000,
            last_fired_at: Some(1719400000),
        };
        let json = serde_json::to_value(&s).unwrap();
        assert_eq!(json["name"], "github-ci");
        assert_eq!(json["topic"], "ci.events");
        assert_eq!(json["enabled"], true);
        assert_eq!(json["createdAt"], 1719300000_i64);
        assert_eq!(json["lastFiredAt"], 1719400000_i64);
        let back: Source = serde_json::from_value(json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn source_last_fired_at_is_omitted() {
        let s = Source {
            name: "cron".into(),
            topic: "cron.tick".into(),
            enabled: true,
            created_at: 1719300000,
            last_fired_at: None,
        };
        let json = serde_json::to_value(&s).unwrap();
        assert!(json.get("lastFiredAt").is_none());
        let back: Source = serde_json::from_value(json).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn source_register_request_roundtrips() {
        let req = SourceRegisterRequest {
            name: "github-ci".into(),
            topic: Some("ci.events".into()),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["name"], "github-ci");
        assert_eq!(json["topic"], "ci.events");
        let back: SourceRegisterRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn source_register_response_roundtrips() {
        let resp = SourceRegisterResponse {
            source: Source {
                name: "github-ci".into(),
                topic: "ci.events".into(),
                enabled: true,
                created_at: 1719300000,
                last_fired_at: None,
            },
            token: "tok_abc123".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["source"]["name"], "github-ci");
        assert_eq!(json["token"], "tok_abc123");
        let back: SourceRegisterResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn source_ref_roundtrips() {
        let r = SourceRef {
            name: "github-ci".into(),
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["name"], "github-ci");
        let back: SourceRef = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn source_list_response_roundtrips() {
        let resp = SourceListResponse {
            sources: vec![Source {
                name: "cron".into(),
                topic: "cron.tick".into(),
                enabled: false,
                created_at: 1719300000,
                last_fired_at: None,
            }],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["sources"][0]["name"], "cron");
        let back: SourceListResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn source_token_response_roundtrips() {
        let resp = SourceTokenResponse {
            name: "github-ci".into(),
            token: "tok_new99".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "github-ci");
        assert_eq!(json["token"], "tok_new99");
        let back: SourceTokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn push_request_roundtrips() {
        let req = PushRequest {
            source: "github-ci".into(),
            topic: Some("ci.events".into()),
            summary: Some("Build passed".into()),
            body: "All checks green on main@abc1234".into(),
            meta: Some(serde_json::json!({ "sha": "abc1234", "branch": "main" })),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["source"], "github-ci");
        assert_eq!(json["topic"], "ci.events");
        assert_eq!(json["summary"], "Build passed");
        assert_eq!(json["body"], "All checks green on main@abc1234");
        assert_eq!(json["meta"]["sha"], "abc1234");
        let back: PushRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn push_response_roundtrips() {
        let resp = PushResponse {
            topic: "ci.events".into(),
            message_id: Some(MessageId("m_push".into())),
            queued_to: 3,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["topic"], "ci.events");
        assert_eq!(json["messageId"], "m_push");
        assert_eq!(json["queuedTo"], 3_u32);
        assert!(json.get("deliveredTo").is_none());
        let back: PushResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn header_constants() {
        assert_eq!(SOURCE_SIGNATURE_HEADER, "X-Nexus-Signature");
        assert_eq!(SOURCE_TIMESTAMP_HEADER, "X-Nexus-Timestamp");
    }
}

mod threads {
    use nexus_contracts::threads::*;

    #[test]
    fn create_thread_roundtrips() {
        let req = CreateThreadRequest {
            name: "backend".into(),
            members: vec!["dylan".into()],
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["name"], "backend");
        assert_eq!(json["members"][0], "dylan");
        let back: CreateThreadRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn thread_list_and_members_roundtrip() {
        let list = ThreadListResponse {
            threads: vec![ThreadSummary {
                name: "backend".into(),
                topic: Some("backend work".into()),
                description: Some("daemon and store coordination".into()),
                members: vec!["ben".into(), "dylan".into()],
                last_at: Some(1_700_000_000),
                latest_seq: Some(42),
            }],
        };
        let json = serde_json::to_value(&list).unwrap();
        assert_eq!(json["threads"][0]["topic"], "backend work");
        assert_eq!(json["threads"][0]["lastAt"], 1_700_000_000i64);
        assert_eq!(json["threads"][0]["latestSeq"], 42);
        let back: ThreadListResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, list);

        let mem = ThreadMembersResponse {
            name: "backend".into(),
            members: vec!["ben".into()],
        };
        let mj = serde_json::to_value(&mem).unwrap();
        assert_eq!(mj["members"][0], "ben");
        let mback: ThreadMembersResponse = serde_json::from_value(mj).unwrap();
        assert_eq!(mback, mem);
    }

    #[test]
    fn join_leave_members_request_roundtrip() {
        let j = JoinThreadRequest {
            name: "backend".into(),
        };
        assert_eq!(serde_json::to_value(&j).unwrap()["name"], "backend");
        let l = LeaveThreadRequest {
            name: "backend".into(),
        };
        assert_eq!(serde_json::to_value(&l).unwrap()["name"], "backend");
        let a = ArchiveThreadRequest {
            name: "backend".into(),
        };
        assert_eq!(serde_json::to_value(&a).unwrap()["name"], "backend");
        let d = DeleteThreadRequest {
            name: "backend".into(),
        };
        assert_eq!(serde_json::to_value(&d).unwrap()["name"], "backend");
        let r = RenameThreadRequest {
            name: "backend".into(),
            new_name: "research".into(),
        };
        let rj = serde_json::to_value(&r).unwrap();
        assert_eq!(rj["name"], "backend");
        assert_eq!(rj["newName"], "research");
        let m = ThreadMembersRequest {
            name: "backend".into(),
        };
        assert_eq!(serde_json::to_value(&m).unwrap()["name"], "backend");
        assert_eq!(
            serde_json::from_value::<JoinThreadRequest>(serde_json::json!({"name":"x"})).unwrap(),
            JoinThreadRequest { name: "x".into() }
        );
    }
}

mod prompt {
    use nexus_contracts::{
        AgentId, CommandQueueAction, CommandQueueMutationRequest, SteerCapability, SteerDelivery,
        SteerRequest, SteerResponse,
    };

    #[test]
    #[allow(deprecated)] // Compatibility decoding remains supported; production no longer emits it.
    fn steer_request_and_response_roundtrip() {
        let request = SteerRequest {
            agent_id: Some(AgentId("a_otto".into())),
            name: "otto".into(),
            text: "focus on the failing test".into(),
            client_message_id: Some("cm_1".into()),
        };
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["agentId"], "a_otto");
        assert_eq!(json["clientMessageId"], "cm_1");
        assert_eq!(
            serde_json::from_value::<SteerRequest>(json).unwrap(),
            request
        );

        let response = SteerResponse {
            accepted: true,
            delivery: SteerDelivery::FallbackStarted,
            turn_id: Some("turn_1".into()),
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["delivery"], "fallback_started");
        assert_eq!(json["turnId"], "turn_1");
        assert_eq!(
            serde_json::from_value::<SteerResponse>(json).unwrap(),
            response
        );
    }

    #[test]
    fn durable_queue_mutation_uses_exact_redirect_and_capability_tokens() {
        let request = CommandQueueMutationRequest {
            name: "otto".into(),
            agent_id: Some(AgentId("a_otto".into())),
            action: CommandQueueAction::RedirectNow,
            client_mutation_id: "mut_1".into(),
            command_id: Some("cmd_1".into()),
            expected_revision: Some(4),
            text: None,
            command_ids: Vec::new(),
            expected_revisions: Vec::new(),
        };
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["action"], "redirect_now");
        assert_eq!(json["clientMutationId"], "mut_1");
        assert_eq!(json["expectedRevision"], 4);
        assert_eq!(
            serde_json::from_value::<CommandQueueMutationRequest>(json).unwrap(),
            request
        );

        assert_eq!(
            serde_json::to_value(SteerCapability::NativeSteer).unwrap(),
            "native_steer"
        );
        assert_eq!(
            serde_json::to_value(SteerCapability::InterruptAndSend).unwrap(),
            "interrupt_and_send"
        );
        assert_eq!(serde_json::to_value(SteerCapability::None).unwrap(), "none");
    }
}

mod topics {
    use nexus_contracts::topics::*;

    #[test]
    fn subscribe_roundtrips() {
        let req = SubscribeRequest {
            topic: "ci".into(),
            group: Some("workers".into()),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["topic"], "ci");
        assert_eq!(json["group"], "workers");
        let back: SubscribeRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn subscribe_response_and_list_roundtrip() {
        let resp = SubscribeResponse {
            topic: "ci".into(),
            cursor: 42,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["cursor"], 42);
        let back: SubscribeResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);

        let list = TopicListResponse {
            topics: vec![TopicSummary {
                topic: "pub".into(),
                subscribers: 3,
            }],
        };
        let lj = serde_json::to_value(&list).unwrap();
        assert_eq!(lj["topics"][0]["subscribers"], 3);
        let lback: TopicListResponse = serde_json::from_value(lj).unwrap();
        assert_eq!(lback, list);
    }

    #[test]
    fn unsubscribe_roundtrips() {
        let u = UnsubscribeRequest { topic: "ci".into() };
        assert_eq!(serde_json::to_value(&u).unwrap()["topic"], "ci");
    }
}
