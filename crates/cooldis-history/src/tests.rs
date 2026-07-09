use super::*;

fn coords(tenant: &str, user: &str, session: &str) -> ThreadCoordinates {
    ThreadCoordinates::new(tenant, user, session)
}

fn message_texts(messages: &[CanonicalMessage]) -> Vec<&str> {
    messages
        .iter()
        .map(|message| match message {
            CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or(""),
        })
        .collect()
}

fn message_timestamps(messages: &[CanonicalMessage]) -> Vec<i64> {
    messages
        .iter()
        .map(|message| match message {
            CanonicalMessage::User { timestamp_ms, .. }
            | CanonicalMessage::Assistant { timestamp_ms, .. }
            | CanonicalMessage::ToolResult { timestamp_ms, .. } => *timestamp_ms,
        })
        .collect()
}

#[tokio::test]
async fn append_only_context_follows_active_branch() {
    let store = InMemorySessionStore::new();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let root = store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let left = store
        .append(
            &coordinates,
            Some(root.entry_id),
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("left"),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &coordinates,
            Some(root.entry_id),
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("right"),
            },
        )
        .await
        .unwrap();

    assert_ne!(
        store.active_leaf(&coordinates).await.unwrap(),
        Some(left.entry_id)
    );
    let context = store.build_context(&coordinates).await.unwrap();
    assert_eq!(context.messages.len(), 2);
    assert_eq!(context.entries[0].entry_id, root.entry_id);
    assert_eq!(message_texts(&context.messages), vec!["root", "right"]);
}

#[tokio::test]
async fn select_branch_restores_checkpoint_leaf() {
    let store = InMemorySessionStore::new();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let checkpoint_leaf = store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Runtime {
                kind: "thread_checkpoint".to_string(),
                payload: serde_json::json!({"checkpoint_id":"checkpoint"}),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("after"),
            },
        )
        .await
        .unwrap();

    store
        .select_branch(&coordinates, Some(checkpoint_leaf.entry_id))
        .await
        .unwrap();

    let context = store.build_context(&coordinates).await.unwrap();
    assert_eq!(message_texts(&context.messages), vec!["root"]);
    assert_eq!(
        context.entries.last().unwrap().entry_id,
        checkpoint_leaf.entry_id
    );
}

#[tokio::test]
async fn clone_branch_copies_source_lineage_into_new_thread() {
    let store = InMemorySessionStore::new();
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    store
        .append(
            &source,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let source_leaf = store
        .append(
            &source,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("source"),
            },
        )
        .await
        .unwrap();

    let target_leaf = store
        .clone_branch(&source, Some(source_leaf.entry_id), &target)
        .await
        .unwrap()
        .unwrap();

    assert_ne!(target_leaf, source_leaf.entry_id);
    let target_context = store.build_context(&target).await.unwrap();
    assert_eq!(
        message_texts(&target_context.messages),
        vec!["root", "source"]
    );
    assert!(
        target_context
            .entries
            .iter()
            .all(|entry| entry.coordinates == target)
    );
}

#[tokio::test]
async fn fork_by_reference_builds_context_without_copying_source_events() {
    let store = InMemorySessionStore::new();
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    let root = store
        .append(
            &source,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let source_leaf = store
        .append(
            &source,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("source"),
            },
        )
        .await
        .unwrap();
    let source_stream = EventStreamId::for_thread(&source);
    let target_stream = EventStreamId::for_thread(&target);

    store
        .fork_by_reference(
            &source,
            &target,
            ThreadBaseRef {
                child_thread_id: target.thread_id,
                parent_thread_id: source.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(source_leaf.entry_id),
                parent_stream_id: source_stream.clone(),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: Some("sha256:parent".to_string()),
                reason: ThreadForkReason::ManifestUpdate,
                created_at_ms: now_ms(),
            },
        )
        .await
        .unwrap();

    let target_context = store.build_context(&target).await.unwrap();
    assert_eq!(
        message_texts(&target_context.messages),
        vec!["root", "source"]
    );
    assert_eq!(target_context.entries[0].coordinates, source);
    assert_eq!(target_context.entries[1].coordinates, source);
    assert_eq!(
        target_context.source_cuts,
        vec![SessionContextSourceCut {
            coordinates: source.clone(),
            stream_id: source_stream.clone(),
            inherited: true,
            entry_ids: vec![root.entry_id, source_leaf.entry_id],
        }]
    );
    assert!(
        store
            .read_events(&target_stream, None)
            .await
            .unwrap()
            .is_empty()
    );

    let child = store
        .append(
            &target,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("child"),
            },
        )
        .await
        .unwrap();
    let target_context = store.build_context(&target).await.unwrap();
    assert_eq!(
        message_texts(&target_context.messages),
        vec!["root", "source", "child"]
    );
    assert_eq!(target_context.entries.last().unwrap().coordinates, target);
    assert_eq!(
        target_context.source_cuts.last(),
        Some(&SessionContextSourceCut {
            coordinates: target.clone(),
            stream_id: target_stream,
            inherited: false,
            entry_ids: vec![child.entry_id],
        })
    );
}

#[tokio::test]
async fn fork_by_reference_rejects_cycles_and_cross_scope_edges() {
    let store = InMemorySessionStore::new();
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    let source_leaf = store
        .append(
            &source,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("source"),
            },
        )
        .await
        .unwrap();

    store
        .fork_by_reference(
            &source,
            &target,
            ThreadBaseRef {
                child_thread_id: target.thread_id,
                parent_thread_id: source.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(source_leaf.entry_id),
                parent_stream_id: EventStreamId::for_thread(&source),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: ThreadForkReason::Manual,
                created_at_ms: now_ms(),
            },
        )
        .await
        .unwrap();

    let cycle_err = store
        .fork_by_reference(
            &target,
            &source,
            ThreadBaseRef {
                child_thread_id: source.thread_id,
                parent_thread_id: target.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: None,
                parent_stream_id: EventStreamId::for_thread(&target),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: ThreadForkReason::Manual,
                created_at_ms: now_ms(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(cycle_err, HistoryError::ThreadBaseCycle { .. }));

    let wrong_scope_target = coords("tenant_b", "user_1", "session_1");
    let scope_err = store
        .fork_by_reference(
            &source,
            &wrong_scope_target,
            ThreadBaseRef {
                child_thread_id: wrong_scope_target.thread_id,
                parent_thread_id: source.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(source_leaf.entry_id),
                parent_stream_id: EventStreamId::for_thread(&source),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: ThreadForkReason::Manual,
                created_at_ms: now_ms(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        scope_err,
        HistoryError::ThreadScopeMismatch { .. }
    ));
}

#[tokio::test]
async fn compaction_clears_prior_model_visible_messages() {
    let store = InMemorySessionStore::new();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("old"),
            },
        )
        .await
        .unwrap();
    let compacted = store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Compaction {
                summary: "old summary".to_string(),
            },
        )
        .await
        .unwrap();

    let context = store.build_context(&coordinates).await.unwrap();
    assert_eq!(context.entries.len(), 2);
    assert_eq!(context.entries.last().unwrap().entry_id, compacted.entry_id);
    assert_eq!(
        message_texts(&context.messages),
        vec!["Compacted conversation summary:\nold summary"]
    );
}

#[test]
fn model_visible_context_rebuild_preserves_persisted_timestamps() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let mut old = SessionEntry::new(
        coordinates.clone(),
        None,
        SessionEntryKind::Message {
            message: CanonicalMessage::User {
                content: vec![CanonicalContent::text("old")],
                timestamp_ms: 10,
            },
        },
    );
    old.created_at_ms = 10;
    let mut compacted = SessionEntry::new(
        coordinates.clone(),
        Some(old.entry_id),
        SessionEntryKind::Compaction {
            summary: "old summary".to_string(),
        },
    );
    compacted.created_at_ms = 20;
    let mut hook = SessionEntry::new(
        coordinates,
        Some(compacted.entry_id),
        SessionEntryKind::CustomContextMessage {
            message: CanonicalMessage::User {
                content: vec![CanonicalContent::text("persisted hook context")],
                timestamp_ms: 30,
            },
        },
    );
    hook.created_at_ms = 30;
    let entries = vec![old, compacted, hook];
    let reopened_entries = entries
        .iter()
        .map(|entry| decode_entry(&serde_json::to_string(entry).unwrap()).unwrap())
        .collect::<Vec<_>>();

    let mut first = Vec::new();
    append_model_visible_messages(&entries, &mut first);
    let mut reopened = Vec::new();
    append_model_visible_messages(&reopened_entries, &mut reopened);

    assert_eq!(first, reopened);
    assert_eq!(
        message_texts(&first),
        vec![
            "Compacted conversation summary:\nold summary",
            "persisted hook context"
        ]
    );
    assert_eq!(message_timestamps(&first), vec![20, 30]);
}

#[tokio::test]
async fn histories_are_isolated_by_thread_coordinates() {
    let store = InMemorySessionStore::new();
    let first = coords("tenant_a", "user_1", "session_1");
    let second = coords("tenant_b", "user_1", "session_1");
    store
        .append(
            &first,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("first"),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &second,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("second"),
            },
        )
        .await
        .unwrap();

    assert_eq!(
        message_texts(&store.build_context(&first).await.unwrap().messages),
        vec!["first"]
    );
    assert_eq!(
        message_texts(&store.build_context(&second).await.unwrap().messages),
        vec!["second"]
    );
}

#[test]
fn event_kind_parse_round_trips_and_fails_closed() {
    let expected = [
        "session.entry.appended",
        "context.compile.completed",
        "context.summary.completed",
        "context.read_plan.set",
        "manifest.compile.completed",
        "manifest.bind.completed",
        "tool.universe.discovery.completed",
        "tool.universe.call.completed",
        "tool.call.requested",
        "tool.call.suspended",
        "tool.call.decision",
        "tool.call.completed",
        "turn.submitted",
        "turn.waiting",
        "turn.resumed",
        "turn.completed",
        "approval.requested",
        "approval.resolved",
        "mandate.started",
        "mandate.revoked",
        "turn.continue.requested",
        "turn.continuation.accepted",
        "turn.continuation.rejected",
        "loop.completed",
        "loop.blocked",
        "loop.budget_exhausted",
        "loop.denied",
        "coupling.run.completed",
        "coupling.run.failed",
        "placement.decision",
        "thread.spawn.requested",
        "thread.spawned",
        "thread.joined",
        "policy.bound",
        "grant.petitioned",
        "timer.fired",
        "io.ingress.received",
        "io.egress.requested",
        "io.egress.delivered",
        "io.egress.failed",
        "admission.decided",
    ];
    assert_eq!(EVENT_KIND_SCHEMA_VERSION, "cooldis.events/0.2");
    let kinds = EventKind::all();
    assert_eq!(
        kinds.iter().map(|kind| kind.as_str()).collect::<Vec<_>>(),
        expected
    );
    for kind in kinds {
        assert_eq!(kind.as_str().parse::<EventKind>().unwrap(), *kind);
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(serde_json::from_str::<EventKind>(&json).unwrap(), *kind);
    }

    let err = "unknown.event.kind".parse::<EventKind>().unwrap_err();
    assert!(matches!(err, HistoryError::Codec(message) if message.contains("unknown event kind")));
    let err = serde_json::from_str::<EventKind>("\"unknown.event.kind\"").unwrap_err();
    assert!(err.to_string().contains("unknown event kind"));
}

#[test]
fn event_kind_payload_schema_ids_are_frozen_for_stream_schema_v1() {
    assert_eq!(
        EventKind::ContextCompileCompleted.payload_schema_id(),
        "cooldis.event.context.compile.completed/1"
    );
    assert_eq!(
        EventKind::ContextSummaryCompleted.payload_schema_id(),
        "cooldis.event.context.summary.completed/1"
    );
    assert_eq!(
        EventKind::ContextReadPlanSet.payload_schema_id(),
        "cooldis.event.context.read_plan.set/1"
    );
    assert_eq!(
        EventKind::ThreadSpawnRequested.payload_schema_id(),
        "cooldis.event.thread.spawn.requested/1"
    );
    assert_eq!(
        EventKind::ThreadSpawned.payload_schema_id(),
        "cooldis.event.thread.spawned/1"
    );
    assert_eq!(
        EventKind::ThreadJoined.payload_schema_id(),
        "cooldis.event.thread.joined/1"
    );
    assert_eq!(
        EventKind::PolicyBound.payload_schema_id(),
        "cooldis.event.policy.bound/1"
    );
    assert_eq!(
        EventKind::GrantPetitioned.payload_schema_id(),
        "cooldis.event.grant.petitioned/1"
    );
    assert_eq!(
        EventKind::TimerFired.payload_schema_id(),
        "cooldis.event.timer.fired/1"
    );
    assert_eq!(
        EventKind::IoIngressReceived.payload_schema_id(),
        "cooldis.event.io.ingress.received/1"
    );
    assert_eq!(
        EventKind::IoEgressRequested.payload_schema_id(),
        "cooldis.event.io.egress.requested/1"
    );
    assert_eq!(
        EventKind::IoEgressDelivered.payload_schema_id(),
        "cooldis.event.io.egress.delivered/1"
    );
    assert_eq!(
        EventKind::IoEgressFailed.payload_schema_id(),
        "cooldis.event.io.egress.failed/1"
    );
    assert_eq!(
        EventKind::AdmissionDecided.payload_schema_id(),
        "cooldis.event.admission.decided/1"
    );
}

#[test]
fn events_0_2_payload_fixtures_round_trip_and_validate() {
    let parent_thread_id = ThreadId::parse_str("018f0000-0000-7000-8000-000000000001").unwrap();
    let child_thread_id = ThreadId::parse_str("018f0000-0000-7000-8000-000000000002").unwrap();
    let spawned_event_id = EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000003").unwrap(),
    );
    let checkpoint_id = ThreadCheckpointId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000006").unwrap(),
    );
    let leaf_entry_id = SessionEntryId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000007").unwrap(),
    );
    let mandate_event_id = EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000004").unwrap(),
    );
    let ingress_event_id = EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000005").unwrap(),
    );
    let registry = stream_schema_registry_v1().unwrap();

    let cases = [
        (
            EventKind::ThreadSpawnRequested,
            serde_json::to_value(ThreadSpawnRequestedPayload {
                parent_thread_id,
                parent_turn_id: Some("turn-parent".to_string()),
                child_agent_ref: "agent://release-worker".to_string(),
                initial_submission: "collect release evidence".to_string(),
                correlation_id: "spawn-release-worker-1".to_string(),
                block_parent: true,
            })
            .unwrap(),
        ),
        (
            EventKind::ThreadSpawned,
            serde_json::to_value(ThreadSpawnedPayload {
                parent_thread_id,
                parent_turn_id: Some("turn-parent".to_string()),
                child_thread_id,
                child_manifest_hash: "sha256:child-manifest".to_string(),
                child_policy_hash: Some("sha256:child-policy".to_string()),
                granted: vec![
                    "threads.spawn".to_string(),
                    "stream.write:control".to_string(),
                ],
                inputs_hash: "sha256:inputs".to_string(),
                fork: None,
            })
            .unwrap(),
        ),
        (
            EventKind::ThreadSpawned,
            serde_json::to_value(ThreadSpawnedPayload {
                parent_thread_id,
                parent_turn_id: None,
                child_thread_id,
                child_manifest_hash: "sha256:fork-child-manifest".to_string(),
                child_policy_hash: None,
                granted: vec!["threads.spawn".to_string()],
                inputs_hash: "sha256:fork-inputs".to_string(),
                fork: Some(ThreadSpawnedForkPayload {
                    mode: "clone".to_string(),
                    source_cut: ThreadSpawnedForkSourceCutPayload {
                        thread_id: parent_thread_id,
                        checkpoint_id,
                        leaf_entry_id: Some(leaf_entry_id),
                        stream_id: EventStreamId::new(format!("thread:{parent_thread_id}")),
                        stream_to_sequence: Some(EventSequence::new(42)),
                    },
                }),
            })
            .unwrap(),
        ),
        (
            EventKind::ThreadJoined,
            serde_json::to_value(ThreadJoinedPayload {
                child_thread_id,
                spawned_event_id,
                terminal_state: ThreadTerminalState::Completed,
                result_digest: Some("sha256:result".to_string()),
            })
            .unwrap(),
        ),
        (
            EventKind::PolicyBound,
            serde_json::to_value(PolicyBoundPayload {
                policy_kind: PolicyKind::AdmissionRoute,
                policy_id: "route:telegram".to_string(),
                content_hash: "sha256:policy".to_string(),
                valid_from_note: "valid until next policy.bound of same policy_id".to_string(),
            })
            .unwrap(),
        ),
        (
            EventKind::GrantPetitioned,
            serde_json::to_value(GrantPetitionedPayload {
                thread_id: child_thread_id,
                requested: vec!["net:https://api.example.test".to_string()],
                reason: "tool needs outbound API access".to_string(),
                evidence_event_ids: Some(vec![spawned_event_id]),
            })
            .unwrap(),
        ),
        (
            EventKind::TimerFired,
            serde_json::to_value(TimerFiredPayload {
                mandate_event_id,
                scheduled_for: "2026-07-04T12:00:00Z".to_string(),
                occurrence_index: 3,
                catch_up: true,
            })
            .unwrap(),
        ),
        (
            EventKind::IoIngressReceived,
            serde_json::to_value(IoIngressReceivedPayload {
                route_id: Some("route:telegram".to_string()),
                dedupe_key: Some("telegram:42".to_string()),
                external_conversation_id: Some("chat-1".to_string()),
                external_actor_id: Some("actor-1".to_string()),
                external_message_id: Some("message-1".to_string()),
                envelope_digest: "sha256:ingress".to_string(),
            })
            .unwrap(),
        ),
        (
            EventKind::IoEgressDelivered,
            serde_json::to_value(IoEgressDeliveredPayload {
                route_id: "route:telegram".to_string(),
                egress_kind: "telegram.reply".to_string(),
                external_message_id: Some("message-2".to_string()),
                attempts: 2,
            })
            .unwrap(),
        ),
        (
            EventKind::IoEgressRequested,
            serde_json::to_value(IoEgressRequestedPayload {
                egress_kind: serde_json::json!({
                    "type": "platform_action",
                    "action": "reaction",
                    "payload": {
                        "message_id": "message-1",
                        "emoji": "👍"
                    }
                }),
                resolved_target: Some(serde_json::json!({
                    "source": {"protocol": "telegram.bot", "instance_id": "main"},
                    "conversation": {
                        "external_conversation_id": "chat-1",
                        "kind": "direct"
                    },
                    "actor": {"external_actor_id": "actor-1"},
                    "metadata": {}
                })),
                requested_by_tool_call_id: "call_1".to_string(),
                quote: Some("hello there".to_string()),
                match_event_id: Some(ingress_event_id),
            })
            .unwrap(),
        ),
        (
            EventKind::IoEgressFailed,
            serde_json::to_value(IoEgressFailedPayload {
                route_id: "route:telegram".to_string(),
                egress_kind: "telegram.reply".to_string(),
                attempts: 3,
                error_class: "rate_limited".to_string(),
                dead_lettered: true,
            })
            .unwrap(),
        ),
        (
            EventKind::AdmissionDecided,
            serde_json::to_value(AdmissionDecidedPayload {
                route_id: "route:telegram".to_string(),
                policy_hash: "sha256:admission-policy".to_string(),
                decision: AdmissionDecision::Coalesce,
                admissible: Some(vec![
                    AdmissionDecision::Queue,
                    AdmissionDecision::Coalesce,
                    AdmissionDecision::Reject,
                ]),
                source_ingress_event_ids: vec![ingress_event_id],
            })
            .unwrap(),
        ),
    ];

    for (kind, payload) in cases {
        let json = serde_json::to_string(&payload).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, payload);
        registry
            .validate(kind.payload_schema_id(), &decoded)
            .unwrap();
    }
}

#[test]
fn events_0_2_optional_fields_deserialize_when_absent() {
    let parent_thread_id = ThreadId::parse_str("018f0000-0000-7000-8000-000000000011").unwrap();
    let child_thread_id = ThreadId::parse_str("018f0000-0000-7000-8000-000000000012").unwrap();
    let spawned_event_id = EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000013").unwrap(),
    );
    let ingress_event_id = EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000014").unwrap(),
    );

    let spawned: ThreadSpawnedPayload = serde_json::from_value(serde_json::json!({
        "schema": EventKind::ThreadSpawned.payload_schema_id(),
        "parent_thread_id": parent_thread_id,
        "child_thread_id": child_thread_id,
        "child_manifest_hash": "sha256:child-manifest",
        "granted": [],
        "inputs_hash": "sha256:inputs"
    }))
    .unwrap();
    assert_eq!(spawned.parent_turn_id, None);
    assert_eq!(spawned.child_policy_hash, None);
    assert_eq!(spawned.fork, None);

    let joined: ThreadJoinedPayload = serde_json::from_value(serde_json::json!({
        "schema": EventKind::ThreadJoined.payload_schema_id(),
        "child_thread_id": child_thread_id,
        "spawned_event_id": spawned_event_id,
        "terminal_state": "budget_exhausted"
    }))
    .unwrap();
    assert_eq!(joined.result_digest, None);

    let ingress: IoIngressReceivedPayload = serde_json::from_value(serde_json::json!({
        "schema": EventKind::IoIngressReceived.payload_schema_id(),
        "envelope_digest": "sha256:ingress"
    }))
    .unwrap();
    assert_eq!(ingress.route_id, None);
    assert_eq!(ingress.external_message_id, None);

    let admission: AdmissionDecidedPayload = serde_json::from_value(serde_json::json!({
        "schema": EventKind::AdmissionDecided.payload_schema_id(),
        "route_id": "route:telegram",
        "policy_hash": "sha256:admission-policy",
        "decision": "queue",
        "source_ingress_event_ids": [ingress_event_id]
    }))
    .unwrap();
    assert_eq!(admission.admissible, None);
}

#[test]
fn events_0_1_style_stream_record_still_parses_and_unknown_kind_fails_closed() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let record = serde_json::json!({
        "schema": STREAM_RECORD_SCHEMA_V1,
        "event_id": "018f0000-0000-7000-8000-000000000101",
        "stream_id": EventStreamId::for_thread(&coordinates),
        "sequence": 1,
        "coordinates": coordinates,
        "created_at_ms": 1_772_640_000_000i64,
        "kind": "turn.completed",
        "origin": "discharged",
        "payload_schema": EventKind::TurnCompleted.payload_schema_id(),
        "provenance": {"source_event_ids": ["018f0000-0000-7000-8000-000000000100"]},
        "payload": {"turn_id": "turn-1"}
    });
    let parsed: StreamRecordEnvelopeV1 = serde_json::from_value(record).unwrap();
    assert_eq!(
        parsed.kind.parse::<EventKind>().unwrap(),
        EventKind::TurnCompleted
    );
    stream_schema_registry_v1()
        .unwrap()
        .validate(
            STREAM_RECORD_SCHEMA_V1,
            &serde_json::to_value(parsed).unwrap(),
        )
        .unwrap();

    let unknown = serde_json::json!({
        "kind": "unknown.event.kind"
    });
    let err = serde_json::from_value::<EventKind>(unknown["kind"].clone()).unwrap_err();
    assert!(err.to_string().contains("unknown event kind"));
}

#[test]
fn canonical_usage_survives_assistant_session_entry_stream_record() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let usage = CanonicalUsage {
        input_tokens: 11,
        output_tokens: 7,
        cache_creation_input_tokens: 3,
        cache_read_input_tokens: 5,
    };
    let entry = SessionEntry::new(
        coordinates.clone(),
        None,
        SessionEntryKind::Message {
            message: CanonicalMessage::assistant_with_usage(
                "test-provider",
                ProviderApi::OpenAIResponses,
                "model-1",
                vec![CanonicalContent::text("hello")],
                usage.clone(),
                CanonicalStopReason::EndTurn,
            ),
        },
    );
    let event = EventRecord::from_new(
        EventStreamId::for_thread(&coordinates),
        EventSequence::new(1),
        session_entry_event(&entry),
    );
    let envelope = event.to_stream_record_v1();
    assert_eq!(envelope.kind, EventKind::SessionEntryAppended.as_str());
    assert_eq!(
        envelope.payload["usage"],
        serde_json::to_value(usage).unwrap()
    );
    event.validate_stream_record_v1().unwrap();
}

#[test]
fn event_record_renders_stream_schema_v1_envelope() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let record = EventRecord::from_new(
        stream_id.clone(),
        EventSequence::new(7),
        NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::ContextReadPlanSet,
            serde_json::json!({
                "schema": "cooldis.event.context.read_plan.set/1",
                "scope": "thread",
                "name": "history.default",
                "read_plan": {
                    "schema": "cooldis.context.read_plan/1",
                    "name": "history.default",
                    "source_stream": stream_id.as_str(),
                    "frontier": "compile_frontier",
                    "entries": []
                }
            }),
            EventProvenance {
                source_streams: vec![stream_id.clone()],
                discharged_by: Some("controller:context-budget".to_string()),
                function: Some("context_read_plan/v1".to_string()),
                config_hash: Some("sha256:context-budget-config".to_string()),
                ..EventProvenance::default()
            },
        ),
    );

    let envelope = record.to_stream_record_v1();
    assert_eq!(envelope.schema, STREAM_RECORD_SCHEMA_V1);
    assert_eq!(envelope.event_id, record.id);
    assert_eq!(envelope.stream_id, stream_id);
    assert_eq!(envelope.sequence, EventSequence::new(7));
    assert_eq!(envelope.kind, "context.read_plan.set");
    assert_eq!(
        envelope.payload_schema,
        "cooldis.event.context.read_plan.set/1"
    );
    assert_eq!(envelope.payload["read_plan"]["name"], "history.default");
    assert!(!envelope.provenance.is_empty());

    let json = serde_json::to_value(&envelope).unwrap();
    assert_eq!(json["schema"], "cooldis.stream.record/1");
    assert_eq!(json["event_id"], record.id.to_string());
    assert_eq!(
        json["provenance"]["config_hash"],
        "sha256:context-budget-config"
    );
    assert_eq!(json["sequence"], 7);
    assert_eq!(json["origin"], "discharged");
}

#[test]
fn stream_routing_decision_v1_uses_envelope_fields_not_payload() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let record = EventRecord::from_new(
        stream_id.clone(),
        EventSequence::new(8),
        NewEventRecord::witnessed(
            coordinates,
            EventKind::ToolCallCompleted,
            serde_json::json!({
                "schema": EventKind::ToolCallCompleted.payload_schema_id(),
                "payload_bait": {
                    "kind": "placement.decision",
                    "stream_id": "control:pretend",
                    "routing_profile": "runtime_trace"
                }
            }),
        ),
    );
    let mut envelope = record.to_stream_record_v1();
    envelope.trace_context = Some(serde_json::json!({
        "trace_id": "trace-1",
        "span_id": "span-1"
    }));

    let decision = envelope.route_decision_v1();
    assert_eq!(decision.schema, STREAM_ROUTING_DECISION_SCHEMA_V1);
    assert_eq!(decision.event_id, record.id);
    assert_eq!(decision.stream_id, stream_id);
    assert_eq!(
        decision.routes,
        vec![
            StreamRouteProfile::AuthorityStore,
            StreamRouteProfile::ExportBundle,
            StreamRouteProfile::ModelTrace,
            StreamRouteProfile::BrowserSafeProjection,
            StreamRouteProfile::AnalyticsAggregate,
        ]
    );
    assert_eq!(decision.keys.kind, "tool.call.completed");
    assert_eq!(decision.keys.trace_id.as_deref(), Some("trace-1"));

    let mut baited = envelope;
    baited.payload = serde_json::json!({
        "kind": "loop.budget_exhausted",
        "payload_schema": "cooldis.event.loop.budget_exhausted/1",
        "routing_profile": "analytics_aggregate"
    });
    assert_eq!(baited.route_decision_v1(), decision);

    stream_schema_registry_v1()
        .unwrap()
        .validate(
            STREAM_ROUTING_DECISION_SCHEMA_V1,
            &serde_json::to_value(decision).unwrap(),
        )
        .unwrap();
}

#[test]
fn stream_routing_decision_v1_separates_runtime_and_model_trace_profiles() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let record = EventRecord::from_new(
        stream_id,
        EventSequence::new(9),
        NewEventRecord::discharged(
            coordinates,
            EventKind::PlacementDecision,
            serde_json::json!({
                "schema": EventKind::PlacementDecision.payload_schema_id(),
                "placement": "local"
            }),
            EventProvenance {
                discharged_by: Some("controller:placement".to_string()),
                ..EventProvenance::default()
            },
        ),
    );

    let decision = record.route_decision_v1();
    assert!(decision.routes.contains(&StreamRouteProfile::RuntimeTrace));
    assert!(!decision.routes.contains(&StreamRouteProfile::ModelTrace));
    assert_eq!(
        decision.keys.discharged_by.as_deref(),
        Some("controller:placement")
    );
    assert_eq!(decision.keys.stream_id, record.stream_id);
}

#[test]
fn stream_append_ack_v1_freezes_ack_classes_and_tail_identity() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let first = EventRecord::from_new(
        stream_id.clone(),
        EventSequence::new(10),
        NewEventRecord::witnessed(
            coordinates.clone(),
            EventKind::TurnSubmitted,
            serde_json::json!({
                "schema": EventKind::TurnSubmitted.payload_schema_id(),
                "turn_id": "turn-1"
            }),
        ),
    );
    let second = EventRecord::from_new(
        stream_id.clone(),
        EventSequence::new(11),
        NewEventRecord::witnessed(
            coordinates,
            EventKind::TurnCompleted,
            serde_json::json!({
                "schema": EventKind::TurnCompleted.payload_schema_id(),
                "turn_id": "turn-1"
            }),
        ),
    );

    let ack = StreamAppendAckV1::from_appended(
        stream_id.clone(),
        &[first.clone(), second.clone()],
        vec![
            StreamAckClass::LocalCommitted,
            StreamAckClass::QueryProjected,
        ],
    )
    .unwrap();
    assert_eq!(ack.schema, STREAM_APPEND_ACK_SCHEMA_V1);
    assert_eq!(ack.stream_id, stream_id);
    assert_eq!(ack.start_sequence, EventSequence::new(10));
    assert_eq!(ack.end_sequence, EventSequence::new(11));
    assert_eq!(ack.tail_sequence, EventSequence::new(11));
    assert_eq!(ack.tail_event_id, second.id);
    assert_eq!(
        ack.acks,
        vec![
            StreamAckClass::LocalCommitted,
            StreamAckClass::QueryProjected
        ]
    );

    stream_schema_registry_v1()
        .unwrap()
        .validate(
            STREAM_APPEND_ACK_SCHEMA_V1,
            &serde_json::to_value(ack).unwrap(),
        )
        .unwrap();

    let empty =
        StreamAppendAckV1::from_appended(stream_id, &[], vec![StreamAckClass::LocalCommitted])
            .unwrap_err();
    assert!(
        empty
            .to_string()
            .contains("append ack requires at least one event")
    );
}

#[test]
fn stream_backend_capabilities_v1_freezes_sqlite_reference_shape() {
    let capabilities =
        StreamBackendCapabilitiesV1::sqlite_local("/tmp/cooldis/session_history.sqlite3");

    assert_eq!(capabilities.schema, STREAM_BACKEND_CAPABILITIES_SCHEMA_V1);
    assert_eq!(capabilities.backend_kind, StreamBackendKindV1::Sqlite);
    assert_eq!(
        capabilities.storage_scope,
        StreamStorageScopeV1::LocalEmbedded
    );
    assert_eq!(
        capabilities.ack_classes,
        vec![
            StreamAckClass::LocalCommitted,
            StreamAckClass::QueryProjected
        ]
    );
    assert!(capabilities.supports_atomic_batch_append);
    assert!(capabilities.supports_verified_cursor_replay);
    assert!(capabilities.supports_query_projection);
    assert!(!capabilities.supports_expected_tail);
    assert!(!capabilities.supports_fencing_tokens);
    assert!(!capabilities.supports_live_follow);
    assert!(!capabilities.supports_broadcast);
    assert!(!capabilities.supports_cold_archive);
    assert_eq!(
        capabilities.local_path.as_deref(),
        Some("/tmp/cooldis/session_history.sqlite3")
    );

    stream_schema_registry_v1()
        .unwrap()
        .validate(
            STREAM_BACKEND_CAPABILITIES_SCHEMA_V1,
            &serde_json::to_value(capabilities).unwrap(),
        )
        .unwrap();
}

#[test]
fn stream_schema_registry_v1_validates_envelopes_and_context_payloads() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let source_range = ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence: EventSequence::new(1),
        to_sequence: EventSequence::new(2),
    };
    let read_plan = serde_json::json!({
        "schema": CONTEXT_READ_PLAN_SCHEMA_V1,
        "name": "history.default",
        "source_stream": stream_id.as_str(),
        "frontier": "compile_frontier",
        "entries": [{
            "kind": "raw_range",
            "stream_id": stream_id.as_str(),
            "range": {
                "from": "start",
                "to": {"sequence": 2}
            }
        }]
    });
    let compile = EventRecord::from_new(
        stream_id.clone(),
        EventSequence::new(3),
        NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::ContextCompileCompleted,
            serde_json::json!({
                "schema": EventKind::ContextCompileCompleted.payload_schema_id(),
                "strategy": "naive_assembly",
                "output_hash": "sha256:compiled-context",
                "read_plan": read_plan.clone(),
            }),
            EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_range: Some(source_range.clone()),
                source_ranges: vec![source_range.clone()],
                discharged_by: Some("projection:context-compiler".to_string()),
                function: Some("naive_assembly/v1".to_string()),
                ..EventProvenance::default()
            },
        ),
    );
    let summary = EventRecord::from_new(
        stream_id.clone(),
        EventSequence::new(4),
        NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::ContextSummaryCompleted,
            serde_json::json!({
                "schema": EventKind::ContextSummaryCompleted.payload_schema_id(),
                "role": "summary_checkpoint",
                "text": "The compacted discharge text stays inside the stream payload.",
                "covered_ranges": [{
                    "stream_id": stream_id.as_str(),
                    "from_sequence": 1,
                    "to_sequence": 2
                }],
                "content": {
                    "sha256": "sha256:summary-content"
                }
            }),
            EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_range: Some(source_range.clone()),
                source_ranges: vec![source_range],
                discharged_by: Some("projection:context-summarizer".to_string()),
                function: Some("context_summary/v1".to_string()),
                ..EventProvenance::default()
            },
        ),
    );
    let read_plan_set = EventRecord::from_new(
        stream_id.clone(),
        EventSequence::new(5),
        NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::ContextReadPlanSet,
            serde_json::json!({
                "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
                "scope": "thread",
                "name": "history.default",
                "pipeline_id": "context.default",
                "source_id": stream_id.as_str(),
                "summary_event_id": summary.id.to_string(),
                "read_plan": {
                    "schema": CONTEXT_READ_PLAN_SCHEMA_V1,
                    "name": "history.default",
                    "source_stream": stream_id.as_str(),
                    "frontier": "compile_frontier",
                    "entries": [{
                        "kind": "event_ref",
                        "stream_id": stream_id.as_str(),
                        "event_id": summary.id.to_string(),
                        "event_role": "summary_checkpoint",
                        "covers": {
                            "from": "start",
                            "to": {"sequence": 2}
                        }
                    }]
                }
            }),
            EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_event_ids: vec![summary.id],
                discharged_by: Some("controller:context-budget".to_string()),
                function: Some("context_read_plan/v1".to_string()),
                ..EventProvenance::default()
            },
        ),
    );

    let registry = stream_schema_registry_v1().unwrap();
    for record in [&compile, &summary, &read_plan_set] {
        record.validate_stream_record_v1().unwrap();
        validate_context_payload_schema_v1(record.kind, &record.payload).unwrap();
        registry
            .validate(
                STREAM_RECORD_SCHEMA_V1,
                &serde_json::to_value(record.to_stream_record_v1()).unwrap(),
            )
            .unwrap();
    }

    let mut missing_stream_id = serde_json::to_value(compile.to_stream_record_v1()).unwrap();
    missing_stream_id
        .as_object_mut()
        .unwrap()
        .remove("stream_id");
    let err = registry
        .validate(STREAM_RECORD_SCHEMA_V1, &missing_stream_id)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("missing required property \"stream_id\"")
    );

    let mut malformed_plan = read_plan;
    malformed_plan["entries"][0]["kind"] = serde_json::json!("keep_everything");
    let err = registry
        .validate(CONTEXT_READ_PLAN_SCHEMA_V1, &malformed_plan)
        .unwrap_err();
    assert!(err.to_string().contains("allowed enum values"));

    let mut debug_export = serde_json::json!({
        "schema": DEBUG_THREAD_EXPORT_SCHEMA_V1,
        "threadId": coordinates.thread_id.to_string(),
        "generatedAtMs": 1_771_718_499_999i64,
        "backend": {
            "kind": "sqlite",
            "sessionStorePath": "/tmp/cooldis/session_history.sqlite3",
            "ackClasses": ["local_committed", "query_projected"]
        },
        "ackClasses": ["local_committed", "query_projected"],
        "redaction": {
            "enabled": true,
            "mode": "secret-shaped-json-keys",
            "replacement": "[REDACTED]",
            "redactedKeys": []
        },
        "thread": null,
        "streams": [{
            "selector": "thread",
            "streamId": stream_id.as_str(),
            "backend": {
                "kind": "sqlite",
                "sessionStorePath": "/tmp/cooldis/session_history.sqlite3"
            },
            "ackClasses": ["local_committed", "query_projected"],
            "range": {
                "fromSequence": 1,
                "fromCursor": "djE6MQ==",
                "lastExportedSequence": null,
                "lastExportedStreamCursor": null,
                "toCursor": null,
                "tailSequence": null,
                "tailStreamCursor": null,
                "tailCursor": "djE6MQ=="
            },
            "data": [],
            "eventCount": 0,
            "truncated": false,
            "cursor": null,
            "streamCursor": null
        }],
        "receipts": []
    });
    registry
        .validate(DEBUG_THREAD_EXPORT_SCHEMA_V1, &debug_export)
        .unwrap();
    debug_export.as_object_mut().unwrap().remove("streams");
    let err = registry
        .validate(DEBUG_THREAD_EXPORT_SCHEMA_V1, &debug_export)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("missing required property \"streams\"")
    );

    let mut debug_export_with_extra = serde_json::json!({
        "schema": DEBUG_THREAD_EXPORT_SCHEMA_V1,
        "threadId": coordinates.thread_id.to_string(),
        "generatedAtMs": 1_771_718_499_999i64,
        "backend": {
            "kind": "sqlite",
            "sessionStorePath": "/tmp/cooldis/session_history.sqlite3",
            "ackClasses": ["local_committed", "query_projected"]
        },
        "ackClasses": ["local_committed", "query_projected"],
        "redaction": {
            "enabled": true,
            "mode": "secret-shaped-json-keys",
            "replacement": "[REDACTED]",
            "redactedKeys": []
        },
        "thread": null,
        "streams": [],
        "receipts": [],
        "surprise": true
    });
    let err = registry
        .validate(DEBUG_THREAD_EXPORT_SCHEMA_V1, &debug_export_with_extra)
        .unwrap_err();
    assert!(err.to_string().contains("unexpected property \"surprise\""));
    debug_export_with_extra
        .as_object_mut()
        .unwrap()
        .remove("surprise");
    registry
        .validate(DEBUG_THREAD_EXPORT_SCHEMA_V1, &debug_export_with_extra)
        .unwrap();
}

#[tokio::test]
async fn discharged_control_event_kinds_require_provenance() {
    let discharged_kinds = [
        EventKind::ToolCallRequested,
        EventKind::ToolCallSuspended,
        EventKind::ToolCallDecision,
        EventKind::TurnWaiting,
        EventKind::TurnResumed,
        EventKind::TurnCompleted,
        EventKind::ApprovalRequested,
        EventKind::TurnContinueRequested,
        EventKind::TurnContinuationAccepted,
        EventKind::TurnContinuationRejected,
        EventKind::LoopCompleted,
        EventKind::LoopBlocked,
        EventKind::LoopBudgetExhausted,
        EventKind::LoopDenied,
        EventKind::CouplingRunCompleted,
        EventKind::CouplingRunFailed,
        EventKind::PlacementDecision,
    ];
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);

    for kind in discharged_kinds {
        let store = InMemorySessionStore::new();
        let record = NewEventRecord::discharged(
            coordinates.clone(),
            kind,
            serde_json::json!({"kind": kind.as_str()}),
            EventProvenance::default(),
        );
        let record_id = record.id;
        let err = store
            .append_events(&stream_id, vec![record])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            HistoryError::DischargedWithoutProvenance(id) if id == record_id
        ));
    }
}

#[tokio::test]
async fn discharged_events_without_provenance_are_rejected() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let record = NewEventRecord::discharged(
        coordinates.clone(),
        EventKind::ContextCompileCompleted,
        serde_json::json!({"output_hash": "sha256:test"}),
        EventProvenance::default(),
    );
    let record_id = record.id;

    let memory_store = InMemorySessionStore::new();
    let err = memory_store
        .append_events(&stream_id, vec![record.clone()])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        HistoryError::DischargedWithoutProvenance(id) if id == record_id
    ));
    assert!(
        memory_store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn loop_discharged_session_entry_records_triggering_event_id() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let store = InMemorySessionStore::new();
    let submitted = store
        .append_events(
            &stream_id,
            vec![NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::TurnSubmitted,
                serde_json::json!({
                    "schema": EventKind::TurnSubmitted.payload_schema_id(),
                    "turn_id": "turn-1",
                }),
            )],
        )
        .await
        .unwrap()
        .remove(0);

    let assistant_entry = store
        .append_with_provenance(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::assistant(
                    "test-provider",
                    ProviderApi::OpenAIResponses,
                    "model-1",
                    vec![CanonicalContent::text("hello back")],
                    CanonicalStopReason::EndTurn,
                ),
            },
            EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_event_ids: vec![submitted.id],
                discharged_by: Some("propagator:agent-loop".to_string()),
                function: Some("session_entry_append/v1".to_string()),
                ..EventProvenance::default()
            },
        )
        .await
        .unwrap();

    let events = store.read_events(&stream_id, None).await.unwrap();
    let assistant_event = events
        .iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload["entry_id"] == assistant_entry.entry_id.to_string()
        })
        .unwrap();
    assert_eq!(assistant_event.origin, EventOrigin::Discharged);
    assert_eq!(
        assistant_event.provenance.source_event_ids,
        vec![submitted.id]
    );
    assert_eq!(
        assistant_event.provenance.discharged_by.as_deref(),
        Some("propagator:agent-loop")
    );
}

#[tokio::test]
async fn in_memory_append_events_rejects_partial_batch_without_mutation() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let valid = NewEventRecord::witnessed(
        coordinates.clone(),
        EventKind::SessionEntryAppended,
        serde_json::json!({"entry_id": "entry-1"}),
    );
    let invalid = NewEventRecord::discharged(
        coordinates.clone(),
        EventKind::ContextCompileCompleted,
        serde_json::json!({"output_hash": "sha256:test"}),
        EventProvenance::default(),
    );
    let invalid_id = invalid.id;
    let store = InMemorySessionStore::new();

    let err = store
        .append_events(&stream_id, vec![valid.clone(), invalid])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        HistoryError::DischargedWithoutProvenance(id) if id == invalid_id
    ));
    assert!(
        store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );

    let appended = store.append_events(&stream_id, vec![valid]).await.unwrap();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].sequence.get(), 1);
}

#[tokio::test]
async fn in_memory_append_events_validate_stream_schema_before_mutation() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let valid = NewEventRecord::witnessed(
        coordinates.clone(),
        EventKind::SessionEntryAppended,
        serde_json::json!({"entry_id": "entry-1"}),
    );
    let invalid = NewEventRecord::witnessed(
        coordinates,
        EventKind::TurnSubmitted,
        serde_json::json!("not-an-object-payload"),
    );
    let store = InMemorySessionStore::new();

    let err = store
        .append_events(&stream_id, vec![valid.clone(), invalid])
        .await
        .unwrap_err();
    assert!(matches!(err, HistoryError::Codec(message) if message.contains("expected object")));
    assert!(
        store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );

    let appended = store.append_events(&stream_id, vec![valid]).await.unwrap();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].sequence.get(), 1);
}

#[tokio::test]
async fn in_memory_append_events_validates_io_egress_requested_payload_schema() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let invalid = NewEventRecord::discharged(
        coordinates.clone(),
        EventKind::IoEgressRequested,
        serde_json::json!({
            "schema": EventKind::IoEgressRequested.payload_schema_id(),
            "requested_by_tool_call_id": "call_1"
        }),
        EventProvenance {
            source_streams: vec![stream_id.clone()],
            discharged_by: Some("rpc:append_events".to_string()),
            function: Some("io_egress_requested/v1".to_string()),
            ..EventProvenance::default()
        },
    );
    let store = InMemorySessionStore::new();

    let err = store
        .append_events(&stream_id, vec![invalid])
        .await
        .unwrap_err();
    assert!(matches!(err, HistoryError::Codec(message) if message.contains("egress_kind")));
    assert!(
        store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn in_memory_stream_cursor_reads_strictly_after_verified_event() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let store = InMemorySessionStore::new();

    let appended = store
        .append_events(
            &stream_id,
            vec![
                NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnSubmitted,
                    serde_json::json!({"schema": "cooldis.event.turn.submitted/1", "turn_id": "turn-1"}),
                ),
                NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::ToolCallCompleted,
                    serde_json::json!({"schema": "cooldis.event.tool.call.completed/1", "call_id": "call-1"}),
                ),
                NewEventRecord::witnessed(
                    coordinates,
                    EventKind::TurnCompleted,
                    serde_json::json!({"schema": "cooldis.event.turn.completed/1", "turn_id": "turn-1"}),
                ),
            ],
        )
        .await
        .unwrap();
    let cursor = appended[0].cursor_v1();

    let replay = store
        .read_events_after_cursor(&stream_id, &cursor)
        .await
        .unwrap();
    assert_eq!(
        replay
            .iter()
            .map(|event| event.sequence.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(replay[0].id, appended[1].id);
    assert_eq!(replay[1].id, appended[2].id);

    let wrong_stream = EventStreamId::new("control:wrong-thread");
    let stream_err = store
        .read_events_after_cursor(&wrong_stream, &cursor)
        .await
        .unwrap_err();
    assert!(matches!(
        stream_err,
        HistoryError::StreamCursorStreamMismatch { .. }
    ));

    let tampered = StreamCursorV1 {
        event_id: appended[2].id,
        ..cursor
    };
    let cursor_err = store
        .read_events_after_cursor(&stream_id, &tampered)
        .await
        .unwrap_err();
    assert!(matches!(
        cursor_err,
        HistoryError::StreamCursorMismatch { .. }
    ));
}
