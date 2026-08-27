use crate::EventStore as _;
use crate::SessionStore as _;
fn coords(tenant: &str, user: &str, session: &str) -> verlet_runtime_contracts::ThreadCoordinates {
    verlet_runtime_contracts::ThreadCoordinates::new(tenant, user, session)
}

fn message_texts(messages: &[crate::CanonicalMessage]) -> Vec<&str> {
    messages
        .iter()
        .map(|message| match message {
            crate::CanonicalMessage::User { content, .. }
            | crate::CanonicalMessage::Assistant { content, .. }
            | crate::CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    crate::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or(""),
        })
        .collect()
}

fn message_timestamps(messages: &[crate::CanonicalMessage]) -> Vec<i64> {
    messages
        .iter()
        .map(|message| match message {
            crate::CanonicalMessage::User { timestamp_ms, .. }
            | crate::CanonicalMessage::Assistant { timestamp_ms, .. }
            | crate::CanonicalMessage::ToolResult { timestamp_ms, .. } => *timestamp_ms,
        })
        .collect()
}

#[tokio::test]
async fn append_only_context_follows_active_branch() {
    let store = crate::InMemorySessionStore::new();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let root = store
        .append(
            &coordinates,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let left = store
        .append(
            &coordinates,
            Some(root.entry_id),
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("left"),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &coordinates,
            Some(root.entry_id),
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("right"),
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
    let store = crate::InMemorySessionStore::new();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    store
        .append(
            &coordinates,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let checkpoint_leaf = store
        .append(
            &coordinates,
            None,
            crate::SessionEntryKind::Runtime {
                kind: "thread_checkpoint".to_string(),
                payload: serde_json::json!({"checkpoint_id":"checkpoint"}),
            },
        )
        .await
        .unwrap();
    let after = store
        .append(
            &coordinates,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("after"),
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

    store.select_branch(&coordinates, None).await.unwrap();

    let events = store
        .read_events(&crate::EventStreamId::for_thread(&coordinates), None)
        .await
        .unwrap();
    let selections = events
        .iter()
        .filter(|event| event.kind == crate::EventKind::ThreadBranchSelected)
        .collect::<Vec<_>>();
    assert_eq!(selections.len(), 2);
    assert_eq!(selections[0].origin, crate::EventOrigin::Witnessed);
    assert!(selections[0].provenance.is_empty());
    assert_eq!(
        selections[0].to_stream_record_v1().payload_schema,
        crate::EventKind::ThreadBranchSelected.payload_schema_id()
    );
    assert_eq!(
        serde_json::from_value::<crate::ThreadBranchSelectedPayload>(selections[0].payload.clone())
            .unwrap(),
        crate::ThreadBranchSelectedPayload {
            thread_id: coordinates.thread_id,
            selected_entry_id: Some(checkpoint_leaf.entry_id),
            prior_entry_id: Some(after.entry_id),
        }
    );
    assert_eq!(
        serde_json::from_value::<crate::ThreadBranchSelectedPayload>(selections[1].payload.clone())
            .unwrap(),
        crate::ThreadBranchSelectedPayload {
            thread_id: coordinates.thread_id,
            selected_entry_id: None,
            prior_entry_id: Some(checkpoint_leaf.entry_id),
        }
    );
}

#[tokio::test]
async fn clone_branch_copies_source_lineage_into_new_thread() {
    let store = crate::InMemorySessionStore::new();
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    store
        .append(
            &source,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let source_leaf = store
        .append(
            &source,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("source"),
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
    let store = crate::InMemorySessionStore::new();
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    let root = store
        .append(
            &source,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let source_leaf = store
        .append(
            &source,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("source"),
            },
        )
        .await
        .unwrap();
    let source_stream = crate::EventStreamId::for_thread(&source);
    let target_stream = crate::EventStreamId::for_thread(&target);

    store
        .fork_by_reference(
            &source,
            &target,
            crate::ThreadBaseRef {
                child_thread_id: target.thread_id,
                parent_thread_id: source.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(source_leaf.entry_id),
                parent_stream_id: source_stream.clone(),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: Some("sha256:parent".to_string()),
                reason: crate::ThreadForkReason::ManifestUpdate,
                created_at_ms: crate::now_ms(),
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
        vec![crate::SessionContextSourceCut {
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
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("child"),
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
        Some(&crate::SessionContextSourceCut {
            coordinates: target.clone(),
            stream_id: target_stream,
            inherited: false,
            entry_ids: vec![child.entry_id],
        })
    );
}

#[tokio::test]
async fn fork_by_reference_rejects_cycles_and_cross_scope_edges() {
    let store = crate::InMemorySessionStore::new();
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    let source_leaf = store
        .append(
            &source,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("source"),
            },
        )
        .await
        .unwrap();

    store
        .fork_by_reference(
            &source,
            &target,
            crate::ThreadBaseRef {
                child_thread_id: target.thread_id,
                parent_thread_id: source.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(source_leaf.entry_id),
                parent_stream_id: crate::EventStreamId::for_thread(&source),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: crate::ThreadForkReason::Manual,
                created_at_ms: crate::now_ms(),
            },
        )
        .await
        .unwrap();

    let cycle_err = store
        .fork_by_reference(
            &target,
            &source,
            crate::ThreadBaseRef {
                child_thread_id: source.thread_id,
                parent_thread_id: target.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: None,
                parent_stream_id: crate::EventStreamId::for_thread(&target),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: crate::ThreadForkReason::Manual,
                created_at_ms: crate::now_ms(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        cycle_err,
        crate::HistoryError::ThreadBaseCycle { .. }
    ));

    let wrong_scope_target = coords("tenant_b", "user_1", "session_1");
    let scope_err = store
        .fork_by_reference(
            &source,
            &wrong_scope_target,
            crate::ThreadBaseRef {
                child_thread_id: wrong_scope_target.thread_id,
                parent_thread_id: source.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(source_leaf.entry_id),
                parent_stream_id: crate::EventStreamId::for_thread(&source),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: crate::ThreadForkReason::Manual,
                created_at_ms: crate::now_ms(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        scope_err,
        crate::HistoryError::ThreadScopeMismatch { .. }
    ));
}

#[tokio::test]
async fn compaction_clears_prior_model_visible_messages() {
    let store = crate::InMemorySessionStore::new();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    store
        .append(
            &coordinates,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("old"),
            },
        )
        .await
        .unwrap();
    let compacted = store
        .append(
            &coordinates,
            None,
            crate::SessionEntryKind::Compaction {
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
    let mut old = crate::SessionEntry::new(
        coordinates.clone(),
        None,
        crate::SessionEntryKind::Message {
            message: crate::CanonicalMessage::User {
                content: vec![crate::CanonicalContent::text("old")],
                timestamp_ms: 10,
            },
        },
    );
    old.created_at_ms = 10;
    let mut compacted = crate::SessionEntry::new(
        coordinates.clone(),
        Some(old.entry_id),
        crate::SessionEntryKind::Compaction {
            summary: "old summary".to_string(),
        },
    );
    compacted.created_at_ms = 20;
    let mut hook = crate::SessionEntry::new(
        coordinates,
        Some(compacted.entry_id),
        crate::SessionEntryKind::CustomContextMessage {
            message: crate::CanonicalMessage::User {
                content: vec![crate::CanonicalContent::text("persisted hook context")],
                timestamp_ms: 30,
            },
        },
    );
    hook.created_at_ms = 30;
    let entries = vec![old, compacted, hook];
    let reopened_entries = entries
        .iter()
        .map(|entry| crate::decode_entry(&serde_json::to_string(entry).unwrap()).unwrap())
        .collect::<Vec<_>>();

    let mut first = Vec::new();
    crate::append_model_visible_messages(&entries, &mut first);
    let mut reopened = Vec::new();
    crate::append_model_visible_messages(&reopened_entries, &mut reopened);

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
    let store = crate::InMemorySessionStore::new();
    let first = coords("tenant_a", "user_1", "session_1");
    let second = coords("tenant_b", "user_1", "session_1");
    store
        .append(
            &first,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("first"),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &second,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("second"),
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
        "binding.attached",
        "binding.detached",
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
        "turn.failed",
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
        "thread.branch.selected",
        "thread.reload.degraded",
        "policy.bound",
        "grant.petitioned",
        "timer.fired",
        "client.record.appended",
        "io.ingress.received",
        "io.ingress.claimed",
        "io.ingress.settled",
        "io.egress.requested",
        "io.egress.delivered",
        "io.egress.failed",
        "admission.decided",
    ];
    assert_eq!(crate::EVENT_KIND_SCHEMA_VERSION, "cooldis.events/0.5");
    let kinds = <crate::EventKind as strum::VariantArray>::VARIANTS;
    let actual: Vec<&str> = kinds.iter().map(|kind| kind.as_ref()).collect();
    assert_eq!(actual, expected);
    for kind in kinds {
        let parsed: crate::EventKind = kind.as_ref().parse().unwrap();
        assert_eq!(parsed, *kind);
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(
            serde_json::from_str::<crate::EventKind>(&json).unwrap(),
            *kind
        );
    }

    let err = crate::EventKind::try_from("unknown.event.kind".to_string()).unwrap_err();
    assert!(
        matches!(err, crate::HistoryError::Codec(message) if message.contains("unknown event kind"))
    );
    let err = serde_json::from_str::<crate::EventKind>("\"unknown.event.kind\"").unwrap_err();
    assert!(err.to_string().contains("unknown event kind"));
}

#[test]
fn v04_journal_event_kinds_remain_decodable_after_v05_bump() {
    let v04_kinds = [
        "session.entry.appended",
        "context.compile.completed",
        "context.summary.completed",
        "context.read_plan.set",
        "manifest.compile.completed",
        "manifest.bind.completed",
        "binding.attached",
        "binding.detached",
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
        "thread.branch.selected",
        "thread.reload.degraded",
        "policy.bound",
        "grant.petitioned",
        "timer.fired",
        "client.record.appended",
        "io.ingress.received",
        "io.ingress.claimed",
        "io.ingress.settled",
        "io.egress.requested",
        "io.egress.delivered",
        "io.egress.failed",
        "admission.decided",
    ];
    let coordinates = coords("tenant_a", "user_1", "v04-journal");
    let stream_id = crate::EventStreamId::for_thread(&coordinates);

    for (index, encoded_kind) in v04_kinds.into_iter().enumerate() {
        let kind = encoded_kind.parse::<crate::EventKind>().unwrap();
        let record = crate::EventRecord::from_new(
            stream_id.clone(),
            crate::EventSequence::new(index as i64 + 1),
            crate::NewEventRecord::witnessed(
                coordinates.clone(),
                kind,
                serde_json::json!({"legacy": true}),
            ),
        );
        let decoded =
            serde_json::from_value::<crate::EventRecord>(serde_json::to_value(&record).unwrap())
                .unwrap();

        assert_eq!(
            decoded.kind, kind,
            "failed to decode v0.4 kind {encoded_kind}"
        );
    }
}

#[test]
fn turn_failed_payload_serializes_closed_class_and_truncates_utf8_message() {
    let payload = crate::TurnFailedPayload::new(
        "turn-1",
        crate::TurnFailureErrorClass::ProviderHttp,
        Some("openai".to_string()),
        Some(400),
        "é".repeat(600),
        2,
    );

    assert_eq!(payload.message.as_bytes().len(), 1024);
    assert!(payload.message.is_char_boundary(payload.message.len()));
    assert_eq!(
        serde_json::to_value(&payload).unwrap(),
        serde_json::json!({
            "turn_id": "turn-1",
            "error_class": "provider_http",
            "provider_id": "openai",
            "http_status": 400,
            "message": "é".repeat(512),
            "retries_attempted": 2,
        })
    );

    let decoded: crate::TurnFailedPayload =
        serde_json::from_value(serde_json::to_value(payload).unwrap()).unwrap();
    assert_eq!(
        decoded.error_class,
        crate::TurnFailureErrorClass::ProviderHttp
    );
    crate::stream_schema_registry_v1()
        .validate(
            &crate::EventKind::TurnFailed.payload_schema_id(),
            &serde_json::to_value(decoded).unwrap(),
        )
        .unwrap();
}

#[test]
fn event_kind_payload_schema_ids_are_frozen_for_stream_schema_v1() {
    assert_eq!(
        crate::EventKind::BindingAttached.payload_schema_id(),
        "cooldis.event.binding.attached/1"
    );
    assert_eq!(
        crate::EventKind::BindingDetached.payload_schema_id(),
        "cooldis.event.binding.detached/1"
    );
    assert_eq!(
        crate::EventKind::TurnFailed.payload_schema_id(),
        "cooldis.event.turn.failed/1"
    );
    assert_eq!(
        crate::EventKind::ContextCompileCompleted.payload_schema_id(),
        "cooldis.event.context.compile.completed/1"
    );
    assert_eq!(
        crate::EventKind::ContextSummaryCompleted.payload_schema_id(),
        "cooldis.event.context.summary.completed/1"
    );
    assert_eq!(
        crate::EventKind::ContextReadPlanSet.payload_schema_id(),
        "cooldis.event.context.read_plan.set/1"
    );
    assert_eq!(
        crate::EventKind::ThreadSpawnRequested.payload_schema_id(),
        "cooldis.event.thread.spawn.requested/1"
    );
    assert_eq!(
        crate::EventKind::ThreadSpawned.payload_schema_id(),
        "cooldis.event.thread.spawned/1"
    );
    assert_eq!(
        crate::EventKind::ThreadJoined.payload_schema_id(),
        "cooldis.event.thread.joined/1"
    );
    assert_eq!(
        crate::EventKind::ThreadBranchSelected.payload_schema_id(),
        "cooldis.event.thread.branch.selected/1"
    );
    assert_eq!(
        crate::EventKind::ThreadReloadDegraded.payload_schema_id(),
        "cooldis.event.thread.reload.degraded/1"
    );
    assert_eq!(
        crate::EventKind::PolicyBound.payload_schema_id(),
        "cooldis.event.policy.bound/1"
    );
    assert_eq!(
        crate::EventKind::GrantPetitioned.payload_schema_id(),
        "cooldis.event.grant.petitioned/1"
    );
    assert_eq!(
        crate::EventKind::TimerFired.payload_schema_id(),
        "cooldis.event.timer.fired/1"
    );
    assert_eq!(
        crate::EventKind::ClientRecordAppended.payload_schema_id(),
        "cooldis.event.client.record.appended/1"
    );
    assert_eq!(
        crate::EventKind::IoIngressReceived.payload_schema_id(),
        "cooldis.event.io.ingress.received/1"
    );
    assert_eq!(
        crate::EventKind::IoIngressClaimed.payload_schema_id(),
        "cooldis.event.io.ingress.claimed/1"
    );
    assert_eq!(
        crate::EventKind::IoIngressSettled.payload_schema_id(),
        "cooldis.event.io.ingress.settled/1"
    );
    assert_eq!(
        crate::EventKind::IoEgressRequested.payload_schema_id(),
        "cooldis.event.io.egress.requested/1"
    );
    assert_eq!(
        crate::EventKind::IoEgressDelivered.payload_schema_id(),
        "cooldis.event.io.egress.delivered/1"
    );
    assert_eq!(
        crate::EventKind::IoEgressFailed.payload_schema_id(),
        "cooldis.event.io.egress.failed/1"
    );
    assert_eq!(
        crate::EventKind::AdmissionDecided.payload_schema_id(),
        "cooldis.event.admission.decided/1"
    );
}

#[test]
fn binding_attached_payload_wire_shape_is_frozen() {
    let payload = crate::BindingAttachedPayload {
        name: "search-tools".to_string(),
        artifact_hash: "sha256:search-tools".to_string(),
        operations: vec!["search".to_string(), "answer".to_string()],
        direct_tools: vec![crate::BindingAttachedDirectToolBinding {
            tool_name: "search_web".to_string(),
            operation: "search".to_string(),
            effect_class: crate::BindingEffectClass::Pure,
        }],
        attachment_config: crate::BindingAttachmentConfig {
            allowed_secrets: std::collections::BTreeSet::from(["SEARCH_TOKEN".to_string()]),
            allowed_private_network: std::collections::BTreeMap::from([(
                "http://127.0.0.1:*".to_string(),
                std::collections::BTreeSet::from(["GET".to_string(), "POST".to_string()]),
            )]),
            bound_parameters: std::collections::BTreeMap::from([(
                "root".to_string(),
                serde_json::json!("/workspace"),
            )]),
        },
        effect_class: crate::BindingEffectClass::Idempotent,
        requested_by: "principal:operator".to_string(),
        decided_by: "principal:operator".to_string(),
        decision_event_id: None,
    };

    let expected = serde_json::json!({
        "name": "search-tools",
        "artifact_hash": "sha256:search-tools",
        "operations": ["search", "answer"],
        "direct_tools": [{
            "tool_name": "search_web",
            "operation": "search",
            "effect_class": "pure"
        }],
        "attachment_config": {
            "allowed_secrets": ["SEARCH_TOKEN"],
            "allowed_private_network": {
                "http://127.0.0.1:*": ["GET", "POST"]
            },
            "bound_parameters": {"root": "/workspace"}
        },
        "effect_class": "idempotent",
        "requested_by": "principal:operator",
        "decided_by": "principal:operator"
    });
    let actual = serde_json::to_value(&payload).unwrap();

    assert_eq!(actual, expected);
    assert_eq!(
        serde_json::from_value::<crate::BindingAttachedPayload>(actual).unwrap(),
        payload
    );
    crate::stream_schema_registry_v1()
        .validate(
            &crate::EventKind::BindingAttached.payload_schema_id(),
            &expected,
        )
        .unwrap();
}

#[test]
fn pre_bound_parameter_attachment_payload_remains_byte_stable() {
    let legacy = serde_json::json!({
        "name": "search-tools",
        "artifact_hash": "sha256:search-tools",
        "attachment_config": {
            "allowed_secrets": ["SEARCH_TOKEN"]
        },
        "requested_by": "principal:operator",
        "decided_by": "principal:operator"
    });
    let decoded: crate::BindingAttachedPayload = serde_json::from_value(legacy.clone()).unwrap();

    assert!(decoded.attachment_config.bound_parameters.is_empty());
    assert_eq!(serde_json::to_value(decoded).unwrap(), legacy);
    crate::stream_schema_registry_v1()
        .validate(
            &crate::EventKind::BindingAttached.payload_schema_id(),
            &legacy,
        )
        .unwrap();
}

#[test]
fn binding_detached_payload_wire_shape_is_frozen() {
    let attach_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000041").unwrap(),
    );
    let payload = crate::BindingDetachedPayload {
        attach_event_id,
        requested_by: "principal:operator".to_string(),
        decided_by: "principal:operator".to_string(),
        decision_event_id: None,
    };
    let expected = serde_json::json!({
        "attach_event_id": "018f0000-0000-7000-8000-000000000041",
        "requested_by": "principal:operator",
        "decided_by": "principal:operator"
    });
    let actual = serde_json::to_value(&payload).unwrap();

    assert_eq!(actual, expected);
    assert_eq!(
        serde_json::from_value::<crate::BindingDetachedPayload>(actual).unwrap(),
        payload
    );
    crate::stream_schema_registry_v1()
        .validate(
            &crate::EventKind::BindingDetached.payload_schema_id(),
            &expected,
        )
        .unwrap();
}

#[test]
fn binding_payloads_reject_unknown_fields() {
    let attached = serde_json::json!({
        "name": "search-tools",
        "artifact_hash": "sha256:search-tools",
        "requested_by": "principal:operator",
        "decided_by": "principal:operator",
        "scheduled_detach": true
    });
    assert!(serde_json::from_value::<crate::BindingAttachedPayload>(attached).is_err());

    let detached = serde_json::json!({
        "attach_event_id": "018f0000-0000-7000-8000-000000000041",
        "requested_by": "principal:operator",
        "decided_by": "principal:operator",
        "scheduled_detach": true
    });
    assert!(serde_json::from_value::<crate::BindingDetachedPayload>(detached).is_err());
}

#[test]
fn client_record_carrier_payload_schema_is_registered_and_strict() {
    let registry = crate::stream_schema_registry_v1();
    let schema = crate::EventKind::ClientRecordAppended.payload_schema_id();
    registry
        .validate(
            &schema,
            &serde_json::json!({
                "client_kind": "placement.bound",
                "client_schema": "verlet.orch.placement.bound/1",
                "principal_id": "operator:root",
                "body": {"agent": "agent://worker@1.0.0"},
            }),
        )
        .unwrap();
    assert!(
        registry
            .validate(
                &schema,
                &serde_json::json!({
                    "client_kind": "placement.bound",
                    "client_schema": "verlet.orch.placement.bound/1",
                    "principal_id": "operator:root",
                }),
            )
            .is_err()
    );
}

#[test]
fn thread_reload_degraded_payload_schema_is_registered() {
    let thread_id =
        verlet_runtime_contracts::ThreadId::parse_str("018f0000-0000-7000-8000-000000000001")
            .unwrap();
    let payload = serde_json::to_value(crate::ThreadReloadDegradedPayload {
        thread_id,
        missing: vec![
            "topology".to_string(),
            "parent_thread_id".to_string(),
            "metadata".to_string(),
        ],
        fallback: "fabricated_root".to_string(),
    })
    .unwrap();

    crate::stream_schema_registry_v1()
        .validate(
            &crate::EventKind::ThreadReloadDegraded.payload_schema_id(),
            &payload,
        )
        .unwrap();
}

#[test]
fn ingress_outcome_payloads_round_trip_whole_and_validate() {
    let witness_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000011").unwrap(),
    );
    let admission_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000012").unwrap(),
    );
    let claim_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000013").unwrap(),
    );
    let evidence_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000014").unwrap(),
    );
    let claim = crate::IoIngressClaimedPayload {
        ingress_envelope_ids: vec!["ingress-1".to_string()],
        ingress_witness_event_ids: vec![witness_event_id],
        admission_event_id,
        intent: crate::IngressOutcomeIntent::Turn {
            turn_id: "turn-1".to_string(),
            submission_mode: "queue".to_string(),
            input_digest: "sha256:input".to_string(),
        },
    };
    let settle = crate::IoIngressSettledPayload {
        claim_event_id,
        ingress_envelope_ids: vec!["ingress-1".to_string()],
        evidence_event_id: Some(evidence_event_id),
        settled_by: crate::IngressSettledBy::Recovery,
    };
    let claim_value = serde_json::to_value(&claim).unwrap();
    let settle_value = serde_json::to_value(&settle).unwrap();
    let registry = crate::stream_schema_registry_v1();

    registry
        .validate(
            &crate::EventKind::IoIngressClaimed.payload_schema_id(),
            &claim_value,
        )
        .unwrap();
    registry
        .validate(
            &crate::EventKind::IoIngressSettled.payload_schema_id(),
            &settle_value,
        )
        .unwrap();
    assert_eq!(
        serde_json::from_value::<crate::IoIngressClaimedPayload>(claim_value).unwrap(),
        claim
    );
    assert_eq!(
        serde_json::from_value::<crate::IoIngressSettledPayload>(settle_value).unwrap(),
        settle
    );
}

#[test]
fn events_0_3_payload_fixtures_round_trip_and_validate() {
    let parent_thread_id =
        verlet_runtime_contracts::ThreadId::parse_str("018f0000-0000-7000-8000-000000000001")
            .unwrap();
    let child_thread_id =
        verlet_runtime_contracts::ThreadId::parse_str("018f0000-0000-7000-8000-000000000002")
            .unwrap();
    let spawned_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000003").unwrap(),
    );
    let checkpoint_id = verlet_runtime_contracts::ThreadCheckpointId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000006").unwrap(),
    );
    let leaf_entry_id = crate::SessionEntryId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000007").unwrap(),
    );
    let mandate_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000004").unwrap(),
    );
    let ingress_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000005").unwrap(),
    );
    let registry = crate::stream_schema_registry_v1();

    let cases = [
        (
            crate::EventKind::ThreadSpawnRequested,
            serde_json::to_value(crate::ThreadSpawnRequestedPayload {
                parent_thread_id,
                parent_turn_id: Some("turn-parent".to_string()),
                task_name: None,
                submitted_turn_id: None,
                child_agent_ref: "agent://release-worker".to_string(),
                initial_submission: "collect release evidence".to_string(),
                correlation_id: "spawn-release-worker-1".to_string(),
                block_parent: true,
            })
            .unwrap(),
        ),
        (
            crate::EventKind::ThreadSpawned,
            serde_json::to_value(crate::ThreadSpawnedPayload {
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
            crate::EventKind::ThreadSpawned,
            serde_json::to_value(crate::ThreadSpawnedPayload {
                parent_thread_id,
                parent_turn_id: None,
                child_thread_id,
                child_manifest_hash: "sha256:fork-child-manifest".to_string(),
                child_policy_hash: None,
                granted: vec!["threads.spawn".to_string()],
                inputs_hash: "sha256:fork-inputs".to_string(),
                fork: Some(crate::ThreadSpawnedForkPayload {
                    mode: "clone".to_string(),
                    claim_event_id: Some(spawned_event_id),
                    source_cut: crate::ThreadSpawnedForkSourceCutPayload {
                        thread_id: parent_thread_id,
                        checkpoint_id,
                        leaf_entry_id: Some(leaf_entry_id),
                        stream_id: crate::EventStreamId::new(format!("thread:{parent_thread_id}")),
                        stream_to_sequence: Some(crate::EventSequence::new(42)),
                    },
                }),
            })
            .unwrap(),
        ),
        (
            crate::EventKind::ThreadJoined,
            serde_json::to_value(crate::ThreadJoinedPayload {
                child_thread_id,
                spawned_event_id,
                terminal_state: crate::ThreadTerminalState::Completed,
                result_digest: Some("sha256:result".to_string()),
            })
            .unwrap(),
        ),
        (
            crate::EventKind::ThreadBranchSelected,
            serde_json::to_value(crate::ThreadBranchSelectedPayload {
                thread_id: child_thread_id,
                selected_entry_id: Some(leaf_entry_id),
                prior_entry_id: None,
            })
            .unwrap(),
        ),
        (
            crate::EventKind::PolicyBound,
            serde_json::to_value(crate::PolicyBoundPayload {
                policy_kind: crate::PolicyKind::AdmissionRoute,
                policy_id: "route:telegram".to_string(),
                content_hash: "sha256:policy".to_string(),
                valid_from_note: "valid until next policy.bound of same policy_id".to_string(),
            })
            .unwrap(),
        ),
        (
            crate::EventKind::GrantPetitioned,
            serde_json::to_value(crate::GrantPetitionedPayload {
                thread_id: child_thread_id,
                requested: vec!["net:https://api.example.test".to_string()],
                reason: "tool needs outbound API access".to_string(),
                evidence_event_ids: Some(vec![spawned_event_id]),
            })
            .unwrap(),
        ),
        (
            crate::EventKind::TimerFired,
            serde_json::to_value(crate::TimerFiredPayload {
                mandate_event_id,
                scheduled_for: "2026-07-04T12:00:00Z".to_string(),
                occurrence_index: 3,
                catch_up: true,
            })
            .unwrap(),
        ),
        (
            crate::EventKind::IoIngressReceived,
            serde_json::to_value(crate::IoIngressReceivedPayload {
                route_id: Some("route:telegram".to_string()),
                dedupe_key: Some("telegram:42".to_string()),
                external_conversation_id: Some("chat-1".to_string()),
                external_actor_id: Some("actor-1".to_string()),
                external_message_id: Some("message-1".to_string()),
                content: None,
                envelope_digest: "sha256:ingress".to_string(),
            })
            .unwrap(),
        ),
        (
            crate::EventKind::IoEgressDelivered,
            serde_json::to_value(crate::IoEgressDeliveredPayload {
                route_id: "route:telegram".to_string(),
                egress_kind: "telegram.reply".to_string(),
                external_message_id: Some("message-2".to_string()),
                attempts: 2,
            })
            .unwrap(),
        ),
        (
            crate::EventKind::IoEgressRequested,
            serde_json::to_value(crate::IoEgressRequestedPayload {
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
            crate::EventKind::IoEgressFailed,
            serde_json::to_value(crate::IoEgressFailedPayload {
                route_id: "route:telegram".to_string(),
                egress_kind: "telegram.reply".to_string(),
                attempts: 3,
                error_class: "rate_limited".to_string(),
                dead_lettered: true,
            })
            .unwrap(),
        ),
        (
            crate::EventKind::AdmissionDecided,
            serde_json::to_value(crate::AdmissionDecidedPayload {
                route_id: "route:telegram".to_string(),
                policy_hash: "sha256:admission-policy".to_string(),
                decision: crate::AdmissionDecision::Coalesce,
                admissible: Some(vec![
                    crate::AdmissionDecision::Queue,
                    crate::AdmissionDecision::Coalesce,
                    crate::AdmissionDecision::Reject,
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
            .validate(&kind.payload_schema_id(), &decoded)
            .unwrap();
    }
}

#[test]
fn events_0_2_optional_fields_deserialize_when_absent() {
    let parent_thread_id =
        verlet_runtime_contracts::ThreadId::parse_str("018f0000-0000-7000-8000-000000000011")
            .unwrap();
    let child_thread_id =
        verlet_runtime_contracts::ThreadId::parse_str("018f0000-0000-7000-8000-000000000012")
            .unwrap();
    let spawned_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000013").unwrap(),
    );
    let ingress_event_id = crate::EventRecordId::from_uuid(
        uuid::Uuid::parse_str("018f0000-0000-7000-8000-000000000014").unwrap(),
    );

    let spawned: crate::ThreadSpawnedPayload = serde_json::from_value(serde_json::json!({
        "schema": crate::EventKind::ThreadSpawned.payload_schema_id(),
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

    let spawned_fork: crate::ThreadSpawnedPayload = serde_json::from_value(serde_json::json!({
        "schema": crate::EventKind::ThreadSpawned.payload_schema_id(),
        "parent_thread_id": parent_thread_id,
        "child_thread_id": child_thread_id,
        "child_manifest_hash": "sha256:child-manifest",
        "granted": [],
        "inputs_hash": "sha256:inputs",
        "fork": {
            "mode": "clone",
            "sourceCut": {
                "threadId": parent_thread_id,
                "checkpointId": "018f0000-0000-7000-8000-000000000015",
                "leafEntryId": null,
                "streamId": format!("thread:{parent_thread_id}"),
                "streamToSequence": null
            }
        }
    }))
    .unwrap();
    assert_eq!(spawned_fork.fork.unwrap().claim_event_id, None);

    let joined: crate::ThreadJoinedPayload = serde_json::from_value(serde_json::json!({
        "schema": crate::EventKind::ThreadJoined.payload_schema_id(),
        "child_thread_id": child_thread_id,
        "spawned_event_id": spawned_event_id,
        "terminal_state": "budget_exhausted"
    }))
    .unwrap();
    assert_eq!(joined.result_digest, None);

    let ingress: crate::IoIngressReceivedPayload = serde_json::from_value(serde_json::json!({
        "schema": crate::EventKind::IoIngressReceived.payload_schema_id(),
        "envelope_digest": "sha256:ingress"
    }))
    .unwrap();
    assert_eq!(ingress.route_id, None);
    assert_eq!(ingress.external_message_id, None);

    let admission: crate::AdmissionDecidedPayload = serde_json::from_value(serde_json::json!({
        "schema": crate::EventKind::AdmissionDecided.payload_schema_id(),
        "route_id": "route:telegram",
        "policy_hash": "sha256:admission-policy",
        "decision": "queue",
        "source_ingress_event_ids": [ingress_event_id]
    }))
    .unwrap();
    assert_eq!(admission.admissible, None);
}

#[test]
fn thread_spawn_payload_schemas_type_only_fields_owned_by_their_wire_structs() {
    let spawned = crate::thread_spawned_payload_schema_v1();
    assert_eq!(
        spawned["properties"]["inputs_hash"],
        serde_json::json!({"type": "string"})
    );

    let requested = crate::thread_spawn_requested_payload_schema_v1();
    assert!(requested["properties"].get("inputs_hash").is_none());
    assert_eq!(
        requested["properties"]["task_name"],
        serde_json::json!({"type": "string"})
    );
    assert_eq!(
        requested["properties"]["submitted_turn_id"],
        serde_json::json!({"type": "string"})
    );
}

#[test]
fn events_0_1_style_stream_record_still_parses_and_unknown_kind_fails_closed() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let record = serde_json::json!({
        "schema": crate::STREAM_RECORD_SCHEMA_V1,
        "event_id": "018f0000-0000-7000-8000-000000000101",
        "stream_id": crate::EventStreamId::for_thread(&coordinates),
        "sequence": 1,
        "coordinates": coordinates,
        "created_at_ms": 1_772_640_000_000i64,
        "kind": "turn.completed",
        "origin": "discharged",
        "payload_schema": crate::EventKind::TurnCompleted.payload_schema_id(),
        "provenance": {"source_event_ids": ["018f0000-0000-7000-8000-000000000100"]},
        "payload": {"turn_id": "turn-1"}
    });
    let parsed: crate::StreamRecordEnvelopeV1 = serde_json::from_value(record).unwrap();
    assert_eq!(
        parsed.kind.parse::<crate::EventKind>().unwrap(),
        crate::EventKind::TurnCompleted
    );
    crate::stream_schema_registry_v1()
        .validate(
            crate::STREAM_RECORD_SCHEMA_V1,
            &serde_json::to_value(parsed).unwrap(),
        )
        .unwrap();

    let unknown = serde_json::json!({
        "kind": "unknown.event.kind"
    });
    let err = serde_json::from_value::<crate::EventKind>(unknown["kind"].clone()).unwrap_err();
    assert!(err.to_string().contains("unknown event kind"));
}

#[test]
fn canonical_usage_survives_assistant_session_entry_stream_record() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let usage = crate::CanonicalUsage {
        input_tokens: 11,
        output_tokens: 7,
        cache_creation_input_tokens: 3,
        cache_read_input_tokens: 5,
    };
    let entry = crate::SessionEntry::new(
        coordinates.clone(),
        None,
        crate::SessionEntryKind::Message {
            message: crate::CanonicalMessage::assistant_with_usage(
                "test-provider",
                crate::ProviderApi::OpenAIResponses,
                "model-1",
                vec![crate::CanonicalContent::text("hello")],
                usage.clone(),
                crate::CanonicalStopReason::EndTurn,
            ),
        },
    );
    let event = crate::EventRecord::from_new(
        crate::EventStreamId::for_thread(&coordinates),
        crate::EventSequence::new(1),
        crate::session_entry_event(&entry),
    );
    let envelope = event.to_stream_record_v1();
    assert_eq!(
        envelope.kind,
        crate::EventKind::SessionEntryAppended.as_ref()
    );
    assert_eq!(
        envelope.payload["usage"],
        serde_json::to_value(usage).unwrap()
    );
    event.validate_stream_record_v1().unwrap();
}

#[test]
fn event_record_renders_stream_schema_v1_envelope() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let record = crate::EventRecord::from_new(
        stream_id.clone(),
        crate::EventSequence::new(7),
        crate::NewEventRecord::discharged(
            coordinates.clone(),
            crate::EventKind::ContextReadPlanSet,
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
            crate::EventProvenance {
                source_streams: vec![stream_id.clone()],
                discharged_by: Some("controller:context-budget".to_string()),
                function: Some("context_read_plan/v1".to_string()),
                config_hash: Some("sha256:context-budget-config".to_string()),
                ..crate::EventProvenance::default()
            },
        ),
    );

    let envelope = record.to_stream_record_v1();
    assert_eq!(envelope.schema, crate::STREAM_RECORD_SCHEMA_V1);
    assert_eq!(envelope.event_id, record.id);
    assert_eq!(envelope.stream_id, stream_id);
    assert_eq!(envelope.sequence, crate::EventSequence::new(7));
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
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let record = crate::EventRecord::from_new(
        stream_id.clone(),
        crate::EventSequence::new(8),
        crate::NewEventRecord::witnessed(
            coordinates,
            crate::EventKind::ToolCallCompleted,
            serde_json::json!({
                "schema": crate::EventKind::ToolCallCompleted.payload_schema_id(),
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
    assert_eq!(decision.schema, crate::STREAM_ROUTING_DECISION_SCHEMA_V1);
    assert_eq!(decision.event_id, record.id);
    assert_eq!(decision.stream_id, stream_id);
    assert_eq!(
        decision.routes,
        vec![
            crate::StreamRouteProfile::AuthorityStore,
            crate::StreamRouteProfile::ExportBundle,
            crate::StreamRouteProfile::ModelTrace,
            crate::StreamRouteProfile::BrowserSafeProjection,
            crate::StreamRouteProfile::AnalyticsAggregate,
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

    crate::stream_schema_registry_v1()
        .validate(
            crate::STREAM_ROUTING_DECISION_SCHEMA_V1,
            &serde_json::to_value(decision).unwrap(),
        )
        .unwrap();
}

#[test]
fn stream_routing_decision_v1_separates_runtime_and_model_trace_profiles() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = crate::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let record = crate::EventRecord::from_new(
        stream_id,
        crate::EventSequence::new(9),
        crate::NewEventRecord::discharged(
            coordinates,
            crate::EventKind::PlacementDecision,
            serde_json::json!({
                "schema": crate::EventKind::PlacementDecision.payload_schema_id(),
                "placement": "local"
            }),
            crate::EventProvenance {
                discharged_by: Some("controller:placement".to_string()),
                ..crate::EventProvenance::default()
            },
        ),
    );

    let decision = record.route_decision_v1();
    assert!(
        decision
            .routes
            .contains(&crate::StreamRouteProfile::RuntimeTrace)
    );
    assert!(
        !decision
            .routes
            .contains(&crate::StreamRouteProfile::ModelTrace)
    );
    assert_eq!(
        decision.keys.discharged_by.as_deref(),
        Some("controller:placement")
    );
    assert_eq!(decision.keys.stream_id, record.stream_id);
}

#[test]
fn turn_failed_routes_as_model_trace_without_analytics_aggregation() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let record = crate::EventRecord::from_new(
        crate::EventStreamId::for_thread(&coordinates),
        crate::EventSequence::new(10),
        crate::NewEventRecord::discharged(
            coordinates,
            crate::EventKind::TurnFailed,
            serde_json::to_value(crate::TurnFailedPayload::new(
                "turn-1",
                crate::TurnFailureErrorClass::Runner,
                None,
                None,
                "runner failed",
                0,
            ))
            .unwrap(),
            crate::EventProvenance {
                discharged_by: Some("propagator:agent-loop".to_string()),
                ..crate::EventProvenance::default()
            },
        ),
    );

    let decision = record.route_decision_v1();
    assert!(
        decision
            .routes
            .contains(&crate::StreamRouteProfile::ModelTrace)
    );
    assert!(
        !decision
            .routes
            .contains(&crate::StreamRouteProfile::AnalyticsAggregate)
    );
}

#[test]
fn stream_append_ack_v1_freezes_ack_classes_and_tail_identity() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let first = crate::EventRecord::from_new(
        stream_id.clone(),
        crate::EventSequence::new(10),
        crate::NewEventRecord::witnessed(
            coordinates.clone(),
            crate::EventKind::TurnSubmitted,
            serde_json::json!({
                "schema": crate::EventKind::TurnSubmitted.payload_schema_id(),
                "turn_id": "turn-1"
            }),
        ),
    );
    let second = crate::EventRecord::from_new(
        stream_id.clone(),
        crate::EventSequence::new(11),
        crate::NewEventRecord::witnessed(
            coordinates,
            crate::EventKind::TurnCompleted,
            serde_json::json!({
                "schema": crate::EventKind::TurnCompleted.payload_schema_id(),
                "turn_id": "turn-1"
            }),
        ),
    );

    let ack = crate::StreamAppendAckV1::from_appended(
        stream_id.clone(),
        &[first.clone(), second.clone()],
        vec![
            crate::StreamAckClass::LocalCommitted,
            crate::StreamAckClass::QueryProjected,
        ],
    )
    .unwrap();
    assert_eq!(ack.schema, crate::STREAM_APPEND_ACK_SCHEMA_V1);
    assert_eq!(ack.stream_id, stream_id);
    assert_eq!(ack.start_sequence, crate::EventSequence::new(10));
    assert_eq!(ack.end_sequence, crate::EventSequence::new(11));
    assert_eq!(ack.tail_sequence, crate::EventSequence::new(11));
    assert_eq!(ack.tail_event_id, second.id);
    assert_eq!(
        ack.acks,
        vec![
            crate::StreamAckClass::LocalCommitted,
            crate::StreamAckClass::QueryProjected
        ]
    );

    crate::stream_schema_registry_v1()
        .validate(
            crate::STREAM_APPEND_ACK_SCHEMA_V1,
            &serde_json::to_value(ack).unwrap(),
        )
        .unwrap();

    let empty = crate::StreamAppendAckV1::from_appended(
        stream_id,
        &[],
        vec![crate::StreamAckClass::LocalCommitted],
    )
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
        crate::StreamBackendCapabilitiesV1::sqlite_local("/tmp/verlet/session_history.turso");

    assert_eq!(
        capabilities.schema,
        crate::STREAM_BACKEND_CAPABILITIES_SCHEMA_V1
    );
    assert_eq!(
        capabilities.backend_kind,
        crate::StreamBackendKindV1::Sqlite
    );
    assert_eq!(
        capabilities.storage_scope,
        crate::StreamStorageScopeV1::LocalEmbedded
    );
    assert_eq!(
        capabilities.ack_classes,
        vec![
            crate::StreamAckClass::LocalCommitted,
            crate::StreamAckClass::QueryProjected
        ]
    );
    assert!(capabilities.supports_atomic_batch_append);
    assert!(capabilities.supports_verified_cursor_replay);
    assert!(capabilities.supports_query_projection);
    assert!(capabilities.supports_expected_tail);
    assert!(!capabilities.supports_fencing_tokens);
    assert!(!capabilities.supports_live_follow);
    assert!(!capabilities.supports_broadcast);
    assert!(!capabilities.supports_cold_archive);
    assert_eq!(
        capabilities.local_path.as_deref(),
        Some("/tmp/verlet/session_history.turso")
    );

    crate::stream_schema_registry_v1()
        .validate(
            crate::STREAM_BACKEND_CAPABILITIES_SCHEMA_V1,
            &serde_json::to_value(capabilities).unwrap(),
        )
        .unwrap();
}

#[test]
fn stream_schema_registry_v1_is_cached_once_per_process() {
    let first = crate::stream_schema_registry_v1();
    let second = crate::stream_schema_registry_v1();

    assert!(std::ptr::eq(first, second));
}

#[test]
fn stream_schema_registry_v1_validates_envelopes_and_context_payloads() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let source_range = crate::ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence: crate::EventSequence::new(1),
        to_sequence: crate::EventSequence::new(2),
    };
    let read_plan = serde_json::json!({
        "schema": crate::CONTEXT_READ_PLAN_SCHEMA_V1,
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
    let compile = crate::EventRecord::from_new(
        stream_id.clone(),
        crate::EventSequence::new(3),
        crate::NewEventRecord::discharged(
            coordinates.clone(),
            crate::EventKind::ContextCompileCompleted,
            serde_json::json!({
                "schema": crate::EventKind::ContextCompileCompleted.payload_schema_id(),
                "strategy": "naive_assembly",
                "output_hash": "sha256:compiled-context",
                "read_plan": read_plan.clone(),
            }),
            crate::EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_range: Some(source_range.clone()),
                source_ranges: vec![source_range.clone()],
                discharged_by: Some("projection:context-compiler".to_string()),
                function: Some("naive_assembly/v1".to_string()),
                ..crate::EventProvenance::default()
            },
        ),
    );
    let summary = crate::EventRecord::from_new(
        stream_id.clone(),
        crate::EventSequence::new(4),
        crate::NewEventRecord::discharged(
            coordinates.clone(),
            crate::EventKind::ContextSummaryCompleted,
            serde_json::json!({
                "schema": crate::EventKind::ContextSummaryCompleted.payload_schema_id(),
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
            crate::EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_range: Some(source_range.clone()),
                source_ranges: vec![source_range],
                discharged_by: Some("projection:context-summarizer".to_string()),
                function: Some("context_summary/v1".to_string()),
                ..crate::EventProvenance::default()
            },
        ),
    );
    let read_plan_set = crate::EventRecord::from_new(
        stream_id.clone(),
        crate::EventSequence::new(5),
        crate::NewEventRecord::discharged(
            coordinates.clone(),
            crate::EventKind::ContextReadPlanSet,
            serde_json::json!({
                "schema": crate::EventKind::ContextReadPlanSet.payload_schema_id(),
                "scope": "thread",
                "name": "history.default",
                "pipeline_id": "context.default",
                "source_id": stream_id.as_str(),
                "summary_event_id": summary.id.to_string(),
                "read_plan": {
                    "schema": crate::CONTEXT_READ_PLAN_SCHEMA_V1,
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
            crate::EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_event_ids: vec![summary.id],
                discharged_by: Some("controller:context-budget".to_string()),
                function: Some("context_read_plan/v1".to_string()),
                ..crate::EventProvenance::default()
            },
        ),
    );

    let registry = crate::stream_schema_registry_v1();
    for record in [&compile, &summary, &read_plan_set] {
        record.validate_stream_record_v1().unwrap();
        crate::validate_context_payload_schema_v1(record.kind, &record.payload).unwrap();
        registry
            .validate(
                crate::STREAM_RECORD_SCHEMA_V1,
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
        .validate(crate::STREAM_RECORD_SCHEMA_V1, &missing_stream_id)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("missing required property \"stream_id\"")
    );

    let mut malformed_plan = read_plan;
    malformed_plan["entries"][0]["kind"] = serde_json::json!("keep_everything");
    let err = registry
        .validate(crate::CONTEXT_READ_PLAN_SCHEMA_V1, &malformed_plan)
        .unwrap_err();
    assert!(err.to_string().contains("allowed enum values"));

    let mut debug_export = serde_json::json!({
        "schema": crate::DEBUG_THREAD_EXPORT_SCHEMA_V1,
        "threadId": coordinates.thread_id.to_string(),
        "generatedAtMs": 1_771_718_499_999i64,
        "backend": {
            "kind": "sqlite",
            "sessionStorePath": "/tmp/verlet/session_history.turso",
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
                "sessionStorePath": "/tmp/verlet/session_history.turso"
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
        .validate(crate::DEBUG_THREAD_EXPORT_SCHEMA_V1, &debug_export)
        .unwrap();
    debug_export.as_object_mut().unwrap().remove("streams");
    let err = registry
        .validate(crate::DEBUG_THREAD_EXPORT_SCHEMA_V1, &debug_export)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("missing required property \"streams\"")
    );

    let mut debug_export_with_extra = serde_json::json!({
        "schema": crate::DEBUG_THREAD_EXPORT_SCHEMA_V1,
        "threadId": coordinates.thread_id.to_string(),
        "generatedAtMs": 1_771_718_499_999i64,
        "backend": {
            "kind": "sqlite",
            "sessionStorePath": "/tmp/verlet/session_history.turso",
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
        .validate(
            crate::DEBUG_THREAD_EXPORT_SCHEMA_V1,
            &debug_export_with_extra,
        )
        .unwrap_err();
    assert!(err.to_string().contains("unexpected property \"surprise\""));
    debug_export_with_extra
        .as_object_mut()
        .unwrap()
        .remove("surprise");
    registry
        .validate(
            crate::DEBUG_THREAD_EXPORT_SCHEMA_V1,
            &debug_export_with_extra,
        )
        .unwrap();
}

#[test]
fn legacy_ingress_received_payload_decodes_without_witnessed_content() {
    let raw = serde_json::json!({
        "route_id": "legacy-route",
        "dedupe_key": "legacy:dispatch",
        "envelope_digest": "sha256:legacy"
    });

    let decoded: crate::IoIngressReceivedPayload = serde_json::from_value(raw).unwrap();

    assert!(decoded.content.is_none());
}

#[tokio::test]
async fn discharged_control_event_kinds_require_provenance() {
    let discharged_kinds = [
        crate::EventKind::ToolCallRequested,
        crate::EventKind::ToolCallSuspended,
        crate::EventKind::ToolCallDecision,
        crate::EventKind::TurnWaiting,
        crate::EventKind::TurnResumed,
        crate::EventKind::TurnCompleted,
        crate::EventKind::ApprovalRequested,
        crate::EventKind::TurnContinueRequested,
        crate::EventKind::TurnContinuationAccepted,
        crate::EventKind::TurnContinuationRejected,
        crate::EventKind::LoopCompleted,
        crate::EventKind::LoopBlocked,
        crate::EventKind::LoopBudgetExhausted,
        crate::EventKind::LoopDenied,
        crate::EventKind::CouplingRunCompleted,
        crate::EventKind::CouplingRunFailed,
        crate::EventKind::PlacementDecision,
    ];
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = crate::EventStreamId::for_thread(&coordinates);

    for kind in discharged_kinds {
        let store = crate::InMemorySessionStore::new();
        let record = crate::NewEventRecord::discharged(
            coordinates.clone(),
            kind,
            serde_json::json!({"kind": kind.as_ref()}),
            crate::EventProvenance::default(),
        );
        let record_id = record.id;
        let err = store
            .append_events(&stream_id, vec![record])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::HistoryError::DischargedWithoutProvenance(id) if id == record_id
        ));
    }
}

#[tokio::test]
async fn discharged_events_without_provenance_are_rejected() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let record = crate::NewEventRecord::discharged(
        coordinates.clone(),
        crate::EventKind::ContextCompileCompleted,
        serde_json::json!({"output_hash": "sha256:test"}),
        crate::EventProvenance::default(),
    );
    let record_id = record.id;

    let memory_store = crate::InMemorySessionStore::new();
    let err = memory_store
        .append_events(&stream_id, vec![record.clone()])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        crate::HistoryError::DischargedWithoutProvenance(id) if id == record_id
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
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let store = crate::InMemorySessionStore::new();
    let submitted = store
        .append_events(
            &stream_id,
            vec![crate::NewEventRecord::witnessed(
                coordinates.clone(),
                crate::EventKind::TurnSubmitted,
                serde_json::json!({
                    "schema": crate::EventKind::TurnSubmitted.payload_schema_id(),
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
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::assistant(
                    "test-provider",
                    crate::ProviderApi::OpenAIResponses,
                    "model-1",
                    vec![crate::CanonicalContent::text("hello back")],
                    crate::CanonicalStopReason::EndTurn,
                ),
            },
            crate::EventProvenance {
                source_streams: vec![stream_id.clone()],
                source_event_ids: vec![submitted.id],
                discharged_by: Some("propagator:agent-loop".to_string()),
                function: Some("session_entry_append/v1".to_string()),
                ..crate::EventProvenance::default()
            },
        )
        .await
        .unwrap();

    let events = store.read_events(&stream_id, None).await.unwrap();
    let assistant_event = events
        .iter()
        .find(|event| {
            event.kind == crate::EventKind::SessionEntryAppended
                && event.payload["entry_id"] == assistant_entry.entry_id.to_string()
        })
        .unwrap();
    assert_eq!(assistant_event.origin, crate::EventOrigin::Discharged);
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
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let valid = crate::NewEventRecord::witnessed(
        coordinates.clone(),
        crate::EventKind::SessionEntryAppended,
        serde_json::json!({"entry_id": "entry-1"}),
    );
    let invalid = crate::NewEventRecord::discharged(
        coordinates.clone(),
        crate::EventKind::ContextCompileCompleted,
        serde_json::json!({"output_hash": "sha256:test"}),
        crate::EventProvenance::default(),
    );
    let invalid_id = invalid.id;
    let store = crate::InMemorySessionStore::new();

    let err = store
        .append_events(&stream_id, vec![valid.clone(), invalid])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        crate::HistoryError::DischargedWithoutProvenance(id) if id == invalid_id
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
async fn in_memory_failed_session_append_leaves_entries_and_lease_epoch_unchanged() {
    let coordinates = coords("tenant_a", "user_1", "lease-rollback");
    let store = crate::InMemorySessionStore::new();
    let higher = store.clone().with_lease_epoch(9);
    let lower = store.with_lease_epoch(8);

    let error = higher
        .append_with_provenance(
            &coordinates,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::assistant(
                    "test-provider",
                    crate::ProviderApi::OpenAIResponses,
                    "test-model",
                    vec![crate::CanonicalContent::text("must roll back")],
                    crate::CanonicalStopReason::EndTurn,
                ),
            },
            crate::EventProvenance::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::HistoryError::DischargedWithoutProvenance(_)
    ));
    assert!(
        higher
            .build_context(&coordinates)
            .await
            .unwrap()
            .entries
            .is_empty()
    );

    lower
        .append(
            &coordinates,
            None,
            crate::SessionEntryKind::Message {
                message: crate::CanonicalMessage::user_text("lower epoch remains current"),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn in_memory_append_events_validate_stream_schema_before_mutation() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let valid = crate::NewEventRecord::witnessed(
        coordinates.clone(),
        crate::EventKind::SessionEntryAppended,
        serde_json::json!({"entry_id": "entry-1"}),
    );
    let invalid = crate::NewEventRecord::witnessed(
        coordinates,
        crate::EventKind::TurnSubmitted,
        serde_json::json!("not-an-object-payload"),
    );
    let shared = crate::InMemorySessionStore::new();
    let store = shared.clone().with_lease_epoch(9);
    let lower = shared.with_lease_epoch(8);

    let err = store
        .append_events(&stream_id, vec![valid.clone(), invalid])
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::HistoryError::Codec(message) if message.contains("expected object"))
    );
    assert!(
        store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );

    let appended = lower.append_events(&stream_id, vec![valid]).await.unwrap();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].sequence.get(), 1);
}

#[tokio::test]
async fn in_memory_append_events_validates_io_egress_requested_payload_schema() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let invalid = crate::NewEventRecord::discharged(
        coordinates.clone(),
        crate::EventKind::IoEgressRequested,
        serde_json::json!({
            "schema": crate::EventKind::IoEgressRequested.payload_schema_id(),
            "requested_by_tool_call_id": "call_1"
        }),
        crate::EventProvenance {
            source_streams: vec![stream_id.clone()],
            discharged_by: Some("rpc:append_events".to_string()),
            function: Some("io_egress_requested/v1".to_string()),
            ..crate::EventProvenance::default()
        },
    );
    let store = crate::InMemorySessionStore::new();

    let err = store
        .append_events(&stream_id, vec![invalid])
        .await
        .unwrap_err();
    assert!(matches!(err, crate::HistoryError::Codec(message) if message.contains("egress_kind")));
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
    let stream_id = crate::EventStreamId::for_thread(&coordinates);
    let store = crate::InMemorySessionStore::new();

    let appended = store
        .append_events(
            &stream_id,
            vec![
                crate::NewEventRecord::witnessed(
                    coordinates.clone(),
                    crate::EventKind::TurnSubmitted,
                    serde_json::json!({"schema": "cooldis.event.turn.submitted/1", "turn_id": "turn-1"}),
                ),
                crate::NewEventRecord::witnessed(
                    coordinates.clone(),
                    crate::EventKind::ToolCallCompleted,
                    serde_json::json!({"schema": "cooldis.event.tool.call.completed/1", "call_id": "call-1"}),
                ),
                crate::NewEventRecord::witnessed(
                    coordinates,
                    crate::EventKind::TurnCompleted,
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

    let wrong_stream = crate::EventStreamId::new("control:wrong-thread");
    let stream_err = store
        .read_events_after_cursor(&wrong_stream, &cursor)
        .await
        .unwrap_err();
    assert!(matches!(
        stream_err,
        crate::HistoryError::StreamCursorStreamMismatch { .. }
    ));

    let tampered = crate::StreamCursorV1 {
        event_id: appended[2].id,
        ..cursor
    };
    let cursor_err = store
        .read_events_after_cursor(&stream_id, &tampered)
        .await
        .unwrap_err();
    assert!(matches!(
        cursor_err,
        crate::HistoryError::StreamCursorMismatch { .. }
    ));
}
