mod support;

use verlet::kernel::coupling_scheduler::CouplingExecutor as _;
use verlet_provider::ProviderWireAdapter as _;

#[test]
fn runtime_event_kind_contract_matches_fixture() {
    let child_thread_id = thread_id(2);
    let checkpoint_id = checkpoint_id(1);
    let cases = vec![
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ThreadStarted {
            parent_thread_id: None,
            topology: verlet_runtime_contracts::ThreadTopology::root(),
            metadata: std::collections::BTreeMap::from([(
                "cooldis.agent.manifest_hash".to_string(),
                "sha256:manifest".to_string(),
            )]),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ThreadInteraction {
            interaction_id: runtime_event_id(1),
            kind: verlet_runtime_contracts::ThreadInteractionKind::PromptSubmitted,
            source_thread_id: thread_id(1),
            target_thread_id: child_thread_id,
            source_turn_id: None,
            target_turn_id: Some("turn-2".to_string()),
            result_preview: None,
            metadata: std::collections::BTreeMap::from([(
                "operation".to_string(),
                "cooldis.submit_to_thread".to_string(),
            )]),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::TextDelta {
            text: "hello".to_string(),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
            call_id: "call_1".to_string(),
            name: "bash".to_string(),
            input: serde_json::json!({"command":"pwd"}),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
            call_id: "call_1".to_string(),
            output: "ok".to_string(),
            success: true,
            duration_ms: Some(17),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolLog {
            call_id: "call_1".to_string(),
            tool_name: "bash".to_string(),
            level: verlet_runtime_contracts::RuntimeToolLogLevel::Info,
            message: "tool completed".to_string(),
            metadata: std::collections::BTreeMap::from([(
                "duration_ms".to_string(),
                "17".to_string(),
            )]),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::HookStarted {
            hook_id: "pre-echo".to_string(),
            event_name: verlet::agent::hooks::HookEventName::PreToolUse,
            matcher: Some("echo_search".to_string()),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::HookCompleted {
            hook_id: "pre-echo".to_string(),
            event_name: verlet::agent::hooks::HookEventName::PreToolUse,
            status: verlet::agent::hooks::HookRunStatus::Completed,
            duration_ms: 12,
            message: None,
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ApprovalRequested {
            approval_id: "approval_1".to_string(),
            action: "write_file".to_string(),
            metadata: std::collections::BTreeMap::from([(
                "path".to_string(),
                "/workspace/a".to_string(),
            )]),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ApprovalResolved {
            approval_id: "approval_1".to_string(),
            decision: verlet_runtime_contracts::RuntimeApprovalDecision::Approved,
            reason: None,
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::PermissionDecision {
            call_id: "call_1".to_string(),
            tool_name: "bash".to_string(),
            decision: verlet_runtime_contracts::RuntimePermissionDecision::Deny,
            reason: Some("policy denied".to_string()),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ContextCompiled {
            diagnostics: verlet::kernel::context_compiler::AgentContextCompilationDiagnostics {
                input_entry_count: 2,
                output_message_count: 1,
                system_block_count: 1,
                tool_count: 1,
                attachment_count: 0,
                retained_text_bytes: 11,
                truncated_text_bytes: 4,
                dropped_entries: Vec::new(),
            },
            provider_dropped_messages: 1,
            provider_truncated_text_bytes: 2,
            provider_retained_text_bytes: 9,
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestStarted {
            request_id: "req_1".to_string(),
            turn_id: "turn-1".to_string(),
            provider: "openai".to_string(),
            api: "openai_responses".to_string(),
            model: "gpt-test".to_string(),
            mode: verlet_runtime_contracts::RuntimeModelRequestMode::Complete,
            purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose::Turn,
            system_block_count: 1,
            message_count: 2,
            tool_count: 3,
            max_tokens: 128,
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestCompleted {
            request_id: "req_1".to_string(),
            turn_id: "turn-1".to_string(),
            provider: "openai".to_string(),
            api: "openai_responses".to_string(),
            model: "gpt-test".to_string(),
            mode: verlet_runtime_contracts::RuntimeModelRequestMode::Complete,
            purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose::Turn,
            duration_ms: 25,
            usage: verlet_runtime_contracts::RuntimeUsage {
                input_tokens: 1,
                output_tokens: 2,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 4,
            },
            stop_reason: verlet_history::CanonicalStopReason::EndTurn,
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestRetryScheduled {
            request_id: "req_1".to_string(),
            next_request_id: "req_1_retry".to_string(),
            turn_id: "turn-1".to_string(),
            provider: "openai".to_string(),
            api: "openai_responses".to_string(),
            model: "gpt-test".to_string(),
            mode: verlet_runtime_contracts::RuntimeModelRequestMode::Complete,
            purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose::Turn,
            attempt: 1,
            next_attempt: 2,
            delay_ms: 50,
            error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::RateLimited,
            error: "rate limited".to_string(),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFallbackSelected {
            request_id: "req_1".to_string(),
            turn_id: "turn-1".to_string(),
            from_provider: "openai".to_string(),
            from_api: "openai_responses".to_string(),
            from_model: "gpt-test".to_string(),
            to_provider: "fallback".to_string(),
            to_api: "openai_responses".to_string(),
            to_model: "gpt-fallback".to_string(),
            mode: verlet_runtime_contracts::RuntimeModelRequestMode::Complete,
            purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose::Turn,
            error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable,
            error: "provider down".to_string(),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFailed {
            request_id: "req_2".to_string(),
            turn_id: "turn-1".to_string(),
            provider: "openai".to_string(),
            api: "openai_responses".to_string(),
            model: "gpt-test".to_string(),
            mode: verlet_runtime_contracts::RuntimeModelRequestMode::Stream,
            purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose::Compaction,
            duration_ms: 3,
            error_class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable,
            error: "network".to_string(),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
            state: verlet_runtime_contracts::RuntimeTerminalState::Completed,
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Timeout {
            operation: "turn".to_string(),
            timeout_ms: 100,
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
            code: "max_pending_inputs".to_string(),
            message: "full".to_string(),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
            action: "abort_runtime".to_string(),
            reason: "timeout".to_string(),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Usage {
            usage: verlet_runtime_contracts::RuntimeUsage {
                input_tokens: 1,
                output_tokens: 2,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 4,
            },
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::SubthreadStarted { child_thread_id },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::SubthreadFinished {
            child_thread_id,
            status: verlet_runtime_contracts::ThreadLifecycleStatus::Stopped,
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Checkpoint {
            checkpoint_id,
            label: Some("label".to_string()),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Compaction {
            trigger: verlet::kernel::compaction::CompactionTrigger::Manual,
            summary: "summary".to_string(),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
            reason: "stop".to_string(),
        },
        verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed {
            code: "runtime_execution".to_string(),
            message: "boom".to_string(),
        },
    ];
    let actual = serde_json::to_value(cases).unwrap();
    crate::support::assert_json_fixture("contracts/runtime_event_kinds.json", actual);
}

#[test]
fn ingress_outcome_protocol_contract_matches_fixture() {
    let witness_event_id = event_record_id(40);
    let admission_event_id = event_record_id(41);
    let claim_event_id = event_record_id(42);
    let evidence_event_id = event_record_id(43);
    let claim = verlet_history::IoIngressClaimedPayload {
        ingress_envelope_ids: vec!["ingress-1".to_string()],
        ingress_witness_event_ids: vec![witness_event_id],
        admission_event_id,
        intent: verlet_history::IngressOutcomeIntent::Turn {
            turn_id: "turn-1".to_string(),
            submission_mode: "queue".to_string(),
            input_digest: "sha256:input".to_string(),
        },
    };
    let settle = verlet_history::IoIngressSettledPayload {
        claim_event_id,
        ingress_envelope_ids: vec!["ingress-1".to_string()],
        evidence_event_id: Some(evidence_event_id),
        settled_by: verlet_history::IngressSettledBy::Recovery,
    };
    let claim_value = serde_json::to_value(&claim).unwrap();
    let settle_value = serde_json::to_value(&settle).unwrap();
    let registry = verlet_history::stream_schema_registry_v1();
    registry
        .validate(
            verlet_history::EventKind::IoIngressClaimed.payload_schema_id(),
            &claim_value,
        )
        .unwrap();
    registry
        .validate(
            verlet_history::EventKind::IoIngressSettled.payload_schema_id(),
            &settle_value,
        )
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/ingress_outcome_protocol_v1.json",
        serde_json::json!({
            "claim": {
                "kind": verlet_history::EventKind::IoIngressClaimed.as_str(),
                "payload_schema": verlet_history::EventKind::IoIngressClaimed.payload_schema_id(),
                "payload": claim_value,
            },
            "settle": {
                "kind": verlet_history::EventKind::IoIngressSettled.as_str(),
                "payload_schema": verlet_history::EventKind::IoIngressSettled.payload_schema_id(),
                "payload": settle_value,
            }
        }),
    );
}

#[test]
fn stream_schema_v1_contract_matches_fixture() {
    let coordinates = coordinates();
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let source_range = verlet_history::ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence: verlet_history::EventSequence::new(1),
        to_sequence: verlet_history::EventSequence::new(3),
    };
    let retained_tail_range = verlet_history::ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence: verlet_history::EventSequence::new(4),
        to_sequence: verlet_history::EventSequence::new(6),
    };
    let full_compile_range = verlet_history::ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence: verlet_history::EventSequence::new(1),
        to_sequence: verlet_history::EventSequence::new(6),
    };
    let summary_event_id = event_record_id(2);
    let summary_text =
        "Keep the user intent and the published operation result; drop provider scaffolding.";

    let compile = verlet_history::EventRecord {
        id: event_record_id(1),
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(4),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_000,
        kind: verlet_history::EventKind::ContextCompileCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_range: Some(source_range.clone()),
            source_ranges: vec![source_range.clone()],
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("naive_assembly/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": "cooldis.event.context.compile.completed/1",
            "strategy": "naive_assembly",
            "output_hash": "sha256:compiled-context-fixture",
            "read_plan": {
                "schema": "cooldis.context.read_plan/1",
                "name": "history.default",
                "source_stream": stream_id.as_str(),
                "frontier": "compile_frontier",
                "entries": [{
                    "kind": "raw_range",
                    "stream_id": stream_id.as_str(),
                    "range": {
                        "from": "start",
                        "to": {
                            "sequence": 3
                        }
                    }
                }]
            }
        }),
    };
    let summary = verlet_history::EventRecord {
        id: summary_event_id,
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(5),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_100,
        kind: verlet_history::EventKind::ContextSummaryCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_range: Some(source_range.clone()),
            source_ranges: vec![source_range.clone()],
            discharged_by: Some("projection:context-summarizer".to_string()),
            function: Some("context_summary/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": "cooldis.event.context.summary.completed/1",
            "role": "summary_checkpoint",
            "text": summary_text,
            "covered_ranges": [{
                "stream_id": stream_id.as_str(),
                "from_sequence": 1,
                "to_sequence": 3
            }],
            "content": {
                "sha256": "sha256:860287ebb966c1b7d834c626a89b62626a4491116dfbc56ac6a39f363542cd98"
            }
        }),
    };
    let read_plan_set = verlet_history::EventRecord {
        id: event_record_id(3),
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(6),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_200,
        kind: verlet_history::EventKind::ContextReadPlanSet,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_event_ids: vec![summary_event_id],
            source_range: Some(source_range.clone()),
            source_ranges: vec![source_range.clone()],
            discharged_by: Some("controller:context-budget".to_string()),
            function: Some("context_read_plan/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": "cooldis.event.context.read_plan.set/1",
            "scope": "thread",
            "name": "history.default",
            "pipeline_id": "context.default",
            "source_id": stream_id.as_str(),
            "summary_event_id": summary_event_id.to_string(),
            "read_plan": {
                "schema": "cooldis.context.read_plan/1",
                "name": "history.default",
                "source_stream": stream_id.as_str(),
                "frontier": "compile_frontier",
                "entries": [{
                    "kind": "event_ref",
                    "stream_id": stream_id.as_str(),
                    "event_id": summary_event_id.to_string(),
                    "event_role": "summary_checkpoint",
                    "covers": {
                        "from": "start",
                        "to": {
                            "sequence": 3
                        }
                    }
                }]
            }
        }),
    };
    let compile_after_policy = verlet_history::EventRecord {
        id: event_record_id(4),
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(7),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_300,
        kind: verlet_history::EventKind::ContextCompileCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_event_ids: vec![summary_event_id],
            source_range: Some(full_compile_range),
            source_ranges: vec![source_range.clone(), retained_tail_range],
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("read_plan_assembly/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": "cooldis.event.context.compile.completed/1",
            "strategy": "read_plan_assembly",
            "output_hash": "sha256:compiled-context-after-drop-range",
            "read_plan": {
                "schema": "cooldis.context.read_plan/1",
                "name": "history.after_compaction",
                "source_stream": stream_id.as_str(),
                "frontier": "compile_frontier",
                "entries": [
                    {
                        "kind": "event_ref",
                        "stream_id": stream_id.as_str(),
                        "event_id": summary_event_id.to_string(),
                        "event_role": "summary_checkpoint",
                        "covers": {
                            "from": "start",
                            "to": {
                                "sequence": 3
                            }
                        }
                    },
                    {
                        "kind": "drop_range",
                        "stream_id": stream_id.as_str(),
                        "reason": "covered_by_summary_checkpoint",
                        "range": {
                            "from": "start",
                            "to": {
                                "sequence": 3
                            }
                        }
                    },
                    {
                        "kind": "raw_range",
                        "stream_id": stream_id.as_str(),
                        "range": {
                            "from": {
                                "sequence": 4
                            },
                            "to": {
                                "sequence": 6
                            }
                        }
                    }
                ]
            }
        }),
    };
    let branch_selection = verlet_history::EventRecord {
        id: event_record_id(5),
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(8),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_400,
        kind: verlet_history::EventKind::ThreadBranchSelected,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::to_value(verlet_history::ThreadBranchSelectedPayload {
            thread_id: coordinates.thread_id,
            selected_entry_id: Some(session_entry_id(2)),
            prior_entry_id: Some(session_entry_id(3)),
        })
        .unwrap(),
    };
    let reload_degraded = verlet_history::EventRecord {
        id: event_record_id(6),
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(9),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_500,
        kind: verlet_history::EventKind::ThreadReloadDegraded,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::json!({
            "thread_id": coordinates.thread_id.to_string(),
            "missing": ["topology", "parent_thread_id", "metadata"],
            "fallback": "fabricated_root"
        }),
    };

    let records = vec![
        compile,
        summary,
        read_plan_set,
        compile_after_policy,
        branch_selection.clone(),
        reload_degraded,
    ];
    let schema_registry = verlet_history::stream_schema_registry_v1();
    for record in &records {
        record.validate_stream_record_v1().unwrap();
        verlet_history::validate_context_payload_schema_v1(record.kind, &record.payload).unwrap();
        schema_registry
            .validate(
                verlet_history::STREAM_RECORD_SCHEMA_V1,
                &serde_json::to_value(record.to_stream_record_v1()).unwrap(),
            )
            .unwrap();
    }
    let cursors = records
        .iter()
        .map(verlet_history::EventRecord::cursor_v1)
        .collect::<Vec<_>>();
    for cursor in &cursors {
        cursor.validate_stream_cursor_v1().unwrap();
        schema_registry
            .validate(
                verlet_history::STREAM_CURSOR_SCHEMA_V1,
                &serde_json::to_value(cursor).unwrap(),
            )
            .unwrap();
    }
    let append_acks = vec![
        verlet_history::StreamAppendAckV1::from_appended(
            stream_id.clone(),
            &records,
            vec![
                verlet_history::StreamAckClass::LocalCommitted,
                verlet_history::StreamAckClass::QueryProjected,
            ],
        )
        .unwrap(),
    ];
    for ack in &append_acks {
        schema_registry
            .validate(
                verlet_history::STREAM_APPEND_ACK_SCHEMA_V1,
                &serde_json::to_value(ack).unwrap(),
            )
            .unwrap();
    }
    let backend_capabilities = vec![verlet_history::StreamBackendCapabilitiesV1::sqlite_local(
        "/tmp/verlet/session_history.sqlite3",
    )];
    for capabilities in &backend_capabilities {
        schema_registry
            .validate(
                verlet_history::STREAM_BACKEND_CAPABILITIES_SCHEMA_V1,
                &serde_json::to_value(capabilities).unwrap(),
            )
            .unwrap();
    }
    let routing = records
        .iter()
        .map(verlet_history::EventRecord::route_decision_v1)
        .collect::<Vec<_>>();
    for decision in &routing {
        schema_registry
            .validate(
                verlet_history::STREAM_ROUTING_DECISION_SCHEMA_V1,
                &serde_json::to_value(decision).unwrap(),
            )
            .unwrap();
    }

    crate::support::assert_json_fixture(
        "contracts/stream_schema_v1.json",
        serde_json::json!({
            "append_acks": append_acks,
            "backend_capabilities": backend_capabilities,
            "branch_selection": branch_selection.to_stream_record_v1(),
            "cursors": cursors,
            "records": records
                .iter()
                .map(verlet_history::EventRecord::to_stream_record_v1)
                .collect::<Vec<_>>(),
            "routing": routing
        }),
    );
}

#[test]
fn debug_thread_export_v1_contract_matches_fixture() {
    let coordinates = coordinates();
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let submitted = verlet_history::EventRecord {
        id: event_record_id(20),
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(1),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_020,
        kind: verlet_history::EventKind::TurnSubmitted,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::TurnSubmitted.payload_schema_id(),
            "turn_id": "turn-1",
            "input_text": "export evidence"
        }),
    };
    let bind = verlet_history::EventRecord {
        id: event_record_id(21),
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(2),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_021,
        kind: verlet_history::EventKind::ManifestBindCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_event_ids: vec![submitted.id],
            discharged_by: Some("manifest:bind".to_string()),
            function: Some("manifest_bind/v1".to_string()),
            config_hash: Some("sha256:manifest-bind-config".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::ManifestBindCompleted.payload_schema_id(),
            "agent_ref": "agent://export-fixture@0.1.0",
            "manifest_hash": "sha256:manifest"
        }),
    };
    let mut data = vec![
        serde_json::to_value(submitted.to_stream_record_v1()).unwrap(),
        serde_json::to_value(bind.to_stream_record_v1()).unwrap(),
    ];
    for (record, event) in [&submitted, &bind].into_iter().zip(&mut data) {
        let object = event.as_object_mut().unwrap();
        object.insert(
            "eventId".to_string(),
            serde_json::json!(record.id.to_string()),
        );
        object.insert("atMs".to_string(), serde_json::json!(record.created_at_ms));
    }
    let bind_cursor = serde_json::to_value(bind.cursor_v1()).unwrap();
    let bundle = serde_json::json!({
        "schema": verlet_history::DEBUG_THREAD_EXPORT_SCHEMA_V1,
        "threadId": coordinates.thread_id.to_string(),
        "generatedAtMs": 1_771_718_499_999i64,
        "backend": {
            "kind": "sqlite",
            "sessionStorePath": "/tmp/verlet/session_history.sqlite3",
            "ackClasses": ["local_committed", "query_projected"]
        },
        "ackClasses": ["local_committed", "query_projected"],
        "redaction": {
            "enabled": true,
            "mode": "secret-shaped-json-keys",
            "replacement": "[REDACTED]",
            "redactedKeys": ["api_key"]
        },
        "thread": null,
        "streams": [{
            "selector": "thread",
            "streamId": stream_id.as_str(),
            "backend": {
                "kind": "sqlite",
                "sessionStorePath": "/tmp/verlet/session_history.sqlite3"
            },
            "ackClasses": ["local_committed", "query_projected"],
            "range": {
                "fromSequence": 1,
                "fromCursor": "djE6MQ==",
                "lastExportedSequence": 2,
                "lastExportedStreamCursor": bind_cursor,
                "toCursor": "djE6Mw==",
                "tailSequence": 2,
                "tailStreamCursor": bind_cursor,
                "tailCursor": "djE6Mw=="
            },
            "data": data,
            "eventCount": 2,
            "truncated": false,
            "cursor": null,
            "streamCursor": null
        }],
        "receipts": [{
            "eventId": bind.id.to_string(),
            "streamId": stream_id.as_str(),
            "sequence": bind.sequence.get(),
            "kind": bind.kind.as_str(),
            "origin": bind.origin.as_str(),
            "payloadSchema": bind.kind.payload_schema_id(),
            "createdAtMs": bind.created_at_ms
        }]
    });

    verlet_history::stream_schema_registry_v1()
        .validate(verlet_history::DEBUG_THREAD_EXPORT_SCHEMA_V1, &bundle)
        .unwrap();
    crate::support::assert_json_fixture("contracts/debug_thread_export_v1.json", bundle);
}

#[test]
fn coupling_template_catalog_v1_contract_matches_fixture() {
    let catalog = verlet::agent::coupling_templates::coupling_template_catalog_v1();
    let ids = catalog
        .templates
        .iter()
        .map(|template| template.id.clone())
        .collect::<Vec<_>>();
    let mut maturity = std::collections::BTreeMap::<String, Vec<String>>::new();
    let mut must_have = Vec::new();
    let mut channel_decision_required = Vec::new();
    let mut runtime_executable = Vec::new();

    for template in &catalog.templates {
        maturity
            .entry(coupling_template_maturity_label(template.maturity).to_string())
            .or_default()
            .push(template.id.clone());
        if template.runtime_executable {
            runtime_executable.push(template.id.clone());
        }
        if template.must_have {
            must_have.push(template.id.clone());
        }
        if template.channel_decision_required {
            channel_decision_required.push(template.id.clone());
        }
    }

    crate::support::assert_json_fixture(
        "contracts/coupling_template_catalog_v1.json",
        serde_json::json!({
            "schema": catalog.schema,
            "ids": ids,
            "maturity": maturity,
            "must_have": must_have,
            "runtime_executable": runtime_executable,
            "channel_decision_required": channel_decision_required,
        }),
    );
}

#[tokio::test]
async fn stdlib_queue_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let control_stream_id =
        verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let submitted = verlet_history::EventRecord {
        id: event_record_id(30),
        stream_id: thread_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(1),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_030,
        kind: verlet_history::EventKind::TurnSubmitted,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::json!({
            "turn_id": "turn-1",
            "entry_id": "entry-1",
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let task_coupling = std_queue_task_bound_coupling();
    let task_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: submitted.id,
                trigger_event_id: submitted.id,
                trigger_stream_id: submitted.stream_id.to_string(),
                trigger_sequence: submitted.sequence.get(),
                coupling_id: task_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: task_coupling.clone(),
            trigger_event: submitted.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 1),
            source_events: vec![submitted.clone()],
        })
        .await
        .unwrap();

    let task_run = verlet::kernel::coupling_scheduler::CouplingRunReceipt {
        coupling_id: verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        status: verlet::kernel::coupling_scheduler::CouplingRunStatus::Completed,
        reason: None,
        root_event_id: submitted.id,
        trigger_event_id: submitted.id,
        trigger_stream_id: submitted.stream_id.to_string(),
        trigger_sequence: submitted.sequence.get(),
        snapshot_id: "snapshot-a".to_string(),
        depth: 0,
        source_cut: coupling_source_cut(&thread_stream_id, 1),
        source_event_ids: vec![submitted.id],
        discharged_event_ids: vec![event_record_id(31)],
        function_ref: task_coupling.function_ref.clone(),
        config_hash: task_coupling.config_hash.clone(),
        budget_spent: verlet::kernel::coupling_scheduler::CouplingBudgetSpent {
            discharge_events: 1,
        },
    };
    let task_run_event = verlet_history::EventRecord {
        id: event_record_id(32),
        stream_id: control_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(1),
        coordinates,
        created_at_ms: 1_771_718_400_032,
        kind: verlet_history::EventKind::CouplingRunCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_event_ids: vec![submitted.id],
            discharged_by: Some(format!(
                "coupling:{}",
                verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID
            )),
            function: Some(task_coupling.function_ref.clone()),
            config_hash: Some(task_coupling.config_hash.clone()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::to_value(task_run).unwrap(),
    };

    let callback_coupling = std_queue_completion_callback_bound_coupling();
    let callback_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: submitted.id,
                trigger_event_id: task_run_event.id,
                trigger_stream_id: task_run_event.stream_id.to_string(),
                trigger_sequence: task_run_event.sequence.get(),
                coupling_id: callback_coupling.id.clone(),
                depth: 1,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: callback_coupling,
            trigger_event: task_run_event.clone(),
            source_cut: coupling_source_cut(&control_stream_id, 1),
            source_events: vec![task_run_event],
        })
        .await
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/stdlib_queue_couplings.json",
        serde_json::json!({
            "queue_task": discharges_json(&task_result.discharges),
            "completion_callback": discharges_json(&callback_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_context_spill_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let compile = verlet_history::EventRecord {
        id: event_record_id(40),
        stream_id: thread_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(5),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_040,
        kind: verlet_history::EventKind::ContextCompileCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_ranges: vec![verlet_history::ObservationSourceRange {
                stream_id: thread_stream_id.clone(),
                from_sequence: verlet_history::EventSequence::new(1),
                to_sequence: verlet_history::EventSequence::new(4),
            }],
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("fixture_compile/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::ContextCompileCompleted.payload_schema_id(),
            "strategy": "fixture",
            "output_hash": "sha256:compile-output",
            "retained_text_bytes": 2000,
            "truncated_text_bytes": 640,
            "read_plan": {
                "schema": "cooldis.context.read_plan/1",
                "name": "history.default",
                "source_stream": thread_stream_id.as_str(),
                "frontier": "compile_frontier",
                "entries": [{
                    "kind": "raw_range",
                    "stream_id": thread_stream_id.as_str(),
                    "range": {
                        "from": "start",
                        "to": {"sequence": 4}
                    }
                }]
            }
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let spill_coupling = std_context_spill_bound_coupling();
    let spill_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: compile.id,
                trigger_event_id: compile.id,
                trigger_stream_id: compile.stream_id.to_string(),
                trigger_sequence: compile.sequence.get(),
                coupling_id: spill_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: spill_coupling,
            trigger_event: compile.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 5),
            source_events: vec![compile],
        })
        .await
        .unwrap();

    for discharge in &spill_result.discharges {
        verlet_history::validate_context_payload_schema_v1(discharge.kind, &discharge.payload)
            .unwrap();
    }
    crate::support::assert_json_fixture(
        "contracts/stdlib_context_spill_coupling.json",
        serde_json::json!({
            "context_spill": discharges_json(&spill_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_context_truncate_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let compile = verlet_history::EventRecord {
        id: event_record_id(42),
        stream_id: thread_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(10),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_042,
        kind: verlet_history::EventKind::ContextCompileCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_ranges: vec![verlet_history::ObservationSourceRange {
                stream_id: thread_stream_id.clone(),
                from_sequence: verlet_history::EventSequence::new(1),
                to_sequence: verlet_history::EventSequence::new(10),
            }],
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("fixture_compile/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::ContextCompileCompleted.payload_schema_id(),
            "strategy": "fixture",
            "output_hash": "sha256:compile-output-tail",
            "retained_text_bytes": 1200,
            "truncated_text_bytes": 1800,
            "read_plan": {
                "schema": "cooldis.context.read_plan/1",
                "name": "history.default",
                "source_stream": thread_stream_id.as_str(),
                "frontier": "compile_frontier",
                "entries": [{
                    "kind": "raw_range",
                    "stream_id": thread_stream_id.as_str(),
                    "range": {
                        "from": "start",
                        "to": {"sequence": 10}
                    }
                }]
            }
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let truncate_coupling = std_context_truncate_bound_coupling();
    let truncate_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: compile.id,
                trigger_event_id: compile.id,
                trigger_stream_id: compile.stream_id.to_string(),
                trigger_sequence: compile.sequence.get(),
                coupling_id: truncate_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: truncate_coupling,
            trigger_event: compile.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 10),
            source_events: vec![compile],
        })
        .await
        .unwrap();

    for discharge in &truncate_result.discharges {
        verlet_history::validate_context_payload_schema_v1(discharge.kind, &discharge.payload)
            .unwrap();
    }
    crate::support::assert_json_fixture(
        "contracts/stdlib_context_truncate_coupling.json",
        serde_json::json!({
            "context_truncate": discharges_json(&truncate_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_context_summarize_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let completed = verlet_history::EventRecord {
        id: event_record_id(43),
        stream_id: thread_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(11),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_043,
        kind: verlet_history::EventKind::TurnCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("runtime:provider-loop".to_string()),
            function: Some("turn_completion/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::TurnCompleted.payload_schema_id(),
            "turn_id": "turn-1",
            "output_text": "The user wants SQLite first, S2 later, and explicit segment maps."
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let summarize_coupling = std_context_summarize_bound_coupling();
    let summarize_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: completed.id,
                trigger_event_id: completed.id,
                trigger_stream_id: completed.stream_id.to_string(),
                trigger_sequence: completed.sequence.get(),
                coupling_id: summarize_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: summarize_coupling,
            trigger_event: completed.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 11),
            source_events: vec![completed],
        })
        .await
        .unwrap();

    for discharge in &summarize_result.discharges {
        verlet_history::validate_context_payload_schema_v1(discharge.kind, &discharge.payload)
            .unwrap();
    }
    crate::support::assert_json_fixture(
        "contracts/stdlib_context_summarize_coupling.json",
        serde_json::json!({
            "context_summarize": discharges_json(&summarize_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_memory_extract_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let completed = verlet_history::EventRecord {
        id: event_record_id(45),
        stream_id: thread_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(6),
        coordinates,
        created_at_ms: 1_771_718_400_045,
        kind: verlet_history::EventKind::TurnCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("runtime:provider-loop".to_string()),
            function: Some("turn_completion/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::TurnCompleted.payload_schema_id(),
            "turn_id": "turn-1",
            "output_text": "User prefers SQLite first, then S2 as stream backend."
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let memory_coupling = std_memory_extract_bound_coupling();
    let memory_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: event_record_id(44),
                trigger_event_id: completed.id,
                trigger_stream_id: completed.stream_id.to_string(),
                trigger_sequence: completed.sequence.get(),
                coupling_id: memory_coupling.id.clone(),
                depth: 1,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: memory_coupling,
            trigger_event: completed.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 6),
            source_events: vec![completed],
        })
        .await
        .unwrap();

    for discharge in &memory_result.discharges {
        verlet_history::validate_context_payload_schema_v1(discharge.kind, &discharge.payload)
            .unwrap();
    }
    crate::support::assert_json_fixture(
        "contracts/stdlib_memory_extract_coupling.json",
        serde_json::json!({
            "memory_extract": discharges_json(&memory_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_memory_recall_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let memory_stream_id =
        verlet_history::EventStreamId::new(format!("derived:memory:{}", coordinates.thread_id));
    let memory = verlet_history::EventRecord {
        id: event_record_id(46),
        stream_id: memory_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(2),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_046,
        kind: verlet_history::EventKind::ContextSummaryCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_event_ids: vec![event_record_id(45)],
            discharged_by: Some(format!(
                "coupling:{}",
                verlet::kernel::stdlib_couplings::STD_MEMORY_EXTRACT_TEMPLATE_ID
            )),
            function: Some(format!(
                "op://std-memory-extract/run@sha256:{}",
                "f".repeat(64)
            )),
            config_hash: Some("sha256:memory-extract".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id(),
            "role": "summary_checkpoint",
            "text": "User prefers SQLite first, then S2 as stream backend.",
            "covered_ranges": [{
                "stream_id": thread_stream_id.as_str(),
                "from_sequence": 1,
                "to_sequence": 6
            }],
            "content": {
                "sha256": "sha256:5ab05010794bf58b10e96d872372f426357516f95dd71a7e5c1098fc18251517"
            },
            "template_id": verlet::kernel::stdlib_couplings::STD_MEMORY_EXTRACT_TEMPLATE_ID,
            "memory_kind": "observation"
        }),
    };
    let submitted = verlet_history::EventRecord {
        id: event_record_id(47),
        stream_id: thread_stream_id,
        sequence: verlet_history::EventSequence::new(7),
        coordinates,
        created_at_ms: 1_771_718_400_047,
        kind: verlet_history::EventKind::TurnSubmitted,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::TurnSubmitted.payload_schema_id(),
            "turn_id": "turn-2",
            "input_text": "What should we use for V1 stream storage?"
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let recall_coupling = std_memory_recall_bound_coupling();
    let recall_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: submitted.id,
                trigger_event_id: submitted.id,
                trigger_stream_id: submitted.stream_id.to_string(),
                trigger_sequence: submitted.sequence.get(),
                coupling_id: recall_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: recall_coupling,
            trigger_event: submitted,
            source_cut: coupling_source_cut(&memory_stream_id, 2),
            source_events: vec![memory],
        })
        .await
        .unwrap();

    for discharge in &recall_result.discharges {
        verlet_history::validate_context_payload_schema_v1(discharge.kind, &discharge.payload)
            .unwrap();
    }
    crate::support::assert_json_fixture(
        "contracts/stdlib_memory_recall_coupling.json",
        serde_json::json!({
            "memory_recall": discharges_json(&recall_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_prompt_dynamic_instructions_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let manifest_bind = verlet_history::EventRecord {
        id: event_record_id(70),
        stream_id: thread_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(8),
        coordinates,
        created_at_ms: 1_771_718_400_070,
        kind: verlet_history::EventKind::ManifestBindCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("manifest:bind".to_string()),
            function: Some("manifest_bind/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::ManifestBindCompleted.payload_schema_id(),
            "agent_ref": "agent://release-verifier@0.1.0",
            "manifest_hash": "sha256:manifest"
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let instruction_coupling = std_prompt_dynamic_instructions_bound_coupling();
    let instruction_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: manifest_bind.id,
                trigger_event_id: manifest_bind.id,
                trigger_stream_id: manifest_bind.stream_id.to_string(),
                trigger_sequence: manifest_bind.sequence.get(),
                coupling_id: instruction_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: instruction_coupling,
            trigger_event: manifest_bind.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 8),
            source_events: vec![manifest_bind],
        })
        .await
        .unwrap();

    for discharge in &instruction_result.discharges {
        verlet_history::validate_context_payload_schema_v1(discharge.kind, &discharge.payload)
            .unwrap();
    }
    crate::support::assert_json_fixture(
        "contracts/stdlib_prompt_dynamic_instructions_coupling.json",
        serde_json::json!({
            "prompt_dynamic_instructions": discharges_json(&instruction_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_prompt_steer_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let control_stream_id =
        verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let completed = verlet_history::EventRecord {
        id: event_record_id(72),
        stream_id: thread_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(9),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_072,
        kind: verlet_history::EventKind::TurnCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("runtime:provider-loop".to_string()),
            function: Some("turn_completion/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::TurnCompleted.payload_schema_id(),
            "turn_id": "turn-1",
            "output_text": "Need one more clarification turn."
        }),
    };
    let approval = verlet_history::EventRecord {
        id: event_record_id(73),
        stream_id: control_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(2),
        coordinates,
        created_at_ms: 1_771_718_400_073,
        kind: verlet_history::EventKind::ApprovalResolved,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::ApprovalResolved.payload_schema_id(),
            "approval_id": "approval-instructions",
            "decision": "approved"
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let continuation_coupling = std_prompt_steer_continuation_bound_coupling();
    let continuation_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: completed.id,
                trigger_event_id: completed.id,
                trigger_stream_id: completed.stream_id.to_string(),
                trigger_sequence: completed.sequence.get(),
                coupling_id: continuation_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: continuation_coupling,
            trigger_event: completed.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 9),
            source_events: vec![completed],
        })
        .await
        .unwrap();
    let read_plan_coupling = std_prompt_steer_read_plan_bound_coupling();
    let read_plan_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: event_record_id(72),
                trigger_event_id: approval.id,
                trigger_stream_id: approval.stream_id.to_string(),
                trigger_sequence: approval.sequence.get(),
                coupling_id: read_plan_coupling.id.clone(),
                depth: 1,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: read_plan_coupling,
            trigger_event: approval.clone(),
            source_cut: coupling_source_cut(&control_stream_id, 2),
            source_events: vec![approval],
        })
        .await
        .unwrap();

    for discharge in continuation_result
        .discharges
        .iter()
        .chain(read_plan_result.discharges.iter())
    {
        if discharge.kind == verlet_history::EventKind::ContextReadPlanSet {
            verlet_history::validate_context_payload_schema_v1(discharge.kind, &discharge.payload)
                .unwrap();
        }
    }
    crate::support::assert_json_fixture(
        "contracts/stdlib_prompt_steer_coupling.json",
        serde_json::json!({
            "prompt_steer_continue": discharges_json(&continuation_result.discharges),
            "prompt_steer_read_plan": discharges_json(&read_plan_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_failure_deadletter_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let control_stream_id =
        verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let failed = verlet_history::EventRecord {
        id: event_record_id(50),
        stream_id: control_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(3),
        coordinates,
        created_at_ms: 1_771_718_400_050,
        kind: verlet_history::EventKind::CouplingRunFailed,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![verlet_history::EventStreamId::new("thread:fixture-thread")],
            discharged_by: Some(format!(
                "coupling:{}",
                verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID
            )),
            function: Some(format!("op://std-queue-task/run@sha256:{}", "a".repeat(64))),
            config_hash: Some("sha256:queue-task".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "coupling_id": verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID,
            "status": "failed",
            "reason": "remote service unavailable",
            "root_event_id": event_record_id(48).to_string(),
            "trigger_event_id": event_record_id(49).to_string(),
            "trigger_stream_id": "thread:fixture-thread",
            "trigger_sequence": 2,
            "snapshot_id": "snapshot-a",
            "depth": 0,
            "source_cut": {"entries": []},
            "source_event_ids": [],
            "discharged_event_ids": [],
            "function_ref": format!("op://std-queue-task/run@sha256:{}", "a".repeat(64)),
            "config_hash": "sha256:queue-task",
            "budget_spent": {"discharge_events": 0}
        }),
    };
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let deadletter_coupling = std_failure_deadletter_bound_coupling();
    let deadletter_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: event_record_id(48),
                trigger_event_id: failed.id,
                trigger_stream_id: failed.stream_id.to_string(),
                trigger_sequence: failed.sequence.get(),
                coupling_id: deadletter_coupling.id.clone(),
                depth: 1,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: deadletter_coupling,
            trigger_event: failed.clone(),
            source_cut: coupling_source_cut(&control_stream_id, 3),
            source_events: vec![failed],
        })
        .await
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/stdlib_failure_deadletter_coupling.json",
        serde_json::json!({
            "failure_deadletter": discharges_json(&deadletter_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_retry_with_budget_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let control_stream_id =
        verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let retryable = failed_coupling_run_event(
        coordinates.clone(),
        control_stream_id.clone(),
        event_record_id(60),
        4,
        serde_json::json!({
            "attempt": 1,
            "error_class": "retryable",
            "reason": "provider network hiccup"
        }),
    );
    let exhausted = failed_coupling_run_event(
        coordinates,
        control_stream_id.clone(),
        event_record_id(61),
        5,
        serde_json::json!({
            "attempt": 2,
            "error_class": "retryable",
            "reason": "provider network hiccup"
        }),
    );
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let retry_coupling = std_retry_with_budget_bound_coupling();
    let retry_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: event_record_id(58),
                trigger_event_id: retryable.id,
                trigger_stream_id: retryable.stream_id.to_string(),
                trigger_sequence: retryable.sequence.get(),
                coupling_id: retry_coupling.id.clone(),
                depth: 1,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: retry_coupling.clone(),
            trigger_event: retryable.clone(),
            source_cut: coupling_source_cut(&control_stream_id, 4),
            source_events: vec![retryable],
        })
        .await
        .unwrap();
    let exhausted_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: event_record_id(58),
                trigger_event_id: exhausted.id,
                trigger_stream_id: exhausted.stream_id.to_string(),
                trigger_sequence: exhausted.sequence.get(),
                coupling_id: retry_coupling.id.clone(),
                depth: 1,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: retry_coupling,
            trigger_event: exhausted.clone(),
            source_cut: coupling_source_cut(&control_stream_id, 5),
            source_events: vec![exhausted],
        })
        .await
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/stdlib_retry_with_budget_coupling.json",
        serde_json::json!({
            "retry_continue": discharges_json(&retry_result.discharges),
            "retry_exhausted": discharges_json(&exhausted_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_schedule_cron_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let control_stream_id =
        verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let accepted_mandate = schedule_mandate_started_event(
        coordinates.clone(),
        control_stream_id.clone(),
        event_record_id(80),
        6,
    );
    let accepted = timer_fired_event(
        coordinates.clone(),
        control_stream_id.clone(),
        event_record_id(81),
        7,
        accepted_mandate.id,
        1,
        "2026-01-01T00:01:00.000Z",
    );
    let exhausted_mandate = schedule_mandate_started_event(
        coordinates.clone(),
        control_stream_id.clone(),
        event_record_id(82),
        8,
    );
    let exhausted = timer_fired_event(
        coordinates,
        control_stream_id.clone(),
        event_record_id(83),
        9,
        exhausted_mandate.id,
        2,
        "2026-01-01T00:02:00.000Z",
    );
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let schedule_coupling = std_schedule_cron_bound_coupling();
    let schedule_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: accepted.id,
                trigger_event_id: accepted.id,
                trigger_stream_id: accepted.stream_id.to_string(),
                trigger_sequence: accepted.sequence.get(),
                coupling_id: schedule_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: schedule_coupling.clone(),
            trigger_event: accepted.clone(),
            source_cut: coupling_source_cut(&control_stream_id, 7),
            source_events: vec![accepted_mandate, accepted],
        })
        .await
        .unwrap();
    let exhausted_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: exhausted.id,
                trigger_event_id: exhausted.id,
                trigger_stream_id: exhausted.stream_id.to_string(),
                trigger_sequence: exhausted.sequence.get(),
                coupling_id: schedule_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: schedule_coupling,
            trigger_event: exhausted.clone(),
            source_cut: coupling_source_cut(&control_stream_id, 9),
            source_events: vec![exhausted_mandate, exhausted],
        })
        .await
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/stdlib_schedule_cron_coupling.json",
        serde_json::json!({
            "schedule_continue": discharges_json(&schedule_result.discharges),
            "schedule_exhausted": discharges_json(&exhausted_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_supervisor_child_completion_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let control_stream_id =
        verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let child_completed = verlet_history::EventRecord {
        id: event_record_id(100),
        stream_id: thread_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(12),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_100,
        kind: verlet_history::EventKind::TurnCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("runtime:child-thread".to_string()),
            function: Some("child_turn_completion/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::TurnCompleted.payload_schema_id(),
            "turn_id": "child-turn-1",
            "parent_thread_id": coordinates.thread_id.to_string(),
            "child_thread_id": thread_id(2).to_string(),
            "status": "completed",
            "output_text": "child finished release evidence collection"
        }),
    };
    let mut spawn_receipt_payload =
        serde_json::to_value(verlet::kernel::coupling_scheduler::CouplingRunReceipt {
            coupling_id: "std::supervisor.spawn".to_string(),
            role: verlet::agent::manifest_bind::CouplingRole::Controller,
            status: verlet::kernel::coupling_scheduler::CouplingRunStatus::Completed,
            reason: None,
            root_event_id: event_record_id(98),
            trigger_event_id: event_record_id(99),
            trigger_stream_id: thread_stream_id.to_string(),
            trigger_sequence: 11,
            snapshot_id: "snapshot-a".to_string(),
            depth: 0,
            source_cut: coupling_source_cut(&thread_stream_id, 11),
            source_event_ids: vec![event_record_id(99)],
            discharged_event_ids: vec![event_record_id(100)],
            function_ref: format!("op://std-supervisor-spawn/run@sha256:{}", "i".repeat(64)),
            config_hash: "sha256:supervisor-spawn".to_string(),
            budget_spent: verlet::kernel::coupling_scheduler::CouplingBudgetSpent {
                discharge_events: 1,
            },
        })
        .unwrap();
    spawn_receipt_payload["parent_thread_id"] =
        serde_json::json!(coordinates.thread_id.to_string());
    spawn_receipt_payload["child_thread_id"] = serde_json::json!(thread_id(2).to_string());
    spawn_receipt_payload["child_turn_id"] = serde_json::json!("child-turn-1");
    let spawn_completed = verlet_history::EventRecord {
        id: event_record_id(101),
        stream_id: control_stream_id.clone(),
        sequence: verlet_history::EventSequence::new(13),
        coordinates,
        created_at_ms: 1_771_718_400_101,
        kind: verlet_history::EventKind::CouplingRunCompleted,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("coupling:std::supervisor.spawn".to_string()),
            function: Some(format!(
                "op://std-supervisor-spawn/run@sha256:{}",
                "i".repeat(64)
            )),
            config_hash: Some("sha256:supervisor-spawn".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: spawn_receipt_payload,
    };

    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let loop_coupling = std_supervisor_child_completion_bound_coupling(serde_json::json!({
        "on_completed": "complete_loop",
        "reason": "child work joined back to supervisor"
    }));
    let loop_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: child_completed.id,
                trigger_event_id: child_completed.id,
                trigger_stream_id: child_completed.stream_id.to_string(),
                trigger_sequence: child_completed.sequence.get(),
                coupling_id: loop_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: loop_coupling,
            trigger_event: child_completed.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 12),
            source_events: vec![child_completed],
        })
        .await
        .unwrap();

    let continue_coupling = std_supervisor_child_completion_bound_coupling(serde_json::json!({
        "watch_coupling_id": "std::supervisor.spawn",
        "on_completed": "request_continuation",
        "loop_id": "supervisor-release",
        "parent_turn_id": "parent-turn-1",
        "next_turn_input": "incorporate child release evidence",
        "reason": "child completion should resume the supervisor"
    }));
    let continue_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: event_record_id(98),
                trigger_event_id: spawn_completed.id,
                trigger_stream_id: spawn_completed.stream_id.to_string(),
                trigger_sequence: spawn_completed.sequence.get(),
                coupling_id: continue_coupling.id.clone(),
                depth: 1,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: continue_coupling,
            trigger_event: spawn_completed.clone(),
            source_cut: coupling_source_cut(&control_stream_id, 13),
            source_events: vec![spawn_completed],
        })
        .await
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/stdlib_supervisor_child_completion_coupling.json",
        serde_json::json!({
            "child_turn_completed": discharges_json(&loop_result.discharges),
            "spawn_receipt_continue": discharges_json(&continue_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_permission_tool_gate_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let allow_request = tool_call_requested_event(
        coordinates.clone(),
        thread_stream_id.clone(),
        event_record_id(90),
        9,
        "call-allow",
    );
    let wait_request = tool_call_requested_event(
        coordinates,
        thread_stream_id.clone(),
        event_record_id(91),
        10,
        "call-wait",
    );
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let allow_coupling = std_permission_tool_gate_bound_coupling(serde_json::json!({
        "decision": "allow",
        "reason": "allowed by V1 tool gate fixture",
    }));
    let wait_coupling = std_permission_tool_gate_bound_coupling(serde_json::json!({
        "decision": "wait",
        "approval_id": "approval-shell-call",
        "reason": "operator approval required",
    }));
    let allow_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: allow_request.id,
                trigger_event_id: allow_request.id,
                trigger_stream_id: allow_request.stream_id.to_string(),
                trigger_sequence: allow_request.sequence.get(),
                coupling_id: allow_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: allow_coupling,
            trigger_event: allow_request.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 9),
            source_events: vec![allow_request],
        })
        .await
        .unwrap();
    let wait_result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: wait_request.id,
                trigger_event_id: wait_request.id,
                trigger_stream_id: wait_request.stream_id.to_string(),
                trigger_sequence: wait_request.sequence.get(),
                coupling_id: wait_coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling: wait_coupling,
            trigger_event: wait_request.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 10),
            source_events: vec![wait_request],
        })
        .await
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/stdlib_permission_tool_gate_coupling.json",
        serde_json::json!({
            "tool_gate_allow": discharges_json(&allow_result.discharges),
            "tool_gate_wait": discharges_json(&wait_result.discharges),
        }),
    );
}

#[tokio::test]
async fn stdlib_permission_approval_gate_coupling_receipts_match_fixture() {
    let coordinates = coordinates();
    let thread_stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let request = tool_call_requested_event(
        coordinates,
        thread_stream_id.clone(),
        event_record_id(92),
        11,
        "call-approval",
    );
    let executor = verlet::kernel::stdlib_couplings::StdlibCouplingExecutor;
    let coupling = std_permission_approval_gate_bound_coupling(serde_json::json!({
        "approval_id": "approval-shell-call",
        "reason": "operator approval required",
        "resume_token": "resume-shell-call"
    }));
    let result = executor
        .invoke(verlet::kernel::coupling_scheduler::CouplingInvocation {
            activation: verlet::kernel::coupling_scheduler::CouplingActivation {
                root_event_id: request.id,
                trigger_event_id: request.id,
                trigger_stream_id: request.stream_id.to_string(),
                trigger_sequence: request.sequence.get(),
                coupling_id: coupling.id.clone(),
                depth: 0,
                snapshot_id: "snapshot-a".to_string(),
            },
            coupling,
            trigger_event: request.clone(),
            source_cut: coupling_source_cut(&thread_stream_id, 11),
            source_events: vec![request],
        })
        .await
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/stdlib_permission_approval_gate_coupling.json",
        serde_json::json!({
            "approval_gate": discharges_json(&result.discharges),
        }),
    );
}

#[test]
fn hook_contracts_match_fixture() {
    let snapshot = turn_snapshot();
    let value = serde_json::json!({
        "pre_tool_request": verlet::agent::hooks::HookRequest::PreToolUse(verlet::agent::hooks::PreToolUseHookRequest {
            turn_context: snapshot.clone(),
            call_id: "call_1".to_string(),
            tool_name: "echo_search".to_string(),
            arguments: serde_json::json!({"input":"original"}),
        }),
        "post_tool_request": verlet::agent::hooks::HookRequest::PostToolUse(verlet::agent::hooks::PostToolUseHookRequest {
            turn_context: snapshot,
            call_id: "call_1".to_string(),
            tool_name: "echo_search".to_string(),
            arguments: serde_json::json!({"input":"rewritten"}),
            output: "echo:rewritten".to_string(),
            success: true,
        }),
        "handler_output": verlet::agent::hooks::HookHandlerOutput {
            updated_input: Some(serde_json::json!({"input":"rewritten"})),
            additional_context: Some("hook context".to_string()),
            feedback: Some("feedback context".to_string()),
            replacement_output: Some("replacement".to_string()),
            ..verlet::agent::hooks::HookHandlerOutput::default()
        },
        "run_record": verlet::agent::hooks::HookRunRecord {
            hook_id: "pre-echo".to_string(),
            event_name: verlet::agent::hooks::HookEventName::PreToolUse,
            matcher: Some("echo_search".to_string()),
            status: verlet::agent::hooks::HookRunStatus::Completed,
            started_at_ms: 10,
            completed_at_ms: 22,
            duration_ms: 12,
            message: None,
        },
    });
    crate::support::assert_json_fixture("contracts/hooks.json", value);
}

#[test]
fn thread_lifecycle_contracts_match_fixture() {
    let coordinates = coordinates();
    let signal = verlet_runtime_contracts::ThreadSignal {
        id: signal_id(1),
        coordinates: coordinates.clone(),
        kind: verlet_runtime_contracts::ThreadSignalKind::UserSteer,
        metadata: std::collections::BTreeMap::from([("turn_id".to_string(), "turn-2".to_string())]),
        created_at_ms: 100,
    };
    let checkpoint = verlet::kernel::runtime_host::runtime_api::ThreadCheckpoint {
        id: checkpoint_id(1),
        coordinates: coordinates.clone(),
        lineage: verlet::kernel::runtime_host::runtime_api::ThreadCheckpointLineage::Root,
        parent_checkpoint_id: None,
        active_entry_id: Some(session_entry_id(1)),
        label: Some("after-tool".to_string()),
        metadata: std::collections::BTreeMap::from([(
            "source".to_string(),
            "contract".to_string(),
        )]),
        created_at_ms: 200,
    };
    let lifecycle = verlet_runtime_contracts::ThreadLifecycleRecord {
        coordinates: coordinates.clone(),
        parent_thread_id: Some(thread_id(9)),
        topology: verlet_runtime_contracts::ThreadTopology::spawned_from(thread_id(9)),
        status: verlet_runtime_contracts::ThreadLifecycleStatus::Idle,
        latest_signal_id: Some(signal_id(1)),
        latest_checkpoint_id: Some(checkpoint_id(1)),
        created_at_ms: 1,
        updated_at_ms: 201,
        metadata: std::collections::BTreeMap::from([(
            "tenant_home".to_string(),
            "/tmp/tenant".to_string(),
        )]),
    };
    let receipt = verlet::kernel::runtime_host::kernel_control::AgentProcessSubmitReceipt {
        operation: "submit_to_thread".to_string(),
        caller_thread_id: thread_id(9),
        target_thread_id: thread_id(1),
        interaction_id: runtime_event_id(2),
        status: verlet_runtime_contracts::ThreadStatus::Running,
        turn_id: "turn-2".to_string(),
        dispatch_id: verlet_runtime_contracts::handle::DispatchId::new("submit-dispatch-2"),
    };
    crate::support::assert_json_fixture(
        "contracts/thread_lifecycle.json",
        serde_json::json!({
            "signal": signal,
            "checkpoint": checkpoint,
            "lifecycle": lifecycle,
            "agent_process_submit": receipt,
        }),
    );
}

#[test]
fn provider_wire_request_contracts_match_fixture() {
    let openai_responses = verlet_provider::OpenAIResponsesAdapter::default()
        .build_request_body(&provider_request(
            verlet_history::ProviderApi::OpenAIResponses,
            "gpt-fixture",
        ))
        .unwrap();
    let openai_chat = verlet_provider::OpenAIChatCompletionsAdapter
        .build_request_body(&provider_request(
            verlet_history::ProviderApi::OpenAIChatCompletions,
            "gpt-chat-fixture",
        ))
        .unwrap();
    let anthropic = verlet_provider::AnthropicMessagesAdapter
        .build_request_body(&provider_request(
            verlet_history::ProviderApi::AnthropicMessages,
            "claude-fixture",
        ))
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/provider_wire_requests.json",
        serde_json::json!({
            "openai_responses": openai_responses,
            "openai_chat_completions": openai_chat,
            "anthropic_messages": anthropic,
        }),
    );
}

#[test]
fn abi_and_process_contracts_match_fixture() {
    let operation_id = operation_id(1);
    let manifest = verlet_abi::WasmOperationManifest {
        abi: "cooldis_0.1".to_string(),
        operations: vec![verlet_abi::WasmOperationDefinition {
            id: 7,
            name: "search".to_string(),
            input: verlet_abi::WasmOperationValueKind::Json,
            output: verlet_abi::WasmOperationValueKind::Json,
            events: verlet_abi::WasmOperationEventKind::Jsonl,
            mode: verlet_abi::WasmOperationMode::Sync,
            required_capabilities: vec!["net:https://api.example.test".to_string()],
        }],
    };
    let operation = verlet_operations::RegisteredOperation {
        name: "search".to_string(),
        manifest: manifest.clone(),
        capability_grants: std::collections::BTreeSet::from([
            "net:https://api.example.test".to_string()
        ]),
        metadata: std::collections::BTreeMap::from([(
            "owner".to_string(),
            serde_json::json!("contracts"),
        )]),
    };
    let output = verlet_process::process::VerletProcessOutput {
        stdout: b"{\"ok\":true}\n".to_vec(),
        stderr: b"{\"level\":\"info\"}\n".to_vec(),
        terminal: Some(
            verlet_process::process::VerletProcessTerminalState::Completed {
                status: verlet_process::process::VerletProcessExitStatus::success(),
            },
        ),
        stdout_truncated: false,
        stderr_truncated: false,
        artifacts: vec![verlet_process::process::VerletProcessArtifact {
            artifact_id: "artifact_1".to_string(),
            path: Some(std::path::PathBuf::from("/workspace/out.json")),
            mime_type: Some("application/json".to_string()),
        }],
        file_deltas: vec![verlet_process::process::VerletProcessFileDelta {
            kind: verlet_process::bridge::FileDeltaKind::Write,
            path: std::path::PathBuf::from("/workspace/out.json"),
            target: None,
        }],
    };
    let operation_events = vec![
        verlet_process::bridge::OperationEvent::Started { operation_id },
        verlet_process::bridge::OperationEvent::Log {
            operation_id,
            level: verlet_process::bridge::OperationLogLevel::Info,
            message: "operation ready".to_string(),
        },
        verlet_process::bridge::OperationEvent::FileDelta {
            operation_id,
            kind: verlet_process::bridge::FileDeltaKind::Write,
            path: std::path::PathBuf::from("/workspace/out.json"),
            target: None,
        },
        verlet_process::bridge::OperationEvent::Completed {
            operation_id,
            status: verlet_process::bridge::OperationExitStatus::exited(0),
        },
    ];
    let mut unix_exec =
        verlet_process::bridge::UnixExecPayload::new("verlet run search search", "/workspace");
    unix_exec = unix_exec.with_mode(verlet_process::bridge::UnixExecutionMode::VirtualOnly);

    crate::support::assert_json_fixture(
        "contracts/abi_process.json",
        serde_json::json!({
            "bridge_backend_kind": verlet_process::bridge::BridgeBackendKind::InProcess,
            "manifest": manifest,
            "operation_events": operation_events,
            "projection": verlet_operations::OperationProjectionSet::from_registered(&operation),
            "process_output": output,
            "unix_exec_payload": unix_exec,
        }),
    );
}

fn coupling_template_maturity_label(
    maturity: verlet::agent::coupling_templates::CouplingTemplateMaturity,
) -> &'static str {
    match maturity {
        verlet::agent::coupling_templates::CouplingTemplateMaturity::KernelBacked => {
            "kernel_backed"
        }
        verlet::agent::coupling_templates::CouplingTemplateMaturity::InterfaceOnly => {
            "interface_only"
        }
        verlet::agent::coupling_templates::CouplingTemplateMaturity::ReferenceOnly => {
            "reference_only"
        }
    }
}

fn std_queue_task_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::TurnSubmitted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet_history::EventKind::TurnSubmitted],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet_history::EventKind::TurnWaiting],
        },
        function_ref: format!("op://std-queue-task/run@sha256:{}", "a".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-queue-task".to_string(),
            artifact_hash: "a".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({}),
        config_hash: "sha256:queue-task".to_string(),
    }
}

fn std_queue_completion_callback_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::CouplingRunCompleted,
        trigger_match: std::collections::BTreeMap::from([(
            "coupling_id".to_string(),
            serde_json::json!(verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID),
        )]),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![verlet_history::EventKind::CouplingRunCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet_history::EventKind::LoopCompleted],
        },
        function_ref: format!(
            "op://std-queue-completion-callback/run@sha256:{}",
            "b".repeat(64)
        ),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-queue-completion-callback".to_string(),
            artifact_hash: "b".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "watch_coupling_id": verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID,
            "on_completed": "complete_loop",
        }),
        config_hash: "sha256:queue-callback".to_string(),
    }
}

fn std_context_spill_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_CONTEXT_SPILL_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Projection,
        trigger_kind: verlet_history::EventKind::ContextCompileCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet_history::EventKind::ContextCompileCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![
                verlet_history::EventKind::ContextSummaryCompleted,
                verlet_history::EventKind::ContextReadPlanSet,
            ],
        },
        function_ref: format!("op://std-context-spill/run@sha256:{}", "c".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-context-spill".to_string(),
            artifact_hash: "c".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(2),
        },
        config: serde_json::json!({
            "summary_event_id": event_record_id(41).to_string(),
            "summary_text": "Spilled 640 bytes from context.compile.completed 018f0000-0000-7000-8000-000000000028.",
        }),
        config_hash: "sha256:context-spill".to_string(),
    }
}

fn std_context_truncate_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_CONTEXT_TRUNCATE_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::ContextCompileCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet_history::EventKind::ContextCompileCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet_history::EventKind::ContextReadPlanSet],
        },
        function_ref: format!("op://std-context-truncate/run@sha256:{}", "d".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-context-truncate".to_string(),
            artifact_hash: "d".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "retain_tail_events": 3,
            "reason": "fixture keeps only the raw tail",
        }),
        config_hash: "sha256:context-truncate".to_string(),
    }
}

fn std_context_summarize_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_CONTEXT_SUMMARIZE_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Projection,
        trigger_kind: verlet_history::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![
                verlet_history::EventKind::SessionEntryAppended,
                verlet_history::EventKind::TurnCompleted,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![
                verlet_history::EventKind::ContextSummaryCompleted,
                verlet_history::EventKind::ContextReadPlanSet,
            ],
        },
        function_ref: format!("op://std-context-summarize/run@sha256:{}", "e".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-context-summarize".to_string(),
            artifact_hash: "e".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(2),
        },
        config: serde_json::json!({
            "summary_event_id": event_record_id(44).to_string(),
        }),
        config_hash: "sha256:context-summarize".to_string(),
    }
}

fn std_memory_extract_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_MEMORY_EXTRACT_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Projection,
        trigger_kind: verlet_history::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![
                verlet_history::EventKind::TurnCompleted,
                verlet_history::EventKind::ToolCallCompleted,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "derived:memory".to_string(),
            kinds: vec![verlet_history::EventKind::ContextSummaryCompleted],
        },
        function_ref: format!("op://std-memory-extract/run@sha256:{}", "f".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-memory-extract".to_string(),
            artifact_hash: "f".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:memory".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({}),
        config_hash: "sha256:memory-extract".to_string(),
    }
}

fn std_memory_recall_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_MEMORY_RECALL_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Projection,
        trigger_kind: verlet_history::EventKind::TurnSubmitted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "derived:memory".to_string(),
            kinds: vec![verlet_history::EventKind::ContextSummaryCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![verlet_history::EventKind::ContextReadPlanSet],
        },
        function_ref: format!("op://std-memory-recall/run@sha256:{}", "g".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-memory-recall".to_string(),
            artifact_hash: "g".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:derived:memory".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({}),
        config_hash: "sha256:memory-recall".to_string(),
    }
}

fn std_prompt_steer_continuation_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_PROMPT_STEER_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet_history::EventKind::TurnCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet_history::EventKind::TurnContinueRequested],
        },
        function_ref: format!("op://std-prompt-steer/run@sha256:{}", "h".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-prompt-steer".to_string(),
            artifact_hash: "h".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "action": "request_continuation",
            "parent_turn_id": "turn-1",
            "loop_id": "prompt-steer",
            "next_turn_input": "Ask the user to pick the deployment lane.",
            "reason": "need explicit release lane choice"
        }),
        config_hash: "sha256:prompt-steer-continue".to_string(),
    }
}

fn std_prompt_steer_read_plan_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_PROMPT_STEER_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::ApprovalResolved,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![verlet_history::EventKind::ApprovalResolved],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet_history::EventKind::ContextReadPlanSet],
        },
        function_ref: format!("op://std-prompt-steer/run@sha256:{}", "h".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-prompt-steer".to_string(),
            artifact_hash: "h".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "action": "set_read_plan",
            "checkpoint_event_id": event_record_id(74).to_string(),
            "checkpoint_stream_id": "derived:context:instruction-fixture",
            "event_role": "instruction_checkpoint",
            "reason": "approved steering instructions"
        }),
        config_hash: "sha256:prompt-steer-read-plan".to_string(),
    }
}

fn std_prompt_dynamic_instructions_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID
            .to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Projection,
        trigger_kind: verlet_history::EventKind::ManifestBindCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![
                verlet_history::EventKind::ManifestBindCompleted,
                verlet_history::EventKind::ContextCompileCompleted,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![
                verlet_history::EventKind::ContextSummaryCompleted,
                verlet_history::EventKind::ContextReadPlanSet,
            ],
        },
        function_ref: format!(
            "op://std-prompt-dynamic-instructions/run@sha256:{}",
            "h".repeat(64)
        ),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-prompt-dynamic-instructions".to_string(),
            artifact_hash: "h".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(2),
        },
        config: serde_json::json!({
            "instruction_event_id": event_record_id(71).to_string(),
            "instruction_name": "instructions.default",
            "instruction_text": "When choosing the V1 stream backend, prefer SQLite event sourcing unless a live integration explicitly asks for S2.",
        }),
        config_hash: "sha256:prompt-dynamic-instructions".to_string(),
    }
}

fn std_permission_tool_gate_bound_coupling(
    config: serde_json::Value,
) -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_PERMISSION_TOOL_GATE_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::ToolCallRequested,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet_history::EventKind::ToolCallRequested],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet_history::EventKind::ToolCallDecision,
                verlet_history::EventKind::ToolCallSuspended,
            ],
        },
        function_ref: format!(
            "op://std-permission-tool-gate/run@sha256:{}",
            "p".repeat(64)
        ),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-permission-tool-gate".to_string(),
            artifact_hash: "p".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config,
        config_hash: "sha256:permission-tool-gate".to_string(),
    }
}

fn std_permission_approval_gate_bound_coupling(
    config: serde_json::Value,
) -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::ToolCallRequested,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet_history::EventKind::ToolCallRequested],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet_history::EventKind::ApprovalRequested,
                verlet_history::EventKind::ToolCallSuspended,
            ],
        },
        function_ref: format!(
            "op://std-permission-approval-gate/run@sha256:{}",
            "a".repeat(64)
        ),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-permission-approval-gate".to_string(),
            artifact_hash: "a".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(2),
        },
        config,
        config_hash: "sha256:permission-approval-gate".to_string(),
    }
}

fn std_failure_deadletter_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_FAILURE_DEADLETTER_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Projection,
        trigger_kind: verlet_history::EventKind::CouplingRunFailed,
        trigger_match: std::collections::BTreeMap::from([(
            "status".to_string(),
            serde_json::json!("failed"),
        )]),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![
                verlet_history::EventKind::CouplingRunFailed,
                verlet_history::EventKind::LoopBlocked,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "derived:deadletter".to_string(),
            kinds: vec![verlet_history::EventKind::CouplingRunFailed],
        },
        function_ref: format!("op://std-failure-deadletter/run@sha256:{}", "d".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-failure-deadletter".to_string(),
            artifact_hash: "d".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:derived:deadletter".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "reason": "deadletter failed control facts for inspection",
        }),
        config_hash: "sha256:failure-deadletter".to_string(),
    }
}

fn std_retry_with_budget_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_RETRY_WITH_BUDGET_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::CouplingRunFailed,
        trigger_match: std::collections::BTreeMap::from([(
            "status".to_string(),
            serde_json::json!("failed"),
        )]),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![verlet_history::EventKind::CouplingRunFailed],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet_history::EventKind::TurnContinueRequested,
                verlet_history::EventKind::LoopBudgetExhausted,
            ],
        },
        function_ref: format!("op://std-retry-with-budget/run@sha256:{}", "e".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-retry-with-budget".to_string(),
            artifact_hash: "e".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "max_attempts": 2,
            "loop_id": "loop-1",
            "parent_turn_id": "turn-1",
            "next_turn_input": "retry last failed step",
            "retryable_error_classes": ["retryable"],
            "reason": "retry remote transient failure",
        }),
        config_hash: "sha256:retry-with-budget".to_string(),
    }
}

fn std_schedule_cron_bound_coupling() -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_SCHEDULE_CRON_TEMPLATE_ID.to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::TimerFired,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![
                verlet_history::EventKind::MandateStarted,
                verlet_history::EventKind::MandateRevoked,
                verlet_history::EventKind::TimerFired,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet_history::EventKind::TurnContinueRequested,
                verlet_history::EventKind::LoopBudgetExhausted,
            ],
        },
        function_ref: format!("op://std-schedule-cron/run@sha256:{}", "s".repeat(64)),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-schedule-cron".to_string(),
            artifact_hash: "s".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "max_occurrences": 2,
            "schedule_id": "nightly-summary",
            "loop_id": "loop-nightly",
            "parent_turn_id": "turn-nightly-root",
            "next_turn_input": "run scheduled nightly summary",
            "reason": "scheduled occurrence accepted",
        }),
        config_hash: "sha256:schedule-cron".to_string(),
    }
}

fn std_supervisor_child_completion_bound_coupling(
    config: serde_json::Value,
) -> verlet::agent::manifest_bind::BoundCoupling {
    verlet::agent::manifest_bind::BoundCoupling {
        id: verlet::kernel::stdlib_couplings::STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID
            .to_string(),
        role: verlet::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: verlet_history::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![
                verlet_history::EventKind::TurnCompleted,
                verlet_history::EventKind::CouplingRunCompleted,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet_history::EventKind::TurnContinueRequested,
                verlet_history::EventKind::LoopCompleted,
            ],
        },
        function_ref: format!(
            "op://std-supervisor-child-completion/run@sha256:{}",
            "j".repeat(64)
        ),
        function: verlet::agent::manifest_bind::BoundCouplingFunction {
            name: "std-supervisor-child-completion".to_string(),
            artifact_hash: "j".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config,
        config_hash: "sha256:supervisor-child-completion".to_string(),
    }
}

fn failed_coupling_run_event(
    coordinates: verlet_runtime_contracts::ThreadCoordinates,
    stream_id: verlet_history::EventStreamId,
    id: verlet_history::EventRecordId,
    sequence: i64,
    fields: serde_json::Value,
) -> verlet_history::EventRecord {
    let mut payload = serde_json::json!({
        "coupling_id": verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID,
        "status": "failed",
        "root_event_id": event_record_id(58).to_string(),
        "trigger_event_id": event_record_id(59).to_string(),
        "trigger_stream_id": "thread:fixture-thread",
        "trigger_sequence": 3,
        "snapshot_id": "snapshot-a",
        "depth": 0,
        "source_cut": {"entries": []},
        "source_event_ids": [],
        "discharged_event_ids": [],
        "function_ref": format!("op://std-queue-task/run@sha256:{}", "a".repeat(64)),
        "config_hash": "sha256:queue-task",
        "budget_spent": {"discharge_events": 0}
    });
    if let Some(object) = payload.as_object_mut()
        && let Some(fields) = fields.as_object()
    {
        object.extend(fields.clone());
    }
    verlet_history::EventRecord {
        id,
        stream_id,
        sequence: verlet_history::EventSequence::new(sequence),
        coordinates,
        created_at_ms: 1_771_718_400_060 + sequence,
        kind: verlet_history::EventKind::CouplingRunFailed,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![verlet_history::EventStreamId::new("thread:fixture-thread")],
            discharged_by: Some(format!(
                "coupling:{}",
                verlet::kernel::stdlib_couplings::STD_QUEUE_TASK_TEMPLATE_ID
            )),
            function: Some(format!("op://std-queue-task/run@sha256:{}", "a".repeat(64))),
            config_hash: Some("sha256:queue-task".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload,
    }
}

fn tool_call_requested_event(
    coordinates: verlet_runtime_contracts::ThreadCoordinates,
    stream_id: verlet_history::EventStreamId,
    id: verlet_history::EventRecordId,
    sequence: i64,
    call_id: &str,
) -> verlet_history::EventRecord {
    verlet_history::EventRecord {
        id,
        stream_id,
        sequence: verlet_history::EventSequence::new(sequence),
        coordinates,
        created_at_ms: 1_771_718_400_090 + sequence,
        kind: verlet_history::EventKind::ToolCallRequested,
        origin: verlet_history::EventOrigin::Discharged,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![verlet_history::EventStreamId::new("thread:fixture-thread")],
            discharged_by: Some("runtime:provider-loop".to_string()),
            function: Some("provider_tool_request/v1".to_string()),
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::ToolCallRequested.payload_schema_id(),
            "subject": {
                "turn_id": "turn-1",
                "call_id": call_id,
            },
            "snapshot_id": "snapshot-a",
            "tool_name": "shell.exec",
            "arguments": {
                "cmd": "date",
            },
        }),
    }
}

fn schedule_mandate_started_event(
    coordinates: verlet_runtime_contracts::ThreadCoordinates,
    stream_id: verlet_history::EventStreamId,
    id: verlet_history::EventRecordId,
    sequence: i64,
) -> verlet_history::EventRecord {
    let thread_id = coordinates.thread_id.to_string();
    verlet_history::EventRecord {
        id,
        stream_id,
        sequence: verlet_history::EventSequence::new(sequence),
        coordinates,
        created_at_ms: 1_771_718_400_080 + sequence,
        kind: verlet_history::EventKind::MandateStarted,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::json!({
            "schema": verlet_history::EventKind::MandateStarted.payload_schema_id(),
            "subject": {
                "thread_id": thread_id.clone(),
                "loop_id": "loop-nightly"
            },
            "mandate_id": "mandate-nightly-summary",
            "snapshot_id": "schedule.v1",
            "thread_id": thread_id,
            "schedule": {
                "interval": {
                    "every_ms": 60000
                }
            },
            "max_occurrences": 2,
            "catch_up": "skip_missed",
            "input_template": "run scheduled nightly summary for {scheduled_for}"
        }),
    }
}

fn timer_fired_event(
    coordinates: verlet_runtime_contracts::ThreadCoordinates,
    stream_id: verlet_history::EventStreamId,
    id: verlet_history::EventRecordId,
    sequence: i64,
    mandate_event_id: verlet_history::EventRecordId,
    occurrence_index: u64,
    scheduled_for: &str,
) -> verlet_history::EventRecord {
    verlet_history::EventRecord {
        id,
        stream_id: stream_id.clone(),
        sequence: verlet_history::EventSequence::new(sequence),
        coordinates,
        created_at_ms: 1_771_718_400_080 + sequence,
        kind: verlet_history::EventKind::TimerFired,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance {
            source_streams: vec![stream_id],
            source_event_ids: vec![mandate_event_id],
            ..verlet_history::EventProvenance::default()
        },
        payload: serde_json::json!({
            "mandate_event_id": mandate_event_id.to_string(),
            "scheduled_for": scheduled_for,
            "occurrence_index": occurrence_index,
            "catch_up": false
        }),
    }
}

fn coupling_source_cut(
    stream_id: &verlet_history::EventStreamId,
    max_sequence: i64,
) -> verlet::kernel::coupling_scheduler::CouplingSourceCut {
    verlet::kernel::coupling_scheduler::CouplingSourceCut {
        entries: vec![verlet::kernel::coupling_scheduler::CouplingSourceCutEntry {
            stream_id: stream_id.to_string(),
            max_sequence,
        }],
    }
}

fn discharges_json(
    discharges: &[verlet::kernel::coupling_scheduler::CouplingDischarge],
) -> Vec<serde_json::Value> {
    discharges
        .iter()
        .map(|discharge| {
            let mut value = serde_json::json!({
                "stream": discharge.stream,
                "kind": discharge.kind.as_str(),
                "payload": discharge.payload,
            });
            if let Some(event_id) = discharge.event_id
                && let Some(object) = value.as_object_mut()
            {
                object.insert(
                    "event_id".to_string(),
                    serde_json::json!(event_id.to_string()),
                );
            }
            value
        })
        .collect()
}

fn provider_request(
    api: verlet_history::ProviderApi,
    model: &str,
) -> verlet_provider::ProviderRequest {
    verlet_provider::ProviderRequest {
        api,
        provider: "fixture-provider".to_string(),
        model: model.to_string(),
        system: vec![verlet_provider::SystemBlock::text("Be precise.")],
        messages: vec![
            verlet_history::CanonicalMessage::user_text("hello"),
            verlet_history::CanonicalMessage::assistant(
                "fixture-provider",
                verlet_history::ProviderApi::Other("fixture".to_string()),
                model,
                vec![
                    verlet_history::CanonicalContent::text("thinking done"),
                    verlet_history::CanonicalContent::tool_call(
                        "call_1|fc_1",
                        "search",
                        serde_json::json!({"query":"verlet"}),
                    ),
                ],
                verlet_history::CanonicalStopReason::ToolUse,
            ),
            verlet_history::CanonicalMessage::tool_result("call_1|fc_1", "search", "result", false),
        ],
        tools: vec![verlet_provider::ToolDefinition::new(
            "search",
            "Search docs.",
            serde_json::json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": false
            }),
        )],
        max_tokens: 64,
        temperature: None,
        thinking: None,
    }
}

fn turn_snapshot() -> verlet::kernel::runtime_host::turn::TurnContextSnapshot {
    verlet::kernel::runtime_host::turn::TurnContextSnapshot {
        turn_id: "turn-1".to_string(),
        trace_id: "trace-1".to_string(),
        coordinates: coordinates(),
        parent_thread_id: Some(thread_id(9)),
        topology: verlet_runtime_contracts::ThreadTopology::spawned_from(thread_id(9)),
        cwd: Some(std::path::PathBuf::from("/workspace")),
        workspace_roots: vec![std::path::PathBuf::from("/workspace")],
        model: Some("gpt-test".to_string()),
        provider: Some("openai".to_string()),
        thinking: None,
        permission_profile: Some("workspace-write".to_string()),
        provider_metadata: std::collections::BTreeMap::from([(
            "tier".to_string(),
            "test".to_string(),
        )]),
        metadata: std::collections::BTreeMap::from([(
            "source".to_string(),
            "contract".to_string(),
        )]),
        environment: std::collections::BTreeMap::from([(
            "VERLET_TEST".to_string(),
            "1".to_string(),
        )]),
        model_visible_context: vec!["extra context".to_string()],
        budget: verlet_runtime_contracts::TurnBudget {
            max_tool_rounds: Some(4),
            max_output_tokens: Some(128),
            max_context_text_bytes: Some(2048),
        },
        cancellation_requested: false,
    }
}

fn coordinates() -> verlet_runtime_contracts::ThreadCoordinates {
    verlet_runtime_contracts::ThreadCoordinates {
        tenant_id: "tenant_a".to_string(),
        user_id: "user_1".to_string(),
        session_id: "session_1".to_string(),
        thread_id: thread_id(1),
    }
}

fn thread_id(n: u128) -> verlet_runtime_contracts::ThreadId {
    verlet_runtime_contracts::ThreadId::parse_str(&format!("018f0000-0000-7000-8000-{n:012x}"))
        .unwrap()
}

fn checkpoint_id(n: u128) -> verlet_runtime_contracts::ThreadCheckpointId {
    verlet_runtime_contracts::ThreadCheckpointId::from_uuid(uuid(n))
}

fn signal_id(n: u128) -> verlet_runtime_contracts::ThreadSignalId {
    verlet_runtime_contracts::ThreadSignalId::from_uuid(uuid(n))
}

fn runtime_event_id(n: u128) -> verlet_runtime_contracts::RuntimeEventId {
    verlet_runtime_contracts::RuntimeEventId::from_uuid(uuid(n))
}

fn event_record_id(n: u128) -> verlet_history::EventRecordId {
    verlet_history::EventRecordId::from_uuid(uuid(n))
}

fn session_entry_id(n: u128) -> verlet_history::SessionEntryId {
    verlet_history::SessionEntryId::from_uuid(uuid(n))
}

fn operation_id(n: u128) -> verlet_process::bridge::OperationId {
    verlet_process::bridge::OperationId::from_uuid(uuid(n))
}

fn uuid(n: u128) -> uuid::Uuid {
    uuid::Uuid::parse_str(&format!("018f0000-0000-7000-8000-{n:012x}")).unwrap()
}
