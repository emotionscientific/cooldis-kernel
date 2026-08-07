mod support;

use verlet::CouplingExecutor as _;
use verlet::ProviderWireAdapter as _;

#[test]
fn runtime_event_kind_contract_matches_fixture() {
    let child_thread_id = thread_id(2);
    let checkpoint_id = checkpoint_id(1);
    let cases = vec![
        verlet::RuntimeEventKind::ThreadStarted {
            parent_thread_id: None,
            topology: verlet::ThreadTopology::root(),
            metadata: std::collections::BTreeMap::from([(
                "cooldis.agent.manifest_hash".to_string(),
                "sha256:manifest".to_string(),
            )]),
        },
        verlet::RuntimeEventKind::ThreadInteraction {
            interaction_id: runtime_event_id(1),
            kind: verlet::ThreadInteractionKind::PromptSubmitted,
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
        verlet::RuntimeEventKind::TextDelta {
            text: "hello".to_string(),
        },
        verlet::RuntimeEventKind::ToolCallStarted {
            call_id: "call_1".to_string(),
            name: "bash".to_string(),
            input: serde_json::json!({"command":"pwd"}),
        },
        verlet::RuntimeEventKind::ToolCallResult {
            call_id: "call_1".to_string(),
            output: "ok".to_string(),
            success: true,
            duration_ms: Some(17),
        },
        verlet::RuntimeEventKind::ToolLog {
            call_id: "call_1".to_string(),
            tool_name: "bash".to_string(),
            level: verlet::RuntimeToolLogLevel::Info,
            message: "tool completed".to_string(),
            metadata: std::collections::BTreeMap::from([(
                "duration_ms".to_string(),
                "17".to_string(),
            )]),
        },
        verlet::RuntimeEventKind::HookStarted {
            hook_id: "pre-echo".to_string(),
            event_name: verlet::HookEventName::PreToolUse,
            matcher: Some("echo_search".to_string()),
        },
        verlet::RuntimeEventKind::HookCompleted {
            hook_id: "pre-echo".to_string(),
            event_name: verlet::HookEventName::PreToolUse,
            status: verlet::HookRunStatus::Completed,
            duration_ms: 12,
            message: None,
        },
        verlet::RuntimeEventKind::ApprovalRequested {
            approval_id: "approval_1".to_string(),
            action: "write_file".to_string(),
            metadata: std::collections::BTreeMap::from([(
                "path".to_string(),
                "/workspace/a".to_string(),
            )]),
        },
        verlet::RuntimeEventKind::ApprovalResolved {
            approval_id: "approval_1".to_string(),
            decision: verlet::RuntimeApprovalDecision::Approved,
            reason: None,
        },
        verlet::RuntimeEventKind::PermissionDecision {
            call_id: "call_1".to_string(),
            tool_name: "bash".to_string(),
            decision: verlet::RuntimePermissionDecision::Deny,
            reason: Some("policy denied".to_string()),
        },
        verlet::RuntimeEventKind::ContextCompiled {
            diagnostics: verlet::AgentContextCompilationDiagnostics {
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
        verlet::RuntimeEventKind::ModelRequestStarted {
            request_id: "req_1".to_string(),
            turn_id: "turn-1".to_string(),
            provider: "openai".to_string(),
            api: "openai_responses".to_string(),
            model: "gpt-test".to_string(),
            mode: verlet::RuntimeModelRequestMode::Complete,
            purpose: verlet::RuntimeModelRequestPurpose::Turn,
            system_block_count: 1,
            message_count: 2,
            tool_count: 3,
            max_tokens: 128,
        },
        verlet::RuntimeEventKind::ModelRequestCompleted {
            request_id: "req_1".to_string(),
            turn_id: "turn-1".to_string(),
            provider: "openai".to_string(),
            api: "openai_responses".to_string(),
            model: "gpt-test".to_string(),
            mode: verlet::RuntimeModelRequestMode::Complete,
            purpose: verlet::RuntimeModelRequestPurpose::Turn,
            duration_ms: 25,
            usage: verlet::RuntimeUsage {
                input_tokens: 1,
                output_tokens: 2,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 4,
            },
            stop_reason: verlet::CanonicalStopReason::EndTurn,
        },
        verlet::RuntimeEventKind::ModelRequestRetryScheduled {
            request_id: "req_1".to_string(),
            next_request_id: "req_1_retry".to_string(),
            turn_id: "turn-1".to_string(),
            provider: "openai".to_string(),
            api: "openai_responses".to_string(),
            model: "gpt-test".to_string(),
            mode: verlet::RuntimeModelRequestMode::Complete,
            purpose: verlet::RuntimeModelRequestPurpose::Turn,
            attempt: 1,
            next_attempt: 2,
            delay_ms: 50,
            error_class: verlet::RuntimeModelRequestErrorClass::RateLimited,
            error: "rate limited".to_string(),
        },
        verlet::RuntimeEventKind::ModelRequestFallbackSelected {
            request_id: "req_1".to_string(),
            turn_id: "turn-1".to_string(),
            from_provider: "openai".to_string(),
            from_api: "openai_responses".to_string(),
            from_model: "gpt-test".to_string(),
            to_provider: "fallback".to_string(),
            to_api: "openai_responses".to_string(),
            to_model: "gpt-fallback".to_string(),
            mode: verlet::RuntimeModelRequestMode::Complete,
            purpose: verlet::RuntimeModelRequestPurpose::Turn,
            error_class: verlet::RuntimeModelRequestErrorClass::Retryable,
            error: "provider down".to_string(),
        },
        verlet::RuntimeEventKind::ModelRequestFailed {
            request_id: "req_2".to_string(),
            turn_id: "turn-1".to_string(),
            provider: "openai".to_string(),
            api: "openai_responses".to_string(),
            model: "gpt-test".to_string(),
            mode: verlet::RuntimeModelRequestMode::Stream,
            purpose: verlet::RuntimeModelRequestPurpose::Compaction,
            duration_ms: 3,
            error_class: verlet::RuntimeModelRequestErrorClass::Retryable,
            error: "network".to_string(),
        },
        verlet::RuntimeEventKind::Terminal {
            state: verlet::RuntimeTerminalState::Completed,
        },
        verlet::RuntimeEventKind::Timeout {
            operation: "turn".to_string(),
            timeout_ms: 100,
        },
        verlet::RuntimeEventKind::PolicyRejected {
            code: "max_pending_inputs".to_string(),
            message: "full".to_string(),
        },
        verlet::RuntimeEventKind::Recovery {
            action: "abort_runtime".to_string(),
            reason: "timeout".to_string(),
        },
        verlet::RuntimeEventKind::Usage {
            usage: verlet::RuntimeUsage {
                input_tokens: 1,
                output_tokens: 2,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 4,
            },
        },
        verlet::RuntimeEventKind::SubthreadStarted { child_thread_id },
        verlet::RuntimeEventKind::SubthreadFinished {
            child_thread_id,
            status: verlet::ThreadLifecycleStatus::Stopped,
        },
        verlet::RuntimeEventKind::Checkpoint {
            checkpoint_id,
            label: Some("label".to_string()),
        },
        verlet::RuntimeEventKind::Compaction {
            trigger: verlet::CompactionTrigger::Manual,
            summary: "summary".to_string(),
        },
        verlet::RuntimeEventKind::Cancelled {
            reason: "stop".to_string(),
        },
        verlet::RuntimeEventKind::Failed {
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
    let claim = verlet::IoIngressClaimedPayload {
        ingress_envelope_ids: vec!["ingress-1".to_string()],
        ingress_witness_event_ids: vec![witness_event_id],
        admission_event_id,
        intent: verlet::IngressOutcomeIntent::Turn {
            turn_id: "turn-1".to_string(),
            submission_mode: "queue".to_string(),
            input_digest: "sha256:input".to_string(),
        },
    };
    let settle = verlet::IoIngressSettledPayload {
        claim_event_id,
        ingress_envelope_ids: vec!["ingress-1".to_string()],
        evidence_event_id: Some(evidence_event_id),
        settled_by: verlet::IngressSettledBy::Recovery,
    };
    let claim_value = serde_json::to_value(&claim).unwrap();
    let settle_value = serde_json::to_value(&settle).unwrap();
    let registry = verlet::stream_schema_registry_v1();
    registry
        .validate(
            verlet::EventKind::IoIngressClaimed.payload_schema_id(),
            &claim_value,
        )
        .unwrap();
    registry
        .validate(
            verlet::EventKind::IoIngressSettled.payload_schema_id(),
            &settle_value,
        )
        .unwrap();

    crate::support::assert_json_fixture(
        "contracts/ingress_outcome_protocol_v1.json",
        serde_json::json!({
            "claim": {
                "kind": verlet::EventKind::IoIngressClaimed.as_str(),
                "payload_schema": verlet::EventKind::IoIngressClaimed.payload_schema_id(),
                "payload": claim_value,
            },
            "settle": {
                "kind": verlet::EventKind::IoIngressSettled.as_str(),
                "payload_schema": verlet::EventKind::IoIngressSettled.payload_schema_id(),
                "payload": settle_value,
            }
        }),
    );
}

#[test]
fn stream_schema_v1_contract_matches_fixture() {
    let coordinates = coordinates();
    let stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let source_range = verlet::ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence: verlet::EventSequence::new(1),
        to_sequence: verlet::EventSequence::new(3),
    };
    let retained_tail_range = verlet::ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence: verlet::EventSequence::new(4),
        to_sequence: verlet::EventSequence::new(6),
    };
    let full_compile_range = verlet::ObservationSourceRange {
        stream_id: stream_id.clone(),
        from_sequence: verlet::EventSequence::new(1),
        to_sequence: verlet::EventSequence::new(6),
    };
    let summary_event_id = event_record_id(2);
    let summary_text =
        "Keep the user intent and the published operation result; drop provider scaffolding.";

    let compile = verlet::EventRecord {
        id: event_record_id(1),
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(4),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_000,
        kind: verlet::EventKind::ContextCompileCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_range: Some(source_range.clone()),
            source_ranges: vec![source_range.clone()],
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("naive_assembly/v1".to_string()),
            ..verlet::EventProvenance::default()
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
    let summary = verlet::EventRecord {
        id: summary_event_id,
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(5),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_100,
        kind: verlet::EventKind::ContextSummaryCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_range: Some(source_range.clone()),
            source_ranges: vec![source_range.clone()],
            discharged_by: Some("projection:context-summarizer".to_string()),
            function: Some("context_summary/v1".to_string()),
            ..verlet::EventProvenance::default()
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
    let read_plan_set = verlet::EventRecord {
        id: event_record_id(3),
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(6),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_200,
        kind: verlet::EventKind::ContextReadPlanSet,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_event_ids: vec![summary_event_id],
            source_range: Some(source_range.clone()),
            source_ranges: vec![source_range.clone()],
            discharged_by: Some("controller:context-budget".to_string()),
            function: Some("context_read_plan/v1".to_string()),
            ..verlet::EventProvenance::default()
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
    let compile_after_policy = verlet::EventRecord {
        id: event_record_id(4),
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(7),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_300,
        kind: verlet::EventKind::ContextCompileCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_event_ids: vec![summary_event_id],
            source_range: Some(full_compile_range),
            source_ranges: vec![source_range.clone(), retained_tail_range],
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("read_plan_assembly/v1".to_string()),
            ..verlet::EventProvenance::default()
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
    let branch_selection = verlet::EventRecord {
        id: event_record_id(5),
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(8),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_400,
        kind: verlet::EventKind::ThreadBranchSelected,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance::default(),
        payload: serde_json::to_value(verlet::kernel::history::ThreadBranchSelectedPayload {
            thread_id: coordinates.thread_id,
            selected_entry_id: Some(session_entry_id(2)),
            prior_entry_id: Some(session_entry_id(3)),
        })
        .unwrap(),
    };
    let reload_degraded = verlet::EventRecord {
        id: event_record_id(6),
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(9),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_500,
        kind: verlet::EventKind::ThreadReloadDegraded,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance::default(),
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
    let schema_registry = verlet::stream_schema_registry_v1();
    for record in &records {
        record.validate_stream_record_v1().unwrap();
        verlet::validate_context_payload_schema_v1(record.kind, &record.payload).unwrap();
        schema_registry
            .validate(
                verlet::STREAM_RECORD_SCHEMA_V1,
                &serde_json::to_value(record.to_stream_record_v1()).unwrap(),
            )
            .unwrap();
    }
    let cursors = records
        .iter()
        .map(verlet::EventRecord::cursor_v1)
        .collect::<Vec<_>>();
    for cursor in &cursors {
        cursor.validate_stream_cursor_v1().unwrap();
        schema_registry
            .validate(
                verlet::STREAM_CURSOR_SCHEMA_V1,
                &serde_json::to_value(cursor).unwrap(),
            )
            .unwrap();
    }
    let append_acks = vec![
        verlet::StreamAppendAckV1::from_appended(
            stream_id.clone(),
            &records,
            vec![
                verlet::StreamAckClass::LocalCommitted,
                verlet::StreamAckClass::QueryProjected,
            ],
        )
        .unwrap(),
    ];
    for ack in &append_acks {
        schema_registry
            .validate(
                verlet::STREAM_APPEND_ACK_SCHEMA_V1,
                &serde_json::to_value(ack).unwrap(),
            )
            .unwrap();
    }
    let backend_capabilities = vec![verlet::StreamBackendCapabilitiesV1::sqlite_local(
        "/tmp/verlet/session_history.sqlite3",
    )];
    for capabilities in &backend_capabilities {
        schema_registry
            .validate(
                verlet::STREAM_BACKEND_CAPABILITIES_SCHEMA_V1,
                &serde_json::to_value(capabilities).unwrap(),
            )
            .unwrap();
    }
    let routing = records
        .iter()
        .map(verlet::EventRecord::route_decision_v1)
        .collect::<Vec<_>>();
    for decision in &routing {
        schema_registry
            .validate(
                verlet::STREAM_ROUTING_DECISION_SCHEMA_V1,
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
                .map(verlet::EventRecord::to_stream_record_v1)
                .collect::<Vec<_>>(),
            "routing": routing
        }),
    );
}

#[test]
fn debug_thread_export_v1_contract_matches_fixture() {
    let coordinates = coordinates();
    let stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let submitted = verlet::EventRecord {
        id: event_record_id(20),
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(1),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_020,
        kind: verlet::EventKind::TurnSubmitted,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance::default(),
        payload: serde_json::json!({
            "schema": verlet::EventKind::TurnSubmitted.payload_schema_id(),
            "turn_id": "turn-1",
            "input_text": "export evidence"
        }),
    };
    let bind = verlet::EventRecord {
        id: event_record_id(21),
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(2),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_021,
        kind: verlet::EventKind::ManifestBindCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_event_ids: vec![submitted.id],
            discharged_by: Some("manifest:bind".to_string()),
            function: Some("manifest_bind/v1".to_string()),
            config_hash: Some("sha256:manifest-bind-config".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::ManifestBindCompleted.payload_schema_id(),
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
        "schema": verlet::DEBUG_THREAD_EXPORT_SCHEMA_V1,
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

    verlet::stream_schema_registry_v1()
        .validate(verlet::DEBUG_THREAD_EXPORT_SCHEMA_V1, &bundle)
        .unwrap();
    crate::support::assert_json_fixture("contracts/debug_thread_export_v1.json", bundle);
}

#[test]
fn coupling_template_catalog_v1_contract_matches_fixture() {
    let catalog = verlet::coupling_template_catalog_v1();
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let control_stream_id =
        verlet::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let submitted = verlet::EventRecord {
        id: event_record_id(30),
        stream_id: thread_stream_id.clone(),
        sequence: verlet::EventSequence::new(1),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_030,
        kind: verlet::EventKind::TurnSubmitted,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance::default(),
        payload: serde_json::json!({
            "turn_id": "turn-1",
            "entry_id": "entry-1",
        }),
    };
    let executor = verlet::StdlibCouplingExecutor;
    let task_coupling = std_queue_task_bound_coupling();
    let task_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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

    let task_run = verlet::CouplingRunReceipt {
        coupling_id: verlet::STD_QUEUE_TASK_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        status: verlet::CouplingRunStatus::Completed,
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
        budget_spent: verlet::CouplingBudgetSpent {
            discharge_events: 1,
        },
    };
    let task_run_event = verlet::EventRecord {
        id: event_record_id(32),
        stream_id: control_stream_id.clone(),
        sequence: verlet::EventSequence::new(1),
        coordinates,
        created_at_ms: 1_771_718_400_032,
        kind: verlet::EventKind::CouplingRunCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_event_ids: vec![submitted.id],
            discharged_by: Some(format!("coupling:{}", verlet::STD_QUEUE_TASK_TEMPLATE_ID)),
            function: Some(task_coupling.function_ref.clone()),
            config_hash: Some(task_coupling.config_hash.clone()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::to_value(task_run).unwrap(),
    };

    let callback_coupling = std_queue_completion_callback_bound_coupling();
    let callback_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let compile = verlet::EventRecord {
        id: event_record_id(40),
        stream_id: thread_stream_id.clone(),
        sequence: verlet::EventSequence::new(5),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_040,
        kind: verlet::EventKind::ContextCompileCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_ranges: vec![verlet::ObservationSourceRange {
                stream_id: thread_stream_id.clone(),
                from_sequence: verlet::EventSequence::new(1),
                to_sequence: verlet::EventSequence::new(4),
            }],
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("fixture_compile/v1".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::ContextCompileCompleted.payload_schema_id(),
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
    let executor = verlet::StdlibCouplingExecutor;
    let spill_coupling = std_context_spill_bound_coupling();
    let spill_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        verlet::validate_context_payload_schema_v1(discharge.kind, &discharge.payload).unwrap();
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let compile = verlet::EventRecord {
        id: event_record_id(42),
        stream_id: thread_stream_id.clone(),
        sequence: verlet::EventSequence::new(10),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_042,
        kind: verlet::EventKind::ContextCompileCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_ranges: vec![verlet::ObservationSourceRange {
                stream_id: thread_stream_id.clone(),
                from_sequence: verlet::EventSequence::new(1),
                to_sequence: verlet::EventSequence::new(10),
            }],
            discharged_by: Some("projection:context-compiler".to_string()),
            function: Some("fixture_compile/v1".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::ContextCompileCompleted.payload_schema_id(),
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
    let executor = verlet::StdlibCouplingExecutor;
    let truncate_coupling = std_context_truncate_bound_coupling();
    let truncate_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        verlet::validate_context_payload_schema_v1(discharge.kind, &discharge.payload).unwrap();
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let completed = verlet::EventRecord {
        id: event_record_id(43),
        stream_id: thread_stream_id.clone(),
        sequence: verlet::EventSequence::new(11),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_043,
        kind: verlet::EventKind::TurnCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("runtime:provider-loop".to_string()),
            function: Some("turn_completion/v1".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::TurnCompleted.payload_schema_id(),
            "turn_id": "turn-1",
            "output_text": "The user wants SQLite first, S2 later, and explicit segment maps."
        }),
    };
    let executor = verlet::StdlibCouplingExecutor;
    let summarize_coupling = std_context_summarize_bound_coupling();
    let summarize_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        verlet::validate_context_payload_schema_v1(discharge.kind, &discharge.payload).unwrap();
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let completed = verlet::EventRecord {
        id: event_record_id(45),
        stream_id: thread_stream_id.clone(),
        sequence: verlet::EventSequence::new(6),
        coordinates,
        created_at_ms: 1_771_718_400_045,
        kind: verlet::EventKind::TurnCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("runtime:provider-loop".to_string()),
            function: Some("turn_completion/v1".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::TurnCompleted.payload_schema_id(),
            "turn_id": "turn-1",
            "output_text": "User prefers SQLite first, then S2 as stream backend."
        }),
    };
    let executor = verlet::StdlibCouplingExecutor;
    let memory_coupling = std_memory_extract_bound_coupling();
    let memory_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        verlet::validate_context_payload_schema_v1(discharge.kind, &discharge.payload).unwrap();
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let memory_stream_id =
        verlet::EventStreamId::new(format!("derived:memory:{}", coordinates.thread_id));
    let memory = verlet::EventRecord {
        id: event_record_id(46),
        stream_id: memory_stream_id.clone(),
        sequence: verlet::EventSequence::new(2),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_046,
        kind: verlet::EventKind::ContextSummaryCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            source_event_ids: vec![event_record_id(45)],
            discharged_by: Some(format!(
                "coupling:{}",
                verlet::STD_MEMORY_EXTRACT_TEMPLATE_ID
            )),
            function: Some(format!(
                "op://std-memory-extract/run@sha256:{}",
                "f".repeat(64)
            )),
            config_hash: Some("sha256:memory-extract".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::ContextSummaryCompleted.payload_schema_id(),
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
            "template_id": verlet::STD_MEMORY_EXTRACT_TEMPLATE_ID,
            "memory_kind": "observation"
        }),
    };
    let submitted = verlet::EventRecord {
        id: event_record_id(47),
        stream_id: thread_stream_id,
        sequence: verlet::EventSequence::new(7),
        coordinates,
        created_at_ms: 1_771_718_400_047,
        kind: verlet::EventKind::TurnSubmitted,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance::default(),
        payload: serde_json::json!({
            "schema": verlet::EventKind::TurnSubmitted.payload_schema_id(),
            "turn_id": "turn-2",
            "input_text": "What should we use for V1 stream storage?"
        }),
    };
    let executor = verlet::StdlibCouplingExecutor;
    let recall_coupling = std_memory_recall_bound_coupling();
    let recall_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        verlet::validate_context_payload_schema_v1(discharge.kind, &discharge.payload).unwrap();
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let manifest_bind = verlet::EventRecord {
        id: event_record_id(70),
        stream_id: thread_stream_id.clone(),
        sequence: verlet::EventSequence::new(8),
        coordinates,
        created_at_ms: 1_771_718_400_070,
        kind: verlet::EventKind::ManifestBindCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("manifest:bind".to_string()),
            function: Some("manifest_bind/v1".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::ManifestBindCompleted.payload_schema_id(),
            "agent_ref": "agent://release-verifier@0.1.0",
            "manifest_hash": "sha256:manifest"
        }),
    };
    let executor = verlet::StdlibCouplingExecutor;
    let instruction_coupling = std_prompt_dynamic_instructions_bound_coupling();
    let instruction_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        verlet::validate_context_payload_schema_v1(discharge.kind, &discharge.payload).unwrap();
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let control_stream_id =
        verlet::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let completed = verlet::EventRecord {
        id: event_record_id(72),
        stream_id: thread_stream_id.clone(),
        sequence: verlet::EventSequence::new(9),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_072,
        kind: verlet::EventKind::TurnCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("runtime:provider-loop".to_string()),
            function: Some("turn_completion/v1".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::TurnCompleted.payload_schema_id(),
            "turn_id": "turn-1",
            "output_text": "Need one more clarification turn."
        }),
    };
    let approval = verlet::EventRecord {
        id: event_record_id(73),
        stream_id: control_stream_id.clone(),
        sequence: verlet::EventSequence::new(2),
        coordinates,
        created_at_ms: 1_771_718_400_073,
        kind: verlet::EventKind::ApprovalResolved,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance::default(),
        payload: serde_json::json!({
            "schema": verlet::EventKind::ApprovalResolved.payload_schema_id(),
            "approval_id": "approval-instructions",
            "decision": "approved"
        }),
    };
    let executor = verlet::StdlibCouplingExecutor;
    let continuation_coupling = std_prompt_steer_continuation_bound_coupling();
    let continuation_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        if discharge.kind == verlet::EventKind::ContextReadPlanSet {
            verlet::validate_context_payload_schema_v1(discharge.kind, &discharge.payload).unwrap();
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
        verlet::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let failed = verlet::EventRecord {
        id: event_record_id(50),
        stream_id: control_stream_id.clone(),
        sequence: verlet::EventSequence::new(3),
        coordinates,
        created_at_ms: 1_771_718_400_050,
        kind: verlet::EventKind::CouplingRunFailed,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![verlet::EventStreamId::new("thread:fixture-thread")],
            discharged_by: Some(format!("coupling:{}", verlet::STD_QUEUE_TASK_TEMPLATE_ID)),
            function: Some(format!("op://std-queue-task/run@sha256:{}", "a".repeat(64))),
            config_hash: Some("sha256:queue-task".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "coupling_id": verlet::STD_QUEUE_TASK_TEMPLATE_ID,
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
    let executor = verlet::StdlibCouplingExecutor;
    let deadletter_coupling = std_failure_deadletter_bound_coupling();
    let deadletter_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        verlet::EventStreamId::new(format!("control:{}", coordinates.thread_id));
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
    let executor = verlet::StdlibCouplingExecutor;
    let retry_coupling = std_retry_with_budget_bound_coupling();
    let retry_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        verlet::EventStreamId::new(format!("control:{}", coordinates.thread_id));
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
    let executor = verlet::StdlibCouplingExecutor;
    let schedule_coupling = std_schedule_cron_bound_coupling();
    let schedule_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let control_stream_id =
        verlet::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let child_completed = verlet::EventRecord {
        id: event_record_id(100),
        stream_id: thread_stream_id.clone(),
        sequence: verlet::EventSequence::new(12),
        coordinates: coordinates.clone(),
        created_at_ms: 1_771_718_400_100,
        kind: verlet::EventKind::TurnCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("runtime:child-thread".to_string()),
            function: Some("child_turn_completion/v1".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::TurnCompleted.payload_schema_id(),
            "turn_id": "child-turn-1",
            "parent_thread_id": coordinates.thread_id.to_string(),
            "child_thread_id": thread_id(2).to_string(),
            "status": "completed",
            "output_text": "child finished release evidence collection"
        }),
    };
    let mut spawn_receipt_payload = serde_json::to_value(verlet::CouplingRunReceipt {
        coupling_id: "std::supervisor.spawn".to_string(),
        role: verlet::CouplingRole::Controller,
        status: verlet::CouplingRunStatus::Completed,
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
        budget_spent: verlet::CouplingBudgetSpent {
            discharge_events: 1,
        },
    })
    .unwrap();
    spawn_receipt_payload["parent_thread_id"] =
        serde_json::json!(coordinates.thread_id.to_string());
    spawn_receipt_payload["child_thread_id"] = serde_json::json!(thread_id(2).to_string());
    spawn_receipt_payload["child_turn_id"] = serde_json::json!("child-turn-1");
    let spawn_completed = verlet::EventRecord {
        id: event_record_id(101),
        stream_id: control_stream_id.clone(),
        sequence: verlet::EventSequence::new(13),
        coordinates,
        created_at_ms: 1_771_718_400_101,
        kind: verlet::EventKind::CouplingRunCompleted,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![thread_stream_id.clone()],
            discharged_by: Some("coupling:std::supervisor.spawn".to_string()),
            function: Some(format!(
                "op://std-supervisor-spawn/run@sha256:{}",
                "i".repeat(64)
            )),
            config_hash: Some("sha256:supervisor-spawn".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: spawn_receipt_payload,
    };

    let executor = verlet::StdlibCouplingExecutor;
    let loop_coupling = std_supervisor_child_completion_bound_coupling(serde_json::json!({
        "on_completed": "complete_loop",
        "reason": "child work joined back to supervisor"
    }));
    let loop_result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
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
    let executor = verlet::StdlibCouplingExecutor;
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
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
    let thread_stream_id = verlet::EventStreamId::for_thread(&coordinates);
    let request = tool_call_requested_event(
        coordinates,
        thread_stream_id.clone(),
        event_record_id(92),
        11,
        "call-approval",
    );
    let executor = verlet::StdlibCouplingExecutor;
    let coupling = std_permission_approval_gate_bound_coupling(serde_json::json!({
        "approval_id": "approval-shell-call",
        "reason": "operator approval required",
        "resume_token": "resume-shell-call"
    }));
    let result = executor
        .invoke(verlet::CouplingInvocation {
            activation: verlet::CouplingActivation {
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
        "pre_tool_request": verlet::HookRequest::PreToolUse(verlet::PreToolUseHookRequest {
            turn_context: snapshot.clone(),
            call_id: "call_1".to_string(),
            tool_name: "echo_search".to_string(),
            arguments: serde_json::json!({"input":"original"}),
        }),
        "post_tool_request": verlet::HookRequest::PostToolUse(verlet::PostToolUseHookRequest {
            turn_context: snapshot,
            call_id: "call_1".to_string(),
            tool_name: "echo_search".to_string(),
            arguments: serde_json::json!({"input":"rewritten"}),
            output: "echo:rewritten".to_string(),
            success: true,
        }),
        "handler_output": verlet::HookHandlerOutput {
            updated_input: Some(serde_json::json!({"input":"rewritten"})),
            additional_context: Some("hook context".to_string()),
            feedback: Some("feedback context".to_string()),
            replacement_output: Some("replacement".to_string()),
            ..verlet::HookHandlerOutput::default()
        },
        "run_record": verlet::HookRunRecord {
            hook_id: "pre-echo".to_string(),
            event_name: verlet::HookEventName::PreToolUse,
            matcher: Some("echo_search".to_string()),
            status: verlet::HookRunStatus::Completed,
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
    let signal = verlet::ThreadSignal {
        id: signal_id(1),
        coordinates: coordinates.clone(),
        kind: verlet::ThreadSignalKind::UserSteer,
        metadata: std::collections::BTreeMap::from([("turn_id".to_string(), "turn-2".to_string())]),
        created_at_ms: 100,
    };
    let checkpoint = verlet::ThreadCheckpoint {
        id: checkpoint_id(1),
        coordinates: coordinates.clone(),
        lineage: verlet::ThreadCheckpointLineage::Root,
        parent_checkpoint_id: None,
        active_entry_id: Some(session_entry_id(1)),
        label: Some("after-tool".to_string()),
        metadata: std::collections::BTreeMap::from([(
            "source".to_string(),
            "contract".to_string(),
        )]),
        created_at_ms: 200,
    };
    let lifecycle = verlet::ThreadLifecycleRecord {
        coordinates: coordinates.clone(),
        parent_thread_id: Some(thread_id(9)),
        topology: verlet::ThreadTopology::spawned_from(thread_id(9)),
        status: verlet::ThreadLifecycleStatus::Idle,
        latest_signal_id: Some(signal_id(1)),
        latest_checkpoint_id: Some(checkpoint_id(1)),
        created_at_ms: 1,
        updated_at_ms: 201,
        metadata: std::collections::BTreeMap::from([(
            "tenant_home".to_string(),
            "/tmp/tenant".to_string(),
        )]),
    };
    let receipt = verlet::AgentProcessSubmitReceipt {
        operation: "submit_to_thread".to_string(),
        caller_thread_id: thread_id(9),
        target_thread_id: thread_id(1),
        interaction_id: runtime_event_id(2),
        status: verlet::ThreadStatus::Running,
        turn_id: "turn-2".to_string(),
        dispatch_id: verlet_runtime_contracts::DispatchId::new("submit-dispatch-2"),
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
    let openai_responses = verlet::OpenAIResponsesAdapter::default()
        .build_request_body(&provider_request(
            verlet::ProviderApi::OpenAIResponses,
            "gpt-fixture",
        ))
        .unwrap();
    let openai_chat = verlet::OpenAIChatCompletionsAdapter
        .build_request_body(&provider_request(
            verlet::ProviderApi::OpenAIChatCompletions,
            "gpt-chat-fixture",
        ))
        .unwrap();
    let anthropic = verlet::AnthropicMessagesAdapter
        .build_request_body(&provider_request(
            verlet::ProviderApi::AnthropicMessages,
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
    let manifest = verlet::WasmOperationManifest {
        abi: "cooldis_0.1".to_string(),
        operations: vec![verlet::WasmOperationDefinition {
            id: 7,
            name: "search".to_string(),
            input: verlet::WasmOperationValueKind::Json,
            output: verlet::WasmOperationValueKind::Json,
            events: verlet::WasmOperationEventKind::Jsonl,
            mode: verlet::WasmOperationMode::Sync,
            required_capabilities: vec!["net:https://api.example.test".to_string()],
        }],
    };
    let operation = verlet::RegisteredOperation {
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
    let output = verlet::VerletProcessOutput {
        stdout: b"{\"ok\":true}\n".to_vec(),
        stderr: b"{\"level\":\"info\"}\n".to_vec(),
        terminal: Some(verlet::VerletProcessTerminalState::Completed {
            status: verlet::VerletProcessExitStatus::success(),
        }),
        stdout_truncated: false,
        stderr_truncated: false,
        artifacts: vec![verlet::VerletProcessArtifact {
            artifact_id: "artifact_1".to_string(),
            path: Some(std::path::PathBuf::from("/workspace/out.json")),
            mime_type: Some("application/json".to_string()),
        }],
        file_deltas: vec![verlet::VerletProcessFileDelta {
            kind: verlet::FileDeltaKind::Write,
            path: std::path::PathBuf::from("/workspace/out.json"),
            target: None,
        }],
    };
    let operation_events = vec![
        verlet::OperationEvent::Started { operation_id },
        verlet::OperationEvent::Log {
            operation_id,
            level: verlet::OperationLogLevel::Info,
            message: "operation ready".to_string(),
        },
        verlet::OperationEvent::FileDelta {
            operation_id,
            kind: verlet::FileDeltaKind::Write,
            path: std::path::PathBuf::from("/workspace/out.json"),
            target: None,
        },
        verlet::OperationEvent::Completed {
            operation_id,
            status: verlet::OperationExitStatus::exited(0),
        },
    ];
    let mut unix_exec = verlet::UnixExecPayload::new("verlet run search search", "/workspace");
    unix_exec = unix_exec.with_mode(verlet::UnixExecutionMode::VirtualOnly);

    crate::support::assert_json_fixture(
        "contracts/abi_process.json",
        serde_json::json!({
            "bridge_backend_kind": verlet::BridgeBackendKind::InProcess,
            "manifest": manifest,
            "operation_events": operation_events,
            "projection": verlet::OperationProjectionSet::from_registered(&operation),
            "process_output": output,
            "unix_exec_payload": unix_exec,
        }),
    );
}

fn coupling_template_maturity_label(maturity: verlet::CouplingTemplateMaturity) -> &'static str {
    match maturity {
        verlet::CouplingTemplateMaturity::KernelBacked => "kernel_backed",
        verlet::CouplingTemplateMaturity::InterfaceOnly => "interface_only",
        verlet::CouplingTemplateMaturity::ReferenceOnly => "reference_only",
    }
}

fn std_queue_task_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_QUEUE_TASK_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::TurnSubmitted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet::EventKind::TurnSubmitted],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet::EventKind::TurnWaiting],
        },
        function_ref: format!("op://std-queue-task/run@sha256:{}", "a".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-queue-task".to_string(),
            artifact_hash: "a".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({}),
        config_hash: "sha256:queue-task".to_string(),
    }
}

fn std_queue_completion_callback_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::CouplingRunCompleted,
        trigger_match: std::collections::BTreeMap::from([(
            "coupling_id".to_string(),
            serde_json::json!(verlet::STD_QUEUE_TASK_TEMPLATE_ID),
        )]),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![verlet::EventKind::CouplingRunCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet::EventKind::LoopCompleted],
        },
        function_ref: format!(
            "op://std-queue-completion-callback/run@sha256:{}",
            "b".repeat(64)
        ),
        function: verlet::BoundCouplingFunction {
            name: "std-queue-completion-callback".to_string(),
            artifact_hash: "b".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "watch_coupling_id": verlet::STD_QUEUE_TASK_TEMPLATE_ID,
            "on_completed": "complete_loop",
        }),
        config_hash: "sha256:queue-callback".to_string(),
    }
}

fn std_context_spill_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_CONTEXT_SPILL_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Projection,
        trigger_kind: verlet::EventKind::ContextCompileCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet::EventKind::ContextCompileCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![
                verlet::EventKind::ContextSummaryCompleted,
                verlet::EventKind::ContextReadPlanSet,
            ],
        },
        function_ref: format!("op://std-context-spill/run@sha256:{}", "c".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-context-spill".to_string(),
            artifact_hash: "c".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
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

fn std_context_truncate_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_CONTEXT_TRUNCATE_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::ContextCompileCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet::EventKind::ContextCompileCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet::EventKind::ContextReadPlanSet],
        },
        function_ref: format!("op://std-context-truncate/run@sha256:{}", "d".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-context-truncate".to_string(),
            artifact_hash: "d".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
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

fn std_context_summarize_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_CONTEXT_SUMMARIZE_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Projection,
        trigger_kind: verlet::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![
                verlet::EventKind::SessionEntryAppended,
                verlet::EventKind::TurnCompleted,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![
                verlet::EventKind::ContextSummaryCompleted,
                verlet::EventKind::ContextReadPlanSet,
            ],
        },
        function_ref: format!("op://std-context-summarize/run@sha256:{}", "e".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-context-summarize".to_string(),
            artifact_hash: "e".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(2),
        },
        config: serde_json::json!({
            "summary_event_id": event_record_id(44).to_string(),
        }),
        config_hash: "sha256:context-summarize".to_string(),
    }
}

fn std_memory_extract_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_MEMORY_EXTRACT_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Projection,
        trigger_kind: verlet::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![
                verlet::EventKind::TurnCompleted,
                verlet::EventKind::ToolCallCompleted,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "derived:memory".to_string(),
            kinds: vec![verlet::EventKind::ContextSummaryCompleted],
        },
        function_ref: format!("op://std-memory-extract/run@sha256:{}", "f".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-memory-extract".to_string(),
            artifact_hash: "f".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:memory".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({}),
        config_hash: "sha256:memory-extract".to_string(),
    }
}

fn std_memory_recall_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_MEMORY_RECALL_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Projection,
        trigger_kind: verlet::EventKind::TurnSubmitted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "derived:memory".to_string(),
            kinds: vec![verlet::EventKind::ContextSummaryCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![verlet::EventKind::ContextReadPlanSet],
        },
        function_ref: format!("op://std-memory-recall/run@sha256:{}", "g".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-memory-recall".to_string(),
            artifact_hash: "g".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:derived:memory".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({}),
        config_hash: "sha256:memory-recall".to_string(),
    }
}

fn std_prompt_steer_continuation_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_PROMPT_STEER_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet::EventKind::TurnCompleted],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet::EventKind::TurnContinueRequested],
        },
        function_ref: format!("op://std-prompt-steer/run@sha256:{}", "h".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-prompt-steer".to_string(),
            artifact_hash: "h".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
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

fn std_prompt_steer_read_plan_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_PROMPT_STEER_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::ApprovalResolved,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![verlet::EventKind::ApprovalResolved],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![verlet::EventKind::ContextReadPlanSet],
        },
        function_ref: format!("op://std-prompt-steer/run@sha256:{}", "h".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-prompt-steer".to_string(),
            artifact_hash: "h".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
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

fn std_prompt_dynamic_instructions_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Projection,
        trigger_kind: verlet::EventKind::ManifestBindCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![
                verlet::EventKind::ManifestBindCompleted,
                verlet::EventKind::ContextCompileCompleted,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![
                verlet::EventKind::ContextSummaryCompleted,
                verlet::EventKind::ContextReadPlanSet,
            ],
        },
        function_ref: format!(
            "op://std-prompt-dynamic-instructions/run@sha256:{}",
            "h".repeat(64)
        ),
        function: verlet::BoundCouplingFunction {
            name: "std-prompt-dynamic-instructions".to_string(),
            artifact_hash: "h".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
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

fn std_permission_tool_gate_bound_coupling(config: serde_json::Value) -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_PERMISSION_TOOL_GATE_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::ToolCallRequested,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet::EventKind::ToolCallRequested],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet::EventKind::ToolCallDecision,
                verlet::EventKind::ToolCallSuspended,
            ],
        },
        function_ref: format!(
            "op://std-permission-tool-gate/run@sha256:{}",
            "p".repeat(64)
        ),
        function: verlet::BoundCouplingFunction {
            name: "std-permission-tool-gate".to_string(),
            artifact_hash: "p".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config,
        config_hash: "sha256:permission-tool-gate".to_string(),
    }
}

fn std_permission_approval_gate_bound_coupling(config: serde_json::Value) -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::ToolCallRequested,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![verlet::EventKind::ToolCallRequested],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet::EventKind::ApprovalRequested,
                verlet::EventKind::ToolCallSuspended,
            ],
        },
        function_ref: format!(
            "op://std-permission-approval-gate/run@sha256:{}",
            "a".repeat(64)
        ),
        function: verlet::BoundCouplingFunction {
            name: "std-permission-approval-gate".to_string(),
            artifact_hash: "a".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(2),
        },
        config,
        config_hash: "sha256:permission-approval-gate".to_string(),
    }
}

fn std_failure_deadletter_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_FAILURE_DEADLETTER_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Projection,
        trigger_kind: verlet::EventKind::CouplingRunFailed,
        trigger_match: std::collections::BTreeMap::from([(
            "status".to_string(),
            serde_json::json!("failed"),
        )]),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![
                verlet::EventKind::CouplingRunFailed,
                verlet::EventKind::LoopBlocked,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "derived:deadletter".to_string(),
            kinds: vec![verlet::EventKind::CouplingRunFailed],
        },
        function_ref: format!("op://std-failure-deadletter/run@sha256:{}", "d".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-failure-deadletter".to_string(),
            artifact_hash: "d".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:derived:deadletter".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config: serde_json::json!({
            "reason": "deadletter failed control facts for inspection",
        }),
        config_hash: "sha256:failure-deadletter".to_string(),
    }
}

fn std_retry_with_budget_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_RETRY_WITH_BUDGET_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::CouplingRunFailed,
        trigger_match: std::collections::BTreeMap::from([(
            "status".to_string(),
            serde_json::json!("failed"),
        )]),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![verlet::EventKind::CouplingRunFailed],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet::EventKind::TurnContinueRequested,
                verlet::EventKind::LoopBudgetExhausted,
            ],
        },
        function_ref: format!("op://std-retry-with-budget/run@sha256:{}", "e".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-retry-with-budget".to_string(),
            artifact_hash: "e".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
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

fn std_schedule_cron_bound_coupling() -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_SCHEDULE_CRON_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::TimerFired,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![
                verlet::EventKind::MandateStarted,
                verlet::EventKind::MandateRevoked,
                verlet::EventKind::TimerFired,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet::EventKind::TurnContinueRequested,
                verlet::EventKind::LoopBudgetExhausted,
            ],
        },
        function_ref: format!("op://std-schedule-cron/run@sha256:{}", "s".repeat(64)),
        function: verlet::BoundCouplingFunction {
            name: "std-schedule-cron".to_string(),
            artifact_hash: "s".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
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
) -> verlet::BoundCoupling {
    verlet::BoundCoupling {
        id: verlet::STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID.to_string(),
        role: verlet::CouplingRole::Controller,
        trigger_kind: verlet::EventKind::TurnCompleted,
        trigger_match: std::collections::BTreeMap::new(),
        trigger_quota: verlet::AgentManifestCouplingQuota::default(),
        source_selectors: vec![verlet::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![
                verlet::EventKind::TurnCompleted,
                verlet::EventKind::CouplingRunCompleted,
            ],
            scope: None,
            since: None,
        }],
        sink: verlet::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                verlet::EventKind::TurnContinueRequested,
                verlet::EventKind::LoopCompleted,
            ],
        },
        function_ref: format!(
            "op://std-supervisor-child-completion/run@sha256:{}",
            "j".repeat(64)
        ),
        function: verlet::BoundCouplingFunction {
            name: "std-supervisor-child-completion".to_string(),
            artifact_hash: "j".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: verlet::AgentManifestCouplingBudget {
            max_ms: None,
            max_discharge_events: Some(1),
        },
        config,
        config_hash: "sha256:supervisor-child-completion".to_string(),
    }
}

fn failed_coupling_run_event(
    coordinates: verlet::ThreadCoordinates,
    stream_id: verlet::EventStreamId,
    id: verlet::EventRecordId,
    sequence: i64,
    fields: serde_json::Value,
) -> verlet::EventRecord {
    let mut payload = serde_json::json!({
        "coupling_id": verlet::STD_QUEUE_TASK_TEMPLATE_ID,
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
    verlet::EventRecord {
        id,
        stream_id,
        sequence: verlet::EventSequence::new(sequence),
        coordinates,
        created_at_ms: 1_771_718_400_060 + sequence,
        kind: verlet::EventKind::CouplingRunFailed,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![verlet::EventStreamId::new("thread:fixture-thread")],
            discharged_by: Some(format!("coupling:{}", verlet::STD_QUEUE_TASK_TEMPLATE_ID)),
            function: Some(format!("op://std-queue-task/run@sha256:{}", "a".repeat(64))),
            config_hash: Some("sha256:queue-task".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload,
    }
}

fn tool_call_requested_event(
    coordinates: verlet::ThreadCoordinates,
    stream_id: verlet::EventStreamId,
    id: verlet::EventRecordId,
    sequence: i64,
    call_id: &str,
) -> verlet::EventRecord {
    verlet::EventRecord {
        id,
        stream_id,
        sequence: verlet::EventSequence::new(sequence),
        coordinates,
        created_at_ms: 1_771_718_400_090 + sequence,
        kind: verlet::EventKind::ToolCallRequested,
        origin: verlet::EventOrigin::Discharged,
        provenance: verlet::EventProvenance {
            source_streams: vec![verlet::EventStreamId::new("thread:fixture-thread")],
            discharged_by: Some("runtime:provider-loop".to_string()),
            function: Some("provider_tool_request/v1".to_string()),
            ..verlet::EventProvenance::default()
        },
        payload: serde_json::json!({
            "schema": verlet::EventKind::ToolCallRequested.payload_schema_id(),
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
    coordinates: verlet::ThreadCoordinates,
    stream_id: verlet::EventStreamId,
    id: verlet::EventRecordId,
    sequence: i64,
) -> verlet::EventRecord {
    let thread_id = coordinates.thread_id.to_string();
    verlet::EventRecord {
        id,
        stream_id,
        sequence: verlet::EventSequence::new(sequence),
        coordinates,
        created_at_ms: 1_771_718_400_080 + sequence,
        kind: verlet::EventKind::MandateStarted,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance::default(),
        payload: serde_json::json!({
            "schema": verlet::EventKind::MandateStarted.payload_schema_id(),
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
    coordinates: verlet::ThreadCoordinates,
    stream_id: verlet::EventStreamId,
    id: verlet::EventRecordId,
    sequence: i64,
    mandate_event_id: verlet::EventRecordId,
    occurrence_index: u64,
    scheduled_for: &str,
) -> verlet::EventRecord {
    verlet::EventRecord {
        id,
        stream_id: stream_id.clone(),
        sequence: verlet::EventSequence::new(sequence),
        coordinates,
        created_at_ms: 1_771_718_400_080 + sequence,
        kind: verlet::EventKind::TimerFired,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance {
            source_streams: vec![stream_id],
            source_event_ids: vec![mandate_event_id],
            ..verlet::EventProvenance::default()
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
    stream_id: &verlet::EventStreamId,
    max_sequence: i64,
) -> verlet::CouplingSourceCut {
    verlet::CouplingSourceCut {
        entries: vec![verlet::CouplingSourceCutEntry {
            stream_id: stream_id.to_string(),
            max_sequence,
        }],
    }
}

fn discharges_json(discharges: &[verlet::CouplingDischarge]) -> Vec<serde_json::Value> {
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

fn provider_request(api: verlet::ProviderApi, model: &str) -> verlet::ProviderRequest {
    verlet::ProviderRequest {
        api,
        provider: "fixture-provider".to_string(),
        model: model.to_string(),
        system: vec![verlet::SystemBlock::text("Be precise.")],
        messages: vec![
            verlet::CanonicalMessage::user_text("hello"),
            verlet::CanonicalMessage::assistant(
                "fixture-provider",
                verlet::ProviderApi::Other("fixture".to_string()),
                model,
                vec![
                    verlet::CanonicalContent::text("thinking done"),
                    verlet::CanonicalContent::tool_call(
                        "call_1|fc_1",
                        "search",
                        serde_json::json!({"query":"verlet"}),
                    ),
                ],
                verlet::CanonicalStopReason::ToolUse,
            ),
            verlet::CanonicalMessage::tool_result("call_1|fc_1", "search", "result", false),
        ],
        tools: vec![verlet::ToolDefinition::new(
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

fn turn_snapshot() -> verlet::TurnContextSnapshot {
    verlet::TurnContextSnapshot {
        turn_id: "turn-1".to_string(),
        trace_id: "trace-1".to_string(),
        coordinates: coordinates(),
        parent_thread_id: Some(thread_id(9)),
        topology: verlet::ThreadTopology::spawned_from(thread_id(9)),
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
        budget: verlet::TurnBudget {
            max_tool_rounds: Some(4),
            max_output_tokens: Some(128),
            max_context_text_bytes: Some(2048),
        },
        cancellation_requested: false,
    }
}

fn coordinates() -> verlet::ThreadCoordinates {
    verlet::ThreadCoordinates {
        tenant_id: "tenant_a".to_string(),
        user_id: "user_1".to_string(),
        session_id: "session_1".to_string(),
        thread_id: thread_id(1),
    }
}

fn thread_id(n: u128) -> verlet::ThreadId {
    verlet::ThreadId::parse_str(&format!("018f0000-0000-7000-8000-{n:012x}")).unwrap()
}

fn checkpoint_id(n: u128) -> verlet::ThreadCheckpointId {
    verlet::ThreadCheckpointId::from_uuid(uuid(n))
}

fn signal_id(n: u128) -> verlet::ThreadSignalId {
    verlet::ThreadSignalId::from_uuid(uuid(n))
}

fn runtime_event_id(n: u128) -> verlet::RuntimeEventId {
    verlet::RuntimeEventId::from_uuid(uuid(n))
}

fn event_record_id(n: u128) -> verlet::EventRecordId {
    verlet::EventRecordId::from_uuid(uuid(n))
}

fn session_entry_id(n: u128) -> verlet::SessionEntryId {
    verlet::SessionEntryId::from_uuid(uuid(n))
}

fn operation_id(n: u128) -> verlet::OperationId {
    verlet::OperationId::from_uuid(uuid(n))
}

fn uuid(n: u128) -> uuid::Uuid {
    uuid::Uuid::parse_str(&format!("018f0000-0000-7000-8000-{n:012x}")).unwrap()
}
