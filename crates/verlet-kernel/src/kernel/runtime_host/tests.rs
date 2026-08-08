use crate::kernel::history::EventStore as _;
use crate::kernel::history::SessionStore as _;
use chrono::TimeZone as _;
#[test]
fn thread_topology_distinguishes_spawn_attribution_from_branch_lineage() {
    let source_thread_id = crate::kernel::runtime_host::ThreadId::new();
    let checkpoint_id = crate::kernel::runtime_host::ThreadCheckpointId::new();

    let spawned = crate::kernel::runtime_host::ThreadTopology::spawned_from(source_thread_id);
    assert_eq!(
        spawned.initiation,
        crate::kernel::runtime_host::ThreadInitiationSource::Thread {
            thread_id: source_thread_id,
            turn_id: None,
            event_id: None,
        }
    );
    assert_eq!(
        spawned.lineage,
        crate::kernel::runtime_host::ThreadLineage::Root
    );
    assert_eq!(
        spawned
            .spawn_attribution
            .as_ref()
            .map(|attribution| attribution.source_thread_id),
        Some(source_thread_id)
    );
    assert_eq!(
        spawned.compatibility_parent_thread_id(),
        Some(source_thread_id)
    );
    assert_eq!(spawned.spawn_source_thread_id(), Some(source_thread_id));
    assert_eq!(spawned.branch_parent_thread_id(), None);
    assert_eq!(spawned.controller_thread_id(), Some(source_thread_id));

    let branched = crate::kernel::runtime_host::ThreadTopology::branch_from(
        source_thread_id,
        Some(checkpoint_id),
    );
    assert_eq!(
        branched.initiation,
        crate::kernel::runtime_host::ThreadInitiationSource::Root
    );
    assert_eq!(
        branched.lineage,
        crate::kernel::runtime_host::ThreadLineage::Branch {
            parent_thread_id: source_thread_id,
            checkpoint_id: Some(checkpoint_id),
        }
    );
    assert_eq!(branched.spawn_attribution, None);
    assert_eq!(
        branched.compatibility_parent_thread_id(),
        Some(source_thread_id)
    );
    assert_eq!(branched.spawn_source_thread_id(), None);
    assert_eq!(branched.branch_parent_thread_id(), Some(source_thread_id));
    assert_eq!(branched.controller_thread_id(), None);
}

#[test]
fn turn_context_snapshot_carries_stable_turn_identity_and_cancellation() {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let input = crate::kernel::runtime_host::TurnInput::text("hello")
        .with_cwd("/tmp/verlet-turn")
        .with_workspace_root("/workspace")
        .with_model("gpt-test")
        .with_provider("openai")
        .with_permission_profile("workspace-write")
        .with_provider_metadata("region", "us")
        .with_metadata("source", "unit-test");
    let coordinates =
        crate::kernel::runtime_host::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let context = crate::kernel::runtime_host::TurnContext::new(
        crate::kernel::runtime_host::ThreadContext::root(coordinates.clone()),
        "turn-1",
        &input,
        cancellation.clone(),
    )
    .with_budget(crate::kernel::runtime_host::TurnBudget {
        max_tool_rounds: Some(8),
        max_output_tokens: Some(128),
        max_context_text_bytes: Some(4096),
    })
    .add_model_visible_context("hook context");

    let snapshot = context.snapshot();
    assert_eq!(snapshot.turn_id, "turn-1");
    assert_eq!(snapshot.coordinates, coordinates);
    assert_eq!(snapshot.model.as_deref(), Some("gpt-test"));
    assert_eq!(snapshot.provider.as_deref(), Some("openai"));
    assert_eq!(
        snapshot.permission_profile.as_deref(),
        Some("workspace-write")
    );
    assert_eq!(
        snapshot.provider_metadata.get("region").map(String::as_str),
        Some("us")
    );
    assert_eq!(
        snapshot.metadata.get("source").map(String::as_str),
        Some("unit-test")
    );
    assert_eq!(snapshot.model_visible_context, vec!["hook context"]);
    assert_eq!(snapshot.budget.max_tool_rounds, Some(8));
    assert!(!snapshot.cancellation_requested);

    cancellation.cancel();
    assert!(context.snapshot().cancellation_requested);
}

#[tokio::test]
async fn runtime_services_schedule_bound_stdlib_context_spill_on_context_compile() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default(),
    )
    .with_bound_coupling_set(crate::agent::manifest_bind::BoundCouplingSet::new(
        "snapshot-a",
        vec![runtime_std_context_spill_coupling()],
    ));
    let coordinates =
        crate::kernel::runtime_host::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let session_entry = services
        .append_user_message(&coordinates, "hello")
        .await
        .unwrap();

    services
        .record_context_compile_receipt(
            &coordinates,
            &[session_entry],
            serde_json::json!({
                "strategy": "naive_assembly",
                "output_hash": "sha256:test",
                "truncated_text_bytes": 640
            }),
        )
        .await
        .unwrap();

    let derived_stream = crate::kernel::history::EventStreamId::new(format!(
        "derived:context:{}",
        coordinates.thread_id
    ));
    let derived_events = store.read_events(&derived_stream, None).await.unwrap();
    let summary = derived_events
        .iter()
        .find(|event| event.kind == crate::kernel::history::EventKind::ContextSummaryCompleted)
        .unwrap();
    let read_plan = derived_events
        .iter()
        .find(|event| event.kind == crate::kernel::history::EventKind::ContextReadPlanSet)
        .unwrap();
    assert_eq!(
        summary.provenance.discharged_by.as_deref(),
        Some("coupling:std::context.spill")
    );
    assert_eq!(
        read_plan.payload["summary_event_id"],
        summary.id.to_string()
    );
    assert_eq!(
        read_plan.payload["read_plan"]["entries"][0]["event_id"],
        summary.id.to_string()
    );

    let control_stream =
        crate::kernel::history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let control_events = store.read_events(&control_stream, None).await.unwrap();
    let receipt = control_events
        .iter()
        .find(|event| event.kind == crate::kernel::history::EventKind::CouplingRunCompleted)
        .unwrap();
    assert_eq!(receipt.payload["coupling_id"], "std::context.spill");
    assert_eq!(receipt.payload["status"], "completed");
    assert_eq!(
        receipt.payload["discharged_event_ids"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn context_compile_receipt_event_carries_discharged_provenance() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default(),
    );
    let coordinates =
        crate::kernel::runtime_host::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let session_entry = services
        .append_user_message(&coordinates, "hello")
        .await
        .unwrap();
    let stream_id = crate::kernel::history::EventStreamId::for_thread(&coordinates);
    let payload = serde_json::json!({
        "strategy": "naive_assembly",
        "output_hash": "sha256:test",
    });

    let observation = services
        .record_context_compile_receipt(
            &coordinates,
            std::slice::from_ref(&session_entry),
            payload.clone(),
        )
        .await
        .unwrap();

    let events = store.read_events(&stream_id, None).await.unwrap();
    let compile_event = events
        .iter()
        .find(|event| event.kind == crate::kernel::history::EventKind::ContextCompileCompleted)
        .unwrap();
    assert_eq!(
        compile_event.origin,
        crate::kernel::history::EventOrigin::Discharged
    );
    assert_eq!(compile_event.payload["strategy"], payload["strategy"]);
    assert_eq!(compile_event.payload["output_hash"], payload["output_hash"]);
    assert_eq!(
        compile_event.payload["schema"],
        "cooldis.event.context.compile.completed/1"
    );
    assert_eq!(
        compile_event.payload["read_plan"]["schema"],
        "cooldis.context.read_plan/1"
    );
    assert_eq!(
        compile_event.payload["read_plan"]["name"],
        "history.default"
    );
    assert_eq!(
        compile_event.payload["read_plan"]["source_stream"],
        stream_id.as_str()
    );
    assert!(
        compile_event.payload["read_plan"]["entries"]
            .as_array()
            .is_some_and(|entries| !entries.is_empty())
    );
    assert_eq!(
        compile_event.provenance.source_streams,
        vec![stream_id.clone()]
    );
    assert!(compile_event.provenance.source_event_ids.is_empty());
    assert_eq!(
        compile_event.provenance.discharged_by.as_deref(),
        Some("projection:context-compiler")
    );
    assert_eq!(
        compile_event.provenance.function.as_deref(),
        Some("naive_assembly/v1")
    );
    let source_range = compile_event.provenance.source_range.clone().unwrap();
    assert_eq!(source_range.stream_id, stream_id);
    assert_eq!(source_range.from_sequence.get(), 1);
    assert_eq!(source_range.to_sequence.get(), 1);

    assert_eq!(
        observation.provenance.source_event_ids,
        vec![compile_event.id]
    );
    assert_eq!(
        observation.provenance.source_range,
        Some(source_range.clone())
    );
    assert_eq!(observation.provenance.derivation_strategy, "naive_assembly");
    assert_eq!(observation.provenance.derivation_version, "v1");
}

#[tokio::test]
async fn manifest_bind_receipts_emit_policy_bound_for_bound_coupling_set() {
    let first =
        policy_bound_content_hash_for_config(serde_json::json!({"pattern": "rm -rf"})).await;
    let same = policy_bound_content_hash_for_config(serde_json::json!({"pattern": "rm -rf"})).await;
    let edited = policy_bound_content_hash_for_config(serde_json::json!({"pattern": "curl"})).await;

    assert_eq!(first, same);
    assert_ne!(first, edited);
}

#[tokio::test]
async fn context_compile_receipt_carries_borrowed_prefix_source_ranges() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let services = crate::kernel::runtime_host::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default(),
    );
    let parent =
        crate::kernel::runtime_host::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let child =
        crate::kernel::runtime_host::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let parent_entry = services
        .append_user_message(&parent, "parent")
        .await
        .unwrap();
    let parent_stream = crate::kernel::history::EventStreamId::for_thread(&parent);
    let child_stream = crate::kernel::history::EventStreamId::for_thread(&child);
    store
        .fork_by_reference(
            &parent,
            &child,
            crate::kernel::history::ThreadBaseRef {
                child_thread_id: child.thread_id,
                parent_thread_id: parent.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(parent_entry.entry_id),
                parent_stream_id: parent_stream.clone(),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: crate::kernel::history::ThreadForkReason::ManifestUpdate,
                created_at_ms: crate::kernel::history::now_ms(),
            },
        )
        .await
        .unwrap();
    let child_entry = services.append_user_message(&child, "child").await.unwrap();
    let context = services.build_session_context(&child).await.unwrap();

    services
        .record_context_compile_receipt_with_source_cuts(
            &child,
            &context.entries,
            &context.source_cuts,
            serde_json::json!({"strategy": "naive_assembly", "output_hash": "sha256:test"}),
        )
        .await
        .unwrap();

    let events = store.read_events(&child_stream, None).await.unwrap();
    let compile_event = events
        .iter()
        .find(|event| event.kind == crate::kernel::history::EventKind::ContextCompileCompleted)
        .unwrap();
    assert_eq!(
        compile_event.provenance.source_streams,
        vec![parent_stream.clone(), child_stream.clone()]
    );
    assert_eq!(
        compile_event.payload["read_plan"]["schema"],
        "cooldis.context.read_plan/1"
    );
    assert_eq!(
        compile_event.payload["read_plan"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(compile_event.provenance.source_ranges.len(), 2);
    assert!(compile_event.provenance.source_ranges.iter().any(|range| {
        range.stream_id == parent_stream
            && range.from_sequence.get() == 1
            && range.to_sequence.get() == 1
    }));
    assert!(compile_event.provenance.source_ranges.iter().any(|range| {
        range.stream_id == child_stream
            && range.from_sequence.get() == 1
            && range.to_sequence.get() == 1
    }));
    assert_eq!(
        compile_event
            .provenance
            .source_range
            .as_ref()
            .unwrap()
            .stream_id,
        child_stream
    );
    assert_eq!(
        context.source_cuts[0].entry_ids,
        vec![parent_entry.entry_id]
    );
    assert_eq!(context.source_cuts[1].entry_ids, vec![child_entry.entry_id]);
}

fn runtime_std_context_spill_coupling() -> crate::agent::manifest_bind::BoundCoupling {
    crate::agent::manifest_bind::BoundCoupling {
        id: "std::context.spill".to_string(),
        role: crate::agent::manifest_bind::CouplingRole::Projection,
        trigger_kind: crate::kernel::history::EventKind::ContextCompileCompleted,
        trigger_match: Default::default(),
        trigger_quota: crate::agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![crate::kernel::history::EventKind::ContextCompileCompleted],
            scope: None,
            since: None,
        }],
        sink: crate::agent::manifest_bind::BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![
                crate::kernel::history::EventKind::ContextSummaryCompleted,
                crate::kernel::history::EventKind::ContextReadPlanSet,
            ],
        },
        function_ref: format!("op://std-context-spill/run@sha256:{}", "c".repeat(64)),
        function: crate::agent::manifest_bind::BoundCouplingFunction {
            name: "std-context-spill".to_string(),
            artifact_hash: "c".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: crate::agent::manifest_schema::AgentManifestCouplingBudget {
            max_discharge_events: Some(2),
            max_ms: None,
        },
        config: serde_json::json!({}),
        config_hash: "sha256:context-spill".to_string(),
    }
}

fn runtime_std_schedule_cron_timer_coupling() -> crate::agent::manifest_bind::BoundCoupling {
    crate::agent::manifest_bind::BoundCoupling {
        id: "std::schedule.cron".to_string(),
        role: crate::agent::manifest_bind::CouplingRole::Controller,
        trigger_kind: crate::kernel::history::EventKind::TimerFired,
        trigger_match: Default::default(),
        trigger_quota: crate::agent::manifest_schema::AgentManifestCouplingQuota::default(),
        source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![
                crate::kernel::history::EventKind::MandateStarted,
                crate::kernel::history::EventKind::MandateRevoked,
                crate::kernel::history::EventKind::TimerFired,
            ],
            scope: None,
            since: None,
        }],
        sink: crate::agent::manifest_bind::BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                crate::kernel::history::EventKind::TurnContinueRequested,
                crate::kernel::history::EventKind::LoopBudgetExhausted,
            ],
        },
        function_ref: format!("op://std-schedule-cron/run@sha256:{}", "s".repeat(64)),
        function: crate::agent::manifest_bind::BoundCouplingFunction {
            name: "std-schedule-cron".to_string(),
            artifact_hash: "s".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: crate::agent::manifest_schema::AgentManifestCouplingBudget {
            max_discharge_events: Some(1),
            max_ms: None,
        },
        config: serde_json::json!({
            "max_occurrences": 2,
            "mandate_scope": "match_all",
            "schedule_id": "nightly-summary",
            "loop_id": "loop-nightly",
            "parent_turn_id": "turn-nightly-root",
        }),
        config_hash: "sha256:schedule-cron".to_string(),
    }
}

struct EchoRuntimeFactory;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for EchoRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(EchoRuntime))
    }
}

struct EchoRuntime;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for EchoRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        services: crate::kernel::runtime_host::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Started { context });
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Stopped);
                    let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(crate::kernel::runtime_host::ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Running);
                            if let Ok(entry) = services.append_user_turn_input(&coordinates, &turn_id, &input).await {
                                let _ = events.send(crate::kernel::runtime_host::ThreadEvent::CanonicalMirror { thread_id, entry });
                            }
                            let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Output {
                                thread_id,
                                text: format!("{turn_id}:{}", input.text_projection()),
                            });
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::ThreadCommand::Cancel { reason }) => {
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Cancelling);
                            let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Signal {
                                thread_id,
                                signal: crate::kernel::runtime_host::ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::ThreadCommand::CancelTurn { .. }) => {}
                        Some(crate::kernel::runtime_host::ThreadCommand::Compact { .. }) => {
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::ThreadCommand::Shutdown) | None => {
                            let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Signal {
                                thread_id,
                                signal: crate::kernel::runtime_host::ThreadSignal::shutdown(&coordinates),
                            });
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Stopped);
                            let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct AdmissionTestStore {
    inner: crate::kernel::history::InMemorySessionStore,
    admission_barrier: Option<std::sync::Arc<AdmissionAppendBarrier>>,
    manifest_barrier: Option<std::sync::Arc<AdmissionAppendBarrier>>,
    select_branch_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl AdmissionTestStore {
    fn blocking(admission_barrier: std::sync::Arc<AdmissionAppendBarrier>) -> Self {
        Self {
            inner: crate::kernel::history::InMemorySessionStore::new(),
            admission_barrier: Some(admission_barrier),
            manifest_barrier: None,
            select_branch_calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn blocking_manifest(manifest_barrier: std::sync::Arc<AdmissionAppendBarrier>) -> Self {
        Self {
            inner: crate::kernel::history::InMemorySessionStore::new(),
            admission_barrier: None,
            manifest_barrier: Some(manifest_barrier),
            select_branch_calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn tracking_selects() -> Self {
        Self {
            inner: crate::kernel::history::InMemorySessionStore::new(),
            admission_barrier: None,
            manifest_barrier: None,
            select_branch_calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl crate::kernel::history::SessionStore for AdmissionTestStore {
    async fn append(
        &self,
        coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
        parent_entry_id: Option<crate::kernel::history::SessionEntryId>,
        kind: crate::kernel::history::SessionEntryKind,
    ) -> crate::kernel::history::HistoryResult<crate::kernel::history::SessionEntry> {
        self.inner.append(coordinates, parent_entry_id, kind).await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
        parent_entry_id: Option<crate::kernel::history::SessionEntryId>,
        kind: crate::kernel::history::SessionEntryKind,
        provenance: crate::kernel::history::EventProvenance,
    ) -> crate::kernel::history::HistoryResult<crate::kernel::history::SessionEntry> {
        self.inner
            .append_with_provenance(coordinates, parent_entry_id, kind, provenance)
            .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
        turn_id: &str,
        kind: crate::kernel::history::SessionEntryKind,
    ) -> crate::kernel::history::HistoryResult<crate::kernel::history::SessionEntry> {
        self.inner
            .append_turn_input(coordinates, turn_id, kind)
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
    ) -> crate::kernel::history::HistoryResult<Option<crate::kernel::history::SessionEntryId>> {
        self.inner.active_leaf(coordinates).await
    }

    async fn select_branch(
        &self,
        coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
        leaf_entry_id: Option<crate::kernel::history::SessionEntryId>,
    ) -> crate::kernel::history::HistoryResult<()> {
        self.select_branch_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.select_branch(coordinates, leaf_entry_id).await
    }

    async fn build_context(
        &self,
        coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
    ) -> crate::kernel::history::HistoryResult<crate::kernel::history::SessionContext> {
        self.inner.build_context(coordinates).await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
        source_leaf: Option<crate::kernel::history::SessionEntryId>,
        target_coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
    ) -> crate::kernel::history::HistoryResult<Option<crate::kernel::history::SessionEntryId>> {
        self.inner
            .clone_branch(source_coordinates, source_leaf, target_coordinates)
            .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
        target_coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
        base: crate::kernel::history::ThreadBaseRef,
    ) -> crate::kernel::history::HistoryResult<()> {
        self.inner
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
    }
}

#[async_trait::async_trait]
impl crate::kernel::history::EventStore for AdmissionTestStore {
    async fn append_events(
        &self,
        stream_id: &crate::kernel::history::EventStreamId,
        records: Vec<crate::kernel::history::NewEventRecord>,
    ) -> crate::kernel::history::HistoryResult<Vec<crate::kernel::history::EventRecord>> {
        let appends_admission = records
            .iter()
            .any(|record| record.kind == crate::kernel::history::EventKind::AdmissionDecided);
        if appends_admission && let Some(barrier) = &self.admission_barrier {
            barrier.arrive_and_wait().await;
        }
        let appends_manifest_bind = records
            .iter()
            .any(|record| record.kind == crate::kernel::history::EventKind::ManifestBindCompleted);
        if appends_manifest_bind && let Some(barrier) = &self.manifest_barrier {
            barrier.arrive_and_wait().await;
        }
        self.inner.append_events(stream_id, records).await
    }

    async fn read_events(
        &self,
        stream_id: &crate::kernel::history::EventStreamId,
        from_sequence: Option<crate::kernel::history::EventSequence>,
    ) -> crate::kernel::history::HistoryResult<Vec<crate::kernel::history::EventRecord>> {
        self.inner.read_events(stream_id, from_sequence).await
    }
}

#[async_trait::async_trait]
// lexicon-allow: observation_store - test wrapper must implement the history observation trait
impl crate::kernel::history::ObservationStore for AdmissionTestStore {
    async fn append_observation(
        &self,
        record: crate::kernel::history::NewObservationRecord,
    ) -> crate::kernel::history::HistoryResult<crate::kernel::history::ObservationRecord> {
        self.inner.append_observation(record).await
    }

    async fn list_observations(
        &self,
        scope: &crate::kernel::runtime_host::ThreadCoordinates,
        kind: Option<&str>,
    ) -> crate::kernel::history::HistoryResult<Vec<crate::kernel::history::ObservationRecord>> {
        self.inner.list_observations(scope, kind).await
    }
}

#[derive(Default)]
struct AdmissionAppendBarrier {
    entered: std::sync::atomic::AtomicUsize,
    entered_notify: tokio::sync::Notify,
    released: std::sync::atomic::AtomicBool,
    release_notify: tokio::sync::Notify,
}

impl AdmissionAppendBarrier {
    async fn arrive_and_wait(&self) {
        let release = self.release_notify.notified();
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered_notify.notify_waiters();
        if !self.released.load(std::sync::atomic::Ordering::SeqCst) {
            release.await;
        }
    }

    async fn wait_for_entries(&self, expected: usize) {
        loop {
            let entered = self.entered_notify.notified();
            if self.entered.load(std::sync::atomic::Ordering::SeqCst) >= expected {
                return;
            }
            entered.await;
        }
    }

    fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

struct AssistantHistoryRuntimeFactory;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for AssistantHistoryRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(AssistantHistoryRuntime))
    }
}

struct AssistantHistoryRuntime;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for AssistantHistoryRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        services: crate::kernel::runtime_host::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Started { context });
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Stopped);
                    let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(crate::kernel::runtime_host::ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Running);
                            if let Ok(entry) = services.append_user_turn_input(&coordinates, &turn_id, &input).await {
                                let _ = events.send(crate::kernel::runtime_host::ThreadEvent::CanonicalMirror { thread_id, entry });
                            }
                            let output = format!("{turn_id}:{}", input.text_projection());
                            let _ = services.append_session_entry(
                                &coordinates,
                                None,
                                crate::kernel::history::SessionEntryKind::Message {
                                    message: crate::kernel::history::CanonicalMessage::assistant(
                                        "test",
                                        crate::kernel::history::ProviderApi::Other("test".to_string()),
                                        "test-model",
                                        vec![crate::kernel::history::CanonicalContent::text(output.clone())],
                                        crate::kernel::history::CanonicalStopReason::EndTurn,
                                    ),
                                },
                            ).await;
                            let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Output {
                                thread_id,
                                text: output,
                            });
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::ThreadCommand::Cancel { reason }) => {
                            let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::ThreadCommand::CancelTurn { .. }) => {}
                        Some(crate::kernel::runtime_host::ThreadCommand::Compact { .. }) => {
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
                        }
                        Some(crate::kernel::runtime_host::ThreadCommand::Shutdown) | None => {
                            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Stopped);
                            let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

struct InspectTurnInputRuntimeFactory;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for InspectTurnInputRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(InspectTurnInputRuntime))
    }
}

struct InspectTurnInputRuntime;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for InspectTurnInputRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        _services: crate::kernel::runtime_host::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        _cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
        while let Some(command) = commands.recv().await {
            match command {
                crate::kernel::runtime_host::ThreadCommand::Submit { input, .. } => {
                    let mut parts = Vec::new();
                    parts.push(format!("text={}", input.text_projection()));
                    parts.push(format!(
                        "cwd={}",
                        input
                            .cwd
                            .as_ref()
                            .map(|path| path.display().to_string())
                            .unwrap_or_default()
                    ));
                    parts.push(format!("roots={}", input.workspace_roots.len()));
                    parts.push(format!(
                        "permission={}",
                        input.permission_profile.as_deref().unwrap_or_default()
                    ));
                    parts.push(format!(
                        "model={}",
                        input.model.as_deref().unwrap_or_default()
                    ));
                    parts.push(format!(
                        "provider={}",
                        input.provider.as_deref().unwrap_or_default()
                    ));
                    parts.push(format!(
                        "provider_request_id={}",
                        input
                            .provider_metadata
                            .get("request_id")
                            .map(String::as_str)
                            .unwrap_or_default()
                    ));
                    for content in &input.content {
                        match content {
                            crate::kernel::runtime_host::TurnContent::Image {
                                mime_type, ..
                            } => {
                                parts.push(format!("image={mime_type}"));
                            }
                            crate::kernel::runtime_host::TurnContent::FileRef {
                                path,
                                mime_type,
                                size_bytes,
                                sha256,
                                ..
                            } => {
                                parts.push(format!(
                                    "file={}:{}:{}:{}",
                                    path.display(),
                                    mime_type.as_deref().unwrap_or_default(),
                                    size_bytes.unwrap_or_default(),
                                    sha256.as_deref().unwrap_or_default()
                                ));
                            }
                            crate::kernel::runtime_host::TurnContent::Text { .. } => {}
                        }
                    }
                    let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Output {
                        thread_id,
                        text: parts.join("|"),
                    });
                }
                crate::kernel::runtime_host::ThreadCommand::Cancel { .. } => {}
                crate::kernel::runtime_host::ThreadCommand::CancelTurn { .. } => {}
                crate::kernel::runtime_host::ThreadCommand::Compact { .. } => {}
                crate::kernel::runtime_host::ThreadCommand::ResumeToolCall { .. } => {}
                crate::kernel::runtime_host::ThreadCommand::Shutdown => break,
            }
        }
    }
}

struct StuckRuntimeFactory;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for StuckRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(StuckRuntime))
    }
}

struct StuckRuntime;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for StuckRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        services: crate::kernel::runtime_host::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        _cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Started { context });
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
        if let Some(crate::kernel::runtime_host::ThreadCommand::Submit { turn_id, input, .. }) =
            commands.recv().await
        {
            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Running);
            if let Ok(entry) = services
                .append_user_turn_input(&coordinates, &turn_id, &input)
                .await
            {
                let _ = events.send(crate::kernel::runtime_host::ThreadEvent::CanonicalMirror {
                    thread_id,
                    entry,
                });
            }
            std::future::pending::<()>().await;
        }
    }
}

#[derive(Default)]
struct GatedTurnRuntimeState {
    first_started: tokio::sync::Notify,
    release_first: tokio::sync::Notify,
    second_started: tokio::sync::Notify,
    release_second: tokio::sync::Notify,
}

struct GatedTurnRuntimeFactory {
    state: std::sync::Arc<GatedTurnRuntimeState>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for GatedTurnRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(GatedTurnRuntime {
            state: std::sync::Arc::clone(&self.state),
        }))
    }
}

struct GatedTurnRuntime {
    state: std::sync::Arc<GatedTurnRuntimeState>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for GatedTurnRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        services: crate::kernel::runtime_host::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        _cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Started { context });
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);

        for (expected_turn_id, started, release) in [
            (
                "turn-a",
                &self.state.first_started,
                &self.state.release_first,
            ),
            (
                "turn-b",
                &self.state.second_started,
                &self.state.release_second,
            ),
        ] {
            let Some(crate::kernel::runtime_host::ThreadCommand::Submit { turn_id, input, .. }) =
                commands.recv().await
            else {
                return;
            };
            assert_eq!(turn_id, expected_turn_id);
            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Running);
            if let Ok(entry) = services
                .append_user_turn_input(&coordinates, &turn_id, &input)
                .await
            {
                let _ = events.send(crate::kernel::runtime_host::ThreadEvent::CanonicalMirror {
                    thread_id,
                    entry,
                });
            }
            started.notify_one();
            release.notified().await;
            drop(input);
            let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
        }
    }
}

#[derive(Default)]
struct WatchdogHandoffState {
    first_started: tokio::sync::Notify,
    release_first: tokio::sync::Notify,
    second_started: tokio::sync::Notify,
    stale_cancel_applied: std::sync::atomic::AtomicBool,
    stale_cancel_observed: tokio::sync::Notify,
}

struct WatchdogHandoffRuntimeFactory {
    state: std::sync::Arc<WatchdogHandoffState>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for WatchdogHandoffRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(WatchdogHandoffRuntime {
            state: std::sync::Arc::clone(&self.state),
        }))
    }
}

struct WatchdogHandoffRuntime {
    state: std::sync::Arc<WatchdogHandoffState>,
}

impl WatchdogHandoffState {
    async fn wait_for_stale_cancel(&self) {
        loop {
            let observed = self.stale_cancel_observed.notified();
            if self
                .stale_cancel_applied
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return;
            }
            observed.await;
        }
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for WatchdogHandoffRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        services: crate::kernel::runtime_host::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        _events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        _cancellation: tokio_util::sync::CancellationToken,
    ) {
        let coordinates = context.coordinates;
        let Some(crate::kernel::runtime_host::ThreadCommand::Submit { turn_id, input, .. }) =
            commands.recv().await
        else {
            return;
        };
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Running);
        let _ = services
            .append_user_turn_input(&coordinates, &turn_id, &input)
            .await;
        self.state.first_started.notify_one();
        self.state.release_first.notified().await;
        drop(input);
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);

        let Some(crate::kernel::runtime_host::ThreadCommand::Submit { turn_id, input, .. }) =
            commands.recv().await
        else {
            return;
        };
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Running);
        let _ = services
            .append_user_turn_input(&coordinates, &turn_id, &input)
            .await;
        self.state.second_started.notify_one();

        if let Some(crate::kernel::runtime_host::ThreadCommand::Cancel { .. }) =
            commands.recv().await
        {
            self.state
                .stale_cancel_applied
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.state.stale_cancel_observed.notify_waiters();
        }
    }
}

#[derive(Default)]
struct DrainedPendingInputState {
    first_started: tokio::sync::Notify,
    queued_input_drained: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

struct DrainedPendingInputRuntimeFactory {
    state: std::sync::Arc<DrainedPendingInputState>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for DrainedPendingInputRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(DrainedPendingInputRuntime {
            state: std::sync::Arc::clone(&self.state),
        }))
    }
}

struct DrainedPendingInputRuntime {
    state: std::sync::Arc<DrainedPendingInputState>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for DrainedPendingInputRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        services: crate::kernel::runtime_host::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        _events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        _cancellation: tokio_util::sync::CancellationToken,
    ) {
        let coordinates = context.coordinates;
        let Some(crate::kernel::runtime_host::ThreadCommand::Submit { turn_id, input, .. }) =
            commands.recv().await
        else {
            return;
        };
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Running);
        let _ = services
            .append_user_turn_input(&coordinates, &turn_id, &input)
            .await;
        self.state.first_started.notify_one();

        let queued_input = match commands.recv().await {
            Some(crate::kernel::runtime_host::ThreadCommand::Submit { input, .. }) => input,
            _ => return,
        };
        self.state.queued_input_drained.notify_one();
        self.state.release.notified().await;
        drop(queued_input);
    }
}

#[derive(Default)]
struct GatedShutdownFactory {
    builds: std::sync::atomic::AtomicUsize,
    shutdown_received: std::sync::Arc<tokio::sync::Notify>,
    release_shutdown: std::sync::Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for GatedShutdownFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        if self
            .builds
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            Ok(Box::new(GatedShutdownRuntime {
                shutdown_received: std::sync::Arc::clone(&self.shutdown_received),
                release_shutdown: std::sync::Arc::clone(&self.release_shutdown),
            }))
        } else {
            Ok(Box::new(EchoRuntime))
        }
    }
}

struct GatedShutdownRuntime {
    shutdown_received: std::sync::Arc<tokio::sync::Notify>,
    release_shutdown: std::sync::Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for GatedShutdownRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        _services: crate::kernel::runtime_host::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        _events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        _cancellation: tokio_util::sync::CancellationToken,
    ) {
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
        while let Some(command) = commands.recv().await {
            if matches!(
                command,
                crate::kernel::runtime_host::ThreadCommand::Shutdown
            ) {
                self.shutdown_received.notify_one();
                self.release_shutdown.notified().await;
                let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Stopped);
                return;
            }
        }
        let _ = context;
    }
}

#[derive(Default)]
struct ControlledChildBuildFactory {
    child_builds: std::sync::atomic::AtomicUsize,
    child_build_notify: tokio::sync::Notify,
    released: std::sync::atomic::AtomicBool,
    release_notify: tokio::sync::Notify,
    child_runtime_starts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ControlledChildBuildFactory {
    async fn wait_for_child_builds(&self, expected: usize) {
        loop {
            let entered = self.child_build_notify.notified();
            if self.child_builds.load(std::sync::atomic::Ordering::SeqCst) >= expected {
                return;
            }
            entered.await;
        }
    }

    fn release_builds(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for ControlledChildBuildFactory {
    async fn build(
        &self,
        context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        let is_child = context.parent_thread_id.is_some();
        if is_child {
            let release = self.release_notify.notified();
            self.child_builds
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.child_build_notify.notify_waiters();
            if !self.released.load(std::sync::atomic::Ordering::SeqCst) {
                release.await;
            }
        }
        Ok(Box::new(CountingEchoRuntime {
            starts: is_child.then(|| std::sync::Arc::clone(&self.child_runtime_starts)),
        }))
    }
}

struct BlockingThreadBuildFactory {
    target_thread_id: crate::kernel::runtime_host::ThreadId,
    entered: std::sync::atomic::AtomicBool,
    entered_notify: tokio::sync::Notify,
    released: std::sync::atomic::AtomicBool,
    release_notify: tokio::sync::Notify,
}

impl BlockingThreadBuildFactory {
    fn new(target_thread_id: crate::kernel::runtime_host::ThreadId) -> Self {
        Self {
            target_thread_id,
            entered: std::sync::atomic::AtomicBool::new(false),
            entered_notify: tokio::sync::Notify::new(),
            released: std::sync::atomic::AtomicBool::new(false),
            release_notify: tokio::sync::Notify::new(),
        }
    }

    async fn wait_until_blocked(&self) {
        loop {
            let entered = self.entered_notify.notified();
            if self.entered.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            entered.await;
        }
    }

    fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for BlockingThreadBuildFactory {
    async fn build(
        &self,
        context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        if context.coordinates.thread_id == self.target_thread_id {
            let release = self.release_notify.notified();
            self.entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.entered_notify.notify_waiters();
            if !self.released.load(std::sync::atomic::Ordering::SeqCst) {
                release.await;
            }
        }
        Ok(Box::new(EchoRuntime))
    }
}

struct CountingEchoRuntime {
    starts: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for CountingEchoRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        services: crate::kernel::runtime_host::RuntimeServices,
        commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        if let Some(starts) = &self.starts {
            starts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Box::new(EchoRuntime)
            .run(context, services, commands, events, status, cancellation)
            .await;
    }
}

#[derive(Default)]
struct FailFirstChildBuildFactory {
    failed: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for FailFirstChildBuildFactory {
    async fn build(
        &self,
        context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        if context.parent_thread_id.is_some()
            && !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "controlled child build failure".to_string(),
            ));
        }
        Ok(Box::new(EchoRuntime))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistrationProbeOutcome {
    Pending = 0,
    DroppedWithoutRun = 1,
    ObservedThreadNotFound = 2,
    ObservedRegistered = 3,
    ObservedOtherError = 4,
}

#[derive(Default)]
struct RegistrationProbeFactory {
    build_completed: std::sync::atomic::AtomicBool,
    build_notify: tokio::sync::Notify,
    outcome: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    outcome_notify: std::sync::Arc<tokio::sync::Notify>,
}

impl RegistrationProbeFactory {
    async fn wait_for_build(&self) {
        loop {
            let completed = self.build_notify.notified();
            if self
                .build_completed
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return;
            }
            completed.await;
        }
    }

    async fn wait_for_outcome(&self) -> RegistrationProbeOutcome {
        loop {
            let completed = self.outcome_notify.notified();
            match self.outcome.load(std::sync::atomic::Ordering::SeqCst) {
                0 => completed.await,
                1 => return RegistrationProbeOutcome::DroppedWithoutRun,
                2 => return RegistrationProbeOutcome::ObservedThreadNotFound,
                3 => return RegistrationProbeOutcome::ObservedRegistered,
                4 => return RegistrationProbeOutcome::ObservedOtherError,
                outcome => panic!("unexpected registration probe outcome {outcome}"),
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for RegistrationProbeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        self.build_completed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.build_notify.notify_waiters();
        Ok(Box::new(RegistrationProbeRuntime {
            outcome: std::sync::Arc::clone(&self.outcome),
            outcome_notify: std::sync::Arc::clone(&self.outcome_notify),
        }))
    }
}

struct RegistrationProbeRuntime {
    outcome: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    outcome_notify: std::sync::Arc<tokio::sync::Notify>,
}

impl RegistrationProbeRuntime {
    fn record(&self, outcome: RegistrationProbeOutcome) {
        self.outcome
            .store(outcome as usize, std::sync::atomic::Ordering::SeqCst);
        self.outcome_notify.notify_waiters();
    }
}

impl Drop for RegistrationProbeRuntime {
    fn drop(&mut self) {
        if self
            .outcome
            .compare_exchange(
                RegistrationProbeOutcome::Pending as usize,
                RegistrationProbeOutcome::DroppedWithoutRun as usize,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
        {
            self.outcome_notify.notify_waiters();
        }
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for RegistrationProbeRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        services: crate::kernel::runtime_host::RuntimeServices,
        commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let outcome = match services
            .kernel_control()
            .expect("runtime host supplies kernel control")
            .thread_status(&context, context.coordinates.thread_id)
            .await
        {
            Ok(_) => RegistrationProbeOutcome::ObservedRegistered,
            Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(_)) => {
                RegistrationProbeOutcome::ObservedThreadNotFound
            }
            Err(_) => RegistrationProbeOutcome::ObservedOtherError,
        };
        self.record(outcome);
        Box::new(EchoRuntime)
            .run(context, services, commands, events, status, cancellation)
            .await;
    }
}

#[derive(Default)]
struct CancellationTrackedFactory {
    active_runs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    runtime_started: std::sync::Arc<tokio::sync::Notify>,
    runtime_stopped: std::sync::Arc<tokio::sync::Notify>,
}

impl CancellationTrackedFactory {
    async fn wait_until_stopped(&self) {
        loop {
            let stopped = self.runtime_stopped.notified();
            if self.active_runs.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                return;
            }
            stopped.await;
        }
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for CancellationTrackedFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(CancellationTrackedRuntime {
            active_runs: std::sync::Arc::clone(&self.active_runs),
            runtime_started: std::sync::Arc::clone(&self.runtime_started),
            runtime_stopped: std::sync::Arc::clone(&self.runtime_stopped),
        }))
    }
}

struct CancellationTrackedRuntime {
    active_runs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    runtime_started: std::sync::Arc<tokio::sync::Notify>,
    runtime_stopped: std::sync::Arc<tokio::sync::Notify>,
}

struct ActiveRunGuard {
    active_runs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    runtime_stopped: std::sync::Arc<tokio::sync::Notify>,
}

impl Drop for ActiveRunGuard {
    fn drop(&mut self) {
        self.active_runs
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.runtime_stopped.notify_waiters();
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for CancellationTrackedRuntime {
    async fn run(
        self: Box<Self>,
        _context: crate::kernel::runtime_host::ThreadContext,
        _services: crate::kernel::runtime_host::RuntimeServices,
        _commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        _events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        _status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        self.active_runs
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _guard = ActiveRunGuard {
            active_runs: std::sync::Arc::clone(&self.active_runs),
            runtime_stopped: std::sync::Arc::clone(&self.runtime_stopped),
        };
        self.runtime_started.notify_waiters();
        cancellation.cancelled().await;
    }
}

struct BlockingLifecycleSink {
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::ThreadLifecycleSink for BlockingLifecycleSink {
    async fn thread_started(
        &self,
        _handle: crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

struct FailingAfterRuntimeStartsSink {
    active_runs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    runtime_started: std::sync::Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::ThreadLifecycleSink for FailingAfterRuntimeStartsSink {
    async fn thread_started(
        &self,
        _handle: crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        loop {
            let started = self.runtime_started.notified();
            if self.active_runs.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                break;
            }
            started.await;
        }
        Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "controlled lifecycle sink failure".to_string(),
        ))
    }
}

struct HistoryFailingChildLifecycleSink {
    child_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::ThreadLifecycleSink for HistoryFailingChildLifecycleSink {
    async fn thread_started(
        &self,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if handle.context().parent_thread_id.is_some() {
            self.child_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(crate::kernel::runtime_host::VerletError::History(
                "controlled child lifecycle history failure".to_string(),
            ));
        }
        Ok(())
    }
}

struct ExitRuntimeFactory;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntimeFactory for ExitRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::kernel::runtime_host::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<Box<dyn crate::kernel::runtime_host::AgentRuntime>>
    {
        Ok(Box::new(ExitRuntime))
    }
}

struct ExitRuntime;

#[async_trait::async_trait]
impl crate::kernel::runtime_host::AgentRuntime for ExitRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::kernel::runtime_host::ThreadContext,
        _services: crate::kernel::runtime_host::RuntimeServices,
        _commands: tokio::sync::mpsc::Receiver<crate::kernel::runtime_host::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::kernel::runtime_host::ThreadStatus>,
        _cancellation: tokio_util::sync::CancellationToken,
    ) {
        let _ = events.send(crate::kernel::runtime_host::ThreadEvent::Started { context });
        let _ = status.send(crate::kernel::runtime_host::ThreadStatus::Idle);
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}

fn coords(
    tenant: &str,
    user: &str,
    session: &str,
) -> crate::kernel::runtime_host::ThreadCoordinates {
    crate::kernel::runtime_host::ThreadCoordinates::new(tenant, user, session)
}

#[test]
fn runtime_event_kind_serializes_stable_shapes() {
    let checkpoint_id = crate::kernel::runtime_host::ThreadCheckpointId::new();
    let child_thread_id = crate::kernel::runtime_host::ThreadId::new();
    let interaction_id = crate::kernel::runtime_host::RuntimeEventId::new();
    let source_thread_id = crate::kernel::runtime_host::ThreadId::new();
    let cases = vec![
        (
            crate::kernel::runtime_host::RuntimeEventKind::ThreadInteraction {
                interaction_id,
                kind: crate::kernel::runtime_host::ThreadInteractionKind::PromptSubmitted,
                source_thread_id,
                target_thread_id: child_thread_id,
                source_turn_id: None,
                target_turn_id: Some("turn-2".to_string()),
                result_preview: None,
                metadata: std::collections::BTreeMap::from([(
                    "operation".to_string(),
                    "cooldis.submit_to_thread".to_string(),
                )]),
            },
            serde_json::json!({"type":"thread_interaction","interaction_id":interaction_id.to_string(),"kind":"prompt_submitted","source_thread_id":source_thread_id.to_string(),"target_thread_id":child_thread_id.to_string(),"target_turn_id":"turn-2","metadata":{"operation":"cooldis.submit_to_thread"}}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::TextDelta {
                text: "hello".to_string(),
            },
            serde_json::json!({"type":"text_delta","text":"hello"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ThinkingDelta {
                text: "plan".to_string(),
            },
            serde_json::json!({"type":"thinking_delta","text":"plan"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ToolCallStarted {
                call_id: "call_1".to_string(),
                name: "bash".to_string(),
                input: serde_json::json!({"command":"pwd"}),
            },
            serde_json::json!({"type":"tool_call_started","call_id":"call_1","name":"bash","input":{"command":"pwd"}}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ToolCallResult {
                call_id: "call_1".to_string(),
                output: "ok".to_string(),
                success: true,
                duration_ms: None,
            },
            serde_json::json!({"type":"tool_call_result","call_id":"call_1","output":"ok","success":true}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ToolCallResult {
                call_id: "call_2".to_string(),
                output: "ok".to_string(),
                success: true,
                duration_ms: Some(17),
            },
            serde_json::json!({"type":"tool_call_result","call_id":"call_2","output":"ok","success":true,"duration_ms":17}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ToolLog {
                call_id: "call_2".to_string(),
                tool_name: "bash".to_string(),
                level: crate::kernel::runtime_host::RuntimeToolLogLevel::Info,
                message: "tool completed".to_string(),
                metadata: std::collections::BTreeMap::from([(
                    "duration_ms".to_string(),
                    "17".to_string(),
                )]),
            },
            serde_json::json!({"type":"tool_log","call_id":"call_2","tool_name":"bash","level":"info","message":"tool completed","metadata":{"duration_ms":"17"}}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::HookStarted {
                hook_id: "pre-echo".to_string(),
                event_name: crate::agent::hooks::HookEventName::PreToolUse,
                matcher: Some("echo_search".to_string()),
            },
            serde_json::json!({"type":"hook_started","hook_id":"pre-echo","event_name":"pre_tool_use","matcher":"echo_search"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::HookCompleted {
                hook_id: "pre-echo".to_string(),
                event_name: crate::agent::hooks::HookEventName::PreToolUse,
                status: crate::agent::hooks::HookRunStatus::Completed,
                duration_ms: 12,
                message: None,
            },
            serde_json::json!({"type":"hook_completed","hook_id":"pre-echo","event_name":"pre_tool_use","status":"completed","duration_ms":12}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ApprovalRequested {
                approval_id: "approval_1".to_string(),
                action: "write_file".to_string(),
                metadata: std::collections::BTreeMap::from([(
                    "path".to_string(),
                    "/workspace/a".to_string(),
                )]),
            },
            serde_json::json!({"type":"approval_requested","approval_id":"approval_1","action":"write_file","metadata":{"path":"/workspace/a"}}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ApprovalResolved {
                approval_id: "approval_1".to_string(),
                decision: crate::kernel::runtime_host::RuntimeApprovalDecision::Approved,
                reason: None,
            },
            serde_json::json!({"type":"approval_resolved","approval_id":"approval_1","decision":"approved"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::PermissionDecision {
                call_id: "call_1".to_string(),
                tool_name: "bash".to_string(),
                decision: crate::kernel::runtime_host::RuntimePermissionDecision::Deny,
                reason: Some("policy denied".to_string()),
            },
            serde_json::json!({"type":"permission_decision","call_id":"call_1","tool_name":"bash","decision":"deny","reason":"policy denied"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ContextCompiled {
                diagnostics: crate::kernel::context_compiler::AgentContextCompilationDiagnostics {
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
            serde_json::json!({"type":"context_compiled","diagnostics":{"input_entry_count":2,"output_message_count":1,"system_block_count":1,"tool_count":1,"attachment_count":0,"retained_text_bytes":11,"truncated_text_bytes":4},"provider_dropped_messages":1,"provider_truncated_text_bytes":2,"provider_retained_text_bytes":9}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ModelRequestStarted {
                request_id: "req_1".to_string(),
                turn_id: "turn-1".to_string(),
                provider: "openai".to_string(),
                api: "openai_responses".to_string(),
                model: "gpt-test".to_string(),
                mode: crate::kernel::runtime_host::RuntimeModelRequestMode::Complete,
                purpose: crate::kernel::runtime_host::RuntimeModelRequestPurpose::Turn,
                system_block_count: 1,
                message_count: 2,
                tool_count: 3,
                max_tokens: 128,
            },
            serde_json::json!({"type":"model_request_started","request_id":"req_1","turn_id":"turn-1","provider":"openai","api":"openai_responses","model":"gpt-test","mode":"complete","purpose":"turn","system_block_count":1,"message_count":2,"tool_count":3,"max_tokens":128}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ModelRequestCompleted {
                request_id: "req_1".to_string(),
                turn_id: "turn-1".to_string(),
                provider: "openai".to_string(),
                api: "openai_responses".to_string(),
                model: "gpt-test".to_string(),
                mode: crate::kernel::runtime_host::RuntimeModelRequestMode::Complete,
                purpose: crate::kernel::runtime_host::RuntimeModelRequestPurpose::Turn,
                duration_ms: 25,
                usage: crate::kernel::runtime_host::RuntimeUsage {
                    input_tokens: 1,
                    output_tokens: 2,
                    cache_creation_input_tokens: 3,
                    cache_read_input_tokens: 4,
                },
                stop_reason: crate::kernel::history::CanonicalStopReason::EndTurn,
            },
            serde_json::json!({"type":"model_request_completed","request_id":"req_1","turn_id":"turn-1","provider":"openai","api":"openai_responses","model":"gpt-test","mode":"complete","purpose":"turn","duration_ms":25,"usage":{"input_tokens":1,"output_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":4},"stop_reason":"end_turn"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ModelRequestRetryScheduled {
                request_id: "req_1".to_string(),
                next_request_id: "req_1_retry".to_string(),
                turn_id: "turn-1".to_string(),
                provider: "openai".to_string(),
                api: "openai_responses".to_string(),
                model: "gpt-test".to_string(),
                mode: crate::kernel::runtime_host::RuntimeModelRequestMode::Complete,
                purpose: crate::kernel::runtime_host::RuntimeModelRequestPurpose::Turn,
                attempt: 1,
                next_attempt: 2,
                delay_ms: 50,
                error_class:
                    crate::kernel::runtime_host::RuntimeModelRequestErrorClass::RateLimited,
                error: "rate limited".to_string(),
            },
            serde_json::json!({"type":"model_request_retry_scheduled","request_id":"req_1","next_request_id":"req_1_retry","turn_id":"turn-1","provider":"openai","api":"openai_responses","model":"gpt-test","mode":"complete","purpose":"turn","attempt":1,"next_attempt":2,"delay_ms":50,"error_class":"rate_limited","error":"rate limited"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ModelRequestFallbackSelected {
                request_id: "req_1".to_string(),
                turn_id: "turn-1".to_string(),
                from_provider: "openai".to_string(),
                from_api: "openai_responses".to_string(),
                from_model: "gpt-test".to_string(),
                to_provider: "fallback".to_string(),
                to_api: "openai_responses".to_string(),
                to_model: "gpt-fallback".to_string(),
                mode: crate::kernel::runtime_host::RuntimeModelRequestMode::Complete,
                purpose: crate::kernel::runtime_host::RuntimeModelRequestPurpose::Turn,
                error_class: crate::kernel::runtime_host::RuntimeModelRequestErrorClass::Retryable,
                error: "provider down".to_string(),
            },
            serde_json::json!({"type":"model_request_fallback_selected","request_id":"req_1","turn_id":"turn-1","from_provider":"openai","from_api":"openai_responses","from_model":"gpt-test","to_provider":"fallback","to_api":"openai_responses","to_model":"gpt-fallback","mode":"complete","purpose":"turn","error_class":"retryable","error":"provider down"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::ModelRequestFailed {
                request_id: "req_2".to_string(),
                turn_id: "turn-1".to_string(),
                provider: "openai".to_string(),
                api: "openai_responses".to_string(),
                model: "gpt-test".to_string(),
                mode: crate::kernel::runtime_host::RuntimeModelRequestMode::Stream,
                purpose: crate::kernel::runtime_host::RuntimeModelRequestPurpose::Compaction,
                duration_ms: 3,
                error_class: crate::kernel::runtime_host::RuntimeModelRequestErrorClass::Retryable,
                error: "network".to_string(),
            },
            serde_json::json!({"type":"model_request_failed","request_id":"req_2","turn_id":"turn-1","provider":"openai","api":"openai_responses","model":"gpt-test","mode":"stream","purpose":"compaction","duration_ms":3,"error_class":"retryable","error":"network"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Terminal {
                state: crate::kernel::runtime_host::RuntimeTerminalState::Completed,
            },
            serde_json::json!({"type":"terminal","state":"completed"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Terminal {
                state: crate::kernel::runtime_host::RuntimeTerminalState::TimedOut,
            },
            serde_json::json!({"type":"terminal","state":"timed_out"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Timeout {
                operation: "turn".to_string(),
                timeout_ms: 100,
            },
            serde_json::json!({"type":"timeout","operation":"turn","timeout_ms":100}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::PolicyRejected {
                code: "max_pending_inputs".to_string(),
                message: "full".to_string(),
            },
            serde_json::json!({"type":"policy_rejected","code":"max_pending_inputs","message":"full"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Recovery {
                action: "abort_runtime".to_string(),
                reason: "timeout".to_string(),
            },
            serde_json::json!({"type":"recovery","action":"abort_runtime","reason":"timeout"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Usage {
                usage: crate::kernel::runtime_host::RuntimeUsage {
                    input_tokens: 1,
                    output_tokens: 2,
                    cache_creation_input_tokens: 3,
                    cache_read_input_tokens: 4,
                },
            },
            serde_json::json!({"type":"usage","usage":{"input_tokens":1,"output_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":4}}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::SubthreadStarted { child_thread_id },
            serde_json::json!({"type":"subthread_started","child_thread_id":child_thread_id.to_string()}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::SubthreadFinished {
                child_thread_id,
                status: crate::kernel::runtime_host::ThreadLifecycleStatus::Stopped,
            },
            serde_json::json!({"type":"subthread_finished","child_thread_id":child_thread_id.to_string(),"status":"stopped"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Checkpoint {
                checkpoint_id,
                label: Some("label".to_string()),
            },
            serde_json::json!({"type":"checkpoint","checkpoint_id":checkpoint_id.to_string(),"label":"label"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Compaction {
                trigger: crate::CompactionTrigger::Manual,
                summary: "summary".to_string(),
            },
            serde_json::json!({"type":"compaction","trigger":"manual","summary":"summary"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Cancelled {
                reason: "stop".to_string(),
            },
            serde_json::json!({"type":"cancelled","reason":"stop"}),
        ),
        (
            crate::kernel::runtime_host::RuntimeEventKind::Failed {
                code: "runtime_execution".to_string(),
                message: "boom".to_string(),
            },
            serde_json::json!({"type":"failed","code":"runtime_execution","message":"boom"}),
        ),
    ];

    for (kind, expected) in cases {
        assert_eq!(serde_json::to_value(kind).unwrap(), expected);
    }
}

#[tokio::test]
async fn lifecycle_record_tracks_root_start_checkpoint_and_stop() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();

    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Idle).await;
    let record = thread.lifecycle_record().await;
    assert_eq!(
        record.status,
        crate::kernel::runtime_host::ThreadLifecycleStatus::Idle
    );
    assert_eq!(record.parent_thread_id, None);

    let checkpoint = host
        .create_checkpoint(
            thread.context().coordinates.thread_id,
            None,
            Some("before-stop".to_string()),
            std::collections::BTreeMap::from([(
                "opaque_app_id".to_string(),
                "app_123".to_string(),
            )]),
        )
        .await
        .unwrap();
    assert_eq!(
        checkpoint.lineage,
        crate::kernel::runtime_host::ThreadCheckpointLineage::Root
    );
    assert_eq!(checkpoint.coordinates, thread.context().coordinates);
    assert_eq!(checkpoint.label.as_deref(), Some("before-stop"));

    let record = thread.lifecycle_record().await;
    assert_eq!(record.latest_checkpoint_id, Some(checkpoint.id));
    assert!(record.latest_signal_id.is_some());

    thread
        .send(crate::kernel::runtime_host::ThreadCommand::Shutdown)
        .await
        .unwrap();
    thread.wait().await;
    let record = thread.lifecycle_record().await;
    assert_eq!(
        record.status,
        crate::kernel::runtime_host::ThreadLifecycleStatus::Stopped
    );
}

#[tokio::test]
async fn lifecycle_snapshot_returns_records_not_only_thin_status() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Idle).await;

    let snapshot = host.lifecycle_snapshot().await;

    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(
        snapshot.records[0].coordinates,
        thread.context().coordinates
    );
    assert_eq!(
        snapshot.records[0].status,
        crate::kernel::runtime_host::ThreadLifecycleStatus::Idle
    );
}

#[tokio::test]
async fn text_submit_helper_matches_structured_text_turn_canonical_record() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let old = host
        .start_thread(
            coords("tenant_a", "user_1", "old"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let new = host
        .start_thread(
            coords("tenant_a", "user_1", "new"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut old_events = old.subscribe_events();
    let mut new_events = new.subscribe_events();

    host.submit(old.context().coordinates.thread_id, "turn", "hello")
        .await
        .unwrap();
    host.submit_turn(
        new.context().coordinates.thread_id,
        "turn",
        crate::kernel::runtime_host::TurnInput::text("hello"),
    )
    .await
    .unwrap();
    assert_output(&mut old_events, "turn:hello").await;
    assert_output(&mut new_events, "turn:hello").await;

    assert_eq!(
        canonical_user_content(&old.session_context().await.unwrap()),
        canonical_user_content(&new.session_context().await.unwrap())
    );
}

#[tokio::test]
async fn runtime_host_submit_records_surface_admission_before_turn_execution() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "host-submit"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn", "hello")
        .await
        .unwrap();
    assert_output(&mut events, "turn:hello").await;

    let control_events = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let thread_events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let admission = crate::kernel::admission::assert_admission_precedes_turn_records(
        &control_events,
        &thread_events,
    );
    assert_eq!(
        admission.payload["schema"],
        crate::kernel::history::EventKind::AdmissionDecided.payload_schema_id()
    );
    assert_eq!(admission.payload["route_id"], "surface:host-submit");
    assert_eq!(admission.payload["decision"], "queue");
    assert_eq!(
        admission.payload["admissible"],
        serde_json::json!(["queue"])
    );
    assert_eq!(
        admission.payload["source_ingress_event_ids"],
        serde_json::json!([])
    );
    assert_eq!(
        admission.origin,
        crate::kernel::history::EventOrigin::Discharged
    );
    assert_eq!(
        admission.provenance.discharged_by.as_deref(),
        Some("policy:admission_surface:host-submit")
    );
    assert_eq!(
        admission.provenance.function.as_deref(),
        Some("surface_admission/v1")
    );
    assert_eq!(
        admission.provenance.config_hash.as_deref(),
        admission.payload["policy_hash"].as_str()
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn failed_admission_append_prevents_runtime_host_turn_execution() {
    let store = std::sync::Arc::new(
        crate::test_support::FaultingRuntimeStore::new(std::sync::Arc::new(
            crate::kernel::history::InMemorySessionStore::new(),
        ))
        .fail_nth("append_events", 1, "admission append failed"),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "failed-admission"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Idle).await;
    let mut events = thread.subscribe_events();

    let err = host
        .submit(thread.context().coordinates.thread_id, "turn", "blocked")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("admission append failed"),
        "unexpected error: {err}"
    );
    assert_eq!(store.call_count("append_events"), 1);

    // tight-timeout: paused time deterministically proves failed admission emits no output
    let output = tokio::time::timeout(tokio::time::Duration::from_millis(100), async {
        loop {
            if let crate::kernel::runtime_host::ThreadEvent::Output { text, .. } =
                events.recv().await.unwrap()
            {
                return text;
            }
        }
    })
    .await;
    assert!(
        output.is_err(),
        "turn executed after failed admission append"
    );
    assert!(thread.session_context().await.unwrap().entries.is_empty());
}

#[tokio::test]
async fn closed_thread_rejection_does_not_append_admission() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(ExitRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "closed-submit"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Failed).await;

    let err = host
        .submit(
            thread.context().coordinates.thread_id,
            "turn-after-close",
            "blocked",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, crate::kernel::runtime_host::VerletError::ThreadClosed(thread_id) if thread_id == thread.context().coordinates.thread_id)
    );

    let control_events = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        control_events
            .iter()
            .all(|event| event.kind != crate::kernel::history::EventKind::AdmissionDecided),
        "closed thread submit must not leave an orphan admission.decided"
    );
}

#[tokio::test]
async fn structured_image_turn_maps_to_canonical_user_content() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "image"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit_turn(
        thread.context().coordinates.thread_id,
        "turn",
        crate::kernel::runtime_host::TurnInput::new([
            crate::kernel::runtime_host::TurnContent::text("look"),
            crate::kernel::runtime_host::TurnContent::image("base64-image", "image/png"),
        ]),
    )
    .await
    .unwrap();
    assert_output(&mut events, "turn:look").await;

    let content = canonical_user_content(&thread.session_context().await.unwrap());
    assert_eq!(
        content,
        vec![vec![
            crate::kernel::history::CanonicalContent::text("look"),
            crate::kernel::history::CanonicalContent::Image {
                data: "base64-image".to_string(),
                mime_type: "image/png".to_string(),
            },
        ]]
    );
}

#[tokio::test]
async fn file_and_runtime_context_reach_runtime_boundary_without_canonicalizing_file() {
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        InspectTurnInputRuntimeFactory,
    ));
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "file"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();
    let input = crate::kernel::runtime_host::TurnInput::new([
        crate::kernel::runtime_host::TurnContent::text("inspect"),
        crate::kernel::runtime_host::TurnContent::file_ref("/workspace/report.txt")
            .with_mime_type("text/plain")
            .with_size_bytes(42)
            .with_sha256("abc123"),
        crate::kernel::runtime_host::TurnContent::image("inline", "image/jpeg"),
    ])
    .with_cwd("/workspace")
    .with_workspace_root("/workspace")
    .with_permission_profile("source-write")
    .with_model("gpt-test")
    .with_provider("openai")
    .with_provider_metadata("request_id", "provider-req")
    .with_metadata("product_upload_id", "upload_123");

    host.submit_turn(thread.context().coordinates.thread_id, "turn", input)
        .await
        .unwrap();
    let output = next_output(&mut events).await;

    assert!(output.contains("text=inspect"));
    assert!(output.contains("cwd=/workspace"));
    assert!(output.contains("roots=1"));
    assert!(output.contains("permission=source-write"));
    assert!(output.contains("model=gpt-test"));
    assert!(output.contains("provider=openai"));
    assert!(output.contains("provider_request_id=provider-req"));
    assert!(output.contains("image=image/jpeg"));
    assert!(output.contains("file=/workspace/report.txt:text/plain:42:abc123"));
}

#[tokio::test]
async fn child_lifecycle_is_visible_through_runtime_event_stream() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut root_events = root.subscribe_events();
    let child = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::spawned_from(
                root.context().coordinates.thread_id,
            ),
        )
        .await
        .unwrap();

    let started = assert_runtime_kind(&mut root_events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::SubthreadStarted { child_thread_id }
                if *child_thread_id == child.context().coordinates.thread_id
        )
    })
    .await;
    assert_eq!(started.coordinates, root.context().coordinates);

    host.shutdown_thread(child.context().coordinates.thread_id)
        .await
        .unwrap();
    assert_runtime_kind(&mut root_events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::SubthreadFinished {
                child_thread_id,
                status: crate::kernel::runtime_host::ThreadLifecycleStatus::Stopped,
            } if *child_thread_id == child.context().coordinates.thread_id
        )
    })
    .await;
}

#[tokio::test]
async fn cross_thread_prompt_and_result_events_do_not_rewrite_lineage() {
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        AssistantHistoryRuntimeFactory,
    ));
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "root"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let root_id = root.context().coordinates.thread_id;
    let mut root_events = root.subscribe_events();
    let child = host
        .start_thread(
            coords("tenant_a", "user_1", "root"),
            crate::kernel::runtime_host::ThreadTopology::spawned_from(root_id),
        )
        .await
        .unwrap();
    let child_id = child.context().coordinates.thread_id;
    let mut child_interaction_events = child.subscribe_events();
    let mut child_output_events = child.subscribe_events();
    wait_for_status(&root, crate::kernel::runtime_host::ThreadStatus::Idle).await;
    wait_for_status(&child, crate::kernel::runtime_host::ThreadStatus::Idle).await;

    let control = host.kernel_control();
    let receipt = control
        .submit_to_thread(
            root.context(),
            child_id,
            Some("turn-cross".to_string()),
            crate::kernel::runtime_host::TurnInput::text("from root"),
        )
        .await
        .unwrap();
    assert_eq!(receipt.caller_thread_id, root_id);
    assert_eq!(receipt.target_thread_id, child_id);
    assert_eq!(receipt.turn_id, "turn-cross");

    let submitted = assert_runtime_kind(&mut root_events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::ThreadInteraction {
                interaction_id,
                kind: crate::kernel::runtime_host::ThreadInteractionKind::PromptSubmitted,
                source_thread_id,
                target_thread_id,
                target_turn_id: Some(target_turn_id),
                ..
            } if *interaction_id == receipt.interaction_id
                && *source_thread_id == root_id
                && *target_thread_id == child_id
                && target_turn_id == "turn-cross"
        )
    })
    .await;
    assert_eq!(submitted.coordinates.thread_id, root_id);

    let received = assert_runtime_kind(&mut child_interaction_events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::ThreadInteraction {
                interaction_id,
                kind: crate::kernel::runtime_host::ThreadInteractionKind::PromptReceived,
                source_thread_id,
                target_thread_id,
                target_turn_id: Some(target_turn_id),
                ..
            } if *interaction_id == receipt.interaction_id
                && *source_thread_id == root_id
                && *target_thread_id == child_id
                && target_turn_id == "turn-cross"
        )
    })
    .await;
    assert_eq!(received.coordinates.thread_id, child_id);
    assert_output(&mut child_output_events, "turn-cross:from root").await;
    let control_events = child.read_control_events().await.unwrap();
    let thread_events = child.read_thread_events(None).await.unwrap();
    let admission = crate::kernel::admission::assert_admission_precedes_turn_records(
        &control_events,
        &thread_events,
    );
    assert_eq!(
        admission.payload["route_id"],
        "surface:kernel-thread-submit"
    );

    let wait = control
        .wait_thread(root.context(), child_id, Some(1_000))
        .await
        .unwrap();
    assert!(!wait.timed_out);
    assert_eq!(wait.latest_output.as_deref(), Some("turn-cross:from root"));
    let result_interaction_id = wait
        .result_interaction_id
        .expect("wait should attach the child result to the caller");
    assert_runtime_kind(&mut root_events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::ThreadInteraction {
                interaction_id,
                kind: crate::kernel::runtime_host::ThreadInteractionKind::ResultAttached,
                source_thread_id,
                target_thread_id,
                result_preview: Some(result_preview),
                ..
            } if *interaction_id == result_interaction_id
                && *source_thread_id == child_id
                && *target_thread_id == root_id
                && result_preview == "turn-cross:from root"
        )
    })
    .await;

    let child_topology = child.lifecycle_record().await.topology;
    assert_eq!(
        child_topology.lineage,
        crate::kernel::runtime_host::ThreadLineage::Root
    );
    assert_eq!(child_topology.spawn_source_thread_id(), Some(root_id));
    assert_eq!(child_topology.branch_parent_thread_id(), None);
    assert!(canonical_user_content(&root.session_context().await.unwrap()).is_empty());
    assert_eq!(
        canonical_user_content(&child.session_context().await.unwrap()),
        vec![vec![crate::kernel::history::CanonicalContent::text(
            "from root"
        )]]
    );
}

#[tokio::test]
async fn loop_continuation_accepts_request_and_submits_next_turn_once() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "loop"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let coordinates = thread.context().coordinates.clone();
    let mut events = thread.subscribe_events();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Idle).await;
    let parent = append_loop_parent_completed(store.as_ref(), &coordinates, "turn-1").await;
    append_loop_mandate_started(
        store.as_ref(),
        &coordinates,
        "loop-1",
        "snapshot-loop",
        Some(2),
    )
    .await;
    append_loop_continue_request(
        store.as_ref(),
        &coordinates,
        parent.id,
        "loop-1",
        "turn-1",
        "snapshot-loop",
        "retry from loop",
    )
    .await;

    let receipt = host
        .continue_turn_if_requested(thread_id, "loop-1", "turn-1", "turn-2", 1_000, 0)
        .await
        .unwrap();

    let accepted_event_id = match &receipt {
        crate::kernel::runtime_host::LoopContinuationReceipt::Accepted {
            loop_id,
            parent_turn_id,
            next_turn_id,
            accepted_event_id,
        } => {
            assert_eq!(loop_id, "loop-1");
            assert_eq!(parent_turn_id, "turn-1");
            assert_eq!(next_turn_id, "turn-2");
            *accepted_event_id
        }
        other => panic!("expected accepted continuation, got {other:?}"),
    };
    assert_output(&mut events, "turn-2:retry from loop").await;

    let replay = host
        .continue_turn_if_requested(thread_id, "loop-1", "turn-1", "turn-2", 1_000, 1)
        .await
        .unwrap();
    assert_eq!(replay, receipt);

    let control_events = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(&coordinates),
            None,
        )
        .await
        .unwrap();
    let accepted = control_events
        .iter()
        .filter(|event| event.kind == crate::kernel::history::EventKind::TurnContinuationAccepted)
        .collect::<Vec<_>>();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].id, accepted_event_id);
    let payload = serde_json::from_value::<
        crate::kernel::control_decision::TurnContinuationAcceptedPayload,
    >(accepted[0].payload.clone())
    .unwrap();
    assert_eq!(payload.next_turn_id, "turn-2");
    assert_eq!(payload.mandate_id, "mandate-loop-1");

    let thread_events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&coordinates),
            None,
        )
        .await
        .unwrap();
    let submitted = thread_events
        .iter()
        .filter(|event| {
            event.kind == crate::kernel::history::EventKind::TurnSubmitted
                && event.payload["turn_id"].as_str() == Some("turn-2")
        })
        .collect::<Vec<_>>();
    assert_eq!(submitted.len(), 1);
    assert_eq!(
        submitted[0].origin,
        crate::kernel::history::EventOrigin::Discharged
    );
    assert_eq!(
        submitted[0].provenance.source_event_ids,
        vec![accepted_event_id]
    );
}

#[tokio::test]
async fn catch_up_continuation_fired_before_expiry_is_rejected_after_expiry() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "expired-catch-up"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let coordinates = thread.context().coordinates.clone();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Idle).await;
    let parent = append_loop_parent_completed(store.as_ref(), &coordinates, "turn-1").await;
    store
        .append_events(
            &crate::kernel::control_decision::control_stream_id(&coordinates),
            vec![crate::kernel::history::NewEventRecord::witnessed(
                coordinates.clone(),
                crate::kernel::history::EventKind::MandateStarted,
                serde_json::to_value(crate::kernel::control_decision::MandateStartedPayload {
                    subject: crate::kernel::control_decision::MandateSubject {
                        thread_id: Some(coordinates.thread_id.to_string()),
                        loop_id: Some("loop-catch-up".to_string()),
                    },
                    mandate_id: "mandate-catch-up".to_string(),
                    snapshot_id: "schedule.v1".to_string(),
                    thread_id: Some(coordinates.thread_id.to_string()),
                    max_continuations: None,
                    expires_at_ms: Some(1_000),
                    schedule: Some(
                        crate::kernel::control_decision::MandateSchedulePayload::Interval {
                            every_ms: 60_000,
                        },
                    ),
                    max_occurrences: None,
                    catch_up: Some(
                        crate::kernel::control_decision::MandateCatchUpPolicy::CoalesceMissed,
                    ),
                    input_template: Some("catch up".to_string()),
                })
                .unwrap(),
            )],
        )
        .await
        .unwrap();
    let mut request = crate::kernel::history::NewEventRecord::discharged(
        coordinates.clone(),
        crate::kernel::history::EventKind::TurnContinueRequested,
        serde_json::to_value(
            crate::kernel::control_decision::TurnContinueRequestedPayload {
                subject: crate::kernel::control_decision::TurnContinuationSubject {
                    loop_id: "loop-catch-up".to_string(),
                    parent_turn_id: "turn-1".to_string(),
                },
                snapshot_id: "schedule.v1".to_string(),
                next_turn_input: "catch up".to_string(),
            },
        )
        .unwrap(),
        crate::kernel::history::EventProvenance {
            source_streams: vec![crate::kernel::history::EventStreamId::for_thread(
                &coordinates,
            )],
            source_event_ids: vec![parent.id],
            discharged_by: Some("coupling:std::schedule.cron".to_string()),
            function: Some("schedule_continuation/v1".to_string()),
            ..crate::kernel::history::EventProvenance::default()
        },
    );
    request.created_at_ms = 900;
    store
        .append_events(
            &crate::kernel::control_decision::control_stream_id(&coordinates),
            vec![request],
        )
        .await
        .unwrap();

    let receipt = host
        .continue_turn_if_requested(thread_id, "loop-catch-up", "turn-1", "turn-2", 1_001, 0)
        .await
        .unwrap();

    assert!(matches!(
        receipt,
        crate::kernel::runtime_host::LoopContinuationReceipt::Rejected { reason, .. }
            if reason == "continuation mandate expired at 1000 (1970-01-01T00:00:01.000Z)"
    ));
    let thread_events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(thread_events.iter().all(|event| {
        event.kind != crate::kernel::history::EventKind::TurnSubmitted
            || event.payload["turn_id"].as_str() != Some("turn-2")
    }));
    let control_events = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(&coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(control_events.iter().any(|event| {
        event.kind == crate::kernel::history::EventKind::TurnContinuationRejected
            && event.payload["reason"]
                == "continuation mandate expired at 1000 (1970-01-01T00:00:01.000Z)"
    }));
}

#[tokio::test]
async fn schedule_timer_fired_continuation_is_accepted_and_runs_offline_provider() {
    let root = std::env::temp_dir()
        .join("verlet-runtime-host-tests")
        .join(uuid::Uuid::now_v7().to_string());
    let store = crate::SqliteSessionStore::open(root.join("history.sqlite3"))
        .await
        .unwrap();
    let mut config = crate::AgentLoopConfig::new(
        crate::kernel::history::ProviderApi::Other("local_offline".to_string()),
        "local_offline",
        "gpt-test",
    );
    config.max_tokens = 128;
    let provider = std::sync::Arc::new(crate::LocalOfflineProviderClient::new(
        "local_offline",
        "gpt-test",
    ));
    let factory = std::sync::Arc::new(crate::AgentLoopFactory::new(config, provider));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory,
        std::sync::Arc::new(store.clone()),
    );
    let coordinates =
        crate::kernel::runtime_host::ThreadCoordinates::new("tenant_a", "user_1", "scheduled");
    let thread = host
        .start_thread(
            coordinates.clone(),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let mut events = thread.subscribe_events();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Idle).await;

    let start = chrono::Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .unwrap();
    let due = start + chrono::Duration::minutes(1);
    let mandate = append_scheduled_loop_mandate(&store, &coordinates, "loop-nightly", start).await;
    let sink = std::sync::Arc::new(WitnessTimerFiredSink {
        store: store.clone(),
        coordinates: coordinates.clone(),
    });
    let clock = std::sync::Arc::new(RuntimeFakeClock::new(due));
    let route = crate::VerletDaemonClockRoute::new("clock-main", store.clone(), sink, clock)
        .with_started_at(start);

    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);
    let control_events = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(&coordinates),
            None,
        )
        .await
        .unwrap();
    let fired = control_events
        .iter()
        .filter(|event| event.kind == crate::kernel::history::EventKind::TimerFired)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].provenance.source_event_ids, vec![mandate.id]);

    let executor = crate::StdlibCouplingExecutor;
    let scheduler = crate::CouplingScheduler::new(&store, &executor);
    let receipt = scheduler
        .run_batch(
            &crate::agent::manifest_bind::BoundCouplingSet::new(
                "schedule.v1",
                vec![runtime_std_schedule_cron_timer_coupling()],
            ),
            fired,
        )
        .await
        .unwrap();
    assert_eq!(receipt.runs.len(), 1);
    assert_eq!(receipt.runs[0].coupling_id, "std::schedule.cron");
    assert_eq!(receipt.runs[0].discharged_event_ids.len(), 1);

    let continuation = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(&coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.kind == crate::kernel::history::EventKind::TurnContinueRequested)
        .unwrap();
    assert_eq!(
        continuation.payload["next_turn_input"],
        "wake at 2026-01-01T00:01:00.000Z"
    );

    let continuation_receipt = host
        .continue_turn_if_requested(
            thread_id,
            "loop-nightly",
            "turn-nightly-root",
            "turn-nightly-1",
            due.timestamp_millis(),
            0,
        )
        .await
        .unwrap();
    match continuation_receipt {
        crate::kernel::runtime_host::LoopContinuationReceipt::Accepted {
            loop_id,
            parent_turn_id,
            next_turn_id,
            ..
        } => {
            assert_eq!(loop_id, "loop-nightly");
            assert_eq!(parent_turn_id, "turn-nightly-root");
            assert_eq!(next_turn_id, "turn-nightly-1");
        }
        other => panic!("expected accepted schedule continuation, got {other:?}"),
    }
    assert_output(&mut events, "local:wake at 2026-01-01T00:01:00.000Z").await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn loop_continuation_without_mandate_rejects_without_submitting() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "loop-denied"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let coordinates = thread.context().coordinates.clone();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Idle).await;
    let parent = append_loop_parent_completed(store.as_ref(), &coordinates, "turn-1").await;
    append_loop_continue_request(
        store.as_ref(),
        &coordinates,
        parent.id,
        "loop-1",
        "turn-1",
        "snapshot-loop",
        "retry from loop",
    )
    .await;

    let receipt = host
        .continue_turn_if_requested(thread_id, "loop-1", "turn-1", "turn-2", 1_000, 0)
        .await
        .unwrap();

    match receipt {
        crate::kernel::runtime_host::LoopContinuationReceipt::Rejected {
            loop_id,
            parent_turn_id,
            reason,
            ..
        } => {
            assert_eq!(loop_id, "loop-1");
            assert_eq!(parent_turn_id, "turn-1");
            assert_eq!(reason, "continuation has no active mandate");
        }
        other => panic!("expected rejected continuation, got {other:?}"),
    }
    let thread_events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        thread_events
            .iter()
            .all(|event| event.kind != crate::kernel::history::EventKind::TurnSubmitted)
    );
}

#[tokio::test]
async fn execution_policy_limits_child_threads() {
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(EchoRuntimeFactory),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default().with_max_child_threads(1),
    );
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut root_events = root.subscribe_events();
    host.start_thread(
        coords("tenant_a", "user_1", "s1"),
        crate::kernel::runtime_host::ThreadTopology::spawned_from(
            root.context().coordinates.thread_id,
        ),
    )
    .await
    .unwrap();

    let err = match host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::spawned_from(
                root.context().coordinates.thread_id,
            ),
        )
        .await
    {
        Ok(_) => panic!("child start unexpectedly succeeded"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        crate::kernel::runtime_host::VerletError::ThreadPolicyViolation {
            code: "max_child_threads",
            ..
        }
    ));
    assert_runtime_kind(&mut root_events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::PolicyRejected { code, .. } if code == "max_child_threads"
        )
    })
    .await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn concurrent_child_starts_reserve_policy_slots_before_runtime_build() {
    let factory = std::sync::Arc::new(ControlledChildBuildFactory::default());
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        factory.clone(),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default().with_max_child_threads(2),
    );
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "concurrent-child-cap"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let root_thread_id = root.context().coordinates.thread_id;
    let mut starts = Vec::new();
    for _ in 0..3 {
        let host = host.clone();
        starts.push(tokio::spawn(async move {
            host.start_thread(
                coords("tenant_a", "user_1", "concurrent-child-cap"),
                crate::kernel::runtime_host::ThreadTopology::spawned_from(root_thread_id),
            )
            .await
        }));
    }

    factory.wait_for_child_builds(2).await;
    factory.release_builds();
    let mut results = Vec::new();
    for start in starts {
        results.push(start.await.unwrap());
    }

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
    assert_eq!(
        results
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    Err(
                        crate::kernel::runtime_host::VerletError::ThreadPolicyViolation {
                            code: "max_child_threads",
                            ..
                        }
                    )
                )
            })
            .count(),
        1
    );
    assert_eq!(host.children_of(root_thread_id).await.len(), 2);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn failed_child_build_releases_reserved_policy_slot() {
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(FailFirstChildBuildFactory::default()),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default().with_max_child_threads(1),
    );
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "failed-child-reservation"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let root_thread_id = root.context().coordinates.thread_id;

    assert!(matches!(
        host.start_thread(
            coords("tenant_a", "user_1", "failed-child-reservation"),
            crate::kernel::runtime_host::ThreadTopology::spawned_from(root_thread_id),
        )
        .await,
        Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(_))
    ));
    host.start_thread(
        coords("tenant_a", "user_1", "failed-child-reservation"),
        crate::kernel::runtime_host::ThreadTopology::spawned_from(root_thread_id),
    )
    .await
    .unwrap();

    assert_eq!(host.children_of(root_thread_id).await.len(), 1);
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn execution_policy_rejects_submit_when_command_queue_is_full() {
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(StuckRuntimeFactory),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default().with_max_pending_inputs(1),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hold")
        .await
        .unwrap();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Running).await;
    host.submit(thread.context().coordinates.thread_id, "turn-2", "queued")
        .await
        .unwrap();
    let err = host
        .submit(thread.context().coordinates.thread_id, "turn-3", "rejected")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        crate::kernel::runtime_host::VerletError::ThreadPolicyViolation {
            code: "max_pending_inputs",
            ..
        }
    ));
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::PolicyRejected { code, .. } if code == "max_pending_inputs"
        )
    })
    .await;
    assert_eq!(
        canonical_user_content(&thread.session_context().await.unwrap()),
        vec![vec![crate::kernel::history::CanonicalContent::text("hold")]]
    );
    thread.abort().await;
}

#[tokio::test]
async fn user_turn_input_persistence_adopts_existing_entry_by_turn_id() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let coordinates = coords("tenant_a", "user_1", "turn-input-idempotency");
    let first_services = crate::kernel::runtime_host::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default(),
    );
    let recovered_services = crate::kernel::runtime_host::RuntimeServices::new(
        store.clone(),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default(),
    );
    let input = crate::kernel::runtime_host::TurnInput::text("persist exactly once");

    let first = first_services
        .append_user_turn_input(&coordinates, "turn-stable", &input)
        .await
        .unwrap();
    let recovered = recovered_services
        .append_user_turn_input(&coordinates, "turn-stable", &input)
        .await
        .unwrap();

    assert_eq!(recovered.entry_id, first.entry_id);
    let context = store.build_context(&coordinates).await.unwrap();
    assert_eq!(
        canonical_user_content(&context),
        vec![vec![crate::kernel::history::CanonicalContent::text(
            "persist exactly once"
        )]]
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn queued_turn_watchdog_starts_only_when_that_turn_begins_executing() {
    let state = std::sync::Arc::new(GatedTurnRuntimeState::default());
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(GatedTurnRuntimeFactory {
            state: std::sync::Arc::clone(&state),
        }),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default().with_turn_timeout_ms(100),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "watchdog"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-a", "hold")
        .await
        .unwrap();
    state.first_started.notified().await;

    tokio::time::advance(tokio::time::Duration::from_millis(50)).await;
    host.submit(thread.context().coordinates.thread_id, "turn-b", "queued")
        .await
        .unwrap();

    tokio::time::advance(tokio::time::Duration::from_millis(50)).await;
    // tight-timeout: paused time deterministically bounds the first turn watchdog event
    tokio::time::timeout(
        tokio::time::Duration::from_millis(1),
        assert_runtime_kind(&mut events, |kind| {
            matches!(
                kind,
                crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
            )
        }),
    )
    .await
    .expect("the active first turn watchdog did not fire at its deadline");

    assert!(
        // tight-timeout: paused time proves the queued turn watchdog remains inactive
        tokio::time::timeout(
            tokio::time::Duration::from_millis(51),
            assert_runtime_kind(&mut events, |kind| {
                matches!(
                    kind,
                    crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
                )
            }),
        )
        .await
        .is_err(),
        "the queued second turn started its watchdog before execution"
    );

    state.release_first.notify_one();
    state.second_started.notified().await;

    assert!(
        // tight-timeout: paused time proves the second watchdog stays quiet before its deadline
        tokio::time::timeout(
            tokio::time::Duration::from_millis(99),
            assert_runtime_kind(&mut events, |kind| {
                matches!(
                    kind,
                    crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
                )
            }),
        )
        .await
        .is_err(),
        "the second turn watchdog fired before its execution deadline"
    );
    // tight-timeout: paused time deterministically crosses the second turn watchdog deadline
    tokio::time::timeout(
        tokio::time::Duration::from_millis(2),
        assert_runtime_kind(&mut events, |kind| {
            matches!(
                kind,
                crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
            )
        }),
    )
    .await
    .expect("the second turn watchdog did not start with execution");

    thread.abort().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn timed_out_turn_cancellation_does_not_apply_to_the_next_turn() {
    let state = std::sync::Arc::new(WatchdogHandoffState::default());
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(WatchdogHandoffRuntimeFactory {
            state: std::sync::Arc::clone(&state),
        }),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default().with_turn_timeout_ms(100),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "watchdog-handoff"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();
    let thread_id = thread.context().coordinates.thread_id;

    host.submit(thread_id, "turn-a", "active").await.unwrap();
    state.first_started.notified().await;
    host.submit(thread_id, "turn-b", "queued").await.unwrap();

    tokio::time::advance(tokio::time::Duration::from_millis(100)).await;
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
        )
    })
    .await;
    state.release_first.notify_one();
    state.second_started.notified().await;

    assert!(
        // tight-timeout: paused time deterministically proves the stale cancellation stays absent
        tokio::time::timeout(
            tokio::time::Duration::from_millis(1),
            state.wait_for_stale_cancel()
        )
        .await
        .is_err(),
        "turn A's timeout cancellation was applied after turn B started"
    );
    thread.abort().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn turn_watchdog_cancels_without_waiting_for_full_command_queue() {
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(StuckRuntimeFactory),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default()
            .with_turn_timeout_ms(100)
            .with_cancel_grace_timeout_ms(20),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "watchdog-full-queue"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    host.submit(thread_id, "active", "hold").await.unwrap();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Running).await;

    for queued in 0..thread.thread.command_capacity {
        host.submit(thread_id, format!("queued-{queued}"), "queued")
            .await
            .unwrap();
    }
    assert_eq!(
        thread.queued_command_count(),
        thread.thread.command_capacity
    );

    tokio::time::advance(tokio::time::Duration::from_millis(100)).await;
    wait_for_status(
        &thread,
        crate::kernel::runtime_host::ThreadStatus::Cancelling,
    )
    .await;
    tokio::time::advance(tokio::time::Duration::from_millis(20)).await;
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Failed).await;

    host.shutdown_thread(thread_id).await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn concurrent_submits_reserve_pending_input_slots_atomically() {
    let barrier = std::sync::Arc::new(AdmissionAppendBarrier::default());
    let store = std::sync::Arc::new(AdmissionTestStore::blocking(barrier.clone()));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store_and_policy(
        std::sync::Arc::new(StuckRuntimeFactory),
        store,
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default().with_max_pending_inputs(2),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "concurrent-submit-cap"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let mut submits = Vec::new();
    for turn in 0..3 {
        let host = host.clone();
        submits.push(tokio::spawn(async move {
            host.submit(thread_id, format!("turn-{turn}"), "queued")
                .await
        }));
    }

    barrier.wait_for_entries(2).await;
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        barrier.entered.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "over-cap submits must be rejected before entering the async admission append"
    );
    barrier.release();
    let mut results = Vec::new();
    for submit in submits {
        results.push(submit.await.unwrap());
    }

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
    assert_eq!(
        results
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    Err(
                        crate::kernel::runtime_host::VerletError::ThreadPolicyViolation {
                            code: "max_pending_inputs",
                            ..
                        }
                    )
                )
            })
            .count(),
        1
    );
    thread.abort().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn pending_input_cap_counts_commands_drained_into_runtime_queues() {
    let state = std::sync::Arc::new(DrainedPendingInputState::default());
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(DrainedPendingInputRuntimeFactory {
            state: std::sync::Arc::clone(&state),
        }),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default().with_max_pending_inputs(1),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "drained-pending-input"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;

    host.submit(thread_id, "turn-a", "active").await.unwrap();
    state.first_started.notified().await;
    host.submit(thread_id, "turn-b", "pending").await.unwrap();
    state.queued_input_drained.notified().await;

    assert!(matches!(
        host.submit(thread_id, "turn-c", "over-cap").await,
        Err(
            crate::kernel::runtime_host::VerletError::ThreadPolicyViolation {
                code: "max_pending_inputs",
                ..
            }
        )
    ));

    state.release.notify_one();
    thread.abort().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn duplicate_start_is_rejected_before_factory_build_or_runtime_spawn() {
    let factory = std::sync::Arc::new(ControlledChildBuildFactory::default());
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory.clone());
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "duplicate-start"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let duplicate_coordinates = coords("tenant_a", "user_1", "duplicate-start");
    let topology = crate::kernel::runtime_host::ThreadTopology::spawned_from(
        root.context().coordinates.thread_id,
    );
    let first_host = host.clone();
    let first_coordinates = duplicate_coordinates.clone();
    let first_topology = topology.clone();
    let first = tokio::spawn(async move {
        first_host
            .start_thread(first_coordinates, first_topology)
            .await
    });
    factory.wait_for_child_builds(1).await;

    let second_host = host.clone();
    let mut second = tokio::spawn(async move {
        second_host
            .start_thread(duplicate_coordinates, topology)
            .await
    });
    let mut early_second_result = None;
    tokio::select! {
        result = &mut second => {
            early_second_result = Some(result.unwrap());
        }
        _ = factory.wait_for_child_builds(2) => {}
    }

    assert_eq!(
        factory
            .child_builds
            .load(std::sync::atomic::Ordering::SeqCst),
        1,
        "duplicate start must be rejected before runtime construction"
    );
    factory.release_builds();
    let first = first.await.unwrap().unwrap();
    let second = match early_second_result {
        Some(result) => result,
        None => second.await.unwrap(),
    };
    assert!(matches!(
        second,
        Err(crate::kernel::runtime_host::VerletError::ThreadAlreadyExists(_))
    ));
    wait_for_status(&first, crate::kernel::runtime_host::ThreadStatus::Idle).await;
    assert_eq!(
        factory
            .child_runtime_starts
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cancelled_start_wakes_reservation_waiters() {
    let coordinates = coords("tenant_a", "user_1", "cancelled-start-reservation");
    let thread_id = coordinates.thread_id;
    let factory = std::sync::Arc::new(BlockingThreadBuildFactory::new(thread_id));
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory.clone());
    let start_host = host.clone();
    let start = tokio::spawn(async move {
        start_host
            .start_thread(
                coordinates,
                crate::kernel::runtime_host::ThreadTopology::root(),
            )
            .await
    });
    factory.wait_until_blocked().await;

    let wait_host = host.clone();
    let mut waiter = tokio::spawn(async move {
        wait_host.wait_for_thread_start_reservation(thread_id).await;
    });
    assert!(
        // tight-timeout: paused time deterministically proves the reservation waiter stays pending
        tokio::time::timeout(tokio::time::Duration::from_millis(250), &mut waiter)
            .await
            .is_err(),
        "waiter must remain pending while the start reservation is held"
    );

    start.abort();
    match start.await {
        Err(err) => assert!(err.is_cancelled()),
        Ok(_) => panic!("blocked start unexpectedly completed"),
    }
    // tight-timeout: paused time deterministically bounds the reservation wakeup
    tokio::time::timeout(tokio::time::Duration::from_secs(1), waiter)
        .await
        .expect("reservation waiter should wake after start cancellation")
        .unwrap();
    assert!(matches!(
        host.get_thread(thread_id).await,
        Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(missing)) if missing == thread_id
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn runtime_is_registered_before_its_first_kernel_control_call() {
    let factory = std::sync::Arc::new(RegistrationProbeFactory::default());
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory.clone());
    let registration_barrier = host.inner.threads.read().await;
    let start_host = host.clone();
    let start = tokio::spawn(async move {
        start_host
            .start_thread(
                coords("tenant_a", "user_1", "startup-registration"),
                crate::kernel::runtime_host::ThreadTopology::root(),
            )
            .await
    });

    factory.wait_for_build().await;
    start.abort();
    match start.await {
        Err(err) => assert!(err.is_cancelled()),
        Ok(_) => panic!("registration-barrier start unexpectedly completed"),
    }
    drop(registration_barrier);

    assert_eq!(
        factory.wait_for_outcome().await,
        RegistrationProbeOutcome::DroppedWithoutRun
    );
    assert!(host.snapshot().await.threads.is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn lifecycle_sink_failure_joins_spawned_runtime_before_returning() {
    let factory = std::sync::Arc::new(CancellationTrackedFactory::default());
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory.clone());
    host.set_lifecycle_sink(Some(std::sync::Arc::new(FailingAfterRuntimeStartsSink {
        active_runs: std::sync::Arc::clone(&factory.active_runs),
        runtime_started: std::sync::Arc::clone(&factory.runtime_started),
    })))
    .await;

    assert!(matches!(
        host.start_thread(
            coords("tenant_a", "user_1", "sink-start-failure"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await,
        Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(_))
    ));
    assert_eq!(
        factory
            .active_runs
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "failed start returned before its runtime task was joined"
    );
    assert!(host.snapshot().await.threads.is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cancelled_start_after_publication_cleans_up_registered_runtime() {
    let factory = std::sync::Arc::new(CancellationTrackedFactory::default());
    let sink_entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let sink_release = std::sync::Arc::new(tokio::sync::Notify::new());
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory.clone());
    host.set_lifecycle_sink(Some(std::sync::Arc::new(BlockingLifecycleSink {
        entered: std::sync::Arc::clone(&sink_entered),
        release: std::sync::Arc::clone(&sink_release),
    })))
    .await;
    let coordinates = coords("tenant_a", "user_1", "cancel-after-publication");
    let thread_id = coordinates.thread_id;
    let start_host = host.clone();
    let start = tokio::spawn(async move {
        start_host
            .start_thread(
                coordinates,
                crate::kernel::runtime_host::ThreadTopology::root(),
            )
            .await
    });

    sink_entered.notified().await;
    assert_eq!(
        factory
            .active_runs
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    start.abort();
    match start.await {
        Err(err) => assert!(err.is_cancelled()),
        Ok(_) => panic!("blocked lifecycle-sink start unexpectedly completed"),
    }

    // tight-timeout: paused time deterministically bounds cancelled runtime shutdown
    tokio::time::timeout(
        tokio::time::Duration::from_millis(1),
        factory.wait_until_stopped(),
    )
    .await
    .expect("cancelled start left its published runtime running");
    assert!(matches!(
        host.get_thread(thread_id).await,
        Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(missing)) if missing == thread_id
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn turn_timeout_emits_timeout_and_cancel_timeout_recovery_events() {
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(StuckRuntimeFactory),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default()
            .with_turn_timeout_ms(20)
            .with_cancel_grace_timeout_ms(20),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hold")
        .await
        .unwrap();

    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
        )
    })
    .await;
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "cancel"
        )
    })
    .await;
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::Failed { code, .. } if code == "cancel_timeout"
        )
    })
    .await;
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Failed).await;
    assert_eq!(
        canonical_user_content(&thread.session_context().await.unwrap()),
        vec![vec![crate::kernel::history::CanonicalContent::text("hold")]]
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn explicit_cancel_timeout_marks_thread_failed_without_extra_history() {
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(StuckRuntimeFactory),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default()
            .with_cancel_grace_timeout_ms(20),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hold")
        .await
        .unwrap();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Running).await;
    let err = host
        .cancel(thread.context().coordinates.thread_id, "stop")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        crate::kernel::runtime_host::VerletError::ThreadPolicyViolation {
            code: "cancel_timeout",
            ..
        }
    ));
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "cancel"
        )
    })
    .await;
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Failed).await;
    assert_eq!(
        canonical_user_content(&thread.session_context().await.unwrap()),
        vec![vec![crate::kernel::history::CanonicalContent::text("hold")]]
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn shutdown_timeout_aborts_runtime_and_removes_thread() {
    let host = crate::kernel::runtime_host::RuntimeHost::with_policy(
        std::sync::Arc::new(StuckRuntimeFactory),
        crate::kernel::runtime_host::RuntimeExecutionPolicy::default()
            .with_shutdown_grace_timeout_ms(20),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hold")
        .await
        .unwrap();
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Running).await;
    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();

    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::Timeout { operation, .. } if operation == "shutdown"
        )
    })
    .await;
    assert!(matches!(
        host.get_thread(thread.context().coordinates.thread_id)
            .await,
        Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(_))
    ));
}

#[tokio::test]
async fn runtime_exit_without_terminal_status_is_marked_failed() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(ExitRuntimeFactory));
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            crate::kernel::runtime_host::RuntimeEventKind::Recovery { action, .. } if action == "mark_failed"
        )
    })
    .await;
    wait_for_status(&thread, crate::kernel::runtime_host::ThreadStatus::Failed).await;
}

#[tokio::test]
async fn resume_and_fork_use_loaded_checkpoint_records() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let checkpoint = host
        .create_checkpoint(
            thread.context().coordinates.thread_id,
            None,
            Some("checkpoint".to_string()),
            std::collections::BTreeMap::from([("opaque".to_string(), "value".to_string())]),
        )
        .await
        .unwrap();

    let fork = host
        .fork_thread(thread.context().coordinates.thread_id, Some(checkpoint.id))
        .await
        .unwrap();
    assert_eq!(
        fork.context().parent_thread_id,
        Some(thread.context().coordinates.thread_id)
    );
    let checkpoint_id = checkpoint.id.to_string();
    assert_eq!(
        fork.lifecycle_record()
            .await
            .metadata
            .get("forked_from_checkpoint_id")
            .map(String::as_str),
        Some(checkpoint_id.as_str())
    );

    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();
    let resumed = host.resume_thread(checkpoint.id).await.unwrap();
    assert_eq!(resumed.context().coordinates, checkpoint.coordinates);
    assert_eq!(
        resumed
            .lifecycle_record()
            .await
            .metadata
            .get("opaque")
            .map(String::as_str),
        Some("value")
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reserved_fork_adopts_a_cloned_branch_without_start_history() {
    let child_thread_id = crate::kernel::runtime_host::ThreadId::new();
    let factory = std::sync::Arc::new(BlockingThreadBuildFactory::new(child_thread_id));
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory.clone(),
        store.clone(),
    );
    let parent = host
        .start_thread(
            coords("tenant_a", "user_1", "fork-reserved-no-start"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let first_checkpoint = host
        .create_checkpoint(
            parent.context().coordinates.thread_id,
            None,
            Some("first fork cut".to_string()),
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();

    let fork_host = host.clone();
    let attempted_checkpoint = first_checkpoint.clone();
    let first_attempt = tokio::spawn(async move {
        fork_host
            .fork_thread_from_checkpoint_with_id(attempted_checkpoint, child_thread_id)
            .await
    });
    factory.wait_until_blocked().await;
    assert!(
        store
            .active_leaf(&crate::kernel::runtime_host::ThreadCoordinates {
                tenant_id: parent.context().coordinates.tenant_id.clone(),
                user_id: parent.context().coordinates.user_id.clone(),
                session_id: parent.context().coordinates.session_id.clone(),
                thread_id: child_thread_id,
            })
            .await
            .unwrap()
            .is_some(),
        "the cut must land after the branch clone"
    );
    first_attempt.abort();
    match first_attempt.await {
        Err(err) => assert!(err.is_cancelled()),
        Ok(_) => panic!("blocked fork unexpectedly completed"),
    }

    let recovery_checkpoint = host
        .create_checkpoint(
            parent.context().coordinates.thread_id,
            None,
            Some("recovery checkpoint".to_string()),
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
    factory.release();
    let child = host
        .fork_thread_from_checkpoint_with_id(recovery_checkpoint, child_thread_id)
        .await
        .unwrap();
    assert!(matches!(
        child.context().topology.lineage,
        crate::kernel::runtime_host::ThreadLineage::Branch {
            parent_thread_id,
            checkpoint_id: Some(checkpoint_id),
        } if parent_thread_id == parent.context().coordinates.thread_id
            && checkpoint_id == first_checkpoint.id
    ));
    let events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&child.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let first_checkpoint_id = first_checkpoint.id.to_string();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == crate::kernel::history::EventKind::SessionEntryAppended
                    && event.payload["runtime_kind"].as_str() == Some("thread_started")
                    && event.payload["runtime_payload"]["metadata"]["forked_from_checkpoint_id"]
                        .as_str()
                        == Some(first_checkpoint_id.as_str())
            })
            .count(),
        1,
        "recovery must append one child start identity over the existing clone"
    );
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fork_checkpoint_reconciles_one_shot_append_failures() {
    for fail_after_commit in [false, true] {
        let inner = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
        let faulting = if fail_after_commit {
            crate::test_support::FaultingRuntimeStore::new(inner.clone()).fail_nth_after(
                "append",
                2,
                "fork checkpoint append committed before disconnect",
            )
        } else {
            crate::test_support::FaultingRuntimeStore::new(inner.clone()).fail_nth(
                "append",
                2,
                "fork checkpoint append failed before commit",
            )
        };
        let store = std::sync::Arc::new(faulting);
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(EchoRuntimeFactory),
            store.clone(),
        );
        let parent = host
            .start_thread(
                coords("tenant_a", "user_1", "fork-checkpoint-append-fault"),
                crate::kernel::runtime_host::ThreadTopology::root(),
            )
            .await
            .unwrap();

        let checkpoint = host
            .create_checkpoint(
                parent.context().coordinates.thread_id,
                None,
                Some("fork cut".to_string()),
                std::collections::BTreeMap::new(),
            )
            .await
            .expect("a one-shot checkpoint append fault must be reconciled");

        let events = inner
            .read_events(
                &crate::kernel::history::EventStreamId::for_thread(&parent.context().coordinates),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.kind == crate::kernel::history::EventKind::SessionEntryAppended
                        && event.payload["runtime_kind"].as_str() == Some("thread_checkpoint")
                        && event.payload["runtime_payload"]["checkpoint_id"].as_str()
                            == Some(checkpoint.id.to_string().as_str())
                })
                .count(),
            1,
            "checkpoint reconciliation must not duplicate an after-fault commit"
        );
        host.shutdown_all().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn checkpoint_reconciliation_reads_authoritative_event_history() {
    let inner = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let store = std::sync::Arc::new(
        crate::test_support::FaultingRuntimeStore::new(inner)
            .fail_nth_after(
                "append",
                2,
                "fork checkpoint append committed before disconnect",
            )
            .fail_nth(
                "build_context",
                1,
                "selected branch projection is temporarily unavailable",
            ),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let parent = host
        .start_thread(
            coords(
                "tenant_a",
                "user_1",
                "checkpoint-authoritative-reconciliation",
            ),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();

    host.create_checkpoint(
        parent.context().coordinates.thread_id,
        None,
        Some("fork cut".to_string()),
        std::collections::BTreeMap::new(),
    )
    .await
    .expect("reconciliation must use the durable event stream, not a branch projection");

    assert_eq!(store.call_count("read_events"), 1);
    assert_eq!(store.call_count("build_context"), 0);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn checkpoint_reconciliation_reports_append_and_read_failures() {
    for fail_after_commit in [false, true] {
        let inner = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
        let faulting = if fail_after_commit {
            crate::test_support::FaultingRuntimeStore::new(inner.clone()).fail_nth_after(
                "append",
                2,
                "checkpoint append committed before disconnect",
            )
        } else {
            crate::test_support::FaultingRuntimeStore::new(inner.clone()).fail_nth(
                "append",
                2,
                "checkpoint append failed before commit",
            )
        };
        let store = std::sync::Arc::new(faulting.fail_nth(
            "read_events",
            1,
            "checkpoint reconciliation read failed",
        ));
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(EchoRuntimeFactory),
            store,
        );
        let parent = host
            .start_thread(
                coords(
                    "tenant_a",
                    "user_1",
                    "checkpoint-reconciliation-read-failure",
                ),
                crate::kernel::runtime_host::ThreadTopology::root(),
            )
            .await
            .unwrap();

        let error = host
            .create_checkpoint(
                parent.context().coordinates.thread_id,
                None,
                Some("fork cut".to_string()),
                std::collections::BTreeMap::new(),
            )
            .await
            .expect_err("a failed reconciliation read cannot prove append success");
        let message = error.to_string();
        assert!(
            message.contains(if fail_after_commit {
                "checkpoint append committed before disconnect"
            } else {
                "checkpoint append failed before commit"
            }),
            "the primary append failure must remain visible: {message}"
        );
        assert!(
            message.contains("checkpoint reconciliation read failed"),
            "the reconciliation read failure must be included: {message}"
        );

        let events = inner
            .read_events(
                &crate::kernel::history::EventStreamId::for_thread(&parent.context().coordinates),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.kind == crate::kernel::history::EventKind::SessionEntryAppended
                        && event.payload["runtime_kind"].as_str() == Some("thread_checkpoint")
                })
                .count(),
            usize::from(fail_after_commit),
            "a reconciliation read failure must not add a second durable effect"
        );
        host.shutdown_all().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reserved_fork_child_start_reconciles_one_shot_append_failures() {
    for fail_after_commit in [false, true] {
        let inner = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
        let faulting = if fail_after_commit {
            crate::test_support::FaultingRuntimeStore::new(inner.clone()).fail_nth_after(
                "append",
                3,
                "fork child start append committed before disconnect",
            )
        } else {
            crate::test_support::FaultingRuntimeStore::new(inner.clone()).fail_nth(
                "append",
                3,
                "fork child start append failed before commit",
            )
        };
        let store = std::sync::Arc::new(faulting);
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(EchoRuntimeFactory),
            store.clone(),
        );
        let parent = host
            .start_thread(
                coords("tenant_a", "user_1", "fork-child-start-append-fault"),
                crate::kernel::runtime_host::ThreadTopology::root(),
            )
            .await
            .unwrap();
        let checkpoint = host
            .create_checkpoint(
                parent.context().coordinates.thread_id,
                None,
                Some("fork cut".to_string()),
                std::collections::BTreeMap::new(),
            )
            .await
            .unwrap();
        let child_thread_id = crate::kernel::runtime_host::ThreadId::new();

        let child = host
            .fork_thread_from_checkpoint_with_id(checkpoint, child_thread_id)
            .await
            .expect("a one-shot reserved child identity append fault must be reconciled");

        assert_eq!(child.context().coordinates.thread_id, child_thread_id);
        let events = inner
            .read_events(
                &crate::kernel::history::EventStreamId::for_thread(&child.context().coordinates),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.kind == crate::kernel::history::EventKind::SessionEntryAppended
                        && event.payload["runtime_kind"].as_str() == Some("thread_started")
                        && event.payload["runtime_payload"]["metadata"]["forked_from_thread_id"]
                            .as_str()
                            .is_some_and(|id| {
                                id == parent.context().coordinates.thread_id.to_string()
                            })
                })
                .count(),
            1,
            "reserved child recovery must keep one durable start identity"
        );
        host.shutdown_all().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reserved_fork_child_does_not_retry_lifecycle_history_errors() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let child_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    host.set_lifecycle_sink(Some(std::sync::Arc::new(
        HistoryFailingChildLifecycleSink {
            child_calls: std::sync::Arc::clone(&child_calls),
        },
    )))
    .await;
    let parent = host
        .start_thread(
            coords("tenant_a", "user_1", "fork-child-lifecycle-history-error"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let checkpoint = host
        .create_checkpoint(
            parent.context().coordinates.thread_id,
            None,
            Some("fork cut".to_string()),
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();

    let error = match host.fork_thread_from_checkpoint(checkpoint).await {
        Ok(_) => panic!("the controlled lifecycle sink failure must surface"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("controlled child lifecycle history failure")
    );
    assert_eq!(
        child_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a lifecycle history error is not an identity-append retry signal"
    );
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn resume_rejects_checkpoints_created_by_non_root_threads() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let parent = host
        .start_thread(
            coords("tenant_a", "user_1", "resume-parent"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let parent_thread_id = parent.context().coordinates.thread_id;

    for topology in [
        crate::kernel::runtime_host::ThreadTopology::spawned_from(parent_thread_id),
        crate::kernel::runtime_host::ThreadTopology::branch_from(parent_thread_id, None),
    ] {
        let child = host
            .start_thread(coords("tenant_a", "user_1", "resume-child"), topology)
            .await
            .unwrap();
        let child_thread_id = child.context().coordinates.thread_id;
        let checkpoint = host
            .create_checkpoint(
                child_thread_id,
                None,
                None,
                std::collections::BTreeMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            checkpoint.lineage,
            crate::kernel::runtime_host::ThreadCheckpointLineage::Parent { parent_thread_id }
        );
        host.shutdown_thread(child_thread_id).await.unwrap();

        assert!(
            matches!(
                host.resume_thread(checkpoint.id).await,
                Err(crate::kernel::runtime_host::VerletError::CheckpointResumeRequiresRoot {
                    checkpoint_id,
                    thread_id,
                    parent_thread_id: recorded_parent_thread_id,
                }) if checkpoint_id == checkpoint.id
                    && thread_id == child_thread_id
                    && recorded_parent_thread_id == parent_thread_id
            ),
            "non-root checkpoint resume must return the typed root-only error"
        );
    }

    let root_checkpoint = host
        .create_checkpoint(
            parent_thread_id,
            None,
            None,
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
    let mut legacy_value = serde_json::to_value(&root_checkpoint).unwrap();
    legacy_value.as_object_mut().unwrap().remove("lineage");
    let legacy_checkpoint: crate::kernel::runtime_host::ThreadCheckpoint =
        serde_json::from_value(legacy_value).unwrap();
    assert_eq!(
        legacy_checkpoint.lineage,
        crate::kernel::runtime_host::ThreadCheckpointLineage::Unknown
    );
    assert!(matches!(
        host.resume_thread_from_checkpoint(legacy_checkpoint).await,
        Err(crate::kernel::runtime_host::VerletError::CheckpointResumeLineageUnknown {
            checkpoint_id,
            thread_id,
        }) if checkpoint_id == root_checkpoint.id && thread_id == parent_thread_id
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn concurrent_resume_rejects_reserved_duplicate_before_store_mutation() {
    let coordinates = coords("tenant_a", "user_1", "resume-reservation");
    let thread_id = coordinates.thread_id;
    let factory = std::sync::Arc::new(BlockingThreadBuildFactory::new(thread_id));
    let store = std::sync::Arc::new(AdmissionTestStore::tracking_selects());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        factory.clone(),
        store.clone(),
    );
    let start_host = host.clone();
    let start_coordinates = coordinates.clone();
    let start = tokio::spawn(async move {
        start_host
            .start_thread(
                start_coordinates,
                crate::kernel::runtime_host::ThreadTopology::root(),
            )
            .await
    });
    factory.wait_until_blocked().await;

    let checkpoint = crate::kernel::runtime_host::ThreadCheckpoint {
        id: crate::kernel::runtime_host::ThreadCheckpointId::new(),
        coordinates,
        lineage: crate::kernel::runtime_host::ThreadCheckpointLineage::Root,
        parent_checkpoint_id: None,
        active_entry_id: None,
        label: None,
        metadata: std::collections::BTreeMap::new(),
        created_at_ms: 0,
    };
    assert!(matches!(
        host.resume_thread_from_checkpoint(checkpoint).await,
        Err(crate::kernel::runtime_host::VerletError::ThreadAlreadyExists(existing)) if existing == thread_id
    ));
    assert_eq!(
        store
            .select_branch_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );

    factory.release();
    start.await.unwrap().unwrap();
    host.shutdown_thread(thread_id).await.unwrap();
}

#[tokio::test]
async fn host_runs_multiple_tenants_and_routes_events() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let a1 = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let a2 = host
        .start_thread(
            coords("tenant_a", "user_1", "s2"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let b1 = host
        .start_thread(
            coords("tenant_b", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();

    let mut a1_events = a1.subscribe_events();
    let mut a2_events = a2.subscribe_events();
    let mut b1_events = b1.subscribe_events();

    host.submit(a1.context().coordinates.thread_id, "t1", "hello")
        .await
        .unwrap();
    host.submit(a2.context().coordinates.thread_id, "t2", "world")
        .await
        .unwrap();
    host.submit(b1.context().coordinates.thread_id, "t3", "other")
        .await
        .unwrap();

    assert_output(&mut a1_events, "t1:hello").await;
    assert_output(&mut a2_events, "t2:world").await;
    assert_output(&mut b1_events, "t3:other").await;

    let snapshot = host.snapshot().await;
    assert_eq!(snapshot.threads.len(), 3);
    assert!(
        snapshot
            .threads
            .iter()
            .any(|thread| thread.context.coordinates.tenant_id == "tenant_b")
    );
}

#[tokio::test]
async fn host_mirrors_submits_into_thread_scoped_history() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let a = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let b = host
        .start_thread(
            coords("tenant_b", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut a_events = a.subscribe_events();
    let mut b_events = b.subscribe_events();

    host.submit(a.context().coordinates.thread_id, "turn-a", "hello")
        .await
        .unwrap();
    host.submit(b.context().coordinates.thread_id, "turn-b", "world")
        .await
        .unwrap();

    assert_canonical_mirror(&mut a_events, "hello").await;
    assert_canonical_mirror(&mut b_events, "world").await;

    let a_context = a.session_context().await.unwrap();
    let b_context = host
        .session_context(b.context().coordinates.thread_id)
        .await
        .unwrap();
    assert_eq!(message_texts(&a_context.messages), vec!["hello"]);
    assert_eq!(message_texts(&b_context.messages), vec!["world"]);
    assert_ne!(
        a_context.entries[0].coordinates.tenant_id,
        b_context.entries[0].coordinates.tenant_id
    );
}

#[tokio::test]
async fn cancelling_one_thread_does_not_cancel_siblings() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let parent = host
        .start_thread(
            coords("tenant_a", "user_1", "root"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let parent_id = parent.context().coordinates.thread_id;
    let child = host
        .start_thread(
            coords("tenant_a", "user_1", "child"),
            crate::kernel::runtime_host::ThreadTopology::spawned_from(parent_id),
        )
        .await
        .unwrap();

    let mut child_events = child.subscribe_events();
    let prior_signal = parent.lifecycle_record().await.latest_signal_id;

    host.cancel(parent_id, "test cancel").await.unwrap();
    assert_ne!(
        parent.lifecycle_record().await.latest_signal_id,
        prior_signal
    );

    host.submit(
        child.context().coordinates.thread_id,
        "child-turn",
        "still running",
    )
    .await
    .unwrap();
    assert_output(&mut child_events, "child-turn:still running").await;

    let children = host.children_of(parent_id).await;
    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].context().coordinates.thread_id,
        child.context().coordinates.thread_id
    );
}

#[tokio::test]
async fn shutdown_removes_only_the_target_thread() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let a = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let b = host
        .start_thread(
            coords("tenant_b", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();

    host.shutdown_thread(a.context().coordinates.thread_id)
        .await
        .unwrap();

    assert!(
        host.get_thread(a.context().coordinates.thread_id)
            .await
            .is_err()
    );
    assert!(
        host.get_thread(b.context().coordinates.thread_id)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn shutdown_all_drains_every_thread() {
    let host =
        crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
    let a = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let b = host
        .start_thread(
            coords("tenant_a", "user_1", "s2"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();

    let stopped = host.shutdown_all().await.unwrap();

    assert_eq!(stopped.len(), 2);
    assert!(
        stopped
            .iter()
            .any(|thread_id| *thread_id == a.context().coordinates.thread_id)
    );
    assert!(
        stopped
            .iter()
            .any(|thread_id| *thread_id == b.context().coordinates.thread_id)
    );
    assert!(host.snapshot().await.threads.is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn stale_shutdown_cleanup_does_not_remove_same_id_replacement() {
    let thread_id =
        crate::kernel::runtime_host::ThreadId::parse_str("00000000-0000-0000-0000-000000000051")
            .unwrap();
    let coordinates = crate::kernel::runtime_host::ThreadCoordinates {
        tenant_id: "tenant_a".to_string(),
        user_id: "user_1".to_string(),
        session_id: "stale-shutdown-cleanup".to_string(),
        thread_id,
    };
    let factory = std::sync::Arc::new(GatedShutdownFactory::default());
    let host = crate::kernel::runtime_host::RuntimeHost::new(factory.clone());
    let old = host
        .start_thread(
            coordinates.clone(),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    wait_for_status(&old, crate::kernel::runtime_host::ThreadStatus::Idle).await;

    let shutdown_host = host.clone();
    let shutdown = tokio::spawn(async move { shutdown_host.shutdown_thread(thread_id).await });
    factory.shutdown_received.notified().await;

    let removed = host
        .inner
        .threads
        .write()
        .await
        .remove(&thread_id)
        .expect("old runtime must still be registered");
    assert!(std::sync::Arc::ptr_eq(&removed, &old.thread));
    let replacement = host
        .start_thread(
            coordinates,
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    wait_for_status(
        &replacement,
        crate::kernel::runtime_host::ThreadStatus::Idle,
    )
    .await;

    factory.release_shutdown.notify_one();
    shutdown.await.unwrap().unwrap();

    let resident = host
        .get_thread(thread_id)
        .await
        .expect("stale shutdown cleanup removed the replacement runtime");
    assert!(std::sync::Arc::ptr_eq(
        &resident.thread,
        &replacement.thread
    ));
    host.shutdown_thread(thread_id).await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn shutdown_all_uses_repeatable_children_before_parent_effect_order() {
    let parent_thread_id =
        crate::kernel::runtime_host::ThreadId::parse_str("00000000-0000-0000-0000-000000000001")
            .unwrap();
    let first_child_thread_id =
        crate::kernel::runtime_host::ThreadId::parse_str("00000000-0000-0000-0000-000000000002")
            .unwrap();
    let second_child_thread_id =
        crate::kernel::runtime_host::ThreadId::parse_str("00000000-0000-0000-0000-000000000003")
            .unwrap();
    let expected_order = vec![
        first_child_thread_id,
        second_child_thread_id,
        parent_thread_id,
    ];
    let expected_finished = vec![first_child_thread_id, second_child_thread_id];

    for run in 0..32 {
        let host =
            crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(EchoRuntimeFactory));
        let parent = host
            .start_thread(
                crate::kernel::runtime_host::ThreadCoordinates {
                    tenant_id: "tenant_a".to_string(),
                    user_id: "user_1".to_string(),
                    session_id: format!("shutdown-order-{run}"),
                    thread_id: parent_thread_id,
                },
                crate::kernel::runtime_host::ThreadTopology::root(),
            )
            .await
            .unwrap();
        host.start_thread(
            crate::kernel::runtime_host::ThreadCoordinates {
                tenant_id: "tenant_a".to_string(),
                user_id: "user_1".to_string(),
                session_id: format!("shutdown-order-{run}"),
                thread_id: second_child_thread_id,
            },
            crate::kernel::runtime_host::ThreadTopology::spawned_from(parent_thread_id),
        )
        .await
        .unwrap();
        host.start_thread(
            crate::kernel::runtime_host::ThreadCoordinates {
                tenant_id: "tenant_a".to_string(),
                user_id: "user_1".to_string(),
                session_id: format!("shutdown-order-{run}"),
                thread_id: first_child_thread_id,
            },
            crate::kernel::runtime_host::ThreadTopology::spawned_from(parent_thread_id),
        )
        .await
        .unwrap();
        let mut parent_events = parent.subscribe_events();

        assert_eq!(host.shutdown_all().await.unwrap(), expected_order);

        let mut finished = Vec::new();
        while let Ok(event) = parent_events.try_recv() {
            if let crate::kernel::runtime_host::ThreadEvent::Runtime {
                event:
                    crate::kernel::runtime_host::RuntimeEvent {
                        kind:
                            crate::kernel::runtime_host::RuntimeEventKind::SubthreadFinished {
                                child_thread_id,
                                ..
                            },
                        ..
                    },
                ..
            } = event
            {
                finished.push(child_thread_id);
            }
        }
        assert_eq!(finished, expected_finished, "shutdown run {run}");
    }
}

async fn assert_output(
    events: &mut tokio::sync::broadcast::Receiver<crate::kernel::runtime_host::ThreadEvent>,
    expected: &str,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::ThreadEvent::Output { text, .. } = event {
            assert_eq!(text, expected);
            return;
        }
    }
}

async fn next_output(
    events: &mut tokio::sync::broadcast::Receiver<crate::kernel::runtime_host::ThreadEvent>,
) -> String {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::ThreadEvent::Output { text, .. } = event {
            return text;
        }
    }
}

async fn assert_canonical_mirror(
    events: &mut tokio::sync::broadcast::Receiver<crate::kernel::runtime_host::ThreadEvent>,
    expected_text: &str,
) {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::ThreadEvent::CanonicalMirror { entry, .. } = event {
            match entry.kind {
                crate::kernel::history::SessionEntryKind::Message { message } => {
                    assert_eq!(message_texts(&[message]), vec![expected_text]);
                }
                other => panic!("unexpected canonical mirror entry: {other:?}"),
            }
            return;
        }
    }
}

fn message_texts(messages: &[crate::kernel::history::CanonicalMessage]) -> Vec<&str> {
    messages
        .iter()
        .map(|message| match message {
            crate::kernel::history::CanonicalMessage::User { content, .. }
            | crate::kernel::history::CanonicalMessage::Assistant { content, .. }
            | crate::kernel::history::CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    crate::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or(""),
        })
        .collect()
}

async fn policy_bound_content_hash_for_config(config: serde_json::Value) -> String {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let mut coupling = runtime_std_context_spill_coupling();
    coupling.id = "test.policy".to_string();
    coupling.function_ref = format!("op://policy/check@sha256:{}", "d".repeat(64));
    coupling.config = config;
    coupling.config_hash = "sha256:test-config".to_string();
    let coupling_set =
        crate::agent::manifest_bind::BoundCouplingSet::new("snapshot-a", vec![coupling]);
    let metadata = std::collections::BTreeMap::from([(
        crate::kernel::runtime_host::THREAD_BOUND_COUPLING_SET_METADATA.to_string(),
        serde_json::to_string(&coupling_set).unwrap(),
    )]);
    let thread = host
        .start_thread_with_topology_and_metadata(
            crate::kernel::runtime_host::ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            crate::kernel::runtime_host::ThreadTopology::root(),
            metadata,
        )
        .await
        .unwrap();

    thread
        .record_manifest_receipts(
            serde_json::json!({
                "ref_uri": "agent://policy@0.1.0",
                "manifest_hash": "snapshot-a",
                "source_hash": "sha256:source"
            }),
            serde_json::json!({
                "ref_uri": "agent://policy@0.1.0",
                "manifest_hash": "snapshot-a"
            }),
        )
        .await
        .unwrap();

    let events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let bind_position = events
        .iter()
        .position(|event| event.kind == crate::kernel::history::EventKind::ManifestBindCompleted)
        .unwrap();
    let policy_position = events
        .iter()
        .position(|event| event.kind == crate::kernel::history::EventKind::PolicyBound)
        .unwrap();
    assert!(bind_position < policy_position);
    let policy = &events[policy_position];
    assert_eq!(
        policy.payload["schema"],
        crate::kernel::history::EventKind::PolicyBound.payload_schema_id()
    );
    assert_eq!(policy.payload["policy_kind"], "coupling_set");
    assert_eq!(policy.payload["policy_id"], "coupling_set:snapshot-a");
    policy.payload["content_hash"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn manifest_bind_receipt_and_placement_witness_share_one_atomic_append() {
    let store = std::sync::Arc::new(
        crate::test_support::FaultingRuntimeStore::new(std::sync::Arc::new(
            crate::kernel::history::InMemorySessionStore::new(),
        ))
        .fail_nth(
            "append_events",
            2,
            "a second manifest append must not occur",
        ),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "placement-atomic"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();

    thread
        .record_manifest_receipts(
            serde_json::json!({
                "ref_uri": "agent://placement@0.1.0",
                "manifest_hash": "snapshot-placement",
                "source_hash": "sha256:source"
            }),
            serde_json::json!({
                "ref_uri": "agent://placement@0.1.0",
                "manifest_hash": "snapshot-placement",
                "placement": {"target": "local"}
            }),
        )
        .await
        .unwrap();

    assert_eq!(store.call_count("append_events"), 1);
    let events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == crate::kernel::history::EventKind::ManifestBindCompleted)
            .count(),
        1
    );
    let placement_events = events
        .iter()
        .filter(|event| event.kind == crate::kernel::history::EventKind::PlacementDecision)
        .collect::<Vec<_>>();
    assert_eq!(placement_events.len(), 1);
    assert_eq!(
        placement_events[0].origin,
        crate::kernel::history::EventOrigin::Witnessed
    );
    assert_eq!(placement_events[0].payload["placement"], "local");
    assert_eq!(
        placement_events[0].payload["snapshot_id"],
        "snapshot-placement"
    );
}

#[tokio::test]
async fn cancelled_manifest_receipt_caller_cannot_leave_a_half_witnessed_workspace() {
    let barrier = std::sync::Arc::new(AdmissionAppendBarrier::default());
    let store = std::sync::Arc::new(AdmissionTestStore::blocking_manifest(barrier.clone()));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "workspace-bind-cancelled"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let coordinates = thread.context().coordinates.clone();
    let recorder = tokio::spawn(async move {
        thread
            .record_manifest_receipts(
                serde_json::json!({
                    "ref_uri": "agent://workspace@0.1.0",
                    "manifest_hash": "snapshot-workspace",
                    "source_hash": "sha256:source"
                }),
                serde_json::json!({
                    "ref_uri": "agent://workspace@0.1.0",
                    "manifest_hash": "snapshot-workspace",
                    "placement": {"target": "local"},
                    "workspace": {
                        "guest_path": "/work",
                        "host_path": "/tmp/workspace-bind-cancelled",
                        "mode": "rw"
                    }
                }),
            )
            .await
    });

    barrier.wait_for_entries(1).await;
    recorder.abort();
    assert!(recorder.await.unwrap_err().is_cancelled());
    barrier.release();

    let stream_id = crate::kernel::history::EventStreamId::for_thread(&coordinates);
    let events = tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            let events = store.read_events(&stream_id, None).await.unwrap();
            if events
                .iter()
                .any(|event| event.kind == crate::kernel::history::EventKind::ManifestBindCompleted)
            {
                break events;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shielded manifest append did not finish after caller cancellation");
    let bind = events
        .iter()
        .find(|event| event.kind == crate::kernel::history::EventKind::ManifestBindCompleted)
        .unwrap();
    assert_eq!(bind.payload["workspace"]["guest_path"], "/work");
    assert!(
        events
            .iter()
            .any(|event| event.kind == crate::kernel::history::EventKind::PlacementDecision),
        "the placement witness must commit in the same append"
    );
}

#[tokio::test]
async fn remote_manifest_receipt_rejects_a_workspace_before_witnessing_it() {
    let store = std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new());
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "remote-workspace-bind"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let coordinates = thread.context().coordinates.clone();

    let error = thread
        .record_remote_manifest_receipts(
            serde_json::json!({
                "ref_uri": "agent://workspace@0.1.0",
                "manifest_hash": "snapshot-workspace",
                "source_hash": "sha256:source"
            }),
            serde_json::json!({
                "ref_uri": "agent://workspace@0.1.0",
                "manifest_hash": "snapshot-workspace",
                "placement": {
                    "target": "remote",
                    "executor_ref": "executor://cluster/default"
                },
                "workspace": {
                    "guest_path": "/work",
                    "host_path": "/tmp/remote-workspace-bind",
                    "mode": "rw"
                }
            }),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("require local placement"));
    assert!(
        store
            .read_events(
                &crate::kernel::history::EventStreamId::for_thread(&coordinates),
                None
            )
            .await
            .unwrap()
            .iter()
            .all(|event| event.kind != crate::kernel::history::EventKind::ManifestBindCompleted)
    );
}

#[tokio::test]
async fn failed_manifest_batch_leaves_no_bind_receipt_without_placement_witness() {
    let store = std::sync::Arc::new(
        crate::test_support::FaultingRuntimeStore::new(std::sync::Arc::new(
            crate::kernel::history::InMemorySessionStore::new(),
        ))
        .fail_nth("append_events", 1, "manifest batch failed"),
    );
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "placement-failed"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();

    let err = thread
        .record_manifest_receipts(
            serde_json::json!({"manifest_hash": "snapshot-placement"}),
            serde_json::json!({
                "manifest_hash": "snapshot-placement",
                "placement": {"target": "local"}
            }),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("manifest batch failed"));

    let events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .all(|event| event.kind != crate::kernel::history::EventKind::ManifestBindCompleted)
    );
    assert!(
        events
            .iter()
            .all(|event| event.kind != crate::kernel::history::EventKind::PlacementDecision)
    );
}

#[tokio::test]
async fn manifest_receipt_append_fails_closed_for_non_local_placement() {
    let store = std::sync::Arc::new(crate::test_support::FaultingRuntimeStore::new(
        std::sync::Arc::new(crate::kernel::history::InMemorySessionStore::new()),
    ));
    let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
        std::sync::Arc::new(EchoRuntimeFactory),
        store.clone(),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "placement-fail-closed"),
            crate::kernel::runtime_host::ThreadTopology::root(),
        )
        .await
        .unwrap();

    let err = thread
        .record_manifest_receipts(
            serde_json::json!({"manifest_hash": "snapshot-placement"}),
            serde_json::json!({
                "manifest_hash": "snapshot-placement",
                "placement": {"target": "remote"}
            }),
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("placement target remote"));
    assert!(err.to_string().contains("remote EventStore backend"));
    assert_eq!(store.call_count("append_events"), 0);
    let events = store
        .read_events(
            &crate::kernel::history::EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(events.iter().all(|event| {
        !matches!(
            event.kind,
            crate::kernel::history::EventKind::ManifestBindCompleted
                | crate::kernel::history::EventKind::PlacementDecision
        )
    }));
}

async fn assert_runtime_kind(
    events: &mut tokio::sync::broadcast::Receiver<crate::kernel::runtime_host::ThreadEvent>,
    predicate: impl Fn(&crate::kernel::runtime_host::RuntimeEventKind) -> bool,
) -> crate::kernel::runtime_host::RuntimeEvent {
    loop {
        let event = tokio::time::timeout(tokio::time::Duration::from_secs(30), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let crate::kernel::runtime_host::ThreadEvent::Runtime { event, .. } = event
            && predicate(&event.kind)
        {
            return event;
        }
    }
}

fn canonical_user_content(
    context: &crate::kernel::history::SessionContext,
) -> Vec<Vec<crate::kernel::history::CanonicalContent>> {
    context
        .entries
        .iter()
        .filter_map(|entry| match &entry.kind {
            crate::kernel::history::SessionEntryKind::Message {
                message: crate::kernel::history::CanonicalMessage::User { content, .. },
            } => Some(content.clone()),
            _ => None,
        })
        .collect()
}

#[derive(Clone)]
struct RuntimeFakeClock {
    now: std::sync::Arc<std::sync::Mutex<chrono::DateTime<chrono::Utc>>>,
}

impl RuntimeFakeClock {
    fn new(now: chrono::DateTime<chrono::Utc>) -> Self {
        Self {
            now: std::sync::Arc::new(std::sync::Mutex::new(now)),
        }
    }
}

impl crate::DaemonClock for RuntimeFakeClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        *self.now.lock().unwrap()
    }
}

struct WitnessTimerFiredSink {
    store: crate::SqliteSessionStore,
    coordinates: crate::kernel::runtime_host::ThreadCoordinates,
}

#[async_trait::async_trait]
impl verlet_io_core::IngressSink for WitnessTimerFiredSink {
    async fn submit(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
        let verlet_io_core::IngressContent::Event { kind, payload } = &envelope.content else {
            return Err(verlet_io_core::IoError::Bridge(
                "clock route emitted non-event ingress".to_string(),
            ));
        };
        if kind != "timer.fired" {
            return Err(verlet_io_core::IoError::Bridge(format!(
                "clock route emitted unexpected event kind {kind:?}"
            )));
        }
        let timer =
            serde_json::from_value::<crate::kernel::history::TimerFiredPayload>(payload.clone())
                .map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!("invalid timer.fired payload: {err}"))
                })?;
        let mandate_event_id = timer.mandate_event_id;
        let control_stream = crate::kernel::control_decision::control_stream_id(&self.coordinates);
        let mut record = crate::kernel::history::NewEventRecord::witnessed(
            self.coordinates.clone(),
            crate::kernel::history::EventKind::TimerFired,
            serde_json::to_value(timer).map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("encode timer.fired payload: {err}"))
            })?,
        );
        record.provenance = crate::kernel::history::EventProvenance {
            source_streams: vec![control_stream.clone()],
            source_event_ids: vec![mandate_event_id],
            ..crate::kernel::history::EventProvenance::default()
        };
        self.store
            .append_events(&control_stream, vec![record])
            .await
            .map_err(|err| verlet_io_core::IoError::Bridge(format!("append timer.fired: {err}")))?;
        Ok(verlet_io_core::IngressAck::accepted(&envelope))
    }
}

async fn append_scheduled_loop_mandate(
    store: &crate::SqliteSessionStore,
    coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
    loop_id: &str,
    created_at: chrono::DateTime<chrono::Utc>,
) -> crate::kernel::history::EventRecord {
    let mut record = crate::kernel::history::NewEventRecord::witnessed(
        coordinates.clone(),
        crate::kernel::history::EventKind::MandateStarted,
        serde_json::to_value(crate::kernel::control_decision::MandateStartedPayload {
            subject: crate::kernel::control_decision::MandateSubject {
                thread_id: Some(coordinates.thread_id.to_string()),
                loop_id: Some(loop_id.to_string()),
            },
            mandate_id: format!("mandate-{loop_id}"),
            snapshot_id: "schedule.v1".to_string(),
            thread_id: Some(coordinates.thread_id.to_string()),
            max_continuations: None,
            expires_at_ms: None,
            schedule: Some(
                crate::kernel::control_decision::MandateSchedulePayload::Interval {
                    every_ms: 60_000,
                },
            ),
            max_occurrences: Some(2),
            catch_up: Some(crate::kernel::control_decision::MandateCatchUpPolicy::SkipMissed),
            input_template: Some("wake at {scheduled_for}".to_string()),
        })
        .unwrap(),
    );
    record.created_at_ms = created_at.timestamp_millis();
    store
        .append_events(
            &crate::kernel::control_decision::control_stream_id(coordinates),
            vec![record],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn append_loop_parent_completed(
    store: &crate::kernel::history::InMemorySessionStore,
    coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
    turn_id: &str,
) -> crate::kernel::history::EventRecord {
    store
        .append_events(
            &crate::kernel::history::EventStreamId::for_thread(coordinates),
            vec![crate::kernel::history::NewEventRecord::witnessed(
                coordinates.clone(),
                crate::kernel::history::EventKind::TurnCompleted,
                serde_json::json!({ "turn_id": turn_id }),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn append_loop_mandate_started(
    store: &crate::kernel::history::InMemorySessionStore,
    coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
    loop_id: &str,
    snapshot_id: &str,
    max_continuations: Option<u32>,
) {
    store
        .append_events(
            &crate::kernel::control_decision::control_stream_id(coordinates),
            vec![crate::kernel::history::NewEventRecord::witnessed(
                coordinates.clone(),
                crate::kernel::history::EventKind::MandateStarted,
                serde_json::to_value(crate::kernel::control_decision::MandateStartedPayload {
                    subject: crate::kernel::control_decision::MandateSubject {
                        thread_id: None,
                        loop_id: Some(loop_id.to_string()),
                    },
                    mandate_id: format!("mandate-{loop_id}"),
                    snapshot_id: snapshot_id.to_string(),
                    thread_id: Some(coordinates.thread_id.to_string()),
                    max_continuations,
                    expires_at_ms: None,
                    schedule: None,
                    max_occurrences: None,
                    catch_up: None,
                    input_template: None,
                })
                .unwrap(),
            )],
        )
        .await
        .unwrap();
}

async fn append_loop_continue_request(
    store: &crate::kernel::history::InMemorySessionStore,
    coordinates: &crate::kernel::runtime_host::ThreadCoordinates,
    parent_event_id: crate::kernel::history::EventRecordId,
    loop_id: &str,
    parent_turn_id: &str,
    snapshot_id: &str,
    next_turn_input: &str,
) -> crate::kernel::history::EventRecord {
    store
        .append_events(
            &crate::kernel::control_decision::control_stream_id(coordinates),
            vec![crate::kernel::history::NewEventRecord::discharged(
                coordinates.clone(),
                crate::kernel::history::EventKind::TurnContinueRequested,
                serde_json::to_value(
                    crate::kernel::control_decision::TurnContinueRequestedPayload {
                        subject: crate::kernel::control_decision::TurnContinuationSubject {
                            loop_id: loop_id.to_string(),
                            parent_turn_id: parent_turn_id.to_string(),
                        },
                        snapshot_id: snapshot_id.to_string(),
                        next_turn_input: next_turn_input.to_string(),
                    },
                )
                .unwrap(),
                crate::kernel::history::EventProvenance {
                    source_streams: vec![crate::kernel::history::EventStreamId::for_thread(
                        coordinates,
                    )],
                    source_event_ids: vec![parent_event_id],
                    discharged_by: Some("coupling:loop-test".to_string()),
                    function: Some("op://test/loop@sha256:test".to_string()),
                    ..crate::kernel::history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn wait_for_status(
    thread: &crate::kernel::runtime_host::RuntimeThreadHandle,
    expected: crate::kernel::runtime_host::ThreadStatus,
) {
    let mut status = thread.subscribe_status();
    tokio::time::timeout(tokio::time::Duration::from_secs(30), async {
        loop {
            if *status.borrow() == expected {
                return;
            }
            status.changed().await.expect("status channel closed");
        }
    })
    .await
    .expect("status timed out");
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TsAgentThreadFixture {
    tenant_id: String,
    user_id: String,
    session_id: String,
    thread_id: String,
    parent_thread_id: Option<String>,
    status: String,
    latest_signal_id: Option<String>,
    latest_checkpoint_id: Option<String>,
    metadata: std::collections::BTreeMap<String, String>,
}

impl TsAgentThreadFixture {
    fn from_lifecycle(record: &crate::kernel::runtime_host::ThreadLifecycleRecord) -> Self {
        Self {
            tenant_id: record.coordinates.tenant_id.clone(),
            user_id: record.coordinates.user_id.clone(),
            session_id: record.coordinates.session_id.clone(),
            thread_id: record.coordinates.thread_id.to_string(),
            parent_thread_id: record.parent_thread_id.map(|id| id.to_string()),
            status: record.status.as_ref().to_string(),
            latest_signal_id: record.latest_signal_id.map(|id| id.to_string()),
            latest_checkpoint_id: record.latest_checkpoint_id.map(|id| id.to_string()),
            metadata: record.metadata.clone(),
        }
    }

    fn into_lifecycle(self) -> crate::kernel::runtime_host::ThreadLifecycleRecord {
        let coordinates = crate::kernel::runtime_host::ThreadCoordinates {
            tenant_id: self.tenant_id,
            user_id: self.user_id,
            session_id: self.session_id,
            thread_id: crate::kernel::runtime_host::ThreadId::parse_str(&self.thread_id).unwrap(),
        };
        let status: crate::kernel::runtime_host::ThreadLifecycleStatus =
            self.status.parse().unwrap();
        crate::kernel::runtime_host::ThreadLifecycleRecord {
            coordinates,
            parent_thread_id: self
                .parent_thread_id
                .map(|id| crate::kernel::runtime_host::ThreadId::parse_str(&id).unwrap()),
            topology: crate::kernel::runtime_host::ThreadTopology::root(),
            status,
            latest_signal_id: self.latest_signal_id.map(|id| {
                crate::kernel::runtime_host::ThreadSignalId::from_uuid(
                    uuid::Uuid::parse_str(&id).unwrap(),
                )
            }),
            latest_checkpoint_id: self.latest_checkpoint_id.map(|id| {
                crate::kernel::runtime_host::ThreadCheckpointId::from_uuid(
                    uuid::Uuid::parse_str(&id).unwrap(),
                )
            }),
            created_at_ms: 0,
            updated_at_ms: 0,
            metadata: self.metadata,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TsAgentThreadSignalFixture {
    signal_id: String,
    tenant_id: String,
    user_id: String,
    session_id: String,
    thread_id: String,
    kind: String,
    metadata: std::collections::BTreeMap<String, String>,
}

impl TsAgentThreadSignalFixture {
    fn from_signal(signal: &crate::kernel::runtime_host::ThreadSignal) -> Self {
        Self {
            signal_id: signal.id.to_string(),
            tenant_id: signal.coordinates.tenant_id.clone(),
            user_id: signal.coordinates.user_id.clone(),
            session_id: signal.coordinates.session_id.clone(),
            thread_id: signal.coordinates.thread_id.to_string(),
            kind: signal.kind.as_ref().to_string(),
            metadata: signal.metadata.clone(),
        }
    }

    fn into_signal(self) -> crate::kernel::runtime_host::ThreadSignal {
        let kind: crate::kernel::runtime_host::ThreadSignalKind = self.kind.parse().unwrap();
        crate::kernel::runtime_host::ThreadSignal {
            id: crate::kernel::runtime_host::ThreadSignalId::from_uuid(
                uuid::Uuid::parse_str(&self.signal_id).unwrap(),
            ),
            coordinates: crate::kernel::runtime_host::ThreadCoordinates {
                tenant_id: self.tenant_id,
                user_id: self.user_id,
                session_id: self.session_id,
                thread_id: crate::kernel::runtime_host::ThreadId::parse_str(&self.thread_id)
                    .unwrap(),
            },
            kind,
            metadata: self.metadata,
            created_at_ms: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TsAgentThreadCheckpointFixture {
    checkpoint_id: String,
    tenant_id: String,
    user_id: String,
    session_id: String,
    thread_id: String,
    lineage: crate::kernel::runtime_host::ThreadCheckpointLineage,
    parent_checkpoint_id: Option<String>,
    active_entry_id: Option<String>,
    label: Option<String>,
    metadata: std::collections::BTreeMap<String, String>,
}

impl TsAgentThreadCheckpointFixture {
    fn from_checkpoint(checkpoint: &crate::kernel::runtime_host::ThreadCheckpoint) -> Self {
        Self {
            checkpoint_id: checkpoint.id.to_string(),
            tenant_id: checkpoint.coordinates.tenant_id.clone(),
            user_id: checkpoint.coordinates.user_id.clone(),
            session_id: checkpoint.coordinates.session_id.clone(),
            thread_id: checkpoint.coordinates.thread_id.to_string(),
            lineage: checkpoint.lineage,
            parent_checkpoint_id: checkpoint.parent_checkpoint_id.map(|id| id.to_string()),
            active_entry_id: checkpoint.active_entry_id.map(|id| id.to_string()),
            label: checkpoint.label.clone(),
            metadata: checkpoint.metadata.clone(),
        }
    }

    fn into_checkpoint(self) -> crate::kernel::runtime_host::ThreadCheckpoint {
        crate::kernel::runtime_host::ThreadCheckpoint {
            id: crate::kernel::runtime_host::ThreadCheckpointId::from_uuid(
                uuid::Uuid::parse_str(&self.checkpoint_id).unwrap(),
            ),
            coordinates: crate::kernel::runtime_host::ThreadCoordinates {
                tenant_id: self.tenant_id,
                user_id: self.user_id,
                session_id: self.session_id,
                thread_id: crate::kernel::runtime_host::ThreadId::parse_str(&self.thread_id)
                    .unwrap(),
            },
            lineage: self.lineage,
            parent_checkpoint_id: self.parent_checkpoint_id.map(|id| {
                crate::kernel::runtime_host::ThreadCheckpointId::from_uuid(
                    uuid::Uuid::parse_str(&id).unwrap(),
                )
            }),
            active_entry_id: self.active_entry_id.map(|id| {
                crate::kernel::history::SessionEntryId::from_uuid(
                    uuid::Uuid::parse_str(&id).unwrap(),
                )
            }),
            label: self.label,
            metadata: self.metadata,
            created_at_ms: 0,
        }
    }
}

#[test]
fn ts_style_lifecycle_thread_fixture_round_trips_core_fields_and_metadata() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let parent_thread_id = crate::kernel::runtime_host::ThreadId::new();
    let record = crate::kernel::runtime_host::ThreadLifecycleRecord {
        coordinates: coordinates.clone(),
        parent_thread_id: Some(parent_thread_id),
        topology: crate::kernel::runtime_host::ThreadTopology::spawned_from(parent_thread_id),
        status: crate::kernel::runtime_host::ThreadLifecycleStatus::Running,
        latest_signal_id: Some(crate::kernel::runtime_host::ThreadSignalId::new()),
        latest_checkpoint_id: Some(crate::kernel::runtime_host::ThreadCheckpointId::new()),
        created_at_ms: 10,
        updated_at_ms: 20,
        metadata: std::collections::BTreeMap::from([
            ("auth_subject".to_string(), "telegram:123".to_string()),
            ("billing_ledger_id".to_string(), "ledger_456".to_string()),
        ]),
    };

    let fixture = TsAgentThreadFixture::from_lifecycle(&record);
    let roundtrip = fixture.into_lifecycle();

    assert_eq!(roundtrip.coordinates, record.coordinates);
    assert_eq!(roundtrip.parent_thread_id, record.parent_thread_id);
    assert_eq!(roundtrip.status, record.status);
    assert_eq!(roundtrip.latest_signal_id, record.latest_signal_id);
    assert_eq!(roundtrip.latest_checkpoint_id, record.latest_checkpoint_id);
    assert_eq!(roundtrip.metadata, record.metadata);
}

#[test]
fn ts_style_signal_and_checkpoint_fixtures_round_trip_without_product_fields() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let parent_thread_id = crate::kernel::runtime_host::ThreadId::new();
    let signal = crate::kernel::runtime_host::ThreadSignal::user_steer(&coordinates, "turn-1")
        .with_metadata(std::collections::BTreeMap::from([(
            "source_message_id".to_string(),
            "msg_123".to_string(),
        )]));
    let checkpoint = crate::kernel::runtime_host::ThreadCheckpoint {
        id: crate::kernel::runtime_host::ThreadCheckpointId::new(),
        coordinates: coordinates.clone(),
        lineage: crate::kernel::runtime_host::ThreadCheckpointLineage::Parent { parent_thread_id },
        parent_checkpoint_id: Some(crate::kernel::runtime_host::ThreadCheckpointId::new()),
        active_entry_id: Some(crate::kernel::history::SessionEntryId::new()),
        label: Some("after-tool".to_string()),
        metadata: std::collections::BTreeMap::from([(
            "app_checkpoint_id".to_string(),
            "app_ckpt".to_string(),
        )]),
        created_at_ms: 30,
    };

    let signal_roundtrip = TsAgentThreadSignalFixture::from_signal(&signal).into_signal();
    let checkpoint_roundtrip =
        TsAgentThreadCheckpointFixture::from_checkpoint(&checkpoint).into_checkpoint();

    assert_eq!(signal_roundtrip.id, signal.id);
    assert_eq!(signal_roundtrip.coordinates, signal.coordinates);
    assert_eq!(signal_roundtrip.kind, signal.kind);
    assert_eq!(signal_roundtrip.metadata, signal.metadata);
    assert_eq!(checkpoint_roundtrip.id, checkpoint.id);
    assert_eq!(checkpoint_roundtrip.coordinates, checkpoint.coordinates);
    assert_eq!(checkpoint_roundtrip.lineage, checkpoint.lineage);
    assert_eq!(
        checkpoint_roundtrip.parent_checkpoint_id,
        checkpoint.parent_checkpoint_id
    );
    assert_eq!(
        checkpoint_roundtrip.active_entry_id,
        checkpoint.active_entry_id
    );
    assert_eq!(checkpoint_roundtrip.label, checkpoint.label);
    assert_eq!(checkpoint_roundtrip.metadata, checkpoint.metadata);
}
