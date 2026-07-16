use super::*;
use crate::agent::hooks::{HookEventName, HookRunStatus};
use crate::agent::manifest_bind::{
    BoundCoupling, BoundCouplingFunction, BoundCouplingSelector, BoundCouplingSet,
    BoundCouplingSink, CouplingRole,
};
use crate::agent::manifest_schema::{AgentManifestCouplingBudget, AgentManifestCouplingQuota};
use crate::kernel::context_compiler::AgentContextCompilationDiagnostics;
use crate::kernel::control_decision::{
    MandateCatchUpPolicy, MandateSchedulePayload, MandateStartedPayload, MandateSubject,
    TurnContinuationAcceptedPayload, TurnContinuationSubject, TurnContinueRequestedPayload,
    control_stream_id,
};
use crate::kernel::history::{
    CanonicalContent,
    CanonicalMessage,
    CanonicalStopReason,
    EventKind,
    EventOrigin,
    EventProvenance,
    EventRecord,
    EventRecordId,
    EventSequence,
    EventStore,
    EventStreamId,
    HistoryResult,
    NewEventRecord,
    NewObservationRecord,
    ObservationRecord,
    // lexicon-allow: observation_store - test wrapper must implement the history observation trait
    ObservationStore,
    ProviderApi,
    SessionEntry,
    SessionEntryId,
    SessionEntryKind,
    SessionStore,
    ThreadBaseRef,
    ThreadForkReason,
    TimerFiredPayload,
};
use crate::test_support::FaultingRuntimeStore;
use crate::{
    CanonicalProviderRuntimeConfig, CanonicalProviderRuntimeFactory, CooldisDaemonClockRoute,
    CouplingScheduler, DaemonClock, LocalOfflineProviderClient, SqliteSessionStore,
    StdlibCouplingExecutor,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use cooldis_io_core::{
    IngressAck, IngressContent, IngressEnvelope, IngressSink, IoError, IoResult,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[test]
fn thread_topology_distinguishes_spawn_attribution_from_branch_lineage() {
    let source_thread_id = ThreadId::new();
    let checkpoint_id = ThreadCheckpointId::new();

    let spawned = ThreadTopology::spawned_from(source_thread_id);
    assert_eq!(
        spawned.initiation,
        ThreadInitiationSource::Thread {
            thread_id: source_thread_id,
            turn_id: None,
            event_id: None,
        }
    );
    assert_eq!(spawned.lineage, ThreadLineage::Root);
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

    let branched = ThreadTopology::branch_from(source_thread_id, Some(checkpoint_id));
    assert_eq!(branched.initiation, ThreadInitiationSource::Root);
    assert_eq!(
        branched.lineage,
        ThreadLineage::Branch {
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
    let cancellation = CancellationToken::new();
    let input = TurnInput::text("hello")
        .with_cwd("/tmp/cooldis-turn")
        .with_workspace_root("/workspace")
        .with_model("gpt-test")
        .with_provider("openai")
        .with_permission_profile("workspace-write")
        .with_provider_metadata("region", "us")
        .with_metadata("source", "unit-test");
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let context = TurnContext::new(
        ThreadContext::root(coordinates.clone()),
        "turn-1",
        &input,
        cancellation.clone(),
    )
    .with_budget(TurnBudget {
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
    let store = Arc::new(InMemorySessionStore::new());
    let services = RuntimeServices::new(store.clone(), RuntimeExecutionPolicy::default())
        .with_bound_coupling_set(BoundCouplingSet::new(
            "snapshot-a",
            vec![runtime_std_context_spill_coupling()],
        ));
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
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

    let derived_stream = EventStreamId::new(format!("derived:context:{}", coordinates.thread_id));
    let derived_events = store.read_events(&derived_stream, None).await.unwrap();
    let summary = derived_events
        .iter()
        .find(|event| event.kind == EventKind::ContextSummaryCompleted)
        .unwrap();
    let read_plan = derived_events
        .iter()
        .find(|event| event.kind == EventKind::ContextReadPlanSet)
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

    let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let control_events = store.read_events(&control_stream, None).await.unwrap();
    let receipt = control_events
        .iter()
        .find(|event| event.kind == EventKind::CouplingRunCompleted)
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
    let store = Arc::new(InMemorySessionStore::new());
    let services = RuntimeServices::new(store.clone(), RuntimeExecutionPolicy::default());
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let session_entry = services
        .append_user_message(&coordinates, "hello")
        .await
        .unwrap();
    let stream_id = EventStreamId::for_thread(&coordinates);
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
        .find(|event| event.kind == EventKind::ContextCompileCompleted)
        .unwrap();
    assert_eq!(compile_event.origin, EventOrigin::Discharged);
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
    let store = Arc::new(InMemorySessionStore::new());
    let services = RuntimeServices::new(store.clone(), RuntimeExecutionPolicy::default());
    let parent = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let child = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let parent_entry = services
        .append_user_message(&parent, "parent")
        .await
        .unwrap();
    let parent_stream = EventStreamId::for_thread(&parent);
    let child_stream = EventStreamId::for_thread(&child);
    store
        .fork_by_reference(
            &parent,
            &child,
            ThreadBaseRef {
                child_thread_id: child.thread_id,
                parent_thread_id: parent.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(parent_entry.entry_id),
                parent_stream_id: parent_stream.clone(),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: ThreadForkReason::ManifestUpdate,
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
        .find(|event| event.kind == EventKind::ContextCompileCompleted)
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

fn runtime_std_context_spill_coupling() -> BoundCoupling {
    BoundCoupling {
        id: "std::context.spill".to_string(),
        role: CouplingRole::Projection,
        trigger_kind: EventKind::ContextCompileCompleted,
        trigger_match: Default::default(),
        trigger_quota: AgentManifestCouplingQuota::default(),
        source_selectors: vec![BoundCouplingSelector {
            stream: "thread".to_string(),
            kinds: vec![EventKind::ContextCompileCompleted],
            scope: None,
            since: None,
        }],
        sink: BoundCouplingSink {
            stream: "derived:context".to_string(),
            kinds: vec![
                EventKind::ContextSummaryCompleted,
                EventKind::ContextReadPlanSet,
            ],
        },
        function_ref: format!("op://std-context-spill/run@sha256:{}", "c".repeat(64)),
        function: BoundCouplingFunction {
            name: "std-context-spill".to_string(),
            artifact_hash: "c".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:thread".to_string(),
            "stream.write:derived:context".to_string(),
        ],
        budget: AgentManifestCouplingBudget {
            max_discharge_events: Some(2),
            max_ms: None,
        },
        config: serde_json::json!({}),
        config_hash: "sha256:context-spill".to_string(),
    }
}

fn runtime_std_schedule_cron_timer_coupling() -> BoundCoupling {
    BoundCoupling {
        id: "std::schedule.cron".to_string(),
        role: CouplingRole::Controller,
        trigger_kind: EventKind::TimerFired,
        trigger_match: Default::default(),
        trigger_quota: AgentManifestCouplingQuota::default(),
        source_selectors: vec![BoundCouplingSelector {
            stream: "control".to_string(),
            kinds: vec![
                EventKind::MandateStarted,
                EventKind::MandateRevoked,
                EventKind::TimerFired,
            ],
            scope: None,
            since: None,
        }],
        sink: BoundCouplingSink {
            stream: "control".to_string(),
            kinds: vec![
                EventKind::TurnContinueRequested,
                EventKind::LoopBudgetExhausted,
            ],
        },
        function_ref: format!("op://std-schedule-cron/run@sha256:{}", "s".repeat(64)),
        function: BoundCouplingFunction {
            name: "std-schedule-cron".to_string(),
            artifact_hash: "s".repeat(64),
            operation_name: Some("run".to_string()),
        },
        grants: vec![
            "stream.read:control".to_string(),
            "stream.write:control".to_string(),
        ],
        budget: AgentManifestCouplingBudget {
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

#[async_trait]
impl AgentRuntimeFactory for EchoRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(EchoRuntime))
    }
}

struct EchoRuntime;

#[async_trait]
impl AgentRuntime for EchoRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(ThreadStatus::Stopped);
                    let _ = events.send(ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(ThreadStatus::Running);
                            if let Ok(entry) = services.append_user_turn_input(&coordinates, &turn_id, &input).await {
                                let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                            }
                            let _ = events.send(ThreadEvent::Output {
                                thread_id,
                                text: format!("{turn_id}:{}", input.text_projection()),
                            });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Cancel { reason }) => {
                            let _ = status.send(ThreadStatus::Cancelling);
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::CancelTurn { .. }) => {}
                        Some(ThreadCommand::Compact { .. }) => {
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Shutdown) | None => {
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::shutdown(&coordinates),
                            });
                            let _ = status.send(ThreadStatus::Stopped);
                            let _ = events.send(ThreadEvent::Stopped { thread_id });
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
    inner: InMemorySessionStore,
    admission_barrier: Option<Arc<AdmissionAppendBarrier>>,
    manifest_barrier: Option<Arc<AdmissionAppendBarrier>>,
    select_branch_calls: Arc<AtomicUsize>,
}

impl AdmissionTestStore {
    fn blocking(admission_barrier: Arc<AdmissionAppendBarrier>) -> Self {
        Self {
            inner: InMemorySessionStore::new(),
            admission_barrier: Some(admission_barrier),
            manifest_barrier: None,
            select_branch_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn blocking_manifest(manifest_barrier: Arc<AdmissionAppendBarrier>) -> Self {
        Self {
            inner: InMemorySessionStore::new(),
            admission_barrier: None,
            manifest_barrier: Some(manifest_barrier),
            select_branch_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn tracking_selects() -> Self {
        Self {
            inner: InMemorySessionStore::new(),
            admission_barrier: None,
            manifest_barrier: None,
            select_branch_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl SessionStore for AdmissionTestStore {
    async fn append(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.inner.append(coordinates, parent_entry_id, kind).await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry> {
        self.inner
            .append_with_provenance(coordinates, parent_entry_id, kind, provenance)
            .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: &str,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.inner
            .append_turn_input(coordinates, turn_id, kind)
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        self.inner.active_leaf(coordinates).await
    }

    async fn select_branch(
        &self,
        coordinates: &ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()> {
        self.select_branch_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.select_branch(coordinates, leaf_entry_id).await
    }

    async fn build_context(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<SessionContext> {
        self.inner.build_context(coordinates).await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        self.inner
            .clone_branch(source_coordinates, source_leaf, target_coordinates)
            .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> HistoryResult<()> {
        self.inner
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
    }
}

#[async_trait]
impl EventStore for AdmissionTestStore {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let appends_admission = records
            .iter()
            .any(|record| record.kind == EventKind::AdmissionDecided);
        if appends_admission && let Some(barrier) = &self.admission_barrier {
            barrier.arrive_and_wait().await;
        }
        let appends_manifest_bind = records
            .iter()
            .any(|record| record.kind == EventKind::ManifestBindCompleted);
        if appends_manifest_bind && let Some(barrier) = &self.manifest_barrier {
            barrier.arrive_and_wait().await;
        }
        self.inner.append_events(stream_id, records).await
    }

    async fn read_events(
        &self,
        stream_id: &EventStreamId,
        from_sequence: Option<EventSequence>,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.inner.read_events(stream_id, from_sequence).await
    }
}

#[async_trait]
// lexicon-allow: observation_store - test wrapper must implement the history observation trait
impl ObservationStore for AdmissionTestStore {
    async fn append_observation(
        &self,
        record: NewObservationRecord,
    ) -> HistoryResult<ObservationRecord> {
        self.inner.append_observation(record).await
    }

    async fn list_observations(
        &self,
        scope: &ThreadCoordinates,
        kind: Option<&str>,
    ) -> HistoryResult<Vec<ObservationRecord>> {
        self.inner.list_observations(scope, kind).await
    }
}

#[derive(Default)]
struct AdmissionAppendBarrier {
    entered: AtomicUsize,
    entered_notify: Notify,
    released: AtomicBool,
    release_notify: Notify,
}

impl AdmissionAppendBarrier {
    async fn arrive_and_wait(&self) {
        let release = self.release_notify.notified();
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.entered_notify.notify_waiters();
        if !self.released.load(Ordering::SeqCst) {
            release.await;
        }
    }

    async fn wait_for_entries(&self, expected: usize) {
        loop {
            let entered = self.entered_notify.notified();
            if self.entered.load(Ordering::SeqCst) >= expected {
                return;
            }
            entered.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

struct AssistantHistoryRuntimeFactory;

#[async_trait]
impl AgentRuntimeFactory for AssistantHistoryRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(AssistantHistoryRuntime))
    }
}

struct AssistantHistoryRuntime;

#[async_trait]
impl AgentRuntime for AssistantHistoryRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(ThreadStatus::Stopped);
                    let _ = events.send(ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(ThreadStatus::Running);
                            if let Ok(entry) = services.append_user_turn_input(&coordinates, &turn_id, &input).await {
                                let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                            }
                            let output = format!("{turn_id}:{}", input.text_projection());
                            let _ = services.append_session_entry(
                                &coordinates,
                                None,
                                SessionEntryKind::Message {
                                    message: CanonicalMessage::assistant(
                                        "test",
                                        ProviderApi::Other("test".to_string()),
                                        "test-model",
                                        vec![CanonicalContent::text(output.clone())],
                                        CanonicalStopReason::EndTurn,
                                    ),
                                },
                            ).await;
                            let _ = events.send(ThreadEvent::Output {
                                thread_id,
                                text: output,
                            });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Cancel { reason }) => {
                            let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::CancelTurn { .. }) => {}
                        Some(ThreadCommand::Compact { .. }) => {
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Shutdown) | None => {
                            let _ = status.send(ThreadStatus::Stopped);
                            let _ = events.send(ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

struct InspectTurnInputRuntimeFactory;

#[async_trait]
impl AgentRuntimeFactory for InspectTurnInputRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(InspectTurnInputRuntime))
    }
}

struct InspectTurnInputRuntime;

#[async_trait]
impl AgentRuntime for InspectTurnInputRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        _services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let _ = status.send(ThreadStatus::Idle);
        while let Some(command) = commands.recv().await {
            match command {
                ThreadCommand::Submit { input, .. } => {
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
                            TurnContent::Image { mime_type, .. } => {
                                parts.push(format!("image={mime_type}"));
                            }
                            TurnContent::FileRef {
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
                            TurnContent::Text { .. } => {}
                        }
                    }
                    let _ = events.send(ThreadEvent::Output {
                        thread_id,
                        text: parts.join("|"),
                    });
                }
                ThreadCommand::Cancel { .. } => {}
                ThreadCommand::CancelTurn { .. } => {}
                ThreadCommand::Compact { .. } => {}
                ThreadCommand::ResumeToolCall { .. } => {}
                ThreadCommand::Shutdown => break,
            }
        }
    }
}

struct StuckRuntimeFactory;

#[async_trait]
impl AgentRuntimeFactory for StuckRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(StuckRuntime))
    }
}

struct StuckRuntime;

#[async_trait]
impl AgentRuntime for StuckRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);
        if let Some(ThreadCommand::Submit { turn_id, input, .. }) = commands.recv().await {
            let _ = status.send(ThreadStatus::Running);
            if let Ok(entry) = services
                .append_user_turn_input(&coordinates, &turn_id, &input)
                .await
            {
                let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
            }
            std::future::pending::<()>().await;
        }
    }
}

#[derive(Default)]
struct GatedTurnRuntimeState {
    first_started: Notify,
    release_first: Notify,
    second_started: Notify,
    release_second: Notify,
}

struct GatedTurnRuntimeFactory {
    state: Arc<GatedTurnRuntimeState>,
}

#[async_trait]
impl AgentRuntimeFactory for GatedTurnRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(GatedTurnRuntime {
            state: Arc::clone(&self.state),
        }))
    }
}

struct GatedTurnRuntime {
    state: Arc<GatedTurnRuntimeState>,
}

#[async_trait]
impl AgentRuntime for GatedTurnRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);

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
            let Some(ThreadCommand::Submit { turn_id, input, .. }) = commands.recv().await else {
                return;
            };
            assert_eq!(turn_id, expected_turn_id);
            let _ = status.send(ThreadStatus::Running);
            if let Ok(entry) = services
                .append_user_turn_input(&coordinates, &turn_id, &input)
                .await
            {
                let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
            }
            started.notify_one();
            release.notified().await;
            drop(input);
            let _ = status.send(ThreadStatus::Idle);
        }
    }
}

#[derive(Default)]
struct WatchdogHandoffState {
    first_started: Notify,
    release_first: Notify,
    second_started: Notify,
    stale_cancel_applied: AtomicBool,
    stale_cancel_observed: Notify,
}

struct WatchdogHandoffRuntimeFactory {
    state: Arc<WatchdogHandoffState>,
}

#[async_trait]
impl AgentRuntimeFactory for WatchdogHandoffRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(WatchdogHandoffRuntime {
            state: Arc::clone(&self.state),
        }))
    }
}

struct WatchdogHandoffRuntime {
    state: Arc<WatchdogHandoffState>,
}

impl WatchdogHandoffState {
    async fn wait_for_stale_cancel(&self) {
        loop {
            let observed = self.stale_cancel_observed.notified();
            if self.stale_cancel_applied.load(Ordering::SeqCst) {
                return;
            }
            observed.await;
        }
    }
}

#[async_trait]
impl AgentRuntime for WatchdogHandoffRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        _events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let coordinates = context.coordinates;
        let Some(ThreadCommand::Submit { turn_id, input, .. }) = commands.recv().await else {
            return;
        };
        let _ = status.send(ThreadStatus::Running);
        let _ = services
            .append_user_turn_input(&coordinates, &turn_id, &input)
            .await;
        self.state.first_started.notify_one();
        self.state.release_first.notified().await;
        drop(input);
        let _ = status.send(ThreadStatus::Idle);

        let Some(ThreadCommand::Submit { turn_id, input, .. }) = commands.recv().await else {
            return;
        };
        let _ = status.send(ThreadStatus::Running);
        let _ = services
            .append_user_turn_input(&coordinates, &turn_id, &input)
            .await;
        self.state.second_started.notify_one();

        if let Some(ThreadCommand::Cancel { .. }) = commands.recv().await {
            self.state
                .stale_cancel_applied
                .store(true, Ordering::SeqCst);
            self.state.stale_cancel_observed.notify_waiters();
        }
    }
}

#[derive(Default)]
struct DrainedPendingInputState {
    first_started: Notify,
    queued_input_drained: Notify,
    release: Notify,
}

struct DrainedPendingInputRuntimeFactory {
    state: Arc<DrainedPendingInputState>,
}

#[async_trait]
impl AgentRuntimeFactory for DrainedPendingInputRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(DrainedPendingInputRuntime {
            state: Arc::clone(&self.state),
        }))
    }
}

struct DrainedPendingInputRuntime {
    state: Arc<DrainedPendingInputState>,
}

#[async_trait]
impl AgentRuntime for DrainedPendingInputRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        _events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let coordinates = context.coordinates;
        let Some(ThreadCommand::Submit { turn_id, input, .. }) = commands.recv().await else {
            return;
        };
        let _ = status.send(ThreadStatus::Running);
        let _ = services
            .append_user_turn_input(&coordinates, &turn_id, &input)
            .await;
        self.state.first_started.notify_one();

        let queued_input = match commands.recv().await {
            Some(ThreadCommand::Submit { input, .. }) => input,
            _ => return,
        };
        self.state.queued_input_drained.notify_one();
        self.state.release.notified().await;
        drop(queued_input);
    }
}

#[derive(Default)]
struct GatedShutdownFactory {
    builds: AtomicUsize,
    shutdown_received: Arc<Notify>,
    release_shutdown: Arc<Notify>,
}

#[async_trait]
impl AgentRuntimeFactory for GatedShutdownFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        if self.builds.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(Box::new(GatedShutdownRuntime {
                shutdown_received: Arc::clone(&self.shutdown_received),
                release_shutdown: Arc::clone(&self.release_shutdown),
            }))
        } else {
            Ok(Box::new(EchoRuntime))
        }
    }
}

struct GatedShutdownRuntime {
    shutdown_received: Arc<Notify>,
    release_shutdown: Arc<Notify>,
}

#[async_trait]
impl AgentRuntime for GatedShutdownRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        _services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        _events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let _ = status.send(ThreadStatus::Idle);
        while let Some(command) = commands.recv().await {
            if matches!(command, ThreadCommand::Shutdown) {
                self.shutdown_received.notify_one();
                self.release_shutdown.notified().await;
                let _ = status.send(ThreadStatus::Stopped);
                return;
            }
        }
        let _ = context;
    }
}

#[derive(Default)]
struct ControlledChildBuildFactory {
    child_builds: AtomicUsize,
    child_build_notify: Notify,
    released: AtomicBool,
    release_notify: Notify,
    child_runtime_starts: Arc<AtomicUsize>,
}

impl ControlledChildBuildFactory {
    async fn wait_for_child_builds(&self, expected: usize) {
        loop {
            let entered = self.child_build_notify.notified();
            if self.child_builds.load(Ordering::SeqCst) >= expected {
                return;
            }
            entered.await;
        }
    }

    fn release_builds(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

#[async_trait]
impl AgentRuntimeFactory for ControlledChildBuildFactory {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        let is_child = context.parent_thread_id.is_some();
        if is_child {
            let release = self.release_notify.notified();
            self.child_builds.fetch_add(1, Ordering::SeqCst);
            self.child_build_notify.notify_waiters();
            if !self.released.load(Ordering::SeqCst) {
                release.await;
            }
        }
        Ok(Box::new(CountingEchoRuntime {
            starts: is_child.then(|| Arc::clone(&self.child_runtime_starts)),
        }))
    }
}

struct BlockingThreadBuildFactory {
    target_thread_id: ThreadId,
    entered: AtomicBool,
    entered_notify: Notify,
    released: AtomicBool,
    release_notify: Notify,
}

impl BlockingThreadBuildFactory {
    fn new(target_thread_id: ThreadId) -> Self {
        Self {
            target_thread_id,
            entered: AtomicBool::new(false),
            entered_notify: Notify::new(),
            released: AtomicBool::new(false),
            release_notify: Notify::new(),
        }
    }

    async fn wait_until_blocked(&self) {
        loop {
            let entered = self.entered_notify.notified();
            if self.entered.load(Ordering::SeqCst) {
                return;
            }
            entered.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

#[async_trait]
impl AgentRuntimeFactory for BlockingThreadBuildFactory {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        if context.coordinates.thread_id == self.target_thread_id {
            let release = self.release_notify.notified();
            self.entered.store(true, Ordering::SeqCst);
            self.entered_notify.notify_waiters();
            if !self.released.load(Ordering::SeqCst) {
                release.await;
            }
        }
        Ok(Box::new(EchoRuntime))
    }
}

struct CountingEchoRuntime {
    starts: Option<Arc<AtomicUsize>>,
}

#[async_trait]
impl AgentRuntime for CountingEchoRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        if let Some(starts) = &self.starts {
            starts.fetch_add(1, Ordering::SeqCst);
        }
        Box::new(EchoRuntime)
            .run(context, services, commands, events, status, cancellation)
            .await;
    }
}

#[derive(Default)]
struct FailFirstChildBuildFactory {
    failed: AtomicBool,
}

#[async_trait]
impl AgentRuntimeFactory for FailFirstChildBuildFactory {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        if context.parent_thread_id.is_some() && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(CooldisError::RuntimeFactory(
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
    build_completed: AtomicBool,
    build_notify: Notify,
    outcome: Arc<AtomicUsize>,
    outcome_notify: Arc<Notify>,
}

impl RegistrationProbeFactory {
    async fn wait_for_build(&self) {
        loop {
            let completed = self.build_notify.notified();
            if self.build_completed.load(Ordering::SeqCst) {
                return;
            }
            completed.await;
        }
    }

    async fn wait_for_outcome(&self) -> RegistrationProbeOutcome {
        loop {
            let completed = self.outcome_notify.notified();
            match self.outcome.load(Ordering::SeqCst) {
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

#[async_trait]
impl AgentRuntimeFactory for RegistrationProbeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        self.build_completed.store(true, Ordering::SeqCst);
        self.build_notify.notify_waiters();
        Ok(Box::new(RegistrationProbeRuntime {
            outcome: Arc::clone(&self.outcome),
            outcome_notify: Arc::clone(&self.outcome_notify),
        }))
    }
}

struct RegistrationProbeRuntime {
    outcome: Arc<AtomicUsize>,
    outcome_notify: Arc<Notify>,
}

impl RegistrationProbeRuntime {
    fn record(&self, outcome: RegistrationProbeOutcome) {
        self.outcome.store(outcome as usize, Ordering::SeqCst);
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
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            self.outcome_notify.notify_waiters();
        }
    }
}

#[async_trait]
impl AgentRuntime for RegistrationProbeRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        let outcome = match services
            .kernel_control()
            .expect("runtime host supplies kernel control")
            .thread_status(&context, context.coordinates.thread_id)
            .await
        {
            Ok(_) => RegistrationProbeOutcome::ObservedRegistered,
            Err(CooldisError::ThreadNotFound(_)) => {
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
    active_runs: Arc<AtomicUsize>,
    runtime_started: Arc<Notify>,
    runtime_stopped: Arc<Notify>,
}

impl CancellationTrackedFactory {
    async fn wait_until_stopped(&self) {
        loop {
            let stopped = self.runtime_stopped.notified();
            if self.active_runs.load(Ordering::SeqCst) == 0 {
                return;
            }
            stopped.await;
        }
    }
}

#[async_trait]
impl AgentRuntimeFactory for CancellationTrackedFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(CancellationTrackedRuntime {
            active_runs: Arc::clone(&self.active_runs),
            runtime_started: Arc::clone(&self.runtime_started),
            runtime_stopped: Arc::clone(&self.runtime_stopped),
        }))
    }
}

struct CancellationTrackedRuntime {
    active_runs: Arc<AtomicUsize>,
    runtime_started: Arc<Notify>,
    runtime_stopped: Arc<Notify>,
}

struct ActiveRunGuard {
    active_runs: Arc<AtomicUsize>,
    runtime_stopped: Arc<Notify>,
}

impl Drop for ActiveRunGuard {
    fn drop(&mut self) {
        self.active_runs.fetch_sub(1, Ordering::SeqCst);
        self.runtime_stopped.notify_waiters();
    }
}

#[async_trait]
impl AgentRuntime for CancellationTrackedRuntime {
    async fn run(
        self: Box<Self>,
        _context: ThreadContext,
        _services: RuntimeServices,
        _commands: mpsc::Receiver<ThreadCommand>,
        _events: broadcast::Sender<ThreadEvent>,
        _status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        self.active_runs.fetch_add(1, Ordering::SeqCst);
        let _guard = ActiveRunGuard {
            active_runs: Arc::clone(&self.active_runs),
            runtime_stopped: Arc::clone(&self.runtime_stopped),
        };
        self.runtime_started.notify_waiters();
        cancellation.cancelled().await;
    }
}

struct BlockingLifecycleSink {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl ThreadLifecycleSink for BlockingLifecycleSink {
    async fn thread_started(&self, _handle: RuntimeThreadHandle) -> CooldisResult<()> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

struct FailingAfterRuntimeStartsSink {
    active_runs: Arc<AtomicUsize>,
    runtime_started: Arc<Notify>,
}

#[async_trait]
impl ThreadLifecycleSink for FailingAfterRuntimeStartsSink {
    async fn thread_started(&self, _handle: RuntimeThreadHandle) -> CooldisResult<()> {
        loop {
            let started = self.runtime_started.notified();
            if self.active_runs.load(Ordering::SeqCst) > 0 {
                break;
            }
            started.await;
        }
        Err(CooldisError::RuntimeFactory(
            "controlled lifecycle sink failure".to_string(),
        ))
    }
}

struct ExitRuntimeFactory;

#[async_trait]
impl AgentRuntimeFactory for ExitRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(ExitRuntime))
    }
}

struct ExitRuntime;

#[async_trait]
impl AgentRuntime for ExitRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        _services: RuntimeServices,
        _commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        _cancellation: CancellationToken,
    ) {
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn coords(tenant: &str, user: &str, session: &str) -> ThreadCoordinates {
    ThreadCoordinates::new(tenant, user, session)
}

#[test]
fn runtime_event_kind_serializes_stable_shapes() {
    let checkpoint_id = ThreadCheckpointId::new();
    let child_thread_id = ThreadId::new();
    let interaction_id = RuntimeEventId::new();
    let source_thread_id = ThreadId::new();
    let cases = vec![
        (
            RuntimeEventKind::ThreadInteraction {
                interaction_id,
                kind: ThreadInteractionKind::PromptSubmitted,
                source_thread_id,
                target_thread_id: child_thread_id,
                source_turn_id: None,
                target_turn_id: Some("turn-2".to_string()),
                result_preview: None,
                metadata: BTreeMap::from([(
                    "operation".to_string(),
                    "cooldis.submit_to_thread".to_string(),
                )]),
            },
            serde_json::json!({"type":"thread_interaction","interaction_id":interaction_id.to_string(),"kind":"prompt_submitted","source_thread_id":source_thread_id.to_string(),"target_thread_id":child_thread_id.to_string(),"target_turn_id":"turn-2","metadata":{"operation":"cooldis.submit_to_thread"}}),
        ),
        (
            RuntimeEventKind::TextDelta {
                text: "hello".to_string(),
            },
            serde_json::json!({"type":"text_delta","text":"hello"}),
        ),
        (
            RuntimeEventKind::ThinkingDelta {
                text: "plan".to_string(),
            },
            serde_json::json!({"type":"thinking_delta","text":"plan"}),
        ),
        (
            RuntimeEventKind::ToolCallStarted {
                call_id: "call_1".to_string(),
                name: "bash".to_string(),
                input: serde_json::json!({"command":"pwd"}),
            },
            serde_json::json!({"type":"tool_call_started","call_id":"call_1","name":"bash","input":{"command":"pwd"}}),
        ),
        (
            RuntimeEventKind::ToolCallResult {
                call_id: "call_1".to_string(),
                output: "ok".to_string(),
                success: true,
                duration_ms: None,
            },
            serde_json::json!({"type":"tool_call_result","call_id":"call_1","output":"ok","success":true}),
        ),
        (
            RuntimeEventKind::ToolCallResult {
                call_id: "call_2".to_string(),
                output: "ok".to_string(),
                success: true,
                duration_ms: Some(17),
            },
            serde_json::json!({"type":"tool_call_result","call_id":"call_2","output":"ok","success":true,"duration_ms":17}),
        ),
        (
            RuntimeEventKind::ToolLog {
                call_id: "call_2".to_string(),
                tool_name: "bash".to_string(),
                level: RuntimeToolLogLevel::Info,
                message: "tool completed".to_string(),
                metadata: BTreeMap::from([("duration_ms".to_string(), "17".to_string())]),
            },
            serde_json::json!({"type":"tool_log","call_id":"call_2","tool_name":"bash","level":"info","message":"tool completed","metadata":{"duration_ms":"17"}}),
        ),
        (
            RuntimeEventKind::HookStarted {
                hook_id: "pre-echo".to_string(),
                event_name: HookEventName::PreToolUse,
                matcher: Some("echo_search".to_string()),
            },
            serde_json::json!({"type":"hook_started","hook_id":"pre-echo","event_name":"pre_tool_use","matcher":"echo_search"}),
        ),
        (
            RuntimeEventKind::HookCompleted {
                hook_id: "pre-echo".to_string(),
                event_name: HookEventName::PreToolUse,
                status: HookRunStatus::Completed,
                duration_ms: 12,
                message: None,
            },
            serde_json::json!({"type":"hook_completed","hook_id":"pre-echo","event_name":"pre_tool_use","status":"completed","duration_ms":12}),
        ),
        (
            RuntimeEventKind::ApprovalRequested {
                approval_id: "approval_1".to_string(),
                action: "write_file".to_string(),
                metadata: BTreeMap::from([("path".to_string(), "/workspace/a".to_string())]),
            },
            serde_json::json!({"type":"approval_requested","approval_id":"approval_1","action":"write_file","metadata":{"path":"/workspace/a"}}),
        ),
        (
            RuntimeEventKind::ApprovalResolved {
                approval_id: "approval_1".to_string(),
                decision: RuntimeApprovalDecision::Approved,
                reason: None,
            },
            serde_json::json!({"type":"approval_resolved","approval_id":"approval_1","decision":"approved"}),
        ),
        (
            RuntimeEventKind::PermissionDecision {
                call_id: "call_1".to_string(),
                tool_name: "bash".to_string(),
                decision: RuntimePermissionDecision::Deny,
                reason: Some("policy denied".to_string()),
            },
            serde_json::json!({"type":"permission_decision","call_id":"call_1","tool_name":"bash","decision":"deny","reason":"policy denied"}),
        ),
        (
            RuntimeEventKind::ContextCompiled {
                diagnostics: AgentContextCompilationDiagnostics {
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
            RuntimeEventKind::ModelRequestStarted {
                request_id: "req_1".to_string(),
                turn_id: "turn-1".to_string(),
                provider: "openai".to_string(),
                api: "openai_responses".to_string(),
                model: "gpt-test".to_string(),
                mode: RuntimeModelRequestMode::Complete,
                purpose: RuntimeModelRequestPurpose::Turn,
                system_block_count: 1,
                message_count: 2,
                tool_count: 3,
                max_tokens: 128,
            },
            serde_json::json!({"type":"model_request_started","request_id":"req_1","turn_id":"turn-1","provider":"openai","api":"openai_responses","model":"gpt-test","mode":"complete","purpose":"turn","system_block_count":1,"message_count":2,"tool_count":3,"max_tokens":128}),
        ),
        (
            RuntimeEventKind::ModelRequestCompleted {
                request_id: "req_1".to_string(),
                turn_id: "turn-1".to_string(),
                provider: "openai".to_string(),
                api: "openai_responses".to_string(),
                model: "gpt-test".to_string(),
                mode: RuntimeModelRequestMode::Complete,
                purpose: RuntimeModelRequestPurpose::Turn,
                duration_ms: 25,
                usage: RuntimeUsage {
                    input_tokens: 1,
                    output_tokens: 2,
                    cache_creation_input_tokens: 3,
                    cache_read_input_tokens: 4,
                },
                stop_reason: CanonicalStopReason::EndTurn,
            },
            serde_json::json!({"type":"model_request_completed","request_id":"req_1","turn_id":"turn-1","provider":"openai","api":"openai_responses","model":"gpt-test","mode":"complete","purpose":"turn","duration_ms":25,"usage":{"input_tokens":1,"output_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":4},"stop_reason":"end_turn"}),
        ),
        (
            RuntimeEventKind::ModelRequestRetryScheduled {
                request_id: "req_1".to_string(),
                next_request_id: "req_1_retry".to_string(),
                turn_id: "turn-1".to_string(),
                provider: "openai".to_string(),
                api: "openai_responses".to_string(),
                model: "gpt-test".to_string(),
                mode: RuntimeModelRequestMode::Complete,
                purpose: RuntimeModelRequestPurpose::Turn,
                attempt: 1,
                next_attempt: 2,
                delay_ms: 50,
                error_class: RuntimeModelRequestErrorClass::RateLimited,
                error: "rate limited".to_string(),
            },
            serde_json::json!({"type":"model_request_retry_scheduled","request_id":"req_1","next_request_id":"req_1_retry","turn_id":"turn-1","provider":"openai","api":"openai_responses","model":"gpt-test","mode":"complete","purpose":"turn","attempt":1,"next_attempt":2,"delay_ms":50,"error_class":"rate_limited","error":"rate limited"}),
        ),
        (
            RuntimeEventKind::ModelRequestFallbackSelected {
                request_id: "req_1".to_string(),
                turn_id: "turn-1".to_string(),
                from_provider: "openai".to_string(),
                from_api: "openai_responses".to_string(),
                from_model: "gpt-test".to_string(),
                to_provider: "fallback".to_string(),
                to_api: "openai_responses".to_string(),
                to_model: "gpt-fallback".to_string(),
                mode: RuntimeModelRequestMode::Complete,
                purpose: RuntimeModelRequestPurpose::Turn,
                error_class: RuntimeModelRequestErrorClass::Retryable,
                error: "provider down".to_string(),
            },
            serde_json::json!({"type":"model_request_fallback_selected","request_id":"req_1","turn_id":"turn-1","from_provider":"openai","from_api":"openai_responses","from_model":"gpt-test","to_provider":"fallback","to_api":"openai_responses","to_model":"gpt-fallback","mode":"complete","purpose":"turn","error_class":"retryable","error":"provider down"}),
        ),
        (
            RuntimeEventKind::ModelRequestFailed {
                request_id: "req_2".to_string(),
                turn_id: "turn-1".to_string(),
                provider: "openai".to_string(),
                api: "openai_responses".to_string(),
                model: "gpt-test".to_string(),
                mode: RuntimeModelRequestMode::Stream,
                purpose: RuntimeModelRequestPurpose::Compaction,
                duration_ms: 3,
                error_class: RuntimeModelRequestErrorClass::Retryable,
                error: "network".to_string(),
            },
            serde_json::json!({"type":"model_request_failed","request_id":"req_2","turn_id":"turn-1","provider":"openai","api":"openai_responses","model":"gpt-test","mode":"stream","purpose":"compaction","duration_ms":3,"error_class":"retryable","error":"network"}),
        ),
        (
            RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::Completed,
            },
            serde_json::json!({"type":"terminal","state":"completed"}),
        ),
        (
            RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::TimedOut,
            },
            serde_json::json!({"type":"terminal","state":"timed_out"}),
        ),
        (
            RuntimeEventKind::Timeout {
                operation: "turn".to_string(),
                timeout_ms: 100,
            },
            serde_json::json!({"type":"timeout","operation":"turn","timeout_ms":100}),
        ),
        (
            RuntimeEventKind::PolicyRejected {
                code: "max_pending_inputs".to_string(),
                message: "full".to_string(),
            },
            serde_json::json!({"type":"policy_rejected","code":"max_pending_inputs","message":"full"}),
        ),
        (
            RuntimeEventKind::Recovery {
                action: "abort_runtime".to_string(),
                reason: "timeout".to_string(),
            },
            serde_json::json!({"type":"recovery","action":"abort_runtime","reason":"timeout"}),
        ),
        (
            RuntimeEventKind::Usage {
                usage: RuntimeUsage {
                    input_tokens: 1,
                    output_tokens: 2,
                    cache_creation_input_tokens: 3,
                    cache_read_input_tokens: 4,
                },
            },
            serde_json::json!({"type":"usage","usage":{"input_tokens":1,"output_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":4}}),
        ),
        (
            RuntimeEventKind::SubthreadStarted { child_thread_id },
            serde_json::json!({"type":"subthread_started","child_thread_id":child_thread_id.to_string()}),
        ),
        (
            RuntimeEventKind::SubthreadFinished {
                child_thread_id,
                status: ThreadLifecycleStatus::Stopped,
            },
            serde_json::json!({"type":"subthread_finished","child_thread_id":child_thread_id.to_string(),"status":"stopped"}),
        ),
        (
            RuntimeEventKind::Checkpoint {
                checkpoint_id,
                label: Some("label".to_string()),
            },
            serde_json::json!({"type":"checkpoint","checkpoint_id":checkpoint_id.to_string(),"label":"label"}),
        ),
        (
            RuntimeEventKind::Compaction {
                trigger: CompactionTrigger::Manual,
                summary: "summary".to_string(),
            },
            serde_json::json!({"type":"compaction","trigger":"manual","summary":"summary"}),
        ),
        (
            RuntimeEventKind::Cancelled {
                reason: "stop".to_string(),
            },
            serde_json::json!({"type":"cancelled","reason":"stop"}),
        ),
        (
            RuntimeEventKind::Failed {
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
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();

    wait_for_status(&thread, ThreadStatus::Idle).await;
    let record = thread.lifecycle_record().await;
    assert_eq!(record.status, ThreadLifecycleStatus::Idle);
    assert_eq!(record.parent_thread_id, None);

    let checkpoint = host
        .create_checkpoint(
            thread.context().coordinates.thread_id,
            None,
            Some("before-stop".to_string()),
            BTreeMap::from([("opaque_app_id".to_string(), "app_123".to_string())]),
        )
        .await
        .unwrap();
    assert_eq!(checkpoint.lineage, ThreadCheckpointLineage::Root);
    assert_eq!(checkpoint.coordinates, thread.context().coordinates);
    assert_eq!(checkpoint.label.as_deref(), Some("before-stop"));

    let record = thread.lifecycle_record().await;
    assert_eq!(record.latest_checkpoint_id, Some(checkpoint.id));
    assert!(record.latest_signal_id.is_some());

    thread.send(ThreadCommand::Shutdown).await.unwrap();
    thread.wait().await;
    let record = thread.lifecycle_record().await;
    assert_eq!(record.status, ThreadLifecycleStatus::Stopped);
}

#[tokio::test]
async fn lifecycle_snapshot_returns_records_not_only_thin_status() {
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    wait_for_status(&thread, ThreadStatus::Idle).await;

    let snapshot = host.lifecycle_snapshot().await;

    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(
        snapshot.records[0].coordinates,
        thread.context().coordinates
    );
    assert_eq!(snapshot.records[0].status, ThreadLifecycleStatus::Idle);
}

#[tokio::test]
async fn text_submit_helper_matches_structured_text_turn_canonical_record() {
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let old = host
        .start_thread(coords("tenant_a", "user_1", "old"), ThreadTopology::root())
        .await
        .unwrap();
    let new = host
        .start_thread(coords("tenant_a", "user_1", "new"), ThreadTopology::root())
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
        TurnInput::text("hello"),
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
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "host-submit"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn", "hello")
        .await
        .unwrap();
    assert_output(&mut events, "turn:hello").await;

    let control_events = store
        .read_events(&control_stream_id(&thread.context().coordinates), None)
        .await
        .unwrap();
    let thread_events = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
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
        EventKind::AdmissionDecided.payload_schema_id()
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
    assert_eq!(admission.origin, EventOrigin::Discharged);
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
    let store = Arc::new(
        FaultingRuntimeStore::new(Arc::new(InMemorySessionStore::new())).fail_nth(
            "append_events",
            1,
            "admission append failed",
        ),
    );
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "failed-admission"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    wait_for_status(&thread, ThreadStatus::Idle).await;
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

    let output = timeout(Duration::from_millis(100), async {
        loop {
            if let ThreadEvent::Output { text, .. } = events.recv().await.unwrap() {
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
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(Arc::new(ExitRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "closed-submit"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    wait_for_status(&thread, ThreadStatus::Failed).await;

    let err = host
        .submit(
            thread.context().coordinates.thread_id,
            "turn-after-close",
            "blocked",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, CooldisError::ThreadClosed(thread_id) if thread_id == thread.context().coordinates.thread_id)
    );

    let control_events = store
        .read_events(&control_stream_id(&thread.context().coordinates), None)
        .await
        .unwrap();
    assert!(
        control_events
            .iter()
            .all(|event| event.kind != EventKind::AdmissionDecided),
        "closed thread submit must not leave an orphan admission.decided"
    );
}

#[tokio::test]
async fn structured_image_turn_maps_to_canonical_user_content() {
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "image"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit_turn(
        thread.context().coordinates.thread_id,
        "turn",
        TurnInput::new([
            TurnContent::text("look"),
            TurnContent::image("base64-image", "image/png"),
        ]),
    )
    .await
    .unwrap();
    assert_output(&mut events, "turn:look").await;

    let content = canonical_user_content(&thread.session_context().await.unwrap());
    assert_eq!(
        content,
        vec![vec![
            CanonicalContent::text("look"),
            CanonicalContent::Image {
                data: "base64-image".to_string(),
                mime_type: "image/png".to_string(),
            },
        ]]
    );
}

#[tokio::test]
async fn file_and_runtime_context_reach_runtime_boundary_without_canonicalizing_file() {
    let host = RuntimeHost::new(Arc::new(InspectTurnInputRuntimeFactory));
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "file"), ThreadTopology::root())
        .await
        .unwrap();
    let mut events = thread.subscribe_events();
    let input = TurnInput::new([
        TurnContent::text("inspect"),
        TurnContent::file_ref("/workspace/report.txt")
            .with_mime_type("text/plain")
            .with_size_bytes(42)
            .with_sha256("abc123"),
        TurnContent::image("inline", "image/jpeg"),
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
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let root = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let mut root_events = root.subscribe_events();
    let child = host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            ThreadTopology::spawned_from(root.context().coordinates.thread_id),
        )
        .await
        .unwrap();

    let started = assert_runtime_kind(&mut root_events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::SubthreadStarted { child_thread_id }
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
            RuntimeEventKind::SubthreadFinished {
                child_thread_id,
                status: ThreadLifecycleStatus::Stopped,
            } if *child_thread_id == child.context().coordinates.thread_id
        )
    })
    .await;
}

#[tokio::test]
async fn cross_thread_prompt_and_result_events_do_not_rewrite_lineage() {
    let host = RuntimeHost::new(Arc::new(AssistantHistoryRuntimeFactory));
    let root = host
        .start_thread(coords("tenant_a", "user_1", "root"), ThreadTopology::root())
        .await
        .unwrap();
    let root_id = root.context().coordinates.thread_id;
    let mut root_events = root.subscribe_events();
    let child = host
        .start_thread(
            coords("tenant_a", "user_1", "root"),
            ThreadTopology::spawned_from(root_id),
        )
        .await
        .unwrap();
    let child_id = child.context().coordinates.thread_id;
    let mut child_interaction_events = child.subscribe_events();
    let mut child_output_events = child.subscribe_events();
    wait_for_status(&root, ThreadStatus::Idle).await;
    wait_for_status(&child, ThreadStatus::Idle).await;

    let control = host.kernel_control();
    let receipt = control
        .submit_to_thread(
            root.context(),
            child_id,
            Some("turn-cross".to_string()),
            TurnInput::text("from root"),
        )
        .await
        .unwrap();
    assert_eq!(receipt.caller_thread_id, root_id);
    assert_eq!(receipt.target_thread_id, child_id);
    assert_eq!(receipt.turn_id, "turn-cross");

    let submitted = assert_runtime_kind(&mut root_events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::ThreadInteraction {
                interaction_id,
                kind: ThreadInteractionKind::PromptSubmitted,
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
            RuntimeEventKind::ThreadInteraction {
                interaction_id,
                kind: ThreadInteractionKind::PromptReceived,
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
            RuntimeEventKind::ThreadInteraction {
                interaction_id,
                kind: ThreadInteractionKind::ResultAttached,
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
    assert_eq!(child_topology.lineage, ThreadLineage::Root);
    assert_eq!(child_topology.spawn_source_thread_id(), Some(root_id));
    assert_eq!(child_topology.branch_parent_thread_id(), None);
    assert!(canonical_user_content(&root.session_context().await.unwrap()).is_empty());
    assert_eq!(
        canonical_user_content(&child.session_context().await.unwrap()),
        vec![vec![CanonicalContent::text("from root")]]
    );
}

#[tokio::test]
async fn loop_continuation_accepts_request_and_submits_next_turn_once() {
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "loop"), ThreadTopology::root())
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let coordinates = thread.context().coordinates.clone();
    let mut events = thread.subscribe_events();
    wait_for_status(&thread, ThreadStatus::Idle).await;
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
        LoopContinuationReceipt::Accepted {
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
        .read_events(&control_stream_id(&coordinates), None)
        .await
        .unwrap();
    let accepted = control_events
        .iter()
        .filter(|event| event.kind == EventKind::TurnContinuationAccepted)
        .collect::<Vec<_>>();
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].id, accepted_event_id);
    let payload =
        serde_json::from_value::<TurnContinuationAcceptedPayload>(accepted[0].payload.clone())
            .unwrap();
    assert_eq!(payload.next_turn_id, "turn-2");
    assert_eq!(payload.mandate_id, "mandate-loop-1");

    let thread_events = store
        .read_events(&EventStreamId::for_thread(&coordinates), None)
        .await
        .unwrap();
    let submitted = thread_events
        .iter()
        .filter(|event| {
            event.kind == EventKind::TurnSubmitted
                && event.payload["turn_id"].as_str() == Some("turn-2")
        })
        .collect::<Vec<_>>();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].origin, EventOrigin::Discharged);
    assert_eq!(
        submitted[0].provenance.source_event_ids,
        vec![accepted_event_id]
    );
}

#[tokio::test]
async fn schedule_timer_fired_continuation_is_accepted_and_runs_offline_provider() {
    let root = std::env::temp_dir()
        .join("cooldis-runtime-host-tests")
        .join(uuid::Uuid::now_v7().to_string());
    let store = SqliteSessionStore::open(root.join("history.sqlite3"))
        .await
        .unwrap();
    let mut config = CanonicalProviderRuntimeConfig::new(
        ProviderApi::Other("local_offline".to_string()),
        "local_offline",
        "gpt-test",
    );
    config.max_tokens = 128;
    let provider = Arc::new(LocalOfflineProviderClient::new("local_offline", "gpt-test"));
    let factory = Arc::new(CanonicalProviderRuntimeFactory::new(config, provider));
    let host = RuntimeHost::with_session_store(factory, Arc::new(store.clone()));
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "scheduled");
    let thread = host
        .start_thread(coordinates.clone(), ThreadTopology::root())
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let mut events = thread.subscribe_events();
    wait_for_status(&thread, ThreadStatus::Idle).await;

    let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).single().unwrap();
    let due = start + ChronoDuration::minutes(1);
    let mandate = append_scheduled_loop_mandate(&store, &coordinates, "loop-nightly", start).await;
    let sink = Arc::new(WitnessTimerFiredSink {
        store: store.clone(),
        coordinates: coordinates.clone(),
    });
    let clock = Arc::new(RuntimeFakeClock::new(due));
    let route = CooldisDaemonClockRoute::new("clock-main", store.clone(), sink, clock)
        .with_started_at(start);

    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);
    let control_events = store
        .read_events(&control_stream_id(&coordinates), None)
        .await
        .unwrap();
    let fired = control_events
        .iter()
        .filter(|event| event.kind == EventKind::TimerFired)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].provenance.source_event_ids, vec![mandate.id]);

    let executor = StdlibCouplingExecutor;
    let scheduler = CouplingScheduler::new(&store, &executor);
    let receipt = scheduler
        .run_batch(
            &BoundCouplingSet::new(
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
        .read_events(&control_stream_id(&coordinates), None)
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.kind == EventKind::TurnContinueRequested)
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
        LoopContinuationReceipt::Accepted {
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
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "loop-denied"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    let coordinates = thread.context().coordinates.clone();
    wait_for_status(&thread, ThreadStatus::Idle).await;
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
        LoopContinuationReceipt::Rejected {
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
        .read_events(&EventStreamId::for_thread(&coordinates), None)
        .await
        .unwrap();
    assert!(
        thread_events
            .iter()
            .all(|event| event.kind != EventKind::TurnSubmitted)
    );
}

#[tokio::test]
async fn execution_policy_limits_child_threads() {
    let host = RuntimeHost::with_policy(
        Arc::new(EchoRuntimeFactory),
        RuntimeExecutionPolicy::default().with_max_child_threads(1),
    );
    let root = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let mut root_events = root.subscribe_events();
    host.start_thread(
        coords("tenant_a", "user_1", "s1"),
        ThreadTopology::spawned_from(root.context().coordinates.thread_id),
    )
    .await
    .unwrap();

    let err = match host
        .start_thread(
            coords("tenant_a", "user_1", "s1"),
            ThreadTopology::spawned_from(root.context().coordinates.thread_id),
        )
        .await
    {
        Ok(_) => panic!("child start unexpectedly succeeded"),
        Err(err) => err,
    };

    assert!(matches!(
        err,
        CooldisError::ThreadPolicyViolation {
            code: "max_child_threads",
            ..
        }
    ));
    assert_runtime_kind(&mut root_events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::PolicyRejected { code, .. } if code == "max_child_threads"
        )
    })
    .await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn concurrent_child_starts_reserve_policy_slots_before_runtime_build() {
    let factory = Arc::new(ControlledChildBuildFactory::default());
    let host = RuntimeHost::with_policy(
        factory.clone(),
        RuntimeExecutionPolicy::default().with_max_child_threads(2),
    );
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "concurrent-child-cap"),
            ThreadTopology::root(),
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
                ThreadTopology::spawned_from(root_thread_id),
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
                    Err(CooldisError::ThreadPolicyViolation {
                        code: "max_child_threads",
                        ..
                    })
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
    let host = RuntimeHost::with_policy(
        Arc::new(FailFirstChildBuildFactory::default()),
        RuntimeExecutionPolicy::default().with_max_child_threads(1),
    );
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "failed-child-reservation"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let root_thread_id = root.context().coordinates.thread_id;

    assert!(matches!(
        host.start_thread(
            coords("tenant_a", "user_1", "failed-child-reservation"),
            ThreadTopology::spawned_from(root_thread_id),
        )
        .await,
        Err(CooldisError::RuntimeFactory(_))
    ));
    host.start_thread(
        coords("tenant_a", "user_1", "failed-child-reservation"),
        ThreadTopology::spawned_from(root_thread_id),
    )
    .await
    .unwrap();

    assert_eq!(host.children_of(root_thread_id).await.len(), 1);
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn execution_policy_rejects_submit_when_command_queue_is_full() {
    let host = RuntimeHost::with_policy(
        Arc::new(StuckRuntimeFactory),
        RuntimeExecutionPolicy::default().with_max_pending_inputs(1),
    );
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hold")
        .await
        .unwrap();
    wait_for_status(&thread, ThreadStatus::Running).await;
    host.submit(thread.context().coordinates.thread_id, "turn-2", "queued")
        .await
        .unwrap();
    let err = host
        .submit(thread.context().coordinates.thread_id, "turn-3", "rejected")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CooldisError::ThreadPolicyViolation {
            code: "max_pending_inputs",
            ..
        }
    ));
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::PolicyRejected { code, .. } if code == "max_pending_inputs"
        )
    })
    .await;
    assert_eq!(
        canonical_user_content(&thread.session_context().await.unwrap()),
        vec![vec![CanonicalContent::text("hold")]]
    );
    thread.abort().await;
}

#[tokio::test]
async fn user_turn_input_persistence_adopts_existing_entry_by_turn_id() {
    let store = Arc::new(InMemorySessionStore::new());
    let coordinates = coords("tenant_a", "user_1", "turn-input-idempotency");
    let first_services = RuntimeServices::new(store.clone(), RuntimeExecutionPolicy::default());
    let recovered_services = RuntimeServices::new(store.clone(), RuntimeExecutionPolicy::default());
    let input = TurnInput::text("persist exactly once");

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
        vec![vec![CanonicalContent::text("persist exactly once")]]
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn queued_turn_watchdog_starts_only_when_that_turn_begins_executing() {
    let state = Arc::new(GatedTurnRuntimeState::default());
    let host = RuntimeHost::with_policy(
        Arc::new(GatedTurnRuntimeFactory {
            state: Arc::clone(&state),
        }),
        RuntimeExecutionPolicy::default().with_turn_timeout_ms(100),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "watchdog"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-a", "hold")
        .await
        .unwrap();
    state.first_started.notified().await;

    tokio::time::advance(Duration::from_millis(50)).await;
    host.submit(thread.context().coordinates.thread_id, "turn-b", "queued")
        .await
        .unwrap();

    tokio::time::advance(Duration::from_millis(50)).await;
    timeout(
        Duration::from_millis(1),
        assert_runtime_kind(&mut events, |kind| {
            matches!(
                kind,
                RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
            )
        }),
    )
    .await
    .expect("the active first turn watchdog did not fire at its deadline");

    assert!(
        timeout(
            Duration::from_millis(51),
            assert_runtime_kind(&mut events, |kind| {
                matches!(
                    kind,
                    RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
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
        timeout(
            Duration::from_millis(99),
            assert_runtime_kind(&mut events, |kind| {
                matches!(
                    kind,
                    RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
                )
            }),
        )
        .await
        .is_err(),
        "the second turn watchdog fired before its execution deadline"
    );
    timeout(
        Duration::from_millis(2),
        assert_runtime_kind(&mut events, |kind| {
            matches!(
                kind,
                RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
            )
        }),
    )
    .await
    .expect("the second turn watchdog did not start with execution");

    thread.abort().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn timed_out_turn_cancellation_does_not_apply_to_the_next_turn() {
    let state = Arc::new(WatchdogHandoffState::default());
    let host = RuntimeHost::with_policy(
        Arc::new(WatchdogHandoffRuntimeFactory {
            state: Arc::clone(&state),
        }),
        RuntimeExecutionPolicy::default().with_turn_timeout_ms(100),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "watchdog-handoff"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();
    let thread_id = thread.context().coordinates.thread_id;

    host.submit(thread_id, "turn-a", "active").await.unwrap();
    state.first_started.notified().await;
    host.submit(thread_id, "turn-b", "queued").await.unwrap();

    tokio::time::advance(Duration::from_millis(100)).await;
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
        )
    })
    .await;
    state.release_first.notify_one();
    state.second_started.notified().await;

    assert!(
        timeout(Duration::from_millis(1), state.wait_for_stale_cancel())
            .await
            .is_err(),
        "turn A's timeout cancellation was applied after turn B started"
    );
    thread.abort().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn turn_watchdog_cancels_without_waiting_for_full_command_queue() {
    let host = RuntimeHost::with_policy(
        Arc::new(StuckRuntimeFactory),
        RuntimeExecutionPolicy::default()
            .with_turn_timeout_ms(100)
            .with_cancel_grace_timeout_ms(20),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "watchdog-full-queue"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let thread_id = thread.context().coordinates.thread_id;
    host.submit(thread_id, "active", "hold").await.unwrap();
    wait_for_status(&thread, ThreadStatus::Running).await;

    for queued in 0..thread.thread.command_capacity {
        host.submit(thread_id, format!("queued-{queued}"), "queued")
            .await
            .unwrap();
    }
    assert_eq!(
        thread.queued_command_count(),
        thread.thread.command_capacity
    );

    tokio::time::advance(Duration::from_millis(100)).await;
    wait_for_status(&thread, ThreadStatus::Cancelling).await;
    tokio::time::advance(Duration::from_millis(20)).await;
    wait_for_status(&thread, ThreadStatus::Failed).await;

    host.shutdown_thread(thread_id).await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn concurrent_submits_reserve_pending_input_slots_atomically() {
    let barrier = Arc::new(AdmissionAppendBarrier::default());
    let store = Arc::new(AdmissionTestStore::blocking(barrier.clone()));
    let host = RuntimeHost::with_session_store_and_policy(
        Arc::new(StuckRuntimeFactory),
        store,
        RuntimeExecutionPolicy::default().with_max_pending_inputs(2),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "concurrent-submit-cap"),
            ThreadTopology::root(),
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
        barrier.entered.load(Ordering::SeqCst),
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
                    Err(CooldisError::ThreadPolicyViolation {
                        code: "max_pending_inputs",
                        ..
                    })
                )
            })
            .count(),
        1
    );
    thread.abort().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn pending_input_cap_counts_commands_drained_into_runtime_queues() {
    let state = Arc::new(DrainedPendingInputState::default());
    let host = RuntimeHost::with_policy(
        Arc::new(DrainedPendingInputRuntimeFactory {
            state: Arc::clone(&state),
        }),
        RuntimeExecutionPolicy::default().with_max_pending_inputs(1),
    );
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "drained-pending-input"),
            ThreadTopology::root(),
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
        Err(CooldisError::ThreadPolicyViolation {
            code: "max_pending_inputs",
            ..
        })
    ));

    state.release.notify_one();
    thread.abort().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn duplicate_start_is_rejected_before_factory_build_or_runtime_spawn() {
    let factory = Arc::new(ControlledChildBuildFactory::default());
    let host = RuntimeHost::new(factory.clone());
    let root = host
        .start_thread(
            coords("tenant_a", "user_1", "duplicate-start"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let duplicate_coordinates = coords("tenant_a", "user_1", "duplicate-start");
    let topology = ThreadTopology::spawned_from(root.context().coordinates.thread_id);
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
        factory.child_builds.load(Ordering::SeqCst),
        1,
        "duplicate start must be rejected before runtime construction"
    );
    factory.release_builds();
    let first = first.await.unwrap().unwrap();
    let second = match early_second_result {
        Some(result) => result,
        None => second.await.unwrap(),
    };
    assert!(matches!(second, Err(CooldisError::ThreadAlreadyExists(_))));
    wait_for_status(&first, ThreadStatus::Idle).await;
    assert_eq!(factory.child_runtime_starts.load(Ordering::SeqCst), 1);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cancelled_start_wakes_reservation_waiters() {
    let coordinates = coords("tenant_a", "user_1", "cancelled-start-reservation");
    let thread_id = coordinates.thread_id;
    let factory = Arc::new(BlockingThreadBuildFactory::new(thread_id));
    let host = RuntimeHost::new(factory.clone());
    let start_host = host.clone();
    let start = tokio::spawn(async move {
        start_host
            .start_thread(coordinates, ThreadTopology::root())
            .await
    });
    factory.wait_until_blocked().await;

    let wait_host = host.clone();
    let mut waiter = tokio::spawn(async move {
        wait_host.wait_for_thread_start_reservation(thread_id).await;
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut waiter)
            .await
            .is_err(),
        "waiter must remain pending while the start reservation is held"
    );

    start.abort();
    match start.await {
        Err(err) => assert!(err.is_cancelled()),
        Ok(_) => panic!("blocked start unexpectedly completed"),
    }
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("reservation waiter should wake after start cancellation")
        .unwrap();
    assert!(matches!(
        host.get_thread(thread_id).await,
        Err(CooldisError::ThreadNotFound(missing)) if missing == thread_id
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn runtime_is_registered_before_its_first_kernel_control_call() {
    let factory = Arc::new(RegistrationProbeFactory::default());
    let host = RuntimeHost::new(factory.clone());
    let registration_barrier = host.inner.threads.read().await;
    let start_host = host.clone();
    let start = tokio::spawn(async move {
        start_host
            .start_thread(
                coords("tenant_a", "user_1", "startup-registration"),
                ThreadTopology::root(),
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
    let factory = Arc::new(CancellationTrackedFactory::default());
    let host = RuntimeHost::new(factory.clone());
    host.set_lifecycle_sink(Some(Arc::new(FailingAfterRuntimeStartsSink {
        active_runs: Arc::clone(&factory.active_runs),
        runtime_started: Arc::clone(&factory.runtime_started),
    })))
    .await;

    assert!(matches!(
        host.start_thread(
            coords("tenant_a", "user_1", "sink-start-failure"),
            ThreadTopology::root(),
        )
        .await,
        Err(CooldisError::RuntimeFactory(_))
    ));
    assert_eq!(
        factory.active_runs.load(Ordering::SeqCst),
        0,
        "failed start returned before its runtime task was joined"
    );
    assert!(host.snapshot().await.threads.is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cancelled_start_after_publication_cleans_up_registered_runtime() {
    let factory = Arc::new(CancellationTrackedFactory::default());
    let sink_entered = Arc::new(Notify::new());
    let sink_release = Arc::new(Notify::new());
    let host = RuntimeHost::new(factory.clone());
    host.set_lifecycle_sink(Some(Arc::new(BlockingLifecycleSink {
        entered: Arc::clone(&sink_entered),
        release: Arc::clone(&sink_release),
    })))
    .await;
    let coordinates = coords("tenant_a", "user_1", "cancel-after-publication");
    let thread_id = coordinates.thread_id;
    let start_host = host.clone();
    let start = tokio::spawn(async move {
        start_host
            .start_thread(coordinates, ThreadTopology::root())
            .await
    });

    sink_entered.notified().await;
    assert_eq!(factory.active_runs.load(Ordering::SeqCst), 1);
    start.abort();
    match start.await {
        Err(err) => assert!(err.is_cancelled()),
        Ok(_) => panic!("blocked lifecycle-sink start unexpectedly completed"),
    }

    timeout(Duration::from_millis(1), factory.wait_until_stopped())
        .await
        .expect("cancelled start left its published runtime running");
    assert!(matches!(
        host.get_thread(thread_id).await,
        Err(CooldisError::ThreadNotFound(missing)) if missing == thread_id
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn turn_timeout_emits_timeout_and_cancel_timeout_recovery_events() {
    let host = RuntimeHost::with_policy(
        Arc::new(StuckRuntimeFactory),
        RuntimeExecutionPolicy::default()
            .with_turn_timeout_ms(20)
            .with_cancel_grace_timeout_ms(20),
    );
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hold")
        .await
        .unwrap();

    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::Timeout { operation, .. } if operation == "turn"
        )
    })
    .await;
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::Timeout { operation, .. } if operation == "cancel"
        )
    })
    .await;
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::Failed { code, .. } if code == "cancel_timeout"
        )
    })
    .await;
    wait_for_status(&thread, ThreadStatus::Failed).await;
    assert_eq!(
        canonical_user_content(&thread.session_context().await.unwrap()),
        vec![vec![CanonicalContent::text("hold")]]
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn explicit_cancel_timeout_marks_thread_failed_without_extra_history() {
    let host = RuntimeHost::with_policy(
        Arc::new(StuckRuntimeFactory),
        RuntimeExecutionPolicy::default().with_cancel_grace_timeout_ms(20),
    );
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hold")
        .await
        .unwrap();
    wait_for_status(&thread, ThreadStatus::Running).await;
    let err = host
        .cancel(thread.context().coordinates.thread_id, "stop")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CooldisError::ThreadPolicyViolation {
            code: "cancel_timeout",
            ..
        }
    ));
    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::Timeout { operation, .. } if operation == "cancel"
        )
    })
    .await;
    wait_for_status(&thread, ThreadStatus::Failed).await;
    assert_eq!(
        canonical_user_content(&thread.session_context().await.unwrap()),
        vec![vec![CanonicalContent::text("hold")]]
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn shutdown_timeout_aborts_runtime_and_removes_thread() {
    let host = RuntimeHost::with_policy(
        Arc::new(StuckRuntimeFactory),
        RuntimeExecutionPolicy::default().with_shutdown_grace_timeout_ms(20),
    );
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hold")
        .await
        .unwrap();
    wait_for_status(&thread, ThreadStatus::Running).await;
    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();

    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::Timeout { operation, .. } if operation == "shutdown"
        )
    })
    .await;
    assert!(matches!(
        host.get_thread(thread.context().coordinates.thread_id)
            .await,
        Err(CooldisError::ThreadNotFound(_))
    ));
}

#[tokio::test]
async fn runtime_exit_without_terminal_status_is_marked_failed() {
    let host = RuntimeHost::new(Arc::new(ExitRuntimeFactory));
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    assert_runtime_kind(&mut events, |kind| {
        matches!(
            kind,
            RuntimeEventKind::Recovery { action, .. } if action == "mark_failed"
        )
    })
    .await;
    wait_for_status(&thread, ThreadStatus::Failed).await;
}

#[tokio::test]
async fn resume_and_fork_use_loaded_checkpoint_records() {
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let thread = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let checkpoint = host
        .create_checkpoint(
            thread.context().coordinates.thread_id,
            None,
            Some("checkpoint".to_string()),
            BTreeMap::from([("opaque".to_string(), "value".to_string())]),
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
    let child_thread_id = ThreadId::new();
    let factory = Arc::new(BlockingThreadBuildFactory::new(child_thread_id));
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(factory.clone(), store.clone());
    let parent = host
        .start_thread(
            coords("tenant_a", "user_1", "fork-reserved-no-start"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let first_checkpoint = host
        .create_checkpoint(
            parent.context().coordinates.thread_id,
            None,
            Some("first fork cut".to_string()),
            BTreeMap::new(),
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
            .active_leaf(&ThreadCoordinates {
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
            BTreeMap::new(),
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
        ThreadLineage::Branch {
            parent_thread_id,
            checkpoint_id: Some(checkpoint_id),
        } if parent_thread_id == parent.context().coordinates.thread_id
            && checkpoint_id == first_checkpoint.id
    ));
    let events = store
        .read_events(
            &EventStreamId::for_thread(&child.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let first_checkpoint_id = first_checkpoint.id.to_string();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == EventKind::SessionEntryAppended
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
async fn resume_rejects_checkpoints_created_by_non_root_threads() {
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let parent = host
        .start_thread(
            coords("tenant_a", "user_1", "resume-parent"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let parent_thread_id = parent.context().coordinates.thread_id;

    for topology in [
        ThreadTopology::spawned_from(parent_thread_id),
        ThreadTopology::branch_from(parent_thread_id, None),
    ] {
        let child = host
            .start_thread(coords("tenant_a", "user_1", "resume-child"), topology)
            .await
            .unwrap();
        let child_thread_id = child.context().coordinates.thread_id;
        let checkpoint = host
            .create_checkpoint(child_thread_id, None, None, BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(
            checkpoint.lineage,
            ThreadCheckpointLineage::Parent { parent_thread_id }
        );
        host.shutdown_thread(child_thread_id).await.unwrap();

        assert!(
            matches!(
                host.resume_thread(checkpoint.id).await,
                Err(CooldisError::CheckpointResumeRequiresRoot {
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
        .create_checkpoint(parent_thread_id, None, None, BTreeMap::new())
        .await
        .unwrap();
    let mut legacy_value = serde_json::to_value(&root_checkpoint).unwrap();
    legacy_value.as_object_mut().unwrap().remove("lineage");
    let legacy_checkpoint: ThreadCheckpoint = serde_json::from_value(legacy_value).unwrap();
    assert_eq!(legacy_checkpoint.lineage, ThreadCheckpointLineage::Unknown);
    assert!(matches!(
        host.resume_thread_from_checkpoint(legacy_checkpoint).await,
        Err(CooldisError::CheckpointResumeLineageUnknown {
            checkpoint_id,
            thread_id,
        }) if checkpoint_id == root_checkpoint.id && thread_id == parent_thread_id
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn concurrent_resume_rejects_reserved_duplicate_before_store_mutation() {
    let coordinates = coords("tenant_a", "user_1", "resume-reservation");
    let thread_id = coordinates.thread_id;
    let factory = Arc::new(BlockingThreadBuildFactory::new(thread_id));
    let store = Arc::new(AdmissionTestStore::tracking_selects());
    let host = RuntimeHost::with_session_store(factory.clone(), store.clone());
    let start_host = host.clone();
    let start_coordinates = coordinates.clone();
    let start = tokio::spawn(async move {
        start_host
            .start_thread(start_coordinates, ThreadTopology::root())
            .await
    });
    factory.wait_until_blocked().await;

    let checkpoint = ThreadCheckpoint {
        id: ThreadCheckpointId::new(),
        coordinates,
        lineage: ThreadCheckpointLineage::Root,
        parent_checkpoint_id: None,
        active_entry_id: None,
        label: None,
        metadata: BTreeMap::new(),
        created_at_ms: 0,
    };
    assert!(matches!(
        host.resume_thread_from_checkpoint(checkpoint).await,
        Err(CooldisError::ThreadAlreadyExists(existing)) if existing == thread_id
    ));
    assert_eq!(store.select_branch_calls.load(Ordering::SeqCst), 0);

    factory.release();
    start.await.unwrap().unwrap();
    host.shutdown_thread(thread_id).await.unwrap();
}

#[tokio::test]
async fn host_runs_multiple_tenants_and_routes_events() {
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let a1 = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let a2 = host
        .start_thread(coords("tenant_a", "user_1", "s2"), ThreadTopology::root())
        .await
        .unwrap();
    let b1 = host
        .start_thread(coords("tenant_b", "user_1", "s1"), ThreadTopology::root())
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
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let a = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let b = host
        .start_thread(coords("tenant_b", "user_1", "s1"), ThreadTopology::root())
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
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let parent = host
        .start_thread(coords("tenant_a", "user_1", "root"), ThreadTopology::root())
        .await
        .unwrap();
    let parent_id = parent.context().coordinates.thread_id;
    let child = host
        .start_thread(
            coords("tenant_a", "user_1", "child"),
            ThreadTopology::spawned_from(parent_id),
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
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let a = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let b = host
        .start_thread(coords("tenant_b", "user_1", "s1"), ThreadTopology::root())
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
    let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
    let a = host
        .start_thread(coords("tenant_a", "user_1", "s1"), ThreadTopology::root())
        .await
        .unwrap();
    let b = host
        .start_thread(coords("tenant_a", "user_1", "s2"), ThreadTopology::root())
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
    let thread_id = ThreadId::parse_str("00000000-0000-0000-0000-000000000051").unwrap();
    let coordinates = ThreadCoordinates {
        tenant_id: "tenant_a".to_string(),
        user_id: "user_1".to_string(),
        session_id: "stale-shutdown-cleanup".to_string(),
        thread_id,
    };
    let factory = Arc::new(GatedShutdownFactory::default());
    let host = RuntimeHost::new(factory.clone());
    let old = host
        .start_thread(coordinates.clone(), ThreadTopology::root())
        .await
        .unwrap();
    wait_for_status(&old, ThreadStatus::Idle).await;

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
    assert!(Arc::ptr_eq(&removed, &old.thread));
    let replacement = host
        .start_thread(coordinates, ThreadTopology::root())
        .await
        .unwrap();
    wait_for_status(&replacement, ThreadStatus::Idle).await;

    factory.release_shutdown.notify_one();
    shutdown.await.unwrap().unwrap();

    let resident = host
        .get_thread(thread_id)
        .await
        .expect("stale shutdown cleanup removed the replacement runtime");
    assert!(Arc::ptr_eq(&resident.thread, &replacement.thread));
    host.shutdown_thread(thread_id).await.unwrap();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn shutdown_all_uses_repeatable_children_before_parent_effect_order() {
    let parent_thread_id = ThreadId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let first_child_thread_id =
        ThreadId::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let second_child_thread_id =
        ThreadId::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
    let expected_order = vec![
        first_child_thread_id,
        second_child_thread_id,
        parent_thread_id,
    ];
    let expected_finished = vec![first_child_thread_id, second_child_thread_id];

    for run in 0..32 {
        let host = RuntimeHost::new(Arc::new(EchoRuntimeFactory));
        let parent = host
            .start_thread(
                ThreadCoordinates {
                    tenant_id: "tenant_a".to_string(),
                    user_id: "user_1".to_string(),
                    session_id: format!("shutdown-order-{run}"),
                    thread_id: parent_thread_id,
                },
                ThreadTopology::root(),
            )
            .await
            .unwrap();
        host.start_thread(
            ThreadCoordinates {
                tenant_id: "tenant_a".to_string(),
                user_id: "user_1".to_string(),
                session_id: format!("shutdown-order-{run}"),
                thread_id: second_child_thread_id,
            },
            ThreadTopology::spawned_from(parent_thread_id),
        )
        .await
        .unwrap();
        host.start_thread(
            ThreadCoordinates {
                tenant_id: "tenant_a".to_string(),
                user_id: "user_1".to_string(),
                session_id: format!("shutdown-order-{run}"),
                thread_id: first_child_thread_id,
            },
            ThreadTopology::spawned_from(parent_thread_id),
        )
        .await
        .unwrap();
        let mut parent_events = parent.subscribe_events();

        assert_eq!(host.shutdown_all().await.unwrap(), expected_order);

        let mut finished = Vec::new();
        while let Ok(event) = parent_events.try_recv() {
            if let ThreadEvent::Runtime {
                event:
                    RuntimeEvent {
                        kind:
                            RuntimeEventKind::SubthreadFinished {
                                child_thread_id, ..
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

async fn assert_output(events: &mut broadcast::Receiver<ThreadEvent>, expected: &str) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Output { text, .. } = event {
            assert_eq!(text, expected);
            return;
        }
    }
}

async fn next_output(events: &mut broadcast::Receiver<ThreadEvent>) -> String {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Output { text, .. } = event {
            return text;
        }
    }
}

async fn assert_canonical_mirror(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_text: &str,
) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::CanonicalMirror { entry, .. } = event {
            match entry.kind {
                SessionEntryKind::Message { message } => {
                    assert_eq!(message_texts(&[message]), vec![expected_text]);
                }
                other => panic!("unexpected canonical mirror entry: {other:?}"),
            }
            return;
        }
    }
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
                    crate::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or(""),
        })
        .collect()
}

async fn policy_bound_content_hash_for_config(config: serde_json::Value) -> String {
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let mut coupling = runtime_std_context_spill_coupling();
    coupling.id = "test.policy".to_string();
    coupling.function_ref = format!("op://policy/check@sha256:{}", "d".repeat(64));
    coupling.config = config;
    coupling.config_hash = "sha256:test-config".to_string();
    let coupling_set = BoundCouplingSet::new("snapshot-a", vec![coupling]);
    let metadata = BTreeMap::from([(
        THREAD_BOUND_COUPLING_SET_METADATA.to_string(),
        serde_json::to_string(&coupling_set).unwrap(),
    )]);
    let thread = host
        .start_thread_with_topology_and_metadata(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
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
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let bind_position = events
        .iter()
        .position(|event| event.kind == EventKind::ManifestBindCompleted)
        .unwrap();
    let policy_position = events
        .iter()
        .position(|event| event.kind == EventKind::PolicyBound)
        .unwrap();
    assert!(bind_position < policy_position);
    let policy = &events[policy_position];
    assert_eq!(
        policy.payload["schema"],
        EventKind::PolicyBound.payload_schema_id()
    );
    assert_eq!(policy.payload["policy_kind"], "coupling_set");
    assert_eq!(policy.payload["policy_id"], "coupling_set:snapshot-a");
    policy.payload["content_hash"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn manifest_bind_receipt_and_placement_witness_share_one_atomic_append() {
    let store = Arc::new(
        FaultingRuntimeStore::new(Arc::new(InMemorySessionStore::new())).fail_nth(
            "append_events",
            2,
            "a second manifest append must not occur",
        ),
    );
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "placement-atomic"),
            ThreadTopology::root(),
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
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ManifestBindCompleted)
            .count(),
        1
    );
    let placement_events = events
        .iter()
        .filter(|event| event.kind == EventKind::PlacementDecision)
        .collect::<Vec<_>>();
    assert_eq!(placement_events.len(), 1);
    assert_eq!(placement_events[0].origin, EventOrigin::Witnessed);
    assert_eq!(placement_events[0].payload["placement"], "local");
    assert_eq!(
        placement_events[0].payload["snapshot_id"],
        "snapshot-placement"
    );
}

#[tokio::test]
async fn cancelled_manifest_receipt_caller_cannot_leave_a_half_witnessed_workspace() {
    let barrier = Arc::new(AdmissionAppendBarrier::default());
    let store = Arc::new(AdmissionTestStore::blocking_manifest(barrier.clone()));
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "workspace-bind-cancelled"),
            ThreadTopology::root(),
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

    let stream_id = EventStreamId::for_thread(&coordinates);
    let events = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let events = store.read_events(&stream_id, None).await.unwrap();
            if events
                .iter()
                .any(|event| event.kind == EventKind::ManifestBindCompleted)
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
        .find(|event| event.kind == EventKind::ManifestBindCompleted)
        .unwrap();
    assert_eq!(bind.payload["workspace"]["guest_path"], "/work");
    assert!(
        events
            .iter()
            .any(|event| event.kind == EventKind::PlacementDecision),
        "the placement witness must commit in the same append"
    );
}

#[tokio::test]
async fn remote_manifest_receipt_rejects_a_workspace_before_witnessing_it() {
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "remote-workspace-bind"),
            ThreadTopology::root(),
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
            .read_events(&EventStreamId::for_thread(&coordinates), None)
            .await
            .unwrap()
            .iter()
            .all(|event| event.kind != EventKind::ManifestBindCompleted)
    );
}

#[tokio::test]
async fn failed_manifest_batch_leaves_no_bind_receipt_without_placement_witness() {
    let store = Arc::new(
        FaultingRuntimeStore::new(Arc::new(InMemorySessionStore::new())).fail_nth(
            "append_events",
            1,
            "manifest batch failed",
        ),
    );
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "placement-failed"),
            ThreadTopology::root(),
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
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .all(|event| event.kind != EventKind::ManifestBindCompleted)
    );
    assert!(
        events
            .iter()
            .all(|event| event.kind != EventKind::PlacementDecision)
    );
}

#[tokio::test]
async fn manifest_receipt_append_fails_closed_for_non_local_placement() {
    let store = Arc::new(FaultingRuntimeStore::new(Arc::new(
        InMemorySessionStore::new(),
    )));
    let host = RuntimeHost::with_session_store(Arc::new(EchoRuntimeFactory), store.clone());
    let thread = host
        .start_thread(
            coords("tenant_a", "user_1", "placement-fail-closed"),
            ThreadTopology::root(),
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
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(events.iter().all(|event| {
        !matches!(
            event.kind,
            EventKind::ManifestBindCompleted | EventKind::PlacementDecision
        )
    }));
}

async fn assert_runtime_kind(
    events: &mut broadcast::Receiver<ThreadEvent>,
    predicate: impl Fn(&RuntimeEventKind) -> bool,
) -> RuntimeEvent {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Runtime { event, .. } = event
            && predicate(&event.kind)
        {
            return event;
        }
    }
}

fn canonical_user_content(context: &SessionContext) -> Vec<Vec<CanonicalContent>> {
    context
        .entries
        .iter()
        .filter_map(|entry| match &entry.kind {
            SessionEntryKind::Message {
                message: CanonicalMessage::User { content, .. },
            } => Some(content.clone()),
            _ => None,
        })
        .collect()
}

#[derive(Clone)]
struct RuntimeFakeClock {
    now: Arc<std::sync::Mutex<DateTime<Utc>>>,
}

impl RuntimeFakeClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(std::sync::Mutex::new(now)),
        }
    }
}

impl DaemonClock for RuntimeFakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

struct WitnessTimerFiredSink {
    store: SqliteSessionStore,
    coordinates: ThreadCoordinates,
}

#[async_trait]
impl IngressSink for WitnessTimerFiredSink {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        let IngressContent::Event { kind, payload } = &envelope.content else {
            return Err(IoError::Bridge(
                "clock route emitted non-event ingress".to_string(),
            ));
        };
        if kind != "timer.fired" {
            return Err(IoError::Bridge(format!(
                "clock route emitted unexpected event kind {kind:?}"
            )));
        }
        let timer = serde_json::from_value::<TimerFiredPayload>(payload.clone())
            .map_err(|err| IoError::Bridge(format!("invalid timer.fired payload: {err}")))?;
        let mandate_event_id = timer.mandate_event_id;
        let control_stream = control_stream_id(&self.coordinates);
        let mut record = NewEventRecord::witnessed(
            self.coordinates.clone(),
            EventKind::TimerFired,
            serde_json::to_value(timer)
                .map_err(|err| IoError::Bridge(format!("encode timer.fired payload: {err}")))?,
        );
        record.provenance = EventProvenance {
            source_streams: vec![control_stream.clone()],
            source_event_ids: vec![mandate_event_id],
            ..EventProvenance::default()
        };
        self.store
            .append_events(&control_stream, vec![record])
            .await
            .map_err(|err| IoError::Bridge(format!("append timer.fired: {err}")))?;
        Ok(IngressAck::accepted(&envelope))
    }
}

async fn append_scheduled_loop_mandate(
    store: &SqliteSessionStore,
    coordinates: &ThreadCoordinates,
    loop_id: &str,
    created_at: DateTime<Utc>,
) -> EventRecord {
    let mut record = NewEventRecord::witnessed(
        coordinates.clone(),
        EventKind::MandateStarted,
        serde_json::to_value(MandateStartedPayload {
            subject: MandateSubject {
                thread_id: Some(coordinates.thread_id.to_string()),
                loop_id: Some(loop_id.to_string()),
            },
            mandate_id: format!("mandate-{loop_id}"),
            snapshot_id: "schedule.v1".to_string(),
            thread_id: Some(coordinates.thread_id.to_string()),
            max_continuations: None,
            expires_at_ms: None,
            schedule: Some(MandateSchedulePayload::Interval { every_ms: 60_000 }),
            max_occurrences: Some(2),
            catch_up: Some(MandateCatchUpPolicy::SkipMissed),
            input_template: Some("wake at {scheduled_for}".to_string()),
        })
        .unwrap(),
    );
    record.created_at_ms = created_at.timestamp_millis();
    store
        .append_events(&control_stream_id(coordinates), vec![record])
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn append_loop_parent_completed(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    turn_id: &str,
) -> EventRecord {
    store
        .append_events(
            &EventStreamId::for_thread(coordinates),
            vec![NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::TurnCompleted,
                serde_json::json!({ "turn_id": turn_id }),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn append_loop_mandate_started(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    loop_id: &str,
    snapshot_id: &str,
    max_continuations: Option<u32>,
) {
    store
        .append_events(
            &control_stream_id(coordinates),
            vec![NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::MandateStarted,
                serde_json::to_value(MandateStartedPayload {
                    subject: MandateSubject {
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
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    parent_event_id: EventRecordId,
    loop_id: &str,
    parent_turn_id: &str,
    snapshot_id: &str,
    next_turn_input: &str,
) -> EventRecord {
    store
        .append_events(
            &control_stream_id(coordinates),
            vec![NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::TurnContinueRequested,
                serde_json::to_value(TurnContinueRequestedPayload {
                    subject: TurnContinuationSubject {
                        loop_id: loop_id.to_string(),
                        parent_turn_id: parent_turn_id.to_string(),
                    },
                    snapshot_id: snapshot_id.to_string(),
                    next_turn_input: next_turn_input.to_string(),
                })
                .unwrap(),
                EventProvenance {
                    source_streams: vec![EventStreamId::for_thread(coordinates)],
                    source_event_ids: vec![parent_event_id],
                    discharged_by: Some("coupling:loop-test".to_string()),
                    function: Some("op://test/loop@sha256:test".to_string()),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn wait_for_status(thread: &RuntimeThreadHandle, expected: ThreadStatus) {
    let mut status = thread.subscribe_status();
    timeout(Duration::from_secs(2), async {
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
    metadata: BTreeMap<String, String>,
}

impl TsAgentThreadFixture {
    fn from_lifecycle(record: &ThreadLifecycleRecord) -> Self {
        Self {
            tenant_id: record.coordinates.tenant_id.clone(),
            user_id: record.coordinates.user_id.clone(),
            session_id: record.coordinates.session_id.clone(),
            thread_id: record.coordinates.thread_id.to_string(),
            parent_thread_id: record.parent_thread_id.map(|id| id.to_string()),
            status: lifecycle_status_to_ts(record.status).to_string(),
            latest_signal_id: record.latest_signal_id.map(|id| id.to_string()),
            latest_checkpoint_id: record.latest_checkpoint_id.map(|id| id.to_string()),
            metadata: record.metadata.clone(),
        }
    }

    fn into_lifecycle(self) -> ThreadLifecycleRecord {
        let coordinates = ThreadCoordinates {
            tenant_id: self.tenant_id,
            user_id: self.user_id,
            session_id: self.session_id,
            thread_id: ThreadId::parse_str(&self.thread_id).unwrap(),
        };
        ThreadLifecycleRecord {
            coordinates,
            parent_thread_id: self
                .parent_thread_id
                .map(|id| ThreadId::parse_str(&id).unwrap()),
            topology: ThreadTopology::root(),
            status: lifecycle_status_from_ts(&self.status),
            latest_signal_id: self
                .latest_signal_id
                .map(|id| ThreadSignalId::from_uuid(Uuid::parse_str(&id).unwrap())),
            latest_checkpoint_id: self
                .latest_checkpoint_id
                .map(|id| ThreadCheckpointId::from_uuid(Uuid::parse_str(&id).unwrap())),
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
    metadata: BTreeMap<String, String>,
}

impl TsAgentThreadSignalFixture {
    fn from_signal(signal: &ThreadSignal) -> Self {
        Self {
            signal_id: signal.id.to_string(),
            tenant_id: signal.coordinates.tenant_id.clone(),
            user_id: signal.coordinates.user_id.clone(),
            session_id: signal.coordinates.session_id.clone(),
            thread_id: signal.coordinates.thread_id.to_string(),
            kind: signal_kind_to_ts(signal.kind).to_string(),
            metadata: signal.metadata.clone(),
        }
    }

    fn into_signal(self) -> ThreadSignal {
        ThreadSignal {
            id: ThreadSignalId::from_uuid(Uuid::parse_str(&self.signal_id).unwrap()),
            coordinates: ThreadCoordinates {
                tenant_id: self.tenant_id,
                user_id: self.user_id,
                session_id: self.session_id,
                thread_id: ThreadId::parse_str(&self.thread_id).unwrap(),
            },
            kind: signal_kind_from_ts(&self.kind),
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
    lineage: ThreadCheckpointLineage,
    parent_checkpoint_id: Option<String>,
    active_entry_id: Option<String>,
    label: Option<String>,
    metadata: BTreeMap<String, String>,
}

impl TsAgentThreadCheckpointFixture {
    fn from_checkpoint(checkpoint: &ThreadCheckpoint) -> Self {
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

    fn into_checkpoint(self) -> ThreadCheckpoint {
        ThreadCheckpoint {
            id: ThreadCheckpointId::from_uuid(Uuid::parse_str(&self.checkpoint_id).unwrap()),
            coordinates: ThreadCoordinates {
                tenant_id: self.tenant_id,
                user_id: self.user_id,
                session_id: self.session_id,
                thread_id: ThreadId::parse_str(&self.thread_id).unwrap(),
            },
            lineage: self.lineage,
            parent_checkpoint_id: self
                .parent_checkpoint_id
                .map(|id| ThreadCheckpointId::from_uuid(Uuid::parse_str(&id).unwrap())),
            active_entry_id: self
                .active_entry_id
                .map(|id| SessionEntryId::from_uuid(Uuid::parse_str(&id).unwrap())),
            label: self.label,
            metadata: self.metadata,
            created_at_ms: 0,
        }
    }
}

#[test]
fn ts_style_lifecycle_thread_fixture_round_trips_core_fields_and_metadata() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let parent_thread_id = ThreadId::new();
    let record = ThreadLifecycleRecord {
        coordinates: coordinates.clone(),
        parent_thread_id: Some(parent_thread_id),
        topology: ThreadTopology::spawned_from(parent_thread_id),
        status: ThreadLifecycleStatus::Running,
        latest_signal_id: Some(ThreadSignalId::new()),
        latest_checkpoint_id: Some(ThreadCheckpointId::new()),
        created_at_ms: 10,
        updated_at_ms: 20,
        metadata: BTreeMap::from([
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
    let parent_thread_id = ThreadId::new();
    let signal =
        ThreadSignal::user_steer(&coordinates, "turn-1").with_metadata(BTreeMap::from([(
            "source_message_id".to_string(),
            "msg_123".to_string(),
        )]));
    let checkpoint = ThreadCheckpoint {
        id: ThreadCheckpointId::new(),
        coordinates: coordinates.clone(),
        lineage: ThreadCheckpointLineage::Parent { parent_thread_id },
        parent_checkpoint_id: Some(ThreadCheckpointId::new()),
        active_entry_id: Some(SessionEntryId::new()),
        label: Some("after-tool".to_string()),
        metadata: BTreeMap::from([("app_checkpoint_id".to_string(), "app_ckpt".to_string())]),
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

fn lifecycle_status_to_ts(status: ThreadLifecycleStatus) -> &'static str {
    match status {
        ThreadLifecycleStatus::Starting => "starting",
        ThreadLifecycleStatus::Idle => "idle",
        ThreadLifecycleStatus::Running => "running",
        ThreadLifecycleStatus::Cancelling => "cancelling",
        ThreadLifecycleStatus::Stopped => "stopped",
        ThreadLifecycleStatus::Failed => "failed",
    }
}

fn lifecycle_status_from_ts(status: &str) -> ThreadLifecycleStatus {
    match status {
        "starting" => ThreadLifecycleStatus::Starting,
        "idle" => ThreadLifecycleStatus::Idle,
        "running" => ThreadLifecycleStatus::Running,
        "cancelling" => ThreadLifecycleStatus::Cancelling,
        "stopped" => ThreadLifecycleStatus::Stopped,
        "failed" => ThreadLifecycleStatus::Failed,
        other => panic!("unknown ts status: {other}"),
    }
}

fn signal_kind_to_ts(kind: ThreadSignalKind) -> &'static str {
    match kind {
        ThreadSignalKind::InterruptCancel => "interrupt_cancel",
        ThreadSignalKind::Shutdown => "shutdown",
        ThreadSignalKind::UserQueue => "user_queue",
        ThreadSignalKind::UserSteer => "user_steer",
        ThreadSignalKind::UserInterrupt => "user_interrupt",
        ThreadSignalKind::CheckpointRequested => "checkpoint_requested",
        ThreadSignalKind::CheckpointCreated => "checkpoint_created",
        ThreadSignalKind::Failed => "failed",
    }
}

fn signal_kind_from_ts(kind: &str) -> ThreadSignalKind {
    match kind {
        "interrupt_cancel" => ThreadSignalKind::InterruptCancel,
        "shutdown" => ThreadSignalKind::Shutdown,
        "user_queue" => ThreadSignalKind::UserQueue,
        "user_steer" => ThreadSignalKind::UserSteer,
        "user_interrupt" => ThreadSignalKind::UserInterrupt,
        "checkpoint_requested" => ThreadSignalKind::CheckpointRequested,
        "checkpoint_created" => ThreadSignalKind::CheckpointCreated,
        "failed" => ThreadSignalKind::Failed,
        other => panic!("unknown ts signal kind: {other}"),
    }
}
