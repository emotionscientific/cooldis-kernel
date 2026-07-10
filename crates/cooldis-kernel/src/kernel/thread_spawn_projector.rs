use crate::agent::manifest_bind::canonical_json_hash;
use crate::{
    AgentManifestBindReceipt, BoundCouplingSet, CooldisError, CooldisResult, EventKind,
    EventProvenance, EventRecord, EventRecordId, EventSequence, EventStreamId, HistoryError,
    KernelThreadSpawnAgentBinding, KernelThreadSpawnAgentResolver, NewEventRecord, RuntimeHost,
    STD_SUPERVISOR_SPAWN_TEMPLATE_ID, THREAD_AGENT_MANIFEST_HASH_METADATA,
    THREAD_BOUND_COUPLING_SET_METADATA, THREAD_SPAWN_GRANTED_METADATA,
    THREAD_SPAWN_INPUTS_HASH_METADATA, THREADS_SPAWN_CAPABILITY, ThreadCoordinates, ThreadId,
    ThreadSpawnRequestedPayload, ThreadSpawnWitness, TurnInput,
};
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::sync::Arc;

const THREAD_SPAWN_PROJECTOR_DISCHARGED_BY: &str = "projector:thread-spawn";
const THREAD_SPAWN_PROJECTOR_FUNCTION: &str = "thread_spawn_projector/v1";
const UNBOUND_CHILD_AGENT_REF: &str = "unbound";

enum ThreadSpawnProjectionAttempt {
    Projected(ThreadSpawnProjected),
    FenceConflict,
}

#[derive(Clone)]
pub struct ThreadSpawnProjector {
    host: RuntimeHost,
    agent_resolver: Option<Arc<dyn KernelThreadSpawnAgentResolver>>,
    #[cfg(test)]
    snapshot_barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl ThreadSpawnProjector {
    pub fn new(host: RuntimeHost) -> Self {
        Self {
            host,
            agent_resolver: None,
            #[cfg(test)]
            snapshot_barrier: None,
        }
    }

    pub fn with_agent_resolver(
        mut self,
        resolver: Arc<dyn KernelThreadSpawnAgentResolver>,
    ) -> Self {
        self.agent_resolver = Some(resolver);
        self
    }

    #[cfg(test)]
    fn with_snapshot_barrier(mut self, barrier: Arc<tokio::sync::Barrier>) -> Self {
        self.snapshot_barrier = Some(barrier);
        self
    }

    pub async fn project_control_stream(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> CooldisResult<ThreadSpawnProjectionReceipt> {
        let stream_id = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let events = self
            .host
            .runtime_store()
            .read_events(&stream_id, None)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        #[cfg(test)]
        if let Some(barrier) = &self.snapshot_barrier {
            barrier.wait().await;
        }
        let requests = events
            .iter()
            .filter(|event| event.kind == EventKind::ThreadSpawnRequested)
            .filter(|event| !is_spawn_request_claim(event))
            .cloned()
            .collect::<Vec<_>>();
        let mut receipt = ThreadSpawnProjectionReceipt::default();
        let mut decision_events = events;
        for (index, event) in requests.iter().enumerate() {
            if index > 0 {
                decision_events = self
                    .host
                    .runtime_store()
                    .read_events(&stream_id, None)
                    .await
                    .map_err(|err| CooldisError::History(err.to_string()))?;
            }
            let payload = match serde_json::from_value::<ThreadSpawnRequestedPayload>(
                event.payload.clone(),
            ) {
                Ok(payload) => payload,
                Err(err) => {
                    let reason = format!("invalid thread.spawn.requested payload: {err}");
                    let failure = self
                        .append_failure(coordinates, event, None, reason.clone())
                        .await?;
                    receipt.failed.push(ThreadSpawnProjectionFailure {
                        request_event_id: event.id,
                        failure_event_id: failure.id,
                        reason,
                    });
                    continue;
                }
            };
            if spawn_request_already_projected(&decision_events, event.id, &payload.correlation_id)
            {
                receipt.skipped.push(event.id);
                continue;
            }
            let expected_next_sequence = decision_events
                .last()
                .map(|event| EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| EventSequence::new(1));
            match self
                .project_one(coordinates, event, payload, expected_next_sequence)
                .await
            {
                Ok(ThreadSpawnProjectionAttempt::Projected(projected)) => {
                    receipt.projected.push(projected)
                }
                Ok(ThreadSpawnProjectionAttempt::FenceConflict) => receipt.skipped.push(event.id),
                Err(err) => {
                    let reason = err.to_string();
                    let failure = self
                        .append_failure(coordinates, event, None, reason.clone())
                        .await?;
                    receipt.failed.push(ThreadSpawnProjectionFailure {
                        request_event_id: event.id,
                        failure_event_id: failure.id,
                        reason,
                    });
                }
            }
        }
        Ok(receipt)
    }

    async fn project_one(
        &self,
        coordinates: &ThreadCoordinates,
        request_event: &EventRecord,
        payload: ThreadSpawnRequestedPayload,
        expected_next_sequence: EventSequence,
    ) -> CooldisResult<ThreadSpawnProjectionAttempt> {
        if payload.parent_thread_id != coordinates.thread_id {
            return Err(CooldisError::RuntimeExecution(format!(
                "thread.spawn.requested parent_thread_id {} does not match projected stream {}",
                payload.parent_thread_id, coordinates.thread_id
            )));
        }
        let parent = self.host.get_thread(payload.parent_thread_id).await?;
        if !parent_allows_supervisor_spawn(&parent.context().metadata)? {
            return Err(CooldisError::RuntimeExecution(format!(
                "{STD_SUPERVISOR_SPAWN_TEMPLATE_ID} projector requires parent thread bound coupling grant {THREADS_SPAWN_CAPABILITY}"
            )));
        }

        let arguments = json!({
            "agent_ref": payload.child_agent_ref,
            "message": payload.initial_submission,
        });
        let (agent_binding, metadata) = self
            .spawn_metadata(&parent.context().clone(), &arguments)
            .await?;
        let claim = NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::ThreadSpawnRequested,
            request_event.payload.clone(),
            EventProvenance {
                source_streams: vec![request_event.stream_id.clone()],
                source_event_ids: vec![request_event.id],
                discharged_by: Some(THREAD_SPAWN_PROJECTOR_DISCHARGED_BY.to_string()),
                function: Some(THREAD_SPAWN_PROJECTOR_FUNCTION.to_string()),
                ..EventProvenance::default()
            },
        );
        match self
            .host
            .runtime_store()
            .append_events_fenced(
                &request_event.stream_id,
                expected_next_sequence,
                vec![claim],
            )
            .await
        {
            Ok(_) => {}
            Err(HistoryError::AppendFenceConflict { .. }) => {
                return Ok(ThreadSpawnProjectionAttempt::FenceConflict);
            }
            Err(err) => return Err(CooldisError::History(err.to_string())),
        }
        let receipt = self
            .host
            .kernel_control()
            .spawn_child_with_witness(
                parent.context(),
                Some(payload.correlation_id.clone()),
                TurnInput::text(payload.initial_submission.clone()),
                metadata,
                ThreadSpawnWitness {
                    parent_turn_id: payload.parent_turn_id.clone(),
                    correlation_id: Some(payload.correlation_id.clone()),
                    request_stream_id: Some(request_event.stream_id.clone()),
                    request_event_id: Some(request_event.id),
                },
            )
            .await?;
        if let Some(binding) = agent_binding {
            self.host
                .kernel_control()
                .record_manifest_receipts_for_thread(
                    parent.context(),
                    receipt.thread_id,
                    binding.compile_receipt,
                    binding.bind_receipt,
                )
                .await?;
        }
        Ok(ThreadSpawnProjectionAttempt::Projected(
            ThreadSpawnProjected {
                request_event_id: request_event.id,
                child_thread_id: receipt.thread_id,
                submitted_turn_id: receipt.submitted_turn_id,
                correlation_id: payload.correlation_id,
            },
        ))
    }

    async fn spawn_metadata(
        &self,
        caller: &crate::ThreadContext,
        arguments: &JsonValue,
    ) -> CooldisResult<(
        Option<KernelThreadSpawnAgentBinding>,
        BTreeMap<String, String>,
    )> {
        let inputs_hash = canonical_json_hash(arguments)?;
        let agent_ref = arguments
            .get("agent_ref")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .trim();
        let agent_binding = if agent_ref.is_empty() || agent_ref == UNBOUND_CHILD_AGENT_REF {
            None
        } else {
            let resolver = self.agent_resolver.as_ref().ok_or_else(|| {
                CooldisError::RuntimeExecution(
                    "thread spawn projector agent_ref requires a manifest resolver".to_string(),
                )
            })?;
            Some(resolver.resolve_agent_ref(caller, agent_ref).await?)
        };
        let mut metadata = agent_binding
            .as_ref()
            .map(|binding| binding.metadata.clone())
            .unwrap_or_default();
        metadata.insert(THREAD_SPAWN_INPUTS_HASH_METADATA.to_string(), inputs_hash);
        if let Some(binding) = &agent_binding {
            let bind_receipt =
                serde_json::from_value::<AgentManifestBindReceipt>(binding.bind_receipt.clone())
                    .map_err(|err| {
                        CooldisError::RuntimeFactory(format!(
                            "thread spawn projector agent_ref bind receipt is invalid: {err}"
                        ))
                    })?;
            metadata
                .entry(THREAD_AGENT_MANIFEST_HASH_METADATA.to_string())
                .or_insert_with(|| bind_receipt.manifest_hash.clone());
            let granted = serde_json::to_string(&bind_receipt.granted).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to encode thread spawn projector grants: {err}"
                ))
            })?;
            metadata.insert(THREAD_SPAWN_GRANTED_METADATA.to_string(), granted);
        }
        Ok((agent_binding, metadata))
    }

    async fn append_failure(
        &self,
        coordinates: &ThreadCoordinates,
        request_event: &EventRecord,
        correlation_id: Option<String>,
        reason: String,
    ) -> CooldisResult<EventRecord> {
        let payload = json!({
            "schema": EventKind::LoopDenied.payload_schema_id(),
            "template_id": STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
            "request_event_id": request_event.id.to_string(),
            "correlation_id": correlation_id
                .or_else(|| {
                    request_event
                        .payload
                        .get("correlation_id")
                        .and_then(JsonValue::as_str)
                        .map(ToString::to_string)
                }),
            "status": "failed",
            "error_class": "thread_spawn_failed",
            "reason": reason,
        });
        let stream_id = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        self.host
            .runtime_store()
            .append_events(
                &stream_id,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::LoopDenied,
                    payload,
                    EventProvenance {
                        source_streams: vec![request_event.stream_id.clone()],
                        source_event_ids: vec![request_event.id],
                        discharged_by: Some(THREAD_SPAWN_PROJECTOR_DISCHARGED_BY.to_string()),
                        function: Some(THREAD_SPAWN_PROJECTOR_FUNCTION.to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                CooldisError::History("thread spawn failure append returned no record".to_string())
            })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ThreadSpawnProjectionReceipt {
    pub projected: Vec<ThreadSpawnProjected>,
    pub skipped: Vec<EventRecordId>,
    pub failed: Vec<ThreadSpawnProjectionFailure>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnProjected {
    pub request_event_id: EventRecordId,
    pub child_thread_id: ThreadId,
    pub submitted_turn_id: String,
    pub correlation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnProjectionFailure {
    pub request_event_id: EventRecordId,
    pub failure_event_id: EventRecordId,
    pub reason: String,
}

fn parent_allows_supervisor_spawn(metadata: &BTreeMap<String, String>) -> CooldisResult<bool> {
    let Some(raw) = metadata.get(THREAD_BOUND_COUPLING_SET_METADATA) else {
        return Ok(false);
    };
    let coupling_set = serde_json::from_str::<BoundCouplingSet>(raw).map_err(|err| {
        CooldisError::RuntimeFactory(format!("thread bound coupling set is invalid: {err}"))
    })?;
    Ok(coupling_set.couplings.iter().any(|coupling| {
        coupling.id == STD_SUPERVISOR_SPAWN_TEMPLATE_ID
            && coupling
                .grants
                .iter()
                .any(|grant| grant == THREADS_SPAWN_CAPABILITY)
    }))
}

fn spawn_request_already_projected(
    events: &[EventRecord],
    request_event_id: EventRecordId,
    correlation_id: &str,
) -> bool {
    events.iter().any(|event| {
        (is_spawn_request_claim(event)
            && event
                .provenance
                .source_event_ids
                .contains(&request_event_id))
            || (matches!(event.kind, EventKind::ThreadSpawned | EventKind::LoopDenied)
                && (event
                    .provenance
                    .source_event_ids
                    .contains(&request_event_id)
                    || event
                        .payload
                        .get("correlation_id")
                        .and_then(JsonValue::as_str)
                        == Some(correlation_id)))
    })
}

fn is_spawn_request_claim(event: &EventRecord) -> bool {
    event.kind == EventKind::ThreadSpawnRequested
        && event.provenance.discharged_by.as_deref() == Some(THREAD_SPAWN_PROJECTOR_DISCHARGED_BY)
        && event.provenance.function.as_deref() == Some(THREAD_SPAWN_PROJECTOR_FUNCTION)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentManifestCouplingBudget, AgentManifestCouplingQuota, BoundCoupling,
        BoundCouplingFunction, BoundCouplingSelector, BoundCouplingSink, CouplingRole,
        CouplingRunStatus, CouplingScheduler, EventStore, InMemorySessionStore, NewEventRecord,
        RuntimeHost, STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID, StdlibCouplingExecutor,
        ThreadSpawnedPayload, ThreadTopology, VirtualBashRuntimeFactory,
    };
    use serde_json::json;

    #[tokio::test]
    async fn thread_spawn_projector_spawns_child_and_witnesses_thread_spawned() {
        let store = Arc::new(InMemorySessionStore::new());
        let host = RuntimeHost::with_session_store(
            Arc::new(VirtualBashRuntimeFactory::default()),
            store.clone(),
        );
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let root = host
            .start_thread_with_topology_and_metadata(
                coordinates.clone(),
                ThreadTopology::root(),
                parent_metadata_with_spawn_grant(),
            )
            .await
            .unwrap();
        let request = append_spawn_requested(&store, &coordinates, "projector-spawn-1").await;

        let receipt = ThreadSpawnProjector::new(host.clone())
            .project_control_stream(&coordinates)
            .await
            .unwrap();

        assert_eq!(receipt.projected.len(), 1);
        assert_eq!(receipt.projected[0].request_event_id, request.id);
        assert_eq!(receipt.projected[0].correlation_id, "projector-spawn-1");

        let children = host.children_of(coordinates.thread_id).await;
        assert_eq!(children.len(), 1);
        assert_eq!(
            children[0].context().parent_thread_id,
            Some(coordinates.thread_id)
        );

        let control_events = root.read_control_events().await.unwrap();
        let claim = control_events
            .iter()
            .find(|event| is_spawn_request_claim(event))
            .unwrap();
        assert_eq!(claim.provenance.source_event_ids, vec![request.id]);
        let spawned = control_events
            .iter()
            .find(|event| event.kind == EventKind::ThreadSpawned)
            .unwrap();
        assert!(claim.sequence.get() < spawned.sequence.get());
        let payload: ThreadSpawnedPayload =
            serde_json::from_value(spawned.payload.clone()).unwrap();
        assert_eq!(payload.parent_thread_id, coordinates.thread_id);
        assert_eq!(
            payload.child_thread_id,
            receipt.projected[0].child_thread_id
        );
        assert_eq!(payload.parent_turn_id.as_deref(), Some("parent-turn-1"));
        assert_eq!(spawned.payload["correlation_id"], "projector-spawn-1");
        assert_eq!(spawned.provenance.source_event_ids, vec![request.id]);

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_thread_spawn_projectors_produce_exactly_one_spawn() {
        let store = Arc::new(InMemorySessionStore::new());
        let host = RuntimeHost::with_session_store(
            Arc::new(VirtualBashRuntimeFactory::default()),
            store.clone(),
        );
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        append_spawn_requested(&store, &coordinates, "projector-race-1").await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first =
            ThreadSpawnProjector::new(host.clone()).with_snapshot_barrier(Arc::clone(&barrier));
        let second = ThreadSpawnProjector::new(host.clone()).with_snapshot_barrier(barrier);

        let (first_receipt, second_receipt) = tokio::join!(
            first.project_control_stream(&coordinates),
            second.project_control_stream(&coordinates),
        );
        let receipts = [first_receipt.unwrap(), second_receipt.unwrap()];

        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.projected.len())
                .sum::<usize>(),
            1
        );
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.skipped.len())
                .sum::<usize>(),
            1
        );
        assert!(receipts.iter().all(|receipt| receipt.failed.is_empty()));
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 1);
        assert_eq!(
            store
                .read_events(
                    &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                    None,
                )
                .await
                .unwrap()
                .into_iter()
                .filter(|event| event.kind == EventKind::ThreadSpawned)
                .count(),
            1
        );
        assert_eq!(
            store
                .read_events(
                    &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                    None,
                )
                .await
                .unwrap()
                .into_iter()
                .filter(is_spawn_request_claim)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn supervisor_spawn_projector_then_child_completion_resumes_parent() {
        let store = Arc::new(InMemorySessionStore::new());
        let host = RuntimeHost::with_session_store(
            Arc::new(VirtualBashRuntimeFactory::default()),
            store.clone(),
        );
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let thread_stream = EventStreamId::for_thread(&coordinates);
        let submitted = store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnSubmitted,
                    json!({
                        "schema": EventKind::TurnSubmitted.payload_schema_id(),
                        "turn_id": "parent-turn-1",
                    }),
                )],
            )
            .await
            .unwrap();
        let executor = StdlibCouplingExecutor;
        let scheduler = CouplingScheduler::new(store.as_ref(), &executor);
        let spawn_receipt = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_supervisor_spawn_coupling(json!({
                        "initial_submission": "echo child evidence",
                        "parent_turn_id": "parent-turn-1",
                        "correlation_id": "projector-spawn-2",
                        "block_parent": true,
                    }))],
                ),
                submitted,
            )
            .await
            .unwrap();
        assert_eq!(spawn_receipt.runs[0].status, CouplingRunStatus::Completed);

        let projection = ThreadSpawnProjector::new(host.clone())
            .project_control_stream(&coordinates)
            .await
            .unwrap();
        let child_thread_id = projection.projected[0].child_thread_id;
        let completed =
            append_routed_child_completion(&store, &coordinates, child_thread_id, "child-turn-1")
                .await;
        let completion = scheduler
            .run_batch(
                &BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_supervisor_child_completion_coupling(json!({
                        "watch_coupling_id": STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
                        "on_completed": "request_continuation",
                        "loop_id": "supervisor-release",
                        "parent_turn_id": "parent-turn-1",
                        "next_turn_input": "incorporate child release evidence",
                        "reason": "child completion should resume the supervisor",
                    }))],
                ),
                completed,
            )
            .await
            .unwrap();

        assert_eq!(completion.runs.len(), 1);
        assert_eq!(completion.runs[0].status, CouplingRunStatus::Completed);
        let control_events = store
            .read_events(
                &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        let continued = control_events
            .iter()
            .find(|event| event.kind == EventKind::TurnContinueRequested)
            .unwrap();
        assert_eq!(
            continued.payload["subject"]["parent_turn_id"],
            "parent-turn-1"
        );
        assert_eq!(
            continued.payload["child"]["child_thread_id"],
            child_thread_id.to_string()
        );

        host.shutdown_all().await.unwrap();
    }

    async fn append_spawn_requested(
        store: &InMemorySessionStore,
        coordinates: &ThreadCoordinates,
        correlation_id: &str,
    ) -> EventRecord {
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mut payload = serde_json::to_value(ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            child_agent_ref: UNBOUND_CHILD_AGENT_REF.to_string(),
            initial_submission: "echo projected child".to_string(),
            correlation_id: correlation_id.to_string(),
            block_parent: true,
        })
        .unwrap();
        payload["schema"] = json!(EventKind::ThreadSpawnRequested.payload_schema_id());
        store
            .append_events(
                &control_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ThreadSpawnRequested,
                    payload,
                    EventProvenance {
                        source_streams: vec![EventStreamId::for_thread(coordinates)],
                        source_event_ids: vec![EventRecordId::new()],
                        discharged_by: Some("coupling:std::supervisor.spawn".to_string()),
                        function: Some("op://std-supervisor-spawn/run".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
    }

    async fn append_routed_child_completion(
        store: &InMemorySessionStore,
        coordinates: &ThreadCoordinates,
        child_thread_id: ThreadId,
        child_turn_id: &str,
    ) -> Vec<EventRecord> {
        let thread_stream = EventStreamId::for_thread(coordinates);
        store
            .append_events(
                &thread_stream,
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::TurnCompleted,
                    json!({
                        "schema": EventKind::TurnCompleted.payload_schema_id(),
                        "turn_id": child_turn_id,
                        "parent_thread_id": coordinates.thread_id.to_string(),
                        "child_thread_id": child_thread_id.to_string(),
                        "status": "completed",
                        "output_text": "child finished release evidence collection",
                    }),
                    EventProvenance {
                        source_streams: vec![EventStreamId::new(format!(
                            "thread:{}",
                            child_thread_id
                        ))],
                        discharged_by: Some("runtime:child-thread".to_string()),
                        function: Some("child_turn_completion/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
    }

    fn parent_metadata_with_spawn_grant() -> BTreeMap<String, String> {
        BTreeMap::from([(
            THREAD_BOUND_COUPLING_SET_METADATA.to_string(),
            serde_json::to_string(&BoundCouplingSet::new(
                "snapshot-a",
                vec![std_supervisor_spawn_coupling(json!({
                    "initial_submission": "echo projected child",
                }))],
            ))
            .unwrap(),
        )])
    }

    fn std_supervisor_spawn_coupling(config: serde_json::Value) -> crate::BoundCoupling {
        BoundCoupling {
            id: STD_SUPERVISOR_SPAWN_TEMPLATE_ID.to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::TurnSubmitted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::TurnSubmitted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::ThreadSpawnRequested, EventKind::TurnWaiting],
            },
            function_ref: format!("op://std-supervisor-spawn/run@sha256:{}", "i".repeat(64)),
            function: BoundCouplingFunction {
                name: "std-supervisor-spawn".to_string(),
                artifact_hash: "i".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
                THREADS_SPAWN_CAPABILITY.to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config,
            config_hash: "sha256:supervisor-spawn".to_string(),
        }
    }

    fn std_supervisor_child_completion_coupling(config: serde_json::Value) -> crate::BoundCoupling {
        BoundCoupling {
            id: STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID.to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: AgentManifestCouplingQuota::default(),
            source_selectors: vec![BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![EventKind::TurnContinueRequested, EventKind::LoopCompleted],
            },
            function_ref: format!(
                "op://std-supervisor-child-completion/run@sha256:{}",
                "j".repeat(64)
            ),
            function: BoundCouplingFunction {
                name: "std-supervisor-child-completion".to_string(),
                artifact_hash: "j".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config,
            config_hash: "sha256:supervisor-child-completion".to_string(),
        }
    }
}
