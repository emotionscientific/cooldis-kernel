use crate::agent::manifest_bind::canonical_json_hash;
use crate::{
    AgentManifestBindReceipt, BoundCouplingSet, CooldisError, CooldisResult, EventKind,
    EventProvenance, EventRecord, EventRecordId, EventSequence, EventStreamId, HistoryError,
    KernelThreadSpawnAgentBinding, KernelThreadSpawnAgentResolver, NewEventRecord, RuntimeHost,
    RuntimeThreadHandle, STD_SUPERVISOR_SPAWN_TEMPLATE_ID, THREAD_AGENT_MANIFEST_HASH_METADATA,
    THREAD_BOUND_COUPLING_SET_METADATA, THREAD_SPAWN_GRANTED_METADATA,
    THREAD_SPAWN_INPUTS_HASH_METADATA, THREADS_SPAWN_CAPABILITY, ThreadCoordinates, ThreadId,
    ThreadSpawnRequestedPayload, ThreadSpawnWitness, ThreadSpawnedPayload, TurnInput,
};
use cooldis_runtime_contracts::{DispatchId, HandleId};
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

const THREAD_SPAWN_PROJECTOR_DISCHARGED_BY: &str = "projector:thread-spawn";
const THREAD_SPAWN_PROJECTOR_FUNCTION: &str = "thread_spawn_projector/v1";
const THREAD_SPAWN_DISPATCH_FUNCTION: &str = "thread_spawn_dispatch/v1";
const THREAD_SPAWN_CLAIM_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const THREAD_SPAWN_CLAIM_POLL_INTERVAL: Duration = Duration::from_millis(10);
const UNBOUND_CHILD_AGENT_REF: &str = "unbound";

enum FencedDecisionAppend {
    Appended(EventRecordId),
    AlreadyProjected,
}

#[cfg(test)]
struct ProjectionPause {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
impl Default for ProjectionPause {
    fn default() -> Self {
        Self {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

#[cfg(test)]
impl ProjectionPause {
    async fn pause(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }

    async fn wait_until_paused(&self) {
        self.entered.acquire().await.unwrap().forget();
    }

    fn release(&self) {
        self.release.add_permits(1);
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum FencedAppendReceiptOverride {
    Empty,
    WrongEventId,
    WrongProvenance,
}

#[derive(Clone)]
pub struct ThreadSpawnProjector {
    host: RuntimeHost,
    agent_resolver: Option<Arc<dyn KernelThreadSpawnAgentResolver>>,
    claimed_dispatch_wait_timeout: Duration,
    #[cfg(test)]
    snapshot_barrier: Option<Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    snapshot_pause: Option<Arc<ProjectionPause>>,
    #[cfg(test)]
    after_claim_pause: Option<Arc<ProjectionPause>>,
    #[cfg(test)]
    fenced_append_receipt_override: Option<FencedAppendReceiptOverride>,
}

impl ThreadSpawnProjector {
    pub fn new(host: RuntimeHost) -> Self {
        Self {
            host,
            agent_resolver: None,
            claimed_dispatch_wait_timeout: THREAD_SPAWN_CLAIM_WAIT_TIMEOUT,
            #[cfg(test)]
            snapshot_barrier: None,
            #[cfg(test)]
            snapshot_pause: None,
            #[cfg(test)]
            after_claim_pause: None,
            #[cfg(test)]
            fenced_append_receipt_override: None,
        }
    }

    pub fn with_agent_resolver(
        mut self,
        resolver: Arc<dyn KernelThreadSpawnAgentResolver>,
    ) -> Self {
        self.agent_resolver = Some(resolver);
        self
    }

    /// Atomically folds or records a thread-spawn dispatch, then returns the
    /// original thread handle. The control-stream append fence is the
    /// serialization point: only the caller that claims the one durable
    /// request performs the child effect; retries fold the request and its
    /// spawned/failure decision instead of appending or executing again.
    pub async fn dispatch_request(
        &self,
        coordinates: &ThreadCoordinates,
        payload: ThreadSpawnRequestedPayload,
    ) -> CooldisResult<ThreadSpawnDispatchReceipt> {
        self.dispatch_request_with_authority(coordinates, payload, true)
            .await
    }

    pub(crate) async fn dispatch_request_with_authority(
        &self,
        coordinates: &ThreadCoordinates,
        payload: ThreadSpawnRequestedPayload,
        require_supervisor_grant: bool,
    ) -> CooldisResult<ThreadSpawnDispatchReceipt> {
        if payload.parent_thread_id != coordinates.thread_id {
            return Err(CooldisError::RuntimeExecution(format!(
                "thread spawn dispatch parent {} does not match control stream {}",
                payload.parent_thread_id, coordinates.thread_id
            )));
        }
        let dispatch_id = DispatchId::new(payload.correlation_id.clone());
        let stream_id = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mut claimed_wait_started_at = None;
        #[cfg(test)]
        let mut first_snapshot = true;
        loop {
            let events = self
                .host
                .runtime_store()
                .read_events(&stream_id, None)
                .await
                .map_err(|err| CooldisError::History(err.to_string()))?;
            #[cfg(test)]
            if first_snapshot && let Some(barrier) = &self.snapshot_barrier {
                barrier.wait().await;
            }
            #[cfg(test)]
            {
                first_snapshot = false;
            }

            if let Some(folded) = fold_thread_spawn_dispatch(&events, &dispatch_id) {
                if let Some(handle) = folded.handle {
                    ThreadId::parse_str(&handle.id).map_err(|err| {
                        CooldisError::History(format!(
                            "thread spawn dispatch {} folded invalid child handle: {err}",
                            dispatch_id
                        ))
                    })?;
                    return Ok(ThreadSpawnDispatchReceipt {
                        request_event_id: folded.request_event_id,
                        handle,
                        dispatch_id,
                        submitted_turn_id: folded.submitted_turn_id,
                        task_name: folded.task_name,
                    });
                }
                if let Some(reason) = folded.failure_reason {
                    return Err(CooldisError::RuntimeExecution(reason));
                }
                if folded.claimed {
                    let started_at =
                        claimed_wait_started_at.get_or_insert_with(tokio::time::Instant::now);
                    let elapsed = started_at.elapsed();
                    if elapsed >= self.claimed_dispatch_wait_timeout {
                        return Err(CooldisError::RuntimeExecution(format!(
                            "thread spawn dispatch {dispatch_id} is claimed without a terminal decision; refusing to replay the child effect"
                        )));
                    }
                    tokio::time::sleep(
                        THREAD_SPAWN_CLAIM_POLL_INTERVAL
                            .min(self.claimed_dispatch_wait_timeout - elapsed),
                    )
                    .await;
                    continue;
                }
                let request_event = events
                    .iter()
                    .find(|event| event.id == folded.request_event_id)
                    .expect("folded request event must come from the folded event slice");
                match self
                    .claim_request(request_event, dispatch_id.as_str(), &events)
                    .await?
                {
                    FencedDecisionAppend::AlreadyProjected => {
                        claimed_wait_started_at.get_or_insert_with(tokio::time::Instant::now);
                        continue;
                    }
                    FencedDecisionAppend::Appended(_) => {
                        let parent = self.host.get_thread(coordinates.thread_id).await?;
                        let request_payload = serde_json::from_value(request_event.payload.clone())
                            .map_err(|err| {
                                CooldisError::History(format!(
                                    "thread spawn dispatch payload decode failed: {err}"
                                ))
                            })?;
                        let projected = match self
                            .project_claimed(
                                parent,
                                request_event,
                                request_payload,
                                require_supervisor_grant,
                            )
                            .await
                        {
                            Ok(projected) => projected,
                            Err(err) => {
                                let reason = err.to_string();
                                self.append_failure(
                                    coordinates,
                                    request_event,
                                    Some(dispatch_id.as_str()),
                                    reason,
                                )
                                .await?;
                                return Err(err);
                            }
                        };
                        return Ok(ThreadSpawnDispatchReceipt {
                            request_event_id: projected.request_event_id,
                            handle: HandleId::thread(projected.child_thread_id),
                            dispatch_id,
                            submitted_turn_id: Some(projected.submitted_turn_id),
                            task_name: projected.task_name,
                        });
                    }
                }
            }

            let mut value = serde_json::to_value(&payload).map_err(|err| {
                CooldisError::History(format!(
                    "thread spawn dispatch payload encode failed: {err}"
                ))
            })?;
            value["schema"] = json!(EventKind::ThreadSpawnRequested.payload_schema_id());
            let request = NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::ThreadSpawnRequested,
                value,
                EventProvenance {
                    discharged_by: Some("dispatcher:thread-spawn".to_string()),
                    function: Some(THREAD_SPAWN_DISPATCH_FUNCTION.to_string()),
                    ..EventProvenance::default()
                },
            );
            let expected_next_sequence = events
                .last()
                .map(|event| EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| EventSequence::new(1));
            match self
                .host
                .runtime_store()
                .append_events_fenced(&stream_id, expected_next_sequence, vec![request])
                .await
            {
                Ok(_) | Err(HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(CooldisError::History(err.to_string())),
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn with_snapshot_barrier(mut self, barrier: Arc<tokio::sync::Barrier>) -> Self {
        self.snapshot_barrier = Some(barrier);
        self
    }

    #[cfg(test)]
    fn with_claimed_dispatch_wait_timeout(mut self, timeout: Duration) -> Self {
        self.claimed_dispatch_wait_timeout = timeout;
        self
    }

    #[cfg(test)]
    fn with_snapshot_pause(mut self, pause: Arc<ProjectionPause>) -> Self {
        self.snapshot_pause = Some(pause);
        self
    }

    #[cfg(test)]
    fn with_after_claim_pause(mut self, pause: Arc<ProjectionPause>) -> Self {
        self.after_claim_pause = Some(pause);
        self
    }

    #[cfg(test)]
    fn with_fenced_append_receipt_override(
        mut self,
        receipt_override: FencedAppendReceiptOverride,
    ) -> Self {
        self.fenced_append_receipt_override = Some(receipt_override);
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
        #[cfg(test)]
        if let Some(pause) = &self.snapshot_pause {
            pause.pause().await;
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
                    self.record_preclaim_failure(
                        &mut receipt,
                        coordinates,
                        event,
                        event
                            .payload
                            .get("correlation_id")
                            .and_then(JsonValue::as_str),
                        reason,
                        &decision_events,
                    )
                    .await?;
                    continue;
                }
            };
            if spawn_request_already_projected(
                &decision_events,
                event.id,
                Some(&payload.correlation_id),
            ) {
                receipt.skipped.push(event.id);
                continue;
            }

            if event.coordinates.thread_id != coordinates.thread_id {
                let reason = format!(
                    "thread.spawn.requested event thread {} does not match projected stream {}",
                    event.coordinates.thread_id, coordinates.thread_id
                );
                self.record_preclaim_failure(
                    &mut receipt,
                    coordinates,
                    event,
                    Some(&payload.correlation_id),
                    reason,
                    &decision_events,
                )
                .await?;
                continue;
            }
            if event.coordinates.scope() != coordinates.scope() {
                let reason = CooldisError::ThreadScopeMismatch {
                    thread_id: coordinates.thread_id,
                    requested: Box::new(coordinates.scope()),
                    actual: Box::new(event.coordinates.scope()),
                }
                .to_string();
                self.record_preclaim_failure(
                    &mut receipt,
                    coordinates,
                    event,
                    Some(&payload.correlation_id),
                    reason,
                    &decision_events,
                )
                .await?;
                continue;
            }
            if payload.parent_thread_id != coordinates.thread_id {
                let reason = format!(
                    "thread.spawn.requested parent_thread_id {} does not match projected stream {}",
                    payload.parent_thread_id, coordinates.thread_id
                );
                self.record_preclaim_failure(
                    &mut receipt,
                    coordinates,
                    event,
                    Some(&payload.correlation_id),
                    reason,
                    &decision_events,
                )
                .await?;
                continue;
            }
            let parent = match self.host.get_thread(payload.parent_thread_id).await {
                Ok(parent) => parent,
                Err(err) => {
                    let reason = err.to_string();
                    self.record_preclaim_failure(
                        &mut receipt,
                        coordinates,
                        event,
                        Some(&payload.correlation_id),
                        reason,
                        &decision_events,
                    )
                    .await?;
                    continue;
                }
            };
            if parent.context().coordinates.scope() != coordinates.scope() {
                let reason = CooldisError::ThreadScopeMismatch {
                    thread_id: coordinates.thread_id,
                    requested: Box::new(coordinates.scope()),
                    actual: Box::new(parent.context().coordinates.scope()),
                }
                .to_string();
                self.record_preclaim_failure(
                    &mut receipt,
                    &parent.context().coordinates,
                    event,
                    Some(&payload.correlation_id),
                    reason,
                    &decision_events,
                )
                .await?;
                continue;
            }

            match self
                .claim_request(event, &payload.correlation_id, &decision_events)
                .await?
            {
                FencedDecisionAppend::AlreadyProjected => {
                    receipt.skipped.push(event.id);
                    continue;
                }
                FencedDecisionAppend::Appended(_) => {}
            }
            match self.project_claimed(parent, event, payload, true).await {
                Ok(projected) => receipt.projected.push(projected),
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

    async fn claim_request(
        &self,
        request_event: &EventRecord,
        correlation_id: &str,
        decision_events: &[EventRecord],
    ) -> CooldisResult<FencedDecisionAppend> {
        let claim = NewEventRecord::discharged(
            request_event.coordinates.clone(),
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
        let appended = self
            .append_fenced_decision(request_event, Some(correlation_id), decision_events, claim)
            .await?;
        if matches!(appended, FencedDecisionAppend::Appended(_)) {
            #[cfg(test)]
            if let Some(pause) = &self.after_claim_pause {
                pause.pause().await;
            }
        }
        Ok(appended)
    }

    async fn project_claimed(
        &self,
        parent: RuntimeThreadHandle,
        request_event: &EventRecord,
        payload: ThreadSpawnRequestedPayload,
        require_supervisor_grant: bool,
    ) -> CooldisResult<ThreadSpawnProjected> {
        if require_supervisor_grant && !parent_allows_supervisor_spawn(&parent.context().metadata)?
        {
            return Err(CooldisError::RuntimeExecution(format!(
                "{STD_SUPERVISOR_SPAWN_TEMPLATE_ID} projector requires parent thread bound coupling grant {THREADS_SPAWN_CAPABILITY}"
            )));
        }

        let arguments = if let Some(task_name) = &payload.task_name {
            let mut arguments = json!({
                "task_name": task_name,
                "message": payload.initial_submission,
            });
            if payload.child_agent_ref != UNBOUND_CHILD_AGENT_REF {
                arguments["agent_ref"] = json!(payload.child_agent_ref);
            }
            arguments
        } else {
            json!({
                "agent_ref": payload.child_agent_ref,
                "message": payload.initial_submission,
            })
        };
        let (agent_binding, metadata) = self
            .spawn_metadata(&parent.context().clone(), &arguments)
            .await?;
        let submitted_turn_id = payload
            .submitted_turn_id
            .clone()
            .unwrap_or_else(|| format!("thread-spawn-{}", request_event.id));
        let receipt = self
            .host
            .kernel_control()
            .spawn_child_with_witness(
                parent.context(),
                payload
                    .task_name
                    .clone()
                    .or_else(|| Some(payload.correlation_id.clone())),
                TurnInput::text(payload.initial_submission.clone()),
                metadata,
                ThreadSpawnWitness {
                    parent_turn_id: payload.parent_turn_id.clone(),
                    correlation_id: Some(payload.correlation_id.clone()),
                    request_stream_id: Some(request_event.stream_id.clone()),
                    request_event_id: Some(request_event.id),
                    submitted_turn_id: Some(submitted_turn_id),
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
        Ok(ThreadSpawnProjected {
            request_event_id: request_event.id,
            child_thread_id: receipt.thread_id,
            submitted_turn_id: receipt.submitted_turn_id,
            correlation_id: payload.correlation_id,
            task_name: payload.task_name,
        })
    }

    async fn append_fenced_decision(
        &self,
        request_event: &EventRecord,
        correlation_id: Option<&str>,
        decision_events: &[EventRecord],
        decision: NewEventRecord,
    ) -> CooldisResult<FencedDecisionAppend> {
        let mut events = decision_events.to_vec();
        loop {
            if spawn_request_already_projected(&events, request_event.id, correlation_id) {
                return Ok(FencedDecisionAppend::AlreadyProjected);
            }
            let expected_next_sequence = events
                .last()
                .map(|event| EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| EventSequence::new(1));
            let expected = EventRecord::from_new(
                request_event.stream_id.clone(),
                expected_next_sequence,
                decision.clone(),
            );
            match self
                .host
                .runtime_store()
                .append_events_fenced(
                    &request_event.stream_id,
                    expected_next_sequence,
                    vec![decision.clone()],
                )
                .await
            {
                Ok(appended) => {
                    #[cfg(test)]
                    let appended = {
                        let mut appended = appended;
                        if let Some(receipt_override) = self.fenced_append_receipt_override {
                            match receipt_override {
                                FencedAppendReceiptOverride::Empty => appended.clear(),
                                FencedAppendReceiptOverride::WrongEventId => {
                                    if let Some(event) = appended.first_mut() {
                                        event.id = EventRecordId::new();
                                    }
                                }
                                FencedAppendReceiptOverride::WrongProvenance => {
                                    if let Some(event) = appended.first_mut() {
                                        event.provenance.discharged_by = None;
                                    }
                                }
                            }
                        }
                        appended
                    };
                    if appended.len() != 1 {
                        return Err(CooldisError::History(format!(
                            "fenced thread spawn decision append returned {} record(s), expected 1",
                            appended.len()
                        )));
                    }
                    let appended = appended.into_iter().next().ok_or_else(|| {
                        CooldisError::History(
                            "fenced thread spawn decision append returned no record".to_string(),
                        )
                    })?;
                    if appended != expected {
                        return Err(CooldisError::History(format!(
                            "fenced thread spawn decision append returned unexpected record {} at {} sequence {}",
                            appended.id,
                            appended.stream_id,
                            appended.sequence.get()
                        )));
                    }
                    return Ok(FencedDecisionAppend::Appended(appended.id));
                }
                Err(HistoryError::AppendFenceConflict { .. }) => {
                    events = self
                        .host
                        .runtime_store()
                        .read_events(&request_event.stream_id, None)
                        .await
                        .map_err(|err| CooldisError::History(err.to_string()))?;
                }
                Err(err) => return Err(CooldisError::History(err.to_string())),
            }
        }
    }

    async fn record_preclaim_failure(
        &self,
        receipt: &mut ThreadSpawnProjectionReceipt,
        failure_coordinates: &ThreadCoordinates,
        request_event: &EventRecord,
        correlation_id: Option<&str>,
        reason: String,
        decision_events: &[EventRecord],
    ) -> CooldisResult<()> {
        let failure =
            self.failure_record(failure_coordinates, request_event, correlation_id, &reason);
        match self
            .append_fenced_decision(request_event, correlation_id, decision_events, failure)
            .await?
        {
            FencedDecisionAppend::Appended(failure_event_id) => {
                receipt.failed.push(ThreadSpawnProjectionFailure {
                    request_event_id: request_event.id,
                    failure_event_id,
                    reason,
                });
            }
            FencedDecisionAppend::AlreadyProjected => receipt.skipped.push(request_event.id),
        }
        Ok(())
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
        correlation_id: Option<&str>,
        reason: String,
    ) -> CooldisResult<EventRecord> {
        let failure = self.failure_record(coordinates, request_event, correlation_id, &reason);
        let expected_failure = failure.clone();
        let mut appended = self
            .host
            .runtime_store()
            .append_events(&request_event.stream_id, vec![failure])
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        if appended.len() != 1 {
            return Err(CooldisError::History(format!(
                "thread spawn failure append returned {} record(s), expected 1",
                appended.len()
            )));
        }
        let appended = appended.pop().ok_or_else(|| {
            CooldisError::History("thread spawn failure append returned no record".to_string())
        })?;
        let expected = EventRecord::from_new(
            request_event.stream_id.clone(),
            appended.sequence,
            expected_failure,
        );
        if appended != expected {
            return Err(CooldisError::History(format!(
                "thread spawn failure append returned unexpected record {} on {}",
                appended.id, appended.stream_id
            )));
        }
        Ok(appended)
    }

    fn failure_record(
        &self,
        coordinates: &ThreadCoordinates,
        request_event: &EventRecord,
        correlation_id: Option<&str>,
        reason: &str,
    ) -> NewEventRecord {
        let payload = json!({
            "schema": EventKind::LoopDenied.payload_schema_id(),
            "template_id": STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
            "request_event_id": request_event.id.to_string(),
            "correlation_id": correlation_id
                .map(ToString::to_string)
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
        NewEventRecord::discharged(
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
        )
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
    pub task_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnProjectionFailure {
    pub request_event_id: EventRecordId,
    pub failure_event_id: EventRecordId,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnDispatchReceipt {
    pub request_event_id: EventRecordId,
    pub handle: HandleId,
    pub dispatch_id: DispatchId,
    pub submitted_turn_id: Option<String>,
    pub task_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnDispatchFold {
    pub request_event_id: EventRecordId,
    pub handle: Option<HandleId>,
    pub submitted_turn_id: Option<String>,
    pub task_name: Option<String>,
    pub failure_reason: Option<String>,
    pub claimed: bool,
}

/// Durable spawn-time binding for a thread handle.
///
/// The binding is a fold, not a second record: the original
/// `thread.spawn.requested` anchors the consumer control stream and dispatch
/// identity, while its witnessed `thread.spawned` decision supplies the
/// minted child handle. Terminal ingress resolution uses this carrier after
/// restart and never depends on a live subscription to the child.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ThreadHandleBinding {
    pub spawned_event_id: EventRecordId,
    pub consumer: ThreadCoordinates,
    pub dispatch_id: DispatchId,
    pub handle: HandleId,
}

/// Folds the durable records for one dispatch identity. The original
/// non-claim `thread.spawn.requested` record anchors the fold; spawned and
/// failure decisions join by its existing `correlation_id` wire value. Raw
/// pre-handle-lane request payloads decode because every newer field is
/// optional with a serde default.
pub fn fold_thread_spawn_dispatch(
    events: &[EventRecord],
    dispatch_id: &DispatchId,
) -> Option<ThreadSpawnDispatchFold> {
    let (request, payload) = events.iter().find_map(|event| {
        if event.kind != EventKind::ThreadSpawnRequested || is_spawn_request_claim(event) {
            return None;
        }
        let payload =
            serde_json::from_value::<ThreadSpawnRequestedPayload>(event.payload.clone()).ok()?;
        (payload.correlation_id == dispatch_id.as_str()).then_some((event, payload))
    })?;
    let spawned = events.iter().find(|event| {
        event.kind == EventKind::ThreadSpawned
            && event
                .payload
                .get("correlation_id")
                .and_then(JsonValue::as_str)
                == Some(dispatch_id.as_str())
    });
    let handle = spawned
        .and_then(|event| event.payload.get("child_thread_id"))
        .and_then(JsonValue::as_str)
        .and_then(|value| ThreadId::parse_str(value).ok())
        .map(HandleId::thread);
    let failure_reason = events
        .iter()
        .find(|event| {
            event.kind == EventKind::LoopDenied
                && event
                    .payload
                    .get("correlation_id")
                    .and_then(JsonValue::as_str)
                    == Some(dispatch_id.as_str())
        })
        .and_then(|event| event.payload.get("reason"))
        .and_then(JsonValue::as_str)
        .map(ToString::to_string);
    let claimed = events.iter().any(|event| {
        is_spawn_request_claim(event)
            && (event.provenance.source_event_ids.contains(&request.id)
                || event
                    .payload
                    .get("correlation_id")
                    .and_then(JsonValue::as_str)
                    == Some(dispatch_id.as_str()))
    });
    Some(ThreadSpawnDispatchFold {
        request_event_id: request.id,
        handle,
        submitted_turn_id: Some(
            payload
                .submitted_turn_id
                .unwrap_or_else(|| format!("thread-spawn-{}", request.id)),
        ),
        task_name: payload.task_name,
        failure_reason,
        claimed,
    })
}

/// Folds all valid thread-handle bindings witnessed on one consumer control
/// stream. Conflicting records fail closed so settlement cannot be routed to
/// an ambiguous consumer or handle.
pub(crate) fn fold_thread_handle_bindings(
    events: &[EventRecord],
) -> CooldisResult<Vec<ThreadHandleBinding>> {
    let mut bindings = BTreeMap::<String, ThreadHandleBinding>::new();
    for spawned in events
        .iter()
        .filter(|event| event.kind == EventKind::ThreadSpawned)
    {
        let Some(correlation_id) = spawned
            .payload
            .get("correlation_id")
            .and_then(JsonValue::as_str)
        else {
            continue;
        };
        let spawned_payload = serde_json::from_value::<ThreadSpawnedPayload>(
            spawned.payload.clone(),
        )
        .map_err(|err| {
            CooldisError::History(format!(
                "thread handle binding spawned payload decode failed: {err}"
            ))
        })?;
        if spawned_payload.parent_thread_id != spawned.coordinates.thread_id {
            return Err(CooldisError::History(format!(
                "thread handle binding parent {} does not match consumer stream {}",
                spawned_payload.parent_thread_id, spawned.coordinates.thread_id
            )));
        }
        let request = events
            .iter()
            .filter(|event| {
                event.kind == EventKind::ThreadSpawnRequested && !is_spawn_request_claim(event)
            })
            .find_map(|event| {
                let payload =
                    serde_json::from_value::<ThreadSpawnRequestedPayload>(event.payload.clone())
                        .ok()?;
                (payload.correlation_id == correlation_id).then_some((event, payload))
            })
            .ok_or_else(|| {
                CooldisError::History(format!(
                    "thread handle binding {correlation_id} is missing its spawn request"
                ))
            })?;
        if request.1.parent_thread_id != spawned.coordinates.thread_id {
            return Err(CooldisError::History(format!(
                "thread handle binding request parent {} does not match consumer stream {}",
                request.1.parent_thread_id, spawned.coordinates.thread_id
            )));
        }
        if !spawned.provenance.source_event_ids.contains(&request.0.id) {
            return Err(CooldisError::History(format!(
                "thread handle binding {correlation_id} spawned receipt is missing request provenance"
            )));
        }
        let binding = ThreadHandleBinding {
            spawned_event_id: spawned.id,
            consumer: spawned.coordinates.clone(),
            dispatch_id: DispatchId::new(correlation_id),
            handle: HandleId::thread(spawned_payload.child_thread_id),
        };
        if let Some(existing) = bindings.insert(correlation_id.to_string(), binding.clone())
            && existing != binding
        {
            return Err(CooldisError::History(format!(
                "thread handle binding {correlation_id} has conflicting spawned receipts"
            )));
        }
    }
    Ok(bindings.into_values().collect())
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
    correlation_id: Option<&str>,
) -> bool {
    events.iter().any(|event| {
        (is_spawn_request_claim(event)
            && (event
                .provenance
                .source_event_ids
                .contains(&request_event_id)
                || correlation_id.is_some_and(|correlation_id| {
                    event
                        .payload
                        .get("correlation_id")
                        .and_then(JsonValue::as_str)
                        == Some(correlation_id)
                })))
            || (matches!(event.kind, EventKind::ThreadSpawned | EventKind::LoopDenied)
                && (event
                    .provenance
                    .source_event_ids
                    .contains(&request_event_id)
                    || correlation_id.is_some_and(|correlation_id| {
                        event
                            .payload
                            .get("correlation_id")
                            .and_then(JsonValue::as_str)
                            == Some(correlation_id)
                    })))
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

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn unrelated_control_append_does_not_strand_spawn_request() {
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
        append_spawn_requested(&store, &coordinates, "projector-unrelated-append").await;
        let pause = Arc::new(ProjectionPause::default());
        let projector = ThreadSpawnProjector::new(host.clone()).with_snapshot_pause(pause.clone());
        let projected_coordinates = coordinates.clone();
        let projection = tokio::spawn(async move {
            projector
                .project_control_stream(&projected_coordinates)
                .await
        });

        pause.wait_until_paused().await;
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        store
            .append_events(
                &control_stream,
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::TurnSubmitted,
                    json!({
                        "schema": EventKind::TurnSubmitted.payload_schema_id(),
                        "turn_id": "unrelated-control-writer",
                    }),
                )],
            )
            .await
            .unwrap();
        pause.release();

        let receipt = projection.await.unwrap().unwrap();
        assert_eq!(receipt.projected.len(), 1);
        assert!(receipt.skipped.is_empty());
        assert!(receipt.failed.is_empty());
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 1);
        assert_eq!(
            store
                .read_events(&control_stream, None)
                .await
                .unwrap()
                .into_iter()
                .filter(is_spawn_request_claim)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_requests_with_same_correlation_produce_one_child() {
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
        append_spawn_requested(&store, &coordinates, "shared-correlation").await;
        append_spawn_requested(&store, &coordinates, "shared-correlation").await;
        let pause = Arc::new(ProjectionPause::default());
        let first = ThreadSpawnProjector::new(host.clone()).with_after_claim_pause(pause.clone());
        let first_coordinates = coordinates.clone();
        let first_projection =
            tokio::spawn(async move { first.project_control_stream(&first_coordinates).await });

        pause.wait_until_paused().await;
        let second_receipt = ThreadSpawnProjector::new(host.clone())
            .project_control_stream(&coordinates)
            .await
            .unwrap();
        pause.release();
        let first_receipt = first_projection.await.unwrap().unwrap();
        let receipts = [first_receipt, second_receipt];

        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.projected.len())
                .sum::<usize>(),
            1
        );
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 1);
        let control_stream = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let control_events = store.read_events(&control_stream, None).await.unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(|event| is_spawn_request_claim(event))
                .count(),
            1
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == EventKind::ThreadSpawned)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_dispatches_with_same_id_append_one_request_and_return_one_handle() {
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
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first =
            ThreadSpawnProjector::new(host.clone()).with_snapshot_barrier(Arc::clone(&barrier));
        let second = ThreadSpawnProjector::new(host.clone()).with_snapshot_barrier(barrier);
        let request = ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            task_name: Some("worker".to_string()),
            submitted_turn_id: Some("thread-spawn-dispatch-race-1".to_string()),
            child_agent_ref: UNBOUND_CHILD_AGENT_REF.to_string(),
            initial_submission: "echo one child".to_string(),
            correlation_id: "dispatch-race-1".to_string(),
            block_parent: false,
        };

        let (first, second) = tokio::join!(
            first.dispatch_request(&coordinates, request.clone()),
            second.dispatch_request(&coordinates, request),
        );
        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first, second);
        let control_events = store
            .read_events(
                &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(|event| {
                    event.kind == EventKind::ThreadSpawnRequested && !is_spawn_request_claim(event)
                })
                .count(),
            1
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| is_spawn_request_claim(event))
                .count(),
            1
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == EventKind::ThreadSpawned)
                .count(),
            1
        );
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 1);

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_dispatches_with_different_ids_append_two_requests_and_spawn_two_children() {
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
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first =
            ThreadSpawnProjector::new(host.clone()).with_snapshot_barrier(Arc::clone(&barrier));
        let second = ThreadSpawnProjector::new(host.clone()).with_snapshot_barrier(barrier);
        let request = |dispatch_id: &str, task_name: &str| ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            task_name: Some(task_name.to_string()),
            submitted_turn_id: Some(format!("thread-spawn-{dispatch_id}")),
            child_agent_ref: UNBOUND_CHILD_AGENT_REF.to_string(),
            initial_submission: format!("echo {task_name}"),
            correlation_id: dispatch_id.to_string(),
            block_parent: false,
        };

        let (first, second) = tokio::join!(
            first.dispatch_request(
                &coordinates,
                request("dispatch-race-distinct-1", "worker-1")
            ),
            second.dispatch_request(
                &coordinates,
                request("dispatch-race-distinct-2", "worker-2")
            ),
        );
        let first = first.unwrap();
        let second = second.unwrap();

        assert_ne!(first.handle, second.handle);
        let control_events = store
            .read_events(
                &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(|event| {
                    event.kind == EventKind::ThreadSpawnRequested && !is_spawn_request_claim(event)
                })
                .count(),
            2
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| is_spawn_request_claim(event))
                .count(),
            2
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == EventKind::ThreadSpawned)
                .count(),
            2
        );
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 2);

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retry_after_dispatch_claim_without_decision_fails_closed_instead_of_spinning() {
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
        let request = ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            task_name: Some("worker".to_string()),
            submitted_turn_id: Some("thread-spawn-dispatch-dead-claim-1".to_string()),
            child_agent_ref: UNBOUND_CHILD_AGENT_REF.to_string(),
            initial_submission: "echo never started".to_string(),
            correlation_id: "dispatch-dead-claim-1".to_string(),
            block_parent: false,
        };
        let pause = Arc::new(ProjectionPause::default());
        let projector =
            ThreadSpawnProjector::new(host.clone()).with_after_claim_pause(pause.clone());
        let dispatched_coordinates = coordinates.clone();
        let dispatched_request = request.clone();
        let dispatch = tokio::spawn(async move {
            projector
                .dispatch_request(&dispatched_coordinates, dispatched_request)
                .await
        });

        pause.wait_until_paused().await;
        dispatch.abort();
        assert!(dispatch.await.unwrap_err().is_cancelled());

        let err = ThreadSpawnProjector::new(host.clone())
            .with_claimed_dispatch_wait_timeout(std::time::Duration::ZERO)
            .dispatch_request(&coordinates, request)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("claimed without a terminal decision"),
            "unexpected error: {err}"
        );
        assert!(host.children_of(coordinates.thread_id).await.is_empty());
        let control_events = store
            .read_events(
                &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(|event| is_spawn_request_claim(event))
                .count(),
            1
        );
        assert!(
            control_events
                .iter()
                .all(|event| event.kind != EventKind::ThreadSpawned)
        );

        host.shutdown_all().await.unwrap();
    }

    #[test]
    fn legacy_spawn_request_decodes_and_folds_to_original_handle() {
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        let stream_id = EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let request_id = EventRecordId::new();
        let mut legacy_request = NewEventRecord::witnessed(
            coordinates.clone(),
            EventKind::ThreadSpawnRequested,
            serde_json::from_str(
                r#"{"schema":"cooldis.thread.spawn.requested/1","parent_thread_id":"018f0000-0000-7000-8000-000000000001","parent_turn_id":"parent-turn-1","child_agent_ref":"unbound","initial_submission":"legacy child","correlation_id":"legacy-dispatch-1","block_parent":false}"#,
            )
            .unwrap(),
        );
        legacy_request.id = request_id;
        let request =
            EventRecord::from_new(stream_id.clone(), EventSequence::new(1), legacy_request);
        let child_thread_id = ThreadId::new();
        let spawned = EventRecord::from_new(
            stream_id,
            EventSequence::new(2),
            NewEventRecord::witnessed(
                coordinates,
                EventKind::ThreadSpawned,
                json!({
                    "schema": EventKind::ThreadSpawned.payload_schema_id(),
                    "parent_thread_id": "018f0000-0000-7000-8000-000000000001",
                    "child_thread_id": child_thread_id,
                    "child_manifest_hash": "unbound",
                    "granted": [],
                    "inputs_hash": "sha256:legacy",
                    "correlation_id": "legacy-dispatch-1"
                }),
            ),
        );

        let folded =
            fold_thread_spawn_dispatch(&[request, spawned], &DispatchId::new("legacy-dispatch-1"))
                .unwrap();
        assert_eq!(folded.request_event_id, request_id);
        assert_eq!(folded.handle, Some(HandleId::thread(child_thread_id)));
        assert_eq!(
            folded.submitted_turn_id.as_deref(),
            Some(format!("thread-spawn-{request_id}").as_str())
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_rejections_emit_one_failure() {
        let store = Arc::new(InMemorySessionStore::new());
        let host = RuntimeHost::with_session_store(
            Arc::new(VirtualBashRuntimeFactory::default()),
            store.clone(),
        );
        let coordinates = ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            ThreadTopology::root(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
        append_spawn_requested(&store, &coordinates, "projector-rejected-race").await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first = ThreadSpawnProjector::new(host.clone()).with_snapshot_barrier(barrier.clone());
        let second = ThreadSpawnProjector::new(host.clone()).with_snapshot_barrier(barrier);

        let (first_receipt, second_receipt) = tokio::join!(
            first.project_control_stream(&coordinates),
            second.project_control_stream(&coordinates),
        );
        let receipts = [first_receipt.unwrap(), second_receipt.unwrap()];

        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.failed.len())
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
        assert!(receipts.iter().all(|receipt| receipt.projected.is_empty()));
        assert!(host.children_of(coordinates.thread_id).await.is_empty());
        let control_events = store
            .read_events(
                &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(|event| is_spawn_request_claim(event))
                .count(),
            1
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == EventKind::LoopDenied)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn forged_projection_coordinates_cannot_spawn_in_another_scope() {
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
        let mut forged = coordinates.clone();
        forged.tenant_id = "other-tenant".to_string();
        append_spawn_requested(&store, &forged, "projector-forged-scope").await;

        let receipt = ThreadSpawnProjector::new(host.clone())
            .project_control_stream(&forged)
            .await
            .unwrap();

        assert_eq!(receipt.failed.len(), 1);
        assert!(receipt.projected.is_empty());
        assert!(host.children_of(coordinates.thread_id).await.is_empty());
        let control_events = store
            .read_events(
                &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert!(!control_events.iter().any(is_spawn_request_claim));
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == EventKind::LoopDenied)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_fenced_append_receipt_prevents_spawn() {
        for receipt_override in [
            FencedAppendReceiptOverride::Empty,
            FencedAppendReceiptOverride::WrongEventId,
            FencedAppendReceiptOverride::WrongProvenance,
        ] {
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
            append_spawn_requested(&store, &coordinates, "projector-invalid-claim-receipt").await;

            let result = ThreadSpawnProjector::new(host.clone())
                .with_fenced_append_receipt_override(receipt_override)
                .project_control_stream(&coordinates)
                .await;

            assert!(result.is_err());
            assert!(host.children_of(coordinates.thread_id).await.is_empty());
            host.shutdown_all().await.unwrap();
        }
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
            task_name: None,
            submitted_turn_id: None,
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
