const THREAD_SPAWN_PROJECTOR_DISCHARGED_BY: &str = "projector:thread-spawn";
const THREAD_SPAWN_PROJECTOR_FUNCTION: &str = "thread_spawn_projector/v1";
const THREAD_SPAWN_DISPATCH_FUNCTION: &str = "thread_spawn_dispatch/v1";
const THREAD_SPAWN_CLAIM_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const THREAD_SPAWN_CLAIM_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);
const UNBOUND_CHILD_AGENT_REF: &str = "unbound";

enum FencedDecisionAppend {
    Appended(verlet_history::EventRecordId),
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
    host: crate::kernel::runtime_host::RuntimeHost,
    agent_resolver:
        Option<std::sync::Arc<dyn crate::agent::agent_process::KernelThreadSpawnAgentResolver>>,
    claimed_dispatch_wait_timeout: std::time::Duration,
    #[cfg(test)]
    snapshot_barrier: Option<std::sync::Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    snapshot_pause: Option<std::sync::Arc<ProjectionPause>>,
    #[cfg(test)]
    after_claim_pause: Option<std::sync::Arc<ProjectionPause>>,
    #[cfg(test)]
    fenced_append_receipt_override: Option<FencedAppendReceiptOverride>,
}

impl ThreadSpawnProjector {
    pub fn new(host: crate::kernel::runtime_host::RuntimeHost) -> Self {
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
        resolver: std::sync::Arc<dyn crate::agent::agent_process::KernelThreadSpawnAgentResolver>,
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
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        payload: verlet_history::ThreadSpawnRequestedPayload,
    ) -> crate::kernel::runtime_host::VerletResult<ThreadSpawnDispatchReceipt> {
        self.dispatch_request_with_authority(coordinates, payload, true)
            .await
    }

    pub(crate) async fn dispatch_request_with_authority(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        payload: verlet_history::ThreadSpawnRequestedPayload,
        require_supervisor_grant: bool,
    ) -> crate::kernel::runtime_host::VerletResult<ThreadSpawnDispatchReceipt> {
        if payload.parent_thread_id != coordinates.thread_id {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                format!(
                    "thread spawn dispatch parent {} does not match control stream {}",
                    payload.parent_thread_id, coordinates.thread_id
                ),
            ));
        }
        let dispatch_id =
            verlet_runtime_contracts::handle::DispatchId::new(payload.correlation_id.clone());
        let stream_id =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mut claimed_wait_started_at = None;
        #[cfg(test)]
        let mut first_snapshot = true;
        loop {
            let events = self
                .host
                .runtime_store()
                .read_events(&stream_id, None)
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(err.to_string())
                })?;
            #[cfg(test)]
            if first_snapshot && let Some(barrier) = &self.snapshot_barrier {
                barrier.wait().await;
            }
            #[cfg(test)]
            {
                first_snapshot = false;
            }

            if let Some(folded) = fold_thread_spawn_dispatch(&events, &dispatch_id) {
                let request_event = events
                    .iter()
                    .find(|event| event.id == folded.request_event_id)
                    .ok_or_else(|| {
                        crate::kernel::runtime_host::VerletError::History(
                            "folded thread spawn request is absent from its event slice"
                                .to_string(),
                        )
                    })?;
                let folded_payload = serde_json::from_value::<
                    verlet_history::ThreadSpawnRequestedPayload,
                >(request_event.payload.clone())
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "folded thread spawn request payload decode failed: {err}"
                    ))
                })?;
                if !spawn_request_belongs_to_parent(request_event, &folded_payload, coordinates) {
                    return Err(crate::kernel::runtime_host::VerletError::History(
                        "thread spawn dispatch matched an out-of-scope request record".to_string(),
                    ));
                }
                if let Some(handle) = folded.handle {
                    verlet_runtime_contracts::ThreadId::parse_str(&handle.id).map_err(|err| {
                        crate::kernel::runtime_host::VerletError::History(format!(
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
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        reason,
                    ));
                }
                if folded.claimed {
                    let started_at =
                        claimed_wait_started_at.get_or_insert_with(tokio::time::Instant::now);
                    let elapsed = started_at.elapsed();
                    if elapsed >= self.claimed_dispatch_wait_timeout {
                        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                            format!(
                                "thread spawn dispatch {dispatch_id} is claimed without a terminal decision; refusing to replay the child effect"
                            ),
                        ));
                    }
                    tokio::time::sleep(
                        THREAD_SPAWN_CLAIM_POLL_INTERVAL
                            .min(self.claimed_dispatch_wait_timeout - elapsed),
                    )
                    .await;
                    continue;
                }
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
                                crate::kernel::runtime_host::VerletError::History(format!(
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
                            handle: verlet_runtime_contracts::handle::HandleId::thread(
                                projected.child_thread_id,
                            ),
                            dispatch_id,
                            submitted_turn_id: Some(projected.submitted_turn_id),
                            task_name: projected.task_name,
                        });
                    }
                }
            }

            if let Some(task_name) = payload.task_name.as_deref() {
                if task_name.trim().is_empty() {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        "thread_spawn task_name must not be empty".to_string(),
                    ));
                }
                let task_name_is_reserved = events.iter().any(|event| {
                    if event.kind != verlet_history::EventKind::ThreadSpawnRequested
                        || is_spawn_request_claim(event)
                    {
                        return false;
                    }
                    serde_json::from_value::<verlet_history::ThreadSpawnRequestedPayload>(
                        event.payload.clone(),
                    )
                    .is_ok_and(|existing| {
                        spawn_request_belongs_to_parent(event, &existing, coordinates)
                            && existing.task_name.as_deref() == Some(task_name)
                            && existing.correlation_id != dispatch_id.as_str()
                    })
                });
                if task_name_is_reserved {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        format!(
                            "thread_spawn task_name {task_name:?} is already bound under this parent; retry with the original dispatch or choose a new task_name"
                        ),
                    ));
                }
            }

            let mut value = serde_json::to_value(&payload).map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "thread spawn dispatch payload encode failed: {err}"
                ))
            })?;
            value["schema"] = serde_json::json!(
                verlet_history::EventKind::ThreadSpawnRequested.payload_schema_id()
            );
            let request = verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::ThreadSpawnRequested,
                value,
                verlet_history::EventProvenance {
                    discharged_by: Some("dispatcher:thread-spawn".to_string()),
                    function: Some(THREAD_SPAWN_DISPATCH_FUNCTION.to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            );
            let expected_next_sequence = events
                .last()
                .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| verlet_history::EventSequence::new(1));
            match self
                .host
                .runtime_store()
                .append_events_fenced(&stream_id, expected_next_sequence, vec![request])
                .await
            {
                Ok(_) | Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => {
                    return Err(crate::kernel::runtime_host::VerletError::History(
                        err.to_string(),
                    ));
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn with_snapshot_barrier(
        mut self,
        barrier: std::sync::Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.snapshot_barrier = Some(barrier);
        self
    }

    #[cfg(test)]
    fn with_claimed_dispatch_wait_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.claimed_dispatch_wait_timeout = timeout;
        self
    }

    #[cfg(test)]
    fn with_snapshot_pause(mut self, pause: std::sync::Arc<ProjectionPause>) -> Self {
        self.snapshot_pause = Some(pause);
        self
    }

    #[cfg(test)]
    fn with_after_claim_pause(mut self, pause: std::sync::Arc<ProjectionPause>) -> Self {
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
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> crate::kernel::runtime_host::VerletResult<ThreadSpawnProjectionReceipt> {
        let stream_id =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let events = self
            .host
            .runtime_store()
            .read_events(&stream_id, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
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
            .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawnRequested)
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
                    .map_err(|err| {
                        crate::kernel::runtime_host::VerletError::History(err.to_string())
                    })?;
            }
            let payload = match serde_json::from_value::<verlet_history::ThreadSpawnRequestedPayload>(
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
                            .and_then(serde_json::Value::as_str),
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
                let reason = crate::kernel::runtime_host::VerletError::ThreadScopeMismatch {
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
                let reason = crate::kernel::runtime_host::VerletError::ThreadScopeMismatch {
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
        request_event: &verlet_history::EventRecord,
        correlation_id: &str,
        decision_events: &[verlet_history::EventRecord],
    ) -> crate::kernel::runtime_host::VerletResult<FencedDecisionAppend> {
        let claim = verlet_history::NewEventRecord::discharged(
            request_event.coordinates.clone(),
            verlet_history::EventKind::ThreadSpawnRequested,
            request_event.payload.clone(),
            verlet_history::EventProvenance {
                source_streams: vec![request_event.stream_id.clone()],
                source_event_ids: vec![request_event.id],
                discharged_by: Some(THREAD_SPAWN_PROJECTOR_DISCHARGED_BY.to_string()),
                function: Some(THREAD_SPAWN_PROJECTOR_FUNCTION.to_string()),
                ..verlet_history::EventProvenance::default()
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
        parent: crate::kernel::runtime_host::RuntimeThreadHandle,
        request_event: &verlet_history::EventRecord,
        payload: verlet_history::ThreadSpawnRequestedPayload,
        require_supervisor_grant: bool,
    ) -> crate::kernel::runtime_host::VerletResult<ThreadSpawnProjected> {
        if require_supervisor_grant && !parent_allows_supervisor_spawn(&parent.context().metadata)?
        {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                format!(
                    "{STD_SUPERVISOR_SPAWN_TEMPLATE_ID} projector requires parent thread bound coupling grant {THREADS_SPAWN_CAPABILITY}",
                    STD_SUPERVISOR_SPAWN_TEMPLATE_ID =
                        crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
                    THREADS_SPAWN_CAPABILITY =
                        crate::operations::kernel_packages::THREADS_SPAWN_CAPABILITY
                ),
            ));
        }

        let arguments = if let Some(task_name) = &payload.task_name {
            let mut arguments = serde_json::json!({
                "task_name": task_name,
                "message": payload.initial_submission,
            });
            if payload.child_agent_ref != UNBOUND_CHILD_AGENT_REF {
                arguments["agent_ref"] = serde_json::json!(payload.child_agent_ref);
            }
            arguments
        } else {
            serde_json::json!({
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
        let (placement, has_workspace) = agent_binding
            .as_ref()
            .map(|binding| {
                serde_json::from_value::<crate::agent::manifest_bind::AgentManifestBindReceipt>(
                    binding.bind_receipt.clone(),
                )
                .map(|receipt| {
                    (
                        receipt.placement.unwrap_or_default().target,
                        receipt.workspace.is_some(),
                    )
                })
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "thread spawn projector agent_ref bind receipt is invalid: {err}"
                    ))
                })
            })
            .transpose()?
            .unwrap_or((
                crate::kernel::control_decision::PlacementTarget::Local,
                false,
            ));
        if placement != crate::kernel::control_decision::PlacementTarget::Local && has_workspace {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "workspace bindings require local placement and cannot cross the remote child executor boundary"
                    .to_string(),
            ));
        }
        let task_name = payload
            .task_name
            .clone()
            .or_else(|| Some(payload.correlation_id.clone()));
        let witness = crate::kernel::runtime_host::kernel_control::ThreadSpawnWitness {
            parent_turn_id: payload.parent_turn_id.clone(),
            correlation_id: Some(payload.correlation_id.clone()),
            request_stream_id: Some(request_event.stream_id.clone()),
            request_event_id: Some(request_event.id),
            submitted_turn_id: Some(submitted_turn_id),
        };
        let receipt = match placement {
            crate::kernel::control_decision::PlacementTarget::Local => {
                if let Some(binding) = agent_binding {
                    self.host
                        .kernel_control()
                        .spawn_bound_child_with_witness(
                            parent.context(),
                            task_name,
                            crate::kernel::runtime_host::turn::TurnInput::text(
                                payload.initial_submission.clone(),
                            ),
                            metadata,
                            witness,
                            binding.compile_receipt,
                            binding.bind_receipt,
                        )
                        .await?
                } else {
                    self.host
                        .kernel_control()
                        .spawn_child_with_witness(
                            parent.context(),
                            task_name,
                            crate::kernel::runtime_host::turn::TurnInput::text(
                                payload.initial_submission.clone(),
                            ),
                            metadata,
                            witness,
                        )
                        .await?
                }
            }
            crate::kernel::control_decision::PlacementTarget::Remote => {
                let (compile_payload, bind_payload) = agent_binding
                    .map(|binding| (Some(binding.compile_receipt), Some(binding.bind_receipt)))
                    .unwrap_or_default();
                self.host
                    .kernel_control()
                    .spawn_remote_child_with_witness(
                        parent.context(),
                        task_name,
                        crate::kernel::runtime_host::turn::TurnInput::text(
                            payload.initial_submission.clone(),
                        ),
                        metadata,
                        witness,
                        compile_payload,
                        bind_payload,
                    )
                    .await?
            }
            crate::kernel::control_decision::PlacementTarget::Sandbox => {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "placement target sandbox requires the remote EventStore backend capability, which is not available"
                        .to_string(),
                ));
            }
        };
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
        request_event: &verlet_history::EventRecord,
        correlation_id: Option<&str>,
        decision_events: &[verlet_history::EventRecord],
        decision: verlet_history::NewEventRecord,
    ) -> crate::kernel::runtime_host::VerletResult<FencedDecisionAppend> {
        let mut events = decision_events.to_vec();
        loop {
            if spawn_request_already_projected(&events, request_event.id, correlation_id) {
                return Ok(FencedDecisionAppend::AlreadyProjected);
            }
            let expected_next_sequence = events
                .last()
                .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| verlet_history::EventSequence::new(1));
            let expected = verlet_history::EventRecord::from_new(
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
                                        event.id = verlet_history::EventRecordId::new();
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
                        return Err(crate::kernel::runtime_host::VerletError::History(format!(
                            "fenced thread spawn decision append returned {} record(s), expected 1",
                            appended.len()
                        )));
                    }
                    let appended = appended.into_iter().next().ok_or_else(|| {
                        crate::kernel::runtime_host::VerletError::History(
                            "fenced thread spawn decision append returned no record".to_string(),
                        )
                    })?;
                    if appended != expected {
                        return Err(crate::kernel::runtime_host::VerletError::History(format!(
                            "fenced thread spawn decision append returned unexpected record {} at {} sequence {}",
                            appended.id,
                            appended.stream_id,
                            appended.sequence.get()
                        )));
                    }
                    return Ok(FencedDecisionAppend::Appended(appended.id));
                }
                Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => {
                    events = self
                        .host
                        .runtime_store()
                        .read_events(&request_event.stream_id, None)
                        .await
                        .map_err(|err| {
                            crate::kernel::runtime_host::VerletError::History(err.to_string())
                        })?;
                }
                Err(err) => {
                    return Err(crate::kernel::runtime_host::VerletError::History(
                        err.to_string(),
                    ));
                }
            }
        }
    }

    async fn record_preclaim_failure(
        &self,
        receipt: &mut ThreadSpawnProjectionReceipt,
        failure_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        request_event: &verlet_history::EventRecord,
        correlation_id: Option<&str>,
        reason: String,
        decision_events: &[verlet_history::EventRecord],
    ) -> crate::kernel::runtime_host::VerletResult<()> {
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
        caller: &verlet_runtime_contracts::ThreadContext,
        arguments: &serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<(
        Option<crate::agent::agent_process::KernelThreadSpawnAgentBinding>,
        std::collections::BTreeMap<String, String>,
    )> {
        let inputs_hash = crate::agent::manifest_bind::canonical_json_hash(arguments)?;
        let agent_ref = arguments
            .get("agent_ref")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .trim();
        let agent_binding = if agent_ref.is_empty() || agent_ref == UNBOUND_CHILD_AGENT_REF {
            None
        } else {
            let resolver = self.agent_resolver.as_ref().ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "thread spawn projector agent_ref requires a manifest resolver".to_string(),
                )
            })?;
            Some(resolver.resolve_agent_ref(caller, agent_ref).await?)
        };
        let mut metadata = agent_binding
            .as_ref()
            .map(|binding| binding.metadata.clone())
            .unwrap_or_default();
        metadata.insert(
            crate::kernel::runtime_host::THREAD_SPAWN_INPUTS_HASH_METADATA.to_string(),
            inputs_hash,
        );
        if let Some(binding) = &agent_binding {
            let bind_receipt = serde_json::from_value::<
                crate::agent::manifest_bind::AgentManifestBindReceipt,
            >(binding.bind_receipt.clone())
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "thread spawn projector agent_ref bind receipt is invalid: {err}"
                ))
            })?;
            metadata
                .entry(crate::kernel::runtime_host::THREAD_AGENT_MANIFEST_HASH_METADATA.to_string())
                .or_insert_with(|| bind_receipt.manifest_hash.clone());
            let granted = serde_json::to_string(&bind_receipt.granted).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to encode thread spawn projector grants: {err}"
                ))
            })?;
            metadata.insert(
                crate::kernel::runtime_host::THREAD_SPAWN_GRANTED_METADATA.to_string(),
                granted,
            );
        }
        Ok((agent_binding, metadata))
    }

    async fn append_failure(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        request_event: &verlet_history::EventRecord,
        correlation_id: Option<&str>,
        reason: String,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
        let failure = self.failure_record(coordinates, request_event, correlation_id, &reason);
        let expected_failure = failure.clone();
        let mut appended = self
            .host
            .runtime_store()
            .append_events(&request_event.stream_id, vec![failure])
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        if appended.len() != 1 {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread spawn failure append returned {} record(s), expected 1",
                appended.len()
            )));
        }
        let appended = appended.pop().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(
                "thread spawn failure append returned no record".to_string(),
            )
        })?;
        let expected = verlet_history::EventRecord::from_new(
            request_event.stream_id.clone(),
            appended.sequence,
            expected_failure,
        );
        if appended != expected {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread spawn failure append returned unexpected record {} on {}",
                appended.id, appended.stream_id
            )));
        }
        Ok(appended)
    }

    fn failure_record(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        request_event: &verlet_history::EventRecord,
        correlation_id: Option<&str>,
        reason: &str,
    ) -> verlet_history::NewEventRecord {
        let payload = serde_json::json!({
            "schema": verlet_history::EventKind::LoopDenied.payload_schema_id(),
            "template_id": crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
            "request_event_id": request_event.id.to_string(),
            "correlation_id": correlation_id
                .map(ToString::to_string)
                .or_else(|| {
                    request_event
                        .payload
                        .get("correlation_id")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string)
                }),
            "status": "failed",
            "error_class": "thread_spawn_failed",
            "reason": reason,
        });
        verlet_history::NewEventRecord::discharged(
            coordinates.clone(),
            verlet_history::EventKind::LoopDenied,
            payload,
            verlet_history::EventProvenance {
                source_streams: vec![request_event.stream_id.clone()],
                source_event_ids: vec![request_event.id],
                discharged_by: Some(THREAD_SPAWN_PROJECTOR_DISCHARGED_BY.to_string()),
                function: Some(THREAD_SPAWN_PROJECTOR_FUNCTION.to_string()),
                ..verlet_history::EventProvenance::default()
            },
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ThreadSpawnProjectionReceipt {
    pub projected: Vec<ThreadSpawnProjected>,
    pub skipped: Vec<verlet_history::EventRecordId>,
    pub failed: Vec<ThreadSpawnProjectionFailure>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnProjected {
    pub request_event_id: verlet_history::EventRecordId,
    pub child_thread_id: verlet_runtime_contracts::ThreadId,
    pub submitted_turn_id: String,
    pub correlation_id: String,
    pub task_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnProjectionFailure {
    pub request_event_id: verlet_history::EventRecordId,
    pub failure_event_id: verlet_history::EventRecordId,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnDispatchReceipt {
    pub request_event_id: verlet_history::EventRecordId,
    pub handle: verlet_runtime_contracts::handle::HandleId,
    pub dispatch_id: verlet_runtime_contracts::handle::DispatchId,
    pub submitted_turn_id: Option<String>,
    pub task_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadSpawnDispatchFold {
    pub request_event_id: verlet_history::EventRecordId,
    pub handle: Option<verlet_runtime_contracts::handle::HandleId>,
    pub submitted_turn_id: Option<String>,
    pub task_name: Option<String>,
    pub failure_reason: Option<String>,
    pub claimed: bool,
}

/// Receipt returned whenever a parent-scoped `task_name` is resolved to its
/// durable child handle. The resolver folds the existing spawn request and
/// witnessed spawn decision; it does not consult a registry or an in-memory
/// alias map. Raw handle identity remains in this runtime receipt and is not
/// projected into model-visible tool output.
#[derive(Clone, Debug, PartialEq)]
pub struct ThreadTaskNameResolutionReceipt {
    pub task_name: String,
    pub parent_thread_id: verlet_runtime_contracts::ThreadId,
    pub request_event_id: verlet_history::EventRecordId,
    pub spawned_event_id: verlet_history::EventRecordId,
    pub dispatch_id: verlet_runtime_contracts::handle::DispatchId,
    pub handle: verlet_runtime_contracts::handle::HandleId,
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
    pub request_event_id: verlet_history::EventRecordId,
    pub spawned_event_id: verlet_history::EventRecordId,
    pub submitted_turn_id: String,
    pub consumer: verlet_runtime_contracts::ThreadCoordinates,
    pub dispatch_id: verlet_runtime_contracts::handle::DispatchId,
    pub handle: verlet_runtime_contracts::handle::HandleId,
}

/// Folds the durable records for one dispatch identity. The original
/// non-claim `thread.spawn.requested` record anchors the fold; spawned and
/// failure decisions join by its existing `correlation_id` wire value. Raw
/// pre-handle-lane request payloads decode because every newer field is
/// optional with a serde default.
pub fn fold_thread_spawn_dispatch(
    events: &[verlet_history::EventRecord],
    dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
) -> Option<ThreadSpawnDispatchFold> {
    let (request, payload) = events.iter().find_map(|event| {
        if event.kind != verlet_history::EventKind::ThreadSpawnRequested
            || is_spawn_request_claim(event)
        {
            return None;
        }
        let payload = serde_json::from_value::<verlet_history::ThreadSpawnRequestedPayload>(
            event.payload.clone(),
        )
        .ok()?;
        (payload.correlation_id == dispatch_id.as_str()).then_some((event, payload))
    })?;
    let spawned = events.iter().find(|event| {
        event.kind == verlet_history::EventKind::ThreadSpawned
            && event
                .payload
                .get("correlation_id")
                .and_then(serde_json::Value::as_str)
                == Some(dispatch_id.as_str())
    });
    let handle = spawned
        .and_then(|event| event.payload.get("child_thread_id"))
        .and_then(serde_json::Value::as_str)
        .and_then(|value| verlet_runtime_contracts::ThreadId::parse_str(value).ok())
        .map(verlet_runtime_contracts::handle::HandleId::thread);
    let failure_reason = events
        .iter()
        .find(|event| {
            event.kind == verlet_history::EventKind::LoopDenied
                && event
                    .payload
                    .get("correlation_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(dispatch_id.as_str())
        })
        .and_then(|event| event.payload.get("reason"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let claimed = events.iter().any(|event| {
        is_spawn_request_claim(event)
            && (event.provenance.source_event_ids.contains(&request.id)
                || event
                    .payload
                    .get("correlation_id")
                    .and_then(serde_json::Value::as_str)
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

/// Resolves one model-facing child `task_name` by folding the parent's durable
/// spawn request and witnessed spawn records. A task name is reserved for the
/// lifetime of its parent, including after the child reaches terminal state.
/// Multiple witnessed handles for one name are legacy/corrupt ambiguity and
/// fail closed rather than preferring a live child over a completed child.
pub fn fold_thread_task_name_resolution(
    events: &[verlet_history::EventRecord],
    parent: &verlet_runtime_contracts::ThreadCoordinates,
    task_name: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<ThreadTaskNameResolutionReceipt>> {
    let mut resolutions = Vec::new();
    for request in events.iter().filter(|event| {
        event.kind == verlet_history::EventKind::ThreadSpawnRequested
            && !is_spawn_request_claim(event)
    }) {
        if request.coordinates.thread_id != parent.thread_id
            || request.coordinates.scope() != parent.scope()
        {
            continue;
        }
        let payload = serde_json::from_value::<verlet_history::ThreadSpawnRequestedPayload>(
            request.payload.clone(),
        )
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "thread task_name resolution spawn request decode failed: {err}"
            ))
        })?;
        if payload.parent_thread_id != parent.thread_id
            || payload.task_name.as_deref() != Some(task_name)
        {
            continue;
        }
        for spawned in events.iter().filter(|event| {
            event.kind == verlet_history::EventKind::ThreadSpawned
                && event.coordinates.thread_id == parent.thread_id
                && event.coordinates.scope() == parent.scope()
                && event
                    .payload
                    .get("correlation_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(payload.correlation_id.as_str())
                && event.provenance.source_event_ids.contains(&request.id)
        }) {
            let spawned_payload = serde_json::from_value::<verlet_history::ThreadSpawnedPayload>(
                spawned.payload.clone(),
            )
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "thread task_name resolution spawned payload decode failed: {err}"
                ))
            })?;
            if spawned_payload.parent_thread_id != parent.thread_id {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
                    "thread task_name {task_name:?} spawned receipt has the wrong parent"
                )));
            }
            resolutions.push(ThreadTaskNameResolutionReceipt {
                task_name: task_name.to_string(),
                parent_thread_id: parent.thread_id,
                request_event_id: request.id,
                spawned_event_id: spawned.id,
                dispatch_id: verlet_runtime_contracts::handle::DispatchId::new(
                    payload.correlation_id.clone(),
                ),
                handle: verlet_runtime_contracts::handle::HandleId::thread(
                    spawned_payload.child_thread_id,
                ),
            });
        }
    }
    match resolutions.len() {
        0 => Ok(None),
        1 => Ok(resolutions.pop()),
        _ => Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!("thread task_name {task_name:?} is ambiguous under this parent"),
        )),
    }
}

/// Folds all valid thread-handle bindings witnessed on one consumer control
/// stream. Conflicting records fail closed so settlement cannot be routed to
/// an ambiguous consumer or handle.
pub(crate) fn fold_thread_handle_bindings(
    events: &[verlet_history::EventRecord],
) -> crate::kernel::runtime_host::VerletResult<Vec<ThreadHandleBinding>> {
    let mut bindings = std::collections::BTreeMap::<String, ThreadHandleBinding>::new();
    for spawned in events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawned)
    {
        let Some(correlation_id) = spawned
            .payload
            .get("correlation_id")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let spawned_payload =
            serde_json::from_value::<verlet_history::ThreadSpawnedPayload>(spawned.payload.clone())
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "thread handle binding spawned payload decode failed: {err}"
                    ))
                })?;
        if spawned_payload.parent_thread_id != spawned.coordinates.thread_id {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread handle binding parent {} does not match consumer stream {}",
                spawned_payload.parent_thread_id, spawned.coordinates.thread_id
            )));
        }
        let request = events
            .iter()
            .filter(|event| {
                event.kind == verlet_history::EventKind::ThreadSpawnRequested
                    && !is_spawn_request_claim(event)
            })
            .find_map(|event| {
                let payload =
                    serde_json::from_value::<verlet_history::ThreadSpawnRequestedPayload>(
                        event.payload.clone(),
                    )
                    .ok()?;
                (payload.correlation_id == correlation_id).then_some((event, payload))
            })
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "thread handle binding {correlation_id} is missing its spawn request"
                ))
            })?;
        if request.1.parent_thread_id != spawned.coordinates.thread_id {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread handle binding request parent {} does not match consumer stream {}",
                request.1.parent_thread_id, spawned.coordinates.thread_id
            )));
        }
        if !spawned.provenance.source_event_ids.contains(&request.0.id) {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread handle binding {correlation_id} spawned receipt is missing request provenance"
            )));
        }
        let binding = ThreadHandleBinding {
            request_event_id: request.0.id,
            spawned_event_id: spawned.id,
            submitted_turn_id: request
                .1
                .submitted_turn_id
                .clone()
                .unwrap_or_else(|| format!("thread-spawn-{}", request.0.id)),
            consumer: spawned.coordinates.clone(),
            dispatch_id: verlet_runtime_contracts::handle::DispatchId::new(correlation_id),
            handle: verlet_runtime_contracts::handle::HandleId::thread(
                spawned_payload.child_thread_id,
            ),
        };
        if let Some(existing) = bindings.insert(correlation_id.to_string(), binding.clone())
            && existing != binding
        {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread handle binding {correlation_id} has conflicting spawned receipts"
            )));
        }
    }
    Ok(bindings.into_values().collect())
}

fn parent_allows_supervisor_spawn(
    metadata: &std::collections::BTreeMap<String, String>,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    let Some(raw) = metadata.get(crate::kernel::runtime_host::THREAD_BOUND_COUPLING_SET_METADATA)
    else {
        return Ok(false);
    };
    let coupling_set = serde_json::from_str::<crate::agent::manifest_bind::BoundCouplingSet>(raw)
        .map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "thread bound coupling set is invalid: {err}"
        ))
    })?;
    let allows_spawn = coupling_set.couplings.iter().any(|coupling| {
        coupling.id == crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID
            && coupling
                .grants
                .iter()
                .any(|grant| grant == crate::operations::kernel_packages::THREADS_SPAWN_CAPABILITY)
    });
    if !allows_spawn {
        return Ok(false);
    }
    Ok(true)
}

fn spawn_request_already_projected(
    events: &[verlet_history::EventRecord],
    request_event_id: verlet_history::EventRecordId,
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
                        .and_then(serde_json::Value::as_str)
                        == Some(correlation_id)
                })))
            || (matches!(
                event.kind,
                verlet_history::EventKind::ThreadSpawned | verlet_history::EventKind::LoopDenied
            ) && (event
                .provenance
                .source_event_ids
                .contains(&request_event_id)
                || correlation_id.is_some_and(|correlation_id| {
                    event
                        .payload
                        .get("correlation_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(correlation_id)
                })))
    })
}

pub(crate) fn is_spawn_request_claim(event: &verlet_history::EventRecord) -> bool {
    event.kind == verlet_history::EventKind::ThreadSpawnRequested
        && event.provenance.discharged_by.as_deref() == Some(THREAD_SPAWN_PROJECTOR_DISCHARGED_BY)
        && event.provenance.function.as_deref() == Some(THREAD_SPAWN_PROJECTOR_FUNCTION)
}

fn spawn_request_belongs_to_parent(
    event: &verlet_history::EventRecord,
    payload: &verlet_history::ThreadSpawnRequestedPayload,
    parent: &verlet_runtime_contracts::ThreadCoordinates,
) -> bool {
    event.coordinates.thread_id == parent.thread_id
        && event.coordinates.scope() == parent.scope()
        && payload.parent_thread_id == parent.thread_id
}

#[cfg(test)]
mod tests {
    use verlet_history::EventStore as _;

    #[tokio::test]
    async fn thread_spawn_projector_spawns_child_and_witnesses_thread_spawned() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let root = host
            .start_thread_with_topology_and_metadata(
                coordinates.clone(),
                verlet_runtime_contracts::ThreadTopology::root(),
                parent_metadata_with_spawn_grant(),
            )
            .await
            .unwrap();
        let request = append_spawn_requested(&store, &coordinates, "projector-spawn-1").await;

        let receipt =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
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
            .find(|event| crate::kernel::thread_spawn_projector::is_spawn_request_claim(event))
            .unwrap();
        assert_eq!(claim.provenance.source_event_ids, vec![request.id]);
        let spawned = control_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::ThreadSpawned)
            .unwrap();
        assert!(claim.sequence.get() < spawned.sequence.get());
        let payload: verlet_history::ThreadSpawnedPayload =
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

    struct ForgedRemoteWorkspaceResolver;

    #[async_trait::async_trait]
    impl crate::agent::agent_process::KernelThreadSpawnAgentResolver for ForgedRemoteWorkspaceResolver {
        async fn resolve_agent_ref(
            &self,
            _caller: &verlet_runtime_contracts::ThreadContext,
            agent_ref: &str,
        ) -> crate::kernel::runtime_host::VerletResult<
            crate::agent::agent_process::KernelThreadSpawnAgentBinding,
        > {
            Ok(crate::agent::agent_process::KernelThreadSpawnAgentBinding {
                metadata: std::collections::BTreeMap::new(),
                compile_receipt: serde_json::json!({
                    "ref_uri": agent_ref,
                    "manifest_hash": "sha256:forged-remote-workspace",
                    "source_hash": "sha256:source"
                }),
                bind_receipt: serde_json::to_value(crate::agent::manifest_bind::AgentManifestBindReceipt {
                    ref_uri: agent_ref.to_string(),
                    manifest_hash: "sha256:forged-remote-workspace".to_string(),
                    model_profile_id: "default".to_string(),
                    model_profile_origin: None,
                    provider_id: "test".to_string(),
                    model_id: "model".to_string(),
                    tool_ids: Vec::new(),
                    operation_bindings: Vec::new(),
                    skill_packages: Vec::new(),
                    skill_discovery: None,
                    static_context_segments: Vec::new(),
                    tool_universes: Vec::new(),
                    couplings: Vec::new(),
                    granted: Vec::new(),
                    grant_bindings: Vec::new(),
                    effective_runtime: verlet_agent::manifest_schema::AgentManifestRuntimeDefaults::default(),
                    overridden_keys: Vec::new(),
                    placement: Some(crate::agent::manifest_bind::AgentManifestPlacementBinding {
                        target: crate::kernel::control_decision::PlacementTarget::Remote,
                        executor_ref: Some("executor://cluster/default".to_string()),
                        config: std::collections::BTreeMap::new(),
                    }),
                    placement_origin: None,
                    workspace: Some(crate::agent::manifest_bind::AgentManifestResolvedWorkspaceMount {
                        guest_path: std::path::PathBuf::from("/work"),
                        host_path: std::path::PathBuf::from("/tmp/forged-remote-workspace"),
                        mode: verlet_agent::manifest_schema::AgentManifestWorkspaceMode::ReadWrite,
                    }),
                    workspace_origin: None,
                })
                .unwrap(),
            })
        }
    }

    #[tokio::test]
    async fn thread_spawn_projector_rejects_forged_remote_workspace_binding() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "remote-workspace");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let payload = serde_json::to_value(verlet_history::ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            task_name: None,
            submitted_turn_id: None,
            child_agent_ref: "agent://forged-remote-workspace@0.1.0".to_string(),
            initial_submission: "must not start".to_string(),
            correlation_id: "remote-workspace-dispatch".to_string(),
            block_parent: true,
        })
        .unwrap();
        let mut payload = payload;
        payload["schema"] =
            serde_json::json!(verlet_history::EventKind::ThreadSpawnRequested.payload_schema_id());
        store
            .append_events(
                &control_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ThreadSpawnRequested,
                    payload,
                    verlet_history::EventProvenance {
                        source_streams: vec![verlet_history::EventStreamId::for_thread(
                            &coordinates,
                        )],
                        source_event_ids: vec![verlet_history::EventRecordId::new()],
                        discharged_by: Some("coupling:std::supervisor.spawn".to_string()),
                        function: Some("op://std-supervisor-spawn/run".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();

        let receipt =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
                .with_agent_resolver(std::sync::Arc::new(ForgedRemoteWorkspaceResolver))
                .project_control_stream(&coordinates)
                .await
                .unwrap();

        assert_eq!(receipt.failed.len(), 1);
        assert!(receipt.failed[0].reason.contains("require local placement"));
        assert!(host.children_of(coordinates.thread_id).await.is_empty());
        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_thread_spawn_projectors_produce_exactly_one_spawn() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        append_spawn_requested(&store, &coordinates, "projector-race-1").await;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(std::sync::Arc::clone(&barrier));
        let second = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(barrier);

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
                    &verlet_history::EventStreamId::new(format!(
                        "control:{}",
                        coordinates.thread_id
                    )),
                    None,
                )
                .await
                .unwrap()
                .into_iter()
                .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawned)
                .count(),
            1
        );
        assert_eq!(
            store
                .read_events(
                    &verlet_history::EventStreamId::new(format!(
                        "control:{}",
                        coordinates.thread_id
                    )),
                    None,
                )
                .await
                .unwrap()
                .into_iter()
                .filter(crate::kernel::thread_spawn_projector::is_spawn_request_claim)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn unrelated_control_append_does_not_strand_spawn_request() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        append_spawn_requested(&store, &coordinates, "projector-unrelated-append").await;
        let pause =
            std::sync::Arc::new(crate::kernel::thread_spawn_projector::ProjectionPause::default());
        let projector =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
                .with_snapshot_pause(pause.clone());
        let projected_coordinates = coordinates.clone();
        let projection = tokio::spawn(async move {
            projector
                .project_control_stream(&projected_coordinates)
                .await
        });

        pause.wait_until_paused().await;
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        store
            .append_events(
                &control_stream,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnSubmitted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::TurnSubmitted.payload_schema_id(),
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
                .filter(crate::kernel::thread_spawn_projector::is_spawn_request_claim)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_requests_with_same_correlation_produce_one_child() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        append_spawn_requested(&store, &coordinates, "shared-correlation").await;
        append_spawn_requested(&store, &coordinates, "shared-correlation").await;
        let pause =
            std::sync::Arc::new(crate::kernel::thread_spawn_projector::ProjectionPause::default());
        let first = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_after_claim_pause(pause.clone());
        let first_coordinates = coordinates.clone();
        let first_projection =
            tokio::spawn(async move { first.project_control_stream(&first_coordinates).await });

        pause.wait_until_paused().await;
        let second_receipt =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
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
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let control_events = store.read_events(&control_stream, None).await.unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(
                    |event| crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                )
                .count(),
            1
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawned)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_dispatches_with_same_id_append_one_request_and_return_one_handle() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(std::sync::Arc::clone(&barrier));
        let second = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(barrier);
        let request = verlet_history::ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            task_name: Some("worker".to_string()),
            submitted_turn_id: Some("thread-spawn-dispatch-race-1".to_string()),
            child_agent_ref: crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                .to_string(),
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
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(|event| {
                    event.kind == verlet_history::EventKind::ThreadSpawnRequested
                        && !crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                })
                .count(),
            1
        );
        assert_eq!(
            control_events
                .iter()
                .filter(
                    |event| crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                )
                .count(),
            1
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawned)
                .count(),
            1
        );
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 1);

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_task_name_folds_same_dispatch_and_rejects_a_new_dispatch() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let request = |dispatch_id: &str| verlet_history::ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            task_name: Some("worker".to_string()),
            submitted_turn_id: Some(format!("thread-spawn-{dispatch_id}")),
            child_agent_ref: crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                .to_string(),
            initial_submission: "echo worker".to_string(),
            correlation_id: dispatch_id.to_string(),
            block_parent: false,
        };
        let projector =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone());

        let first = projector
            .dispatch_request(&coordinates, request("dispatch-1"))
            .await
            .unwrap();
        let retry = projector
            .dispatch_request(&coordinates, request("dispatch-1"))
            .await
            .unwrap();
        let err = projector
            .dispatch_request(&coordinates, request("dispatch-2"))
            .await
            .unwrap_err();

        assert_eq!(first, retry);
        assert_eq!(
            err.to_string(),
            "runtime execution failed: thread_spawn task_name \"worker\" is already bound under this parent; retry with the original dispatch or choose a new task_name"
        );
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 1);
        let events = store
            .read_events(
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawnRequested)
                .filter(
                    |event| !crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                )
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_new_dispatches_for_one_task_name_spawn_exactly_one_child() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(barrier.clone());
        let second = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(barrier);
        let request = |dispatch_id: &str| verlet_history::ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: None,
            task_name: Some("worker".to_string()),
            submitted_turn_id: Some(format!("thread-spawn-{dispatch_id}")),
            child_agent_ref: crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                .to_string(),
            initial_submission: "echo worker".to_string(),
            correlation_id: dispatch_id.to_string(),
            block_parent: false,
        };

        let (first, second) = tokio::join!(
            first.dispatch_request(&coordinates, request("dispatch-1")),
            second.dispatch_request(&coordinates, request("dispatch-2")),
        );

        assert_eq!(
            [first.is_ok(), second.is_ok()]
                .into_iter()
                .filter(|ok| *ok)
                .count(),
            1
        );
        let error = [first, second].into_iter().find_map(Result::err).unwrap();
        assert!(
            error
                .to_string()
                .contains("thread_spawn task_name \"worker\" is already bound under this parent")
        );
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 1);
        let events = store
            .read_events(
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawnRequested)
                .filter(
                    |event| !crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                )
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn task_name_reservations_are_scoped_to_the_parent_control_stream() {
        let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
            crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
        ));
        let first_parent =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let second_parent =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        for coordinates in [&first_parent, &second_parent] {
            host.start_thread_with_topology_and_metadata(
                coordinates.clone(),
                verlet_runtime_contracts::ThreadTopology::root(),
                parent_metadata_with_spawn_grant(),
            )
            .await
            .unwrap();
        }
        let request = |coordinates: &verlet_runtime_contracts::ThreadCoordinates,
                       dispatch_id: &str| {
            verlet_history::ThreadSpawnRequestedPayload {
                parent_thread_id: coordinates.thread_id,
                parent_turn_id: None,
                task_name: Some("worker".to_string()),
                submitted_turn_id: Some(format!("thread-spawn-{dispatch_id}")),
                child_agent_ref: crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                    .to_string(),
                initial_submission: "echo worker".to_string(),
                correlation_id: dispatch_id.to_string(),
                block_parent: false,
            }
        };

        let first = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .dispatch_request(&first_parent, request(&first_parent, "dispatch-1"))
            .await
            .unwrap();
        let second = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .dispatch_request(&second_parent, request(&second_parent, "dispatch-2"))
            .await
            .unwrap();

        assert_ne!(first.handle, second.handle);
        assert_eq!(host.children_of(first_parent.thread_id).await.len(), 1);
        assert_eq!(host.children_of(second_parent.thread_id).await.len(), 1);
        host.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn foreign_spawn_request_cannot_fold_or_reserve_a_parent_task_name() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let foreign =
            verlet_runtime_contracts::ThreadCoordinates::new("other-tenant", "user", "session");
        let request = |parent: &verlet_runtime_contracts::ThreadCoordinates, dispatch_id: &str| {
            verlet_history::ThreadSpawnRequestedPayload {
                parent_thread_id: parent.thread_id,
                parent_turn_id: None,
                task_name: Some("worker".to_string()),
                submitted_turn_id: Some(format!("thread-spawn-{dispatch_id}")),
                child_agent_ref: crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                    .to_string(),
                initial_submission: "echo worker".to_string(),
                correlation_id: dispatch_id.to_string(),
                block_parent: false,
            }
        };
        store
            .append_events(
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                vec![verlet_history::NewEventRecord::witnessed(
                    foreign.clone(),
                    verlet_history::EventKind::ThreadSpawnRequested,
                    serde_json::to_value(request(&foreign, "dispatch-foreign")).unwrap(),
                )],
            )
            .await
            .unwrap();
        let projector =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone());

        let fold_err = projector
            .dispatch_request(&coordinates, request(&coordinates, "dispatch-foreign"))
            .await
            .unwrap_err();
        assert!(
            matches!(fold_err, crate::kernel::runtime_host::VerletError::History(message) if message == "thread spawn dispatch matched an out-of-scope request record")
        );
        let receipt = projector
            .dispatch_request(&coordinates, request(&coordinates, "dispatch-local"))
            .await
            .unwrap();

        assert_eq!(receipt.task_name.as_deref(), Some("worker"));
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 1);
        host.shutdown_all().await.unwrap();
    }

    #[test]
    fn task_name_resolution_receipt_fails_closed_between_completed_and_live_legacy_handles() {
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let stream_id =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let request = |sequence: i64, dispatch_id: &str| {
            verlet_history::EventRecord::from_new(
                stream_id.clone(),
                verlet_history::EventSequence::new(sequence),
                verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::ThreadSpawnRequested,
                    serde_json::to_value(verlet_history::ThreadSpawnRequestedPayload {
                        parent_thread_id: coordinates.thread_id,
                        parent_turn_id: None,
                        task_name: Some("worker".to_string()),
                        submitted_turn_id: None,
                        child_agent_ref:
                            crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                                .to_string(),
                        initial_submission: "work".to_string(),
                        correlation_id: dispatch_id.to_string(),
                        block_parent: false,
                    })
                    .unwrap(),
                ),
            )
        };
        let first_request = request(1, "dispatch-1");
        let first_child = verlet_runtime_contracts::ThreadId::new();
        let mut first_spawn = verlet_history::NewEventRecord::witnessed(
            coordinates.clone(),
            verlet_history::EventKind::ThreadSpawned,
            serde_json::json!({
                "parent_thread_id": coordinates.thread_id,
                "child_thread_id": first_child,
                "child_manifest_hash": "unbound",
                "granted": [],
                "inputs_hash": "sha256:first",
                "correlation_id": "dispatch-1"
            }),
        );
        first_spawn.provenance.source_event_ids = vec![first_request.id];
        first_spawn.provenance.source_streams = vec![stream_id.clone()];
        let first_spawn = verlet_history::EventRecord::from_new(
            stream_id.clone(),
            verlet_history::EventSequence::new(2),
            first_spawn,
        );

        let receipt = crate::kernel::thread_spawn_projector::fold_thread_task_name_resolution(
            &[first_request.clone(), first_spawn.clone()],
            &coordinates,
            "worker",
        )
        .unwrap()
        .unwrap();
        assert_eq!(receipt.task_name, "worker");
        assert_eq!(receipt.request_event_id, first_request.id);
        assert_eq!(receipt.spawned_event_id, first_spawn.id);
        assert_eq!(
            receipt.handle,
            verlet_runtime_contracts::handle::HandleId::thread(first_child)
        );
        assert_eq!(
            receipt.dispatch_id,
            verlet_runtime_contracts::handle::DispatchId::new("dispatch-1")
        );

        let conflicting_child = verlet_runtime_contracts::ThreadId::new();
        let mut conflicting_spawn = verlet_history::NewEventRecord::witnessed(
            coordinates.clone(),
            verlet_history::EventKind::ThreadSpawned,
            serde_json::json!({
                "parent_thread_id": coordinates.thread_id,
                "child_thread_id": conflicting_child,
                "child_manifest_hash": "unbound",
                "granted": [],
                "inputs_hash": "sha256:conflicting",
                "correlation_id": "dispatch-1"
            }),
        );
        conflicting_spawn.provenance.source_event_ids = vec![first_request.id];
        conflicting_spawn.provenance.source_streams = vec![stream_id.clone()];
        let conflicting_spawn = verlet_history::EventRecord::from_new(
            stream_id.clone(),
            verlet_history::EventSequence::new(3),
            conflicting_spawn,
        );
        let err = crate::kernel::thread_spawn_projector::fold_thread_task_name_resolution(
            &[
                first_request.clone(),
                first_spawn.clone(),
                conflicting_spawn,
            ],
            &coordinates,
            "worker",
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "runtime execution failed: thread task_name \"worker\" is ambiguous under this parent"
        );

        let completed = verlet_history::EventRecord::from_new(
            stream_id.clone(),
            verlet_history::EventSequence::new(4),
            verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::ThreadJoined,
                serde_json::json!({
                    "child_thread_id": first_child,
                    "spawned_event_id": first_spawn.id,
                    "terminal_state": "completed"
                }),
            ),
        );
        let second_request = request(5, "dispatch-2");
        let second_child = verlet_runtime_contracts::ThreadId::new();
        let mut second_spawn = verlet_history::NewEventRecord::witnessed(
            coordinates.clone(),
            verlet_history::EventKind::ThreadSpawned,
            serde_json::json!({
                "parent_thread_id": coordinates.thread_id,
                "child_thread_id": second_child,
                "child_manifest_hash": "unbound",
                "granted": [],
                "inputs_hash": "sha256:second",
                "correlation_id": "dispatch-2"
            }),
        );
        second_spawn.provenance.source_event_ids = vec![second_request.id];
        second_spawn.provenance.source_streams = vec![stream_id.clone()];
        let second_spawn = verlet_history::EventRecord::from_new(
            stream_id,
            verlet_history::EventSequence::new(6),
            second_spawn,
        );
        let err = crate::kernel::thread_spawn_projector::fold_thread_task_name_resolution(
            &[
                first_request,
                first_spawn,
                completed,
                second_request,
                second_spawn,
            ],
            &coordinates,
            "worker",
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "runtime execution failed: thread task_name \"worker\" is ambiguous under this parent"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_dispatches_with_different_ids_append_two_requests_and_spawn_two_children() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(std::sync::Arc::clone(&barrier));
        let second = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(barrier);
        let request =
            |dispatch_id: &str, task_name: &str| verlet_history::ThreadSpawnRequestedPayload {
                parent_thread_id: coordinates.thread_id,
                parent_turn_id: Some("parent-turn-1".to_string()),
                task_name: Some(task_name.to_string()),
                submitted_turn_id: Some(format!("thread-spawn-{dispatch_id}")),
                child_agent_ref: crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                    .to_string(),
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
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(|event| {
                    event.kind == verlet_history::EventKind::ThreadSpawnRequested
                        && !crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                })
                .count(),
            2
        );
        assert_eq!(
            control_events
                .iter()
                .filter(
                    |event| crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                )
                .count(),
            2
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == verlet_history::EventKind::ThreadSpawned)
                .count(),
            2
        );
        assert_eq!(host.children_of(coordinates.thread_id).await.len(), 2);

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retry_after_dispatch_claim_without_decision_fails_closed_instead_of_spinning() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let request = verlet_history::ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            task_name: Some("worker".to_string()),
            submitted_turn_id: Some("thread-spawn-dispatch-dead-claim-1".to_string()),
            child_agent_ref: crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                .to_string(),
            initial_submission: "echo never started".to_string(),
            correlation_id: "dispatch-dead-claim-1".to_string(),
            block_parent: false,
        };
        let pause =
            std::sync::Arc::new(crate::kernel::thread_spawn_projector::ProjectionPause::default());
        let projector =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
                .with_after_claim_pause(pause.clone());
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

        let err = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
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
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(
                    |event| crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                )
                .count(),
            1
        );
        assert!(
            control_events
                .iter()
                .all(|event| event.kind != verlet_history::EventKind::ThreadSpawned)
        );

        host.shutdown_all().await.unwrap();
    }

    #[test]
    fn legacy_spawn_request_decodes_and_folds_to_original_handle() {
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let stream_id =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let request_id = verlet_history::EventRecordId::new();
        let mut legacy_request = verlet_history::NewEventRecord::witnessed(
            coordinates.clone(),
            verlet_history::EventKind::ThreadSpawnRequested,
            serde_json::from_str(
                r#"{"schema":"cooldis.thread.spawn.requested/1","parent_thread_id":"018f0000-0000-7000-8000-000000000001","parent_turn_id":"parent-turn-1","child_agent_ref":"unbound","initial_submission":"legacy child","correlation_id":"legacy-dispatch-1","block_parent":false}"#,
            )
            .unwrap(),
        );
        legacy_request.id = request_id;
        let request = verlet_history::EventRecord::from_new(
            stream_id.clone(),
            verlet_history::EventSequence::new(1),
            legacy_request,
        );
        let child_thread_id = verlet_runtime_contracts::ThreadId::new();
        let spawned = verlet_history::EventRecord::from_new(
            stream_id,
            verlet_history::EventSequence::new(2),
            verlet_history::NewEventRecord::witnessed(
                coordinates,
                verlet_history::EventKind::ThreadSpawned,
                serde_json::json!({
                    "schema": verlet_history::EventKind::ThreadSpawned.payload_schema_id(),
                    "parent_thread_id": "018f0000-0000-7000-8000-000000000001",
                    "child_thread_id": child_thread_id,
                    "child_manifest_hash": "unbound",
                    "granted": [],
                    "inputs_hash": "sha256:legacy",
                    "correlation_id": "legacy-dispatch-1"
                }),
            ),
        );

        let folded = crate::kernel::thread_spawn_projector::fold_thread_spawn_dispatch(
            &[request, spawned],
            &verlet_runtime_contracts::handle::DispatchId::new("legacy-dispatch-1"),
        )
        .unwrap();
        assert_eq!(folded.request_event_id, request_id);
        assert_eq!(
            folded.handle,
            Some(verlet_runtime_contracts::handle::HandleId::thread(
                child_thread_id
            ))
        );
        assert_eq!(
            folded.submitted_turn_id.as_deref(),
            Some(format!("thread-spawn-{request_id}").as_str())
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn racing_rejections_emit_one_failure() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
        append_spawn_requested(&store, &coordinates, "projector-rejected-race").await;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(barrier.clone());
        let second = crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
            .with_snapshot_barrier(barrier);

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
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            control_events
                .iter()
                .filter(
                    |event| crate::kernel::thread_spawn_projector::is_spawn_request_claim(event)
                )
                .count(),
            1
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == verlet_history::EventKind::LoopDenied)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn forged_projection_coordinates_cannot_spawn_in_another_scope() {
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let mut forged = coordinates.clone();
        forged.tenant_id = "other-tenant".to_string();
        append_spawn_requested(&store, &forged, "projector-forged-scope").await;

        let receipt =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
                .project_control_stream(&forged)
                .await
                .unwrap();

        assert_eq!(receipt.failed.len(), 1);
        assert!(receipt.projected.is_empty());
        assert!(host.children_of(coordinates.thread_id).await.is_empty());
        let control_events = store
            .read_events(
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        assert!(
            !control_events
                .iter()
                .any(crate::kernel::thread_spawn_projector::is_spawn_request_claim)
        );
        assert_eq!(
            control_events
                .iter()
                .filter(|event| event.kind == verlet_history::EventKind::LoopDenied)
                .count(),
            1
        );

        host.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_fenced_append_receipt_prevents_spawn() {
        for receipt_override in [
            crate::kernel::thread_spawn_projector::FencedAppendReceiptOverride::Empty,
            crate::kernel::thread_spawn_projector::FencedAppendReceiptOverride::WrongEventId,
            crate::kernel::thread_spawn_projector::FencedAppendReceiptOverride::WrongProvenance,
        ] {
            let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
            let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
                std::sync::Arc::new(
                    crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
                ),
                store.clone(),
            );
            let coordinates =
                verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
            host.start_thread_with_topology_and_metadata(
                coordinates.clone(),
                verlet_runtime_contracts::ThreadTopology::root(),
                parent_metadata_with_spawn_grant(),
            )
            .await
            .unwrap();
            append_spawn_requested(&store, &coordinates, "projector-invalid-claim-receipt").await;

            let result =
                crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
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
        let store = std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let host = crate::kernel::runtime_host::RuntimeHost::with_session_store(
            std::sync::Arc::new(
                crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
            ),
            store.clone(),
        );
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        host.start_thread_with_topology_and_metadata(
            coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            parent_metadata_with_spawn_grant(),
        )
        .await
        .unwrap();
        let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
        let submitted = store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnSubmitted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::TurnSubmitted.payload_schema_id(),
                        "turn_id": "parent-turn-1",
                    }),
                )],
            )
            .await
            .unwrap();
        let executor = crate::kernel::stdlib_couplings::StdlibCouplingExecutor;
        let scheduler =
            crate::kernel::coupling_scheduler::CouplingScheduler::new(store.as_ref(), &executor);
        let spawn_receipt = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_supervisor_spawn_coupling(serde_json::json!({
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
        assert_eq!(
            spawn_receipt.runs[0].status,
            crate::kernel::coupling_scheduler::CouplingRunStatus::Completed
        );

        let projection =
            crate::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host.clone())
                .project_control_stream(&coordinates)
                .await
                .unwrap();
        let child_thread_id = projection.projected[0].child_thread_id;
        let completed =
            append_routed_child_completion(&store, &coordinates, child_thread_id, "child-turn-1")
                .await;
        let completion = scheduler
            .run_batch(
                &crate::agent::manifest_bind::BoundCouplingSet::new(
                    "snapshot-a",
                    vec![std_supervisor_child_completion_coupling(
                        serde_json::json!({
                            "watch_coupling_id": crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
                            "on_completed": "request_continuation",
                            "loop_id": "supervisor-release",
                            "parent_turn_id": "parent-turn-1",
                            "next_turn_input": "incorporate child release evidence",
                            "reason": "child completion should resume the supervisor",
                        }),
                    )],
                ),
                completed,
            )
            .await
            .unwrap();

        assert_eq!(completion.runs.len(), 1);
        assert_eq!(
            completion.runs[0].status,
            crate::kernel::coupling_scheduler::CouplingRunStatus::Completed
        );
        let control_events = store
            .read_events(
                &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                None,
            )
            .await
            .unwrap();
        let continued = control_events
            .iter()
            .find(|event| event.kind == verlet_history::EventKind::TurnContinueRequested)
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
        store: &verlet_history::InMemorySessionStore,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        correlation_id: &str,
    ) -> verlet_history::EventRecord {
        let control_stream =
            verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id));
        let mut payload = serde_json::to_value(verlet_history::ThreadSpawnRequestedPayload {
            parent_thread_id: coordinates.thread_id,
            parent_turn_id: Some("parent-turn-1".to_string()),
            task_name: None,
            submitted_turn_id: None,
            child_agent_ref: crate::kernel::thread_spawn_projector::UNBOUND_CHILD_AGENT_REF
                .to_string(),
            initial_submission: "echo projected child".to_string(),
            correlation_id: correlation_id.to_string(),
            block_parent: true,
        })
        .unwrap();
        payload["schema"] =
            serde_json::json!(verlet_history::EventKind::ThreadSpawnRequested.payload_schema_id());
        store
            .append_events(
                &control_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::ThreadSpawnRequested,
                    payload,
                    verlet_history::EventProvenance {
                        source_streams: vec![verlet_history::EventStreamId::for_thread(
                            coordinates,
                        )],
                        source_event_ids: vec![verlet_history::EventRecordId::new()],
                        discharged_by: Some("coupling:std::supervisor.spawn".to_string()),
                        function: Some("op://std-supervisor-spawn/run".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
    }

    async fn append_routed_child_completion(
        store: &verlet_history::InMemorySessionStore,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        child_thread_id: verlet_runtime_contracts::ThreadId,
        child_turn_id: &str,
    ) -> Vec<verlet_history::EventRecord> {
        let thread_stream = verlet_history::EventStreamId::for_thread(coordinates);
        store
            .append_events(
                &thread_stream,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnCompleted,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::TurnCompleted.payload_schema_id(),
                        "turn_id": child_turn_id,
                        "parent_thread_id": coordinates.thread_id.to_string(),
                        "child_thread_id": child_thread_id.to_string(),
                        "status": "completed",
                        "output_text": "child finished release evidence collection",
                    }),
                    verlet_history::EventProvenance {
                        source_streams: vec![verlet_history::EventStreamId::new(format!(
                            "thread:{}",
                            child_thread_id
                        ))],
                        discharged_by: Some("runtime:child-thread".to_string()),
                        function: Some("child_turn_completion/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
    }

    fn parent_metadata_with_spawn_grant() -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::from([(
            crate::kernel::runtime_host::THREAD_BOUND_COUPLING_SET_METADATA.to_string(),
            serde_json::to_string(&crate::agent::manifest_bind::BoundCouplingSet::new(
                "snapshot-a",
                vec![std_supervisor_spawn_coupling(serde_json::json!({
                    "initial_submission": "echo projected child",
                }))],
            ))
            .unwrap(),
        )])
    }

    fn std_supervisor_spawn_coupling(
        config: serde_json::Value,
    ) -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: crate::kernel::stdlib_couplings::STD_SUPERVISOR_SPAWN_TEMPLATE_ID.to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::TurnSubmitted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::TurnSubmitted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
                stream: "control".to_string(),
                kinds: vec![
                    verlet_history::EventKind::ThreadSpawnRequested,
                    verlet_history::EventKind::TurnWaiting,
                ],
            },
            function_ref: format!("op://std-supervisor-spawn/run@sha256:{}", "i".repeat(64)),
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-supervisor-spawn".to_string(),
                artifact_hash: "i".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
                crate::operations::kernel_packages::THREADS_SPAWN_CAPABILITY.to_string(),
            ],
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(2),
                max_ms: None,
            },
            config,
            config_hash: "sha256:supervisor-spawn".to_string(),
        }
    }

    fn std_supervisor_child_completion_coupling(
        config: serde_json::Value,
    ) -> crate::agent::manifest_bind::BoundCoupling {
        crate::agent::manifest_bind::BoundCoupling {
            id: crate::kernel::stdlib_couplings::STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID
                .to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::TurnCompleted,
            trigger_match: Default::default(),
            trigger_quota: verlet_agent::manifest_schema::AgentManifestCouplingQuota::default(),
            source_selectors: vec![crate::agent::manifest_bind::BoundCouplingSelector {
                stream: "thread".to_string(),
                kinds: vec![verlet_history::EventKind::TurnCompleted],
                scope: None,
                since: None,
            }],
            sink: crate::agent::manifest_bind::BoundCouplingSink {
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
            function: crate::agent::manifest_bind::BoundCouplingFunction {
                name: "std-supervisor-child-completion".to_string(),
                artifact_hash: "j".repeat(64),
                operation_name: Some("run".to_string()),
            },
            grants: vec![
                "stream.read:thread".to_string(),
                "stream.write:control".to_string(),
            ],
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget {
                max_discharge_events: Some(1),
                max_ms: None,
            },
            config,
            config_hash: "sha256:supervisor-child-completion".to_string(),
        }
    }
}
