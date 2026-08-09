//! Durable process-handle dispatch and terminal settlement.
//!
//! This is the common kernel-side chokepoint for every handle-returning
//! process surface. A dispatch is witnessed as observe-only ADR 0003 ingress
//! before the backend starts. Retrying the same dispatch folds that witness
//! and returns its original live handle; if the witness survives but the
//! owning manager entry does not, the claim is dead and the retry fails
//! closed rather than executing again.
//!
//! Terminal observation is push-first. The owning surface polls its own
//! manager snapshot and retries the outcome ingress until the durable lane
//! acknowledges it, then and only then removes the manager entry. A hard
//! daemon restart cannot reattach an orphaned local process; EMO-426's sweep
//! will fold witnessed dispatches without outcomes to `failed`, retryable,
//! with exit status unknown.

const TERMINAL_MONITOR_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);
const SETUP_FAILURE_MAX_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

pub(crate) type ProcessHandleTask =
    std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;
pub(crate) type ProcessHandleTaskSpawner =
    std::sync::Arc<dyn Fn(ProcessHandleTask) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct ProcessHandleDispatcher {
    inner: std::sync::Arc<ProcessHandleDispatcherInner>,
}

struct ProcessHandleDispatcherInner {
    store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
    ingress: std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink>,
    locks: tokio::sync::Mutex<
        std::collections::BTreeMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>,
    >,
    terminal_monitors:
        tokio::sync::Mutex<std::collections::HashSet<verlet_process::process::VerletProcessId>>,
    live_bindings: tokio::sync::Mutex<
        std::collections::HashMap<
            verlet_process::process::VerletProcessId,
            verlet_runtime_contracts::handle::HandleDispatchEnvelope,
        >,
    >,
    task_owner: Option<ProcessHandleTaskOwner>,
}

#[derive(Clone)]
struct ProcessHandleTaskOwner {
    cancellation: tokio_util::sync::CancellationToken,
    spawn: ProcessHandleTaskSpawner,
}

impl ProcessHandleDispatcher {
    pub fn new(
        store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
        ingress: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink,
        >,
    ) -> Self {
        Self::new_inner(store, ingress, None)
    }

    pub(crate) fn new_with_task_owner(
        store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
        ingress: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink,
        >,
        cancellation: tokio_util::sync::CancellationToken,
        spawn: ProcessHandleTaskSpawner,
    ) -> Self {
        Self::new_inner(
            store,
            ingress,
            Some(ProcessHandleTaskOwner {
                cancellation,
                spawn,
            }),
        )
    }

    fn new_inner(
        store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
        ingress: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink,
        >,
        task_owner: Option<ProcessHandleTaskOwner>,
    ) -> Self {
        Self {
            inner: std::sync::Arc::new(ProcessHandleDispatcherInner {
                store,
                ingress,
                locks: tokio::sync::Mutex::new(std::collections::BTreeMap::new()),
                terminal_monitors: tokio::sync::Mutex::new(std::collections::HashSet::new()),
                live_bindings: tokio::sync::Mutex::new(std::collections::HashMap::new()),
                task_owner,
            }),
        }
    }

    async fn await_while_owned<F>(&self, future: F) -> Option<F::Output>
    where
        F: std::future::Future,
    {
        let Some(owner) = &self.inner.task_owner else {
            return Some(future.await);
        };
        tokio::select! {
            _ = owner.cancellation.cancelled() => None,
            output = future => Some(output),
        }
    }

    /// Whether this daemon generation still owns a live backend binding for
    /// `dispatch_id`. Startup recovery uses the dispatcher's actual registry,
    /// never a timestamp or stream heuristic, to avoid failing live work.
    pub(crate) async fn is_live_dispatch(
        &self,
        dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
    ) -> bool {
        self.inner
            .live_bindings
            .lock()
            .await
            .values()
            .any(|binding| binding.dispatch_id == *dispatch_id)
    }

    /// Pins daemon startup ordering: EMO-426 runs before any surface may
    /// install a process binding in this generation.
    pub(crate) async fn assert_startup_registry_empty(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let live_bindings = self.inner.live_bindings.lock().await;
        let terminal_monitors = self.inner.terminal_monitors.lock().await;
        if live_bindings.is_empty() && terminal_monitors.is_empty() {
            return Ok(());
        }
        Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!(
                "startup recovery requires an empty process dispatcher registry ({} live bindings, {} terminal monitors)",
                live_bindings.len(),
                terminal_monitors.len(),
            ),
        ))
    }

    /// Submit the stable observer-death outcome through the same process
    /// ingress sink and envelope builder used by live terminal monitors.
    pub(crate) async fn submit_recovery_outcome(
        &self,
        binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        validate_binding(
            binding,
            &binding.consumer,
            &binding.dispatch_id,
            &binding.command_digest,
        )?;
        self.inner
            .ingress
            .submit_process_handle_envelope(recovery_outcome_envelope(binding)?)
            .await
    }

    /// Starts a backend only after the dispatch witness has settled.
    pub async fn dispatch_start(
        &self,
        consumer: &verlet_runtime_contracts::ThreadCoordinates,
        dispatch_id: verlet_runtime_contracts::handle::DispatchId,
        command_digest: String,
        manager: verlet_process::live::AsyncExecutionManager,
        backend: std::sync::Arc<dyn verlet_process::live::LiveProcessBackend>,
        request: verlet_process::live::AsyncProcessStartRequest,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_process::live::AsyncProcessOutcome> {
        self.dispatch_start_inner(
            consumer,
            dispatch_id,
            command_digest,
            manager,
            backend,
            request,
            None,
        )
        .await
    }

    pub async fn dispatch_start_cancellable(
        &self,
        consumer: &verlet_runtime_contracts::ThreadCoordinates,
        dispatch_id: verlet_runtime_contracts::handle::DispatchId,
        command_digest: String,
        manager: verlet_process::live::AsyncExecutionManager,
        backend: std::sync::Arc<dyn verlet_process::live::LiveProcessBackend>,
        request: verlet_process::live::AsyncProcessStartRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_process::live::AsyncProcessOutcome> {
        self.dispatch_start_inner(
            consumer,
            dispatch_id,
            command_digest,
            manager,
            backend,
            request,
            Some(cancellation),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_start_inner(
        &self,
        consumer: &verlet_runtime_contracts::ThreadCoordinates,
        dispatch_id: verlet_runtime_contracts::handle::DispatchId,
        command_digest: String,
        manager: verlet_process::live::AsyncExecutionManager,
        backend: std::sync::Arc<dyn verlet_process::live::LiveProcessBackend>,
        request: verlet_process::live::AsyncProcessStartRequest,
        cancellation: Option<tokio_util::sync::CancellationToken>,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_process::live::AsyncProcessOutcome> {
        let dispatch_lock = {
            let mut locks = self.inner.locks.lock().await;
            locks
                .entry(dispatch_id.to_string())
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = dispatch_lock.lock().await;

        if let Some(binding) = self.fold_dispatch(consumer, &dispatch_id).await? {
            validate_binding(&binding, consumer, &dispatch_id, &command_digest)?;
            let process_id = parse_process_handle(&binding)?;
            self.inner
                .live_bindings
                .lock()
                .await
                .insert(process_id, binding.clone());
            let outcome = match manager.snapshot(process_id, request.output_cap_bytes).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    self.forget_settled_binding(&binding, process_id, &dispatch_lock)
                        .await;
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        format!(
                            "process dispatch {dispatch_id} is durably claimed by handle {process_id}, but its live registry entry is gone; refusing to re-execute"
                        ),
                    ));
                }
            };
            self.ensure_terminal_monitor(
                binding,
                manager,
                request.output_cap_bytes,
                std::sync::Arc::clone(&dispatch_lock),
            )
            .await;
            return Ok(outcome);
        }

        let process_id = verlet_process::process::VerletProcessId::new();
        let binding = verlet_runtime_contracts::handle::HandleDispatchEnvelope {
            dispatch_id: dispatch_id.clone(),
            handle: verlet_runtime_contracts::handle::HandleId::process(process_id.to_string()),
            consumer: consumer.clone(),
            command_digest,
        };
        self.inner
            .ingress
            .submit_process_handle_envelope(dispatch_envelope(&binding)?)
            .await?;

        let folded = self.fold_dispatch(consumer, &dispatch_id).await?.ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "process dispatch {dispatch_id} was acknowledged but its durable witness was not observable"
            ))
        })?;
        if folded != binding {
            validate_binding(&folded, consumer, &dispatch_id, &binding.command_digest)?;
            let original_id = parse_process_handle(&folded)?;
            self.inner
                .live_bindings
                .lock()
                .await
                .insert(original_id, folded);
            return match manager
                .snapshot(original_id, request.output_cap_bytes)
                .await
            {
                Ok(outcome) => Ok(outcome),
                Err(_) => {
                    self.forget_settled_binding(&binding, original_id, &dispatch_lock)
                        .await;
                    Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        format!(
                            "process dispatch {dispatch_id} lost the durable serialization race and original handle {original_id} is not in this live registry; refusing to re-execute"
                        ),
                    ))
                }
            };
        }

        self.inner
            .live_bindings
            .lock()
            .await
            .insert(process_id, binding.clone());

        let output_cap = request.output_cap_bytes;
        let this = self.clone();
        let start_binding = binding.clone();
        let start_manager = manager.clone();
        let start_dispatch_lock = std::sync::Arc::clone(&dispatch_lock);
        let start = tokio::spawn(async move {
            let request = request
                .with_process_id(process_id)
                .retain_terminal_until_acknowledged();
            let started = match cancellation {
                Some(cancellation) => {
                    start_manager
                        .start_cancellable(backend, request, cancellation)
                        .await
                }
                None => start_manager.start(backend, request).await,
            };
            match started {
                Ok(outcome) => {
                    this.ensure_terminal_monitor(
                        start_binding,
                        start_manager,
                        output_cap,
                        start_dispatch_lock,
                    )
                    .await;
                    Ok(outcome)
                }
                Err(err) => {
                    this.deliver_setup_failure(&start_binding, &err.to_string())
                        .await;
                    this.forget_settled_binding(&start_binding, process_id, &start_dispatch_lock)
                        .await;
                    Err(crate::kernel::runtime_host::VerletError::from(err))
                }
            }
        });
        start.await.map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "process dispatch {dispatch_id} start task failed: {err}"
            ))
        })?
    }

    /// Proves that a public pull/control verb addresses a handle returned by
    /// this owning dispatch surface rather than reaching the registry by raw
    /// process id alone.
    pub async fn require_live_handle(
        &self,
        process_id: verlet_process::process::VerletProcessId,
        consumer: Option<&verlet_runtime_contracts::ThreadCoordinates>,
    ) -> crate::kernel::runtime_host::VerletResult<
        verlet_runtime_contracts::handle::HandleDispatchEnvelope,
    > {
        let binding = self
            .inner
            .live_bindings
            .lock()
            .await
            .get(&process_id)
            .cloned()
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                    "process {process_id} is not bound to a witnessed dispatch on this owning surface"
                ))
            })?;
        if consumer.is_some_and(|consumer| consumer != &binding.consumer) {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                format!("process {process_id} belongs to a different handle consumer"),
            ));
        }
        Ok(binding)
    }

    async fn ensure_terminal_monitor(
        &self,
        binding: verlet_runtime_contracts::handle::HandleDispatchEnvelope,
        manager: verlet_process::live::AsyncExecutionManager,
        output_cap_bytes: usize,
        dispatch_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    ) {
        let Ok(process_id) = parse_process_handle(&binding) else {
            return;
        };
        if !self.inner.terminal_monitors.lock().await.insert(process_id) {
            return;
        }
        let this = self.clone();
        let monitor = async move {
            loop {
                let Some(snapshot) = this
                    .await_while_owned(manager.snapshot(process_id, output_cap_bytes))
                    .await
                else {
                    break;
                };
                match snapshot {
                    Ok(outcome)
                        if outcome.snapshot.status
                            != verlet_process::live::ProcessSnapshotStatus::Running =>
                    {
                        let envelope = match terminal_envelope(&binding, &outcome.snapshot) {
                            Ok(envelope) => envelope,
                            Err(err) => {
                                eprintln!(
                                    "verlet process settlement {} encode failed: {err}",
                                    binding.dispatch_id
                                );
                                if this
                                    .await_while_owned(tokio::time::sleep(
                                        TERMINAL_MONITOR_INTERVAL,
                                    ))
                                    .await
                                    .is_none()
                                {
                                    break;
                                }
                                continue;
                            }
                        };
                        match this
                            .inner
                            .ingress
                            .submit_process_handle_envelope(envelope)
                            .await
                        {
                            Ok(()) => {
                                if let Err(err) = manager.acknowledge_terminal(process_id).await {
                                    eprintln!(
                                        "verlet process settlement {} could not release terminal entry: {err}",
                                        binding.dispatch_id
                                    );
                                    if this
                                        .await_while_owned(tokio::time::sleep(
                                            TERMINAL_MONITOR_INTERVAL,
                                        ))
                                        .await
                                        .is_none()
                                    {
                                        break;
                                    }
                                    continue;
                                }
                                this.forget_settled_binding(&binding, process_id, &dispatch_lock)
                                    .await;
                                break;
                            }
                            Err(err) => eprintln!(
                                "verlet process settlement {} retrying after ingress failure: {err}",
                                binding.dispatch_id
                            ),
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!(
                            "verlet process settlement {} invariant violation: live registry entry unavailable before acknowledgement; retrying: {err}",
                            binding.dispatch_id
                        );
                    }
                }
                if this
                    .await_while_owned(tokio::time::sleep(TERMINAL_MONITOR_INTERVAL))
                    .await
                    .is_none()
                {
                    break;
                }
            }
            this.inner
                .terminal_monitors
                .lock()
                .await
                .remove(&process_id);
        };
        let accepted = match &self.inner.task_owner {
            Some(owner) => (owner.spawn)(Box::pin(monitor)),
            None => {
                tokio::spawn(monitor);
                true
            }
        };
        if !accepted {
            self.inner
                .terminal_monitors
                .lock()
                .await
                .remove(&process_id);
        }
    }

    async fn deliver_setup_failure(
        &self,
        binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
        reason: &str,
    ) {
        let envelope = setup_failure_envelope(binding, reason);
        let mut retry_interval = TERMINAL_MONITOR_INTERVAL;
        loop {
            let Some(delivery) = self
                .await_while_owned(
                    self.inner
                        .ingress
                        .submit_process_handle_envelope(envelope.clone()),
                )
                .await
            else {
                return;
            };
            match delivery {
                Ok(()) => break,
                Err(err) => eprintln!(
                    "verlet process setup failure {} retrying after ingress failure in {}ms: {err}",
                    binding.dispatch_id,
                    retry_interval.as_millis()
                ),
            }
            if self
                .await_while_owned(tokio::time::sleep(retry_interval))
                .await
                .is_none()
            {
                return;
            }
            retry_interval = retry_interval
                .saturating_mul(2)
                .min(SETUP_FAILURE_MAX_RETRY_INTERVAL);
        }
    }

    async fn forget_settled_binding(
        &self,
        binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
        process_id: verlet_process::process::VerletProcessId,
        dispatch_lock: &std::sync::Arc<tokio::sync::Mutex<()>>,
    ) {
        self.inner.live_bindings.lock().await.remove(&process_id);
        let mut locks = self.inner.locks.lock().await;
        let owns_map_entry = locks
            .get(binding.dispatch_id.as_str())
            .is_some_and(|current| std::sync::Arc::ptr_eq(current, dispatch_lock));
        if owns_map_entry {
            locks.remove(binding.dispatch_id.as_str());
        }
    }

    async fn fold_dispatch(
        &self,
        consumer: &verlet_runtime_contracts::ThreadCoordinates,
        dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<verlet_runtime_contracts::handle::HandleDispatchEnvelope>,
    > {
        let events = self
            .inner
            .store
            .read_events(
                &crate::kernel::control_decision::control_stream_id(consumer),
                None,
            )
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        let mut found = None;
        for event in events
            .into_iter()
            .filter(|event| event.kind == verlet_history::EventKind::IoIngressReceived)
        {
            if event
                .payload
                .pointer("/content/payload/dispatch_id")
                .and_then(serde_json::Value::as_str)
                != Some(dispatch_id.as_str())
            {
                continue;
            }
            let witness =
                serde_json::from_value::<verlet_history::IoIngressReceivedPayload>(event.payload)
                    .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "decode ingress witness: {err}"
                    ))
                })?;
            let content = witness.content.ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "process dispatch {dispatch_id} witness omitted its fold content"
                ))
            })?;
            let content = serde_json::from_value::<verlet_io_core::IngressContent>(content)
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "decode process dispatch content: {err}"
                    ))
                })?;
            let verlet_io_core::IngressContent::Event { kind, payload } = content else {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
                    "process dispatch {dispatch_id} witness is not event content"
                )));
            };
            if kind != verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
                    "process dispatch {dispatch_id} witness has content kind {kind}"
                )));
            }
            let binding = serde_json::from_value::<
                verlet_runtime_contracts::handle::HandleDispatchEnvelope,
            >(payload)
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "decode process dispatch envelope: {err}"
                ))
            })?;
            if let Some(existing) = &found
                && existing != &binding
            {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
                    "process dispatch {dispatch_id} has conflicting durable witnesses"
                )));
            }
            found = Some(binding);
        }
        Ok(found)
    }
}

pub fn command_digest(bytes: &[u8]) -> String {
    verlet_agent::contracts::sha256_hex(bytes)
}

pub(crate) fn validate_binding(
    binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
    consumer: &verlet_runtime_contracts::ThreadCoordinates,
    dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
    command_digest: &str,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if binding.consumer != *consumer
        || binding.dispatch_id != *dispatch_id
        || binding.command_digest != command_digest
        || binding.handle.kind != verlet_runtime_contracts::handle::HandleKind::Process
    {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!("process dispatch {dispatch_id} retry does not match its durable request"),
        ));
    }
    Ok(())
}

fn parse_process_handle(
    binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
) -> crate::kernel::runtime_host::VerletResult<verlet_process::process::VerletProcessId> {
    if binding.handle.kind != verlet_runtime_contracts::handle::HandleKind::Process {
        return Err(crate::kernel::runtime_host::VerletError::History(format!(
            "dispatch {} is not bound to a process handle",
            binding.dispatch_id
        )));
    }
    binding.handle.id.parse().map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "dispatch {} has invalid process handle: {err}",
            binding.dispatch_id
        ))
    })
}

fn dispatch_envelope(
    binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
) -> crate::kernel::runtime_host::VerletResult<verlet_io_core::IngressEnvelope> {
    let mut envelope = verlet_io_core::IngressEnvelope::new(
        verlet_io_core::IoSource::new("cooldis.handle", "process"),
        verlet_io_core::IoConversation::new(
            format!("thread:{}", binding.consumer.thread_id),
            verlet_io_core::ConversationKind::System,
        ),
        verlet_io_core::IngressContent::Event {
            kind: verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND.to_string(),
            payload: serde_json::to_value(binding).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                    "encode process dispatch: {err}"
                ))
            })?,
        },
        now_ms(),
    )
    .with_dedupe_key(verlet_io_core::IoDedupeKey::new(
        verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND,
        binding.dispatch_id.to_string(),
    ))
    .with_delivery(verlet_io_core::IoDelivery::new(
        binding.dispatch_id.to_string(),
    ))
    .with_principal(verlet_io_core::IoPrincipal::new(
        binding.consumer.tenant_id.clone(),
        binding.consumer.user_id.clone(),
        format!("handle:{}", binding.dispatch_id),
    ))
    .with_metadata(
        "cooldis_route_id",
        verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND,
    )
    .with_metadata("cooldis_route_policy", "observe_only");
    envelope.id = deterministic_ingress_id("dispatch", &binding.dispatch_id);
    Ok(envelope)
}

fn terminal_envelope(
    binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
    snapshot: &verlet_process::live::AsyncProcessSnapshot,
) -> crate::kernel::runtime_host::VerletResult<verlet_io_core::IngressEnvelope> {
    let (outcome, outcome_reason, retryable) = terminal_projection(snapshot)?;
    outcome_envelope(
        binding,
        verlet_runtime_contracts::handle::HandleTerminalEnvelope {
            dispatch_id: binding.dispatch_id.clone(),
            handle: binding.handle.clone(),
            outcome,
            outcome_reason,
            result: None,
            result_schema_id: None,
            artifact_refs: Vec::new(),
            usage: None,
            retryable,
        },
    )
}

fn setup_failure_envelope(
    binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
    reason: &str,
) -> verlet_io_core::IngressEnvelope {
    outcome_envelope(
        binding,
        verlet_runtime_contracts::handle::HandleTerminalEnvelope {
            dispatch_id: binding.dispatch_id.clone(),
            handle: binding.handle.clone(),
            outcome: verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed,
            outcome_reason: Some(format!("process setup failed before spawn: {reason}")),
            result: None,
            result_schema_id: None,
            artifact_refs: Vec::new(),
            usage: None,
            retryable: true,
        },
    )
    .expect("serializing the fixed process setup failure envelope cannot fail")
}

/// Stable terminal envelope for a process whose durable dispatch survived
/// the daemon generation that owned its unreattachable host backend.
pub(crate) fn recovery_outcome_envelope(
    binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
) -> crate::kernel::runtime_host::VerletResult<verlet_io_core::IngressEnvelope> {
    outcome_envelope(
        binding,
        verlet_runtime_contracts::handle::HandleTerminalEnvelope {
            dispatch_id: binding.dispatch_id.clone(),
            handle: binding.handle.clone(),
            outcome: verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed,
            outcome_reason: Some(
                "startup recovery after process observer death; exit status unknown".to_string(),
            ),
            result: None,
            result_schema_id: None,
            artifact_refs: Vec::new(),
            usage: None,
            retryable: true,
        },
    )
}

fn outcome_envelope(
    binding: &verlet_runtime_contracts::handle::HandleDispatchEnvelope,
    terminal: verlet_runtime_contracts::handle::HandleTerminalEnvelope,
) -> crate::kernel::runtime_host::VerletResult<verlet_io_core::IngressEnvelope> {
    let mut envelope = verlet_io_core::IngressEnvelope::new(
        verlet_io_core::IoSource::new("cooldis.handle", "process"),
        verlet_io_core::IoConversation::new(
            format!("thread:{}", binding.consumer.thread_id),
            verlet_io_core::ConversationKind::System,
        ),
        verlet_io_core::IngressContent::Event {
            kind: verlet_runtime_contracts::handle::HANDLE_OUTCOME_CONTENT_KIND.to_string(),
            payload: serde_json::to_value(terminal).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                    "encode process settlement: {err}"
                ))
            })?,
        },
        now_ms(),
    )
    .with_dedupe_key(verlet_io_core::IoDedupeKey::new(
        verlet_runtime_contracts::handle::HANDLE_OUTCOME_CONTENT_KIND,
        binding.dispatch_id.to_string(),
    ))
    .with_delivery(verlet_io_core::IoDelivery::new(
        binding.dispatch_id.to_string(),
    ))
    .with_principal(verlet_io_core::IoPrincipal::new(
        binding.consumer.tenant_id.clone(),
        binding.consumer.user_id.clone(),
        format!("handle:{}", binding.dispatch_id),
    ))
    .with_metadata(
        "cooldis_route_id",
        verlet_runtime_contracts::handle::HANDLE_OUTCOME_CONTENT_KIND,
    )
    .with_metadata("cooldis_route_policy", "queue_per_conversation");
    envelope.id = deterministic_ingress_id("outcome", &binding.dispatch_id);
    Ok(envelope)
}

/// Projects backend terminal detail onto ADR 0006's three outcomes.
///
/// Non-zero ordinary exits are deterministic failures and not retryable.
/// Missing exit codes indicate signal termination and are retryable because
/// the signal source is not known here. Setup/backend failures and timeouts
/// are retryable; an explicit terminate request is cancelled and not
/// retryable. The backend's single terminal event wins terminate/natural-exit
/// races.
fn terminal_projection(
    snapshot: &verlet_process::live::AsyncProcessSnapshot,
) -> crate::kernel::runtime_host::VerletResult<(
    verlet_runtime_contracts::handle::HandleTerminalOutcome,
    Option<String>,
    bool,
)> {
    let terminal = snapshot
        .events
        .iter()
        .rev()
        .find(|event| event.kind.is_terminal())
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "terminal process snapshot {} has no terminal event",
                snapshot.label
            ))
        })?;
    Ok(terminal_kind_projection(&terminal.kind))
}

fn terminal_kind_projection(
    kind: &verlet_process::process::VerletProcessEventKind,
) -> (
    verlet_runtime_contracts::handle::HandleTerminalOutcome,
    Option<String>,
    bool,
) {
    match kind {
        verlet_process::process::VerletProcessEventKind::Completed { status } if status.success => {
            (
                verlet_runtime_contracts::handle::HandleTerminalOutcome::Completed,
                Some("exit status 0".to_string()),
                false,
            )
        }
        verlet_process::process::VerletProcessEventKind::Completed { status } => {
            match status.code {
                Some(code) => (
                    verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed,
                    Some(format!("exit status {code}")),
                    false,
                ),
                None => (
                    verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed,
                    Some("process terminated by signal; exit status unavailable".to_string()),
                    true,
                ),
            }
        }
        verlet_process::process::VerletProcessEventKind::Failed { code, message } => (
            verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed,
            Some(format!("{code}: {message}")),
            true,
        ),
        verlet_process::process::VerletProcessEventKind::TimedOut {
            timeout_ms,
            message,
        } => (
            verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed,
            Some(match timeout_ms {
                Some(ms) => format!("timed out after {ms}ms: {message}"),
                None => format!("timed out: {message}"),
            }),
            true,
        ),
        verlet_process::process::VerletProcessEventKind::Cancelled { reason } => (
            verlet_runtime_contracts::handle::HandleTerminalOutcome::Cancelled,
            Some(reason.clone()),
            false,
        ),
        _ => unreachable!("terminal event predicate and projection are exhaustive"),
    }
}

fn deterministic_ingress_id(
    stage: &str,
    dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
) -> String {
    let digest =
        verlet_agent::contracts::sha256_hex(format!("{stage}:{}", dispatch_id.as_str()).as_bytes());
    let digest = digest.strip_prefix("sha256:").unwrap_or(&digest);
    format!("ing-handle-{}", &digest[..32])
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {

    struct RecordingIngress {
        store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
        consumer: verlet_runtime_contracts::ThreadCoordinates,
    }

    #[async_trait::async_trait]
    impl crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink for RecordingIngress {
        async fn submit_process_handle_envelope(
            &self,
            envelope: verlet_io_core::IngressEnvelope,
        ) -> crate::kernel::runtime_host::VerletResult<()> {
            let verlet_io_core::IngressContent::Event { kind, .. } = &envelope.content else {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "test process ingress requires event content".to_string(),
                ));
            };
            let record = verlet_history::NewEventRecord::witnessed(
                self.consumer.clone(),
                verlet_history::EventKind::IoIngressReceived,
                serde_json::to_value(verlet_history::IoIngressReceivedPayload {
                    route_id: Some(kind.clone()),
                    dedupe_key: envelope.dedupe_key.map(|key| key.stable_key()),
                    external_conversation_id: Some(envelope.conversation.external_conversation_id),
                    external_actor_id: None,
                    external_message_id: None,
                    content: Some(serde_json::to_value(envelope.content).unwrap()),
                    envelope_digest: "sha256:test-process-ingress".to_string(),
                })
                .unwrap(),
            );
            self.store
                .append_events(
                    &crate::kernel::control_decision::control_stream_id(&self.consumer),
                    vec![record],
                )
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(err.to_string())
                })?;
            Ok(())
        }
    }

    struct DelayedTimeoutBackend;

    #[async_trait::async_trait]
    impl verlet_process::live::LiveProcessBackend for DelayedTimeoutBackend {
        fn backend_kind(&self) -> verlet_process::process::VerletProcessBackend {
            verlet_process::process::VerletProcessBackend::Bridge
        }

        async fn start(
            &self,
            request: verlet_process::live::LiveProcessStartRequest,
            process: verlet_process::process::VerletProcessHandle,
            cancellation: tokio_util::sync::CancellationToken,
        ) -> verlet_process::VerletProcessResult<verlet_process::live::LiveProcessSpawn> {
            process.record(verlet_process::process::VerletProcessEventKind::Started {
                command: Some("delayed timeout".to_string()),
            });
            let join = tokio::spawn(async move {
                cancellation.cancelled().await;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                process.record(verlet_process::process::VerletProcessEventKind::TimedOut {
                    timeout_ms: Some(request.deadline.remaining().as_millis() as u64),
                    message: "execution deadline elapsed".to_string(),
                });
                Ok(())
            });
            Ok(verlet_process::live::LiveProcessSpawn { stdin: None, join })
        }
    }

    struct DelayedCompletionBackend {
        started: std::sync::Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl verlet_process::live::LiveProcessBackend for DelayedCompletionBackend {
        fn backend_kind(&self) -> verlet_process::process::VerletProcessBackend {
            verlet_process::process::VerletProcessBackend::Bridge
        }

        async fn start(
            &self,
            _request: verlet_process::live::LiveProcessStartRequest,
            process: verlet_process::process::VerletProcessHandle,
            _cancellation: tokio_util::sync::CancellationToken,
        ) -> verlet_process::VerletProcessResult<verlet_process::live::LiveProcessSpawn> {
            process.record(verlet_process::process::VerletProcessEventKind::Started {
                command: Some("delayed completion".to_string()),
            });
            self.started.notify_one();
            let join = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                process.record(verlet_process::process::VerletProcessEventKind::Completed {
                    status: verlet_process::process::VerletProcessExitStatus::exited(0),
                });
                Ok(())
            });
            Ok(verlet_process::live::LiveProcessSpawn { stdin: None, join })
        }
    }

    async fn outcome_count(
        store: &std::sync::Arc<dyn verlet_history::RuntimeStore>,
        consumer: &verlet_runtime_contracts::ThreadCoordinates,
        dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
    ) -> usize {
        store
            .read_events(
                &crate::kernel::control_decision::control_stream_id(consumer),
                None,
            )
            .await
            .unwrap()
            .iter()
            .filter(|event| {
                event.kind == verlet_history::EventKind::IoIngressReceived
                    && event
                        .payload
                        .get("route_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(verlet_runtime_contracts::handle::HANDLE_OUTCOME_CONTENT_KIND)
                    && event
                        .payload
                        .pointer("/content/payload/dispatch_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(dispatch_id.as_str())
            })
            .count()
    }

    #[tokio::test]
    async fn expired_process_survives_cleanup_until_timeout_outcome_is_acknowledged() {
        let store: std::sync::Arc<dyn verlet_history::RuntimeStore> =
            std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let consumer = verlet_runtime_contracts::ThreadCoordinates::new(
            "tenant",
            "user",
            "deadline-settlement",
        );
        let dispatcher = crate::kernel::process_handle_dispatch::ProcessHandleDispatcher::new(
            std::sync::Arc::clone(&store),
            std::sync::Arc::new(RecordingIngress {
                store: std::sync::Arc::clone(&store),
                consumer: consumer.clone(),
            }),
        );
        let manager = verlet_process::live::AsyncExecutionManager::default();
        let backend: std::sync::Arc<dyn verlet_process::live::LiveProcessBackend> =
            std::sync::Arc::new(DelayedTimeoutBackend);
        let dispatch_id = verlet_runtime_contracts::handle::DispatchId::new("deadline-dispatch-a");

        dispatcher
            .dispatch_start(
                &consumer,
                dispatch_id.clone(),
                crate::kernel::process_handle_dispatch::command_digest(b"process-a"),
                manager.clone(),
                std::sync::Arc::clone(&backend),
                verlet_process::live::AsyncProcessStartRequest::virtual_bash_script("process-a")
                    .with_deadline(verlet_process::execution::ExecutionDeadline::from_now(
                        std::time::Duration::from_millis(5),
                    ))
                    .with_yield_time(std::time::Duration::ZERO),
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        dispatcher
            .dispatch_start(
                &consumer,
                verlet_runtime_contracts::handle::DispatchId::new("deadline-dispatch-b"),
                crate::kernel::process_handle_dispatch::command_digest(b"process-b"),
                manager,
                backend,
                verlet_process::live::AsyncProcessStartRequest::virtual_bash_script("process-b")
                    .with_deadline(verlet_process::execution::ExecutionDeadline::from_now(
                        std::time::Duration::from_secs(1),
                    ))
                    .with_yield_time(std::time::Duration::ZERO),
            )
            .await
            .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if outcome_count(&store, &consumer, &dispatch_id).await == 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expired process timeout outcome should settle");
        assert!(
            !dispatcher
                .inner
                .locks
                .lock()
                .await
                .contains_key(dispatch_id.as_str())
        );
        assert!(
            !dispatcher
                .inner
                .live_bindings
                .lock()
                .await
                .values()
                .any(|binding| binding.dispatch_id == dispatch_id)
        );

        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        assert_eq!(outcome_count(&store, &consumer, &dispatch_id).await, 1);
    }

    #[tokio::test]
    async fn caller_cancellation_after_backend_start_does_not_cancel_settlement_monitor() {
        let store: std::sync::Arc<dyn verlet_history::RuntimeStore> =
            std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
        let consumer = verlet_runtime_contracts::ThreadCoordinates::new(
            "tenant",
            "user",
            "cancelled-dispatch",
        );
        let dispatcher = crate::kernel::process_handle_dispatch::ProcessHandleDispatcher::new(
            std::sync::Arc::clone(&store),
            std::sync::Arc::new(RecordingIngress {
                store: std::sync::Arc::clone(&store),
                consumer: consumer.clone(),
            }),
        );
        let manager = verlet_process::live::AsyncExecutionManager::default();
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let started_wait = started.notified();
        let backend: std::sync::Arc<dyn verlet_process::live::LiveProcessBackend> =
            std::sync::Arc::new(DelayedCompletionBackend {
                started: std::sync::Arc::clone(&started),
            });
        let dispatch_id =
            verlet_runtime_contracts::handle::DispatchId::new("cancelled-after-start");
        let task_dispatcher = dispatcher.clone();
        let task_consumer = consumer.clone();
        let task_dispatch_id = dispatch_id.clone();
        let task = tokio::spawn(async move {
            task_dispatcher
                .dispatch_start(
                    &task_consumer,
                    task_dispatch_id,
                    crate::kernel::process_handle_dispatch::command_digest(b"cancelled-process"),
                    manager,
                    backend,
                    verlet_process::live::AsyncProcessStartRequest::virtual_bash_script(
                        "cancelled-process",
                    )
                    .with_deadline(verlet_process::execution::ExecutionDeadline::from_now(
                        std::time::Duration::from_secs(1),
                    ))
                    .with_yield_time(std::time::Duration::from_secs(1)),
                )
                .await
        });

        started_wait.await;
        task.abort();
        let _ = task.await;

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if outcome_count(&store, &consumer, &dispatch_id).await == 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached monitor should settle after caller cancellation");
        assert!(dispatcher.inner.locks.lock().await.is_empty());
        assert!(dispatcher.inner.live_bindings.lock().await.is_empty());
    }

    #[test]
    fn outcome_mapping_pins_exit_signal_and_terminate_edges() {
        assert_eq!(
            crate::kernel::process_handle_dispatch::terminal_kind_projection(
                &verlet_process::process::VerletProcessEventKind::Completed {
                    status: verlet_process::process::VerletProcessExitStatus::exited(0),
                }
            ),
            (
                verlet_runtime_contracts::handle::HandleTerminalOutcome::Completed,
                Some("exit status 0".to_string()),
                false,
            )
        );
        assert_eq!(
            crate::kernel::process_handle_dispatch::terminal_kind_projection(
                &verlet_process::process::VerletProcessEventKind::Completed {
                    status: verlet_process::process::VerletProcessExitStatus::exited(23),
                }
            ),
            (
                verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed,
                Some("exit status 23".to_string()),
                false,
            )
        );
        assert_eq!(
            crate::kernel::process_handle_dispatch::terminal_kind_projection(
                &verlet_process::process::VerletProcessEventKind::Completed {
                    status: verlet_process::process::VerletProcessExitStatus {
                        code: None,
                        success: false,
                    },
                }
            ),
            (
                verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed,
                Some("process terminated by signal; exit status unavailable".to_string()),
                true,
            )
        );
        assert_eq!(
            crate::kernel::process_handle_dispatch::terminal_kind_projection(
                &verlet_process::process::VerletProcessEventKind::Cancelled {
                    reason: "operator requested".to_string(),
                }
            ),
            (
                verlet_runtime_contracts::handle::HandleTerminalOutcome::Cancelled,
                Some("operator requested".to_string()),
                false,
            )
        );
    }

    #[test]
    fn setup_failure_is_retryable_process_outcome_without_usage() {
        let binding = verlet_runtime_contracts::handle::HandleDispatchEnvelope {
            dispatch_id: verlet_runtime_contracts::handle::DispatchId::new(
                "setup-failure-dispatch",
            ),
            handle: verlet_runtime_contracts::handle::HandleId::process(
                "018f0000-0000-7000-8000-000000000420",
            ),
            consumer: verlet_runtime_contracts::ThreadCoordinates {
                tenant_id: "tenant".to_string(),
                user_id: "user".to_string(),
                session_id: "session".to_string(),
                thread_id: verlet_runtime_contracts::ThreadId::parse_str(
                    "018f0000-0000-7000-8000-000000000419",
                )
                .unwrap(),
            },
            command_digest: "sha256:command".to_string(),
        };
        let envelope = crate::kernel::process_handle_dispatch::setup_failure_envelope(
            &binding,
            "executable not found",
        );

        assert_eq!(
            envelope.dedupe_key,
            Some(verlet_io_core::IoDedupeKey::new(
                verlet_runtime_contracts::handle::HANDLE_OUTCOME_CONTENT_KIND,
                "setup-failure-dispatch"
            ))
        );
        let verlet_io_core::IngressContent::Event { payload, .. } = envelope.content else {
            panic!("setup failure must be event ingress");
        };
        let terminal: verlet_runtime_contracts::handle::HandleTerminalEnvelope =
            serde_json::from_value(payload).unwrap();
        assert_eq!(
            terminal.handle.kind,
            verlet_runtime_contracts::handle::HandleKind::Process
        );
        assert_eq!(
            terminal.outcome,
            verlet_runtime_contracts::handle::HandleTerminalOutcome::Failed
        );
        assert!(terminal.retryable);
        assert!(terminal.usage.is_none());
        assert!(
            terminal
                .outcome_reason
                .as_deref()
                .unwrap()
                .contains("executable not found")
        );
    }
}

#[cfg(test)]
pub(crate) fn test_process_dispatcher(
    store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
    consumer: verlet_runtime_contracts::ThreadCoordinates,
) -> ProcessHandleDispatcher {
    struct TestIngress {
        store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
        consumer: verlet_runtime_contracts::ThreadCoordinates,
    }

    #[async_trait::async_trait]
    impl crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink for TestIngress {
        async fn submit_process_handle_envelope(
            &self,
            envelope: verlet_io_core::IngressEnvelope,
        ) -> crate::kernel::runtime_host::VerletResult<()> {
            let verlet_io_core::IngressContent::Event { kind, .. } = &envelope.content else {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "test process ingress requires event content".to_string(),
                ));
            };
            if kind != verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND {
                return Ok(());
            }
            let record = verlet_history::NewEventRecord::witnessed(
                self.consumer.clone(),
                verlet_history::EventKind::IoIngressReceived,
                serde_json::to_value(verlet_history::IoIngressReceivedPayload {
                    route_id: Some(
                        verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND.to_string(),
                    ),
                    dedupe_key: envelope.dedupe_key.map(|key| key.stable_key()),
                    external_conversation_id: Some(envelope.conversation.external_conversation_id),
                    external_actor_id: None,
                    external_message_id: None,
                    content: Some(serde_json::to_value(envelope.content).unwrap()),
                    envelope_digest: "sha256:test-process-dispatch".to_string(),
                })
                .unwrap(),
            );
            self.store
                .append_events(
                    &crate::kernel::control_decision::control_stream_id(&self.consumer),
                    vec![record],
                )
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(err.to_string())
                })?;
            Ok(())
        }
    }

    ProcessHandleDispatcher::new(
        std::sync::Arc::clone(&store),
        std::sync::Arc::new(TestIngress { store, consumer }),
    )
}
