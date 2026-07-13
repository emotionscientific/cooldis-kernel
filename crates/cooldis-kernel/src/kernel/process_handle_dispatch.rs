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

use crate::{
    CooldisError, CooldisResult, EventKind, IoIngressReceivedPayload, ProcessHandleIngressSink,
    RuntimeStore, ThreadCoordinates, control_stream_id,
};
use cooldis_io_core::{
    ConversationKind, IngressContent, IngressEnvelope, IoConversation, IoDedupeKey, IoSource,
};
use cooldis_process::{
    AsyncExecutionManager, AsyncProcessOutcome, AsyncProcessSnapshot, AsyncProcessStartRequest,
    CooldisProcessEventKind, CooldisProcessId, LiveProcessBackend, ProcessSnapshotStatus,
};
use cooldis_runtime_contracts::{
    DispatchId, HANDLE_DISPATCH_CONTENT_KIND, HANDLE_OUTCOME_CONTENT_KIND, HandleDispatchEnvelope,
    HandleId, HandleKind, HandleTerminalEnvelope, HandleTerminalOutcome,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

const TERMINAL_MONITOR_INTERVAL: Duration = Duration::from_millis(25);
const SETUP_FAILURE_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct ProcessHandleDispatcher {
    inner: Arc<ProcessHandleDispatcherInner>,
}

struct ProcessHandleDispatcherInner {
    store: Arc<dyn RuntimeStore>,
    ingress: Arc<dyn ProcessHandleIngressSink>,
    locks: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    terminal_monitors: Mutex<HashSet<CooldisProcessId>>,
    live_bindings: Mutex<HashMap<CooldisProcessId, HandleDispatchEnvelope>>,
}

impl ProcessHandleDispatcher {
    pub fn new(store: Arc<dyn RuntimeStore>, ingress: Arc<dyn ProcessHandleIngressSink>) -> Self {
        Self {
            inner: Arc::new(ProcessHandleDispatcherInner {
                store,
                ingress,
                locks: Mutex::new(BTreeMap::new()),
                terminal_monitors: Mutex::new(HashSet::new()),
                live_bindings: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Starts a backend only after the dispatch witness has settled.
    pub async fn dispatch_start(
        &self,
        consumer: &ThreadCoordinates,
        dispatch_id: DispatchId,
        command_digest: String,
        manager: AsyncExecutionManager,
        backend: Arc<dyn LiveProcessBackend>,
        request: AsyncProcessStartRequest,
    ) -> CooldisResult<AsyncProcessOutcome> {
        let dispatch_lock = {
            let mut locks = self.inner.locks.lock().await;
            locks
                .entry(dispatch_id.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
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
                    return Err(CooldisError::RuntimeExecution(format!(
                        "process dispatch {dispatch_id} is durably claimed by handle {process_id}, but its live registry entry is gone; refusing to re-execute"
                    )));
                }
            };
            self.ensure_terminal_monitor(
                binding,
                manager,
                request.output_cap_bytes,
                Arc::clone(&dispatch_lock),
            )
            .await;
            return Ok(outcome);
        }

        let process_id = CooldisProcessId::new();
        let binding = HandleDispatchEnvelope {
            dispatch_id: dispatch_id.clone(),
            handle: HandleId::process(process_id.to_string()),
            consumer: consumer.clone(),
            command_digest,
        };
        self.inner
            .ingress
            .submit_process_handle_envelope(dispatch_envelope(&binding)?)
            .await?;

        let folded = self.fold_dispatch(consumer, &dispatch_id).await?.ok_or_else(|| {
            CooldisError::History(format!(
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
                    Err(CooldisError::RuntimeExecution(format!(
                        "process dispatch {dispatch_id} lost the durable serialization race and original handle {original_id} is not in this live registry; refusing to re-execute"
                    )))
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
        let start_dispatch_lock = Arc::clone(&dispatch_lock);
        let start = tokio::spawn(async move {
            match start_manager
                .start(
                    backend,
                    request
                        .with_process_id(process_id)
                        .retain_terminal_until_acknowledged(),
                )
                .await
            {
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
                    Err(CooldisError::from(err))
                }
            }
        });
        start.await.map_err(|err| {
            CooldisError::RuntimeExecution(format!(
                "process dispatch {dispatch_id} start task failed: {err}"
            ))
        })?
    }

    /// Proves that a public pull/control verb addresses a handle returned by
    /// this owning dispatch surface rather than reaching the registry by raw
    /// process id alone.
    pub async fn require_live_handle(
        &self,
        process_id: CooldisProcessId,
        consumer: Option<&ThreadCoordinates>,
    ) -> CooldisResult<HandleDispatchEnvelope> {
        let binding = self
            .inner
            .live_bindings
            .lock()
            .await
            .get(&process_id)
            .cloned()
            .ok_or_else(|| {
                CooldisError::RuntimeExecution(format!(
                    "process {process_id} is not bound to a witnessed dispatch on this owning surface"
                ))
            })?;
        if consumer.is_some_and(|consumer| consumer != &binding.consumer) {
            return Err(CooldisError::RuntimeExecution(format!(
                "process {process_id} belongs to a different handle consumer"
            )));
        }
        Ok(binding)
    }

    async fn ensure_terminal_monitor(
        &self,
        binding: HandleDispatchEnvelope,
        manager: AsyncExecutionManager,
        output_cap_bytes: usize,
        dispatch_lock: Arc<Mutex<()>>,
    ) {
        let Ok(process_id) = parse_process_handle(&binding) else {
            return;
        };
        if !self.inner.terminal_monitors.lock().await.insert(process_id) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                match manager.snapshot(process_id, output_cap_bytes).await {
                    Ok(outcome) if outcome.snapshot.status != ProcessSnapshotStatus::Running => {
                        let envelope = match terminal_envelope(&binding, &outcome.snapshot) {
                            Ok(envelope) => envelope,
                            Err(err) => {
                                eprintln!(
                                    "cooldis process settlement {} encode failed: {err}",
                                    binding.dispatch_id
                                );
                                tokio::time::sleep(TERMINAL_MONITOR_INTERVAL).await;
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
                                        "cooldis process settlement {} could not release terminal entry: {err}",
                                        binding.dispatch_id
                                    );
                                    tokio::time::sleep(TERMINAL_MONITOR_INTERVAL).await;
                                    continue;
                                }
                                this.forget_settled_binding(&binding, process_id, &dispatch_lock)
                                    .await;
                                break;
                            }
                            Err(err) => eprintln!(
                                "cooldis process settlement {} retrying after ingress failure: {err}",
                                binding.dispatch_id
                            ),
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!(
                            "cooldis process settlement {} invariant violation: live registry entry unavailable before acknowledgement; retrying: {err}",
                            binding.dispatch_id
                        );
                    }
                }
                tokio::time::sleep(TERMINAL_MONITOR_INTERVAL).await;
            }
            this.inner
                .terminal_monitors
                .lock()
                .await
                .remove(&process_id);
        });
    }

    async fn deliver_setup_failure(&self, binding: &HandleDispatchEnvelope, reason: &str) {
        let envelope = setup_failure_envelope(binding, reason);
        let mut retry_interval = TERMINAL_MONITOR_INTERVAL;
        loop {
            match self
                .inner
                .ingress
                .submit_process_handle_envelope(envelope.clone())
                .await
            {
                Ok(()) => break,
                Err(err) => eprintln!(
                    "cooldis process setup failure {} retrying after ingress failure in {}ms: {err}",
                    binding.dispatch_id,
                    retry_interval.as_millis()
                ),
            }
            tokio::time::sleep(retry_interval).await;
            retry_interval = retry_interval
                .saturating_mul(2)
                .min(SETUP_FAILURE_MAX_RETRY_INTERVAL);
        }
    }

    async fn forget_settled_binding(
        &self,
        binding: &HandleDispatchEnvelope,
        process_id: CooldisProcessId,
        dispatch_lock: &Arc<Mutex<()>>,
    ) {
        self.inner.live_bindings.lock().await.remove(&process_id);
        let mut locks = self.inner.locks.lock().await;
        let owns_map_entry = locks
            .get(binding.dispatch_id.as_str())
            .is_some_and(|current| Arc::ptr_eq(current, dispatch_lock));
        if owns_map_entry {
            locks.remove(binding.dispatch_id.as_str());
        }
    }

    async fn fold_dispatch(
        &self,
        consumer: &ThreadCoordinates,
        dispatch_id: &DispatchId,
    ) -> CooldisResult<Option<HandleDispatchEnvelope>> {
        let events = self
            .inner
            .store
            .read_events(&control_stream_id(consumer), None)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        let mut found = None;
        for event in events
            .into_iter()
            .filter(|event| event.kind == EventKind::IoIngressReceived)
        {
            if event
                .payload
                .pointer("/content/payload/dispatch_id")
                .and_then(serde_json::Value::as_str)
                != Some(dispatch_id.as_str())
            {
                continue;
            }
            let witness = serde_json::from_value::<IoIngressReceivedPayload>(event.payload)
                .map_err(|err| CooldisError::History(format!("decode ingress witness: {err}")))?;
            let content = witness.content.ok_or_else(|| {
                CooldisError::History(format!(
                    "process dispatch {dispatch_id} witness omitted its fold content"
                ))
            })?;
            let content = serde_json::from_value::<IngressContent>(content).map_err(|err| {
                CooldisError::History(format!("decode process dispatch content: {err}"))
            })?;
            let IngressContent::Event { kind, payload } = content else {
                return Err(CooldisError::History(format!(
                    "process dispatch {dispatch_id} witness is not event content"
                )));
            };
            if kind != HANDLE_DISPATCH_CONTENT_KIND {
                return Err(CooldisError::History(format!(
                    "process dispatch {dispatch_id} witness has content kind {kind}"
                )));
            }
            let binding =
                serde_json::from_value::<HandleDispatchEnvelope>(payload).map_err(|err| {
                    CooldisError::History(format!("decode process dispatch envelope: {err}"))
                })?;
            if let Some(existing) = &found
                && existing != &binding
            {
                return Err(CooldisError::History(format!(
                    "process dispatch {dispatch_id} has conflicting durable witnesses"
                )));
            }
            found = Some(binding);
        }
        Ok(found)
    }
}

pub fn command_digest(bytes: &[u8]) -> String {
    cooldis_agent::contracts::sha256_hex(bytes)
}

fn validate_binding(
    binding: &HandleDispatchEnvelope,
    consumer: &ThreadCoordinates,
    dispatch_id: &DispatchId,
    command_digest: &str,
) -> CooldisResult<()> {
    if binding.consumer != *consumer
        || binding.dispatch_id != *dispatch_id
        || binding.command_digest != command_digest
        || binding.handle.kind != HandleKind::Process
    {
        return Err(CooldisError::RuntimeExecution(format!(
            "process dispatch {dispatch_id} retry does not match its durable request"
        )));
    }
    Ok(())
}

fn parse_process_handle(binding: &HandleDispatchEnvelope) -> CooldisResult<CooldisProcessId> {
    if binding.handle.kind != HandleKind::Process {
        return Err(CooldisError::History(format!(
            "dispatch {} is not bound to a process handle",
            binding.dispatch_id
        )));
    }
    binding.handle.id.parse().map_err(|err| {
        CooldisError::History(format!(
            "dispatch {} has invalid process handle: {err}",
            binding.dispatch_id
        ))
    })
}

fn dispatch_envelope(binding: &HandleDispatchEnvelope) -> CooldisResult<IngressEnvelope> {
    let mut envelope = IngressEnvelope::new(
        IoSource::new("cooldis.handle", "process"),
        IoConversation::new(
            format!("thread:{}", binding.consumer.thread_id),
            ConversationKind::System,
        ),
        IngressContent::Event {
            kind: HANDLE_DISPATCH_CONTENT_KIND.to_string(),
            payload: serde_json::to_value(binding).map_err(|err| {
                CooldisError::RuntimeExecution(format!("encode process dispatch: {err}"))
            })?,
        },
        now_ms(),
    )
    .with_dedupe_key(IoDedupeKey::new(
        HANDLE_DISPATCH_CONTENT_KIND,
        binding.dispatch_id.to_string(),
    ))
    .with_metadata("cooldis_route_id", HANDLE_DISPATCH_CONTENT_KIND)
    .with_metadata("cooldis_route_policy", "observe_only");
    envelope.id = deterministic_ingress_id("dispatch", &binding.dispatch_id);
    Ok(envelope)
}

fn terminal_envelope(
    binding: &HandleDispatchEnvelope,
    snapshot: &AsyncProcessSnapshot,
) -> CooldisResult<IngressEnvelope> {
    let (outcome, outcome_reason, retryable) = terminal_projection(snapshot)?;
    outcome_envelope(
        binding,
        HandleTerminalEnvelope {
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

fn setup_failure_envelope(binding: &HandleDispatchEnvelope, reason: &str) -> IngressEnvelope {
    outcome_envelope(
        binding,
        HandleTerminalEnvelope {
            dispatch_id: binding.dispatch_id.clone(),
            handle: binding.handle.clone(),
            outcome: HandleTerminalOutcome::Failed,
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

fn outcome_envelope(
    binding: &HandleDispatchEnvelope,
    terminal: HandleTerminalEnvelope,
) -> CooldisResult<IngressEnvelope> {
    let mut envelope = IngressEnvelope::new(
        IoSource::new("cooldis.handle", "process"),
        IoConversation::new(
            format!("thread:{}", binding.consumer.thread_id),
            ConversationKind::System,
        ),
        IngressContent::Event {
            kind: HANDLE_OUTCOME_CONTENT_KIND.to_string(),
            payload: serde_json::to_value(terminal).map_err(|err| {
                CooldisError::RuntimeExecution(format!("encode process settlement: {err}"))
            })?,
        },
        now_ms(),
    )
    .with_dedupe_key(IoDedupeKey::new(
        HANDLE_OUTCOME_CONTENT_KIND,
        binding.dispatch_id.to_string(),
    ))
    .with_metadata("cooldis_route_id", HANDLE_OUTCOME_CONTENT_KIND)
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
    snapshot: &AsyncProcessSnapshot,
) -> CooldisResult<(HandleTerminalOutcome, Option<String>, bool)> {
    let terminal = snapshot
        .events
        .iter()
        .rev()
        .find(|event| event.kind.is_terminal())
        .ok_or_else(|| {
            CooldisError::RuntimeExecution(format!(
                "terminal process snapshot {} has no terminal event",
                snapshot.label
            ))
        })?;
    Ok(terminal_kind_projection(&terminal.kind))
}

fn terminal_kind_projection(
    kind: &CooldisProcessEventKind,
) -> (HandleTerminalOutcome, Option<String>, bool) {
    match kind {
        CooldisProcessEventKind::Completed { status } if status.success => (
            HandleTerminalOutcome::Completed,
            Some("exit status 0".to_string()),
            false,
        ),
        CooldisProcessEventKind::Completed { status } => match status.code {
            Some(code) => (
                HandleTerminalOutcome::Failed,
                Some(format!("exit status {code}")),
                false,
            ),
            None => (
                HandleTerminalOutcome::Failed,
                Some("process terminated by signal; exit status unavailable".to_string()),
                true,
            ),
        },
        CooldisProcessEventKind::Failed { code, message } => (
            HandleTerminalOutcome::Failed,
            Some(format!("{code}: {message}")),
            true,
        ),
        CooldisProcessEventKind::TimedOut {
            timeout_ms,
            message,
        } => (
            HandleTerminalOutcome::Failed,
            Some(match timeout_ms {
                Some(ms) => format!("timed out after {ms}ms: {message}"),
                None => format!("timed out: {message}"),
            }),
            true,
        ),
        CooldisProcessEventKind::Cancelled { reason } => (
            HandleTerminalOutcome::Cancelled,
            Some(reason.clone()),
            false,
        ),
        _ => unreachable!("terminal event predicate and projection are exhaustive"),
    }
}

fn deterministic_ingress_id(stage: &str, dispatch_id: &DispatchId) -> String {
    let digest = cooldis_agent::contracts::sha256_hex(
        format!("{stage}:{}", dispatch_id.as_str()).as_bytes(),
    );
    let digest = digest.strip_prefix("sha256:").unwrap_or(&digest);
    format!("ing-handle-{}", &digest[..32])
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use cooldis_process::{
        CooldisProcessBackend, CooldisProcessExitStatus, CooldisProcessHandle, ExecutionDeadline,
        LiveProcessSpawn, LiveProcessStartRequest,
    };
    use tokio_util::sync::CancellationToken;

    struct RecordingIngress {
        store: Arc<dyn RuntimeStore>,
        consumer: ThreadCoordinates,
    }

    #[async_trait::async_trait]
    impl ProcessHandleIngressSink for RecordingIngress {
        async fn submit_process_handle_envelope(
            &self,
            envelope: IngressEnvelope,
        ) -> CooldisResult<()> {
            let IngressContent::Event { kind, .. } = &envelope.content else {
                return Err(CooldisError::RuntimeExecution(
                    "test process ingress requires event content".to_string(),
                ));
            };
            let record = crate::NewEventRecord::witnessed(
                self.consumer.clone(),
                EventKind::IoIngressReceived,
                serde_json::to_value(IoIngressReceivedPayload {
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
                .append_events(&control_stream_id(&self.consumer), vec![record])
                .await
                .map_err(|err| CooldisError::History(err.to_string()))?;
            Ok(())
        }
    }

    struct DelayedTimeoutBackend;

    #[async_trait::async_trait]
    impl LiveProcessBackend for DelayedTimeoutBackend {
        fn backend_kind(&self) -> CooldisProcessBackend {
            CooldisProcessBackend::Bridge
        }

        async fn start(
            &self,
            request: LiveProcessStartRequest,
            process: CooldisProcessHandle,
            cancellation: CancellationToken,
        ) -> cooldis_process::CooldisProcessResult<LiveProcessSpawn> {
            process.record(CooldisProcessEventKind::Started {
                command: Some("delayed timeout".to_string()),
            });
            let join = tokio::spawn(async move {
                cancellation.cancelled().await;
                tokio::time::sleep(Duration::from_millis(50)).await;
                process.record(CooldisProcessEventKind::TimedOut {
                    timeout_ms: Some(request.deadline.remaining().as_millis() as u64),
                    message: "execution deadline elapsed".to_string(),
                });
                Ok(())
            });
            Ok(LiveProcessSpawn { stdin: None, join })
        }
    }

    struct DelayedCompletionBackend {
        started: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl LiveProcessBackend for DelayedCompletionBackend {
        fn backend_kind(&self) -> CooldisProcessBackend {
            CooldisProcessBackend::Bridge
        }

        async fn start(
            &self,
            _request: LiveProcessStartRequest,
            process: CooldisProcessHandle,
            _cancellation: CancellationToken,
        ) -> cooldis_process::CooldisProcessResult<LiveProcessSpawn> {
            process.record(CooldisProcessEventKind::Started {
                command: Some("delayed completion".to_string()),
            });
            self.started.notify_one();
            let join = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                process.record(CooldisProcessEventKind::Completed {
                    status: CooldisProcessExitStatus::exited(0),
                });
                Ok(())
            });
            Ok(LiveProcessSpawn { stdin: None, join })
        }
    }

    async fn outcome_count(
        store: &Arc<dyn RuntimeStore>,
        consumer: &ThreadCoordinates,
        dispatch_id: &DispatchId,
    ) -> usize {
        store
            .read_events(&control_stream_id(consumer), None)
            .await
            .unwrap()
            .iter()
            .filter(|event| {
                event.kind == EventKind::IoIngressReceived
                    && event
                        .payload
                        .get("route_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(HANDLE_OUTCOME_CONTENT_KIND)
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
        let store: Arc<dyn RuntimeStore> = Arc::new(crate::InMemorySessionStore::new());
        let consumer = ThreadCoordinates::new("tenant", "user", "deadline-settlement");
        let dispatcher = ProcessHandleDispatcher::new(
            Arc::clone(&store),
            Arc::new(RecordingIngress {
                store: Arc::clone(&store),
                consumer: consumer.clone(),
            }),
        );
        let manager = AsyncExecutionManager::default();
        let backend: Arc<dyn LiveProcessBackend> = Arc::new(DelayedTimeoutBackend);
        let dispatch_id = DispatchId::new("deadline-dispatch-a");

        dispatcher
            .dispatch_start(
                &consumer,
                dispatch_id.clone(),
                command_digest(b"process-a"),
                manager.clone(),
                Arc::clone(&backend),
                AsyncProcessStartRequest::virtual_bash_script("process-a")
                    .with_deadline(ExecutionDeadline::from_now(Duration::from_millis(5)))
                    .with_yield_time(Duration::ZERO),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        dispatcher
            .dispatch_start(
                &consumer,
                DispatchId::new("deadline-dispatch-b"),
                command_digest(b"process-b"),
                manager,
                backend,
                AsyncProcessStartRequest::virtual_bash_script("process-b")
                    .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(1)))
                    .with_yield_time(Duration::ZERO),
            )
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if outcome_count(&store, &consumer, &dispatch_id).await == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
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

        tokio::time::sleep(Duration::from_millis(75)).await;
        assert_eq!(outcome_count(&store, &consumer, &dispatch_id).await, 1);
    }

    #[tokio::test]
    async fn caller_cancellation_after_backend_start_does_not_cancel_settlement_monitor() {
        let store: Arc<dyn RuntimeStore> = Arc::new(crate::InMemorySessionStore::new());
        let consumer = ThreadCoordinates::new("tenant", "user", "cancelled-dispatch");
        let dispatcher = ProcessHandleDispatcher::new(
            Arc::clone(&store),
            Arc::new(RecordingIngress {
                store: Arc::clone(&store),
                consumer: consumer.clone(),
            }),
        );
        let manager = AsyncExecutionManager::default();
        let started = Arc::new(tokio::sync::Notify::new());
        let started_wait = started.notified();
        let backend: Arc<dyn LiveProcessBackend> = Arc::new(DelayedCompletionBackend {
            started: Arc::clone(&started),
        });
        let dispatch_id = DispatchId::new("cancelled-after-start");
        let task_dispatcher = dispatcher.clone();
        let task_consumer = consumer.clone();
        let task_dispatch_id = dispatch_id.clone();
        let task = tokio::spawn(async move {
            task_dispatcher
                .dispatch_start(
                    &task_consumer,
                    task_dispatch_id,
                    command_digest(b"cancelled-process"),
                    manager,
                    backend,
                    AsyncProcessStartRequest::virtual_bash_script("cancelled-process")
                        .with_deadline(ExecutionDeadline::from_now(Duration::from_secs(1)))
                        .with_yield_time(Duration::from_secs(1)),
                )
                .await
        });

        started_wait.await;
        task.abort();
        let _ = task.await;

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if outcome_count(&store, &consumer, &dispatch_id).await == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
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
            terminal_kind_projection(&CooldisProcessEventKind::Completed {
                status: CooldisProcessExitStatus::exited(0),
            }),
            (
                HandleTerminalOutcome::Completed,
                Some("exit status 0".to_string()),
                false,
            )
        );
        assert_eq!(
            terminal_kind_projection(&CooldisProcessEventKind::Completed {
                status: CooldisProcessExitStatus::exited(23),
            }),
            (
                HandleTerminalOutcome::Failed,
                Some("exit status 23".to_string()),
                false,
            )
        );
        assert_eq!(
            terminal_kind_projection(&CooldisProcessEventKind::Completed {
                status: CooldisProcessExitStatus {
                    code: None,
                    success: false,
                },
            }),
            (
                HandleTerminalOutcome::Failed,
                Some("process terminated by signal; exit status unavailable".to_string()),
                true,
            )
        );
        assert_eq!(
            terminal_kind_projection(&CooldisProcessEventKind::Cancelled {
                reason: "operator requested".to_string(),
            }),
            (
                HandleTerminalOutcome::Cancelled,
                Some("operator requested".to_string()),
                false,
            )
        );
    }

    #[test]
    fn setup_failure_is_retryable_process_outcome_without_usage() {
        let binding = HandleDispatchEnvelope {
            dispatch_id: DispatchId::new("setup-failure-dispatch"),
            handle: HandleId::process("018f0000-0000-7000-8000-000000000420"),
            consumer: ThreadCoordinates {
                tenant_id: "tenant".to_string(),
                user_id: "user".to_string(),
                session_id: "session".to_string(),
                thread_id: crate::ThreadId::parse_str("018f0000-0000-7000-8000-000000000419")
                    .unwrap(),
            },
            command_digest: "sha256:command".to_string(),
        };
        let envelope = setup_failure_envelope(&binding, "executable not found");

        assert_eq!(
            envelope.dedupe_key,
            Some(IoDedupeKey::new(
                HANDLE_OUTCOME_CONTENT_KIND,
                "setup-failure-dispatch"
            ))
        );
        let IngressContent::Event { payload, .. } = envelope.content else {
            panic!("setup failure must be event ingress");
        };
        let terminal: HandleTerminalEnvelope = serde_json::from_value(payload).unwrap();
        assert_eq!(terminal.handle.kind, HandleKind::Process);
        assert_eq!(terminal.outcome, HandleTerminalOutcome::Failed);
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
    store: Arc<dyn RuntimeStore>,
    consumer: ThreadCoordinates,
) -> ProcessHandleDispatcher {
    struct TestIngress {
        store: Arc<dyn RuntimeStore>,
        consumer: ThreadCoordinates,
    }

    #[async_trait::async_trait]
    impl ProcessHandleIngressSink for TestIngress {
        async fn submit_process_handle_envelope(
            &self,
            envelope: IngressEnvelope,
        ) -> CooldisResult<()> {
            let IngressContent::Event { kind, .. } = &envelope.content else {
                return Err(CooldisError::RuntimeExecution(
                    "test process ingress requires event content".to_string(),
                ));
            };
            if kind != HANDLE_DISPATCH_CONTENT_KIND {
                return Ok(());
            }
            let record = crate::NewEventRecord::witnessed(
                self.consumer.clone(),
                EventKind::IoIngressReceived,
                serde_json::to_value(IoIngressReceivedPayload {
                    route_id: Some(HANDLE_DISPATCH_CONTENT_KIND.to_string()),
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
                .append_events(&control_stream_id(&self.consumer), vec![record])
                .await
                .map_err(|err| CooldisError::History(err.to_string()))?;
            Ok(())
        }
    }

    ProcessHandleDispatcher::new(
        Arc::clone(&store),
        Arc::new(TestIngress { store, consumer }),
    )
}
