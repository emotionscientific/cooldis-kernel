use crate::CompactionTrigger;
use crate::agent::manifest_bind::BoundCouplingSet;
use crate::kernel::admission::{
    AdmissionGateContext, HOST_SUBMIT_SURFACE, append_admission_decided,
};
use crate::kernel::control_decision::{TurnContinuationDecision, TurnContinuationDecisionRequest};
use crate::kernel::history::{InMemorySessionStore, RuntimeStore, SessionContext, ThreadBaseRef};
use cooldis_agent::CooldisAgentError;
use cooldis_operations::CooldisOperationsError;
use cooldis_process::CooldisProcessError;
use cooldis_vbash::CooldisVirtualBashError;
use cooldis_wasm::CooldisWasmError;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod context_read_plan;
mod kernel_control;
mod loop_continuation;
mod runtime_api;
mod runtime_events;
mod runtime_services;
mod runtime_utils;
mod thread_handle;
mod turn;

pub use kernel_control::{
    AgentProcessCheckpointReceipt, AgentProcessChildRef, AgentProcessChildrenReceipt,
    AgentProcessLifecycleReceipt, AgentProcessSpawnReceipt, AgentProcessStatusReceipt,
    AgentProcessSubmitReceipt, AgentProcessWaitReceipt, RuntimeKernelControl, ThreadSpawnWitness,
};
pub use loop_continuation::LoopContinuationReceipt;
use loop_continuation::{
    append_continuation_accepted_event, append_continuation_rejected_event,
    append_loop_turn_submitted_event, decide_continuation, existing_continuation_receipt,
    latest_turn_continue_request, turn_submitted_event,
};
pub use runtime_api::{
    AgentRuntime, AgentRuntimeFactory, RuntimeHostLifecycleSnapshot, RuntimeHostSnapshot,
    ThreadCheckpoint, ThreadCommand, ThreadEvent, ThreadLifecycleSink, ThreadSnapshot,
};
pub use runtime_events::{RuntimeEvent, RuntimeEventKind, emit_runtime_event};
pub use runtime_services::{RuntimeExecutionPolicy, RuntimeServices};
pub use turn::{TurnContent, TurnContext, TurnContextSnapshot, TurnInput};

pub use cooldis_runtime_contracts::{
    RuntimeApprovalDecision, RuntimeEventId, RuntimeModelRequestErrorClass,
    RuntimeModelRequestMode, RuntimeModelRequestPurpose, RuntimePermissionDecision,
    RuntimeTerminalState, RuntimeToolLogLevel, RuntimeUsage, ThreadCheckpointId, ThreadContext,
    ThreadCoordinates, ThreadId, ThreadInitiationSource, ThreadInteractionKind,
    ThreadLifecycleRecord, ThreadLifecycleStatus, ThreadLineage, ThreadScope, ThreadSignal,
    ThreadSignalId, ThreadSignalKind, ThreadSpawnAttribution, ThreadStatus, ThreadTopology,
    TurnBudget, TurnSubmissionMode,
};

pub type CooldisResult<T> = Result<T, CooldisError>;

pub const THREAD_BOUND_COUPLING_SET_METADATA: &str = "cooldis.agent.bound_coupling_set";
pub const THREAD_AGENT_MANIFEST_HASH_METADATA: &str = "cooldis.agent.manifest_hash";
pub const THREAD_SPAWN_GRANTED_METADATA: &str = "cooldis.thread_spawn.granted";
pub const THREAD_SPAWN_INPUTS_HASH_METADATA: &str = "cooldis.thread_spawn.inputs_hash";
pub const THREAD_OPERATION_REGISTRY_ROOT_METADATA: &str = "cooldis.agent.operation_registry_root";

#[derive(Debug, Error)]
pub enum CooldisError {
    #[error("tenant not found: {0}")]
    TenantNotFound(String),
    #[error("tenant already exists: {0}")]
    TenantAlreadyExists(String),
    #[error("thread not found: {0}")]
    ThreadNotFound(ThreadId),
    #[error("thread already exists: {0}")]
    ThreadAlreadyExists(ThreadId),
    #[error("parent thread not found: {0}")]
    ParentThreadNotFound(ThreadId),
    #[error("parent thread {parent_thread_id} belongs to {actual:?}, not {requested:?}")]
    ParentThreadScopeMismatch {
        parent_thread_id: ThreadId,
        requested: Box<ThreadScope>,
        actual: Box<ThreadScope>,
    },
    #[error("related thread not found: {0}")]
    RelatedThreadNotFound(ThreadId),
    #[error("related thread {thread_id} belongs to {actual:?}, not {requested:?}")]
    RelatedThreadScopeMismatch {
        thread_id: ThreadId,
        requested: Box<ThreadScope>,
        actual: Box<ThreadScope>,
    },
    #[error("thread {thread_id} belongs to {actual:?}, not {requested:?}")]
    ThreadScopeMismatch {
        thread_id: ThreadId,
        requested: Box<ThreadScope>,
        actual: Box<ThreadScope>,
    },
    #[error("invalid thread topology: {0}")]
    ThreadTopologyInvalid(String),
    #[error("thread command channel closed: {0}")]
    ThreadClosed(ThreadId),
    #[error("runtime factory failed: {0}")]
    RuntimeFactory(String),
    #[error("runtime execution failed: {0}")]
    RuntimeExecution(String),
    #[error("history store failed: {0}")]
    History(String),
    #[error("thread {thread_id} policy violation {code}: {message}")]
    ThreadPolicyViolation {
        thread_id: ThreadId,
        code: &'static str,
        message: String,
    },
    #[error("lifecycle operation {operation} is not supported yet: {reason}")]
    LifecycleUnsupported {
        operation: &'static str,
        reason: String,
    },
}

impl From<CooldisProcessError> for CooldisError {
    fn from(err: CooldisProcessError) -> Self {
        CooldisError::RuntimeExecution(err.to_string())
    }
}

impl From<CooldisVirtualBashError> for CooldisError {
    fn from(err: CooldisVirtualBashError) -> Self {
        match err {
            CooldisVirtualBashError::RuntimeExecution(message) => {
                CooldisError::RuntimeExecution(message)
            }
            CooldisVirtualBashError::RuntimeFactory(message) => {
                CooldisError::RuntimeFactory(message)
            }
        }
    }
}

impl From<CooldisAgentError> for CooldisError {
    fn from(err: CooldisAgentError) -> Self {
        match err {
            CooldisAgentError::RuntimeExecution(message) => CooldisError::RuntimeExecution(message),
            CooldisAgentError::RuntimeFactory(message) => CooldisError::RuntimeFactory(message),
            CooldisAgentError::Operations(err) => err.into(),
        }
    }
}

impl From<CooldisOperationsError> for CooldisError {
    fn from(err: CooldisOperationsError) -> Self {
        match err {
            CooldisOperationsError::RuntimeExecution(message) => {
                CooldisError::RuntimeExecution(message)
            }
            CooldisOperationsError::RuntimeFactory(message) => {
                CooldisError::RuntimeFactory(message)
            }
        }
    }
}

impl From<CooldisWasmError> for CooldisError {
    fn from(err: CooldisWasmError) -> Self {
        match err {
            CooldisWasmError::RuntimeFactory(message) => CooldisError::RuntimeFactory(message),
            CooldisWasmError::RuntimeExecution(message) => CooldisError::RuntimeExecution(message),
        }
    }
}

fn bound_coupling_set_from_metadata(
    metadata: &BTreeMap<String, String>,
) -> CooldisResult<Option<BoundCouplingSet>> {
    let Some(raw) = metadata.get(THREAD_BOUND_COUPLING_SET_METADATA) else {
        return Ok(None);
    };
    serde_json::from_str::<BoundCouplingSet>(raw)
        .map(Some)
        .map_err(|err| {
            CooldisError::RuntimeFactory(format!("thread bound coupling set is invalid: {err}"))
        })
}

fn operation_registry_root_from_metadata(metadata: &BTreeMap<String, String>) -> Option<PathBuf> {
    metadata
        .get(THREAD_OPERATION_REGISTRY_ROOT_METADATA)
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
}

#[derive(Clone)]
pub struct RuntimeHost {
    inner: Arc<RuntimeHostInner>,
}

struct RuntimeHostInner {
    factory: Arc<dyn AgentRuntimeFactory>,
    runtime_store: Arc<dyn RuntimeStore>,
    execution_policy: RuntimeExecutionPolicy,
    threads: RwLock<HashMap<ThreadId, Arc<RuntimeThread>>>,
    checkpoints: Mutex<HashMap<ThreadCheckpointId, ThreadCheckpoint>>,
    lifecycle_sink: RwLock<Option<Arc<dyn ThreadLifecycleSink>>>,
}

struct RuntimeThread {
    context: ThreadContext,
    services: RuntimeServices,
    command_tx: mpsc::Sender<ThreadCommand>,
    command_capacity: usize,
    event_tx: broadcast::Sender<ThreadEvent>,
    status_tx: watch::Sender<ThreadStatus>,
    status_rx: watch::Receiver<ThreadStatus>,
    cancellation: CancellationToken,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    lifecycle: Mutex<ThreadLifecycleRecord>,
    checkpoints: Mutex<Vec<ThreadCheckpoint>>,
    turn_sequence: AtomicU64,
}

#[derive(Clone)]
pub struct RuntimeThreadHandle {
    thread: Arc<RuntimeThread>,
}

impl RuntimeHost {
    pub fn kernel_control(&self) -> RuntimeKernelControl {
        RuntimeKernelControl::new(Arc::downgrade(&self.inner))
    }

    pub fn new(factory: Arc<dyn AgentRuntimeFactory>) -> Self {
        Self::with_session_store(factory, Arc::new(InMemorySessionStore::new()))
    }

    pub fn with_policy(
        factory: Arc<dyn AgentRuntimeFactory>,
        execution_policy: RuntimeExecutionPolicy,
    ) -> Self {
        Self::with_session_store_and_policy(
            factory,
            Arc::new(InMemorySessionStore::new()),
            execution_policy,
        )
    }

    pub fn with_session_store(
        factory: Arc<dyn AgentRuntimeFactory>,
        runtime_store: Arc<dyn RuntimeStore>,
    ) -> Self {
        Self::with_session_store_and_policy(
            factory,
            runtime_store,
            RuntimeExecutionPolicy::default(),
        )
    }

    pub fn with_session_store_and_policy(
        factory: Arc<dyn AgentRuntimeFactory>,
        runtime_store: Arc<dyn RuntimeStore>,
        execution_policy: RuntimeExecutionPolicy,
    ) -> Self {
        Self {
            inner: Arc::new(RuntimeHostInner {
                factory,
                runtime_store,
                execution_policy,
                threads: RwLock::new(HashMap::new()),
                checkpoints: Mutex::new(HashMap::new()),
                lifecycle_sink: RwLock::new(None),
            }),
        }
    }

    pub async fn set_lifecycle_sink(&self, sink: Option<Arc<dyn ThreadLifecycleSink>>) {
        *self.inner.lifecycle_sink.write().await = sink;
    }

    async fn lifecycle_sink(&self) -> Option<Arc<dyn ThreadLifecycleSink>> {
        self.inner.lifecycle_sink.read().await.clone()
    }

    pub fn runtime_store(&self) -> Arc<dyn RuntimeStore> {
        Arc::clone(&self.inner.runtime_store)
    }

    pub fn execution_policy(&self) -> &RuntimeExecutionPolicy {
        &self.inner.execution_policy
    }

    pub async fn start_thread(
        &self,
        coordinates: ThreadCoordinates,
        topology: ThreadTopology,
    ) -> CooldisResult<RuntimeThreadHandle> {
        self.start_thread_with_topology_and_metadata(coordinates, topology, BTreeMap::new())
            .await
    }

    pub async fn start_thread_with_topology_and_metadata(
        &self,
        coordinates: ThreadCoordinates,
        topology: ThreadTopology,
        metadata: BTreeMap<String, String>,
    ) -> CooldisResult<RuntimeThreadHandle> {
        let parent_thread_id = topology.compatibility_parent_thread_id();
        if let Some(parent_thread_id) = parent_thread_id
            && let Some(max_child_threads) = self.inner.execution_policy.max_child_threads
        {
            let child_count = self.children_of(parent_thread_id).await.len();
            if child_count >= max_child_threads {
                if let Ok(parent) = self.get_thread(parent_thread_id).await {
                    parent.emit_runtime(RuntimeEventKind::PolicyRejected {
                        code: "max_child_threads".to_string(),
                        message: format!(
                            "parent thread already has {child_count} child thread(s); max is {max_child_threads}"
                        ),
                    });
                }
                return Err(CooldisError::ThreadPolicyViolation {
                    thread_id: parent_thread_id,
                    code: "max_child_threads",
                    message: format!(
                        "parent thread already has {child_count} child thread(s); max is {max_child_threads}"
                    ),
                });
            }
        }
        let context =
            ThreadContext::with_topology_and_metadata(coordinates, topology, metadata.clone());
        let thread_id = context.coordinates.thread_id;
        let runtime = self.inner.factory.build(&context).await?;
        let (command_tx, command_rx) = mpsc::channel(512);
        let command_capacity = command_tx.max_capacity();
        let (event_tx, _) = broadcast::channel(1024);
        let (status_tx, status_rx) = watch::channel(ThreadStatus::Starting);
        let runtime_status_rx = status_rx.clone();
        let runtime_run_status_tx = status_tx.clone();
        let runtime_exit_status_tx = status_tx.clone();
        let cancellation = CancellationToken::new();
        let runtime_cancellation = cancellation.clone();
        let runtime_events = event_tx.clone();
        let runtime_exit_events = event_tx.clone();
        let runtime_context = context.clone();
        let runtime_exit_coordinates = context.coordinates.clone();
        let runtime_parent_thread_id = context.parent_thread_id;
        let mut services = RuntimeServices::new(
            Arc::clone(&self.inner.runtime_store),
            self.inner.execution_policy.clone(),
        )
        .with_kernel_control(self.kernel_control());
        if let Some(coupling_set) = bound_coupling_set_from_metadata(&context.metadata)? {
            services = services.with_bound_coupling_set(coupling_set);
        }
        if let Some(root) = operation_registry_root_from_metadata(&context.metadata) {
            services = services.with_operation_registry_root(root);
        }
        let runtime_services = services.clone();

        let join_handle = tokio::spawn(async move {
            runtime
                .run(
                    runtime_context,
                    runtime_services,
                    command_rx,
                    runtime_events,
                    runtime_run_status_tx,
                    runtime_cancellation,
                )
                .await;
            let latest_status = *runtime_status_rx.borrow();
            if !matches!(latest_status, ThreadStatus::Stopped | ThreadStatus::Failed) {
                let _ = runtime_exit_status_tx.send(ThreadStatus::Failed);
                emit_runtime_event(
                    &runtime_exit_events,
                    &runtime_exit_coordinates,
                    RuntimeEventKind::Recovery {
                        action: "mark_failed".to_string(),
                        reason: "runtime exited without a terminal status".to_string(),
                    },
                );
                emit_runtime_event(
                    &runtime_exit_events,
                    &runtime_exit_coordinates,
                    RuntimeEventKind::Failed {
                        code: "runtime_exited".to_string(),
                        message: "runtime exited without a terminal status".to_string(),
                    },
                );
            }
        });
        let lifecycle =
            ThreadLifecycleRecord::new(&context, ThreadLifecycleStatus::Starting, metadata);
        let thread = Arc::new(RuntimeThread {
            context,
            services,
            command_tx,
            command_capacity,
            event_tx,
            status_tx,
            status_rx,
            cancellation,
            join_handle: Mutex::new(Some(join_handle)),
            lifecycle: Mutex::new(lifecycle),
            checkpoints: Mutex::new(Vec::new()),
            turn_sequence: AtomicU64::new(0),
        });

        {
            let mut threads = self.inner.threads.write().await;
            if threads.contains_key(&thread_id) {
                thread.cancellation.cancel();
                return Err(CooldisError::ThreadAlreadyExists(thread_id));
            }
            threads.insert(thread_id, Arc::clone(&thread));
        }

        let handle = RuntimeThreadHandle {
            thread: Arc::clone(&thread),
        };
        if let Some(sink) = self.lifecycle_sink().await
            && let Err(err) = sink.thread_started(handle.clone()).await
        {
            thread.cancellation.cancel();
            self.inner.threads.write().await.remove(&thread_id);
            return Err(err);
        }

        if let Some(parent_thread_id) = runtime_parent_thread_id {
            if let Ok(parent) = self.get_thread(parent_thread_id).await {
                parent.emit_runtime(RuntimeEventKind::SubthreadStarted {
                    child_thread_id: thread_id,
                });
            }
        }

        Ok(handle)
    }

    pub async fn get_thread(&self, thread_id: ThreadId) -> CooldisResult<RuntimeThreadHandle> {
        let threads = self.inner.threads.read().await;
        let thread = threads
            .get(&thread_id)
            .cloned()
            .ok_or(CooldisError::ThreadNotFound(thread_id))?;
        Ok(RuntimeThreadHandle { thread })
    }

    pub async fn submit(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> CooldisResult<()> {
        self.submit_turn_with_mode(
            thread_id,
            turn_id,
            TurnInput::text(input.into()),
            TurnSubmissionMode::Queue,
        )
        .await
    }

    pub async fn submit_turn(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: TurnInput,
    ) -> CooldisResult<()> {
        self.submit_turn_with_mode(thread_id, turn_id, input, TurnSubmissionMode::Queue)
            .await
    }

    pub async fn submit_with_mode(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
        mode: TurnSubmissionMode,
    ) -> CooldisResult<()> {
        self.submit_turn_with_mode(thread_id, turn_id, TurnInput::text(input.into()), mode)
            .await
    }

    pub async fn steer(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> CooldisResult<()> {
        self.submit_with_mode(thread_id, turn_id, input, TurnSubmissionMode::Steer)
            .await
    }

    pub async fn interrupt_with(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> CooldisResult<()> {
        self.submit_with_mode(thread_id, turn_id, input, TurnSubmissionMode::Interrupt)
            .await
    }

    pub async fn resume_tool_call(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        call_id: impl Into<String>,
    ) -> CooldisResult<()> {
        let thread = self.get_thread(thread_id).await?;
        thread
            .send(ThreadCommand::ResumeToolCall {
                turn_id: turn_id.into(),
                call_id: call_id.into(),
            })
            .await
    }

    pub async fn continue_turn_if_requested(
        &self,
        thread_id: ThreadId,
        loop_id: impl Into<String>,
        parent_turn_id: impl Into<String>,
        next_turn_id: impl Into<String>,
        now_ms: i64,
        completed_continuations: u32,
    ) -> CooldisResult<LoopContinuationReceipt> {
        let loop_id = loop_id.into();
        let parent_turn_id = parent_turn_id.into();
        let next_turn_id = next_turn_id.into();
        let thread = self.get_thread(thread_id).await?;
        let coordinates = thread.context().coordinates.clone();
        let Some((request_event, request_payload)) = latest_turn_continue_request(
            thread.thread.services.runtime_store().as_ref(),
            &coordinates,
            &loop_id,
            &parent_turn_id,
        )
        .await?
        else {
            return Ok(LoopContinuationReceipt::NoRequest);
        };
        if let Some(receipt) = existing_continuation_receipt(
            thread.thread.services.runtime_store().as_ref(),
            &coordinates,
            &request_payload.subject,
            &request_payload.snapshot_id,
        )
        .await?
        {
            if let LoopContinuationReceipt::Accepted {
                next_turn_id,
                accepted_event_id,
                ..
            } = &receipt
                && turn_submitted_event(
                    thread.thread.services.runtime_store().as_ref(),
                    &coordinates,
                    next_turn_id,
                )
                .await?
                .is_none()
            {
                append_loop_turn_submitted_event(
                    thread.thread.services.runtime_store().as_ref(),
                    &coordinates,
                    next_turn_id,
                    *accepted_event_id,
                )
                .await?;
                self.submit_turn_with_admission(
                    thread_id,
                    next_turn_id.clone(),
                    TurnInput::text(request_payload.next_turn_input),
                    TurnSubmissionMode::Queue,
                    None,
                )
                .await?;
            }
            return Ok(receipt);
        }
        let source_event_id = request_event
            .provenance
            .source_event_ids
            .first()
            .copied()
            .unwrap_or(request_event.id);
        match decide_continuation(
            thread.thread.services.runtime_store().as_ref(),
            TurnContinuationDecisionRequest {
                coordinates: coordinates.clone(),
                subject: request_payload.subject.clone(),
                snapshot_id: request_payload.snapshot_id.clone(),
                request_event_id: source_event_id,
                now_ms,
                completed_continuations,
            },
        )
        .await?
        {
            TurnContinuationDecision::NoRequest => Ok(LoopContinuationReceipt::NoRequest),
            TurnContinuationDecision::Accept {
                consumed_request_id,
                mandate_id,
                next_turn_input,
            } => {
                let accepted = append_continuation_accepted_event(
                    thread.thread.services.runtime_store().as_ref(),
                    &coordinates,
                    &request_payload.subject,
                    &request_payload.snapshot_id,
                    &mandate_id,
                    &next_turn_id,
                    consumed_request_id,
                )
                .await?;
                append_loop_turn_submitted_event(
                    thread.thread.services.runtime_store().as_ref(),
                    &coordinates,
                    &next_turn_id,
                    accepted.id,
                )
                .await?;
                self.submit_turn_with_admission(
                    thread_id,
                    next_turn_id.clone(),
                    TurnInput::text(next_turn_input),
                    TurnSubmissionMode::Queue,
                    None,
                )
                .await?;
                Ok(LoopContinuationReceipt::Accepted {
                    loop_id,
                    parent_turn_id,
                    next_turn_id,
                    accepted_event_id: accepted.id,
                })
            }
            TurnContinuationDecision::Reject {
                consumed_request_id,
                reason,
                ..
            } => {
                let rejected = append_continuation_rejected_event(
                    thread.thread.services.runtime_store().as_ref(),
                    &coordinates,
                    &request_payload.subject,
                    &request_payload.snapshot_id,
                    &reason,
                    consumed_request_id.unwrap_or(request_event.id),
                )
                .await?;
                Ok(LoopContinuationReceipt::Rejected {
                    loop_id,
                    parent_turn_id,
                    reason,
                    rejected_event_id: rejected.id,
                })
            }
        }
    }

    pub async fn submit_turn_with_mode(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: TurnInput,
        mode: TurnSubmissionMode,
    ) -> CooldisResult<()> {
        let admission = AdmissionGateContext::surface_default(HOST_SUBMIT_SURFACE, Vec::new())?;
        self.submit_turn_with_admission(thread_id, turn_id, input, mode, Some(admission))
            .await
    }

    pub(crate) async fn submit_turn_with_admission(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        input: TurnInput,
        mode: TurnSubmissionMode,
        admission: Option<AdmissionGateContext>,
    ) -> CooldisResult<()> {
        let turn_id = turn_id.into();
        let thread = self.get_thread(thread_id).await?;
        if mode == TurnSubmissionMode::Queue
            && let Some(max_pending_inputs) = self.inner.execution_policy.max_pending_inputs
        {
            let queued_commands = thread.queued_command_count();
            if queued_commands >= max_pending_inputs {
                let message = format!(
                    "thread has {queued_commands} queued command(s); max pending input count is {max_pending_inputs}"
                );
                thread.emit_runtime(RuntimeEventKind::PolicyRejected {
                    code: "max_pending_inputs".to_string(),
                    message: message.clone(),
                });
                return Err(CooldisError::ThreadPolicyViolation {
                    thread_id,
                    code: "max_pending_inputs",
                    message,
                });
            }
        }
        let command_permit = thread.reserve_command().await?;
        if let Some(admission) = admission {
            append_admission_decided(&thread, admission).await?;
        }
        let turn_sequence = thread.next_turn_sequence();
        command_permit.send(ThreadCommand::Submit {
            turn_id: turn_id.clone(),
            input,
            mode,
        });
        self.spawn_turn_timeout_watchdog(thread.clone(), turn_sequence);
        thread
            .record_signal(ThreadSignal::user_submit(
                &thread.context().coordinates,
                turn_id,
                mode,
            ))
            .await;
        Ok(())
    }

    pub async fn compact_thread(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        summary: Option<String>,
    ) -> CooldisResult<()> {
        let thread = self.get_thread(thread_id).await?;
        thread
            .send(ThreadCommand::Compact {
                turn_id: turn_id.into(),
                trigger: CompactionTrigger::Manual,
                summary,
            })
            .await?;
        Ok(())
    }

    pub async fn cancel(
        &self,
        thread_id: ThreadId,
        reason: impl Into<String>,
    ) -> CooldisResult<()> {
        let reason = reason.into();
        let thread = self.get_thread(thread_id).await?;
        thread.emit_runtime(RuntimeEventKind::Recovery {
            action: "cancel_requested".to_string(),
            reason: reason.clone(),
        });
        thread
            .send(ThreadCommand::Cancel {
                reason: reason.clone(),
            })
            .await?;
        thread
            .record_signal(ThreadSignal::interrupt_cancel(
                &thread.context().coordinates,
                reason,
            ))
            .await;
        self.wait_for_cancel_grace(&thread).await?;
        Ok(())
    }

    pub async fn shutdown_thread(&self, thread_id: ThreadId) -> CooldisResult<()> {
        let thread = self.get_thread(thread_id).await?;
        let parent_thread_id = thread.context().parent_thread_id;
        thread.emit_runtime(RuntimeEventKind::Recovery {
            action: "shutdown_requested".to_string(),
            reason: "shutdown_thread".to_string(),
        });
        match thread.send(ThreadCommand::Shutdown).await {
            Ok(()) => {
                thread
                    .record_signal(ThreadSignal::shutdown(&thread.context().coordinates))
                    .await;
            }
            Err(CooldisError::ThreadClosed(_)) => {
                thread.thread.cancellation.cancel();
            }
            Err(err) => return Err(err),
        }
        let timed_out = self.wait_for_shutdown(&thread).await?;
        self.inner.threads.write().await.remove(&thread_id);
        if let Some(parent_thread_id) = parent_thread_id {
            if let Ok(parent) = self.get_thread(parent_thread_id).await {
                parent.emit_runtime(RuntimeEventKind::SubthreadFinished {
                    child_thread_id: thread_id,
                    status: if timed_out {
                        ThreadLifecycleStatus::Failed
                    } else {
                        ThreadLifecycleStatus::Stopped
                    },
                });
            }
        }
        Ok(())
    }

    pub async fn session_context(&self, thread_id: ThreadId) -> CooldisResult<SessionContext> {
        self.get_thread(thread_id).await?.session_context().await
    }

    pub async fn shutdown_all(&self) -> CooldisResult<Vec<ThreadId>> {
        let thread_ids = {
            let threads = self.inner.threads.read().await;
            threads.keys().copied().collect::<Vec<_>>()
        };
        let mut stopped = Vec::with_capacity(thread_ids.len());
        for thread_id in thread_ids {
            self.shutdown_thread(thread_id).await?;
            stopped.push(thread_id);
        }
        stopped.sort_by_key(std::string::ToString::to_string);
        Ok(stopped)
    }

    fn spawn_turn_timeout_watchdog(&self, thread: RuntimeThreadHandle, turn_sequence: u64) {
        let Some(timeout_ms) = self.inner.execution_policy.turn_timeout_ms else {
            return;
        };
        let cancel_grace_timeout_ms = self.inner.execution_policy.cancel_grace_timeout_ms;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
            if thread.current_turn_sequence() != turn_sequence
                || thread.status() != ThreadStatus::Running
            {
                return;
            }
            thread.emit_runtime(RuntimeEventKind::Timeout {
                operation: "turn".to_string(),
                timeout_ms,
            });
            thread.emit_runtime(RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::TimedOut,
            });
            let reason = format!("turn exceeded {timeout_ms}ms timeout");
            let _ = thread
                .send(ThreadCommand::Cancel {
                    reason: reason.clone(),
                })
                .await;
            thread
                .record_signal(ThreadSignal::interrupt_cancel(
                    &thread.context().coordinates,
                    reason.clone(),
                ))
                .await;
            if let Some(cancel_timeout_ms) = cancel_grace_timeout_ms {
                tokio::time::sleep(Duration::from_millis(cancel_timeout_ms)).await;
                if thread.current_turn_sequence() == turn_sequence
                    && matches!(
                        thread.status(),
                        ThreadStatus::Running | ThreadStatus::Cancelling
                    )
                {
                    thread.emit_runtime(RuntimeEventKind::Timeout {
                        operation: "cancel".to_string(),
                        timeout_ms: cancel_timeout_ms,
                    });
                    thread.emit_runtime(RuntimeEventKind::Recovery {
                        action: "abort_runtime".to_string(),
                        reason: "cancel grace timeout elapsed after turn timeout".to_string(),
                    });
                    thread.set_status(ThreadStatus::Failed);
                    thread.emit_runtime(RuntimeEventKind::Failed {
                        code: "cancel_timeout".to_string(),
                        message: "runtime did not cancel within grace timeout".to_string(),
                    });
                    thread.abort().await;
                }
            }
        });
    }

    async fn wait_for_cancel_grace(&self, thread: &RuntimeThreadHandle) -> CooldisResult<()> {
        let Some(timeout_ms) = self.inner.execution_policy.cancel_grace_timeout_ms else {
            return Ok(());
        };
        let completed = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
            loop {
                if !matches!(
                    thread.status(),
                    ThreadStatus::Running | ThreadStatus::Cancelling
                ) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        if completed {
            thread.emit_runtime(RuntimeEventKind::Recovery {
                action: "cancel_completed".to_string(),
                reason: "runtime returned to a recoverable state".to_string(),
            });
            return Ok(());
        }
        thread.emit_runtime(RuntimeEventKind::Timeout {
            operation: "cancel".to_string(),
            timeout_ms,
        });
        thread.emit_runtime(RuntimeEventKind::Recovery {
            action: "abort_runtime".to_string(),
            reason: "cancel grace timeout elapsed".to_string(),
        });
        thread.set_status(ThreadStatus::Failed);
        thread.emit_runtime(RuntimeEventKind::Failed {
            code: "cancel_timeout".to_string(),
            message: "runtime did not cancel within grace timeout".to_string(),
        });
        thread.abort().await;
        Err(CooldisError::ThreadPolicyViolation {
            thread_id: thread.context().coordinates.thread_id,
            code: "cancel_timeout",
            message: "runtime did not cancel within grace timeout".to_string(),
        })
    }

    async fn wait_for_shutdown(&self, thread: &RuntimeThreadHandle) -> CooldisResult<bool> {
        let Some(timeout_ms) = self.inner.execution_policy.shutdown_grace_timeout_ms else {
            thread.wait().await;
            thread.emit_runtime(RuntimeEventKind::Recovery {
                action: "shutdown_completed".to_string(),
                reason: "runtime stopped".to_string(),
            });
            return Ok(false);
        };
        if thread
            .wait_timeout_or_abort(Duration::from_millis(timeout_ms))
            .await
        {
            thread.emit_runtime(RuntimeEventKind::Recovery {
                action: "shutdown_completed".to_string(),
                reason: "runtime stopped".to_string(),
            });
            return Ok(false);
        }
        thread.emit_runtime(RuntimeEventKind::Timeout {
            operation: "shutdown".to_string(),
            timeout_ms,
        });
        thread.emit_runtime(RuntimeEventKind::Recovery {
            action: "abort_runtime".to_string(),
            reason: "shutdown grace timeout elapsed".to_string(),
        });
        thread.thread.cancellation.cancel();
        thread.set_status(ThreadStatus::Failed);
        thread.abort().await;
        Ok(true)
    }

    pub async fn children_of(&self, parent_thread_id: ThreadId) -> Vec<RuntimeThreadHandle> {
        let threads = self.inner.threads.read().await;
        threads
            .values()
            .filter(|thread| thread.context.parent_thread_id == Some(parent_thread_id))
            .cloned()
            .map(|thread| RuntimeThreadHandle { thread })
            .collect()
    }

    pub async fn children_of_at(
        &self,
        parent_coordinates: &ThreadCoordinates,
    ) -> CooldisResult<Vec<RuntimeThreadHandle>> {
        let parent = self.get_thread(parent_coordinates.thread_id).await?;
        let requested_scope = parent_coordinates.scope();
        let actual_scope = parent.context().coordinates.scope();
        if requested_scope != actual_scope {
            return Err(CooldisError::ThreadScopeMismatch {
                thread_id: parent_coordinates.thread_id,
                requested: Box::new(requested_scope),
                actual: Box::new(actual_scope),
            });
        }
        Ok(self.children_of(parent_coordinates.thread_id).await)
    }

    pub async fn create_checkpoint(
        &self,
        thread_id: ThreadId,
        parent_checkpoint_id: Option<ThreadCheckpointId>,
        label: Option<String>,
        metadata: BTreeMap<String, String>,
    ) -> CooldisResult<ThreadCheckpoint> {
        let checkpoint = self
            .get_thread(thread_id)
            .await?
            .create_checkpoint(parent_checkpoint_id, label, metadata)
            .await?;
        self.inner
            .checkpoints
            .lock()
            .await
            .insert(checkpoint.id, checkpoint.clone());
        Ok(checkpoint)
    }

    pub async fn resume_thread(
        &self,
        checkpoint_id: ThreadCheckpointId,
    ) -> CooldisResult<RuntimeThreadHandle> {
        let checkpoint = self
            .inner
            .checkpoints
            .lock()
            .await
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| CooldisError::LifecycleUnsupported {
                operation: "resume_thread",
                reason: format!("checkpoint {checkpoint_id} is not loaded in this host"),
            })?;
        self.resume_thread_from_checkpoint(checkpoint).await
    }

    pub async fn resume_thread_from_checkpoint(
        &self,
        checkpoint: ThreadCheckpoint,
    ) -> CooldisResult<RuntimeThreadHandle> {
        if self
            .inner
            .threads
            .read()
            .await
            .contains_key(&checkpoint.coordinates.thread_id)
        {
            return Err(CooldisError::ThreadAlreadyExists(
                checkpoint.coordinates.thread_id,
            ));
        }
        self.inner
            .checkpoints
            .lock()
            .await
            .insert(checkpoint.id, checkpoint.clone());
        self.inner
            .runtime_store
            .select_branch(&checkpoint.coordinates, checkpoint.active_entry_id)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        self.start_thread_with_topology_and_metadata(
            checkpoint.coordinates.clone(),
            ThreadTopology::root(),
            checkpoint.metadata.clone(),
        )
        .await
    }

    pub async fn fork_thread(
        &self,
        thread_id: ThreadId,
        checkpoint_id: Option<ThreadCheckpointId>,
    ) -> CooldisResult<RuntimeThreadHandle> {
        let checkpoint_id = checkpoint_id.ok_or_else(|| CooldisError::LifecycleUnsupported {
            operation: "fork_thread",
            reason: "fork requires an explicit checkpoint id".to_string(),
        })?;
        let checkpoint = self
            .inner
            .checkpoints
            .lock()
            .await
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| CooldisError::LifecycleUnsupported {
                operation: "fork_thread",
                reason: format!("checkpoint {checkpoint_id} is not loaded in this host"),
            })?;
        if checkpoint.coordinates.thread_id != thread_id {
            return Err(CooldisError::ThreadScopeMismatch {
                thread_id,
                requested: Box::new(ThreadScope {
                    tenant_id: checkpoint.coordinates.tenant_id.clone(),
                    user_id: checkpoint.coordinates.user_id.clone(),
                    session_id: checkpoint.coordinates.session_id.clone(),
                }),
                actual: Box::new(checkpoint.coordinates.scope()),
            });
        }
        self.fork_thread_from_checkpoint(checkpoint).await
    }

    pub async fn fork_thread_from_checkpoint(
        &self,
        checkpoint: ThreadCheckpoint,
    ) -> CooldisResult<RuntimeThreadHandle> {
        let fork_coordinates = ThreadCoordinates::new(
            checkpoint.coordinates.tenant_id.clone(),
            checkpoint.coordinates.user_id.clone(),
            checkpoint.coordinates.session_id.clone(),
        );
        self.inner
            .runtime_store
            .clone_branch(
                &checkpoint.coordinates,
                checkpoint.active_entry_id,
                &fork_coordinates,
            )
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        let mut metadata = checkpoint.metadata.clone();
        metadata.insert(
            "forked_from_thread_id".to_string(),
            checkpoint.coordinates.thread_id.to_string(),
        );
        metadata.insert(
            "forked_from_checkpoint_id".to_string(),
            checkpoint.id.to_string(),
        );
        self.start_thread_with_topology_and_metadata(
            fork_coordinates,
            ThreadTopology::branch_from(checkpoint.coordinates.thread_id, Some(checkpoint.id)),
            metadata,
        )
        .await
    }

    pub async fn checkpoint(
        &self,
        checkpoint_id: ThreadCheckpointId,
    ) -> CooldisResult<ThreadCheckpoint> {
        self.inner
            .checkpoints
            .lock()
            .await
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| CooldisError::LifecycleUnsupported {
                operation: "checkpoint",
                reason: format!("checkpoint {checkpoint_id} is not loaded in this host"),
            })
    }

    pub async fn fork_history_by_reference(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> CooldisResult<()> {
        self.inner
            .runtime_store
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))
    }

    pub async fn snapshot(&self) -> RuntimeHostSnapshot {
        let threads = self.inner.threads.read().await;
        let mut snapshots = Vec::with_capacity(threads.len());
        for thread in threads.values() {
            snapshots.push(ThreadSnapshot {
                context: thread.context.clone(),
                status: *thread.status_rx.borrow(),
            });
        }
        snapshots.sort_by_key(|snapshot| snapshot.context.coordinates.thread_id.to_string());
        RuntimeHostSnapshot { threads: snapshots }
    }

    pub async fn lifecycle_snapshot(&self) -> RuntimeHostLifecycleSnapshot {
        let threads = {
            let threads = self.inner.threads.read().await;
            threads
                .values()
                .cloned()
                .map(|thread| RuntimeThreadHandle { thread })
                .collect::<Vec<_>>()
        };
        let mut records = Vec::with_capacity(threads.len());
        for thread in threads {
            records.push(thread.lifecycle_record().await);
        }
        records.sort_by_key(|record| record.coordinates.thread_id.to_string());
        RuntimeHostLifecycleSnapshot { records }
    }
}

#[cfg(test)]
mod tests;
