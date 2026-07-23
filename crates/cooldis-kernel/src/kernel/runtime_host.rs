use crate::CompactionTrigger;
use crate::agent::manifest_bind::BoundCouplingSet;
use crate::kernel::admission::{AdmissionGateContext, HOST_SUBMIT_SURFACE};
use crate::kernel::control_decision::{TurnContinuationDecision, TurnContinuationDecisionRequest};
use crate::kernel::history::{
    EventKind, EventStreamId, InMemorySessionStore, RuntimeStore, SessionContext, SessionEntryKind,
    ThreadBaseRef,
};
use crate::kernel::process_handle_dispatch::ProcessHandleDispatcher;
use cooldis_agent::CooldisAgentError;
use cooldis_operations::CooldisOperationsError;
use cooldis_process::CooldisProcessError;
use cooldis_vbash::CooldisVirtualBashError;
use cooldis_wasm::CooldisWasmError;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, Semaphore, broadcast, mpsc, oneshot, watch};
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
    AgentRuntime, AgentRuntimeFactory, ProcessHandleIngressSink, RuntimeHostLifecycleSnapshot,
    RuntimeHostSnapshot, ThreadCheckpoint, ThreadCheckpointLineage, ThreadCommand, ThreadEvent,
    ThreadLifecycleSink, ThreadSnapshot,
};
pub use runtime_events::{RuntimeEvent, RuntimeEventKind, emit_runtime_event};
pub(crate) use runtime_services::append_thread_joined_first_wins;
pub use runtime_services::{RuntimeExecutionPolicy, RuntimeServices};
use turn::TurnWatchdogHandle;
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
    #[error("{0}")]
    RpcClient(String),
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
    #[error(
        "checkpoint {checkpoint_id} cannot resume thread {thread_id}: checkpoint resume only supports root threads; parent lineage {parent_thread_id} cannot be restored"
    )]
    CheckpointResumeRequiresRoot {
        checkpoint_id: ThreadCheckpointId,
        thread_id: ThreadId,
        parent_thread_id: ThreadId,
    },
    #[error(
        "checkpoint {checkpoint_id} cannot resume thread {thread_id}: root lineage was not recorded; recreate the checkpoint before resuming"
    )]
    CheckpointResumeLineageUnknown {
        checkpoint_id: ThreadCheckpointId,
        thread_id: ThreadId,
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

fn fork_child_context_is_compatible(
    context: &ThreadContext,
    coordinates: &ThreadCoordinates,
    parent_thread_id: ThreadId,
) -> bool {
    context.coordinates == *coordinates
        && context.topology.branch_parent_thread_id() == Some(parent_thread_id)
        && context.metadata.get("forked_from_thread_id") == Some(&parent_thread_id.to_string())
}

struct RuntimeHostInner {
    factory: Arc<dyn AgentRuntimeFactory>,
    runtime_store: Arc<dyn RuntimeStore>,
    execution_policy: RuntimeExecutionPolicy,
    threads: RwLock<HashMap<ThreadId, Arc<RuntimeThread>>>,
    thread_start_reservations: StdMutex<HashMap<ThreadId, ThreadStartReservationState>>,
    checkpoints: Mutex<HashMap<ThreadCheckpointId, ThreadCheckpoint>>,
    lifecycle_sink: RwLock<Option<Arc<dyn ThreadLifecycleSink>>>,
    process_handle_ingress: RwLock<Option<Arc<dyn ProcessHandleIngressSink>>>,
    process_handle_dispatcher: RwLock<Option<ProcessHandleDispatcher>>,
    remote_thread_executor:
        RwLock<Option<Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>>>,
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
    pending_input_slots: Option<Arc<Semaphore>>,
    turn_reservations: StdMutex<HashSet<String>>,
}

struct ThreadStartReservationState {
    context: ThreadContext,
    settled: watch::Sender<bool>,
}

struct ThreadStartReservation<'a> {
    reservations: &'a StdMutex<HashMap<ThreadId, ThreadStartReservationState>>,
    thread_id: ThreadId,
    settled: watch::Sender<bool>,
    committed: bool,
}

struct TurnIdReservation {
    thread: Arc<RuntimeThread>,
    turn_id: String,
    committed: bool,
}

impl Drop for TurnIdReservation {
    fn drop(&mut self) {
        if !self.committed {
            lock_unpoisoned(&self.thread.turn_reservations).remove(&self.turn_id);
        }
    }
}

impl ThreadStartReservation<'_> {
    fn commit(&mut self) {
        self.committed = true;
        let _ = self.settled.send(true);
    }
}

impl Drop for ThreadStartReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            lock_unpoisoned(self.reservations).remove(&self.thread_id);
            let _ = self.settled.send(true);
        }
    }
}

fn lock_unpoisoned<T>(mutex: &StdMutex<T>) -> StdMutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

#[derive(Clone)]
pub struct RuntimeThreadHandle {
    thread: Arc<RuntimeThread>,
}

pub(crate) struct ReservedTurnSubmission {
    host: RuntimeHost,
    thread: RuntimeThreadHandle,
    reservation: Option<TurnIdReservation>,
    command_permit: Option<mpsc::OwnedPermit<ThreadCommand>>,
    turn_id: String,
    input: Option<TurnInput>,
    mode: TurnSubmissionMode,
    turn_watchdog: Option<TurnWatchdogHandle>,
}

struct PublishedThreadStartGuard {
    host: RuntimeHost,
    thread: Arc<RuntimeThread>,
    armed: bool,
}

impl PublishedThreadStartGuard {
    fn new(host: RuntimeHost, thread: Arc<RuntimeThread>) -> Self {
        Self {
            host,
            thread,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PublishedThreadStartGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.thread.cancellation.cancel();
        let host = self.host.clone();
        let thread = Arc::clone(&self.thread);
        tokio::spawn(async move {
            RuntimeThreadHandle {
                thread: Arc::clone(&thread),
            }
            .abort()
            .await;
            host.remove_thread_if_current(&thread).await;
        });
    }
}

impl ReservedTurnSubmission {
    /// Publishes a submission after all fallible admission work has completed.
    pub(super) async fn submit_unchecked(self) -> bool {
        let Self {
            host,
            thread,
            mut reservation,
            command_permit,
            turn_id,
            input,
            mode,
            turn_watchdog,
        } = self;
        let (Some(command_permit), Some(input)) = (command_permit, input) else {
            return false;
        };
        let _ = command_permit.send(ThreadCommand::Submit {
            turn_id: turn_id.clone(),
            input,
            mode,
        });
        if let Some(reservation) = reservation.as_mut() {
            reservation.committed = true;
        }
        if let Some(turn_watchdog) = turn_watchdog {
            host.spawn_turn_timeout_watchdog(thread.clone(), turn_watchdog);
        }
        thread
            .record_signal(ThreadSignal::user_submit(
                &thread.context().coordinates,
                turn_id,
                mode,
            ))
            .await;
        true
    }
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
                thread_start_reservations: StdMutex::new(HashMap::new()),
                checkpoints: Mutex::new(HashMap::new()),
                lifecycle_sink: RwLock::new(None),
                process_handle_ingress: RwLock::new(None),
                process_handle_dispatcher: RwLock::new(None),
                remote_thread_executor: RwLock::new(None),
            }),
        }
    }

    pub async fn set_lifecycle_sink(&self, sink: Option<Arc<dyn ThreadLifecycleSink>>) {
        *self.inner.lifecycle_sink.write().await = sink;
    }

    pub(crate) async fn set_process_handle_dispatcher(
        &self,
        dispatcher: Option<ProcessHandleDispatcher>,
    ) {
        *self.inner.process_handle_dispatcher.write().await = dispatcher;
    }

    pub async fn set_process_handle_ingress(
        &self,
        sink: Option<Arc<dyn ProcessHandleIngressSink>>,
    ) {
        *self.inner.process_handle_ingress.write().await = sink;
    }

    async fn process_handle_ingress(&self) -> Option<Arc<dyn ProcessHandleIngressSink>> {
        self.inner.process_handle_ingress.read().await.clone()
    }

    async fn process_handle_dispatcher(&self) -> Option<ProcessHandleDispatcher> {
        self.inner.process_handle_dispatcher.read().await.clone()
    }

    pub async fn set_remote_thread_executor(
        &self,
        executor: Option<Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>>,
    ) {
        *self.inner.remote_thread_executor.write().await = executor;
    }

    pub(crate) async fn remote_thread_executor(
        &self,
    ) -> Option<Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>> {
        self.inner.remote_thread_executor.read().await.clone()
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
        self.start_thread_with_topology_and_metadata_inner(coordinates, topology, metadata, true)
            .await
    }

    pub(crate) async fn load_thread_with_topology_and_metadata(
        &self,
        coordinates: ThreadCoordinates,
        topology: ThreadTopology,
        metadata: BTreeMap<String, String>,
    ) -> CooldisResult<RuntimeThreadHandle> {
        self.start_thread_with_topology_and_metadata_inner(coordinates, topology, metadata, false)
            .await
    }

    async fn start_thread_with_topology_and_metadata_inner(
        &self,
        coordinates: ThreadCoordinates,
        topology: ThreadTopology,
        metadata: BTreeMap<String, String>,
        record_start_identity: bool,
    ) -> CooldisResult<RuntimeThreadHandle> {
        let context =
            ThreadContext::with_topology_and_metadata(coordinates, topology, metadata.clone());
        let start_reservation = self.reserve_thread_start(&context).await?;
        self.start_reserved_thread(
            context,
            metadata,
            start_reservation,
            record_start_identity,
            false,
            true,
        )
        .await
    }

    async fn reserve_thread_start<'a>(
        &'a self,
        context: &ThreadContext,
    ) -> CooldisResult<ThreadStartReservation<'a>> {
        let thread_id = context.coordinates.thread_id;
        let parent_thread_id = context.parent_thread_id;
        let threads = self.inner.threads.read().await;
        let mut reservations = lock_unpoisoned(&self.inner.thread_start_reservations);
        if threads.contains_key(&thread_id) || reservations.contains_key(&thread_id) {
            return Err(CooldisError::ThreadAlreadyExists(thread_id));
        }
        if let Some(parent_thread_id) = parent_thread_id
            && let Some(max_child_threads) = self.inner.execution_policy.max_child_threads
        {
            let child_count = threads
                .values()
                .filter(|thread| thread.context.parent_thread_id == Some(parent_thread_id))
                .count()
                + reservations
                    .values()
                    .filter(|reservation| {
                        reservation.context.parent_thread_id == Some(parent_thread_id)
                    })
                    .count();
            if child_count >= max_child_threads {
                let parent = threads.get(&parent_thread_id).cloned();
                let message = format!(
                    "parent thread already has {child_count} child thread(s); max is {max_child_threads}"
                );
                drop(reservations);
                drop(threads);
                if let Some(parent) = parent {
                    RuntimeThreadHandle { thread: parent }.emit_runtime(
                        RuntimeEventKind::PolicyRejected {
                            code: "max_child_threads".to_string(),
                            message: message.clone(),
                        },
                    );
                }
                return Err(CooldisError::ThreadPolicyViolation {
                    thread_id: parent_thread_id,
                    code: "max_child_threads",
                    message,
                });
            }
        }
        let (settled, _) = watch::channel(false);
        reservations.insert(
            thread_id,
            ThreadStartReservationState {
                context: context.clone(),
                settled: settled.clone(),
            },
        );
        Ok(ThreadStartReservation {
            reservations: &self.inner.thread_start_reservations,
            thread_id,
            settled,
            committed: false,
        })
    }

    /// Waits until an in-flight start either finishes publishing or releases
    /// its reservation after failure.
    pub(crate) async fn wait_for_thread_start_reservation(&self, thread_id: ThreadId) {
        let settled = lock_unpoisoned(&self.inner.thread_start_reservations)
            .get(&thread_id)
            .map(|reservation| reservation.settled.subscribe());
        let Some(mut settled) = settled else {
            return;
        };
        if *settled.borrow() {
            return;
        }
        let _ = settled.changed().await;
    }

    async fn start_reserved_thread(
        &self,
        context: ThreadContext,
        metadata: BTreeMap<String, String>,
        mut start_reservation: ThreadStartReservation<'_>,
        record_start_identity: bool,
        reconcile_start_identity_append: bool,
        notify_lifecycle_sink: bool,
    ) -> CooldisResult<RuntimeThreadHandle> {
        let thread_id = context.coordinates.thread_id;
        let runtime = self.inner.factory.build(&context).await?;
        let command_channel_capacity = self
            .inner
            .execution_policy
            .max_pending_inputs
            .unwrap_or(512)
            .min(tokio::sync::Semaphore::MAX_PERMITS)
            .max(512);
        let (command_tx, command_rx) = mpsc::channel(command_channel_capacity);
        let command_capacity = command_tx.max_capacity();
        let pending_input_slots = self
            .inner
            .execution_policy
            .max_pending_inputs
            .map(|limit| Arc::new(Semaphore::new(limit.min(Semaphore::MAX_PERMITS))));
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
        .with_kernel_control(self.kernel_control())
        .with_process_handle_ingress(self.process_handle_ingress().await)
        .with_process_handle_dispatcher(self.process_handle_dispatcher().await);
        if let Some(coupling_set) = bound_coupling_set_from_metadata(&context.metadata)? {
            services = services.with_bound_coupling_set(coupling_set);
        }
        if let Some(root) = operation_registry_root_from_metadata(&context.metadata) {
            services = services.with_operation_registry_root(root);
        }
        let runtime_services = services.clone();

        let (runtime_start_tx, runtime_start_rx) = oneshot::channel();
        let join_handle = tokio::spawn(async move {
            if runtime_start_rx.await.is_err() {
                return;
            }
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
            pending_input_slots,
            turn_reservations: StdMutex::new(HashSet::new()),
        });

        {
            let mut threads = self.inner.threads.write().await;
            let mut reservations = lock_unpoisoned(&self.inner.thread_start_reservations);
            let reservation = reservations.remove(&thread_id);
            debug_assert!(reservation.is_some());
            let previous = threads.insert(thread_id, Arc::clone(&thread));
            debug_assert!(previous.is_none());
            start_reservation.commit();
        }
        let mut published_start = PublishedThreadStartGuard::new(self.clone(), Arc::clone(&thread));

        let handle = RuntimeThreadHandle {
            thread: Arc::clone(&thread),
        };
        if record_start_identity {
            let start_identity = if reconcile_start_identity_append {
                handle
                    .record_thread_start_identity_with_reconciliation()
                    .await
            } else {
                handle.record_thread_start_identity().await.map(|_| ())
            };
            if let Err(err) = start_identity {
                thread.cancellation.cancel();
                handle.abort().await;
                self.remove_thread_if_current(&thread).await;
                published_start.disarm();
                return Err(err);
            }
        }
        let _ = runtime_start_tx.send(());
        if notify_lifecycle_sink
            && let Some(sink) = self.lifecycle_sink().await
            && let Err(err) = sink.thread_started(handle.clone()).await
        {
            thread.cancellation.cancel();
            handle.abort().await;
            self.remove_thread_if_current(&thread).await;
            published_start.disarm();
            return Err(err);
        }

        if let Some(parent_thread_id) = runtime_parent_thread_id {
            if let Ok(parent) = self.get_thread(parent_thread_id).await {
                parent.emit_runtime(RuntimeEventKind::SubthreadStarted {
                    child_thread_id: thread_id,
                });
            }
        }

        published_start.disarm();
        Ok(handle)
    }

    async fn remove_thread_if_current(&self, thread: &Arc<RuntimeThread>) -> bool {
        let thread_id = thread.context.coordinates.thread_id;
        let mut threads = self.inner.threads.write().await;
        if threads
            .get(&thread_id)
            .is_some_and(|current| Arc::ptr_eq(current, thread))
        {
            threads.remove(&thread_id);
            true
        } else {
            false
        }
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
                crate::kernel::admission::submit_turn(
                    self,
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
                crate::kernel::admission::submit_turn(
                    self,
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
        crate::kernel::admission::submit_turn(
            self,
            thread_id,
            turn_id,
            input,
            mode,
            Some(admission),
        )
        .await
    }

    pub(super) async fn reserve_turn_submission_at_choke_point(
        &self,
        thread_id: ThreadId,
        turn_id: impl Into<String>,
        mut input: TurnInput,
        mode: TurnSubmissionMode,
        admission: Option<AdmissionGateContext>,
    ) -> CooldisResult<ReservedTurnSubmission> {
        let turn_id = turn_id.into();
        let thread = self.get_thread(thread_id).await?;
        let duplicate = {
            let mut reservations = lock_unpoisoned(&thread.thread.turn_reservations);
            !reservations.insert(turn_id.clone())
        };
        if duplicate {
            return Ok(ReservedTurnSubmission {
                host: self.clone(),
                thread,
                reservation: None,
                command_permit: None,
                turn_id,
                input: None,
                mode,
                turn_watchdog: None,
            });
        }
        let reservation = TurnIdReservation {
            thread: Arc::clone(&thread.thread),
            turn_id: turn_id.clone(),
            committed: false,
        };
        if mode == TurnSubmissionMode::Queue
            && let Some(max_pending_inputs) = self.inner.execution_policy.max_pending_inputs
        {
            let pending_input_slots =
                thread.thread.pending_input_slots.as_ref().ok_or_else(|| {
                    CooldisError::RuntimeExecution(
                        "configured pending-input policy has no slot semaphore".to_string(),
                    )
                })?;
            let pending_input_permit = match Arc::clone(pending_input_slots).try_acquire_owned() {
                Ok(permit) => permit,
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    let effective_limit = max_pending_inputs.min(Semaphore::MAX_PERMITS);
                    let queued_commands =
                        effective_limit.saturating_sub(pending_input_slots.available_permits());
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
                Err(tokio::sync::TryAcquireError::Closed) => {
                    return Err(CooldisError::ThreadClosed(thread_id));
                }
            };
            input.set_pending_input_permit(pending_input_permit);
        }
        let command_permit = thread
            .thread
            .command_tx
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| CooldisError::ThreadClosed(thread_id))?;
        if let Some(admission) = admission {
            crate::kernel::admission::append_admission_decided(&thread, admission).await?;
        }
        let turn_watchdog = if self.inner.execution_policy.turn_timeout_ms.is_some() {
            Some(thread.thread.services.register_turn_watchdog(&mut input))
        } else {
            None
        };
        Ok(ReservedTurnSubmission {
            host: self.clone(),
            thread,
            reservation: Some(reservation),
            command_permit: Some(command_permit),
            turn_id,
            input: Some(input),
            mode,
            turn_watchdog,
        })
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
            .record_signal(ThreadSignal::interrupt_cancel(
                &thread.context().coordinates,
                reason.clone(),
            ))
            .await;
        if matches!(
            thread.status(),
            ThreadStatus::Starting
                | ThreadStatus::Idle
                | ThreadStatus::Stopped
                | ThreadStatus::Failed
        ) && thread.queued_command_count() == 0
        {
            return Ok(());
        }
        thread
            .send(ThreadCommand::Cancel {
                reason: reason.clone(),
            })
            .await?;
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
        let removed = self.remove_thread_if_current(&thread.thread).await;
        if removed && let Some(parent_thread_id) = parent_thread_id {
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

    /// Shuts down descendants before registered ancestors, with thread-id order
    /// breaking ties at the same topology depth.
    pub async fn shutdown_all(&self) -> CooldisResult<Vec<ThreadId>> {
        fn registered_depth(
            thread_id: ThreadId,
            parents: &HashMap<ThreadId, Option<ThreadId>>,
            path: &mut Vec<ThreadId>,
        ) -> usize {
            if path.contains(&thread_id) {
                return 0;
            }
            path.push(thread_id);
            let depth = parents
                .get(&thread_id)
                .copied()
                .flatten()
                .filter(|parent_thread_id| parents.contains_key(parent_thread_id))
                .map(|parent_thread_id| 1 + registered_depth(parent_thread_id, parents, path))
                .unwrap_or(0);
            path.pop();
            depth
        }

        let parents = {
            let threads = self.inner.threads.read().await;
            threads
                .iter()
                .map(|(thread_id, thread)| (*thread_id, thread.context.parent_thread_id))
                .collect::<HashMap<_, _>>()
        };
        let mut thread_ids = parents.keys().copied().collect::<Vec<_>>();
        thread_ids.sort_by(|left, right| {
            let left_depth = registered_depth(*left, &parents, &mut Vec::new());
            let right_depth = registered_depth(*right, &parents, &mut Vec::new());
            right_depth
                .cmp(&left_depth)
                .then_with(|| left.to_string().cmp(&right.to_string()))
        });
        let mut stopped = Vec::with_capacity(thread_ids.len());
        for thread_id in thread_ids {
            self.shutdown_thread(thread_id).await?;
            stopped.push(thread_id);
        }
        Ok(stopped)
    }

    fn spawn_turn_timeout_watchdog(
        &self,
        thread: RuntimeThreadHandle,
        mut watchdog: TurnWatchdogHandle,
    ) {
        let Some(timeout_ms) = self.inner.execution_policy.turn_timeout_ms else {
            return;
        };
        let cancel_grace_timeout_ms = self.inner.execution_policy.cancel_grace_timeout_ms;
        let watchdog_token_id = watchdog.id();
        let thread = Arc::downgrade(&thread.thread);
        tokio::spawn(async move {
            if !watchdog.wait_until_started().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
            let Some(thread) = thread.upgrade() else {
                return;
            };
            let thread = RuntimeThreadHandle { thread };
            if thread.status() != ThreadStatus::Running || !watchdog.try_timeout() {
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
            match thread.try_reserve_command() {
                Ok(command_permit) => {
                    command_permit.send(ThreadCommand::CancelTurn {
                        watchdog_token_id,
                        reason: reason.clone(),
                    });
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    if !watchdog.is_timed_out() || thread.status() != ThreadStatus::Running {
                        return;
                    }
                    thread.set_status(ThreadStatus::Cancelling);
                    thread.thread.cancellation.cancel();
                }
            }
            thread
                .record_signal(ThreadSignal::interrupt_cancel(
                    &thread.context().coordinates,
                    reason.clone(),
                ))
                .await;
            if let Some(cancel_timeout_ms) = cancel_grace_timeout_ms {
                tokio::time::sleep(Duration::from_millis(cancel_timeout_ms)).await;
                if watchdog.is_timed_out()
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

    /// Resumes only checkpoints explicitly recorded with root lineage; V1
    /// resume rejects parent or unknown lineage instead of flattening it.
    pub async fn resume_thread_from_checkpoint(
        &self,
        checkpoint: ThreadCheckpoint,
    ) -> CooldisResult<RuntimeThreadHandle> {
        match checkpoint.lineage {
            ThreadCheckpointLineage::Root => {}
            ThreadCheckpointLineage::Parent { parent_thread_id } => {
                return Err(CooldisError::CheckpointResumeRequiresRoot {
                    checkpoint_id: checkpoint.id,
                    thread_id: checkpoint.coordinates.thread_id,
                    parent_thread_id,
                });
            }
            ThreadCheckpointLineage::Unknown => {
                return Err(CooldisError::CheckpointResumeLineageUnknown {
                    checkpoint_id: checkpoint.id,
                    thread_id: checkpoint.coordinates.thread_id,
                });
            }
        }
        let metadata = checkpoint.metadata.clone();
        let context = ThreadContext::with_topology_and_metadata(
            checkpoint.coordinates.clone(),
            ThreadTopology::root(),
            metadata.clone(),
        );
        let start_reservation = self.reserve_thread_start(&context).await?;
        self.inner
            .runtime_store
            .select_branch(&checkpoint.coordinates, checkpoint.active_entry_id)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
        self.inner
            .checkpoints
            .lock()
            .await
            .insert(checkpoint.id, checkpoint);
        self.start_reserved_thread(context, metadata, start_reservation, false, false, true)
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
        self.fork_thread_from_checkpoint_with_id_inner(checkpoint, ThreadId::new(), true)
            .await
    }

    pub(crate) async fn fork_thread_from_checkpoint_with_id(
        &self,
        checkpoint: ThreadCheckpoint,
        child_thread_id: ThreadId,
    ) -> CooldisResult<RuntimeThreadHandle> {
        self.fork_thread_from_checkpoint_with_id_inner(checkpoint, child_thread_id, false)
            .await
    }

    async fn fork_thread_from_checkpoint_with_id_inner(
        &self,
        checkpoint: ThreadCheckpoint,
        child_thread_id: ThreadId,
        notify_lifecycle_sink: bool,
    ) -> CooldisResult<RuntimeThreadHandle> {
        let fork_coordinates = ThreadCoordinates {
            tenant_id: checkpoint.coordinates.tenant_id.clone(),
            user_id: checkpoint.coordinates.user_id.clone(),
            session_id: checkpoint.coordinates.session_id.clone(),
            thread_id: child_thread_id,
        };
        let mut metadata = checkpoint.metadata.clone();
        metadata.insert(
            "forked_from_thread_id".to_string(),
            checkpoint.coordinates.thread_id.to_string(),
        );
        metadata.insert(
            "forked_from_checkpoint_id".to_string(),
            checkpoint.id.to_string(),
        );
        let topology =
            ThreadTopology::branch_from(checkpoint.coordinates.thread_id, Some(checkpoint.id));
        let desired_context = ThreadContext::with_topology_and_metadata(
            fork_coordinates.clone(),
            topology,
            metadata.clone(),
        );
        loop {
            match self.get_thread(child_thread_id).await {
                Ok(handle) => {
                    if !fork_child_context_is_compatible(
                        handle.context(),
                        &fork_coordinates,
                        checkpoint.coordinates.thread_id,
                    ) {
                        return Err(CooldisError::History(format!(
                            "reserved fork child {child_thread_id} has incompatible runtime identity"
                        )));
                    }
                    return Ok(handle);
                }
                Err(CooldisError::ThreadNotFound(_)) => {}
                Err(err) => return Err(err),
            }

            let start_reservation = match self.reserve_thread_start(&desired_context).await {
                Ok(reservation) => reservation,
                Err(CooldisError::ThreadAlreadyExists(existing)) if existing == child_thread_id => {
                    self.wait_for_thread_start_reservation(child_thread_id)
                        .await;
                    continue;
                }
                Err(err) => return Err(err),
            };
            let stream_id = EventStreamId::for_thread(&fork_coordinates);
            let events = self
                .inner
                .runtime_store
                .read_events(&stream_id, None)
                .await
                .map_err(|err| CooldisError::History(err.to_string()))?;
            let mut durable_start_context = None;
            for start in events.iter().rev().filter(|event| {
                event.kind == EventKind::SessionEntryAppended
                    && event
                        .payload
                        .get("entry_kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("runtime")
                    && event
                        .payload
                        .get("runtime_kind")
                        .and_then(serde_json::Value::as_str)
                        == Some("thread_started")
            }) {
                let Some(payload) = start
                    .payload
                    .get("runtime_payload")
                    .and_then(serde_json::Value::as_object)
                else {
                    continue;
                };
                let Ok(topology) = serde_json::from_value(payload["topology"].clone()) else {
                    continue;
                };
                let Ok(start_metadata) = serde_json::from_value(payload["metadata"].clone()) else {
                    continue;
                };
                let context = ThreadContext::with_topology_and_metadata(
                    fork_coordinates.clone(),
                    topology,
                    start_metadata,
                );
                if fork_child_context_is_compatible(
                    &context,
                    &fork_coordinates,
                    checkpoint.coordinates.thread_id,
                ) {
                    durable_start_context = Some(context);
                    break;
                }
            }
            let has_start_identity = durable_start_context.is_some();
            let has_cloned_branch = self
                .inner
                .runtime_store
                .active_leaf(&fork_coordinates)
                .await
                .map_err(|err| CooldisError::History(err.to_string()))?
                .is_some();
            let (context, start_metadata) = if let Some(context) = durable_start_context {
                (context.clone(), context.metadata.clone())
            } else if has_cloned_branch {
                let cloned_context = self
                    .inner
                    .runtime_store
                    .build_context(&fork_coordinates)
                    .await
                    .map_err(|err| CooldisError::History(err.to_string()))?;
                let checkpoint_payload = cloned_context
                    .entries
                    .iter()
                    .rev()
                    .find_map(|entry| match &entry.kind {
                        SessionEntryKind::Runtime { kind, payload }
                            if kind == "thread_checkpoint" =>
                        {
                            Some(payload)
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        CooldisError::History(format!(
                            "reserved fork child {child_thread_id} has cloned history without a checkpoint"
                        ))
                    })?;
                let checkpoint_id = checkpoint_payload
                    .get("checkpoint_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        CooldisError::History(format!(
                            "reserved fork child {child_thread_id} has cloned history with an invalid checkpoint"
                        ))
                    })
                    .and_then(|id| {
                        ThreadCheckpointId::parse_str(id).map_err(|err| {
                            CooldisError::History(format!(
                                "reserved fork child {child_thread_id} checkpoint is invalid: {err}"
                            ))
                        })
                    })?;
                let mut recovered_metadata: BTreeMap<String, String> = checkpoint_payload
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|err| {
                        CooldisError::History(format!(
                            "reserved fork child {child_thread_id} checkpoint metadata is invalid: {err}"
                        ))
                    })?
                    .unwrap_or_default();
                recovered_metadata.insert(
                    "forked_from_thread_id".to_string(),
                    checkpoint.coordinates.thread_id.to_string(),
                );
                recovered_metadata.insert(
                    "forked_from_checkpoint_id".to_string(),
                    checkpoint_id.to_string(),
                );
                let context = ThreadContext::with_topology_and_metadata(
                    fork_coordinates.clone(),
                    ThreadTopology::branch_from(
                        checkpoint.coordinates.thread_id,
                        Some(checkpoint_id),
                    ),
                    recovered_metadata,
                );
                (context.clone(), context.metadata.clone())
            } else {
                (desired_context.clone(), metadata.clone())
            };
            if !has_start_identity && !has_cloned_branch {
                self.inner
                    .runtime_store
                    .clone_branch(
                        &checkpoint.coordinates,
                        checkpoint.active_entry_id,
                        &fork_coordinates,
                    )
                    .await
                    .map_err(|err| CooldisError::History(err.to_string()))?;
            }
            return self
                .start_reserved_thread(
                    context,
                    start_metadata,
                    start_reservation,
                    !has_start_identity,
                    true,
                    notify_lifecycle_sink,
                )
                .await;
        }
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
