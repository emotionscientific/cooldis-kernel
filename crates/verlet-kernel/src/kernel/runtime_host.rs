mod context_read_plan;
pub mod kernel_control;
mod loop_continuation;
pub mod runtime_api;
pub mod runtime_events;
pub mod runtime_services;
mod runtime_utils;
mod thread_handle;
pub mod turn;

pub type VerletResult<T> = Result<T, VerletError>;

pub const THREAD_BOUND_COUPLING_SET_METADATA: &str = "cooldis.agent.bound_coupling_set";
pub const THREAD_AGENT_MANIFEST_HASH_METADATA: &str = "cooldis.agent.manifest_hash";
pub const THREAD_SPAWN_INPUTS_HASH_METADATA: &str = "cooldis.thread_spawn.inputs_hash";
pub const THREAD_OPERATION_REGISTRY_ROOT_METADATA: &str = "cooldis.agent.operation_registry_root";

#[derive(Debug, thiserror::Error)]
pub enum VerletError {
    #[error("tenant not found: {0}")]
    TenantNotFound(String),
    #[error("tenant already exists: {0}")]
    TenantAlreadyExists(String),
    #[error("thread not found: {0}")]
    ThreadNotFound(verlet_runtime_contracts::ThreadId),
    #[error("thread already exists: {0}")]
    ThreadAlreadyExists(verlet_runtime_contracts::ThreadId),
    #[error("parent thread not found: {0}")]
    ParentThreadNotFound(verlet_runtime_contracts::ThreadId),
    #[error("parent thread {parent_thread_id} belongs to {actual:?}, not {requested:?}")]
    ParentThreadScopeMismatch {
        parent_thread_id: verlet_runtime_contracts::ThreadId,
        requested: Box<verlet_runtime_contracts::ThreadScope>,
        actual: Box<verlet_runtime_contracts::ThreadScope>,
    },
    #[error("related thread not found: {0}")]
    RelatedThreadNotFound(verlet_runtime_contracts::ThreadId),
    #[error("related thread {thread_id} belongs to {actual:?}, not {requested:?}")]
    RelatedThreadScopeMismatch {
        thread_id: verlet_runtime_contracts::ThreadId,
        requested: Box<verlet_runtime_contracts::ThreadScope>,
        actual: Box<verlet_runtime_contracts::ThreadScope>,
    },
    #[error("thread {thread_id} belongs to {actual:?}, not {requested:?}")]
    ThreadScopeMismatch {
        thread_id: verlet_runtime_contracts::ThreadId,
        requested: Box<verlet_runtime_contracts::ThreadScope>,
        actual: Box<verlet_runtime_contracts::ThreadScope>,
    },
    #[error("invalid thread topology: {0}")]
    ThreadTopologyInvalid(String),
    #[error("thread command channel closed: {0}")]
    ThreadClosed(verlet_runtime_contracts::ThreadId),
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
        thread_id: verlet_runtime_contracts::ThreadId,
        code: &'static str,
        message: String,
    },
    #[error(
        "checkpoint {checkpoint_id} cannot resume thread {thread_id}: checkpoint resume only supports root threads; parent lineage {parent_thread_id} cannot be restored"
    )]
    CheckpointResumeRequiresRoot {
        checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
        thread_id: verlet_runtime_contracts::ThreadId,
        parent_thread_id: verlet_runtime_contracts::ThreadId,
    },
    #[error(
        "checkpoint {checkpoint_id} cannot resume thread {thread_id}: root lineage was not recorded; recreate the checkpoint before resuming"
    )]
    CheckpointResumeLineageUnknown {
        checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
        thread_id: verlet_runtime_contracts::ThreadId,
    },
    #[error("lifecycle operation {operation} is not supported yet: {reason}")]
    LifecycleUnsupported {
        operation: &'static str,
        reason: String,
    },
}

impl From<verlet_process::VerletProcessError> for VerletError {
    fn from(err: verlet_process::VerletProcessError) -> Self {
        VerletError::RuntimeExecution(err.to_string())
    }
}

impl From<verlet_vbash::VerletVirtualBashError> for VerletError {
    fn from(err: verlet_vbash::VerletVirtualBashError) -> Self {
        match err {
            verlet_vbash::VerletVirtualBashError::RuntimeExecution(message) => {
                VerletError::RuntimeExecution(message)
            }
            verlet_vbash::VerletVirtualBashError::RuntimeFactory(message) => {
                VerletError::RuntimeFactory(message)
            }
        }
    }
}

impl From<verlet_agent::VerletAgentError> for VerletError {
    fn from(err: verlet_agent::VerletAgentError) -> Self {
        match err {
            verlet_agent::VerletAgentError::RuntimeExecution(message) => {
                VerletError::RuntimeExecution(message)
            }
            verlet_agent::VerletAgentError::RuntimeFactory(message) => {
                VerletError::RuntimeFactory(message)
            }
            verlet_agent::VerletAgentError::Operations(err) => err.into(),
        }
    }
}

impl From<verlet_operations::VerletOperationsError> for VerletError {
    fn from(err: verlet_operations::VerletOperationsError) -> Self {
        match err {
            verlet_operations::VerletOperationsError::RuntimeExecution(message) => {
                VerletError::RuntimeExecution(message)
            }
            verlet_operations::VerletOperationsError::RuntimeFactory(message) => {
                VerletError::RuntimeFactory(message)
            }
        }
    }
}

impl From<verlet_wasm::VerletWasmError> for VerletError {
    fn from(err: verlet_wasm::VerletWasmError) -> Self {
        match err {
            verlet_wasm::VerletWasmError::RuntimeFactory(message) => {
                VerletError::RuntimeFactory(message)
            }
            verlet_wasm::VerletWasmError::RuntimeExecution(message) => {
                VerletError::RuntimeExecution(message)
            }
        }
    }
}

fn bound_coupling_set_from_metadata(
    metadata: &std::collections::BTreeMap<String, String>,
) -> VerletResult<Option<crate::agent::manifest_bind::BoundCouplingSet>> {
    let Some(raw) = metadata.get(THREAD_BOUND_COUPLING_SET_METADATA) else {
        return Ok(None);
    };
    serde_json::from_str::<crate::agent::manifest_bind::BoundCouplingSet>(raw)
        .map(Some)
        .map_err(|err| {
            VerletError::RuntimeFactory(format!("thread bound coupling set is invalid: {err}"))
        })
}

fn operation_registry_root_from_metadata(
    metadata: &std::collections::BTreeMap<String, String>,
) -> Option<std::path::PathBuf> {
    metadata
        .get(THREAD_OPERATION_REGISTRY_ROOT_METADATA)
        .filter(|value| !value.trim().is_empty())
        .map(std::path::PathBuf::from)
}

#[derive(Clone)]
pub struct RuntimeHost {
    inner: std::sync::Arc<RuntimeHostInner>,
}

fn fork_child_context_is_compatible(
    context: &verlet_runtime_contracts::ThreadContext,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    parent_thread_id: verlet_runtime_contracts::ThreadId,
) -> bool {
    context.coordinates == *coordinates
        && context.topology.branch_parent_thread_id() == Some(parent_thread_id)
        && context.metadata.get("forked_from_thread_id") == Some(&parent_thread_id.to_string())
}

struct RuntimeHostInner {
    factory: std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory>,
    runtime_store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
    execution_policy: crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy,
    threads: tokio::sync::RwLock<
        std::collections::HashMap<
            verlet_runtime_contracts::ThreadId,
            std::sync::Arc<RuntimeThread>,
        >,
    >,
    thread_start_reservations: std::sync::Mutex<
        std::collections::HashMap<verlet_runtime_contracts::ThreadId, ThreadStartReservationState>,
    >,
    checkpoints: tokio::sync::Mutex<
        std::collections::HashMap<
            verlet_runtime_contracts::ThreadCheckpointId,
            crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
        >,
    >,
    lifecycle_sink: tokio::sync::RwLock<
        Option<std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ThreadLifecycleSink>>,
    >,
    process_handle_ingress: tokio::sync::RwLock<
        Option<
            std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink>,
        >,
    >,
    process_handle_dispatcher: tokio::sync::RwLock<
        Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher>,
    >,
    remote_thread_executor: tokio::sync::RwLock<
        Option<std::sync::Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>>,
    >,
}

struct RuntimeThread {
    context: verlet_runtime_contracts::ThreadContext,
    services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
    command_tx: tokio::sync::mpsc::Sender<crate::kernel::runtime_host::runtime_api::ThreadCommand>,
    command_capacity: usize,
    event_tx: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    status_tx: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    status_rx: tokio::sync::watch::Receiver<verlet_runtime_contracts::ThreadStatus>,
    cancellation: tokio_util::sync::CancellationToken,
    join_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    lifecycle: tokio::sync::Mutex<verlet_runtime_contracts::ThreadLifecycleRecord>,
    checkpoints:
        tokio::sync::Mutex<Vec<crate::kernel::runtime_host::runtime_api::ThreadCheckpoint>>,
    pending_input_slots: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    turn_reservations: std::sync::Mutex<std::collections::HashSet<String>>,
}

struct ThreadStartReservationState {
    context: verlet_runtime_contracts::ThreadContext,
    settled: tokio::sync::watch::Sender<bool>,
}

struct ThreadStartReservation<'a> {
    reservations: &'a std::sync::Mutex<
        std::collections::HashMap<verlet_runtime_contracts::ThreadId, ThreadStartReservationState>,
    >,
    thread_id: verlet_runtime_contracts::ThreadId,
    settled: tokio::sync::watch::Sender<bool>,
    committed: bool,
}

struct TurnIdReservation {
    thread: std::sync::Arc<RuntimeThread>,
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

fn lock_unpoisoned<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

#[derive(Clone)]
pub struct RuntimeThreadHandle {
    thread: std::sync::Arc<RuntimeThread>,
}

pub(crate) struct ReservedTurnSubmission {
    host: RuntimeHost,
    thread: RuntimeThreadHandle,
    reservation: Option<TurnIdReservation>,
    command_permit: Option<
        tokio::sync::mpsc::OwnedPermit<crate::kernel::runtime_host::runtime_api::ThreadCommand>,
    >,
    turn_id: String,
    input: Option<crate::kernel::runtime_host::turn::TurnInput>,
    mode: verlet_runtime_contracts::TurnSubmissionMode,
    turn_watchdog: Option<crate::kernel::runtime_host::turn::TurnWatchdogHandle>,
}

struct PublishedThreadStartGuard {
    host: RuntimeHost,
    thread: std::sync::Arc<RuntimeThread>,
    armed: bool,
}

impl PublishedThreadStartGuard {
    fn new(host: RuntimeHost, thread: std::sync::Arc<RuntimeThread>) -> Self {
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
        let thread = std::sync::Arc::clone(&self.thread);
        tokio::spawn(async move {
            RuntimeThreadHandle {
                thread: std::sync::Arc::clone(&thread),
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
        let _ = command_permit.send(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                turn_id: turn_id.clone(),
                input,
                mode,
            },
        );
        if let Some(reservation) = reservation.as_mut() {
            reservation.committed = true;
        }
        if let Some(turn_watchdog) = turn_watchdog {
            host.spawn_turn_timeout_watchdog(thread.clone(), turn_watchdog);
        }
        thread
            .record_signal(verlet_runtime_contracts::ThreadSignal::user_submit(
                &thread.context().coordinates,
                turn_id,
                mode,
            ))
            .await;
        true
    }
}

impl RuntimeHost {
    pub fn kernel_control(
        &self,
    ) -> crate::kernel::runtime_host::kernel_control::RuntimeKernelControl {
        crate::kernel::runtime_host::kernel_control::RuntimeKernelControl::new(
            std::sync::Arc::downgrade(&self.inner),
        )
    }

    pub fn new(
        factory: std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory>,
    ) -> Self {
        Self::with_session_store(
            factory,
            std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
        )
    }

    pub fn with_policy(
        factory: std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory>,
        execution_policy: crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy,
    ) -> Self {
        Self::with_session_store_and_policy(
            factory,
            std::sync::Arc::new(verlet_history::InMemorySessionStore::new()),
            execution_policy,
        )
    }

    pub fn with_session_store(
        factory: std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory>,
        runtime_store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
    ) -> Self {
        Self::with_session_store_and_policy(
            factory,
            runtime_store,
            crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy::default(),
        )
    }

    pub fn with_session_store_and_policy(
        factory: std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory>,
        runtime_store: std::sync::Arc<dyn verlet_history::RuntimeStore>,
        execution_policy: crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy,
    ) -> Self {
        Self {
            inner: std::sync::Arc::new(RuntimeHostInner {
                factory,
                runtime_store,
                execution_policy,
                threads: tokio::sync::RwLock::new(std::collections::HashMap::new()),
                thread_start_reservations: std::sync::Mutex::new(std::collections::HashMap::new()),
                checkpoints: tokio::sync::Mutex::new(std::collections::HashMap::new()),
                lifecycle_sink: tokio::sync::RwLock::new(None),
                process_handle_ingress: tokio::sync::RwLock::new(None),
                process_handle_dispatcher: tokio::sync::RwLock::new(None),
                remote_thread_executor: tokio::sync::RwLock::new(None),
            }),
        }
    }

    pub async fn set_lifecycle_sink(
        &self,
        sink: Option<
            std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ThreadLifecycleSink>,
        >,
    ) {
        *self.inner.lifecycle_sink.write().await = sink;
    }

    pub(crate) async fn set_process_handle_dispatcher(
        &self,
        dispatcher: Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher>,
    ) {
        *self.inner.process_handle_dispatcher.write().await = dispatcher;
    }

    pub async fn set_process_handle_ingress(
        &self,
        sink: Option<
            std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink>,
        >,
    ) {
        *self.inner.process_handle_ingress.write().await = sink;
    }

    async fn process_handle_ingress(
        &self,
    ) -> Option<
        std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink>,
    > {
        self.inner.process_handle_ingress.read().await.clone()
    }

    async fn process_handle_dispatcher(
        &self,
    ) -> Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher> {
        self.inner.process_handle_dispatcher.read().await.clone()
    }

    pub async fn set_remote_thread_executor(
        &self,
        executor: Option<
            std::sync::Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>,
        >,
    ) {
        *self.inner.remote_thread_executor.write().await = executor;
    }

    pub(crate) async fn remote_thread_executor(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::daemon::remote_store::placement::RemoteThreadExecutor>>
    {
        self.inner.remote_thread_executor.read().await.clone()
    }

    async fn lifecycle_sink(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::ThreadLifecycleSink>>
    {
        self.inner.lifecycle_sink.read().await.clone()
    }

    pub fn runtime_store(&self) -> std::sync::Arc<dyn verlet_history::RuntimeStore> {
        std::sync::Arc::clone(&self.inner.runtime_store)
    }

    pub fn execution_policy(
        &self,
    ) -> &crate::kernel::runtime_host::runtime_services::RuntimeExecutionPolicy {
        &self.inner.execution_policy
    }

    pub async fn start_thread(
        &self,
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        topology: verlet_runtime_contracts::ThreadTopology,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.start_thread_with_topology_and_metadata(
            coordinates,
            topology,
            std::collections::BTreeMap::new(),
        )
        .await
    }

    pub async fn start_thread_with_topology_and_metadata(
        &self,
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        topology: verlet_runtime_contracts::ThreadTopology,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.start_thread_with_topology_and_metadata_inner(coordinates, topology, metadata, true)
            .await
    }

    pub(crate) async fn load_thread_with_topology_and_metadata(
        &self,
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        topology: verlet_runtime_contracts::ThreadTopology,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.start_thread_with_topology_and_metadata_inner(coordinates, topology, metadata, false)
            .await
    }

    async fn start_thread_with_topology_and_metadata_inner(
        &self,
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        topology: verlet_runtime_contracts::ThreadTopology,
        metadata: std::collections::BTreeMap<String, String>,
        record_start_identity: bool,
    ) -> VerletResult<RuntimeThreadHandle> {
        let context = verlet_runtime_contracts::ThreadContext::with_topology_and_metadata(
            coordinates,
            topology,
            metadata.clone(),
        );
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
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> VerletResult<ThreadStartReservation<'a>> {
        let thread_id = context.coordinates.thread_id;
        let parent_thread_id = context.parent_thread_id;
        let threads = self.inner.threads.read().await;
        let mut reservations = lock_unpoisoned(&self.inner.thread_start_reservations);
        if threads.contains_key(&thread_id) || reservations.contains_key(&thread_id) {
            return Err(VerletError::ThreadAlreadyExists(thread_id));
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
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                            code: "max_child_threads".to_string(),
                            message: message.clone(),
                        },
                    );
                }
                return Err(VerletError::ThreadPolicyViolation {
                    thread_id: parent_thread_id,
                    code: "max_child_threads",
                    message,
                });
            }
        }
        let (settled, _) = tokio::sync::watch::channel(false);
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
    pub(crate) async fn wait_for_thread_start_reservation(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) {
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
        context: verlet_runtime_contracts::ThreadContext,
        metadata: std::collections::BTreeMap<String, String>,
        mut start_reservation: ThreadStartReservation<'_>,
        record_start_identity: bool,
        reconcile_start_identity_append: bool,
        notify_lifecycle_sink: bool,
    ) -> VerletResult<RuntimeThreadHandle> {
        let thread_id = context.coordinates.thread_id;
        let runtime = self.inner.factory.build(&context).await?;
        let command_channel_capacity = self
            .inner
            .execution_policy
            .max_pending_inputs
            .unwrap_or(512)
            .min(tokio::sync::Semaphore::MAX_PERMITS)
            .max(512);
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(command_channel_capacity);
        let command_capacity = command_tx.max_capacity();
        let pending_input_slots = self.inner.execution_policy.max_pending_inputs.map(|limit| {
            std::sync::Arc::new(tokio::sync::Semaphore::new(
                limit.min(tokio::sync::Semaphore::MAX_PERMITS),
            ))
        });
        let (event_tx, _) = tokio::sync::broadcast::channel(1024);
        let (status_tx, status_rx) =
            tokio::sync::watch::channel(verlet_runtime_contracts::ThreadStatus::Starting);
        let runtime_status_rx = status_rx.clone();
        let runtime_run_status_tx = status_tx.clone();
        let runtime_exit_status_tx = status_tx.clone();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let runtime_cancellation = cancellation.clone();
        let runtime_events = event_tx.clone();
        let runtime_exit_events = event_tx.clone();
        let runtime_context = context.clone();
        let runtime_exit_coordinates = context.coordinates.clone();
        let runtime_parent_thread_id = context.parent_thread_id;
        let mut services = crate::kernel::runtime_host::runtime_services::RuntimeServices::new(
            std::sync::Arc::clone(&self.inner.runtime_store),
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

        let (runtime_start_tx, runtime_start_rx) = tokio::sync::oneshot::channel();
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
            if !matches!(
                latest_status,
                verlet_runtime_contracts::ThreadStatus::Stopped
                    | verlet_runtime_contracts::ThreadStatus::Failed
            ) {
                let _ = runtime_exit_status_tx.send(verlet_runtime_contracts::ThreadStatus::Failed);
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    &runtime_exit_events,
                    &runtime_exit_coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                        action: "mark_failed".to_string(),
                        reason: "runtime exited without a terminal status".to_string(),
                    },
                );
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    &runtime_exit_events,
                    &runtime_exit_coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed {
                        code: "runtime_exited".to_string(),
                        message: "runtime exited without a terminal status".to_string(),
                    },
                );
            }
        });
        let lifecycle = verlet_runtime_contracts::ThreadLifecycleRecord::new(
            &context,
            verlet_runtime_contracts::ThreadLifecycleStatus::Starting,
            metadata,
        );
        let thread = std::sync::Arc::new(RuntimeThread {
            context,
            services,
            command_tx,
            command_capacity,
            event_tx,
            status_tx,
            status_rx,
            cancellation,
            join_handle: tokio::sync::Mutex::new(Some(join_handle)),
            lifecycle: tokio::sync::Mutex::new(lifecycle),
            checkpoints: tokio::sync::Mutex::new(Vec::new()),
            pending_input_slots,
            turn_reservations: std::sync::Mutex::new(std::collections::HashSet::new()),
        });

        {
            let mut threads = self.inner.threads.write().await;
            let mut reservations = lock_unpoisoned(&self.inner.thread_start_reservations);
            let reservation = reservations.remove(&thread_id);
            debug_assert!(reservation.is_some());
            let previous = threads.insert(thread_id, std::sync::Arc::clone(&thread));
            debug_assert!(previous.is_none());
            start_reservation.commit();
        }
        let mut published_start =
            PublishedThreadStartGuard::new(self.clone(), std::sync::Arc::clone(&thread));

        let handle = RuntimeThreadHandle {
            thread: std::sync::Arc::clone(&thread),
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
                parent.emit_runtime(crate::kernel::runtime_host::runtime_events::RuntimeEventKind::SubthreadStarted {
                    child_thread_id: thread_id,
                });
            }
        }

        published_start.disarm();
        Ok(handle)
    }

    async fn remove_thread_if_current(&self, thread: &std::sync::Arc<RuntimeThread>) -> bool {
        let thread_id = thread.context.coordinates.thread_id;
        let mut threads = self.inner.threads.write().await;
        if threads
            .get(&thread_id)
            .is_some_and(|current| std::sync::Arc::ptr_eq(current, thread))
        {
            threads.remove(&thread_id);
            true
        } else {
            false
        }
    }

    pub async fn get_thread(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> VerletResult<RuntimeThreadHandle> {
        let threads = self.inner.threads.read().await;
        let thread = threads
            .get(&thread_id)
            .cloned()
            .ok_or(VerletError::ThreadNotFound(thread_id))?;
        Ok(RuntimeThreadHandle { thread })
    }

    pub async fn submit(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> VerletResult<()> {
        self.submit_turn_with_mode(
            thread_id,
            turn_id,
            crate::kernel::runtime_host::turn::TurnInput::text(input.into()),
            verlet_runtime_contracts::TurnSubmissionMode::Queue,
        )
        .await
    }

    pub async fn submit_turn(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
    ) -> VerletResult<()> {
        self.submit_turn_with_mode(
            thread_id,
            turn_id,
            input,
            verlet_runtime_contracts::TurnSubmissionMode::Queue,
        )
        .await
    }

    pub async fn submit_with_mode(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
    ) -> VerletResult<()> {
        self.submit_turn_with_mode(
            thread_id,
            turn_id,
            crate::kernel::runtime_host::turn::TurnInput::text(input.into()),
            mode,
        )
        .await
    }

    pub async fn steer(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> VerletResult<()> {
        self.submit_with_mode(
            thread_id,
            turn_id,
            input,
            verlet_runtime_contracts::TurnSubmissionMode::Steer,
        )
        .await
    }

    pub async fn interrupt_with(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: impl Into<String>,
    ) -> VerletResult<()> {
        self.submit_with_mode(
            thread_id,
            turn_id,
            input,
            verlet_runtime_contracts::TurnSubmissionMode::Interrupt,
        )
        .await
    }

    pub async fn resume_tool_call(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        call_id: impl Into<String>,
    ) -> VerletResult<()> {
        let thread = self.get_thread(thread_id).await?;
        thread
            .send(
                crate::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall {
                    turn_id: turn_id.into(),
                    call_id: call_id.into(),
                },
            )
            .await
    }

    pub async fn continue_turn_if_requested(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        loop_id: impl Into<String>,
        parent_turn_id: impl Into<String>,
        next_turn_id: impl Into<String>,
        now_ms: i64,
        completed_continuations: u32,
    ) -> VerletResult<crate::kernel::runtime_host::loop_continuation::LoopContinuationReceipt> {
        let loop_id = loop_id.into();
        let parent_turn_id = parent_turn_id.into();
        let next_turn_id = next_turn_id.into();
        let thread = self.get_thread(thread_id).await?;
        let coordinates = thread.context().coordinates.clone();
        let Some((request_event, request_payload)) =
            crate::kernel::runtime_host::loop_continuation::latest_turn_continue_request(
                thread.thread.services.runtime_store().as_ref(),
                &coordinates,
                &loop_id,
                &parent_turn_id,
            )
            .await?
        else {
            return Ok(
                crate::kernel::runtime_host::loop_continuation::LoopContinuationReceipt::NoRequest,
            );
        };
        if let Some(receipt) =
            crate::kernel::runtime_host::loop_continuation::existing_continuation_receipt(
                thread.thread.services.runtime_store().as_ref(),
                &coordinates,
                &request_payload.subject,
                &request_payload.snapshot_id,
            )
            .await?
        {
            if let crate::kernel::runtime_host::loop_continuation::LoopContinuationReceipt::Accepted {
                next_turn_id,
                accepted_event_id,
                ..
            } = &receipt
                && crate::kernel::runtime_host::loop_continuation::turn_submitted_event(
                    thread.thread.services.runtime_store().as_ref(),
                    &coordinates,
                    next_turn_id,
                )
                .await?
                .is_none()
            {
                crate::kernel::runtime_host::loop_continuation::append_loop_turn_submitted_event(
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
                    crate::kernel::runtime_host::turn::TurnInput::text(request_payload.next_turn_input),
                    verlet_runtime_contracts::TurnSubmissionMode::Queue,
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
        match crate::kernel::runtime_host::loop_continuation::decide_continuation(
            thread.thread.services.runtime_store().as_ref(),
            crate::kernel::control_decision::TurnContinuationDecisionRequest {
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
            crate::kernel::control_decision::TurnContinuationDecision::NoRequest => Ok(
                crate::kernel::runtime_host::loop_continuation::LoopContinuationReceipt::NoRequest,
            ),
            crate::kernel::control_decision::TurnContinuationDecision::Accept {
                consumed_request_id,
                mandate_id,
                next_turn_input,
            } => {
                let accepted = crate::kernel::runtime_host::loop_continuation::append_continuation_accepted_event(
                    thread.thread.services.runtime_store().as_ref(),
                    &coordinates,
                    &request_payload.subject,
                    &request_payload.snapshot_id,
                    &mandate_id,
                    &next_turn_id,
                    consumed_request_id,
                )
                .await?;
                crate::kernel::runtime_host::loop_continuation::append_loop_turn_submitted_event(
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
                    crate::kernel::runtime_host::turn::TurnInput::text(next_turn_input),
                    verlet_runtime_contracts::TurnSubmissionMode::Queue,
                    None,
                )
                .await?;
                Ok(crate::kernel::runtime_host::loop_continuation::LoopContinuationReceipt::Accepted {
                    loop_id,
                    parent_turn_id,
                    next_turn_id,
                    accepted_event_id: accepted.id,
                })
            }
            crate::kernel::control_decision::TurnContinuationDecision::Reject {
                consumed_request_id,
                reason,
                ..
            } => {
                let rejected = crate::kernel::runtime_host::loop_continuation::append_continuation_rejected_event(
                    thread.thread.services.runtime_store().as_ref(),
                    &coordinates,
                    &request_payload.subject,
                    &request_payload.snapshot_id,
                    &reason,
                    consumed_request_id.unwrap_or(request_event.id),
                )
                .await?;
                Ok(crate::kernel::runtime_host::loop_continuation::LoopContinuationReceipt::Rejected {
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
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        input: crate::kernel::runtime_host::turn::TurnInput,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
    ) -> VerletResult<()> {
        let admission = crate::kernel::admission::AdmissionGateContext::surface_default(
            crate::kernel::admission::HOST_SUBMIT_SURFACE,
            Vec::new(),
        )?;
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
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        mut input: crate::kernel::runtime_host::turn::TurnInput,
        mode: verlet_runtime_contracts::TurnSubmissionMode,
        admission: Option<crate::kernel::admission::AdmissionGateContext>,
    ) -> VerletResult<ReservedTurnSubmission> {
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
            thread: std::sync::Arc::clone(&thread.thread),
            turn_id: turn_id.clone(),
            committed: false,
        };
        if mode == verlet_runtime_contracts::TurnSubmissionMode::Queue
            && let Some(max_pending_inputs) = self.inner.execution_policy.max_pending_inputs
        {
            let pending_input_slots =
                thread.thread.pending_input_slots.as_ref().ok_or_else(|| {
                    VerletError::RuntimeExecution(
                        "configured pending-input policy has no slot semaphore".to_string(),
                    )
                })?;
            let pending_input_permit = match std::sync::Arc::clone(pending_input_slots)
                .try_acquire_owned()
            {
                Ok(permit) => permit,
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    let effective_limit =
                        max_pending_inputs.min(tokio::sync::Semaphore::MAX_PERMITS);
                    let queued_commands =
                        effective_limit.saturating_sub(pending_input_slots.available_permits());
                    let message = format!(
                        "thread has {queued_commands} queued command(s); max pending input count is {max_pending_inputs}"
                    );
                    thread.emit_runtime(crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                        code: "max_pending_inputs".to_string(),
                        message: message.clone(),
                    });
                    return Err(VerletError::ThreadPolicyViolation {
                        thread_id,
                        code: "max_pending_inputs",
                        message,
                    });
                }
                Err(tokio::sync::TryAcquireError::Closed) => {
                    return Err(VerletError::ThreadClosed(thread_id));
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
            .map_err(|_| VerletError::ThreadClosed(thread_id))?;
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
        thread_id: verlet_runtime_contracts::ThreadId,
        turn_id: impl Into<String>,
        summary: Option<String>,
    ) -> VerletResult<()> {
        let thread = self.get_thread(thread_id).await?;
        thread
            .send(
                crate::kernel::runtime_host::runtime_api::ThreadCommand::Compact {
                    turn_id: turn_id.into(),
                    trigger: crate::kernel::compaction::CompactionTrigger::Manual,
                    summary,
                },
            )
            .await?;
        Ok(())
    }

    pub async fn cancel(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        reason: impl Into<String>,
    ) -> VerletResult<()> {
        let reason = reason.into();
        let thread = self.get_thread(thread_id).await?;
        thread.emit_runtime(
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                action: "cancel_requested".to_string(),
                reason: reason.clone(),
            },
        );
        thread
            .record_signal(verlet_runtime_contracts::ThreadSignal::interrupt_cancel(
                &thread.context().coordinates,
                reason.clone(),
            ))
            .await;
        if matches!(
            thread.status(),
            verlet_runtime_contracts::ThreadStatus::Starting
                | verlet_runtime_contracts::ThreadStatus::Idle
                | verlet_runtime_contracts::ThreadStatus::Stopped
                | verlet_runtime_contracts::ThreadStatus::Failed
        ) && thread.queued_command_count() == 0
        {
            return Ok(());
        }
        thread
            .send(
                crate::kernel::runtime_host::runtime_api::ThreadCommand::Cancel {
                    reason: reason.clone(),
                },
            )
            .await?;
        self.wait_for_cancel_grace(&thread).await?;
        Ok(())
    }

    pub async fn shutdown_thread(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> VerletResult<()> {
        let thread = self.get_thread(thread_id).await?;
        let parent_thread_id = thread.context().parent_thread_id;
        thread.emit_runtime(
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                action: "shutdown_requested".to_string(),
                reason: "shutdown_thread".to_string(),
            },
        );
        match thread
            .send(crate::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown)
            .await
        {
            Ok(()) => {
                thread
                    .record_signal(verlet_runtime_contracts::ThreadSignal::shutdown(
                        &thread.context().coordinates,
                    ))
                    .await;
            }
            Err(VerletError::ThreadClosed(_)) => {
                thread.thread.cancellation.cancel();
            }
            Err(err) => return Err(err),
        }
        let timed_out = self.wait_for_shutdown(&thread).await?;
        let removed = self.remove_thread_if_current(&thread.thread).await;
        if removed && let Some(parent_thread_id) = parent_thread_id {
            if let Ok(parent) = self.get_thread(parent_thread_id).await {
                parent.emit_runtime(crate::kernel::runtime_host::runtime_events::RuntimeEventKind::SubthreadFinished {
                    child_thread_id: thread_id,
                    status: if timed_out {
                        verlet_runtime_contracts::ThreadLifecycleStatus::Failed
                    } else {
                        verlet_runtime_contracts::ThreadLifecycleStatus::Stopped
                    },
                });
            }
        }
        Ok(())
    }

    pub async fn session_context(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> VerletResult<verlet_history::SessionContext> {
        self.get_thread(thread_id).await?.session_context().await
    }

    /// Shuts down descendants before registered ancestors, with thread-id order
    /// breaking ties at the same topology depth.
    pub async fn shutdown_all(&self) -> VerletResult<Vec<verlet_runtime_contracts::ThreadId>> {
        fn registered_depth(
            thread_id: verlet_runtime_contracts::ThreadId,
            parents: &std::collections::HashMap<
                verlet_runtime_contracts::ThreadId,
                Option<verlet_runtime_contracts::ThreadId>,
            >,
            path: &mut Vec<verlet_runtime_contracts::ThreadId>,
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
                .collect::<std::collections::HashMap<_, _>>()
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
        mut watchdog: crate::kernel::runtime_host::turn::TurnWatchdogHandle,
    ) {
        let Some(timeout_ms) = self.inner.execution_policy.turn_timeout_ms else {
            return;
        };
        let cancel_grace_timeout_ms = self.inner.execution_policy.cancel_grace_timeout_ms;
        let watchdog_token_id = watchdog.id();
        let thread = std::sync::Arc::downgrade(&thread.thread);
        tokio::spawn(async move {
            if !watchdog.wait_until_started().await {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(timeout_ms)).await;
            let Some(thread) = thread.upgrade() else {
                return;
            };
            let thread = RuntimeThreadHandle { thread };
            if thread.status() != verlet_runtime_contracts::ThreadStatus::Running
                || !watchdog.try_timeout()
            {
                return;
            }
            thread.emit_runtime(
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Timeout {
                    operation: "turn".to_string(),
                    timeout_ms,
                },
            );
            thread.emit_runtime(
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                    state: verlet_runtime_contracts::RuntimeTerminalState::TimedOut,
                },
            );
            let reason = format!("turn exceeded {timeout_ms}ms timeout");
            match thread.try_reserve_command() {
                Ok(command_permit) => {
                    command_permit.send(
                        crate::kernel::runtime_host::runtime_api::ThreadCommand::CancelTurn {
                            watchdog_token_id,
                            reason: reason.clone(),
                        },
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    if !watchdog.is_timed_out()
                        || thread.status() != verlet_runtime_contracts::ThreadStatus::Running
                    {
                        return;
                    }
                    thread.set_status(verlet_runtime_contracts::ThreadStatus::Cancelling);
                    thread.thread.cancellation.cancel();
                }
            }
            thread
                .record_signal(verlet_runtime_contracts::ThreadSignal::interrupt_cancel(
                    &thread.context().coordinates,
                    reason.clone(),
                ))
                .await;
            if let Some(cancel_timeout_ms) = cancel_grace_timeout_ms {
                tokio::time::sleep(std::time::Duration::from_millis(cancel_timeout_ms)).await;
                if watchdog.is_timed_out()
                    && matches!(
                        thread.status(),
                        verlet_runtime_contracts::ThreadStatus::Running
                            | verlet_runtime_contracts::ThreadStatus::Cancelling
                    )
                {
                    thread.emit_runtime(
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Timeout {
                            operation: "cancel".to_string(),
                            timeout_ms: cancel_timeout_ms,
                        },
                    );
                    thread.emit_runtime(
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                            action: "abort_runtime".to_string(),
                            reason: "cancel grace timeout elapsed after turn timeout".to_string(),
                        },
                    );
                    thread.set_status(verlet_runtime_contracts::ThreadStatus::Failed);
                    thread.emit_runtime(
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed {
                            code: "cancel_timeout".to_string(),
                            message: "runtime did not cancel within grace timeout".to_string(),
                        },
                    );
                    thread.abort().await;
                }
            }
        });
    }

    async fn wait_for_cancel_grace(&self, thread: &RuntimeThreadHandle) -> VerletResult<()> {
        let Some(timeout_ms) = self.inner.execution_policy.cancel_grace_timeout_ms else {
            return Ok(());
        };
        let completed = tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), async {
            loop {
                if !matches!(
                    thread.status(),
                    verlet_runtime_contracts::ThreadStatus::Running
                        | verlet_runtime_contracts::ThreadStatus::Cancelling
                ) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        if completed {
            thread.emit_runtime(
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                    action: "cancel_completed".to_string(),
                    reason: "runtime returned to a recoverable state".to_string(),
                },
            );
            return Ok(());
        }
        thread.emit_runtime(
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Timeout {
                operation: "cancel".to_string(),
                timeout_ms,
            },
        );
        thread.emit_runtime(
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                action: "abort_runtime".to_string(),
                reason: "cancel grace timeout elapsed".to_string(),
            },
        );
        thread.set_status(verlet_runtime_contracts::ThreadStatus::Failed);
        thread.emit_runtime(
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed {
                code: "cancel_timeout".to_string(),
                message: "runtime did not cancel within grace timeout".to_string(),
            },
        );
        thread.abort().await;
        Err(VerletError::ThreadPolicyViolation {
            thread_id: thread.context().coordinates.thread_id,
            code: "cancel_timeout",
            message: "runtime did not cancel within grace timeout".to_string(),
        })
    }

    async fn wait_for_shutdown(&self, thread: &RuntimeThreadHandle) -> VerletResult<bool> {
        let Some(timeout_ms) = self.inner.execution_policy.shutdown_grace_timeout_ms else {
            thread.wait().await;
            thread.emit_runtime(
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                    action: "shutdown_completed".to_string(),
                    reason: "runtime stopped".to_string(),
                },
            );
            return Ok(false);
        };
        if thread
            .wait_timeout_or_abort(std::time::Duration::from_millis(timeout_ms))
            .await
        {
            thread.emit_runtime(
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                    action: "shutdown_completed".to_string(),
                    reason: "runtime stopped".to_string(),
                },
            );
            return Ok(false);
        }
        thread.emit_runtime(
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Timeout {
                operation: "shutdown".to_string(),
                timeout_ms,
            },
        );
        thread.emit_runtime(
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                action: "abort_runtime".to_string(),
                reason: "shutdown grace timeout elapsed".to_string(),
            },
        );
        thread.thread.cancellation.cancel();
        thread.set_status(verlet_runtime_contracts::ThreadStatus::Failed);
        thread.abort().await;
        Ok(true)
    }

    pub async fn children_of(
        &self,
        parent_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> Vec<RuntimeThreadHandle> {
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
        parent_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> VerletResult<Vec<RuntimeThreadHandle>> {
        let parent = self.get_thread(parent_coordinates.thread_id).await?;
        let requested_scope = parent_coordinates.scope();
        let actual_scope = parent.context().coordinates.scope();
        if requested_scope != actual_scope {
            return Err(VerletError::ThreadScopeMismatch {
                thread_id: parent_coordinates.thread_id,
                requested: Box::new(requested_scope),
                actual: Box::new(actual_scope),
            });
        }
        Ok(self.children_of(parent_coordinates.thread_id).await)
    }

    pub async fn create_checkpoint(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        parent_checkpoint_id: Option<verlet_runtime_contracts::ThreadCheckpointId>,
        label: Option<String>,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> VerletResult<crate::kernel::runtime_host::runtime_api::ThreadCheckpoint> {
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
        checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
    ) -> VerletResult<RuntimeThreadHandle> {
        let checkpoint = self
            .inner
            .checkpoints
            .lock()
            .await
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| VerletError::LifecycleUnsupported {
                operation: "resume_thread",
                reason: format!("checkpoint {checkpoint_id} is not loaded in this host"),
            })?;
        self.resume_thread_from_checkpoint(checkpoint).await
    }

    /// Resumes only checkpoints explicitly recorded with root lineage; V1
    /// resume rejects parent or unknown lineage instead of flattening it.
    pub async fn resume_thread_from_checkpoint(
        &self,
        checkpoint: crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    ) -> VerletResult<RuntimeThreadHandle> {
        match checkpoint.lineage {
            crate::kernel::runtime_host::runtime_api::ThreadCheckpointLineage::Root => {}
            crate::kernel::runtime_host::runtime_api::ThreadCheckpointLineage::Parent {
                parent_thread_id,
            } => {
                return Err(VerletError::CheckpointResumeRequiresRoot {
                    checkpoint_id: checkpoint.id,
                    thread_id: checkpoint.coordinates.thread_id,
                    parent_thread_id,
                });
            }
            crate::kernel::runtime_host::runtime_api::ThreadCheckpointLineage::Unknown => {
                return Err(VerletError::CheckpointResumeLineageUnknown {
                    checkpoint_id: checkpoint.id,
                    thread_id: checkpoint.coordinates.thread_id,
                });
            }
        }
        let metadata = checkpoint.metadata.clone();
        let context = verlet_runtime_contracts::ThreadContext::with_topology_and_metadata(
            checkpoint.coordinates.clone(),
            verlet_runtime_contracts::ThreadTopology::root(),
            metadata.clone(),
        );
        let start_reservation = self.reserve_thread_start(&context).await?;
        self.inner
            .runtime_store
            .select_branch(&checkpoint.coordinates, checkpoint.active_entry_id)
            .await
            .map_err(|err| VerletError::History(err.to_string()))?;
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
        thread_id: verlet_runtime_contracts::ThreadId,
        checkpoint_id: Option<verlet_runtime_contracts::ThreadCheckpointId>,
    ) -> VerletResult<RuntimeThreadHandle> {
        let checkpoint_id = checkpoint_id.ok_or_else(|| VerletError::LifecycleUnsupported {
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
            .ok_or_else(|| VerletError::LifecycleUnsupported {
                operation: "fork_thread",
                reason: format!("checkpoint {checkpoint_id} is not loaded in this host"),
            })?;
        if checkpoint.coordinates.thread_id != thread_id {
            return Err(VerletError::ThreadScopeMismatch {
                thread_id,
                requested: Box::new(verlet_runtime_contracts::ThreadScope {
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
        checkpoint: crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.fork_thread_from_checkpoint_with_id_inner(
            checkpoint,
            verlet_runtime_contracts::ThreadId::new(),
            true,
        )
        .await
    }

    pub(crate) async fn fork_thread_from_checkpoint_with_id(
        &self,
        checkpoint: crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
        child_thread_id: verlet_runtime_contracts::ThreadId,
    ) -> VerletResult<RuntimeThreadHandle> {
        self.fork_thread_from_checkpoint_with_id_inner(checkpoint, child_thread_id, false)
            .await
    }

    async fn fork_thread_from_checkpoint_with_id_inner(
        &self,
        checkpoint: crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
        child_thread_id: verlet_runtime_contracts::ThreadId,
        notify_lifecycle_sink: bool,
    ) -> VerletResult<RuntimeThreadHandle> {
        let fork_coordinates = verlet_runtime_contracts::ThreadCoordinates {
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
        let topology = verlet_runtime_contracts::ThreadTopology::branch_from(
            checkpoint.coordinates.thread_id,
            Some(checkpoint.id),
        );
        let desired_context = verlet_runtime_contracts::ThreadContext::with_topology_and_metadata(
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
                        return Err(VerletError::History(format!(
                            "reserved fork child {child_thread_id} has incompatible runtime identity"
                        )));
                    }
                    return Ok(handle);
                }
                Err(VerletError::ThreadNotFound(_)) => {}
                Err(err) => return Err(err),
            }

            let start_reservation = match self.reserve_thread_start(&desired_context).await {
                Ok(reservation) => reservation,
                Err(VerletError::ThreadAlreadyExists(existing)) if existing == child_thread_id => {
                    self.wait_for_thread_start_reservation(child_thread_id)
                        .await;
                    continue;
                }
                Err(err) => return Err(err),
            };
            let stream_id = verlet_history::EventStreamId::for_thread(&fork_coordinates);
            let events = self
                .inner
                .runtime_store
                .read_events(&stream_id, None)
                .await
                .map_err(|err| VerletError::History(err.to_string()))?;
            let mut durable_start_context = None;
            for start in events.iter().rev().filter(|event| {
                event.kind == verlet_history::EventKind::SessionEntryAppended
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
                let context = verlet_runtime_contracts::ThreadContext::with_topology_and_metadata(
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
                .map_err(|err| VerletError::History(err.to_string()))?
                .is_some();
            let (context, start_metadata) = if let Some(context) = durable_start_context {
                (context.clone(), context.metadata.clone())
            } else if has_cloned_branch {
                let cloned_context = self
                    .inner
                    .runtime_store
                    .build_context(&fork_coordinates)
                    .await
                    .map_err(|err| VerletError::History(err.to_string()))?;
                let checkpoint_payload = cloned_context
                    .entries
                    .iter()
                    .rev()
                    .find_map(|entry| match &entry.kind {
                        verlet_history::SessionEntryKind::Runtime { kind, payload }
                            if kind == "thread_checkpoint" =>
                        {
                            Some(payload)
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        VerletError::History(format!(
                            "reserved fork child {child_thread_id} has cloned history without a checkpoint"
                        ))
                    })?;
                let checkpoint_id = checkpoint_payload
                    .get("checkpoint_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        VerletError::History(format!(
                            "reserved fork child {child_thread_id} has cloned history with an invalid checkpoint"
                        ))
                    })
                    .and_then(|id| {
                        verlet_runtime_contracts::ThreadCheckpointId::parse_str(id).map_err(|err| {
                            VerletError::History(format!(
                                "reserved fork child {child_thread_id} checkpoint is invalid: {err}"
                            ))
                        })
                    })?;
                let mut recovered_metadata: std::collections::BTreeMap<String, String> = checkpoint_payload
                    .get("metadata")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|err| {
                        VerletError::History(format!(
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
                let context = verlet_runtime_contracts::ThreadContext::with_topology_and_metadata(
                    fork_coordinates.clone(),
                    verlet_runtime_contracts::ThreadTopology::branch_from(
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
                    .map_err(|err| VerletError::History(err.to_string()))?;
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
        checkpoint_id: verlet_runtime_contracts::ThreadCheckpointId,
    ) -> VerletResult<crate::kernel::runtime_host::runtime_api::ThreadCheckpoint> {
        self.inner
            .checkpoints
            .lock()
            .await
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| VerletError::LifecycleUnsupported {
                operation: "checkpoint",
                reason: format!("checkpoint {checkpoint_id} is not loaded in this host"),
            })
    }

    pub async fn fork_history_by_reference(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        base: verlet_history::ThreadBaseRef,
    ) -> VerletResult<()> {
        self.inner
            .runtime_store
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
            .map_err(|err| VerletError::History(err.to_string()))
    }

    pub async fn snapshot(&self) -> crate::kernel::runtime_host::runtime_api::RuntimeHostSnapshot {
        let threads = self.inner.threads.read().await;
        let mut snapshots = Vec::with_capacity(threads.len());
        for thread in threads.values() {
            snapshots.push(crate::kernel::runtime_host::runtime_api::ThreadSnapshot {
                context: thread.context.clone(),
                status: *thread.status_rx.borrow(),
            });
        }
        snapshots.sort_by_key(|snapshot| snapshot.context.coordinates.thread_id.to_string());
        crate::kernel::runtime_host::runtime_api::RuntimeHostSnapshot { threads: snapshots }
    }

    pub async fn lifecycle_snapshot(
        &self,
    ) -> crate::kernel::runtime_host::runtime_api::RuntimeHostLifecycleSnapshot {
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
        crate::kernel::runtime_host::runtime_api::RuntimeHostLifecycleSnapshot { records }
    }
}

#[cfg(test)]
mod tests;
