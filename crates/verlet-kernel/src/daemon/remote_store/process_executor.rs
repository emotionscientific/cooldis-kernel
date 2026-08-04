//! Process-backed remote placement executor.
//!
//! The parent retains the only handle to each child process. Queue pulls and
//! stream pushes cross the authenticated sync endpoint; neither side opens
//! the other's SQLite file. Child processes admit queue rows through the
//! ordinary durable ingress claim/settle lane, then propagate their local
//! thread stream back under a fenced lease.

use super::endpoint::{
    SYNC_INGRESS_QUEUE_ACK_SCHEMA_V1, SyncIngressQueueAckRequestV1, SyncIngressQueueAcknowledger,
    SyncPullSource,
};
use super::endpoint_http::HttpSyncClient;
use super::lease::{
    SqliteStreamLeaseAuthority, StreamLeaseAuthority, StreamLeaseGrantV1, StreamLeaseLineage,
    StreamPrefixScope, SyncCredentialAuthority,
};
use super::placement::{
    RemoteThreadExecutor, RemoteThreadObservation, RemoteThreadSpawnRequest,
    RemoteThreadSubmitRequest, RemoteThreadWaitObservation,
};
use super::propagator::{
    LocalFirstStreamPropagator, PropagationStep, SqlitePropagationStateStore,
    StreamPropagationState, StreamPropagator,
};
use super::queue::{
    RemoteIngressQueue, RemoteIngressQueueEntryV1, SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1,
    SqliteRemoteIngressQueue, remote_ingress_queue_stream_id,
};
use super::tail::{RemoteStreamTail, RemoteStreamTailCursor, SqliteRemoteStreamTail};
use crate::daemon::recovery_sweep::project_child_terminal_record;
use crate::kernel::runtime_host::append_thread_joined_first_wins;
use crate::{
    CanonicalContent, CanonicalMessage, EventKind, EventStore, EventStreamId, SessionEntryKind,
    SessionStore, SqliteSessionStore, SystemDaemonClock, ThreadContext, ThreadCoordinates,
    ThreadId, ThreadStartRequest, ThreadStatus, ThreadTerminalState, TurnInput, VerletAppServer,
    VerletAppServerConfig, VerletDaemonIoBridge, VerletError, VerletResult,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::{Mutex, watch};
use verlet_io_core::{
    AdmissionDecision, ConversationKind, IngressContent, IngressEnvelope, IoConversation,
    IoDedupeKey, IoDelivery, IoError, IoPrincipal, IoSource, IoTurnInput, ResolvedIoTarget,
    ThreadAddress,
};
use verlet_sqlite::{TransactionBehavior, params};

const REMOTE_CHILD_COMMAND: &str = "__remote-child";
const REMOTE_TURN_ID_METADATA: &str = "cooldis_remote_turn_id";
const REMOTE_INPUT_METADATA: &str = "cooldis_remote_turn_input";
const CHILD_POLL_INTERVAL: Duration = Duration::from_millis(20);
const CHILD_RETRY_MAX: Duration = Duration::from_secs(1);
const PARENT_TAIL_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Secret-bearing bootstrap sent once over the child's stdin pipe. It is
/// deliberately not `Debug`: bearer tokens must never reach diagnostics.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RemoteChildBootstrapV1 {
    pub schema: String,
    pub request: RemoteThreadSpawnRequest,
    pub sync_endpoint: String,
    pub stream_lease: StreamLeaseGrantV1,
    pub stream_bearer_token: String,
    pub queue_bearer_token: String,
    pub daemon_config_path: Option<PathBuf>,
    pub runtime_home: PathBuf,
    pub state_home: PathBuf,
}

const REMOTE_CHILD_BOOTSTRAP_SCHEMA_V1: &str = "cooldis.remote_child.bootstrap/1";

pub(crate) struct ProcessRemoteThreadExecutor {
    inner: Arc<ProcessRemoteThreadExecutorInner>,
}

struct ProcessRemoteThreadExecutorInner {
    store: SqliteSessionStore,
    queue: SqliteRemoteIngressQueue,
    authority: Arc<SqliteStreamLeaseAuthority>,
    sync_endpoint: String,
    daemon_config_path: Option<PathBuf>,
    child_root: PathBuf,
    executable: PathBuf,
    spawn_lock: Mutex<()>,
    states: StdRwLock<HashMap<ThreadId, Arc<RemoteChildState>>>,
}

struct RemoteChildState {
    child: ThreadContext,
    status: watch::Sender<ThreadStatus>,
    _process: AbortOnDrop,
    _tail: AbortOnDrop,
}

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Clone for ProcessRemoteThreadExecutor {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for ProcessRemoteThreadExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessRemoteThreadExecutor")
            .field("sync_endpoint", &self.inner.sync_endpoint)
            .field("child_root", &self.inner.child_root)
            .finish_non_exhaustive()
    }
}

impl ProcessRemoteThreadExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        store: SqliteSessionStore,
        authority: Arc<SqliteStreamLeaseAuthority>,
        sync_endpoint: String,
        daemon_config_path: Option<PathBuf>,
        child_root: PathBuf,
        executable: PathBuf,
    ) -> VerletResult<Self> {
        let queue = SqliteRemoteIngressQueue::new(store.clone()).await?;
        Ok(Self {
            inner: Arc::new(ProcessRemoteThreadExecutorInner {
                store,
                queue,
                authority,
                sync_endpoint,
                daemon_config_path,
                child_root,
                executable,
                spawn_lock: Mutex::new(()),
                states: StdRwLock::new(HashMap::new()),
            }),
        })
    }

    async fn spawn_shielded(&self, request: RemoteThreadSpawnRequest) -> VerletResult<()> {
        let inner = Arc::clone(&self.inner);
        let settlement_store = inner.store.clone();
        let settlement_request = request.clone();
        tokio::spawn(async move {
            let outcome = inner.spawn(request).await;
            if let Err(error) = &outcome {
                let reason = format!("remote child process failed to start: {error}");
                if let Err(settlement_error) = settle_remote_spawn_failure(
                    settlement_store,
                    settlement_request,
                    reason,
                )
                .await
                {
                    eprintln!(
                        "verlet remote child bootstrap failure settlement failed: {settlement_error}"
                    );
                }
            }
            outcome
        })
            .await
            .map_err(|error| {
                VerletError::RuntimeExecution(format!(
                    "remote child spawn transaction task failed: {error}"
                ))
            })?
    }

    fn state(&self, thread_id: ThreadId) -> VerletResult<Arc<RemoteChildState>> {
        self.inner
            .states
            .read()
            .map_err(|_| remote_error("remote child state lock poisoned"))?
            .get(&thread_id)
            .cloned()
            .ok_or(VerletError::ThreadNotFound(thread_id))
    }
}

impl ProcessRemoteThreadExecutorInner {
    async fn spawn(self: Arc<Self>, request: RemoteThreadSpawnRequest) -> VerletResult<()> {
        // A duplicate projector/retry must not race the check below and boot
        // two OS children. The whole bootstrap is already detached from the
        // caller; serializing it also makes state installation the sole
        // visible completion point.
        let _spawn_guard = self.spawn_lock.lock().await;
        let thread_id = request.child.coordinates.thread_id;
        if self
            .states
            .read()
            .map_err(|_| remote_error("remote child state lock poisoned"))?
            .contains_key(&thread_id)
        {
            return Ok(());
        }

        let stream_id = EventStreamId::for_thread(&request.child.coordinates);
        let stream_grant = self
            .authority
            .grant_lease(
                &StreamPrefixScope::new(stream_id.as_str()),
                &request.dispatch_id,
                StreamLeaseLineage::default(),
            )
            .await?;
        let (_, stream_bearer_token) = self.authority.mint_credential(&stream_grant).await?;
        let queue_stream_id = remote_ingress_queue_stream_id(thread_id);
        let queue_grant = self
            .authority
            .grant_lease(
                &StreamPrefixScope::new(queue_stream_id.as_str()),
                &request.dispatch_id,
                StreamLeaseLineage::default(),
            )
            .await?;
        let (_, queue_bearer_token) = self.authority.mint_credential(&queue_grant).await?;

        self.queue
            .enqueue(queue_entry(
                &request.child.coordinates,
                request.turn_id.clone(),
                request.dispatch_id.clone(),
                &request.input,
            )?)
            .await?;

        let runtime_home = self.child_root.join(thread_id.to_string()).join("runtime");
        let state_home = self.child_root.join(thread_id.to_string()).join("state");
        std::fs::create_dir_all(&runtime_home)
            .map_err(|error| remote_error(format!("create remote child runtime home: {error}")))?;
        std::fs::create_dir_all(&state_home)
            .map_err(|error| remote_error(format!("create remote child state home: {error}")))?;
        let bootstrap = RemoteChildBootstrapV1 {
            schema: REMOTE_CHILD_BOOTSTRAP_SCHEMA_V1.to_string(),
            request: request.clone(),
            sync_endpoint: self.sync_endpoint.clone(),
            stream_lease: stream_grant,
            stream_bearer_token,
            queue_bearer_token,
            daemon_config_path: self.daemon_config_path.clone(),
            runtime_home,
            state_home,
        };
        let encoded = serde_json::to_vec(&bootstrap)
            .map_err(|error| remote_error(format!("encode remote child bootstrap: {error}")))?;
        let mut command = Command::new(&self.executable);
        command
            .arg(REMOTE_CHILD_COMMAND)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| remote_error(format!("spawn remote child process: {error}")))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| remote_error("remote child stdin pipe is unavailable"))?;
        stdin
            .write_all(&encoded)
            .await
            .map_err(|error| remote_error(format!("write remote child bootstrap: {error}")))?;
        stdin
            .shutdown()
            .await
            .map_err(|error| remote_error(format!("close remote child bootstrap: {error}")))?;

        let (status_tx, _) = watch::channel(ThreadStatus::Starting);
        let process_status = status_tx.clone();
        let process_store = self.store.clone();
        let process_request = request.clone();
        let process_task = tokio::spawn(async move {
            let outcome = child.wait().await;
            if !matches!(
                *process_status.borrow(),
                ThreadStatus::Idle | ThreadStatus::Stopped
            ) {
                let _ = process_status.send(ThreadStatus::Failed);
            }
            let reason = match outcome {
                Ok(status) => {
                    format!("remote child process exited before durable terminal state ({status})")
                }
                Err(error) => format!(
                    "remote child process wait failed before durable terminal state ({error})"
                ),
            };
            if let Err(error) =
                settle_remote_process_death(process_store, process_request, reason).await
            {
                eprintln!("verlet remote child process death settlement failed: {error}");
            }
        });
        let tail = SqliteRemoteStreamTail::new(self.store.clone());
        let tail_store = self.store.clone();
        let tail_request = request.clone();
        let tail_status = status_tx.clone();
        let tail_task = tokio::spawn(async move {
            run_parent_tail(tail, tail_store, tail_request, tail_status).await;
        });
        let state = Arc::new(RemoteChildState {
            child: request.child.clone(),
            status: status_tx,
            _process: AbortOnDrop(process_task.abort_handle()),
            _tail: AbortOnDrop(tail_task.abort_handle()),
        });
        let replaced = self
            .states
            .write()
            .map_err(|_| remote_error("remote child state lock poisoned"))?
            .insert(thread_id, state);
        if replaced.is_some() {
            return Err(remote_error(
                "remote child state was concurrently installed",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl RemoteThreadExecutor for ProcessRemoteThreadExecutor {
    async fn context(&self, thread_id: ThreadId) -> Option<ThreadContext> {
        self.inner
            .states
            .read()
            .ok()
            .and_then(|states| states.get(&thread_id).map(|state| state.child.clone()))
    }

    async fn spawn(&self, request: RemoteThreadSpawnRequest) -> VerletResult<()> {
        self.spawn_shielded(request).await
    }

    async fn submit(&self, request: RemoteThreadSubmitRequest) -> VerletResult<ThreadStatus> {
        let state = self.state(request.target_thread_id)?;
        self.inner
            .queue
            .enqueue(queue_entry(
                &state.child.coordinates,
                request.turn_id,
                request.dispatch_id,
                &request.input,
            )?)
            .await?;
        let _ = state.status.send(ThreadStatus::Running);
        Ok(ThreadStatus::Running)
    }

    async fn observe(&self, thread_id: ThreadId) -> VerletResult<RemoteThreadObservation> {
        let state = self.state(thread_id)?;
        let stream_id = EventStreamId::for_thread(&state.child.coordinates);
        let records = self
            .inner
            .store
            .read_events(&stream_id, None)
            .await
            .map_err(|error| VerletError::History(error.to_string()))?;
        let status = fold_remote_status(&records).unwrap_or(*state.status.borrow());
        let latest_output =
            latest_assistant_output(&self.inner.store, &state.child.coordinates).await?;
        Ok(RemoteThreadObservation {
            status,
            latest_output,
        })
    }

    async fn wait(
        &self,
        thread_id: ThreadId,
        timeout_ms: Option<u64>,
    ) -> VerletResult<RemoteThreadWaitObservation> {
        let state = self.state(thread_id)?;
        let mut status = state.status.subscribe();
        let wait = async {
            loop {
                if matches!(
                    *status.borrow(),
                    ThreadStatus::Idle | ThreadStatus::Stopped | ThreadStatus::Failed
                ) {
                    break;
                }
                if status.changed().await.is_err() {
                    break;
                }
            }
        };
        let timed_out = match timeout_ms {
            Some(timeout_ms) => tokio::time::timeout(Duration::from_millis(timeout_ms), wait)
                .await
                .is_err(),
            None => {
                wait.await;
                false
            }
        };
        Ok(RemoteThreadWaitObservation {
            observation: self.observe(thread_id).await?,
            timed_out,
        })
    }
}

async fn run_parent_tail(
    tail: SqliteRemoteStreamTail,
    store: SqliteSessionStore,
    request: RemoteThreadSpawnRequest,
    status: watch::Sender<ThreadStatus>,
) {
    let stream_id = EventStreamId::for_thread(&request.child.coordinates);
    let mut cursor = RemoteStreamTailCursor {
        stream_id: stream_id.clone(),
        cursor: None,
    };
    let mut joined = false;
    loop {
        match tail.poll(&cursor).await {
            Ok(page) => {
                let next = page.next;
                let mut page_folded = true;
                for record in page.records {
                    if let Some(folded) = fold_remote_status(std::slice::from_ref(&record)) {
                        let _ = status.send(folded);
                    }
                    if joined {
                        continue;
                    }
                    match settle_remote_terminal_record(store.clone(), request.clone(), record)
                        .await
                    {
                        Ok(true) => joined = true,
                        Ok(false) => {}
                        Err(error) => {
                            eprintln!("verlet remote tail failed to fold terminal record: {error}");
                            page_folded = false;
                            break;
                        }
                    }
                }
                // Advance only after every first-wins fold in this snapshot
                // succeeded. A transient parent-store failure must replay the
                // durable child terminal instead of skipping it forever.
                if page_folded {
                    cursor = next;
                }
            }
            Err(error) => {
                // Isolation is per child task: one malformed/diverged stream
                // reports and retries without stopping any other tail.
                eprintln!("verlet remote tail poll failed for {stream_id}: {error}");
            }
        }
        tokio::time::sleep(PARENT_TAIL_POLL_INTERVAL).await;
    }
}

fn remote_parent_coordinates(
    request: &RemoteThreadSpawnRequest,
) -> VerletResult<ThreadCoordinates> {
    let parent_thread_id = request
        .child
        .parent_thread_id
        .ok_or_else(|| remote_error("remote child bootstrap requires parent thread topology"))?;
    let mut parent = request.child.coordinates.clone();
    parent.thread_id = parent_thread_id;
    Ok(parent)
}

#[allow(clippy::too_many_arguments)]
async fn append_remote_join_shielded(
    store: SqliteSessionStore,
    request: RemoteThreadSpawnRequest,
    terminal_state: ThreadTerminalState,
    result_digest: Option<String>,
    reason: Option<String>,
    source_event: Option<(EventStreamId, crate::EventRecordId)>,
    discharged_by: &'static str,
    function: &'static str,
) -> VerletResult<bool> {
    let parent = remote_parent_coordinates(&request)?;
    tokio::spawn(async move {
        append_thread_joined_first_wins(
            &store,
            parent,
            request.child.coordinates,
            request.spawned_event_id,
            terminal_state,
            result_digest,
            reason,
            source_event,
            discharged_by,
            function,
        )
        .await
        .map(|joined| joined.appended)
    })
    .await
    .map_err(|error| remote_error(format!("remote child join settlement task failed: {error}")))?
}

/// Fold one durable terminal for the spawn-dispatched turn. The boolean says
/// whether the record named that turn; the shared durable fence may already
/// have been won by process-death settlement or startup recovery.
async fn settle_remote_terminal_record(
    store: SqliteSessionStore,
    request: RemoteThreadSpawnRequest,
    record: crate::EventRecord,
) -> VerletResult<bool> {
    let Some(terminal) = project_child_terminal_record(&record, &request.turn_id) else {
        return Ok(false);
    };
    append_remote_join_shielded(
        store,
        request,
        terminal.state,
        terminal.outcome_reason.clone(),
        terminal.outcome_reason,
        Some((terminal.stream_id, terminal.event_id)),
        "tail:remote-thread",
        "remote_thread_join/v1",
    )
    .await?;
    Ok(true)
}

async fn settle_remote_process_death(
    store: SqliteSessionStore,
    request: RemoteThreadSpawnRequest,
    reason: String,
) -> VerletResult<bool> {
    append_remote_join_shielded(
        store,
        request,
        ThreadTerminalState::Failed,
        Some(reason.clone()),
        Some(reason),
        None,
        "monitor:remote-child-process",
        "remote_child_process_wait/v1",
    )
    .await
}

async fn settle_remote_spawn_failure(
    store: SqliteSessionStore,
    request: RemoteThreadSpawnRequest,
    reason: String,
) -> VerletResult<bool> {
    append_remote_join_shielded(
        store,
        request,
        ThreadTerminalState::Failed,
        Some(reason.clone()),
        Some(reason),
        None,
        "executor:remote-thread",
        "remote_thread_spawn/v1",
    )
    .await
}

fn queue_entry(
    target: &ThreadCoordinates,
    turn_id: String,
    dispatch_id: verlet_runtime_contracts::DispatchId,
    input: &TurnInput,
) -> VerletResult<RemoteIngressQueueEntryV1> {
    let target_thread_id = target.thread_id;
    let source = IoSource::new("cooldis.remote", "ingress");
    let mut envelope = IngressEnvelope::new(
        source.clone(),
        IoConversation::new(target_thread_id.to_string(), ConversationKind::System),
        IngressContent::text(input.text_projection()),
        0,
    )
    .with_dedupe_key(IoDedupeKey::for_source(&source, dispatch_id.as_str()))
    .with_delivery(IoDelivery::new(dispatch_id.to_string()))
    .with_principal(IoPrincipal::new(
        target.tenant_id.clone(),
        target.user_id.clone(),
        format!("remote:{dispatch_id}"),
    ));
    envelope.id = format!("remote-ingress-{}", sha256_hex(dispatch_id.as_str()));
    envelope
        .metadata
        .insert(REMOTE_TURN_ID_METADATA.to_string(), turn_id);
    envelope.metadata.insert(
        REMOTE_INPUT_METADATA.to_string(),
        serde_json::to_string(input)
            .map_err(|error| remote_error(format!("encode remote turn input: {error}")))?,
    );
    Ok(RemoteIngressQueueEntryV1 {
        schema: SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1.to_string(),
        dispatch_id,
        target_thread_id,
        envelope,
        // Queue identity must be byte-stable across retries. The durable row
        // sequence is the authoritative enqueue order; this field therefore
        // carries the stable dispatch epoch rather than a retry-local clock.
        enqueued_at_ms: 0,
    })
}

fn fold_remote_status(records: &[crate::EventRecord]) -> Option<ThreadStatus> {
    records.iter().rev().find_map(|record| match record.kind {
        EventKind::TurnCompleted | EventKind::LoopCompleted => Some(ThreadStatus::Idle),
        EventKind::LoopDenied | EventKind::LoopBlocked | EventKind::LoopBudgetExhausted => {
            Some(ThreadStatus::Failed)
        }
        EventKind::TurnSubmitted => Some(ThreadStatus::Running),
        _ => None,
    })
}

async fn latest_assistant_output(
    store: &SqliteSessionStore,
    coordinates: &ThreadCoordinates,
) -> VerletResult<Option<String>> {
    let context = store
        .build_context(coordinates)
        .await
        .map_err(|error| VerletError::History(error.to_string()))?;
    Ok(context.entries.iter().rev().find_map(|entry| {
        if entry.coordinates.thread_id != coordinates.thread_id {
            return None;
        }
        let (SessionEntryKind::Message {
            message: CanonicalMessage::Assistant { content, .. },
        }
        | SessionEntryKind::CustomContextMessage {
            message: CanonicalMessage::Assistant { content, .. },
        }) = &entry.kind
        else {
            return None;
        };
        let text = content
            .iter()
            .filter_map(|content| match content {
                CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        (!text.is_empty()).then_some(text)
    }))
}

pub(crate) async fn run_remote_child(
    mut app_config: VerletAppServerConfig,
    bootstrap: RemoteChildBootstrapV1,
) -> VerletResult<()> {
    if bootstrap.schema != REMOTE_CHILD_BOOTSTRAP_SCHEMA_V1 {
        return Err(remote_error("unsupported remote child bootstrap schema"));
    }
    reject_remote_workspace_binding(bootstrap.request.bind_payload.as_ref())?;
    #[cfg(unix)]
    // SAFETY: getppid has no pointer arguments or caller-side invariants.
    let parent_process_id = unsafe { libc::getppid() };
    app_config.runtime_home = bootstrap.runtime_home.clone();
    app_config.state_home = bootstrap.state_home.clone();
    let app = VerletAppServer::new_local(app_config).await?;
    let supervisor = app.supervisor();
    let child = &bootstrap.request.child;
    let parent_thread_id = child
        .parent_thread_id
        .ok_or_else(|| remote_error("remote child bootstrap requires parent thread topology"))?;
    let _parent = match supervisor
        .get_thread(&child.coordinates.tenant_id, parent_thread_id)
        .await
    {
        Ok(parent) => parent,
        Err(VerletError::ThreadNotFound(_)) => {
            supervisor
                .start_thread_with_id(
                    ThreadStartRequest {
                        tenant_id: child.coordinates.tenant_id.clone(),
                        user_id: child.coordinates.user_id.clone(),
                        session_id: child.coordinates.session_id.clone(),
                        topology: Default::default(),
                        metadata: Default::default(),
                    },
                    parent_thread_id,
                )
                .await?
        }
        Err(error) => return Err(error),
    };
    let child_handle = match supervisor
        .get_thread(&child.coordinates.tenant_id, child.coordinates.thread_id)
        .await
    {
        Ok(child) => child,
        Err(VerletError::ThreadNotFound(_)) => {
            supervisor
                .start_thread_with_id(
                    ThreadStartRequest {
                        tenant_id: child.coordinates.tenant_id.clone(),
                        user_id: child.coordinates.user_id.clone(),
                        session_id: child.coordinates.session_id.clone(),
                        topology: child.topology.clone(),
                        metadata: child.metadata.clone(),
                    },
                    child.coordinates.thread_id,
                )
                .await?
        }
        Err(error) => return Err(error),
    };
    if let (Some(compile), Some(bind)) = (
        bootstrap.request.compile_payload.clone(),
        bootstrap.request.bind_payload.clone(),
    ) {
        child_handle
            .record_remote_manifest_receipts(compile, bind)
            .await?;
    }

    let local_store = SqliteSessionStore::open(app.session_store_path())
        .await
        .map_err(|error| VerletError::History(error.to_string()))?;
    init_child_cursor_schema(local_store.clone()).await?;
    let state_store = Arc::new(SqlitePropagationStateStore::new(local_store.clone()).await?);
    let stream_id = EventStreamId::for_thread(&child.coordinates);
    let mut propagation_state = match state_store.load(&stream_id).await? {
        Some(state) => state,
        None => {
            let state = StreamPropagationState {
                stream_id: stream_id.clone(),
                lease: bootstrap.stream_lease.clone(),
                pushed_through: None,
            };
            state_store.persist(&state).await?;
            state
        }
    };
    let http = Arc::new(HttpSyncClient::new(bootstrap.sync_endpoint)?);
    let propagator = LocalFirstStreamPropagator::new(
        local_store.clone(),
        http.clone(),
        http.clone(),
        http.clone(),
        state_store,
        bootstrap.stream_bearer_token,
        Arc::new(SystemDaemonClock),
    );
    let bridge = VerletDaemonIoBridge::from_app_server(&app);
    let queue_stream_id = remote_ingress_queue_stream_id(child.coordinates.thread_id);
    let mut queue_cursor = load_child_cursor(&local_store, child.coordinates.thread_id).await?;
    let mut endpoint_backoff = CHILD_POLL_INTERVAL;
    loop {
        #[cfg(unix)]
        if !remote_parent_process_is_alive(parent_process_id) {
            return Err(remote_error(
                "remote child observed its parent daemon process exit",
            ));
        }
        let records = match http
            .pull_after(
                &bootstrap.queue_bearer_token,
                &queue_stream_id,
                queue_cursor.clone(),
            )
            .await
        {
            Ok(records) => records,
            Err(error) if is_transient_sync_error(&error) => {
                tokio::time::sleep(endpoint_backoff).await;
                endpoint_backoff = next_child_retry(endpoint_backoff);
                continue;
            }
            Err(error) => return Err(error),
        };
        let mut retry_queue_delivery = false;
        for record in records {
            let entry = serde_json::from_value::<RemoteIngressQueueEntryV1>(record.payload.clone())
                .map_err(|error| {
                    remote_error(format!("decode remote child queue entry: {error}"))
                })?;
            if entry.target_thread_id != child.coordinates.thread_id {
                return Err(remote_error(
                    "remote child queue entry escaped its target prefix",
                ));
            }
            let turn_id = entry
                .envelope
                .metadata
                .get(REMOTE_TURN_ID_METADATA)
                .cloned()
                .ok_or_else(|| remote_error("remote child queue entry is missing turn id"))?;
            let turn_input = entry
                .envelope
                .metadata
                .get(REMOTE_INPUT_METADATA)
                .map(|encoded| serde_json::from_str::<TurnInput>(encoded))
                .transpose()
                .map_err(|error| remote_error(format!("decode remote turn input: {error}")))?
                .unwrap_or_else(|| TurnInput::text(entry.envelope.content.text_projection()));
            let mut target = ResolvedIoTarget::new(
                ThreadAddress::new(
                    child.coordinates.tenant_id.clone(),
                    child.coordinates.user_id.clone(),
                    child.coordinates.session_id.clone(),
                )
                .with_thread_id(child.coordinates.thread_id.to_string()),
            );
            target.create_thread_if_missing = false;
            let mut io_input = IoTurnInput::text(turn_input.text_projection());
            io_input.metadata = turn_input.metadata.clone();
            match bridge
                .submit_durable_remote_envelope(
                    entry.envelope,
                    target,
                    AdmissionDecision::queue(turn_id, io_input),
                    1,
                )
                .await
            {
                Ok(_) => {}
                Err(
                    error @ (IoError::InvalidEnvelope(_)
                    | IoError::UnknownProtocol(_)
                    | IoError::PolicyRejected(_)),
                ) => {
                    return Err(remote_error(format!(
                        "remote child ingress rejected: {error}"
                    )));
                }
                Err(IoError::Queue(_) | IoError::Delivery(_) | IoError::Bridge(_)) => {
                    // The ordinary durable ingress lane owns dedupe. Retrying the
                    // same queue row is safe whether the failed call committed
                    // nothing or lost its response after committing the claim.
                    retry_queue_delivery = true;
                    break;
                }
            }
            match http
                .acknowledge_ingress(
                    &bootstrap.queue_bearer_token,
                    SyncIngressQueueAckRequestV1 {
                        schema: SYNC_INGRESS_QUEUE_ACK_SCHEMA_V1.to_string(),
                        target_thread_id: child.coordinates.thread_id,
                        dispatch_id: entry.dispatch_id,
                    },
                )
                .await
            {
                Ok(()) => {}
                Err(error) if is_transient_sync_error(&error) => {
                    retry_queue_delivery = true;
                    break;
                }
                Err(error) => return Err(error),
            }
            queue_cursor = Some(crate::StreamCursorV1::new(
                record.stream_id.clone(),
                record.sequence,
                record.event_id,
            ));
            persist_child_cursor(
                local_store.clone(),
                child.coordinates.thread_id,
                queue_cursor.as_ref().expect("queue cursor was assigned"),
            )
            .await?;
        }
        if retry_queue_delivery {
            tokio::time::sleep(endpoint_backoff).await;
            endpoint_backoff = next_child_retry(endpoint_backoff);
            continue;
        }
        match propagator.propagate_once(&mut propagation_state).await? {
            PropagationStep::LeaseFenced => {
                return Err(remote_error("remote child stream lease was fenced"));
            }
            PropagationStep::StreamDiverged { .. } => {
                return Err(remote_error("remote child stream diverged from parent"));
            }
            PropagationStep::Converged | PropagationStep::Advanced { .. } => {
                endpoint_backoff = CHILD_POLL_INTERVAL;
            }
            PropagationStep::EndpointUnavailable => {
                tokio::time::sleep(endpoint_backoff).await;
                endpoint_backoff = next_child_retry(endpoint_backoff);
                continue;
            }
        }
        tokio::time::sleep(CHILD_POLL_INTERVAL).await;
    }
}

fn reject_remote_workspace_binding(bind_payload: Option<&serde_json::Value>) -> VerletResult<()> {
    if bind_payload
        .and_then(|payload| payload.get("workspace"))
        .is_some_and(|workspace| !workspace.is_null())
    {
        return Err(remote_error(
            "remote child bootstrap cannot carry a local host workspace binding",
        ));
    }
    Ok(())
}

fn is_transient_sync_error(error: &VerletError) -> bool {
    matches!(error, VerletError::RuntimeExecution(_))
}

fn next_child_retry(current: Duration) -> Duration {
    current.saturating_mul(2).min(CHILD_RETRY_MAX)
}

#[cfg(unix)]
fn remote_parent_process_is_alive(expected_parent: libc::pid_t) -> bool {
    // Parentage cannot be confused by PID reuse: Unix reparents this process
    // when the daemon dies, so the current PPID must remain the one captured
    // before entering the long-lived child loop. A captured PPID of 1 is
    // ambiguous — the daemon may itself be a container's init — so the
    // monitor is disabled rather than declaring a PID-1 daemon dead.
    expected_parent <= 1 || unsafe { libc::getppid() == expected_parent }
}

async fn init_child_cursor_schema(store: SqliteSessionStore) -> VerletResult<()> {
    child_cursor_transaction(store, None).await.map(|_| ())
}

async fn load_child_cursor(
    store: &SqliteSessionStore,
    thread_id: ThreadId,
) -> VerletResult<Option<crate::StreamCursorV1>> {
    let connection = store
        .sqlite_database()
        .connect()
        .await
        .map_err(|error| VerletError::History(error.to_string()))?;
    let mut rows = connection
        .query(
            "SELECT cursor_json FROM cooldis_remote_child_queue_cursor WHERE thread_id = ?1",
            params![thread_id.to_string()],
        )
        .await
        .map_err(|error| VerletError::History(error.to_string()))?;
    rows.next()
        .await
        .map_err(|error| VerletError::History(error.to_string()))?
        .map(|row| {
            let encoded = row
                .get::<String>(0)
                .map_err(|error| VerletError::History(error.to_string()))?;
            serde_json::from_str(&encoded)
                .map_err(|error| VerletError::History(format!("decode child cursor: {error}")))
        })
        .transpose()
}

async fn persist_child_cursor(
    store: SqliteSessionStore,
    thread_id: ThreadId,
    cursor: &crate::StreamCursorV1,
) -> VerletResult<()> {
    let encoded = serde_json::to_string(cursor)
        .map_err(|error| VerletError::History(format!("encode child cursor: {error}")))?;
    child_cursor_transaction(store, Some((thread_id, encoded)))
        .await
        .map(|_| ())
}

async fn child_cursor_transaction(
    store: SqliteSessionStore,
    update: Option<(ThreadId, String)>,
) -> VerletResult<()> {
    tokio::spawn(async move {
        let database = store.sqlite_database();
        let mut connection = database
            .connect()
            .await
            .map_err(|error| VerletError::History(error.to_string()))?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(|error| VerletError::History(error.to_string()))?;
        transaction
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS cooldis_remote_child_queue_cursor (
                    thread_id TEXT PRIMARY KEY NOT NULL,
                    cursor_json TEXT NOT NULL
                );",
            )
            .await
            .map_err(|error| VerletError::History(error.to_string()))?;
        if let Some((thread_id, encoded)) = update {
            transaction
                .execute(
                    "INSERT INTO cooldis_remote_child_queue_cursor (thread_id, cursor_json)
                     VALUES (?1, ?2)
                     ON CONFLICT(thread_id) DO UPDATE SET cursor_json = excluded.cursor_json",
                    params![thread_id.to_string(), encoded],
                )
                .await
                .map_err(|error| VerletError::History(error.to_string()))?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| VerletError::History(error.to_string()))?;
        Ok(())
    })
    .await
    .map_err(|error| {
        VerletError::History(format!(
            "remote child cursor transaction task failed: {error}"
        ))
    })?
}

fn sha256_hex(value: &str) -> String {
    use std::fmt::Write as _;

    Sha256::digest(value.as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut encoded, byte| {
            let _ = write!(encoded, "{byte:02x}");
            encoded
        })
}

fn remote_error(message: impl Into<String>) -> VerletError {
    VerletError::RuntimeExecution(message.into())
}

pub(crate) fn is_remote_child_command(command: &std::ffi::OsStr) -> bool {
    command == REMOTE_CHILD_COMMAND
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EventRecord, EventRecordId, EventSequence, NewEventRecord, ThreadJoinedPayload,
        ThreadTopology, control_stream_id,
    };

    fn request_fixture() -> RemoteThreadSpawnRequest {
        let parent = ThreadCoordinates::new("tenant", "user", "session");
        let child = crate::ThreadContext::with_topology_and_metadata(
            ThreadCoordinates::new("tenant", "user", "session"),
            ThreadTopology::spawned_from(parent.thread_id),
            Default::default(),
        );
        RemoteThreadSpawnRequest {
            child,
            task_name: Some("remote-test".to_string()),
            turn_id: "spawn-turn".to_string(),
            dispatch_id: verlet_runtime_contracts::DispatchId::new("spawn-dispatch"),
            input: TurnInput::text("run"),
            spawned_event_id: EventRecordId::new(),
            compile_payload: None,
            bind_payload: None,
        }
    }

    #[test]
    fn remote_child_bootstrap_rejects_workspace_before_constructing_the_runtime() {
        let bind = serde_json::json!({
            "placement": {"target": "remote"},
            "workspace": {
                "guest_path": "/work",
                "host_path": "/tmp/remote-workspace",
                "mode": "rw"
            }
        });

        let error = reject_remote_workspace_binding(Some(&bind)).unwrap_err();

        assert!(error.to_string().contains("cannot carry"));
    }

    fn child_record(
        request: &RemoteThreadSpawnRequest,
        kind: EventKind,
        turn_id: &str,
    ) -> EventRecord {
        let stream_id = EventStreamId::for_thread(&request.child.coordinates);
        EventRecord::from_new(
            stream_id,
            EventSequence::new(1),
            NewEventRecord::witnessed(
                request.child.coordinates.clone(),
                kind,
                serde_json::json!({"turn_id": turn_id}),
            ),
        )
    }

    async fn joined_payloads(
        store: &SqliteSessionStore,
        request: &RemoteThreadSpawnRequest,
    ) -> Vec<ThreadJoinedPayload> {
        let parent = remote_parent_coordinates(request).unwrap();
        store
            .read_events(&control_stream_id(&parent), None)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == EventKind::ThreadJoined)
            .map(|event| serde_json::from_value(event.payload).unwrap())
            .collect()
    }

    #[test]
    fn loop_completed_folds_remote_status_to_idle() {
        let request = request_fixture();
        let record = child_record(&request, EventKind::LoopCompleted, "spawn-turn");
        assert_eq!(fold_remote_status(&[record]), Some(ThreadStatus::Idle));
    }

    #[tokio::test]
    async fn spawn_turn_failure_terminals_settle_with_emo426_state_projection() {
        for (kind, expected) in [
            (EventKind::LoopDenied, ThreadTerminalState::Failed),
            (EventKind::LoopBlocked, ThreadTerminalState::Failed),
            (
                EventKind::LoopBudgetExhausted,
                ThreadTerminalState::BudgetExhausted,
            ),
        ] {
            let store = SqliteSessionStore::in_memory().await.unwrap();
            let request = request_fixture();
            let record = child_record(&request, kind, "spawn-turn");
            assert!(
                settle_remote_terminal_record(store.clone(), request.clone(), record)
                    .await
                    .unwrap()
            );
            let joined = joined_payloads(&store, &request).await;
            assert_eq!(joined.len(), 1);
            assert_eq!(joined[0].terminal_state, expected);
        }
    }

    #[tokio::test]
    async fn process_death_late_terminal_duplicate_tail_and_recovery_share_one_join_fence() {
        let store = SqliteSessionStore::in_memory().await.unwrap();
        let request = request_fixture();
        let terminal = child_record(&request, EventKind::TurnCompleted, "spawn-turn");
        let parent = remote_parent_coordinates(&request).unwrap();
        let reason = "remote child process died before durable terminal state".to_string();

        let death = settle_remote_process_death(store.clone(), request.clone(), reason.clone());
        let tail = settle_remote_terminal_record(store.clone(), request.clone(), terminal.clone());
        let duplicate_tail =
            settle_remote_terminal_record(store.clone(), request.clone(), terminal.clone());
        let recovery = append_thread_joined_first_wins(
            &store,
            parent,
            request.child.coordinates.clone(),
            request.spawned_event_id,
            ThreadTerminalState::Failed,
            Some(reason.clone()),
            Some(reason),
            Some((terminal.stream_id.clone(), terminal.id)),
            "recovery:startup-sweep",
            "thread_join_recovery/v1",
        );
        let (death, tail, duplicate_tail, recovery) =
            tokio::join!(death, tail, duplicate_tail, recovery);
        death.unwrap();
        tail.unwrap();
        duplicate_tail.unwrap();
        recovery.unwrap();

        assert_eq!(joined_payloads(&store, &request).await.len(), 1);

        let death_only_store = SqliteSessionStore::in_memory().await.unwrap();
        assert!(
            settle_remote_process_death(
                death_only_store.clone(),
                request.clone(),
                "remote child process exited with status 17".to_string(),
            )
            .await
            .unwrap()
        );
        let death_join = joined_payloads(&death_only_store, &request).await;
        assert_eq!(death_join.len(), 1);
        assert_eq!(death_join[0].terminal_state, ThreadTerminalState::Failed);
        assert!(
            death_join[0]
                .result_digest
                .as_deref()
                .is_some_and(|value| value.contains("remote child process"))
        );
    }

    #[tokio::test]
    async fn successful_spawn_turn_settles_once_and_later_submit_turn_cannot_rejoin() {
        let store = SqliteSessionStore::in_memory().await.unwrap();
        let request = request_fixture();
        let completed = child_record(&request, EventKind::TurnCompleted, "spawn-turn");
        assert!(
            settle_remote_terminal_record(store.clone(), request.clone(), completed)
                .await
                .unwrap()
        );
        let later = child_record(&request, EventKind::LoopDenied, "later-submit-turn");
        assert!(
            !settle_remote_terminal_record(store.clone(), request.clone(), later)
                .await
                .unwrap()
        );
        let joined = joined_payloads(&store, &request).await;
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].terminal_state, ThreadTerminalState::Completed);
    }

    #[tokio::test]
    async fn detached_bootstrap_failure_settles_after_the_caller_is_cancelled() {
        let store = SqliteSessionStore::in_memory().await.unwrap();
        let authority = Arc::new(
            SqliteStreamLeaseAuthority::new(
                store.clone(),
                super::super::endpoint::VerletDaemonSyncConfig::default(),
                Arc::new(SystemDaemonClock),
            )
            .await
            .unwrap(),
        );
        let child_root = std::env::temp_dir().join(format!(
            "verlet-remote-cancelled-bootstrap-{}",
            uuid::Uuid::now_v7()
        ));
        let executor = ProcessRemoteThreadExecutor::new(
            store.clone(),
            authority,
            "http://127.0.0.1:1".to_string(),
            None,
            child_root.clone(),
            child_root.join("missing-verlet-executable"),
        )
        .await
        .unwrap();
        let request = request_fixture();
        let spawn_guard = executor.inner.spawn_lock.lock().await;
        let caller_executor = executor.clone();
        let caller_request = request.clone();
        let caller = tokio::spawn(async move { caller_executor.spawn(caller_request).await });
        tokio::task::yield_now().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(spawn_guard);

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if !joined_payloads(&store, &request).await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached bootstrap failure did not settle the durable spawn");
        let joined = joined_payloads(&store, &request).await;
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].terminal_state, ThreadTerminalState::Failed);
        assert!(
            joined[0]
                .result_digest
                .as_deref()
                .is_some_and(|reason| reason.contains("failed to start"))
        );
        let _ = std::fs::remove_dir_all(child_root);
    }
}
