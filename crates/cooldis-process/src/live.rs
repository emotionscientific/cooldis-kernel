//! Instance-local async process registry.
//!
//! Entries owned by a durable dispatcher opt into terminal retention and are
//! removed only by `acknowledge_terminal` after outcome ingress settles.
//! Other callers consume terminal entries through `start`/`poll`/`write`/
//! `terminate`; abandoned non-dispatched terminals remain eligible for idle
//! cleanup. Expired running entries are always cancelled in place so their
//! final backend event remains observable before either cleanup policy runs.

use crate::{
    CooldisProcessBackend, CooldisProcessError, CooldisProcessEvent, CooldisProcessEventKind,
    CooldisProcessExitStatus, CooldisProcessHandle, CooldisProcessId, CooldisProcessOutput,
    CooldisProcessResult, CooldisProcessTerminalState, ExecutionDeadline, process_error,
};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_MAX_PROCESSES: usize = 64;
const DEFAULT_YIELD_TIME: Duration = Duration::from_millis(10);

#[derive(Clone, Debug)]
pub struct AsyncExecutionManagerConfig {
    pub idle_timeout: Duration,
    pub max_processes: usize,
}

impl Default for AsyncExecutionManagerConfig {
    fn default() -> Self {
        Self {
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_processes: DEFAULT_MAX_PROCESSES,
        }
    }
}

#[derive(Clone)]
pub struct AsyncExecutionManager {
    inner: Arc<AsyncExecutionManagerInner>,
}

struct AsyncExecutionManagerInner {
    config: AsyncExecutionManagerConfig,
    entries: Mutex<HashMap<CooldisProcessId, ProcessEntry>>,
}

struct ProcessEntry {
    process: CooldisProcessHandle,
    owner: AsyncProcessOwner,
    stdin: Option<mpsc::Sender<Vec<u8>>>,
    cancellation: CancellationToken,
    join: JoinHandle<CooldisProcessResult<()>>,
    deadline: ExecutionDeadline,
    idle_timeout: Duration,
    last_used: Instant,
    termination_reason: Option<String>,
    retain_terminal_until_acknowledged: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AsyncProcessOwner {
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub call_id: Option<String>,
    pub surface: Option<String>,
}

impl AsyncProcessOwner {
    pub fn app_server_command() -> Self {
        Self {
            surface: Some("app-server:command/exec".to_string()),
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct AsyncProcessStartRequest {
    pub process_id: Option<CooldisProcessId>,
    pub owner: AsyncProcessOwner,
    pub invocation: LiveProcessInvocation,
    pub deadline: ExecutionDeadline,
    pub idle_timeout: Option<Duration>,
    pub output_cap_bytes: usize,
    pub yield_time: Duration,
    pub retain_terminal_until_acknowledged: bool,
}

impl AsyncProcessStartRequest {
    pub fn host_command(command: Vec<String>, cwd: PathBuf) -> Self {
        Self {
            process_id: None,
            owner: AsyncProcessOwner::default(),
            invocation: LiveProcessInvocation::HostCommand {
                command,
                cwd,
                env: BTreeMap::new(),
                pipe_stdin: false,
            },
            deadline: ExecutionDeadline::from_now(Duration::from_secs(30)),
            idle_timeout: None,
            output_cap_bytes: 1024 * 1024,
            yield_time: DEFAULT_YIELD_TIME,
            retain_terminal_until_acknowledged: false,
        }
    }

    pub fn virtual_bash_script(script: impl Into<String>) -> Self {
        Self {
            process_id: None,
            owner: AsyncProcessOwner::default(),
            invocation: LiveProcessInvocation::VirtualBashScript {
                script: script.into(),
            },
            deadline: ExecutionDeadline::from_now(Duration::from_secs(30)),
            idle_timeout: None,
            output_cap_bytes: 1024 * 1024,
            yield_time: DEFAULT_YIELD_TIME,
            retain_terminal_until_acknowledged: false,
        }
    }

    pub fn with_owner(mut self, owner: AsyncProcessOwner) -> Self {
        self.owner = owner;
        self
    }

    pub fn with_process_id(mut self, process_id: CooldisProcessId) -> Self {
        self.process_id = Some(process_id);
        self
    }

    pub fn with_deadline(mut self, deadline: ExecutionDeadline) -> Self {
        self.deadline = deadline;
        self
    }

    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    pub fn with_output_cap_bytes(mut self, output_cap_bytes: usize) -> Self {
        self.output_cap_bytes = output_cap_bytes;
        self
    }

    pub fn with_yield_time(mut self, yield_time: Duration) -> Self {
        self.yield_time = yield_time;
        self
    }

    /// Retains terminal state until its durable owning surface explicitly
    /// acknowledges outcome settlement.
    pub fn retain_terminal_until_acknowledged(mut self) -> Self {
        self.retain_terminal_until_acknowledged = true;
        self
    }

    pub fn with_env(mut self, env: BTreeMap<String, Option<String>>) -> Self {
        if let LiveProcessInvocation::HostCommand { env: host_env, .. } = &mut self.invocation {
            *host_env = env;
        }
        self
    }

    pub fn pipe_stdin(mut self, pipe_stdin: bool) -> Self {
        if let LiveProcessInvocation::HostCommand {
            pipe_stdin: host_pipe_stdin,
            ..
        } = &mut self.invocation
        {
            *host_pipe_stdin = pipe_stdin;
        }
        self
    }
}

#[derive(Clone, Debug)]
pub enum LiveProcessInvocation {
    HostCommand {
        command: Vec<String>,
        cwd: PathBuf,
        env: BTreeMap<String, Option<String>>,
        pipe_stdin: bool,
    },
    VirtualBashScript {
        script: String,
    },
    VirtualLabel {
        label: String,
    },
}

impl LiveProcessInvocation {
    fn label(&self) -> String {
        match self {
            Self::HostCommand { command, .. } if command.is_empty() => {
                "<empty command>".to_string()
            }
            Self::HostCommand { command, .. } => command.join(" "),
            Self::VirtualBashScript { script } => script.clone(),
            Self::VirtualLabel { label } => label.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LiveProcessStartRequest {
    pub invocation: LiveProcessInvocation,
    pub deadline: ExecutionDeadline,
    pub output_cap_bytes: usize,
}

#[derive(Debug)]
pub struct LiveProcessSpawn {
    pub stdin: Option<mpsc::Sender<Vec<u8>>>,
    pub join: JoinHandle<CooldisProcessResult<()>>,
}

#[async_trait]
pub trait LiveProcessBackend: Send + Sync + 'static {
    fn backend_kind(&self) -> CooldisProcessBackend;

    async fn start(
        &self,
        request: LiveProcessStartRequest,
        process: CooldisProcessHandle,
        cancellation: CancellationToken,
    ) -> CooldisProcessResult<LiveProcessSpawn>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsyncProcessOutcome {
    pub snapshot: AsyncProcessSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsyncProcessSnapshot {
    pub process_id: Option<CooldisProcessId>,
    pub backend: CooldisProcessBackend,
    pub label: String,
    pub status: ProcessSnapshotStatus,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub events: Vec<CooldisProcessEvent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessSnapshotStatus {
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

impl ProcessSnapshotStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
        }
    }
}

impl AsyncExecutionManager {
    pub fn new(config: AsyncExecutionManagerConfig) -> Self {
        Self {
            inner: Arc::new(AsyncExecutionManagerInner {
                config,
                entries: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub async fn start(
        &self,
        backend: Arc<dyn LiveProcessBackend>,
        request: AsyncProcessStartRequest,
    ) -> CooldisProcessResult<AsyncProcessOutcome> {
        self.cleanup_expired().await;
        let process = CooldisProcessHandle::with_process_id(
            request.process_id.unwrap_or_default(),
            backend.backend_kind(),
            request.invocation.label(),
        );
        let process_id = process.process_id();
        let cancellation = CancellationToken::new();
        let spawn = backend
            .start(
                LiveProcessStartRequest {
                    invocation: request.invocation,
                    deadline: request.deadline.clone(),
                    output_cap_bytes: request.output_cap_bytes,
                },
                process.clone(),
                cancellation.clone(),
            )
            .await?;

        {
            let mut entries = self.inner.entries.lock().await;
            if entries.len() >= self.inner.config.max_processes {
                cancellation.cancel();
                return Err(process_error("async process limit reached"));
            }
            entries.insert(
                process_id,
                ProcessEntry {
                    process: process.clone(),
                    owner: request.owner,
                    stdin: spawn.stdin,
                    cancellation,
                    join: spawn.join,
                    deadline: request.deadline,
                    idle_timeout: request
                        .idle_timeout
                        .unwrap_or(self.inner.config.idle_timeout),
                    last_used: Instant::now(),
                    termination_reason: None,
                    retain_terminal_until_acknowledged: request.retain_terminal_until_acknowledged,
                },
            );
        }

        self.wait_for_snapshot(process_id, request.yield_time, request.output_cap_bytes)
            .await
    }

    pub async fn poll(
        &self,
        process_id: CooldisProcessId,
        yield_time: Duration,
        max_output_bytes: usize,
    ) -> CooldisProcessResult<AsyncProcessOutcome> {
        self.touch(process_id).await?;
        self.wait_for_snapshot(process_id, yield_time, max_output_bytes)
            .await
    }

    pub async fn write(
        &self,
        process_id: CooldisProcessId,
        bytes: Vec<u8>,
        yield_time: Duration,
        max_output_bytes: usize,
    ) -> CooldisProcessResult<AsyncProcessOutcome> {
        let stdin = {
            let mut entries = self.inner.entries.lock().await;
            let entry = entries
                .get_mut(&process_id)
                .ok_or_else(|| process_error(format!("process {process_id} was not found")))?;
            entry.last_used = Instant::now();
            entry.stdin.clone().ok_or_else(|| {
                process_error(format!(
                    "process {process_id} does not support stdin writes"
                ))
            })?
        };
        stdin
            .send(bytes)
            .await
            .map_err(|_| process_error(format!("process {process_id} stdin is closed")))?;
        self.wait_for_snapshot(process_id, yield_time, max_output_bytes)
            .await
    }

    pub async fn terminate(
        &self,
        process_id: CooldisProcessId,
        reason: impl Into<String>,
        yield_time: Duration,
        max_output_bytes: usize,
    ) -> CooldisProcessResult<AsyncProcessOutcome> {
        let cancellation = {
            let mut entries = self.inner.entries.lock().await;
            let entry = entries
                .get_mut(&process_id)
                .ok_or_else(|| process_error(format!("process {process_id} was not found")))?;
            entry.last_used = Instant::now();
            entry.termination_reason = Some(reason.into());
            entry.cancellation.clone()
        };
        cancellation.cancel();
        self.wait_for_snapshot(process_id, yield_time, max_output_bytes)
            .await
    }

    pub async fn subscribe(
        &self,
        process_id: CooldisProcessId,
    ) -> CooldisProcessResult<tokio::sync::broadcast::Receiver<CooldisProcessEvent>> {
        let entries = self.inner.entries.lock().await;
        entries
            .get(&process_id)
            .map(|entry| entry.process.subscribe())
            .ok_or_else(|| process_error(format!("process {process_id} was not found")))
    }

    /// Returns the current fold of a live registry entry without consuming
    /// terminal state. Owning surfaces use this for dispatch-id retries.
    pub async fn snapshot(
        &self,
        process_id: CooldisProcessId,
        max_output_bytes: usize,
    ) -> CooldisProcessResult<AsyncProcessOutcome> {
        let (process, termination_reason) = {
            let entries = self.inner.entries.lock().await;
            let entry = entries
                .get(&process_id)
                .ok_or_else(|| process_error(format!("process {process_id} was not found")))?;
            (entry.process.clone(), entry.termination_reason.clone())
        };
        let mut snapshot = snapshot_from_process(&process, max_output_bytes);
        apply_termination_reason(&mut snapshot, termination_reason.as_deref());
        Ok(AsyncProcessOutcome { snapshot })
    }

    /// Removes a terminal registry entry only after the owning surface has
    /// received a durable acknowledgement for its outcome ingress. Running
    /// entries fail closed and remain registered.
    pub async fn acknowledge_terminal(
        &self,
        process_id: CooldisProcessId,
    ) -> CooldisProcessResult<bool> {
        let mut entries = self.inner.entries.lock().await;
        let Some(entry) = entries.get(&process_id) else {
            return Ok(false);
        };
        if entry.process.output().terminal.is_none() {
            return Err(process_error(format!(
                "process {process_id} is not terminal"
            )));
        }
        entries.remove(&process_id);
        Ok(true)
    }

    async fn touch(&self, process_id: CooldisProcessId) -> CooldisProcessResult<()> {
        let mut entries = self.inner.entries.lock().await;
        let entry = entries
            .get_mut(&process_id)
            .ok_or_else(|| process_error(format!("process {process_id} was not found")))?;
        entry.last_used = Instant::now();
        Ok(())
    }

    async fn wait_for_snapshot(
        &self,
        process_id: CooldisProcessId,
        yield_time: Duration,
        max_output_bytes: usize,
    ) -> CooldisProcessResult<AsyncProcessOutcome> {
        let (process, retain_terminal_until_acknowledged) = {
            let entries = self.inner.entries.lock().await;
            entries
                .get(&process_id)
                .map(|entry| {
                    (
                        entry.process.clone(),
                        entry.retain_terminal_until_acknowledged,
                    )
                })
                .ok_or_else(|| process_error(format!("process {process_id} was not found")))?
        };
        let mut events = process.subscribe();
        let deadline = Instant::now() + yield_time;
        loop {
            let mut snapshot = snapshot_from_process(&process, max_output_bytes);
            let termination_reason = self
                .inner
                .entries
                .lock()
                .await
                .get(&process_id)
                .and_then(|entry| entry.termination_reason.clone());
            apply_termination_reason(&mut snapshot, termination_reason.as_deref());
            if snapshot.status != ProcessSnapshotStatus::Running || Instant::now() >= deadline {
                if snapshot.status != ProcessSnapshotStatus::Running
                    && !retain_terminal_until_acknowledged
                {
                    self.inner.entries.lock().await.remove(&process_id);
                }
                return Ok(AsyncProcessOutcome { snapshot });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(AsyncProcessOutcome { snapshot });
            }
            let _ = tokio::time::timeout(remaining, events.recv()).await;
        }
    }

    async fn cleanup_expired(&self) {
        let now = Instant::now();
        let mut remove = Vec::new();
        let entries = self.inner.entries.lock().await;
        for (process_id, entry) in entries.iter() {
            let output = entry.process.output();
            let idle_expired = now.duration_since(entry.last_used) > entry.idle_timeout;
            let deadline_expired = entry.deadline.remaining().is_zero();
            let _ = &entry.owner;
            let _ = &entry.join;
            if output.terminal.is_some() {
                if idle_expired && !entry.retain_terminal_until_acknowledged {
                    remove.push(*process_id);
                }
            } else if idle_expired || deadline_expired {
                entry.cancellation.cancel();
            }
        }
        drop(entries);
        if !remove.is_empty() {
            let mut entries = self.inner.entries.lock().await;
            for process_id in remove {
                entries.remove(&process_id);
            }
        }
    }
}

impl Default for AsyncExecutionManager {
    fn default() -> Self {
        Self::new(AsyncExecutionManagerConfig::default())
    }
}

#[derive(Clone, Debug, Default)]
pub struct HostBashLiveBackend;

#[async_trait]
impl LiveProcessBackend for HostBashLiveBackend {
    fn backend_kind(&self) -> CooldisProcessBackend {
        CooldisProcessBackend::HostBash
    }

    async fn start(
        &self,
        request: LiveProcessStartRequest,
        process: CooldisProcessHandle,
        cancellation: CancellationToken,
    ) -> CooldisProcessResult<LiveProcessSpawn> {
        let LiveProcessInvocation::HostCommand {
            command,
            cwd,
            env,
            pipe_stdin,
        } = request.invocation
        else {
            return Err(process_error("host bash backend requires a host command"));
        };
        if command.is_empty() {
            return Err(process_error("host command requires a non-empty argv"));
        }

        let mut child_command = Command::new(&command[0]);
        child_command
            .args(&command[1..])
            .current_dir(cwd)
            .stdin(if pipe_stdin {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for (key, value) in env {
            if let Some(value) = value {
                child_command.env(key, value);
            } else {
                child_command.env_remove(key);
            }
        }
        #[cfg(unix)]
        unsafe {
            child_command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let mut child = child_command.spawn().map_err(process_error)?;
        process.record(CooldisProcessEventKind::Started {
            command: Some(command.join(" ")),
        });
        let child_id = child.id();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdin = child.stdin.take();
        let stdout_cap = Arc::new(StdMutex::new(StreamCapState::default()));
        let stderr_cap = Arc::new(StdMutex::new(StreamCapState::default()));
        let stdout_task = stdout.map(|stdout| {
            spawn_reader(
                stdout,
                process.clone(),
                ProcessStream::Stdout,
                request.output_cap_bytes,
                stdout_cap,
            )
        });
        let stderr_task = stderr.map(|stderr| {
            spawn_reader(
                stderr,
                process.clone(),
                ProcessStream::Stderr,
                request.output_cap_bytes,
                stderr_cap,
            )
        });
        let (stdin_tx, stdin_task) = if let Some(mut stdin) = stdin {
            let (tx, mut rx) = mpsc::channel::<Vec<u8>>(16);
            let task: JoinHandle<CooldisProcessResult<()>> = tokio::spawn(async move {
                while let Some(bytes) = rx.recv().await {
                    stdin.write_all(&bytes).await.map_err(process_error)?;
                    stdin.flush().await.map_err(process_error)?;
                }
                Ok::<(), CooldisProcessError>(())
            });
            (Some(tx), Some(task))
        } else {
            (None, None)
        };

        let join = tokio::spawn(async move {
            let terminal =
                wait_for_host_child(&mut child, child_id, request.deadline, cancellation.clone())
                    .await;
            if let Some(task) = stdout_task {
                let _ = task.await;
            }
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            if let Some(task) = stdin_task {
                task.abort();
            }
            process.record(terminal);
            Ok(())
        });

        Ok(LiveProcessSpawn {
            stdin: stdin_tx,
            join,
        })
    }
}

#[derive(Default)]
struct StreamCapState {
    written: usize,
    truncated: bool,
}

#[derive(Clone, Copy)]
enum ProcessStream {
    Stdout,
    Stderr,
}

fn spawn_reader<R>(
    mut reader: R,
    process: CooldisProcessHandle,
    stream: ProcessStream,
    max_output_bytes: usize,
    state: Arc<StdMutex<StreamCapState>>,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buffer = [0_u8; 8192];
        loop {
            let Ok(read) = reader.read(&mut buffer).await else {
                break;
            };
            if read == 0 {
                break;
            }
            record_capped_stream_bytes(&process, stream, &buffer[..read], max_output_bytes, &state);
        }
    })
}

fn record_capped_stream_bytes(
    process: &CooldisProcessHandle,
    stream: ProcessStream,
    bytes: &[u8],
    max_output_bytes: usize,
    state: &Arc<StdMutex<StreamCapState>>,
) {
    let mut state = state.lock().unwrap();
    if state.written >= max_output_bytes {
        if !state.truncated {
            state.truncated = true;
            process.record(CooldisProcessEventKind::OutputTruncated {
                stdout: matches!(stream, ProcessStream::Stdout),
                stderr: matches!(stream, ProcessStream::Stderr),
            });
        }
        return;
    }
    let remaining = max_output_bytes - state.written;
    let take = remaining.min(bytes.len());
    if take > 0 {
        let payload = bytes[..take].to_vec();
        match stream {
            ProcessStream::Stdout => {
                process.record(CooldisProcessEventKind::Stdout { bytes: payload })
            }
            ProcessStream::Stderr => {
                process.record(CooldisProcessEventKind::Stderr { bytes: payload })
            }
        };
        state.written += take;
    }
    if take < bytes.len() && !state.truncated {
        state.truncated = true;
        process.record(CooldisProcessEventKind::OutputTruncated {
            stdout: matches!(stream, ProcessStream::Stdout),
            stderr: matches!(stream, ProcessStream::Stderr),
        });
    }
}

async fn wait_for_host_child(
    child: &mut tokio::process::Child,
    child_id: Option<u32>,
    deadline: ExecutionDeadline,
    cancellation: CancellationToken,
) -> CooldisProcessEventKind {
    let timeout = tokio::time::sleep(deadline.remaining());
    tokio::pin!(timeout);
    tokio::select! {
        status = child.wait() => match status {
            Ok(status) => CooldisProcessEventKind::Completed {
                status: CooldisProcessExitStatus {
                    code: status.code(),
                    success: status.success(),
                },
            },
            Err(err) => CooldisProcessEventKind::Failed {
                code: "wait_failed".to_string(),
                message: err.to_string(),
            },
        },
        _ = cancellation.cancelled() => {
            terminate_child(child, child_id).await;
            CooldisProcessEventKind::Cancelled {
                reason: "process terminated".to_string(),
            }
        }
        _ = &mut timeout => {
            terminate_child(child, child_id).await;
            CooldisProcessEventKind::TimedOut {
                timeout_ms: Some(deadline.timeout_ms()),
                message: format!("process timed out after {}ms", deadline.timeout_ms()),
            }
        }
    }
}

async fn terminate_child(child: &mut tokio::process::Child, child_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(child_id) = child_id {
        unsafe {
            libc::killpg(child_id as libc::pid_t, libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    let _ = child.kill().await;

    if tokio::time::timeout(Duration::from_millis(250), child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

fn snapshot_from_process(
    process: &CooldisProcessHandle,
    max_output_bytes: usize,
) -> AsyncProcessSnapshot {
    let output = process.output();
    let status = snapshot_status(&output);
    let exit_code = output.exit_code();
    let (stdout, stdout_truncated_by_snapshot) =
        cap_snapshot_bytes(output.stdout, max_output_bytes);
    let (stderr, stderr_truncated_by_snapshot) =
        cap_snapshot_bytes(output.stderr, max_output_bytes);
    AsyncProcessSnapshot {
        process_id: Some(process.process_id()),
        backend: process.backend().clone(),
        label: process.label().to_string(),
        status,
        exit_code,
        stdout,
        stderr,
        stdout_truncated: output.stdout_truncated || stdout_truncated_by_snapshot,
        stderr_truncated: output.stderr_truncated || stderr_truncated_by_snapshot,
        events: process.events(),
    }
}

fn apply_termination_reason(snapshot: &mut AsyncProcessSnapshot, reason: Option<&str>) {
    if snapshot.status != ProcessSnapshotStatus::Cancelled {
        return;
    }
    let Some(reason) = reason else {
        return;
    };
    if let Some(event) = snapshot
        .events
        .iter_mut()
        .rev()
        .find(|event| matches!(event.kind, CooldisProcessEventKind::Cancelled { .. }))
    {
        event.kind = CooldisProcessEventKind::Cancelled {
            reason: reason.to_string(),
        };
    }
}

fn snapshot_status(output: &CooldisProcessOutput) -> ProcessSnapshotStatus {
    match &output.terminal {
        None => ProcessSnapshotStatus::Running,
        Some(CooldisProcessTerminalState::Completed { .. }) => ProcessSnapshotStatus::Completed,
        Some(CooldisProcessTerminalState::Failed { .. }) => ProcessSnapshotStatus::Failed,
        Some(CooldisProcessTerminalState::TimedOut { .. }) => ProcessSnapshotStatus::TimedOut,
        Some(CooldisProcessTerminalState::Cancelled { .. }) => ProcessSnapshotStatus::Cancelled,
    }
}

fn cap_snapshot_bytes(mut bytes: Vec<u8>, max_output_bytes: usize) -> (Vec<u8>, bool) {
    if bytes.len() <= max_output_bytes {
        return (bytes, false);
    }
    bytes.truncate(max_output_bytes);
    (bytes, true)
}

impl From<CooldisProcessError> for std::io::Error {
    fn from(err: CooldisProcessError) -> Self {
        std::io::Error::other(err.to_string())
    }
}
