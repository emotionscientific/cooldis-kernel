use futures_util::StreamExt as _;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct VerletProcessId(uuid::Uuid);

/// Source of process ids for one process manager (EMO-552). Replaces the
/// former process-global deterministic-ids switch so co-resident kernel
/// instances can each carry their own source (random in production, a
/// per-instance deterministic counter under simulation).
pub trait ProcessIdSource: Send + Sync {
    fn next_process_id(&self) -> VerletProcessId;
}

/// Production source: time-ordered random ids.
pub struct RandomProcessIds;

impl ProcessIdSource for RandomProcessIds {
    fn next_process_id(&self) -> VerletProcessId {
        VerletProcessId(uuid::Uuid::now_v7())
    }
}

/// Deterministic per-source counter (1, 2, 3, ...). One instance of this
/// type per simulated harness; two harnesses with separate sources produce
/// identical id sequences independently.
#[derive(Default)]
pub struct DeterministicProcessIds {
    next: std::sync::atomic::AtomicU64,
}

impl DeterministicProcessIds {
    pub fn new() -> Self {
        Self {
            next: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl ProcessIdSource for DeterministicProcessIds {
    fn next_process_id(&self) -> VerletProcessId {
        let next = self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        VerletProcessId(uuid::Uuid::from_u128(u128::from(next)))
    }
}

impl VerletProcessId {
    pub fn new() -> Self {
        RandomProcessIds.next_process_id()
    }
}

impl Default for VerletProcessId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for VerletProcessId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for VerletProcessId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        uuid::Uuid::parse_str(value).map(Self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerletProcessBackend {
    VirtualBash,
    HostBash,
    RemoteLinux,
    WasmOperation,
    Sandbox,
    Bridge,
    RuntimeThread,
    Other(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletProcessExitStatus {
    pub code: Option<i32>,
    pub success: bool,
}

impl VerletProcessExitStatus {
    pub fn exited(code: i32) -> Self {
        Self {
            code: Some(code),
            success: code == 0,
        }
    }

    pub fn success() -> Self {
        Self::exited(0)
    }
}

impl From<crate::bridge::OperationExitStatus> for VerletProcessExitStatus {
    fn from(status: crate::bridge::OperationExitStatus) -> Self {
        Self {
            code: status.code,
            success: status.success,
        }
    }
}

impl From<VerletProcessExitStatus> for crate::bridge::OperationExitStatus {
    fn from(status: VerletProcessExitStatus) -> Self {
        Self {
            code: status.code,
            success: status.success,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VerletProcessTerminalState {
    Completed {
        status: VerletProcessExitStatus,
    },
    Failed {
        code: String,
        message: String,
    },
    Cancelled {
        reason: String,
    },
    TimedOut {
        timeout_ms: Option<u64>,
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VerletProcessEventKind {
    Started {
        command: Option<String>,
    },
    Stdout {
        bytes: Vec<u8>,
    },
    Stderr {
        bytes: Vec<u8>,
    },
    Log {
        level: crate::bridge::OperationLogLevel,
        message: String,
    },
    Artifact {
        artifact_id: String,
        path: Option<std::path::PathBuf>,
        mime_type: Option<String>,
    },
    FileDelta {
        kind: crate::bridge::FileDeltaKind,
        path: std::path::PathBuf,
        target: Option<std::path::PathBuf>,
    },
    Frame {
        frame_id: String,
        mime_type: String,
    },
    OutputTruncated {
        stdout: bool,
        stderr: bool,
    },
    Completed {
        status: VerletProcessExitStatus,
    },
    Failed {
        code: String,
        message: String,
    },
    Cancelled {
        reason: String,
    },
    TimedOut {
        timeout_ms: Option<u64>,
        message: String,
    },
}

impl VerletProcessEventKind {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. }
                | Self::Failed { .. }
                | Self::Cancelled { .. }
                | Self::TimedOut { .. }
        )
    }

    fn terminal_state(&self) -> Option<VerletProcessTerminalState> {
        match self {
            Self::Completed { status } => {
                Some(VerletProcessTerminalState::Completed { status: *status })
            }
            Self::Failed { code, message } => Some(VerletProcessTerminalState::Failed {
                code: code.clone(),
                message: message.clone(),
            }),
            Self::Cancelled { reason } => Some(VerletProcessTerminalState::Cancelled {
                reason: reason.clone(),
            }),
            Self::TimedOut {
                timeout_ms,
                message,
            } => Some(VerletProcessTerminalState::TimedOut {
                timeout_ms: *timeout_ms,
                message: message.clone(),
            }),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletProcessEvent {
    pub process_id: VerletProcessId,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub backend: VerletProcessBackend,
    pub kind: VerletProcessEventKind,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletProcessOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub terminal: Option<VerletProcessTerminalState>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub artifacts: Vec<VerletProcessArtifact>,
    pub file_deltas: Vec<VerletProcessFileDelta>,
}

impl VerletProcessOutput {
    pub fn exit_code(&self) -> Option<i32> {
        match &self.terminal {
            Some(VerletProcessTerminalState::Completed { status }) => status.code,
            Some(VerletProcessTerminalState::TimedOut { .. }) => Some(124),
            Some(VerletProcessTerminalState::Cancelled { .. }) => Some(130),
            Some(VerletProcessTerminalState::Failed { .. }) => Some(1),
            None => None,
        }
    }

    pub fn success(&self) -> bool {
        matches!(
            self.terminal,
            Some(VerletProcessTerminalState::Completed {
                status: VerletProcessExitStatus { success: true, .. }
            })
        )
    }

    pub fn stdout_text_lossy(&self) -> String {
        String::from_utf8_lossy(&self.stdout).to_string()
    }

    pub fn stderr_text_lossy(&self) -> String {
        String::from_utf8_lossy(&self.stderr).to_string()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletProcessArtifact {
    pub artifact_id: String,
    pub path: Option<std::path::PathBuf>,
    pub mime_type: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletProcessFileDelta {
    pub kind: crate::bridge::FileDeltaKind,
    pub path: std::path::PathBuf,
    pub target: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WasmOperationOutput {
    pub manifest: verlet_abi::WasmOperationManifest,
    pub operation: verlet_abi::WasmOperationDefinition,
    pub output: Vec<u8>,
    pub events: Vec<u8>,
    pub invocation_context: verlet_abi::InvocationContext,
}

#[derive(Clone)]
pub struct VerletProcessHandle {
    process_id: VerletProcessId,
    backend: VerletProcessBackend,
    label: String,
    inner: std::sync::Arc<VerletProcessLogInner>,
}

struct VerletProcessLogInner {
    events: std::sync::Mutex<Vec<VerletProcessEvent>>,
    next_sequence: std::sync::atomic::AtomicU64,
    live_tx: tokio::sync::broadcast::Sender<VerletProcessEvent>,
}

impl VerletProcessHandle {
    pub fn new(backend: VerletProcessBackend, label: impl Into<String>) -> Self {
        Self::with_process_id(VerletProcessId::new(), backend, label)
    }

    /// Constructs a process handle around an id allocated before backend
    /// startup. The kernel uses this to durably witness dispatch identity
    /// before allowing the external process effect.
    pub fn with_process_id(
        process_id: VerletProcessId,
        backend: VerletProcessBackend,
        label: impl Into<String>,
    ) -> Self {
        let (live_tx, _) = tokio::sync::broadcast::channel(1024);
        Self {
            process_id,
            backend,
            label: label.into(),
            inner: std::sync::Arc::new(VerletProcessLogInner {
                events: std::sync::Mutex::new(Vec::new()),
                next_sequence: std::sync::atomic::AtomicU64::new(0),
                live_tx,
            }),
        }
    }

    pub fn process_id(&self) -> VerletProcessId {
        self.process_id
    }

    pub fn backend(&self) -> &VerletProcessBackend {
        &self.backend
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<VerletProcessEvent> {
        self.inner.live_tx.subscribe()
    }

    pub fn events(&self) -> Vec<VerletProcessEvent> {
        self.inner.events.lock().unwrap().clone()
    }

    pub fn record(&self, kind: VerletProcessEventKind) -> VerletProcessEvent {
        let event = VerletProcessEvent {
            process_id: self.process_id,
            sequence: self
                .inner
                .next_sequence
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1,
            timestamp_ms: unix_timestamp_ms(),
            backend: self.backend.clone(),
            kind,
        };
        self.inner.events.lock().unwrap().push(event.clone());
        let _ = self.inner.live_tx.send(event.clone());
        event
    }

    pub fn output(&self) -> VerletProcessOutput {
        let events = self.events();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut terminal = None;
        let mut stdout_truncated = false;
        let mut stderr_truncated = false;
        let mut artifacts = Vec::new();
        let mut file_deltas = Vec::new();
        for event in events {
            match event.kind {
                VerletProcessEventKind::Stdout { bytes } => stdout.extend(bytes),
                VerletProcessEventKind::Stderr { bytes } => stderr.extend(bytes),
                VerletProcessEventKind::Artifact {
                    artifact_id,
                    path,
                    mime_type,
                } => artifacts.push(VerletProcessArtifact {
                    artifact_id,
                    path,
                    mime_type,
                }),
                VerletProcessEventKind::FileDelta { kind, path, target } => {
                    file_deltas.push(VerletProcessFileDelta { kind, path, target })
                }
                VerletProcessEventKind::OutputTruncated { stdout, stderr } => {
                    stdout_truncated |= stdout;
                    stderr_truncated |= stderr;
                }
                kind => {
                    if let Some(state) = kind.terminal_state() {
                        terminal = Some(state);
                    }
                }
            }
        }
        VerletProcessOutput {
            stdout,
            stderr,
            terminal,
            stdout_truncated,
            stderr_truncated,
            artifacts,
            file_deltas,
        }
    }

    pub fn from_virtual_command(
        command: impl Into<String>,
        output: crate::execution::VirtualCommandOutput,
    ) -> Self {
        let command = command.into();
        let stdout_truncated = output.stdout_truncated;
        let stderr_truncated = output.stderr_truncated;
        let process = Self::new(VerletProcessBackend::VirtualBash, command.clone());
        process.record(VerletProcessEventKind::Started {
            command: Some(command),
        });
        if !output.stdout.is_empty() {
            process.record(VerletProcessEventKind::Stdout {
                bytes: output.stdout.into_bytes(),
            });
        }
        if !output.stderr.is_empty() {
            process.record(VerletProcessEventKind::Stderr {
                bytes: output.stderr.clone().into_bytes(),
            });
        }
        if stdout_truncated || stderr_truncated {
            process.record(VerletProcessEventKind::OutputTruncated {
                stdout: stdout_truncated,
                stderr: stderr_truncated,
            });
            process.record(VerletProcessEventKind::Log {
                level: crate::bridge::OperationLogLevel::Warn,
                message: format!(
                    "process output was truncated: stdout={stdout_truncated}, stderr={stderr_truncated}"
                ),
            });
        }
        record_exit_from_virtual_output(&process, output.exit_code, &output.stderr);
        process
    }

    pub fn from_external_command(
        request: &crate::execution::ExternalCommandRequest,
        result: crate::execution::ExternalCommandResult,
    ) -> Self {
        let output = result.output;
        let stdout_truncated = output.stdout_truncated;
        let stderr_truncated = output.stderr_truncated;
        let backend = match request.executor {
            crate::execution::ExternalExecutorKind::HostBash => VerletProcessBackend::HostBash,
            crate::execution::ExternalExecutorKind::RemoteLinux => {
                VerletProcessBackend::RemoteLinux
            }
        };
        let label = request.label();
        let process = Self::new(backend, label.clone());
        process.record(VerletProcessEventKind::Started {
            command: Some(label),
        });
        if !output.stdout.is_empty() {
            process.record(VerletProcessEventKind::Stdout {
                bytes: output.stdout.into_bytes(),
            });
        }
        if !output.stderr.is_empty() {
            process.record(VerletProcessEventKind::Stderr {
                bytes: output.stderr.clone().into_bytes(),
            });
        }
        for write in result.file_writes {
            process.record(VerletProcessEventKind::FileDelta {
                kind: crate::bridge::FileDeltaKind::Write,
                path: write.path,
                target: None,
            });
        }
        if stdout_truncated || stderr_truncated {
            process.record(VerletProcessEventKind::OutputTruncated {
                stdout: stdout_truncated,
                stderr: stderr_truncated,
            });
            process.record(VerletProcessEventKind::Log {
                level: crate::bridge::OperationLogLevel::Warn,
                message: format!(
                    "process output was truncated: stdout={stdout_truncated}, stderr={stderr_truncated}"
                ),
            });
        }
        record_exit_from_virtual_output(&process, output.exit_code, &output.stderr);
        process
    }

    pub fn from_wasm_operation_output(
        registered_name: impl Into<Option<String>>,
        output: WasmOperationOutput,
    ) -> Self {
        let registered_name = registered_name.into();
        let label = registered_name
            .as_deref()
            .map(|name| format!("{name}/{}", output.operation.name))
            .unwrap_or_else(|| output.operation.name.clone());
        let process = Self::new(VerletProcessBackend::WasmOperation, label.clone());
        process.record(VerletProcessEventKind::Started {
            command: Some(label),
        });
        if !output.output.is_empty() {
            process.record(VerletProcessEventKind::Stdout {
                bytes: output.output,
            });
        }
        if !output.events.is_empty() {
            process.record(VerletProcessEventKind::Stderr {
                bytes: output.events,
            });
        }
        process.record(VerletProcessEventKind::Completed {
            status: VerletProcessExitStatus::success(),
        });
        process
    }

    pub fn record_bridge_event(&self, event: crate::bridge::OperationEvent) -> VerletProcessEvent {
        self.record(match event {
            crate::bridge::OperationEvent::Started { .. } => {
                VerletProcessEventKind::Started { command: None }
            }
            crate::bridge::OperationEvent::Stdout { bytes, .. } => {
                VerletProcessEventKind::Stdout { bytes }
            }
            crate::bridge::OperationEvent::Stderr { bytes, .. } => {
                VerletProcessEventKind::Stderr { bytes }
            }
            crate::bridge::OperationEvent::Log { level, message, .. } => {
                VerletProcessEventKind::Log { level, message }
            }
            crate::bridge::OperationEvent::Artifact {
                artifact_id,
                path,
                mime_type,
                ..
            } => VerletProcessEventKind::Artifact {
                artifact_id,
                path,
                mime_type,
            },
            crate::bridge::OperationEvent::FileDelta {
                kind, path, target, ..
            } => VerletProcessEventKind::FileDelta { kind, path, target },
            crate::bridge::OperationEvent::Frame {
                frame_id,
                mime_type,
                ..
            } => VerletProcessEventKind::Frame {
                frame_id,
                mime_type,
            },
            crate::bridge::OperationEvent::Completed { status, .. } => {
                VerletProcessEventKind::Completed {
                    status: status.into(),
                }
            }
            crate::bridge::OperationEvent::Failed { code, message, .. } => {
                VerletProcessEventKind::Failed { code, message }
            }
            crate::bridge::OperationEvent::Cancelled { reason, .. } => {
                VerletProcessEventKind::Cancelled { reason }
            }
        })
    }

    pub fn from_bridge_events(
        label: impl Into<String>,
        events: Vec<crate::bridge::OperationEvent>,
    ) -> Self {
        let process = Self::new(VerletProcessBackend::Bridge, label);
        for event in events {
            process.record_bridge_event(event);
        }
        process
    }

    pub fn attach_bridge_event_stream(
        &self,
        mut stream: crate::bridge::OperationEventStream,
    ) -> tokio::task::JoinHandle<crate::VerletProcessResult<()>> {
        let process = self.clone();
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                process.record_bridge_event(event?);
            }
            Ok(())
        })
    }
}

impl From<&VerletProcessOutput> for crate::execution::VirtualCommandOutput {
    fn from(output: &VerletProcessOutput) -> Self {
        Self {
            stdout: output.stdout_text_lossy(),
            stderr: output.stderr_text_lossy(),
            exit_code: output.exit_code().unwrap_or(1),
            stdout_truncated: output.stdout_truncated,
            stderr_truncated: output.stderr_truncated,
        }
    }
}

fn record_exit_from_virtual_output(process: &VerletProcessHandle, exit_code: i32, stderr: &str) {
    if exit_code == 124 && stderr.contains("timed out") {
        process.record(VerletProcessEventKind::TimedOut {
            timeout_ms: None,
            message: stderr.trim().to_string(),
        });
    } else {
        process.record(VerletProcessEventKind::Completed {
            status: VerletProcessExitStatus::exited(exit_code),
        });
    }
}

fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;
