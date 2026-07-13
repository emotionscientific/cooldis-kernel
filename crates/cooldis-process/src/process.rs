use crate::{
    CooldisProcessResult, ExternalCommandRequest, ExternalCommandResult, ExternalExecutorKind,
    FileDeltaKind, OperationEvent, OperationEventStream, OperationExitStatus, OperationLogLevel,
    VirtualCommandOutput,
};
use cooldis_abi::{InvocationContext, WasmOperationDefinition, WasmOperationManifest};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct CooldisProcessId(Uuid);

static FORCE_DETERMINISTIC_PROCESS_IDS: AtomicBool = AtomicBool::new(false);
static NEXT_DETERMINISTIC_PROCESS_ID: AtomicU64 = AtomicU64::new(1);

pub fn set_deterministic_process_ids_for_tests(enabled: bool) {
    FORCE_DETERMINISTIC_PROCESS_IDS.store(enabled, Ordering::SeqCst);
    if enabled {
        NEXT_DETERMINISTIC_PROCESS_ID.store(1, Ordering::SeqCst);
    }
}

impl CooldisProcessId {
    pub fn new() -> Self {
        if FORCE_DETERMINISTIC_PROCESS_IDS.load(Ordering::SeqCst) {
            let next = NEXT_DETERMINISTIC_PROCESS_ID.fetch_add(1, Ordering::SeqCst);
            return Self(Uuid::from_u128(u128::from(next)));
        }
        Self(Uuid::now_v7())
    }
}

impl Default for CooldisProcessId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CooldisProcessId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for CooldisProcessId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CooldisProcessBackend {
    VirtualBash,
    HostBash,
    RemoteLinux,
    WasmOperation,
    Sandbox,
    Bridge,
    RuntimeThread,
    Other(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisProcessExitStatus {
    pub code: Option<i32>,
    pub success: bool,
}

impl CooldisProcessExitStatus {
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

impl From<OperationExitStatus> for CooldisProcessExitStatus {
    fn from(status: OperationExitStatus) -> Self {
        Self {
            code: status.code,
            success: status.success,
        }
    }
}

impl From<CooldisProcessExitStatus> for OperationExitStatus {
    fn from(status: CooldisProcessExitStatus) -> Self {
        Self {
            code: status.code,
            success: status.success,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CooldisProcessTerminalState {
    Completed {
        status: CooldisProcessExitStatus,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CooldisProcessEventKind {
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
        level: OperationLogLevel,
        message: String,
    },
    Artifact {
        artifact_id: String,
        path: Option<PathBuf>,
        mime_type: Option<String>,
    },
    FileDelta {
        kind: FileDeltaKind,
        path: PathBuf,
        target: Option<PathBuf>,
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
        status: CooldisProcessExitStatus,
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

impl CooldisProcessEventKind {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. }
                | Self::Failed { .. }
                | Self::Cancelled { .. }
                | Self::TimedOut { .. }
        )
    }

    fn terminal_state(&self) -> Option<CooldisProcessTerminalState> {
        match self {
            Self::Completed { status } => {
                Some(CooldisProcessTerminalState::Completed { status: *status })
            }
            Self::Failed { code, message } => Some(CooldisProcessTerminalState::Failed {
                code: code.clone(),
                message: message.clone(),
            }),
            Self::Cancelled { reason } => Some(CooldisProcessTerminalState::Cancelled {
                reason: reason.clone(),
            }),
            Self::TimedOut {
                timeout_ms,
                message,
            } => Some(CooldisProcessTerminalState::TimedOut {
                timeout_ms: *timeout_ms,
                message: message.clone(),
            }),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisProcessEvent {
    pub process_id: CooldisProcessId,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub backend: CooldisProcessBackend,
    pub kind: CooldisProcessEventKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisProcessOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub terminal: Option<CooldisProcessTerminalState>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub artifacts: Vec<CooldisProcessArtifact>,
    pub file_deltas: Vec<CooldisProcessFileDelta>,
}

impl CooldisProcessOutput {
    pub fn exit_code(&self) -> Option<i32> {
        match &self.terminal {
            Some(CooldisProcessTerminalState::Completed { status }) => status.code,
            Some(CooldisProcessTerminalState::TimedOut { .. }) => Some(124),
            Some(CooldisProcessTerminalState::Cancelled { .. }) => Some(130),
            Some(CooldisProcessTerminalState::Failed { .. }) => Some(1),
            None => None,
        }
    }

    pub fn success(&self) -> bool {
        matches!(
            self.terminal,
            Some(CooldisProcessTerminalState::Completed {
                status: CooldisProcessExitStatus { success: true, .. }
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisProcessArtifact {
    pub artifact_id: String,
    pub path: Option<PathBuf>,
    pub mime_type: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisProcessFileDelta {
    pub kind: FileDeltaKind,
    pub path: PathBuf,
    pub target: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WasmOperationOutput {
    pub manifest: WasmOperationManifest,
    pub operation: WasmOperationDefinition,
    pub output: Vec<u8>,
    pub events: Vec<u8>,
    pub invocation_context: InvocationContext,
}

#[derive(Clone)]
pub struct CooldisProcessHandle {
    process_id: CooldisProcessId,
    backend: CooldisProcessBackend,
    label: String,
    inner: Arc<CooldisProcessLogInner>,
}

struct CooldisProcessLogInner {
    events: Mutex<Vec<CooldisProcessEvent>>,
    next_sequence: AtomicU64,
    live_tx: broadcast::Sender<CooldisProcessEvent>,
}

impl CooldisProcessHandle {
    pub fn new(backend: CooldisProcessBackend, label: impl Into<String>) -> Self {
        Self::with_process_id(CooldisProcessId::new(), backend, label)
    }

    /// Constructs a process handle around an id allocated before backend
    /// startup. The kernel uses this to durably witness dispatch identity
    /// before allowing the external process effect.
    pub fn with_process_id(
        process_id: CooldisProcessId,
        backend: CooldisProcessBackend,
        label: impl Into<String>,
    ) -> Self {
        let (live_tx, _) = broadcast::channel(1024);
        Self {
            process_id,
            backend,
            label: label.into(),
            inner: Arc::new(CooldisProcessLogInner {
                events: Mutex::new(Vec::new()),
                next_sequence: AtomicU64::new(0),
                live_tx,
            }),
        }
    }

    pub fn process_id(&self) -> CooldisProcessId {
        self.process_id
    }

    pub fn backend(&self) -> &CooldisProcessBackend {
        &self.backend
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CooldisProcessEvent> {
        self.inner.live_tx.subscribe()
    }

    pub fn events(&self) -> Vec<CooldisProcessEvent> {
        self.inner.events.lock().unwrap().clone()
    }

    pub fn record(&self, kind: CooldisProcessEventKind) -> CooldisProcessEvent {
        let event = CooldisProcessEvent {
            process_id: self.process_id,
            sequence: self.inner.next_sequence.fetch_add(1, Ordering::SeqCst) + 1,
            timestamp_ms: unix_timestamp_ms(),
            backend: self.backend.clone(),
            kind,
        };
        self.inner.events.lock().unwrap().push(event.clone());
        let _ = self.inner.live_tx.send(event.clone());
        event
    }

    pub fn output(&self) -> CooldisProcessOutput {
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
                CooldisProcessEventKind::Stdout { bytes } => stdout.extend(bytes),
                CooldisProcessEventKind::Stderr { bytes } => stderr.extend(bytes),
                CooldisProcessEventKind::Artifact {
                    artifact_id,
                    path,
                    mime_type,
                } => artifacts.push(CooldisProcessArtifact {
                    artifact_id,
                    path,
                    mime_type,
                }),
                CooldisProcessEventKind::FileDelta { kind, path, target } => {
                    file_deltas.push(CooldisProcessFileDelta { kind, path, target })
                }
                CooldisProcessEventKind::OutputTruncated { stdout, stderr } => {
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
        CooldisProcessOutput {
            stdout,
            stderr,
            terminal,
            stdout_truncated,
            stderr_truncated,
            artifacts,
            file_deltas,
        }
    }

    pub fn from_virtual_command(command: impl Into<String>, output: VirtualCommandOutput) -> Self {
        let command = command.into();
        let stdout_truncated = output.stdout_truncated;
        let stderr_truncated = output.stderr_truncated;
        let process = Self::new(CooldisProcessBackend::VirtualBash, command.clone());
        process.record(CooldisProcessEventKind::Started {
            command: Some(command),
        });
        if !output.stdout.is_empty() {
            process.record(CooldisProcessEventKind::Stdout {
                bytes: output.stdout.into_bytes(),
            });
        }
        if !output.stderr.is_empty() {
            process.record(CooldisProcessEventKind::Stderr {
                bytes: output.stderr.clone().into_bytes(),
            });
        }
        if stdout_truncated || stderr_truncated {
            process.record(CooldisProcessEventKind::OutputTruncated {
                stdout: stdout_truncated,
                stderr: stderr_truncated,
            });
            process.record(CooldisProcessEventKind::Log {
                level: OperationLogLevel::Warn,
                message: format!(
                    "process output was truncated: stdout={stdout_truncated}, stderr={stderr_truncated}"
                ),
            });
        }
        record_exit_from_virtual_output(&process, output.exit_code, &output.stderr);
        process
    }

    pub fn from_external_command(
        request: &ExternalCommandRequest,
        result: ExternalCommandResult,
    ) -> Self {
        let output = result.output;
        let stdout_truncated = output.stdout_truncated;
        let stderr_truncated = output.stderr_truncated;
        let backend = match request.executor {
            ExternalExecutorKind::HostBash => CooldisProcessBackend::HostBash,
            ExternalExecutorKind::RemoteLinux => CooldisProcessBackend::RemoteLinux,
        };
        let label = request.label();
        let process = Self::new(backend, label.clone());
        process.record(CooldisProcessEventKind::Started {
            command: Some(label),
        });
        if !output.stdout.is_empty() {
            process.record(CooldisProcessEventKind::Stdout {
                bytes: output.stdout.into_bytes(),
            });
        }
        if !output.stderr.is_empty() {
            process.record(CooldisProcessEventKind::Stderr {
                bytes: output.stderr.clone().into_bytes(),
            });
        }
        for write in result.file_writes {
            process.record(CooldisProcessEventKind::FileDelta {
                kind: FileDeltaKind::Write,
                path: write.path,
                target: None,
            });
        }
        if stdout_truncated || stderr_truncated {
            process.record(CooldisProcessEventKind::OutputTruncated {
                stdout: stdout_truncated,
                stderr: stderr_truncated,
            });
            process.record(CooldisProcessEventKind::Log {
                level: OperationLogLevel::Warn,
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
        let process = Self::new(CooldisProcessBackend::WasmOperation, label.clone());
        process.record(CooldisProcessEventKind::Started {
            command: Some(label),
        });
        if !output.output.is_empty() {
            process.record(CooldisProcessEventKind::Stdout {
                bytes: output.output,
            });
        }
        if !output.events.is_empty() {
            process.record(CooldisProcessEventKind::Stderr {
                bytes: output.events,
            });
        }
        process.record(CooldisProcessEventKind::Completed {
            status: CooldisProcessExitStatus::success(),
        });
        process
    }

    pub fn record_bridge_event(&self, event: OperationEvent) -> CooldisProcessEvent {
        self.record(match event {
            OperationEvent::Started { .. } => CooldisProcessEventKind::Started { command: None },
            OperationEvent::Stdout { bytes, .. } => CooldisProcessEventKind::Stdout { bytes },
            OperationEvent::Stderr { bytes, .. } => CooldisProcessEventKind::Stderr { bytes },
            OperationEvent::Log { level, message, .. } => {
                CooldisProcessEventKind::Log { level, message }
            }
            OperationEvent::Artifact {
                artifact_id,
                path,
                mime_type,
                ..
            } => CooldisProcessEventKind::Artifact {
                artifact_id,
                path,
                mime_type,
            },
            OperationEvent::FileDelta {
                kind, path, target, ..
            } => CooldisProcessEventKind::FileDelta { kind, path, target },
            OperationEvent::Frame {
                frame_id,
                mime_type,
                ..
            } => CooldisProcessEventKind::Frame {
                frame_id,
                mime_type,
            },
            OperationEvent::Completed { status, .. } => CooldisProcessEventKind::Completed {
                status: status.into(),
            },
            OperationEvent::Failed { code, message, .. } => {
                CooldisProcessEventKind::Failed { code, message }
            }
            OperationEvent::Cancelled { reason, .. } => {
                CooldisProcessEventKind::Cancelled { reason }
            }
        })
    }

    pub fn from_bridge_events(label: impl Into<String>, events: Vec<OperationEvent>) -> Self {
        let process = Self::new(CooldisProcessBackend::Bridge, label);
        for event in events {
            process.record_bridge_event(event);
        }
        process
    }

    pub fn attach_bridge_event_stream(
        &self,
        mut stream: OperationEventStream,
    ) -> JoinHandle<CooldisProcessResult<()>> {
        let process = self.clone();
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                process.record_bridge_event(event?);
            }
            Ok(())
        })
    }
}

impl From<&CooldisProcessOutput> for VirtualCommandOutput {
    fn from(output: &CooldisProcessOutput) -> Self {
        Self {
            stdout: output.stdout_text_lossy(),
            stderr: output.stderr_text_lossy(),
            exit_code: output.exit_code().unwrap_or(1),
            stdout_truncated: output.stdout_truncated,
            stderr_truncated: output.stderr_truncated,
        }
    }
}

fn record_exit_from_virtual_output(process: &CooldisProcessHandle, exit_code: i32, stderr: &str) {
    if exit_code == 124 && stderr.contains("timed out") {
        process.record(CooldisProcessEventKind::TimedOut {
            timeout_ms: None,
            message: stderr.trim().to_string(),
        });
    } else {
        process.record(CooldisProcessEventKind::Completed {
            status: CooldisProcessExitStatus::exited(exit_code),
        });
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;
