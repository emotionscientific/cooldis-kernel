mod bridge;
mod execution;
mod live;
mod process;

pub use bridge::{
    BROWSER_NAMESPACE, BridgeBackendKind, BridgeCapabilities, BridgeScope, BridgeSession,
    BridgeSessionId, COMPUTER_NAMESPACE, CapabilityBridge, CapabilityDescriptor, CapabilityGrant,
    FS_NAMESPACE, FileDeltaKind, OpenBridgeSessionRequest, OperationEvent, OperationEventStream,
    OperationExitStatus, OperationId, OperationLogLevel, OperationRequest, PROCEDURE_NAMESPACE,
    REDUCER_NAMESPACE, RejectingCapabilityBridge, UNIX_EXEC_OPERATION, UNIX_NAMESPACE,
    UnixExecPayload, UnixExecutionMode,
};
pub use execution::{
    ExecutionDeadline, ExternalCommandExecutor, ExternalCommandInvocation, ExternalCommandRequest,
    ExternalCommandResult, ExternalExecutorKind, ExternalFileWrite, HostBashExecutor,
    HostBashExecutorConfig, RejectingExternalCommandExecutor, VirtualCommandOutput,
};
pub use live::{
    AsyncExecutionManager, AsyncExecutionManagerConfig, AsyncProcessOutcome, AsyncProcessOwner,
    AsyncProcessSnapshot, AsyncProcessStartRequest, HostBashLiveBackend, LiveProcessBackend,
    LiveProcessInvocation, LiveProcessSpawn, LiveProcessStartRequest, ProcessSnapshotStatus,
};
pub use process::{
    VerletProcessArtifact, VerletProcessBackend, VerletProcessEvent, VerletProcessEventKind,
    VerletProcessExitStatus, VerletProcessFileDelta, VerletProcessHandle, VerletProcessId,
    VerletProcessOutput, VerletProcessTerminalState, WasmOperationOutput,
    set_deterministic_process_ids_for_tests,
};

pub type VerletProcessResult<T> = Result<T, VerletProcessError>;

#[derive(Debug, thiserror::Error)]
pub enum VerletProcessError {
    #[error("process execution failed: {0}")]
    Execution(String),
}

pub(crate) fn process_error(err: impl std::fmt::Display) -> VerletProcessError {
    VerletProcessError::Execution(err.to_string())
}
