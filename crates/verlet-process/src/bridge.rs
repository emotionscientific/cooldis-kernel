pub const UNIX_NAMESPACE: &str = "unix";
pub const UNIX_EXEC_OPERATION: &str = "exec";
pub const FS_NAMESPACE: &str = "fs";
pub const COMPUTER_NAMESPACE: &str = "computer";
pub const BROWSER_NAMESPACE: &str = "browser";
pub const PROCEDURE_NAMESPACE: &str = "procedure";
pub const REDUCER_NAMESPACE: &str = "reducer";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct BridgeSessionId(uuid::Uuid);

impl BridgeSessionId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }
}

impl Default for BridgeSessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for BridgeSessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct OperationId(uuid::Uuid);

impl OperationId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for OperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BridgeScope {
    pub tenant_id: String,
    pub user_id: String,
    pub session_id: String,
    pub thread_id: Option<verlet_runtime_contracts::ThreadId>,
}

impl BridgeScope {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        session_id: impl Into<String>,
        thread_id: Option<verlet_runtime_contracts::ThreadId>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            session_id: session_id.into(),
            thread_id,
        }
    }

    pub fn from_thread(coordinates: &verlet_runtime_contracts::ThreadCoordinates) -> Self {
        Self {
            tenant_id: coordinates.tenant_id.clone(),
            user_id: coordinates.user_id.clone(),
            session_id: coordinates.session_id.clone(),
            thread_id: Some(coordinates.thread_id),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeBackendKind {
    InProcess,
    LocalDaemon,
    RemoteService,
    SandboxFleet,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnixExecutionMode {
    VirtualOnly,
    RealOnly,
    Hybrid,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CapabilityDescriptor {
    pub namespace: String,
    pub operations: std::collections::BTreeSet<String>,
    pub backend_kind: BridgeBackendKind,
    pub description: Option<String>,
}

impl CapabilityDescriptor {
    pub fn new(
        namespace: impl Into<String>,
        backend_kind: BridgeBackendKind,
        operations: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            operations: operations
                .into_iter()
                .map(Into::into)
                .collect::<std::collections::BTreeSet<_>>(),
            backend_kind,
            description: None,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn supports(&self, operation: &str) -> bool {
        self.operations.contains(operation)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BridgeCapabilities {
    pub backend_id: String,
    pub backend_kind: BridgeBackendKind,
    pub capabilities: Vec<CapabilityDescriptor>,
}

impl BridgeCapabilities {
    pub fn new(backend_id: impl Into<String>, backend_kind: BridgeBackendKind) -> Self {
        Self {
            backend_id: backend_id.into(),
            backend_kind,
            capabilities: Vec::new(),
        }
    }

    pub fn with_capability(mut self, capability: CapabilityDescriptor) -> Self {
        self.capabilities.push(capability);
        self
    }

    pub fn supports(&self, namespace: &str, operation: &str) -> bool {
        self.capabilities
            .iter()
            .any(|capability| capability.namespace == namespace && capability.supports(operation))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CapabilityGrant {
    pub namespace: String,
    pub operations: std::collections::BTreeSet<String>,
}

impl CapabilityGrant {
    pub fn new(
        namespace: impl Into<String>,
        operations: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            operations: operations
                .into_iter()
                .map(Into::into)
                .collect::<std::collections::BTreeSet<_>>(),
        }
    }

    pub fn allows(&self, namespace: &str, operation: &str) -> bool {
        self.namespace == namespace && self.operations.contains(operation)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OpenBridgeSessionRequest {
    pub scope: BridgeScope,
    pub requested_capabilities: Vec<CapabilityGrant>,
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BridgeSession {
    pub session_id: BridgeSessionId,
    pub scope: BridgeScope,
    pub granted_capabilities: Vec<CapabilityGrant>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OperationRequest {
    pub operation_id: OperationId,
    pub bridge_session_id: BridgeSessionId,
    pub scope: BridgeScope,
    pub namespace: String,
    pub operation: String,
    pub payload: serde_json::Value,
    pub timeout_ms: Option<u64>,
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl OperationRequest {
    pub fn new(
        bridge_session_id: BridgeSessionId,
        scope: BridgeScope,
        namespace: impl Into<String>,
        operation: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            operation_id: OperationId::new(),
            bridge_session_id,
            scope,
            namespace: namespace.into(),
            operation: operation.into(),
            payload,
            timeout_ms: None,
            metadata: std::collections::BTreeMap::new(),
        }
    }

    pub fn unix_exec(
        bridge_session_id: BridgeSessionId,
        scope: BridgeScope,
        payload: UnixExecPayload,
    ) -> Self {
        Self::new(
            bridge_session_id,
            scope,
            UNIX_NAMESPACE,
            UNIX_EXEC_OPERATION,
            serde_json::to_value(payload).expect("UnixExecPayload serializes"),
        )
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UnixExecPayload {
    pub command: String,
    pub cwd: std::path::PathBuf,
    pub env: std::collections::BTreeMap<String, String>,
    pub stdin: Option<Vec<u8>>,
    pub mode: UnixExecutionMode,
}

impl UnixExecPayload {
    pub fn new(command: impl Into<String>, cwd: impl Into<std::path::PathBuf>) -> Self {
        Self {
            command: command.into(),
            cwd: cwd.into(),
            env: std::collections::BTreeMap::new(),
            stdin: None,
            mode: UnixExecutionMode::Hybrid,
        }
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub fn with_stdin(mut self, stdin: impl Into<Vec<u8>>) -> Self {
        self.stdin = Some(stdin.into());
        self
    }

    pub fn with_mode(mut self, mode: UnixExecutionMode) -> Self {
        self.mode = mode;
        self
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OperationEvent {
    Started {
        operation_id: OperationId,
    },
    Stdout {
        operation_id: OperationId,
        bytes: Vec<u8>,
    },
    Stderr {
        operation_id: OperationId,
        bytes: Vec<u8>,
    },
    Log {
        operation_id: OperationId,
        level: OperationLogLevel,
        message: String,
    },
    Artifact {
        operation_id: OperationId,
        artifact_id: String,
        path: Option<std::path::PathBuf>,
        mime_type: Option<String>,
    },
    FileDelta {
        operation_id: OperationId,
        kind: FileDeltaKind,
        path: std::path::PathBuf,
        target: Option<std::path::PathBuf>,
    },
    Frame {
        operation_id: OperationId,
        frame_id: String,
        mime_type: String,
    },
    Completed {
        operation_id: OperationId,
        status: OperationExitStatus,
    },
    Failed {
        operation_id: OperationId,
        code: String,
        message: String,
    },
    Cancelled {
        operation_id: OperationId,
        reason: String,
    },
}

impl OperationEvent {
    pub fn operation_id(&self) -> OperationId {
        match self {
            OperationEvent::Started { operation_id }
            | OperationEvent::Stdout { operation_id, .. }
            | OperationEvent::Stderr { operation_id, .. }
            | OperationEvent::Log { operation_id, .. }
            | OperationEvent::Artifact { operation_id, .. }
            | OperationEvent::FileDelta { operation_id, .. }
            | OperationEvent::Frame { operation_id, .. }
            | OperationEvent::Completed { operation_id, .. }
            | OperationEvent::Failed { operation_id, .. }
            | OperationEvent::Cancelled { operation_id, .. } => *operation_id,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            OperationEvent::Completed { .. }
                | OperationEvent::Failed { .. }
                | OperationEvent::Cancelled { .. }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileDeltaKind {
    Write,
    Append,
    Mkdir,
    Remove,
    Rename,
    Copy,
    Chmod,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OperationExitStatus {
    pub code: Option<i32>,
    pub success: bool,
}

impl OperationExitStatus {
    pub fn exited(code: i32) -> Self {
        Self {
            code: Some(code),
            success: code == 0,
        }
    }

    pub fn success() -> Self {
        Self {
            code: Some(0),
            success: true,
        }
    }
}

pub type OperationEventStream =
    futures_util::stream::BoxStream<'static, crate::VerletProcessResult<OperationEvent>>;

#[async_trait::async_trait]
pub trait CapabilityBridge: Send + Sync + 'static {
    async fn capabilities(&self) -> crate::VerletProcessResult<BridgeCapabilities>;

    async fn open_session(
        &self,
        request: OpenBridgeSessionRequest,
    ) -> crate::VerletProcessResult<BridgeSession>;

    async fn invoke(
        &self,
        request: OperationRequest,
    ) -> crate::VerletProcessResult<OperationEventStream>;

    async fn cancel(&self, operation_id: OperationId) -> crate::VerletProcessResult<()>;

    async fn close_session(
        &self,
        bridge_session_id: BridgeSessionId,
    ) -> crate::VerletProcessResult<()>;
}

#[derive(Clone, Debug)]
pub struct RejectingCapabilityBridge {
    capabilities: BridgeCapabilities,
}

impl RejectingCapabilityBridge {
    pub fn new(capabilities: BridgeCapabilities) -> Self {
        Self { capabilities }
    }
}

#[async_trait::async_trait]
impl CapabilityBridge for RejectingCapabilityBridge {
    async fn capabilities(&self) -> crate::VerletProcessResult<BridgeCapabilities> {
        Ok(self.capabilities.clone())
    }

    async fn open_session(
        &self,
        request: OpenBridgeSessionRequest,
    ) -> crate::VerletProcessResult<BridgeSession> {
        Ok(BridgeSession {
            session_id: BridgeSessionId::new(),
            scope: request.scope,
            granted_capabilities: request.requested_capabilities,
        })
    }

    async fn invoke(
        &self,
        request: OperationRequest,
    ) -> crate::VerletProcessResult<OperationEventStream> {
        let event = OperationEvent::Failed {
            operation_id: request.operation_id,
            code: "capability_unavailable".to_string(),
            message: format!(
                "capability bridge rejected {}.{}",
                request.namespace, request.operation
            ),
        };
        Ok(Box::pin(futures_util::stream::once(
            async move { Ok(event) },
        )))
    }

    async fn cancel(&self, _operation_id: OperationId) -> crate::VerletProcessResult<()> {
        Ok(())
    }

    async fn close_session(
        &self,
        _bridge_session_id: BridgeSessionId,
    ) -> crate::VerletProcessResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
