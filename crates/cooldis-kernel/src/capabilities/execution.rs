use crate::capabilities::vfs::{CooldisVfs, ObjectStoreMountConfig};
use crate::kernel::history::{
    CanonicalContent, CanonicalMessage, CanonicalStopReason, ProviderApi,
};
use crate::{
    AgentKernelToolCall, AgentKernelToolProvider, AgentRuntime, AgentRuntimeFactory, CooldisError,
    CooldisResult, OperationRegistry, RuntimeEventKind, RuntimeServices, RuntimeTerminalState,
    SessionEntryKind, ThreadCommand, ThreadContext, ThreadEvent, ThreadSignal, ThreadStatus,
    ToolDefinition, TurnContextSnapshot, TurnSubmissionMode, emit_runtime_event,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
pub use cooldis_vbash::{
    BASH_TOOL, BashExecutionPolicy, BashkitExecutionConfig, BashkitExecutionHarness,
    BashkitLiveBackend, CommandRoute, CommandRoutingPolicy, VbashOperationRegistry, VirtualFile,
    VirtualMount, VirtualMountBackend, VirtualMountMode, absolute_mount_path,
    apply_external_file_writes, cooldis_usage, default_virtual_mounts, deny_output,
    enforce_output_limit, exec_result_from_virtual_output, missing_operation_capability_grants,
    operation_shell_command_name, operation_shell_command_names, operation_shell_input,
    operation_shell_manual, operation_shell_reserved_commands, reserved_operation_shell_commands,
    summarize_operation_shell_commands, validate_mounts, virtual_command_output_from_exec_result,
};
use serde::Deserialize;
use serde_json::json;
use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

pub use cooldis_process::{
    AsyncExecutionManager, AsyncProcessOwner, AsyncProcessSnapshot, AsyncProcessStartRequest,
    CooldisProcessId, CooldisProcessResult, ExecutionDeadline, ExternalCommandExecutor,
    ExternalCommandInvocation, ExternalCommandRequest, ExternalCommandResult, ExternalExecutorKind,
    ExternalFileWrite, HostBashExecutor, HostBashExecutorConfig, LiveProcessBackend,
    ProcessSnapshotStatus, RejectingExternalCommandExecutor, VirtualCommandOutput,
};

pub const PROCESS_EXEC_TOOL: &str = "process_exec";
pub const WRITE_STDIN_TOOL: &str = "write_stdin";

#[derive(Clone)]
pub struct VirtualBashRuntimeConfig {
    pub cwd: PathBuf,
    pub execution_timeout: Duration,
    pub parser_timeout: Duration,
    pub max_commands: usize,
    pub max_loop_iterations: usize,
    pub max_output_bytes: usize,
    pub mounts: Vec<VirtualMount>,
    pub operation_registry: Option<Arc<OperationRegistry>>,
    /// Thread workspace VFS shared with catalog-loaded operations when both surfaces
    /// must re-present one filesystem tree.
    pub workspace_vfs: Option<Arc<CooldisVfs>>,
    pub capability_grants: BTreeSet<String>,
    pub execution_policy: BashExecutionPolicy,
    pub external_executor: Option<Arc<dyn ExternalCommandExecutor>>,
}

impl std::fmt::Debug for VirtualBashRuntimeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VirtualBashRuntimeConfig")
            .field("cwd", &self.cwd)
            .field("execution_timeout", &self.execution_timeout)
            .field("parser_timeout", &self.parser_timeout)
            .field("max_commands", &self.max_commands)
            .field("max_loop_iterations", &self.max_loop_iterations)
            .field("max_output_bytes", &self.max_output_bytes)
            .field("mounts", &self.mounts)
            .field(
                "operation_registry",
                &self
                    .operation_registry
                    .as_ref()
                    .map(|_| "<OperationRegistry>"),
            )
            .field(
                "workspace_vfs",
                &self.workspace_vfs.as_ref().map(|_| "<CooldisVfs>"),
            )
            .field("capability_grants", &self.capability_grants)
            .field("execution_policy", &self.execution_policy)
            .field(
                "external_executor",
                &self
                    .external_executor
                    .as_ref()
                    .map(|_| "<ExternalCommandExecutor>"),
            )
            .finish()
    }
}

impl Default for VirtualBashRuntimeConfig {
    fn default() -> Self {
        Self {
            cwd: PathBuf::from("/workspace"),
            execution_timeout: Duration::from_secs(10),
            parser_timeout: Duration::from_secs(2),
            max_commands: 10_000,
            max_loop_iterations: 10_000,
            max_output_bytes: 1_048_576,
            mounts: default_virtual_mounts(),
            operation_registry: None,
            workspace_vfs: None,
            capability_grants: BTreeSet::new(),
            execution_policy: BashExecutionPolicy::virtual_only(),
            external_executor: None,
        }
    }
}

impl VirtualBashRuntimeConfig {
    pub fn with_mount(mut self, mount: VirtualMount) -> Self {
        self.mounts.push(mount);
        self
    }

    pub fn with_writable_mount(mut self, path: impl Into<PathBuf>) -> Self {
        self.mounts.push(VirtualMount::writable(path));
        self
    }

    pub fn with_readonly_mount(
        mut self,
        path: impl Into<PathBuf>,
        files: Vec<VirtualFile>,
    ) -> Self {
        self.mounts.push(VirtualMount::readonly(path, files));
        self
    }

    pub fn with_object_store_mount(
        mut self,
        path: impl Into<PathBuf>,
        config: ObjectStoreMountConfig,
    ) -> Self {
        self.mounts.push(VirtualMount::object_store(path, config));
        self
    }

    pub fn with_readonly_object_store_mount(
        mut self,
        path: impl Into<PathBuf>,
        config: ObjectStoreMountConfig,
    ) -> Self {
        self.mounts
            .push(VirtualMount::readonly_object_store(path, config));
        self
    }

    pub fn with_readonly_skill_file(
        mut self,
        path: impl Into<PathBuf>,
        content: impl Into<Vec<u8>>,
    ) -> Self {
        let file = VirtualFile::new(path, content);
        if let Some(skills) = self.mounts.iter_mut().find(|mount| {
            mount.path == Path::new("/skills") && mount.mode == VirtualMountMode::ReadOnly
        }) {
            skills.files.push(file);
        } else {
            self.mounts
                .push(VirtualMount::readonly("/skills", vec![file]));
        }
        self
    }

    pub fn with_operation_registry(mut self, registry: Arc<OperationRegistry>) -> Self {
        self.operation_registry = Some(registry);
        self
    }

    /// Reuse a thread workspace VFS instead of constructing a private bash tree.
    pub fn with_workspace_vfs(mut self, vfs: Arc<CooldisVfs>) -> Self {
        self.workspace_vfs = Some(vfs);
        self
    }

    pub fn with_capability_grant(mut self, grant: impl Into<String>) -> Self {
        self.capability_grants.insert(grant.into());
        self
    }

    pub fn with_capability_grants(mut self, grants: impl IntoIterator<Item = String>) -> Self {
        self.capability_grants.extend(grants);
        self
    }

    pub fn with_execution_policy(mut self, policy: BashExecutionPolicy) -> Self {
        self.execution_policy = policy;
        self
    }

    pub fn with_external_executor(mut self, executor: Arc<dyn ExternalCommandExecutor>) -> Self {
        self.external_executor = Some(executor);
        self
    }

    pub fn with_host_bash_executor(mut self, workspace_root: impl Into<PathBuf>) -> Self {
        self.external_executor = Some(Arc::new(HostBashExecutor::new(workspace_root)));
        self
    }
}

impl From<VirtualBashRuntimeConfig> for BashkitExecutionConfig {
    fn from(config: VirtualBashRuntimeConfig) -> Self {
        Self {
            cwd: config.cwd,
            execution_timeout: config.execution_timeout,
            parser_timeout: config.parser_timeout,
            max_commands: config.max_commands,
            max_loop_iterations: config.max_loop_iterations,
            max_output_bytes: config.max_output_bytes,
            mounts: config.mounts,
            operation_registry: config.operation_registry.map(|registry| {
                Arc::new(KernelVbashOperationRegistry::new(registry))
                    as Arc<dyn VbashOperationRegistry>
            }),
            workspace_vfs: config.workspace_vfs,
            capability_grants: config.capability_grants,
            execution_policy: config.execution_policy,
            external_executor: config.external_executor,
        }
    }
}

#[async_trait]
impl VbashOperationRegistry for KernelVbashOperationRegistry {
    async fn describe(&self, name: &str) -> Option<cooldis_operations::RegisteredOperation> {
        self.registry.describe(name).await
    }

    async fn list(&self) -> Vec<cooldis_operations::RegisteredOperation> {
        self.registry.list().await
    }

    async fn invoke_process_output(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: Vec<u8>,
    ) -> Result<VirtualCommandOutput, String> {
        let process = self
            .registry
            .invoke_process(registered_name, operation_name, input)
            .await
            .map_err(|err| err.to_string())?;
        Ok(VirtualCommandOutput::from(&process.output()))
    }
}

struct KernelVbashOperationRegistry {
    registry: Arc<OperationRegistry>,
}

impl KernelVbashOperationRegistry {
    fn new(registry: Arc<OperationRegistry>) -> Self {
        Self { registry }
    }
}

pub struct BashToolProvider {
    config: VirtualBashRuntimeConfig,
    harness: Mutex<Option<BashkitExecutionHarness>>,
    process_manager: AsyncExecutionManager,
    live_backend: Arc<dyn LiveProcessBackend>,
}

impl BashToolProvider {
    pub fn new(config: VirtualBashRuntimeConfig) -> Self {
        let live_backend: Arc<dyn LiveProcessBackend> =
            Arc::new(BashkitLiveBackend::new(config.clone()));
        Self {
            config,
            harness: Mutex::new(None),
            process_manager: AsyncExecutionManager::default(),
            live_backend,
        }
    }
}

#[async_trait]
impl AgentKernelToolProvider for BashToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        let mut description =
            "Run a command inside the Cooldis virtual bash environment.".to_string();
        if let Some(registry) = &self.config.operation_registry {
            let reserved_commands =
                operation_shell_reserved_commands(&self.config.execution_policy);
            let registry_adapter = KernelVbashOperationRegistry::new(Arc::clone(registry));
            let shell_commands =
                operation_shell_command_names(&registry_adapter, &reserved_commands).await;
            if !shell_commands.is_empty() {
                description.push_str(&format!(
                    " Published operation commands are available directly: {}.",
                    summarize_operation_shell_commands(&shell_commands)
                ));
            }
        }
        vec![
            ToolDefinition::new(
                BASH_TOOL,
                description,
                json!({
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "Shell command to execute."
                        }
                    },
                    "required": ["command"],
                    "additionalProperties": false
                }),
            ),
            ToolDefinition::new(
                PROCESS_EXEC_TOOL,
                "Start or poll a Codex-style process handle. This is the provider-safe projection of process.exec over Cooldis virtual bash.",
                json!({
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "Virtual bash script to start."
                        },
                        "process_id": {
                            "type": "string",
                            "description": "Existing Cooldis process id to poll."
                        },
                        "yield_time_ms": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "How long to wait for output or terminal state before returning."
                        },
                        "timeout_ms": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Hard execution deadline for a newly started process."
                        },
                        "output_bytes_cap": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Maximum stdout/stderr bytes retained for this snapshot."
                        }
                    },
                    "additionalProperties": false
                }),
            ),
            ToolDefinition::new(
                WRITE_STDIN_TOOL,
                "Write bytes to a Cooldis process handle, then poll it. Bashkit virtual bash returns structured unsupported until it has an input sink.",
                json!({
                    "type": "object",
                    "properties": {
                        "process_id": {
                            "type": "string",
                            "description": "Cooldis process id returned by process_exec."
                        },
                        "delta_base64": {
                            "type": "string",
                            "description": "Base64 encoded stdin bytes."
                        },
                        "yield_time_ms": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "How long to wait for output or terminal state before returning."
                        },
                        "output_bytes_cap": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Maximum stdout/stderr bytes retained for this snapshot."
                        }
                    },
                    "required": ["process_id", "delta_base64"],
                    "additionalProperties": false
                }),
            ),
        ]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        match call.tool_name.as_str() {
            BASH_TOOL => self.invoke_bash_tool(call).await.map(Some),
            PROCESS_EXEC_TOOL => self.invoke_process_exec_tool(call).await.map(Some),
            WRITE_STDIN_TOOL => self.invoke_write_stdin_tool(call).await.map(Some),
            _ => Ok(None),
        }
    }
}

impl BashToolProvider {
    async fn invoke_bash_tool(&self, call: AgentKernelToolCall) -> CooldisResult<CanonicalMessage> {
        let args: BashToolArgs = serde_json::from_value(call.arguments).map_err(|err| {
            CooldisError::RuntimeExecution(format!(
                "tool {BASH_TOOL:?} has invalid arguments: {err}"
            ))
        })?;
        let mut harness = self.harness.lock().await;
        if harness.is_none() {
            *harness = Some(BashkitExecutionHarness::new(self.config.clone()).await?);
        }
        let harness = harness.as_mut().ok_or_else(|| {
            CooldisError::RuntimeExecution("bash harness did not initialize".to_string())
        })?;
        let output = harness.execute(&args.command).await?;
        let output_json = serde_json::to_string(&json!({
            "stdout": output.stdout,
            "stderr": output.stderr,
            "exit_code": output.exit_code,
            "stdout_truncated": output.stdout_truncated,
            "stderr_truncated": output.stderr_truncated,
        }))
        .map_err(execution_error)?;
        Ok(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            output_json,
            !output.success(),
        ))
    }

    async fn invoke_process_exec_tool(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<CanonicalMessage> {
        let call_id = call.call_id;
        let tool_name = call.tool_name;
        let turn_context = call.turn_context;
        let args: ProcessExecToolArgs = serde_json::from_value(call.arguments).map_err(|err| {
            CooldisError::RuntimeExecution(format!(
                "tool {PROCESS_EXEC_TOOL:?} has invalid arguments: {err}"
            ))
        })?;
        let output_cap = process_output_cap(args.output_bytes_cap, self.config.max_output_bytes);
        let yield_time = process_yield_time(args.yield_time_ms);
        let outcome = if let Some(process_id) = args.process_id {
            let process_id = process_id.parse::<CooldisProcessId>().map_err(|err| {
                CooldisError::RuntimeExecution(format!(
                    "tool {PROCESS_EXEC_TOOL:?} requires a valid Cooldis process_id: {err}"
                ))
            })?;
            self.process_manager
                .poll(process_id, yield_time, output_cap)
                .await?
        } else {
            let command = args.command.ok_or_else(|| {
                CooldisError::RuntimeExecution(format!(
                    "tool {PROCESS_EXEC_TOOL:?} requires command or process_id"
                ))
            })?;
            let timeout = args
                .timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(self.config.execution_timeout);
            let request = AsyncProcessStartRequest::virtual_bash_script(command)
                .with_owner(process_tool_owner(
                    &turn_context,
                    &call_id,
                    "kernel-tool:process_exec",
                ))
                .with_deadline(ExecutionDeadline::from_now(timeout))
                .with_yield_time(yield_time)
                .with_output_cap_bytes(output_cap);
            self.process_manager
                .start(Arc::clone(&self.live_backend), request)
                .await?
        };
        let is_error = process_snapshot_is_error(&outcome.snapshot);
        let output_json = serde_json::to_string(&process_snapshot_json(&outcome.snapshot))
            .map_err(execution_error)?;
        Ok(CanonicalMessage::tool_result(
            call_id,
            tool_name,
            output_json,
            is_error,
        ))
    }

    async fn invoke_write_stdin_tool(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<CanonicalMessage> {
        let call_id = call.call_id;
        let tool_name = call.tool_name;
        let args: WriteStdinToolArgs = serde_json::from_value(call.arguments).map_err(|err| {
            CooldisError::RuntimeExecution(format!(
                "tool {WRITE_STDIN_TOOL:?} has invalid arguments: {err}"
            ))
        })?;
        let process_id = args.process_id.parse::<CooldisProcessId>().map_err(|err| {
            CooldisError::RuntimeExecution(format!(
                "tool {WRITE_STDIN_TOOL:?} requires a valid Cooldis process_id: {err}"
            ))
        })?;
        let bytes = STANDARD.decode(args.delta_base64).map_err(|err| {
            CooldisError::RuntimeExecution(format!(
                "tool {WRITE_STDIN_TOOL:?} requires valid base64 delta_base64: {err}"
            ))
        })?;
        let output_cap = process_output_cap(args.output_bytes_cap, self.config.max_output_bytes);
        let yield_time = process_yield_time(args.yield_time_ms);
        match self
            .process_manager
            .write(process_id, bytes, yield_time, output_cap)
            .await
        {
            Ok(outcome) => {
                let is_error = process_snapshot_is_error(&outcome.snapshot);
                let output_json = serde_json::to_string(&process_snapshot_json(&outcome.snapshot))
                    .map_err(execution_error)?;
                Ok(CanonicalMessage::tool_result(
                    call_id,
                    tool_name,
                    output_json,
                    is_error,
                ))
            }
            Err(err) => {
                let output_json = serde_json::to_string(&json!({
                    "status": "unsupported",
                    "process_id": process_id.to_string(),
                    "error": err.to_string(),
                }))
                .map_err(execution_error)?;
                Ok(CanonicalMessage::tool_result(
                    call_id,
                    tool_name,
                    output_json,
                    true,
                ))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BashToolArgs {
    command: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessExecToolArgs {
    command: Option<String>,
    process_id: Option<String>,
    yield_time_ms: Option<u64>,
    timeout_ms: Option<u64>,
    output_bytes_cap: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteStdinToolArgs {
    process_id: String,
    delta_base64: String,
    yield_time_ms: Option<u64>,
    output_bytes_cap: Option<usize>,
}

fn process_tool_owner(
    turn_context: &Option<TurnContextSnapshot>,
    call_id: &str,
    surface: &str,
) -> AsyncProcessOwner {
    AsyncProcessOwner {
        thread_id: turn_context
            .as_ref()
            .map(|snapshot| snapshot.coordinates.thread_id.to_string()),
        turn_id: turn_context
            .as_ref()
            .map(|snapshot| snapshot.turn_id.clone()),
        call_id: Some(call_id.to_string()),
        surface: Some(surface.to_string()),
    }
}

fn process_yield_time(yield_time_ms: Option<u64>) -> Duration {
    Duration::from_millis(yield_time_ms.unwrap_or(10_000).min(30_000))
}

fn process_output_cap(output_bytes_cap: Option<usize>, default_cap: usize) -> usize {
    output_bytes_cap
        .unwrap_or(default_cap)
        .clamp(1, default_cap.max(1))
}

fn process_snapshot_is_error(snapshot: &AsyncProcessSnapshot) -> bool {
    match snapshot.status {
        ProcessSnapshotStatus::Running | ProcessSnapshotStatus::Completed => {
            snapshot.exit_code.is_some_and(|code| code != 0)
        }
        ProcessSnapshotStatus::Failed
        | ProcessSnapshotStatus::TimedOut
        | ProcessSnapshotStatus::Cancelled => true,
    }
}

fn process_snapshot_json(snapshot: &AsyncProcessSnapshot) -> serde_json::Value {
    json!({
        "process_id": snapshot.process_id.map(|process_id| process_id.to_string()),
        "backend": snapshot.backend,
        "label": snapshot.label,
        "status": snapshot.status.as_str(),
        "exit_code": snapshot.exit_code,
        "stdout": String::from_utf8_lossy(&snapshot.stdout),
        "stderr": String::from_utf8_lossy(&snapshot.stderr),
        "truncated": snapshot.stdout_truncated || snapshot.stderr_truncated,
        "stdout_truncated": snapshot.stdout_truncated,
        "stderr_truncated": snapshot.stderr_truncated,
    })
}

#[derive(Clone, Debug)]
pub struct VirtualBashRuntimeFactory {
    config: VirtualBashRuntimeConfig,
}

impl VirtualBashRuntimeFactory {
    pub fn new(config: VirtualBashRuntimeConfig) -> Self {
        Self { config }
    }
}

impl Default for VirtualBashRuntimeFactory {
    fn default() -> Self {
        Self::new(VirtualBashRuntimeConfig::default())
    }
}

#[async_trait]
impl AgentRuntimeFactory for VirtualBashRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(VirtualBashRuntime {
            config: self.config.clone(),
        }))
    }
}

struct VirtualBashRuntime {
    config: VirtualBashRuntimeConfig,
}

#[async_trait]
impl AgentRuntime for VirtualBashRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let harness = match BashkitExecutionHarness::new(self.config).await {
            Ok(harness) => harness,
            Err(err) => {
                let _ = status.send(ThreadStatus::Failed);
                let _ = events.send(ThreadEvent::Failed {
                    thread_id,
                    message: err.to_string(),
                });
                return;
            }
        };
        let mut harness = Some(harness);

        emit_runtime_event(
            &events,
            &coordinates,
            RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
                metadata: context.metadata.clone(),
            },
        );
        let _ = events.send(ThreadEvent::Started { context });
        let _ = status.send(ThreadStatus::Idle);
        let mut pending_submits = VecDeque::new();

        loop {
            if let Some(ThreadCommand::Submit { turn_id, input, .. }) = pending_submits.pop_front()
            {
                let _ = status.send(ThreadStatus::Running);
                match services
                    .append_user_turn_input(&coordinates, &turn_id, &input)
                    .await
                {
                    Ok(entry) => {
                        let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                    }
                    Err(err) => {
                        let _ = status.send(ThreadStatus::Failed);
                        let _ = events.send(ThreadEvent::Failed {
                            thread_id,
                            message: err.to_string(),
                        });
                        break;
                    }
                }
                let watchdog_token_id = input.turn_watchdog_id();
                if run_virtual_turn(
                    &mut harness,
                    &services,
                    &coordinates,
                    thread_id,
                    input.text_projection(),
                    watchdog_token_id,
                    &events,
                    &status,
                    &mut commands,
                    &cancellation,
                    &mut pending_submits,
                )
                .await
                {
                    break;
                }
                continue;
            }

            tokio::select! {
                _ = cancellation.cancelled() => {
                    if let Some(harness) = harness.as_ref() {
                        harness.cancel();
                    }
                    break;
                }
                command = commands.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    match command {
                        ThreadCommand::Submit { turn_id, input, mode } => {
                            if mode == TurnSubmissionMode::Steer {
                                emit_runtime_event(
                                    &events,
                                    &coordinates,
                                    RuntimeEventKind::PolicyRejected {
                                        code: "no_active_turn".to_string(),
                                        message: "steer input requires an active virtual bash turn".to_string(),
                                    },
                                );
                                continue;
                            }
                            let _ = status.send(ThreadStatus::Running);
                            match services.append_user_turn_input(&coordinates, &turn_id, &input).await {
                                Ok(entry) => {
                                    let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                                }
                                Err(err) => {
                                    let _ = status.send(ThreadStatus::Failed);
                                    let _ = events.send(ThreadEvent::Failed {
                                        thread_id,
                                        message: err.to_string(),
                                    });
                                    break;
                                }
                            }
                            let watchdog_token_id = input.turn_watchdog_id();
                            if run_virtual_turn(
                                &mut harness,
                                &services,
                                &coordinates,
                                thread_id,
                                input.text_projection(),
                                watchdog_token_id,
                                &events,
                                &status,
                                &mut commands,
                                &cancellation,
                                &mut pending_submits,
                            )
                            .await
                            {
                                break;
                            }
                        }
                        ThreadCommand::Cancel { reason } => {
                            let _ = status.send(ThreadStatus::Cancelling);
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        ThreadCommand::CancelTurn { .. } => {}
                        ThreadCommand::Compact { .. } => {
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::PolicyRejected {
                                    code: "compact_unsupported".to_string(),
                                    message: "Virtual bash runtime does not support Cooldis compaction commands".to_string(),
                                },
                            );
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        ThreadCommand::ResumeToolCall { .. } => {
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::PolicyRejected {
                                    code: "tool_resume_unsupported".to_string(),
                                    message: "Virtual bash runtime does not support provider tool-call resume".to_string(),
                                },
                            );
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        ThreadCommand::Shutdown => {
                            let _ = events.send(ThreadEvent::Signal {
                                thread_id,
                                signal: ThreadSignal::shutdown(&coordinates),
                            });
                            emit_runtime_event(
                                &events,
                                &coordinates,
                                RuntimeEventKind::Terminal {
                                    state: RuntimeTerminalState::Stopped,
                                },
                            );
                            break;
                        }
                    }
                }
            }
        }

        emit_runtime_event(
            &events,
            &coordinates,
            RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::Stopped,
            },
        );
        let _ = status.send(ThreadStatus::Stopped);
        let _ = events.send(ThreadEvent::Stopped { thread_id });
    }
}

async fn run_virtual_turn(
    harness: &mut Option<BashkitExecutionHarness>,
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    input: String,
    watchdog_token_id: Option<u64>,
    events: &broadcast::Sender<ThreadEvent>,
    status: &watch::Sender<ThreadStatus>,
    commands: &mut mpsc::Receiver<ThreadCommand>,
    cancellation: &CancellationToken,
    pending_submits: &mut VecDeque<ThreadCommand>,
) -> bool {
    let Some(turn_harness) = harness.take() else {
        let _ = status.send(ThreadStatus::Failed);
        let _ = events.send(ThreadEvent::Failed {
            thread_id,
            message: "virtual bash execution harness was unavailable".to_string(),
        });
        return true;
    };
    let cancel_flag = turn_harness.cancellation_flag();
    let mut cancelled_reason = None;
    let mut shutdown_after_turn = false;
    let mut failed = false;
    let mut runtime_cancellation_observed = false;
    let mut accept_control_commands = true;
    let mut execute = match spawn_virtual_bash_execution(turn_harness, input) {
        Ok(execute) => execute,
        Err(err) => {
            let _ = status.send(ThreadStatus::Failed);
            let _ = events.send(ThreadEvent::Failed {
                thread_id,
                message: err.to_string(),
            });
            return true;
        }
    };

    let result = loop {
        tokio::select! {
            biased;

            _ = cancellation.cancelled(), if !runtime_cancellation_observed => {
                runtime_cancellation_observed = true;
                cancel_flag.store(true, Ordering::SeqCst);
                cancelled_reason = Some("runtime cancellation requested".to_string());
                accept_control_commands = false;
            }
            // Keep control traffic ahead of completed execution so cancel/interrupt
            // wins races with a finishing virtual-bash turn.
            command = commands.recv(), if accept_control_commands => {
                match command {
                    Some(ThreadCommand::Cancel { reason }) => {
                        let _ = status.send(ThreadStatus::Cancelling);
                        let _ = events.send(ThreadEvent::Signal {
                            thread_id,
                            signal: ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        cancel_flag.store(true, Ordering::SeqCst);
                        cancelled_reason = Some(reason);
                        accept_control_commands = false;
                    }
                    Some(ThreadCommand::CancelTurn {
                        watchdog_token_id: target_token_id,
                        reason,
                    }) => {
                        if watchdog_token_id != Some(target_token_id) {
                            continue;
                        }
                        let _ = status.send(ThreadStatus::Cancelling);
                        let _ = events.send(ThreadEvent::Signal {
                            thread_id,
                            signal: ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        cancel_flag.store(true, Ordering::SeqCst);
                        cancelled_reason = Some(reason);
                        accept_control_commands = false;
                    }
                    Some(ThreadCommand::Shutdown) => {
                        let _ = events.send(ThreadEvent::Signal {
                            thread_id,
                            signal: ThreadSignal::shutdown(coordinates),
                        });
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::Terminal {
                                state: RuntimeTerminalState::Stopped,
                            },
                        );
                        cancel_flag.store(true, Ordering::SeqCst);
                        shutdown_after_turn = true;
                        accept_control_commands = false;
                    }
                    Some(ThreadCommand::Submit { turn_id, input, mode }) => {
                        match mode {
                            TurnSubmissionMode::Queue => {
                                let _ = events.send(ThreadEvent::Signal {
                                    thread_id,
                                    signal: ThreadSignal::user_queue(coordinates, turn_id.clone()),
                                });
                                pending_submits.push_back(ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode,
                                });
                            }
                            TurnSubmissionMode::Steer => {
                                emit_runtime_event(
                                    events,
                                    coordinates,
                                    RuntimeEventKind::PolicyRejected {
                                        code: "active_turn_not_steerable".to_string(),
                                        message: "Virtual bash runtime does not support same-turn steering".to_string(),
                                    },
                                );
                            }
                            TurnSubmissionMode::Interrupt => {
                                let reason = format!("interrupted by turn {turn_id}");
                                let _ = status.send(ThreadStatus::Cancelling);
                                let _ = events.send(ThreadEvent::Signal {
                                    thread_id,
                                    signal: ThreadSignal::user_interrupt(coordinates, turn_id.clone()),
                                });
                                emit_runtime_event(
                                    events,
                                    coordinates,
                                    RuntimeEventKind::Cancelled {
                                        reason: reason.clone(),
                                    },
                                );
                                cancel_flag.store(true, Ordering::SeqCst);
                                cancelled_reason = Some(reason);
                                accept_control_commands = false;
                                pending_submits.push_front(ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode: TurnSubmissionMode::Queue,
                                });
                            }
                        }
                    }
                    Some(ThreadCommand::Compact { .. }) => {
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::PolicyRejected {
                                code: "compact_unsupported".to_string(),
                                message: "Virtual bash runtime does not support Cooldis compaction commands".to_string(),
                            },
                        );
                    }
                    Some(ThreadCommand::ResumeToolCall { .. }) => {
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::PolicyRejected {
                                code: "tool_resume_unsupported".to_string(),
                                message: "Virtual bash runtime does not support provider tool-call resume".to_string(),
                            },
                        );
                    }
                    None => {
                        let _ = events.send(ThreadEvent::Signal {
                            thread_id,
                            signal: ThreadSignal::shutdown(coordinates),
                        });
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::Terminal {
                                state: RuntimeTerminalState::Stopped,
                            },
                        );
                        cancel_flag.store(true, Ordering::SeqCst);
                        shutdown_after_turn = true;
                        accept_control_commands = false;
                    }
                }
            }
            result = &mut execute => {
                match result {
                    Ok(VirtualBashExecutionResult { harness: returned_harness, result }) => {
                        *harness = Some(returned_harness);
                        break result;
                    }
                    Err(err) => {
                        break Err(CooldisError::RuntimeExecution(format!(
                            "virtual bash execution thread stopped before returning a result: {err}"
                        )));
                    }
                }
            }
        }
    };

    let was_cancelled = cancelled_reason.is_some();
    if let Some(reason) = cancelled_reason {
        let _ = status.send(ThreadStatus::Idle);
        emit_runtime_event(
            events,
            coordinates,
            RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::Cancelled,
            },
        );
        let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
    } else {
        match result {
            Ok(output) => {
                let text = output.event_text();
                if !text.is_empty() {
                    emit_runtime_event(
                        events,
                        coordinates,
                        RuntimeEventKind::TextDelta { text: text.clone() },
                    );
                    let _ = events.send(ThreadEvent::Output {
                        thread_id,
                        text: text.clone(),
                    });
                    mirror_virtual_output(services, coordinates, thread_id, text, events).await;
                }
            }
            Err(err) => {
                let _ = status.send(ThreadStatus::Failed);
                let _ = events.send(ThreadEvent::Signal {
                    thread_id,
                    signal: ThreadSignal::failed(coordinates, err.to_string()),
                });
                emit_runtime_event(
                    events,
                    coordinates,
                    RuntimeEventKind::Failed {
                        code: "runtime_execution".to_string(),
                        message: err.to_string(),
                    },
                );
                let _ = events.send(ThreadEvent::Failed {
                    thread_id,
                    message: err.to_string(),
                });
                failed = true;
            }
        }
    }

    if !shutdown_after_turn && !was_cancelled && !failed {
        let _ = status.send(ThreadStatus::Idle);
    }
    shutdown_after_turn || failed
}

struct VirtualBashExecutionResult {
    harness: BashkitExecutionHarness,
    result: CooldisResult<VirtualCommandOutput>,
}

fn spawn_virtual_bash_execution(
    mut harness: BashkitExecutionHarness,
    input: String,
) -> CooldisResult<oneshot::Receiver<VirtualBashExecutionResult>> {
    let (tx, rx) = oneshot::channel();
    thread::Builder::new()
        .name("cooldis-vbash".to_string())
        .spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(execution_error)
                .and_then(|runtime| Ok(runtime.block_on(harness.execute(&input))?));
            let _ = tx.send(VirtualBashExecutionResult { harness, result });
        })
        .map_err(execution_error)?;
    Ok(rx)
}

async fn mirror_virtual_output(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    text: String,
    events: &broadcast::Sender<ThreadEvent>,
) {
    if let Ok(entry) = services
        .append_session_entry(
            coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::assistant(
                    "cooldis",
                    ProviderApi::Other("virtual_bash".to_string()),
                    "bashkit",
                    vec![CanonicalContent::text(text)],
                    CanonicalStopReason::EndTurn,
                ),
            },
        )
        .await
    {
        let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
    }
}

fn execution_error(err: impl std::fmt::Display) -> CooldisError {
    CooldisError::RuntimeExecution(err.to_string())
}

#[cfg(test)]
mod tests;
