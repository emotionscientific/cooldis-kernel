use base64::Engine as _;
use sha2::Digest as _;
pub use verlet_vbash::{
    BASH_TOOL, BashExecutionPolicy, BashkitExecutionConfig, BashkitExecutionHarness,
    BashkitLiveBackend, CommandRoute, CommandRoutingPolicy, OverflowPlan,
    SPILL_RETENTION_MAX_BYTES, VbashOperationRegistry, VirtualFile, VirtualMount,
    VirtualMountBackend, VirtualMountMode, absolute_mount_path, apply_external_file_writes,
    build_emergency_spill_stub, default_virtual_mounts, deny_output, enforce_output_limit,
    exec_result_from_virtual_output, format_spill_stub, missing_operation_capability_grants,
    operation_shell_command_name, operation_shell_command_names, operation_shell_input,
    operation_shell_manual, operation_shell_reserved_commands, plan_output_overflow,
    reserved_operation_shell_commands, summarize_operation_shell_commands, validate_mounts,
    verlet_usage, virtual_command_output_from_exec_result,
};

pub use verlet_process::{
    AsyncExecutionManager, AsyncProcessOwner, AsyncProcessSnapshot, AsyncProcessStartRequest,
    ExecutionDeadline, ExternalCommandExecutor, ExternalCommandInvocation, ExternalCommandRequest,
    ExternalCommandResult, ExternalExecutorKind, ExternalFileWrite, HostBashExecutor,
    HostBashExecutorConfig, LiveProcessBackend, ProcessSnapshotStatus,
    RejectingExternalCommandExecutor, VerletProcessId, VerletProcessResult, VirtualCommandOutput,
};

pub const PROCESS_EXEC_TOOL: &str = "process_exec";
pub const WRITE_STDIN_TOOL: &str = "write_stdin";

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolOutputSpillReceipt {
    pub path: String,
    pub total_bytes: usize,
    pub preview_bytes: usize,
    #[serde(default, skip_serializing_if = "is_false")]
    pub retention_truncated: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolOutputSpill {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<ToolOutputSpillReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<ToolOutputSpillReceipt>,
}

impl ToolOutputSpill {
    pub fn is_empty(&self) -> bool {
        self.stdout.is_none() && self.stderr.is_none()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BashToolResultPayload {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    #[serde(default, skip_serializing_if = "ToolOutputSpill::is_empty")]
    pub spill: ToolOutputSpill,
}

#[derive(Clone)]
pub struct VirtualBashRuntimeConfig {
    pub cwd: std::path::PathBuf,
    pub execution_timeout: std::time::Duration,
    pub parser_timeout: std::time::Duration,
    pub max_commands: usize,
    pub max_loop_iterations: usize,
    pub max_output_bytes: usize,
    pub mounts: Vec<VirtualMount>,
    pub operation_registry: Option<std::sync::Arc<crate::OperationRegistry>>,
    /// Thread workspace VFS shared with catalog-loaded operations when both surfaces
    /// must re-present one filesystem tree.
    pub workspace_vfs: Option<std::sync::Arc<crate::capabilities::vfs::VerletVfs>>,
    pub capability_grants: std::collections::BTreeSet<String>,
    pub capability_grant_expiries: Vec<crate::AgentManifestGrantExpiry>,
    pub execution_policy: BashExecutionPolicy,
    pub external_executor: Option<std::sync::Arc<dyn ExternalCommandExecutor>>,
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
                &self.workspace_vfs.as_ref().map(|_| "<VerletVfs>"),
            )
            .field("capability_grants", &self.capability_grants)
            .field("capability_grant_expiries", &self.capability_grant_expiries)
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
            cwd: std::path::PathBuf::from("/workspace"),
            execution_timeout: std::time::Duration::from_secs(10),
            parser_timeout: std::time::Duration::from_secs(2),
            max_commands: 10_000,
            max_loop_iterations: 10_000,
            max_output_bytes: 1_048_576,
            mounts: default_virtual_mounts(),
            operation_registry: None,
            workspace_vfs: None,
            capability_grants: std::collections::BTreeSet::new(),
            capability_grant_expiries: Vec::new(),
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

    pub fn with_writable_mount(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.mounts.push(VirtualMount::writable(path));
        self
    }

    pub fn with_readonly_mount(
        mut self,
        path: impl Into<std::path::PathBuf>,
        files: Vec<VirtualFile>,
    ) -> Self {
        self.mounts.push(VirtualMount::readonly(path, files));
        self
    }

    pub fn with_object_store_mount(
        mut self,
        path: impl Into<std::path::PathBuf>,
        config: crate::capabilities::vfs::ObjectStoreMountConfig,
    ) -> Self {
        self.mounts.push(VirtualMount::object_store(path, config));
        self
    }

    pub fn with_readonly_object_store_mount(
        mut self,
        path: impl Into<std::path::PathBuf>,
        config: crate::capabilities::vfs::ObjectStoreMountConfig,
    ) -> Self {
        self.mounts
            .push(VirtualMount::readonly_object_store(path, config));
        self
    }

    pub fn with_readonly_skill_file(
        mut self,
        path: impl Into<std::path::PathBuf>,
        content: impl Into<Vec<u8>>,
    ) -> Self {
        let file = VirtualFile::new(path, content);
        if let Some(skills) = self.mounts.iter_mut().find(|mount| {
            mount.path == std::path::Path::new("/skills")
                && mount.mode == VirtualMountMode::ReadOnly
        }) {
            skills.files.push(file);
        } else {
            self.mounts
                .push(VirtualMount::readonly("/skills", vec![file]));
        }
        self
    }

    pub fn with_operation_registry(
        mut self,
        registry: std::sync::Arc<crate::OperationRegistry>,
    ) -> Self {
        self.operation_registry = Some(registry);
        self
    }

    /// Reuse a thread workspace VFS instead of constructing a private bash tree.
    pub fn with_workspace_vfs(
        mut self,
        vfs: std::sync::Arc<crate::capabilities::vfs::VerletVfs>,
    ) -> Self {
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

    pub fn with_capability_grant_expiries(
        mut self,
        expiries: impl IntoIterator<Item = crate::AgentManifestGrantExpiry>,
    ) -> Self {
        self.capability_grant_expiries.extend(expiries);
        self
    }

    pub fn with_execution_policy(mut self, policy: BashExecutionPolicy) -> Self {
        self.execution_policy = policy;
        self
    }

    pub fn with_external_executor(
        mut self,
        executor: std::sync::Arc<dyn ExternalCommandExecutor>,
    ) -> Self {
        self.external_executor = Some(executor);
        self
    }

    pub fn with_host_bash_executor(
        mut self,
        workspace_root: impl Into<std::path::PathBuf>,
    ) -> Self {
        self.external_executor = Some(std::sync::Arc::new(HostBashExecutor::new(workspace_root)));
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
                std::sync::Arc::new(KernelVbashOperationRegistry::new(registry))
                    as std::sync::Arc<dyn VbashOperationRegistry>
            }),
            workspace_vfs: config.workspace_vfs,
            capability_grants: config.capability_grants,
            execution_policy: config.execution_policy,
            external_executor: config.external_executor,
        }
    }
}

#[async_trait::async_trait]
impl VbashOperationRegistry for KernelVbashOperationRegistry {
    async fn describe(&self, name: &str) -> Option<verlet_operations::RegisteredOperation> {
        self.registry.describe(name).await
    }

    async fn list(&self) -> Vec<verlet_operations::RegisteredOperation> {
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
    registry: std::sync::Arc<crate::OperationRegistry>,
}

impl KernelVbashOperationRegistry {
    fn new(registry: std::sync::Arc<crate::OperationRegistry>) -> Self {
        Self { registry }
    }
}

pub struct BashToolProvider {
    config: VirtualBashRuntimeConfig,
    harness: tokio::sync::Mutex<Option<BashkitExecutionHarness>>,
    process_manager: AsyncExecutionManager,
    live_backend: std::sync::Arc<dyn LiveProcessBackend>,
    process_dispatcher: Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher>,
}

impl BashToolProvider {
    pub fn new(config: VirtualBashRuntimeConfig) -> Self {
        let live_backend: std::sync::Arc<dyn LiveProcessBackend> =
            std::sync::Arc::new(BashkitLiveBackend::new(config.clone()));
        Self {
            config,
            harness: tokio::sync::Mutex::new(None),
            process_manager: AsyncExecutionManager::default(),
            live_backend,
            process_dispatcher: None,
        }
    }

    pub fn with_process_dispatcher(
        mut self,
        dispatcher: crate::kernel::process_handle_dispatch::ProcessHandleDispatcher,
    ) -> Self {
        self.process_dispatcher = Some(dispatcher);
        self
    }
}

#[async_trait::async_trait]
impl crate::AgentKernelToolProvider for BashToolProvider {
    async fn tool_definitions(&self) -> Vec<crate::ToolDefinition> {
        let mut description =
            "Run a command inside the Verlet virtual bash environment.".to_string();
        if let Some(registry) = &self.config.operation_registry {
            let reserved_commands =
                operation_shell_reserved_commands(&self.config.execution_policy);
            let registry_adapter =
                KernelVbashOperationRegistry::new(std::sync::Arc::clone(registry));
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
            crate::ToolDefinition::new(
                BASH_TOOL,
                description,
                serde_json::json!({
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
            crate::ToolDefinition::new(
                PROCESS_EXEC_TOOL,
                "Start or poll a Codex-style process handle. This is the provider-safe projection of process.exec over Verlet virtual bash.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "Virtual bash script to start."
                        },
                        "process_id": {
                            "type": "string",
                            "description": "Existing Verlet process id to poll."
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
                            "description": "Maximum stdout/stderr bytes kept inline before spill for this snapshot."
                        }
                    },
                    "additionalProperties": false
                }),
            ),
            crate::ToolDefinition::new(
                WRITE_STDIN_TOOL,
                "Write bytes to a Verlet process handle, then poll it. Bashkit virtual bash returns structured unsupported until it has an input sink.",
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "process_id": {
                            "type": "string",
                            "description": "Verlet process id returned by process_exec."
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
                            "description": "Maximum stdout/stderr bytes kept inline before spill for this snapshot."
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
        call: crate::AgentKernelToolCall,
    ) -> crate::VerletResult<Option<crate::kernel::history::CanonicalMessage>> {
        self.invoke_tool_call_inner(call, None).await
    }

    async fn invoke_tool_call_at(
        &self,
        call: crate::AgentKernelToolCall,
        now_ms: i64,
    ) -> crate::VerletResult<Option<crate::kernel::history::CanonicalMessage>> {
        crate::agent::manifest_bind::ensure_grant_expiries_live(
            &self.config.capability_grant_expiries,
            now_ms,
        )?;
        self.invoke_tool_call_inner(call, None).await
    }

    async fn invoke_tool_call_cancellable(
        &self,
        call: crate::AgentKernelToolCall,
        cancellation: crate::ToolInvocationCancellation,
    ) -> crate::VerletResult<crate::AgentKernelToolOutcome> {
        self.invoke_tool_call_inner(call, Some(cancellation))
            .await
            .map(crate::AgentKernelToolOutcome::Completed)
    }

    async fn invoke_tool_call_cancellable_at(
        &self,
        call: crate::AgentKernelToolCall,
        cancellation: crate::ToolInvocationCancellation,
        now_ms: i64,
    ) -> crate::VerletResult<crate::AgentKernelToolOutcome> {
        crate::agent::manifest_bind::ensure_grant_expiries_live(
            &self.config.capability_grant_expiries,
            now_ms,
        )?;
        self.invoke_tool_call_inner(call, Some(cancellation))
            .await
            .map(crate::AgentKernelToolOutcome::Completed)
    }
}

impl BashToolProvider {
    async fn invoke_tool_call_inner(
        &self,
        call: crate::AgentKernelToolCall,
        cancellation: Option<crate::ToolInvocationCancellation>,
    ) -> crate::VerletResult<Option<crate::kernel::history::CanonicalMessage>> {
        match call.tool_name.as_str() {
            BASH_TOOL => self
                .invoke_bash_tool(call, cancellation.as_ref())
                .await
                .map(Some),
            PROCESS_EXEC_TOOL => self
                .invoke_process_exec_tool(call, cancellation.as_ref())
                .await
                .map(Some),
            WRITE_STDIN_TOOL => self
                .invoke_write_stdin_tool(call, cancellation.as_ref())
                .await
                .map(Some),
            _ => Ok(None),
        }
    }

    async fn invoke_bash_tool(
        &self,
        call: crate::AgentKernelToolCall,
        cancellation: Option<&crate::ToolInvocationCancellation>,
    ) -> crate::VerletResult<crate::kernel::history::CanonicalMessage> {
        let args: BashToolArgs = serde_json::from_value(call.arguments).map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "tool {BASH_TOOL:?} has invalid arguments: {err}"
            ))
        })?;
        let mut harness = self.harness.lock().await;
        if harness.is_none() {
            *harness = Some(BashkitExecutionHarness::new(self.config.clone()).await?);
        }
        let harness = harness.as_mut().ok_or_else(|| {
            crate::VerletError::RuntimeExecution("bash harness did not initialize".to_string())
        })?;
        let output = match cancellation {
            Some(cancellation) => {
                harness
                    .execute_full_output_cancellable(&args.command, cancellation.token().clone())
                    .await?
            }
            None => harness.execute_full_output(&args.command).await?,
        };
        let is_error = !output.success();
        let (stdout, stdout_spill, stdout_spilled) = present_output_stream(
            harness,
            output.stdout.as_bytes(),
            self.config.max_output_bytes,
            output.stdout_truncated,
            &spill_path(&call.call_id, "stdout"),
        )
        .await;
        let (stderr, stderr_spill, stderr_spilled) = present_output_stream(
            harness,
            output.stderr.as_bytes(),
            self.config.max_output_bytes,
            output.stderr_truncated,
            &spill_path(&call.call_id, "stderr"),
        )
        .await;
        let spill = ToolOutputSpill {
            stdout: stdout_spill,
            stderr: stderr_spill,
        };
        let mut output = serde_json::json!({
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": output.exit_code,
            "stdout_truncated": output.stdout_truncated || stdout_spilled,
            "stderr_truncated": output.stderr_truncated || stderr_spilled,
        });
        insert_spill(&mut output, spill)?;
        let output_json = serde_json::to_string(&output).map_err(execution_error)?;
        Ok(crate::kernel::history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            output_json,
            is_error,
        ))
    }

    async fn invoke_process_exec_tool(
        &self,
        call: crate::AgentKernelToolCall,
        cancellation: Option<&crate::ToolInvocationCancellation>,
    ) -> crate::VerletResult<crate::kernel::history::CanonicalMessage> {
        let call_id = call.call_id;
        let tool_name = call.tool_name;
        let turn_context = call.turn_context;
        let args: ProcessExecToolArgs = serde_json::from_value(call.arguments).map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "tool {PROCESS_EXEC_TOOL:?} has invalid arguments: {err}"
            ))
        })?;
        let output_cap = process_output_cap(args.output_bytes_cap, self.config.max_output_bytes);
        let yield_time = process_yield_time(args.yield_time_ms);
        let mut dispatch_id = None;
        let outcome = if let Some(process_id) = args.process_id {
            let process_id = process_id.parse::<VerletProcessId>().map_err(|err| {
                crate::VerletError::RuntimeExecution(format!(
                    "tool {PROCESS_EXEC_TOOL:?} requires a valid Verlet process_id: {err}"
                ))
            })?;
            self.require_process_handle(process_id).await?;
            match cancellation {
                Some(cancellation) => {
                    tokio::select! {
                        biased;
                        _ = cancellation.token().cancelled() => {
                            self.terminate_process_for_tool(
                                process_id,
                                cancellation.grace(),
                            ).await?
                        }
                        outcome = self.process_manager.poll(
                            process_id,
                            yield_time,
                            SPILL_RETENTION_MAX_BYTES,
                        ) => outcome?,
                    }
                }
                None => {
                    self.process_manager
                        .poll(process_id, yield_time, SPILL_RETENTION_MAX_BYTES)
                        .await?
                }
            }
        } else {
            let command = args.command.ok_or_else(|| {
                crate::VerletError::RuntimeExecution(format!(
                    "tool {PROCESS_EXEC_TOOL:?} requires command or process_id"
                ))
            })?;
            let consumer = turn_context
                .as_ref()
                .map(|context| context.coordinates.clone())
                .ok_or_else(|| {
                    crate::VerletError::RuntimeExecution(
                        "process_exec requires a turn context for durable consumer binding"
                            .to_string(),
                    )
                })?;
            let dispatcher = self.process_dispatcher.as_ref().ok_or_else(|| {
                crate::VerletError::RuntimeExecution(
                    "process_exec requires the durable process dispatch ingress lane".to_string(),
                )
            })?;
            let timeout = args
                .timeout_ms
                .map(std::time::Duration::from_millis)
                .unwrap_or(self.config.execution_timeout);
            let digest = crate::kernel::process_handle_dispatch::command_digest(command.as_bytes());
            let request = AsyncProcessStartRequest::virtual_bash_script(command)
                .with_owner(process_tool_owner(
                    &turn_context,
                    &call_id,
                    "kernel-tool:process_exec",
                ))
                .with_deadline(ExecutionDeadline::from_now(timeout))
                .with_yield_time(yield_time)
                .with_output_cap_bytes(SPILL_RETENTION_MAX_BYTES);
            let id = verlet_runtime_contracts::DispatchId::new(call_id.clone());
            let mut outcome = match cancellation {
                Some(cancellation) => {
                    dispatcher
                        .dispatch_start_cancellable(
                            &consumer,
                            id.clone(),
                            digest,
                            self.process_manager.clone(),
                            std::sync::Arc::clone(&self.live_backend),
                            request,
                            cancellation.token().clone(),
                        )
                        .await?
                }
                None => {
                    dispatcher
                        .dispatch_start(
                            &consumer,
                            id.clone(),
                            digest,
                            self.process_manager.clone(),
                            std::sync::Arc::clone(&self.live_backend),
                            request,
                        )
                        .await?
                }
            };
            if let Some(cancellation) = cancellation
                && cancellation.is_cancelled()
                && outcome.snapshot.status == ProcessSnapshotStatus::Running
                && let Some(process_id) = outcome.snapshot.process_id
            {
                outcome = self
                    .terminate_process_for_tool(process_id, cancellation.grace())
                    .await?;
            }
            dispatch_id = Some(id);
            outcome
        };
        let is_error = process_snapshot_is_error(&outcome.snapshot);
        let mut output = self
            .process_snapshot_json_with_spill(&outcome.snapshot, &call_id, output_cap)
            .await?;
        if let Some(dispatch_id) = dispatch_id {
            output["dispatch_id"] = serde_json::json!(dispatch_id.to_string());
        }
        let output_json = serde_json::to_string(&output).map_err(execution_error)?;
        Ok(crate::kernel::history::CanonicalMessage::tool_result(
            call_id,
            tool_name,
            output_json,
            is_error,
        ))
    }

    async fn invoke_write_stdin_tool(
        &self,
        call: crate::AgentKernelToolCall,
        cancellation: Option<&crate::ToolInvocationCancellation>,
    ) -> crate::VerletResult<crate::kernel::history::CanonicalMessage> {
        let call_id = call.call_id;
        let tool_name = call.tool_name;
        let args: WriteStdinToolArgs = serde_json::from_value(call.arguments).map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "tool {WRITE_STDIN_TOOL:?} has invalid arguments: {err}"
            ))
        })?;
        let process_id = args.process_id.parse::<VerletProcessId>().map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "tool {WRITE_STDIN_TOOL:?} requires a valid Verlet process_id: {err}"
            ))
        })?;
        self.require_process_handle(process_id).await?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(args.delta_base64)
            .map_err(|err| {
                crate::VerletError::RuntimeExecution(format!(
                    "tool {WRITE_STDIN_TOOL:?} requires valid base64 delta_base64: {err}"
                ))
            })?;
        let output_cap = process_output_cap(args.output_bytes_cap, self.config.max_output_bytes);
        let yield_time = process_yield_time(args.yield_time_ms);
        let execution = match cancellation {
            Some(cancellation) => {
                tokio::select! {
                    biased;
                    _ = cancellation.token().cancelled() => {
                        self.terminate_process_for_tool(
                            process_id,
                            cancellation.grace(),
                        ).await
                    }
                    outcome = self.process_manager.write(
                        process_id,
                        bytes,
                        yield_time,
                        SPILL_RETENTION_MAX_BYTES,
                    ) => outcome,
                }
            }
            None => {
                self.process_manager
                    .write(process_id, bytes, yield_time, SPILL_RETENTION_MAX_BYTES)
                    .await
            }
        };
        match execution {
            Ok(outcome) => {
                let is_error = process_snapshot_is_error(&outcome.snapshot);
                let output = self
                    .process_snapshot_json_with_spill(&outcome.snapshot, &call_id, output_cap)
                    .await?;
                let output_json = serde_json::to_string(&output).map_err(execution_error)?;
                Ok(crate::kernel::history::CanonicalMessage::tool_result(
                    call_id,
                    tool_name,
                    output_json,
                    is_error,
                ))
            }
            Err(err) => {
                let output_json = serde_json::to_string(&serde_json::json!({
                    "status": "unsupported",
                    "process_id": process_id.to_string(),
                    "error": err.to_string(),
                }))
                .map_err(execution_error)?;
                Ok(crate::kernel::history::CanonicalMessage::tool_result(
                    call_id,
                    tool_name,
                    output_json,
                    true,
                ))
            }
        }
    }
}

impl BashToolProvider {
    async fn terminate_process_for_tool(
        &self,
        process_id: VerletProcessId,
        grace: std::time::Duration,
    ) -> VerletProcessResult<verlet_process::AsyncProcessOutcome> {
        let mut outcome = self
            .process_manager
            .terminate(
                process_id,
                "tool invocation cancelled",
                grace,
                SPILL_RETENTION_MAX_BYTES,
            )
            .await?;
        while outcome.snapshot.status == ProcessSnapshotStatus::Running {
            outcome = self
                .process_manager
                .poll(
                    process_id,
                    std::time::Duration::from_secs(1),
                    SPILL_RETENTION_MAX_BYTES,
                )
                .await?;
        }
        Ok(outcome)
    }

    async fn require_process_handle(&self, process_id: VerletProcessId) -> crate::VerletResult<()> {
        self.process_dispatcher
            .as_ref()
            .ok_or_else(|| {
                crate::VerletError::RuntimeExecution(
                    "process handle verbs require the durable process dispatch ingress lane"
                        .to_string(),
                )
            })?
            .require_live_handle(process_id, None)
            .await
            .map(|_| ())
    }

    async fn process_snapshot_json_with_spill(
        &self,
        snapshot: &AsyncProcessSnapshot,
        call_id: &str,
        output_cap: usize,
    ) -> crate::VerletResult<serde_json::Value> {
        let mut harness = self.harness.lock().await;
        if harness.is_none() {
            *harness = Some(BashkitExecutionHarness::new(self.config.clone()).await?);
        }
        let harness = harness.as_ref().ok_or_else(|| {
            crate::VerletError::RuntimeExecution("bash harness did not initialize".to_string())
        })?;
        let (stdout, stdout_spill, stdout_spilled) = present_output_stream(
            harness,
            &snapshot.stdout,
            output_cap,
            snapshot.stdout_truncated,
            &spill_path(call_id, "stdout"),
        )
        .await;
        let (stderr, stderr_spill, stderr_spilled) = present_output_stream(
            harness,
            &snapshot.stderr,
            output_cap,
            snapshot.stderr_truncated,
            &spill_path(call_id, "stderr"),
        )
        .await;
        let mut output = process_snapshot_json(snapshot, stdout, stderr);
        output["truncated"] = serde_json::json!(
            snapshot.stdout_truncated
                || snapshot.stderr_truncated
                || stdout_spilled
                || stderr_spilled
        );
        output["stdout_truncated"] = serde_json::json!(snapshot.stdout_truncated || stdout_spilled);
        output["stderr_truncated"] = serde_json::json!(snapshot.stderr_truncated || stderr_spilled);
        insert_spill(
            &mut output,
            ToolOutputSpill {
                stdout: stdout_spill,
                stderr: stderr_spill,
            },
        )?;
        Ok(output)
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BashToolArgs {
    command: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessExecToolArgs {
    command: Option<String>,
    process_id: Option<String>,
    yield_time_ms: Option<u64>,
    timeout_ms: Option<u64>,
    output_bytes_cap: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteStdinToolArgs {
    process_id: String,
    delta_base64: String,
    yield_time_ms: Option<u64>,
    output_bytes_cap: Option<usize>,
}

fn process_tool_owner(
    turn_context: &Option<crate::TurnContextSnapshot>,
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

fn process_yield_time(yield_time_ms: Option<u64>) -> std::time::Duration {
    std::time::Duration::from_millis(yield_time_ms.unwrap_or(10_000).min(30_000))
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

fn process_snapshot_json(
    snapshot: &AsyncProcessSnapshot,
    stdout: String,
    stderr: String,
) -> serde_json::Value {
    let status: &str = snapshot.status.as_ref();
    serde_json::json!({
        "process_id": snapshot.process_id.map(|process_id| process_id.to_string()),
        "backend": snapshot.backend,
        "label": snapshot.label,
        "status": status,
        "exit_code": snapshot.exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "truncated": snapshot.stdout_truncated || snapshot.stderr_truncated,
        "stdout_truncated": snapshot.stdout_truncated,
        "stderr_truncated": snapshot.stderr_truncated,
    })
}

async fn present_output_stream(
    harness: &BashkitExecutionHarness,
    raw: &[u8],
    max_output_bytes: usize,
    retention_truncated: bool,
    path: &str,
) -> (String, Option<ToolOutputSpillReceipt>, bool) {
    match plan_output_overflow(raw, max_output_bytes, retention_truncated, path) {
        OverflowPlan::Inline(plan) => (plan.content, None, false),
        OverflowPlan::Spill(plan) => {
            let stored = harness
                .write_spill_file_if_available(&plan.path, plan.raw)
                .await
                .unwrap_or(false);
            if stored {
                let receipt = ToolOutputSpillReceipt {
                    path: plan.path.clone(),
                    total_bytes: plan.total_bytes,
                    preview_bytes: plan.preview_bytes,
                    retention_truncated: plan.retention_truncated,
                };
                (format_spill_stub(&plan), Some(receipt), true)
            } else {
                (
                    build_emergency_spill_stub(plan.raw, &plan.path, plan.retention_truncated),
                    None,
                    true,
                )
            }
        }
    }
}

fn insert_spill(output: &mut serde_json::Value, spill: ToolOutputSpill) -> crate::VerletResult<()> {
    if spill.is_empty() {
        return Ok(());
    }
    output["spill"] = serde_json::to_value(spill).map_err(execution_error)?;
    Ok(())
}

fn spill_path(call_id: &str, stream: &str) -> String {
    const ENCODED_PREFIX: &str = "_encoded_";
    const MAX_COMPONENT_BYTES: usize = 160;

    let requires_encoding = call_id.starts_with(ENCODED_PREFIX)
        || call_id
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_'));
    let mut safe_call_id = if requires_encoding {
        let mut encoded = String::with_capacity(call_id.len().saturating_mul(3));
        encoded.push_str(ENCODED_PREFIX);
        for byte in call_id.bytes() {
            if byte.is_ascii_alphanumeric() || byte == b'-' {
                encoded.push(char::from(byte));
            } else {
                encoded.push('_');
                push_hex_byte(&mut encoded, byte);
            }
        }
        encoded
    } else {
        call_id.to_string()
    };
    if safe_call_id.len() > MAX_COMPONENT_BYTES {
        let digest = sha2::Sha256::digest(call_id.as_bytes());
        let mut digest_hex = String::with_capacity(digest.len() * 2);
        for byte in digest {
            push_hex_byte(&mut digest_hex, byte);
        }
        safe_call_id = format!("{ENCODED_PREFIX}h_{}_{}", &safe_call_id[..80], digest_hex);
    }
    format!("/spill/{safe_call_id}.{stream}.txt")
}

fn push_hex_byte(output: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(char::from(HEX[usize::from(byte >> 4)]));
    output.push(char::from(HEX[usize::from(byte & 0x0f)]));
}

fn is_false(value: &bool) -> bool {
    !*value
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

#[async_trait::async_trait]
impl crate::AgentRuntimeFactory for VirtualBashRuntimeFactory {
    async fn build(
        &self,
        _context: &crate::ThreadContext,
    ) -> crate::VerletResult<Box<dyn crate::AgentRuntime>> {
        Ok(Box::new(VirtualBashRuntime {
            config: self.config.clone(),
        }))
    }
}

struct VirtualBashRuntime {
    config: VirtualBashRuntimeConfig,
}

#[async_trait::async_trait]
impl crate::AgentRuntime for VirtualBashRuntime {
    async fn run(
        self: Box<Self>,
        context: crate::ThreadContext,
        services: crate::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<crate::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<crate::ThreadEvent>,
        status: tokio::sync::watch::Sender<crate::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        let harness = match BashkitExecutionHarness::new(self.config).await {
            Ok(harness) => harness,
            Err(err) => {
                let _ = status.send(crate::ThreadStatus::Failed);
                let _ = events.send(crate::ThreadEvent::Failed {
                    thread_id,
                    message: err.to_string(),
                });
                return;
            }
        };
        let mut harness = Some(harness);

        crate::emit_runtime_event(
            &events,
            &coordinates,
            crate::RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
                metadata: context.metadata.clone(),
            },
        );
        let _ = events.send(crate::ThreadEvent::Started { context });
        let _ = status.send(crate::ThreadStatus::Idle);
        let mut pending_submits = std::collections::VecDeque::new();

        loop {
            if let Some(crate::ThreadCommand::Submit { turn_id, input, .. }) =
                pending_submits.pop_front()
            {
                let _ = status.send(crate::ThreadStatus::Running);
                match services
                    .append_user_turn_input(&coordinates, &turn_id, &input)
                    .await
                {
                    Ok(entry) => {
                        let _ =
                            events.send(crate::ThreadEvent::CanonicalMirror { thread_id, entry });
                    }
                    Err(err) => {
                        let _ = status.send(crate::ThreadStatus::Failed);
                        let _ = events.send(crate::ThreadEvent::Failed {
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
                        crate::ThreadCommand::Submit { turn_id, input, mode } => {
                            if mode == crate::TurnSubmissionMode::Steer {
                                crate::emit_runtime_event(
                                    &events,
                                    &coordinates,
                                    crate::RuntimeEventKind::PolicyRejected {
                                        code: "no_active_turn".to_string(),
                                        message: "steer input requires an active virtual bash turn".to_string(),
                                    },
                                );
                                continue;
                            }
                            let _ = status.send(crate::ThreadStatus::Running);
                            match services.append_user_turn_input(&coordinates, &turn_id, &input).await {
                                Ok(entry) => {
                                    let _ = events.send(crate::ThreadEvent::CanonicalMirror { thread_id, entry });
                                }
                                Err(err) => {
                                    let _ = status.send(crate::ThreadStatus::Failed);
                                    let _ = events.send(crate::ThreadEvent::Failed {
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
                        crate::ThreadCommand::Cancel { reason } => {
                            let _ = status.send(crate::ThreadStatus::Cancelling);
                            let _ = events.send(crate::ThreadEvent::Signal {
                                thread_id,
                                signal: crate::ThreadSignal::interrupt_cancel(&coordinates, reason.clone()),
                            });
                            let _ = events.send(crate::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(crate::ThreadStatus::Idle);
                        }
                        crate::ThreadCommand::CancelTurn { .. } => {}
                        crate::ThreadCommand::Compact { .. } => {
                            crate::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::RuntimeEventKind::PolicyRejected {
                                    code: "compact_unsupported".to_string(),
                                    message: "Virtual bash runtime does not support Verlet compaction commands".to_string(),
                                },
                            );
                            let _ = status.send(crate::ThreadStatus::Idle);
                        }
                        crate::ThreadCommand::ResumeToolCall { .. } => {
                            crate::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::RuntimeEventKind::PolicyRejected {
                                    code: "tool_resume_unsupported".to_string(),
                                    message: "Virtual bash runtime does not support provider tool-call resume".to_string(),
                                },
                            );
                            let _ = status.send(crate::ThreadStatus::Idle);
                        }
                        crate::ThreadCommand::Shutdown => {
                            let _ = events.send(crate::ThreadEvent::Signal {
                                thread_id,
                                signal: crate::ThreadSignal::shutdown(&coordinates),
                            });
                            crate::emit_runtime_event(
                                &events,
                                &coordinates,
                                crate::RuntimeEventKind::Terminal {
                                    state: crate::RuntimeTerminalState::Stopped,
                                },
                            );
                            break;
                        }
                    }
                }
            }
        }

        crate::emit_runtime_event(
            &events,
            &coordinates,
            crate::RuntimeEventKind::Terminal {
                state: crate::RuntimeTerminalState::Stopped,
            },
        );
        let _ = status.send(crate::ThreadStatus::Stopped);
        let _ = events.send(crate::ThreadEvent::Stopped { thread_id });
    }
}

async fn run_virtual_turn(
    harness: &mut Option<BashkitExecutionHarness>,
    services: &crate::RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    input: String,
    watchdog_token_id: Option<u64>,
    events: &tokio::sync::broadcast::Sender<crate::ThreadEvent>,
    status: &tokio::sync::watch::Sender<crate::ThreadStatus>,
    commands: &mut tokio::sync::mpsc::Receiver<crate::ThreadCommand>,
    cancellation: &tokio_util::sync::CancellationToken,
    pending_submits: &mut std::collections::VecDeque<crate::ThreadCommand>,
) -> bool {
    let Some(turn_harness) = harness.take() else {
        let _ = status.send(crate::ThreadStatus::Failed);
        let _ = events.send(crate::ThreadEvent::Failed {
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
            let _ = status.send(crate::ThreadStatus::Failed);
            let _ = events.send(crate::ThreadEvent::Failed {
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
                cancel_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                cancelled_reason = Some("runtime cancellation requested".to_string());
                accept_control_commands = false;
            }
            // Keep control traffic ahead of completed execution so cancel/interrupt
            // wins races with a finishing virtual-bash turn.
            command = commands.recv(), if accept_control_commands => {
                match command {
                    Some(crate::ThreadCommand::Cancel { reason }) => {
                        let _ = status.send(crate::ThreadStatus::Cancelling);
                        let _ = events.send(crate::ThreadEvent::Signal {
                            thread_id,
                            signal: crate::ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        cancel_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        cancelled_reason = Some(reason);
                        accept_control_commands = false;
                    }
                    Some(crate::ThreadCommand::CancelTurn {
                        watchdog_token_id: target_token_id,
                        reason,
                    }) => {
                        if watchdog_token_id != Some(target_token_id) {
                            continue;
                        }
                        let _ = status.send(crate::ThreadStatus::Cancelling);
                        let _ = events.send(crate::ThreadEvent::Signal {
                            thread_id,
                            signal: crate::ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
                        });
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        cancel_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        cancelled_reason = Some(reason);
                        accept_control_commands = false;
                    }
                    Some(crate::ThreadCommand::Shutdown) => {
                        let _ = events.send(crate::ThreadEvent::Signal {
                            thread_id,
                            signal: crate::ThreadSignal::shutdown(coordinates),
                        });
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::Terminal {
                                state: crate::RuntimeTerminalState::Stopped,
                            },
                        );
                        cancel_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        shutdown_after_turn = true;
                        accept_control_commands = false;
                    }
                    Some(crate::ThreadCommand::Submit { turn_id, input, mode }) => {
                        match mode {
                            crate::TurnSubmissionMode::Queue => {
                                let _ = events.send(crate::ThreadEvent::Signal {
                                    thread_id,
                                    signal: crate::ThreadSignal::user_queue(coordinates, turn_id.clone()),
                                });
                                pending_submits.push_back(crate::ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode,
                                });
                            }
                            crate::TurnSubmissionMode::Steer => {
                                crate::emit_runtime_event(
                                    events,
                                    coordinates,
                                    crate::RuntimeEventKind::PolicyRejected {
                                        code: "active_turn_not_steerable".to_string(),
                                        message: "Virtual bash runtime does not support same-turn steering".to_string(),
                                    },
                                );
                            }
                            crate::TurnSubmissionMode::Interrupt => {
                                let reason = format!("interrupted by turn {turn_id}");
                                let _ = status.send(crate::ThreadStatus::Cancelling);
                                let _ = events.send(crate::ThreadEvent::Signal {
                                    thread_id,
                                    signal: crate::ThreadSignal::user_interrupt(coordinates, turn_id.clone()),
                                });
                                crate::emit_runtime_event(
                                    events,
                                    coordinates,
                                    crate::RuntimeEventKind::Cancelled {
                                        reason: reason.clone(),
                                    },
                                );
                                cancel_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                                cancelled_reason = Some(reason);
                                accept_control_commands = false;
                                pending_submits.push_front(crate::ThreadCommand::Submit {
                                    turn_id,
                                    input,
                                    mode: crate::TurnSubmissionMode::Queue,
                                });
                            }
                        }
                    }
                    Some(crate::ThreadCommand::Compact { .. }) => {
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::PolicyRejected {
                                code: "compact_unsupported".to_string(),
                                message: "Virtual bash runtime does not support Verlet compaction commands".to_string(),
                            },
                        );
                    }
                    Some(crate::ThreadCommand::ResumeToolCall { .. }) => {
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::PolicyRejected {
                                code: "tool_resume_unsupported".to_string(),
                                message: "Virtual bash runtime does not support provider tool-call resume".to_string(),
                            },
                        );
                    }
                    None => {
                        let _ = events.send(crate::ThreadEvent::Signal {
                            thread_id,
                            signal: crate::ThreadSignal::shutdown(coordinates),
                        });
                        crate::emit_runtime_event(
                            events,
                            coordinates,
                            crate::RuntimeEventKind::Terminal {
                                state: crate::RuntimeTerminalState::Stopped,
                            },
                        );
                        cancel_flag.store(true, std::sync::atomic::Ordering::SeqCst);
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
                        break Err(crate::VerletError::RuntimeExecution(format!(
                            "virtual bash execution thread stopped before returning a result: {err}"
                        )));
                    }
                }
            }
        }
    };

    let was_cancelled = cancelled_reason.is_some();
    if let Some(reason) = cancelled_reason {
        let _ = status.send(crate::ThreadStatus::Idle);
        crate::emit_runtime_event(
            events,
            coordinates,
            crate::RuntimeEventKind::Terminal {
                state: crate::RuntimeTerminalState::Cancelled,
            },
        );
        let _ = events.send(crate::ThreadEvent::Cancelled { thread_id, reason });
    } else {
        match result {
            Ok(output) => {
                let text = output.event_text();
                if !text.is_empty() {
                    crate::emit_runtime_event(
                        events,
                        coordinates,
                        crate::RuntimeEventKind::TextDelta { text: text.clone() },
                    );
                    let _ = events.send(crate::ThreadEvent::Output {
                        thread_id,
                        text: text.clone(),
                    });
                    mirror_virtual_output(services, coordinates, thread_id, text, events).await;
                }
            }
            Err(err) => {
                let _ = status.send(crate::ThreadStatus::Failed);
                let _ = events.send(crate::ThreadEvent::Signal {
                    thread_id,
                    signal: crate::ThreadSignal::failed(coordinates, err.to_string()),
                });
                crate::emit_runtime_event(
                    events,
                    coordinates,
                    crate::RuntimeEventKind::Failed {
                        code: "runtime_execution".to_string(),
                        message: err.to_string(),
                    },
                );
                let _ = events.send(crate::ThreadEvent::Failed {
                    thread_id,
                    message: err.to_string(),
                });
                failed = true;
            }
        }
    }

    if !shutdown_after_turn && !was_cancelled && !failed {
        let _ = status.send(crate::ThreadStatus::Idle);
    }
    shutdown_after_turn || failed
}

struct VirtualBashExecutionResult {
    harness: BashkitExecutionHarness,
    result: crate::VerletResult<VirtualCommandOutput>,
}

fn spawn_virtual_bash_execution(
    mut harness: BashkitExecutionHarness,
    input: String,
) -> crate::VerletResult<tokio::sync::oneshot::Receiver<VirtualBashExecutionResult>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("verlet-vbash".to_string())
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
    services: &crate::RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    text: String,
    events: &tokio::sync::broadcast::Sender<crate::ThreadEvent>,
) {
    if let Ok(entry) = services
        .append_session_entry(
            coordinates,
            None,
            crate::SessionEntryKind::Message {
                message: crate::kernel::history::CanonicalMessage::assistant(
                    "verlet",
                    crate::kernel::history::ProviderApi::Other("virtual_bash".to_string()),
                    "bashkit",
                    vec![crate::kernel::history::CanonicalContent::text(text)],
                    crate::kernel::history::CanonicalStopReason::EndTurn,
                ),
            },
        )
        .await
    {
        let _ = events.send(crate::ThreadEvent::CanonicalMirror { thread_id, entry });
    }
}

fn execution_error(err: impl std::fmt::Display) -> crate::VerletError {
    crate::VerletError::RuntimeExecution(err.to_string())
}

#[cfg(test)]
mod tests;
