use crate::{
    BashExecutionPolicy, CommandRoute, CooldisVirtualBashError, CooldisVirtualBashResult,
    VirtualFile, VirtualMount, VirtualMountBackend, VirtualMountMode, absolute_mount_path,
    apply_external_file_writes, apply_patch_to_bashkit, cooldis_usage, default_virtual_mounts,
    deny_output, enforce_output_limit, exec_result_from_virtual_output,
    missing_operation_capability_grants, operation_shell_command_name, operation_shell_input,
    operation_shell_manual, operation_shell_reserved_commands, validate_mounts,
    virtual_bash_execution_error, virtual_command_output_from_exec_result,
};
use async_trait::async_trait;
use bashkit::{
    Bash, Builtin, BuiltinContext, BuiltinRegistry, ExecResult, ExecutionExtensions,
    ExecutionLimits, FileSystem, InMemoryFs,
};
use cooldis_operations::{OperationProjection, RegisteredOperation};
use cooldis_process::{
    CooldisProcessBackend, CooldisProcessError, CooldisProcessEventKind, CooldisProcessExitStatus,
    CooldisProcessHandle, ExecutionDeadline, ExternalCommandExecutor, ExternalCommandInvocation,
    ExternalCommandRequest, LiveProcessBackend, LiveProcessInvocation, LiveProcessSpawn,
    LiveProcessStartRequest, VirtualCommandOutput,
};
use cooldis_vfs::{
    CooldisVfs, CooldisVfsBackend, ManagedObjectStoreFs, ObjectStoreMountConfig,
    ReadOnlyFileSystem, VfsMutation,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait VbashOperationRegistry: Send + Sync + 'static {
    async fn describe(&self, name: &str) -> Option<RegisteredOperation>;
    async fn list(&self) -> Vec<RegisteredOperation>;
    async fn invoke_process_output(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: Vec<u8>,
    ) -> Result<VirtualCommandOutput, String>;
}

#[derive(Clone)]
pub struct BashkitExecutionConfig {
    pub cwd: PathBuf,
    pub execution_timeout: Duration,
    pub parser_timeout: Duration,
    pub max_commands: usize,
    pub max_loop_iterations: usize,
    pub max_output_bytes: usize,
    pub mounts: Vec<VirtualMount>,
    pub operation_registry: Option<Arc<dyn VbashOperationRegistry>>,
    pub workspace_vfs: Option<Arc<CooldisVfs>>,
    pub capability_grants: BTreeSet<String>,
    pub execution_policy: BashExecutionPolicy,
    pub external_executor: Option<Arc<dyn ExternalCommandExecutor>>,
}

impl std::fmt::Debug for BashkitExecutionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BashkitExecutionConfig")
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
                    .map(|_| "<VbashOperationRegistry>"),
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

impl Default for BashkitExecutionConfig {
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

impl BashkitExecutionConfig {
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

    pub fn with_operation_registry(mut self, registry: Arc<dyn VbashOperationRegistry>) -> Self {
        self.operation_registry = Some(registry);
        self
    }

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

    fn limits(&self) -> ExecutionLimits {
        ExecutionLimits::new()
            .timeout(self.execution_timeout)
            .parser_timeout(self.parser_timeout)
            .max_commands(self.max_commands)
            .max_loop_iterations(self.max_loop_iterations)
            .max_stdout_bytes(self.max_output_bytes)
            .max_stderr_bytes(self.max_output_bytes)
    }
}

pub struct BashkitExecutionHarness {
    shell: Bash,
    vfs: Arc<CooldisVfs>,
    cancellation: Arc<AtomicBool>,
    operation_shell_commands: Option<OperationShellCommandRegistry>,
    cwd: PathBuf,
    execution_timeout: Duration,
    execution_policy: BashExecutionPolicy,
    external_executor: Option<Arc<dyn ExternalCommandExecutor>>,
    max_output_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct BashkitLiveBackend {
    config: BashkitExecutionConfig,
}

impl BashkitLiveBackend {
    pub fn new(config: impl Into<BashkitExecutionConfig>) -> Self {
        Self {
            config: config.into(),
        }
    }
}

#[async_trait]
impl LiveProcessBackend for BashkitLiveBackend {
    fn backend_kind(&self) -> CooldisProcessBackend {
        CooldisProcessBackend::VirtualBash
    }

    async fn start(
        &self,
        request: LiveProcessStartRequest,
        process: CooldisProcessHandle,
        cancellation: CancellationToken,
    ) -> cooldis_process::CooldisProcessResult<LiveProcessSpawn> {
        let LiveProcessInvocation::VirtualBashScript { script } = request.invocation else {
            return Err(CooldisProcessError::Execution(
                "bashkit live backend requires a virtual bash script invocation".to_string(),
            ));
        };
        let mut config = self.config.clone();
        config.execution_timeout = request.deadline.timeout;
        config.max_output_bytes = request.output_cap_bytes;
        process.record(CooldisProcessEventKind::Started {
            command: Some(script.clone()),
        });
        let join = tokio::spawn(async move {
            let mut harness = BashkitExecutionHarness::new(config)
                .await
                .map_err(|err| CooldisProcessError::Execution(err.to_string()))?;
            let cancellation_flag = harness.cancellation_flag();
            let cancellation_task = tokio::spawn(async move {
                cancellation.cancelled().await;
                cancellation_flag.store(true, Ordering::SeqCst);
            });
            let result: CooldisVirtualBashResult<CooldisProcessHandle> =
                harness.execute_process(&script).await;
            cancellation_task.abort();
            match result {
                Ok(handle) => {
                    let output = handle.output();
                    let exit_code = output.exit_code().unwrap_or(1);
                    if !output.stdout.is_empty() {
                        process.record(CooldisProcessEventKind::Stdout {
                            bytes: output.stdout,
                        });
                    }
                    if !output.stderr.is_empty() {
                        process.record(CooldisProcessEventKind::Stderr {
                            bytes: output.stderr,
                        });
                    }
                    if output.stdout_truncated || output.stderr_truncated {
                        process.record(CooldisProcessEventKind::OutputTruncated {
                            stdout: output.stdout_truncated,
                            stderr: output.stderr_truncated,
                        });
                    }
                    process.record(match exit_code {
                        124 => CooldisProcessEventKind::TimedOut {
                            timeout_ms: Some(request.deadline.timeout_ms()),
                            message: "virtual bash execution timed out".to_string(),
                        },
                        code => CooldisProcessEventKind::Completed {
                            status: CooldisProcessExitStatus::exited(code),
                        },
                    });
                }
                Err(err) => {
                    process.record(CooldisProcessEventKind::Failed {
                        code: "virtual_bash_failed".to_string(),
                        message: err.to_string(),
                    });
                }
            }
            Ok(())
        });
        Ok(LiveProcessSpawn { stdin: None, join })
    }
}

impl BashkitExecutionHarness {
    pub async fn new(config: impl Into<BashkitExecutionConfig>) -> CooldisVirtualBashResult<Self> {
        let config = config.into();
        validate_mounts(&config.mounts)?;
        let uses_shared_workspace_vfs = config.workspace_vfs.is_some();
        let vfs = config.workspace_vfs.clone().unwrap_or_else(|| {
            let root: Arc<dyn CooldisVfsBackend> = Arc::new(InMemoryFs::new());
            Arc::new(CooldisVfs::new(root))
        });

        let limits = config.limits();
        let shell_fs: Arc<dyn FileSystem> = vfs.clone();
        let cwd = config.cwd.clone();
        let operation_registry = config.operation_registry.clone();
        let capability_grants = config.capability_grants.clone();
        let execution_timeout = config.execution_timeout;
        let execution_policy = config.execution_policy.clone();
        let external_executor = config.external_executor.clone();
        let max_output_bytes = config.max_output_bytes;
        let reserved_shell_commands = operation_shell_reserved_commands(&execution_policy);
        let mut operation_shell_commands = operation_registry.as_ref().map(|registry| {
            OperationShellCommandRegistry::new(
                Arc::clone(registry),
                capability_grants.clone(),
                reserved_shell_commands,
            )
        });
        if let Some(shell_commands) = operation_shell_commands.as_mut() {
            shell_commands.sync().await;
        }
        let mut builder = Bash::builder()
            .fs(shell_fs)
            .cwd(config.cwd)
            .limits(limits)
            .builtin("apply_patch", Box::new(ApplyPatchBuiltin))
            .builtin(
                "cooldis",
                Box::new(CooldisBuiltin {
                    registry: operation_registry.clone(),
                    capability_grants: capability_grants.clone(),
                }),
            )
            .builtin(
                "man",
                Box::new(ManBuiltin {
                    registry: operation_registry.clone(),
                }),
            );
        if let Some(shell_commands) = &operation_shell_commands {
            builder = builder.builtin_registry(shell_commands.builtin_registry());
        }
        for (command, route) in &execution_policy.routing.named_proxy_routes {
            if *route == CommandRoute::VirtualBash {
                continue;
            }
            builder = builder.builtin(
                command.clone(),
                Box::new(ExternalCommandProxyBuiltin {
                    command: command.clone(),
                    route: *route,
                    executor: external_executor.clone(),
                    max_output_bytes,
                }),
            );
        }
        let shell = builder.build();

        for mount in config.mounts {
            if uses_shared_workspace_vfs && vfs.has_mount(&mount.path) {
                continue;
            }
            let fs: Arc<dyn CooldisVfsBackend> = match mount.backend {
                VirtualMountBackend::Memory => Arc::new(InMemoryFs::new()),
                VirtualMountBackend::ObjectStore(config) => {
                    Arc::new(ManagedObjectStoreFs::new(config).map_err(execution_error)?)
                }
            };
            for file in mount.files {
                let path = absolute_mount_path(file.path);
                let mode = match mount.mode {
                    VirtualMountMode::ReadWrite => 0o644,
                    VirtualMountMode::ReadOnly => 0o444,
                };
                fs.write_file(&path, &file.content)
                    .await
                    .map_err(execution_error)?;
                fs.chmod(&path, mode).await.map_err(execution_error)?;
            }

            let fs: Arc<dyn CooldisVfsBackend> = match mount.mode {
                VirtualMountMode::ReadWrite => fs,
                VirtualMountMode::ReadOnly => Arc::new(ReadOnlyFileSystem::new(fs)),
            };
            vfs.mount(mount.path, fs).map_err(execution_error)?;
        }

        let cancellation = shell.cancellation_token();
        Ok(Self {
            shell,
            vfs,
            cancellation,
            operation_shell_commands,
            cwd,
            execution_timeout,
            execution_policy,
            external_executor,
            max_output_bytes,
        })
    }

    pub async fn execute(
        &mut self,
        script: &str,
    ) -> CooldisVirtualBashResult<VirtualCommandOutput> {
        let process = self.execute_process(script).await?;
        Ok(VirtualCommandOutput::from(&process.output()))
    }

    pub async fn execute_process(
        &mut self,
        script: &str,
    ) -> CooldisVirtualBashResult<CooldisProcessHandle> {
        self.cancellation.store(false, Ordering::SeqCst);
        let deadline = ExecutionDeadline::from_now(self.execution_timeout);
        match self.execution_policy.routing.default_route {
            CommandRoute::VirtualBash => {
                if let Some(shell_commands) = self.operation_shell_commands.as_mut() {
                    shell_commands.sync().await;
                }
                let output = self
                    .shell
                    .exec_with_extensions(script, ExecutionExtensions::new().with(deadline.clone()))
                    .await
                    .map(virtual_command_output_from_exec_result)
                    .map_err(execution_error)?;
                self.vfs.flush().await.map_err(execution_error)?;
                Ok(CooldisProcessHandle::from_virtual_command(script, output))
            }
            CommandRoute::HostBash | CommandRoute::RemoteLinux => {
                let executor = self.external_executor.clone().ok_or_else(|| {
                    CooldisVirtualBashError::RuntimeExecution(
                        "external command executor is not configured".to_string(),
                    )
                })?;
                let request = ExternalCommandRequest {
                    invocation: ExternalCommandInvocation::Script(script.to_string()),
                    executor: self
                        .execution_policy
                        .routing
                        .default_route
                        .executor_kind()
                        .expect("external route should resolve to executor kind"),
                    cwd: self.cwd.clone(),
                    stdin: None,
                    deadline,
                    max_output_bytes: self.max_output_bytes,
                };
                let result = executor
                    .exec(request.clone())
                    .await
                    .map_err(execution_error)?;
                apply_external_file_writes(self.vfs.as_ref(), &result).await?;
                self.vfs.flush().await.map_err(execution_error)?;
                Ok(CooldisProcessHandle::from_external_command(
                    &request, result,
                ))
            }
            CommandRoute::Deny => {
                let output = deny_output(script);
                self.vfs.flush().await.map_err(execution_error)?;
                Ok(CooldisProcessHandle::from_virtual_command(script, output))
            }
        }
    }

    pub fn cancellation_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancellation)
    }

    pub fn cancel(&self) {
        self.cancellation.store(true, Ordering::SeqCst);
    }

    pub async fn read_file(&self, path: impl AsRef<Path>) -> CooldisVirtualBashResult<Vec<u8>> {
        self.vfs
            .read_file(path.as_ref())
            .await
            .map_err(execution_error)
    }

    pub fn mutations(&self) -> Vec<VfsMutation> {
        self.vfs.mutations()
    }

    pub fn clear_mutations(&self) {
        self.vfs.clear_mutations();
    }
}

struct ApplyPatchBuiltin;

#[async_trait]
impl Builtin for ApplyPatchBuiltin {
    async fn execute(&self, ctx: BuiltinContext<'_>) -> bashkit::Result<ExecResult> {
        let patch = if let Some(stdin) = ctx.stdin {
            stdin.to_string()
        } else {
            ctx.args.join("\n")
        };

        match apply_patch_to_bashkit(ctx.fs, ctx.cwd, &patch).await {
            Ok(summary) => Ok(ExecResult::ok(summary)),
            Err(message) => Ok(ExecResult::err(format!("apply_patch: {message}\n"), 1)),
        }
    }
}

struct OperationShellCommandRegistry {
    operation_registry: Arc<dyn VbashOperationRegistry>,
    builtin_registry: BuiltinRegistry,
    capability_grants: BTreeSet<String>,
    reserved_commands: BTreeSet<String>,
    active_commands: BTreeSet<String>,
}

impl OperationShellCommandRegistry {
    fn new(
        operation_registry: Arc<dyn VbashOperationRegistry>,
        capability_grants: BTreeSet<String>,
        reserved_commands: BTreeSet<String>,
    ) -> Self {
        Self {
            operation_registry,
            builtin_registry: BuiltinRegistry::new(),
            capability_grants,
            reserved_commands,
            active_commands: BTreeSet::new(),
        }
    }

    fn builtin_registry(&self) -> BuiltinRegistry {
        self.builtin_registry.clone()
    }

    async fn sync(&mut self) {
        let next_commands = operation_shell_command_names(
            self.operation_registry.as_ref(),
            &self.reserved_commands,
        )
        .await;
        for command in next_commands.difference(&self.active_commands) {
            self.builtin_registry.insert(
                command.clone(),
                Arc::new(OperationShellCommandBuiltin {
                    command: command.clone(),
                    registry: Arc::clone(&self.operation_registry),
                    capability_grants: self.capability_grants.clone(),
                }),
            );
        }
        for command in self.active_commands.difference(&next_commands) {
            self.builtin_registry.remove(command);
        }
        self.active_commands = next_commands;
    }
}

struct OperationShellCommandBuiltin {
    command: String,
    registry: Arc<dyn VbashOperationRegistry>,
    capability_grants: BTreeSet<String>,
}

#[async_trait]
impl Builtin for OperationShellCommandBuiltin {
    async fn execute(&self, ctx: BuiltinContext<'_>) -> bashkit::Result<ExecResult> {
        let projection =
            match operation_projection_for_shell_command(self.registry.as_ref(), &self.command)
                .await
            {
                Ok(projection) => projection,
                Err(err) => return Ok(ExecResult::err(format!("cooldis: {err}\n"), 127)),
            };
        let input = match operation_shell_input(&projection, ctx.args, ctx.stdin) {
            Ok(input) => input,
            Err(err) => return Ok(ExecResult::err(format!("cooldis: {err}\n"), 2)),
        };
        Ok(invoke_operation_projection(
            self.registry.as_ref(),
            &self.capability_grants,
            &projection,
            input,
        )
        .await)
    }
}

struct CooldisBuiltin {
    registry: Option<Arc<dyn VbashOperationRegistry>>,
    capability_grants: BTreeSet<String>,
}

#[async_trait]
impl Builtin for CooldisBuiltin {
    async fn execute(&self, ctx: BuiltinContext<'_>) -> bashkit::Result<ExecResult> {
        let Some(registry) = &self.registry else {
            return Ok(ExecResult::err(
                "cooldis: no operation registry is mounted in this virtual bash\n",
                127,
            ));
        };
        let Some(subcommand) = ctx.args.first().map(String::as_str) else {
            return Ok(ExecResult::err(cooldis_usage(), 2));
        };
        let (projection, input) = match subcommand {
            "run" => {
                if ctx.args.len() != 3 {
                    return Ok(ExecResult::err(cooldis_usage(), 2));
                }
                let registered_name = ctx.args[1].clone();
                let operation_name = ctx.args[2].clone();
                let stdin = ctx.stdin.unwrap_or_default().as_bytes().to_vec();
                let Some(record) = registry.describe(&registered_name).await else {
                    return Ok(ExecResult::err(
                        format!(
                            "cooldis: registered operation {registered_name:?} was not found\n"
                        ),
                        127,
                    ));
                };
                let Some(projection) = record
                    .projections()
                    .operations
                    .into_iter()
                    .find(|projection| projection.operation_name == operation_name)
                else {
                    return Ok(ExecResult::err(
                        format!(
                            "cooldis: operation {operation_name:?} is not registered under {registered_name:?}\n"
                        ),
                        127,
                    ));
                };
                (projection, stdin)
            }
            _ => return Ok(ExecResult::err(cooldis_usage(), 2)),
        };
        Ok(invoke_operation_projection(
            registry.as_ref(),
            &self.capability_grants,
            &projection,
            input,
        )
        .await)
    }
}

struct ManBuiltin {
    registry: Option<Arc<dyn VbashOperationRegistry>>,
}

#[async_trait]
impl Builtin for ManBuiltin {
    async fn execute(&self, ctx: BuiltinContext<'_>) -> bashkit::Result<ExecResult> {
        let Some(registry) = &self.registry else {
            return Ok(ExecResult::err(
                "man: no operation registry is mounted in this virtual bash\n",
                127,
            ));
        };
        if ctx.args.len() != 1 {
            return Ok(ExecResult::err(
                "usage: man <operation-command>\n".to_string(),
                2,
            ));
        }
        let command = &ctx.args[0];
        let projection =
            match operation_projection_for_shell_command(registry.as_ref(), command).await {
                Ok(projection) => projection,
                Err(err) => return Ok(ExecResult::err(format!("man: {err}\n"), 127)),
            };
        Ok(ExecResult::ok(operation_shell_manual(command, &projection)))
    }
}

struct ExternalCommandProxyBuiltin {
    command: String,
    route: CommandRoute,
    executor: Option<Arc<dyn ExternalCommandExecutor>>,
    max_output_bytes: usize,
}

#[async_trait]
impl Builtin for ExternalCommandProxyBuiltin {
    async fn execute(&self, ctx: BuiltinContext<'_>) -> bashkit::Result<ExecResult> {
        if self.route == CommandRoute::Deny {
            return Ok(ExecResult::err(
                format!(
                    "cooldis: command denied by routing policy: {}\n",
                    self.command
                ),
                126,
            ));
        }
        let Some(executor) = self.executor.clone() else {
            return Ok(ExecResult::err(
                "cooldis: external command executor is not configured\n",
                127,
            ));
        };
        let Some(executor_kind) = self.route.executor_kind() else {
            return Ok(ExecResult::err(
                format!(
                    "cooldis: command {:?} is not an external proxy route\n",
                    self.command
                ),
                127,
            ));
        };
        let deadline = ctx
            .execution_extension::<ExecutionDeadline>()
            .cloned()
            .unwrap_or_else(|| ExecutionDeadline::from_now(Duration::ZERO));
        let request = ExternalCommandRequest {
            invocation: ExternalCommandInvocation::Argv {
                command: self.command.clone(),
                args: ctx.args.to_vec(),
            },
            executor: executor_kind,
            cwd: ctx.cwd.clone(),
            stdin: ctx.stdin.map(ToString::to_string),
            deadline,
            max_output_bytes: self.max_output_bytes,
        };

        let result = match executor.exec(request).await {
            Ok(result) => result,
            Err(err) => return Ok(ExecResult::err(format!("cooldis: {err}\n"), 1)),
        };
        if let Err(err) = apply_external_file_writes(ctx.fs.as_ref(), &result).await {
            return Ok(ExecResult::err(format!("cooldis: {err}\n"), 1));
        }
        Ok(exec_result_from_virtual_output(enforce_output_limit(
            result.output,
            self.max_output_bytes,
        )))
    }
}

async fn operation_shell_command_counts(
    registry: &dyn VbashOperationRegistry,
) -> BTreeMap<String, usize> {
    let mut commands = BTreeMap::new();
    for record in registry.list().await {
        for projection in record.projections().operations {
            let command = operation_shell_command_name(&projection);
            if !command.is_empty() {
                *commands.entry(command).or_insert(0) += 1;
            }
        }
    }
    commands
}

pub async fn operation_shell_command_names(
    registry: &dyn VbashOperationRegistry,
    reserved_commands: &BTreeSet<String>,
) -> BTreeSet<String> {
    operation_shell_command_counts(registry)
        .await
        .into_iter()
        .filter_map(|(command, count)| {
            (count == 1 && !reserved_commands.contains(&command)).then_some(command)
        })
        .collect()
}

async fn operation_projection_for_shell_command(
    registry: &dyn VbashOperationRegistry,
    command: &str,
) -> CooldisVirtualBashResult<OperationProjection> {
    let mut matches = Vec::new();
    for record in registry.list().await {
        for projection in record.projections().operations {
            if operation_shell_command_name(&projection) == command {
                matches.push(projection);
            }
        }
    }
    match matches.len() {
        0 => Err(CooldisVirtualBashError::RuntimeExecution(format!(
            "operation command {command:?} was not found"
        ))),
        1 => Ok(matches.remove(0)),
        _ => Err(CooldisVirtualBashError::RuntimeExecution(format!(
            "operation command {command:?} is ambiguous across published operations"
        ))),
    }
}

async fn invoke_operation_projection(
    registry: &dyn VbashOperationRegistry,
    capability_grants: &BTreeSet<String>,
    projection: &OperationProjection,
    input: Vec<u8>,
) -> ExecResult {
    let registered_name = &projection.registered_name;
    let operation_name = &projection.operation_name;
    if projection.abi.has_hidden_durable_sink() {
        return ExecResult::err(
            format!(
                "cooldis: operation {registered_name:?}/{operation_name:?} has a hidden durable sink\n"
            ),
            126,
        );
    }
    let missing = missing_operation_capability_grants(projection, capability_grants);
    if !missing.is_empty() {
        return ExecResult::err(
            format!(
                "cooldis: operation {registered_name:?}/{operation_name:?} missing capability grants: {}\n",
                missing.join(", ")
            ),
            126,
        );
    }

    match registry
        .invoke_process_output(registered_name, operation_name, input)
        .await
    {
        Ok(output) => exec_result_from_virtual_output(output),
        Err(err) => ExecResult::err(format!("cooldis: {err}\n"), 1),
    }
}

fn execution_error(err: impl std::fmt::Display) -> CooldisVirtualBashError {
    virtual_bash_execution_error(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cooldis_abi::{
        WasmOperationDefinition, WasmOperationEventKind, WasmOperationManifest, WasmOperationMode,
        WasmOperationValueKind,
    };

    struct StaticOperationRegistry {
        record: RegisteredOperation,
    }

    #[async_trait]
    impl VbashOperationRegistry for StaticOperationRegistry {
        async fn describe(&self, name: &str) -> Option<RegisteredOperation> {
            (self.record.name == name).then(|| self.record.clone())
        }

        async fn list(&self) -> Vec<RegisteredOperation> {
            vec![self.record.clone()]
        }

        async fn invoke_process_output(
            &self,
            _registered_name: &str,
            _operation_name: &str,
            _input: Vec<u8>,
        ) -> Result<VirtualCommandOutput, String> {
            Err("manual test should not invoke operation".to_string())
        }
    }

    #[test]
    fn virtual_bash_man_projects_live_operation_command_contract() {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let registry = Arc::new(StaticOperationRegistry {
                    record: RegisteredOperation {
                        name: "contacts".to_string(),
                        manifest: WasmOperationManifest {
                            abi: "cooldis.operation/0.1".to_string(),
                            operations: vec![WasmOperationDefinition {
                                id: 1,
                                name: "lookup".to_string(),
                                input: WasmOperationValueKind::Json,
                                output: WasmOperationValueKind::Json,
                                events: WasmOperationEventKind::Jsonl,
                                mode: WasmOperationMode::Sync,
                                required_capabilities: vec![
                                    "net:https://api.example.test".to_string(),
                                ],
                            }],
                        },
                        capability_grants: ["net:https://api.example.test".to_string()]
                            .into_iter()
                            .collect(),
                        metadata: BTreeMap::new(),
                    },
                });
                let mut harness = BashkitExecutionHarness::new(
                    BashkitExecutionConfig::default().with_operation_registry(registry),
                )
                .await
                .unwrap();

                let output = harness.execute("man lookup").await.unwrap();

                assert_eq!(output.exit_code, 0);
                assert!(output.stderr.is_empty());
                assert!(
                    output
                        .stdout
                        .contains("NAME\n  lookup - lookup from contacts")
                );
                assert!(output.stdout.contains("USAGE\n  lookup [input]"));
                assert!(output.stdout.contains("  cooldis run contacts lookup"));
                assert!(output.stdout.contains("STDIN\n  json"));
                assert!(output.stdout.contains("STDOUT\n  json"));
                assert!(
                    output
                        .stdout
                        .contains("CAPABILITIES\n  net:https://api.example.test")
                );
                assert!(
                    output
                        .stdout
                        .contains("EXIT STATUS\n  0 operation succeeded")
                );
            });
    }
}
