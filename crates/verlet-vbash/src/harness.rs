use bashkit::FileSystem as _;

#[async_trait::async_trait]
pub trait VbashOperationRegistry: Send + Sync + 'static {
    async fn describe(&self, name: &str) -> Option<verlet_operations::RegisteredOperation>;
    async fn list(&self) -> Vec<verlet_operations::RegisteredOperation>;
    async fn invoke_process_output(
        &self,
        registered_name: &str,
        operation_name: &str,
        input: Vec<u8>,
    ) -> Result<verlet_process::execution::VirtualCommandOutput, String>;
}

#[derive(Clone)]
pub struct BashkitExecutionConfig {
    pub cwd: std::path::PathBuf,
    pub execution_timeout: std::time::Duration,
    pub parser_timeout: std::time::Duration,
    pub max_commands: usize,
    pub max_loop_iterations: usize,
    pub max_output_bytes: usize,
    pub mounts: Vec<crate::VirtualMount>,
    pub operation_registry: Option<std::sync::Arc<dyn VbashOperationRegistry>>,
    pub workspace_vfs: Option<std::sync::Arc<verlet_vfs::VerletVfs>>,
    pub capability_grants: std::collections::BTreeSet<String>,
    pub execution_policy: crate::BashExecutionPolicy,
    pub external_executor:
        Option<std::sync::Arc<dyn verlet_process::execution::ExternalCommandExecutor>>,
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
                &self.workspace_vfs.as_ref().map(|_| "<VerletVfs>"),
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
            cwd: std::path::PathBuf::from("/workspace"),
            execution_timeout: std::time::Duration::from_secs(10),
            parser_timeout: std::time::Duration::from_secs(2),
            max_commands: 10_000,
            max_loop_iterations: 10_000,
            max_output_bytes: 1_048_576,
            mounts: crate::default_virtual_mounts(),
            operation_registry: None,
            workspace_vfs: None,
            capability_grants: std::collections::BTreeSet::new(),
            execution_policy: crate::BashExecutionPolicy::virtual_only(),
            external_executor: None,
        }
    }
}

impl BashkitExecutionConfig {
    pub fn with_mount(mut self, mount: crate::VirtualMount) -> Self {
        self.mounts.push(mount);
        self
    }

    pub fn with_writable_mount(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.mounts.push(crate::VirtualMount::writable(path));
        self
    }

    pub fn with_readonly_mount(
        mut self,
        path: impl Into<std::path::PathBuf>,
        files: Vec<crate::VirtualFile>,
    ) -> Self {
        self.mounts.push(crate::VirtualMount::readonly(path, files));
        self
    }

    pub fn with_object_store_mount(
        mut self,
        path: impl Into<std::path::PathBuf>,
        config: verlet_vfs::ObjectStoreMountConfig,
    ) -> Self {
        self.mounts
            .push(crate::VirtualMount::object_store(path, config));
        self
    }

    pub fn with_readonly_object_store_mount(
        mut self,
        path: impl Into<std::path::PathBuf>,
        config: verlet_vfs::ObjectStoreMountConfig,
    ) -> Self {
        self.mounts
            .push(crate::VirtualMount::readonly_object_store(path, config));
        self
    }

    pub fn with_readonly_skill_file(
        mut self,
        path: impl Into<std::path::PathBuf>,
        content: impl Into<Vec<u8>>,
    ) -> Self {
        let file = crate::VirtualFile::new(path, content);
        if let Some(skills) = self.mounts.iter_mut().find(|mount| {
            mount.path == std::path::Path::new("/skills")
                && mount.mode == crate::VirtualMountMode::ReadOnly
        }) {
            skills.files.push(file);
        } else {
            self.mounts
                .push(crate::VirtualMount::readonly("/skills", vec![file]));
        }
        self
    }

    pub fn with_operation_registry(
        mut self,
        registry: std::sync::Arc<dyn VbashOperationRegistry>,
    ) -> Self {
        self.operation_registry = Some(registry);
        self
    }

    pub fn with_workspace_vfs(mut self, vfs: std::sync::Arc<verlet_vfs::VerletVfs>) -> Self {
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

    pub fn with_execution_policy(mut self, policy: crate::BashExecutionPolicy) -> Self {
        self.execution_policy = policy;
        self
    }

    pub fn with_external_executor(
        mut self,
        executor: std::sync::Arc<dyn verlet_process::execution::ExternalCommandExecutor>,
    ) -> Self {
        self.external_executor = Some(executor);
        self
    }

    fn limits(&self) -> bashkit::ExecutionLimits {
        bashkit::ExecutionLimits::new()
            .timeout(self.execution_timeout)
            .parser_timeout(self.parser_timeout)
            .max_commands(self.max_commands)
            .max_loop_iterations(self.max_loop_iterations)
            .max_stdout_bytes(crate::SPILL_RETENTION_MAX_BYTES)
            .max_stderr_bytes(crate::SPILL_RETENTION_MAX_BYTES)
    }
}

pub struct BashkitExecutionHarness {
    shell: bashkit::Bash,
    vfs: std::sync::Arc<verlet_vfs::VerletVfs>,
    cancellation: std::sync::Arc<std::sync::atomic::AtomicBool>,
    operation_shell_commands: Option<OperationShellCommandRegistry>,
    cwd: std::path::PathBuf,
    execution_timeout: std::time::Duration,
    execution_policy: crate::BashExecutionPolicy,
    external_executor:
        Option<std::sync::Arc<dyn verlet_process::execution::ExternalCommandExecutor>>,
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

#[async_trait::async_trait]
impl verlet_process::live::LiveProcessBackend for BashkitLiveBackend {
    fn backend_kind(&self) -> verlet_process::process::VerletProcessBackend {
        verlet_process::process::VerletProcessBackend::VirtualBash
    }

    async fn start(
        &self,
        request: verlet_process::live::LiveProcessStartRequest,
        process: verlet_process::process::VerletProcessHandle,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> verlet_process::VerletProcessResult<verlet_process::live::LiveProcessSpawn> {
        let verlet_process::live::LiveProcessInvocation::VirtualBashScript { script } =
            request.invocation
        else {
            return Err(verlet_process::VerletProcessError::Execution(
                "bashkit live backend requires a virtual bash script invocation".to_string(),
            ));
        };
        let mut config = self.config.clone();
        config.execution_timeout = request.deadline.timeout;
        config.max_output_bytes = request.output_cap_bytes;
        process.record(verlet_process::process::VerletProcessEventKind::Started {
            command: Some(script.clone()),
        });
        let join = tokio::spawn(async move {
            let mut harness = BashkitExecutionHarness::new(config)
                .await
                .map_err(|err| verlet_process::VerletProcessError::Execution(err.to_string()))?;
            let execution_cancellation = cancellation.clone();
            let result: crate::VerletVirtualBashResult<
                verlet_process::process::VerletProcessHandle,
            > = harness
                .execute_process_cancellable(&script, execution_cancellation)
                .await;
            let cancelled = cancellation.is_cancelled();
            match result {
                Ok(handle) => {
                    let output = handle.output();
                    let exit_code = output.exit_code().unwrap_or(1);
                    if !output.stdout.is_empty() {
                        process.record(verlet_process::process::VerletProcessEventKind::Stdout {
                            bytes: output.stdout,
                        });
                    }
                    if !output.stderr.is_empty() {
                        process.record(verlet_process::process::VerletProcessEventKind::Stderr {
                            bytes: output.stderr,
                        });
                    }
                    if output.stdout_truncated || output.stderr_truncated {
                        process.record(
                            verlet_process::process::VerletProcessEventKind::OutputTruncated {
                                stdout: output.stdout_truncated,
                                stderr: output.stderr_truncated,
                            },
                        );
                    }
                    process.record(match (cancelled, exit_code) {
                        (true, _) => verlet_process::process::VerletProcessEventKind::Cancelled {
                            reason: "virtual bash execution cancelled".to_string(),
                        },
                        (false, 124) => verlet_process::process::VerletProcessEventKind::TimedOut {
                            timeout_ms: Some(request.deadline.timeout_ms()),
                            message: "virtual bash execution timed out".to_string(),
                        },
                        (false, code) => {
                            verlet_process::process::VerletProcessEventKind::Completed {
                                status: verlet_process::process::VerletProcessExitStatus::exited(
                                    code,
                                ),
                            }
                        }
                    });
                }
                Err(err) => {
                    process.record(verlet_process::process::VerletProcessEventKind::Failed {
                        code: "virtual_bash_failed".to_string(),
                        message: err.to_string(),
                    });
                }
            }
            Ok(())
        });
        Ok(verlet_process::live::LiveProcessSpawn { stdin: None, join })
    }
}

impl BashkitExecutionHarness {
    pub async fn new(
        config: impl Into<BashkitExecutionConfig>,
    ) -> crate::VerletVirtualBashResult<Self> {
        let config = config.into();
        crate::validate_mounts(&config.mounts)?;
        let uses_shared_workspace_vfs = config.workspace_vfs.is_some();
        let vfs = config.workspace_vfs.clone().unwrap_or_else(|| {
            let limits = bashkit::FsLimits::default()
                .max_file_size(crate::SPILL_RETENTION_MAX_BYTES as u64)
                .max_total_bytes(crate::SPILL_VFS_MAX_BYTES as u64);
            let root: std::sync::Arc<dyn verlet_vfs::VerletVfsBackend> =
                std::sync::Arc::new(bashkit::InMemoryFs::with_limits(limits));
            std::sync::Arc::new(verlet_vfs::VerletVfs::new(root))
        });

        let limits = config.limits();
        let shell_fs: std::sync::Arc<dyn bashkit::FileSystem> = vfs.clone();
        let cwd = config.cwd.clone();
        let operation_registry = config.operation_registry.clone();
        let capability_grants = config.capability_grants.clone();
        let execution_timeout = config.execution_timeout;
        let execution_policy = config.execution_policy.clone();
        let external_executor = config.external_executor.clone();
        let max_output_bytes = config.max_output_bytes;
        let reserved_shell_commands = crate::operation_shell_reserved_commands(&execution_policy);
        let mut operation_shell_commands = operation_registry.as_ref().map(|registry| {
            OperationShellCommandRegistry::new(
                std::sync::Arc::clone(registry),
                capability_grants.clone(),
                reserved_shell_commands,
            )
        });
        if let Some(shell_commands) = operation_shell_commands.as_mut() {
            shell_commands.sync().await;
        }
        let mut builder = bashkit::Bash::builder()
            .fs(shell_fs)
            .cwd(config.cwd)
            .limits(limits)
            .builtin("apply_patch", Box::new(ApplyPatchBuiltin))
            .builtin(
                "verlet",
                Box::new(VerletBuiltin {
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
            if *route == crate::CommandRoute::VirtualBash {
                continue;
            }
            builder = builder.builtin(
                command.clone(),
                Box::new(ExternalCommandProxyBuiltin {
                    command: command.clone(),
                    route: *route,
                    executor: external_executor.clone(),
                }),
            );
        }
        let shell = builder.build();

        for mount in config.mounts {
            if uses_shared_workspace_vfs && vfs.has_mount(&mount.path) {
                continue;
            }
            let fs: std::sync::Arc<dyn verlet_vfs::VerletVfsBackend> = match mount.backend {
                crate::VirtualMountBackend::Memory => {
                    std::sync::Arc::new(bashkit::InMemoryFs::new())
                }
                crate::VirtualMountBackend::ObjectStore(config) => std::sync::Arc::new(
                    verlet_vfs::ManagedObjectStoreFs::new(config).map_err(execution_error)?,
                ),
            };
            for file in mount.files {
                let path = crate::absolute_mount_path(file.path);
                let mode = match mount.mode {
                    crate::VirtualMountMode::ReadWrite => 0o644,
                    crate::VirtualMountMode::ReadOnly => 0o444,
                };
                fs.write_file(&path, &file.content)
                    .await
                    .map_err(execution_error)?;
                fs.chmod(&path, mode).await.map_err(execution_error)?;
            }

            let fs: std::sync::Arc<dyn verlet_vfs::VerletVfsBackend> = match mount.mode {
                crate::VirtualMountMode::ReadWrite => fs,
                crate::VirtualMountMode::ReadOnly => {
                    std::sync::Arc::new(verlet_vfs::ReadOnlyFileSystem::new(fs))
                }
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
    ) -> crate::VerletVirtualBashResult<verlet_process::execution::VirtualCommandOutput> {
        Ok(crate::enforce_output_limit(
            self.execute_full_output(script).await?,
            self.max_output_bytes,
        ))
    }

    pub async fn execute_full_output(
        &mut self,
        script: &str,
    ) -> crate::VerletVirtualBashResult<verlet_process::execution::VirtualCommandOutput> {
        let process = self.execute_process(script).await?;
        Ok(verlet_process::execution::VirtualCommandOutput::from(
            &process.output(),
        ))
    }

    /// Executes one script while exposing the caller's cancellation token to
    /// process-backed routes. The in-interpreter path continues to observe
    /// bashkit's existing atomic cancellation flag.
    pub async fn execute_full_output_cancellable(
        &mut self,
        script: &str,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> crate::VerletVirtualBashResult<verlet_process::execution::VirtualCommandOutput> {
        let process = self
            .execute_process_cancellable(script, cancellation)
            .await?;
        Ok(verlet_process::execution::VirtualCommandOutput::from(
            &process.output(),
        ))
    }

    pub async fn execute_process(
        &mut self,
        script: &str,
    ) -> crate::VerletVirtualBashResult<verlet_process::process::VerletProcessHandle> {
        self.execute_process_inner(script, None).await
    }

    pub async fn execute_process_cancellable(
        &mut self,
        script: &str,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> crate::VerletVirtualBashResult<verlet_process::process::VerletProcessHandle> {
        let cancellation_flag = self.cancellation_flag();
        let cancellation_wait = cancellation.cancelled();
        let execution = self.execute_process_inner(script, Some(cancellation.clone()));
        tokio::pin!(cancellation_wait);
        tokio::pin!(execution);
        tokio::select! {
            result = &mut execution => result,
            _ = &mut cancellation_wait => {
                cancellation_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                execution.await
            }
        }
    }

    async fn execute_process_inner(
        &mut self,
        script: &str,
        cancellation: Option<tokio_util::sync::CancellationToken>,
    ) -> crate::VerletVirtualBashResult<verlet_process::process::VerletProcessHandle> {
        self.cancellation
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let deadline =
            verlet_process::execution::ExecutionDeadline::from_now(self.execution_timeout);
        match self.execution_policy.routing.default_route {
            crate::CommandRoute::VirtualBash => {
                if let Some(shell_commands) = self.operation_shell_commands.as_mut() {
                    shell_commands.sync().await;
                }
                let mut extensions = bashkit::ExecutionExtensions::new().with(deadline.clone());
                if let Some(cancellation) = cancellation.clone() {
                    extensions = extensions.with(cancellation);
                }
                let output = self
                    .shell
                    .exec_with_extensions(script, extensions)
                    .await
                    .map(crate::virtual_command_output_from_exec_result)
                    .map_err(execution_error)?;
                self.vfs.flush().await.map_err(execution_error)?;
                Ok(
                    verlet_process::process::VerletProcessHandle::from_virtual_command(
                        script, output,
                    ),
                )
            }
            crate::CommandRoute::HostBash | crate::CommandRoute::RemoteLinux => {
                let executor = self.external_executor.clone().ok_or_else(|| {
                    crate::VerletVirtualBashError::RuntimeExecution(
                        "external command executor is not configured".to_string(),
                    )
                })?;
                let request = verlet_process::execution::ExternalCommandRequest {
                    invocation: verlet_process::execution::ExternalCommandInvocation::Script(
                        script.to_string(),
                    ),
                    executor: self
                        .execution_policy
                        .routing
                        .default_route
                        .executor_kind()
                        .expect("external route should resolve to executor kind"),
                    cwd: self.cwd.clone(),
                    stdin: None,
                    deadline,
                    max_output_bytes: crate::SPILL_RETENTION_MAX_BYTES,
                };
                let mut result = match cancellation {
                    Some(cancellation) => {
                        executor
                            .exec_cancellable(request.clone(), cancellation)
                            .await
                    }
                    None => executor.exec(request.clone()).await,
                }
                .map_err(execution_error)?;
                result.output =
                    crate::enforce_output_limit(result.output, crate::SPILL_RETENTION_MAX_BYTES);
                crate::apply_external_file_writes(self.vfs.as_ref(), &result).await?;
                self.vfs.flush().await.map_err(execution_error)?;
                Ok(
                    verlet_process::process::VerletProcessHandle::from_external_command(
                        &request, result,
                    ),
                )
            }
            crate::CommandRoute::Deny => {
                let output = crate::deny_output(script);
                self.vfs.flush().await.map_err(execution_error)?;
                Ok(
                    verlet_process::process::VerletProcessHandle::from_virtual_command(
                        script, output,
                    ),
                )
            }
        }
    }

    pub fn cancellation_flag(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        std::sync::Arc::clone(&self.cancellation)
    }

    pub fn cancel(&self) {
        self.cancellation
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub async fn read_file(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> crate::VerletVirtualBashResult<Vec<u8>> {
        self.vfs
            .read_file(path.as_ref())
            .await
            .map_err(execution_error)
    }

    pub async fn write_spill_file_if_available(
        &self,
        path: impl AsRef<std::path::Path>,
        content: &[u8],
    ) -> crate::VerletVirtualBashResult<bool> {
        let path = path.as_ref();
        self.vfs
            .mkdir(std::path::Path::new("/spill"), true)
            .await
            .map_err(execution_error)?;
        if self.vfs.exists(path).await.map_err(execution_error)? {
            let existing = self.vfs.read_file(path).await.map_err(execution_error)?;
            self.vfs.flush().await.map_err(execution_error)?;
            return Ok(existing == content);
        }
        self.vfs
            .write_file(path, content)
            .await
            .map_err(execution_error)?;
        self.vfs.flush().await.map_err(execution_error)?;
        Ok(true)
    }

    pub fn mutations(&self) -> Vec<verlet_vfs::VfsMutation> {
        self.vfs.mutations()
    }

    pub fn clear_mutations(&self) {
        self.vfs.clear_mutations();
    }
}

struct ApplyPatchBuiltin;

#[async_trait::async_trait]
impl bashkit::Builtin for ApplyPatchBuiltin {
    async fn execute(
        &self,
        ctx: bashkit::BuiltinContext<'_>,
    ) -> bashkit::Result<bashkit::ExecResult> {
        let patch = if let Some(stdin) = ctx.stdin {
            stdin.to_string()
        } else {
            ctx.args.join("\n")
        };

        match crate::apply_patch::apply_patch_to_bashkit(ctx.fs, ctx.cwd, &patch).await {
            Ok(summary) => Ok(bashkit::ExecResult::ok(summary)),
            Err(message) => Ok(bashkit::ExecResult::err(
                format!("apply_patch: {message}\n"),
                1,
            )),
        }
    }
}

struct OperationShellCommandRegistry {
    operation_registry: std::sync::Arc<dyn VbashOperationRegistry>,
    builtin_registry: bashkit::BuiltinRegistry,
    capability_grants: std::collections::BTreeSet<String>,
    reserved_commands: std::collections::BTreeSet<String>,
    active_commands: std::collections::BTreeSet<String>,
}

impl OperationShellCommandRegistry {
    fn new(
        operation_registry: std::sync::Arc<dyn VbashOperationRegistry>,
        capability_grants: std::collections::BTreeSet<String>,
        reserved_commands: std::collections::BTreeSet<String>,
    ) -> Self {
        Self {
            operation_registry,
            builtin_registry: bashkit::BuiltinRegistry::new(),
            capability_grants,
            reserved_commands,
            active_commands: std::collections::BTreeSet::new(),
        }
    }

    fn builtin_registry(&self) -> bashkit::BuiltinRegistry {
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
                std::sync::Arc::new(OperationShellCommandBuiltin {
                    command: command.clone(),
                    registry: std::sync::Arc::clone(&self.operation_registry),
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
    registry: std::sync::Arc<dyn VbashOperationRegistry>,
    capability_grants: std::collections::BTreeSet<String>,
}

#[async_trait::async_trait]
impl bashkit::Builtin for OperationShellCommandBuiltin {
    async fn execute(
        &self,
        ctx: bashkit::BuiltinContext<'_>,
    ) -> bashkit::Result<bashkit::ExecResult> {
        let projection =
            match operation_projection_for_shell_command(self.registry.as_ref(), &self.command)
                .await
            {
                Ok(projection) => projection,
                Err(err) => return Ok(bashkit::ExecResult::err(format!("verlet: {err}\n"), 127)),
            };
        let input = match crate::operation_shell_input(
            &projection,
            ctx.args,
            ctx.stdin.map(|stdin| &**stdin),
        ) {
            Ok(input) => input,
            Err(err) => return Ok(bashkit::ExecResult::err(format!("verlet: {err}\n"), 2)),
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

struct VerletBuiltin {
    registry: Option<std::sync::Arc<dyn VbashOperationRegistry>>,
    capability_grants: std::collections::BTreeSet<String>,
}

#[async_trait::async_trait]
impl bashkit::Builtin for VerletBuiltin {
    async fn execute(
        &self,
        ctx: bashkit::BuiltinContext<'_>,
    ) -> bashkit::Result<bashkit::ExecResult> {
        let Some(registry) = &self.registry else {
            return Ok(bashkit::ExecResult::err(
                "verlet: no operation registry is mounted in this virtual bash\n",
                127,
            ));
        };
        let Some(subcommand) = ctx.args.first().map(String::as_str) else {
            return Ok(bashkit::ExecResult::err(crate::verlet_usage(), 2));
        };
        let (projection, input) = match subcommand {
            "run" => {
                if ctx.args.len() != 3 {
                    return Ok(bashkit::ExecResult::err(crate::verlet_usage(), 2));
                }
                let registered_name = ctx.args[1].clone();
                let operation_name = ctx.args[2].clone();
                let stdin = ctx
                    .stdin
                    .map_or_else(Vec::new, |stdin| stdin.as_bytes().to_vec());
                let Some(record) = registry.describe(&registered_name).await else {
                    return Ok(bashkit::ExecResult::err(
                        format!("verlet: registered operation {registered_name:?} was not found\n"),
                        127,
                    ));
                };
                let Some(projection) = record
                    .projections()
                    .operations
                    .into_iter()
                    .find(|projection| projection.operation_name == operation_name)
                else {
                    return Ok(bashkit::ExecResult::err(
                        format!(
                            "verlet: operation {operation_name:?} is not registered under {registered_name:?}\n"
                        ),
                        127,
                    ));
                };
                (projection, stdin)
            }
            _ => return Ok(bashkit::ExecResult::err(crate::verlet_usage(), 2)),
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
    registry: Option<std::sync::Arc<dyn VbashOperationRegistry>>,
}

#[async_trait::async_trait]
impl bashkit::Builtin for ManBuiltin {
    async fn execute(
        &self,
        ctx: bashkit::BuiltinContext<'_>,
    ) -> bashkit::Result<bashkit::ExecResult> {
        let Some(registry) = &self.registry else {
            return Ok(bashkit::ExecResult::err(
                "man: no operation registry is mounted in this virtual bash\n",
                127,
            ));
        };
        if ctx.args.len() != 1 {
            return Ok(bashkit::ExecResult::err(
                "usage: man <operation-command>\n".to_string(),
                2,
            ));
        }
        let command = &ctx.args[0];
        let projection =
            match operation_projection_for_shell_command(registry.as_ref(), command).await {
                Ok(projection) => projection,
                Err(err) => return Ok(bashkit::ExecResult::err(format!("man: {err}\n"), 127)),
            };
        Ok(bashkit::ExecResult::ok(crate::operation_shell_manual(
            command,
            &projection,
        )))
    }
}

struct ExternalCommandProxyBuiltin {
    command: String,
    route: crate::CommandRoute,
    executor: Option<std::sync::Arc<dyn verlet_process::execution::ExternalCommandExecutor>>,
}

#[async_trait::async_trait]
impl bashkit::Builtin for ExternalCommandProxyBuiltin {
    async fn execute(
        &self,
        ctx: bashkit::BuiltinContext<'_>,
    ) -> bashkit::Result<bashkit::ExecResult> {
        if self.route == crate::CommandRoute::Deny {
            return Ok(bashkit::ExecResult::err(
                format!(
                    "verlet: command denied by routing policy: {}\n",
                    self.command
                ),
                126,
            ));
        }
        let Some(executor) = self.executor.clone() else {
            return Ok(bashkit::ExecResult::err(
                "verlet: external command executor is not configured\n",
                127,
            ));
        };
        let Some(executor_kind) = self.route.executor_kind() else {
            return Ok(bashkit::ExecResult::err(
                format!(
                    "verlet: command {:?} is not an external proxy route\n",
                    self.command
                ),
                127,
            ));
        };
        let deadline = ctx
            .execution_extension::<verlet_process::execution::ExecutionDeadline>()
            .and_then(|deadline| deadline.try_with(Clone::clone).ok())
            .unwrap_or_else(|| {
                verlet_process::execution::ExecutionDeadline::from_now(std::time::Duration::ZERO)
            });
        let request = verlet_process::execution::ExternalCommandRequest {
            invocation: verlet_process::execution::ExternalCommandInvocation::Argv {
                command: self.command.clone(),
                args: ctx.args.to_vec(),
            },
            executor: executor_kind,
            cwd: ctx.cwd.clone(),
            stdin: ctx.stdin.map(ToString::to_string),
            deadline,
            max_output_bytes: crate::SPILL_RETENTION_MAX_BYTES,
        };

        let execution = match ctx
            .execution_extension::<tokio_util::sync::CancellationToken>()
            .and_then(|cancellation| cancellation.try_with(Clone::clone).ok())
        {
            Some(cancellation) => executor.exec_cancellable(request, cancellation).await,
            None => executor.exec(request).await,
        };
        let result = match execution {
            Ok(result) => result,
            Err(err) => return Ok(bashkit::ExecResult::err(format!("verlet: {err}\n"), 1)),
        };
        if let Err(err) = crate::apply_external_file_writes(ctx.fs.as_ref(), &result).await {
            return Ok(bashkit::ExecResult::err(format!("verlet: {err}\n"), 1));
        }
        Ok(crate::exec_result_from_virtual_output(
            crate::enforce_output_limit(result.output, crate::SPILL_RETENTION_MAX_BYTES),
        ))
    }
}

async fn operation_shell_command_counts(
    registry: &dyn VbashOperationRegistry,
) -> std::collections::BTreeMap<String, usize> {
    let mut commands = std::collections::BTreeMap::new();
    for record in registry.list().await {
        for projection in record.projections().operations {
            let command = crate::operation_shell_command_name(&projection);
            if !command.is_empty() {
                *commands.entry(command).or_insert(0) += 1;
            }
        }
    }
    commands
}

pub async fn operation_shell_command_names(
    registry: &dyn VbashOperationRegistry,
    reserved_commands: &std::collections::BTreeSet<String>,
) -> std::collections::BTreeSet<String> {
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
) -> crate::VerletVirtualBashResult<verlet_operations::OperationProjection> {
    let mut matches = Vec::new();
    for record in registry.list().await {
        for projection in record.projections().operations {
            if crate::operation_shell_command_name(&projection) == command {
                matches.push(projection);
            }
        }
    }
    match matches.len() {
        0 => Err(crate::VerletVirtualBashError::RuntimeExecution(format!(
            "operation command {command:?} was not found"
        ))),
        1 => Ok(matches.remove(0)),
        _ => Err(crate::VerletVirtualBashError::RuntimeExecution(format!(
            "operation command {command:?} is ambiguous across published operations"
        ))),
    }
}

async fn invoke_operation_projection(
    registry: &dyn VbashOperationRegistry,
    capability_grants: &std::collections::BTreeSet<String>,
    projection: &verlet_operations::OperationProjection,
    input: Vec<u8>,
) -> bashkit::ExecResult {
    let registered_name = &projection.registered_name;
    let operation_name = &projection.operation_name;
    if projection.abi.has_hidden_durable_sink() {
        return bashkit::ExecResult::err(
            format!(
                "verlet: operation {registered_name:?}/{operation_name:?} has a hidden durable sink\n"
            ),
            126,
        );
    }
    let missing = crate::missing_operation_capability_grants(projection, capability_grants);
    if !missing.is_empty() {
        return bashkit::ExecResult::err(
            format!(
                "verlet: operation {registered_name:?}/{operation_name:?} missing capability grants: {}\n",
                missing.join(", ")
            ),
            126,
        );
    }

    match registry
        .invoke_process_output(registered_name, operation_name, input)
        .await
    {
        Ok(output) => crate::exec_result_from_virtual_output(output),
        Err(err) => bashkit::ExecResult::err(format!("verlet: {err}\n"), 1),
    }
}

fn execution_error(err: impl std::fmt::Display) -> crate::VerletVirtualBashError {
    crate::virtual_bash_execution_error(err)
}

#[cfg(test)]
mod tests {
    use bashkit::FileSystem as _;

    struct SpillTestFs {
        inner: bashkit::InMemoryFs,
        flush_calls: std::sync::atomic::AtomicUsize,
        first_flush_entered: tokio::sync::Notify,
        fail_reads: std::sync::atomic::AtomicBool,
    }

    impl SpillTestFs {
        fn new() -> Self {
            Self {
                inner: bashkit::InMemoryFs::new(),
                flush_calls: std::sync::atomic::AtomicUsize::new(0),
                first_flush_entered: tokio::sync::Notify::new(),
                fail_reads: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl bashkit::FileSystemExt for SpillTestFs {
        fn usage(&self) -> bashkit::FsUsage {
            self.inner.usage()
        }

        fn limits(&self) -> bashkit::FsLimits {
            self.inner.limits()
        }
    }

    #[async_trait::async_trait]
    impl bashkit::FileSystem for SpillTestFs {
        async fn read_file(&self, path: &std::path::Path) -> bashkit::Result<Vec<u8>> {
            if self.fail_reads.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(std::io::Error::other("injected read failure").into());
            }
            self.inner.read_file(path).await
        }

        async fn write_file(&self, path: &std::path::Path, content: &[u8]) -> bashkit::Result<()> {
            self.inner.write_file(path, content).await
        }

        async fn append_file(&self, path: &std::path::Path, content: &[u8]) -> bashkit::Result<()> {
            self.inner.append_file(path, content).await
        }

        async fn mkdir(&self, path: &std::path::Path, recursive: bool) -> bashkit::Result<()> {
            self.inner.mkdir(path, recursive).await
        }

        async fn remove(&self, path: &std::path::Path, recursive: bool) -> bashkit::Result<()> {
            self.inner.remove(path, recursive).await
        }

        async fn stat(&self, path: &std::path::Path) -> bashkit::Result<bashkit::Metadata> {
            self.inner.stat(path).await
        }

        async fn read_dir(
            &self,
            path: &std::path::Path,
        ) -> bashkit::Result<Vec<bashkit::DirEntry>> {
            self.inner.read_dir(path).await
        }

        async fn exists(&self, path: &std::path::Path) -> bashkit::Result<bool> {
            self.inner.exists(path).await
        }

        async fn rename(
            &self,
            from: &std::path::Path,
            to: &std::path::Path,
        ) -> bashkit::Result<()> {
            self.inner.rename(from, to).await
        }

        async fn copy(&self, from: &std::path::Path, to: &std::path::Path) -> bashkit::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn symlink(
            &self,
            target: &std::path::Path,
            link: &std::path::Path,
        ) -> bashkit::Result<()> {
            self.inner.symlink(target, link).await
        }

        async fn read_link(&self, path: &std::path::Path) -> bashkit::Result<std::path::PathBuf> {
            self.inner.read_link(path).await
        }

        async fn chmod(&self, path: &std::path::Path, mode: u32) -> bashkit::Result<()> {
            self.inner.chmod(path, mode).await
        }

        async fn set_modified_time(
            &self,
            path: &std::path::Path,
            time: std::time::SystemTime,
        ) -> bashkit::Result<()> {
            self.inner.set_modified_time(path, time).await
        }
    }

    #[async_trait::async_trait]
    impl verlet_vfs::VerletVfsBackend for SpillTestFs {
        async fn flush(&self) -> bashkit::Result<()> {
            let call = self
                .flush_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call == 0 {
                self.first_flush_entered.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }

    struct StaticOperationRegistry {
        record: verlet_operations::RegisteredOperation,
    }

    #[test]
    fn bashkit_capture_limits_use_the_spill_retention_ceiling() {
        let limits = crate::harness::BashkitExecutionConfig::default().limits();

        assert_eq!(limits.max_stdout_bytes, crate::SPILL_RETENTION_MAX_BYTES);
        assert_eq!(limits.max_stderr_bytes, crate::SPILL_RETENTION_MAX_BYTES);
    }

    #[tokio::test]
    async fn cancelled_spill_flush_releases_the_harness_lock_and_retry_flushes() {
        let root = std::sync::Arc::new(SpillTestFs::new());
        let vfs = std::sync::Arc::new(verlet_vfs::VerletVfs::new(root.clone()));
        let harness = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::harness::BashkitExecutionHarness::new(
                crate::harness::BashkitExecutionConfig::default().with_workspace_vfs(vfs),
            )
            .await
            .unwrap(),
        ));
        let writing = std::sync::Arc::clone(&harness);
        let task = tokio::spawn(async move {
            writing
                .lock()
                .await
                .write_spill_file_if_available("/spill/call.stdout.txt", b"complete")
                .await
        });

        root.first_flush_entered.notified().await;
        task.abort();
        let guard = tokio::time::timeout(std::time::Duration::from_secs(30), harness.lock())
            .await
            .expect("cancelled spill must release the harness mutex");
        assert_eq!(
            guard.read_file("/spill/call.stdout.txt").await.unwrap(),
            b"complete"
        );
        assert!(
            guard
                .write_spill_file_if_available("/spill/call.stdout.txt", b"complete")
                .await
                .unwrap()
        );
        assert_eq!(
            root.flush_calls.load(std::sync::atomic::Ordering::SeqCst),
            2
        );
    }

    #[tokio::test]
    async fn spill_read_failure_never_overwrites_an_existing_file() {
        let root = std::sync::Arc::new(SpillTestFs::new());
        root.inner
            .mkdir(std::path::Path::new("/spill"), true)
            .await
            .unwrap();
        root.inner
            .write_file(std::path::Path::new("/spill/call.stdout.txt"), b"first")
            .await
            .unwrap();
        root.fail_reads
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let vfs = std::sync::Arc::new(verlet_vfs::VerletVfs::new(root.clone()));
        let harness = crate::harness::BashkitExecutionHarness::new(
            crate::harness::BashkitExecutionConfig::default().with_workspace_vfs(vfs),
        )
        .await
        .unwrap();

        assert!(
            harness
                .write_spill_file_if_available("/spill/call.stdout.txt", b"second")
                .await
                .is_err()
        );
        root.fail_reads
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            harness.read_file("/spill/call.stdout.txt").await.unwrap(),
            b"first"
        );
    }

    #[async_trait::async_trait]
    impl crate::harness::VbashOperationRegistry for StaticOperationRegistry {
        async fn describe(&self, name: &str) -> Option<verlet_operations::RegisteredOperation> {
            (self.record.name == name).then(|| self.record.clone())
        }

        async fn list(&self) -> Vec<verlet_operations::RegisteredOperation> {
            vec![self.record.clone()]
        }

        async fn invoke_process_output(
            &self,
            _registered_name: &str,
            _operation_name: &str,
            _input: Vec<u8>,
        ) -> Result<verlet_process::execution::VirtualCommandOutput, String> {
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
                let registry = std::sync::Arc::new(StaticOperationRegistry {
                    record: verlet_operations::RegisteredOperation {
                        name: "contacts".to_string(),
                        manifest: verlet_abi::WasmOperationManifest {
                            abi: "cooldis.operation/0.1".to_string(),
                            operations: vec![verlet_abi::WasmOperationDefinition {
                                id: 1,
                                name: "lookup".to_string(),
                                input: verlet_abi::WasmOperationValueKind::Json,
                                output: verlet_abi::WasmOperationValueKind::Json,
                                events: verlet_abi::WasmOperationEventKind::Jsonl,
                                mode: verlet_abi::WasmOperationMode::Sync,
                                required_capabilities: vec![
                                    "net:https://api.example.test".to_string(),
                                ],
                            }],
                        },
                        capability_grants: ["net:https://api.example.test".to_string()]
                            .into_iter()
                            .collect(),
                        metadata: std::collections::BTreeMap::new(),
                    },
                });
                let mut harness = crate::harness::BashkitExecutionHarness::new(
                    crate::harness::BashkitExecutionConfig::default()
                        .with_operation_registry(registry),
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
                assert!(output.stdout.contains("  verlet run contacts lookup"));
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
