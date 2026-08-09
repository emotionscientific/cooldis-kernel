use base64::Engine as _;

const APP_SERVER_CWD_METADATA: &str = "cooldis.app_server.cwd";
const DEFAULT_PROCESS_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PROCESS_YIELD_MS: u64 = 10_000;
const MAX_PROCESS_YIELD_MS: u64 = 30_000;
const DEFAULT_PROCESS_OUTPUT_CAP_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct KernelThreadOperationProvider {
    control: crate::kernel::runtime_host::kernel_control::RuntimeKernelControl,
    caller: verlet_runtime_contracts::ThreadContext,
    agent_resolver: Option<std::sync::Arc<dyn KernelThreadSpawnAgentResolver>>,
}

impl KernelThreadOperationProvider {
    pub fn new(
        control: crate::kernel::runtime_host::kernel_control::RuntimeKernelControl,
        caller: verlet_runtime_contracts::ThreadContext,
    ) -> Self {
        Self {
            control,
            caller,
            agent_resolver: None,
        }
    }

    pub fn with_agent_resolver(
        mut self,
        resolver: std::sync::Arc<dyn KernelThreadSpawnAgentResolver>,
    ) -> Self {
        self.agent_resolver = Some(resolver);
        self
    }

    async fn invoke_json(
        &self,
        operation_name: &str,
        arguments: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.invoke_json_with_dispatch(operation_name, arguments, None)
            .await
    }

    async fn invoke_json_with_dispatch(
        &self,
        operation_name: &str,
        arguments: serde_json::Value,
        injected_dispatch_id: Option<verlet_runtime_contracts::handle::DispatchId>,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        let value = match operation_name {
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION => {
                let args: ThreadSpawnArgs = decode_args(operation_name, arguments)?;
                require_non_empty(&args.task_name, "task_name")?;
                let dispatch_id = injected_dispatch_id
                    .or_else(|| {
                        args.dispatch_id
                            .map(verlet_runtime_contracts::handle::DispatchId::new)
                    })
                    .unwrap_or_else(|| {
                        verlet_runtime_contracts::handle::DispatchId::new(
                            uuid::Uuid::now_v7().to_string(),
                        )
                    });
                let receipt = self
                    .control
                    .dispatch_thread_spawn(
                        &self.caller,
                        dispatch_id,
                        args.task_name.clone(),
                        args.message,
                        args.agent_ref,
                        self.agent_resolver.clone(),
                    )
                    .await
                    .map_err(|err| model_thread_spawn_error(&args.task_name, err))?;
                serde_json::json!({
                    "operation": "cooldis.thread_spawn",
                    "task_name": args.task_name,
                    "status": receipt.status,
                })
            }
            crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION => {
                let args: ThreadSubmitArgs = decode_args(operation_name, arguments)?;
                let resolution = self
                    .control
                    .resolve_child_task_name(&self.caller, &args.task_name)
                    .await
                    .map_err(|err| {
                        model_task_resolution_error(operation_name, &args.task_name, err)
                    })?;
                let target_thread_id =
                    resolved_thread_id(operation_name, &args.task_name, &resolution.handle.id)?;
                let dispatch_id = injected_dispatch_id
                    .or_else(|| {
                        args.dispatch_id
                            .map(verlet_runtime_contracts::handle::DispatchId::new)
                    })
                    .unwrap_or_else(|| {
                        verlet_runtime_contracts::handle::DispatchId::new(
                            uuid::Uuid::now_v7().to_string(),
                        )
                    });
                let receipt = self
                    .control
                    .submit_to_thread_with_dispatch(
                        &self.caller,
                        target_thread_id,
                        dispatch_id,
                        crate::kernel::runtime_host::turn::TurnInput::text(args.message),
                    )
                    .await
                    .map_err(|err| {
                        model_task_dispatch_error(
                            operation_name,
                            &args.task_name,
                            "submit dispatch",
                            err,
                        )
                    })?;
                serde_json::json!({
                    "operation": "cooldis.thread_submit",
                    "task_name": args.task_name,
                    "status": receipt.status,
                })
            }
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION => {
                let args: ThreadWaitArgs = decode_args(operation_name, arguments)?;
                let resolution = self
                    .control
                    .resolve_child_task_name(&self.caller, &args.task_name)
                    .await
                    .map_err(|err| {
                        model_task_resolution_error(operation_name, &args.task_name, err)
                    })?;
                let target_thread_id =
                    resolved_thread_id(operation_name, &args.task_name, &resolution.handle.id)?;
                let receipt = self
                    .control
                    .wait_thread(&self.caller, target_thread_id, args.timeout_ms)
                    .await
                    .map_err(|err| {
                        model_task_dispatch_error(
                            operation_name,
                            &args.task_name,
                            "wait dispatch",
                            err,
                        )
                    })?;
                serde_json::json!({
                    "operation": "cooldis.thread_wait",
                    "task_name": args.task_name,
                    "status": receipt.status,
                })
            }
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION => {
                let args: ThreadStatusArgs = decode_args(operation_name, arguments)?;
                let resolution = self
                    .control
                    .resolve_child_task_name(&self.caller, &args.task_name)
                    .await
                    .map_err(|err| {
                        model_task_resolution_error(operation_name, &args.task_name, err)
                    })?;
                let target_thread_id =
                    resolved_thread_id(operation_name, &args.task_name, &resolution.handle.id)?;
                let receipt = self
                    .control
                    .thread_status(&self.caller, target_thread_id)
                    .await
                    .map_err(|err| {
                        model_task_dispatch_error(
                            operation_name,
                            &args.task_name,
                            "status dispatch",
                            err,
                        )
                    })?;
                serde_json::json!({
                    "operation": "cooldis.thread_status",
                    "task_name": args.task_name,
                    "status": receipt.status,
                })
            }
            crate::operations::kernel_packages::THREAD_CANCEL_OPERATION => {
                let args: ThreadCancelArgs = decode_args(operation_name, arguments)?;
                let resolution = self
                    .control
                    .resolve_child_task_name(&self.caller, &args.task_name)
                    .await
                    .map_err(|err| {
                        model_task_resolution_error(operation_name, &args.task_name, err)
                    })?;
                let target_thread_id =
                    resolved_thread_id(operation_name, &args.task_name, &resolution.handle.id)?;
                let receipt = self
                    .control
                    .cancel_thread(
                        &self.caller,
                        target_thread_id,
                        "thread_cancel operation".to_string(),
                    )
                    .await
                    .map_err(|err| {
                        model_task_dispatch_error(
                            operation_name,
                            &args.task_name,
                            "cancel dispatch",
                            err,
                        )
                    })?;
                serde_json::json!({
                    "operation": "cooldis.thread_cancel",
                    "task_name": args.task_name,
                    "status": receipt.status,
                })
            }
            _ => {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    format!(
                        "unknown kernel operation {VERLET_THREADS_PACKAGE}/{operation_name}",
                        VERLET_THREADS_PACKAGE =
                            crate::operations::kernel_packages::VERLET_THREADS_PACKAGE
                    ),
                ));
            }
        };
        Ok(value)
    }
}

#[derive(Clone)]
pub struct KernelScheduleOperationProvider {
    control: crate::kernel::runtime_host::kernel_control::RuntimeKernelControl,
    caller: verlet_runtime_contracts::ThreadContext,
}

impl KernelScheduleOperationProvider {
    pub fn new(
        control: crate::kernel::runtime_host::kernel_control::RuntimeKernelControl,
        caller: verlet_runtime_contracts::ThreadContext,
    ) -> Self {
        Self { control, caller }
    }

    async fn invoke_json(
        &self,
        operation_name: &str,
        arguments: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        let value = match operation_name {
            crate::operations::kernel_packages::MANDATE_START_OPERATION => {
                let args: MandateStartArgs = decode_schedule_args(operation_name, arguments)?;
                let target_thread_id = optional_target_thread_id(
                    &self.caller,
                    args.thread_id.as_deref(),
                    "thread_id",
                )?;
                let receipt = self
                    .control
                    .start_mandate(
                        &self.caller,
                        target_thread_id,
                        crate::kernel::mandate_lifecycle::MandateStartRequest {
                            schedule: args.schedule,
                            max_occurrences: args.max_occurrences,
                            catch_up: args.catch_up,
                            input_template: args.input_template,
                            snapshot_id: None,
                            expires_at: args.expires_at,
                        },
                    )
                    .await?;
                serde_json::json!({
                    "operation": "cooldis.mandate_start",
                    "status": "started",
                    "thread_id": target_thread_id.to_string(),
                    "mandate_event_id": receipt.event.id.to_string(),
                    "stream_id": receipt.event.stream_id.as_str(),
                    "sequence": receipt.event.sequence.get(),
                })
            }
            crate::operations::kernel_packages::MANDATE_REVOKE_OPERATION => {
                let args: MandateRevokeArgs = decode_schedule_args(operation_name, arguments)?;
                let target_thread_id = optional_target_thread_id(
                    &self.caller,
                    args.thread_id.as_deref(),
                    "thread_id",
                )?;
                let mandate_event_id = crate::kernel::mandate_lifecycle::parse_mandate_event_id(
                    &args.mandate_event_id,
                )?;
                let receipt = self
                    .control
                    .revoke_mandate(&self.caller, target_thread_id, mandate_event_id)
                    .await?;
                let status: &str = receipt.status.as_ref();
                serde_json::json!({
                    "operation": "cooldis.mandate_revoke",
                    "status": status,
                    "thread_id": target_thread_id.to_string(),
                    "mandate_event_id": mandate_event_id.to_string(),
                    "revoked_event_id": receipt.revoke_event.id.to_string(),
                })
            }
            crate::operations::kernel_packages::MANDATE_LIST_OPERATION => {
                let args: MandateListArgs = decode_schedule_args(operation_name, arguments)?;
                let target_thread_id = optional_target_thread_id(
                    &self.caller,
                    args.thread_id.as_deref(),
                    "thread_id",
                )?;
                let mandates = self
                    .control
                    .list_mandates(&self.caller, target_thread_id)
                    .await?
                    .iter()
                    .map(active_mandate_json)
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "operation": "cooldis.mandate_list",
                    "thread_id": target_thread_id.to_string(),
                    "mandates": mandates,
                })
            }
            _ => {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    format!(
                        "unknown kernel operation {VERLET_SCHEDULE_PACKAGE}/{operation_name}",
                        VERLET_SCHEDULE_PACKAGE =
                            crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE
                    ),
                ));
            }
        };
        Ok(value)
    }
}

#[derive(Clone)]
pub struct KernelProcessOperationProvider {
    caller: verlet_runtime_contracts::ThreadContext,
    process_manager: verlet_process::live::AsyncExecutionManager,
    live_backend: std::sync::Arc<dyn verlet_process::live::LiveProcessBackend>,
    default_cwd: std::path::PathBuf,
    default_output_cap_bytes: usize,
    process_dispatcher: Option<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher>,
}

impl KernelProcessOperationProvider {
    pub fn new(
        caller: verlet_runtime_contracts::ThreadContext,
        default_cwd: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            caller,
            process_manager: verlet_process::live::AsyncExecutionManager::default(),
            live_backend: std::sync::Arc::new(verlet_process::live::HostBashLiveBackend),
            default_cwd: default_cwd.into(),
            default_output_cap_bytes: DEFAULT_PROCESS_OUTPUT_CAP_BYTES,
            process_dispatcher: None,
        }
    }

    pub fn with_process_manager(
        mut self,
        process_manager: verlet_process::live::AsyncExecutionManager,
    ) -> Self {
        self.process_manager = process_manager;
        self
    }

    pub fn with_backend(
        mut self,
        backend: std::sync::Arc<dyn verlet_process::live::LiveProcessBackend>,
    ) -> Self {
        self.live_backend = backend;
        self
    }

    pub fn with_process_dispatcher(
        mut self,
        dispatcher: crate::kernel::process_handle_dispatch::ProcessHandleDispatcher,
    ) -> Self {
        self.process_dispatcher = Some(dispatcher);
        self
    }

    async fn invoke_json(
        &self,
        operation_name: &str,
        arguments: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.invoke_json_with_dispatch(operation_name, arguments, None)
            .await
    }

    async fn invoke_json_with_dispatch(
        &self,
        operation_name: &str,
        arguments: serde_json::Value,
        injected_dispatch_id: Option<verlet_runtime_contracts::handle::DispatchId>,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        let value = match operation_name {
            crate::operations::kernel_packages::PROCESS_EXEC_OPERATION => {
                let args: ProcessExecArgs = decode_process_args(operation_name, arguments)?;
                let explicit_dispatch_id = args.dispatch_id.clone();
                let command_bytes = serde_json::to_vec(&args.command).map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                        "encode process command for dispatch digest: {err}"
                    ))
                })?;
                if args.command.is_empty() {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        format!(
                            "operation {VERLET_PROCESS_PACKAGE}/{operation_name} requires a non-empty command argv",
                            VERLET_PROCESS_PACKAGE =
                                crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE
                        ),
                    ));
                }
                let default_cwd = self.effective_default_cwd();
                let cwd = resolve_process_cwd(&default_cwd, args.cwd.as_deref());
                let env = args
                    .env
                    .into_iter()
                    .map(|(key, value)| (key, Some(value)))
                    .collect::<std::collections::BTreeMap<_, _>>();
                let timeout = std::time::Duration::from_millis(
                    args.timeout_ms.unwrap_or(DEFAULT_PROCESS_TIMEOUT_MS),
                );
                let output_cap =
                    process_output_cap(args.output_bytes_cap, self.default_output_cap_bytes);
                let yield_time = process_yield_time(args.yield_time_ms);
                let request =
                    verlet_process::live::AsyncProcessStartRequest::host_command(args.command, cwd)
                        .with_owner(
                            self.process_owner("kernel-operation:verlet-process/process_exec"),
                        )
                        .with_env(env)
                        .pipe_stdin(args.stream_stdin)
                        .with_deadline(verlet_process::execution::ExecutionDeadline::from_now(
                            timeout,
                        ))
                        .with_yield_time(yield_time)
                        .with_output_cap_bytes(output_cap);
                let dispatch_id = injected_dispatch_id
                    .or_else(|| {
                        explicit_dispatch_id.map(verlet_runtime_contracts::handle::DispatchId::new)
                    })
                    .unwrap_or_else(|| {
                        verlet_runtime_contracts::handle::DispatchId::new(
                            uuid::Uuid::now_v7().to_string(),
                        )
                    });
                let dispatcher = self.process_dispatcher.as_ref().ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        "process_exec requires the durable process dispatch ingress lane"
                            .to_string(),
                    )
                })?;
                let outcome = dispatcher
                    .dispatch_start(
                        &self.caller.coordinates,
                        dispatch_id.clone(),
                        crate::kernel::process_handle_dispatch::command_digest(&command_bytes),
                        self.process_manager.clone(),
                        std::sync::Arc::clone(&self.live_backend),
                        request,
                    )
                    .await?;
                let mut value =
                    process_snapshot_output_json("cooldis.process_exec", &outcome.snapshot);
                value["dispatch_id"] = serde_json::json!(dispatch_id.to_string());
                value
            }
            crate::operations::kernel_packages::PROCESS_POLL_OPERATION => {
                let args: ProcessHandleArgs = decode_process_args(operation_name, arguments)?;
                let process_id = parse_process_id(&args.process_id, "process_id")?;
                self.require_process_handle(process_id).await?;
                let outcome = self
                    .process_manager
                    .poll(
                        process_id,
                        process_yield_time(args.yield_time_ms),
                        process_output_cap(args.output_bytes_cap, self.default_output_cap_bytes),
                    )
                    .await?;
                process_snapshot_output_json("cooldis.process_poll", &outcome.snapshot)
            }
            crate::operations::kernel_packages::PROCESS_WRITE_OPERATION => {
                let args: ProcessWriteArgs = decode_process_args(operation_name, arguments)?;
                let process_id = parse_process_id(&args.process_id, "process_id")?;
                self.require_process_handle(process_id).await?;
                let bytes = base64::engine::general_purpose::STANDARD.decode(args.delta_base64).map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                        "operation {VERLET_PROCESS_PACKAGE}/{operation_name} requires valid base64 delta_base64: {err}",
                                                             VERLET_PROCESS_PACKAGE = crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE
                                                         ))
                })?;
                let outcome = self
                    .process_manager
                    .write(
                        process_id,
                        bytes,
                        process_yield_time(args.yield_time_ms),
                        process_output_cap(args.output_bytes_cap, self.default_output_cap_bytes),
                    )
                    .await?;
                process_snapshot_output_json("cooldis.process_write", &outcome.snapshot)
            }
            crate::operations::kernel_packages::PROCESS_TERMINATE_OPERATION => {
                let args: ProcessTerminateArgs = decode_process_args(operation_name, arguments)?;
                let process_id = parse_process_id(&args.process_id, "process_id")?;
                self.require_process_handle(process_id).await?;
                let outcome = self
                    .process_manager
                    .terminate(
                        process_id,
                        args.reason
                            .unwrap_or_else(|| "verlet-process terminate requested".to_string()),
                        process_yield_time(args.yield_time_ms),
                        self.default_output_cap_bytes,
                    )
                    .await?;
                process_snapshot_output_json("cooldis.process_terminate", &outcome.snapshot)
            }
            _ => {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    format!(
                        "unknown kernel operation {VERLET_PROCESS_PACKAGE}/{operation_name}",
                        VERLET_PROCESS_PACKAGE =
                            crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE
                    ),
                ));
            }
        };
        Ok(value)
    }

    fn process_owner(&self, surface: &str) -> verlet_process::live::AsyncProcessOwner {
        verlet_process::live::AsyncProcessOwner {
            thread_id: Some(self.caller.coordinates.thread_id.to_string()),
            turn_id: None,
            call_id: None,
            surface: Some(surface.to_string()),
        }
    }

    async fn require_process_handle(
        &self,
        process_id: verlet_process::process::VerletProcessId,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.process_dispatcher
            .as_ref()
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "process handle verbs require the durable process dispatch ingress lane"
                        .to_string(),
                )
            })?
            .require_live_handle(process_id, Some(&self.caller.coordinates))
            .await
            .map(|_| ())
    }

    fn effective_default_cwd(&self) -> std::path::PathBuf {
        self.caller
            .metadata
            .get(APP_SERVER_CWD_METADATA)
            .filter(|cwd| !cwd.trim().is_empty())
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| self.default_cwd.clone())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KernelNotifyOperationProvider;

impl KernelNotifyOperationProvider {
    async fn invoke_json(
        &self,
        operation_name: &str,
        arguments: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        let value = match operation_name {
            crate::operations::kernel_packages::NOTIFY_PREVIEW_OPERATION => {
                let args: NotifyPreviewArgs = decode_notify_args(operation_name, arguments)?;
                let NotifyPreviewArgs {
                    channel,
                    subject,
                    body,
                    severity,
                } = args;
                require_non_empty(&channel, "channel")?;
                require_non_empty(&body, "body")?;
                let mut value = serde_json::json!({
                    "operation": "cooldis.notify_preview",
                    "status": "recorded",
                    "delivery": "not_sent",
                    "channel": channel,
                    "body": body,
                    "severity": severity.unwrap_or_else(|| "info".to_string()),
                    "channel_decision_required": true,
                    "reason": "V1 records notification intent; channel-specific delivery adapters are explicit operations."
                });
                if let Some(subject) = subject {
                    value["subject"] = serde_json::json!(subject);
                }
                value
            }
            crate::operations::kernel_packages::CHANNEL_EMIT_OPERATION => {
                let args: ChannelEmitArgs = decode_notify_args(operation_name, arguments)?;
                let ChannelEmitArgs {
                    channel,
                    message,
                    thread_id,
                } = args;
                require_non_empty(&channel, "channel")?;
                require_non_empty(&message, "message")?;
                let mut value = serde_json::json!({
                    "operation": "cooldis.channel_emit",
                    "status": "recorded",
                    "delivery": "not_sent",
                    "channel": channel,
                    "message": message,
                    "channel_decision_required": true,
                    "reason": "V1 records channel egress intent; channel-specific delivery adapters are explicit operations."
                });
                if let Some(thread_id) = thread_id {
                    value["thread_id"] = serde_json::json!(thread_id);
                }
                value
            }
            _ => {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    format!(
                        "unknown kernel operation {VERLET_NOTIFY_PACKAGE}/{operation_name}",
                        VERLET_NOTIFY_PACKAGE =
                            crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE
                    ),
                ));
            }
        };
        Ok(value)
    }
}

#[derive(Clone, Debug)]
pub struct KernelThreadSpawnAgentBinding {
    pub metadata: std::collections::BTreeMap<String, String>,
    pub compile_receipt: serde_json::Value,
    pub bind_receipt: serde_json::Value,
}

#[async_trait::async_trait]
pub trait KernelThreadSpawnAgentResolver: Send + Sync {
    /// Alias used when the model omits `agent_ref`. Runtime integrations with a
    /// synthesized default manifest return that alias; lower-level runtimes may
    /// retain the unbound compatibility path by returning `None`.
    fn default_agent_ref(
        &self,
        _caller: &verlet_runtime_contracts::ThreadContext,
    ) -> Option<String> {
        None
    }

    async fn resolve_agent_ref(
        &self,
        caller: &verlet_runtime_contracts::ThreadContext,
        agent_ref: &str,
    ) -> crate::kernel::runtime_host::VerletResult<KernelThreadSpawnAgentBinding>;
}

#[async_trait::async_trait]
impl verlet_operations::operation_registry::KernelOperationDispatcher
    for KernelThreadOperationProvider
{
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        let arguments: serde_json::Value =
            serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let value = self
            .invoke_json(operation_name, arguments)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }

    async fn invoke_kernel_operation_with_metadata(
        &self,
        operation_name: &str,
        input: Vec<u8>,
        metadata: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        let arguments: serde_json::Value =
            serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let dispatch_id = metadata
            .get("cooldis.tool_call_id")
            .and_then(serde_json::Value::as_str)
            .map(verlet_runtime_contracts::handle::DispatchId::new);
        let value = self
            .invoke_json_with_dispatch(operation_name, arguments, dispatch_id)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }
}

#[async_trait::async_trait]
impl verlet_operations::operation_registry::KernelOperationDispatcher
    for KernelScheduleOperationProvider
{
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        let arguments: serde_json::Value =
            serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let value = self
            .invoke_json(operation_name, arguments)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }
}

#[async_trait::async_trait]
impl verlet_operations::operation_registry::KernelOperationDispatcher
    for KernelProcessOperationProvider
{
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        let arguments: serde_json::Value =
            serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let value = self
            .invoke_json(operation_name, arguments)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }

    async fn invoke_kernel_operation_with_metadata(
        &self,
        operation_name: &str,
        input: Vec<u8>,
        metadata: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        let arguments: serde_json::Value =
            serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let dispatch_id = metadata
            .get("cooldis.tool_call_id")
            .and_then(serde_json::Value::as_str)
            .map(verlet_runtime_contracts::handle::DispatchId::new);
        let value = self
            .invoke_json_with_dispatch(operation_name, arguments, dispatch_id)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }
}

#[async_trait::async_trait]
impl verlet_operations::operation_registry::KernelOperationDispatcher
    for KernelNotifyOperationProvider
{
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        let arguments: serde_json::Value =
            serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let value = self
            .invoke_json(operation_name, arguments)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadSpawnArgs {
    task_name: String,
    message: String,
    #[serde(default)]
    agent_ref: Option<String>,
    #[serde(default)]
    dispatch_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadSubmitArgs {
    task_name: String,
    message: String,
    #[serde(default)]
    dispatch_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadWaitArgs {
    task_name: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadStatusArgs {
    task_name: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadCancelArgs {
    task_name: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MandateStartArgs {
    #[serde(default)]
    thread_id: Option<String>,
    schedule: crate::kernel::control_decision::MandateSchedulePayload,
    #[serde(default)]
    max_occurrences: Option<u32>,
    #[serde(default)]
    catch_up: Option<crate::kernel::control_decision::MandateCatchUpPolicy>,
    #[serde(default)]
    input_template: Option<String>,
    #[serde(default)]
    expires_at: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MandateRevokeArgs {
    #[serde(default)]
    thread_id: Option<String>,
    mandate_event_id: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MandateListArgs {
    #[serde(default)]
    thread_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessExecArgs {
    command: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    stream_stdin: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    yield_time_ms: Option<u64>,
    #[serde(default)]
    output_bytes_cap: Option<usize>,
    #[serde(default)]
    dispatch_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessHandleArgs {
    process_id: String,
    #[serde(default)]
    yield_time_ms: Option<u64>,
    #[serde(default)]
    output_bytes_cap: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessWriteArgs {
    process_id: String,
    delta_base64: String,
    #[serde(default)]
    yield_time_ms: Option<u64>,
    #[serde(default)]
    output_bytes_cap: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessTerminateArgs {
    process_id: String,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    yield_time_ms: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NotifyPreviewArgs {
    channel: String,
    #[serde(default)]
    subject: Option<String>,
    body: String,
    #[serde(default)]
    severity: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelEmitArgs {
    channel: String,
    message: String,
    #[serde(default)]
    thread_id: Option<String>,
}

fn decode_args<T: serde::de::DeserializeOwned>(
    operation_name: &str,
    arguments: serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<T> {
    serde_json::from_value(arguments).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "operation {VERLET_THREADS_PACKAGE}/{operation_name} has invalid arguments: {err}",
            VERLET_THREADS_PACKAGE = crate::operations::kernel_packages::VERLET_THREADS_PACKAGE
        ))
    })
}

fn decode_schedule_args<T: serde::de::DeserializeOwned>(
    operation_name: &str,
    arguments: serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<T> {
    serde_json::from_value(arguments).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "operation {VERLET_SCHEDULE_PACKAGE}/{operation_name} has invalid arguments: {err}",
            VERLET_SCHEDULE_PACKAGE = crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE
        ))
    })
}

fn decode_process_args<T: serde::de::DeserializeOwned>(
    operation_name: &str,
    arguments: serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<T> {
    serde_json::from_value(arguments).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "operation {VERLET_PROCESS_PACKAGE}/{operation_name} has invalid arguments: {err}",
            VERLET_PROCESS_PACKAGE = crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE
        ))
    })
}

fn decode_notify_args<T: serde::de::DeserializeOwned>(
    operation_name: &str,
    arguments: serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<T> {
    serde_json::from_value(arguments).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "operation {VERLET_NOTIFY_PACKAGE}/{operation_name} has invalid arguments: {err}",
            VERLET_NOTIFY_PACKAGE = crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE
        ))
    })
}

fn require_non_empty(value: &str, field: &str) -> crate::kernel::runtime_host::VerletResult<()> {
    if value.trim().is_empty() {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!("{field} must not be empty"),
        ));
    }
    Ok(())
}

fn resolved_thread_id(
    operation_name: &str,
    task_name: &str,
    handle_id: &str,
) -> crate::kernel::runtime_host::VerletResult<verlet_runtime_contracts::ThreadId> {
    verlet_runtime_contracts::ThreadId::parse_str(handle_id).map_err(|err| {
        model_task_dispatch_error(operation_name, task_name, "resolved handle decode", err)
    })
}

fn task_target_unavailable(
    operation_name: &str,
    task_name: &str,
) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
        "{operation_name} task_name {task_name:?} target is not available"
    ))
}

fn model_task_resolution_error(
    operation_name: &str,
    task_name: &str,
    err: crate::kernel::runtime_host::VerletError,
) -> crate::kernel::runtime_host::VerletError {
    let safe_not_found = format!("thread task_name {task_name:?} was not found under this parent");
    let safe_ambiguity = format!("thread task_name {task_name:?} is ambiguous under this parent");
    if matches!(
        &err,
        crate::kernel::runtime_host::VerletError::RuntimeExecution(message)
            if message == "thread task_name must not be empty"
                || message == &safe_not_found
                || message == &safe_ambiguity
    ) {
        err
    } else {
        eprintln!(
            "verlet model thread operation {operation_name} task_name {task_name:?} resolution failed: {err}"
        );
        task_target_unavailable(operation_name, task_name)
    }
}

fn model_thread_spawn_error(
    task_name: &str,
    err: crate::kernel::runtime_host::VerletError,
) -> crate::kernel::runtime_host::VerletError {
    let safe_duplicate = format!(
        "thread_spawn task_name {task_name:?} is already bound under this parent; retry with the original dispatch or choose a new task_name"
    );
    if matches!(
        &err,
        crate::kernel::runtime_host::VerletError::RuntimeExecution(message)
            if message == &safe_duplicate
    ) {
        err
    } else {
        eprintln!(
            "verlet model thread operation thread_spawn task_name {task_name:?} dispatch failed: {err}"
        );
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "thread_spawn task_name {task_name:?} failed"
        ))
    }
}

fn model_task_dispatch_error(
    operation_name: &str,
    task_name: &str,
    phase: &str,
    err: impl std::fmt::Display,
) -> crate::kernel::runtime_host::VerletError {
    eprintln!(
        "verlet model thread operation {operation_name} task_name {task_name:?} {phase} failed: {err}"
    );
    task_target_unavailable(operation_name, task_name)
}

fn optional_target_thread_id(
    caller: &verlet_runtime_contracts::ThreadContext,
    value: Option<&str>,
    field: &str,
) -> crate::kernel::runtime_host::VerletResult<verlet_runtime_contracts::ThreadId> {
    match value {
        Some(value) => parse_thread_id(value, field),
        None => Ok(caller.coordinates.thread_id),
    }
}

fn parse_thread_id(
    value: &str,
    field: &str,
) -> crate::kernel::runtime_host::VerletResult<verlet_runtime_contracts::ThreadId> {
    verlet_runtime_contracts::ThreadId::parse_str(value).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "{field} is not a valid Verlet thread id: {err}"
        ))
    })
}

fn parse_process_id(
    value: &str,
    field: &str,
) -> crate::kernel::runtime_host::VerletResult<verlet_process::process::VerletProcessId> {
    value
        .parse::<verlet_process::process::VerletProcessId>()
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "{field} is not a valid Verlet process id: {err}"
            ))
        })
}

fn resolve_process_cwd(default_cwd: &std::path::Path, cwd: Option<&str>) -> std::path::PathBuf {
    match cwd {
        Some(cwd) if !cwd.trim().is_empty() => {
            let path = std::path::PathBuf::from(cwd);
            if path.is_absolute() {
                path
            } else {
                default_cwd.join(path)
            }
        }
        _ => default_cwd.to_path_buf(),
    }
}

fn process_yield_time(yield_time_ms: Option<u64>) -> std::time::Duration {
    std::time::Duration::from_millis(
        yield_time_ms
            .unwrap_or(DEFAULT_PROCESS_YIELD_MS)
            .min(MAX_PROCESS_YIELD_MS),
    )
}

fn process_output_cap(output_bytes_cap: Option<usize>, default_cap: usize) -> usize {
    output_bytes_cap
        .unwrap_or(default_cap)
        .clamp(1, default_cap.max(1))
}

fn process_snapshot_output_json(
    operation: &str,
    snapshot: &verlet_process::live::AsyncProcessSnapshot,
) -> serde_json::Value {
    let status: &str = snapshot.status.as_ref();
    let mut value = serde_json::json!({
        "operation": operation,
        "status": status,
        "backend": &snapshot.backend,
        "label": snapshot.label,
        "stdout": String::from_utf8_lossy(&snapshot.stdout).into_owned(),
        "stderr": String::from_utf8_lossy(&snapshot.stderr).into_owned(),
        "truncated": snapshot.stdout_truncated || snapshot.stderr_truncated,
        "stdout_truncated": snapshot.stdout_truncated,
        "stderr_truncated": snapshot.stderr_truncated,
        "event_count": snapshot.events.len(),
    });
    if let Some(process_id) = snapshot.process_id {
        value["process_id"] = serde_json::json!(process_id.to_string());
    }
    if let Some(exit_code) = snapshot.exit_code {
        value["exit_code"] = serde_json::json!(exit_code);
    }
    value
}

fn active_mandate_json(
    mandate: &crate::kernel::mandate_lifecycle::ActiveMandate,
) -> serde_json::Value {
    serde_json::json!({
        "mandate_event_id": mandate.event.id.to_string(),
        "mandate_id": mandate.payload.mandate_id.clone(),
        "thread_id": mandate
            .payload
            .subject
            .thread_id
            .as_deref()
            .or(mandate.payload.thread_id.as_deref()),
        "schedule": mandate.payload.schedule.clone(),
        "max_occurrences": mandate.payload.max_occurrences,
        "catch_up": mandate.payload.catch_up,
        "input_template": mandate.payload.input_template.clone(),
        "created_at_ms": mandate.event.created_at_ms,
    })
}

fn operations_runtime_error(
    err: impl std::fmt::Display,
) -> verlet_operations::VerletOperationsError {
    verlet_operations::VerletOperationsError::RuntimeExecution(err.to_string())
}

#[cfg(test)]
mod tests;
