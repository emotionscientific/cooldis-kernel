use crate::agent::manifest_bind::canonical_json_hash;
use crate::{
    ActiveMandate, AgentManifestBindReceipt, CHANNEL_EMIT_OPERATION, COOLDIS_NOTIFY_PACKAGE,
    COOLDIS_PROCESS_PACKAGE, COOLDIS_SCHEDULE_PACKAGE, COOLDIS_THREADS_PACKAGE, CooldisError,
    CooldisResult, KernelOperationDispatcher, MANDATE_LIST_OPERATION, MANDATE_REVOKE_OPERATION,
    MANDATE_START_OPERATION, MandateCatchUpPolicy, MandateSchedulePayload, MandateStartRequest,
    NOTIFY_PREVIEW_OPERATION, PROCESS_EXEC_OPERATION, PROCESS_POLL_OPERATION,
    PROCESS_TERMINATE_OPERATION, PROCESS_WRITE_OPERATION, RuntimeKernelControl,
    THREAD_AGENT_MANIFEST_HASH_METADATA, THREAD_CANCEL_OPERATION, THREAD_SPAWN_GRANTED_METADATA,
    THREAD_SPAWN_INPUTS_HASH_METADATA, THREAD_SPAWN_OPERATION, THREAD_STATUS_OPERATION,
    THREAD_SUBMIT_OPERATION, THREAD_WAIT_OPERATION, ThreadContext, ThreadId, TurnInput,
    parse_mandate_event_id,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cooldis_process::{
    AsyncExecutionManager, AsyncProcessOwner, AsyncProcessSnapshot, AsyncProcessStartRequest,
    CooldisProcessId, ExecutionDeadline, HostBashLiveBackend, LiveProcessBackend,
};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const APP_SERVER_CWD_METADATA: &str = "cooldis.app_server.cwd";
const DEFAULT_PROCESS_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PROCESS_YIELD_MS: u64 = 10_000;
const MAX_PROCESS_YIELD_MS: u64 = 30_000;
const DEFAULT_PROCESS_OUTPUT_CAP_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct KernelThreadOperationProvider {
    control: RuntimeKernelControl,
    caller: ThreadContext,
    agent_resolver: Option<Arc<dyn KernelThreadSpawnAgentResolver>>,
}

impl KernelThreadOperationProvider {
    pub fn new(control: RuntimeKernelControl, caller: ThreadContext) -> Self {
        Self {
            control,
            caller,
            agent_resolver: None,
        }
    }

    pub fn with_agent_resolver(
        mut self,
        resolver: Arc<dyn KernelThreadSpawnAgentResolver>,
    ) -> Self {
        self.agent_resolver = Some(resolver);
        self
    }

    async fn invoke_json(&self, operation_name: &str, arguments: Value) -> CooldisResult<Value> {
        let value = match operation_name {
            THREAD_SPAWN_OPERATION => {
                let inputs_hash = canonical_json_hash(&arguments)?;
                let args: ThreadSpawnArgs = decode_args(operation_name, arguments)?;
                let agent_binding = if let Some(agent_ref) = args.agent_ref.as_deref() {
                    let resolver = self.agent_resolver.as_ref().ok_or_else(|| {
                        CooldisError::RuntimeExecution(
                            "thread_spawn agent_ref requires a manifest resolver from the runtime"
                                .to_string(),
                        )
                    })?;
                    Some(resolver.resolve_agent_ref(&self.caller, agent_ref).await?)
                } else {
                    None
                };
                let metadata = agent_binding
                    .as_ref()
                    .map(|binding| binding.metadata.clone())
                    .unwrap_or_default();
                let mut metadata = metadata;
                metadata.insert(THREAD_SPAWN_INPUTS_HASH_METADATA.to_string(), inputs_hash);
                if let Some(binding) = &agent_binding {
                    let bind_receipt = serde_json::from_value::<AgentManifestBindReceipt>(
                        binding.bind_receipt.clone(),
                    )
                    .map_err(|err| {
                        CooldisError::RuntimeFactory(format!(
                            "thread_spawn agent_ref bind receipt is invalid: {err}"
                        ))
                    })?;
                    metadata
                        .entry(THREAD_AGENT_MANIFEST_HASH_METADATA.to_string())
                        .or_insert_with(|| bind_receipt.manifest_hash.clone());
                    let granted = serde_json::to_string(&bind_receipt.granted).map_err(|err| {
                        CooldisError::RuntimeFactory(format!(
                            "failed to encode thread_spawn grants: {err}"
                        ))
                    })?;
                    metadata.insert(THREAD_SPAWN_GRANTED_METADATA.to_string(), granted);
                }
                let receipt = self
                    .control
                    .spawn_subagent(
                        &self.caller,
                        Some(args.task_name),
                        TurnInput::text(args.message),
                        metadata,
                    )
                    .await?;
                if let Some(binding) = agent_binding {
                    self.control
                        .record_manifest_receipts_for_thread(
                            &self.caller,
                            receipt.thread_id,
                            binding.compile_receipt,
                            binding.bind_receipt,
                        )
                        .await?;
                }
                let mut value = serde_json::to_value(receipt).map_err(json_error)?;
                value["operation"] = json!("cooldis.thread_spawn");
                value
            }
            THREAD_SUBMIT_OPERATION => {
                let args: ThreadSubmitArgs = decode_args(operation_name, arguments)?;
                let target_thread_id = parse_thread_id(&args.target_thread_id, "target_thread_id")?;
                let mut value = serde_json::to_value(
                    self.control
                        .submit_to_thread(
                            &self.caller,
                            target_thread_id,
                            None,
                            TurnInput::text(args.message),
                        )
                        .await?,
                )
                .map_err(json_error)?;
                value["operation"] = json!("cooldis.thread_submit");
                value
            }
            THREAD_WAIT_OPERATION => {
                let args: ThreadWaitArgs = decode_args(operation_name, arguments)?;
                let target_thread_id = parse_thread_id(&args.target_thread_id, "target_thread_id")?;
                let mut value = serde_json::to_value(
                    self.control
                        .wait_thread(&self.caller, target_thread_id, args.timeout_ms)
                        .await?,
                )
                .map_err(json_error)?;
                value["operation"] = json!("cooldis.thread_wait");
                value
            }
            THREAD_STATUS_OPERATION => {
                let args: ThreadStatusArgs = decode_args(operation_name, arguments)?;
                let target_thread_id = optional_target_thread_id(
                    &self.caller,
                    args.target_thread_id.as_deref(),
                    "target_thread_id",
                )?;
                let mut value = serde_json::to_value(
                    self.control
                        .thread_status(&self.caller, target_thread_id)
                        .await?,
                )
                .map_err(json_error)?;
                let children = self
                    .control
                    .children_of(&self.caller, target_thread_id)
                    .await?;
                value["operation"] = json!("cooldis.thread_status");
                value["children"] = serde_json::to_value(children.children).map_err(json_error)?;
                value
            }
            THREAD_CANCEL_OPERATION => {
                let args: ThreadCancelArgs = decode_args(operation_name, arguments)?;
                let target_thread_id = parse_thread_id(&args.target_thread_id, "target_thread_id")?;
                let mut value = serde_json::to_value(
                    self.control
                        .cancel_thread(
                            &self.caller,
                            target_thread_id,
                            "thread_cancel operation".to_string(),
                        )
                        .await?,
                )
                .map_err(json_error)?;
                value["operation"] = json!("cooldis.thread_cancel");
                value
            }
            _ => {
                return Err(CooldisError::RuntimeExecution(format!(
                    "unknown kernel operation {COOLDIS_THREADS_PACKAGE}/{operation_name}"
                )));
            }
        };
        Ok(value)
    }
}

#[derive(Clone)]
pub struct KernelScheduleOperationProvider {
    control: RuntimeKernelControl,
    caller: ThreadContext,
}

impl KernelScheduleOperationProvider {
    pub fn new(control: RuntimeKernelControl, caller: ThreadContext) -> Self {
        Self { control, caller }
    }

    async fn invoke_json(&self, operation_name: &str, arguments: Value) -> CooldisResult<Value> {
        let value = match operation_name {
            MANDATE_START_OPERATION => {
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
                        MandateStartRequest {
                            schedule: args.schedule,
                            max_occurrences: args.max_occurrences,
                            catch_up: args.catch_up,
                            input_template: args.input_template,
                            snapshot_id: None,
                        },
                    )
                    .await?;
                json!({
                    "operation": "cooldis.mandate_start",
                    "status": "started",
                    "thread_id": target_thread_id.to_string(),
                    "mandate_event_id": receipt.event.id.to_string(),
                    "stream_id": receipt.event.stream_id.as_str(),
                    "sequence": receipt.event.sequence.get(),
                })
            }
            MANDATE_REVOKE_OPERATION => {
                let args: MandateRevokeArgs = decode_schedule_args(operation_name, arguments)?;
                let target_thread_id = optional_target_thread_id(
                    &self.caller,
                    args.thread_id.as_deref(),
                    "thread_id",
                )?;
                let mandate_event_id = parse_mandate_event_id(&args.mandate_event_id)?;
                let receipt = self
                    .control
                    .revoke_mandate(&self.caller, target_thread_id, mandate_event_id)
                    .await?;
                json!({
                    "operation": "cooldis.mandate_revoke",
                    "status": receipt.status.as_str(),
                    "thread_id": target_thread_id.to_string(),
                    "mandate_event_id": mandate_event_id.to_string(),
                    "revoked_event_id": receipt.revoke_event.id.to_string(),
                })
            }
            MANDATE_LIST_OPERATION => {
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
                json!({
                    "operation": "cooldis.mandate_list",
                    "thread_id": target_thread_id.to_string(),
                    "mandates": mandates,
                })
            }
            _ => {
                return Err(CooldisError::RuntimeExecution(format!(
                    "unknown kernel operation {COOLDIS_SCHEDULE_PACKAGE}/{operation_name}"
                )));
            }
        };
        Ok(value)
    }
}

#[derive(Clone)]
pub struct KernelProcessOperationProvider {
    caller: ThreadContext,
    process_manager: AsyncExecutionManager,
    live_backend: Arc<dyn LiveProcessBackend>,
    default_cwd: PathBuf,
    default_output_cap_bytes: usize,
}

impl KernelProcessOperationProvider {
    pub fn new(caller: ThreadContext, default_cwd: impl Into<PathBuf>) -> Self {
        Self {
            caller,
            process_manager: AsyncExecutionManager::default(),
            live_backend: Arc::new(HostBashLiveBackend),
            default_cwd: default_cwd.into(),
            default_output_cap_bytes: DEFAULT_PROCESS_OUTPUT_CAP_BYTES,
        }
    }

    pub fn with_process_manager(mut self, process_manager: AsyncExecutionManager) -> Self {
        self.process_manager = process_manager;
        self
    }

    pub fn with_backend(mut self, backend: Arc<dyn LiveProcessBackend>) -> Self {
        self.live_backend = backend;
        self
    }

    async fn invoke_json(&self, operation_name: &str, arguments: Value) -> CooldisResult<Value> {
        let value = match operation_name {
            PROCESS_EXEC_OPERATION => {
                let args: ProcessExecArgs = decode_process_args(operation_name, arguments)?;
                if args.command.is_empty() {
                    return Err(CooldisError::RuntimeExecution(format!(
                        "operation {COOLDIS_PROCESS_PACKAGE}/{operation_name} requires a non-empty command argv"
                    )));
                }
                let default_cwd = self.effective_default_cwd();
                let cwd = resolve_process_cwd(&default_cwd, args.cwd.as_deref());
                let env = args
                    .env
                    .into_iter()
                    .map(|(key, value)| (key, Some(value)))
                    .collect::<BTreeMap<_, _>>();
                let timeout =
                    Duration::from_millis(args.timeout_ms.unwrap_or(DEFAULT_PROCESS_TIMEOUT_MS));
                let output_cap =
                    process_output_cap(args.output_bytes_cap, self.default_output_cap_bytes);
                let yield_time = process_yield_time(args.yield_time_ms);
                let request = AsyncProcessStartRequest::host_command(args.command, cwd)
                    .with_owner(self.process_owner("kernel-operation:cooldis-process/process_exec"))
                    .with_env(env)
                    .pipe_stdin(args.stream_stdin)
                    .with_deadline(ExecutionDeadline::from_now(timeout))
                    .with_yield_time(yield_time)
                    .with_output_cap_bytes(output_cap);
                let outcome = self
                    .process_manager
                    .start(Arc::clone(&self.live_backend), request)
                    .await?;
                process_snapshot_output_json("cooldis.process_exec", &outcome.snapshot)
            }
            PROCESS_POLL_OPERATION => {
                let args: ProcessHandleArgs = decode_process_args(operation_name, arguments)?;
                let process_id = parse_process_id(&args.process_id, "process_id")?;
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
            PROCESS_WRITE_OPERATION => {
                let args: ProcessWriteArgs = decode_process_args(operation_name, arguments)?;
                let process_id = parse_process_id(&args.process_id, "process_id")?;
                let bytes = STANDARD.decode(args.delta_base64).map_err(|err| {
                    CooldisError::RuntimeExecution(format!(
                        "operation {COOLDIS_PROCESS_PACKAGE}/{operation_name} requires valid base64 delta_base64: {err}"
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
            PROCESS_TERMINATE_OPERATION => {
                let args: ProcessTerminateArgs = decode_process_args(operation_name, arguments)?;
                let process_id = parse_process_id(&args.process_id, "process_id")?;
                let outcome = self
                    .process_manager
                    .terminate(
                        process_id,
                        args.reason
                            .unwrap_or_else(|| "cooldis-process terminate requested".to_string()),
                        process_yield_time(args.yield_time_ms),
                        self.default_output_cap_bytes,
                    )
                    .await?;
                process_snapshot_output_json("cooldis.process_terminate", &outcome.snapshot)
            }
            _ => {
                return Err(CooldisError::RuntimeExecution(format!(
                    "unknown kernel operation {COOLDIS_PROCESS_PACKAGE}/{operation_name}"
                )));
            }
        };
        Ok(value)
    }

    fn process_owner(&self, surface: &str) -> AsyncProcessOwner {
        AsyncProcessOwner {
            thread_id: Some(self.caller.coordinates.thread_id.to_string()),
            turn_id: None,
            call_id: None,
            surface: Some(surface.to_string()),
        }
    }

    fn effective_default_cwd(&self) -> PathBuf {
        self.caller
            .metadata
            .get(APP_SERVER_CWD_METADATA)
            .filter(|cwd| !cwd.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| self.default_cwd.clone())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KernelNotifyOperationProvider;

impl KernelNotifyOperationProvider {
    async fn invoke_json(&self, operation_name: &str, arguments: Value) -> CooldisResult<Value> {
        let value = match operation_name {
            NOTIFY_PREVIEW_OPERATION => {
                let args: NotifyPreviewArgs = decode_notify_args(operation_name, arguments)?;
                let NotifyPreviewArgs {
                    channel,
                    subject,
                    body,
                    severity,
                } = args;
                require_non_empty(&channel, "channel")?;
                require_non_empty(&body, "body")?;
                let mut value = json!({
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
                    value["subject"] = json!(subject);
                }
                value
            }
            CHANNEL_EMIT_OPERATION => {
                let args: ChannelEmitArgs = decode_notify_args(operation_name, arguments)?;
                let ChannelEmitArgs {
                    channel,
                    message,
                    thread_id,
                } = args;
                require_non_empty(&channel, "channel")?;
                require_non_empty(&message, "message")?;
                let mut value = json!({
                    "operation": "cooldis.channel_emit",
                    "status": "recorded",
                    "delivery": "not_sent",
                    "channel": channel,
                    "message": message,
                    "channel_decision_required": true,
                    "reason": "V1 records channel egress intent; channel-specific delivery adapters are explicit operations."
                });
                if let Some(thread_id) = thread_id {
                    value["thread_id"] = json!(thread_id);
                }
                value
            }
            _ => {
                return Err(CooldisError::RuntimeExecution(format!(
                    "unknown kernel operation {COOLDIS_NOTIFY_PACKAGE}/{operation_name}"
                )));
            }
        };
        Ok(value)
    }
}

#[derive(Clone, Debug)]
pub struct KernelThreadSpawnAgentBinding {
    pub metadata: BTreeMap<String, String>,
    pub compile_receipt: Value,
    pub bind_receipt: Value,
}

#[async_trait]
pub trait KernelThreadSpawnAgentResolver: Send + Sync {
    async fn resolve_agent_ref(
        &self,
        caller: &ThreadContext,
        agent_ref: &str,
    ) -> CooldisResult<KernelThreadSpawnAgentBinding>;
}

#[async_trait]
impl KernelOperationDispatcher for KernelThreadOperationProvider {
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> cooldis_operations::CooldisResult<Vec<u8>> {
        let arguments: Value = serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let value = self
            .invoke_json(operation_name, arguments)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }
}

#[async_trait]
impl KernelOperationDispatcher for KernelScheduleOperationProvider {
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> cooldis_operations::CooldisResult<Vec<u8>> {
        let arguments: Value = serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let value = self
            .invoke_json(operation_name, arguments)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }
}

#[async_trait]
impl KernelOperationDispatcher for KernelProcessOperationProvider {
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> cooldis_operations::CooldisResult<Vec<u8>> {
        let arguments: Value = serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let value = self
            .invoke_json(operation_name, arguments)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }
}

#[async_trait]
impl KernelOperationDispatcher for KernelNotifyOperationProvider {
    async fn invoke_kernel_operation(
        &self,
        operation_name: &str,
        input: Vec<u8>,
    ) -> cooldis_operations::CooldisResult<Vec<u8>> {
        let arguments: Value = serde_json::from_slice(&input).map_err(operations_runtime_error)?;
        let value = self
            .invoke_json(operation_name, arguments)
            .await
            .map_err(operations_runtime_error)?;
        serde_json::to_vec(&value).map_err(operations_runtime_error)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadSpawnArgs {
    task_name: String,
    message: String,
    #[serde(default)]
    agent_ref: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadSubmitArgs {
    target_thread_id: String,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadWaitArgs {
    target_thread_id: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadStatusArgs {
    #[serde(default)]
    target_thread_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadCancelArgs {
    target_thread_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MandateStartArgs {
    #[serde(default)]
    thread_id: Option<String>,
    schedule: MandateSchedulePayload,
    #[serde(default)]
    max_occurrences: Option<u32>,
    #[serde(default)]
    catch_up: Option<MandateCatchUpPolicy>,
    #[serde(default)]
    input_template: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MandateRevokeArgs {
    #[serde(default)]
    thread_id: Option<String>,
    mandate_event_id: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct MandateListArgs {
    #[serde(default)]
    thread_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessExecArgs {
    command: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    stream_stdin: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    yield_time_ms: Option<u64>,
    #[serde(default)]
    output_bytes_cap: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessHandleArgs {
    process_id: String,
    #[serde(default)]
    yield_time_ms: Option<u64>,
    #[serde(default)]
    output_bytes_cap: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessWriteArgs {
    process_id: String,
    delta_base64: String,
    #[serde(default)]
    yield_time_ms: Option<u64>,
    #[serde(default)]
    output_bytes_cap: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessTerminateArgs {
    process_id: String,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    yield_time_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotifyPreviewArgs {
    channel: String,
    #[serde(default)]
    subject: Option<String>,
    body: String,
    #[serde(default)]
    severity: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelEmitArgs {
    channel: String,
    message: String,
    #[serde(default)]
    thread_id: Option<String>,
}

fn decode_args<T: DeserializeOwned>(operation_name: &str, arguments: Value) -> CooldisResult<T> {
    serde_json::from_value(arguments).map_err(|err| {
        CooldisError::RuntimeExecution(format!(
            "operation {COOLDIS_THREADS_PACKAGE}/{operation_name} has invalid arguments: {err}"
        ))
    })
}

fn decode_schedule_args<T: DeserializeOwned>(
    operation_name: &str,
    arguments: Value,
) -> CooldisResult<T> {
    serde_json::from_value(arguments).map_err(|err| {
        CooldisError::RuntimeExecution(format!(
            "operation {COOLDIS_SCHEDULE_PACKAGE}/{operation_name} has invalid arguments: {err}"
        ))
    })
}

fn decode_process_args<T: DeserializeOwned>(
    operation_name: &str,
    arguments: Value,
) -> CooldisResult<T> {
    serde_json::from_value(arguments).map_err(|err| {
        CooldisError::RuntimeExecution(format!(
            "operation {COOLDIS_PROCESS_PACKAGE}/{operation_name} has invalid arguments: {err}"
        ))
    })
}

fn decode_notify_args<T: DeserializeOwned>(
    operation_name: &str,
    arguments: Value,
) -> CooldisResult<T> {
    serde_json::from_value(arguments).map_err(|err| {
        CooldisError::RuntimeExecution(format!(
            "operation {COOLDIS_NOTIFY_PACKAGE}/{operation_name} has invalid arguments: {err}"
        ))
    })
}

fn require_non_empty(value: &str, field: &str) -> CooldisResult<()> {
    if value.trim().is_empty() {
        return Err(CooldisError::RuntimeExecution(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn optional_target_thread_id(
    caller: &ThreadContext,
    value: Option<&str>,
    field: &str,
) -> CooldisResult<ThreadId> {
    match value {
        Some(value) => parse_thread_id(value, field),
        None => Ok(caller.coordinates.thread_id),
    }
}

fn parse_thread_id(value: &str, field: &str) -> CooldisResult<ThreadId> {
    ThreadId::parse_str(value).map_err(|err| {
        CooldisError::RuntimeExecution(format!("{field} is not a valid Cooldis thread id: {err}"))
    })
}

fn parse_process_id(value: &str, field: &str) -> CooldisResult<CooldisProcessId> {
    value.parse::<CooldisProcessId>().map_err(|err| {
        CooldisError::RuntimeExecution(format!("{field} is not a valid Cooldis process id: {err}"))
    })
}

fn resolve_process_cwd(default_cwd: &Path, cwd: Option<&str>) -> PathBuf {
    match cwd {
        Some(cwd) if !cwd.trim().is_empty() => {
            let path = PathBuf::from(cwd);
            if path.is_absolute() {
                path
            } else {
                default_cwd.join(path)
            }
        }
        _ => default_cwd.to_path_buf(),
    }
}

fn process_yield_time(yield_time_ms: Option<u64>) -> Duration {
    Duration::from_millis(
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

fn process_snapshot_output_json(operation: &str, snapshot: &AsyncProcessSnapshot) -> Value {
    let mut value = json!({
        "operation": operation,
        "status": snapshot.status.as_str(),
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
        value["process_id"] = json!(process_id.to_string());
    }
    if let Some(exit_code) = snapshot.exit_code {
        value["exit_code"] = json!(exit_code);
    }
    value
}

fn active_mandate_json(mandate: &ActiveMandate) -> Value {
    json!({
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

fn json_error(err: serde_json::Error) -> CooldisError {
    CooldisError::RuntimeExecution(err.to_string())
}

fn operations_runtime_error(
    err: impl std::fmt::Display,
) -> cooldis_operations::CooldisOperationsError {
    cooldis_operations::CooldisOperationsError::RuntimeExecution(err.to_string())
}

#[cfg(test)]
mod tests;
