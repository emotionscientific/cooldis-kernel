//! The kernel's native agent loop: thread stream -> model -> thread stream.
//!
//! A tool round ends only after the assistant entry and every tool result have
//! been persisted. Before polling the next model-request assembly, the loop
//! gives ready thread commands priority. The next assembly then folds persisted
//! steer inputs admitted since the active turn began that are absent from its
//! prior `context.compile.completed` receipts and injects them as user-role hook
//! context. The compile receipt for that request is the durable delivery
//! witness; a steer that misses the final tool boundary remains ordinary
//! persisted history for the next turn.
//!
//! Cancellation exposure is explicit: draining a ready steer awaits the
//! existing idempotent turn-input append (an ingress-backed steer cannot settle
//! its claim until that entry exists), and the delivery fold adds one read-only
//! event-store await. Cancellation or a crash before the compile receipt leaves
//! the entry eligible for a later boundary; once the receipt commits, replay
//! uses that witness and the persisted user entry remains in ordinary request
//! history.

use futures_util::FutureExt as _;
use futures_util::StreamExt as _;

const MAX_TOOL_ROUTER_ROUNDS: usize = 8;
const DETACHED_COMPLETION_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(250);
const TOOL_INVOCATION_AWAITED: u8 = 0;
const TOOL_INVOCATION_ABANDONED: u8 = 1;
const TOOL_INVOCATION_SETTLED: u8 = 2;
pub(crate) const THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA: &str =
    "cooldis.agent.runtime.max_tool_rounds";
const HOOK_MUTATION_WITNESS_OBSERVATION_KIND: &str = "host.hook.mutation_witnessed";
const HOOK_MUTATION_WITNESS_OBSERVATION_SCHEMA_V1: &str =
    "cooldis.observation.host_hook_mutation/1";

fn reattach_late_tool_result_entries(
    entries: Vec<verlet_history::SessionEntry>,
) -> Vec<verlet_history::SessionEntry> {
    let mut issuer_by_call_id = std::collections::HashMap::<String, Option<usize>>::new();
    for (index, entry) in entries.iter().enumerate() {
        let verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::Assistant { content, .. },
        } = &entry.kind
        else {
            continue;
        };
        for block in content {
            let verlet_history::CanonicalContent::ToolCall { id, .. } = block else {
                continue;
            };
            issuer_by_call_id
                .entry(id.clone())
                .and_modify(|issuer| *issuer = None)
                .or_insert(Some(index));
        }
    }

    let mut results_before = std::collections::BTreeMap::<usize, Vec<usize>>::new();
    let mut moved = std::collections::HashSet::new();
    for (result_index, entry) in entries.iter().enumerate() {
        let verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. },
        } = &entry.kind
        else {
            continue;
        };
        let Some(Some(issuer_index)) = issuer_by_call_id.get(tool_call_id).copied() else {
            continue;
        };
        if issuer_index >= result_index
            || entries[issuer_index + 1..result_index].iter().any(|entry| {
                matches!(
                    &entry.kind,
                    verlet_history::SessionEntryKind::Compaction { .. }
                )
            })
        {
            continue;
        }
        let Some(turn_input_index) = (issuer_index + 1..result_index).find(|&index| {
            entries[index].turn_id.is_some()
                && matches!(
                    &entries[index].kind,
                    verlet_history::SessionEntryKind::Message {
                        message: verlet_history::CanonicalMessage::User { .. }
                    }
                )
        }) else {
            continue;
        };
        results_before
            .entry(turn_input_index)
            .or_default()
            .push(result_index);
        moved.insert(result_index);
    }
    if moved.is_empty() {
        return entries;
    }

    let mut output = Vec::with_capacity(entries.len());
    for index in 0..entries.len() {
        if let Some(result_indices) = results_before.remove(&index) {
            for result_index in result_indices {
                output.push(entries[result_index].clone());
            }
        }
        if !moved.contains(&index) {
            output.push(entries[index].clone());
        }
    }
    output
}

fn default_process_dispatcher_cwd() -> std::path::PathBuf {
    std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentLoopConfig {
    pub provider: String,
    pub api: verlet_history::ProviderApi,
    pub model: String,
    #[serde(default)]
    pub system: Vec<verlet_provider::SystemBlock>,
    #[serde(default)]
    pub tools: Vec<verlet_provider::ToolDefinition>,
    pub max_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<verlet_provider::ThinkingConfig>,
    #[serde(default)]
    pub stream: bool,
}

impl AgentLoopConfig {
    pub fn new(
        api: verlet_history::ProviderApi,
        provider: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            api,
            model: model.into(),
            system: Vec::new(),
            tools: Vec::new(),
            max_tokens: 1024,
            temperature: None,
            thinking: None,
            stream: false,
        }
    }

    fn request_from_messages(
        &self,
        messages: Vec<verlet_history::CanonicalMessage>,
    ) -> verlet_provider::ProviderRequest {
        verlet_provider::ProviderRequest {
            api: self.api.clone(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            system: self.system.clone(),
            messages,
            tools: self.tools.clone(),
            max_tokens: self.max_tokens,
            temperature: self.temperature,
            thinking: self.thinking.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModelRequestRetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl ModelRequestRetryPolicy {
    pub fn disabled() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        }
    }

    pub fn fixed(max_attempts: u32, delay_ms: u64) -> Self {
        Self {
            max_attempts,
            initial_backoff_ms: delay_ms,
            max_backoff_ms: delay_ms,
        }
    }

    fn attempts(self) -> u32 {
        self.max_attempts.max(1)
    }

    fn delay_ms(self, attempt: u32) -> u64 {
        let base = self.initial_backoff_ms;
        if base == 0 {
            return 0;
        }
        let exponent = attempt.saturating_sub(1).min(31);
        let factor = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let delay = base.saturating_mul(factor);
        if self.max_backoff_ms == 0 {
            delay
        } else {
            delay.min(self.max_backoff_ms)
        }
    }
}

impl Default for ModelRequestRetryPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Clone)]
struct ModelRequestEndpoint {
    config: AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
}

#[derive(Clone)]
pub struct AgentLoopFactory {
    config: AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    model_request_retry_policy: ModelRequestRetryPolicy,
    model_request_fallbacks: Vec<ModelRequestEndpoint>,
    tool_router: Option<std::sync::Arc<crate::agent::agent_tool_router::AgentToolRouter>>,
    bash_tool_config: Option<crate::capabilities::execution::VirtualBashRuntimeConfig>,
    thread_spawn_agent_resolver:
        Option<std::sync::Arc<dyn crate::agent::agent_process::KernelThreadSpawnAgentResolver>>,
    hook_pipeline: Option<std::sync::Arc<crate::agent::hooks::HookPipeline>>,
    tool_permission_gate: std::sync::Arc<dyn crate::agent::tool_interceptor::ToolPermissionGate>,
    context_compile_policy: crate::kernel::context_compiler::AgentContextCompilePolicy,
    compaction_policy: crate::kernel::compaction::CompactionPolicy,
    max_tool_rounds: Option<usize>,
}

impl AgentLoopFactory {
    pub fn new(
        config: AgentLoopConfig,
        client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    ) -> Self {
        Self {
            config,
            client,
            model_request_retry_policy: ModelRequestRetryPolicy::default(),
            model_request_fallbacks: Vec::new(),
            tool_router: None,
            bash_tool_config: None,
            thread_spawn_agent_resolver: None,
            hook_pipeline: None,
            tool_permission_gate: std::sync::Arc::new(
                crate::agent::tool_interceptor::AllowAllToolPermissionGate,
            ),
            context_compile_policy:
                crate::kernel::context_compiler::AgentContextCompilePolicy::unbounded(),
            compaction_policy: crate::kernel::compaction::CompactionPolicy::disabled(),
            max_tool_rounds: Some(MAX_TOOL_ROUTER_ROUNDS),
        }
    }

    pub fn with_model_request_retry_policy(mut self, policy: ModelRequestRetryPolicy) -> Self {
        self.model_request_retry_policy = policy;
        self
    }

    pub fn with_model_request_fallback(
        mut self,
        config: AgentLoopConfig,
        client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    ) -> Self {
        self.model_request_fallbacks
            .push(ModelRequestEndpoint { config, client });
        self
    }

    pub fn with_tool_router(
        mut self,
        tool_router: std::sync::Arc<crate::agent::agent_tool_router::AgentToolRouter>,
    ) -> Self {
        self.tool_router = Some(tool_router);
        self
    }

    pub fn with_operation_registry(
        mut self,
        operation_registry: std::sync::Arc<
            verlet_operations::operation_registry::OperationRegistry,
        >,
    ) -> Self {
        self.tool_router = Some(std::sync::Arc::new(
            crate::agent::agent_tool_router::AgentToolRouter::new(operation_registry),
        ));
        self
    }

    pub fn with_bash_tool(
        mut self,
        config: crate::capabilities::execution::VirtualBashRuntimeConfig,
    ) -> Self {
        self.bash_tool_config = Some(config);
        self
    }

    pub fn with_thread_spawn_agent_resolver(
        mut self,
        resolver: std::sync::Arc<dyn crate::agent::agent_process::KernelThreadSpawnAgentResolver>,
    ) -> Self {
        self.thread_spawn_agent_resolver = Some(resolver);
        self
    }

    // lexicon-allow: hook - existing host debug hook API name retained for compatibility.
    pub fn with_hook_pipeline(
        mut self,
        hook_pipeline: std::sync::Arc<crate::agent::hooks::HookPipeline>,
    ) -> Self {
        self.hook_pipeline = Some(hook_pipeline);
        self
    }

    pub fn with_tool_permission_gate(
        mut self,
        tool_permission_gate: std::sync::Arc<
            dyn crate::agent::tool_interceptor::ToolPermissionGate,
        >,
    ) -> Self {
        self.tool_permission_gate = tool_permission_gate;
        self
    }

    pub fn with_context_compile_policy(
        mut self,
        policy: crate::kernel::context_compiler::AgentContextCompilePolicy,
    ) -> Self {
        self.context_compile_policy = policy;
        self
    }

    pub fn with_compaction_policy(
        mut self,
        policy: crate::kernel::compaction::CompactionPolicy,
    ) -> Self {
        self.compaction_policy = policy;
        self
    }
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory for AgentLoopFactory {
    async fn build(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> crate::kernel::runtime_host::VerletResult<
        Box<dyn crate::kernel::runtime_host::runtime_api::AgentRuntime>,
    > {
        let max_tool_rounds = match context
            .metadata
            .get(THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA)
        {
            Some(value) if value == "unlimited" => None,
            Some(value) => {
                let rounds = value.parse::<usize>().map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "manifest runtime max_tool_rounds metadata is invalid: {err}"
                    ))
                })?;
                if rounds == 0 {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                        "manifest runtime max_tool_rounds metadata must be > 0 or \"unlimited\""
                            .to_string(),
                    ));
                }
                Some(rounds)
            }
            None => self.max_tool_rounds,
        };
        Ok(Box::new(AgentLoop {
            config: self.config.clone(),
            client: std::sync::Arc::clone(&self.client),
            model_request_retry_policy: self.model_request_retry_policy,
            model_request_fallbacks: self.model_request_fallbacks.clone(),
            tool_router: self.tool_router.clone(),
            bash_tool_config: self.bash_tool_config.clone(),
            thread_spawn_agent_resolver: self.thread_spawn_agent_resolver.clone(),
            hook_pipeline: self.hook_pipeline.clone(),
            tool_permission_gate: std::sync::Arc::clone(&self.tool_permission_gate),
            context_compile_policy: self.context_compile_policy.clone(),
            compaction_policy: self.compaction_policy.clone(),
            max_tool_rounds,
            strict_tool_router_unknowns: self.tool_router.is_some()
                || self.bash_tool_config.is_some(),
        }))
    }
}

struct AgentLoop {
    config: AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    model_request_retry_policy: ModelRequestRetryPolicy,
    model_request_fallbacks: Vec<ModelRequestEndpoint>,
    tool_router: Option<std::sync::Arc<crate::agent::agent_tool_router::AgentToolRouter>>,
    bash_tool_config: Option<crate::capabilities::execution::VirtualBashRuntimeConfig>,
    thread_spawn_agent_resolver:
        Option<std::sync::Arc<dyn crate::agent::agent_process::KernelThreadSpawnAgentResolver>>,
    hook_pipeline: Option<std::sync::Arc<crate::agent::hooks::HookPipeline>>,
    tool_permission_gate: std::sync::Arc<dyn crate::agent::tool_interceptor::ToolPermissionGate>,
    context_compile_policy: crate::kernel::context_compiler::AgentContextCompilePolicy,
    compaction_policy: crate::kernel::compaction::CompactionPolicy,
    max_tool_rounds: Option<usize>,
    strict_tool_router_unknowns: bool,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::AgentRuntime for AgentLoop {
    async fn run(
        self: Box<Self>,
        context: verlet_runtime_contracts::ThreadContext,
        services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<
            crate::kernel::runtime_host::runtime_api::ThreadCommand,
        >,
        events: tokio::sync::broadcast::Sender<
            crate::kernel::runtime_host::runtime_api::ThreadEvent,
        >,
        status: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let mut runtime = *self;
        runtime.mount_agent_process_tools(&context, &services).await;
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            &events,
            &coordinates,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
                metadata: context.metadata.clone(),
            },
        );
        let _ = events.send(
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Started {
                context: context.clone(),
            },
        );
        if let Err(err) =
            sweep_cancelled_turn_tool_calls(&runtime, &context, &services, thread_id, &events).await
        {
            fail_provider_turn(
                &coordinates,
                thread_id,
                &events,
                &status,
                "tool_cancellation_recovery",
                err.to_string(),
            );
            return;
        }
        match runtime
            .run_session_start_hooks(&context, &services, thread_id, &events)
            .await
        {
            Ok(should_stop) => {
                if should_stop {
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        &events,
                        &coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                            state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                        },
                    );
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                    let _ = events.send(
                        crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped {
                            thread_id,
                        },
                    );
                    return;
                }
            }
            Err(err) => {
                fail_provider_turn(
                    &coordinates,
                    thread_id,
                    &events,
                    &status,
                    "hook_pipeline",
                    err.to_string(),
                );
                return;
            }
        }
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        let mut pending_commands = std::collections::VecDeque::new();

        loop {
            if let Some(command) = pending_commands.pop_front() {
                if run_idle_provider_command(
                    &runtime,
                    &context,
                    command,
                    &coordinates,
                    &services,
                    thread_id,
                    &events,
                    &status,
                    &mut commands,
                    &cancellation,
                    &mut pending_commands,
                )
                .await
                {
                    break;
                }
                continue;
            }

            tokio::select! {
                _ = cancellation.cancelled() => {
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        &events,
                        &coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                            state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                        },
                    );
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                    let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    let Some(command) = command else {
                        let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                            thread_id,
                            signal: verlet_runtime_contracts::ThreadSignal::shutdown(&coordinates),
                        });
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            &events,
                            &coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                                state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                            },
                        );
                        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                        let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
                        break;
                    };
                    if run_idle_provider_command(
                        &runtime,
                        &context,
                        command,
                        &coordinates,
                        &services,
                        thread_id,
                        &events,
                        &status,
                        &mut commands,
                        &cancellation,
                        &mut pending_commands,
                    )
                    .await
                    {
                        break;
                    }
                }
            }
        }
    }
}

impl AgentLoop {
    async fn mount_agent_process_tools(
        &mut self,
        context: &verlet_runtime_contracts::ThreadContext,
        services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    ) {
        let control = services.kernel_control();
        if self.tool_router.is_none() && self.bash_tool_config.is_none() {
            return;
        }
        let had_explicit_router = self.tool_router.is_some();
        let mut router = self
            .tool_router
            .as_ref()
            .map(|router| router.as_ref().clone())
            .unwrap_or_else(|| {
                crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
                    verlet_operations::operation_registry::OperationRegistry::new(),
                ))
            });
        if !had_explicit_router && self.bash_tool_config.is_none() {
            self.strict_tool_router_unknowns = false;
        }
        if let Some(control) = control.clone() {
            let mut provider = crate::agent::agent_process::KernelThreadOperationProvider::new(
                control.clone(),
                context.clone(),
            );
            if let Some(resolver) = &self.thread_spawn_agent_resolver {
                provider = provider.with_agent_resolver(std::sync::Arc::clone(resolver));
            }
            let dispatcher: std::sync::Arc<
                dyn verlet_operations::operation_registry::KernelOperationDispatcher,
            > = std::sync::Arc::new(provider);
            let _ = router
                .operation_registry()
                .set_kernel_dispatcher(
                    crate::operations::kernel_packages::VERLET_THREADS_PACKAGE,
                    std::sync::Arc::clone(&dispatcher),
                )
                .await;
            if let Some(config) = &self.bash_tool_config
                && let Some(registry) = &config.operation_registry
            {
                let _ = registry
                    .set_kernel_dispatcher(
                        crate::operations::kernel_packages::VERLET_THREADS_PACKAGE,
                        std::sync::Arc::clone(&dispatcher),
                    )
                    .await;
            }
            let schedule_dispatcher: std::sync::Arc<
                dyn verlet_operations::operation_registry::KernelOperationDispatcher,
            > = std::sync::Arc::new(
                crate::agent::agent_process::KernelScheduleOperationProvider::new(
                    control.clone(),
                    context.clone(),
                ),
            );
            let _ = router
                .operation_registry()
                .set_kernel_dispatcher(
                    crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE,
                    std::sync::Arc::clone(&schedule_dispatcher),
                )
                .await;
            if let Some(config) = &self.bash_tool_config
                && let Some(registry) = &config.operation_registry
            {
                let _ = registry
                    .set_kernel_dispatcher(
                        crate::operations::kernel_packages::VERLET_SCHEDULE_PACKAGE,
                        std::sync::Arc::clone(&schedule_dispatcher),
                    )
                    .await;
            }
        }
        let process_cwd = self
            .bash_tool_config
            .as_ref()
            .map(|config| config.cwd.clone())
            .unwrap_or_else(default_process_dispatcher_cwd);
        let process_handle_dispatcher = services.process_handle_dispatcher().or_else(|| {
            services.process_handle_ingress().map(|ingress| {
                crate::kernel::process_handle_dispatch::ProcessHandleDispatcher::new(
                    services.runtime_store(),
                    ingress,
                )
            })
        });
        let mut process_provider = crate::agent::agent_process::KernelProcessOperationProvider::new(
            context.clone(),
            process_cwd,
        );
        if let Some(dispatcher) = process_handle_dispatcher.clone() {
            process_provider = process_provider.with_process_dispatcher(dispatcher);
        }
        let process_dispatcher: std::sync::Arc<
            dyn verlet_operations::operation_registry::KernelOperationDispatcher,
        > = std::sync::Arc::new(process_provider);
        let _ = router
            .operation_registry()
            .set_kernel_dispatcher(
                crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE,
                std::sync::Arc::clone(&process_dispatcher),
            )
            .await;
        if let Some(config) = &self.bash_tool_config
            && let Some(registry) = &config.operation_registry
        {
            let _ = registry
                .set_kernel_dispatcher(
                    crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE,
                    std::sync::Arc::clone(&process_dispatcher),
                )
                .await;
        }
        let notify_dispatcher: std::sync::Arc<
            dyn verlet_operations::operation_registry::KernelOperationDispatcher,
        > = std::sync::Arc::new(crate::agent::agent_process::KernelNotifyOperationProvider);
        let _ = router
            .operation_registry()
            .set_kernel_dispatcher(
                crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE,
                std::sync::Arc::clone(&notify_dispatcher),
            )
            .await;
        if let Some(config) = &self.bash_tool_config
            && let Some(registry) = &config.operation_registry
        {
            let _ = registry
                .set_kernel_dispatcher(
                    crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE,
                    std::sync::Arc::clone(&notify_dispatcher),
                )
                .await;
        }
        if let Some(config) = &self.bash_tool_config {
            let mut provider =
                crate::capabilities::execution::BashToolProvider::new(config.clone());
            if let Some(dispatcher) = process_handle_dispatcher {
                provider = provider.with_process_dispatcher(dispatcher);
            }
            router = router.with_kernel_tool_provider(std::sync::Arc::new(provider));
        }
        self.tool_router = Some(std::sync::Arc::new(router));
    }

    async fn run_session_start_hooks(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
        services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
        thread_id: verlet_runtime_contracts::ThreadId,
        events: &tokio::sync::broadcast::Sender<
            crate::kernel::runtime_host::runtime_api::ThreadEvent,
        >,
    ) -> crate::kernel::runtime_host::VerletResult<bool> {
        let Some(hook_pipeline) = &self.hook_pipeline else {
            return Ok(false);
        };
        let coordinates = &context.coordinates;
        let outcome = hook_pipeline
            .run_session_start(
                crate::agent::hooks::SessionStartHookRequest {
                    coordinates: coordinates.clone(),
                    parent_thread_id: context.parent_thread_id,
                    source: "startup".to_string(),
                    cwd: None,
                    provider: self.config.provider.clone(),
                    model: self.config.model.clone(),
                    permission_profile: None,
                },
                |spec| emit_hook_started(events, coordinates, spec),
            )
            .await;
        emit_hook_records(events, coordinates, &outcome.records);
        append_hook_mutation_witnesses(services, coordinates, outcome.mutation_witnesses).await?;
        append_hook_contexts(
            services,
            coordinates,
            thread_id,
            events,
            outcome.additional_contexts,
        )
        .await?;
        Ok(outcome.should_stop)
    }

    fn turn_context(
        &self,
        thread_context: &verlet_runtime_contracts::ThreadContext,
        turn_id: String,
        input: &crate::kernel::runtime_host::turn::TurnInput,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> crate::kernel::runtime_host::turn::TurnContext {
        crate::kernel::runtime_host::turn::TurnContext::new(
            thread_context.clone(),
            turn_id,
            input,
            cancellation,
        )
        .with_effective_model_provider(self.config.provider.clone(), self.config.model.clone())
        .with_budget(verlet_runtime_contracts::TurnBudget {
            max_tool_rounds: self.max_tool_rounds,
            max_output_tokens: Some(self.config.max_tokens),
            max_context_text_bytes: self
                .client
                .capabilities()
                .and_then(|capabilities| capabilities.context_policy.max_text_bytes),
        })
    }

    async fn run_turn(
        &self,
        turn_context: &crate::kernel::runtime_host::turn::TurnContext,
        turn_delivery_start_sequence: verlet_history::EventSequence,
        turn_anchor_timestamp_ms: i64,
        services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
        events: &tokio::sync::broadcast::Sender<
            crate::kernel::runtime_host::runtime_api::ThreadEvent,
        >,
        fold_intra_turn_steers: bool,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_history::CanonicalMessage> {
        let coordinates = turn_context.coordinates();
        let context = services.build_session_context(coordinates).await?;
        let source_cuts = context.source_cuts.clone();
        let session_entries = context.entries;
        let steering_contexts = if fold_intra_turn_steers {
            undelivered_intra_turn_steering_contexts(
                services,
                coordinates,
                &turn_context.turn_id,
                turn_delivery_start_sequence,
                &session_entries,
            )
            .await?
        } else {
            Vec::new()
        };
        let steering_entry_ids = steering_contexts
            .iter()
            .map(|steer| steer.entry_id)
            .collect::<std::collections::HashSet<_>>();
        let assembly_session_entries = reattach_late_tool_result_entries(
            session_entries
                .iter()
                .filter(|entry| !steering_entry_ids.contains(&entry.entry_id))
                .cloned()
                .collect(),
        );
        let memory_contexts = services
            .build_recall_read_plan_contexts(coordinates)
            .await?;
        let instruction_contexts = services
            .build_instruction_read_plan_contexts(coordinates)
            .await?;
        let skill_context_segments = skill_context_segments_from_thread(&turn_context.thread)?;
        let static_context_segments = static_context_segments_from_thread(&turn_context.thread)?;
        let environment_contexts = skill_context_segments
            .iter()
            .map(|segment| segment.content.clone())
            .chain(memory_contexts)
            .chain(instruction_contexts)
            .collect::<Vec<_>>();
        let compiled_context = crate::kernel::context_compiler::AgentContextCompiler::compile(
            crate::kernel::context_compiler::AgentContextCompileInput {
                system: self.config.system.clone(),
                static_system_sources: static_context_segments.clone(),
                session_entries: assembly_session_entries,
                turn_anchor_timestamp_ms,
                turn_context: turn_context.snapshot(),
                hook_contexts: steering_contexts
                    .into_iter()
                    .map(|steer| steer.context)
                    .collect(),
                environment_contexts,
                attachments: Vec::new(),
                tools: self.tool_definitions().await,
                policy: self.context_compile_policy.clone(),
            },
        );
        let mut request = self
            .config
            .request_from_messages(compiled_context.messages.clone());
        request.system = compiled_context.system.clone();
        request.tools = compiled_context.tools.clone();
        if let Some(thinking) = &turn_context.thinking {
            request.thinking = Some(thinking.clone());
        }
        let transformed = verlet_provider::provider_transform::normalize_history_for_target(
            std::mem::take(&mut request.messages),
            &request.api,
            &request.provider,
        );
        request.messages = transformed.messages;
        let mut replay_transform = transformed.counts;
        let agent_diagnostics = compiled_context.diagnostics.clone();
        let mut provider_dropped_messages = 0;
        let mut provider_truncated_text_bytes = 0;
        let mut provider_retained_text_bytes = agent_diagnostics.retained_text_bytes;
        let mode = if self.config.stream {
            verlet_provider::ProviderRequestMode::Stream
        } else {
            verlet_provider::ProviderRequestMode::Complete
        };
        if let Some(capabilities) = self.client.capabilities() {
            let (compiled, provider_compilation) =
                verlet_provider::compile_provider_request_context(
                    request,
                    &capabilities.context_policy,
                );
            request = compiled;
            let transformed = verlet_provider::provider_transform::normalize_history_for_target(
                std::mem::take(&mut request.messages),
                &request.api,
                &request.provider,
            );
            request.messages = transformed.messages;
            replay_transform.add_assign(transformed.counts);
            provider_dropped_messages = provider_compilation.dropped_messages;
            provider_truncated_text_bytes = provider_compilation.truncated_text_bytes;
            provider_retained_text_bytes = provider_compilation.retained_text_bytes;
        }
        let receipt_payload = context_compile_receipt_payload(
            &turn_context.turn_id,
            &session_entries,
            &compiled_context,
            &context_receipt_static_segments(&static_context_segments, &skill_context_segments),
            &agent_diagnostics,
            &replay_transform,
            provider_dropped_messages,
            provider_truncated_text_bytes,
            provider_retained_text_bytes,
        )?;
        services
            .record_context_compile_receipt_with_source_cuts(
                coordinates,
                &session_entries,
                &source_cuts,
                receipt_payload,
            )
            .await?;
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            events,
            coordinates,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ContextCompiled {
                diagnostics: agent_diagnostics,
                provider_dropped_messages,
                provider_truncated_text_bytes,
                provider_retained_text_bytes,
            },
        );
        let executed = execute_provider_request(
            self,
            turn_context,
            coordinates,
            &request,
            mode,
            verlet_runtime_contracts::RuntimeModelRequestPurpose::Turn,
            events,
        )
        .await?;
        let response = executed.response;
        Ok(verlet_history::CanonicalMessage::assistant_with_usage(
            executed.request.provider,
            executed.request.api,
            executed.request.model,
            response.content,
            response.usage,
            response.stop_reason,
        ))
    }

    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        let mut tools = self.config.tools.clone();
        let Some(tool_router) = &self.tool_router else {
            return tools;
        };
        let mut names = tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        for tool in tool_router.tool_definitions().await {
            if names.insert(tool.name.clone()) {
                tools.push(tool);
            }
        }
        tools
    }
}

/// Settles interrupted child-turn requests left dangling by daemon shutdown.
///
/// Both streams are read through their current tails before deciding what is
/// missing. In particular, a completion that legally follows the parent's
/// terminal `thread.joined` is included and is never overwritten by recovery.
async fn sweep_cancelled_turn_tool_calls(
    runtime: &AgentLoop,
    thread_context: &verlet_runtime_contracts::ThreadContext,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let Some(parent_thread_id) = thread_context.parent_thread_id else {
        return Ok(());
    };
    let coordinates = &thread_context.coordinates;
    let thread_events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let submitted_turns = thread_events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::TurnSubmitted)
        .filter_map(|event| {
            event
                .payload
                .get("turn_id")
                .and_then(serde_json::Value::as_str)
                .map(|turn_id| (event.id, turn_id.to_string()))
        })
        .collect::<std::collections::HashMap<_, _>>();

    let mut parent_coordinates = coordinates.clone();
    parent_coordinates.thread_id = parent_thread_id;
    let parent_events = services
        .runtime_store()
        .read_events(
            &crate::kernel::control_decision::control_stream_id(&parent_coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut cancelled_turns = std::collections::BTreeSet::new();
    let child_thread_id = coordinates.thread_id.to_string();
    for event in parent_events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ThreadJoined)
    {
        if event
            .payload
            .get("child_thread_id")
            .and_then(serde_json::Value::as_str)
            != Some(child_thread_id.as_str())
            || event
                .payload
                .get("terminal_state")
                .and_then(serde_json::Value::as_str)
                != Some("cancelled")
        {
            continue;
        }
        for source_event_id in &event.provenance.source_event_ids {
            if let Some(turn_id) = submitted_turns.get(source_event_id) {
                cancelled_turns.insert(turn_id.clone());
            }
        }
    }
    if cancelled_turns.is_empty() {
        return Ok(());
    }

    let mut completed = std::collections::BTreeMap::<
        crate::kernel::control_decision::ToolCallSubject,
        Vec<crate::kernel::control_decision::ToolCallCompletedPayload>,
    >::new();
    let mut next_finish_order = std::collections::HashMap::<String, u64>::new();
    let mut requests = std::collections::BTreeMap::new();
    for event in &thread_events {
        match event.kind {
            verlet_history::EventKind::ToolCallCompleted => {
                let Some(turn_id) = event
                    .payload
                    .get("subject")
                    .and_then(|subject| subject.get("turn_id"))
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                if !cancelled_turns.contains(turn_id) {
                    continue;
                }
                let payload = serde_json::from_value::<crate::kernel::control_decision::ToolCallCompletedPayload>(
                    event.payload.clone(),
                )
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "tool.call.completed {} payload is invalid during cancellation recovery: {err}",
                        event.id
                    ))
                })?;
                next_finish_order
                    .entry(payload.subject.turn_id.clone())
                    .and_modify(|next| {
                        *next = (*next).max(payload.finish_order.unwrap_or(0).saturating_add(1))
                    })
                    .or_insert_with(|| payload.finish_order.unwrap_or(0).saturating_add(1));
                completed
                    .entry(payload.subject.clone())
                    .or_default()
                    .push(payload);
            }
            verlet_history::EventKind::ToolCallRequested => {
                let Some(turn_id) = event
                    .payload
                    .get("subject")
                    .and_then(|subject| subject.get("turn_id"))
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                if !cancelled_turns.contains(turn_id) {
                    continue;
                }
                let payload = serde_json::from_value::<crate::kernel::control_decision::ToolCallRequestedPayload>(
                    event.payload.clone(),
                )
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "tool.call.requested {} payload is invalid during cancellation recovery: {err}",
                        event.id
                    ))
                })?;
                requests.insert(payload.subject.clone(), (event.id, payload));
            }
            _ => {}
        }
    }

    for (request_event_id, request) in requests.into_values() {
        if completed.get(&request.subject).is_some_and(|completions| {
            completions.iter().any(|completion| {
                completion.snapshot_id == request.snapshot_id
                    && completion.args_fingerprint == request.args_fingerprint
            })
        }) {
            continue;
        }
        let finish_order = next_finish_order
            .entry(request.subject.turn_id.clone())
            .or_default();
        let witness = WitnessedToolCall {
            tool_call: ProviderToolCall {
                id: request.subject.call_id.clone(),
                name: request.tool_name.clone(),
                arguments: request.arguments.clone(),
            },
            snapshot_id: request.snapshot_id.clone(),
            args_fingerprint: request.args_fingerprint.clone(),
            request_event_id,
            holds: request
                .holds
                .clone()
                .into_iter()
                .map(serde_json::from_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "tool hold payload is invalid during cancellation recovery: {err}"
                    ))
                })?,
            recovery_action: ToolRecoveryAction::ConservativeFailure,
            recovery_source_event_id: None,
            recovery_fingerprint_mismatch: false,
        };
        let turn_context = runtime.turn_context(
            thread_context,
            request.subject.turn_id.clone(),
            &crate::kernel::runtime_host::turn::TurnInput::text(""),
            tokio_util::sync::CancellationToken::new(),
        );
        let current_finish_order = *finish_order;
        let outcome = cancelled_tool_call_outcome(
            &witness,
            current_finish_order,
            crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace,
            "tool call was abandoned by an interrupt before daemon shutdown",
        );
        *finish_order = finish_order.saturating_add(1);
        if let Some(result) = existing_tool_result_message(
            services,
            coordinates,
            request_event_id,
            &request.subject.call_id,
            &request.snapshot_id,
            request.args_fingerprint.as_deref(),
        )
        .await?
        {
            let success = matches!(
                result,
                verlet_history::CanonicalMessage::ToolResult {
                    is_error: false,
                    ..
                }
            );
            append_tool_completion_event(
                services,
                coordinates,
                request.subject.turn_id.clone(),
                request.subject.call_id.clone(),
                request.snapshot_id.clone(),
                request.tool_name.clone(),
                request.args_fingerprint.clone(),
                success,
                Some(0),
                Some(current_finish_order),
                Some(crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace),
            )
            .await?;
        } else {
            append_detached_tool_call_outcome(
                services,
                &turn_context,
                thread_id,
                events,
                Ok(outcome),
            )
            .await?;
        }
    }
    Ok(())
}

async fn run_idle_provider_command(
    runtime: &AgentLoop,
    thread_context: &verlet_runtime_contracts::ThreadContext,
    command: crate::kernel::runtime_host::runtime_api::ThreadCommand,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    status: &tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    commands: &mut tokio::sync::mpsc::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
    runtime_cancellation: &tokio_util::sync::CancellationToken,
    pending_commands: &mut std::collections::VecDeque<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
) -> bool {
    match command {
        crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
            turn_id,
            input,
            mode,
        } => {
            if mode == verlet_runtime_contracts::TurnSubmissionMode::Steer {
                match services
                    .append_user_turn_input(coordinates, &turn_id, &input)
                    .await
                {
                    Ok(entry) => {
                        let _ =
                            events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror { thread_id, entry });
                    }
                    Err(err) => {
                        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Failed);
                        let _ = events.send(
                            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed {
                                thread_id,
                                message: err.to_string(),
                            },
                        );
                        return true;
                    }
                }
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    events,
                    coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                        code: "no_active_turn".to_string(),
                        message: "steer input requires an active provider turn".to_string(),
                    },
                );
                return false;
            }
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Running);
            if let Err(err) = run_auto_compaction_if_needed(
                runtime,
                thread_context,
                format!("{turn_id}:auto_compact"),
                coordinates,
                services,
                thread_id,
                events,
            )
            .await
            {
                fail_provider_turn(
                    coordinates,
                    thread_id,
                    events,
                    status,
                    "compaction",
                    err.to_string(),
                );
                return true;
            }
            let (turn_source_event_id, turn_delivery_start_sequence, turn_anchor_timestamp_ms) =
                match services
                    .append_user_turn_input(coordinates, &turn_id, &input)
                    .await
                {
                    Ok(entry) => {
                        let submitted = match append_turn_submitted_event(
                            services,
                            coordinates,
                            &turn_id,
                            &entry,
                        )
                        .await
                        {
                            Ok(submitted) => submitted,
                            Err(err) => {
                                let _ = status.send(verlet_runtime_contracts::ThreadStatus::Failed);
                                let _ = events.send(
                                    crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed {
                                        thread_id,
                                        message: err.to_string(),
                                    },
                                );
                                return true;
                            }
                        };
                        let _ =
                            events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror { thread_id, entry });
                        (submitted.id, submitted.sequence, submitted.created_at_ms)
                    }
                    Err(err) => {
                        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Failed);
                        let _ = events.send(
                            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed {
                                thread_id,
                                message: err.to_string(),
                            },
                        );
                        return true;
                    }
                };
            run_provider_turn(
                runtime,
                thread_context,
                turn_id,
                input,
                turn_source_event_id,
                turn_delivery_start_sequence,
                turn_anchor_timestamp_ms,
                coordinates,
                services,
                thread_id,
                events,
                status,
                commands,
                runtime_cancellation,
                pending_commands,
            )
            .await
        }
        crate::kernel::runtime_host::runtime_api::ThreadCommand::Compact {
            turn_id,
            trigger,
            summary,
        } => {
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Running);
            match run_compaction(
                runtime,
                thread_context,
                turn_id,
                trigger,
                summary,
                services,
                thread_id,
                events,
            )
            .await
            {
                Ok(()) => {
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                    false
                }
                Err(err) => {
                    fail_provider_turn(
                        coordinates,
                        thread_id,
                        events,
                        status,
                        "compaction",
                        err.to_string(),
                    );
                    true
                }
            }
        }
        crate::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall {
            turn_id,
            call_id,
        } => {
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Running);
            match resume_pending_tool_call(
                runtime,
                thread_context,
                &turn_id,
                &call_id,
                services,
                thread_id,
                events,
            )
            .await
            {
                Ok(ToolResumeOutcome::Resumed {
                    source_event_id,
                    turn_delivery_start_sequence,
                    turn_anchor_timestamp_ms,
                }) => {
                    run_provider_turn(
                        runtime,
                        thread_context,
                        turn_id,
                        crate::kernel::runtime_host::turn::TurnInput::text(""),
                        source_event_id,
                        turn_delivery_start_sequence,
                        turn_anchor_timestamp_ms,
                        coordinates,
                        services,
                        thread_id,
                        events,
                        status,
                        commands,
                        runtime_cancellation,
                        pending_commands,
                    )
                    .await
                }
                Ok(ToolResumeOutcome::StillWaiting | ToolResumeOutcome::AlreadyCompleted) => {
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                    false
                }
                Err(err) => {
                    fail_provider_turn(
                        coordinates,
                        thread_id,
                        events,
                        status,
                        "tool_resume",
                        err.to_string(),
                    );
                    true
                }
            }
        }
        crate::kernel::runtime_host::runtime_api::ThreadCommand::Cancel { reason } => {
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                    thread_id,
                    signal: verlet_runtime_contracts::ThreadSignal::interrupt_cancel(
                        coordinates,
                        reason.clone(),
                    ),
                },
            );
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                    thread_id,
                    reason,
                },
            );
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
            false
        }
        crate::kernel::runtime_host::runtime_api::ThreadCommand::CancelTurn { .. } => false,
        crate::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown => {
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                    thread_id,
                    signal: verlet_runtime_contracts::ThreadSignal::shutdown(coordinates),
                },
            );
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                coordinates,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                    state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                },
            );
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
            let _ = events
                .send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
            true
        }
    }
}

async fn run_auto_compaction_if_needed(
    runtime: &AgentLoop,
    thread_context: &verlet_runtime_contracts::ThreadContext,
    turn_id: String,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let Some(max_text_bytes) = runtime.compaction_policy.auto_max_context_text_bytes else {
        return Ok(());
    };
    let context = services.build_session_context(coordinates).await?;
    let memory_contexts = services
        .build_recall_read_plan_contexts(coordinates)
        .await?;
    let instruction_contexts = services
        .build_instruction_read_plan_contexts(coordinates)
        .await?;
    let skill_context_segments = skill_context_segments_from_thread(thread_context)?;
    let static_context_segments = static_context_segments_from_thread(thread_context)?;
    let environment_contexts = skill_context_segments
        .iter()
        .map(|segment| segment.content.clone())
        .chain(memory_contexts)
        .chain(instruction_contexts)
        .collect::<Vec<_>>();
    let turn_anchor_timestamp_ms = if context.entries.is_empty() && environment_contexts.is_empty()
    {
        // This preflight emits no timestamped messages, so no persisted anchor is consumed.
        0
    } else {
        persisted_thread_anchor_timestamp_ms(services, coordinates).await?
    };
    let compiled_context = crate::kernel::context_compiler::AgentContextCompiler::compile(
        crate::kernel::context_compiler::AgentContextCompileInput {
            system: runtime.config.system.clone(),
            static_system_sources: static_context_segments,
            session_entries: context.entries,
            turn_anchor_timestamp_ms,
            turn_context: runtime
                .turn_context(
                    thread_context,
                    turn_id.clone(),
                    &crate::kernel::runtime_host::turn::TurnInput::text(""),
                    tokio_util::sync::CancellationToken::new(),
                )
                .snapshot(),
            hook_contexts: Vec::new(),
            environment_contexts,
            attachments: Vec::new(),
            tools: Vec::new(),
            policy: crate::kernel::context_compiler::AgentContextCompilePolicy::unbounded(),
        },
    );
    if compiled_context.diagnostics.retained_text_bytes <= max_text_bytes {
        return Ok(());
    }
    run_compaction(
        runtime,
        thread_context,
        turn_id,
        crate::kernel::compaction::CompactionTrigger::Auto,
        None,
        services,
        thread_id,
        events,
    )
    .await
}

async fn persisted_thread_anchor_timestamp_ms(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> crate::kernel::runtime_host::VerletResult<i64> {
    services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
        .into_iter()
        .min_by_key(|event| event.sequence.get())
        .map(|event| event.created_at_ms)
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "thread {} has no persisted context timestamp anchor",
                coordinates.thread_id
            ))
        })
}

async fn run_compaction(
    runtime: &AgentLoop,
    thread_context: &verlet_runtime_contracts::ThreadContext,
    turn_id: String,
    trigger: crate::kernel::compaction::CompactionTrigger,
    requested_summary: Option<String>,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let input = crate::kernel::runtime_host::turn::TurnInput::text("");
    let turn_context = runtime.turn_context(
        thread_context,
        turn_id,
        &input,
        tokio_util::sync::CancellationToken::new(),
    );
    if let Some(hook_pipeline) = &runtime.hook_pipeline {
        let outcome = hook_pipeline
            .run_pre_compact(
                crate::agent::hooks::PreCompactHookRequest {
                    turn_context: turn_context.snapshot(),
                    trigger,
                    requested_summary: requested_summary.clone(),
                },
                |spec| emit_hook_started(events, turn_context.coordinates(), spec),
            )
            .await;
        emit_hook_records(events, turn_context.coordinates(), &outcome.records);
        append_hook_mutation_witnesses(
            services,
            turn_context.coordinates(),
            outcome.mutation_witnesses,
        )
        .await?;
        if outcome.should_stop {
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                turn_context.coordinates(),
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                    code: "pre_compact_hook".to_string(),
                    message: outcome
                        .stop_reason
                        .unwrap_or_else(|| "PreCompact hook stopped compaction".to_string()),
                },
            );
            return Ok(());
        }
    }

    let summary = match requested_summary {
        Some(summary) if !summary.trim().is_empty() => summary,
        _ => generate_compaction_summary(runtime, &turn_context, services, events).await?,
    };
    let source_context = services
        .build_session_context(turn_context.coordinates())
        .await?;
    let (summary_event, _) = services
        .record_context_summary_checkpoint(
            turn_context.coordinates(),
            &source_context.entries,
            &source_context.source_cuts,
            &summary,
        )
        .await?;
    let entry = services
        .append_session_entry_with_provenance(
            turn_context.coordinates(),
            None,
            verlet_history::SessionEntryKind::Compaction {
                summary: summary.clone(),
            },
            verlet_history::EventProvenance {
                source_streams: vec![verlet_history::EventStreamId::for_thread(
                    turn_context.coordinates(),
                )],
                source_event_ids: vec![summary_event.id],
                discharged_by: Some("projection:context-summarizer".to_string()),
                function: Some("session_entry_compaction/v1".to_string()),
                ..verlet_history::EventProvenance::default()
            },
        )
        .await?;
    let _ = events.send(
        crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror { thread_id, entry },
    );
    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
        events,
        turn_context.coordinates(),
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Compaction {
            trigger,
            summary: summary.clone(),
        },
    );

    if let Some(hook_pipeline) = &runtime.hook_pipeline {
        let outcome = hook_pipeline
            .run_post_compact(
                crate::agent::hooks::PostCompactHookRequest {
                    turn_context: turn_context.snapshot(),
                    trigger,
                    summary,
                },
                |spec| emit_hook_started(events, turn_context.coordinates(), spec),
            )
            .await;
        emit_hook_records(events, turn_context.coordinates(), &outcome.records);
        append_hook_mutation_witnesses(
            services,
            turn_context.coordinates(),
            outcome.mutation_witnesses,
        )
        .await?;
        if outcome.should_stop {
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                turn_context.coordinates(),
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PolicyRejected {
                    code: "post_compact_hook".to_string(),
                    message: outcome
                        .stop_reason
                        .unwrap_or_else(|| "PostCompact hook stopped after compaction".to_string()),
                },
            );
        }
    }
    Ok(())
}

async fn generate_compaction_summary(
    runtime: &AgentLoop,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> crate::kernel::runtime_host::VerletResult<String> {
    let context = services
        .build_session_context(turn_context.coordinates())
        .await?;
    let mut messages = Vec::new();
    verlet_history::append_model_visible_messages(
        &reattach_late_tool_result_entries(context.entries),
        &mut messages,
    );
    let fallback = crate::kernel::compaction::deterministic_compaction_summary(&messages);
    if messages.is_empty() {
        return Ok(fallback);
    }
    let normalized = verlet_provider::provider_transform::normalize_history_for_target(
        messages,
        &runtime.config.api,
        &runtime.config.provider,
    );
    let mut request = runtime.config.request_from_messages(normalized.messages);
    request.system.push(verlet_provider::SystemBlock::text(
        "Summarize the conversation so far for continuation. Preserve decisions, open tasks, tool results, and constraints. Return only the summary.",
    ));
    request.tools = Vec::new();
    request.max_tokens = runtime.config.max_tokens.min(1024);
    request.thinking = None;
    let executed = execute_provider_request(
        runtime,
        turn_context,
        turn_context.coordinates(),
        &request,
        verlet_provider::ProviderRequestMode::Complete,
        verlet_runtime_contracts::RuntimeModelRequestPurpose::Compaction,
        events,
    )
    .await?;
    let response = executed.response;
    let summary = response_content_text(&response.content);
    if summary.trim().is_empty() {
        Ok(fallback)
    } else {
        Ok(summary)
    }
}

struct ExecutedProviderResponse {
    request: verlet_provider::ProviderRequest,
    response: verlet_provider::ProviderResponse,
}

#[derive(Clone, Debug)]
struct ModelRequestAttemptError {
    class: verlet_runtime_contracts::RuntimeModelRequestErrorClass,
    message: String,
}

impl ModelRequestAttemptError {
    fn retryable(&self) -> bool {
        matches!(
            self.class,
            verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable
                | verlet_runtime_contracts::RuntimeModelRequestErrorClass::RateLimited
        )
    }

    fn fallback_eligible(&self) -> bool {
        matches!(
            self.class,
            verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable
                | verlet_runtime_contracts::RuntimeModelRequestErrorClass::RateLimited
                | verlet_runtime_contracts::RuntimeModelRequestErrorClass::UnsupportedCapability
        )
    }
}

async fn execute_provider_request(
    runtime: &AgentLoop,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    request: &verlet_provider::ProviderRequest,
    mode: verlet_provider::ProviderRequestMode,
    purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> crate::kernel::runtime_host::VerletResult<ExecutedProviderResponse> {
    let request_mode = mode;
    let mode = runtime_request_mode(mode);
    let mut endpoints = Vec::with_capacity(runtime.model_request_fallbacks.len() + 1);
    endpoints.push(ModelRequestEndpoint {
        config: runtime.config.clone(),
        client: std::sync::Arc::clone(&runtime.client),
    });
    endpoints.extend(runtime.model_request_fallbacks.iter().cloned());
    let retry_policy = runtime.model_request_retry_policy;
    let attempts = retry_policy.attempts();
    let mut last_error = None;

    'endpoints: for (endpoint_index, endpoint) in endpoints.iter().enumerate() {
        let request = request_for_endpoint(request, &endpoint.config);
        for attempt in 1..=attempts {
            let request_id = model_request_id(
                turn_context,
                purpose,
                mode,
                request.messages.len(),
                endpoint_index,
                attempt,
            );
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                coordinates,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestStarted {
                    request_id: request_id.clone(),
                    turn_id: turn_context.turn_id.clone(),
                    provider: request.provider.clone(),
                    api: provider_api_event_label(&request.api),
                    model: request.model.clone(),
                    mode,
                    purpose,
                    system_block_count: request.system.len(),
                    message_count: request.messages.len(),
                    tool_count: request.tools.len(),
                    max_tokens: request.max_tokens,
                },
            );
            let started_at = std::time::Instant::now();
            let result = execute_provider_request_attempt(
                endpoint,
                turn_context,
                coordinates,
                &request,
                request_mode,
                mode,
                events,
            )
            .await;
            let duration_ms = elapsed_ms(started_at);
            match result {
                Ok(response) => {
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        events,
                        coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestCompleted {
                            request_id,
                            turn_id: turn_context.turn_id.clone(),
                            provider: request.provider.clone(),
                            api: provider_api_event_label(&request.api),
                            model: request.model.clone(),
                            mode,
                            purpose,
                            duration_ms,
                            usage: runtime_usage_from_canonical(&response.usage),
                            stop_reason: response.stop_reason,
                        },
                    );
                    return Ok(ExecutedProviderResponse { request, response });
                }
                Err(error) => {
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        events,
                        coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFailed {
                            request_id: request_id.clone(),
                            turn_id: turn_context.turn_id.clone(),
                            provider: request.provider.clone(),
                            api: provider_api_event_label(&request.api),
                            model: request.model.clone(),
                            mode,
                            purpose,
                            duration_ms,
                            error_class: error.class,
                            error: error.message.clone(),
                        },
                    );

                    if attempt < attempts
                        && error.retryable()
                        && !turn_context.cancellation.is_cancelled()
                    {
                        let next_attempt = attempt + 1;
                        let next_request_id = model_request_id(
                            turn_context,
                            purpose,
                            mode,
                            request.messages.len(),
                            endpoint_index,
                            next_attempt,
                        );
                        let delay_ms = retry_policy.delay_ms(attempt);
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestRetryScheduled {
                                request_id,
                                next_request_id,
                                turn_id: turn_context.turn_id.clone(),
                                provider: request.provider.clone(),
                                api: provider_api_event_label(&request.api),
                                model: request.model.clone(),
                                mode,
                                purpose,
                                attempt,
                                next_attempt,
                                delay_ms,
                                error_class: error.class,
                                error: error.message.clone(),
                            },
                        );
                        if delay_ms > 0 {
                            tokio::select! {
                                _ = tokio::time::sleep(std::time::Duration::from_millis(delay_ms)) => {}
                                _ = turn_context.cancellation.cancelled() => {
                                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                                        verlet_provider::ProviderError::Cancelled.to_string(),
                                    ));
                                }
                            }
                        }
                        continue;
                    }

                    if endpoint_index + 1 < endpoints.len()
                        && error.fallback_eligible()
                        && !turn_context.cancellation.is_cancelled()
                    {
                        let next_request =
                            request_for_endpoint(&request, &endpoints[endpoint_index + 1].config);
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ModelRequestFallbackSelected {
                                request_id,
                                turn_id: turn_context.turn_id.clone(),
                                from_provider: request.provider.clone(),
                                from_api: provider_api_event_label(&request.api),
                                from_model: request.model.clone(),
                                to_provider: next_request.provider,
                                to_api: provider_api_event_label(&next_request.api),
                                to_model: next_request.model,
                                mode,
                                purpose,
                                error_class: error.class,
                                error: error.message.clone(),
                            },
                        );
                        last_error = Some(error);
                        continue 'endpoints;
                    }

                    return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        error.message,
                    ));
                }
            }
        }
    }

    Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
        last_error
            .map(|error| error.message)
            .unwrap_or_else(|| "provider request did not run".to_string()),
    ))
}

async fn execute_provider_request_attempt(
    endpoint: &ModelRequestEndpoint,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    request: &verlet_provider::ProviderRequest,
    request_mode: verlet_provider::ProviderRequestMode,
    mode: verlet_runtime_contracts::RuntimeModelRequestMode,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> Result<verlet_provider::ProviderResponse, ModelRequestAttemptError> {
    if let Some(capabilities) = endpoint.client.capabilities() {
        capabilities
            .validate_request(request, request_mode)
            .map_err(classify_provider_error)?;
    }
    match mode {
        verlet_runtime_contracts::RuntimeModelRequestMode::Complete => endpoint
            .client
            .complete_cancellable(request, turn_context.cancellation.clone())
            .await
            .map_err(classify_provider_error),
        verlet_runtime_contracts::RuntimeModelRequestMode::Stream => {
            let stream_events = endpoint
                .client
                .stream_cancellable(request, turn_context.cancellation.clone())
                .await
                .map_err(classify_provider_error)?;
            response_from_stream_events(coordinates, stream_events, events)
        }
    }
}

fn request_for_endpoint(
    request: &verlet_provider::ProviderRequest,
    config: &AgentLoopConfig,
) -> verlet_provider::ProviderRequest {
    let mut request = request.clone();
    request.api = config.api.clone();
    request.provider = config.provider.clone();
    request.model = config.model.clone();
    request
}

fn model_request_id(
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    purpose: verlet_runtime_contracts::RuntimeModelRequestPurpose,
    mode: verlet_runtime_contracts::RuntimeModelRequestMode,
    message_count: usize,
    endpoint_index: usize,
    attempt: u32,
) -> String {
    format!(
        "{}:{}:{}:{}:{}:candidate{}:attempt{}",
        turn_context.trace_id,
        turn_context.turn_id,
        purpose.as_str(),
        mode.as_str(),
        message_count,
        endpoint_index + 1,
        attempt
    )
}

fn classify_provider_error(error: verlet_provider::ProviderError) -> ModelRequestAttemptError {
    let message = error.to_string();
    let class = match error {
        verlet_provider::ProviderError::Cancelled => {
            verlet_runtime_contracts::RuntimeModelRequestErrorClass::Cancelled
        }
        verlet_provider::ProviderError::UnsupportedCapability { .. }
        | verlet_provider::ProviderError::ApiMismatch { .. } => {
            verlet_runtime_contracts::RuntimeModelRequestErrorClass::UnsupportedCapability
        }
        verlet_provider::ProviderError::Http(_) => {
            verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable
        }
        verlet_provider::ProviderError::HttpStatus { status, .. } if status.as_u16() == 429 => {
            verlet_runtime_contracts::RuntimeModelRequestErrorClass::RateLimited
        }
        verlet_provider::ProviderError::HttpStatus { status, .. }
            if status.is_server_error() || matches!(status.as_u16(), 408 | 409 | 425) =>
        {
            verlet_runtime_contracts::RuntimeModelRequestErrorClass::Retryable
        }
        verlet_provider::ProviderError::Decode(_)
        | verlet_provider::ProviderError::HttpStatus { .. } => {
            verlet_runtime_contracts::RuntimeModelRequestErrorClass::Fatal
        }
    };
    ModelRequestAttemptError { class, message }
}

fn stream_assembly_error(message: impl Into<String>) -> ModelRequestAttemptError {
    ModelRequestAttemptError {
        class: verlet_runtime_contracts::RuntimeModelRequestErrorClass::StreamAssembly,
        message: message.into(),
    }
}

fn runtime_request_mode(
    mode: verlet_provider::ProviderRequestMode,
) -> verlet_runtime_contracts::RuntimeModelRequestMode {
    match mode {
        verlet_provider::ProviderRequestMode::Complete => {
            verlet_runtime_contracts::RuntimeModelRequestMode::Complete
        }
        verlet_provider::ProviderRequestMode::Stream => {
            verlet_runtime_contracts::RuntimeModelRequestMode::Stream
        }
    }
}

fn provider_api_event_label(api: &verlet_history::ProviderApi) -> String {
    match api {
        verlet_history::ProviderApi::OpenAIResponses => "openai_responses".to_string(),
        verlet_history::ProviderApi::OpenAIChatCompletions => "openai_chat_completions".to_string(),
        verlet_history::ProviderApi::AnthropicMessages => "anthropic_messages".to_string(),
        verlet_history::ProviderApi::Other(provider_family) => provider_family.clone(),
    }
}

fn runtime_usage_from_canonical(
    usage: &verlet_history::CanonicalUsage,
) -> verlet_runtime_contracts::RuntimeUsage {
    verlet_runtime_contracts::RuntimeUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        cache_read_input_tokens: usage.cache_read_input_tokens,
    }
}

fn elapsed_ms(started_at: std::time::Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn exhausted_tool_round_budget(
    max_tool_rounds: Option<usize>,
    completed_rounds: usize,
) -> Option<usize> {
    max_tool_rounds.filter(|max_tool_rounds| completed_rounds >= *max_tool_rounds)
}

fn response_content_text(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. }
            | verlet_history::CanonicalContent::Thinking { text, .. } => Some(text.as_str()),
            verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

async fn run_provider_turn(
    runtime: &AgentLoop,
    thread_context: &verlet_runtime_contracts::ThreadContext,
    turn_id: String,
    turn_input: crate::kernel::runtime_host::turn::TurnInput,
    turn_source_event_id: verlet_history::EventRecordId,
    turn_delivery_start_sequence: verlet_history::EventSequence,
    turn_anchor_timestamp_ms: i64,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    status: &tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    commands: &mut tokio::sync::mpsc::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
    runtime_cancellation: &tokio_util::sync::CancellationToken,
    pending_commands: &mut std::collections::VecDeque<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
) -> bool {
    let mut tool_rounds = match persisted_tool_rounds_for_turn(
        services,
        coordinates,
        &turn_id,
        turn_delivery_start_sequence,
    )
    .await
    {
        Ok(tool_rounds) => tool_rounds,
        Err(err) => {
            fail_provider_turn(
                coordinates,
                thread_id,
                events,
                status,
                "history",
                err.to_string(),
            );
            return true;
        }
    };
    let tool_cancellation_grace =
        match crate::kernel::control_decision::active_manifest_bind_receipt(
            services.runtime_store().as_ref(),
            coordinates,
        )
        .await
        {
            Ok(Some((_, receipt))) => receipt
                .effective_runtime
                .cancellation_grace_ms
                .map(std::time::Duration::from_millis)
                .unwrap_or(crate::agent::agent_tool_router::DEFAULT_TOOL_CANCELLATION_GRACE),
            Ok(None) => crate::agent::agent_tool_router::DEFAULT_TOOL_CANCELLATION_GRACE,
            Err(err) => {
                fail_provider_turn(
                    coordinates,
                    thread_id,
                    events,
                    status,
                    "history",
                    err.to_string(),
                );
                return true;
            }
        };
    let turn_cancellation = tokio_util::sync::CancellationToken::new();
    let turn_context = runtime.turn_context(
        thread_context,
        turn_id.clone(),
        &turn_input,
        turn_cancellation.clone(),
    );
    if let Some(hook_pipeline) = &runtime.hook_pipeline {
        let outcome = hook_pipeline
            .run_user_prompt_submit(
                crate::agent::hooks::UserPromptSubmitHookRequest {
                    turn_context: turn_context.snapshot(),
                    prompt: turn_input.text_projection(),
                },
                |spec| emit_hook_started(events, coordinates, spec),
            )
            .await;
        emit_hook_records(events, coordinates, &outcome.records);
        if let Err(err) =
            append_hook_mutation_witnesses(services, coordinates, outcome.mutation_witnesses).await
        {
            fail_provider_turn(
                coordinates,
                thread_id,
                events,
                status,
                "hook_pipeline",
                err.to_string(),
            );
            return true;
        }
        if let Err(err) = append_hook_contexts(
            services,
            coordinates,
            thread_id,
            events,
            outcome.additional_contexts,
        )
        .await
        {
            fail_provider_turn(
                coordinates,
                thread_id,
                events,
                status,
                "hook_pipeline",
                err.to_string(),
            );
            return true;
        }
        if outcome.should_stop {
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                coordinates,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                    state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                },
            );
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
            let _ = events
                .send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
            return true;
        }
    }
    if tool_rounds > 0 {
        let pending_suspensions =
            match crate::kernel::control_decision::list_pending_tool_call_suspensions(
                services.runtime_store().as_ref(),
                coordinates,
            )
            .await
            {
                Ok(pending) => pending,
                Err(err) => {
                    fail_provider_turn(
                        coordinates,
                        thread_id,
                        events,
                        status,
                        "history",
                        err.to_string(),
                    );
                    return true;
                }
            };
        if pending_suspensions
            .iter()
            .any(|pending| pending.subject.turn_id == turn_id)
        {
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
            return false;
        }
        let pending_batch =
            match pending_witnessed_tool_batch_for_turn(services, coordinates, &turn_id).await {
                Ok(batch) => batch,
                Err(err) => {
                    fail_provider_turn(
                        coordinates,
                        thread_id,
                        events,
                        status,
                        "history",
                        err.to_string(),
                    );
                    return true;
                }
            };
        if let Some((tool_calls, assistant_entry_id)) = pending_batch {
            match append_tool_results_while_handling_commands(
                runtime,
                &turn_context,
                services,
                thread_id,
                events,
                tool_calls,
                assistant_entry_id,
                &turn_input,
                turn_source_event_id,
                coordinates,
                status,
                commands,
                runtime_cancellation,
                pending_commands,
                tool_cancellation_grace,
            )
            .await
            {
                ToolBatchAwaitOutcome::Completed(Ok(ToolAppendOutcome::Suspended)) => {
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                    return false;
                }
                ToolBatchAwaitOutcome::Completed(Ok(_)) => {}
                ToolBatchAwaitOutcome::Completed(Err(err)) => {
                    fail_provider_turn(
                        coordinates,
                        thread_id,
                        events,
                        status,
                        "tool_router",
                        err.to_string(),
                    );
                    return true;
                }
                ToolBatchAwaitOutcome::Cancelled { reason } => {
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        events,
                        coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
                            reason: reason.clone(),
                        },
                    );
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        events,
                        coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                            state: verlet_runtime_contracts::RuntimeTerminalState::Cancelled,
                        },
                    );
                    let _ = events.send(
                        crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                            thread_id,
                            reason,
                        },
                    );
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                    return false;
                }
                ToolBatchAwaitOutcome::Shutdown => {
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                    let _ = events.send(
                        crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped {
                            thread_id,
                        },
                    );
                    return true;
                }
                ToolBatchAwaitOutcome::Failed { code, reason } => {
                    fail_provider_turn(coordinates, thread_id, events, status, code, reason);
                    return true;
                }
            }
        }
    }
    loop {
        let mut cancelled_reason = None;
        let mut shutdown_after_turn = false;
        let mut failed = false;
        let mut failure_code = None;
        let mut failure_reason = None;
        let mut emit_failure_signal = false;
        let mut terminal_join_recorded = false;
        let mut continue_after_tools = false;
        let mut suspended_after_tools = false;
        let mut last_assistant_text = None;
        if tool_rounds > 0 {
            // This is the tool-round boundary: tool results are durable and no
            // next-round assembly future exists yet. Give the commands that are
            // ready at this boundary priority without letting a continuously
            // refilled channel starve assembly or runtime cancellation.
            let ready_command_count = commands.len();
            let mut handled_ready_commands = 0;
            loop {
                if handled_ready_commands >= ready_command_count && !commands.is_closed() {
                    break;
                }
                let command = match commands.try_recv() {
                    Ok(command) => {
                        handled_ready_commands += 1;
                        Some(command)
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => None,
                };
                let disconnected = command.is_none();
                match handle_active_provider_command(
                    command,
                    &turn_input,
                    &turn_context,
                    turn_source_event_id,
                    coordinates,
                    services,
                    thread_id,
                    events,
                    status,
                    pending_commands,
                    &turn_cancellation,
                    false,
                    false,
                )
                .await
                {
                    ActiveProviderCommandOutcome::Continue => {}
                    ActiveProviderCommandOutcome::Cancelled { reason } => {
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
                                reason: reason.clone(),
                            },
                        );
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                                state: verlet_runtime_contracts::RuntimeTerminalState::Cancelled,
                            },
                        );
                        let _ = events.send(
                            crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                                thread_id,
                                reason,
                            },
                        );
                        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        return false;
                    }
                    ActiveProviderCommandOutcome::Shutdown => {
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                                state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                            },
                        );
                        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                        let _ = events.send(
                            crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped {
                                thread_id,
                            },
                        );
                        return true;
                    }
                    ActiveProviderCommandOutcome::Failed { code, reason } => {
                        fail_provider_turn(coordinates, thread_id, events, status, code, reason);
                        return true;
                    }
                }
                if disconnected {
                    break;
                }
            }
        }
        let turn = runtime.run_turn(
            &turn_context,
            turn_delivery_start_sequence,
            turn_anchor_timestamp_ms,
            services,
            events,
            tool_rounds > 0,
        );
        tokio::pin!(turn);

        let result = loop {
            tokio::select! {
                result = &mut turn => break result,
                _ = runtime_cancellation.cancelled(), if cancelled_reason.is_none() => {
                    let reason = "runtime cancellation requested".to_string();
                    turn_cancellation.cancel();
                    append_terminal_join_until_recorded(
                        services,
                        &turn_context.thread,
                        verlet_history::ThreadTerminalState::Cancelled,
                        Some(reason.clone()),
                        Some(turn_source_event_id),
                    )
                    .await;
                    terminal_join_recorded = true;
                    cancelled_reason = Some(reason);
                }
                command = commands.recv() => {
                    match handle_active_provider_command(
                        command,
                        &turn_input,
                        &turn_context,
                        turn_source_event_id,
                        coordinates,
                        services,
                        thread_id,
                        events,
                        status,
                        pending_commands,
                        &turn_cancellation,
                        cancelled_reason.is_some(),
                        false,
                    )
                    .await
                    {
                        ActiveProviderCommandOutcome::Continue => {}
                        ActiveProviderCommandOutcome::Cancelled { reason } => {
                            terminal_join_recorded = true;
                            cancelled_reason = Some(reason);
                        }
                        ActiveProviderCommandOutcome::Shutdown => {
                            shutdown_after_turn = true;
                        }
                        ActiveProviderCommandOutcome::Failed { code, reason } => {
                            failed = true;
                            failure_code = Some(code);
                            failure_reason = Some(reason);
                            terminal_join_recorded = true;
                        }
                    }
                }
            }
        };

        if let Some(reason) = cancelled_reason {
            if !terminal_join_recorded {
                append_terminal_join_until_recorded(
                    services,
                    &turn_context.thread,
                    verlet_history::ThreadTerminalState::Cancelled,
                    Some(reason.clone()),
                    Some(turn_source_event_id),
                )
                .await;
            }
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                coordinates,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
                    reason: reason.clone(),
                },
            );
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                coordinates,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                    state: verlet_runtime_contracts::RuntimeTerminalState::Cancelled,
                },
            );
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                    thread_id,
                    reason,
                },
            );
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
            return false;
        } else if !failed {
            match result {
                Ok(message) => {
                    let text = text_from_message(&message);
                    last_assistant_text = Some(text.clone());
                    let tool_calls = tool_calls_from_message(&message);
                    if !runtime.config.stream
                        && let Some(usage) = usage_from_message(&message)
                    {
                        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                            events,
                            coordinates,
                            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Usage {
                                usage,
                            },
                        );
                    }
                    if !runtime.config.stream {
                        for tool_call in &tool_calls {
                            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                events,
                                coordinates,
                                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
                                    call_id: tool_call.id.clone(),
                                    name: tool_call.name.clone(),
                                    input: tool_call.arguments.clone(),
                                },
                            );
                        }
                    }
                    match services
                        .append_agent_loop_session_entry(
                            coordinates,
                            None,
                            verlet_history::SessionEntryKind::Message {
                                message: message.clone(),
                            },
                            vec![turn_source_event_id],
                        )
                        .await
                    {
                        Ok(entry) => {
                            let assistant_entry_id = entry.entry_id;
                            let _ = events
                                .send(crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror { thread_id, entry });
                            if !runtime.config.stream {
                                emit_non_stream_content_events(events, coordinates, &message);
                            }
                            if !text.is_empty() {
                                let _ = events.send(
                                    crate::kernel::runtime_host::runtime_api::ThreadEvent::Output {
                                        thread_id,
                                        text,
                                    },
                                );
                            }
                            if runtime.tool_router.is_some() && !tool_calls.is_empty() {
                                if let Some(max_tool_rounds) = exhausted_tool_round_budget(
                                    runtime.max_tool_rounds,
                                    tool_rounds,
                                ) {
                                    failed = true;
                                    failure_code = Some("tool_router");
                                    let reason =
                                        format!("tool router exceeded {max_tool_rounds} rounds");
                                    failure_reason = Some(reason);
                                } else {
                                    match append_tool_results_while_handling_commands(
                                        runtime,
                                        &turn_context,
                                        services,
                                        thread_id,
                                        events,
                                        tool_calls,
                                        assistant_entry_id,
                                        &turn_input,
                                        turn_source_event_id,
                                        coordinates,
                                        status,
                                        commands,
                                        runtime_cancellation,
                                        pending_commands,
                                        tool_cancellation_grace,
                                    )
                                    .await
                                    {
                                        ToolBatchAwaitOutcome::Completed(Ok(outcome)) => {
                                            match outcome {
                                                ToolAppendOutcome::NoTools => {}
                                                ToolAppendOutcome::AppendedResults => {
                                                    tool_rounds += 1;
                                                    continue_after_tools = true;
                                                }
                                                ToolAppendOutcome::Suspended => {
                                                    suspended_after_tools = true;
                                                }
                                            }
                                        }
                                        ToolBatchAwaitOutcome::Completed(Err(err)) => {
                                            failed = true;
                                            failure_code = Some("tool_router");
                                            failure_reason = Some(err.to_string());
                                        }
                                        ToolBatchAwaitOutcome::Cancelled { reason } => {
                                            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                                events,
                                                coordinates,
                                                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Cancelled {
                                                    reason: reason.clone(),
                                                },
                                            );
                                            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                                                events,
                                                coordinates,
                                                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                                                    state: verlet_runtime_contracts::RuntimeTerminalState::Cancelled,
                                                },
                                            );
                                            let _ = events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                                                thread_id,
                                                reason,
                                            });
                                            let _ = status
                                                .send(verlet_runtime_contracts::ThreadStatus::Idle);
                                            return false;
                                        }
                                        ToolBatchAwaitOutcome::Shutdown => {
                                            let _ = status.send(
                                                verlet_runtime_contracts::ThreadStatus::Stopped,
                                            );
                                            let _ = events
                                                .send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
                                            return true;
                                        }
                                        ToolBatchAwaitOutcome::Failed { code, reason } => {
                                            fail_provider_turn(
                                                coordinates,
                                                thread_id,
                                                events,
                                                status,
                                                code,
                                                reason,
                                            );
                                            return true;
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            failed = true;
                            failure_code = Some("history");
                            failure_reason = Some(err.to_string());
                        }
                    }
                }
                Err(err) => {
                    failed = true;
                    failure_code = Some("runtime_execution");
                    failure_reason = Some(err.to_string());
                    emit_failure_signal = true;
                    append_terminal_join_until_recorded(
                        services,
                        &turn_context.thread,
                        verlet_history::ThreadTerminalState::Failed,
                        failure_reason.clone(),
                        Some(turn_source_event_id),
                    )
                    .await;
                    terminal_join_recorded = true;
                }
            }
        }

        if shutdown_after_turn {
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                coordinates,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                    state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                },
            );
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
            let _ = events
                .send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
            return true;
        }
        if failed {
            let reason = failure_reason
                .unwrap_or_else(|| "provider turn failed without reason detail".to_string());
            if !terminal_join_recorded {
                append_terminal_join_until_recorded(
                    services,
                    &turn_context.thread,
                    verlet_history::ThreadTerminalState::Failed,
                    Some(reason.clone()),
                    Some(turn_source_event_id),
                )
                .await;
            }
            if emit_failure_signal {
                let _ = events.send(
                    crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                        thread_id,
                        signal: verlet_runtime_contracts::ThreadSignal::failed(
                            coordinates,
                            reason.clone(),
                        ),
                    },
                );
            }
            fail_provider_turn(
                coordinates,
                thread_id,
                events,
                status,
                failure_code.unwrap_or("runtime_execution"),
                reason,
            );
            return true;
        }
        if suspended_after_tools {
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
            return false;
        }
        if continue_after_tools {
            continue;
        }
        if let Err(err) = run_stop_hooks(
            runtime,
            &turn_context,
            services,
            thread_id,
            events,
            last_assistant_text,
        )
        .await
        {
            let reason = err.to_string();
            append_terminal_join_until_recorded(
                services,
                &turn_context.thread,
                verlet_history::ThreadTerminalState::Failed,
                Some(reason.clone()),
                Some(turn_source_event_id),
            )
            .await;
            fail_provider_turn(
                coordinates,
                thread_id,
                events,
                status,
                "hook_pipeline",
                reason,
            );
            return true;
        }
        if let Err(err) =
            append_turn_completed_event(services, &turn_context.thread, &turn_id).await
        {
            let reason = err.to_string();
            append_terminal_join_until_recorded(
                services,
                &turn_context.thread,
                verlet_history::ThreadTerminalState::Failed,
                Some(reason.clone()),
                Some(turn_source_event_id),
            )
            .await;
            fail_provider_turn(coordinates, thread_id, events, status, "history", reason);
            return true;
        }
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            events,
            coordinates,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                state: verlet_runtime_contracts::RuntimeTerminalState::Completed,
            },
        );
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        return false;
    }
}

async fn persisted_tool_rounds_for_turn(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    turn_delivery_start_sequence: verlet_history::EventSequence,
) -> crate::kernel::runtime_host::VerletResult<usize> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            Some(turn_delivery_start_sequence),
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let session_entry_event_ids = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::SessionEntryAppended)
        .map(|event| event.id)
        .collect::<std::collections::HashSet<_>>();
    let mut assistant_batches = std::collections::HashSet::new();
    for event in events
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
    {
        if event
            .payload
            .get("subject")
            .and_then(|subject| subject.get("turn_id"))
            .and_then(serde_json::Value::as_str)
            == Some(turn_id)
        {
            let assistant_event_id =
                event.provenance.source_event_ids.first().ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "tool.call.requested {} for turn {turn_id} has no assistant source event",
                        event.id
                    ))
                })?;
            if !session_entry_event_ids.contains(assistant_event_id) {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
                    "tool.call.requested {} for turn {turn_id} references assistant source event {} outside the active turn",
                    event.id, assistant_event_id
                )));
            }
            assistant_batches.insert(*assistant_event_id);
        }
    }
    Ok(assistant_batches.len())
}

async fn pending_witnessed_tool_batch_for_turn(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
) -> crate::kernel::runtime_host::VerletResult<
    Option<(Vec<ProviderToolCall>, verlet_history::SessionEntryId)>,
> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let Some(latest_request) = events
        .iter()
        .filter(|event| {
            event.kind == verlet_history::EventKind::ToolCallRequested
                && event.payload["subject"]["turn_id"] == turn_id
        })
        .max_by_key(|event| event.sequence.get())
    else {
        return Ok(None);
    };
    let assistant_source_event_id = latest_request
        .provenance
        .source_event_ids
        .first()
        .copied()
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool.call.requested {} for turn {turn_id} has no assistant source event",
                latest_request.id
            ))
        })?;
    let mut requests = std::collections::BTreeMap::<
        String,
        (
            i64,
            verlet_history::EventRecordId,
            crate::kernel::control_decision::ToolCallRequestedPayload,
        ),
    >::new();
    for event in events.iter().filter(|event| {
        event.kind == verlet_history::EventKind::ToolCallRequested
            && event.provenance.source_event_ids.first() == Some(&assistant_source_event_id)
    }) {
        let request = serde_json::from_value::<
            crate::kernel::control_decision::ToolCallRequestedPayload,
        >(event.payload.clone())
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool.call.requested {} payload is invalid during turn replay: {err}",
                event.id
            ))
        })?;
        if request.subject.turn_id == turn_id {
            let replace = requests
                .get(&request.subject.call_id)
                .is_none_or(|(sequence, _, _)| *sequence < event.sequence.get());
            if replace {
                requests.insert(
                    request.subject.call_id.clone(),
                    (event.sequence.get(), event.id, request),
                );
            }
        }
    }
    let mut completions = Vec::new();
    for event in events.iter().filter(|event| {
        event.kind == verlet_history::EventKind::ToolCallCompleted
            && event.payload["subject"]["turn_id"] == turn_id
    }) {
        completions.push(
            serde_json::from_value::<crate::kernel::control_decision::ToolCallCompletedPayload>(
                event.payload.clone(),
            )
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "tool.call.completed {} payload is invalid during turn replay: {err}",
                    event.id
                ))
            })?,
        );
    }
    let mut incomplete = false;
    for (_, request_event_id, request) in requests.values() {
        let completed = completions.iter().any(|completion| {
            completion.subject == request.subject
                && completion.snapshot_id == request.snapshot_id
                && completion.args_fingerprint == request.args_fingerprint
        });
        let result = existing_tool_result_message(
            services,
            coordinates,
            *request_event_id,
            &request.subject.call_id,
            &request.snapshot_id,
            request.args_fingerprint.as_deref(),
        )
        .await?;
        incomplete |= !completed || result.is_none();
    }
    if !incomplete {
        return Ok(None);
    }

    let assistant_event = events
        .iter()
        .find(|event| {
            event.id == assistant_source_event_id && event.kind == verlet_history::EventKind::SessionEntryAppended
        })
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool request batch source {assistant_source_event_id} is not an assistant session entry"
            ))
        })?;
    let assistant_entry_id = assistant_event
        .payload
        .get("entry_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "assistant session event {assistant_source_event_id} has no entry_id"
            ))
        })?;
    let context = services.build_session_context(coordinates).await?;
    let assistant_entry = context
        .entries
        .iter()
        .find(|entry| entry.entry_id.to_string() == assistant_entry_id)
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "assistant session entry {assistant_entry_id} is missing during turn replay"
            ))
        })?;
    let verlet_history::SessionEntryKind::Message { message } = &assistant_entry.kind else {
        return Err(crate::kernel::runtime_host::VerletError::History(format!(
            "tool request batch source {assistant_entry_id} is not a canonical message"
        )));
    };
    let tool_calls = tool_calls_from_message(message);
    for (_, _, request) in requests.values() {
        let call = tool_calls
            .iter()
            .find(|call| call.id == request.subject.call_id)
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "witnessed assistant batch {assistant_entry_id} lost tool call {}",
                    request.subject.call_id
                ))
            })?;
        let replayed_fingerprint =
            crate::agent::tool_universe::args_fingerprint(&call.name, &call.arguments)?;
        if call.name != request.tool_name
            || call.arguments != request.arguments
            || request
                .args_fingerprint
                .as_deref()
                .is_some_and(|fingerprint| fingerprint != replayed_fingerprint)
        {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "witnessed assistant batch {assistant_entry_id} disagrees with tool request {}",
                request.subject.call_id
            )));
        }
    }
    Ok(Some((tool_calls, assistant_entry.entry_id)))
}

enum ActiveProviderCommandOutcome {
    Continue,
    Cancelled { reason: String },
    Shutdown,
    Failed { code: &'static str, reason: String },
}

#[allow(clippy::too_many_arguments)]
async fn handle_active_provider_command(
    command: Option<crate::kernel::runtime_host::runtime_api::ThreadCommand>,
    turn_input: &crate::kernel::runtime_host::turn::TurnInput,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    turn_source_event_id: verlet_history::EventRecordId,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    status: &tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    pending_commands: &mut std::collections::VecDeque<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
    turn_cancellation: &tokio_util::sync::CancellationToken,
    already_cancelled: bool,
    defer_cancel_terminal: bool,
) -> ActiveProviderCommandOutcome {
    match command {
        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Cancel { reason }) => {
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                    thread_id,
                    signal: verlet_runtime_contracts::ThreadSignal::interrupt_cancel(
                        coordinates,
                        reason.clone(),
                    ),
                },
            );
            turn_cancellation.cancel();
            if already_cancelled {
                return ActiveProviderCommandOutcome::Continue;
            }
            if !defer_cancel_terminal {
                append_terminal_join_until_recorded(
                    services,
                    &turn_context.thread,
                    verlet_history::ThreadTerminalState::Cancelled,
                    Some(reason.clone()),
                    Some(turn_source_event_id),
                )
                .await;
            }
            ActiveProviderCommandOutcome::Cancelled { reason }
        }
        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::CancelTurn {
            watchdog_token_id,
            reason,
        }) => {
            if turn_input.turn_watchdog_id() != Some(watchdog_token_id) {
                return ActiveProviderCommandOutcome::Continue;
            }
            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                    thread_id,
                    signal: verlet_runtime_contracts::ThreadSignal::interrupt_cancel(
                        coordinates,
                        reason.clone(),
                    ),
                },
            );
            turn_cancellation.cancel();
            if already_cancelled {
                return ActiveProviderCommandOutcome::Continue;
            }
            if !defer_cancel_terminal {
                append_terminal_join_until_recorded(
                    services,
                    &turn_context.thread,
                    verlet_history::ThreadTerminalState::Cancelled,
                    Some(reason.clone()),
                    Some(turn_source_event_id),
                )
                .await;
            }
            ActiveProviderCommandOutcome::Cancelled { reason }
        }
        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown) | None => {
            let _ = events.send(
                crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                    thread_id,
                    signal: verlet_runtime_contracts::ThreadSignal::shutdown(coordinates),
                },
            );
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                events,
                coordinates,
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                    state: verlet_runtime_contracts::RuntimeTerminalState::Stopped,
                },
            );
            turn_cancellation.cancel();
            ActiveProviderCommandOutcome::Shutdown
        }
        Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
            turn_id,
            input,
            mode,
        }) => match mode {
            verlet_runtime_contracts::TurnSubmissionMode::Queue => {
                let _ = events.send(
                    crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                        thread_id,
                        signal: verlet_runtime_contracts::ThreadSignal::user_queue(
                            coordinates,
                            turn_id.clone(),
                        ),
                    },
                );
                pending_commands.push_back(
                    crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                        turn_id,
                        input,
                        mode,
                    },
                );
                ActiveProviderCommandOutcome::Continue
            }
            verlet_runtime_contracts::TurnSubmissionMode::Steer => {
                match services
                    .append_user_turn_input(coordinates, &turn_id, &input)
                    .await
                {
                    Ok(entry) => {
                        let _ =
                            events.send(crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror { thread_id, entry });
                    }
                    Err(err) => {
                        let reason = err.to_string();
                        turn_cancellation.cancel();
                        if !defer_cancel_terminal {
                            append_terminal_join_until_recorded(
                                services,
                                &turn_context.thread,
                                verlet_history::ThreadTerminalState::Failed,
                                Some(reason.clone()),
                                Some(turn_source_event_id),
                            )
                            .await;
                        }
                        return ActiveProviderCommandOutcome::Failed {
                            code: "history",
                            reason,
                        };
                    }
                }
                let _ = events.send(
                    crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                        thread_id,
                        signal: verlet_runtime_contracts::ThreadSignal::user_steer(
                            coordinates,
                            turn_id,
                        )
                        .with_metadata(std::collections::BTreeMap::from([(
                            "active_turn_id".to_string(),
                            turn_context.turn_id.clone(),
                        )])),
                    },
                );
                ActiveProviderCommandOutcome::Continue
            }
            verlet_runtime_contracts::TurnSubmissionMode::Interrupt => {
                let reason = format!("interrupted by turn {turn_id}");
                let _ = status.send(verlet_runtime_contracts::ThreadStatus::Cancelling);
                let _ = events.send(
                    crate::kernel::runtime_host::runtime_api::ThreadEvent::Signal {
                        thread_id,
                        signal: verlet_runtime_contracts::ThreadSignal::user_interrupt(
                            coordinates,
                            turn_id.clone(),
                        ),
                    },
                );
                turn_cancellation.cancel();
                pending_commands.push_front(
                    crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                        turn_id,
                        input,
                        mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
                    },
                );
                if already_cancelled {
                    return ActiveProviderCommandOutcome::Continue;
                }
                if !defer_cancel_terminal {
                    append_terminal_join_until_recorded(
                        services,
                        &turn_context.thread,
                        verlet_history::ThreadTerminalState::Cancelled,
                        Some(reason.clone()),
                        Some(turn_source_event_id),
                    )
                    .await;
                }
                ActiveProviderCommandOutcome::Cancelled { reason }
            }
        },
        Some(command @ crate::kernel::runtime_host::runtime_api::ThreadCommand::Compact { .. })
        | Some(
            command @ crate::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall {
                ..
            },
        ) => {
            pending_commands.push_back(command);
            ActiveProviderCommandOutcome::Continue
        }
    }
}

fn fail_provider_turn(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    status: &tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    code: impl Into<String>,
    message: String,
) {
    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Failed);
    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
        events,
        coordinates,
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed {
            code: code.into(),
            message: message.clone(),
        },
    );
    let _ = events
        .send(crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { thread_id, message });
}

async fn append_terminal_join_until_recorded(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    context: &verlet_runtime_contracts::ThreadContext,
    terminal_state: verlet_history::ThreadTerminalState,
    reason: Option<String>,
    source_event_id: Option<verlet_history::EventRecordId>,
) {
    loop {
        match services
            .append_thread_joined_event_if_spawned(
                context,
                terminal_state,
                reason.clone(),
                source_event_id,
            )
            .await
        {
            Ok(_) => return,
            Err(err) => {
                eprintln!(
                    "verlet agent loop could not persist {terminal_state:?} thread.joined for {}: {err}; retrying",
                    context.coordinates.thread_id,
                );
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
    }
}

#[derive(Clone, Debug)]
struct ProviderToolCall {
    id: String,
    name: String,
    arguments: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolAppendOutcome {
    NoTools,
    AppendedResults,
    Suspended,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolResumeOutcome {
    Resumed {
        source_event_id: verlet_history::EventRecordId,
        turn_delivery_start_sequence: verlet_history::EventSequence,
        turn_anchor_timestamp_ms: i64,
    },
    StillWaiting,
    AlreadyCompleted,
}

#[derive(Clone, Debug)]
struct PendingToolCallRequest {
    request_event_id: verlet_history::EventRecordId,
    assistant_source_event_id: verlet_history::EventRecordId,
    subject: crate::kernel::control_decision::ToolCallSubject,
    snapshot_id: String,
    tool_name: String,
    arguments: serde_json::Value,
    args_fingerprint: Option<String>,
    holds: Vec<tool_holds::ToolHold>,
}

enum ResumedToolCallAction {
    Execute(serde_json::Value),
    Deny(String),
}

#[derive(Clone)]
struct WitnessedToolCall {
    tool_call: ProviderToolCall,
    snapshot_id: String,
    args_fingerprint: Option<String>,
    request_event_id: verlet_history::EventRecordId,
    holds: Vec<tool_holds::ToolHold>,
    recovery_action: ToolRecoveryAction,
    recovery_source_event_id: Option<verlet_history::EventRecordId>,
    recovery_fingerprint_mismatch: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolRecoveryAction {
    Reuse,
    Reexecute,
    ConservativeFailure,
}

/// Recovery honors the effect class; a recorded outcome is reused only under
/// a matching fingerprint within the same snapshot.
fn tool_recovery_action(
    effect_class: verlet_agent::manifest_schema::EffectClass,
    outcome_exists: bool,
    fingerprint_matches: bool,
) -> ToolRecoveryAction {
    if outcome_exists && fingerprint_matches {
        ToolRecoveryAction::Reuse
    } else if effect_class == verlet_agent::manifest_schema::EffectClass::AtMostOnce {
        ToolRecoveryAction::ConservativeFailure
    } else {
        ToolRecoveryAction::Reexecute
    }
}

fn effect_class_from_bind_receipt(
    receipt: &crate::agent::manifest_bind::AgentManifestBindReceipt,
    tool_name: &str,
    arguments: &serde_json::Value,
) -> verlet_agent::manifest_schema::EffectClass {
    if let Some(effect_class) = receipt
        .operation_bindings
        .iter()
        .flat_map(|binding| &binding.direct_tools)
        .find(|tool| tool.tool_name == tool_name)
        .map(|tool| tool.effect_class)
    {
        return effect_class;
    }
    if let Some(effect_class) = receipt
        .tool_universes
        .iter()
        .flat_map(|universe| &universe.tools)
        .find(|tool| tool.tool_name == tool_name)
        .map(|tool| tool.effect_class)
    {
        return effect_class;
    }
    let operation_name = if tool_name == verlet_vbash::BASH_TOOL {
        arguments
            .get("command")
            .and_then(serde_json::Value::as_str)
            .and_then(exact_single_bash_operation_name)
    } else {
        Some(tool_name)
    };
    operation_name
        .into_iter()
        .flat_map(|operation_name| {
            receipt
                .operation_bindings
                .iter()
                .filter(move |binding| {
                    binding
                        .operations
                        .iter()
                        .any(|operation| operation == operation_name)
                })
                .map(|binding| binding.effect_class)
        })
        .max()
        .unwrap_or_default()
}

fn exact_single_bash_operation_name(command: &str) -> Option<&str> {
    // Bashkit accepts full shell scripts, including pipelines, control lists,
    // substitutions, redirections, assignments, and quoted command names.
    // Recovery may inherit a declared class only for the deliberately smaller
    // single-simple-command subset whose executable is unambiguous here.
    if command.chars().any(|character| {
        character.is_control()
            || matches!(
                character,
                ';' | '&' | '|' | '<' | '>' | '$' | '`' | '\\' | '\'' | '"' | '(' | ')' | '{' | '}'
            )
    }) {
        return None;
    }
    let operation_name = command.split_whitespace().next()?;
    (!operation_name.contains('=')).then_some(operation_name)
}

pub(crate) fn effect_class_for_request(
    events: &[verlet_history::EventRecord],
    request: &crate::kernel::control_decision::ToolCallRequestedPayload,
) -> crate::kernel::runtime_host::VerletResult<verlet_agent::manifest_schema::EffectClass> {
    let Some(event) = events.iter().rev().find(|event| {
        event.kind == verlet_history::EventKind::ManifestBindCompleted
            && event
                .payload
                .get("manifest_hash")
                .and_then(serde_json::Value::as_str)
                == Some(request.snapshot_id.as_str())
    }) else {
        return Ok(verlet_agent::manifest_schema::EffectClass::AtMostOnce);
    };
    let receipt = serde_json::from_value::<crate::agent::manifest_bind::AgentManifestBindReceipt>(
        event.payload.clone(),
    )
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "manifest.bind.completed {} payload is invalid: {err}",
            event.id
        ))
    })?;
    Ok(effect_class_from_bind_receipt(
        &receipt,
        &request.tool_name,
        &request.arguments,
    ))
}

async fn apply_tool_recovery_actions(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    calls: &mut [WitnessedToolCall],
) -> crate::kernel::runtime_host::VerletResult<()> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let has_prior_request = calls.iter().any(|call| {
        events.iter().any(|event| {
            event.kind == verlet_history::EventKind::ToolCallRequested
                && event.id != call.request_event_id
                && event.payload["subject"]["turn_id"] == turn_id
                && event.payload["subject"]["call_id"] == call.tool_call.id
        })
    });
    if !has_prior_request {
        return Ok(());
    }
    for call in calls {
        let prior_request = events
            .iter()
            .filter(|event| {
                event.kind == verlet_history::EventKind::ToolCallRequested
                    && event.id != call.request_event_id
                    && event.payload["subject"]["turn_id"] == turn_id
                    && event.payload["subject"]["call_id"] == call.tool_call.id
            })
            .max_by_key(|event| event.sequence.get());
        let Some(prior_request) = prior_request else {
            continue;
        };
        let request = serde_json::from_value::<
            crate::kernel::control_decision::ToolCallRequestedPayload,
        >(prior_request.payload.clone())
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool.call.requested {} payload is invalid during recovery: {err}",
                prior_request.id
            ))
        })?;
        let result = existing_tool_result_message(
            services,
            coordinates,
            prior_request.id,
            &call.tool_call.id,
            &call.snapshot_id,
            call.args_fingerprint.as_deref(),
        )
        .await?;
        let fingerprint_matches =
            crate::kernel::control_decision::tool_invocation_fingerprint_matches(
                &request.snapshot_id,
                request.args_fingerprint.as_deref(),
                &call.snapshot_id,
                call.args_fingerprint.as_deref(),
            );
        let mut effect_class = effect_class_for_request(&events, &request)?;
        if effect_class != verlet_agent::manifest_schema::EffectClass::AtMostOnce
            && request.tool_name == verlet_vbash::BASH_TOOL
            && matches!(
                crate::kernel::control_decision::decide_tool_call(
                    services.runtime_store().as_ref(),
                    crate::kernel::control_decision::ToolDecisionRequest {
                        coordinates: coordinates.clone(),
                        subject: request.subject.clone(),
                        snapshot_id: request.snapshot_id.clone(),
                        request_event_id: prior_request.id,
                    },
                )
                .await?,
                crate::kernel::control_decision::ToolCallDecision::Rewrite { .. }
            )
        {
            // The request fingerprint and class describe the original bash
            // script, not a controller-rewritten script. Without a witnessed
            // class for the rewritten command, automatic replay must fail
            // closed even when the original operation was retryable.
            effect_class = verlet_agent::manifest_schema::EffectClass::AtMostOnce;
        }
        // The canonical result is the reusable durable outcome and is appended
        // before tool.call.completed. A completion without that result is an
        // anomalous/legacy partial record, so recovery degrades by effect class
        // instead of entering Reuse and hard-erroring on a missing message.
        call.recovery_action =
            tool_recovery_action(effect_class, result.is_some(), fingerprint_matches);
        call.recovery_source_event_id = result.map(|_| prior_request.id);
        call.recovery_fingerprint_mismatch = !fingerprint_matches;
    }
    Ok(())
}

#[derive(Clone)]
enum PreparedToolCallOutcome {
    Completed {
        call_id: String,
        tool_name: String,
        snapshot_id: String,
        args_fingerprint: Option<String>,
        source_event_id: verlet_history::EventRecordId,
        finish_order: u64,
        cancellation: Option<crate::kernel::control_decision::ToolCallCancellation>,
        outcome: Box<crate::agent::tool_interceptor::ToolExecutionOutcome>,
    },
    Denied {
        call_id: String,
        tool_name: String,
        snapshot_id: String,
        args_fingerprint: Option<String>,
        source_event_id: verlet_history::EventRecordId,
        finish_order: u64,
        reason: String,
    },
    Suspended {
        call_id: String,
        snapshot_id: String,
        waiting_on_event_id: verlet_history::EventRecordId,
        approval_id: Option<String>,
        reason: Option<String>,
    },
    Abandoned,
}

enum ToolCallMonitorOutcome {
    Settled(crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome>),
    Abandoned,
}

enum PreparedToolBatch {
    NoTools,
    Outcomes(Vec<crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome>>),
}

fn tool_calls_from_message(message: &verlet_history::CanonicalMessage) -> Vec<ProviderToolCall> {
    match message {
        verlet_history::CanonicalMessage::Assistant { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                verlet_history::CanonicalContent::ToolCall {
                    id,
                    name,
                    arguments,
                } => Some(ProviderToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                }),
                _ => None,
            })
            .collect(),
        verlet_history::CanonicalMessage::User { .. }
        | verlet_history::CanonicalMessage::ToolResult { .. } => Vec::new(),
    }
}

fn skill_context_segments_from_thread(
    thread: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<
    Vec<crate::agent::manifest_bind::AgentManifestStaticContextSegment>,
> {
    let Some(raw) = thread
        .metadata
        .get(crate::agent::manifest_bind::THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA)
    else {
        return Ok(Vec::new());
    };
    let segments = serde_json::from_str::<
        Vec<crate::agent::manifest_bind::AgentManifestStaticContextSegment>,
    >(raw)
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "thread manifest skill context segments are invalid: {err}"
        ))
    })?;
    for segment in &segments {
        let expected = verlet_agent::contracts::sha256_hex(segment.content.as_bytes());
        if segment.content_sha256 != expected {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "thread manifest skill context segment {:?} content hash mismatch: expected {}, got {}",
                    segment.id, expected, segment.content_sha256
                ),
            ));
        }
    }
    Ok(segments)
}

fn static_context_segments_from_thread(
    thread: &verlet_runtime_contracts::ThreadContext,
) -> crate::kernel::runtime_host::VerletResult<
    Vec<crate::agent::manifest_bind::AgentManifestStaticContextSegment>,
> {
    let Some(raw) = thread
        .metadata
        .get(crate::agent::manifest_bind::THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA)
    else {
        return Ok(Vec::new());
    };
    let segments = serde_json::from_str::<
        Vec<crate::agent::manifest_bind::AgentManifestStaticContextSegment>,
    >(raw)
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "thread manifest static context segments are invalid: {err}"
        ))
    })?;
    for segment in &segments {
        let expected = verlet_agent::contracts::sha256_hex(segment.content.as_bytes());
        if segment.content_sha256 != expected {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "thread manifest static context segment {:?} content hash mismatch: expected {}, got {}",
                    segment.id, expected, segment.content_sha256
                ),
            ));
        }
    }
    Ok(segments)
}

fn context_receipt_static_segments(
    static_context_segments: &[crate::agent::manifest_bind::AgentManifestStaticContextSegment],
    skill_context_segments: &[crate::agent::manifest_bind::AgentManifestStaticContextSegment],
) -> Vec<crate::agent::manifest_bind::AgentManifestStaticContextSegment> {
    static_context_segments
        .iter()
        .chain(skill_context_segments)
        .cloned()
        .collect()
}

fn context_compile_receipt_payload(
    turn_id: &str,
    session_entries: &[verlet_history::SessionEntry],
    compiled_context: &crate::kernel::context_compiler::CompiledAgentContext,
    static_context_segments: &[crate::agent::manifest_bind::AgentManifestStaticContextSegment],
    diagnostics: &crate::kernel::context_compiler::AgentContextCompilationDiagnostics,
    replay_transform: &verlet_provider::provider_transform::ReplayTransformCounts,
    provider_dropped_messages: usize,
    provider_truncated_text_bytes: usize,
    provider_retained_text_bytes: usize,
) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
    let encoded_messages = serde_json::to_vec(&compiled_context.messages).map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "context receipt codec failed: {err}"
        ))
    })?;
    let diagnostics = serde_json::to_value(diagnostics).map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "context receipt codec failed: {err}"
        ))
    })?;
    let replay_transform = serde_json::to_value(replay_transform).map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "context receipt codec failed: {err}"
        ))
    })?;
    let static_context_segments = static_context_segments
        .iter()
        .map(|segment| {
            serde_json::json!({
                "id": &segment.id,
                "assembler": &segment.assembler,
                "input": &segment.input,
                "pinned": segment.pinned,
                "budget_share": segment.budget_share,
                "ref_uri": &segment.ref_uri,
                "content_sha256": &segment.content_sha256,
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "turn_id": turn_id,
        "strategy": "naive_assembly",
        "strategy_version": "v1",
        "session_entry_ids": session_entries
            .iter()
            .map(|entry| entry.entry_id.to_string())
            .collect::<Vec<_>>(),
        "observation_ids": Vec::<String>::new(),
        "system_block_count": compiled_context.system.len(),
        "message_count": compiled_context.messages.len(),
        "tool_count": compiled_context.tools.len(),
        "static_context_segments": static_context_segments,
        "diagnostics": diagnostics,
        "replay_transform": replay_transform,
        "provider_dropped_messages": provider_dropped_messages,
        "provider_truncated_text_bytes": provider_truncated_text_bytes,
        "provider_retained_text_bytes": provider_retained_text_bytes,
        "output_hash": verlet_agent::contracts::sha256_hex(&encoded_messages),
    }))
}

async fn resume_pending_tool_call(
    runtime: &AgentLoop,
    thread_context: &verlet_runtime_contracts::ThreadContext,
    turn_id: &str,
    call_id: &str,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> crate::kernel::runtime_host::VerletResult<ToolResumeOutcome> {
    let Some(tool_router) = &runtime.tool_router else {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            "tool resume requires a tool router".to_string(),
        ));
    };
    let Some(request) =
        pending_tool_call_request(services, &thread_context.coordinates, turn_id, call_id).await?
    else {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!("missing tool.call.requested for pending call {turn_id}/{call_id}"),
        ));
    };
    if matching_tool_call_completed_exists(
        services,
        &thread_context.coordinates,
        turn_id,
        call_id,
        &request.snapshot_id,
        request.args_fingerprint.as_deref(),
    )
    .await?
    {
        return Ok(ToolResumeOutcome::AlreadyCompleted);
    }
    if !tool_call_suspension_exists(services, &thread_context.coordinates, turn_id, call_id).await?
    {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!("no pending suspended tool call {turn_id}/{call_id}"),
        ));
    }
    let turn_submitted =
        existing_turn_submitted_event(services, &thread_context.coordinates, turn_id)
            .await?
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "turn {turn_id} has no persisted context timestamp anchor"
                ))
            })?;
    let turn_anchor_timestamp_ms = turn_submitted.created_at_ms;
    let turn_delivery_start_sequence = turn_submitted.sequence;
    let decision = crate::kernel::control_decision::decide_tool_call(
        services.runtime_store().as_ref(),
        crate::kernel::control_decision::ToolDecisionRequest {
            coordinates: thread_context.coordinates.clone(),
            subject: request.subject.clone(),
            snapshot_id: request.snapshot_id.clone(),
            request_event_id: request.request_event_id,
        },
    )
    .await?;
    let (consumed_fact_id, action) = match decision {
        crate::kernel::control_decision::ToolCallDecision::NoDecision
        | crate::kernel::control_decision::ToolCallDecision::Wait { .. } => {
            return Ok(ToolResumeOutcome::StillWaiting);
        }
        crate::kernel::control_decision::ToolCallDecision::Allow { consumed_fact_id } => (
            consumed_fact_id,
            ResumedToolCallAction::Execute(request.arguments),
        ),
        crate::kernel::control_decision::ToolCallDecision::Rewrite {
            consumed_fact_id,
            arguments,
        } => (consumed_fact_id, ResumedToolCallAction::Execute(arguments)),
        crate::kernel::control_decision::ToolCallDecision::Deny {
            consumed_fact_id,
            reason,
            ..
        } => (
            consumed_fact_id.unwrap_or(request.request_event_id),
            ResumedToolCallAction::Deny(reason),
        ),
    };
    let resumed = append_turn_resumed_event(
        services,
        &thread_context.coordinates,
        turn_id,
        consumed_fact_id,
    )
    .await?;
    let turn_context = runtime.turn_context(
        thread_context,
        turn_id.to_string(),
        &crate::kernel::runtime_host::turn::TurnInput::text(""),
        tokio_util::sync::CancellationToken::new(),
    );
    match action {
        ResumedToolCallAction::Execute(arguments) => {
            let interceptor = crate::agent::tool_interceptor::ToolExecutionInterceptor::new(
                std::sync::Arc::clone(tool_router),
            )
            .with_hook_pipeline(runtime.hook_pipeline.clone())
            .with_permission_gate(std::sync::Arc::clone(&runtime.tool_permission_gate));
            execute_resumed_tool_call_with_interceptor(
                &interceptor,
                services,
                &turn_context,
                thread_id,
                events,
                call_id.to_string(),
                request.tool_name,
                arguments,
                request.holds,
                request.snapshot_id,
                request.args_fingerprint,
                request.request_event_id,
            )
            .await?;
        }
        ResumedToolCallAction::Deny(reason) => {
            append_denied_tool_result(
                services,
                &turn_context,
                thread_id,
                events,
                call_id.to_string(),
                request.tool_name,
                request.snapshot_id,
                request.args_fingerprint,
                reason,
                Some(0),
                None,
                request.request_event_id,
            )
            .await?;
        }
    }
    if !tool_call_batch_completed(
        services,
        &thread_context.coordinates,
        turn_id,
        request.assistant_source_event_id,
    )
    .await?
    {
        return Ok(ToolResumeOutcome::StillWaiting);
    }
    Ok(ToolResumeOutcome::Resumed {
        source_event_id: resumed.id,
        turn_delivery_start_sequence,
        turn_anchor_timestamp_ms,
    })
}

enum ToolBatchAwaitOutcome {
    Completed(crate::kernel::runtime_host::VerletResult<ToolAppendOutcome>),
    Cancelled { reason: String },
    Shutdown,
    Failed { code: &'static str, reason: String },
}

fn tool_batch_command_must_wait_for_commit(
    command: &Option<crate::kernel::runtime_host::runtime_api::ThreadCommand>,
) -> bool {
    !matches!(
        command,
        Some(
            crate::kernel::runtime_host::runtime_api::ThreadCommand::Submit {
                mode: verlet_runtime_contracts::TurnSubmissionMode::Queue,
                ..
            }
        ) | Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::Compact { .. })
            | Some(crate::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall { .. })
    )
}

#[allow(clippy::too_many_arguments)]
async fn handle_tool_batch_command(
    command: Option<crate::kernel::runtime_host::runtime_api::ThreadCommand>,
    turn_input: &crate::kernel::runtime_host::turn::TurnInput,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    turn_source_event_id: verlet_history::EventRecordId,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    status: &tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    pending_commands: &mut std::collections::VecDeque<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
    defer_cancel_terminal: bool,
) -> Option<ToolBatchAwaitOutcome> {
    match handle_active_provider_command(
        command,
        turn_input,
        turn_context,
        turn_source_event_id,
        coordinates,
        services,
        thread_id,
        events,
        status,
        pending_commands,
        &turn_context.cancellation,
        false,
        defer_cancel_terminal,
    )
    .await
    {
        ActiveProviderCommandOutcome::Continue => None,
        ActiveProviderCommandOutcome::Cancelled { reason } => {
            Some(ToolBatchAwaitOutcome::Cancelled { reason })
        }
        ActiveProviderCommandOutcome::Shutdown => Some(ToolBatchAwaitOutcome::Shutdown),
        ActiveProviderCommandOutcome::Failed { code, reason } => {
            Some(ToolBatchAwaitOutcome::Failed { code, reason })
        }
    }
}

async fn append_deferred_tool_batch_terminal(
    outcome: &ToolBatchAwaitOutcome,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    turn_source_event_id: verlet_history::EventRecordId,
) {
    let (terminal_state, reason) = match outcome {
        ToolBatchAwaitOutcome::Cancelled { reason } => (
            verlet_history::ThreadTerminalState::Cancelled,
            Some(reason.clone()),
        ),
        ToolBatchAwaitOutcome::Failed { reason, .. } => (
            verlet_history::ThreadTerminalState::Failed,
            Some(reason.clone()),
        ),
        ToolBatchAwaitOutcome::Shutdown | ToolBatchAwaitOutcome::Completed(_) => return,
    };
    append_terminal_join_until_recorded(
        services,
        &turn_context.thread,
        terminal_state,
        reason,
        Some(turn_source_event_id),
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn append_tool_results_while_handling_commands(
    runtime: &AgentLoop,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    tool_calls: Vec<ProviderToolCall>,
    assistant_entry_id: verlet_history::SessionEntryId,
    turn_input: &crate::kernel::runtime_host::turn::TurnInput,
    turn_source_event_id: verlet_history::EventRecordId,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    status: &tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    commands: &mut tokio::sync::mpsc::Receiver<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
    runtime_cancellation: &tokio_util::sync::CancellationToken,
    pending_commands: &mut std::collections::VecDeque<
        crate::kernel::runtime_host::runtime_api::ThreadCommand,
    >,
    cancellation_grace: std::time::Duration,
) -> ToolBatchAwaitOutcome {
    let preparation = prepare_tool_results(
        runtime,
        turn_context,
        services,
        events,
        tool_calls,
        assistant_entry_id,
        cancellation_grace,
    );
    tokio::pin!(preparation);
    let batch = loop {
        tokio::select! {
            biased;
            _ = runtime_cancellation.cancelled() => {
                let reason = "runtime cancellation requested".to_string();
                turn_context.cancellation.cancel();
                let batch = match preparation.as_mut().await {
                    Ok(batch) => batch,
                    Err(err) => return ToolBatchAwaitOutcome::Completed(Err(err)),
                };
                if let Err(err) = append_tool_results(
                    turn_context,
                    services,
                    thread_id,
                    events,
                    batch,
                )
                .await
                {
                    return ToolBatchAwaitOutcome::Completed(Err(err));
                }
                append_terminal_join_until_recorded(
                    services,
                    &turn_context.thread,
                    verlet_history::ThreadTerminalState::Cancelled,
                    Some(reason.clone()),
                    Some(turn_source_event_id),
                )
                .await;
                return ToolBatchAwaitOutcome::Cancelled { reason };
            }
            command = commands.recv() => {
                if let Some(outcome) = handle_tool_batch_command(
                    command,
                    turn_input,
                    turn_context,
                    turn_source_event_id,
                    coordinates,
                    services,
                    thread_id,
                    events,
                    status,
                    pending_commands,
                    true,
                )
                .await
                {
                    let batch = match preparation.as_mut().await {
                        Ok(batch) => batch,
                        Err(err) => return ToolBatchAwaitOutcome::Completed(Err(err)),
                    };
                    if let Err(err) = append_tool_results(
                        turn_context,
                        services,
                        thread_id,
                        events,
                        batch,
                    )
                    .await
                    {
                        return ToolBatchAwaitOutcome::Completed(Err(err));
                    }
                    append_deferred_tool_batch_terminal(
                        &outcome,
                        services,
                        turn_context,
                        turn_source_event_id,
                    )
                    .await;
                    return outcome;
                }
            }
            result = &mut preparation => match result {
                Ok(batch) => break batch,
                Err(err) => return ToolBatchAwaitOutcome::Completed(Err(err)),
            },
        }
    };

    let commit = append_tool_results(turn_context, services, thread_id, events, batch);
    tokio::pin!(commit);
    loop {
        tokio::select! {
            biased;
            _ = runtime_cancellation.cancelled() => {
                let result = commit.as_mut().await;
                if let Err(err) = result {
                    return ToolBatchAwaitOutcome::Completed(Err(err));
                }
                let reason = "runtime cancellation requested".to_string();
                turn_context.cancellation.cancel();
                append_terminal_join_until_recorded(
                    services,
                    &turn_context.thread,
                    verlet_history::ThreadTerminalState::Cancelled,
                    Some(reason.clone()),
                    Some(turn_source_event_id),
                )
                .await;
                return ToolBatchAwaitOutcome::Cancelled { reason };
            }
            command = commands.recv() => {
                if tool_batch_command_must_wait_for_commit(&command) {
                    let committed = match commit.as_mut().await {
                        Ok(outcome) => outcome,
                        Err(err) => return ToolBatchAwaitOutcome::Completed(Err(err)),
                    };
                    return handle_tool_batch_command(
                        command,
                        turn_input,
                        turn_context,
                        turn_source_event_id,
                        coordinates,
                        services,
                        thread_id,
                        events,
                        status,
                        pending_commands,
                        false,
                    )
                    .await
                    .unwrap_or(ToolBatchAwaitOutcome::Completed(Ok(committed)));
                }
                if let Some(outcome) = handle_tool_batch_command(
                    command,
                    turn_input,
                    turn_context,
                    turn_source_event_id,
                    coordinates,
                    services,
                    thread_id,
                    events,
                    status,
                    pending_commands,
                    false,
                )
                .await
                {
                    let result = commit.as_mut().await;
                    return match result {
                        Ok(_) => outcome,
                        Err(err) => ToolBatchAwaitOutcome::Completed(Err(err)),
                    };
                }
            }
            result = &mut commit => return ToolBatchAwaitOutcome::Completed(result),
        }
    }
}

async fn prepare_tool_results(
    runtime: &AgentLoop,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    tool_calls: Vec<ProviderToolCall>,
    assistant_entry_id: verlet_history::SessionEntryId,
    cancellation_grace: std::time::Duration,
) -> crate::kernel::runtime_host::VerletResult<PreparedToolBatch> {
    let Some(tool_router) = &runtime.tool_router else {
        return Ok(PreparedToolBatch::NoTools);
    };
    let mut call_ids = std::collections::HashSet::new();
    for tool_call in &tool_calls {
        if !call_ids.insert(tool_call.id.as_str()) {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                format!(
                    "duplicate tool call id {:?} in one assistant batch",
                    tool_call.id
                ),
            ));
        }
    }
    let tool_calls = if runtime.strict_tool_router_unknowns {
        tool_calls
    } else {
        let tool_names = tool_router
            .tool_definitions()
            .await
            .into_iter()
            .map(|tool| tool.name)
            .collect::<std::collections::BTreeSet<_>>();
        tool_calls
            .into_iter()
            .filter(|tool_call| tool_names.contains(&tool_call.name))
            .collect::<Vec<_>>()
    };
    if tool_calls.is_empty() {
        return Ok(PreparedToolBatch::NoTools);
    }
    let planned_wait_edges = tool_holds::plan_tool_call_batch(&tool_calls);
    let interceptor = crate::agent::tool_interceptor::ToolExecutionInterceptor::new(
        std::sync::Arc::clone(tool_router),
    )
    .with_hook_pipeline(runtime.hook_pipeline.clone())
    .with_permission_gate(std::sync::Arc::clone(&runtime.tool_permission_gate));
    let active_snapshot_id = crate::kernel::control_decision::active_manifest_bind_receipt(
        services.runtime_store().as_ref(),
        turn_context.coordinates(),
    )
    .await?
    .map(|(_, receipt)| receipt.manifest_hash)
    .unwrap_or_else(|| "unbound".to_string());
    let calls_with_snapshots = tool_calls
        .into_iter()
        .map(|tool_call| (tool_call, active_snapshot_id.clone()))
        .collect::<Vec<_>>();
    let request_events = append_tool_call_requested_events(
        services,
        turn_context,
        &calls_with_snapshots,
        assistant_entry_id,
    )
    .await?;
    let mut witnessed_calls = Vec::with_capacity(calls_with_snapshots.len());
    for ((tool_call, active_snapshot_id), request_event) in
        calls_with_snapshots.into_iter().zip(request_events)
    {
        let holds = decode_witnessed_tool_holds(&request_event)?;
        let request_payload = serde_json::from_value::<
            crate::kernel::control_decision::ToolCallRequestedPayload,
        >(request_event.payload.clone())
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool.call.requested payload is invalid: {err}"
            ))
        })?;
        witnessed_calls.push(WitnessedToolCall {
            tool_call,
            snapshot_id: active_snapshot_id,
            args_fingerprint: request_payload.args_fingerprint,
            request_event_id: request_event.id,
            holds,
            recovery_action: ToolRecoveryAction::Reexecute,
            recovery_source_event_id: None,
            recovery_fingerprint_mismatch: false,
        });
    }
    apply_tool_recovery_actions(
        services,
        turn_context.coordinates(),
        &turn_context.turn_id,
        &mut witnessed_calls,
    )
    .await?;
    let witnessed_wait_edges = tool_holds::batch_wait_edges(
        &witnessed_calls
            .iter()
            .map(|call| call.holds.clone())
            .collect::<Vec<_>>(),
    );
    if witnessed_wait_edges != planned_wait_edges {
        return Err(crate::kernel::runtime_host::VerletError::History(
            "witnessed tool holds disagree with the planned batch schedule".to_string(),
        ));
    }

    Ok(PreparedToolBatch::Outcomes(
        execute_tool_call_batch(
            &interceptor,
            services,
            turn_context,
            events,
            witnessed_calls,
            witnessed_wait_edges,
            cancellation_grace,
        )
        .await,
    ))
}

async fn append_tool_results(
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    batch: PreparedToolBatch,
) -> crate::kernel::runtime_host::VerletResult<ToolAppendOutcome> {
    let PreparedToolBatch::Outcomes(outcomes) = batch else {
        return Ok(ToolAppendOutcome::NoTools);
    };
    let mut suspended = false;
    let mut first_error = None;
    for outcome in outcomes {
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
                continue;
            }
        };
        match outcome {
            PreparedToolCallOutcome::Completed {
                call_id,
                tool_name,
                snapshot_id,
                args_fingerprint,
                source_event_id,
                finish_order,
                cancellation,
                outcome,
            } => {
                append_tool_execution_outcome(
                    services,
                    turn_context,
                    thread_id,
                    events,
                    call_id,
                    tool_name,
                    snapshot_id,
                    args_fingerprint,
                    source_event_id,
                    Some(finish_order),
                    cancellation,
                    *outcome,
                    false,
                )
                .await?;
            }
            PreparedToolCallOutcome::Denied {
                call_id,
                tool_name,
                snapshot_id,
                args_fingerprint,
                source_event_id,
                finish_order,
                reason,
            } => {
                append_denied_tool_result(
                    services,
                    turn_context,
                    thread_id,
                    events,
                    call_id,
                    tool_name,
                    snapshot_id,
                    args_fingerprint,
                    reason,
                    Some(finish_order),
                    None,
                    source_event_id,
                )
                .await?;
            }
            PreparedToolCallOutcome::Suspended {
                call_id,
                snapshot_id,
                waiting_on_event_id,
                approval_id,
                reason,
            } => {
                append_turn_waiting_event(
                    services,
                    turn_context,
                    &call_id,
                    &snapshot_id,
                    waiting_on_event_id,
                    approval_id,
                    reason,
                )
                .await?;
                suspended = true;
            }
            PreparedToolCallOutcome::Abandoned => {}
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(if suspended {
        ToolAppendOutcome::Suspended
    } else {
        ToolAppendOutcome::AppendedResults
    })
}

fn decode_witnessed_tool_holds(
    request_event: &verlet_history::EventRecord,
) -> crate::kernel::runtime_host::VerletResult<Vec<tool_holds::ToolHold>> {
    serde_json::from_value(
        request_event
            .payload
            .get("holds")
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Array(Vec::new())),
    )
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "tool hold payload is invalid: {err}"
        ))
    })
}

type ToolInvocationTaskOutput = (
    crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome>,
    bool,
);

#[allow(clippy::too_many_arguments)]
async fn run_owned_tool_invocation(
    interceptor: crate::agent::tool_interceptor::ToolExecutionInterceptor,
    services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: crate::kernel::runtime_host::turn::TurnContext,
    events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    call: WitnessedToolCall,
    witness: WitnessedToolCall,
    finish_order: std::sync::Arc<std::sync::atomic::AtomicU64>,
    cancellation: crate::agent::agent_tool_router::ToolInvocationCancellation,
    settlement: std::sync::Arc<std::sync::atomic::AtomicU8>,
) -> ToolInvocationTaskOutput {
    let thread_id = turn_context.coordinates().thread_id;
    let outcome = std::panic::AssertUnwindSafe(prepare_tool_call(
        &interceptor,
        &services,
        &turn_context,
        &events,
        call,
        std::sync::Arc::clone(&finish_order),
        cancellation.clone(),
    ))
    .catch_unwind()
    .await
    .unwrap_or_else(|panic| {
        Ok(failed_tool_call_outcome(
            &witness,
            finish_order.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            &format!(
                "tool invocation panicked: {}",
                panic_payload_message(&panic)
            ),
        ))
    });
    let cancelled_at_settlement = cancellation.is_cancelled();
    match settlement.compare_exchange(
        TOOL_INVOCATION_AWAITED,
        TOOL_INVOCATION_SETTLED,
        std::sync::atomic::Ordering::SeqCst,
        std::sync::atomic::Ordering::SeqCst,
    ) {
        Ok(_) => (outcome, cancelled_at_settlement),
        Err(TOOL_INVOCATION_ABANDONED) => {
            let detached = settle_prepared_for_cancellation(
                outcome,
                &witness,
                &finish_order,
                crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace,
            );
            append_detached_tool_call_outcome_until_recorded(
                &services,
                &turn_context,
                thread_id,
                &events,
                detached,
            )
            .await;
            (Ok(PreparedToolCallOutcome::Abandoned), true)
        }
        Err(state) => (
            Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                format!("tool invocation entered unexpected settlement state {state}"),
            )),
            cancelled_at_settlement,
        ),
    }
}

async fn settled_tool_invocation(
    joined: Result<ToolInvocationTaskOutput, tokio::task::JoinError>,
    witness: &WitnessedToolCall,
    finish_order: &std::sync::Arc<std::sync::atomic::AtomicU64>,
    cancellation: &crate::agent::agent_tool_router::ToolInvocationCancellation,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> ToolCallMonitorOutcome {
    let cancellation_observed = cancellation.is_cancelled();
    let outcome = match joined {
        Ok((outcome, cancelled_at_settlement))
            if cancelled_at_settlement || cancellation_observed =>
        {
            settle_prepared_for_cancellation(
                outcome,
                witness,
                finish_order,
                crate::kernel::control_decision::ToolCallCancellation::CancelledAcknowledged,
            )
        }
        Ok((outcome, _)) => outcome,
        Err(err) if cancellation_observed => Ok(cancelled_tool_call_outcome(
            witness,
            finish_order.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            crate::kernel::control_decision::ToolCallCancellation::CancelledAcknowledged,
            &format!("cancelled tool invocation task failed: {err}"),
        )),
        Err(err) => Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!("tool invocation task failed: {err}"),
        )),
    };
    if cancellation_observed {
        append_detached_tool_call_outcome_until_recorded(
            services,
            turn_context,
            turn_context.coordinates().thread_id,
            events,
            outcome,
        )
        .await;
        return ToolCallMonitorOutcome::Abandoned;
    }
    ToolCallMonitorOutcome::Settled(outcome)
}

async fn monitor_owned_tool_invocation_inner(
    index: usize,
    mut invocation: tokio::task::JoinHandle<ToolInvocationTaskOutput>,
    witness: WitnessedToolCall,
    finish_order: std::sync::Arc<std::sync::atomic::AtomicU64>,
    cancellation: crate::agent::agent_tool_router::ToolInvocationCancellation,
    settlement: std::sync::Arc<std::sync::atomic::AtomicU8>,
    services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: crate::kernel::runtime_host::turn::TurnContext,
    events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> (usize, ToolCallMonitorOutcome) {
    let outcome = tokio::select! {
        joined = &mut invocation => {
            settled_tool_invocation(
                joined,
                &witness,
                &finish_order,
                &cancellation,
                &services,
                &turn_context,
                &events,
            ).await
        }
        _ = cancellation.cancelled_then_grace_elapsed() => {
            match settlement.compare_exchange(
                TOOL_INVOCATION_AWAITED,
                TOOL_INVOCATION_ABANDONED,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => ToolCallMonitorOutcome::Abandoned,
                Err(TOOL_INVOCATION_SETTLED) => {
                    settled_tool_invocation(
                        invocation.await,
                        &witness,
                        &finish_order,
                        &cancellation,
                        &services,
                        &turn_context,
                        &events,
                    ).await
                },
                Err(state) => ToolCallMonitorOutcome::Settled(Err(
                    crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                        "tool invocation monitor entered unexpected settlement state {state}"
                    )),
                )),
            }
        }
    };
    (index, outcome)
}

#[allow(clippy::too_many_arguments)]
async fn monitor_owned_tool_invocation(
    index: usize,
    invocation: tokio::task::JoinHandle<ToolInvocationTaskOutput>,
    witness: WitnessedToolCall,
    finish_order: std::sync::Arc<std::sync::atomic::AtomicU64>,
    cancellation: crate::agent::agent_tool_router::ToolInvocationCancellation,
    settlement: std::sync::Arc<std::sync::atomic::AtomicU8>,
    services: crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: crate::kernel::runtime_host::turn::TurnContext,
    events: tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> (usize, ToolCallMonitorOutcome) {
    let recovery_witness = witness.clone();
    let recovery_finish_order = std::sync::Arc::clone(&finish_order);
    let recovery_cancellation = cancellation.clone();
    let recovery_settlement = std::sync::Arc::clone(&settlement);
    let recovery_services = services.clone();
    let recovery_turn_context = turn_context.clone();
    let recovery_events = events.clone();
    match std::panic::AssertUnwindSafe(monitor_owned_tool_invocation_inner(
        index,
        invocation,
        witness,
        finish_order,
        cancellation,
        settlement,
        services,
        turn_context,
        events,
    ))
    .catch_unwind()
    .await
    {
        Ok(outcome) => outcome,
        Err(panic) => {
            let message = panic_payload_message(&panic);
            crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                &recovery_events,
                recovery_turn_context.coordinates(),
                crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
                    action: "recover_tool_invocation_monitor_panic".to_string(),
                    reason: format!(
                        "{}/{}: {message}",
                        recovery_witness.tool_call.id, recovery_witness.tool_call.name
                    ),
                },
            );
            eprintln!(
                "verlet tool invocation monitor panicked for {}/{}: {message}",
                recovery_witness.tool_call.id, recovery_witness.tool_call.name
            );
            match recovery_settlement.compare_exchange(
                TOOL_INVOCATION_AWAITED,
                TOOL_INVOCATION_SETTLED,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Err(TOOL_INVOCATION_ABANDONED) => {}
                Ok(_) | Err(TOOL_INVOCATION_SETTLED) => {
                    let outcome = if recovery_cancellation.is_cancelled() {
                        cancelled_tool_call_outcome(
                            &recovery_witness,
                            recovery_finish_order.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                            crate::kernel::control_decision::ToolCallCancellation::CancelledAcknowledged,
                            "cancelled tool invocation monitor panicked",
                        )
                    } else {
                        failed_tool_call_outcome(
                            &recovery_witness,
                            recovery_finish_order.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                            "tool invocation monitor panicked",
                        )
                    };
                    append_detached_tool_call_outcome_until_recorded(
                        &recovery_services,
                        &recovery_turn_context,
                        recovery_turn_context.coordinates().thread_id,
                        &recovery_events,
                        Ok(outcome),
                    )
                    .await;
                }
                Err(state) => {
                    eprintln!(
                        "verlet tool invocation monitor recovery found unexpected settlement state {state}"
                    );
                }
            }
            (index, ToolCallMonitorOutcome::Abandoned)
        }
    }
}

/// Runs each launched invocation in an owned task and only awaits monitor
/// handles. Dropping the batch future therefore cannot cancel an invocation in
/// the middle of its own completion write.
async fn execute_tool_call_batch(
    interceptor: &crate::agent::tool_interceptor::ToolExecutionInterceptor,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    calls: Vec<WitnessedToolCall>,
    wait_edges: Vec<Vec<usize>>,
    cancellation_grace: std::time::Duration,
) -> Vec<crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome>> {
    let call_count = calls.len();
    let witnesses = calls.clone();
    let mut calls = calls.into_iter().map(Some).collect::<Vec<_>>();
    let mut launched = vec![false; call_count];
    let mut completed = vec![false; call_count];
    let mut outcomes = (0..call_count).map(|_| None).collect::<Vec<_>>();
    let finish_order = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut running = futures_util::stream::FuturesUnordered::new();
    let mut completed_count = 0;

    while completed_count < call_count {
        if turn_context.cancellation.is_cancelled() {
            for index in 0..call_count {
                if launched[index] {
                    continue;
                }
                launched[index] = true;
                completed[index] = true;
                completed_count += 1;
                outcomes[index] = Some(Ok(cancelled_tool_call_outcome(
                    &witnesses[index],
                    finish_order.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    crate::kernel::control_decision::ToolCallCancellation::CancelledAcknowledged,
                    "tool call was cancelled before its hold dependencies released",
                )));
            }
        }

        for index in 0..call_count {
            if launched[index]
                || !wait_edges[index]
                    .iter()
                    .all(|dependency| completed[*dependency])
            {
                continue;
            }
            launched[index] = true;
            let Some(call) = calls[index].take() else {
                completed[index] = true;
                completed_count += 1;
                outcomes[index] = Some(Err(
                    crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                        "tool batch scheduler lost call {index} before launch"
                    )),
                ));
                continue;
            };
            let witness = witnesses[index].clone();
            let invocation_cancellation =
                crate::agent::agent_tool_router::ToolInvocationCancellation::new(
                    turn_context.cancellation.child_token(),
                    cancellation_grace,
                );
            let settlement =
                std::sync::Arc::new(std::sync::atomic::AtomicU8::new(TOOL_INVOCATION_AWAITED));
            let invocation = tokio::spawn(run_owned_tool_invocation(
                interceptor.clone(),
                services.clone(),
                turn_context.clone(),
                events.clone(),
                call,
                witness.clone(),
                std::sync::Arc::clone(&finish_order),
                invocation_cancellation.clone(),
                std::sync::Arc::clone(&settlement),
            ));
            running.push(tokio::spawn(monitor_owned_tool_invocation(
                index,
                invocation,
                witness,
                std::sync::Arc::clone(&finish_order),
                invocation_cancellation,
                settlement,
                services.clone(),
                turn_context.clone(),
                events.clone(),
            )));
        }

        let Some(joined) = running.next().await else {
            break;
        };
        let (index, outcome) = match joined {
            Ok(settled) => settled,
            Err(err) => {
                return witnesses
                    .iter()
                    .enumerate()
                    .map(|(index, witness)| {
                        if completed[index] {
                            match outcomes[index].take() {
                                Some(outcome) => outcome,
                                None => Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                                    "tool batch scheduler lost the completed outcome for {}/{}",
                                    witness.tool_call.id, witness.tool_call.name,
                                ))),
                            }
                        } else {
                            Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                                "tool invocation monitor failed for {}/{}: {err}",
                                witness.tool_call.id, witness.tool_call.name,
                            )))
                        }
                    })
                    .collect();
            }
        };
        completed[index] = true;
        completed_count += 1;
        outcomes[index] = Some(match outcome {
            ToolCallMonitorOutcome::Settled(outcome) => outcome,
            ToolCallMonitorOutcome::Abandoned => Ok(PreparedToolCallOutcome::Abandoned),
        });
    }

    outcomes
        .into_iter()
        .enumerate()
        .map(|(index, outcome)| {
            outcome.unwrap_or_else(|| {
                Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    format!("tool batch scheduler did not execute call {index}"),
                ))
            })
        })
        .collect()
}

async fn prepare_tool_call(
    interceptor: &crate::agent::tool_interceptor::ToolExecutionInterceptor,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    call: WitnessedToolCall,
    finish_counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
    cancellation: crate::agent::agent_tool_router::ToolInvocationCancellation,
) -> crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome> {
    match call.recovery_action {
        ToolRecoveryAction::Reuse => {
            let source_event_id = call.recovery_source_event_id.ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "recorded tool outcome for {}/{} has no reusable canonical result",
                    turn_context.turn_id, call.tool_call.id
                ))
            })?;
            let result = existing_tool_result_message(
                services,
                turn_context.coordinates(),
                source_event_id,
                &call.tool_call.id,
                &call.snapshot_id,
                call.args_fingerprint.as_deref(),
            )
            .await?
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "recorded tool outcome for {}/{} lost its canonical result",
                    turn_context.turn_id, call.tool_call.id
                ))
            })?;
            return Ok(PreparedToolCallOutcome::Completed {
                call_id: call.tool_call.id,
                tool_name: call.tool_call.name,
                snapshot_id: call.snapshot_id,
                args_fingerprint: call.args_fingerprint,
                source_event_id: call.request_event_id,
                finish_order: finish_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                cancellation: None,
                outcome: Box::new(crate::agent::tool_interceptor::ToolExecutionOutcome {
                    result,
                    hook_records: Vec::new(),
                    pre_model_contexts: Vec::new(),
                    post_model_contexts: Vec::new(),
                    permission_decision: None,
                    duration_ms: 0,
                }),
            });
        }
        ToolRecoveryAction::ConservativeFailure => {
            let reason = if call.recovery_fingerprint_mismatch {
                "tool invocation recovery found a fingerprint mismatch; effect class at-most-once forbids automatic re-execution"
            } else {
                "tool invocation was interrupted before a witnessed outcome; effect class at-most-once forbids automatic re-execution"
            };
            return Ok(failed_tool_call_outcome(
                &call,
                finish_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                reason,
            ));
        }
        ToolRecoveryAction::Reexecute => {}
    }
    let call_id = call.tool_call.id;
    let tool_name = call.tool_call.name;
    let mut arguments = call.tool_call.arguments;
    let args_fingerprint = call.args_fingerprint;
    let controller = crate::kernel::control_decision::active_tool_controller_for_request(
        services.runtime_store().as_ref(),
        turn_context.coordinates(),
        &tool_name,
    )
    .await?;
    if let Some(controller) = controller {
        match crate::kernel::control_decision::decide_tool_call(
            services.runtime_store().as_ref(),
            crate::kernel::control_decision::ToolDecisionRequest {
                coordinates: turn_context.coordinates().clone(),
                subject: crate::kernel::control_decision::ToolCallSubject {
                    turn_id: turn_context.turn_id.clone(),
                    call_id: call_id.clone(),
                },
                snapshot_id: controller.snapshot_id,
                request_event_id: call.request_event_id,
            },
        )
        .await?
        {
            crate::kernel::control_decision::ToolCallDecision::NoDecision => {
                return Ok(PreparedToolCallOutcome::Denied {
                    call_id,
                    tool_name,
                    snapshot_id: call.snapshot_id,
                    args_fingerprint,
                    source_event_id: call.request_event_id,
                    finish_order: finish_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    reason: "tool controller did not emit a terminal decision".to_string(),
                });
            }
            crate::kernel::control_decision::ToolCallDecision::Allow { .. } => {}
            crate::kernel::control_decision::ToolCallDecision::Rewrite {
                arguments: rewritten,
                ..
            } => arguments = rewritten,
            crate::kernel::control_decision::ToolCallDecision::Deny { reason, .. } => {
                return Ok(PreparedToolCallOutcome::Denied {
                    call_id,
                    tool_name,
                    snapshot_id: call.snapshot_id,
                    args_fingerprint,
                    source_event_id: call.request_event_id,
                    finish_order: finish_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    reason,
                });
            }
            crate::kernel::control_decision::ToolCallDecision::Wait {
                consumed_fact_id,
                approval_id,
                reason,
            } => {
                return Ok(PreparedToolCallOutcome::Suspended {
                    call_id,
                    snapshot_id: call.snapshot_id,
                    waiting_on_event_id: consumed_fact_id,
                    approval_id,
                    reason,
                });
            }
        }
    }

    let outcome = execute_tool_call_with_interceptor(
        interceptor,
        services,
        turn_context,
        events,
        &call_id,
        &tool_name,
        arguments,
        cancellation,
    )
    .await?;
    Ok(PreparedToolCallOutcome::Completed {
        call_id,
        tool_name,
        snapshot_id: call.snapshot_id,
        args_fingerprint,
        source_event_id: call.request_event_id,
        finish_order: finish_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        cancellation: None,
        outcome: Box::new(outcome),
    })
}

fn settle_prepared_for_cancellation(
    outcome: crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome>,
    witness: &WitnessedToolCall,
    finish_counter: &std::sync::atomic::AtomicU64,
    cancellation: crate::kernel::control_decision::ToolCallCancellation,
) -> crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome> {
    match outcome {
        Ok(PreparedToolCallOutcome::Completed {
            call_id,
            tool_name,
            snapshot_id,
            args_fingerprint,
            source_event_id,
            finish_order,
            outcome,
            ..
        }) => Ok(PreparedToolCallOutcome::Completed {
            call_id,
            tool_name,
            snapshot_id,
            args_fingerprint,
            source_event_id,
            finish_order,
            cancellation: Some(cancellation),
            outcome,
        }),
        Ok(PreparedToolCallOutcome::Denied {
            finish_order,
            reason,
            ..
        }) => Ok(cancelled_tool_call_outcome(
            witness,
            finish_order,
            cancellation,
            &reason,
        )),
        Ok(PreparedToolCallOutcome::Suspended { reason, .. }) => Ok(cancelled_tool_call_outcome(
            witness,
            finish_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            cancellation,
            reason
                .as_deref()
                .unwrap_or("tool call was cancelled while awaiting a controller decision"),
        )),
        Ok(PreparedToolCallOutcome::Abandoned) => Ok(PreparedToolCallOutcome::Abandoned),
        Err(err) => Ok(cancelled_tool_call_outcome(
            witness,
            finish_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            cancellation,
            &format!("cancelled tool invocation failed: {err}"),
        )),
    }
}

fn cancelled_tool_call_outcome(
    witness: &WitnessedToolCall,
    finish_order: u64,
    cancellation: crate::kernel::control_decision::ToolCallCancellation,
    reason: &str,
) -> PreparedToolCallOutcome {
    PreparedToolCallOutcome::Completed {
        call_id: witness.tool_call.id.clone(),
        tool_name: witness.tool_call.name.clone(),
        snapshot_id: witness.snapshot_id.clone(),
        args_fingerprint: witness.args_fingerprint.clone(),
        source_event_id: witness.request_event_id,
        finish_order,
        cancellation: Some(cancellation),
        outcome: Box::new(crate::agent::tool_interceptor::ToolExecutionOutcome {
            result: verlet_history::CanonicalMessage::tool_result(
                witness.tool_call.id.clone(),
                witness.tool_call.name.clone(),
                reason,
                true,
            ),
            hook_records: Vec::new(),
            pre_model_contexts: Vec::new(),
            post_model_contexts: Vec::new(),
            permission_decision: None,
            duration_ms: 0,
        }),
    }
}

fn failed_tool_call_outcome(
    witness: &WitnessedToolCall,
    finish_order: u64,
    reason: &str,
) -> PreparedToolCallOutcome {
    let mut outcome = cancelled_tool_call_outcome(
        witness,
        finish_order,
        crate::kernel::control_decision::ToolCallCancellation::CancelledAcknowledged,
        reason,
    );
    if let PreparedToolCallOutcome::Completed { cancellation, .. } = &mut outcome {
        *cancellation = None;
    }
    outcome
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    "non-string panic payload".to_string()
}

/// Appends the completion from inside an invocation that outlived its grace.
/// All witnessing inputs are captured before the batch monitor can abandon it.
async fn append_detached_tool_call_outcome(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    outcome: crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let PreparedToolCallOutcome::Completed {
        call_id,
        tool_name,
        snapshot_id,
        args_fingerprint,
        source_event_id,
        finish_order,
        cancellation,
        outcome,
    } = outcome?
    else {
        return Ok(());
    };
    append_tool_execution_outcome(
        services,
        turn_context,
        thread_id,
        events,
        call_id,
        tool_name,
        snapshot_id,
        args_fingerprint,
        source_event_id,
        Some(finish_order),
        cancellation,
        *outcome,
        true,
    )
    .await
}

async fn append_detached_tool_call_outcome_until_recorded(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    outcome: crate::kernel::runtime_host::VerletResult<PreparedToolCallOutcome>,
) {
    let prepared = match outcome {
        Ok(prepared @ PreparedToolCallOutcome::Completed { .. }) => prepared,
        Ok(_) => return,
        Err(err) => {
            eprintln!("verlet detached tool completion was not prepared: {err}");
            return;
        }
    };
    let PreparedToolCallOutcome::Completed {
        ref call_id,
        ref tool_name,
        ref snapshot_id,
        ref args_fingerprint,
        ..
    } = prepared
    else {
        return;
    };
    loop {
        match matching_tool_call_completed_exists(
            services,
            turn_context.coordinates(),
            &turn_context.turn_id,
            call_id,
            snapshot_id,
            args_fingerprint.as_deref(),
        )
        .await
        {
            Ok(true) => return,
            Ok(false) => {}
            Err(err) => {
                report_detached_completion_retry(
                    events,
                    turn_context.coordinates(),
                    call_id,
                    tool_name,
                    &err,
                );
                tokio::time::sleep(DETACHED_COMPLETION_RETRY_DELAY).await;
                continue;
            }
        }
        match append_detached_tool_call_outcome(
            services,
            turn_context,
            thread_id,
            events,
            Ok(prepared.clone()),
        )
        .await
        {
            Ok(()) => return,
            Err(err) => {
                report_detached_completion_retry(
                    events,
                    turn_context.coordinates(),
                    call_id,
                    tool_name,
                    &err,
                );
                tokio::time::sleep(DETACHED_COMPLETION_RETRY_DELAY).await;
            }
        }
    }
}

fn report_detached_completion_retry(
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    call_id: &str,
    tool_name: &str,
    error: &crate::kernel::runtime_host::VerletError,
) {
    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
        events,
        coordinates,
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Recovery {
            action: "retry_detached_tool_completion".to_string(),
            reason: format!("{call_id}/{tool_name}: {error}"),
        },
    );
    eprintln!("verlet detached tool completion append failed for {call_id}/{tool_name}: {error}");
}

#[allow(clippy::too_many_arguments)]
async fn execute_resumed_tool_call_with_interceptor(
    interceptor: &crate::agent::tool_interceptor::ToolExecutionInterceptor,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    call_id: String,
    tool_name: String,
    arguments: serde_json::Value,
    holds: Vec<tool_holds::ToolHold>,
    snapshot_id: String,
    args_fingerprint: Option<String>,
    source_event_id: verlet_history::EventRecordId,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let wait_edges = tool_holds::batch_wait_edges(&[holds]);
    if wait_edges != [Vec::<usize>::new()] {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            "single resumed tool call produced an invalid hold schedule".to_string(),
        ));
    }
    let outcome = execute_tool_call_with_interceptor(
        interceptor,
        services,
        turn_context,
        events,
        &call_id,
        &tool_name,
        arguments,
        crate::agent::agent_tool_router::ToolInvocationCancellation::never(),
    )
    .await?;
    append_tool_execution_outcome(
        services,
        turn_context,
        thread_id,
        events,
        call_id,
        tool_name,
        snapshot_id,
        args_fingerprint,
        source_event_id,
        Some(0),
        None,
        outcome,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_tool_call_with_interceptor(
    interceptor: &crate::agent::tool_interceptor::ToolExecutionInterceptor,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    call_id: &str,
    tool_name: &str,
    arguments: serde_json::Value,
    cancellation: crate::agent::agent_tool_router::ToolInvocationCancellation,
) -> crate::kernel::runtime_host::VerletResult<crate::agent::tool_interceptor::ToolExecutionOutcome>
{
    let witness_coordinates = turn_context.coordinates().clone();
    interceptor
        .execute_with_witnessing_cancellable(
            crate::agent::tool_interceptor::ToolExecutionRequest {
                turn_context,
                call_id: call_id.to_string(),
                tool_name: tool_name.to_string(),
                arguments,
            },
            cancellation,
            |spec| emit_hook_started(events, turn_context.coordinates(), spec),
            |witnesses| {
                let coordinates = witness_coordinates.clone();
                async move {
                    append_hook_mutation_witnesses(services, &coordinates, witnesses).await
                }
            },
        )
        .await
}

#[allow(clippy::too_many_arguments)]
async fn append_tool_execution_outcome(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    call_id: String,
    tool_name: String,
    snapshot_id: String,
    args_fingerprint: Option<String>,
    source_event_id: verlet_history::EventRecordId,
    finish_order: Option<u64>,
    cancellation: Option<crate::kernel::control_decision::ToolCallCancellation>,
    outcome: crate::agent::tool_interceptor::ToolExecutionOutcome,
    idempotent_result_append: bool,
) -> crate::kernel::runtime_host::VerletResult<()> {
    emit_hook_records(events, turn_context.coordinates(), &outcome.hook_records);
    if let Some(permission_decision) = &outcome.permission_decision {
        let (decision, reason) = match permission_decision {
            crate::agent::tool_interceptor::ToolPermissionDecision::Allow => (
                verlet_runtime_contracts::RuntimePermissionDecision::Allow,
                None,
            ),
            crate::agent::tool_interceptor::ToolPermissionDecision::Deny { reason } => (
                verlet_runtime_contracts::RuntimePermissionDecision::Deny,
                Some(reason.clone()),
            ),
        };
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            events,
            turn_context.coordinates(),
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::PermissionDecision {
                call_id: call_id.clone(),
                tool_name: tool_name.clone(),
                decision,
                reason,
            },
        );
    }
    append_hook_contexts(
        services,
        turn_context.coordinates(),
        thread_id,
        events,
        outcome.pre_model_contexts,
    )
    .await?;
    let tool_success = matches!(
        &outcome.result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            ..
        }
    );
    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
        events,
        turn_context.coordinates(),
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolLog {
            call_id: call_id.clone(),
            tool_name: tool_name.clone(),
            level: if tool_success {
                verlet_runtime_contracts::RuntimeToolLogLevel::Info
            } else {
                verlet_runtime_contracts::RuntimeToolLogLevel::Error
            },
            message: if tool_success {
                "tool completed".to_string()
            } else {
                "tool failed".to_string()
            },
            metadata: std::collections::BTreeMap::from([
                ("duration_ms".to_string(), outcome.duration_ms.to_string()),
                ("success".to_string(), tool_success.to_string()),
            ]),
        },
    );
    append_tool_result_message(
        services,
        turn_context.coordinates(),
        thread_id,
        events,
        call_id,
        tool_name,
        turn_context.turn_id.clone(),
        snapshot_id,
        args_fingerprint,
        outcome.result,
        Some(outcome.duration_ms),
        finish_order,
        cancellation,
        source_event_id,
        idempotent_result_append,
    )
    .await?;
    append_hook_contexts(
        services,
        turn_context.coordinates(),
        thread_id,
        events,
        outcome.post_model_contexts,
    )
    .await
}

async fn append_turn_submitted_event(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    entry: &verlet_history::SessionEntry,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    if let Some(existing) = existing_turn_submitted_event(services, coordinates, turn_id).await? {
        return Ok(existing);
    }
    services
        .append_thread_event(
            coordinates,
            verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({
                    "turn_id": turn_id,
                    "entry_id": entry.entry_id.to_string(),
                }),
            ),
        )
        .await
}

async fn existing_turn_submitted_event(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::EventRecord>> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    Ok(events
        .into_iter()
        .filter(|event| {
            event.kind == verlet_history::EventKind::TurnSubmitted
                && event
                    .payload
                    .get("turn_id")
                    .and_then(|value| value.as_str())
                    == Some(turn_id)
        })
        .max_by_key(|event| event.sequence.get()))
}

async fn append_turn_completed_event(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_context: &verlet_runtime_contracts::ThreadContext,
    turn_id: &str,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    let coordinates = &thread_context.coordinates;
    let latest_source_id = latest_thread_event_id(services, coordinates).await?;
    let completed = services
        .append_thread_event(
            coordinates,
            verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::TurnCompleted,
                serde_json::json!({
                    "turn_id": turn_id,
                }),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::for_thread(coordinates)],
                    source_event_ids: latest_source_id.into_iter().collect(),
                    discharged_by: Some("propagator:agent-loop".to_string()),
                    function: Some("turn_complete/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            ),
        )
        .await?;
    services
        .append_thread_joined_event_if_spawned(
            thread_context,
            verlet_history::ThreadTerminalState::Completed,
            None,
            Some(completed.id),
        )
        .await?;
    Ok(completed)
}

async fn append_turn_resumed_event(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    consumed_fact_id: verlet_history::EventRecordId,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    services
        .append_control_event(
            coordinates,
            verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::TurnResumed,
                serde_json::json!({
                    "turn_id": turn_id,
                    "consumed_fact_id": consumed_fact_id.to_string(),
                }),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::new(format!(
                        "control:{}",
                        coordinates.thread_id
                    ))],
                    source_event_ids: vec![consumed_fact_id],
                    discharged_by: Some("scheduler:tool-decision".to_string()),
                    function: Some("tool_resume/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            ),
        )
        .await
}

async fn append_tool_call_requested_events(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    calls: &[(ProviderToolCall, String)],
    assistant_entry_id: verlet_history::SessionEntryId,
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
    let assistant_event_id =
        session_entry_event_id(services, turn_context.coordinates(), assistant_entry_id)
            .await?
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "assistant session entry {assistant_entry_id} has no durable source event"
                ))
            })?;
    let mut records = Vec::with_capacity(calls.len());
    for (tool_call, snapshot_id) in calls {
        let args_fingerprint =
            crate::agent::tool_universe::args_fingerprint(&tool_call.name, &tool_call.arguments)?;
        let mut payload =
            serde_json::to_value(crate::kernel::control_decision::ToolCallRequestedPayload {
                subject: crate::kernel::control_decision::ToolCallSubject {
                    turn_id: turn_context.turn_id.clone(),
                    call_id: tool_call.id.clone(),
                },
                snapshot_id: snapshot_id.clone(),
                tool_name: tool_call.name.clone(),
                arguments: tool_call.arguments.clone(),
                args_fingerprint: Some(args_fingerprint),
                holds: tool_holds::derive_tool_holds(&tool_call.name, &tool_call.arguments)
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| {
                        crate::kernel::runtime_host::VerletError::History(format!(
                            "tool hold payload codec failed: {err}"
                        ))
                    })?,
            })
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "tool request payload codec failed: {err}"
                ))
            })?;
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "tool".to_string(),
                serde_json::Value::String(tool_call.name.clone()),
            );
        }
        records.push(verlet_history::NewEventRecord::discharged(
            turn_context.coordinates().clone(),
            verlet_history::EventKind::ToolCallRequested,
            payload,
            verlet_history::EventProvenance {
                source_streams: vec![verlet_history::EventStreamId::for_thread(
                    turn_context.coordinates(),
                )],
                source_event_ids: vec![assistant_event_id],
                discharged_by: Some("propagator:agent-loop".to_string()),
                function: Some("tool_request/v1".to_string()),
                ..verlet_history::EventProvenance::default()
            },
        ));
    }
    services
        .append_thread_events(turn_context.coordinates(), records)
        .await
}

async fn append_denied_tool_result(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    call_id: String,
    tool_name: String,
    snapshot_id: String,
    args_fingerprint: Option<String>,
    reason: String,
    finish_order: Option<u64>,
    cancellation: Option<crate::kernel::control_decision::ToolCallCancellation>,
    source_event_id: verlet_history::EventRecordId,
) -> crate::kernel::runtime_host::VerletResult<()> {
    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
        events,
        turn_context.coordinates(),
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolLog {
            call_id: call_id.clone(),
            tool_name: tool_name.clone(),
            level: verlet_runtime_contracts::RuntimeToolLogLevel::Error,
            message: "tool denied".to_string(),
            metadata: std::collections::BTreeMap::from([
                ("duration_ms".to_string(), "0".to_string()),
                ("success".to_string(), "false".to_string()),
            ]),
        },
    );
    let result = verlet_history::CanonicalMessage::tool_result(
        call_id.clone(),
        tool_name.clone(),
        reason,
        true,
    );
    append_tool_result_message(
        services,
        turn_context.coordinates(),
        thread_id,
        events,
        call_id,
        tool_name.clone(),
        turn_context.turn_id.clone(),
        snapshot_id,
        args_fingerprint,
        result,
        Some(0),
        finish_order,
        cancellation,
        source_event_id,
        false,
    )
    .await
}

async fn append_turn_waiting_event(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    call_id: &str,
    snapshot_id: &str,
    waiting_on_event_id: verlet_history::EventRecordId,
    approval_id: Option<String>,
    reason: Option<String>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    services
        .append_control_event(
            turn_context.coordinates(),
            verlet_history::NewEventRecord::discharged(
                turn_context.coordinates().clone(),
                verlet_history::EventKind::TurnWaiting,
                serde_json::json!({
                    "turn_id": turn_context.turn_id.clone(),
                    "subject": {
                        "turn_id": turn_context.turn_id.clone(),
                        "call_id": call_id,
                    },
                    "snapshot_id": snapshot_id,
                    "waiting_on_event_id": waiting_on_event_id.to_string(),
                    "approval_id": approval_id,
                    "reason": reason,
                    "continuation": "tool.call",
                }),
                verlet_history::EventProvenance {
                    source_streams: vec![verlet_history::EventStreamId::new(format!(
                        "control:{}",
                        turn_context.coordinates().thread_id
                    ))],
                    source_event_ids: vec![waiting_on_event_id],
                    discharged_by: Some("scheduler:tool-decision".to_string()),
                    function: Some("tool_wait/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            ),
        )
        .await?;
    Ok(())
}

async fn session_entry_event_id(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    entry_id: verlet_history::SessionEntryId,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::EventRecordId>> {
    let entry_id = entry_id.to_string();
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    Ok(events
        .into_iter()
        .find(|event| {
            event.kind == verlet_history::EventKind::SessionEntryAppended
                && event
                    .payload
                    .get("entry_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(entry_id.as_str())
        })
        .map(|event| event.id))
}

async fn latest_thread_event_id(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::EventRecordId>> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    Ok(events
        .into_iter()
        .max_by_key(|event| event.sequence.get())
        .map(|event| event.id))
}

async fn pending_tool_call_request(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    call_id: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<PendingToolCallRequest>> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut matches = Vec::new();
    for event in events
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallRequested)
    {
        let payload = serde_json::from_value::<
            crate::kernel::control_decision::ToolCallRequestedPayload,
        >(event.payload.clone())
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool.call.requested payload is invalid: {err}"
            ))
        })?;
        if payload.subject.turn_id == turn_id && payload.subject.call_id == call_id {
            let assistant_source_event_id = event
                .provenance
                .source_event_ids
                .first()
                .copied()
                .ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "tool.call.requested {} for {turn_id}/{call_id} has no assistant source event",
                        event.id
                    ))
                })?;
            matches.push(PendingToolCallRequest {
                request_event_id: event.id,
                assistant_source_event_id,
                subject: payload.subject,
                snapshot_id: payload.snapshot_id,
                tool_name: payload.tool_name,
                arguments: payload.arguments,
                args_fingerprint: payload.args_fingerprint,
                holds: payload
                    .holds
                    .into_iter()
                    .map(serde_json::from_value)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| {
                        crate::kernel::runtime_host::VerletError::History(format!(
                            "tool hold payload is invalid: {err}"
                        ))
                    })?,
            });
        }
    }
    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        count => Err(crate::kernel::runtime_host::VerletError::History(format!(
            "found {count} tool.call.requested events for ambiguous subject {turn_id}/{call_id}"
        ))),
    }
}

async fn tool_call_batch_completed(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    assistant_source_event_id: verlet_history::EventRecordId,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut batch_subjects = std::collections::BTreeMap::new();
    let mut completed_subjects = std::collections::BTreeMap::<
        crate::kernel::control_decision::ToolCallSubject,
        Vec<crate::kernel::control_decision::ToolCallCompletedPayload>,
    >::new();
    for event in events {
        match event.kind {
            verlet_history::EventKind::ToolCallRequested
                if event.provenance.source_event_ids.first()
                    == Some(&assistant_source_event_id) =>
            {
                let payload = serde_json::from_value::<
                    crate::kernel::control_decision::ToolCallRequestedPayload,
                >(event.payload)
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "tool.call.requested payload is invalid: {err}"
                    ))
                })?;
                if payload.subject.turn_id == turn_id
                    && batch_subjects
                        .insert(
                            payload.subject,
                            (payload.snapshot_id, payload.args_fingerprint),
                        )
                        .is_some()
                {
                    return Err(crate::kernel::runtime_host::VerletError::History(format!(
                        "assistant source event {assistant_source_event_id} contains duplicate tool call subjects"
                    )));
                }
            }
            verlet_history::EventKind::ToolCallCompleted
                if event
                    .payload
                    .get("subject")
                    .and_then(|subject| subject.get("turn_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(turn_id) =>
            {
                let payload = serde_json::from_value::<
                    crate::kernel::control_decision::ToolCallCompletedPayload,
                >(event.payload)
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "tool.call.completed payload is invalid: {err}"
                    ))
                })?;
                completed_subjects
                    .entry(payload.subject.clone())
                    .or_default()
                    .push(payload);
            }
            _ => {}
        }
    }
    if batch_subjects.is_empty() {
        return Err(crate::kernel::runtime_host::VerletError::History(format!(
            "assistant source event {assistant_source_event_id} has no tool request batch for turn {turn_id}"
        )));
    }
    Ok(batch_subjects
        .iter()
        .all(|(subject, (snapshot_id, fingerprint))| {
            completed_subjects.get(subject).is_some_and(|completions| {
                completions.iter().any(|completion| {
                    completion.snapshot_id.as_str() == snapshot_id.as_str()
                        && completion.args_fingerprint.as_deref() == fingerprint.as_deref()
                })
            })
        }))
}

async fn matching_tool_call_completed_exists(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    call_id: &str,
    snapshot_id: &str,
    args_fingerprint: Option<&str>,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    for event in events.into_iter().filter(|event| {
        event.kind == verlet_history::EventKind::ToolCallCompleted
            && event.payload["subject"]["turn_id"] == turn_id
            && event.payload["subject"]["call_id"] == call_id
    }) {
        let payload = serde_json::from_value::<
            crate::kernel::control_decision::ToolCallCompletedPayload,
        >(event.payload)
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool.call.completed {} payload is invalid: {err}",
                event.id
            ))
        })?;
        if payload.snapshot_id == snapshot_id
            && payload.args_fingerprint.as_deref() == args_fingerprint
        {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn existing_tool_result_message(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    source_event_id: verlet_history::EventRecordId,
    call_id: &str,
    expected_snapshot_id: &str,
    expected_fingerprint: Option<&str>,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let Some(request_event) = events.iter().find(|event| {
        event.id == source_event_id && event.kind == verlet_history::EventKind::ToolCallRequested
    }) else {
        return Ok(None);
    };
    let request =
        serde_json::from_value::<crate::kernel::control_decision::ToolCallRequestedPayload>(
            request_event.payload.clone(),
        )
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool.call.requested {} payload is invalid while reusing a result: {err}",
                request_event.id
            ))
        })?;
    if !crate::kernel::control_decision::tool_invocation_fingerprint_matches(
        &request.snapshot_id,
        request.args_fingerprint.as_deref(),
        expected_snapshot_id,
        expected_fingerprint,
    ) {
        return Ok(None);
    }
    let result_entry_ids = events
        .into_iter()
        .filter(|event| {
            event.kind == verlet_history::EventKind::SessionEntryAppended
                && event.provenance.source_event_ids.contains(&source_event_id)
        })
        .filter_map(|event| {
            event
                .payload
                .get("entry_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect::<std::collections::HashSet<_>>();
    if result_entry_ids.is_empty() {
        return Ok(None);
    }
    let context = services.build_session_context(coordinates).await?;
    Ok(context.entries.into_iter().find_map(|entry| {
        if !result_entry_ids.contains(&entry.entry_id.to_string()) {
            return None;
        }
        match entry.kind {
            verlet_history::SessionEntryKind::Message {
                message:
                    ref message @ verlet_history::CanonicalMessage::ToolResult {
                        ref tool_call_id, ..
                    },
            } if tool_call_id == call_id => Some(message.clone()),
            _ => None,
        }
    }))
}

async fn tool_call_suspension_exists(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
    call_id: &str,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    for event in events
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallSuspended)
    {
        let payload = serde_json::from_value::<
            crate::kernel::control_decision::ToolCallSuspendedPayload,
        >(event.payload.clone())
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool.call.suspended payload is invalid: {err}"
            ))
        })?;
        if payload.subject.turn_id == turn_id && payload.subject.call_id == call_id {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn append_tool_result_message(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    call_id: String,
    tool_name: String,
    turn_id: String,
    snapshot_id: String,
    args_fingerprint: Option<String>,
    result: verlet_history::CanonicalMessage,
    duration_ms: Option<u64>,
    finish_order: Option<u64>,
    cancellation: Option<crate::kernel::control_decision::ToolCallCancellation>,
    source_event_id: verlet_history::EventRecordId,
    idempotent_result_append: bool,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let existing_result = if idempotent_result_append {
        existing_tool_result_message(
            services,
            coordinates,
            source_event_id,
            &call_id,
            &snapshot_id,
            args_fingerprint.as_deref(),
        )
        .await?
    } else {
        None
    };
    let result_already_persisted = existing_result.is_some();
    let result = existing_result.unwrap_or(result);
    let success = match &result {
        verlet_history::CanonicalMessage::ToolResult { is_error, .. } => !is_error,
        _ => false,
    };
    let output = text_from_message(&result);
    if !result_already_persisted {
        let entry = services
            .append_agent_loop_session_entry(
                coordinates,
                None,
                verlet_history::SessionEntryKind::Message { message: result },
                vec![source_event_id],
            )
            .await?;
        let _ = events.send(
            crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror {
                thread_id,
                entry,
            },
        );
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            events,
            coordinates,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                call_id: call_id.clone(),
                output,
                success,
                duration_ms,
            },
        );
    }
    append_tool_completion_event(
        services,
        coordinates,
        turn_id,
        call_id,
        snapshot_id,
        tool_name,
        args_fingerprint,
        success,
        duration_ms,
        finish_order,
        cancellation,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn append_tool_completion_event(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: String,
    call_id: String,
    snapshot_id: String,
    tool_name: String,
    args_fingerprint: Option<String>,
    success: bool,
    duration_ms: Option<u64>,
    finish_order: Option<u64>,
    cancellation: Option<crate::kernel::control_decision::ToolCallCancellation>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let subject = crate::kernel::control_decision::ToolCallSubject { turn_id, call_id };
    let completion_snapshot_id = snapshot_id.clone();
    let completion_fingerprint = args_fingerprint.clone();
    let record = verlet_history::NewEventRecord::witnessed(
        coordinates.clone(),
        verlet_history::EventKind::ToolCallCompleted,
        serde_json::to_value(crate::kernel::control_decision::ToolCallCompletedPayload {
            subject: subject.clone(),
            snapshot_id,
            tool_name,
            success,
            args_fingerprint,
            duration_ms,
            finish_order,
            cancellation,
        })
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "tool completion payload codec failed: {err}"
            ))
        })?,
    );
    let stream_id = verlet_history::EventStreamId::for_thread(coordinates);
    loop {
        let existing = services
            .runtime_store()
            .read_events(&stream_id, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        for event in existing.iter().filter(|event| {
            event.kind == verlet_history::EventKind::ToolCallCompleted
                && event.payload["subject"]["turn_id"] == subject.turn_id
                && event.payload["subject"]["call_id"] == subject.call_id
        }) {
            let payload = serde_json::from_value::<
                crate::kernel::control_decision::ToolCallCompletedPayload,
            >(event.payload.clone())
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "tool.call.completed {} payload is invalid: {err}",
                    event.id
                ))
            })?;
            if payload.snapshot_id == completion_snapshot_id
                && payload.args_fingerprint == completion_fingerprint
            {
                return Ok(());
            }
        }
        let expected_next_sequence = existing
            .last()
            .map(|event| verlet_history::EventSequence::new(event.sequence.get().saturating_add(1)))
            .unwrap_or_else(|| verlet_history::EventSequence::new(1));
        match services
            .runtime_store()
            .append_events_fenced(&stream_id, expected_next_sequence, vec![record.clone()])
            .await
        {
            Ok(_) => return Ok(()),
            Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
            Err(err) => {
                return Err(crate::kernel::runtime_host::VerletError::History(
                    err.to_string(),
                ));
            }
        }
    }
}

async fn append_hook_contexts(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    contexts: Vec<String>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    for context in contexts
        .into_iter()
        .filter(|context| !context.trim().is_empty())
    {
        let entry = services
            .append_session_entry(
                coordinates,
                None,
                verlet_history::SessionEntryKind::CustomContextMessage {
                    message: verlet_history::CanonicalMessage::user_text(context),
                },
            )
            .await?;
        let _ = events.send(
            crate::kernel::runtime_host::runtime_api::ThreadEvent::CanonicalMirror {
                thread_id,
                entry,
            },
        );
    }
    Ok(())
}

async fn append_hook_mutation_witnesses(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    witnesses: Vec<crate::agent::hooks::HookMutationWitness>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if witnesses.is_empty() {
        return Ok(());
    }
    let store = services.runtime_store();
    for witness in witnesses {
        let mut payload = serde_json::to_value(&witness).map_err(|err| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "hook witness codec failed: {err}"
            ))
        })?;
        if let Some(payload) = payload.as_object_mut() {
            payload.insert(
                "schema".to_string(),
                serde_json::json!(HOOK_MUTATION_WITNESS_OBSERVATION_SCHEMA_V1),
            );
            payload.insert("witnessing".to_string(), serde_json::json!(true));
        }
        let record = verlet_history::NewObservationRecord::new(
            HOOK_MUTATION_WITNESS_OBSERVATION_KIND,
            coordinates.clone(),
            payload,
        )
        .with_provenance(verlet_history::ObservationProvenance {
            derivation_strategy: "host.hook.mutation_witnessing".to_string(),
            derivation_version: "v1".to_string(),
            ..verlet_history::ObservationProvenance::default()
        });
        store
            .append_observation(record)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    }
    Ok(())
}

struct IntraTurnSteeringContext {
    entry_id: verlet_history::SessionEntryId,
    context: String,
}

async fn undelivered_intra_turn_steering_contexts(
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    active_turn_id: &str,
    turn_delivery_start_sequence: verlet_history::EventSequence,
    session_entries: &[verlet_history::SessionEntry],
) -> crate::kernel::runtime_host::VerletResult<Vec<IntraTurnSteeringContext>> {
    // The original turn.submitted event is the safe lower bound: an entry
    // admitted before it belongs to ordinary history, while every steer that
    // can target this active turn and every receipt capable of delivering that
    // steer follows it. Resumed turns deliberately retain the original bound,
    // so a crash before the delivery receipt cannot hide an eligible steer.
    let events = services
        .runtime_store()
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            Some(turn_delivery_start_sequence),
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    Ok(intra_turn_steering_contexts_from_events(
        &events,
        active_turn_id,
        turn_delivery_start_sequence,
        session_entries,
    ))
}

fn intra_turn_steering_contexts_from_events(
    events: &[verlet_history::EventRecord],
    active_turn_id: &str,
    turn_delivery_start_sequence: verlet_history::EventSequence,
    session_entries: &[verlet_history::SessionEntry],
) -> Vec<IntraTurnSteeringContext> {
    let admitted_entry_ids = events
        .iter()
        .filter(|event| {
            event.sequence.get() > turn_delivery_start_sequence.get()
                && event.kind == verlet_history::EventKind::SessionEntryAppended
        })
        .filter_map(|event| event.payload.get("entry_id"))
        .filter_map(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
    let delivered_entry_ids = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ContextCompileCompleted)
        .filter_map(|event| event.payload.get("session_entry_ids"))
        .filter_map(serde_json::Value::as_array)
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<std::collections::BTreeSet<_>>();

    session_entries
        .iter()
        .filter_map(|entry| {
            let turn_id = entry.turn_id.as_deref()?;
            let entry_id = entry.entry_id.to_string();
            if turn_id == active_turn_id
                || !admitted_entry_ids.contains(&entry_id)
                || delivered_entry_ids.contains(&entry_id)
            {
                return None;
            }
            let verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::User { content, .. },
            } = &entry.kind
            else {
                return None;
            };
            steering_context(turn_id, canonical_content_text_projection(content)).map(|context| {
                IntraTurnSteeringContext {
                    entry_id: entry.entry_id,
                    context,
                }
            })
        })
        .collect()
}

fn canonical_content_text_projection(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::Thinking { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn steering_context(turn_id: &str, text: String) -> Option<String> {
    if text.trim().is_empty() {
        return None;
    }
    Some(format!(
        "Additional user steering for active turn {turn_id}:\n{text}"
    ))
}

#[cfg(test)]
mod intra_turn_steering_context_tests {

    #[test]
    fn completed_history_without_compile_receipt_is_not_reclassified_as_steering() {
        let coordinates = verlet_runtime_contracts::ThreadCoordinates::new(
            "tenant_a",
            "user_1",
            "session_completed_history",
        );
        let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
        let old_entry = verlet_history::SessionEntry::for_turn(
            coordinates.clone(),
            None,
            "turn-old",
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("completed historical input"),
            },
        );
        let steer_entry = verlet_history::SessionEntry::for_turn(
            coordinates.clone(),
            Some(old_entry.entry_id),
            "turn-steer",
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("new boundary steer"),
            },
        );
        let event = |sequence, kind, payload| verlet_history::EventRecord {
            id: verlet_history::EventRecordId::new(),
            stream_id: stream_id.clone(),
            sequence: verlet_history::EventSequence::new(sequence),
            coordinates: coordinates.clone(),
            created_at_ms: sequence,
            kind,
            origin: verlet_history::EventOrigin::Witnessed,
            provenance: verlet_history::EventProvenance::default(),
            payload,
        };
        let events = vec![
            event(
                1,
                verlet_history::EventKind::SessionEntryAppended,
                serde_json::json!({
                    "entry_id": old_entry.entry_id.to_string(),
                    "turn_id": "turn-old",
                    "entry_kind": "message",
                }),
            ),
            event(
                2,
                verlet_history::EventKind::TurnCompleted,
                serde_json::json!({"turn_id": "turn-old"}),
            ),
            event(
                3,
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": "turn-active"}),
            ),
            event(
                4,
                verlet_history::EventKind::SessionEntryAppended,
                serde_json::json!({
                    "entry_id": steer_entry.entry_id.to_string(),
                    "turn_id": "turn-steer",
                    "entry_kind": "message",
                }),
            ),
        ];

        let contexts = crate::adapters::agent_loop::intra_turn_steering_contexts_from_events(
            &events,
            "turn-active",
            verlet_history::EventSequence::new(3),
            &[old_entry, steer_entry.clone()],
        );

        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].entry_id, steer_entry.entry_id);
        assert!(contexts[0].context.contains("new boundary steer"));
        assert!(!contexts[0].context.contains("completed historical input"));
    }
}

async fn run_stop_hooks(
    runtime: &AgentLoop,
    turn_context: &crate::kernel::runtime_host::turn::TurnContext,
    services: &crate::kernel::runtime_host::runtime_services::RuntimeServices,
    thread_id: verlet_runtime_contracts::ThreadId,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    last_assistant_message: Option<String>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let Some(hook_pipeline) = &runtime.hook_pipeline else {
        return Ok(());
    };
    let outcome = hook_pipeline
        .run_stop(
            crate::agent::hooks::StopHookRequest {
                turn_context: turn_context.snapshot(),
                last_assistant_message,
            },
            |spec| emit_hook_started(events, turn_context.coordinates(), spec),
        )
        .await;
    emit_hook_records(events, turn_context.coordinates(), &outcome.records);
    append_hook_mutation_witnesses(
        services,
        turn_context.coordinates(),
        outcome.mutation_witnesses,
    )
    .await?;
    append_hook_contexts(
        services,
        turn_context.coordinates(),
        thread_id,
        events,
        outcome.additional_contexts,
    )
    .await
}

fn emit_hook_started(
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    spec: &crate::agent::hooks::HookHandlerSpec,
) {
    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
        events,
        coordinates,
        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::HookStarted {
            hook_id: spec.id.clone(),
            event_name: spec.event_name,
            matcher: spec.matcher.clone(),
        },
    );
}

fn emit_hook_records(
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    records: &[crate::agent::hooks::HookRunRecord],
) {
    for record in records {
        crate::kernel::runtime_host::runtime_events::emit_runtime_event(
            events,
            coordinates,
            crate::kernel::runtime_host::runtime_events::RuntimeEventKind::HookCompleted {
                hook_id: record.hook_id.clone(),
                event_name: record.event_name,
                status: record.status,
                duration_ms: record.duration_ms,
                message: record.message.clone(),
            },
        );
    }
}

fn text_from_message(message: &verlet_history::CanonicalMessage) -> String {
    match message {
        verlet_history::CanonicalMessage::Assistant { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        verlet_history::CanonicalMessage::ToolResult { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        verlet_history::CanonicalMessage::User { .. } => String::new(),
    }
}

fn emit_non_stream_content_events(
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    message: &verlet_history::CanonicalMessage,
) {
    let verlet_history::CanonicalMessage::Assistant { content, .. } = message else {
        return;
    };
    for content in content {
        match content {
            verlet_history::CanonicalContent::Text { text, .. } if !text.is_empty() => {
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    events,
                    coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::TextDelta {
                        text: text.clone(),
                    },
                )
            }
            verlet_history::CanonicalContent::Thinking { text, .. } if !text.is_empty() => {
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    events,
                    coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ThinkingDelta {
                        text: text.clone(),
                    },
                )
            }
            verlet_history::CanonicalContent::Text { .. }
            | verlet_history::CanonicalContent::Thinking { .. }
            | verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => {}
        }
    }
}

#[derive(Default)]
struct PendingToolCall {
    name: Option<String>,
    arguments: String,
}

fn response_from_stream_events(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    stream_events: Vec<verlet_provider::ProviderStreamEvent>,
    events: &tokio::sync::broadcast::Sender<crate::kernel::runtime_host::runtime_api::ThreadEvent>,
) -> Result<verlet_provider::ProviderResponse, ModelRequestAttemptError> {
    let mut content = Vec::new();
    let mut text = String::new();
    let mut usage = verlet_history::CanonicalUsage::default();
    let mut stop_reason = verlet_history::CanonicalStopReason::EndTurn;
    let mut saw_done = false;
    let mut tool_order = Vec::new();
    let mut tool_calls = std::collections::BTreeMap::<String, PendingToolCall>::new();

    for event in stream_events {
        match event {
            verlet_provider::ProviderStreamEvent::TextDelta { text: delta } => {
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    events,
                    coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::TextDelta {
                        text: delta.clone(),
                    },
                );
                text.push_str(&delta);
            }
            verlet_provider::ProviderStreamEvent::ThinkingDelta { text: delta } => {
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    events,
                    coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ThinkingDelta {
                        text: delta.clone(),
                    },
                );
                content.push(verlet_history::CanonicalContent::Thinking {
                    text: delta,
                    provider: verlet_history::ThinkingProvider::Other("stream".to_string()),
                    metadata: verlet_history::ThinkingMetadata::None,
                });
            }
            verlet_provider::ProviderStreamEvent::ToolCallDelta {
                id,
                name,
                arguments_delta,
            } => {
                if !tool_calls.contains_key(&id) {
                    tool_order.push(id.clone());
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        events,
                        coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
                            call_id: id.clone(),
                            name: name.clone().unwrap_or_default(),
                            input: serde_json::json!({}),
                        },
                    );
                }
                let pending = tool_calls.entry(id).or_default();
                if name.is_some() {
                    pending.name = name;
                }
                pending.arguments.push_str(&arguments_delta);
            }
            verlet_provider::ProviderStreamEvent::Content { content: incoming } => {
                if let verlet_history::CanonicalContent::Text {
                    text: incoming_text,
                    ..
                } = &incoming
                    && !text.is_empty()
                    && incoming_text == &text
                {
                    continue;
                }
                if let verlet_history::CanonicalContent::ToolCall {
                    id,
                    name,
                    arguments,
                } = &incoming
                {
                    tool_calls.remove(id);
                    tool_order.retain(|candidate| candidate != id);
                    crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                        events,
                        coordinates,
                        crate::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted {
                            call_id: id.clone(),
                            name: name.clone(),
                            input: arguments.clone(),
                        },
                    );
                }
                content.push(incoming);
            }
            verlet_provider::ProviderStreamEvent::Usage { usage: next_usage } => {
                usage.input_tokens += next_usage.input_tokens;
                usage.output_tokens += next_usage.output_tokens;
                usage.cache_creation_input_tokens += next_usage.cache_creation_input_tokens;
                usage.cache_read_input_tokens += next_usage.cache_read_input_tokens;
                crate::kernel::runtime_host::runtime_events::emit_runtime_event(
                    events,
                    coordinates,
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Usage {
                        usage: verlet_runtime_contracts::RuntimeUsage {
                            input_tokens: next_usage.input_tokens,
                            output_tokens: next_usage.output_tokens,
                            cache_creation_input_tokens: next_usage.cache_creation_input_tokens,
                            cache_read_input_tokens: next_usage.cache_read_input_tokens,
                        },
                    },
                );
            }
            verlet_provider::ProviderStreamEvent::Done {
                stop_reason: reason,
            } => {
                saw_done = true;
                stop_reason = reason;
            }
            verlet_provider::ProviderStreamEvent::Error { message } => {
                return Err(stream_assembly_error(message));
            }
        }
    }

    if !saw_done {
        return Err(stream_assembly_error(
            "provider stream ended before done event",
        ));
    }

    if !text.is_empty() {
        content.insert(0, verlet_history::CanonicalContent::text(text));
    }
    for id in tool_order {
        let Some(pending) = tool_calls.remove(&id) else {
            continue;
        };
        let arguments = if pending.arguments.trim().is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(&pending.arguments).map_err(|err| {
                stream_assembly_error(format!("invalid streamed tool arguments for {id}: {err}"))
            })?
        };
        content.push(verlet_history::CanonicalContent::tool_call(
            id,
            pending.name.unwrap_or_default(),
            arguments,
        ));
    }

    Ok(verlet_provider::ProviderResponse {
        content,
        usage,
        stop_reason,
    })
}

fn usage_from_message(
    message: &verlet_history::CanonicalMessage,
) -> Option<verlet_runtime_contracts::RuntimeUsage> {
    match message {
        verlet_history::CanonicalMessage::Assistant { usage, .. } => {
            Some(verlet_runtime_contracts::RuntimeUsage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                cache_creation_input_tokens: usage.cache_creation_input_tokens,
                cache_read_input_tokens: usage.cache_read_input_tokens,
            })
        }
        verlet_history::CanonicalMessage::User { .. }
        | verlet_history::CanonicalMessage::ToolResult { .. } => None,
    }
}

mod tool_holds;

#[cfg(test)]
mod tests;
