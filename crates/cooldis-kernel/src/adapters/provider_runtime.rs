use crate::agent::contracts::sha256_hex;
use crate::{
    AgentContextCompileInput, AgentContextCompilePolicy, AgentContextCompiler,
    AgentManifestStaticContextSegment, AgentRuntime, AgentRuntimeFactory, AgentToolRouter,
    AllowAllToolPermissionGate, BashToolProvider, COOLDIS_NOTIFY_PACKAGE, COOLDIS_PROCESS_PACKAGE,
    COOLDIS_SCHEDULE_PACKAGE, COOLDIS_THREADS_PACKAGE, CanonicalContent, CanonicalMessage,
    CompactionPolicy, CompactionTrigger, CompiledAgentContext, CooldisError, CooldisResult,
    EventKind, EventProvenance, EventRecordId, EventStreamId, HookHandlerSpec, HookMutationWitness,
    HookPipeline, HookRunRecord, KernelNotifyOperationProvider, KernelOperationDispatcher,
    KernelProcessOperationProvider, KernelScheduleOperationProvider, KernelThreadOperationProvider,
    KernelThreadSpawnAgentResolver, NewEventRecord, NewObservationRecord, ObservationProvenance,
    OperationRegistry, PostCompactHookRequest, PreCompactHookRequest, ProviderApi, ProviderClient,
    ProviderError, ProviderRequest, ProviderRequestMode, ProviderStreamEvent,
    ReplayTransformCounts, RuntimeEventKind, RuntimeModelRequestErrorClass,
    RuntimeModelRequestMode, RuntimeModelRequestPurpose, RuntimePermissionDecision,
    RuntimeServices, RuntimeTerminalState, RuntimeToolLogLevel, RuntimeUsage, SessionEntry,
    SessionEntryId, SessionEntryKind, SessionStartHookRequest, StopHookRequest, SystemBlock,
    THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA, THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA,
    ThinkingConfig, ThreadCommand, ThreadContext, ThreadEvent, ThreadSignal, ThreadStatus,
    ThreadTerminalState, ToolCallCompletedPayload, ToolCallDecision, ToolCallRequestedPayload,
    ToolCallSubject, ToolDecisionRequest, ToolDefinition, ToolExecutionInterceptor,
    ToolExecutionRequest, ToolPermissionDecision, ToolPermissionGate, TurnBudget, TurnContext,
    TurnInput, TurnSubmissionMode, UserPromptSubmitHookRequest, VirtualBashRuntimeConfig,
    active_manifest_bind_receipt, active_tool_controller_for_request,
    compile_provider_request_context, decide_tool_call, deterministic_compaction_summary,
    emit_runtime_event, normalize_history_for_target,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

const MAX_TOOL_ROUTER_ROUNDS: usize = 8;
const HOOK_MUTATION_WITNESS_OBSERVATION_KIND: &str = "host.hook.mutation_witnessed";
const HOOK_MUTATION_WITNESS_OBSERVATION_SCHEMA_V1: &str =
    "cooldis.observation.host_hook_mutation/1";

fn default_process_dispatcher_cwd() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanonicalProviderRuntimeConfig {
    pub provider: String,
    pub api: ProviderApi,
    pub model: String,
    #[serde(default)]
    pub system: Vec<SystemBlock>,
    #[serde(default)]
    pub tools: Vec<ToolDefinition>,
    pub max_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<ThinkingConfig>,
    #[serde(default)]
    pub stream: bool,
}

impl CanonicalProviderRuntimeConfig {
    pub fn new(api: ProviderApi, provider: impl Into<String>, model: impl Into<String>) -> Self {
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

    fn request_from_messages(&self, messages: Vec<CanonicalMessage>) -> ProviderRequest {
        ProviderRequest {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    config: CanonicalProviderRuntimeConfig,
    client: Arc<dyn ProviderClient>,
}

#[derive(Clone)]
pub struct CanonicalProviderRuntimeFactory {
    config: CanonicalProviderRuntimeConfig,
    client: Arc<dyn ProviderClient>,
    model_request_retry_policy: ModelRequestRetryPolicy,
    model_request_fallbacks: Vec<ModelRequestEndpoint>,
    tool_router: Option<Arc<AgentToolRouter>>,
    bash_tool_config: Option<VirtualBashRuntimeConfig>,
    thread_spawn_agent_resolver: Option<Arc<dyn KernelThreadSpawnAgentResolver>>,
    hook_pipeline: Option<Arc<HookPipeline>>,
    tool_permission_gate: Arc<dyn ToolPermissionGate>,
    context_compile_policy: AgentContextCompilePolicy,
    compaction_policy: CompactionPolicy,
}

impl CanonicalProviderRuntimeFactory {
    pub fn new(config: CanonicalProviderRuntimeConfig, client: Arc<dyn ProviderClient>) -> Self {
        Self {
            config,
            client,
            model_request_retry_policy: ModelRequestRetryPolicy::default(),
            model_request_fallbacks: Vec::new(),
            tool_router: None,
            bash_tool_config: None,
            thread_spawn_agent_resolver: None,
            hook_pipeline: None,
            tool_permission_gate: Arc::new(AllowAllToolPermissionGate),
            context_compile_policy: AgentContextCompilePolicy::unbounded(),
            compaction_policy: CompactionPolicy::disabled(),
        }
    }

    pub fn with_model_request_retry_policy(mut self, policy: ModelRequestRetryPolicy) -> Self {
        self.model_request_retry_policy = policy;
        self
    }

    pub fn with_model_request_fallback(
        mut self,
        config: CanonicalProviderRuntimeConfig,
        client: Arc<dyn ProviderClient>,
    ) -> Self {
        self.model_request_fallbacks
            .push(ModelRequestEndpoint { config, client });
        self
    }

    pub fn with_tool_router(mut self, tool_router: Arc<AgentToolRouter>) -> Self {
        self.tool_router = Some(tool_router);
        self
    }

    pub fn with_operation_registry(mut self, operation_registry: Arc<OperationRegistry>) -> Self {
        self.tool_router = Some(Arc::new(AgentToolRouter::new(operation_registry)));
        self
    }

    pub fn with_bash_tool(mut self, config: VirtualBashRuntimeConfig) -> Self {
        self.bash_tool_config = Some(config);
        self
    }

    pub fn with_thread_spawn_agent_resolver(
        mut self,
        resolver: Arc<dyn KernelThreadSpawnAgentResolver>,
    ) -> Self {
        self.thread_spawn_agent_resolver = Some(resolver);
        self
    }

    // lexicon-allow: hook - existing host debug hook API name retained for compatibility.
    pub fn with_hook_pipeline(mut self, hook_pipeline: Arc<HookPipeline>) -> Self {
        self.hook_pipeline = Some(hook_pipeline);
        self
    }

    pub fn with_tool_permission_gate(
        mut self,
        tool_permission_gate: Arc<dyn ToolPermissionGate>,
    ) -> Self {
        self.tool_permission_gate = tool_permission_gate;
        self
    }

    pub fn with_context_compile_policy(mut self, policy: AgentContextCompilePolicy) -> Self {
        self.context_compile_policy = policy;
        self
    }

    pub fn with_compaction_policy(mut self, policy: CompactionPolicy) -> Self {
        self.compaction_policy = policy;
        self
    }
}

#[async_trait]
impl AgentRuntimeFactory for CanonicalProviderRuntimeFactory {
    async fn build(&self, _context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        Ok(Box::new(CanonicalProviderRuntime {
            config: self.config.clone(),
            client: Arc::clone(&self.client),
            model_request_retry_policy: self.model_request_retry_policy,
            model_request_fallbacks: self.model_request_fallbacks.clone(),
            tool_router: self.tool_router.clone(),
            bash_tool_config: self.bash_tool_config.clone(),
            thread_spawn_agent_resolver: self.thread_spawn_agent_resolver.clone(),
            hook_pipeline: self.hook_pipeline.clone(),
            tool_permission_gate: Arc::clone(&self.tool_permission_gate),
            context_compile_policy: self.context_compile_policy.clone(),
            compaction_policy: self.compaction_policy.clone(),
            strict_tool_router_unknowns: self.tool_router.is_some()
                || self.bash_tool_config.is_some(),
        }))
    }
}

struct CanonicalProviderRuntime {
    config: CanonicalProviderRuntimeConfig,
    client: Arc<dyn ProviderClient>,
    model_request_retry_policy: ModelRequestRetryPolicy,
    model_request_fallbacks: Vec<ModelRequestEndpoint>,
    tool_router: Option<Arc<AgentToolRouter>>,
    bash_tool_config: Option<VirtualBashRuntimeConfig>,
    thread_spawn_agent_resolver: Option<Arc<dyn KernelThreadSpawnAgentResolver>>,
    hook_pipeline: Option<Arc<HookPipeline>>,
    tool_permission_gate: Arc<dyn ToolPermissionGate>,
    context_compile_policy: AgentContextCompilePolicy,
    compaction_policy: CompactionPolicy,
    strict_tool_router_unknowns: bool,
}

#[async_trait]
impl AgentRuntime for CanonicalProviderRuntime {
    async fn run(
        self: Box<Self>,
        context: ThreadContext,
        services: RuntimeServices,
        mut commands: mpsc::Receiver<ThreadCommand>,
        events: broadcast::Sender<ThreadEvent>,
        status: watch::Sender<ThreadStatus>,
        cancellation: CancellationToken,
    ) {
        let mut runtime = *self;
        runtime.mount_agent_process_tools(&context, &services).await;
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        emit_runtime_event(
            &events,
            &coordinates,
            RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
            },
        );
        let _ = events.send(ThreadEvent::Started {
            context: context.clone(),
        });
        match runtime
            .run_session_start_hooks(&context, &services, thread_id, &events)
            .await
        {
            Ok(should_stop) => {
                if should_stop {
                    emit_runtime_event(
                        &events,
                        &coordinates,
                        RuntimeEventKind::Terminal {
                            state: RuntimeTerminalState::Stopped,
                        },
                    );
                    let _ = status.send(ThreadStatus::Stopped);
                    let _ = events.send(ThreadEvent::Stopped { thread_id });
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
        let _ = status.send(ThreadStatus::Idle);
        let mut pending_commands = VecDeque::new();

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
                    emit_runtime_event(
                        &events,
                        &coordinates,
                        RuntimeEventKind::Terminal {
                            state: RuntimeTerminalState::Stopped,
                        },
                    );
                    let _ = status.send(ThreadStatus::Stopped);
                    let _ = events.send(ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    let Some(command) = command else {
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
                        let _ = status.send(ThreadStatus::Stopped);
                        let _ = events.send(ThreadEvent::Stopped { thread_id });
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

impl CanonicalProviderRuntime {
    async fn mount_agent_process_tools(
        &mut self,
        context: &ThreadContext,
        services: &RuntimeServices,
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
            .unwrap_or_else(|| AgentToolRouter::new(Arc::new(OperationRegistry::new())));
        if !had_explicit_router && self.bash_tool_config.is_none() {
            self.strict_tool_router_unknowns = false;
        }
        if let Some(control) = control.clone() {
            let mut provider = KernelThreadOperationProvider::new(control.clone(), context.clone());
            if let Some(resolver) = &self.thread_spawn_agent_resolver {
                provider = provider.with_agent_resolver(Arc::clone(resolver));
            }
            let dispatcher: Arc<dyn KernelOperationDispatcher> = Arc::new(provider);
            let _ = router
                .operation_registry()
                .set_kernel_dispatcher(COOLDIS_THREADS_PACKAGE, Arc::clone(&dispatcher))
                .await;
            if let Some(config) = &self.bash_tool_config
                && let Some(registry) = &config.operation_registry
            {
                let _ = registry
                    .set_kernel_dispatcher(COOLDIS_THREADS_PACKAGE, Arc::clone(&dispatcher))
                    .await;
            }
            let schedule_dispatcher: Arc<dyn KernelOperationDispatcher> = Arc::new(
                KernelScheduleOperationProvider::new(control, context.clone()),
            );
            let _ = router
                .operation_registry()
                .set_kernel_dispatcher(COOLDIS_SCHEDULE_PACKAGE, Arc::clone(&schedule_dispatcher))
                .await;
            if let Some(config) = &self.bash_tool_config
                && let Some(registry) = &config.operation_registry
            {
                let _ = registry
                    .set_kernel_dispatcher(
                        COOLDIS_SCHEDULE_PACKAGE,
                        Arc::clone(&schedule_dispatcher),
                    )
                    .await;
            }
        }
        let process_cwd = self
            .bash_tool_config
            .as_ref()
            .map(|config| config.cwd.clone())
            .unwrap_or_else(default_process_dispatcher_cwd);
        let process_dispatcher: Arc<dyn KernelOperationDispatcher> = Arc::new(
            KernelProcessOperationProvider::new(context.clone(), process_cwd),
        );
        let _ = router
            .operation_registry()
            .set_kernel_dispatcher(COOLDIS_PROCESS_PACKAGE, Arc::clone(&process_dispatcher))
            .await;
        if let Some(config) = &self.bash_tool_config
            && let Some(registry) = &config.operation_registry
        {
            let _ = registry
                .set_kernel_dispatcher(COOLDIS_PROCESS_PACKAGE, Arc::clone(&process_dispatcher))
                .await;
        }
        let notify_dispatcher: Arc<dyn KernelOperationDispatcher> =
            Arc::new(KernelNotifyOperationProvider);
        let _ = router
            .operation_registry()
            .set_kernel_dispatcher(COOLDIS_NOTIFY_PACKAGE, Arc::clone(&notify_dispatcher))
            .await;
        if let Some(config) = &self.bash_tool_config
            && let Some(registry) = &config.operation_registry
        {
            let _ = registry
                .set_kernel_dispatcher(COOLDIS_NOTIFY_PACKAGE, Arc::clone(&notify_dispatcher))
                .await;
        }
        if let Some(config) = &self.bash_tool_config {
            router =
                router.with_kernel_tool_provider(Arc::new(BashToolProvider::new(config.clone())));
        }
        self.tool_router = Some(Arc::new(router));
    }

    async fn run_session_start_hooks(
        &self,
        context: &ThreadContext,
        services: &RuntimeServices,
        thread_id: crate::ThreadId,
        events: &broadcast::Sender<ThreadEvent>,
    ) -> CooldisResult<bool> {
        let Some(hook_pipeline) = &self.hook_pipeline else {
            return Ok(false);
        };
        let coordinates = &context.coordinates;
        let outcome = hook_pipeline
            .run_session_start(
                SessionStartHookRequest {
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
        thread_context: &ThreadContext,
        turn_id: String,
        input: &TurnInput,
        cancellation: CancellationToken,
    ) -> TurnContext {
        TurnContext::new(thread_context.clone(), turn_id, input, cancellation)
            .with_effective_model_provider(self.config.provider.clone(), self.config.model.clone())
            .with_budget(TurnBudget {
                max_tool_rounds: Some(MAX_TOOL_ROUTER_ROUNDS),
                max_output_tokens: Some(self.config.max_tokens),
                max_context_text_bytes: self
                    .client
                    .capabilities()
                    .and_then(|capabilities| capabilities.context_policy.max_text_bytes),
            })
    }

    async fn run_turn(
        &self,
        turn_context: &TurnContext,
        services: &RuntimeServices,
        events: &broadcast::Sender<ThreadEvent>,
        steering_contexts: Vec<String>,
    ) -> CooldisResult<CanonicalMessage> {
        let coordinates = turn_context.coordinates();
        let context = services.build_session_context(coordinates).await?;
        let source_cuts = context.source_cuts.clone();
        let session_entries = context.entries;
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
        let compiled_context = AgentContextCompiler::compile(AgentContextCompileInput {
            system: self.config.system.clone(),
            static_system_sources: static_context_segments.clone(),
            session_entries: session_entries.clone(),
            turn_context: turn_context.snapshot(),
            hook_contexts: steering_contexts,
            environment_contexts,
            attachments: Vec::new(),
            tools: self.tool_definitions().await,
            policy: self.context_compile_policy.clone(),
        });
        let mut request = self
            .config
            .request_from_messages(compiled_context.messages.clone());
        request.system = compiled_context.system.clone();
        request.tools = compiled_context.tools.clone();
        if let Some(thinking) = &turn_context.thinking {
            request.thinking = Some(thinking.clone());
        }
        let transformed = normalize_history_for_target(
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
            ProviderRequestMode::Stream
        } else {
            ProviderRequestMode::Complete
        };
        if let Some(capabilities) = self.client.capabilities() {
            let (compiled, provider_compilation) =
                compile_provider_request_context(request, &capabilities.context_policy);
            request = compiled;
            let transformed = normalize_history_for_target(
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
        emit_runtime_event(
            events,
            coordinates,
            RuntimeEventKind::ContextCompiled {
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
            RuntimeModelRequestPurpose::Turn,
            events,
        )
        .await?;
        let response = executed.response;
        Ok(CanonicalMessage::assistant_with_usage(
            executed.request.provider,
            executed.request.api,
            executed.request.model,
            response.content,
            response.usage,
            response.stop_reason,
        ))
    }

    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        let mut tools = self.config.tools.clone();
        let Some(tool_router) = &self.tool_router else {
            return tools;
        };
        let mut names = tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<BTreeSet<_>>();
        for tool in tool_router.tool_definitions().await {
            if names.insert(tool.name.clone()) {
                tools.push(tool);
            }
        }
        tools
    }
}

async fn run_idle_provider_command(
    runtime: &CanonicalProviderRuntime,
    thread_context: &ThreadContext,
    command: ThreadCommand,
    coordinates: &crate::ThreadCoordinates,
    services: &RuntimeServices,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    status: &watch::Sender<ThreadStatus>,
    commands: &mut mpsc::Receiver<ThreadCommand>,
    runtime_cancellation: &CancellationToken,
    pending_commands: &mut VecDeque<ThreadCommand>,
) -> bool {
    match command {
        ThreadCommand::Submit {
            turn_id,
            input,
            mode,
        } => {
            if mode == TurnSubmissionMode::Steer {
                emit_runtime_event(
                    events,
                    coordinates,
                    RuntimeEventKind::PolicyRejected {
                        code: "no_active_turn".to_string(),
                        message: "steer input requires an active provider turn".to_string(),
                    },
                );
                return false;
            }
            let _ = status.send(ThreadStatus::Running);
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
            match services.append_user_turn_input(coordinates, &input).await {
                Ok(entry) => {
                    if let Err(err) =
                        append_turn_submitted_event(services, coordinates, &turn_id, &entry).await
                    {
                        let _ = status.send(ThreadStatus::Failed);
                        let _ = events.send(ThreadEvent::Failed {
                            thread_id,
                            message: err.to_string(),
                        });
                        return true;
                    }
                    let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                }
                Err(err) => {
                    let _ = status.send(ThreadStatus::Failed);
                    let _ = events.send(ThreadEvent::Failed {
                        thread_id,
                        message: err.to_string(),
                    });
                    return true;
                }
            }
            run_provider_turn(
                runtime,
                thread_context,
                turn_id,
                input,
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
        ThreadCommand::Compact {
            turn_id,
            trigger,
            summary,
        } => {
            let _ = status.send(ThreadStatus::Running);
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
                    let _ = status.send(ThreadStatus::Idle);
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
        ThreadCommand::ResumeToolCall { turn_id, call_id } => {
            let _ = status.send(ThreadStatus::Running);
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
                Ok(ToolResumeOutcome::Resumed) => {
                    run_provider_turn(
                        runtime,
                        thread_context,
                        turn_id,
                        TurnInput::text(""),
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
                    let _ = status.send(ThreadStatus::Idle);
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
        ThreadCommand::Cancel { reason } => {
            let _ = status.send(ThreadStatus::Cancelling);
            let _ = events.send(ThreadEvent::Signal {
                thread_id,
                signal: ThreadSignal::interrupt_cancel(coordinates, reason.clone()),
            });
            let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
            let _ = status.send(ThreadStatus::Idle);
            false
        }
        ThreadCommand::Shutdown => {
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
            let _ = status.send(ThreadStatus::Stopped);
            let _ = events.send(ThreadEvent::Stopped { thread_id });
            true
        }
    }
}

async fn run_auto_compaction_if_needed(
    runtime: &CanonicalProviderRuntime,
    thread_context: &ThreadContext,
    turn_id: String,
    coordinates: &crate::ThreadCoordinates,
    services: &RuntimeServices,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
) -> CooldisResult<()> {
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
    let compiled_context = AgentContextCompiler::compile(AgentContextCompileInput {
        system: runtime.config.system.clone(),
        static_system_sources: static_context_segments,
        session_entries: context.entries,
        turn_context: runtime
            .turn_context(
                thread_context,
                turn_id.clone(),
                &TurnInput::text(""),
                CancellationToken::new(),
            )
            .snapshot(),
        hook_contexts: Vec::new(),
        environment_contexts,
        attachments: Vec::new(),
        tools: Vec::new(),
        policy: AgentContextCompilePolicy::unbounded(),
    });
    if compiled_context.diagnostics.retained_text_bytes <= max_text_bytes {
        return Ok(());
    }
    run_compaction(
        runtime,
        thread_context,
        turn_id,
        CompactionTrigger::Auto,
        None,
        services,
        thread_id,
        events,
    )
    .await
}

async fn run_compaction(
    runtime: &CanonicalProviderRuntime,
    thread_context: &ThreadContext,
    turn_id: String,
    trigger: CompactionTrigger,
    requested_summary: Option<String>,
    services: &RuntimeServices,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
) -> CooldisResult<()> {
    let input = TurnInput::text("");
    let turn_context =
        runtime.turn_context(thread_context, turn_id, &input, CancellationToken::new());
    if let Some(hook_pipeline) = &runtime.hook_pipeline {
        let outcome = hook_pipeline
            .run_pre_compact(
                PreCompactHookRequest {
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
            emit_runtime_event(
                events,
                turn_context.coordinates(),
                RuntimeEventKind::PolicyRejected {
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
    services
        .record_context_summary_checkpoint(
            turn_context.coordinates(),
            &source_context.entries,
            &source_context.source_cuts,
            &summary,
        )
        .await?;
    let entry = services
        .append_session_entry(
            turn_context.coordinates(),
            None,
            SessionEntryKind::Compaction {
                summary: summary.clone(),
            },
        )
        .await?;
    let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
    emit_runtime_event(
        events,
        turn_context.coordinates(),
        RuntimeEventKind::Compaction {
            trigger,
            summary: summary.clone(),
        },
    );

    if let Some(hook_pipeline) = &runtime.hook_pipeline {
        let outcome = hook_pipeline
            .run_post_compact(
                PostCompactHookRequest {
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
            emit_runtime_event(
                events,
                turn_context.coordinates(),
                RuntimeEventKind::PolicyRejected {
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
    runtime: &CanonicalProviderRuntime,
    turn_context: &TurnContext,
    services: &RuntimeServices,
    events: &broadcast::Sender<ThreadEvent>,
) -> CooldisResult<String> {
    let context = services
        .build_session_context(turn_context.coordinates())
        .await?;
    let fallback = deterministic_compaction_summary(&context.messages);
    if context.messages.is_empty() {
        return Ok(fallback);
    }
    let mut request = runtime.config.request_from_messages(context.messages);
    request.system.push(SystemBlock::text(
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
        ProviderRequestMode::Complete,
        RuntimeModelRequestPurpose::Compaction,
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
    request: ProviderRequest,
    response: crate::ProviderResponse,
}

#[derive(Clone, Debug)]
struct ModelRequestAttemptError {
    class: RuntimeModelRequestErrorClass,
    message: String,
}

impl ModelRequestAttemptError {
    fn retryable(&self) -> bool {
        matches!(
            self.class,
            RuntimeModelRequestErrorClass::Retryable | RuntimeModelRequestErrorClass::RateLimited
        )
    }

    fn fallback_eligible(&self) -> bool {
        matches!(
            self.class,
            RuntimeModelRequestErrorClass::Retryable
                | RuntimeModelRequestErrorClass::RateLimited
                | RuntimeModelRequestErrorClass::UnsupportedCapability
        )
    }
}

async fn execute_provider_request(
    runtime: &CanonicalProviderRuntime,
    turn_context: &TurnContext,
    coordinates: &crate::ThreadCoordinates,
    request: &ProviderRequest,
    mode: ProviderRequestMode,
    purpose: RuntimeModelRequestPurpose,
    events: &broadcast::Sender<ThreadEvent>,
) -> CooldisResult<ExecutedProviderResponse> {
    let request_mode = mode;
    let mode = runtime_request_mode(mode);
    let mut endpoints = Vec::with_capacity(runtime.model_request_fallbacks.len() + 1);
    endpoints.push(ModelRequestEndpoint {
        config: runtime.config.clone(),
        client: Arc::clone(&runtime.client),
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
            emit_runtime_event(
                events,
                coordinates,
                RuntimeEventKind::ModelRequestStarted {
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
            let started_at = Instant::now();
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
                    emit_runtime_event(
                        events,
                        coordinates,
                        RuntimeEventKind::ModelRequestCompleted {
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
                    emit_runtime_event(
                        events,
                        coordinates,
                        RuntimeEventKind::ModelRequestFailed {
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
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::ModelRequestRetryScheduled {
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
                                    return Err(CooldisError::RuntimeExecution(
                                        ProviderError::Cancelled.to_string(),
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
                        emit_runtime_event(
                            events,
                            coordinates,
                            RuntimeEventKind::ModelRequestFallbackSelected {
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

                    return Err(CooldisError::RuntimeExecution(error.message));
                }
            }
        }
    }

    Err(CooldisError::RuntimeExecution(
        last_error
            .map(|error| error.message)
            .unwrap_or_else(|| "provider request did not run".to_string()),
    ))
}

async fn execute_provider_request_attempt(
    endpoint: &ModelRequestEndpoint,
    turn_context: &TurnContext,
    coordinates: &crate::ThreadCoordinates,
    request: &ProviderRequest,
    request_mode: ProviderRequestMode,
    mode: RuntimeModelRequestMode,
    events: &broadcast::Sender<ThreadEvent>,
) -> Result<crate::ProviderResponse, ModelRequestAttemptError> {
    if let Some(capabilities) = endpoint.client.capabilities() {
        capabilities
            .validate_request(request, request_mode)
            .map_err(classify_provider_error)?;
    }
    match mode {
        RuntimeModelRequestMode::Complete => endpoint
            .client
            .complete_cancellable(request, turn_context.cancellation.clone())
            .await
            .map_err(classify_provider_error),
        RuntimeModelRequestMode::Stream => {
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
    request: &ProviderRequest,
    config: &CanonicalProviderRuntimeConfig,
) -> ProviderRequest {
    let mut request = request.clone();
    request.api = config.api.clone();
    request.provider = config.provider.clone();
    request.model = config.model.clone();
    request
}

fn model_request_id(
    turn_context: &TurnContext,
    purpose: RuntimeModelRequestPurpose,
    mode: RuntimeModelRequestMode,
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

fn classify_provider_error(error: ProviderError) -> ModelRequestAttemptError {
    let message = error.to_string();
    let class = match error {
        ProviderError::Cancelled => RuntimeModelRequestErrorClass::Cancelled,
        ProviderError::UnsupportedCapability { .. } | ProviderError::ApiMismatch { .. } => {
            RuntimeModelRequestErrorClass::UnsupportedCapability
        }
        ProviderError::Http(_) => RuntimeModelRequestErrorClass::Retryable,
        ProviderError::HttpStatus { status, .. } if status.as_u16() == 429 => {
            RuntimeModelRequestErrorClass::RateLimited
        }
        ProviderError::HttpStatus { status, .. }
            if status.is_server_error() || matches!(status.as_u16(), 408 | 409 | 425) =>
        {
            RuntimeModelRequestErrorClass::Retryable
        }
        ProviderError::Decode(_) | ProviderError::HttpStatus { .. } => {
            RuntimeModelRequestErrorClass::Fatal
        }
    };
    ModelRequestAttemptError { class, message }
}

fn stream_assembly_error(message: impl Into<String>) -> ModelRequestAttemptError {
    ModelRequestAttemptError {
        class: RuntimeModelRequestErrorClass::StreamAssembly,
        message: message.into(),
    }
}

fn runtime_request_mode(mode: ProviderRequestMode) -> RuntimeModelRequestMode {
    match mode {
        ProviderRequestMode::Complete => RuntimeModelRequestMode::Complete,
        ProviderRequestMode::Stream => RuntimeModelRequestMode::Stream,
    }
}

fn provider_api_event_label(api: &ProviderApi) -> String {
    match api {
        ProviderApi::OpenAIResponses => "openai_responses".to_string(),
        ProviderApi::OpenAIChatCompletions => "openai_chat_completions".to_string(),
        ProviderApi::AnthropicMessages => "anthropic_messages".to_string(),
        ProviderApi::Other(provider_family) => provider_family.clone(),
    }
}

fn runtime_usage_from_canonical(usage: &crate::CanonicalUsage) -> RuntimeUsage {
    RuntimeUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        cache_read_input_tokens: usage.cache_read_input_tokens,
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn response_content_text(content: &[CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } | CanonicalContent::Thinking { text, .. } => {
                Some(text.as_str())
            }
            CanonicalContent::Image { .. } | CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

async fn run_provider_turn(
    runtime: &CanonicalProviderRuntime,
    thread_context: &ThreadContext,
    turn_id: String,
    turn_input: TurnInput,
    coordinates: &crate::ThreadCoordinates,
    services: &RuntimeServices,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    status: &watch::Sender<ThreadStatus>,
    commands: &mut mpsc::Receiver<ThreadCommand>,
    runtime_cancellation: &CancellationToken,
    pending_commands: &mut VecDeque<ThreadCommand>,
) -> bool {
    let mut tool_rounds = 0;
    let mut steering_contexts = Vec::new();
    let turn_cancellation = CancellationToken::new();
    let turn_context = runtime.turn_context(
        thread_context,
        turn_id.clone(),
        &turn_input,
        turn_cancellation.clone(),
    );
    if let Some(hook_pipeline) = &runtime.hook_pipeline {
        let outcome = hook_pipeline
            .run_user_prompt_submit(
                UserPromptSubmitHookRequest {
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
            emit_runtime_event(
                events,
                coordinates,
                RuntimeEventKind::Terminal {
                    state: RuntimeTerminalState::Stopped,
                },
            );
            let _ = status.send(ThreadStatus::Stopped);
            let _ = events.send(ThreadEvent::Stopped { thread_id });
            return true;
        }
    }
    loop {
        let mut cancelled_reason = None;
        let mut shutdown_after_turn = false;
        let mut failed = false;
        let mut continue_after_tools = false;
        let mut suspended_after_tools = false;
        let mut last_assistant_text = None;
        let turn = runtime.run_turn(&turn_context, services, events, steering_contexts.clone());
        tokio::pin!(turn);

        let result = loop {
            tokio::select! {
                result = &mut turn => break result,
                _ = runtime_cancellation.cancelled() => {
                    turn_cancellation.cancel();
                    cancelled_reason = Some("runtime cancellation requested".to_string());
                }
                command = commands.recv() => {
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
                            turn_cancellation.cancel();
                            cancelled_reason = Some(reason);
                        }
                        Some(ThreadCommand::Shutdown) | None => {
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
                            turn_cancellation.cancel();
                            shutdown_after_turn = true;
                        }
                        Some(ThreadCommand::Submit { turn_id, input, mode }) => {
                            match mode {
                                TurnSubmissionMode::Queue => {
                                    let _ = events.send(ThreadEvent::Signal {
                                        thread_id,
                                        signal: ThreadSignal::user_queue(coordinates, turn_id.clone()),
                                    });
                                    pending_commands.push_back(ThreadCommand::Submit {
                                        turn_id,
                                        input,
                                        mode,
                                    });
                                }
                                TurnSubmissionMode::Steer => {
                                    let _ = events.send(ThreadEvent::Signal {
                                        thread_id,
                                        signal: ThreadSignal::user_steer(
                                            coordinates,
                                            turn_id.clone(),
                                        )
                                        .with_metadata(BTreeMap::from([(
                                            "active_turn_id".to_string(),
                                            turn_context.turn_id.clone(),
                                        )])),
                                    });
                                    if let Some(context) = steering_context(&turn_id, &input) {
                                        steering_contexts.push(context);
                                    }
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
                                    turn_cancellation.cancel();
                                    cancelled_reason = Some(reason);
                                    pending_commands.push_front(ThreadCommand::Submit {
                                        turn_id,
                                        input,
                                        mode: TurnSubmissionMode::Queue,
                                    });
                                }
                            }
                        }
                        Some(command @ ThreadCommand::Compact { .. }) => {
                            pending_commands.push_back(command);
                        }
                        Some(command @ ThreadCommand::ResumeToolCall { .. }) => {
                            pending_commands.push_back(command);
                        }
                    }
                }
            }
        };

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
                Ok(message) => {
                    let text = text_from_message(&message);
                    last_assistant_text = Some(text.clone());
                    let tool_calls = tool_calls_from_message(&message);
                    if !runtime.config.stream
                        && let Some(usage) = usage_from_message(&message)
                    {
                        emit_runtime_event(events, coordinates, RuntimeEventKind::Usage { usage });
                    }
                    if !runtime.config.stream {
                        for tool_call in &tool_calls {
                            emit_runtime_event(
                                events,
                                coordinates,
                                RuntimeEventKind::ToolCallStarted {
                                    call_id: tool_call.id.clone(),
                                    name: tool_call.name.clone(),
                                    input: tool_call.arguments.clone(),
                                },
                            );
                        }
                    }
                    match services
                        .append_session_entry(
                            coordinates,
                            None,
                            SessionEntryKind::Message {
                                message: message.clone(),
                            },
                        )
                        .await
                    {
                        Ok(entry) => {
                            let assistant_entry_id = entry.entry_id;
                            let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
                            if !runtime.config.stream {
                                emit_non_stream_content_events(events, coordinates, &message);
                            }
                            if !text.is_empty() {
                                let _ = events.send(ThreadEvent::Output { thread_id, text });
                            }
                            if runtime.tool_router.is_some() && !tool_calls.is_empty() {
                                if tool_rounds >= MAX_TOOL_ROUTER_ROUNDS {
                                    failed = true;
                                    fail_provider_turn(
                                        coordinates,
                                        thread_id,
                                        events,
                                        status,
                                        "tool_router",
                                        format!(
                                            "tool router exceeded {MAX_TOOL_ROUTER_ROUNDS} rounds"
                                        ),
                                    );
                                } else {
                                    match append_tool_results(
                                        runtime,
                                        &turn_context,
                                        services,
                                        thread_id,
                                        events,
                                        tool_calls,
                                        assistant_entry_id,
                                    )
                                    .await
                                    {
                                        Ok(outcome) => match outcome {
                                            ToolAppendOutcome::NoTools => {}
                                            ToolAppendOutcome::AppendedResults => {
                                                tool_rounds += 1;
                                                continue_after_tools = true;
                                            }
                                            ToolAppendOutcome::Suspended => {
                                                suspended_after_tools = true;
                                            }
                                        },
                                        Err(err) => {
                                            failed = true;
                                            fail_provider_turn(
                                                coordinates,
                                                thread_id,
                                                events,
                                                status,
                                                "tool_router",
                                                err.to_string(),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            failed = true;
                            fail_provider_turn(
                                coordinates,
                                thread_id,
                                events,
                                status,
                                "history",
                                err.to_string(),
                            );
                        }
                    }
                }
                Err(err) => {
                    failed = true;
                    let _ = events.send(ThreadEvent::Signal {
                        thread_id,
                        signal: ThreadSignal::failed(coordinates, err.to_string()),
                    });
                    fail_provider_turn(
                        coordinates,
                        thread_id,
                        events,
                        status,
                        "runtime_execution",
                        err.to_string(),
                    );
                }
            }
        }

        if shutdown_after_turn {
            emit_runtime_event(
                events,
                coordinates,
                RuntimeEventKind::Terminal {
                    state: RuntimeTerminalState::Stopped,
                },
            );
            let _ = status.send(ThreadStatus::Stopped);
            let _ = events.send(ThreadEvent::Stopped { thread_id });
            return true;
        }
        if failed {
            return true;
        }
        if suspended_after_tools {
            let _ = status.send(ThreadStatus::Idle);
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
        if let Err(err) =
            append_turn_completed_event(services, &turn_context.thread, &turn_id).await
        {
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
        emit_runtime_event(
            events,
            coordinates,
            RuntimeEventKind::Terminal {
                state: RuntimeTerminalState::Completed,
            },
        );
        let _ = status.send(ThreadStatus::Idle);
        return false;
    }
}

fn fail_provider_turn(
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    status: &watch::Sender<ThreadStatus>,
    code: impl Into<String>,
    message: String,
) {
    let _ = status.send(ThreadStatus::Failed);
    emit_runtime_event(
        events,
        coordinates,
        RuntimeEventKind::Failed {
            code: code.into(),
            message: message.clone(),
        },
    );
    let _ = events.send(ThreadEvent::Failed { thread_id, message });
}

#[derive(Clone, Debug)]
struct ProviderToolCall {
    id: String,
    name: String,
    arguments: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolAppendOutcome {
    NoTools,
    AppendedResults,
    Suspended,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolResumeOutcome {
    Resumed,
    StillWaiting,
    AlreadyCompleted,
}

#[derive(Clone, Debug)]
struct PendingToolCallRequest {
    request_event_id: EventRecordId,
    subject: ToolCallSubject,
    snapshot_id: String,
    tool_name: String,
    arguments: Value,
}

fn tool_calls_from_message(message: &CanonicalMessage) -> Vec<ProviderToolCall> {
    match message {
        CanonicalMessage::Assistant { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                CanonicalContent::ToolCall {
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
        CanonicalMessage::User { .. } | CanonicalMessage::ToolResult { .. } => Vec::new(),
    }
}

fn skill_context_segments_from_thread(
    thread: &ThreadContext,
) -> CooldisResult<Vec<AgentManifestStaticContextSegment>> {
    let Some(raw) = thread
        .metadata
        .get(THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA)
    else {
        return Ok(Vec::new());
    };
    let segments =
        serde_json::from_str::<Vec<AgentManifestStaticContextSegment>>(raw).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "thread manifest skill context segments are invalid: {err}"
            ))
        })?;
    for segment in &segments {
        let expected = sha256_hex(segment.content.as_bytes());
        if segment.content_sha256 != expected {
            return Err(CooldisError::RuntimeFactory(format!(
                "thread manifest skill context segment {:?} content hash mismatch: expected {}, got {}",
                segment.id, expected, segment.content_sha256
            )));
        }
    }
    Ok(segments)
}

fn static_context_segments_from_thread(
    thread: &ThreadContext,
) -> CooldisResult<Vec<AgentManifestStaticContextSegment>> {
    let Some(raw) = thread
        .metadata
        .get(THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA)
    else {
        return Ok(Vec::new());
    };
    let segments =
        serde_json::from_str::<Vec<AgentManifestStaticContextSegment>>(raw).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "thread manifest static context segments are invalid: {err}"
            ))
        })?;
    for segment in &segments {
        let expected = sha256_hex(segment.content.as_bytes());
        if segment.content_sha256 != expected {
            return Err(CooldisError::RuntimeFactory(format!(
                "thread manifest static context segment {:?} content hash mismatch: expected {}, got {}",
                segment.id, expected, segment.content_sha256
            )));
        }
    }
    Ok(segments)
}

fn context_receipt_static_segments(
    static_context_segments: &[AgentManifestStaticContextSegment],
    skill_context_segments: &[AgentManifestStaticContextSegment],
) -> Vec<AgentManifestStaticContextSegment> {
    static_context_segments
        .iter()
        .chain(skill_context_segments)
        .cloned()
        .collect()
}

fn context_compile_receipt_payload(
    session_entries: &[SessionEntry],
    compiled_context: &CompiledAgentContext,
    static_context_segments: &[AgentManifestStaticContextSegment],
    diagnostics: &crate::AgentContextCompilationDiagnostics,
    replay_transform: &ReplayTransformCounts,
    provider_dropped_messages: usize,
    provider_truncated_text_bytes: usize,
    provider_retained_text_bytes: usize,
) -> CooldisResult<Value> {
    let encoded_messages = serde_json::to_vec(&compiled_context.messages)
        .map_err(|err| CooldisError::History(format!("context receipt codec failed: {err}")))?;
    let diagnostics = serde_json::to_value(diagnostics)
        .map_err(|err| CooldisError::History(format!("context receipt codec failed: {err}")))?;
    let replay_transform = serde_json::to_value(replay_transform)
        .map_err(|err| CooldisError::History(format!("context receipt codec failed: {err}")))?;
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
        "output_hash": sha256_hex(&encoded_messages),
    }))
}

async fn resume_pending_tool_call(
    runtime: &CanonicalProviderRuntime,
    thread_context: &ThreadContext,
    turn_id: &str,
    call_id: &str,
    services: &RuntimeServices,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
) -> CooldisResult<ToolResumeOutcome> {
    let Some(tool_router) = &runtime.tool_router else {
        return Err(CooldisError::RuntimeExecution(
            "tool resume requires a tool router".to_string(),
        ));
    };
    if tool_call_completed_exists(services, &thread_context.coordinates, turn_id, call_id).await? {
        return Ok(ToolResumeOutcome::AlreadyCompleted);
    }
    if !tool_call_suspension_exists(services, &thread_context.coordinates, turn_id, call_id).await?
    {
        return Err(CooldisError::RuntimeExecution(format!(
            "no pending suspended tool call {turn_id}/{call_id}"
        )));
    }
    let Some(request) =
        pending_tool_call_request(services, &thread_context.coordinates, turn_id, call_id).await?
    else {
        return Err(CooldisError::RuntimeExecution(format!(
            "missing tool.call.requested for pending call {turn_id}/{call_id}"
        )));
    };
    let decision = decide_tool_call(
        services.runtime_store().as_ref(),
        ToolDecisionRequest {
            coordinates: thread_context.coordinates.clone(),
            subject: request.subject.clone(),
            snapshot_id: request.snapshot_id.clone(),
            request_event_id: request.request_event_id,
        },
    )
    .await?;
    match decision {
        ToolCallDecision::NoDecision | ToolCallDecision::Wait { .. } => {
            Ok(ToolResumeOutcome::StillWaiting)
        }
        ToolCallDecision::Allow { consumed_fact_id } => {
            append_turn_resumed_event(
                services,
                &thread_context.coordinates,
                turn_id,
                consumed_fact_id,
            )
            .await?;
            let turn_context = runtime.turn_context(
                thread_context,
                turn_id.to_string(),
                &TurnInput::text(""),
                CancellationToken::new(),
            );
            let interceptor = ToolExecutionInterceptor::new(Arc::clone(tool_router))
                .with_hook_pipeline(runtime.hook_pipeline.clone())
                .with_permission_gate(Arc::clone(&runtime.tool_permission_gate));
            execute_tool_call_with_interceptor(
                &interceptor,
                services,
                &turn_context,
                thread_id,
                events,
                call_id.to_string(),
                request.tool_name,
                request.arguments,
                request.snapshot_id,
            )
            .await?;
            Ok(ToolResumeOutcome::Resumed)
        }
        ToolCallDecision::Rewrite {
            consumed_fact_id,
            arguments,
        } => {
            append_turn_resumed_event(
                services,
                &thread_context.coordinates,
                turn_id,
                consumed_fact_id,
            )
            .await?;
            let turn_context = runtime.turn_context(
                thread_context,
                turn_id.to_string(),
                &TurnInput::text(""),
                CancellationToken::new(),
            );
            let interceptor = ToolExecutionInterceptor::new(Arc::clone(tool_router))
                .with_hook_pipeline(runtime.hook_pipeline.clone())
                .with_permission_gate(Arc::clone(&runtime.tool_permission_gate));
            execute_tool_call_with_interceptor(
                &interceptor,
                services,
                &turn_context,
                thread_id,
                events,
                call_id.to_string(),
                request.tool_name,
                arguments,
                request.snapshot_id,
            )
            .await?;
            Ok(ToolResumeOutcome::Resumed)
        }
        ToolCallDecision::Deny {
            consumed_fact_id,
            reason,
            ..
        } => {
            append_turn_resumed_event(
                services,
                &thread_context.coordinates,
                turn_id,
                consumed_fact_id.unwrap_or(request.request_event_id),
            )
            .await?;
            let turn_context = runtime.turn_context(
                thread_context,
                turn_id.to_string(),
                &TurnInput::text(""),
                CancellationToken::new(),
            );
            append_denied_tool_result(
                services,
                &turn_context,
                thread_id,
                events,
                call_id.to_string(),
                request.tool_name,
                request.snapshot_id,
                reason,
            )
            .await?;
            Ok(ToolResumeOutcome::Resumed)
        }
    }
}

async fn append_tool_results(
    runtime: &CanonicalProviderRuntime,
    turn_context: &TurnContext,
    services: &RuntimeServices,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    tool_calls: Vec<ProviderToolCall>,
    assistant_entry_id: SessionEntryId,
) -> CooldisResult<ToolAppendOutcome> {
    let Some(tool_router) = &runtime.tool_router else {
        return Ok(ToolAppendOutcome::NoTools);
    };
    let tool_calls = if runtime.strict_tool_router_unknowns {
        tool_calls
    } else {
        let tool_names = tool_router
            .tool_definitions()
            .await
            .into_iter()
            .map(|tool| tool.name)
            .collect::<BTreeSet<_>>();
        tool_calls
            .into_iter()
            .filter(|tool_call| tool_names.contains(&tool_call.name))
            .collect::<Vec<_>>()
    };
    if tool_calls.is_empty() {
        return Ok(ToolAppendOutcome::NoTools);
    }
    let interceptor = ToolExecutionInterceptor::new(Arc::clone(tool_router))
        .with_hook_pipeline(runtime.hook_pipeline.clone())
        .with_permission_gate(Arc::clone(&runtime.tool_permission_gate));
    for tool_call in tool_calls {
        let call_id = tool_call.id.clone();
        let tool_name = tool_call.name.clone();
        let active_snapshot_id = active_manifest_bind_receipt(
            services.runtime_store().as_ref(),
            turn_context.coordinates(),
        )
        .await?
        .map(|(_, receipt)| receipt.manifest_hash)
        .unwrap_or_else(|| "unbound".to_string());
        let request_event = append_tool_call_requested_event(
            services,
            turn_context,
            &tool_call,
            &active_snapshot_id,
            assistant_entry_id,
        )
        .await?;
        let controller = active_tool_controller_for_request(
            services.runtime_store().as_ref(),
            turn_context.coordinates(),
            &tool_name,
        )
        .await?;
        let mut arguments = tool_call.arguments;
        if let Some(controller) = controller {
            match decide_tool_call(
                services.runtime_store().as_ref(),
                ToolDecisionRequest {
                    coordinates: turn_context.coordinates().clone(),
                    subject: ToolCallSubject {
                        turn_id: turn_context.turn_id.clone(),
                        call_id: call_id.clone(),
                    },
                    snapshot_id: controller.snapshot_id,
                    request_event_id: request_event.id,
                },
            )
            .await?
            {
                ToolCallDecision::NoDecision => {
                    append_denied_tool_result(
                        services,
                        turn_context,
                        thread_id,
                        events,
                        call_id,
                        tool_name,
                        active_snapshot_id,
                        "tool controller did not emit a terminal decision".to_string(),
                    )
                    .await?;
                    continue;
                }
                ToolCallDecision::Allow { .. } => {}
                ToolCallDecision::Rewrite {
                    arguments: rewritten,
                    ..
                } => {
                    arguments = rewritten;
                }
                ToolCallDecision::Deny { reason, .. } => {
                    append_denied_tool_result(
                        services,
                        turn_context,
                        thread_id,
                        events,
                        call_id,
                        tool_name,
                        active_snapshot_id,
                        reason,
                    )
                    .await?;
                    continue;
                }
                ToolCallDecision::Wait {
                    consumed_fact_id,
                    approval_id,
                    reason,
                } => {
                    append_turn_waiting_event(
                        services,
                        turn_context,
                        &call_id,
                        &active_snapshot_id,
                        consumed_fact_id,
                        approval_id,
                        reason,
                    )
                    .await?;
                    return Ok(ToolAppendOutcome::Suspended);
                }
            }
        }
        execute_tool_call_with_interceptor(
            &interceptor,
            services,
            turn_context,
            thread_id,
            events,
            call_id,
            tool_name,
            arguments,
            active_snapshot_id,
        )
        .await?;
    }
    Ok(ToolAppendOutcome::AppendedResults)
}

async fn execute_tool_call_with_interceptor(
    interceptor: &ToolExecutionInterceptor,
    services: &RuntimeServices,
    turn_context: &TurnContext,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    call_id: String,
    tool_name: String,
    arguments: Value,
    snapshot_id: String,
) -> CooldisResult<()> {
    let witness_coordinates = turn_context.coordinates().clone();
    let outcome =
        interceptor
            .execute_with_witnessing(
                ToolExecutionRequest {
                    turn_context,
                    call_id: call_id.clone(),
                    tool_name: tool_name.clone(),
                    arguments,
                },
                |spec| emit_hook_started(events, turn_context.coordinates(), spec),
                |witnesses| {
                    let coordinates = witness_coordinates.clone();
                    async move {
                        append_hook_mutation_witnesses(services, &coordinates, witnesses).await
                    }
                },
            )
            .await?;
    emit_hook_records(events, turn_context.coordinates(), &outcome.hook_records);
    if let Some(permission_decision) = &outcome.permission_decision {
        let (decision, reason) = match permission_decision {
            ToolPermissionDecision::Allow => (RuntimePermissionDecision::Allow, None),
            ToolPermissionDecision::Deny { reason } => {
                (RuntimePermissionDecision::Deny, Some(reason.clone()))
            }
        };
        emit_runtime_event(
            events,
            turn_context.coordinates(),
            RuntimeEventKind::PermissionDecision {
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
        CanonicalMessage::ToolResult {
            is_error: false,
            ..
        }
    );
    emit_runtime_event(
        events,
        turn_context.coordinates(),
        RuntimeEventKind::ToolLog {
            call_id: call_id.clone(),
            tool_name: tool_name.clone(),
            level: if tool_success {
                RuntimeToolLogLevel::Info
            } else {
                RuntimeToolLogLevel::Error
            },
            message: if tool_success {
                "tool completed".to_string()
            } else {
                "tool failed".to_string()
            },
            metadata: BTreeMap::from([
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
        outcome.result,
        Some(outcome.duration_ms),
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
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    turn_id: &str,
    entry: &SessionEntry,
) -> CooldisResult<crate::EventRecord> {
    if let Some(existing) = existing_turn_submitted_event(services, coordinates, turn_id).await? {
        return Ok(existing);
    }
    services
        .append_thread_event(
            coordinates,
            NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::TurnSubmitted,
                serde_json::json!({
                    "turn_id": turn_id,
                    "entry_id": entry.entry_id.to_string(),
                }),
            ),
        )
        .await
}

async fn existing_turn_submitted_event(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    turn_id: &str,
) -> CooldisResult<Option<crate::EventRecord>> {
    let events = services
        .runtime_store()
        .read_events(&EventStreamId::for_thread(coordinates), None)
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    Ok(events
        .into_iter()
        .filter(|event| {
            event.kind == EventKind::TurnSubmitted
                && event
                    .payload
                    .get("turn_id")
                    .and_then(|value| value.as_str())
                    == Some(turn_id)
        })
        .max_by_key(|event| event.sequence.get()))
}

async fn append_turn_completed_event(
    services: &RuntimeServices,
    thread_context: &ThreadContext,
    turn_id: &str,
) -> CooldisResult<crate::EventRecord> {
    let coordinates = &thread_context.coordinates;
    let latest_source_id = latest_thread_event_id(services, coordinates).await?;
    let completed = services
        .append_thread_event(
            coordinates,
            NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::TurnCompleted,
                serde_json::json!({
                    "turn_id": turn_id,
                }),
                EventProvenance {
                    source_streams: vec![EventStreamId::for_thread(coordinates)],
                    source_event_ids: latest_source_id.into_iter().collect(),
                    discharged_by: Some("propagator:agent-loop".to_string()),
                    function: Some("turn_complete/v1".to_string()),
                    ..EventProvenance::default()
                },
            ),
        )
        .await?;
    services
        .append_thread_joined_event_if_spawned(
            thread_context,
            ThreadTerminalState::Completed,
            None,
            Some(completed.id),
        )
        .await?;
    Ok(completed)
}

async fn append_turn_resumed_event(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    turn_id: &str,
    consumed_fact_id: EventRecordId,
) -> CooldisResult<crate::EventRecord> {
    services
        .append_control_event(
            coordinates,
            NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::TurnResumed,
                serde_json::json!({
                    "turn_id": turn_id,
                    "consumed_fact_id": consumed_fact_id.to_string(),
                }),
                EventProvenance {
                    source_streams: vec![EventStreamId::new(format!(
                        "control:{}",
                        coordinates.thread_id
                    ))],
                    source_event_ids: vec![consumed_fact_id],
                    discharged_by: Some("scheduler:tool-decision".to_string()),
                    function: Some("tool_resume/v1".to_string()),
                    ..EventProvenance::default()
                },
            ),
        )
        .await
}

async fn append_tool_call_requested_event(
    services: &RuntimeServices,
    turn_context: &TurnContext,
    tool_call: &ProviderToolCall,
    snapshot_id: &str,
    assistant_entry_id: SessionEntryId,
) -> CooldisResult<crate::EventRecord> {
    let assistant_event_id =
        session_entry_event_id(services, turn_context.coordinates(), assistant_entry_id).await?;
    let mut payload = serde_json::to_value(ToolCallRequestedPayload {
        subject: ToolCallSubject {
            turn_id: turn_context.turn_id.clone(),
            call_id: tool_call.id.clone(),
        },
        snapshot_id: snapshot_id.to_string(),
        tool_name: tool_call.name.clone(),
        arguments: tool_call.arguments.clone(),
    })
    .map_err(|err| CooldisError::History(format!("tool request payload codec failed: {err}")))?;
    if let Some(object) = payload.as_object_mut() {
        object.insert("tool".to_string(), Value::String(tool_call.name.clone()));
    }
    services
        .append_thread_event(
            turn_context.coordinates(),
            NewEventRecord::discharged(
                turn_context.coordinates().clone(),
                EventKind::ToolCallRequested,
                payload,
                EventProvenance {
                    source_streams: vec![EventStreamId::for_thread(turn_context.coordinates())],
                    source_event_ids: assistant_event_id.into_iter().collect(),
                    discharged_by: Some("propagator:agent-loop".to_string()),
                    function: Some("tool_request/v1".to_string()),
                    ..EventProvenance::default()
                },
            ),
        )
        .await
}

async fn append_denied_tool_result(
    services: &RuntimeServices,
    turn_context: &TurnContext,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    call_id: String,
    tool_name: String,
    snapshot_id: String,
    reason: String,
) -> CooldisResult<()> {
    emit_runtime_event(
        events,
        turn_context.coordinates(),
        RuntimeEventKind::ToolLog {
            call_id: call_id.clone(),
            tool_name: tool_name.clone(),
            level: RuntimeToolLogLevel::Error,
            message: "tool denied".to_string(),
            metadata: BTreeMap::from([
                ("duration_ms".to_string(), "0".to_string()),
                ("success".to_string(), "false".to_string()),
            ]),
        },
    );
    let result = CanonicalMessage::tool_result(call_id.clone(), tool_name.clone(), reason, true);
    append_tool_result_message(
        services,
        turn_context.coordinates(),
        thread_id,
        events,
        call_id,
        tool_name.clone(),
        turn_context.turn_id.clone(),
        snapshot_id,
        result,
        Some(0),
    )
    .await
}

async fn append_turn_waiting_event(
    services: &RuntimeServices,
    turn_context: &TurnContext,
    call_id: &str,
    snapshot_id: &str,
    waiting_on_event_id: EventRecordId,
    approval_id: Option<String>,
    reason: Option<String>,
) -> CooldisResult<()> {
    services
        .append_control_event(
            turn_context.coordinates(),
            NewEventRecord::discharged(
                turn_context.coordinates().clone(),
                EventKind::TurnWaiting,
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
                EventProvenance {
                    source_streams: vec![EventStreamId::new(format!(
                        "control:{}",
                        turn_context.coordinates().thread_id
                    ))],
                    source_event_ids: vec![waiting_on_event_id],
                    discharged_by: Some("scheduler:tool-decision".to_string()),
                    function: Some("tool_wait/v1".to_string()),
                    ..EventProvenance::default()
                },
            ),
        )
        .await?;
    Ok(())
}

async fn session_entry_event_id(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    entry_id: SessionEntryId,
) -> CooldisResult<Option<EventRecordId>> {
    let entry_id = entry_id.to_string();
    let events = services
        .runtime_store()
        .read_events(&EventStreamId::for_thread(coordinates), None)
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    Ok(events
        .into_iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload.get("entry_id").and_then(Value::as_str) == Some(entry_id.as_str())
        })
        .map(|event| event.id))
}

async fn latest_thread_event_id(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
) -> CooldisResult<Option<EventRecordId>> {
    let events = services
        .runtime_store()
        .read_events(&EventStreamId::for_thread(coordinates), None)
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    Ok(events
        .into_iter()
        .max_by_key(|event| event.sequence.get())
        .map(|event| event.id))
}

async fn pending_tool_call_request(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    turn_id: &str,
    call_id: &str,
) -> CooldisResult<Option<PendingToolCallRequest>> {
    let events = services
        .runtime_store()
        .read_events(&EventStreamId::for_thread(coordinates), None)
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    let mut matches = Vec::new();
    for event in events
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallRequested)
    {
        let payload = serde_json::from_value::<ToolCallRequestedPayload>(event.payload.clone())
            .map_err(|err| {
                CooldisError::History(format!("tool.call.requested payload is invalid: {err}"))
            })?;
        if payload.subject.turn_id == turn_id && payload.subject.call_id == call_id {
            matches.push(PendingToolCallRequest {
                request_event_id: event.id,
                subject: payload.subject,
                snapshot_id: payload.snapshot_id,
                tool_name: payload.tool_name,
                arguments: payload.arguments,
            });
        }
    }
    Ok(matches.pop())
}

async fn tool_call_completed_exists(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    turn_id: &str,
    call_id: &str,
) -> CooldisResult<bool> {
    let events = services
        .runtime_store()
        .read_events(&EventStreamId::for_thread(coordinates), None)
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    for event in events
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
    {
        let payload = serde_json::from_value::<ToolCallCompletedPayload>(event.payload.clone())
            .map_err(|err| {
                CooldisError::History(format!("tool.call.completed payload is invalid: {err}"))
            })?;
        if payload.subject.turn_id == turn_id && payload.subject.call_id == call_id {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn tool_call_suspension_exists(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    turn_id: &str,
    call_id: &str,
) -> CooldisResult<bool> {
    let events = services
        .runtime_store()
        .read_events(
            &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            None,
        )
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    for event in events
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallSuspended)
    {
        let payload =
            serde_json::from_value::<crate::ToolCallSuspendedPayload>(event.payload.clone())
                .map_err(|err| {
                    CooldisError::History(format!("tool.call.suspended payload is invalid: {err}"))
                })?;
        if payload.subject.turn_id == turn_id && payload.subject.call_id == call_id {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn append_tool_result_message(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    call_id: String,
    tool_name: String,
    turn_id: String,
    snapshot_id: String,
    result: CanonicalMessage,
    duration_ms: Option<u64>,
) -> CooldisResult<()> {
    let success = match &result {
        CanonicalMessage::ToolResult { is_error, .. } => !is_error,
        _ => false,
    };
    let output = text_from_message(&result);
    let entry = services
        .append_session_entry(
            coordinates,
            None,
            SessionEntryKind::Message {
                message: result.clone(),
            },
        )
        .await?;
    let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
    emit_runtime_event(
        events,
        coordinates,
        RuntimeEventKind::ToolCallResult {
            call_id: call_id.clone(),
            output,
            success,
            duration_ms,
        },
    );
    services
        .append_thread_event(
            coordinates,
            NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::ToolCallCompleted,
                serde_json::to_value(ToolCallCompletedPayload {
                    subject: ToolCallSubject { turn_id, call_id },
                    snapshot_id,
                    tool_name,
                    success,
                    duration_ms,
                })
                .map_err(|err| {
                    CooldisError::History(format!("tool completion payload codec failed: {err}"))
                })?,
            ),
        )
        .await?;
    Ok(())
}

async fn append_hook_contexts(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    contexts: Vec<String>,
) -> CooldisResult<()> {
    for context in contexts
        .into_iter()
        .filter(|context| !context.trim().is_empty())
    {
        let entry = services
            .append_session_entry(
                coordinates,
                None,
                SessionEntryKind::CustomContextMessage {
                    message: CanonicalMessage::user_text(context),
                },
            )
            .await?;
        let _ = events.send(ThreadEvent::CanonicalMirror { thread_id, entry });
    }
    Ok(())
}

async fn append_hook_mutation_witnesses(
    services: &RuntimeServices,
    coordinates: &crate::ThreadCoordinates,
    witnesses: Vec<HookMutationWitness>,
) -> CooldisResult<()> {
    if witnesses.is_empty() {
        return Ok(());
    }
    let store = services.runtime_store();
    for witness in witnesses {
        let mut payload = serde_json::to_value(&witness)
            .map_err(|err| CooldisError::History(format!("hook witness codec failed: {err}")))?;
        if let Some(payload) = payload.as_object_mut() {
            payload.insert(
                "schema".to_string(),
                serde_json::json!(HOOK_MUTATION_WITNESS_OBSERVATION_SCHEMA_V1),
            );
            payload.insert("witnessing".to_string(), serde_json::json!(true));
        }
        let record = NewObservationRecord::new(
            HOOK_MUTATION_WITNESS_OBSERVATION_KIND,
            coordinates.clone(),
            payload,
        )
        .with_provenance(ObservationProvenance {
            derivation_strategy: "host.hook.mutation_witnessing".to_string(),
            derivation_version: "v1".to_string(),
            ..ObservationProvenance::default()
        });
        store
            .append_observation(record)
            .await
            .map_err(|err| CooldisError::History(err.to_string()))?;
    }
    Ok(())
}

fn steering_context(turn_id: &str, input: &TurnInput) -> Option<String> {
    let text = input.text_projection();
    if text.trim().is_empty() {
        return None;
    }
    Some(format!(
        "Additional user steering for active turn {turn_id}:\n{text}"
    ))
}

async fn run_stop_hooks(
    runtime: &CanonicalProviderRuntime,
    turn_context: &TurnContext,
    services: &RuntimeServices,
    thread_id: crate::ThreadId,
    events: &broadcast::Sender<ThreadEvent>,
    last_assistant_message: Option<String>,
) -> CooldisResult<()> {
    let Some(hook_pipeline) = &runtime.hook_pipeline else {
        return Ok(());
    };
    let outcome = hook_pipeline
        .run_stop(
            StopHookRequest {
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
    events: &broadcast::Sender<ThreadEvent>,
    coordinates: &crate::ThreadCoordinates,
    spec: &HookHandlerSpec,
) {
    emit_runtime_event(
        events,
        coordinates,
        RuntimeEventKind::HookStarted {
            hook_id: spec.id.clone(),
            event_name: spec.event_name,
            matcher: spec.matcher.clone(),
        },
    );
}

fn emit_hook_records(
    events: &broadcast::Sender<ThreadEvent>,
    coordinates: &crate::ThreadCoordinates,
    records: &[HookRunRecord],
) {
    for record in records {
        emit_runtime_event(
            events,
            coordinates,
            RuntimeEventKind::HookCompleted {
                hook_id: record.hook_id.clone(),
                event_name: record.event_name,
                status: record.status,
                duration_ms: record.duration_ms,
                message: record.message.clone(),
            },
        );
    }
}

fn text_from_message(message: &CanonicalMessage) -> String {
    match message {
        CanonicalMessage::Assistant { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        CanonicalMessage::ToolResult { content, .. } => content
            .iter()
            .filter_map(|content| match content {
                CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        CanonicalMessage::User { .. } => String::new(),
    }
}

fn emit_non_stream_content_events(
    events: &broadcast::Sender<ThreadEvent>,
    coordinates: &crate::ThreadCoordinates,
    message: &CanonicalMessage,
) {
    let CanonicalMessage::Assistant { content, .. } = message else {
        return;
    };
    for content in content {
        match content {
            CanonicalContent::Text { text, .. } if !text.is_empty() => emit_runtime_event(
                events,
                coordinates,
                RuntimeEventKind::TextDelta { text: text.clone() },
            ),
            CanonicalContent::Thinking { text, .. } if !text.is_empty() => emit_runtime_event(
                events,
                coordinates,
                RuntimeEventKind::ThinkingDelta { text: text.clone() },
            ),
            CanonicalContent::Text { .. }
            | CanonicalContent::Thinking { .. }
            | CanonicalContent::Image { .. }
            | CanonicalContent::ToolCall { .. } => {}
        }
    }
}

#[derive(Default)]
struct PendingToolCall {
    name: Option<String>,
    arguments: String,
}

fn response_from_stream_events(
    coordinates: &crate::ThreadCoordinates,
    stream_events: Vec<ProviderStreamEvent>,
    events: &broadcast::Sender<ThreadEvent>,
) -> Result<crate::ProviderResponse, ModelRequestAttemptError> {
    let mut content = Vec::new();
    let mut text = String::new();
    let mut usage = crate::CanonicalUsage::default();
    let mut stop_reason = crate::CanonicalStopReason::EndTurn;
    let mut saw_done = false;
    let mut tool_order = Vec::new();
    let mut tool_calls = BTreeMap::<String, PendingToolCall>::new();

    for event in stream_events {
        match event {
            ProviderStreamEvent::TextDelta { text: delta } => {
                emit_runtime_event(
                    events,
                    coordinates,
                    RuntimeEventKind::TextDelta {
                        text: delta.clone(),
                    },
                );
                text.push_str(&delta);
            }
            ProviderStreamEvent::ThinkingDelta { text: delta } => {
                emit_runtime_event(
                    events,
                    coordinates,
                    RuntimeEventKind::ThinkingDelta {
                        text: delta.clone(),
                    },
                );
                content.push(CanonicalContent::Thinking {
                    text: delta,
                    provider: crate::ThinkingProvider::Other("stream".to_string()),
                    metadata: crate::ThinkingMetadata::None,
                });
            }
            ProviderStreamEvent::ToolCallDelta {
                id,
                name,
                arguments_delta,
            } => {
                if !tool_calls.contains_key(&id) {
                    tool_order.push(id.clone());
                    emit_runtime_event(
                        events,
                        coordinates,
                        RuntimeEventKind::ToolCallStarted {
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
            ProviderStreamEvent::Content { content: incoming } => {
                if let CanonicalContent::Text {
                    text: incoming_text,
                    ..
                } = &incoming
                    && !text.is_empty()
                    && incoming_text == &text
                {
                    continue;
                }
                if let CanonicalContent::ToolCall {
                    id,
                    name,
                    arguments,
                } = &incoming
                {
                    tool_calls.remove(id);
                    tool_order.retain(|candidate| candidate != id);
                    emit_runtime_event(
                        events,
                        coordinates,
                        RuntimeEventKind::ToolCallStarted {
                            call_id: id.clone(),
                            name: name.clone(),
                            input: arguments.clone(),
                        },
                    );
                }
                content.push(incoming);
            }
            ProviderStreamEvent::Usage { usage: next_usage } => {
                usage.input_tokens += next_usage.input_tokens;
                usage.output_tokens += next_usage.output_tokens;
                usage.cache_creation_input_tokens += next_usage.cache_creation_input_tokens;
                usage.cache_read_input_tokens += next_usage.cache_read_input_tokens;
                emit_runtime_event(
                    events,
                    coordinates,
                    RuntimeEventKind::Usage {
                        usage: RuntimeUsage {
                            input_tokens: next_usage.input_tokens,
                            output_tokens: next_usage.output_tokens,
                            cache_creation_input_tokens: next_usage.cache_creation_input_tokens,
                            cache_read_input_tokens: next_usage.cache_read_input_tokens,
                        },
                    },
                );
            }
            ProviderStreamEvent::Done {
                stop_reason: reason,
            } => {
                saw_done = true;
                stop_reason = reason;
            }
            ProviderStreamEvent::Error { message } => {
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
        content.insert(0, CanonicalContent::text(text));
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
        content.push(CanonicalContent::tool_call(
            id,
            pending.name.unwrap_or_default(),
            arguments,
        ));
    }

    Ok(crate::ProviderResponse {
        content,
        usage,
        stop_reason,
    })
}

fn usage_from_message(message: &CanonicalMessage) -> Option<RuntimeUsage> {
    match message {
        CanonicalMessage::Assistant { usage, .. } => Some(RuntimeUsage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cache_creation_input_tokens: usage.cache_creation_input_tokens,
            cache_read_input_tokens: usage.cache_read_input_tokens,
        }),
        CanonicalMessage::User { .. } | CanonicalMessage::ToolResult { .. } => None,
    }
}

#[cfg(test)]
mod tests;
