#![allow(dead_code)]

pub(crate) mod event_trace;
pub(crate) mod fault;
pub(crate) mod fault_plan;
pub(crate) mod invariant_claims;
pub(crate) mod invariant_forks;
pub(crate) mod invariants;
pub(crate) mod scenario;
pub(crate) mod scripted_provider;
pub(crate) mod simulated_io;
pub(crate) mod store_parity;
pub(crate) mod transcript;

#[allow(unused_imports)]
pub use event_trace::{
    EventTrace, assert_event_order, collect_until_cancelled, collect_until_compaction,
    collect_until_failed, collect_until_output, find_event_index, text_from_content,
    text_from_message,
};
#[allow(unused_imports)]
pub use fault::{
    AppliedFaultPlan, FaultingIngressQueue, FaultingProviderClient, FaultingRuntimeStore,
};
#[allow(unused_imports)]
pub use fault_plan::{
    CRASH_CUT_REGISTRY, CUTS_V1, CrashCutHost, CrashCutRegistration, CrashCutSeam,
    FAULT_VOCABULARY_VERSION, FaultComponent, FaultDirective, FaultPlan, FaultTiming, Intensity,
    PROVIDER_OPERATIONS_V1, PlannedAction, QUEUE_OPERATIONS_V1, STORE_OPERATIONS_V1, SplitMix64,
    crash_cut, run_crash_cut,
};
#[allow(unused_imports)]
pub use invariant_claims::Inv6ClaimsSettle;
#[allow(unused_imports)]
pub use invariant_forks::{
    INV7_ONE_CHILD_PER_FORK_CLAIM, INV8_RESERVED_BEFORE_CREATED, OneChildPerForkClaimInvariant,
    ReservedBeforeCreatedInvariant, fork_invariants_v1,
};
#[allow(unused_imports)]
pub use invariants::{
    BoundedQueueInvariant, INV1_REPLAY_EQUIVALENCE, INV2_UNIQUE_ACTIVE_TOPOLOGY,
    INV3_BOUNDED_QUEUE, INV4_NO_DUPLICATE_PROJECTED_OUTPUT, INV5_TERMINAL_CONSISTENCY,
    NoDuplicateProjectedOutputInvariant, ReplayEquivalenceInvariant, TerminalConsistencyInvariant,
    UniqueActiveTopologyInvariant, invariant_set_v1,
};
#[allow(unused_imports)]
pub use scenario::{
    CorpusEntry, InvariantViolation, Scenario, ScenarioBounds, ScenarioFailure, ScenarioInvariant,
    ScenarioOp, ScenarioWorld, StreamIoCrashReceipt, run_scenario, run_stream_io_crash_scenario,
};
#[allow(unused_imports)]
pub use scripted_provider::{
    ScriptedProviderClient, ScriptedProviderStep, provider_factory, response_text,
    response_tool_call, response_tool_call_with_id, streaming_provider_factory,
};
#[allow(unused_imports)]
pub use simulated_io::{
    CrashSurvival, IO_LOCK, IO_OPEN, IO_READ, IO_REMOVE, IO_SYNC, IO_TRUNCATE, IO_UNLOCK, IO_WRITE,
    IoFaultAction, IoFaultPlan, IoFaultRule, IoTranscriptEntry, SimulatedIo,
};
#[allow(unused_imports)]
pub use store_parity::session_store_parity_transcript;
#[allow(unused_imports)]
pub use transcript::{NormalizedTranscript, NormalizedTranscriptItem, TypedTranscript};

async fn scenario_app_server(
    _config: verlet::VerletAppServerConfig,
    _runtime_factory: std::sync::Arc<dyn verlet::AgentRuntimeFactory>,
    _decorate: impl FnOnce(
        std::sync::Arc<dyn verlet::RuntimeStore>,
    ) -> std::sync::Arc<dyn verlet::RuntimeStore>
    + Send
    + 'static,
) -> verlet::VerletResult<verlet::VerletAppServer> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

fn scenario_unit_harness() -> bool {
    false
}

async fn scenario_fork_with_id(
    _server: &verlet::VerletAppServer,
    _parent: &verlet::ThreadCoordinates,
    _child_thread_id: verlet::ThreadId,
) -> verlet::VerletResult<verlet::ThreadCoordinates> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

async fn scenario_project_spawn_snapshot(
    _host: verlet::RuntimeHost,
    _coordinates: verlet::ThreadCoordinates,
    _barrier: std::sync::Arc<tokio::sync::Barrier>,
) -> verlet::VerletResult<verlet::ThreadSpawnProjectionReceipt> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

fn scenario_ingress_binding_barrier(
    _bridge: &verlet::VerletDaemonIoBridge,
) -> std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

fn scenario_pause_after_ingress_claim(
    _bridge: &verlet::VerletDaemonIoBridge,
) -> (
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    std::sync::Arc<tokio::sync::Notify>,
) {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

fn scenario_thread_load_root_barrier(
    _bridge: &verlet::VerletDaemonIoBridge,
) -> std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

pub fn echo_router(tool_name: &str) -> std::sync::Arc<verlet::AgentToolRouter> {
    std::sync::Arc::new(
        verlet::AgentToolRouter::new(std::sync::Arc::new(verlet::OperationRegistry::new()))
            .with_kernel_tool_provider(std::sync::Arc::new(EchoKernelToolProvider::new(tool_name))),
    )
}

pub struct EchoKernelToolProvider {
    tool_name: String,
    seen_calls: std::sync::Mutex<Vec<verlet::AgentKernelToolCall>>,
}

impl EchoKernelToolProvider {
    pub fn new(tool_name: &str) -> Self {
        Self {
            tool_name: tool_name.to_string(),
            seen_calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn seen_calls(&self) -> Vec<verlet::AgentKernelToolCall> {
        self.seen_calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl verlet::AgentKernelToolProvider for EchoKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet::ToolDefinition> {
        vec![verlet::ToolDefinition::new(
            self.tool_name.clone(),
            "Echo input.",
            serde_json::json!({
                "type": "object",
                "properties": {"input": {"type": "string"}},
                "required": ["input"],
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: verlet::AgentKernelToolCall,
    ) -> verlet::VerletResult<Option<verlet::CanonicalMessage>> {
        self.seen_calls.lock().unwrap().push(call.clone());
        let input = call
            .arguments
            .get("input")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        Ok(Some(verlet::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            format!("echo:{input}"),
            false,
        )))
    }
}

pub struct StaticHookHandler {
    spec: verlet::HookHandlerSpec,
    output: verlet::HookHandlerOutput,
    requests: std::sync::Mutex<Vec<verlet::HookRequest>>,
}

impl StaticHookHandler {
    pub fn new(spec: verlet::HookHandlerSpec, output: verlet::HookHandlerOutput) -> Self {
        Self {
            spec,
            output,
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn pre_tool(
        id: &str,
        matcher: &str,
        output: verlet::HookHandlerOutput,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new(
            verlet::HookHandlerSpec {
                id: id.to_string(),
                event_name: verlet::HookEventName::PreToolUse,
                matcher: Some(matcher.to_string()),
            },
            output,
        ))
    }

    pub fn post_tool(
        id: &str,
        matcher: &str,
        output: verlet::HookHandlerOutput,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new(
            verlet::HookHandlerSpec {
                id: id.to_string(),
                event_name: verlet::HookEventName::PostToolUse,
                matcher: Some(matcher.to_string()),
            },
            output,
        ))
    }

    pub fn requests(&self) -> Vec<verlet::HookRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl verlet::HookHandler for StaticHookHandler {
    fn spec(&self) -> verlet::HookHandlerSpec {
        self.spec.clone()
    }

    async fn run(
        &self,
        request: verlet::HookRequest,
    ) -> verlet::VerletResult<verlet::HookHandlerOutput> {
        self.requests.lock().unwrap().push(request);
        Ok(self.output.clone())
    }
}

pub fn hook_pipeline(
    handlers: Vec<std::sync::Arc<dyn verlet::HookHandler>>,
) -> std::sync::Arc<verlet::HookPipeline> {
    let mut pipeline = verlet::HookPipeline::new();
    for handler in handlers {
        pipeline = pipeline.with_handler(handler);
    }
    std::sync::Arc::new(pipeline)
}

pub struct DenyGate {
    reason: String,
}

impl DenyGate {
    pub fn new(reason: &str) -> Self {
        Self {
            reason: reason.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl verlet::ToolPermissionGate for DenyGate {
    async fn check(
        &self,
        _request: verlet::ToolPermissionRequest,
    ) -> verlet::ToolPermissionDecision {
        verlet::ToolPermissionDecision::Deny {
            reason: self.reason.clone(),
        }
    }
}

pub struct RootProviderChildEchoFactory {
    root: std::sync::Arc<verlet::AgentLoopFactory>,
}

impl RootProviderChildEchoFactory {
    pub fn new(root: std::sync::Arc<verlet::AgentLoopFactory>) -> Self {
        Self { root }
    }
}

#[async_trait::async_trait]
impl verlet::AgentRuntimeFactory for RootProviderChildEchoFactory {
    async fn build(
        &self,
        context: &verlet::ThreadContext,
    ) -> verlet::VerletResult<Box<dyn verlet::AgentRuntime>> {
        if context.parent_thread_id.is_some() {
            return Ok(Box::new(ChildEchoRuntime));
        }
        self.root.build(context).await
    }
}

struct ChildEchoRuntime;

#[async_trait::async_trait]
impl verlet::AgentRuntime for ChildEchoRuntime {
    async fn run(
        self: Box<Self>,
        context: verlet::ThreadContext,
        services: verlet::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<verlet::ThreadCommand>,
        events: tokio::sync::broadcast::Sender<verlet::ThreadEvent>,
        status: tokio::sync::watch::Sender<verlet::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        verlet::emit_runtime_event(
            &events,
            &coordinates,
            verlet::RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
                metadata: context.metadata.clone(),
            },
        );
        let _ = events.send(verlet::ThreadEvent::Started { context });
        let _ = status.send(verlet::ThreadStatus::Idle);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(verlet::ThreadStatus::Stopped);
                    let _ = events.send(verlet::ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(verlet::ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(verlet::ThreadStatus::Running);
                            let _ = services
                                .append_user_turn_input(&coordinates, &turn_id, &input)
                                .await;
                            let _ = events.send(verlet::ThreadEvent::Output {
                                thread_id,
                                text: format!("child:{}", input.text_projection()),
                            });
                            let _ = status.send(verlet::ThreadStatus::Idle);
                        }
                        Some(verlet::ThreadCommand::Cancel { reason }) => {
                            let _ = events.send(verlet::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(verlet::ThreadStatus::Idle);
                        }
                        Some(verlet::ThreadCommand::CancelTurn { .. }) => {}
                        Some(verlet::ThreadCommand::Compact { .. }) => {
                            let _ = status.send(verlet::ThreadStatus::Idle);
                        }
                        Some(verlet::ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(verlet::ThreadStatus::Idle);
                        }
                        Some(verlet::ThreadCommand::Shutdown) | None => {
                            let _ = status.send(verlet::ThreadStatus::Stopped);
                            let _ = events.send(verlet::ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

pub fn temp_path(prefix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("verlet-{prefix}-{nanos}"))
}

pub fn fixture_path(relative: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(relative)
}

pub fn assert_json_fixture(relative: &str, actual: serde_json::Value) {
    let path = fixture_path(relative);
    if verlet_runtime_contracts::env_compat::var_os("VERLET_UPDATE_FIXTURES").is_some() {
        let mut text = serde_json::to_string_pretty(&actual).unwrap();
        text.push('\n');
        std::fs::write(&path, text)
            .unwrap_or_else(|err| panic!("write fixture {}: {err}", path.display()));
        return;
    }
    let expected_text = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        let actual_pretty = serde_json::to_string_pretty(&actual).unwrap();
        panic!(
            "read fixture {}: {err}\n\nactual:\n{}\n",
            path.display(),
            actual_pretty
        )
    });
    let expected: serde_json::Value = serde_json::from_str(&expected_text)
        .unwrap_or_else(|err| panic!("parse fixture {}: {err}", path.display()));
    if expected != actual {
        let expected_pretty = serde_json::to_string_pretty(&expected).unwrap();
        let actual_pretty = serde_json::to_string_pretty(&actual).unwrap();
        panic!(
            "fixture {} differed\n\nexpected:\n{}\n\nactual:\n{}\n",
            path.display(),
            expected_pretty,
            actual_pretty
        );
    }
}

pub fn in_memory_store() -> std::sync::Arc<verlet::InMemorySessionStore> {
    std::sync::Arc::new(verlet::InMemorySessionStore::new())
}
