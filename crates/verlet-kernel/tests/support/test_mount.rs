//! The integration-test mount of the shared test-support tree.
//!
//! Every integration-test binary mounts this file as `crate::support`, the same
//! module path `src/lib.rs` mounts `lib_mount.rs` at under `#[cfg(test)]`. One
//! absolute path in both compilations is what lets the support files spell
//! every path out in full instead of reaching through relative parent paths.
//!
//! The `scenario_*` seams are the deliberate difference between the two
//! mounts: `lib_mount.rs` has the real implementations, which can reach
//! crate-private kernel APIs; this file supplies panicking stubs plus a
//! `scenario_unit_harness()` that returns `false`, so integration binaries skip
//! the scenario bodies that need those seams.

#![allow(dead_code)]

#[path = "event_trace.rs"]
pub(crate) mod event_trace;
#[path = "fault.rs"]
pub(crate) mod fault;
#[path = "fault_plan.rs"]
pub(crate) mod fault_plan;
#[path = "invariant_claims.rs"]
pub(crate) mod invariant_claims;
#[path = "invariant_forks.rs"]
pub(crate) mod invariant_forks;
#[path = "invariants.rs"]
pub(crate) mod invariants;
#[path = "scenario.rs"]
pub(crate) mod scenario;
#[path = "scripted_provider.rs"]
pub(crate) mod scripted_provider;
#[path = "simulated_io.rs"]
pub(crate) mod simulated_io;
#[path = "store_parity.rs"]
pub(crate) mod store_parity;
#[path = "transcript.rs"]
pub(crate) mod transcript;

async fn scenario_app_server(
    _config: verlet::adapters::app_server::VerletAppServerConfig,
    _runtime_factory: std::sync::Arc<
        dyn verlet::kernel::runtime_host::runtime_api::AgentRuntimeFactory,
    >,
    _decorate: impl FnOnce(
        std::sync::Arc<dyn verlet_history::RuntimeStore>,
    ) -> std::sync::Arc<dyn verlet_history::RuntimeStore>
    + Send
    + 'static,
) -> verlet::kernel::runtime_host::VerletResult<verlet::adapters::app_server::VerletAppServer> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

fn scenario_unit_harness() -> bool {
    false
}

async fn scenario_fork_with_id(
    _server: &verlet::adapters::app_server::VerletAppServer,
    _parent: &verlet_runtime_contracts::ThreadCoordinates,
    _child_thread_id: verlet_runtime_contracts::ThreadId,
) -> verlet::kernel::runtime_host::VerletResult<verlet_runtime_contracts::ThreadCoordinates> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

async fn scenario_project_spawn_snapshot(
    _host: verlet::kernel::runtime_host::RuntimeHost,
    _coordinates: verlet_runtime_contracts::ThreadCoordinates,
    _barrier: std::sync::Arc<tokio::sync::Barrier>,
) -> verlet::kernel::runtime_host::VerletResult<
    verlet::kernel::thread_spawn_projector::ThreadSpawnProjectionReceipt,
> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

fn scenario_ingress_binding_barrier(
    _bridge: &verlet::daemon::daemon_io::VerletDaemonIoBridge,
) -> std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

fn scenario_pause_after_ingress_claim(
    _bridge: &verlet::daemon::daemon_io::VerletDaemonIoBridge,
) -> (
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    std::sync::Arc<tokio::sync::Notify>,
) {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

fn scenario_thread_load_root_barrier(
    _bridge: &verlet::daemon::daemon_io::VerletDaemonIoBridge,
) -> std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>> {
    panic!("the scenario runner is mounted into the verlet crate test harness")
}

pub fn echo_router(
    tool_name: &str,
) -> std::sync::Arc<verlet::agent::agent_tool_router::AgentToolRouter> {
    std::sync::Arc::new(
        verlet::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
            verlet_operations::operation_registry::OperationRegistry::new(),
        ))
        .with_kernel_tool_provider(std::sync::Arc::new(EchoKernelToolProvider::new(tool_name))),
    )
}

pub struct EchoKernelToolProvider {
    tool_name: String,
    seen_calls: std::sync::Mutex<Vec<verlet::agent::agent_tool_router::AgentKernelToolCall>>,
}

impl EchoKernelToolProvider {
    pub fn new(tool_name: &str) -> Self {
        Self {
            tool_name: tool_name.to_string(),
            seen_calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn seen_calls(&self) -> Vec<verlet::agent::agent_tool_router::AgentKernelToolCall> {
        self.seen_calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl verlet::agent::agent_tool_router::AgentKernelToolProvider for EchoKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
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
        call: verlet::agent::agent_tool_router::AgentKernelToolCall,
    ) -> verlet::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        self.seen_calls.lock().unwrap().push(call.clone());
        let input = call
            .arguments
            .get("input")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            format!("echo:{input}"),
            false,
        )))
    }
}

pub struct StaticHookHandler {
    spec: verlet::agent::hooks::HookHandlerSpec,
    output: verlet::agent::hooks::HookHandlerOutput,
    requests: std::sync::Mutex<Vec<verlet::agent::hooks::HookRequest>>,
}

impl StaticHookHandler {
    pub fn new(
        spec: verlet::agent::hooks::HookHandlerSpec,
        output: verlet::agent::hooks::HookHandlerOutput,
    ) -> Self {
        Self {
            spec,
            output,
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn pre_tool(
        id: &str,
        matcher: &str,
        output: verlet::agent::hooks::HookHandlerOutput,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new(
            verlet::agent::hooks::HookHandlerSpec {
                id: id.to_string(),
                event_name: verlet::agent::hooks::HookEventName::PreToolUse,
                matcher: Some(matcher.to_string()),
            },
            output,
        ))
    }

    pub fn post_tool(
        id: &str,
        matcher: &str,
        output: verlet::agent::hooks::HookHandlerOutput,
    ) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new(
            verlet::agent::hooks::HookHandlerSpec {
                id: id.to_string(),
                event_name: verlet::agent::hooks::HookEventName::PostToolUse,
                matcher: Some(matcher.to_string()),
            },
            output,
        ))
    }

    pub fn requests(&self) -> Vec<verlet::agent::hooks::HookRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl verlet::agent::hooks::HookHandler for StaticHookHandler {
    fn spec(&self) -> verlet::agent::hooks::HookHandlerSpec {
        self.spec.clone()
    }

    async fn run(
        &self,
        request: verlet::agent::hooks::HookRequest,
    ) -> verlet::kernel::runtime_host::VerletResult<verlet::agent::hooks::HookHandlerOutput> {
        self.requests.lock().unwrap().push(request);
        Ok(self.output.clone())
    }
}

pub fn hook_pipeline(
    handlers: Vec<std::sync::Arc<dyn verlet::agent::hooks::HookHandler>>,
) -> std::sync::Arc<verlet::agent::hooks::HookPipeline> {
    let mut pipeline = verlet::agent::hooks::HookPipeline::new();
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
impl verlet::agent::tool_interceptor::ToolPermissionGate for DenyGate {
    async fn check(
        &self,
        _request: verlet::agent::tool_interceptor::ToolPermissionRequest,
    ) -> verlet::agent::tool_interceptor::ToolPermissionDecision {
        verlet::agent::tool_interceptor::ToolPermissionDecision::Deny {
            reason: self.reason.clone(),
        }
    }
}

pub struct RootProviderChildEchoFactory {
    root: std::sync::Arc<verlet::adapters::agent_loop::AgentLoopFactory>,
}

impl RootProviderChildEchoFactory {
    pub fn new(root: std::sync::Arc<verlet::adapters::agent_loop::AgentLoopFactory>) -> Self {
        Self { root }
    }
}

#[async_trait::async_trait]
impl verlet::kernel::runtime_host::runtime_api::AgentRuntimeFactory
    for RootProviderChildEchoFactory
{
    async fn build(
        &self,
        context: &verlet_runtime_contracts::ThreadContext,
    ) -> verlet::kernel::runtime_host::VerletResult<
        Box<dyn verlet::kernel::runtime_host::runtime_api::AgentRuntime>,
    > {
        if context.parent_thread_id.is_some() {
            return Ok(Box::new(ChildEchoRuntime));
        }
        self.root.build(context).await
    }
}

struct ChildEchoRuntime;

#[async_trait::async_trait]
impl verlet::kernel::runtime_host::runtime_api::AgentRuntime for ChildEchoRuntime {
    async fn run(
        self: Box<Self>,
        context: verlet_runtime_contracts::ThreadContext,
        services: verlet::kernel::runtime_host::runtime_services::RuntimeServices,
        mut commands: tokio::sync::mpsc::Receiver<
            verlet::kernel::runtime_host::runtime_api::ThreadCommand,
        >,
        events: tokio::sync::broadcast::Sender<
            verlet::kernel::runtime_host::runtime_api::ThreadEvent,
        >,
        status: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        let thread_id = context.coordinates.thread_id;
        let coordinates = context.coordinates.clone();
        verlet::kernel::runtime_host::runtime_events::emit_runtime_event(
            &events,
            &coordinates,
            verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ThreadStarted {
                parent_thread_id: context.parent_thread_id,
                topology: context.topology.clone(),
                metadata: context.metadata.clone(),
            },
        );
        let _ = events
            .send(verlet::kernel::runtime_host::runtime_api::ThreadEvent::Started { context });
        let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                    let _ = events.send(verlet::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(verlet::kernel::runtime_host::runtime_api::ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Running);
                            let _ = services
                                .append_user_turn_input(&coordinates, &turn_id, &input)
                                .await;
                            let _ = events.send(verlet::kernel::runtime_host::runtime_api::ThreadEvent::Output {
                                thread_id,
                                text: format!("child:{}", input.text_projection()),
                            });
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(verlet::kernel::runtime_host::runtime_api::ThreadCommand::Cancel { reason }) => {
                            let _ = events.send(verlet::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(verlet::kernel::runtime_host::runtime_api::ThreadCommand::CancelTurn { .. }) => {}
                        Some(verlet::kernel::runtime_host::runtime_api::ThreadCommand::Compact { .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(verlet::kernel::runtime_host::runtime_api::ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Idle);
                        }
                        Some(verlet::kernel::runtime_host::runtime_api::ThreadCommand::Shutdown) | None => {
                            let _ = status.send(verlet_runtime_contracts::ThreadStatus::Stopped);
                            let _ = events.send(verlet::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { thread_id });
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

pub fn in_memory_store() -> std::sync::Arc<verlet_history::InMemorySessionStore> {
    std::sync::Arc::new(verlet_history::InMemorySessionStore::new())
}
