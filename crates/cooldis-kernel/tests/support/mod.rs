#![allow(dead_code)]

pub(crate) use cooldis as kernel_test;

mod event_trace;
mod fault;
mod scripted_provider;
mod store_parity;
mod transcript;

#[allow(unused_imports)]
pub use event_trace::*;
#[allow(unused_imports)]
pub use fault::*;
#[allow(unused_imports)]
pub use scripted_provider::*;
#[allow(unused_imports)]
pub use store_parity::*;
#[allow(unused_imports)]
pub use transcript::*;

use async_trait::async_trait;
use cooldis::{
    AgentKernelToolCall, AgentKernelToolProvider, AgentRuntime, AgentRuntimeFactory,
    AgentToolRouter, CanonicalMessage, CanonicalProviderRuntimeFactory, CooldisResult, HookHandler,
    HookHandlerOutput, HookHandlerSpec, HookPipeline, HookRequest, InMemorySessionStore,
    OperationRegistry, RuntimeEventKind, RuntimeServices, ThreadCommand, ThreadContext,
    ThreadEvent, ThreadStatus, ToolDefinition, ToolPermissionDecision, ToolPermissionGate,
    ToolPermissionRequest, emit_runtime_event,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

pub fn echo_router(tool_name: &str) -> Arc<AgentToolRouter> {
    Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(Arc::new(EchoKernelToolProvider::new(tool_name))),
    )
}

pub struct EchoKernelToolProvider {
    tool_name: String,
    seen_calls: Mutex<Vec<AgentKernelToolCall>>,
}

impl EchoKernelToolProvider {
    pub fn new(tool_name: &str) -> Self {
        Self {
            tool_name: tool_name.to_string(),
            seen_calls: Mutex::new(Vec::new()),
        }
    }

    pub fn seen_calls(&self) -> Vec<AgentKernelToolCall> {
        self.seen_calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl AgentKernelToolProvider for EchoKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            self.tool_name.clone(),
            "Echo input.",
            json!({
                "type": "object",
                "properties": {"input": {"type": "string"}},
                "required": ["input"],
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        self.seen_calls.lock().unwrap().push(call.clone());
        let input = call
            .arguments
            .get("input")
            .and_then(Value::as_str)
            .unwrap_or_default();
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            format!("echo:{input}"),
            false,
        )))
    }
}

pub struct StaticHookHandler {
    spec: HookHandlerSpec,
    output: HookHandlerOutput,
    requests: Mutex<Vec<HookRequest>>,
}

impl StaticHookHandler {
    pub fn new(spec: HookHandlerSpec, output: HookHandlerOutput) -> Self {
        Self {
            spec,
            output,
            requests: Mutex::new(Vec::new()),
        }
    }

    pub fn pre_tool(id: &str, matcher: &str, output: HookHandlerOutput) -> Arc<Self> {
        Arc::new(Self::new(
            HookHandlerSpec {
                id: id.to_string(),
                event_name: cooldis::HookEventName::PreToolUse,
                matcher: Some(matcher.to_string()),
            },
            output,
        ))
    }

    pub fn post_tool(id: &str, matcher: &str, output: HookHandlerOutput) -> Arc<Self> {
        Arc::new(Self::new(
            HookHandlerSpec {
                id: id.to_string(),
                event_name: cooldis::HookEventName::PostToolUse,
                matcher: Some(matcher.to_string()),
            },
            output,
        ))
    }

    pub fn requests(&self) -> Vec<HookRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl HookHandler for StaticHookHandler {
    fn spec(&self) -> HookHandlerSpec {
        self.spec.clone()
    }

    async fn run(&self, request: HookRequest) -> CooldisResult<HookHandlerOutput> {
        self.requests.lock().unwrap().push(request);
        Ok(self.output.clone())
    }
}

pub fn hook_pipeline(handlers: Vec<Arc<dyn HookHandler>>) -> Arc<HookPipeline> {
    let mut pipeline = HookPipeline::new();
    for handler in handlers {
        pipeline = pipeline.with_handler(handler);
    }
    Arc::new(pipeline)
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

#[async_trait]
impl ToolPermissionGate for DenyGate {
    async fn check(&self, _request: ToolPermissionRequest) -> ToolPermissionDecision {
        ToolPermissionDecision::Deny {
            reason: self.reason.clone(),
        }
    }
}

pub struct RootProviderChildEchoFactory {
    root: Arc<CanonicalProviderRuntimeFactory>,
}

impl RootProviderChildEchoFactory {
    pub fn new(root: Arc<CanonicalProviderRuntimeFactory>) -> Self {
        Self { root }
    }
}

#[async_trait]
impl AgentRuntimeFactory for RootProviderChildEchoFactory {
    async fn build(&self, context: &ThreadContext) -> CooldisResult<Box<dyn AgentRuntime>> {
        if context.parent_thread_id.is_some() {
            return Ok(Box::new(ChildEchoRuntime));
        }
        self.root.build(context).await
    }
}

struct ChildEchoRuntime;

#[async_trait]
impl AgentRuntime for ChildEchoRuntime {
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
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    let _ = status.send(ThreadStatus::Stopped);
                    let _ = events.send(ThreadEvent::Stopped { thread_id });
                    break;
                }
                command = commands.recv() => {
                    match command {
                        Some(ThreadCommand::Submit { input, .. }) => {
                            let _ = status.send(ThreadStatus::Running);
                            let _ = services.append_user_turn_input(&coordinates, &input).await;
                            let _ = events.send(ThreadEvent::Output {
                                thread_id,
                                text: format!("child:{}", input.text_projection()),
                            });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Cancel { reason }) => {
                            let _ = events.send(ThreadEvent::Cancelled { thread_id, reason });
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::CancelTurn { .. }) => {}
                        Some(ThreadCommand::Compact { .. }) => {
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::ResumeToolCall { .. }) => {
                            let _ = status.send(ThreadStatus::Idle);
                        }
                        Some(ThreadCommand::Shutdown) | None => {
                            let _ = status.send(ThreadStatus::Stopped);
                            let _ = events.send(ThreadEvent::Stopped { thread_id });
                            break;
                        }
                    }
                }
            }
        }
    }
}

pub fn temp_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("cooldis-{prefix}-{nanos}"))
}

pub fn fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(relative)
}

pub fn assert_json_fixture(relative: &str, actual: Value) {
    let path = fixture_path(relative);
    let expected_text = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        let actual_pretty = serde_json::to_string_pretty(&actual).unwrap();
        panic!(
            "read fixture {}: {err}\n\nactual:\n{}\n",
            path.display(),
            actual_pretty
        )
    });
    let expected: Value = serde_json::from_str(&expected_text)
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

pub fn in_memory_store() -> Arc<InMemorySessionStore> {
    Arc::new(InMemorySessionStore::new())
}
