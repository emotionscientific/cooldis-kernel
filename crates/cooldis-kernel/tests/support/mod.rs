#![allow(dead_code)]

use async_trait::async_trait;
use cooldis::{
    AgentKernelToolCall, AgentKernelToolProvider, AgentRuntime, AgentRuntimeFactory,
    AgentToolRouter, CanonicalContent, CanonicalMessage, CanonicalProviderRuntimeConfig,
    CanonicalProviderRuntimeFactory, CanonicalStopReason, CanonicalUsage, CooldisResult,
    HookHandler, HookHandlerOutput, HookHandlerSpec, HookPipeline, HookRequest,
    InMemorySessionStore, OperationRegistry, ProviderApi, ProviderCapabilityRecord, ProviderClient,
    ProviderError, ProviderRequest, ProviderResponse, ProviderResult, ProviderStreamEvent,
    RuntimeEventKind, RuntimeServices, SessionEntry, SessionEntryKind, ThreadCommand,
    ThreadContext, ThreadEvent, ThreadSignal, ThreadStatus, ToolDefinition, ToolPermissionDecision,
    ToolPermissionGate, ToolPermissionRequest, emit_runtime_event,
};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

const EVENT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Default)]
pub struct EventTrace {
    pub runtime_events: Vec<RuntimeEventKind>,
    pub mirrors: Vec<SessionEntry>,
    pub outputs: Vec<String>,
    pub failures: Vec<String>,
    pub cancellations: Vec<String>,
    pub signals: Vec<ThreadSignal>,
}

impl EventTrace {
    pub fn runtime_events(&self) -> &[RuntimeEventKind] {
        &self.runtime_events
    }

    pub fn text_messages(&self) -> Vec<String> {
        self.mirrors
            .iter()
            .filter_map(|entry| match &entry.kind {
                SessionEntryKind::Message { message } => Some(text_from_message(message)),
                _ => None,
            })
            .collect()
    }
}

pub async fn collect_until_output(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        ThreadEvent::Output { text, .. } => {
            if text == expected {
                Some(())
            } else {
                None
            }
        }
        ThreadEvent::Failed { message, .. } => {
            trace.failures.push(message.clone());
            panic!("thread failed before output {expected:?}: {message}; trace: {trace:#?}");
        }
        _ => None,
    })
    .await
}

pub async fn collect_until_failed(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_fragment: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        ThreadEvent::Failed { message, .. } => {
            assert!(
                message.contains(expected_fragment),
                "failure {message:?} did not contain {expected_fragment:?}; trace: {trace:#?}"
            );
            Some(())
        }
        _ => None,
    })
    .await
}

pub async fn collect_until_cancelled(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_reason: &str,
) -> EventTrace {
    collect_until(events, |event, trace| match event {
        ThreadEvent::Cancelled { reason, .. } => {
            assert_eq!(reason, expected_reason, "trace: {trace:#?}");
            Some(())
        }
        ThreadEvent::Failed { message, .. } => {
            trace.failures.push(message.clone());
            panic!("thread failed before cancellation {expected_reason:?}: {message}");
        }
        _ => None,
    })
    .await
}

pub async fn collect_until_compaction(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_summary: &str,
) -> EventTrace {
    collect_until(events, |event, _trace| match event {
        ThreadEvent::Runtime { event, .. } => match &event.kind {
            RuntimeEventKind::Compaction { summary, .. } if summary == expected_summary => Some(()),
            _ => None,
        },
        ThreadEvent::Failed { message, .. } => {
            panic!("thread failed before compaction {expected_summary:?}: {message}");
        }
        _ => None,
    })
    .await
}

async fn collect_until(
    events: &mut broadcast::Receiver<ThreadEvent>,
    mut done: impl FnMut(&ThreadEvent, &mut EventTrace) -> Option<()>,
) -> EventTrace {
    let mut trace = EventTrace::default();
    loop {
        let event = timeout(EVENT_TIMEOUT, events.recv())
            .await
            .unwrap_or_else(|_| panic!("event timed out; trace: {trace:#?}"))
            .expect("event channel closed");
        match &event {
            ThreadEvent::Runtime { event, .. } => trace.runtime_events.push(event.kind.clone()),
            ThreadEvent::CanonicalMirror { entry, .. } => trace.mirrors.push(entry.clone()),
            ThreadEvent::Output { text, .. } => trace.outputs.push(text.clone()),
            ThreadEvent::Failed { message, .. } => trace.failures.push(message.clone()),
            ThreadEvent::Cancelled { reason, .. } => trace.cancellations.push(reason.clone()),
            ThreadEvent::Signal { signal, .. } => trace.signals.push(signal.clone()),
            _ => {}
        }
        if done(&event, &mut trace).is_some() {
            return trace;
        }
    }
}

pub fn find_event_index(
    events: &[RuntimeEventKind],
    label: &str,
    predicate: impl Fn(&RuntimeEventKind) -> bool,
) -> usize {
    events
        .iter()
        .position(predicate)
        .unwrap_or_else(|| panic!("missing runtime event {label}; events: {events:#?}"))
}

pub fn assert_event_order(
    events: &[RuntimeEventKind],
    first_label: &str,
    first: impl Fn(&RuntimeEventKind) -> bool,
    second_label: &str,
    second: impl Fn(&RuntimeEventKind) -> bool,
) {
    let first = find_event_index(events, first_label, first);
    let second = find_event_index(events, second_label, second);
    assert!(
        first < second,
        "expected {first_label} before {second_label}; events: {events:#?}"
    );
}

#[derive(Debug)]
pub enum ScriptedProviderStep {
    Response(ProviderResponse),
    Error(String),
    Pending,
}

#[derive(Default)]
pub struct ScriptedProviderClient {
    requests: Mutex<Vec<ProviderRequest>>,
    stream_requests: Mutex<Vec<ProviderRequest>>,
    responses: Mutex<VecDeque<ScriptedProviderStep>>,
    stream_events: Mutex<VecDeque<Vec<ProviderStreamEvent>>>,
    capabilities: Option<ProviderCapabilityRecord>,
}

impl ScriptedProviderClient {
    pub fn with_responses(responses: Vec<ProviderResponse>) -> Self {
        Self {
            responses: Mutex::new(
                responses
                    .into_iter()
                    .map(ScriptedProviderStep::Response)
                    .collect(),
            ),
            ..Self::default()
        }
    }

    pub fn with_steps(steps: Vec<ScriptedProviderStep>) -> Self {
        Self {
            responses: Mutex::new(steps.into()),
            ..Self::default()
        }
    }

    pub fn with_stream_events(events: Vec<Vec<ProviderStreamEvent>>) -> Self {
        Self {
            stream_events: Mutex::new(events.into()),
            ..Self::default()
        }
    }

    pub fn with_capabilities(mut self, capabilities: ProviderCapabilityRecord) -> Self {
        self.capabilities = Some(capabilities);
        self
    }

    pub fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub fn stream_requests(&self) -> Vec<ProviderRequest> {
        self.stream_requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl ProviderClient for ScriptedProviderClient {
    fn capabilities(&self) -> Option<ProviderCapabilityRecord> {
        self.capabilities.clone()
    }

    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let step = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ProviderError::Decode("no test response queued".to_string()))?;
        match step {
            ScriptedProviderStep::Response(response) => Ok(response),
            ScriptedProviderStep::Error(message) => Err(ProviderError::Decode(message)),
            ScriptedProviderStep::Pending => std::future::pending().await,
        }
    }

    async fn stream(&self, request: &ProviderRequest) -> ProviderResult<Vec<ProviderStreamEvent>> {
        self.stream_requests.lock().unwrap().push(request.clone());
        self.stream_events
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ProviderError::Decode("no test stream queued".to_string()))
    }
}

pub fn response_text(text: &str) -> ProviderResponse {
    ProviderResponse {
        content: vec![CanonicalContent::text(text)],
        usage: CanonicalUsage {
            input_tokens: 1,
            output_tokens: 2,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        },
        stop_reason: CanonicalStopReason::EndTurn,
    }
}

pub fn response_tool_call(name: &str, arguments: Value) -> ProviderResponse {
    response_tool_call_with_id("call_1|fc_1", name, arguments)
}

pub fn response_tool_call_with_id(call_id: &str, name: &str, arguments: Value) -> ProviderResponse {
    ProviderResponse {
        content: vec![CanonicalContent::tool_call(call_id, name, arguments)],
        usage: CanonicalUsage::default(),
        stop_reason: CanonicalStopReason::ToolUse,
    }
}

pub fn provider_factory(client: Arc<dyn ProviderClient>) -> Arc<CanonicalProviderRuntimeFactory> {
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    Arc::new(CanonicalProviderRuntimeFactory::new(config, client))
}

pub fn streaming_provider_factory(
    client: Arc<dyn ProviderClient>,
) -> Arc<CanonicalProviderRuntimeFactory> {
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    config.stream = true;
    Arc::new(CanonicalProviderRuntimeFactory::new(config, client))
}

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

pub fn text_from_message(message: &CanonicalMessage) -> String {
    match message {
        CanonicalMessage::User { content, .. }
        | CanonicalMessage::Assistant { content, .. }
        | CanonicalMessage::ToolResult { content, .. } => text_from_content(content),
    }
}

pub fn text_from_content(content: &[CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
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
