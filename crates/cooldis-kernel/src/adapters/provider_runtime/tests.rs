use super::*;
use crate::EventKind;
use crate::{
    AgentKernelToolCall, AgentKernelToolProvider, AgentManifestBindReceipt,
    AgentManifestCouplingBinding, AgentManifestCouplingBudget, AgentManifestRuntimeDefaults,
    AgentToolRouter, CanonicalStopReason, CanonicalUsage, CommandHookHandler, CouplingRole,
    EventProvenance, EventStore, EventStreamId, HookEventName, HookHandler, HookHandlerOutput,
    HookHandlerSpec, HookRequest, HookRunStatus, InMemorySessionStore, KernelOperationRegistration,
    NewEventRecord, ObservationStore, OperationRegistration, OperationRegistry, OperationToolAlias,
    ProviderCapabilityRecord, ProviderContextPolicy, RuntimeEvent, RuntimeHost, SessionEntry,
    SqliteSessionStore, THREAD_SPAWN_OPERATION, ThreadCoordinates, ThreadTopology,
    ToolCallDecisionOutcomePayload, ToolCallDecisionPayload, ToolCallSubject,
    ToolCallSuspendedPayload, TurnContextSnapshot, WasmRuntimeArtifact,
    cooldis_threads_kernel_package,
};
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, timeout};

#[derive(Default)]
struct RecordingClient {
    requests: Mutex<Vec<ProviderRequest>>,
    responses: Mutex<Vec<crate::ProviderResponse>>,
    capabilities: Option<ProviderCapabilityRecord>,
}

impl RecordingClient {
    fn with_responses(responses: Vec<crate::ProviderResponse>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into_iter().rev().collect()),
            capabilities: None,
        }
    }

    fn with_capabilities(mut self, capabilities: ProviderCapabilityRecord) -> Self {
        self.capabilities = Some(capabilities);
        self
    }

    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

enum ScriptedResponse {
    Pending,
    Error(crate::ProviderError),
    Response(crate::ProviderResponse),
}

struct ScriptedClient {
    requests: Mutex<Vec<ProviderRequest>>,
    responses: Mutex<VecDeque<ScriptedResponse>>,
}

struct StreamingClient {
    requests: Mutex<Vec<ProviderRequest>>,
    events: Mutex<VecDeque<Vec<ProviderStreamEvent>>>,
}

struct TurnContextRecordingKernelToolProvider {
    snapshots: Mutex<Vec<Option<TurnContextSnapshot>>>,
}

struct WitnessCheckingEchoProvider {
    store: Arc<InMemorySessionStore>,
    expected_command_sha256: String,
    seen_arguments: Mutex<Vec<Value>>,
}

struct StaticHookHandler {
    spec: HookHandlerSpec,
    output: HookHandlerOutput,
    requests: Mutex<Vec<HookRequest>>,
}

impl StaticHookHandler {
    fn new(
        id: impl Into<String>,
        event_name: HookEventName,
        matcher: Option<&str>,
        output: HookHandlerOutput,
    ) -> Self {
        Self {
            spec: HookHandlerSpec {
                id: id.into(),
                event_name,
                matcher: matcher.map(str::to_string),
            },
            output,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<HookRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl TurnContextRecordingKernelToolProvider {
    fn new() -> Self {
        Self {
            snapshots: Mutex::new(Vec::new()),
        }
    }

    fn snapshots(&self) -> Vec<Option<TurnContextSnapshot>> {
        self.snapshots.lock().unwrap().clone()
    }
}

impl WitnessCheckingEchoProvider {
    fn seen_arguments(&self) -> Vec<Value> {
        self.seen_arguments.lock().unwrap().clone()
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

#[async_trait]
impl AgentKernelToolProvider for WitnessCheckingEchoProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "echo_search",
            "Echo input after checking hook witnesses.",
            serde_json::json!({"type":"object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        let coordinates = call
            .turn_context
            .as_ref()
            .expect("tool call should carry turn context")
            .coordinates
            .clone();
        let witnesses = self
            .store
            .list_observations(&coordinates, Some("host.hook.mutation_witnessed"))
            .await
            .unwrap();
        assert_eq!(
            witnesses.len(),
            1,
            "pre-tool mutation witness must be appended before the tool runs"
        );
        let payload = &witnesses[0].payload;
        assert_eq!(payload["hook_event_name"].as_str(), Some("pre_tool_use"));
        assert_eq!(
            payload["command_sha256"].as_str(),
            Some(self.expected_command_sha256.as_str())
        );
        assert_eq!(
            payload["mutated_fields"],
            serde_json::json!(["updated_input"])
        );
        assert_eq!(
            payload["tool_input"]["before_sha256"].as_str(),
            Some(
                sha256_hex(
                    &serde_json::to_vec(
                        &serde_json::json!({"input":"original","secret":"before-secret"})
                    )
                    .unwrap()
                )
                .as_str()
            )
        );
        assert_eq!(
            payload["tool_input"]["after_sha256"].as_str(),
            Some(
                sha256_hex(
                    &serde_json::to_vec(
                        &serde_json::json!({"input":"rewritten","secret":"after-secret"})
                    )
                    .unwrap()
                )
                .as_str()
            )
        );
        assert_payload_omits_values(
            payload,
            &["original", "rewritten", "before-secret", "after-secret"],
        );

        self.seen_arguments.lock().unwrap().push(call.arguments);
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "tool original before-secret-output",
            false,
        )))
    }
}

impl ScriptedClient {
    fn new(responses: Vec<ScriptedResponse>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
        }
    }

    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl StreamingClient {
    fn new(events: Vec<Vec<ProviderStreamEvent>>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            events: Mutex::new(events.into()),
        }
    }

    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl ProviderClient for ScriptedClient {
    async fn complete(
        &self,
        request: &ProviderRequest,
    ) -> crate::ProviderResult<crate::ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let response =
            self.responses.lock().unwrap().pop_front().ok_or_else(|| {
                crate::ProviderError::Decode("no test response queued".to_string())
            })?;
        match response {
            ScriptedResponse::Pending => std::future::pending().await,
            ScriptedResponse::Error(error) => Err(error),
            ScriptedResponse::Response(response) => Ok(response),
        }
    }
}

#[async_trait]
impl ProviderClient for RecordingClient {
    fn capabilities(&self) -> Option<crate::ProviderCapabilityRecord> {
        self.capabilities.clone()
    }

    async fn complete(
        &self,
        request: &ProviderRequest,
    ) -> crate::ProviderResult<crate::ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        self.responses
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| crate::ProviderError::Decode("no test response queued".to_string()))
    }
}

#[async_trait]
impl ProviderClient for StreamingClient {
    async fn complete(
        &self,
        _request: &ProviderRequest,
    ) -> crate::ProviderResult<crate::ProviderResponse> {
        Err(crate::ProviderError::Decode(
            "streaming test client requires stream()".to_string(),
        ))
    }

    async fn stream(
        &self,
        request: &ProviderRequest,
    ) -> crate::ProviderResult<Vec<ProviderStreamEvent>> {
        self.requests.lock().unwrap().push(request.clone());
        self.events
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| crate::ProviderError::Decode("no test stream queued".to_string()))
    }
}

#[async_trait]
impl AgentKernelToolProvider for TurnContextRecordingKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "record_turn_context",
            "Record the current Cooldis turn context.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        self.snapshots
            .lock()
            .unwrap()
            .push(call.turn_context.clone());
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "turn context recorded",
            false,
        )))
    }
}

fn response_text(text: &str) -> crate::ProviderResponse {
    crate::ProviderResponse {
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

fn response_tool_call() -> crate::ProviderResponse {
    response_tool_call_named("bash", serde_json::json!({"command":"pwd"}))
}

fn response_tool_call_named(name: &str, arguments: Value) -> crate::ProviderResponse {
    response_tool_call_named_with_id("call_1|fc_1", name, arguments)
}

fn response_tool_call_named_with_id(
    call_id: &str,
    name: &str,
    arguments: Value,
) -> crate::ProviderResponse {
    crate::ProviderResponse {
        content: vec![CanonicalContent::tool_call(call_id, name, arguments)],
        usage: CanonicalUsage::default(),
        stop_reason: CanonicalStopReason::ToolUse,
    }
}

fn runtime_factory(client: Arc<dyn ProviderClient>) -> Arc<CanonicalProviderRuntimeFactory> {
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    Arc::new(CanonicalProviderRuntimeFactory::new(config, client))
}

fn runtime_factory_with_registry(
    client: Arc<dyn ProviderClient>,
    registry: Arc<OperationRegistry>,
) -> Arc<CanonicalProviderRuntimeFactory> {
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    Arc::new(CanonicalProviderRuntimeFactory::new(config, client).with_operation_registry(registry))
}

fn streaming_runtime_factory(
    client: Arc<dyn ProviderClient>,
) -> Arc<CanonicalProviderRuntimeFactory> {
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    config.stream = true;
    Arc::new(CanonicalProviderRuntimeFactory::new(config, client))
}

fn factory(client: Arc<RecordingClient>) -> Arc<CanonicalProviderRuntimeFactory> {
    runtime_factory(client)
}

struct RootProviderChildEchoFactory {
    root: Arc<CanonicalProviderRuntimeFactory>,
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

fn text_messages(messages: &[CanonicalMessage]) -> Vec<String> {
    messages
        .iter()
        .map(|message| match message {
            CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    CanonicalContent::Text { text, .. } => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default(),
        })
        .collect()
}

fn text_from_content(content: &[CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

#[tokio::test]
async fn runtime_builds_each_turn_from_canonical_session_history() {
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_text("first reply"),
        response_text("second reply"),
    ]));
    let host = RuntimeHost::with_session_store(
        factory(Arc::clone(&client)),
        Arc::new(InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    assert_output(&mut events, "first reply").await;
    host.submit(thread.context().coordinates.thread_id, "turn-2", "again")
        .await
        .unwrap();
    assert_output(&mut events, "second reply").await;

    let requests = client.requests();
    assert_eq!(text_messages(&requests[0].messages), vec!["hello"]);
    assert_eq!(
        text_messages(&requests[1].messages),
        vec!["hello", "first reply", "again"]
    );

    let session = thread.session_context().await.unwrap();
    assert_eq!(
        text_messages(&session.messages),
        vec!["hello", "first reply", "again", "second reply"]
    );
    assert!(session.entries.iter().all(is_canonical_message_entry));
}

#[tokio::test]
async fn runtime_applies_provider_context_policy_before_request() {
    let mut capabilities = ProviderCapabilityRecord::for_api(ProviderApi::OpenAIResponses);
    capabilities.context_policy = ProviderContextPolicy {
        max_messages: Some(2),
        max_text_bytes: Some(5),
    };
    let client = Arc::new(
        RecordingClient::with_responses(vec![
            response_text("first reply"),
            response_text("second reply"),
        ])
        .with_capabilities(capabilities),
    );
    let host = RuntimeHost::with_session_store(
        factory(Arc::clone(&client)),
        Arc::new(InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "alpha")
        .await
        .unwrap();
    assert_output(&mut events, "first reply").await;
    host.submit(thread.context().coordinates.thread_id, "turn-2", "bravo")
        .await
        .unwrap();
    assert_output(&mut events, "second reply").await;

    let requests = client.requests();
    assert_eq!(text_messages(&requests[0].messages), vec!["alpha"]);
    assert_eq!(text_messages(&requests[1].messages), vec!["bravo"]);
}

#[tokio::test]
async fn runtime_uses_agent_context_compiler_before_provider_policy() {
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_text("first reply"),
        response_text("second reply"),
    ]));
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let factory = Arc::new(
        CanonicalProviderRuntimeFactory::new(config, client.clone()).with_context_compile_policy(
            AgentContextCompilePolicy {
                max_messages: Some(1),
                max_text_bytes: None,
            },
        ),
    );
    let host = RuntimeHost::with_session_store(factory, Arc::new(InMemorySessionStore::new()));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    assert_output(&mut events, "first reply").await;
    host.submit(thread.context().coordinates.thread_id, "turn-2", "again")
        .await
        .unwrap();
    assert_output(&mut events, "second reply").await;

    let requests = client.requests();
    assert_eq!(text_messages(&requests[0].messages), vec!["hello"]);
    assert_eq!(text_messages(&requests[1].messages), vec!["again"]);
}

#[tokio::test]
async fn runtime_includes_memory_read_plan_context_before_provider_request() {
    let client = Arc::new(RecordingClient::with_responses(vec![response_text(
        "memory-aware reply",
    )]));
    let config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    let factory = Arc::new(CanonicalProviderRuntimeFactory::new(config, client.clone()));
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(factory, store.clone());
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let coordinates = &thread.context().coordinates;
    let thread_stream = EventStreamId::for_thread(coordinates);
    let memory_stream = EventStreamId::new(format!("derived:memory:{}", coordinates.thread_id));
    let memory = store
        .append_events(
            &memory_stream,
            vec![NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::ContextSummaryCompleted,
                serde_json::json!({
                    "schema": EventKind::ContextSummaryCompleted.payload_schema_id(),
                    "role": "summary_checkpoint",
                    "text": "User prefers SQLite first, then S2 as stream backend.",
                    "covered_ranges": [{
                        "stream_id": thread_stream.as_str(),
                        "from_sequence": 1,
                        "to_sequence": 4
                    }],
                    "content": {
                        "sha256": "sha256:memory"
                    },
                    "template_id": "std::memory.extract",
                    "memory_kind": "observation"
                }),
                EventProvenance {
                    source_streams: vec![thread_stream],
                    discharged_by: Some("coupling:std::memory.extract".to_string()),
                    function: Some("op://std-memory-extract/run@sha256:test".to_string()),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    let derived_context_stream =
        EventStreamId::new(format!("derived:context:{}", coordinates.thread_id));
    store
        .append_events(
            &derived_context_stream,
            vec![NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::ContextReadPlanSet,
                serde_json::json!({
                    "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
                    "scope": "thread",
                    "name": "memory.default",
                    "pipeline_id": "context.memory",
                    "source_id": memory_stream.as_str(),
                    "template_id": "std::memory.recall",
                    "read_plan": {
                        "schema": "cooldis.context.read_plan/1",
                        "name": "memory.default",
                        "source_stream": memory_stream.as_str(),
                        "frontier": "compile_frontier",
                        "entries": [{
                            "kind": "event_ref",
                            "stream_id": memory_stream.as_str(),
                            "event_id": memory[0].id.to_string(),
                            "event_role": "memory_checkpoint"
                        }]
                    }
                }),
                EventProvenance {
                    source_streams: vec![memory_stream],
                    source_event_ids: vec![memory[0].id],
                    discharged_by: Some("coupling:std::memory.recall".to_string()),
                    function: Some("op://std-memory-recall/run@sha256:test".to_string()),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "what should we use?",
    )
    .await
    .unwrap();
    assert_output(&mut events, "memory-aware reply").await;

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[0].messages),
        vec![
            "<memory_context>\n- User prefers SQLite first, then S2 as stream backend.\n</memory_context>",
            "what should we use?",
        ]
    );
}

#[tokio::test]
async fn runtime_includes_instruction_read_plan_context_before_provider_request() {
    let client = Arc::new(RecordingClient::with_responses(vec![response_text(
        "instruction-aware reply",
    )]));
    let config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    let factory = Arc::new(CanonicalProviderRuntimeFactory::new(config, client.clone()));
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(factory, store.clone());
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let coordinates = &thread.context().coordinates;
    let thread_stream = EventStreamId::for_thread(coordinates);
    let derived_context_stream =
        EventStreamId::new(format!("derived:context:{}", coordinates.thread_id));
    let instruction = store
        .append_events(
            &derived_context_stream,
            vec![NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::ContextSummaryCompleted,
                serde_json::json!({
                    "schema": EventKind::ContextSummaryCompleted.payload_schema_id(),
                    "role": "summary_checkpoint",
                    "text": "Prefer SQLite event sourcing for V1 unless the live lane asks for S2.",
                    "covered_ranges": [{
                        "stream_id": thread_stream.as_str(),
                        "from_sequence": 1,
                        "to_sequence": 1
                    }],
                    "content": {
                        "sha256": "sha256:instruction"
                    },
                    "template_id": "std::prompt.dynamic_instructions",
                    "instruction_name": "instructions.default"
                }),
                EventProvenance {
                    source_streams: vec![thread_stream],
                    discharged_by: Some("coupling:std::prompt.dynamic_instructions".to_string()),
                    function: Some(
                        "op://std-prompt-dynamic-instructions/run@sha256:test".to_string(),
                    ),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    store
        .append_events(
            &derived_context_stream,
            vec![NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::ContextReadPlanSet,
                serde_json::json!({
                    "schema": EventKind::ContextReadPlanSet.payload_schema_id(),
                    "scope": "thread",
                    "name": "instructions.default",
                    "pipeline_id": "context.instructions",
                    "source_id": derived_context_stream.as_str(),
                    "template_id": "std::prompt.dynamic_instructions",
                    "read_plan": {
                        "schema": "cooldis.context.read_plan/1",
                        "name": "instructions.default",
                        "source_stream": derived_context_stream.as_str(),
                        "frontier": "compile_frontier",
                        "entries": [{
                            "kind": "event_ref",
                            "stream_id": derived_context_stream.as_str(),
                            "event_id": instruction[0].id.to_string(),
                            "event_role": "instruction_checkpoint"
                        }]
                    }
                }),
                EventProvenance {
                    source_streams: vec![derived_context_stream.clone()],
                    source_event_ids: vec![instruction[0].id],
                    discharged_by: Some("coupling:std::prompt.dynamic_instructions".to_string()),
                    function: Some(
                        "op://std-prompt-dynamic-instructions/run@sha256:test".to_string(),
                    ),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "what is the stream backend policy?",
    )
    .await
    .unwrap();
    assert_output(&mut events, "instruction-aware reply").await;

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[0].messages),
        vec![
            "<instruction_context>\n- Prefer SQLite event sourcing for V1 unless the live lane asks for S2.\n</instruction_context>",
            "what is the stream backend policy?",
        ]
    );
}

#[tokio::test]
async fn runtime_emits_model_lifecycle_and_context_diagnostics() {
    let mut capabilities = ProviderCapabilityRecord::for_api(ProviderApi::OpenAIResponses);
    capabilities.context_policy = ProviderContextPolicy {
        max_messages: Some(1),
        max_text_bytes: Some(4),
    };
    let client = Arc::new(
        RecordingClient::with_responses(vec![
            response_text("first reply"),
            response_text("second reply"),
        ])
        .with_capabilities(capabilities),
    );
    let host = RuntimeHost::with_session_store(
        factory(Arc::clone(&client)),
        Arc::new(InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    assert_output(&mut events, "first reply").await;
    host.submit(thread.context().coordinates.thread_id, "turn-2", "again")
        .await
        .unwrap();
    let runtime_events = assert_output_with_runtime_events(&mut events, "second reply").await;

    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ContextCompiled {
                diagnostics,
                provider_dropped_messages: 2,
                provider_truncated_text_bytes: 1,
                provider_retained_text_bytes: 4,
            } if diagnostics.input_entry_count == 3
                && diagnostics.output_message_count == 3
                && diagnostics.retained_text_bytes > 4
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestStarted {
                turn_id,
                provider,
                api,
                model,
                mode: RuntimeModelRequestMode::Complete,
                purpose: RuntimeModelRequestPurpose::Turn,
                message_count: 1,
                max_tokens: 128,
                ..
            } if turn_id == "turn-2"
                && provider == "openai"
                && api == "openai_responses"
                && model == "gpt-test"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestCompleted {
                turn_id,
                usage,
                stop_reason: CanonicalStopReason::EndTurn,
                ..
            } if turn_id == "turn-2"
                && usage.input_tokens == 1
                && usage.output_tokens == 2
        )
    }));
}

#[tokio::test]
async fn runtime_emits_model_request_failed_on_provider_error() {
    let client = Arc::new(RecordingClient::with_responses(Vec::new()));
    let host = RuntimeHost::with_session_store(
        factory(Arc::clone(&client)),
        Arc::new(InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    let runtime_events =
        assert_failed_with_runtime_events(&mut events, "no test response queued").await;

    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestStarted {
                turn_id,
                purpose: RuntimeModelRequestPurpose::Turn,
                ..
            } if turn_id == "turn-1"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestFailed {
                turn_id,
                error,
                ..
            } if turn_id == "turn-1" && error.contains("no test response queued")
        )
    }));
}

#[tokio::test]
async fn model_request_retries_retryable_provider_error() {
    let client = Arc::new(ScriptedClient::new(vec![
        ScriptedResponse::Error(crate::ProviderError::Http("temporary outage".to_string())),
        ScriptedResponse::Response(response_text("retry reply")),
    ]));
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let factory = Arc::new(
        CanonicalProviderRuntimeFactory::new(config, client.clone())
            .with_model_request_retry_policy(ModelRequestRetryPolicy::fixed(2, 0)),
    );
    let host = RuntimeHost::with_session_store(factory, Arc::new(InMemorySessionStore::new()));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    let runtime_events = assert_output_with_runtime_events(&mut events, "retry reply").await;

    assert_eq!(client.requests().len(), 2);
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestFailed {
                error_class: RuntimeModelRequestErrorClass::Retryable,
                error,
                ..
            } if error.contains("temporary outage")
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestRetryScheduled {
                attempt: 1,
                next_attempt: 2,
                delay_ms: 0,
                error_class: RuntimeModelRequestErrorClass::Retryable,
                ..
            }
        )
    }));
}

#[tokio::test]
async fn model_request_does_not_retry_fatal_provider_error() {
    let client = Arc::new(ScriptedClient::new(vec![
        ScriptedResponse::Error(crate::ProviderError::Decode("bad json".to_string())),
        ScriptedResponse::Response(response_text("unused reply")),
    ]));
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let factory = Arc::new(
        CanonicalProviderRuntimeFactory::new(config, client.clone())
            .with_model_request_retry_policy(ModelRequestRetryPolicy::fixed(2, 0)),
    );
    let host = RuntimeHost::with_session_store(factory, Arc::new(InMemorySessionStore::new()));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    let runtime_events = assert_failed_with_runtime_events(&mut events, "bad json").await;

    assert_eq!(client.requests().len(), 1);
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestFailed {
                error_class: RuntimeModelRequestErrorClass::Fatal,
                error,
                ..
            } if error.contains("bad json")
        )
    }));
    assert!(
        !runtime_events
            .iter()
            .any(|event| { matches!(event, RuntimeEventKind::ModelRequestRetryScheduled { .. }) })
    );
}

#[tokio::test]
async fn model_request_falls_back_after_retry_exhaustion() {
    let primary_client = Arc::new(ScriptedClient::new(vec![ScriptedResponse::Error(
        crate::ProviderError::HttpStatus {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            body: "provider down".to_string(),
        },
    )]));
    let fallback_client = Arc::new(RecordingClient::with_responses(vec![response_text(
        "fallback reply",
    )]));
    let mut primary_config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    primary_config.max_tokens = 128;
    let fallback_config = CanonicalProviderRuntimeConfig::new(
        ProviderApi::OpenAIResponses,
        "fallback",
        "gpt-fallback",
    );
    let factory = Arc::new(
        CanonicalProviderRuntimeFactory::new(primary_config, primary_client.clone())
            .with_model_request_fallback(fallback_config, fallback_client.clone()),
    );
    let host = RuntimeHost::with_session_store(factory, Arc::new(InMemorySessionStore::new()));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    let (assistant, runtime_events) = assert_assistant_with_runtime_events(&mut events).await;

    assert_eq!(primary_client.requests().len(), 1);
    assert_eq!(fallback_client.requests().len(), 1);
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestFallbackSelected {
                from_provider,
                from_model,
                to_provider,
                to_model,
                error_class: RuntimeModelRequestErrorClass::Retryable,
                ..
            } if from_provider == "openai"
                && from_model == "gpt-test"
                && to_provider == "fallback"
                && to_model == "gpt-fallback"
        )
    }));
    assert!(matches!(
        assistant,
        CanonicalMessage::Assistant {
            provider,
            api: ProviderApi::OpenAIResponses,
            model,
            content,
            ..
        } if provider == "fallback"
            && model == "gpt-fallback"
            && text_from_content(&content) == "fallback reply"
    ));
}

#[tokio::test]
async fn stream_assembly_requires_terminal_done() {
    let client = Arc::new(StreamingClient::new(vec![vec![
        ProviderStreamEvent::TextDelta {
            text: "partial".to_string(),
        },
    ]]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(streaming_runtime_factory(provider_client));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "stream")
        .await
        .unwrap();
    let runtime_events =
        assert_failed_with_runtime_events(&mut events, "provider stream ended before done event")
            .await;

    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ModelRequestFailed {
                error_class: RuntimeModelRequestErrorClass::StreamAssembly,
                error,
                ..
            } if error.contains("provider stream ended before done event")
        )
    }));
}

#[tokio::test]
async fn stream_and_complete_preserve_equivalent_final_history() {
    let complete_client = Arc::new(RecordingClient::with_responses(vec![response_text(
        "same reply",
    )]));
    let complete_host = RuntimeHost::with_session_store(
        factory(Arc::clone(&complete_client)),
        Arc::new(InMemorySessionStore::new()),
    );
    let complete_thread = complete_host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_complete"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut complete_events = complete_thread.subscribe_events();
    complete_host
        .submit(
            complete_thread.context().coordinates.thread_id,
            "turn-1",
            "hello",
        )
        .await
        .unwrap();
    assert_output(&mut complete_events, "same reply").await;

    let streaming_usage = CanonicalUsage {
        input_tokens: 1,
        output_tokens: 2,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    };
    let stream_client = Arc::new(StreamingClient::new(vec![vec![
        ProviderStreamEvent::TextDelta {
            text: "same reply".to_string(),
        },
        ProviderStreamEvent::Usage {
            usage: streaming_usage,
        },
        ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::EndTurn,
        },
    ]]));
    let provider_client: Arc<dyn ProviderClient> = stream_client;
    let stream_host = RuntimeHost::with_session_store(
        streaming_runtime_factory(provider_client),
        Arc::new(InMemorySessionStore::new()),
    );
    let stream_thread = stream_host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_stream"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut stream_events = stream_thread.subscribe_events();
    stream_host
        .submit(
            stream_thread.context().coordinates.thread_id,
            "turn-1",
            "hello",
        )
        .await
        .unwrap();
    assert_output(&mut stream_events, "same reply").await;
    assert_completed_terminal(&mut stream_events).await;

    let complete_messages = complete_thread.session_context().await.unwrap().messages;
    let stream_messages = stream_thread.session_context().await.unwrap().messages;
    assert_eq!(
        text_messages(&complete_messages),
        text_messages(&stream_messages)
    );
    match (&complete_messages[1], &stream_messages[1]) {
        (
            CanonicalMessage::Assistant {
                content: complete_content,
                usage: complete_usage,
                stop_reason: complete_stop_reason,
                ..
            },
            CanonicalMessage::Assistant {
                content: stream_content,
                usage: stream_usage,
                stop_reason: stream_stop_reason,
                ..
            },
        ) => {
            assert_eq!(complete_content, stream_content);
            assert_eq!(complete_usage, stream_usage);
            assert_eq!(complete_stop_reason, stream_stop_reason);
        }
        other => panic!("unexpected final histories: {other:?}"),
    }
}

#[tokio::test]
async fn manual_compaction_runs_hooks_and_replaces_context_with_model_summary() {
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_text("first reply"),
        response_text("summary from model"),
    ]));
    let pre_hook = Arc::new(StaticHookHandler::new(
        "pre-compact",
        HookEventName::PreCompact,
        Some("manual"),
        HookHandlerOutput::default(),
    ));
    let post_hook = Arc::new(StaticHookHandler::new(
        "post-compact",
        HookEventName::PostCompact,
        Some("manual"),
        HookHandlerOutput::default(),
    ));
    let pre_handler: Arc<dyn HookHandler> = pre_hook.clone();
    let post_handler: Arc<dyn HookHandler> = post_hook.clone();
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let factory = Arc::new(
        CanonicalProviderRuntimeFactory::new(config, client.clone()).with_hook_pipeline(Arc::new(
            HookPipeline::new()
                .with_handler(pre_handler)
                .with_handler(post_handler),
        )),
    );
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(factory, store.clone());
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
        .await
        .unwrap();
    assert_output(&mut events, "first reply").await;
    host.compact_thread(thread.context().coordinates.thread_id, "compact-1", None)
        .await
        .unwrap();
    assert_compaction(&mut events, CompactionTrigger::Manual, "summary from model").await;

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[1].messages),
        vec!["hello", "first reply"]
    );
    assert!(matches!(
        pre_hook.requests().as_slice(),
        [HookRequest::PreCompact(request)] if request.trigger == CompactionTrigger::Manual
    ));
    assert!(matches!(
        post_hook.requests().as_slice(),
        [HookRequest::PostCompact(request)]
            if request.trigger == CompactionTrigger::Manual
                && request.summary == "summary from model"
    ));
    assert_eq!(
        text_messages(&thread.session_context().await.unwrap().messages),
        vec!["Compacted conversation summary:\nsummary from model"]
    );

    let stream_id = EventStreamId::for_thread(&thread.context().coordinates);
    let persisted_events = store.read_events(&stream_id, None).await.unwrap();
    let summary_event = persisted_events
        .iter()
        .find(|event| event.kind == EventKind::ContextSummaryCompleted)
        .expect("compaction should persist a context.summary.completed event");
    assert_eq!(summary_event.origin, crate::EventOrigin::Discharged);
    assert_eq!(
        summary_event.payload["schema"],
        "cooldis.event.context.summary.completed/1"
    );
    assert_eq!(summary_event.payload["text"], "summary from model");
    let expected_summary_hash = format!("sha256:{}", sha256_hex("summary from model".as_bytes()));
    assert_eq!(
        summary_event.payload["content"]["sha256"].as_str(),
        Some(expected_summary_hash.as_str())
    );

    let read_plan_event = persisted_events
        .iter()
        .find(|event| event.kind == EventKind::ContextReadPlanSet)
        .expect("compaction should persist a context.read_plan.set event");
    assert_eq!(read_plan_event.origin, crate::EventOrigin::Discharged);
    assert_eq!(
        read_plan_event.payload["schema"],
        "cooldis.event.context.read_plan.set/1"
    );
    assert_eq!(read_plan_event.payload["name"], "history.default");
    assert_eq!(
        read_plan_event.payload["read_plan"]["schema"],
        "cooldis.context.read_plan/1"
    );
    assert_eq!(
        read_plan_event.provenance.source_event_ids.first().copied(),
        Some(summary_event.id)
    );
}

#[tokio::test]
async fn auto_compaction_triggers_before_next_submit_when_budget_is_exceeded() {
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_text("first reply"),
        response_text("auto summary"),
        response_text("second reply"),
    ]));
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let factory = Arc::new(
        CanonicalProviderRuntimeFactory::new(config, client.clone())
            .with_compaction_policy(CompactionPolicy::auto_at_text_bytes(5)),
    );
    let host = RuntimeHost::with_session_store(factory, Arc::new(InMemorySessionStore::new()));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "hello world",
    )
    .await
    .unwrap();
    assert_output(&mut events, "first reply").await;
    host.submit(thread.context().coordinates.thread_id, "turn-2", "next")
        .await
        .unwrap();
    assert_compaction(&mut events, CompactionTrigger::Auto, "auto summary").await;
    assert_output(&mut events, "second reply").await;

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[1].messages),
        vec!["hello world", "first reply"]
    );
    assert_eq!(
        text_messages(&requests[2].messages),
        vec!["Compacted conversation summary:\nauto summary", "next"]
    );
}

#[tokio::test]
async fn resume_and_fork_after_compaction_preserve_active_branch() {
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_text("root reply"),
        response_text("resumed reply"),
        response_text("fork reply"),
    ]));
    let host = RuntimeHost::with_session_store(
        factory(Arc::clone(&client)),
        Arc::new(InMemorySessionStore::new()),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "root")
        .await
        .unwrap();
    assert_output(&mut events, "root reply").await;
    host.compact_thread(
        thread.context().coordinates.thread_id,
        "compact-1",
        Some("root summary".to_string()),
    )
    .await
    .unwrap();
    assert_compaction(&mut events, CompactionTrigger::Manual, "root summary").await;
    let checkpoint = host
        .create_checkpoint(
            thread.context().coordinates.thread_id,
            None,
            Some("after-compact".to_string()),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();

    let resumed = host
        .resume_thread_from_checkpoint(checkpoint.clone())
        .await
        .unwrap();
    let mut resumed_events = resumed.subscribe_events();
    host.submit(
        resumed.context().coordinates.thread_id,
        "turn-resumed",
        "resumed next",
    )
    .await
    .unwrap();
    assert_output(&mut resumed_events, "resumed reply").await;

    let fork = host
        .fork_thread_from_checkpoint(checkpoint.clone())
        .await
        .unwrap();
    let mut fork_events = fork.subscribe_events();
    host.submit(
        fork.context().coordinates.thread_id,
        "turn-fork",
        "fork next",
    )
    .await
    .unwrap();
    assert_output(&mut fork_events, "fork reply").await;

    assert_eq!(
        text_messages(&resumed.session_context().await.unwrap().messages),
        vec![
            "Compacted conversation summary:\nroot summary",
            "resumed next",
            "resumed reply"
        ]
    );
    assert_eq!(
        text_messages(&fork.session_context().await.unwrap().messages),
        vec![
            "Compacted conversation summary:\nroot summary",
            "fork next",
            "fork reply"
        ]
    );
    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[1].messages),
        vec![
            "Compacted conversation summary:\nroot summary",
            "resumed next"
        ]
    );
    assert_eq!(
        text_messages(&requests[2].messages),
        vec!["Compacted conversation summary:\nroot summary", "fork next"]
    );
}

#[tokio::test]
async fn runtime_isolates_canonical_histories_by_thread() {
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_text("reply a"),
        response_text("reply b"),
    ]));
    let host = RuntimeHost::new(factory(Arc::clone(&client)));
    let a = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let b = host
        .start_thread(
            ThreadCoordinates::new("tenant_b", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut a_events = a.subscribe_events();
    let mut b_events = b.subscribe_events();

    host.submit(a.context().coordinates.thread_id, "turn-a", "from a")
        .await
        .unwrap();
    assert_output(&mut a_events, "reply a").await;
    host.submit(b.context().coordinates.thread_id, "turn-b", "from b")
        .await
        .unwrap();
    assert_output(&mut b_events, "reply b").await;

    let requests = client.requests();
    assert_eq!(text_messages(&requests[0].messages), vec!["from a"]);
    assert_eq!(text_messages(&requests[1].messages), vec!["from b"]);
    assert_eq!(
        text_messages(&a.session_context().await.unwrap().messages),
        vec!["from a", "reply a"]
    );
    assert_eq!(
        text_messages(&b.session_context().await.unwrap().messages),
        vec!["from b", "reply b"]
    );
}

#[tokio::test]
async fn runtime_stores_tool_calls_as_canonical_assistant_content() {
    let client = Arc::new(RecordingClient::with_responses(vec![response_tool_call()]));
    let host = RuntimeHost::new(factory(client));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use bash")
        .await
        .unwrap();
    let assistant = assert_assistant_mirror(&mut events).await;

    match assistant {
        CanonicalMessage::Assistant {
            provider,
            api,
            model,
            content,
            stop_reason,
            ..
        } => {
            assert_eq!(provider, "openai");
            assert_eq!(api, ProviderApi::OpenAIResponses);
            assert_eq!(model, "gpt-test");
            assert_eq!(stop_reason, CanonicalStopReason::ToolUse);
            assert!(matches!(
                content.first(),
                Some(CanonicalContent::ToolCall { id, name, .. })
                    if id == "call_1|fc_1" && name == "bash"
            ));
        }
        other => panic!("unexpected stored message: {other:?}"),
    }

    let session = thread.session_context().await.unwrap();
    assert!(session.entries.iter().all(is_canonical_message_entry));
}

#[tokio::test]
async fn runtime_executes_registry_tool_call_and_continues_with_tool_result() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "cooldis"})),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(runtime_factory_with_registry(provider_client, registry));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use echo")
        .await
        .unwrap();
    let runtime_events = assert_output_with_runtime_events(&mut events, "final reply").await;
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallResult {
                call_id,
                output,
                success: true,
                duration_ms: Some(_),
            } if call_id == "call_1|fc_1" && output == "echo:cooldis"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::PermissionDecision {
                call_id,
                tool_name,
                decision: RuntimePermissionDecision::Allow,
                reason: None,
            } if call_id == "call_1|fc_1" && tool_name == "echo_search"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolLog {
                call_id,
                tool_name,
                level: RuntimeToolLogLevel::Info,
                metadata,
                ..
            } if call_id == "call_1|fc_1"
                && tool_name == "echo_search"
                && metadata.get("success").map(String::as_str) == Some("true")
                && metadata.contains_key("duration_ms")
        )
    }));

    let requests = client.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0]
            .tools
            .iter()
            .any(|tool| tool.name == "echo_search")
    );
    assert!(matches!(
        &requests[1].messages[2],
        CanonicalMessage::ToolResult {
            tool_call_id,
            tool_name,
            content,
            is_error: false,
            ..
        } if tool_call_id == "call_1|fc_1"
            && tool_name == "echo_search"
            && text_from_content(content) == "echo:cooldis"
    ));

    let session = thread.session_context().await.unwrap();
    assert_eq!(
        text_messages(&session.messages),
        vec!["use echo", "", "echo:cooldis", "final reply"]
    );
    assert!(session.entries.iter().all(is_canonical_message_entry));
}

#[tokio::test]
async fn runtime_persists_tool_request_and_completion_facts() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "cooldis"})),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use echo")
        .await
        .unwrap();
    assert_output(&mut events, "final reply").await;

    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let request = records
        .iter()
        .find(|event| event.kind == EventKind::ToolCallRequested)
        .expect("tool call request should be durable");
    assert_eq!(request.origin, crate::EventOrigin::Discharged);
    assert_eq!(request.payload["tool_name"].as_str(), Some("echo_search"));
    assert_eq!(request.payload["tool"].as_str(), Some("echo_search"));
    assert_eq!(
        request.payload["subject"]["turn_id"].as_str(),
        Some("turn-1")
    );
    assert_eq!(
        request.payload["subject"]["call_id"].as_str(),
        Some("call_1|fc_1")
    );
    assert!(
        !request.provenance.source_event_ids.is_empty(),
        "tool requests should point back to the assistant session entry"
    );
    let completed = records
        .iter()
        .find(|event| event.kind == EventKind::ToolCallCompleted)
        .expect("tool completion should be durable");
    assert_eq!(completed.origin, crate::EventOrigin::Witnessed);
    assert_eq!(completed.payload["tool_name"].as_str(), Some("echo_search"));
    assert_eq!(completed.payload["success"].as_bool(), Some(true));
    assert!(records.iter().any(|event| {
        event.kind == EventKind::TurnSubmitted
            && event.origin == crate::EventOrigin::Witnessed
            && event.payload["turn_id"].as_str() == Some("turn-1")
    }));
    assert!(records.iter().any(|event| {
        event.kind == EventKind::TurnCompleted
            && event.origin == crate::EventOrigin::Discharged
            && event.payload["turn_id"].as_str() == Some("turn-1")
            && !event.provenance.source_event_ids.is_empty()
    }));
}

#[tokio::test]
async fn bound_tool_controller_without_terminal_fact_denies_fail_closed() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "cooldis"})),
        response_text("handled denial"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    append_tool_controller_bind_receipt(&store, &thread.context().coordinates, "echo_search").await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use echo")
        .await
        .unwrap();
    assert_output(&mut events, "handled denial").await;

    let requests = client.requests();
    assert_eq!(requests.len(), 2);
    assert!(matches!(
        &requests[1].messages[2],
        CanonicalMessage::ToolResult {
            tool_name,
            content,
            is_error: true,
            ..
        } if tool_name == "echo_search"
            && text_from_content(content)
                .contains("tool controller did not emit a terminal decision")
    ));
    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let completed = records
        .iter()
        .find(|event| event.kind == EventKind::ToolCallCompleted)
        .expect("denial should still write a terminal tool result fact");
    assert_eq!(completed.payload["success"].as_bool(), Some(false));
    assert!(
        records
            .iter()
            .any(|event| event.kind == EventKind::ToolCallRequested)
    );
    assert!(
        !text_messages(&thread.session_context().await.unwrap().messages)
            .iter()
            .any(|text| text == "echo:cooldis"),
        "the operation should not run when a matching controller fails to decide"
    );
}

#[tokio::test]
async fn witnessed_tool_suspension_pauses_turn_without_invoking_tool() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "cooldis"})),
        response_text("should not be requested"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    append_tool_controller_bind_receipt(&store, &thread.context().coordinates, "echo_search").await;
    append_witnessed_tool_suspension(
        &store,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call_1|fc_1",
        "approval-1",
    )
    .await;
    let mut status = thread.subscribe_status();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use echo")
        .await
        .unwrap();
    wait_for_thread_event(
        &store,
        &thread.context().coordinates,
        EventKind::TurnWaiting,
    )
    .await;
    wait_for_status(&mut status, crate::ThreadStatus::Idle).await;

    let requests = client.requests();
    assert_eq!(
        requests.len(),
        1,
        "the provider should not receive a continuation request while the tool is suspended"
    );
    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        records
            .iter()
            .any(|event| event.kind == EventKind::ToolCallRequested)
    );
    assert!(
        records
            .iter()
            .all(|event| event.kind != EventKind::ToolCallCompleted)
    );
    assert!(
        records
            .iter()
            .all(|event| event.kind != EventKind::TurnCompleted)
    );
    let pending =
        crate::list_pending_tool_call_suspensions(store.as_ref(), &thread.context().coordinates)
            .await
            .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].approval_id.as_deref(), Some("approval-1"));
}

#[tokio::test]
async fn resume_tool_call_consumes_decision_and_invokes_once() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "cooldis"})),
        response_text("resumed final"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    append_tool_controller_bind_receipt(&store, &thread.context().coordinates, "echo_search").await;
    append_witnessed_tool_suspension(
        &store,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call_1|fc_1",
        "approval-1",
    )
    .await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use echo")
        .await
        .unwrap();
    wait_for_thread_event(
        &store,
        &thread.context().coordinates,
        EventKind::TurnWaiting,
    )
    .await;
    append_witnessed_tool_decision(
        &store,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call_1|fc_1",
        ToolCallDecisionOutcomePayload::Allow,
    )
    .await;
    host.resume_tool_call(
        thread.context().coordinates.thread_id,
        "turn-1",
        "call_1|fc_1",
    )
    .await
    .unwrap();
    assert_output(&mut events, "resumed final").await;

    let requests = client.requests();
    assert_eq!(requests.len(), 2);
    assert!(matches!(
        &requests[1].messages[2],
        CanonicalMessage::ToolResult {
            tool_call_id,
            tool_name,
            content,
            is_error: false,
            ..
        } if tool_call_id == "call_1|fc_1"
            && tool_name == "echo_search"
            && text_from_content(content) == "echo:cooldis"
    ));
    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        records
            .iter()
            .any(|event| event.kind == EventKind::ToolCallCompleted)
    );
    assert!(
        records
            .iter()
            .any(|event| event.kind == EventKind::TurnCompleted)
    );
    let control_records = store
        .read_events(
            &EventStreamId::new(format!(
                "control:{}",
                thread.context().coordinates.thread_id
            )),
            None,
        )
        .await
        .unwrap();
    assert!(
        control_records
            .iter()
            .any(|event| event.kind == EventKind::TurnResumed)
    );

    host.resume_tool_call(
        thread.context().coordinates.thread_id,
        "turn-1",
        "call_1|fc_1",
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        client.requests().len(),
        2,
        "duplicate resume must not invoke or continue the tool twice"
    );
}

#[tokio::test]
async fn runtime_bash_tool_advertises_and_executes_operation_shell_commands() {
    let registry = named_echo_registry("search", "search").await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named(
            "bash",
            serde_json::json!({
                "command": "command -v search && printf cooldis | search"
            }),
        ),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let bash_config = VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_capability_grants(cooldis_threads_kernel_package().capability_grants);
    let host = RuntimeHost::new(Arc::new(
        CanonicalProviderRuntimeFactory::new(
            CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test"),
            provider_client,
        )
        .with_bash_tool(bash_config),
    ));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "use search",
    )
    .await
    .unwrap();
    let runtime_events = assert_output_with_runtime_events(&mut events, "final reply").await;
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallResult {
                call_id,
                output,
                success: true,
                ..
            } if call_id == "call_1|fc_1"
                && output.contains(r#""exit_code":0"#)
                && output.contains("search\\n")
                && output.contains("echo:cooldis")
        )
    }));

    let requests = client.requests();
    assert_eq!(requests.len(), 2);
    let bash_tool = requests[0]
        .tools
        .iter()
        .find(|tool| tool.name == "bash")
        .expect("bash tool should be advertised");
    assert!(
        bash_tool
            .description
            .contains("Published operation commands are available directly")
    );
    assert!(bash_tool.description.contains("search"));
    assert!(matches!(
        &requests[1].messages[2],
        CanonicalMessage::ToolResult {
            tool_call_id,
            tool_name,
            content,
            is_error: false,
            ..
        } if tool_call_id == "call_1|fc_1"
            && tool_name == "bash"
            && text_from_content(content).contains("echo:cooldis")
    ));
}

#[tokio::test]
async fn runtime_bash_tool_executes_kernel_thread_operation_commands_without_agent_builtin() {
    let registry = kernel_thread_registry().await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named(
            "bash",
            serde_json::json!({
                "command": "if command -v agent >/dev/null 2>&1; then echo agent-present; exit 9; fi; printf '{\"task_name\":\"worker\",\"message\":\"echo child-through-bash\"}' | thread_spawn"
            }),
        ),
        response_text("spawned child from bash"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let bash_config = VirtualBashRuntimeConfig::default()
        .with_operation_registry(registry)
        .with_capability_grants(cooldis_threads_kernel_package().capability_grants);
    let root_factory = CanonicalProviderRuntimeFactory::new(
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test"),
        provider_client,
    )
    .with_bash_tool(bash_config);
    let host = RuntimeHost::new(Arc::new(RootProviderChildEchoFactory {
        root: Arc::new(root_factory),
    }));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "spawn worker from bash",
    )
    .await
    .unwrap();
    let runtime_events =
        assert_output_with_runtime_events(&mut events, "spawned child from bash").await;
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallResult {
                call_id,
                success: true,
                ..
            } if call_id == "call_1|fc_1"
        )
    }));

    let requests = client.requests();
    assert_eq!(requests.len(), 2);
    let bash_tool = requests[0]
        .tools
        .iter()
        .find(|tool| tool.name == "bash")
        .expect("bash tool should be advertised");
    assert!(bash_tool.description.contains(THREAD_SPAWN_OPERATION));
    assert!(!bash_tool.description.contains("agent <"));

    let children = host
        .children_of(thread.context().coordinates.thread_id)
        .await;
    assert_eq!(children.len(), 1);
    let child_session = children[0].session_context().await.unwrap();
    assert_eq!(
        text_messages(&child_session.messages),
        vec!["echo child-through-bash"]
    );
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn runtime_runs_pre_and_post_tool_hooks_around_tool_execution() {
    let registry = echo_registry("echo").await;
    let pre_hook = Arc::new(StaticHookHandler::new(
        "pre-echo",
        HookEventName::PreToolUse,
        Some("echo_search"),
        HookHandlerOutput {
            updated_input: Some(serde_json::json!({"input": "rewritten"})),
            additional_context: Some("pre context".to_string()),
            ..HookHandlerOutput::default()
        },
    ));
    let post_hook = Arc::new(StaticHookHandler::new(
        "post-echo",
        HookEventName::PostToolUse,
        Some("echo_search"),
        HookHandlerOutput {
            replacement_output: Some("hook replacement".to_string()),
            additional_context: Some("post context".to_string()),
            feedback: Some("feedback context".to_string()),
            ..HookHandlerOutput::default()
        },
    ));
    let pre_handler: Arc<dyn HookHandler> = pre_hook.clone();
    let post_handler: Arc<dyn HookHandler> = post_hook.clone();
    let hook_pipeline = Arc::new(
        HookPipeline::new()
            .with_handler(pre_handler)
            .with_handler(post_handler),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "original"})),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let host = RuntimeHost::new(Arc::new(
        CanonicalProviderRuntimeFactory::new(config, provider_client)
            .with_operation_registry(registry)
            .with_hook_pipeline(hook_pipeline),
    ));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use echo")
        .await
        .unwrap();
    let runtime_events = assert_output_with_runtime_events(&mut events, "final reply").await;

    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::HookStarted {
                hook_id,
                event_name: HookEventName::PreToolUse,
                matcher: Some(matcher),
            } if hook_id == "pre-echo" && matcher == "echo_search"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::HookCompleted {
                hook_id,
                event_name: HookEventName::PostToolUse,
                status: HookRunStatus::Completed,
                ..
            } if hook_id == "post-echo"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallResult {
                output,
                success: true,
                ..
            } if output == "hook replacement"
        )
    }));
    assert_eq!(
        runtime_events
            .iter()
            .filter(|event| matches!(
                event,
                RuntimeEventKind::ToolCallStarted { call_id, .. } if call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );
    assert_eq!(
        runtime_events
            .iter()
            .filter(|event| matches!(
                event,
                RuntimeEventKind::ToolCallResult { call_id, .. } if call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );

    let pre_requests = pre_hook.requests();
    assert!(matches!(
        &pre_requests[0],
        HookRequest::PreToolUse(request)
            if request.arguments == serde_json::json!({"input": "original"})
    ));
    let post_requests = post_hook.requests();
    assert!(matches!(
        &post_requests[0],
        HookRequest::PostToolUse(request)
            if request.arguments == serde_json::json!({"input": "rewritten"})
                && request.output == "echo:rewritten"
    ));

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[1].messages),
        vec![
            "use echo",
            "",
            "pre context",
            "hook replacement",
            "post context",
            "feedback context"
        ]
    );
}

#[tokio::test]
async fn mutating_tool_hooks_append_secret_free_witnesses_before_effects() {
    let store = Arc::new(InMemorySessionStore::new());
    let pre_command = r#"cat >/dev/null; printf '%s' '{"updated_input":{"input":"rewritten","secret":"after-secret"}}'"#;
    let post_command = r#"cat >/dev/null; printf '%s' '{"replacement_output":"hook replacement after-secret-output"}'"#;
    let expected_pre_command_sha256 = sha256_hex(pre_command.as_bytes());
    let expected_post_command_sha256 = sha256_hex(post_command.as_bytes());
    let echo_provider = Arc::new(WitnessCheckingEchoProvider {
        store: store.clone(),
        expected_command_sha256: expected_pre_command_sha256.clone(),
        seen_arguments: Mutex::new(Vec::new()),
    });
    let kernel_provider: Arc<dyn AgentKernelToolProvider> = echo_provider.clone();
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(kernel_provider),
    );
    let hook_pipeline = Arc::new(
        HookPipeline::new()
            .with_command_handler(
                CommandHookHandler::new("pre-echo", HookEventName::PreToolUse, pre_command)
                    .with_matcher("echo_search"),
            )
            .with_command_handler(
                CommandHookHandler::new("post-echo", HookEventName::PostToolUse, post_command)
                    .with_matcher("echo_search"),
            ),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named(
            "echo_search",
            serde_json::json!({"input":"original","secret":"before-secret"}),
        ),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(config, provider_client)
                .with_tool_router(router)
                .with_hook_pipeline(hook_pipeline),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use echo")
        .await
        .unwrap();
    assert_output(&mut events, "final reply").await;

    assert_eq!(
        echo_provider.seen_arguments(),
        vec![serde_json::json!({"input":"rewritten","secret":"after-secret"})]
    );
    let witnesses = store
        .list_observations(
            &thread.context().coordinates,
            Some("host.hook.mutation_witnessed"),
        )
        .await
        .unwrap();
    assert_eq!(witnesses.len(), 2);
    let post_payload = witnesses
        .iter()
        .map(|record| &record.payload)
        .find(|payload| payload["hook_event_name"] == "post_tool_use")
        .expect("post-tool replacement should be witnessed");
    assert_eq!(
        post_payload["command_sha256"].as_str(),
        Some(expected_post_command_sha256.as_str())
    );
    assert_eq!(
        post_payload["mutated_fields"],
        serde_json::json!(["replacement_output"])
    );
    assert_eq!(
        post_payload["tool_output"]["before_sha256"].as_str(),
        Some(sha256_hex("tool original before-secret-output".as_bytes()).as_str())
    );
    assert_eq!(
        post_payload["tool_output"]["after_sha256"].as_str(),
        Some(sha256_hex("hook replacement after-secret-output".as_bytes()).as_str())
    );
    for witness in &witnesses {
        assert_payload_omits_values(
            &witness.payload,
            &[
                "original",
                "rewritten",
                "before-secret",
                "after-secret",
                "tool original before-secret-output",
                "hook replacement after-secret-output",
            ],
        );
    }
}

#[tokio::test]
async fn pre_tool_hook_can_block_tool_execution() {
    let registry = echo_registry("echo").await;
    let block_hook = Arc::new(StaticHookHandler::new(
        "block-echo",
        HookEventName::PreToolUse,
        Some("echo_search"),
        HookHandlerOutput {
            should_block: true,
            block_reason: Some("blocked by hook".to_string()),
            additional_context: Some("block context".to_string()),
            ..HookHandlerOutput::default()
        },
    ));
    let hook_handler: Arc<dyn HookHandler> = block_hook.clone();
    let hook_pipeline = Arc::new(HookPipeline::new().with_handler(hook_handler));
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "original"})),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let host = RuntimeHost::new(Arc::new(
        CanonicalProviderRuntimeFactory::new(config, provider_client)
            .with_operation_registry(registry)
            .with_hook_pipeline(hook_pipeline),
    ));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "use echo")
        .await
        .unwrap();
    let runtime_events = assert_output_with_runtime_events(&mut events, "final reply").await;

    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::HookCompleted {
                hook_id,
                event_name: HookEventName::PreToolUse,
                status: HookRunStatus::Blocked,
                message: Some(message),
                ..
            } if hook_id == "block-echo" && message == "blocked by hook"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallResult {
                output,
                success: false,
                ..
            } if output == "blocked by hook"
        )
    }));
    assert_eq!(
        runtime_events
            .iter()
            .filter(|event| matches!(
                event,
                RuntimeEventKind::ToolCallStarted { call_id, .. } if call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );
    assert_eq!(
        runtime_events
            .iter()
            .filter(|event| matches!(
                event,
                RuntimeEventKind::ToolCallResult { call_id, .. } if call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[1].messages),
        vec!["use echo", "", "block context", "blocked by hook"]
    );
}

#[tokio::test]
async fn block_stop_and_observe_only_hook_witnessing() {
    let block_store = Arc::new(InMemorySessionStore::new());
    let block_command = r#"cat >/dev/null; printf '%s' '{"should_block":true,"block_reason":"blocked by hook secret","additional_context":"block context secret"}'"#;
    let block_hook_pipeline = Arc::new(
        HookPipeline::new().with_command_handler(
            CommandHookHandler::new("block-echo", HookEventName::PreToolUse, block_command)
                .with_matcher("echo_search"),
        ),
    );
    let block_client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "original"})),
        response_text("final reply"),
    ]));
    let block_provider_client: Arc<dyn ProviderClient> = block_client.clone();
    let mut block_config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    block_config.max_tokens = 128;
    let block_host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(block_config, block_provider_client)
                .with_operation_registry(echo_registry("echo").await)
                .with_hook_pipeline(block_hook_pipeline),
        ),
        block_store.clone(),
    );
    let block_thread = block_host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_block"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut block_events = block_thread.subscribe_events();

    block_host
        .submit(
            block_thread.context().coordinates.thread_id,
            "turn-1",
            "use echo",
        )
        .await
        .unwrap();
    assert_output(&mut block_events, "final reply").await;
    let block_witnesses = block_store
        .list_observations(
            &block_thread.context().coordinates,
            Some("host.hook.mutation_witnessed"),
        )
        .await
        .unwrap();
    assert_eq!(block_witnesses.len(), 1);
    let block_payload = &block_witnesses[0].payload;
    assert_eq!(
        block_payload["command_sha256"].as_str(),
        Some(sha256_hex(block_command.as_bytes()).as_str())
    );
    assert_mutated_fields(block_payload, &["additional_contexts", "should_block"]);
    assert_payload_omits_values(
        block_payload,
        &["blocked by hook secret", "block context secret"],
    );

    let stop_store = Arc::new(InMemorySessionStore::new());
    let stop_command =
        r#"cat >/dev/null; printf '%s' '{"should_stop":true,"stop_reason":"stop secret"}'"#;
    let stop_hook_pipeline = Arc::new(HookPipeline::new().with_command_handler(
        CommandHookHandler::new("stop-turn", HookEventName::UserPromptSubmit, stop_command),
    ));
    let stop_client = Arc::new(RecordingClient::with_responses(vec![]));
    let stop_provider_client: Arc<dyn ProviderClient> = stop_client.clone();
    let stop_host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                stop_provider_client,
            )
            .with_hook_pipeline(stop_hook_pipeline),
        ),
        stop_store.clone(),
    );
    let stop_thread = stop_host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_stop"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut stop_events = stop_thread.subscribe_events();

    stop_host
        .submit(
            stop_thread.context().coordinates.thread_id,
            "turn-1",
            "stop before provider",
        )
        .await
        .unwrap();
    assert_stopped(&mut stop_events).await;
    assert!(stop_client.requests().is_empty());
    let stop_witnesses = stop_store
        .list_observations(
            &stop_thread.context().coordinates,
            Some("host.hook.mutation_witnessed"),
        )
        .await
        .unwrap();
    assert_eq!(stop_witnesses.len(), 1);
    let stop_payload = &stop_witnesses[0].payload;
    assert_eq!(
        stop_payload["hook_event_name"].as_str(),
        Some("user_prompt_submit")
    );
    assert_eq!(
        stop_payload["command_sha256"].as_str(),
        Some(sha256_hex(stop_command.as_bytes()).as_str())
    );
    assert_mutated_fields(stop_payload, &["should_stop"]);
    assert_payload_omits_values(stop_payload, &["stop secret"]);

    let observe_store = Arc::new(InMemorySessionStore::new());
    let observe_hook_pipeline = Arc::new(
        HookPipeline::new().with_command_handler(
            CommandHookHandler::new("observe-echo", HookEventName::PreToolUse, "cat >/dev/null")
                .with_matcher("echo_search"),
        ),
    );
    let observe_client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "observed"})),
        response_text("final reply"),
    ]));
    let observe_provider_client: Arc<dyn ProviderClient> = observe_client.clone();
    let mut observe_config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    observe_config.max_tokens = 128;
    let observe_host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(observe_config, observe_provider_client)
                .with_operation_registry(echo_registry("echo").await)
                .with_hook_pipeline(observe_hook_pipeline),
        ),
        observe_store.clone(),
    );
    let observe_thread = observe_host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_observe"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut observe_events = observe_thread.subscribe_events();

    observe_host
        .submit(
            observe_thread.context().coordinates.thread_id,
            "turn-1",
            "use echo",
        )
        .await
        .unwrap();
    assert_output(&mut observe_events, "final reply").await;
    let observe_witnesses = observe_store
        .list_observations(
            &observe_thread.context().coordinates,
            Some("host.hook.mutation_witnessed"),
        )
        .await
        .unwrap();
    assert!(observe_witnesses.is_empty());
}

#[tokio::test]
async fn runtime_passes_turn_context_to_tool_router() {
    let kernel_provider = Arc::new(TurnContextRecordingKernelToolProvider::new());
    let tool_provider: Arc<dyn AgentKernelToolProvider> = kernel_provider.clone();
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named_with_id(
            "call_1|fc_1",
            "record_turn_context",
            serde_json::json!({}),
        ),
        response_tool_call_named_with_id(
            "call_2|fc_2",
            "record_turn_context",
            serde_json::json!({}),
        ),
        response_text("final reply"),
    ]));
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(Arc::new(
        CanonicalProviderRuntimeFactory::new(config, provider_client).with_tool_router(router),
    ));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit_turn(
        thread.context().coordinates.thread_id,
        "turn-context-1",
        crate::TurnInput::text("record context")
            .with_cwd("/tmp/cooldis-turn")
            .with_permission_profile("workspace-write")
            .with_metadata("source", "provider-runtime-test"),
    )
    .await
    .unwrap();
    assert_output(&mut events, "final reply").await;

    let snapshots = kernel_provider.snapshots();
    assert_eq!(snapshots.len(), 2);
    let snapshot = snapshots[0].as_ref().expect("turn context snapshot");
    let second_snapshot = snapshots[1].as_ref().expect("second turn context snapshot");
    assert_eq!(snapshot.turn_id, "turn-context-1");
    assert_eq!(second_snapshot.turn_id, snapshot.turn_id);
    assert_eq!(second_snapshot.trace_id, snapshot.trace_id);
    assert_eq!(snapshot.coordinates, thread.context().coordinates);
    assert_eq!(snapshot.model.as_deref(), Some("gpt-test"));
    assert_eq!(snapshot.provider.as_deref(), Some("openai"));
    assert_eq!(
        snapshot.permission_profile.as_deref(),
        Some("workspace-write")
    );
    assert_eq!(
        snapshot.metadata.get("source").map(String::as_str),
        Some("provider-runtime-test")
    );
    assert_eq!(
        snapshot.budget.max_tool_rounds,
        Some(MAX_TOOL_ROUTER_ROUNDS)
    );
    assert_eq!(snapshot.budget.max_output_tokens, Some(128));
    assert!(!snapshot.cancellation_requested);
}

#[tokio::test]
async fn runtime_routes_thread_spawn_operation_through_kernel_dispatch() {
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named(
            THREAD_SPAWN_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "message": "echo child-through-tool",
            }),
        ),
        response_text("spawned child"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let root_factory = CanonicalProviderRuntimeFactory::new(config, provider_client)
        .with_tool_router(Arc::new(kernel_thread_router().await));
    let host = RuntimeHost::new(Arc::new(RootProviderChildEchoFactory {
        root: Arc::new(root_factory),
    }));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "spawn worker",
    )
    .await
    .unwrap();
    let runtime_events = assert_output_with_runtime_events(&mut events, "spawned child").await;

    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallResult {
                call_id,
                output,
                success: true,
                ..
            } if call_id == "call_1|fc_1" && output.contains("cooldis.thread_spawn")
        )
    }));

    let requests = client.requests();
    assert!(
        requests[0]
            .tools
            .iter()
            .any(|tool| tool.name == THREAD_SPAWN_OPERATION)
    );
    assert_eq!(requests.len(), 2);

    let children = host
        .children_of(thread.context().coordinates.thread_id)
        .await;
    assert_eq!(children.len(), 1);
    let child_session = children[0].session_context().await.unwrap();
    assert_eq!(
        text_messages(&child_session.messages),
        vec!["echo child-through-tool"]
    );

    let parent_session = thread.session_context().await.unwrap();
    assert!(
        text_messages(&parent_session.messages)
            .iter()
            .any(|text| text.contains("cooldis.thread_spawn"))
    );
    host.shutdown_all().await.unwrap();
}

async fn kernel_thread_registry() -> Arc<OperationRegistry> {
    let registry = Arc::new(OperationRegistry::new());
    let package = cooldis_threads_kernel_package();
    let mut registration =
        KernelOperationRegistration::new(crate::COOLDIS_THREADS_PACKAGE, package.manifest.clone())
            .with_capability_grants(package.capability_grants.clone());
    registration.metadata.insert(
        crate::OPERATION_METADATA_RUNTIME_KIND.to_string(),
        serde_json::Value::String(crate::KERNEL_RUNTIME_KIND.to_string()),
    );
    registry.register_kernel(registration).await.unwrap();
    registry
}

async fn kernel_thread_router() -> AgentToolRouter {
    let registry = kernel_thread_registry().await;
    let package = cooldis_threads_kernel_package();
    AgentToolRouter::new(registry)
        .with_capability_grants(package.capability_grants)
        .with_tool_aliases(vec![OperationToolAlias {
            tool_name: THREAD_SPAWN_OPERATION.to_string(),
            registered_name: crate::COOLDIS_THREADS_PACKAGE.to_string(),
            operation_name: THREAD_SPAWN_OPERATION.to_string(),
        }])
}

async fn append_tool_controller_bind_receipt(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    tool_name: &str,
) {
    let receipt = AgentManifestBindReceipt {
        ref_uri: "agent://test/controller".to_string(),
        manifest_hash: "snapshot-controller".to_string(),
        model_profile_id: "default".to_string(),
        provider_id: "test".to_string(),
        model_id: "model".to_string(),
        tool_ids: Vec::new(),
        operation_bindings: Vec::new(),
        tool_universes: Vec::new(),
        couplings: vec![AgentManifestCouplingBinding {
            id: "tool_gate".to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::ToolCallRequested.to_string(),
            trigger_match: BTreeMap::from([("tool".to_string(), serde_json::json!(tool_name))]),
            source_streams: vec!["thread".to_string()],
            source_kinds: vec![EventKind::ToolCallRequested.to_string()],
            sink_stream: "control".to_string(),
            sink_kinds: vec![EventKind::ToolCallDecision.to_string()],
            function_ref: "op://policy/tool-gate@sha256:test".to_string(),
            artifact_hash: "test".to_string(),
            operation_name: Some("tool_gate".to_string()),
            grants: Vec::new(),
            budget: AgentManifestCouplingBudget::default(),
            config_hash: "config".to_string(),
        }],
        granted: Vec::new(),
        effective_runtime: AgentManifestRuntimeDefaults::default(),
        overridden_keys: Vec::new(),
    };
    store
        .append_events(
            &EventStreamId::for_thread(coordinates),
            vec![NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::ManifestBindCompleted,
                serde_json::to_value(receipt).unwrap(),
                EventProvenance {
                    source_streams: vec![EventStreamId::for_thread(coordinates)],
                    discharged_by: Some("binder:manifest".to_string()),
                    function: Some("bind/v1".to_string()),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
}

async fn append_witnessed_tool_suspension(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    snapshot_id: &str,
    turn_id: &str,
    call_id: &str,
    approval_id: &str,
) {
    store
        .append_events(
            &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            vec![NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::ToolCallSuspended,
                serde_json::to_value(ToolCallSuspendedPayload {
                    subject: ToolCallSubject {
                        turn_id: turn_id.to_string(),
                        call_id: call_id.to_string(),
                    },
                    snapshot_id: snapshot_id.to_string(),
                    approval_id: Some(approval_id.to_string()),
                    reason: Some("needs human".to_string()),
                })
                .unwrap(),
            )],
        )
        .await
        .unwrap();
}

async fn append_witnessed_tool_decision(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    snapshot_id: &str,
    turn_id: &str,
    call_id: &str,
    outcome: ToolCallDecisionOutcomePayload,
) {
    store
        .append_events(
            &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
            vec![NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::ToolCallDecision,
                serde_json::to_value(ToolCallDecisionPayload {
                    subject: ToolCallSubject {
                        turn_id: turn_id.to_string(),
                        call_id: call_id.to_string(),
                    },
                    snapshot_id: snapshot_id.to_string(),
                    outcome,
                })
                .unwrap(),
            )],
        )
        .await
        .unwrap();
}

async fn wait_for_thread_event(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    kind: EventKind,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let mut records = store
            .read_events(&EventStreamId::for_thread(coordinates), None)
            .await
            .unwrap();
        records.extend(
            store
                .read_events(
                    &EventStreamId::new(format!("control:{}", coordinates.thread_id)),
                    None,
                )
                .await
                .unwrap(),
        );
        if records.iter().any(|event| event.kind == kind) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for thread event kind {kind}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_status(
    status: &mut tokio::sync::watch::Receiver<crate::ThreadStatus>,
    expected: crate::ThreadStatus,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if *status.borrow() == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for status {expected:?}"
        );
        timeout(Duration::from_millis(50), status.changed())
            .await
            .ok();
    }
}

#[tokio::test]
async fn runtime_returns_error_tool_result_for_unknown_tool_and_continues() {
    let registry = Arc::new(OperationRegistry::new());
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("missing_tool", serde_json::json!({})),
        response_text("handled missing tool"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(runtime_factory_with_registry(provider_client, registry));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "use missing",
    )
    .await
    .unwrap();
    let runtime_events =
        assert_output_with_runtime_events(&mut events, "handled missing tool").await;
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallResult {
                call_id,
                output,
                success: false,
                ..
            } if call_id == "call_1|fc_1" && output.contains("unknown tool")
        )
    }));

    let requests = client.requests();
    assert_eq!(requests.len(), 2);
    assert!(matches!(
        &requests[1].messages[2],
        CanonicalMessage::ToolResult {
            tool_name,
            content,
            is_error: true,
            ..
        } if tool_name == "missing_tool"
            && text_from_content(content).contains("unknown tool")
    ));

    let session = thread.session_context().await.unwrap();
    assert_eq!(
        text_messages(&session.messages),
        vec![
            "use missing",
            "",
            "runtime execution failed: unknown tool \"missing_tool\"",
            "handled missing tool"
        ]
    );
}

#[tokio::test]
async fn streaming_runtime_emits_deltas_and_stores_final_canonical_assistant() {
    let client = Arc::new(StreamingClient::new(vec![vec![
        ProviderStreamEvent::TextDelta {
            text: "COOL".to_string(),
        },
        ProviderStreamEvent::ToolCallDelta {
            id: "call_1".to_string(),
            name: Some("bash".to_string()),
            arguments_delta: "{\"command\"".to_string(),
        },
        ProviderStreamEvent::ToolCallDelta {
            id: "call_1".to_string(),
            name: None,
            arguments_delta: ":\"pwd\"}".to_string(),
        },
        ProviderStreamEvent::Usage {
            usage: CanonicalUsage {
                input_tokens: 5,
                output_tokens: 6,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 1,
            },
        },
        ProviderStreamEvent::Done {
            stop_reason: CanonicalStopReason::ToolUse,
        },
    ]]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(streaming_runtime_factory(provider_client));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "stream")
        .await
        .unwrap();
    let (assistant, runtime_events) = assert_assistant_with_runtime_events(&mut events).await;

    assert!(
        runtime_events.iter().any(|event| {
            matches!(event, RuntimeEventKind::TextDelta { text } if text == "COOL")
        })
    );
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::ToolCallStarted {
                call_id,
                name,
                ..
            } if call_id == "call_1" && name == "bash"
        )
    }));
    assert!(runtime_events.iter().any(|event| {
        matches!(
            event,
            RuntimeEventKind::Usage { usage }
                if usage.input_tokens == 5
                    && usage.output_tokens == 6
                    && usage.cache_read_input_tokens == 1
        )
    }));

    match assistant {
        CanonicalMessage::Assistant {
            content,
            usage,
            stop_reason,
            ..
        } => {
            assert_eq!(stop_reason, CanonicalStopReason::ToolUse);
            assert_eq!(usage.input_tokens, 5);
            assert_eq!(usage.output_tokens, 6);
            assert!(matches!(
                &content[0],
                CanonicalContent::Text { text, .. } if text == "COOL"
            ));
            assert!(matches!(
                &content[1],
                CanonicalContent::ToolCall { id, name, arguments }
                    if id == "call_1" && name == "bash" && arguments["command"] == "pwd"
            ));
        }
        other => panic!("unexpected streamed assistant: {other:?}"),
    }
    assert_eq!(
        text_messages(&thread.session_context().await.unwrap().messages),
        vec!["stream", "COOL"]
    );
    assert_eq!(client.requests()[0].messages.len(), 1);
}

#[tokio::test]
async fn checkpoint_resume_after_store_reopen_replays_canonical_context() {
    let path = temp_db_path("cooldis-provider-resume");
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let checkpoint = {
        let client = Arc::new(RecordingClient::with_responses(vec![response_text(
            "first reply",
        )]));
        let store = Arc::new(SqliteSessionStore::open(&path).unwrap());
        let host = RuntimeHost::with_session_store(factory(client), store);
        let thread = host
            .start_thread(coordinates.clone(), ThreadTopology::root())
            .await
            .unwrap();
        let mut events = thread.subscribe_events();

        host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
            .await
            .unwrap();
        assert_output(&mut events, "first reply").await;
        let checkpoint = host
            .create_checkpoint(
                thread.context().coordinates.thread_id,
                None,
                Some("after-first".to_string()),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        host.shutdown_thread(thread.context().coordinates.thread_id)
            .await
            .unwrap();
        checkpoint
    };

    let client = Arc::new(RecordingClient::with_responses(vec![response_text(
        "second reply",
    )]));
    let store = Arc::new(SqliteSessionStore::open(&path).unwrap());
    let host = RuntimeHost::with_session_store(factory(Arc::clone(&client)), store);
    let resumed = host
        .resume_thread_from_checkpoint(checkpoint.clone())
        .await
        .unwrap();
    let mut events = resumed.subscribe_events();

    host.submit(resumed.context().coordinates.thread_id, "turn-2", "second")
        .await
        .unwrap();
    assert_output(&mut events, "second reply").await;

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[0].messages),
        vec!["hello", "first reply", "second"]
    );
    let session = resumed.session_context().await.unwrap();
    assert_eq!(
        text_messages(&session.messages),
        vec!["hello", "first reply", "second", "second reply"]
    );
    assert_eq!(
        checkpoint.active_entry_id,
        Some(session.entries[2].entry_id)
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn context_compile_receipt_observation_survives_session_store_reopen() {
    let path = temp_db_path("cooldis-provider-context-receipt");
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    {
        let client = Arc::new(RecordingClient::with_responses(vec![response_text(
            "first reply",
        )]));
        let store = Arc::new(SqliteSessionStore::open(&path).unwrap());
        let host = RuntimeHost::with_session_store(factory(client), store);
        let thread = host
            .start_thread(coordinates.clone(), ThreadTopology::root())
            .await
            .unwrap();
        let mut events = thread.subscribe_events();

        host.submit(thread.context().coordinates.thread_id, "turn-1", "hello")
            .await
            .unwrap();
        assert_output(&mut events, "first reply").await;
        host.shutdown_thread(thread.context().coordinates.thread_id)
            .await
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let stream_id = EventStreamId::for_thread(&coordinates);
    let events = reopened.read_events(&stream_id, None).await.unwrap();
    let session_events = events
        .iter()
        .filter(|event| event.kind == EventKind::SessionEntryAppended)
        .collect::<Vec<_>>();
    let compile_events = events
        .iter()
        .filter(|event| event.kind == EventKind::ContextCompileCompleted)
        .collect::<Vec<_>>();
    assert_eq!(session_events.len(), 2, "{events:?}");
    assert_eq!(compile_events.len(), 1, "{events:?}");

    let observations = reopened
        .list_observations(&coordinates, Some("compiled_context_receipt"))
        .await
        .unwrap();
    assert_eq!(observations.len(), 1);
    let receipt = &observations[0];
    assert_eq!(receipt.payload["strategy"], "naive_assembly");
    assert_eq!(receipt.payload["message_count"], 1);
    assert_eq!(
        receipt.payload["replay_transform"]["dangling_tool_calls_dropped"],
        0
    );
    assert_eq!(
        receipt.payload["session_entry_ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        receipt.payload["output_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(
        receipt.provenance.source_event_ids,
        vec![compile_events[0].id]
    );
    assert_eq!(
        receipt
            .provenance
            .source_range
            .as_ref()
            .unwrap()
            .to_sequence,
        session_events[0].sequence
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn checkpoint_fork_diverges_from_parent_without_corrupting_active_leaves() {
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_text("root reply"),
        response_text("parent reply"),
        response_text("fork reply"),
    ]));
    let host = RuntimeHost::with_session_store(
        factory(Arc::clone(&client)),
        Arc::new(InMemorySessionStore::new()),
    );
    let parent = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut parent_events = parent.subscribe_events();

    host.submit(parent.context().coordinates.thread_id, "turn-1", "root")
        .await
        .unwrap();
    assert_output(&mut parent_events, "root reply").await;
    let checkpoint = host
        .create_checkpoint(
            parent.context().coordinates.thread_id,
            None,
            Some("branch".to_string()),
            BTreeMap::new(),
        )
        .await
        .unwrap();

    let fork = host
        .fork_thread_from_checkpoint(checkpoint.clone())
        .await
        .unwrap();
    let mut fork_events = fork.subscribe_events();
    host.submit(
        parent.context().coordinates.thread_id,
        "turn-parent",
        "parent next",
    )
    .await
    .unwrap();
    assert_output(&mut parent_events, "parent reply").await;
    host.submit(
        fork.context().coordinates.thread_id,
        "turn-fork",
        "fork next",
    )
    .await
    .unwrap();
    assert_output(&mut fork_events, "fork reply").await;

    assert_eq!(
        text_messages(&parent.session_context().await.unwrap().messages),
        vec!["root", "root reply", "parent next", "parent reply"]
    );
    assert_eq!(
        text_messages(&fork.session_context().await.unwrap().messages),
        vec!["root", "root reply", "fork next", "fork reply"]
    );
    assert_eq!(
        fork.context().parent_thread_id,
        Some(parent.context().coordinates.thread_id)
    );

    let requests = client.requests();
    assert_eq!(
        text_messages(&requests[1].messages),
        vec!["root", "root reply", "parent next"]
    );
    assert_eq!(
        text_messages(&requests[2].messages),
        vec!["root", "root reply", "fork next"]
    );
}

#[tokio::test]
async fn cancelling_provider_turn_does_not_store_cancelled_assistant_and_thread_recovers() {
    let client = Arc::new(ScriptedClient::new(vec![
        ScriptedResponse::Pending,
        ScriptedResponse::Response(response_text("after reply")),
    ]));
    let host = RuntimeHost::new(runtime_factory(client));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "slow")
        .await
        .unwrap();
    assert_user_mirror(&mut events, "slow").await;
    host.cancel(thread.context().coordinates.thread_id, "stop slow")
        .await
        .unwrap();
    assert_cancelled(&mut events, "stop slow").await;

    let session = thread.session_context().await.unwrap();
    assert_eq!(text_messages(&session.messages), vec!["slow"]);

    host.submit(thread.context().coordinates.thread_id, "turn-2", "after")
        .await
        .unwrap();
    assert_output(&mut events, "after reply").await;
    let session = thread.session_context().await.unwrap();
    assert_eq!(
        text_messages(&session.messages),
        vec!["slow", "after", "after reply"]
    );
}

#[tokio::test]
async fn active_submit_defaults_to_pending_user_queue() {
    let client = Arc::new(ScriptedClient::new(vec![
        ScriptedResponse::Pending,
        ScriptedResponse::Response(response_text("queued reply")),
    ]));
    let host = RuntimeHost::new(runtime_factory(client));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "session_1"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "slow")
        .await
        .unwrap();
    assert_user_mirror(&mut events, "slow").await;
    host.submit(
        thread.context().coordinates.thread_id,
        "turn-2",
        "queued input",
    )
    .await
    .unwrap();

    let signal = assert_signal(&mut events, crate::ThreadSignalKind::UserQueue).await;
    assert_eq!(
        signal.metadata.get("turn_id").map(String::as_str),
        Some("turn-2")
    );

    host.cancel(thread.context().coordinates.thread_id, "release slow")
        .await
        .unwrap();
    assert_cancelled(&mut events, "release slow").await;
    assert_user_mirror(&mut events, "queued input").await;
    assert_output(&mut events, "queued reply").await;

    let session = thread.session_context().await.unwrap();
    assert_eq!(
        text_messages(&session.messages),
        vec!["slow", "queued input", "queued reply"]
    );
}

async fn echo_registry(name: &str) -> Arc<OperationRegistry> {
    named_echo_registry(name, "search").await
}

async fn named_echo_registry(name: &str, operation_name: &str) -> Arc<OperationRegistry> {
    let registry = Arc::new(OperationRegistry::new());
    let wasm = wat::parse_str(echo_operation_guest("echo", operation_name))
        .expect("echo operation fixture should compile");
    registry
        .register(OperationRegistration::new(
            name,
            WasmRuntimeArtifact::bytes(wasm),
        ))
        .await
        .unwrap();
    registry
}

fn echo_operation_guest(prefix: &str, operation_name: &str) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": operation_name,
            "input": "bytes",
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": []
        }]
    })
    .to_string();
    let prefix = format!("{prefix}:");
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "{prefix}")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                (local $n i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const 2
                  return
                end
                i32.const 0
                i32.const 1024
                i32.store
                local.get $source
                i32.const 1024
                i32.const 0
                call $source_read
                drop
                i32.const 0
                i32.load
                local.set $n
                i32.const 0
                i32.const {prefix_len}
                i32.store
                local.get $output
                i32.const 8192
                i32.const 0
                call $sink_write
                drop
                i32.const 0
                local.get $n
                i32.store
                local.get $output
                i32.const 1024
                i32.const 0
                call $sink_write
                drop
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
        prefix = wat_bytes(prefix.as_bytes()),
        prefix_len = prefix.len(),
    )
}

fn wat_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| match byte {
            b'\n' => "\\0a".to_string(),
            b'\r' => "\\0d".to_string(),
            b'\t' => "\\09".to_string(),
            b'"' => "\\22".to_string(),
            b'\\' => "\\5c".to_string(),
            0x20..=0x7e => (*byte as char).to_string(),
            _ => format!("\\{byte:02x}"),
        })
        .collect()
}

fn is_canonical_message_entry(entry: &SessionEntry) -> bool {
    matches!(entry.kind, SessionEntryKind::Message { .. })
}

fn temp_db_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}.sqlite3"))
}

async fn assert_output(events: &mut broadcast::Receiver<ThreadEvent>, expected: &str) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Output { text, .. } = event {
            assert_eq!(text, expected);
            return;
        }
    }
}

async fn assert_stopped(events: &mut broadcast::Receiver<ThreadEvent>) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            ThreadEvent::Stopped { .. } => return,
            ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
            _ => {}
        }
    }
}

async fn assert_output_with_runtime_events(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected: &str,
) -> Vec<RuntimeEventKind> {
    let mut runtime_events = Vec::new();
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            ThreadEvent::Runtime { event, .. } => runtime_events.push(event.kind),
            ThreadEvent::Output { text, .. } => {
                assert_eq!(text, expected);
                return runtime_events;
            }
            ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
            _ => {}
        }
    }
}

async fn assert_completed_terminal(events: &mut broadcast::Receiver<ThreadEvent>) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            ThreadEvent::Runtime { event, .. }
                if matches!(
                    event.kind,
                    RuntimeEventKind::Terminal {
                        state: RuntimeTerminalState::Completed,
                    }
                ) =>
            {
                return;
            }
            ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
            _ => {}
        }
    }
}

fn assert_mutated_fields(payload: &Value, expected: &[&str]) {
    let fields = payload["mutated_fields"]
        .as_array()
        .expect("mutated_fields should be an array")
        .iter()
        .map(|field| field.as_str().expect("mutated field should be a string"))
        .collect::<Vec<_>>();
    assert_eq!(fields, expected);
}

fn assert_payload_omits_values(payload: &Value, forbidden_values: &[&str]) {
    let encoded = serde_json::to_string(payload).unwrap();
    for forbidden in forbidden_values {
        assert!(
            !encoded.contains(forbidden),
            "witness payload leaked forbidden value {forbidden:?}: {encoded}"
        );
    }
}

async fn assert_failed_with_runtime_events(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_message_fragment: &str,
) -> Vec<RuntimeEventKind> {
    let mut runtime_events = Vec::new();
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            ThreadEvent::Runtime { event, .. } => runtime_events.push(event.kind),
            ThreadEvent::Failed { message, .. } => {
                assert!(
                    message.contains(expected_message_fragment),
                    "failure message {message:?} did not contain {expected_message_fragment:?}"
                );
                return runtime_events;
            }
            _ => {}
        }
    }
}

async fn assert_compaction(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected_trigger: CompactionTrigger,
    expected_summary: &str,
) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            ThreadEvent::Runtime {
                event:
                    RuntimeEvent {
                        kind: RuntimeEventKind::Compaction { trigger, summary },
                        ..
                    },
                ..
            } => {
                assert_eq!(trigger, expected_trigger);
                assert_eq!(summary, expected_summary);
                return;
            }
            ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
            _ => {}
        }
    }
}

async fn assert_user_mirror(events: &mut broadcast::Receiver<ThreadEvent>, expected: &str) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::CanonicalMirror { entry, .. } = event
            && let SessionEntryKind::Message {
                message: CanonicalMessage::User { content, .. },
            } = entry.kind
        {
            let text = content
                .iter()
                .find_map(|content| match content {
                    CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or_default();
            assert_eq!(text, expected);
            return;
        }
    }
}

async fn assert_assistant_mirror(
    events: &mut broadcast::Receiver<ThreadEvent>,
) -> CanonicalMessage {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::CanonicalMirror { entry, .. } = event {
            if let SessionEntryKind::Message { message } = entry.kind {
                if matches!(message, CanonicalMessage::Assistant { .. }) {
                    return message;
                }
            }
        }
    }
}

async fn assert_assistant_with_runtime_events(
    events: &mut broadcast::Receiver<ThreadEvent>,
) -> (CanonicalMessage, Vec<RuntimeEventKind>) {
    let mut runtime_events = Vec::new();
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        match event {
            ThreadEvent::Runtime { event, .. } => runtime_events.push(event.kind),
            ThreadEvent::CanonicalMirror { entry, .. } => {
                if let SessionEntryKind::Message { message } = entry.kind
                    && matches!(message, CanonicalMessage::Assistant { .. })
                {
                    return (message, runtime_events);
                }
            }
            ThreadEvent::Failed { message, .. } => panic!("thread failed: {message}"),
            _ => {}
        }
    }
}

async fn assert_cancelled(events: &mut broadcast::Receiver<ThreadEvent>, expected: &str) {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Cancelled { reason, .. } = event {
            assert_eq!(reason, expected);
            return;
        }
    }
}

async fn assert_signal(
    events: &mut broadcast::Receiver<ThreadEvent>,
    expected: crate::ThreadSignalKind,
) -> crate::ThreadSignal {
    loop {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("event timed out")
            .expect("event channel closed");
        if let ThreadEvent::Signal { signal, .. } = event
            && signal.kind == expected
        {
            return signal;
        }
    }
}
