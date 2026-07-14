use super::*;
use crate::EventKind;
use crate::test_support::{FaultingProviderClient, FaultingRuntimeStore};
use crate::{
    AgentKernelToolCall, AgentKernelToolProvider, AgentManifestBindReceipt,
    AgentManifestCouplingBinding, AgentManifestCouplingBudget, AgentManifestRuntimeDefaults,
    AgentToolRouter, CanonicalStopReason, CanonicalUsage, CommandHookHandler, CouplingRole,
    EventProvenance, EventRecord, EventSequence, EventStore, EventStreamId, HistoryResult,
    HookEventName, HookHandler, HookHandlerOutput, HookHandlerSpec, HookRequest, HookRunStatus,
    InMemorySessionStore, KernelOperationRegistration, KernelThreadSpawnAgentBinding,
    KernelThreadSpawnAgentResolver, NewEventRecord, NewObservationRecord, ObservationRecord,
    ObservationStore, OperationRegistration, OperationRegistry, OperationToolAlias,
    ProviderCapabilityRecord, ProviderContextPolicy, RuntimeEvent, RuntimeExecutionPolicy,
    RuntimeHost, SessionContext, SessionEntry, SessionEntryId, SessionEntryKind, SessionStore,
    SqliteSessionStore, THREAD_SPAWN_OPERATION, ThreadBaseRef, ThreadCoordinates,
    ThreadJoinedPayload, ThreadSpawnedPayload, ThreadTerminalState, ThreadTopology,
    ToolCallDecisionOutcomePayload, ToolCallDecisionPayload, ToolCallSubject,
    ToolCallSuspendedPayload, ToolInvocationCancellation, TurnContextSnapshot, WasmRuntimeArtifact,
    cooldis_threads_kernel_package,
};
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
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

struct FinishSecondFirstToolProvider {
    second_finished: Notify,
}

struct SerialBlockingToolProvider {
    tool_name: &'static str,
    started: mpsc::UnboundedSender<String>,
    release_first: Notify,
}

struct CancellationAcknowledgingThreadToolProvider {
    started: mpsc::UnboundedSender<String>,
    acknowledged: mpsc::UnboundedSender<String>,
}

struct NonObservingThreadToolProvider {
    started: mpsc::UnboundedSender<String>,
    released: AtomicBool,
    release: Notify,
    never_launched: AtomicBool,
}

struct PanickingAfterGraceToolProvider {
    started: mpsc::UnboundedSender<()>,
    release: Notify,
}

impl NonObservingThreadToolProvider {
    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

struct ImmediateThreadToolProvider;

struct IsolatedFailureToolProvider;

#[derive(Default)]
struct AppendPause {
    entered: AtomicBool,
    entered_notify: Notify,
    released: AtomicBool,
    release_notify: Notify,
}

impl AppendPause {
    async fn arrive_and_wait(&self) {
        self.entered.store(true, Ordering::SeqCst);
        self.entered_notify.notify_waiters();
        while !self.released.load(Ordering::SeqCst) {
            self.release_notify.notified().await;
        }
    }

    async fn wait_until_entered(&self) {
        while !self.entered.load(Ordering::SeqCst) {
            self.entered_notify.notified().await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

#[derive(Clone)]
struct PausingRuntimeStore {
    inner: InMemorySessionStore,
    pause_kind: EventKind,
    pause_once: Arc<AtomicBool>,
    pause: Arc<AppendPause>,
}

impl PausingRuntimeStore {
    fn after_first_append_of(pause_kind: EventKind) -> Self {
        Self {
            inner: InMemorySessionStore::new(),
            pause_kind,
            pause_once: Arc::new(AtomicBool::new(true)),
            pause: Arc::new(AppendPause::default()),
        }
    }
}

#[async_trait]
impl SessionStore for PausingRuntimeStore {
    async fn append(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.inner.append(coordinates, parent_entry_id, kind).await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &ThreadCoordinates,
        parent_entry_id: Option<SessionEntryId>,
        kind: SessionEntryKind,
        provenance: EventProvenance,
    ) -> HistoryResult<SessionEntry> {
        self.inner
            .append_with_provenance(coordinates, parent_entry_id, kind, provenance)
            .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &ThreadCoordinates,
        turn_id: &str,
        kind: SessionEntryKind,
    ) -> HistoryResult<SessionEntry> {
        self.inner
            .append_turn_input(coordinates, turn_id, kind)
            .await
    }

    async fn active_leaf(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        self.inner.active_leaf(coordinates).await
    }

    async fn select_branch(
        &self,
        coordinates: &ThreadCoordinates,
        leaf_entry_id: Option<SessionEntryId>,
    ) -> HistoryResult<()> {
        self.inner.select_branch(coordinates, leaf_entry_id).await
    }

    async fn build_context(
        &self,
        coordinates: &ThreadCoordinates,
    ) -> HistoryResult<SessionContext> {
        self.inner.build_context(coordinates).await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &ThreadCoordinates,
        source_leaf: Option<SessionEntryId>,
        target_coordinates: &ThreadCoordinates,
    ) -> HistoryResult<Option<SessionEntryId>> {
        self.inner
            .clone_branch(source_coordinates, source_leaf, target_coordinates)
            .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &ThreadCoordinates,
        target_coordinates: &ThreadCoordinates,
        base: ThreadBaseRef,
    ) -> HistoryResult<()> {
        self.inner
            .fork_by_reference(source_coordinates, target_coordinates, base)
            .await
    }
}

#[async_trait]
impl EventStore for PausingRuntimeStore {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let should_pause = records.iter().any(|record| record.kind == self.pause_kind)
            && self.pause_once.swap(false, Ordering::SeqCst);
        let appended = self.inner.append_events(stream_id, records).await?;
        if should_pause {
            self.pause.arrive_and_wait().await;
        }
        Ok(appended)
    }

    async fn append_events_fenced(
        &self,
        stream_id: &EventStreamId,
        expected_next_sequence: EventSequence,
        records: Vec<NewEventRecord>,
    ) -> HistoryResult<Vec<EventRecord>> {
        let should_pause = records.iter().any(|record| record.kind == self.pause_kind)
            && self.pause_once.swap(false, Ordering::SeqCst);
        let appended = self
            .inner
            .append_events_fenced(stream_id, expected_next_sequence, records)
            .await?;
        if should_pause {
            self.pause.arrive_and_wait().await;
        }
        Ok(appended)
    }

    async fn read_events(
        &self,
        stream_id: &EventStreamId,
        from_sequence: Option<EventSequence>,
    ) -> HistoryResult<Vec<EventRecord>> {
        self.inner.read_events(stream_id, from_sequence).await
    }
}

#[async_trait]
// lexicon-allow: observation_store - deterministic test store implements the existing history trait.
impl ObservationStore for PausingRuntimeStore {
    async fn append_observation(
        &self,
        record: NewObservationRecord,
    ) -> HistoryResult<ObservationRecord> {
        self.inner.append_observation(record).await
    }

    async fn list_observations(
        &self,
        scope: &ThreadCoordinates,
        kind: Option<&str>,
    ) -> HistoryResult<Vec<ObservationRecord>> {
        self.inner.list_observations(scope, kind).await
    }
}

struct StaticThreadSpawnAgentResolver;

const CHILD_AGENT_REF: &str = "agent://worker@latest";
const CHILD_MANIFEST_HASH: &str = "sha256:child-manifest";

#[async_trait]
impl KernelThreadSpawnAgentResolver for StaticThreadSpawnAgentResolver {
    fn default_agent_ref(&self, _caller: &ThreadContext) -> Option<String> {
        Some(CHILD_AGENT_REF.to_string())
    }

    async fn resolve_agent_ref(
        &self,
        _caller: &ThreadContext,
        agent_ref: &str,
    ) -> CooldisResult<KernelThreadSpawnAgentBinding> {
        assert_eq!(agent_ref, CHILD_AGENT_REF);
        Ok(KernelThreadSpawnAgentBinding {
            metadata: BTreeMap::from([(
                "cooldis.agent.manifest_hash".to_string(),
                CHILD_MANIFEST_HASH.to_string(),
            )]),
            compile_receipt: serde_json::json!({
                "ref_uri": CHILD_AGENT_REF,
                "manifest_hash": CHILD_MANIFEST_HASH,
                "source_hash": "sha256:child-source"
            }),
            bind_receipt: serde_json::to_value(AgentManifestBindReceipt {
                ref_uri: CHILD_AGENT_REF.to_string(),
                manifest_hash: CHILD_MANIFEST_HASH.to_string(),
                model_profile_id: "default".to_string(),
                provider_id: "test".to_string(),
                model_id: "model".to_string(),
                tool_ids: Vec::new(),
                operation_bindings: Vec::new(),
                tool_universes: Vec::new(),
                couplings: Vec::new(),
                skill_packages: Vec::new(),
                skill_discovery: None,
                static_context_segments: Vec::new(),
                granted: vec!["threads.read".to_string()],
                effective_runtime: AgentManifestRuntimeDefaults::default(),
                overridden_keys: Vec::new(),
                placement: None,
                workspace: None,
            })
            .unwrap(),
        })
    }
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

#[async_trait]
impl AgentKernelToolProvider for FinishSecondFirstToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "thread_submit",
            "Deterministic hold-scheduler test tool.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        match call.arguments["slot"].as_str() {
            Some("first") => self.second_finished.notified().await,
            Some("second") => self.second_finished.notify_one(),
            other => panic!("unexpected finish-order slot: {other:?}"),
        }
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            call.arguments["slot"].as_str().unwrap(),
            false,
        )))
    }
}

#[async_trait]
impl AgentKernelToolProvider for SerialBlockingToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            self.tool_name,
            "Deterministic serialization test tool.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        let slot = call.arguments["slot"].as_str().unwrap().to_string();
        self.started.send(slot.clone()).unwrap();
        if slot == "first" {
            self.release_first.notified().await;
        }
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            slot,
            false,
        )))
    }
}

#[async_trait]
impl AgentKernelToolProvider for CancellationAcknowledgingThreadToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "thread_submit",
            "Cancellation-aware interruption test tool.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        _call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        panic!("the interruption test must use the cancellable provider surface")
    }

    async fn invoke_tool_call_cancellable(
        &self,
        call: AgentKernelToolCall,
        cancellation: ToolInvocationCancellation,
    ) -> CooldisResult<crate::AgentKernelToolOutcome> {
        self.started.send(call.call_id.clone()).unwrap();
        cancellation.token().cancelled().await;
        self.acknowledged.send(call.call_id.clone()).unwrap();
        Ok(crate::AgentKernelToolOutcome::Completed(Some(
            CanonicalMessage::tool_result(
                call.call_id,
                call.tool_name,
                "interrupt acknowledged",
                true,
            ),
        )))
    }
}

#[async_trait]
impl AgentKernelToolProvider for NonObservingThreadToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        ["thread_status", "thread_wait"]
            .into_iter()
            .map(|name| {
                ToolDefinition::new(
                    name,
                    "Default-implementation interruption test tool.",
                    serde_json::json!({"type": "object"}),
                )
            })
            .collect()
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        if call.tool_name == "thread_wait" {
            self.never_launched.store(false, Ordering::SeqCst);
        }
        self.started.send(call.call_id.clone()).unwrap();
        while !self.released.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "finished without observing cancellation",
            false,
        )))
    }
}

#[async_trait]
impl AgentKernelToolProvider for PanickingAfterGraceToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "thread_status",
            "Panics after the cancellation monitor abandons it.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        _call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        self.started.send(()).unwrap();
        self.release.notified().await;
        panic!("panic after grace")
    }
}

#[async_trait]
impl AgentKernelToolProvider for ImmediateThreadToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        ["thread_submit", "thread_status"]
            .into_iter()
            .map(|name| {
                ToolDefinition::new(
                    name,
                    "Immediate suspension-batch test tool.",
                    serde_json::json!({"type": "object"}),
                )
            })
            .collect()
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name.clone(),
            format!("{} completed", call.tool_name),
            false,
        )))
    }
}

#[async_trait]
impl AgentKernelToolProvider for IsolatedFailureToolProvider {
    async fn tool_definitions(&self) -> Vec<ToolDefinition> {
        vec![ToolDefinition::new(
            "thread_submit",
            "Per-call failure isolation test tool.",
            serde_json::json!({"type": "object"}),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: AgentKernelToolCall,
    ) -> CooldisResult<Option<CanonicalMessage>> {
        if call.arguments["fail"].as_bool() == Some(true) {
            return Err(CooldisError::RuntimeExecution(
                "expected call failure".to_string(),
            ));
        }
        Ok(Some(CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "sibling completed",
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

fn response_tool_calls(calls: Vec<(&str, &str, Value)>) -> crate::ProviderResponse {
    crate::ProviderResponse {
        content: calls
            .into_iter()
            .map(|(call_id, name, arguments)| CanonicalContent::tool_call(call_id, name, arguments))
            .collect(),
        usage: CanonicalUsage::default(),
        stop_reason: CanonicalStopReason::ToolUse,
    }
}

fn tool_round_responses(rounds: usize) -> Vec<crate::ProviderResponse> {
    let mut responses = (0..rounds)
        .map(|round| {
            response_tool_call_named_with_id(
                &format!("call-{round}"),
                "echo_search",
                serde_json::json!({"input": format!("round-{round}")}),
            )
        })
        .collect::<Vec<_>>();
    responses.push(response_text("final reply"));
    responses
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
        let thread_context = context.clone();
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
                        Some(ThreadCommand::Submit { turn_id, input, .. }) => {
                            let _ = status.send(ThreadStatus::Running);
                            let _ = services.append_user_turn_input(&coordinates, &turn_id, &input).await;
                            let _ = events.send(ThreadEvent::Output {
                                thread_id,
                                text: format!("child:{}", input.text_projection()),
                            });
                            if let Ok(completed) = services
                                .append_thread_event(
                                    &coordinates,
                                    NewEventRecord::discharged(
                                        coordinates.clone(),
                                        EventKind::TurnCompleted,
                                        serde_json::json!({
                                            "turn_id": turn_id,
                                        }),
                                        EventProvenance {
                                            source_streams: vec![EventStreamId::for_thread(&coordinates)],
                                            discharged_by: Some("runtime:child-echo".to_string()),
                                            function: Some("turn_complete/v1".to_string()),
                                            ..EventProvenance::default()
                                        },
                                    ),
                                )
                                .await
                            {
                                let _ = services
                                    .append_thread_joined_event_if_spawned(
                                        &thread_context,
                                        ThreadTerminalState::Completed,
                                        None,
                                        Some(completed.id),
                                    )
                                    .await;
                            }
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn model_request_retries_retryable_provider_error() {
    let inner = Arc::new(RecordingClient::with_responses(vec![response_text(
        "retry reply",
    )]));
    let client = Arc::new(
        FaultingProviderClient::new(inner.clone())
            .fail_nth_http("complete", 1, "temporary outage")
            .delay_nth("complete", 2, Duration::from_millis(25)),
    );
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let factory = Arc::new(
        CanonicalProviderRuntimeFactory::new(config, client.clone())
            .with_model_request_retry_policy(ModelRequestRetryPolicy::fixed(2, 50)),
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

    assert_eq!(client.call_count("complete"), 2);
    assert_eq!(inner.requests().len(), 1);
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
                delay_ms: 50,
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
async fn compaction_reattaches_a_late_tool_result_before_the_replacement_user() {
    let client = Arc::new(RecordingClient::with_responses(vec![response_text(
        "summary after late result",
    )]));
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "compact-late-result");
    let store = Arc::new(InMemorySessionStore::new());
    let first_user = store
        .append_turn_input(
            &coordinates,
            "turn-old",
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("first turn"),
            },
        )
        .await
        .unwrap();
    let assistant = store
        .append(
            &coordinates,
            Some(first_user.entry_id),
            SessionEntryKind::Message {
                message: CanonicalMessage::assistant(
                    "openai",
                    ProviderApi::OpenAIResponses,
                    "gpt-test",
                    vec![CanonicalContent::tool_call(
                        "call-late",
                        "lookup",
                        serde_json::json!({"q": "slow"}),
                    )],
                    CanonicalStopReason::ToolUse,
                ),
            },
        )
        .await
        .unwrap();
    let replacement_user = store
        .append_turn_input(
            &coordinates,
            "turn-new",
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("replacement turn"),
            },
        )
        .await
        .unwrap();
    store
        .append(
            &coordinates,
            Some(replacement_user.entry_id),
            SessionEntryKind::Message {
                message: CanonicalMessage::tool_result(
                    "call-late",
                    "lookup",
                    "settled after cancellation",
                    true,
                ),
            },
        )
        .await
        .unwrap();
    assert_eq!(assistant.parent_entry_id, Some(first_user.entry_id));
    let host = RuntimeHost::with_session_store(
        Arc::new(CanonicalProviderRuntimeFactory::new(
            CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test"),
            client.clone(),
        )),
        store,
    );
    let thread = host
        .start_thread(coordinates, ThreadTopology::root())
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.compact_thread(thread.context().coordinates.thread_id, "compact-1", None)
        .await
        .unwrap();
    assert_compaction(
        &mut events,
        CompactionTrigger::Manual,
        "summary after late result",
    )
    .await;

    let request = client.requests().pop().unwrap();
    assert!(matches!(
        request.messages.as_slice(),
        [
            CanonicalMessage::User { .. },
            CanonicalMessage::Assistant { .. },
            CanonicalMessage::ToolResult { tool_call_id, .. },
            CanonicalMessage::User { .. },
        ] if tool_call_id == "call-late"
    ));
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
async fn default_tool_round_budget_still_fails_after_eight_completed_batches() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(tool_round_responses(9)));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(runtime_factory_with_registry(provider_client, registry));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "round-default"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "loop")
        .await
        .unwrap();
    assert_failed_with_runtime_events(&mut events, "tool router exceeded 8 rounds").await;
    assert_eq!(client.requests().len(), 9);
}

#[tokio::test]
async fn manifest_round_budget_of_sixty_four_allows_nine_tool_batches() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(tool_round_responses(9)));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(runtime_factory_with_registry(provider_client, registry));
    let thread = host
        .start_thread_with_topology_and_metadata(
            ThreadCoordinates::new("tenant_a", "user_1", "round-64"),
            ThreadTopology::root(),
            BTreeMap::from([(
                THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA.to_string(),
                "64".to_string(),
            )]),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "loop")
        .await
        .unwrap();
    assert_output(&mut events, "final reply").await;
    assert_eq!(client.requests().len(), 10);
}

#[tokio::test]
async fn explicit_unlimited_manifest_round_budget_allows_more_than_the_default() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(tool_round_responses(12)));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let host = RuntimeHost::new(runtime_factory_with_registry(provider_client, registry));
    let thread = host
        .start_thread_with_topology_and_metadata(
            ThreadCoordinates::new("tenant_a", "user_1", "round-unlimited"),
            ThreadTopology::root(),
            BTreeMap::from([(
                THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA.to_string(),
                "unlimited".to_string(),
            )]),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "loop")
        .await
        .unwrap();
    assert_output(&mut events, "final reply").await;
    assert_eq!(client.requests().len(), 13);
}

#[tokio::test]
async fn persisted_round_accounting_rejects_a_request_without_an_assistant_source() {
    let store = Arc::new(InMemorySessionStore::new());
    let services = RuntimeServices::new(store.clone(), RuntimeExecutionPolicy::default());
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "round-provenance");
    let malformed_request = || {
        NewEventRecord::discharged(
            coordinates.clone(),
            EventKind::ToolCallRequested,
            serde_json::to_value(ToolCallRequestedPayload {
                subject: ToolCallSubject {
                    turn_id: "turn-1".to_string(),
                    call_id: "call-1".to_string(),
                },
                snapshot_id: "unbound".to_string(),
                tool_name: "thread_status".to_string(),
                arguments: serde_json::json!({"task_name": "worker-a"}),
                holds: Vec::new(),
            })
            .unwrap(),
            EventProvenance {
                source_streams: vec![EventStreamId::for_thread(&coordinates)],
                discharged_by: Some("test:malformed-round".to_string()),
                function: Some("tool_request/v1".to_string()),
                ..EventProvenance::default()
            },
        )
    };
    store
        .append_events(
            &EventStreamId::for_thread(&coordinates),
            vec![malformed_request()],
        )
        .await
        .unwrap();
    let turn_submitted = store
        .append_events(
            &EventStreamId::for_thread(&coordinates),
            vec![NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": "turn-1"}),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

    assert_eq!(
        persisted_tool_rounds_for_turn(&services, &coordinates, "turn-1", turn_submitted.sequence,)
            .await
            .unwrap(),
        0,
        "malformed events before the active turn bound must not affect accounting"
    );
    store
        .append_events(
            &EventStreamId::for_thread(&coordinates),
            vec![malformed_request()],
        )
        .await
        .unwrap();

    let err =
        persisted_tool_rounds_for_turn(&services, &coordinates, "turn-1", turn_submitted.sequence)
            .await
            .unwrap_err();
    assert!(err.to_string().contains("has no assistant source event"));
}

#[tokio::test]
async fn persisted_round_accounting_rejects_a_cross_turn_assistant_source() {
    let store = Arc::new(InMemorySessionStore::new());
    let services = RuntimeServices::new(store.clone(), RuntimeExecutionPolicy::default());
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "round-cross-turn");
    let old_assistant = store
        .append_events(
            &EventStreamId::for_thread(&coordinates),
            vec![NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::SessionEntryAppended,
                serde_json::json!({
                    "entry_id": SessionEntryId::new().to_string(),
                    "entry_kind": "message",
                }),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let turn_submitted = store
        .append_events(
            &EventStreamId::for_thread(&coordinates),
            vec![NewEventRecord::witnessed(
                coordinates.clone(),
                EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": "turn-1"}),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    store
        .append_events(
            &EventStreamId::for_thread(&coordinates),
            vec![NewEventRecord::discharged(
                coordinates.clone(),
                EventKind::ToolCallRequested,
                serde_json::to_value(ToolCallRequestedPayload {
                    subject: ToolCallSubject {
                        turn_id: "turn-1".to_string(),
                        call_id: "call-1".to_string(),
                    },
                    snapshot_id: "unbound".to_string(),
                    tool_name: "thread_status".to_string(),
                    arguments: serde_json::json!({"task_name": "worker-a"}),
                    holds: Vec::new(),
                })
                .unwrap(),
                EventProvenance {
                    source_streams: vec![EventStreamId::for_thread(&coordinates)],
                    source_event_ids: vec![old_assistant.id],
                    discharged_by: Some("test:cross-turn-round".to_string()),
                    function: Some("tool_request/v1".to_string()),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();

    let err =
        persisted_tool_rounds_for_turn(&services, &coordinates, "turn-1", turn_submitted.sequence)
            .await
            .unwrap_err();
    assert!(err.to_string().contains("outside the active turn"));
}

#[tokio::test]
async fn independent_thread_holds_overlap_results_append_in_call_order_and_finish_is_witnessed() {
    let tool_provider = Arc::new(FinishSecondFirstToolProvider {
        second_finished: Notify::new(),
    });
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_calls(vec![
            (
                "call-first",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a", "slot": "first"}),
            ),
            (
                "call-second",
                "thread_submit",
                serde_json::json!({"task_name": "worker-b", "slot": "second"}),
            ),
        ]),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client;
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(config, provider_client).with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "hold-overlap"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "parallel")
        .await
        .unwrap();
    assert_output(&mut events, "final reply").await;

    let session = thread.session_context().await.unwrap();
    let result_ids = session
        .messages
        .iter()
        .filter_map(|message| match message {
            CanonicalMessage::ToolResult { tool_call_id, .. } => Some(tool_call_id.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(result_ids, vec!["call-first", "call-second"]);

    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let requests = records
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallRequested)
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].payload["holds"],
        serde_json::json!([
            {
                "key": {"kind": "kernel_thread", "task_name": "worker-a"},
                "access": "exclusive"
            },
            {"key": {"kind": "global"}, "access": "shared"}
        ])
    );
    let completed = records
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[0].payload["subject"]["call_id"], "call-first");
    assert_eq!(completed[0].payload["finish_order"], 1);
    assert_eq!(completed[1].payload["subject"]["call_id"], "call-second");
    assert_eq!(completed[1].payload["finish_order"], 0);
}

#[tokio::test]
async fn duplicate_model_tool_call_ids_fail_before_the_batch_is_witnessed() {
    let tool_provider: Arc<dyn AgentKernelToolProvider> = Arc::new(ImmediateThreadToolProvider);
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![response_tool_calls(
        vec![
            (
                "duplicate-call",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a"}),
            ),
            (
                "duplicate-call",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a"}),
            ),
        ],
    )]));
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                client,
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "duplicate-tool-call-id"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "duplicate ids",
    )
    .await
    .unwrap();
    assert_failed_with_runtime_events(&mut events, "duplicate tool call id \"duplicate-call\"")
        .await;

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
            .all(|event| event.kind != EventKind::ToolCallRequested),
        "an ambiguous batch must fail before request ids become durable"
    );
}

#[tokio::test]
async fn cancellation_waits_for_buffered_call_order_commit_to_finish() {
    let tool_provider: Arc<dyn AgentKernelToolProvider> = Arc::new(ImmediateThreadToolProvider);
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![response_tool_calls(
        vec![
            (
                "call-first",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a"}),
            ),
            (
                "call-second",
                "thread_submit",
                serde_json::json!({"task_name": "worker-b"}),
            ),
        ],
    )]));
    let store = Arc::new(PausingRuntimeStore::after_first_append_of(
        EventKind::ToolCallCompleted,
    ));
    let pause = Arc::clone(&store.pause);
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                client,
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "cancel-during-tool-commit"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "commit both results",
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(2), pause.wait_until_entered())
        .await
        .expect("first completion append did not reach the pause");

    host.cancel(
        thread.context().coordinates.thread_id,
        "cancel during commit",
    )
    .await
    .unwrap();
    assert!(
        timeout(Duration::from_millis(100), async {
            loop {
                if let ThreadEvent::Cancelled { .. } = events.recv().await.unwrap() {
                    return;
                }
            }
        })
        .await
        .is_err(),
        "terminal cancellation must not overtake the buffered result commit"
    );

    pause.release();
    assert_cancelled(&mut events, "cancel during commit").await;
    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let completed = records
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
        .map(|event| event.payload["subject"]["call_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(completed, vec!["call-first", "call-second"]);
}

#[tokio::test]
async fn cancellation_racing_suspended_turn_commit_observes_the_full_boundary() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("echo_search", serde_json::json!({"input": "cooldis"})),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client;
    let store = Arc::new(PausingRuntimeStore::after_first_append_of(
        EventKind::TurnWaiting,
    ));
    let pause = Arc::clone(&store.pause);
    let host = RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "cancel-during-tool-wait"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    append_tool_controller_bind_receipt(&store.inner, &thread.context().coordinates, "echo_search")
        .await;
    append_witnessed_tool_suspension(
        &store.inner,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call_1|fc_1",
        "approval-1",
    )
    .await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "wait")
        .await
        .unwrap();
    timeout(Duration::from_secs(2), pause.wait_until_entered())
        .await
        .expect("turn.waiting append did not reach the pause");
    host.cancel(
        thread.context().coordinates.thread_id,
        "cancel during suspended commit",
    )
    .await
    .unwrap();
    assert!(
        timeout(Duration::from_millis(100), async {
            loop {
                if let ThreadEvent::Cancelled { .. } = events.recv().await.unwrap() {
                    return;
                }
            }
        })
        .await
        .is_err(),
        "terminal cancellation must not overtake the suspended boundary commit"
    );

    pause.release();
    assert_cancelled(&mut events, "cancel during suspended commit").await;
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
    assert_eq!(
        control_records
            .iter()
            .filter(|event| event.kind == EventKind::TurnWaiting)
            .count(),
        1
    );
    let thread_records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert!(
        thread_records
            .iter()
            .all(|event| event.kind != EventKind::ToolCallCompleted)
    );
}

#[tokio::test]
async fn cancellation_during_atomic_request_append_leaves_all_or_no_batch_witnesses() {
    let tool_provider: Arc<dyn AgentKernelToolProvider> = Arc::new(ImmediateThreadToolProvider);
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![response_tool_calls(
        vec![
            (
                "call-first",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a"}),
            ),
            (
                "call-second",
                "thread_submit",
                serde_json::json!({"task_name": "worker-b"}),
            ),
        ],
    )]));
    let store = Arc::new(PausingRuntimeStore::after_first_append_of(
        EventKind::ToolCallRequested,
    ));
    let pause = Arc::clone(&store.pause);
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                client,
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "cancel-during-request-append"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "cancel request append",
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(2), pause.wait_until_entered())
        .await
        .expect("request batch append did not reach the pause");
    host.cancel(
        thread.context().coordinates.thread_id,
        "cancel request append",
    )
    .await
    .unwrap();
    pause.release();
    assert_cancelled(&mut events, "cancel request append").await;

    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallRequested)
            .count(),
        2
    );
    let completed = records
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
        .map(|event| {
            serde_json::from_value::<ToolCallCompletedPayload>(event.payload.clone()).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(completed.len(), 2);
    assert!(completed.iter().all(|payload| {
        !payload.success
            && payload.cancellation == Some(ToolCallCancellation::CancelledAcknowledged)
    }));
}

#[tokio::test]
async fn conflicting_thread_holds_serialize_in_model_call_order() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let tool_provider = Arc::new(SerialBlockingToolProvider {
        tool_name: "thread_submit",
        started: started_tx,
        release_first: Notify::new(),
    });
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider.clone()),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_calls(vec![
            (
                "call-first",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a", "slot": "first"}),
            ),
            (
                "call-second",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a", "slot": "second"}),
            ),
        ]),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client;
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let host = RuntimeHost::new(Arc::new(
        CanonicalProviderRuntimeFactory::new(config, provider_client).with_tool_router(router),
    ));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "hold-serialize"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "serialize",
    )
    .await
    .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "first"
    );
    assert!(started_rx.try_recv().is_err());
    tool_provider.release_first.notify_one();
    assert_eq!(
        timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "second"
    );
    assert_output(&mut events, "final reply").await;
}

#[tokio::test]
async fn bash_family_holds_prevent_interleaving_before_the_harness_mutex() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let tool_provider = Arc::new(SerialBlockingToolProvider {
        tool_name: "bash",
        started: started_tx,
        release_first: Notify::new(),
    });
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider.clone()),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_calls(vec![
            (
                "call-first",
                "bash",
                serde_json::json!({"command": "first", "slot": "first"}),
            ),
            (
                "call-second",
                "bash",
                serde_json::json!({"command": "second", "slot": "second"}),
            ),
        ]),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client;
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let host = RuntimeHost::new(Arc::new(
        CanonicalProviderRuntimeFactory::new(config, provider_client).with_tool_router(router),
    ));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "bash-hold-serialize"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "serialize bash",
    )
    .await
    .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "first"
    );
    assert!(started_rx.try_recv().is_err());
    tool_provider.release_first.notify_one();
    assert_eq!(
        timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "second"
    );
    assert_output(&mut events, "final reply").await;
}

#[tokio::test]
async fn suspended_batch_finishes_and_appends_other_members_before_turn_waits() {
    let tool_provider: Arc<dyn AgentKernelToolProvider> = Arc::new(ImmediateThreadToolProvider);
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![response_tool_calls(
        vec![
            (
                "call-wait",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a"}),
            ),
            (
                "call-finish",
                "thread_status",
                serde_json::json!({"task_name": "worker-b"}),
            ),
        ],
    )]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(config, provider_client).with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "hold-suspension"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    append_tool_controller_bind_receipt(&store, &thread.context().coordinates, "thread_submit")
        .await;
    append_witnessed_tool_suspension(
        &store,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call-wait",
        "approval-1",
    )
    .await;
    let mut status = thread.subscribe_status();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "mixed batch",
    )
    .await
    .unwrap();
    wait_for_thread_event(
        &store,
        &thread.context().coordinates,
        EventKind::TurnWaiting,
    )
    .await;
    wait_for_status(&mut status, crate::ThreadStatus::Idle).await;

    assert_eq!(client.requests().len(), 1);
    let session = thread.session_context().await.unwrap();
    assert!(session.messages.iter().any(|message| {
        matches!(
            message,
            CanonicalMessage::ToolResult {
                tool_call_id,
                is_error: false,
                ..
            } if tool_call_id == "call-finish"
        )
    }));
    assert!(session.messages.iter().all(|message| {
        !matches!(
            message,
            CanonicalMessage::ToolResult { tool_call_id, .. } if tool_call_id == "call-wait"
        )
    }));
}

#[tokio::test]
async fn provider_waits_for_every_suspended_batch_member_before_continuing() {
    let tool_provider: Arc<dyn AgentKernelToolProvider> = Arc::new(ImmediateThreadToolProvider);
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_calls(vec![
            (
                "call-first",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a"}),
            ),
            (
                "call-second",
                "thread_submit",
                serde_json::json!({"task_name": "worker-b"}),
            ),
        ]),
        response_text("all suspended calls resumed"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                provider_client,
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "all-tools-suspended"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    append_tool_controller_bind_receipt(&store, &thread.context().coordinates, "thread_submit")
        .await;
    for (call_id, approval_id) in [
        ("call-first", "approval-first"),
        ("call-second", "approval-second"),
    ] {
        append_witnessed_tool_suspension(
            &store,
            &thread.context().coordinates,
            "snapshot-controller",
            "turn-1",
            call_id,
            approval_id,
        )
        .await;
    }
    let mut status = thread.subscribe_status();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "suspend both calls",
    )
    .await
    .unwrap();
    wait_for_thread_event(
        &store,
        &thread.context().coordinates,
        EventKind::TurnWaiting,
    )
    .await;
    wait_for_status(&mut status, crate::ThreadStatus::Idle).await;
    for call_id in ["call-first", "call-second"] {
        append_witnessed_tool_decision(
            &store,
            &thread.context().coordinates,
            "snapshot-controller",
            "turn-1",
            call_id,
            ToolCallDecisionOutcomePayload::Allow,
        )
        .await;
    }

    host.resume_tool_call(
        thread.context().coordinates.thread_id,
        "turn-1",
        "call-first",
    )
    .await
    .unwrap();
    wait_for_tool_call_completion(
        &store,
        &thread.context().coordinates,
        "turn-1",
        "call-first",
    )
    .await;
    wait_for_status(&mut status, crate::ThreadStatus::Idle).await;
    assert_eq!(
        client.requests().len(),
        1,
        "the round barrier must remain closed while a sibling has no result"
    );

    host.resume_tool_call(
        thread.context().coordinates.thread_id,
        "turn-1",
        "call-second",
    )
    .await
    .unwrap();
    assert_output(&mut events, "all suspended calls resumed").await;
    assert_eq!(client.requests().len(), 2);
}

#[tokio::test]
async fn failed_tool_call_does_not_cancel_independent_sibling() {
    let tool_provider: Arc<dyn AgentKernelToolProvider> = Arc::new(IsolatedFailureToolProvider);
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_calls(vec![
            (
                "call-fail",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a", "fail": true}),
            ),
            (
                "call-ok",
                "thread_submit",
                serde_json::json!({"task_name": "worker-b"}),
            ),
        ]),
        response_text("final reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client;
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let host = RuntimeHost::new(Arc::new(
        CanonicalProviderRuntimeFactory::new(config, provider_client).with_tool_router(router),
    ));
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "hold-failure-isolation"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "mixed result",
    )
    .await
    .unwrap();
    assert_output(&mut events, "final reply").await;

    let results = thread
        .session_context()
        .await
        .unwrap()
        .messages
        .into_iter()
        .filter_map(|message| match message {
            CanonicalMessage::ToolResult {
                tool_call_id,
                is_error,
                ..
            } => Some((tool_call_id, is_error)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        results,
        vec![
            ("call-fail".to_string(), true),
            ("call-ok".to_string(), false)
        ]
    );
}

#[tokio::test]
async fn failed_conflicting_tool_releases_its_hold_for_the_next_call() {
    let tool_provider: Arc<dyn AgentKernelToolProvider> = Arc::new(IsolatedFailureToolProvider);
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_calls(vec![
            (
                "call-fail",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a", "fail": true}),
            ),
            (
                "call-after",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a"}),
            ),
        ]),
        response_text("final reply"),
    ]));
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                client,
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "hold-error-release"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "release failed hold",
    )
    .await
    .unwrap();
    assert_output(&mut events, "final reply").await;

    let session = thread.session_context().await.unwrap();
    let results = session
        .messages
        .iter()
        .filter_map(|message| match message {
            CanonicalMessage::ToolResult {
                tool_call_id,
                is_error,
                ..
            } => Some((tool_call_id.as_str(), *is_error)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(results, vec![("call-fail", true), ("call-after", false)]);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn interrupt_mid_batch_witnesses_acknowledged_exceeded_and_never_launched_calls() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (acknowledged_tx, mut acknowledged_rx) = mpsc::unbounded_channel();
    let acknowledging_provider = Arc::new(CancellationAcknowledgingThreadToolProvider {
        started: started_tx.clone(),
        acknowledged: acknowledged_tx,
    });
    let non_observing_provider = Arc::new(NonObservingThreadToolProvider {
        started: started_tx,
        released: AtomicBool::new(false),
        release: Notify::new(),
        never_launched: AtomicBool::new(true),
    });
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(acknowledging_provider)
            .with_kernel_tool_provider(non_observing_provider.clone()),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_calls(vec![
            (
                "call-acknowledged",
                "thread_submit",
                serde_json::json!({"task_name": "worker-a"}),
            ),
            (
                "call-exceeded",
                "thread_status",
                serde_json::json!({"task_name": "worker-b"}),
            ),
            (
                "call-never-launched",
                "thread_wait",
                serde_json::json!({"task_name": "worker-b"}),
            ),
        ]),
        response_text("replacement reply"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client;
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(config, provider_client).with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread_with_topology_and_metadata(
            ThreadCoordinates::new("tenant_a", "user_1", "interrupt-tool-batch"),
            ThreadTopology::root(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    append_manifest_runtime_grace(&store, &thread.context().coordinates, 100).await;
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "interrupt batch",
    )
    .await
    .unwrap();
    let mut started = vec![
        started_rx.recv().await.unwrap(),
        started_rx.recv().await.unwrap(),
    ];
    started.sort();
    assert_eq!(started, vec!["call-acknowledged", "call-exceeded"]);
    assert!(non_observing_provider.never_launched.load(Ordering::SeqCst));

    host.submit_with_mode(
        thread.context().coordinates.thread_id,
        "turn-replacement",
        "replacement",
        TurnSubmissionMode::Interrupt,
    )
    .await
    .unwrap();
    assert_eq!(
        acknowledged_rx.recv().await.as_deref(),
        Some("call-acknowledged")
    );
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_millis(99)).await;
    tokio::task::yield_now().await;
    assert!(
        !drain_has_cancelled(&mut events),
        "the turn terminal must remain blocked until the configured grace"
    );

    tokio::time::advance(Duration::from_millis(1)).await;
    let mut saw_cancelled = false;
    for _ in 0..100 {
        tokio::task::yield_now().await;
        saw_cancelled |= drain_has_cancelled(&mut events);
        if saw_cancelled {
            break;
        }
    }
    assert!(saw_cancelled, "interrupt did not settle at grace");

    let before_detached_settlement = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let requests = before_detached_settlement
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallRequested)
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|event| !event.payload["holds"].is_null())
    );
    let completed_before_release = before_detached_settlement
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
        .map(|event| {
            (
                event.payload["subject"]["call_id"]
                    .as_str()
                    .unwrap()
                    .to_string(),
                event.payload["cancellation"].as_str().map(str::to_string),
                event.payload["success"].as_bool().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completed_before_release,
        vec![
            (
                "call-acknowledged".to_string(),
                Some("cancelled_acknowledged".to_string()),
                false,
            ),
            (
                "call-never-launched".to_string(),
                Some("cancelled_acknowledged".to_string()),
                false,
            ),
        ]
    );

    non_observing_provider.release();
    wait_for_tool_completion_count(&store, &thread.context().coordinates, 3).await;
    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    let exceeded = records
        .iter()
        .find(|event| {
            event.kind == EventKind::ToolCallCompleted
                && event.payload["subject"]["call_id"] == "call-exceeded"
        })
        .expect("detached invocation did not settle its own completion");
    assert_eq!(
        exceeded.payload["cancellation"],
        serde_json::json!("cancelled_exceeded_grace")
    );
    assert_eq!(exceeded.payload["success"], true);
    assert!(non_observing_provider.never_launched.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invocation_panic_after_grace_still_self_settles_exactly_once() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let tool_provider = Arc::new(PanickingAfterGraceToolProvider {
        started: started_tx,
        release: Notify::new(),
    });
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider.clone()),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("thread_status", serde_json::json!({"task_name": "worker"})),
    ]));
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                client,
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "panic-after-grace"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    append_manifest_runtime_grace(&store, &thread.context().coordinates, 100).await;
    let mut events = thread.subscribe_events();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "panic")
        .await
        .unwrap();
    started_rx.recv().await.unwrap();
    host.cancel(
        thread.context().coordinates.thread_id,
        "cancel panicking tool",
    )
    .await
    .unwrap();
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_cancelled(&mut events, "cancel panicking tool").await;

    tool_provider.release.notify_waiters();
    wait_for_tool_completion_count(&store, &thread.context().coordinates, 1).await;
    let completions = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
        .map(|event| serde_json::from_value::<ToolCallCompletedPayload>(event.payload).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(completions.len(), 1);
    assert!(!completions[0].success);
    assert_eq!(
        completions[0].cancellation,
        Some(ToolCallCancellation::CancelledExceededGrace)
    );
}

#[tokio::test]
async fn invocation_panic_before_cancellation_is_a_failed_completion() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let tool_provider = Arc::new(PanickingAfterGraceToolProvider {
        started: started_tx,
        release: Notify::new(),
    });
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider.clone()),
    );
    let client: Arc<dyn ProviderClient> = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("thread_status", serde_json::json!({"task_name": "worker"})),
    ]));
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                client,
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "panic-before-cancel"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();

    host.submit(thread.context().coordinates.thread_id, "turn-1", "panic")
        .await
        .unwrap();
    started_rx.recv().await.unwrap();
    tool_provider.release.notify_waiters();
    wait_for_tool_completion_count(&store, &thread.context().coordinates, 1).await;

    let completion = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.kind == EventKind::ToolCallCompleted)
        .map(|event| serde_json::from_value::<ToolCallCompletedPayload>(event.payload).unwrap())
        .unwrap();
    assert!(!completion.success);
    assert_eq!(completion.cancellation, None);
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn monitor_panic_after_settlement_recovers_one_completion() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let (acknowledged_tx, mut acknowledged_rx) = mpsc::unbounded_channel();
    let tool_provider: Arc<dyn AgentKernelToolProvider> =
        Arc::new(CancellationAcknowledgingThreadToolProvider {
            started: started_tx,
            acknowledged: acknowledged_tx,
        });
    let router = Arc::new(
        AgentToolRouter::new(Arc::new(OperationRegistry::new()))
            .with_kernel_tool_provider(tool_provider),
    );
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named("thread_submit", serde_json::json!({"task_name": "worker"})),
    ]));
    let inner = Arc::new(InMemorySessionStore::new());
    let store = Arc::new(FaultingRuntimeStore::new(inner.clone()));
    let host = RuntimeHost::with_session_store(
        Arc::new(
            CanonicalProviderRuntimeFactory::new(
                CanonicalProviderRuntimeConfig::new(
                    ProviderApi::OpenAIResponses,
                    "openai",
                    "gpt-test",
                ),
                client,
            )
            .with_tool_router(router),
        ),
        store.clone(),
    );
    let thread = host
        .start_thread(
            ThreadCoordinates::new("tenant_a", "user_1", "monitor-panic"),
            ThreadTopology::root(),
        )
        .await
        .unwrap();
    let mut events = thread.subscribe_events();

    host.submit(
        thread.context().coordinates.thread_id,
        "turn-1",
        "interrupt",
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    store.panic_next("build_context", "monitor settlement read");
    host.cancel(
        thread.context().coordinates.thread_id,
        "cancel monitor panic",
    )
    .await
    .unwrap();
    timeout(Duration::from_secs(2), acknowledged_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_cancelled(&mut events, "cancel monitor panic").await;

    let records = inner
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|event| {
                event.kind == EventKind::ToolCallCompleted
                    && event.payload["subject"]["call_id"] == "call_1|fc_1"
            })
            .count(),
        1
    );
    let context = inner
        .build_context(&thread.context().coordinates)
        .await
        .unwrap();
    assert_eq!(
        context
            .messages
            .iter()
            .filter(|message| matches!(
                message,
                CanonicalMessage::ToolResult { tool_call_id, .. }
                    if tool_call_id == "call_1|fc_1"
            ))
            .count(),
        1
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn detached_completion_retry_is_idempotent_before_and_after_a_store_failure() {
    for fail_after_append in [false, true] {
        let inner = Arc::new(InMemorySessionStore::new());
        let coordinates = ThreadCoordinates::new(
            "tenant_a",
            "user_1",
            if fail_after_append {
                "detached-fail-after"
            } else {
                "detached-fail-before"
            },
        );
        let request = inner
            .append_events(
                &EventStreamId::for_thread(&coordinates),
                vec![NewEventRecord::discharged(
                    coordinates.clone(),
                    EventKind::ToolCallRequested,
                    serde_json::to_value(ToolCallRequestedPayload {
                        subject: ToolCallSubject {
                            turn_id: "turn-1".to_string(),
                            call_id: "call-1".to_string(),
                        },
                        snapshot_id: "snapshot-1".to_string(),
                        tool_name: "thread_status".to_string(),
                        arguments: serde_json::json!({}),
                        holds: Vec::new(),
                    })
                    .unwrap(),
                    EventProvenance {
                        source_streams: vec![EventStreamId::for_thread(&coordinates)],
                        discharged_by: Some("test:detached-retry".to_string()),
                        function: Some("tool_request/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap()
            .pop()
            .unwrap();
        inner
            .append_events(
                &EventStreamId::for_thread(&coordinates),
                vec![NewEventRecord::witnessed(
                    coordinates.clone(),
                    EventKind::ToolCallCompleted,
                    serde_json::json!({
                        "subject": {"turn_id": "unrelated-turn"},
                        "malformed": true
                    }),
                )],
            )
            .await
            .unwrap();
        let faulting = FaultingRuntimeStore::new(inner.clone());
        let faulting = if fail_after_append {
            faulting.fail_nth_after(
                "append_events_fenced",
                1,
                "completion append failed after commit",
            )
        } else {
            faulting.fail_nth(
                "append_events_fenced",
                1,
                "completion append failed before commit",
            )
        };
        let services = RuntimeServices::new(Arc::new(faulting), RuntimeExecutionPolicy::default());
        let turn_context = TurnContext::new(
            ThreadContext::root(coordinates.clone()),
            "turn-1",
            &TurnInput::text(""),
            CancellationToken::new(),
        );
        let (events, mut event_rx) = broadcast::channel(16);
        let append = tokio::spawn({
            let services = services.clone();
            let turn_context = turn_context.clone();
            async move {
                append_detached_tool_call_outcome_until_recorded(
                    &services,
                    &turn_context,
                    coordinates.thread_id,
                    &events,
                    Ok(PreparedToolCallOutcome::Completed {
                        call_id: "call-1".to_string(),
                        tool_name: "thread_status".to_string(),
                        snapshot_id: "snapshot-1".to_string(),
                        source_event_id: request.id,
                        finish_order: 0,
                        cancellation: Some(ToolCallCancellation::CancelledExceededGrace),
                        outcome: Box::new(ToolExecutionOutcome {
                            result: CanonicalMessage::tool_result(
                                "call-1",
                                "thread_status",
                                "cancelled after grace",
                                true,
                            ),
                            hook_records: Vec::new(),
                            pre_model_contexts: Vec::new(),
                            post_model_contexts: Vec::new(),
                            permission_decision: None,
                            duration_ms: 0,
                        }),
                    }),
                )
                .await;
            }
        });
        loop {
            if matches!(
                event_rx.recv().await.unwrap(),
                ThreadEvent::Runtime {
                    event: RuntimeEvent {
                        kind: RuntimeEventKind::Recovery { ref action, .. },
                        ..
                    },
                    ..
                } if action == "retry_detached_tool_completion"
            ) {
                break;
            }
        }
        tokio::time::advance(DETACHED_COMPLETION_RETRY_DELAY).await;
        append.await.unwrap();

        let completions = inner
            .read_events(&EventStreamId::for_thread(&coordinates), None)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.kind == EventKind::ToolCallCompleted
                    && event.payload["subject"]["turn_id"] == "turn-1"
                    && event.payload["subject"]["call_id"] == "call-1"
            })
            .count();
        let results = inner
            .build_context(&coordinates)
            .await
            .unwrap()
            .entries
            .into_iter()
            .filter(|entry| {
                matches!(
                    &entry.kind,
                    SessionEntryKind::Message {
                        message: CanonicalMessage::ToolResult { tool_call_id, .. }
                    } if tool_call_id == "call-1"
                )
            })
            .count();
        assert_eq!(completions, 1);
        assert_eq!(results, 1);
    }
}

#[tokio::test]
async fn completion_append_is_subject_idempotent_under_concurrency() {
    let store = Arc::new(InMemorySessionStore::new());
    let services = RuntimeServices::new(store.clone(), RuntimeExecutionPolicy::default());
    let coordinates = ThreadCoordinates::new("tenant_a", "user_1", "completion-race");
    let append = || {
        append_tool_completion_event(
            &services,
            &coordinates,
            "turn-1".to_string(),
            "call-1".to_string(),
            "snapshot-1".to_string(),
            "thread_status".to_string(),
            false,
            Some(0),
            Some(0),
            Some(ToolCallCancellation::CancelledExceededGrace),
        )
    };

    let (left, right) = tokio::join!(append(), append());
    left.unwrap();
    right.unwrap();

    let completions = store
        .read_events(&EventStreamId::for_thread(&coordinates), None)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
        .count();
    assert_eq!(completions, 1);
}

#[tokio::test]
async fn resume_sweep_settles_only_dangling_calls_from_the_full_cancelled_turn_window() {
    let store = Arc::new(InMemorySessionStore::new());
    let parent_coordinates = ThreadCoordinates::new("tenant_a", "user_1", "cancel-sweep-parent");
    let child_coordinates = ThreadCoordinates::new("tenant_a", "user_1", "cancel-sweep-child");
    let turn_submitted = store
        .append_events(
            &EventStreamId::for_thread(&child_coordinates),
            vec![NewEventRecord::witnessed(
                child_coordinates.clone(),
                EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": "turn-cancelled"}),
            )],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let request = |call_id: &str| {
        NewEventRecord::discharged(
            child_coordinates.clone(),
            EventKind::ToolCallRequested,
            serde_json::to_value(ToolCallRequestedPayload {
                subject: ToolCallSubject {
                    turn_id: "turn-cancelled".to_string(),
                    call_id: call_id.to_string(),
                },
                snapshot_id: "snapshot-cancelled".to_string(),
                tool_name: "thread_status".to_string(),
                arguments: serde_json::json!({"task_name": "worker"}),
                holds: Vec::new(),
            })
            .unwrap(),
            EventProvenance {
                source_streams: vec![EventStreamId::for_thread(&child_coordinates)],
                source_event_ids: vec![turn_submitted.id],
                discharged_by: Some("test:cancel-sweep".to_string()),
                function: Some("tool_request/v1".to_string()),
                ..EventProvenance::default()
            },
        )
    };
    let requests = store
        .append_events(
            &EventStreamId::for_thread(&child_coordinates),
            vec![
                request("call-dangling"),
                request("call-dangling"),
                request("call-already-completed"),
            ],
        )
        .await
        .unwrap();
    store
        .append_with_provenance(
            &child_coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::tool_result(
                    "call-dangling",
                    "thread_status",
                    "result persisted before the completion fact",
                    false,
                ),
            },
            EventProvenance {
                source_streams: vec![EventStreamId::for_thread(&child_coordinates)],
                source_event_ids: vec![requests[0].id],
                discharged_by: Some("test:partial-detached-append".to_string()),
                function: Some("session_entry_append/v1".to_string()),
                ..EventProvenance::default()
            },
        )
        .await
        .unwrap();
    store
        .append_events(
            &EventStreamId::new(format!("control:{}", parent_coordinates.thread_id)),
            vec![
                NewEventRecord::witnessed(
                    parent_coordinates.clone(),
                    EventKind::ThreadJoined,
                    serde_json::json!({"malformed": "unrelated legacy join"}),
                ),
                NewEventRecord::discharged(
                    parent_coordinates.clone(),
                    EventKind::ThreadJoined,
                    serde_json::json!({
                        "child_thread_id": child_coordinates.thread_id,
                        "terminal_state": "cancelled"
                    }),
                    EventProvenance {
                        source_streams: vec![EventStreamId::for_thread(&child_coordinates)],
                        source_event_ids: vec![turn_submitted.id],
                        discharged_by: Some("test:interrupt".to_string()),
                        function: Some("thread_join/v1".to_string()),
                        ..EventProvenance::default()
                    },
                ),
            ],
        )
        .await
        .unwrap();
    store
        .append_events(
            &EventStreamId::for_thread(&child_coordinates),
            vec![NewEventRecord::discharged(
                child_coordinates.clone(),
                EventKind::ToolCallCompleted,
                serde_json::to_value(ToolCallCompletedPayload {
                    subject: ToolCallSubject {
                        turn_id: "turn-cancelled".to_string(),
                        call_id: "call-already-completed".to_string(),
                    },
                    snapshot_id: "snapshot-cancelled".to_string(),
                    tool_name: "thread_status".to_string(),
                    success: true,
                    duration_ms: Some(7),
                    finish_order: Some(4),
                    cancellation: Some(ToolCallCancellation::CancelledExceededGrace),
                })
                .unwrap(),
                EventProvenance {
                    source_streams: vec![EventStreamId::for_thread(&child_coordinates)],
                    source_event_ids: vec![requests[2].id],
                    discharged_by: Some("test:late-detached-completion".to_string()),
                    function: Some("tool_result/v1".to_string()),
                    ..EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    store
        .append_events(
            &EventStreamId::for_thread(&child_coordinates),
            vec![
                NewEventRecord::witnessed(
                    child_coordinates.clone(),
                    EventKind::ToolCallRequested,
                    serde_json::json!({
                        "subject": {"turn_id": "unrelated-turn"},
                        "malformed": true
                    }),
                ),
                NewEventRecord::witnessed(
                    child_coordinates.clone(),
                    EventKind::ToolCallCompleted,
                    serde_json::json!({
                        "subject": {"turn_id": "unrelated-turn"},
                        "malformed": true
                    }),
                ),
            ],
        )
        .await
        .unwrap();

    let client: Arc<dyn ProviderClient> = Arc::new(RecordingClient::default());
    let host = RuntimeHost::with_session_store(
        Arc::new(CanonicalProviderRuntimeFactory::new(
            CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test"),
            client,
        )),
        store.clone(),
    );
    let child = host
        .load_thread_with_topology_and_metadata(
            child_coordinates.clone(),
            ThreadTopology::spawned_from(parent_coordinates.thread_id),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    wait_for_tool_completion_count(&store, &child_coordinates, 3).await;

    let completed = store
        .read_events(&EventStreamId::for_thread(&child_coordinates), None)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
        .filter_map(|event| serde_json::from_value::<ToolCallCompletedPayload>(event.payload).ok())
        .collect::<Vec<_>>();
    assert_eq!(
        completed.len(),
        2,
        "the late completion must not be duplicated"
    );
    let recovered = completed
        .iter()
        .find(|payload| payload.subject.call_id == "call-dangling")
        .unwrap();
    assert!(
        recovered.success,
        "recovery must preserve the persisted natural tool outcome"
    );
    assert_eq!(
        recovered.cancellation,
        Some(ToolCallCancellation::CancelledExceededGrace)
    );
    assert_eq!(recovered.finish_order, Some(5));
    assert_eq!(
        child
            .session_context()
            .await
            .unwrap()
            .messages
            .iter()
            .filter(|message| matches!(
                message,
                CanonicalMessage::ToolResult { tool_call_id, .. }
                    if tool_call_id == "call-dangling"
            ))
            .count(),
        1,
        "recovery must reuse a canonical result persisted before its completion fact"
    );
    let _ = child;
    host.shutdown_all().await.unwrap();
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
    let submitted = records
        .iter()
        .find(|event| {
            event.kind == EventKind::TurnSubmitted
                && event.origin == crate::EventOrigin::Witnessed
                && event.payload["turn_id"].as_str() == Some("turn-1")
        })
        .expect("turn submission should be durable");
    let assistant_session_entry = records
        .iter()
        .find(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.origin == crate::EventOrigin::Discharged
                && event.provenance.source_event_ids == vec![submitted.id]
        })
        .expect("assistant session entry should cite the submitted turn");
    assert_ne!(assistant_session_entry.id, submitted.id);
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
    assert!(records.iter().any(|event| {
        event.kind == EventKind::SessionEntryAppended
            && event.origin == crate::EventOrigin::Discharged
            && event.provenance.source_event_ids == vec![request.id]
    }));
    let completed = records
        .iter()
        .find(|event| event.kind == EventKind::ToolCallCompleted)
        .expect("tool completion should be durable");
    assert_eq!(completed.origin, crate::EventOrigin::Witnessed);
    assert_eq!(completed.payload["tool_name"].as_str(), Some("echo_search"));
    assert_eq!(completed.payload["success"].as_bool(), Some(true));
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
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
    let request = records
        .iter()
        .find(|event| event.kind == EventKind::ToolCallRequested)
        .expect("resumed call request");
    assert_eq!(
        request.payload["holds"],
        serde_json::json!([{"key": {"kind": "global"}, "access": "exclusive"}])
    );
    let completion = records
        .iter()
        .find(|event| event.kind == EventKind::ToolCallCompleted)
        .expect("resumed call completion");
    assert_eq!(completion.payload["finish_order"], 0);
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
    host.shutdown_thread(thread.context().coordinates.thread_id)
        .await
        .unwrap();
    assert_eq!(
        client.requests().len(),
        2,
        "duplicate resume must not invoke or continue the tool twice"
    );
}

#[tokio::test]
async fn suspended_batch_counts_as_one_round_when_the_turn_resumes() {
    let registry = echo_registry("echo").await;
    let client = Arc::new(RecordingClient::with_responses(vec![
        response_tool_call_named_with_id(
            "call-wait",
            "echo_search",
            serde_json::json!({"input": "first"}),
        ),
        response_tool_call_named_with_id(
            "call-over-budget",
            "echo_search",
            serde_json::json!({"input": "second"}),
        ),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let store = Arc::new(InMemorySessionStore::new());
    let host = RuntimeHost::with_session_store(
        runtime_factory_with_registry(provider_client, registry),
        store.clone(),
    );
    let thread = host
        .start_thread_with_topology_and_metadata(
            ThreadCoordinates::new("tenant_a", "user_1", "resume-round-budget"),
            ThreadTopology::root(),
            BTreeMap::from([(
                THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA.to_string(),
                "1".to_string(),
            )]),
        )
        .await
        .unwrap();
    append_tool_controller_bind_receipt(&store, &thread.context().coordinates, "echo_search").await;
    append_witnessed_tool_suspension(
        &store,
        &thread.context().coordinates,
        "snapshot-controller",
        "turn-1",
        "call-wait",
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
        "call-wait",
        ToolCallDecisionOutcomePayload::Allow,
    )
    .await;
    host.resume_tool_call(
        thread.context().coordinates.thread_id,
        "turn-1",
        "call-wait",
    )
    .await
    .unwrap();

    assert_failed_with_runtime_events(&mut events, "tool router exceeded 1 rounds").await;
    assert_eq!(client.requests().len(), 2);
    let records = store
        .read_events(
            &EventStreamId::for_thread(&thread.context().coordinates),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallRequested)
            .count(),
        1
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
                "dispatch_id": "model-supplied-id-must-not-win",
            }),
        ),
        response_text("spawned child"),
    ]));
    let provider_client: Arc<dyn ProviderClient> = client.clone();
    let mut config =
        CanonicalProviderRuntimeConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
    config.max_tokens = 128;
    let root_factory = CanonicalProviderRuntimeFactory::new(config, provider_client)
        .with_tool_router(Arc::new(kernel_thread_router().await))
        .with_thread_spawn_agent_resolver(Arc::new(StaticThreadSpawnAgentResolver));
    let host = RuntimeHost::new(Arc::new(RootProviderChildEchoFactory {
        root: Arc::new(root_factory),
    }));
    let store = host.runtime_store();
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
            } if call_id == "call_1|fc_1"
                && output.contains(r#""operation":"cooldis.thread_spawn""#)
                && output.contains(r#""task_name":"worker""#)
                && !output.contains("thread_id")
                && !output.contains("handle")
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
    let requested = wait_for_control_event(
        store.as_ref(),
        &thread.context().coordinates,
        EventKind::ThreadSpawnRequested,
    )
    .await;
    let requested_payload: crate::ThreadSpawnRequestedPayload =
        serde_json::from_value(requested.payload.clone()).unwrap();
    assert_eq!(requested_payload.correlation_id, "call_1|fc_1");
    assert_eq!(requested_payload.child_agent_ref, CHILD_AGENT_REF);
    let spawned = wait_for_control_event(
        store.as_ref(),
        &thread.context().coordinates,
        EventKind::ThreadSpawned,
    )
    .await;
    let spawned_payload: ThreadSpawnedPayload =
        serde_json::from_value(spawned.payload.clone()).unwrap();
    assert_eq!(
        spawned_payload.parent_thread_id,
        thread.context().coordinates.thread_id
    );
    assert_eq!(
        spawned_payload.child_thread_id,
        children[0].context().coordinates.thread_id
    );
    assert_eq!(spawned_payload.child_manifest_hash, CHILD_MANIFEST_HASH);
    assert_eq!(spawned_payload.granted, vec!["threads.read".to_string()]);
    assert!(spawned_payload.inputs_hash.starts_with("sha256:"));

    let joined = wait_for_control_event(
        store.as_ref(),
        &thread.context().coordinates,
        EventKind::ThreadJoined,
    )
    .await;
    let joined_payload: ThreadJoinedPayload =
        serde_json::from_value(joined.payload.clone()).unwrap();
    assert_eq!(
        joined_payload.child_thread_id,
        spawned_payload.child_thread_id
    );
    assert_eq!(joined_payload.spawned_event_id, spawned.id);
    assert_eq!(
        joined_payload.terminal_state,
        ThreadTerminalState::Completed
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
        skill_packages: Vec::new(),
        skill_discovery: None,
        static_context_segments: Vec::new(),
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
        placement: None,
        workspace: None,
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

async fn append_manifest_runtime_grace(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    cancellation_grace_ms: u64,
) {
    let receipt = AgentManifestBindReceipt {
        ref_uri: "agent://test/interruption".to_string(),
        manifest_hash: "snapshot-interruption".to_string(),
        model_profile_id: "default".to_string(),
        provider_id: "test".to_string(),
        model_id: "model".to_string(),
        tool_ids: Vec::new(),
        operation_bindings: Vec::new(),
        skill_packages: Vec::new(),
        skill_discovery: None,
        static_context_segments: Vec::new(),
        tool_universes: Vec::new(),
        couplings: Vec::new(),
        granted: Vec::new(),
        effective_runtime: AgentManifestRuntimeDefaults {
            cancellation_grace_ms: Some(cancellation_grace_ms),
            ..AgentManifestRuntimeDefaults::default()
        },
        overridden_keys: vec!["cancellation_grace_ms".to_string()],
        placement: None,
        workspace: None,
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
                    admissible: None,
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

async fn wait_for_tool_call_completion(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    turn_id: &str,
    call_id: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let records = store
            .read_events(&EventStreamId::for_thread(coordinates), None)
            .await
            .unwrap();
        if records.iter().any(|event| {
            event.kind == EventKind::ToolCallCompleted
                && event.payload["subject"]["turn_id"].as_str() == Some(turn_id)
                && event.payload["subject"]["call_id"].as_str() == Some(call_id)
        }) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for tool completion {turn_id}/{call_id}"
        );
        tokio::task::yield_now().await;
    }
}

async fn wait_for_control_event<S: EventStore + ?Sized>(
    store: &S,
    coordinates: &ThreadCoordinates,
    kind: EventKind,
) -> crate::EventRecord {
    let stream_id = EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let records = store.read_events(&stream_id, None).await.unwrap();
        if let Some(record) = records.into_iter().find(|event| event.kind == kind) {
            return record;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for control event kind {kind}"
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
        let store = Arc::new(SqliteSessionStore::open(&path).await.unwrap());
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
    let store = Arc::new(SqliteSessionStore::open(&path).await.unwrap());
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
        let store = Arc::new(SqliteSessionStore::open(&path).await.unwrap());
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

    let reopened = SqliteSessionStore::open(&path).await.unwrap();
    let stream_id = EventStreamId::for_thread(&coordinates);
    let events = reopened.read_events(&stream_id, None).await.unwrap();
    let session_events = events
        .iter()
        .filter(|event| {
            event.kind == EventKind::SessionEntryAppended
                && event.payload.get("runtime_kind").and_then(Value::as_str)
                    != Some("thread_started")
        })
        .collect::<Vec<_>>();
    let compile_events = events
        .iter()
        .filter(|event| event.kind == EventKind::ContextCompileCompleted)
        .collect::<Vec<_>>();
    assert_eq!(session_events.len(), 2, "{events:?}");
    assert_eq!(compile_events.len(), 1, "{events:?}");
    assert_eq!(compile_events[0].payload["turn_id"], "turn-1");

    let observations = reopened
        .list_observations(&coordinates, Some("compiled_context_receipt"))
        .await
        .unwrap();
    assert_eq!(observations.len(), 1);
    let receipt = &observations[0];
    assert_eq!(receipt.payload["turn_id"], "turn-1");
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

fn drain_has_cancelled(events: &mut broadcast::Receiver<ThreadEvent>) -> bool {
    let mut cancelled = false;
    while let Ok(event) = events.try_recv() {
        cancelled |= matches!(event, ThreadEvent::Cancelled { .. });
    }
    cancelled
}

async fn wait_for_tool_completion_count(
    store: &InMemorySessionStore,
    coordinates: &ThreadCoordinates,
    expected: usize,
) {
    for _ in 0..100 {
        let count = store
            .read_events(&EventStreamId::for_thread(coordinates), None)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == EventKind::ToolCallCompleted)
            .count();
        if count == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("tool completion count did not reach {expected}");
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
